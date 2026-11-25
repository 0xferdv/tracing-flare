//! Service 注册表与 target 路由.
//!
//! 设计原理：
//! - 路由表用 `ArcSwap` 发布：读路径（`route/route_name`）无锁，
//!   写路径只在创建动态 service 时复制整表；
//! - target 按路径边界匹配前缀，避免 `app` 误匹配 `application`；
//! - `names` 提供精确名字查找，供 `Logger` 门面路由动态名称；
//! - 每个 service 有自己的 writer/文件句柄/实例锁，动态创建受 `max_dynamic_services` 约束.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::engine::console::ConsoleSink;
use crate::engine::level::Level;
use crate::engine::runtime::{Logger, LoggerBuilder, Sink, SinkConfig};
use crate::engine::sink::{FileSink, FileSinkOptions};
use arc_swap::ArcSwap;

use crate::config::{FlareCompression, FlareRotation, ServiceConfig, UnknownTarget};
use crate::error::FlareError;
use crate::format::TextFormat;
use crate::guard::FlareGuard;
use crate::layer::FlareLayer;
use crate::level::FlareLevel;
use crate::lock::{acquire_instance_lock, release_instance_lock, InstanceLock};
use crate::stats::FlareStats;

pub(crate) struct RouteTable {
    /// 前缀路由表，按前缀长度降序排列，命中第一个路径边界匹配.
    pub(crate) routes: Vec<(String, Arc<ServiceEntry>)>,
    /// 精确 service 名/target 查找，供动态 Logger 门面使用，避免每次 O(services) 扫描.
    pub(crate) names: std::collections::HashMap<String, Arc<ServiceEntry>>,
    /// 未匹配 target 时的 fallback service.
    pub(crate) fallback: Option<Arc<ServiceEntry>>,
}

/// 在路由表中查找 target.
///
/// 参数：`table` 是当前路由表；`target` 是 tracing target.
/// 返回值：第一个前缀匹配或 fallback.
/// 匹配规则：完全相等，或 `target` 以 `prefix` 开头且剩余部分以 `.`/`::`/`/` 开始，
/// 防止 `app` 误匹配 `application`.
fn route_in(table: &RouteTable, target: &str) -> Option<Arc<ServiceEntry>> {
    for (prefix, service) in &table.routes {
        let matches = target == prefix
            || target.strip_prefix(prefix).is_some_and(|rest| {
                rest.starts_with('.') || rest.starts_with("::") || rest.starts_with('/')
            });
        if matches {
            return Some(Arc::clone(service));
        }
    }
    table.fallback.clone()
}

/// 动态 service 创建锁的 stripe 数量.
///
/// 不同名字的首次创建可以并行；同一 stripe 的极少数碰撞只会短暂串行，不会造成重复创建.
const BUILD_LOCK_STRIPES: usize = 64;

/// 按名字选择动态创建锁.
///
/// 参数：`name` 是 service 名；返回值：固定 stripe 下标.
fn build_lock_index(name: &str) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % BUILD_LOCK_STRIPES
}

/// Service 注册/路由核心.
///
/// 字段说明：`state` 原子发布路由表；`generation` 供线程缓存失效；`entries` 是静态 service；
/// `dynamic` 是按需创建的服务；`base_path/template/create_enabled/max_dynamic_services` 是动态创建
/// 策略；`closed/forked` 是生命周期/fork 状态；`build_locks` 按名字分片创建锁；
/// `build_gate` 让 shutdown 等待在途创建；`publish_lock` 串行化路由表发布；`stats` 是 layer 计数；
/// `paths` 是已解析文件路径的 owner 表，构建期和动态创建都据此拒绝同 inode 双 writer.
pub(crate) struct Router {
    state: ArcSwap<RouteTable>,
    generation: AtomicU64,
    entries: Vec<Arc<ServiceEntry>>,
    dynamic: Mutex<HashMap<String, Arc<ServiceEntry>>>,
    base_path: PathBuf,
    template: ServiceConfig,
    create_enabled: bool,
    max_dynamic_services: usize,
    closed: AtomicBool,
    forked: AtomicBool,
    build_locks: [Mutex<()>; BUILD_LOCK_STRIPES],
    build_gate: RwLock<()>,
    publish_lock: Mutex<()>,
    reserved_builds: Mutex<usize>,
    paths: Mutex<HashMap<PathBuf, String>>,
    /// 是否输出 JSON Lines；传给静态与动态 service 的引擎 logger，用于批量格式守卫.
    json_lines: bool,
    pub(crate) stats: Arc<FlareStats>,
}

impl Router {
    /// 把 tracing target 解析为 service.
    ///
    /// 参数：`target` 是 tracing target；返回值：匹配 service 或 fallback.
    /// 读路径只做 ArcSwap load + 前缀扫描，不拿锁.
    pub(crate) fn route(&self, target: &str) -> Option<Arc<ServiceEntry>> {
        let table = self.state.load();
        route_in(&table, target)
    }

    /// 返回路由表版本.
    ///
    /// 参数：无；返回值：单调递增 generation.
    /// 生产线程缓存用它判断是否需要重新解析 target.
    #[inline]
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// 返回所有 service 快照.
    ///
    /// 参数：无；返回值：静态 + 动态 service 的 `Arc` 列表.
    /// 用于 shutdown/flush/health；fork 子进程优先 `try_lock`，避免继承锁死锁.
    pub(crate) fn all_entries(&self) -> Vec<Arc<ServiceEntry>> {
        let mut entries: Vec<Arc<ServiceEntry>> = self.entries.to_vec();
        if self.forked.load(Ordering::Acquire) {
            if let Ok(dynamic) = self.dynamic.try_lock() {
                entries.extend(dynamic.values().cloned());
            }
        } else {
            let dynamic = self
                .dynamic
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            entries.extend(dynamic.values().cloned());
        }
        entries
    }

    /// 是否至少存在一个 service.
    ///
    /// 参数：无；返回值：已发布路由表是否非空.
    /// 用于 reserved-target 事件的快速准入判断.
    /// 读路径只加载 `ArcSwap` 快照，不锁 `dynamic`：静态 service 与动态 service 在发布时都会写入
    /// `routes/names`，因此快照足以回答“是否存在落点”. 原先在静态表为空时读取 `dynamic` 会让每条
    /// Logger 门面事件争用同一把全局 Mutex，且该锁可能被慢速动态创建持有.
    #[inline]
    pub(crate) fn has_services(&self) -> bool {
        let table = self.state.load();
        !table.routes.is_empty() || table.fallback.is_some() || !table.names.is_empty()
    }

    /// 按精确名字/target 查找 service.
    ///
    /// 参数：`name` 是 service 名或 target；返回值：命中项或 `None`.
    pub(crate) fn find(&self, name: &str) -> Option<Arc<ServiceEntry>> {
        let table = self.state.load();
        table.names.get(name).cloned()
    }

    /// 按名称按需创建 service.
    ///
    /// 参数：`name` 是 service 名.
    /// 返回值：新/已存在 service 或创建错误.
    /// 该操作会做文件系统工作，只能从显式生命周期 API 调用，不能出现在 `on_event`.
    pub(crate) fn ensure_service(&self, name: &str) -> Result<Arc<ServiceEntry>, FlareError> {
        if self.forked.load(Ordering::Acquire) {
            return Err(FlareError::Config(
                "cannot create a dynamic service in a forked child".to_string(),
            ));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(FlareError::Config(
                "FlareGuard is shutting down; refusing to create a dynamic service".to_string(),
            ));
        }
        if let Some(entry) = self.find(name) {
            return Ok(entry);
        }
        let mut config = self.template.clone();
        config.name = name.to_string();
        config.target = name.to_string();
        self.ensure_service_with_config(name, config)
    }

    /// 用显式配置按需创建 service.
    ///
    /// 参数：`name` 是 service 名；`config` 是覆盖配置.
    /// 返回值：service 或错误.
    /// 已存在时现有配置优先，supplied config 被忽略.
    /// 命中路径先做 lock-free `find`，只有真正创建时才进入按名字分片的 `build_locks`.
    pub(crate) fn ensure_service_with_config(
        &self,
        name: &str,
        mut config: ServiceConfig,
    ) -> Result<Arc<ServiceEntry>, FlareError> {
        if self.forked.load(Ordering::Acquire) {
            return Err(FlareError::Config(
                "cannot create a dynamic service in a forked child".to_string(),
            ));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(FlareError::Config(
                "FlareGuard is shutting down; refusing to create a dynamic service".to_string(),
            ));
        }
        if let Some(entry) = self.find(name) {
            return Ok(entry);
        }
        let _name_guard = self.build_locks[build_lock_index(name)]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // 读闸允许不同名字并行构建；mark_closed 获取写闸，必须等所有在途构建结束.
        let _build_gate = self
            .build_gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.closed.load(Ordering::Acquire) {
            return Err(FlareError::Config(
                "FlareGuard is shutting down; refusing to create a dynamic service".to_string(),
            ));
        }
        // 双重检查：等待 stripe 锁期间，其他线程可能已经创建了同名 service.
        if let Some(entry) = self.find(name) {
            return Ok(entry);
        }
        if !self.create_enabled {
            return Err(FlareError::Config(format!(
                "service {name:?} does not exist; set unknown_target(Create) and use FlareGuard::logger"
            )));
        }
        config.name = name.to_string();
        config.validate()?;
        let path_keys = self.reserve_service_paths(&config)?;
        if let Err(error) = self.reserve_dynamic_slot(name) {
            self.release_service_paths(&path_keys);
            return Err(error);
        }
        let entry = match build_service(&self.base_path, config, self.json_lines) {
            Ok(entry) => Arc::new(entry),
            Err(error) => {
                self.cancel_dynamic_slot();
                self.release_service_paths(&path_keys);
                return Err(error);
            }
        };
        self.publish_dynamic(name, Arc::clone(&entry));
        Ok(entry)
    }

    /// 预留动态 service 的实际文件路径.
    ///
    /// 参数：`config` 是即将构建的 service 配置.
    /// 返回值：成功返回已插入的路径键；冲突返回配置错误.
    /// 与静态构建期校验共用同一 path identity 规则；动态名字不能通过不同 service 名绕过同 inode
    /// 双 writer 防护. 预留先于文件创建，失败由调用方释放.
    fn reserve_service_paths(&self, config: &ServiceConfig) -> Result<Vec<PathBuf>, FlareError> {
        let keys = service_path_keys(&self.base_path, config);
        if keys.is_empty() {
            return Ok(keys);
        }
        let mut paths = self
            .paths
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in &keys {
            if let Some(owner) = paths.get(key) {
                return Err(FlareError::Config(format!(
                    "log path conflict: service {:?} path {:?} is already used by service {owner:?}",
                    config.name, key
                )));
            }
        }
        for key in &keys {
            paths.insert(key.clone(), config.name.clone());
        }
        Ok(keys)
    }

    /// 释放动态 service 预留但最终构建失败的路径.
    ///
    /// 参数：`keys` 是 [`Self::reserve_service_paths`] 返回的键.
    fn release_service_paths(&self, keys: &[PathBuf]) {
        if keys.is_empty() {
            return;
        }
        let mut paths = self
            .paths
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in keys {
            paths.remove(key);
        }
    }

    /// 预留一个动态 service 名额.
    ///
    /// 参数：`name` 是 service 名，仅用于错误信息.
    /// 返回值：成功预留或达到上限.
    /// 预留和 `dynamic` 插入都在 `publish_lock` 下，避免不同名字并发创建时超过硬上限.
    fn reserve_dynamic_slot(&self, name: &str) -> Result<(), FlareError> {
        let _publish = self
            .publish_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut reserved = self
            .reserved_builds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let dynamic = self
            .dynamic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if dynamic.len().saturating_add(*reserved) >= self.max_dynamic_services {
            return Err(FlareError::Config(format!(
                "dynamic service limit reached ({}); refusing to create {name:?}",
                self.max_dynamic_services
            )));
        }
        *reserved = reserved.saturating_add(1);
        Ok(())
    }

    /// 发布已创建成功的动态 service，并释放预留名额.
    ///
    /// 参数：`name` 是 service 名；`entry` 是已启动的 service.
    /// 返回值：无.
    fn publish_dynamic(&self, name: &str, entry: Arc<ServiceEntry>) {
        let _publish = self
            .publish_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        {
            let mut reserved = self
                .reserved_builds
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *reserved = reserved.saturating_sub(1);
        }
        {
            let mut dynamic = self
                .dynamic
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            dynamic.insert(name.to_string(), Arc::clone(&entry));
        }
        self.publish_route(entry);
    }

    /// 创建失败时释放动态名额.
    ///
    /// 参数：无；返回值：无.
    fn cancel_dynamic_slot(&self) {
        let _publish = self
            .publish_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut reserved = self
            .reserved_builds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *reserved = reserved.saturating_sub(1);
    }

    /// 把新 service 发布到路由表.
    ///
    /// 参数：`entry` 是新建 service.
    /// 返回值：无.
    /// 实现复制 routes/names、按前缀长度降序排序，用 ArcSwap 原子发布，最后递增 generation 让线程缓
    /// 存失效.
    fn publish_route(&self, entry: Arc<ServiceEntry>) {
        let table = self.state.load_full();
        let mut routes = table.routes.clone();
        routes.push((entry.target.clone(), Arc::clone(&entry)));
        routes.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        let mut names = table.names.clone();
        names
            .entry(entry.name.clone())
            .or_insert_with(|| Arc::clone(&entry));
        names
            .entry(entry.target.clone())
            .or_insert_with(|| Arc::clone(&entry));
        self.state.store(Arc::new(RouteTable {
            routes,
            names,
            fallback: table.fallback.clone(),
        }));
        // 先发布路由表再递增 generation，确保观察到新 generation 的缓存不会继续持有旧表派生条目.
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// 按动态 logger 名路由.
    ///
    /// 参数：`name` 是 logger 名；返回值：精确名字/target 命中优先，否则走正常前缀/fallback 路由.
    pub(crate) fn route_name(&self, name: &str) -> Option<Arc<ServiceEntry>> {
        let table = self.state.load();
        if let Some(entry) = table.names.get(name) {
            return Some(Arc::clone(entry));
        }
        route_in(&table, name)
    }

    /// 返回 layer 统计数据.
    ///
    /// 参数：无；返回值：共享 `Arc<FlareStats>`.
    pub(crate) fn stats(&self) -> Arc<FlareStats> {
        Arc::clone(&self.stats)
    }

    /// 标记 router 进入关停，拒绝新的动态 service.
    ///
    /// 参数：无；返回值：无.
    /// 先获取 `build_gate` 写锁，确保所有在途动态创建完成并发布后才置 closed；
    /// 这样 shutdown 的 all_entries 能包含它们.
    pub(crate) fn mark_closed(&self) {
        if self.forked.load(Ordering::Acquire) {
            self.closed.store(true, Ordering::Release);
            crate::logger::clear_all_bindings();
            return;
        }
        // 与动态构建串行化.若 ensure_service 正在进行，先等它完成；随后 all_entries() 才能包含
        // 新建 service.
        let _build_gate = self
            .build_gate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.closed.store(true, Ordering::Release);
        crate::logger::clear_all_bindings();
    }

    /// fork 子进程：丢弃继承的 router 状态.
    ///
    /// 参数：无；返回值：无.
    /// 先设置 forked/closed，再关闭继承锁、让每个 service 丢弃句柄；不释放父进程的实例锁.
    pub(crate) fn discard_after_fork(&self) {
        // 子进程是单线程的，锁可能被子进程中不存在的线程持有；先设置标志再拿任何锁，
        // 并使用无锁回退路径.
        self.forked.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);
        crate::lock::close_all_after_fork();
        for entry in self.all_entries() {
            entry.discard_after_fork();
        }
    }
}

/// 一个已配置 service 的运行时条目.
///
/// 字段：`name/target/level` 是路由与阈值；`logger` 是引擎句柄；`backtrace_on_error` 是错误堆栈开关
/// ；`lock/lock_path` 是实例锁；`discarded/lock_released` 防止 fork 后或重复关停时误释放父进程锁.
pub(crate) struct ServiceEntry {
    /// service 名.
    pub(crate) name: String,
    /// tracing target/前缀.
    pub(crate) target: String,
    /// 阈值.
    pub(crate) level: FlareLevel,
    /// 引擎 logger 句柄.
    pub(crate) logger: Logger,
    /// 是否为 ERROR/CRITICAL 捕获堆栈.
    pub(crate) backtrace_on_error: bool,
    /// 实例锁（可选）.
    lock: Option<Arc<InstanceLock>>,
    /// 锁文件路径（可选）.
    lock_path: Option<std::path::PathBuf>,
    /// fork 后是否已丢弃.
    discarded: AtomicBool,
    /// 是否已经释放过锁租约，防止重复释放.
    lock_released: AtomicBool,
}

impl ServiceEntry {
    /// 预过滤 tracing 级别.
    ///
    /// 参数：`level` 是 tracing 五级；返回值：是否可能被该 service 接受.
    /// 特殊处理 CRITICAL：CRITICAL 事件到达时是 ERROR，阈值也是 CRITICAL 时必须先放行，等`on_event`
    /// 读取`flare_level` 字段后再最终判断.
    pub(crate) fn enabled_for(&self, level: tracing::Level) -> bool {
        let flare = FlareLevel::from_tracing(level);
        if self.level == FlareLevel::Critical && flare == FlareLevel::Error {
            return self.logger.enabled(FlareLevel::Critical.core());
        }
        flare >= self.level && self.logger.enabled(flare.core())
    }

    /// 在收集字段后做最终级别判断.
    ///
    /// 参数：`level` 是原始 Flare 级别；`critical` 表示 `flare_level` 字段是否为 CRITICAL.
    /// 返回值：实际级别是否达到阈值.
    pub(crate) fn accepts_event(&self, level: FlareLevel, critical: bool) -> bool {
        let actual = if critical {
            FlareLevel::Critical
        } else {
            level
        };
        actual >= self.level
    }

    /// fork 子进程：丢弃句柄但不解锁父进程.
    ///
    /// 参数：无；返回值：无.
    pub(crate) fn discard_after_fork(&self) {
        self.discarded.store(true, Ordering::Release);
        self.logger.discard_after_fork();
        if let Some(lock) = &self.lock {
            lock.close_after_fork();
        }
    }

    /// 释放实例锁租约（幂等）.
    ///
    /// 参数：无；返回值：无.
    /// 如果 writer 仍存活或已 fork/已释放，则保留锁，避免另一个进程并发写同一文件.
    pub(crate) fn release_lock(&self) {
        if self.discarded.load(Ordering::Acquire) {
            return;
        }
        if self.logger.alive() {
            return;
        }
        if self.lock_released.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(path) = &self.lock_path {
            release_instance_lock(path);
        }
    }
}

impl Drop for ServiceEntry {
    /// drop 时尽力释放实例锁.
    ///
    /// 参数：`self`；返回值：无.
    /// 幂等；writer 仍存活时保留锁.
    fn drop(&mut self) {
        self.release_lock();
    }
}

/// 把路径转成冲突比较键.
///
/// 参数：`path` 是配置推导出的文件路径.
/// 返回值：Windows 使用小写字符串，其他平台使用原 `PathBuf`.
/// Windows 文件系统通常大小写不敏感；只按 `PathBuf` 比较会漏掉 `App.LOG`/`app.log` 这类冲突.
/// 路径来自配置字符串，`to_string_lossy` 的极小概率非 UTF-8 情况按可见文本比较，宁可拒绝歧义配置.
fn path_identity(path: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        PathBuf::from(path.to_string_lossy().to_lowercase())
    }
    #[cfg(not(windows))]
    {
        path.to_path_buf()
    }
}

/// 返回一个 service 实际会占用的文件路径键.
///
/// 参数：`base_path` 根目录；`config` 是 service 配置.
/// 返回值：所有 shard 的 log/error 路径身份键；`files=false` 时为空.
fn service_path_keys(base_path: &Path, config: &ServiceConfig) -> Vec<PathBuf> {
    if !config.files {
        return Vec::new();
    }
    let shards = config.shards.max(1);
    let mut keys = Vec::with_capacity(shards * 2);
    for shard in 0..shards {
        keys.push(path_identity(&config.log_path_for_shard(base_path, shard)));
        keys.push(path_identity(
            &config.error_log_path_for_shard(base_path, shard),
        ));
    }
    keys
}

/// 校验所有静态 service 的实际文件路径互不冲突.
///
/// 参数：`base_path` 是日志根目录；`configs` 是本次构建会创建文件的 service 配置.
/// 返回值：无冲突时返回路径键到 owner 的映射；冲突返回 `FlareError::Config`.
/// 必须比较“解析后的 log/error 路径 × shard”，不能只比较 service 名/target:
/// `name_as_folder(false)` 时 `app` 的错误文件与 `app_error` 的常规文件可能是同一路径；两个 writer
/// 会各自持有 fd、轮转状态并互相 rename，属于构建期即可判定的数据损坏.
/// 校验放在任何 `build_service` 之前，失败不会创建目录/文件/writer.
fn validate_unique_log_paths(
    base_path: &Path,
    configs: &[&ServiceConfig],
) -> Result<HashMap<PathBuf, String>, FlareError> {
    let mut owners: HashMap<PathBuf, String> = HashMap::new();
    for config in configs {
        let shards = config.shards.max(1);
        for shard in 0..shards {
            let paths = [
                ("log", config.log_path_for_shard(base_path, shard)),
                ("error", config.error_log_path_for_shard(base_path, shard)),
            ];
            for (kind, path) in paths {
                let key = path_identity(&path);
                if let Some(other) = owners.get(&key) {
                    return Err(FlareError::Config(format!(
                        "log path conflict: service {:?} {kind} path {} is also used by service {:?}",
                        config.name,
                        path.display(),
                        other
                    )));
                }
                owners.insert(key, config.name.clone());
            }
        }
    }
    Ok(owners)
}

/// 构建 router、layer 与 guard.
///
/// 参数：`base_path` 根目录；`format` 编译后的文本格式；`default` fallback/模板 service；`services`
/// 显式 service；`unknown_target` 未匹配策略；`max_dynamic_services` 动态上限；`json_lines` 是否
/// JSON 输出.
/// 返回值：`(FlareLayer, FlareGuard)` 或配置/构建错误.
/// 实现先校验全部名字/target/配置，再启动 writer；中途失败会清理已创建 runtime 和锁.
pub(crate) fn build(
    base_path: PathBuf,
    format: TextFormat,
    default: ServiceConfig,
    services: Vec<ServiceConfig>,
    unknown_target: UnknownTarget,
    max_dynamic_services: usize,
    json_lines: bool,
) -> Result<(FlareLayer, FlareGuard), FlareError> {
    for service in &services {
        service.validate()?;
    }
    if services.is_empty() && unknown_target == UnknownTarget::Drop {
        return Err(FlareError::Config(
            "unknown_target=Drop requires at least one configured service".to_string(),
        ));
    }
    let template = default.clone();
    if unknown_target == UnknownTarget::Create {
        // fallback service 同时充当按需创建 service 的模板.
        default.validate()?;
    }

    // 先校验所有名字/target，再创建 runtime；避免为一个最终会失败的配置打开文件、启动 writer.
    let mut seen_names = std::collections::HashSet::new();
    let mut seen_targets = std::collections::HashSet::new();
    let fallback_config = match unknown_target {
        UnknownTarget::Fallback => {
            default.validate()?;
            seen_names.insert(default.name.clone());
            seen_targets.insert(default.target.clone());
            Some(default)
        }
        UnknownTarget::Drop | UnknownTarget::Create => None,
    };
    for service in &services {
        if !seen_names.insert(service.name.clone()) {
            return Err(FlareError::Config(format!(
                "duplicate service name: {:?}",
                service.name
            )));
        }
        if !seen_targets.insert(service.target.clone()) {
            return Err(FlareError::Config(format!(
                "duplicate target: {:?}",
                service.target
            )));
        }
    }
    // 路径冲突必须在创建任何目录/文件/writer 之前拒绝；否则修复配置重试时可能已经留下
    // 两套 writer 交错写过的数据.
    let mut path_configs: Vec<&ServiceConfig> = services.iter().collect();
    if let Some(fallback) = fallback_config.as_ref() {
        path_configs.push(fallback);
    }
    let reserved_paths = validate_unique_log_paths(&base_path, &path_configs)?;

    let mut entries = Vec::with_capacity(services.len() + usize::from(fallback_config.is_some()));
    let fallback = match fallback_config {
        Some(config) => {
            let entry = build_service(&base_path, config, json_lines)?;
            let entry = Arc::new(entry);
            entries.push(Arc::clone(&entry));
            Some(entry)
        }
        None => None,
    };
    let mut routes = Vec::with_capacity(services.len());
    for service in services {
        let entry = match build_service(&base_path, service, json_lines) {
            Ok(entry) => Arc::new(entry),
            Err(error) => {
                // 后续 service 失败时，前面的 runtime 和实例锁已经创建.返回配置错误前必须关闭
                // 它们并释放锁；否则失败的构建会泄漏 writer 线程和锁租约.
                cleanup_entries(&entries);
                return Err(error);
            }
        };
        routes.push((entry.target.clone(), Arc::clone(&entry)));
        entries.push(entry);
    }
    routes.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
    let mut names = std::collections::HashMap::with_capacity(entries.len() * 2);
    // 名字冲突时 service name 优先于 target alias；否则动态 logger 名可能按配置顺序
    // 解析到不相关的 service.
    for entry in &entries {
        names
            .entry(entry.name.clone())
            .or_insert_with(|| Arc::clone(entry));
    }
    for entry in &entries {
        names
            .entry(entry.target.clone())
            .or_insert_with(|| Arc::clone(entry));
    }

    let state = ArcSwap::from_pointee(RouteTable {
        routes,
        names,
        fallback,
    });
    let router = Arc::new(Router {
        state,
        generation: AtomicU64::new(1),
        entries,
        dynamic: Mutex::new(HashMap::new()),
        base_path,
        template,
        create_enabled: unknown_target == UnknownTarget::Create,
        max_dynamic_services,
        closed: AtomicBool::new(false),
        forked: AtomicBool::new(false),
        build_locks: std::array::from_fn(|_| Mutex::new(())),
        build_gate: RwLock::new(()),
        publish_lock: Mutex::new(()),
        reserved_builds: Mutex::new(0),
        paths: Mutex::new(reserved_paths),
        json_lines,
        stats: Arc::new(FlareStats::default()),
    });
    let layer = FlareLayer::new(Arc::clone(&router), format, json_lines);
    let guard = FlareGuard::new(Arc::clone(&router), Duration::from_secs(5));
    Ok((layer, guard))
}

/// 清理已创建但构建最终失败的 service.
///
/// 参数：`entries` 是已启动的 service.
/// 返回值：无.
/// 尽力 close writer 并在 writer 停止后释放锁，避免失败配置泄漏线程/句柄/锁租约.
fn cleanup_entries(entries: &[Arc<ServiceEntry>]) {
    for entry in entries {
        // 尽力而为：close 会 flush/关闭 sink 并停止 writer；`release_lock` 只在 writer 真正
        // 停止后才释放内核锁.
        let _ = entry.logger.close(Duration::from_secs(5));
        entry.release_lock();
    }
}

/// 构建单个 service 条目.
///
/// 参数：`base_path` 根目录；`config` service 配置；`json_lines` 是否 JSON 输出.
/// 返回值：`ServiceEntry` 或配置/IO/锁错误.
/// 校验压缩必须有轮转；按需获取实例锁；失败时释放已获取的锁.
fn build_service(
    base_path: &std::path::Path,
    config: ServiceConfig,
    json_lines: bool,
) -> Result<ServiceEntry, FlareError> {
    let rotation = config.rotation.to_core()?;
    if matches!(config.rotation, FlareRotation::Never) && config.compression.is_some() {
        return Err(FlareError::Config(format!(
            "service {:?}: compression has no effect without rotation",
            config.name
        )));
    }

    let lock_path = if config.instance_lock {
        let mut name = config.log_path(base_path).into_os_string();
        name.push(".lock");
        Some(std::path::PathBuf::from(name))
    } else {
        None
    };
    let lock = match &lock_path {
        Some(path) => Some(acquire_instance_lock(path)?),
        None => None,
    };

    let result = build_service_logger(base_path, &config, rotation, json_lines);
    match result {
        Ok(logger) => Ok(ServiceEntry {
            name: config.name.clone(),
            target: config.target.clone(),
            level: config.level,
            logger,
            backtrace_on_error: config.backtrace_on_error,
            lock,
            lock_path,
            discarded: AtomicBool::new(false),
            lock_released: AtomicBool::new(false),
        }),
        Err(error) => {
            if let Some(path) = &lock_path {
                release_instance_lock(path);
            }
            Err(error)
        }
    }
}

/// 构建 service 的引擎 logger 与 sink.
///
/// 参数：`base_path` 根目录；`config` service 配置；`rotation` 已转换的轮转配置；`json_lines` 是否
/// JSON 输出.
/// 返回值：引擎 `Logger` 或构建错误.
/// 根据 files/console 创建普通/错误文件 sink 与控制台 sink，把级别区间、压缩、缓冲、限流等全部传入
/// 引擎构造器.
fn build_service_logger(
    base_path: &std::path::Path,
    config: &ServiceConfig,
    rotation: Option<crate::engine::rotation::Rotation>,
    json_lines: bool,
) -> Result<Logger, FlareError> {
    let shards = config.shards.max(1);
    let mut builder = LoggerBuilder::new(&config.name)
        .level(config.level.core())
        .queue_size(config.queue_size)
        .overflow(config.overflow.to_engine())
        .json_lines(json_lines)
        .shards(shards);
    if let Some(rate) = config.rate_limit {
        builder = builder.rate_limit(rate);
    }
    if let Some(bytes) = config.max_queued_bytes {
        builder = builder.max_queued_bytes(bytes);
    }

    // 控制台 sink 可以被多个 shard 共享；它内部会串行化写入，避免多线程输出字符交错.
    let console: Option<Arc<dyn Sink>> = config
        .console
        .then(|| Arc::new(ConsoleSink::stderr()) as Arc<dyn Sink>);
    let mut shard_sinks = Vec::with_capacity(shards);
    for shard in 0..shards {
        let mut sinks = Vec::with_capacity(2 + if console.is_some() { 1 } else { 0 });
        if config.files {
            let normal_path = config.log_path_for_shard(base_path, shard);
            let error_path = config.error_log_path_for_shard(base_path, shard);
            let normal = FileSink::new(
                &normal_path,
                FileSinkOptions {
                    rotation: rotation.clone(),
                    min_level: None,
                    max_level: Some(Level::Error.as_i32()),
                    flush_each: config.flush_each,
                    buffer_size: config.file_buffer_size,
                    compression: config.compression.map(FlareCompression::to_engine),
                    sync_on_flush: config.sync_on_flush,
                    sync_each_batch: config.sync_each_batch,
                    follow_symlinks: config.follow_symlinks,
                    ..Default::default()
                },
            )
            .map_err(FlareError::Build)?;
            let error = FileSink::new(
                &error_path,
                FileSinkOptions {
                    rotation: rotation.clone(),
                    min_level: Some(Level::Error.as_i32()),
                    max_level: None,
                    flush_each: config.flush_each,
                    buffer_size: config.file_buffer_size,
                    compression: config.compression.map(FlareCompression::to_engine),
                    sync_on_flush: config.sync_on_flush,
                    sync_each_batch: config.sync_each_batch,
                    follow_symlinks: config.follow_symlinks,
                    ..Default::default()
                },
            )
            .map_err(FlareError::Build)?;
            sinks.push(SinkConfig::new("file", Arc::new(normal)).levels(None, Some(Level::Error)));
            sinks.push(SinkConfig::new("error", Arc::new(error)).levels(Some(Level::Error), None));
        }
        if let Some(console) = &console {
            sinks.push(SinkConfig::new("console", Arc::clone(console)));
        }
        shard_sinks.push(sinks);
    }

    builder
        .build_with_shard_sinks(shard_sinks)
        .map_err(|error| FlareError::Build(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::TextFormat;

    /// 锁定精确 find 与前缀 route_in 的优先级：
    /// 动态 service 未创建前，app.child 路由到静态 app；创建后，精确键优先.
    #[test]
    fn exact_find_and_prefix_route_priority_is_stable() {
        let dir =
            std::env::temp_dir().join(format!("flare-router-priority-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let parent = ServiceConfig::new("app")
            .target("app")
            .rotation(FlareRotation::Never);
        let template = ServiceConfig::new("flare")
            .target("flare")
            .rotation(FlareRotation::Never);
        let (_layer, guard) = build(
            dir.clone(),
            TextFormat::default(),
            template,
            vec![parent],
            UnknownTarget::Create,
            8,
            false,
        )
        .unwrap();
        let router = guard.router();

        let parent_entry = router.find("app").expect("parent service");
        assert!(router.find("app.child").is_none());
        assert!(Arc::ptr_eq(
            &router.route_name("app.child").expect("prefix route"),
            &parent_entry
        ));

        let child = router.ensure_service("app.child").expect("dynamic child");
        assert!(router.find("app.child").is_some());
        assert!(Arc::ptr_eq(
            &router.route_name("app.child").expect("exact route"),
            &child
        ));

        drop(guard);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 验证构建期拒绝解析后路径冲突.
    ///
    /// 参数：无.
    /// 断言：`app` 的 error 文件与 `app_error` 的 log 文件同名时返回配置错误，且不创建文件.
    #[test]
    fn rejects_colliding_resolved_log_paths() {
        let dir =
            std::env::temp_dir().join(format!("flare-router-path-conflict-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let template = ServiceConfig::new("flare")
            .target("flare")
            .rotation(FlareRotation::Never);
        let first = ServiceConfig::new("app")
            .target("app")
            .name_as_folder(false)
            .rotation(FlareRotation::Never);
        let second = ServiceConfig::new("app_error")
            .target("app_error")
            .name_as_folder(false)
            .rotation(FlareRotation::Never);
        let result = build(
            dir.clone(),
            TextFormat::default(),
            template,
            vec![first, second],
            UnknownTarget::Drop,
            8,
            false,
        );
        match result {
            Ok(_) => panic!("colliding resolved paths must be rejected"),
            Err(error) => assert!(
                error.to_string().contains("log path conflict"),
                "unexpected error: {error}"
            ),
        }
        assert!(!dir.join("app.log").exists());
        assert!(!dir.join("app_error.log").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 验证动态 service 也不能与静态 service 解析到同一文件.
    ///
    /// 参数：无.
    /// 断言：`app_error` 动态创建被路径 owner 表拒绝，不会产生第二个 writer.
    #[test]
    fn dynamic_service_rejects_colliding_resolved_path() {
        let dir = std::env::temp_dir().join(format!(
            "flare-router-dynamic-path-conflict-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let template = ServiceConfig::new("flare")
            .target("flare")
            .name_as_folder(false)
            .rotation(FlareRotation::Never);
        let app = ServiceConfig::new("app")
            .target("app")
            .name_as_folder(false)
            .rotation(FlareRotation::Never);
        let (_layer, guard) = build(
            dir.clone(),
            TextFormat::default(),
            template,
            vec![app],
            UnknownTarget::Create,
            8,
            false,
        )
        .unwrap();
        let router = guard.router();
        let error = match router.ensure_service("app_error") {
            Ok(_) => panic!("dynamic colliding path must be rejected"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("log path conflict"),
            "unexpected error: {error}"
        );
        assert!(router.find("app_error").is_none());
        drop(guard);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 验证 `has_services` 只读发布快照，不会阻塞在动态创建锁上.
    ///
    /// 参数：无.
    /// 断言：动态表为空时返回 false；另一线程仍持有 `dynamic` 锁时也必须立即返回；动态 service
    /// 发布后返回 true. 该测试防止热路径重新引入全局 Mutex.
    #[test]
    fn has_services_does_not_lock_dynamic_table() {
        let dir =
            std::env::temp_dir().join(format!("flare-router-has-services-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let template = ServiceConfig::new("flare")
            .target("flare")
            .rotation(FlareRotation::Never);
        let (_layer, guard) = build(
            dir.clone(),
            TextFormat::default(),
            template,
            Vec::new(),
            UnknownTarget::Create,
            8,
            false,
        )
        .unwrap();
        let router = guard.router();
        assert!(!router.has_services());

        let held = router
            .dynamic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (sender, receiver) = std::sync::mpsc::channel();
        let probe = Arc::clone(&router);
        let handle = std::thread::spawn(move || {
            let _ = sender.send(probe.has_services());
        });
        let observed = receiver.recv_timeout(Duration::from_secs(2));
        drop(held);
        assert!(
            !observed.expect("has_services blocked on the dynamic mutex"),
            "empty dynamic table must report no services"
        );
        handle.join().expect("probe thread");

        router.ensure_service("app.child").expect("dynamic child");
        assert!(router.has_services());

        drop(guard);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
