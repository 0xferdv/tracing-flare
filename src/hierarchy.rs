//! Python 风格 logger 层级与 handler 传播（opt-in）.
//!
//! 该模块只在 `python-hierarchy` feature 下编译. 默认 `get_logger` 仍是完整名字缓存，
//! 不解析父子关系、不做级别继承，也不改变写盘次数.
//!
//! 语义：
//! - `a.b.c` 的父级是 `a.b`，`a` 的父级是 root；root 名字为空；
//! - `level` 未设置的 logger 继承最近祖先；所有祖先都未设置时不额外过滤，沿用 service 阈值；
//! - 每个 logger 可挂 handler；`propagate=true` 时继续收集祖先/root handler；
//! - 显式配置了 handler 时，记录只投递到 handler 服务；没有 handler 时回退到原 service 路由；
//! - 同名/同一 service 的 handler 在单次投递中去重，避免重复写文件.
//!
//! 性能：
//! - 全局 `ACTIVE` 为 false 时，`Logger::emit` 完全走原路径；
//! - plan 按 logger 缓存，并用全局 generation 失效；热路径只读 `ArcSwap`，不遍历父链；
//! - handler 列表和 plan 都有硬上限，避免用户输入创建无界内存.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use smallvec::SmallVec;

use crate::engine::record::Record;
use crate::engine::runtime::BATCH_MAX;
use crate::error::FlareError;
use crate::level::FlareLevel;
use crate::logger::Logger;
use crate::router::ServiceEntry;

/// 表示 logger/handler 级别未设置（Python 的 NOTSET）.
const NOTSET: i32 = 0;
/// 单个 logger 最多挂载的 handler 数.
///
/// handler 列表由业务配置产生，必须有界；超过上限时 `add_handler` 返回 false.
pub(crate) const MAX_HANDLERS_PER_LOGGER: usize = 16;
/// 一次传播计划最多收集的 handler 数.
///
/// 父链长度由名字中的点决定，但不能让异常长名字放大单条日志的 cost；达到上限后停止收集.
pub(crate) const MAX_PLAN_HANDLERS: usize = 64;

/// 层级配置 generation.
///
/// 任意 level/propagate/handler 变化都会递增；每个 logger 的 plan 缓存据此失效.
static GENERATION: AtomicU64 = AtomicU64::new(1);
/// 是否至少发生过一次层级配置.
///
/// 默认 false 时 `Logger::emit` 不构建 plan，避免 feature 打开但未使用时增加每事件成本.
static ACTIVE: AtomicBool = AtomicBool::new(false);
/// handler 身份游标；用于 `add_handler` 去重.
static NEXT_HANDLER_ID: AtomicU64 = AtomicU64::new(1);
/// root logger；全局 handler 挂在这里.
static ROOT: OnceLock<Logger> = OnceLock::new();

/// 返回当前层级 generation；参数无.
#[inline]
pub(crate) fn generation() -> u64 {
    GENERATION.load(Ordering::Acquire)
}

/// 标记层级功能已启用；参数无，返回值无.
#[inline]
fn activate() {
    ACTIVE.store(true, Ordering::Release);
}

/// 递增 generation，使所有缓存的 plan 失效；参数无.
fn bump_generation() {
    GENERATION.fetch_add(1, Ordering::Release);
}

/// 是否已有层级配置；参数无.
///
/// 返回值：false 表示 `set_level`/`set_propagate`/`add_handler` 从未被调用，可跳过 plan.
#[inline]
pub(crate) fn is_active() -> bool {
    ACTIVE.load(Ordering::Acquire)
}

/// 主动让所有 plan 失效；router 替换/fork 后调用.
pub(crate) fn invalidate() {
    bump_generation();
}

/// 返回 root logger；参数无.
///
/// root 不绑定 service；它的 handler 就是“global handler 注册表”.
pub(crate) fn root_logger() -> Logger {
    ROOT.get_or_init(|| Logger::new("")).clone()
}

/// 一个 handler 的共享状态.
///
/// `service` 是弱引用，避免 handler 让已关停的 service/writer 无法释放；router 替换后
/// 首次使用会按 `lookup_name` 重新绑定.
struct HandlerInner {
    /// 进程内唯一身份，用于同一 logger 去重.
    id: u64,
    /// 展示名（通常是创建 handler 时的 logger 名）.
    name: Arc<str>,
    /// service 精确名，用于 router 替换后的重绑定.
    lookup_name: Arc<str>,
    /// 目标 service 弱引用.
    service: ArcSwap<Option<Weak<ServiceEntry>>>,
    /// handler 自身的最低级别；NOTSET 表示不额外过滤.
    level: AtomicI32,
}

impl std::fmt::Debug for HandlerInner {
    /// 只输出身份/名字/级别；service 弱引用不进入 Debug.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerInner")
            .field("id", &self.id)
            .field("name", &self.name)
            .field(
                "level",
                &FlareLevel::from_levelno(self.level.load(Ordering::Relaxed)),
            )
            .finish()
    }
}

/// Python 风格 handler 句柄.
///
/// 第一版 handler 复用 Flare service 的 sink/writer，formatter 使用全局 `TextFormat`；
/// handler 生命周期仍由 `FlareGuard` 管理，`flush()` 可通过本对象转发给目标 service.
/// clone 共享同一身份和级别，可安全跨线程使用.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Handler {
    inner: Arc<HandlerInner>,
}

impl Handler {
    /// 按 service/logger 名创建 handler.
    ///
    /// 参数：`name` 是已配置 service 名（或允许动态创建时的 logger 名）.
    /// 返回值：成功为 handler；没有默认 router、service 不存在且不能创建时返回 `FlareError`.
    /// 该调用可能触发一次 Eager service 创建，应放在启动期，不要在日志热路径中创建.
    pub fn new(name: impl AsRef<str>) -> Result<Self, FlareError> {
        let name = name.as_ref();
        let logger = Logger::try_get_logger(name)?;
        Self::from_logger(&logger)
    }

    /// 从已有 logger 的已解析 service 创建 handler.
    ///
    /// 参数：`logger` 必须已绑定 service.
    /// 返回值：handler 或配置错误.
    /// 与 [`Self::new`] 的区别是不再触发 service 创建.
    pub fn from_logger(logger: &Logger) -> Result<Self, FlareError> {
        let service = logger.resolved_service().ok_or_else(|| {
            FlareError::Config(format!(
                "handler {:?} has no service; configure it or enable dynamic creation",
                logger.name()
            ))
        })?;
        Ok(Self {
            inner: Arc::new(HandlerInner {
                id: NEXT_HANDLER_ID.fetch_add(1, Ordering::Relaxed),
                name: Arc::from(logger.name()),
                lookup_name: Arc::from(service.name.as_str()),
                service: ArcSwap::from_pointee(Some(Arc::downgrade(&service))),
                level: AtomicI32::new(NOTSET),
            }),
        })
    }

    /// 返回 handler 展示名；参数无.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// 设置 handler 自身的最低级别.
    ///
    /// 参数：`level=None` 表示 NOTSET，不做 handler 级过滤.
    /// 返回值：无.
    /// 该级别在 logger effective level 之后应用，不会修改目标 service 的全局阈值.
    pub fn set_level(&self, level: Option<FlareLevel>) {
        self.inner
            .level
            .store(level.map_or(NOTSET, FlareLevel::levelno), Ordering::Release);
    }

    /// 返回 handler 自身级别；参数无.
    pub fn level(&self) -> Option<FlareLevel> {
        FlareLevel::from_levelno(self.inner.level.load(Ordering::Acquire))
    }

    /// 判断 handler 是否接受某级别；参数 `level` 是记录级别.
    #[inline]
    pub(crate) fn accepts(&self, level: FlareLevel) -> bool {
        let minimum = self.inner.level.load(Ordering::Relaxed);
        minimum == NOTSET || level.levelno() >= minimum
    }

    /// 返回 handler 目标 service，必要时按名字重绑定.
    ///
    /// 参数：无.
    /// 返回值：仍存活的 service；router 已销毁或 service 不存在时为 `None`.
    pub(crate) fn service(&self) -> Option<Arc<ServiceEntry>> {
        if let Some(weak) = self.inner.service.load().as_ref().as_ref() {
            if let Some(service) = weak.upgrade() {
                return Some(service);
            }
        }
        let service = crate::logger::resolve_service(&self.inner.lookup_name)?;
        self.inner
            .service
            .store(Arc::new(Some(Arc::downgrade(&service))));
        Some(service)
    }

    /// 是否仍能解析到 service；参数无.
    pub fn is_alive(&self) -> bool {
        self.service().is_some()
    }

    /// 刷新 handler 目标 service 的全部 sink.
    ///
    /// 参数：`timeout` 是 flush 屏障预算；返回值：service 不存在或 flush 失败时为 false.
    pub fn flush(&self, timeout: Duration) -> bool {
        self.service()
            .is_some_and(|service| service.logger.flush(timeout))
    }

    /// 返回进程内身份；参数无.
    #[inline]
    pub(crate) fn id(&self) -> u64 {
        self.inner.id
    }
}

/// 一个 logger 的层级可变状态.
pub(crate) struct HierarchyState {
    /// 显式级别；`NOTSET` 表示继承.
    level: AtomicI32,
    /// 是否向父级传播.
    propagate: AtomicBool,
    /// 本 logger 的 handler；修改时复制发布，读路径无锁.
    handlers: ArcSwap<Vec<Handler>>,
    /// 串行化 handler 列表的读-改-写；避免两个并发 add 互相覆盖.
    handler_mutations: Mutex<()>,
    /// 父 logger 缓存；`Some(None)` 表示 root/已确定无父.
    parent: OnceLock<Option<Logger>>,
    /// 最近构建的传播计划.
    plan: ArcSwap<Option<PlanCache>>,
}

impl HierarchyState {
    /// 创建默认层级状态；参数无.
    ///
    /// 默认 NOTSET、propagate=true、无 handler、无缓存 plan.
    pub(crate) fn new() -> Self {
        Self {
            level: AtomicI32::new(NOTSET),
            propagate: AtomicBool::new(true),
            handlers: ArcSwap::from_pointee(Vec::new()),
            handler_mutations: Mutex::new(()),
            parent: OnceLock::new(),
            plan: ArcSwap::from_pointee(None),
        }
    }

    /// 返回显式级别；参数无.
    pub(crate) fn level(&self) -> Option<FlareLevel> {
        FlareLevel::from_levelno(self.level.load(Ordering::Acquire))
    }

    /// 设置显式级别并失效 plan.
    ///
    /// 参数：`level=None` 表示清除级别、恢复继承.
    pub(crate) fn set_level(&self, level: Option<FlareLevel>) {
        self.level
            .store(level.map_or(NOTSET, FlareLevel::levelno), Ordering::Release);
        activate();
        bump_generation();
    }

    /// 是否向父级传播；参数无.
    pub(crate) fn propagate(&self) -> bool {
        self.propagate.load(Ordering::Acquire)
    }

    /// 设置传播开关并失效 plan；参数 `value` 是新值.
    pub(crate) fn set_propagate(&self, value: bool) {
        self.propagate.store(value, Ordering::Release);
        activate();
        bump_generation();
    }

    /// 返回 handler 快照；参数无.
    pub(crate) fn handlers(&self) -> Vec<Handler> {
        self.handlers.load().as_ref().clone()
    }

    /// 添加 handler.
    ///
    /// 参数：`handler` 是待添加句柄.
    /// 返回值：成功 true；同一身份已存在或达到 [`MAX_HANDLERS_PER_LOGGER`] 时 false.
    pub(crate) fn add_handler(&self, handler: Handler) -> bool {
        let _guard = self
            .handler_mutations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self.handlers.load();
        if current.len() >= MAX_HANDLERS_PER_LOGGER
            || current.iter().any(|existing| existing.id() == handler.id())
        {
            return false;
        }
        let mut next = (**current).clone();
        next.push(handler);
        self.handlers.store(Arc::new(next));
        activate();
        bump_generation();
        true
    }

    /// 移除 handler.
    ///
    /// 参数：`handler` 只按身份匹配.
    /// 返回值：是否移除了至少一项.
    pub(crate) fn remove_handler(&self, handler: &Handler) -> bool {
        let _guard = self
            .handler_mutations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self.handlers.load();
        if !current.iter().any(|existing| existing.id() == handler.id()) {
            return false;
        }
        let next: Vec<Handler> = current
            .iter()
            .filter(|existing| existing.id() != handler.id())
            .cloned()
            .collect();
        self.handlers.store(Arc::new(next));
        bump_generation();
        true
    }

    /// 清空本 logger 的 handler.
    ///
    /// 返回值：移除数量.
    pub(crate) fn clear_handlers(&self) -> usize {
        let _guard = self
            .handler_mutations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let removed = self.handlers.load().len();
        if removed != 0 {
            self.handlers.store(Arc::new(Vec::new()));
            bump_generation();
        }
        removed
    }

    /// 获取/初始化父 logger.
    ///
    /// 参数：`logger` 是当前 logger.
    /// 返回值：父句柄；root 返回 `None`.
    /// 父链初始化只发生第一次调用；注册表插入使用独立的短临界区.
    pub(crate) fn parent(&self, logger: &Logger) -> Option<Logger> {
        if let Some(parent) = self.parent.get() {
            return parent.clone();
        }
        let name = logger.name();
        let parent = if name.is_empty() {
            None
        } else {
            match name.rsplit_once('.') {
                Some((prefix, _)) if !prefix.is_empty() => {
                    Some(crate::logger::registered_logger(prefix))
                }
                _ => Some(root_logger()),
            }
        };
        let _ = self.parent.set(parent.clone());
        parent
    }

    /// 构建/读取当前传播计划.
    ///
    /// 参数：`logger` 是发起日志的 logger.
    /// 返回值：共享 plan；同一 generation 内只构建一次.
    pub(crate) fn plan(&self, logger: &Logger) -> Arc<RoutePlan> {
        let current_generation = generation();
        if let Some(cache) = self.plan.load().as_ref() {
            if cache.generation == current_generation {
                return Arc::clone(&cache.plan);
            }
        }
        let mut effective_level = None;
        let mut handlers = Vec::new();
        let mut seen: SmallVec<[u64; 8]> = SmallVec::new();
        let mut current = Some(logger.clone());
        let mut collecting_handlers = true;
        while let Some(node) = current {
            let state = node.hierarchy_state();
            if effective_level.is_none() {
                effective_level = state.level();
            }
            if collecting_handlers {
                for handler in state.handlers.load().iter() {
                    if handlers.len() >= MAX_PLAN_HANDLERS {
                        break;
                    }
                    if seen.iter().any(|id| *id == handler.id()) {
                        continue;
                    }
                    seen.push(handler.id());
                    handlers.push(handler.clone());
                }
                // propagate 只约束 handler 传播；级别继承必须继续上溯，否则
                // propagate=false 会退化成“忽略所有祖先阈值”，与 Python logging 语义相反.
                if handlers.len() >= MAX_PLAN_HANDLERS || !state.propagate() {
                    collecting_handlers = false;
                }
            }
            current = state.parent(&node);
        }
        let plan = Arc::new(RoutePlan {
            effective_level,
            handlers: Arc::from(handlers),
        });
        self.plan.store(Arc::new(Some(PlanCache {
            generation: current_generation,
            plan: Arc::clone(&plan),
        })));
        plan
    }
}

/// plan 缓存项.
struct PlanCache {
    /// 构建时的全局 generation.
    generation: u64,
    /// 共享计划.
    plan: Arc<RoutePlan>,
}

/// 一次日志事件的投递计划.
#[derive(Debug)]
pub(crate) struct RoutePlan {
    /// 最近祖先的显式级别；`None` 表示不额外过滤.
    pub(crate) effective_level: Option<FlareLevel>,
    /// 去重后的 handler 列表.
    pub(crate) handlers: Arc<[Handler]>,
}

impl RoutePlan {
    /// 是否配置了显式 handler；参数无.
    #[inline]
    pub(crate) fn has_handlers(&self) -> bool {
        !self.handlers.is_empty()
    }

    /// 返回符合 handler 级别的目标 service.
    ///
    /// 参数：`level` 是记录的实际级别.
    /// 返回值：去重后的 service 列表；同一 service 被多个 handler 引用只保留一次.
    pub(crate) fn services_for(&self, level: FlareLevel) -> SmallVec<[Arc<ServiceEntry>; 4]> {
        let mut services: SmallVec<[Arc<ServiceEntry>; 4]> = SmallVec::new();
        for handler in self.handlers.iter() {
            if !handler.accepts(level) {
                continue;
            }
            let Some(service) = handler.service() else {
                continue;
            };
            if services
                .iter()
                .any(|existing| Arc::ptr_eq(existing, &service))
            {
                continue;
            }
            services.push(service);
            if services.len() >= MAX_PLAN_HANDLERS {
                break;
            }
        }
        services
    }

    /// 批量路径：把记录投递到 handler service.
    ///
    /// 参数：`records` 是已格式化记录；`blocking` 决定使用 `log_many` 还是 `try_log_many`.
    /// 返回值：单 handler 时为目标 service 实际入队数；多 handler 时为至少提交到一个 handler 的
    /// 源记录数（引擎内部仍可能因队列/限流丢弃）.
    /// 多个 handler 时每条记录会 clone；只有一个 handler 时直接移动，避免高吞吐路径额外复制.
    pub(crate) fn log_many(
        &self,
        records: impl IntoIterator<Item = Record>,
        blocking: bool,
    ) -> usize {
        let mut targets: SmallVec<[(Arc<ServiceEntry>, Option<FlareLevel>); 4]> = SmallVec::new();
        for handler in self.handlers.iter() {
            let Some(service) = handler.service() else {
                continue;
            };
            let handler_level = handler.level();
            if let Some(existing) = targets
                .iter_mut()
                .find(|(target, _)| Arc::ptr_eq(target, &service))
            {
                // 同一 service 被多个 handler 引用时，取最宽松的 handler 级别；
                // 任一 handler 接受就投递一次，避免重复写.
                existing.1 = match (existing.1, handler_level) {
                    (None, _) | (_, None) => None,
                    (Some(current), Some(next)) => Some(current.min(next)),
                };
            } else {
                targets.push((service, handler_level));
                if targets.len() >= MAX_PLAN_HANDLERS {
                    break;
                }
            }
        }
        if targets.is_empty() {
            return 0;
        }
        if targets.len() == 1 {
            let Some((service, minimum)) = targets.pop() else {
                return 0;
            };
            let effective = self.effective_level;
            // 单 handler 直接流式透传给引擎，由引擎按 BATCH_MAX/BATCH_BYTES_MAX 切批；
            // 不能先收集整个迭代器，否则 10M 条输入会绕过队列预算变成 GB 级常驻内存.
            let records = records.into_iter().filter(move |record| {
                !effective.is_some_and(|level| record.level < level.core())
                    && minimum.is_none_or(|minimum| record.level >= minimum.core())
            });
            return if blocking {
                service.logger.log_many(records)
            } else {
                service.logger.try_log_many(records)
            };
        }

        // 多 handler 无法零拷贝，但把物化上界固定为一个引擎批次；
        // 每个 target 只看到当前 chunk，内存约为 BATCH_MAX × Record，而不是整个输入.
        let mut records = records.into_iter();
        let mut buffer: Vec<Record> = Vec::with_capacity(BATCH_MAX);
        let mut accepted = 0usize;
        loop {
            buffer.clear();
            for record in records.by_ref() {
                if self
                    .effective_level
                    .is_some_and(|minimum| record.level < minimum.core())
                {
                    continue;
                }
                buffer.push(record);
                if buffer.len() >= BATCH_MAX {
                    break;
                }
            }
            if buffer.is_empty() {
                break;
            }
            accepted = accepted.saturating_add(
                buffer
                    .iter()
                    .filter(|record| {
                        targets.iter().any(|(_, minimum)| {
                            minimum.is_none_or(|minimum| record.level >= minimum.core())
                        })
                    })
                    .count(),
            );
            for (service, minimum) in &targets {
                let minimum = *minimum;
                let records = buffer
                    .iter()
                    .filter(move |record| {
                        minimum.is_none_or(|minimum| record.level >= minimum.core())
                    })
                    .cloned();
                if blocking {
                    let _ = service.logger.log_many(records);
                } else {
                    let _ = service.logger.try_log_many(records);
                }
            }
        }
        accepted
    }
}
