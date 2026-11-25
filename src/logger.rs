//! 同步/异步日志门面.
//!
//! `Logger` 是 tracing 事件之上的薄门面：它发出保留 target + `flare_logger` 字段，`FlareLayer` 把字
//! 段解析为已配置 service.
//! 这样既保持 tracing/subscriber 作为唯一入口，又支持运行期动态 logger 名（tracing 的 `target:` 是
//! 编译期常量）.
//!
//! 设计要点：
//! - `Logger::new` 创建不缓存的 facade；`get_logger/get_handler` 走进程级注册表，同名返回同一对象；
//! - 同步方法立即 emit；异步方法在首次 poll 时 emit，不阻塞 executor；
//! - 热路径通过 TLS 携带调用点与 logger 名，避免把内部字段复制进每条记录；
//! - 注册表有 65,536 上限，超出后退化为未缓存 facade，防止用户输入生成无限名字.
//!
//! ```no_run
//! use tracing_flare::Logger;
//!
//! let log = Logger::new("billing");
//! log.info("order created");
//! // 异步调用：
//! // log.ainfo("order created").await;
//! ```

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::panic::Location;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use arc_swap::ArcSwap;

use crate::config::{FlareConfig, LoggerConfig};
use crate::error::FlareError;
use crate::level::FlareLevel;
use crate::router::{Router, ServiceEntry};

/// `Logger` 事件使用的保留 tracing target.
///
/// 参数/接口说明：业务代码不应直接使用该 target；事件按 `flare_logger` 字段路由.
/// 保留 target 让 layer 能在 `enabled` 阶段识别门面事件，而不依赖编译期 target.
pub(crate) const LOGGER_TARGET: &str = "__tracing_flare_logger__";
/// 携带动态 logger/service 名的字段.
///
/// 字段仍会发出以兼容其他 tracing layer；`FlareLayer` 优先从 TLS 读取名称，避免复制.
pub(crate) const LOGGER_FIELD: &str = "flare_logger";
/// 携带 CRITICAL 的字段（tracing 没有该级别）.
pub(crate) const CRITICAL_FIELD: &str = "flare_level";

/// Logger 门面的内部状态.
///
/// 字段：
/// - `name`：共享的 service/logger 名；
/// - `service`：最近一次解析到的 service 弱引用；命中时跳过 Router 查找.
///   router 替换、shutdown 或 fork 子进程会清空该绑定，避免使用旧 service.
struct LoggerInner {
    name: Arc<str>,
    service: ArcSwap<Option<Weak<ServiceEntry>>>,
    /// 可选 Python 风格父子/handler 状态；feature 关闭时不存在，不增加对象大小.
    #[cfg(feature = "python-hierarchy")]
    hierarchy: crate::hierarchy::HierarchyState,
}

impl fmt::Debug for LoggerInner {
    /// 只输出名字；service 绑定是内部缓存，不进入 Debug 输出.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoggerInner")
            .field("name", &self.name)
            .finish()
    }
}

/// 同步/异步 logger 门面.
///
/// 参数/接口说明：
/// - 对象可廉价 clone（内部 `Arc<str>`），可跨线程共享；
/// - 使用前需要安装 Flare subscriber，否则 tracing 事件会被全局 dispatcher 丢弃；
/// - 生产场景可用 [`Self::init_production`] 一次安装；之后
///   [`Self::get_logger`] 按名字创建动态 service；
/// - 逐条 `info`/`warn` 走 tracing；最大吞吐用 [`Self::log_many`] 提交预格式化记录；
/// - `get_logger`/`get_handler` 同名返回同一对象；`new` 是不缓存的显式构造；
/// - 异步方法首次 poll 才 emit，未 poll 就 drop 是 no-op；跨线程 executor 请传拥有所有权的 String，
///   不要传借用 `format_args!`.
///
/// # 异步方法语义
///
/// `a*` 方法返回的 future 在首次 poll 时**同步**完成入队，不是后台任务卸载；如果不 `await`，
/// future 被 drop 后日志不会产生. 需要绝对非阻塞的同步入口时直接调用 `info`/`warn`/`error`，
/// 或使用 `try_log_many`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Logger {
    inner: Arc<LoggerInner>,
}

/// `get_logger`/`get_handler` 使用的进程级名字注册表.
///
/// 读路径用 `ArcSwap` 无锁；插入在短临界区内复制 map 后发布，保证同一名字只创建一个对象.
static LOGGERS: OnceLock<ArcSwap<HashMap<String, Logger>>> = OnceLock::new();
/// 注册表插入锁；只保护写入，不参与缓存命中.
static LOGGERS_INSERT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
/// `FlareConfig::install` 注册的默认 router.
///
/// 用 `Weak` 保存：guard drop 后自动失效，避免动态创建继续写已关闭的 service.
/// `ArcSwap` 让每次 `get_logger` 都不需要获取全局 mutex.
static DEFAULT_ROUTER: OnceLock<ArcSwap<Option<Weak<Router>>>> = OnceLock::new();
/// fork 子进程标记.
///
/// 子进程可能继承已加锁的插入锁；插入失败时退化为未缓存 facade，避免子进程永久阻塞.
static FORKED_CHILD: AtomicBool = AtomicBool::new(false);

/// 返回全局注册表（懒初始化）.
///
/// 参数：无；返回值：进程级名字表的共享引用.
fn loggers() -> &'static ArcSwap<HashMap<String, Logger>> {
    LOGGERS.get_or_init(|| ArcSwap::from_pointee(HashMap::new()))
}

/// 返回注册表插入锁.
///
/// 参数：无；返回值：只用于串行化首次插入的锁.
fn insert_lock() -> &'static Mutex<()> {
    LOGGERS_INSERT_LOCK.get_or_init(|| Mutex::new(()))
}

thread_local! {
    /// 当前线程正在 emit 的事件上下文.
    ///
    /// `FlareLayer` 从这里读取 logger 名来路由门面事件，无需复制 `flare_logger` 字段；同时读取
    /// `#[track_caller]` 提供的调用点行号.
    /// 两者放在同一 cell 可把每事件 TLS 访问减半；嵌套 emit 通过 RAII 恢复前一个上下文.
    static EMIT_CONTEXT: RefCell<EmitContext> = const { RefCell::new(EmitContext {
        caller: None,
        logger: None,
        #[cfg(feature = "python-hierarchy")]
        plan: None,
    }) };
}

/// 当前线程正在 emit 的上下文.
///
/// 字段：`caller` 是 `#[track_caller]` 文件/行号；`logger` 是动态 logger 名；
/// `plan` 是本次事件已解析的层级/handler 计划，避免 layer 再查一次注册表.
/// 它们放同一个 TLS cell，减少每事件多组 TLS 读写.
#[derive(Clone, Default)]
struct EmitContext {
    caller: Option<(&'static str, u32)>,
    logger: Option<Arc<str>>,
    #[cfg(feature = "python-hierarchy")]
    plan: Option<Arc<crate::hierarchy::RoutePlan>>,
}

/// emit 上下文的 RAII guard.
///
/// 字段：`previous` 保存进入前的上下文，Drop 时恢复；嵌套 Logger 调用因此不会污染外层.
struct EmitGuard {
    previous: EmitContext,
}

impl EmitGuard {
    /// 设置当前 emit 上下文.
    ///
    /// 参数：`location` 是调用点；`name` 是 logger 名；`plan` 是已解析的层级计划.
    /// 返回值：guard，生命周期覆盖 tracing 事件同步分发.
    #[cfg(feature = "python-hierarchy")]
    #[inline]
    fn set(
        location: &'static Location<'static>,
        name: &Arc<str>,
        plan: Option<Arc<crate::hierarchy::RoutePlan>>,
    ) -> Self {
        let next = EmitContext {
            caller: Some((location.file(), location.line())),
            logger: Some(Arc::clone(name)),
            plan,
        };
        Self {
            previous: EMIT_CONTEXT.with(|context| context.replace(next)),
        }
    }

    /// 设置当前 emit 上下文（无层级 feature）.
    ///
    /// 参数：`location` 是调用点；`name` 是 logger 名.
    /// 返回值：guard，生命周期覆盖 tracing 事件同步分发.
    #[cfg(not(feature = "python-hierarchy"))]
    #[inline]
    fn set(location: &'static Location<'static>, name: &Arc<str>) -> Self {
        let next = EmitContext {
            caller: Some((location.file(), location.line())),
            logger: Some(Arc::clone(name)),
        };
        Self {
            previous: EMIT_CONTEXT.with(|context| context.replace(next)),
        }
    }
}

impl Drop for EmitGuard {
    /// 恢复上一个 emit 上下文.
    ///
    /// 参数：`self`；返回值：无.
    /// 即使事件分发 panic，也会恢复，避免污染后续日志.
    fn drop(&mut self) {
        EMIT_CONTEXT.with(|context| {
            context.replace(std::mem::take(&mut self.previous));
        });
    }
}

/// 返回当前同步 Logger 事件的调用点.
///
/// 参数：无；返回值：`(file, line)` 或 `None`.layer 用它覆盖 tracing 内部调用点，让行号指向业务代码
/// .
#[inline]
pub(crate) fn current_caller() -> Option<(&'static str, u32)> {
    EMIT_CONTEXT.with(|context| context.borrow().caller)
}

/// 在当前 logger 名下运行闭包.
///
/// 参数：`f` 接收 `Option<&str>` 名称；返回值：闭包结果.
/// 设计原因：借用期间不 clone `Arc`，避免每事件一次原子增/减；layer 只在路由时需要名称.
#[inline]
pub(crate) fn with_current_logger<R>(f: impl FnOnce(Option<&str>) -> R) -> R {
    EMIT_CONTEXT.with(|context| f(context.borrow().logger.as_deref()))
}

/// 返回当前事件的层级/handler 计划.
///
/// 参数：无.
/// 返回值：`Logger::emit` 已解析的计划；外部直接 emit 保留 target 或 feature 关闭时为 `None`.
/// 克隆的是 `Arc`，不复制 handler 列表.
#[cfg(feature = "python-hierarchy")]
#[inline]
pub(crate) fn current_plan() -> Option<Arc<crate::hierarchy::RoutePlan>> {
    EMIT_CONTEXT.with(|context| context.borrow().plan.clone())
}

/// 全局 logger 名注册表上限.
///
/// 已存在名字始终保留缓存；新名字超过上限返回未缓存 facade，防止无限唯一名字把注册表撑爆.
/// 动态 service 的数量应由业务租户模型限制，而不是依赖这里兜底.
pub const MAX_REGISTERED_LOGGERS: usize = 65_536;

/// 查询/插入注册表.
///
/// 参数：`name` 是 logger 名；返回值：缓存或未缓存的 facade.
/// 命中路径只做 `ArcSwap` load + map lookup，不获取全局 mutex、不分配.
/// 插入路径在短临界区内复制 map 后整体发布；fork 子进程拿不到插入锁时返回未缓存 facade.
pub(crate) fn registered_logger(name: &str) -> Logger {
    let registry = loggers();
    if let Some(logger) = registry.load().get(name) {
        return logger.clone();
    }

    let insert_guard = if FORKED_CHILD.load(Ordering::Acquire) {
        match insert_lock().try_lock() {
            Ok(guard) => guard,
            Err(_) => return Logger::new(name),
        }
    } else {
        insert_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    };

    // 本线程等待锁期间，另一个线程可能已插入同名 logger.
    if let Some(logger) = registry.load().get(name) {
        return logger.clone();
    }
    let current = registry.load_full();
    if current.len() >= MAX_REGISTERED_LOGGERS {
        return Logger::new(name);
    }
    let logger = Logger::new(name);
    let mut next = (*current).clone();
    next.insert(name.to_string(), logger.clone());
    registry.store(Arc::new(next));
    drop(insert_guard);
    logger
}

/// fork 子进程钩子：停止使用继承的注册表锁.
///
/// 参数：无；返回值：无.
/// 子进程应重新构建配置；如果继承锁可用，已有同名 facade 仍有效.
/// 这里同时清空默认 router，防止子进程继续写父进程 service.
pub(crate) fn after_fork_child() {
    FORKED_CHILD.store(true, Ordering::Release);
    if let Some(slot) = DEFAULT_ROUTER.get() {
        slot.store(Arc::new(None));
    }
    #[cfg(feature = "python-hierarchy")]
    crate::hierarchy::invalidate();
    clear_all_bindings();
}

/// 清空所有已缓存 facade 的 service 绑定.
///
/// 参数：无；返回值：无.
/// router 替换或 fork 子进程后，旧 service 弱引用不再代表当前路由.
pub(crate) fn clear_all_bindings() {
    if let Some(registry) = LOGGERS.get() {
        for logger in registry.load().values() {
            logger.clear_binding();
        }
    }
}

/// 注册默认 router.
///
/// 参数：`router` 是 `FlareConfig::install` 构建的 router；返回值：无.
/// 用 `Weak` 保存；guard drop 后自动失效.
/// 重复注册会替换前一个.
pub(crate) fn register_default_router(router: Arc<Router>) {
    if FORKED_CHILD.load(Ordering::Acquire) {
        return;
    }
    let slot = DEFAULT_ROUTER.get_or_init(|| ArcSwap::from_pointee(None));
    slot.store(Arc::new(Some(Arc::downgrade(&router))));
    #[cfg(feature = "python-hierarchy")]
    crate::hierarchy::invalidate();
    clear_all_bindings();
}

/// 升级默认 router 的弱引用.
///
/// 参数：无；返回值：router 或 `None`.
/// 读路径只做 `ArcSwap` load 和 `Weak::upgrade`，不获取全局 mutex.
fn default_router() -> Option<Arc<Router>> {
    let slot = DEFAULT_ROUTER.get_or_init(|| ArcSwap::from_pointee(None));
    slot.load()
        .as_ref()
        .as_ref()
        .and_then(|weak| weak.upgrade())
}

/// 按名字解析 handler 目标 service.
///
/// 参数：`name` 是 service 名或 target.
/// 返回值：精确名字/前缀路由命中的 service；没有默认 router 时 `None`.
/// 该函数只用于 handler 重绑定，不在日志热路径调用.
#[cfg(feature = "python-hierarchy")]
pub(crate) fn resolve_service(name: &str) -> Option<Arc<ServiceEntry>> {
    default_router().and_then(|router| router.find(name).or_else(|| router.route_name(name)))
}

/// 确保默认 router 中存在该 service.
///
/// 参数：`name` 是 service 名；`config` 是可选覆盖配置.
/// 返回值：创建失败时返回 `FlareError`.
/// 没有默认 router 时直接成功——此时 facade 只构造 tracing 事件，由外部 subscriber 负责路由.
/// 已存在 service 时走 lock-free `find`，不进入 Router 的 build_lock.
fn ensure_default_service(name: &str, config: Option<LoggerConfig>) -> Result<(), FlareError> {
    let Some(router) = default_router() else {
        return Ok(());
    };
    match config {
        Some(config) => router
            .ensure_service_with_config(name, config.into_service())
            .map(|_| ()),
        None => router.ensure_service(name).map(|_| ()),
    }
}

impl Logger {
    /// 全局注册表上限，供调用方/文档引用.
    pub const MAX_REGISTERED_LOGGERS: usize = crate::logger::MAX_REGISTERED_LOGGERS;

    /// 创建不缓存的 facade.
    ///
    /// 参数：`name` 是 service/logger 名.
    /// 返回值：持有 `Arc<str>` 的对象；不查注册表，不创建 service.
    /// 适合启动期保存长期句柄.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(LoggerInner {
                name: Arc::from(name.into()),
                service: ArcSwap::from_pointee(None),
                #[cfg(feature = "python-hierarchy")]
                hierarchy: crate::hierarchy::HierarchyState::new(),
            }),
        }
    }

    /// 返回 root logger；仅 `python-hierarchy`.
    ///
    /// 参数：无.
    /// 返回值：进程级 root facade；名字为空，不绑定 service.
    /// root 的 handler 即全局 handler 注册表；子 logger 默认向 root 传播.
    #[cfg(feature = "python-hierarchy")]
    pub fn root() -> Self {
        crate::hierarchy::root_logger()
    }

    /// 返回父 logger；仅 `python-hierarchy`.
    ///
    /// 参数：无.
    /// 返回值：`a.b.c -> a.b -> a -> root`；root 返回 `None`.
    /// 第一次调用会注册缺失的中间 facade，但不会为它们创建 service/writer.
    #[cfg(feature = "python-hierarchy")]
    pub fn parent(&self) -> Option<Self> {
        self.inner.hierarchy.parent(self)
    }

    /// 设置显式级别；仅 `python-hierarchy`.
    ///
    /// 参数：`level=Some` 设置阈值；`None` 清除并恢复从父级继承.
    /// 返回值：无.
    /// 该级别在 service 阈值之前生效；没有显式级别时不额外过滤.
    #[cfg(feature = "python-hierarchy")]
    pub fn set_level(&self, level: Option<FlareLevel>) {
        self.inner.hierarchy.set_level(level);
    }

    /// 返回本 logger 的显式级别；仅 `python-hierarchy`.
    ///
    /// 参数：无；返回值：`None` 表示 NOTSET，需要向父级查找.
    #[cfg(feature = "python-hierarchy")]
    pub fn level(&self) -> Option<FlareLevel> {
        self.inner.hierarchy.level()
    }

    /// 返回生效级别；仅 `python-hierarchy`.
    ///
    /// 参数：无.
    /// 返回值：最近祖先的显式级别；整条链都未设置时返回 [`FlareLevel::Trace`]
    /// （表示不做额外层级过滤，仍由 service 阈值决定）.
    #[cfg(feature = "python-hierarchy")]
    pub fn effective_level(&self) -> FlareLevel {
        self.inner
            .hierarchy
            .plan(self)
            .effective_level
            .unwrap_or(FlareLevel::Trace)
    }

    /// 设置是否向父级传播 handler；仅 `python-hierarchy`.
    ///
    /// 参数：`propagate=false` 时只使用本 logger 的 handler.
    /// 返回值：无.
    /// 如果最终没有显式 handler，仍回退到本 logger 的 service 落点，避免静默丢日志.
    #[cfg(feature = "python-hierarchy")]
    pub fn set_propagate(&self, propagate: bool) {
        self.inner.hierarchy.set_propagate(propagate);
    }

    /// 返回传播开关；仅 `python-hierarchy`；参数无.
    #[cfg(feature = "python-hierarchy")]
    pub fn propagate(&self) -> bool {
        self.inner.hierarchy.propagate()
    }

    /// 添加 handler；仅 `python-hierarchy`.
    ///
    /// 参数：`handler` 是 [`crate::Handler`] 句柄.
    /// 返回值：成功 true；重复身份或达到每 logger 上限时 false.
    /// 添加后本 logger 及其可传播子孙优先使用显式 handler，而不是隐式 service.
    #[cfg(feature = "python-hierarchy")]
    pub fn add_handler(&self, handler: crate::hierarchy::Handler) -> bool {
        self.inner.hierarchy.add_handler(handler)
    }

    /// 移除 handler；仅 `python-hierarchy`.
    ///
    /// 参数：`handler` 按身份匹配；返回值：是否移除.
    #[cfg(feature = "python-hierarchy")]
    pub fn remove_handler(&self, handler: &crate::hierarchy::Handler) -> bool {
        self.inner.hierarchy.remove_handler(handler)
    }

    /// 清空本 logger 的 handler；仅 `python-hierarchy`.
    ///
    /// 参数：无；返回值：移除数量.
    #[cfg(feature = "python-hierarchy")]
    pub fn clear_handlers(&self) -> usize {
        self.inner.hierarchy.clear_handlers()
    }

    /// 返回本 logger 的 handler 快照；仅 `python-hierarchy`；参数无.
    #[cfg(feature = "python-hierarchy")]
    pub fn handlers(&self) -> Vec<crate::hierarchy::Handler> {
        self.inner.hierarchy.handlers()
    }

    /// 向 root 添加全局 handler；仅 `python-hierarchy`.
    ///
    /// 参数：`handler` 是全局 handler.
    /// 返回值：成功 true；重复/超限 false.
    /// 等价于 `Logger::root().add_handler(handler)`；子 logger 默认会传播到这里.
    #[cfg(feature = "python-hierarchy")]
    pub fn add_global_handler(handler: crate::hierarchy::Handler) -> bool {
        Self::root().add_handler(handler)
    }

    /// 清空全局 handler；仅 `python-hierarchy`.
    ///
    /// 参数：无；返回值：移除数量.
    #[cfg(feature = "python-hierarchy")]
    pub fn clear_global_handlers() -> usize {
        Self::root().clear_handlers()
    }

    /// 返回全局 handler 快照；仅 `python-hierarchy`；参数无.
    #[cfg(feature = "python-hierarchy")]
    pub fn global_handlers() -> Vec<crate::hierarchy::Handler> {
        Self::root().handlers()
    }

    /// 返回当前 facade 缓存的 service.
    ///
    /// 参数：无.
    /// 返回值：仍存活的 service；guard 已释放时返回 `None`.
    fn bound_service(&self) -> Option<Arc<ServiceEntry>> {
        self.inner
            .service
            .load()
            .as_ref()
            .as_ref()
            .and_then(|weak| weak.upgrade())
    }

    /// 判断当前 facade 是否已有可用 service.
    ///
    /// 参数：无；返回值：弱引用仍存活时为 true.
    /// 只检查强引用计数，不 clone `Arc`，用于 get_logger 命中路径.
    fn has_bound_service(&self) -> bool {
        self.inner
            .service
            .load()
            .as_ref()
            .as_ref()
            .is_some_and(|weak| weak.strong_count() > 0)
    }

    /// 绑定当前 facade 到默认 router 中的 service.
    ///
    /// 参数：`name` 是 logger 名.
    /// 返回值：无；没有 router 或 service 时不写入绑定.
    fn bind_service(&self, name: &str) {
        if let Some(router) = default_router() {
            if let Some(entry) = router.find(name) {
                self.inner
                    .service
                    .store(Arc::new(Some(Arc::downgrade(&entry))));
            }
        }
    }

    /// 清空 service 绑定.
    ///
    /// 参数：无；返回值：无.
    /// router 替换、fork 子进程或注册表重建时调用.
    fn clear_binding(&self) {
        self.inner.service.store(Arc::new(None));
    }

    /// 解析当前 facade 对应的 service.
    ///
    /// 参数：无；返回值：缓存的 service，或按默认 router 路由结果.
    pub(crate) fn resolved_service(&self) -> Option<Arc<ServiceEntry>> {
        self.bound_service()
            .or_else(|| default_router().and_then(|router| router.route_name(&self.inner.name)))
    }

    /// 返回层级状态（仅 `python-hierarchy`）.
    ///
    /// 参数：无.
    /// 返回值：当前 logger 的父子/级别/handler 状态.
    /// 供同 crate 的层级模块构建 plan；不是公共 API.
    #[cfg(feature = "python-hierarchy")]
    pub(crate) fn hierarchy_state(&self) -> &crate::hierarchy::HierarchyState {
        &self.inner.hierarchy
    }

    /// 用生产预设安装进程级 subscriber.
    ///
    /// 参数：`base_path` 是日志根目录.
    /// 返回值：生命周期 guard；失败时返回配置/安装错误.
    /// 这是 [`FlareConfig::production`] + [`FlareConfig::install`] 的便捷入口.
    /// 安装后可以直接 `Logger::get_logger("name")`；guard 必须保持存活，drop 时会 flush/close.
    /// 生产预设 `flush_each=false`，不保证实时可见；需要低延迟可见性用
    /// `FlareConfig::production(...).low_latency().install()`.
    pub fn init_production(
        base_path: impl Into<PathBuf>,
    ) -> Result<crate::guard::FlareGuard, FlareError> {
        FlareConfig::production(base_path).install()
    }

    /// 通过底层 logger 提交预格式化批量记录.
    ///
    /// 参数：`records` 是已构造的 [`crate::FlareRecord`] 迭代器.
    /// 返回值：实际入队条数；没有默认 router、没有匹配 service 或全部被过滤时返回 0.
    /// 设计原因：逐条 `info` 仍走 tracing；需要最大吞吐时用本方法绕过 tracing/模板.
    /// 记录消息必须是最终文本，不会再经过 `format()` 模板.
    /// 启用 JSON Lines 时，这里必须传入已经序列化的单行 JSON；批量路径不会替调用方做 JSON 编码.
    /// 调用前应通过 [`Self::get_logger`] 创建/获取 service；`new` 不会创建 service.
    /// 启用 `python-hierarchy` 且存在多个 handler 时，返回的是至少提交到一个 handler 的源记录数.
    pub fn log_many(
        &self,
        records: impl IntoIterator<Item = crate::engine::record::Record>,
    ) -> usize {
        #[cfg(feature = "python-hierarchy")]
        if crate::hierarchy::is_active() {
            let plan = self.inner.hierarchy.plan(self);
            if plan.has_handlers() {
                return plan.log_many(records, true);
            }
            if let Some(minimum) = plan.effective_level {
                let Some(entry) = self.resolved_service() else {
                    return 0;
                };
                return entry.logger.log_many(
                    records
                        .into_iter()
                        .filter(|record| record.level >= minimum.core()),
                );
            }
        }
        let Some(entry) = self.resolved_service() else {
            return 0;
        };
        entry.logger.log_many(records)
    }

    /// 通过底层 logger 提交预格式化批量记录，且永不按 `Overflow::Block` 阻塞.
    ///
    /// 参数：`records` 是已构造的 [`crate::FlareRecord`] 迭代器.
    /// 返回值：实际入队条数；队列满时按 `Overflow::Drop` 丢弃并计数.
    /// 适合 async/执行器线程或不能承受阻塞的生产者.
    /// 与 [`Self::log_many`] 相同：JSON Lines 模式下记录必须是已序列化的单行 JSON.
    pub fn try_log_many(
        &self,
        records: impl IntoIterator<Item = crate::engine::record::Record>,
    ) -> usize {
        #[cfg(feature = "python-hierarchy")]
        if crate::hierarchy::is_active() {
            let plan = self.inner.hierarchy.plan(self);
            if plan.has_handlers() {
                return plan.log_many(records, false);
            }
            if let Some(minimum) = plan.effective_level {
                let Some(entry) = self.resolved_service() else {
                    return 0;
                };
                return entry.logger.try_log_many(
                    records
                        .into_iter()
                        .filter(|record| record.level >= minimum.core()),
                );
            }
        }
        let Some(entry) = self.resolved_service() else {
            return 0;
        };
        entry.logger.try_log_many(records)
    }

    /// Python 风格 logger 查找.
    ///
    /// 参数：`name` 是 logger 名.
    /// 返回值：同名返回同一对象.
    /// 如果全局安装了 `UnknownTarget::Create`，会按需创建 service；否则是零 I/O facade，service 必
    /// 须已配置.
    /// 创建错误被忽略，需要错误请用 [`Self::try_get_logger`].
    /// 命中路径借用 `name`，不会为缓存查找分配 `String`.
    /// 启用 `python-hierarchy` 时，空名字等同于 `Logger::root()`.
    pub fn get_logger(name: impl AsRef<str>) -> Self {
        let name = name.as_ref();
        #[cfg(feature = "python-hierarchy")]
        if name.is_empty() {
            return Self::root();
        }
        let logger = registered_logger(name);
        if logger.has_bound_service() {
            return logger;
        }
        let _ = ensure_default_service(name, None);
        logger.bind_service(name);
        logger
    }

    /// Python 风格 handler 访问器.
    ///
    /// 参数：`name` 是 logger 名；返回值：与 [`Self::get_logger`] 相同的 facade.tracing-flare 中
    /// handler 就是 logger 门面.
    pub fn get_handler(name: impl AsRef<str>) -> Self {
        Self::get_logger(name)
    }

    /// 可失败的 logger 查找.
    ///
    /// 参数：`name` 是 logger 名.
    /// 返回值：成功为 facade，失败为 service 创建错误.
    /// 与 `get_logger` 的区别是不静默吞掉创建错误.
    pub fn try_get_logger(name: impl AsRef<str>) -> Result<Self, FlareError> {
        let name = name.as_ref();
        #[cfg(feature = "python-hierarchy")]
        if name.is_empty() {
            return Ok(Self::root());
        }
        let logger = registered_logger(name);
        if !logger.has_bound_service() {
            ensure_default_service(name, None)?;
            logger.bind_service(name);
        }
        Ok(logger)
    }

    /// 带显式配置的 Python 风格查找.
    ///
    /// 参数：`name` 是 logger 名；`config` 是生产/覆盖配置.
    /// 返回值：facade.
    /// 同名仍返回缓存对象；service 已存在时现有配置优先；创建错误被忽略，需要错误请用
    /// [`Self::try_get_logger_with_config`].
    pub fn get_logger_with_config(name: impl AsRef<str>, config: LoggerConfig) -> Self {
        let name = name.as_ref();
        let logger = registered_logger(name);
        if !logger.has_bound_service() {
            let _ = ensure_default_service(name, Some(config));
            logger.bind_service(name);
        }
        logger
    }

    /// 带显式配置的可失败查找.
    ///
    /// 参数：`name` 是 logger 名；`config` 是覆盖配置.
    /// 返回值：成功为 facade，失败为创建错误.
    pub fn try_get_logger_with_config(
        name: impl AsRef<str>,
        config: LoggerConfig,
    ) -> Result<Self, FlareError> {
        let name = name.as_ref();
        let logger = registered_logger(name);
        if !logger.has_bound_service() {
            ensure_default_service(name, Some(config))?;
            logger.bind_service(name);
        }
        Ok(logger)
    }

    /// 判断两个句柄是否指向同一缓存对象.
    ///
    /// 参数：`other` 是另一个句柄；返回值：`Arc` 指针是否相同.
    /// 未缓存 `new` 创建的句柄总是不同.
    pub fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// 返回 logger/service 名.
    ///
    /// 参数：无；返回值：共享名称引用.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// 发出 TRACE 事件.
    ///
    /// 参数：`message` 任意 `Display`；返回值：无.`#[track_caller]` 让行号指向业务调用点.
    #[track_caller]
    #[inline]
    pub fn trace(&self, message: impl Display) {
        self.emit(FlareLevel::Trace, message, false, Location::caller());
    }

    /// 发出 DEBUG 事件.
    ///
    /// 参数：`message` 任意 `Display`；返回值：无.
    #[track_caller]
    #[inline]
    pub fn debug(&self, message: impl Display) {
        self.emit(FlareLevel::Debug, message, false, Location::caller());
    }

    /// 发出 INFO 事件.
    ///
    /// 参数：`message` 任意 `Display`；返回值：无.
    #[track_caller]
    #[inline]
    pub fn info(&self, message: impl Display) {
        self.emit(FlareLevel::Info, message, false, Location::caller());
    }

    /// 发出 WARN 事件.
    ///
    /// 参数：`message` 任意 `Display`；返回值：无.
    #[track_caller]
    #[inline]
    pub fn warn(&self, message: impl Display) {
        self.emit(FlareLevel::Warn, message, false, Location::caller());
    }

    /// [`Self::warn`] 的别名.
    ///
    /// 参数：`message` 任意 `Display`；返回值：无.
    #[track_caller]
    #[inline]
    pub fn warning(&self, message: impl Display) {
        self.emit(FlareLevel::Warn, message, false, Location::caller());
    }

    /// 发出 ERROR 事件.
    ///
    /// 参数：`message` 任意 `Display`；返回值：无.
    #[track_caller]
    #[inline]
    pub fn error(&self, message: impl Display) {
        self.emit(FlareLevel::Error, message, false, Location::caller());
    }

    /// 发出 CRITICAL 事件（tracing ERROR + `flare_level = "CRITICAL"`）.
    ///
    /// 参数：`message` 任意 `Display`；返回值：无.layer 会把该标记还原为 CRITICAL.
    #[track_caller]
    #[inline]
    pub fn critical(&self, message: impl Display) {
        self.emit(FlareLevel::Critical, message, true, Location::caller());
    }

    /// [`Self::critical`] 的别名.
    ///
    /// 参数：`message` 任意 `Display`；返回值：无.
    #[track_caller]
    #[inline]
    pub fn fatal(&self, message: impl Display) {
        self.emit(FlareLevel::Critical, message, true, Location::caller());
    }

    /// 按显式级别发出事件.
    ///
    /// 参数：`level` 是 Flare 级别；`message` 任意 `Display`；返回值：无.
    #[track_caller]
    #[inline]
    pub fn log(&self, level: FlareLevel, message: impl Display) {
        let critical = level == FlareLevel::Critical;
        self.emit(level, message, critical, Location::caller());
    }

    /// 异步 TRACE.
    ///
    /// 参数：`message` 需在 future 生命周期内有效；返回值：首次 poll 时 emit 的 future，未 poll 直
    /// 接 drop 是 no-op.
    #[track_caller]
    #[inline]
    pub fn atrace<'a>(
        &'a self,
        message: impl Display + 'a,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let location = Location::caller();
        async move {
            self.emit(FlareLevel::Trace, message, false, location);
        }
    }

    /// 异步 DEBUG.
    ///
    /// 参数：`message` 需在 future 生命周期内有效；返回值：首次 poll 时 emit 的 future.
    #[track_caller]
    #[inline]
    pub fn adebug<'a>(
        &'a self,
        message: impl Display + 'a,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let location = Location::caller();
        async move {
            self.emit(FlareLevel::Debug, message, false, location);
        }
    }

    /// 异步 INFO.
    ///
    /// 参数：`message` 需在 future 生命周期内有效；返回值：首次 poll 时 emit 的 future.
    /// # 注意
    /// 这不是后台异步投递：首次 poll 时同步入队，**不 await 就会丢失这条日志**.
    #[track_caller]
    #[inline]
    pub fn ainfo<'a>(
        &'a self,
        message: impl Display + 'a,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let location = Location::caller();
        async move {
            self.emit(FlareLevel::Info, message, false, location);
        }
    }

    /// 异步 WARN.
    ///
    /// 参数：`message` 需在 future 生命周期内有效；返回值：首次 poll 时 emit 的 future.
    #[track_caller]
    #[inline]
    pub fn awarn<'a>(
        &'a self,
        message: impl Display + 'a,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let location = Location::caller();
        async move {
            self.emit(FlareLevel::Warn, message, false, location);
        }
    }

    /// 异步 WARN 别名.
    ///
    /// 参数：`message` 需在 future 生命周期内有效；返回值：首次 poll 时 emit 的 future.
    #[track_caller]
    #[inline]
    pub fn awarning<'a>(
        &'a self,
        message: impl Display + 'a,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let location = Location::caller();
        async move {
            self.emit(FlareLevel::Warn, message, false, location);
        }
    }

    /// 异步 ERROR.
    ///
    /// 参数：`message` 需在 future 生命周期内有效；返回值：首次 poll 时 emit 的 future.
    #[track_caller]
    #[inline]
    pub fn aerror<'a>(
        &'a self,
        message: impl Display + 'a,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let location = Location::caller();
        async move {
            self.emit(FlareLevel::Error, message, false, location);
        }
    }

    /// 异步 CRITICAL.
    ///
    /// 参数：`message` 需在 future 生命周期内有效；返回值：首次 poll 时 emit 的 future.
    #[track_caller]
    #[inline]
    pub fn acritical<'a>(
        &'a self,
        message: impl Display + 'a,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let location = Location::caller();
        async move {
            self.emit(FlareLevel::Critical, message, true, location);
        }
    }

    /// 异步 CRITICAL 别名.
    ///
    /// 参数：`message` 需在 future 生命周期内有效；返回值：首次 poll 时 emit 的 future.
    #[track_caller]
    #[inline]
    pub fn afatal<'a>(
        &'a self,
        message: impl Display + 'a,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let location = Location::caller();
        async move {
            self.emit(FlareLevel::Critical, message, true, location);
        }
    }

    /// 按显式级别异步发出事件.
    ///
    /// 参数：`level` 是 Flare 级别；`message` 需在 future 生命周期内有效；返回值：首次 poll 时 emit
    /// 的 future.
    #[track_caller]
    #[inline]
    pub fn alog<'a>(
        &'a self,
        level: FlareLevel,
        message: impl Display + 'a,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let location = Location::caller();
        let critical = level == FlareLevel::Critical;
        async move {
            self.emit(level, message, critical, location);
        }
    }

    /// 发出一次 tracing 事件.
    ///
    /// 参数：`level` 是 Flare 级别；`message` 是 Display 消息；`critical` 决定是否附加
    /// `flare_level="CRITICAL"`；`location` 是 `#[track_caller]` 调用点.
    /// 返回值：无.
    /// 实现设置 TLS 上下文后用字面量级别宏分发，保持 tracing 的 callsite 常量级别要求.
    #[inline]
    fn emit(
        &self,
        level: FlareLevel,
        message: impl Display,
        critical: bool,
        location: &'static Location<'static>,
    ) {
        let name = self.inner.name.as_ref();
        // 层级过滤在 tracing 事件之前完成：未达到 effective level 时连 callsite 分发都不做.
        #[cfg(feature = "python-hierarchy")]
        let hierarchy_plan = if crate::hierarchy::is_active() {
            let plan = self.inner.hierarchy.plan(self);
            if plan.effective_level.is_some_and(|minimum| level < minimum) {
                return;
            }
            Some(plan)
        } else {
            None
        };
        #[cfg(feature = "python-hierarchy")]
        let hierarchy_uses_handlers = hierarchy_plan
            .as_ref()
            .is_some_and(|plan| plan.has_handlers());
        #[cfg(not(feature = "python-hierarchy"))]
        let hierarchy_uses_handlers = false;
        // 已绑定 service 且没有 handler 时，service 阈值可以在 tracing 分发前判断：
        // 过滤掉的 DEBUG/TRACE 不再走 visitor/format/route 路径. 有 handler 时不能提前按隐式
        // service 过滤，否则会绕过 handler 自己的级别.
        if !hierarchy_uses_handlers {
            if let Some(service) = self.bound_service() {
                if !service.enabled_for(tracing::Level::from(level)) {
                    return;
                }
            }
        }
        #[cfg(feature = "python-hierarchy")]
        let _emit_guard = EmitGuard::set(location, &self.inner.name, hierarchy_plan);
        #[cfg(not(feature = "python-hierarchy"))]
        let _emit_guard = EmitGuard::set(location, &self.inner.name);
        // `tracing` 事件宏要求调用点级别为常量、字段名为字面量，因此这里按字面量级别分发.
        // `flare_logger` / `flare_level` 字面量必须与 LOGGER_FIELD / CRITICAL_FIELD 保持同步，
        // layer 用它们做路由和 CRITICAL 还原.
        macro_rules! emit_at {
            ($tracing_level:expr) => {
                if critical {
                    tracing::event!(
                        target: LOGGER_TARGET,
                        $tracing_level,
                        flare_logger = name,
                        flare_level = "CRITICAL",
                        message = %message
                    );
                } else {
                    tracing::event!(
                        target: LOGGER_TARGET,
                        $tracing_level,
                        flare_logger = name,
                        message = %message
                    );
                }
            };
        }
        match level {
            FlareLevel::Trace => emit_at!(tracing::Level::TRACE),
            FlareLevel::Debug => emit_at!(tracing::Level::DEBUG),
            FlareLevel::Info => emit_at!(tracing::Level::INFO),
            FlareLevel::Warn => emit_at!(tracing::Level::WARN),
            FlareLevel::Error | FlareLevel::Critical => emit_at!(tracing::Level::ERROR),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证 Logger 是 Send+Sync+Clone.
    ///
    /// 参数：无；断言：类型满足跨线程共享要求，clone 后名称不变.
    #[test]
    fn logger_is_send_sync_and_clone() {
        /// 编译期断言类型满足 Send+Sync；参数无.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Logger>();
        let log = Logger::new("svc");
        assert_eq!(log.clone().name(), "svc");
    }

    /// 验证注册表并发安全.
    ///
    /// 参数：无；断言：32 个线程同时 `get_logger` 同一名字，全部返回同一个 `Arc` 对象.
    #[test]
    fn get_logger_registry_is_thread_safe() {
        let first = Logger::get_logger("registry-thread-safe");
        let handles: Vec<_> = (0..32)
            .map(|_| std::thread::spawn(|| Logger::get_logger("registry-thread-safe")))
            .collect();
        for handle in handles {
            assert!(first.is_same(&handle.join().unwrap()));
        }
    }
}
