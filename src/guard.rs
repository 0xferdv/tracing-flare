//! 生命周期守卫与 service 句柄.
//!
//! 设计原理：`FlareConfig::build/install` 返回 `FlareGuard`，由它持有 router 与所有 service
//! runtime 生命周期.
//! 用户把 guard 活到进程/服务结束；guard drop 时自动 shutdown.`FlareHandle` 是单个 service 的轻量句
//! 柄，用于 flush/close/reopen/健康查询，不拥有 writer 线程.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::LoggerConfig;
use crate::engine::runtime::{CloseReport, Logger, LoggerSnapshot};
use crate::router::{Router, ServiceEntry};
use crate::stats::FlareStatsSnapshot;

/// 持有 Flare 全部 runtime 的生命周期守卫.
///
/// 字段：`router` 是 service 注册/路由表；`default_timeout` 是 flush/shutdown 未显式给超时时使用的
/// 默认预算.`FlareGuard` 不实现 Clone；同一进程可以有多个 guard（例如测试），但全局 install 只允许
/// 一个 subscriber.
#[non_exhaustive]
pub struct FlareGuard {
    router: Arc<Router>,
    default_timeout: Duration,
}

/// 单个已配置 service 的句柄.
///
/// 字段：`entry` 是 router 中的 service 条目（名字/target/logger/实例锁）.
/// Clone 只增加引用计数，不复制 writer.
#[derive(Clone)]
#[non_exhaustive]
pub struct FlareHandle {
    entry: Arc<ServiceEntry>,
}

/// 所有 service 与 layer 的聚合健康.
///
/// 字段：`services` 是每个 service 的快照；`layer` 是路由/过滤/转发计数.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct FlareHealth {
    /// 每个 service 的健康快照.
    pub services: Vec<ServiceHealth>,
    /// layer 级计数.
    pub layer: FlareStatsSnapshot,
}

/// 单个 service 的健康.
///
/// 字段：`name`/`target` 是路由名；`snapshot` 是 runtime 计数与 sink 统计.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct ServiceHealth {
    /// service 名.
    pub name: String,
    /// tracing 路由 target.
    pub target: String,
    /// runtime 快照.
    pub snapshot: LoggerSnapshot,
}

/// [`FlareGuard::shutdown_report`] 的结构化结果.
///
/// 字段：`ok` 表示所有 service 的 flush/close 都成功；`timed_out` 表示至少一个 service 超过共享
/// deadline；`services` 是逐 service 结果.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct FlareShutdownReport {
    /// 所有 service 是否成功完成 flush/close.
    pub ok: bool,
    /// 是否至少一个 service 超时.
    pub timed_out: bool,
    /// 逐 service 结果.
    pub services: Vec<ServiceShutdownReport>,
}

/// 单个 service 的关停结果.
///
/// 字段：`name`/`target` 标识 service；`report` 是 `ok/timed_out`；`snapshot` 是关停尝试后的最终计
/// 数与 sink 健康.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct ServiceShutdownReport {
    /// service 名.
    pub name: String,
    /// tracing 路由 target.
    pub target: String,
    /// 关停报告.
    pub report: CloseReport,
    /// 关停后的快照.
    pub snapshot: LoggerSnapshot,
}

impl FlareGuard {
    /// 创建 guard（内部使用）.
    ///
    /// 参数：`router` 是已构建的 router；`default_timeout` 是默认 flush/shutdown 预算.
    /// 返回值：拥有 runtime 的 guard.
    pub(crate) fn new(router: Arc<Router>, default_timeout: Duration) -> Self {
        Self {
            router,
            default_timeout,
        }
    }

    /// 返回 router 的共享引用（内部使用）.
    ///
    /// 参数：无；返回值：`Arc<Router>`；用于 layer/logger 注册.
    pub(crate) fn router(&self) -> Arc<Router> {
        Arc::clone(&self.router)
    }

    /// 注册为 `Logger::get_logger` 自动动态创建的默认 router.
    ///
    /// 参数：无；返回值：无.
    /// 适用于 `build()` + `with_default` 用户；后注册会替换前一个.
    pub fn register_default_router(&self) {
        crate::logger::register_default_router(self.router());
    }

    /// 刷新所有 service，共享一个 deadline.
    ///
    /// 参数：`timeout` 是总预算；`None` 用默认值.
    /// 返回值：所有 service 是否都成功 flush.
    /// 每个 service 使用剩余预算，不会把最坏等待放大成 N×timeout.
    pub fn flush(&self, timeout: Option<Duration>) -> bool {
        let deadline = deadline(timeout.unwrap_or(self.default_timeout));
        let mut ok = true;
        for entry in self.router.all_entries() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ok &= entry.logger.flush(remaining);
        }
        ok
    }

    /// 外部 logrotate 后重开所有 service 的 sink.
    ///
    /// 参数：`timeout` 是总预算；`None` 用默认值.
    /// 返回值：所有 service 是否都成功重开.
    pub fn reopen(&self, timeout: Option<Duration>) -> bool {
        let deadline = deadline(timeout.unwrap_or(self.default_timeout));
        let mut ok = true;
        for entry in self.router.all_entries() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ok &= entry.logger.reopen(remaining);
        }
        ok
    }

    /// 排空并关闭所有 service；幂等.
    ///
    /// 参数：`timeout` 是总预算；`None` 用默认值.
    /// 返回值：`shutdown_report().ok`.
    pub fn shutdown(&self, timeout: Option<Duration>) -> bool {
        self.shutdown_report(timeout).ok
    }

    /// 排空并关闭所有 service，返回逐 service 结果.
    ///
    /// 参数：`timeout` 是共享总预算.
    /// 返回值：`FlareShutdownReport`.
    /// 与 `shutdown` 的区别是能区分超时与 flush/close 失败，并给出最终计数.
    pub fn shutdown_report(&self, timeout: Option<Duration>) -> FlareShutdownReport {
        self.router.mark_closed();
        let deadline = deadline(timeout.unwrap_or(self.default_timeout));
        let mut services = Vec::new();
        for entry in self.router.all_entries() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let report = entry.logger.close_with_report(remaining);
            if !report.timed_out {
                // 超时关停可能仍有 writer 在写；保留实例锁，防止另一个进程并发写同一文件.
                entry.release_lock();
            }
            services.push(ServiceShutdownReport {
                name: entry.name.clone(),
                target: entry.target.clone(),
                report,
                snapshot: entry.logger.snapshot(),
            });
        }
        FlareShutdownReport {
            ok: services.iter().all(|service| service.report.ok),
            timed_out: services.iter().any(|service| service.report.timed_out),
            services,
        }
    }

    /// fork 子进程：丢弃继承的 runtime，并关闭继承的锁句柄.
    ///
    /// 参数：无；返回值：无.
    /// 子进程必须立即调用（或重新构建配置）；绝不释放父进程的实例锁.
    pub fn after_fork_child(&self) {
        crate::logger::after_fork_child();
        self.router.discard_after_fork();
    }

    /// 按 service 名或 target 取句柄.
    ///
    /// 参数：`name` 是 service 名/target；返回值：存在的句柄或 `None`.
    pub fn handle(&self, name: &str) -> Option<FlareHandle> {
        self.router.find(name).map(|entry| FlareHandle { entry })
    }

    /// 创建/复用动态 service 并返回同步/异步门面.
    ///
    /// 参数：`name` 是 service 名.
    /// 返回值：门面或创建错误.
    /// 需要 `UnknownTarget::Create`；该操作会做文件系统工作，应在启动期调用，不要放进 async 热路径.
    pub fn logger(&self, name: &str) -> Result<crate::Logger, crate::FlareError> {
        self.router.ensure_service(name)?;
        Ok(crate::Logger::new(name))
    }

    /// 创建/复用动态 service 并返回生命周期句柄.
    ///
    /// 参数：`name` 是 service 名；返回值：句柄或创建错误.
    pub fn service(&self, name: &str) -> Result<FlareHandle, crate::FlareError> {
        self.router
            .ensure_service(name)
            .map(|entry| FlareHandle { entry })
    }

    /// 带显式配置创建/复用动态 service，返回门面.
    ///
    /// 参数：`name` 是 service 名；`config` 是生产/覆盖配置.
    /// 返回值：门面或创建错误.
    /// service 已存在时现有配置优先；创建需要 `UnknownTarget::Create`.
    pub fn logger_with_config(
        &self,
        name: &str,
        config: LoggerConfig,
    ) -> Result<crate::Logger, crate::FlareError> {
        self.router
            .ensure_service_with_config(name, config.into_service())?;
        Ok(crate::Logger::new(name))
    }

    /// 带显式配置创建/复用动态 service，返回句柄.
    ///
    /// 参数：`name` 是 service 名；`config` 是覆盖配置；返回值：句柄或创建错误.
    pub fn service_with_config(
        &self,
        name: &str,
        config: LoggerConfig,
    ) -> Result<FlareHandle, crate::FlareError> {
        self.router
            .ensure_service_with_config(name, config.into_service())
            .map(|entry| FlareHandle { entry })
    }

    /// 返回所有 service + layer 的健康快照.
    ///
    /// 参数：无；返回值：`FlareHealth`.
    /// 逐 service 读取快照，不是原子一致快照.
    pub fn health(&self) -> FlareHealth {
        FlareHealth {
            services: self
                .router
                .all_entries()
                .iter()
                .map(|entry| ServiceHealth {
                    name: entry.name.clone(),
                    target: entry.target.clone(),
                    snapshot: entry.logger.snapshot(),
                })
                .collect(),
            layer: self.router.stats().snapshot(),
        }
    }

    /// 返回 layer 计数快照.
    ///
    /// 参数：无；返回值：seen/filtered/forwarded/rejected 等计数.
    pub fn stats(&self) -> FlareStatsSnapshot {
        self.router.stats().snapshot()
    }
}

impl Drop for FlareGuard {
    /// drop 时自动 shutdown.
    ///
    /// 参数：`self`；返回值：无.
    /// 忽略结果，保证析构不 panic；需要结果请显式调用 shutdown.
    fn drop(&mut self) {
        let _ = self.shutdown(None);
    }
}

impl FlareHandle {
    /// 返回 service 名.
    ///
    /// 参数：无；返回值：名字引用.
    pub fn name(&self) -> &str {
        &self.entry.name
    }

    /// 返回 tracing target.
    ///
    /// 参数：无；返回值：target 引用.
    pub fn target(&self) -> &str {
        &self.entry.target
    }

    /// 刷新本 service.
    ///
    /// 参数：`timeout` 是总预算；`None` 用 5 秒.
    /// 返回值：是否成功 flush.
    pub fn flush(&self, timeout: Option<Duration>) -> bool {
        self.entry
            .logger
            .flush(timeout.unwrap_or(Duration::from_secs(5)))
    }

    /// 外部 logrotate 后重开本 service.
    ///
    /// 参数：`timeout` 是总预算；`None` 用 5 秒.
    /// 返回值：是否成功重开.
    pub fn reopen(&self, timeout: Option<Duration>) -> bool {
        self.entry
            .logger
            .reopen(timeout.unwrap_or(Duration::from_secs(5)))
    }

    /// 排空并关闭本 service.
    ///
    /// 参数：`timeout` 是总预算；`None` 用 5 秒.
    /// 返回值：`close_report().ok`.
    pub fn close(&self, timeout: Option<Duration>) -> bool {
        self.close_report(timeout).ok
    }

    /// 排空并关闭本 service，返回结构化结果.
    ///
    /// 参数：`timeout` 是总预算；`None` 用 5 秒.
    /// 返回值：`CloseReport`.
    /// 非超时情况下释放实例锁；超时保留锁，避免其他进程并发写.
    pub fn close_report(&self, timeout: Option<Duration>) -> CloseReport {
        let report = self
            .entry
            .logger
            .close_with_report(timeout.unwrap_or(Duration::from_secs(5)));
        if !report.timed_out {
            self.entry.release_lock();
        }
        report
    }

    /// 返回本 service 的 runtime 快照.
    ///
    /// 参数：无；返回值：计数与 sink 统计.
    pub fn snapshot(&self) -> LoggerSnapshot {
        self.entry.logger.snapshot()
    }

    /// 访问底层引擎 logger（高级用法）.
    ///
    /// 参数：无；返回值：`&Logger`.
    /// 可直接调用 `log_many` 批量路径；调用方需自行遵守非阻塞/关停语义.
    pub fn logger(&self) -> &Logger {
        &self.entry.logger
    }
}

/// 安全计算 deadline.
///
/// 参数：`timeout` 是调用方预算.
/// 返回值：deadline.`Instant::checked_add` 溢出时回退到24 小时，避免极端 `Duration::MAX` 触发panic.
fn deadline(timeout: Duration) -> Instant {
    Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(24 * 3600))
}
