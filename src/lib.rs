//! Flare：tracing 原生、自包含的纯 Rust 日志库.
//!
//! 设计分层：
//! - `tracing` 事件/`flare_*` 宏是统一入口，`FlareLayer` 负责路由、级别过滤、格式化和
//!   非阻塞入队；
//! - 每个 service 一个 writer 线程，独占文件/控制台 sink，负责批量写、轮转、压缩、清理；
//! - `Logger` 门面提供 Python 风格的动态名称 API；`FlareConfig` 是唯一的全局构建入口，
//!   默认不安装 subscriber、不替换 allocator、不装信号处理器.
//!
//! 关键保证：热路径不阻塞、资源有界、panic 隔离、关停可证明、丢失可计数.
//! 详见`README.md`、`USAGE.md` 与 `docs/FAULTS.md`.
//!
//! ```no_run
//! use tracing_flare::{FlareConfig, FlareRotation};
//!
//! let _guard = FlareConfig::new()
//!     .base_path("logs")
//!     .rotation(FlareRotation::daily(7))
//!     .service(
//!         tracing_flare::ServiceConfig::new("billing")
//!             .target("billing")
//!             .console(true),
//!     )
//!     .install()
//!     .expect("install flare");
//!
//! tracing_flare::flare_info!(target: "billing", order = 42, "order created");
//!
//! // 动态名称门面（同步 + 异步）：
//! let log = tracing_flare::Logger::new("billing");
//! log.info("order created");
//! // log.ainfo("order created").await;
//! ```

mod allocator;
#[cfg(feature = "appender-lite")]
pub mod appender;
mod config;
mod engine;
mod error;
pub mod format;
pub mod guard;
#[cfg(feature = "python-hierarchy")]
mod hierarchy;
pub mod layer;
pub mod level;
mod lock;
mod logger;
mod macros;
mod router;
pub mod stats;

/// 批量路径使用的核心级别类型；TRACE 在引擎内部表示为 DEBUG.
pub use crate::engine::level::Level as FlareCoreLevel;
/// 高级批量路径的记录类型（`FlareHandle::logger().log_many`）.
pub use crate::engine::record::{
    Field as FlareField, FieldValue as FlareFieldValue, Metadata as FlareMetadata,
    Record as FlareRecord,
};
/// 引擎层公开类型：关停报告、快照、sink 健康/统计.
pub use crate::engine::runtime::{
    CloseReport, Logger as FlareLogger, LoggerSnapshot as FlareSnapshot,
    RecordBatch as FlareRecordBatch, SinkHealth as FlareSinkHealth, SinkStat as FlareSinkStat,
    MAX_PREFORMATTED_BLOCK_BYTES, MAX_SHARDS,
};
/// 配置构建器与轮转/压缩/限流类型.
pub use config::{
    FlareCompression, FlareConfig, FlareOverflow, FlareRotation, LoggerConfig, RateLimit,
    ServiceConfig, SizeSpec, TimeSpec, UnknownTarget,
};
/// 构建/安装阶段的公共错误.
pub use error::FlareError;
/// 文本模板与渲染输入；高级用户可自定义格式.
pub use format::{RenderInput, TextFormat, DEFAULT_DATE_FORMAT, DEFAULT_FORMAT};
/// 生命周期守卫、service 句柄、健康快照与结构化关停报告.
pub use guard::{
    FlareGuard, FlareHandle, FlareHealth, FlareShutdownReport, ServiceHealth, ServiceShutdownReport,
};
/// Python 风格 handler（仅 `python-hierarchy` feature）.
#[cfg(feature = "python-hierarchy")]
pub use hierarchy::Handler;
/// tracing Layer；可单独组合进已有 subscriber.
pub use layer::FlareLayer;
/// Flare 的六级日志级别（tracing 五级 + CRITICAL）.
pub use level::FlareLevel;
/// 动态名称门面（同步/异步方法）.
pub use logger::Logger;
/// layer 级计数器快照.
pub use stats::FlareStatsSnapshot;

/// `flare_*` 宏内部使用的 tracing re-export.
///
/// 公开是为了让宏在调用方 crate 展开时能解析 `$crate::__tracing`；不属于稳定 API，业务代码不应直接
/// 使用.
#[doc(hidden)]
pub use tracing as __tracing;
