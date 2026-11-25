//! 自包含的日志引擎.
//!
//! 架构分层（从入口到落点）：
//! - `runtime`：有界队列、生产者入队、每 service 单 writer、flush/close/reopen 屏障；
//! - `sink` / `console`：文件与控制台落点，负责缓冲、轮转、压缩调度；
//! - `rotation` / `compress`：时间/大小/组合轮转与归档压缩；
//! - `record` / `level`：不可变记录与核心级别类型.
//!
//! 设计原理：生产者只做过滤、格式化、入队；文件 I/O、轮转、压缩、清理全部在 writer线程串行执行.
//! 这样业务线程不会被磁盘阻塞，同时同一 service 内保持 FIFO.
//! 引擎不依赖tracing，也不依赖任何外部 logger crate，tracing 只是它的一种入口适配.

pub(crate) mod compress;
pub(crate) mod console;
pub(crate) mod level;
pub(crate) mod record;
pub(crate) mod rotation;
pub(crate) mod runtime;
pub(crate) mod sink;
