//! 可选的 `tracing-appender` 兼容后端（"appender-lite"）.
//!
//! 它与完整 Flare engine 刻意分离，适合 CLI、工具和低吞吐服务：这些场景希望直接使用标准
//! `tracing-subscriber` + rolling file appender 组合.
//! 它**不**提供 Flare 的大小/组合轮转、压缩、keep 语义、per-service 健康、实例锁或 fork 处理.
//!
//! 启用 `appender-lite` feature 后，把返回的 layer 组合进 `tracing-subscriber` registry：
//!
//! ```no_run
//! use tracing_flare::appender::{AppenderLiteConfig, AppenderRotation};
//! use tracing_subscriber::prelude::*;
//!
//! let (layer, guard) = AppenderLiteConfig::new("logs", "app")
//!     .rotation(AppenderRotation::Daily)
//!     .layer()
//!     .expect("appender");
//! let subscriber = tracing_subscriber::registry().with(layer);
//! tracing::subscriber::with_default(subscriber, || tracing::info!("hello"));
//! drop(guard); // guard drop 时刷新非阻塞 worker
//! ```

use std::path::PathBuf;

use tracing::Subscriber;
use tracing_appender::non_blocking::NonBlockingBuilder;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

use crate::error::FlareError;

/// `tracing-appender` 支持的时间轮转粒度.
///
/// `Never/Minutely/Hourly/Daily/Weekly`；默认 `Daily`.
/// 该枚举只覆盖时间轮转，不支持 Flare的大小/组合轮转.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum AppenderRotation {
    /// 不轮转.
    Never,
    /// 每分钟.
    Minutely,
    /// 每小时.
    Hourly,
    /// 每天（默认）.
    #[default]
    Daily,
    /// 每周.
    Weekly,
}

impl AppenderRotation {
    /// 转成 `tracing_appender::rolling::Rotation`.
    ///
    /// 参数：`self`；返回值：对应常量.
    /// 保持公共枚举与依赖解耦.
    fn to_rolling(self) -> Rotation {
        match self {
            Self::Never => Rotation::NEVER,
            Self::Minutely => Rotation::MINUTELY,
            Self::Hourly => Rotation::HOURLY,
            Self::Daily => Rotation::DAILY,
            Self::Weekly => Rotation::WEEKLY,
        }
    }
}

/// appender-lite 配置.
///
/// 字段：`directory` 输出目录；`file_prefix` 文件名前缀；`rotation` 时间粒度；`max_log_files` 保留
/// 文件数（None 不清理）；`buffered_lines` 非阻塞队列行数；`lossy` 满队列时丢弃还是背压.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AppenderLiteConfig {
    directory: PathBuf,
    file_prefix: String,
    rotation: AppenderRotation,
    max_log_files: Option<usize>,
    buffered_lines: usize,
    lossy: bool,
}

impl AppenderLiteConfig {
    /// 创建每日 appender 配置.
    ///
    /// 参数：`directory` 输出目录；`file_prefix` 文件名前缀.
    /// 返回值：默认 Daily、128k 行缓冲、lossy=true 的配置.
    /// 文件名由 tracing-appender 生成为 `<prefix>.<date>.log`.
    pub fn new(directory: impl Into<PathBuf>, file_prefix: impl Into<String>) -> Self {
        Self {
            directory: directory.into(),
            file_prefix: file_prefix.into(),
            rotation: AppenderRotation::Daily,
            max_log_files: None,
            buffered_lines: 128 * 1024,
            // 非阻塞 + lossy 与 tracing-appender 默认行为一致；另一种选择（背压）会在队列满时
            // 阻塞调用方线程.
            lossy: true,
        }
    }

    /// 设置轮转粒度；参数 `rotation`，返回链式自身.
    pub fn rotation(mut self, rotation: AppenderRotation) -> Self {
        self.rotation = rotation;
        self
    }

    /// 设置保留文件数.
    ///
    /// 参数：`n` 是保留数；0 表示禁用清理.
    /// 返回值：链式自身.
    pub fn max_log_files(mut self, n: usize) -> Self {
        self.max_log_files = (n > 0).then_some(n);
        self
    }

    /// 设置非阻塞队列行数.
    ///
    /// 参数：`lines` 是缓冲行数；返回值：链式自身.
    /// 实际使用 `.max(1)`，避免 0 容量.
    pub fn buffered_lines(mut self, lines: usize) -> Self {
        self.buffered_lines = lines;
        self
    }

    /// 设置满队列策略.
    ///
    /// 参数：`lossy` 为 true 时丢弃（非阻塞），false 时背压可能阻塞调用线程；返回值：链式自身.
    /// 默认 true，保证 async 调用方不会被日志阻塞.
    pub fn lossy(mut self, lossy: bool) -> Self {
        self.lossy = lossy;
        self
    }

    /// 构建 `tracing-subscriber` 格式化 layer 与 worker guard.
    ///
    /// 参数：`self` 是配置.
    /// 返回值：`(impl Layer<S>, AppenderLiteGuard)` 或配置错误.
    /// guard 必须活到 subscriber 结束；drop 时 flush 并 join 非阻塞 worker.
    pub fn layer<S>(self) -> Result<(impl Layer<S>, AppenderLiteGuard), FlareError>
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup> + 'static,
    {
        validate_file_prefix(&self.file_prefix)?;
        let mut builder = RollingFileAppender::builder()
            .rotation(self.rotation.to_rolling())
            .filename_prefix(self.file_prefix)
            .filename_suffix("log");
        if let Some(max) = self.max_log_files {
            builder = builder.max_log_files(max);
        }
        let appender = builder
            .build(&self.directory)
            .map_err(|error| FlareError::Config(format!("appender-lite init failed: {error}")))?;
        let (writer, worker) = NonBlockingBuilder::default()
            .buffered_lines_limit(self.buffered_lines.max(1))
            .lossy(self.lossy)
            .thread_name("flare-appender-lite")
            .finish(appender);
        let layer = tracing_subscriber::fmt::layer()
            .with_writer(writer)
            .with_ansi(false);
        Ok((layer, AppenderLiteGuard { _worker: worker }))
    }
}

/// 持有 appender-lite worker 的 guard.
///
/// 字段：`_worker` 是 tracing-appender 的 `WorkerGuard`；drop 时 flush/join.
#[non_exhaustive]
pub struct AppenderLiteGuard {
    _worker: tracing_appender::non_blocking::WorkerGuard,
}

impl AppenderLiteGuard {
    /// 显式 flush 并 join worker.
    ///
    /// 参数：`self`；返回值：无.
    /// 等价于 drop，但语义更清晰.
    pub fn shutdown(self) {
        drop(self);
    }
}

/// 校验文件名前缀安全.
///
/// 参数：`prefix` 是用户提供的前缀.
/// 返回值：合法 `Ok(())`，否则配置错误.
/// 拒绝空、分隔符、NUL、控制字符、`.`/`..`；Windows 额外拒绝 `:`、尾随点/空格.
fn validate_file_prefix(prefix: &str) -> Result<(), FlareError> {
    if prefix.is_empty() {
        return Err(FlareError::Config(
            "appender-lite file_prefix must not be empty".to_string(),
        ));
    }
    if prefix.contains(['/', '\\', '\0'])
        || prefix.chars().any(char::is_control)
        || prefix == "."
        || prefix == ".."
    {
        return Err(FlareError::Config(format!(
            "appender-lite file_prefix is not a safe file name: {prefix:?}"
        )));
    }
    #[cfg(windows)]
    if prefix.contains(':') || prefix.ends_with('.') || prefix.ends_with(' ') {
        return Err(FlareError::Config(format!(
            "appender-lite file_prefix is not a safe Windows file name: {prefix:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证文件名前缀安全规则.
    ///
    /// 参数：无；断言：空、`.`、`..`、分隔符、NUL 被拒绝；普通前缀通过.
    #[test]
    fn rejects_unsafe_prefixes() {
        for prefix in ["", ".", "..", "a/b", "a\\b", "a\0b"] {
            assert!(validate_file_prefix(prefix).is_err(), "{prefix:?}");
        }
        assert!(validate_file_prefix("my-app").is_ok());
    }
}
