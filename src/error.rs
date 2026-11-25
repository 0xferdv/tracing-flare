//! tracing-flare 的公共错误类型.
//!
//! 设计目标：
//! - **构建期可区分**：配置、logger 构建、文件系统、全局 subscriber、实例锁分别有变体，
//!   调用方可以根据错误决定“改配置 / 重试 / 换路径 / 使用已有 subscriber”；
//! - **保留 source**：`Io` 用 `#[from] io::Error`，不把底层错误拍平成字符串；
//! - **不 panic**：所有可恢复失败通过 `Result<_, FlareError>` 返回；
//! - **转换克制**：只实现 `From<String>`/`From<&str>` 作为配置错误的便捷入口，
//!   不使用 blanket `AsRef<str>`（会放宽错误分类并与 identity/未来 impl 产生 coherence 风险）.

use std::io;

use thiserror::Error;

/// 构建或安装 Flare subscriber 时可能返回的错误.
///
/// 生命周期：错误在 `build()` / `install()` / `log_many` 等入口产生；运行时写盘失败不会走这个类型，
/// 而是进入 sink 计数与健康快照，避免热路径返回错误.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FlareError {
    /// 配置非法：路径组件、轮转参数、模板语法、service 名、限流等.
    ///
    /// 参数 `String` 是面向人的完整说明；这里不保存结构化配置快照，避免错误对象过大.
    #[error("config error: {0}")]
    Config(String),
    /// 底层 core 拒绝构建 logger/sink（例如队列为 0、没有 sink、重复 sink 名）.
    #[error("logger build failed: {0}")]
    Build(String),
    /// 准备 sink 时的文件系统错误.
    ///
    /// `#[from]` 让 `?` 可以直接把 `io::Error` 转成 `FlareError::Io`，保留
    /// `std::error::Error::source()` 链，便于上层打印完整原因.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// 进程级 tracing subscriber 已被其他代码安装，`install()` 无法接管.
    #[error("failed to install tracing subscriber: {0}")]
    Subscriber(String),
    /// 单实例锁获取/释放失败（另一个进程持有同一日志文件，或锁文件不可用）.
    #[error("single-instance lock error: {0}")]
    Lock(String),
}

impl From<String> for FlareError {
    /// 把已经构造好的配置错误信息转成 `Config`.
    ///
    /// 参数 `message` 被原样保存；`From` 不在这里追加前缀，`#[error]` 统一渲染`config error: ...`，
    /// 避免同一条信息出现两次前缀.
    fn from(message: String) -> Self {
        Self::Config(message)
    }
}

impl From<&str> for FlareError {
    /// 把静态/借用的配置错误信息复制为 `Config`.
    ///
    /// 参数 `message` 只在调用栈内有效，因此必须 `to_string()` 拥有化；错误对象会跨函数返回，不能持
    /// 有借用.
    fn from(message: &str) -> Self {
        Self::Config(message.to_string())
    }
}
