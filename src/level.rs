//! Flare 的六级日志级别：tracing 五级 + CRITICAL 兼容层.
//!
//! 设计原理：
//! - `tracing` 只有 TRACE/DEBUG/INFO/WARN/ERROR 五级，而 Flare/Python 侧需要 CRITICAL；
//! - 对外保留六个级别，内部引擎的核心 `Level` 只有五档（无 TRACE），因此 TRACE 在过滤时
//!   映射到 DEBUG，但渲染的 `levelname`/`levelno` 仍保留 TRACE，避免信息丢失；
//! - CRITICAL 在 tracing 事件上表现为 ERROR + `flare_level = "CRITICAL"` 字段，layer 在
//!   路由后恢复为 CRITICAL，再参与阈值判断与格式化.
//!
//! 排序语义：`Trace < Debug < Info < Warn < Error < Critical`，因此 `level >= threshold`可以直接用
//! 于级别过滤.

/// Flare 配置与门面使用的日志级别.
///
/// 派生 `Ord` 的顺序就是严重程度顺序；`Default` 为 `Info`，与多数生产配置一致.
/// `serde` 可选序列化用于健康/配置快照.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub enum FlareLevel {
    /// 最细粒度诊断，默认不落盘；引擎内部按 DEBUG 过滤槽处理.
    Trace,
    /// 开发期诊断.
    Debug,
    /// 常规信息，默认阈值.
    #[default]
    Info,
    /// 可恢复异常/警告；渲染名为 `WARNING`.
    Warn,
    /// 错误；默认进入错误文件 sink.
    Error,
    /// 严重错误；tracing 无此级别，通过 `flare_level` 字段恢复.
    Critical,
}

impl FlareLevel {
    /// 把 tracing 级别映射到 Flare 级别.
    ///
    /// 参数：`level` 是 `tracing::Level`，只有五个合法值.
    /// 返回值：对应 Flare 级别；tracing 没有 CRITICAL，因此不会返回 `Critical`.
    pub const fn from_tracing(level: tracing::Level) -> Self {
        match level {
            tracing::Level::TRACE => Self::Trace,
            tracing::Level::DEBUG => Self::Debug,
            tracing::Level::INFO => Self::Info,
            tracing::Level::WARN => Self::Warn,
            tracing::Level::ERROR => Self::Error,
        }
    }

    /// 映射到引擎核心级别.
    ///
    /// 参数：`self` 是 Flare 级别.
    /// 返回值：核心 `Level`.
    /// 设计原因：核心没有 TRACE，TRACE 与 DEBUG 共用过滤槽；CRITICAL 映射到核心 CRITICAL，以便错误
    /// 文件分流与阈值判断保持正确.
    pub(crate) const fn core(self) -> crate::engine::level::Level {
        match self {
            Self::Trace | Self::Debug => crate::engine::level::Level::Debug,
            Self::Info => crate::engine::level::Level::Info,
            Self::Warn => crate::engine::level::Level::Warning,
            Self::Error => crate::engine::level::Level::Error,
            Self::Critical => crate::engine::level::Level::Critical,
        }
    }

    /// 模板字段 `%(levelno)d` 使用的数值.
    ///
    /// 参数：`self`.
    /// 返回值：5/10/20/30/40/50，与 Python logging 对齐；TRACE 单独用 5，避免与 DEBUG 的 10 冲突.
    pub const fn levelno(self) -> i32 {
        match self {
            Self::Trace => 5,
            Self::Debug => 10,
            Self::Info => 20,
            Self::Warn => 30,
            Self::Error => 40,
            Self::Critical => 50,
        }
    }

    /// 模板字段 `%(levelname)s` 使用的大写标签.
    ///
    /// 参数：`self`.
    /// 返回值：静态字符串，便于渲染时零分配.`Warn` 渲染为 `WARNING`，与 Python/常见日志系统保持一致
    /// .
    pub const fn label(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARNING",
            Self::Error => "ERROR",
            Self::Critical => "CRITICAL",
        }
    }

    /// 把 Python 风格 `levelno` 还原为 Flare 级别.
    ///
    /// 参数：`levelno` 是 5/10/20/30/40/50.
    /// 返回值：合法级别；其他值（包括 NOTSET=0）返回 `None`.
    /// 该函数是 [`Self::levelno`] 的逆映射，供 logger 层级状态只存一个 `AtomicI32` 时使用.
    pub const fn from_levelno(levelno: i32) -> Option<Self> {
        match levelno {
            5 => Some(Self::Trace),
            10 => Some(Self::Debug),
            20 => Some(Self::Info),
            30 => Some(Self::Warn),
            40 => Some(Self::Error),
            50 => Some(Self::Critical),
            _ => None,
        }
    }
}

impl From<tracing::Level> for FlareLevel {
    /// 便捷转换：等价于 [`FlareLevel::from_tracing`].
    ///
    /// 参数：`level` 是 tracing 级别.
    /// 返回值：对应 Flare 级别.
    fn from(level: tracing::Level) -> Self {
        Self::from_tracing(level)
    }
}

impl From<FlareLevel> for tracing::Level {
    /// 反向转换到 tracing 级别.
    ///
    /// 参数：`level` 是 Flare 级别.
    /// 返回值：tracing 级别.CRITICAL 没有 tracing 对应项，因此降级为 ERROR；需要保留 CRITICAL 语义
    /// 时必须同时携带 `flare_level` 字段.
    fn from(level: FlareLevel) -> Self {
        match level {
            FlareLevel::Trace => tracing::Level::TRACE,
            FlareLevel::Debug => tracing::Level::DEBUG,
            FlareLevel::Info => tracing::Level::INFO,
            FlareLevel::Warn => tracing::Level::WARN,
            FlareLevel::Error | FlareLevel::Critical => tracing::Level::ERROR,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证 levelno 与 FlareLevel 的双向映射；参数无.
    ///
    /// 断言：所有级别往返一致，NOTSET/非法值返回 `None`.
    #[test]
    fn levelno_round_trips_and_rejects_notset() {
        for level in [
            FlareLevel::Trace,
            FlareLevel::Debug,
            FlareLevel::Info,
            FlareLevel::Warn,
            FlareLevel::Error,
            FlareLevel::Critical,
        ] {
            assert_eq!(FlareLevel::from_levelno(level.levelno()), Some(level));
        }
        assert_eq!(FlareLevel::from_levelno(0), None);
        assert_eq!(FlareLevel::from_levelno(25), None);
    }
}
