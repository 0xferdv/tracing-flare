//! 引擎核心日志级别.
//!
//! 数值与 Python `logging` 对齐（10/20/30/40/50），方便 Python 与 Rust 共用阈值.
//! 设计取舍：核心不设 TRACE，因为 Flare 的落点/过滤/计数只有五档；`tracing::Level::TRACE`在 layer中
//! 映射为 `Debug`，但文本渲染仍使用 `FlareLevel::Trace`，避免丢失展示信息.

use std::fmt;

/// 五个核心级别，数值与 Python logging 一致.
///
/// `#[repr(i32)]` 保证 `as_i32` 的转换是稳定且零成本的；`Ord` 的顺序就是严重程度.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[repr(i32)]
#[non_exhaustive]
pub enum Level {
    /// 开发期诊断，数值 10.
    Debug = 10,
    /// 常规信息，数值 20.
    Info = 20,
    /// 警告，数值 30；标签为 `WARNING`.
    Warning = 30,
    /// 错误，数值 40.
    Error = 40,
    /// 严重错误，数值 50.
    Critical = 50,
}

impl Level {
    /// 按严重程度排列的全部级别.
    ///
    /// 用途：健康快照中的 `emitted[5]` 与 `Logger::ALL` 的下标必须一致；遍历时可直接用
    /// `ALL.iter().enumerate()` 对应 `index()`.
    pub const ALL: [Level; 5] = [
        Level::Debug,
        Level::Info,
        Level::Warning,
        Level::Error,
        Level::Critical,
    ];

    /// 返回底层整数值.
    ///
    /// 参数：`self`.
    /// 返回值：10/20/30/40/50.用于与 Python 配置、健康快照和模板`%(levelno)d` 对齐.
    pub const fn as_i32(self) -> i32 {
        self as i32
    }

    /// 返回大写标签.
    ///
    /// 参数：`self`.
    /// 返回值：静态字符串.`Warning` 渲染为 `WARNING`，与 Python 一致.
    pub const fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warning => "WARNING",
            Level::Error => "ERROR",
            Level::Critical => "CRITICAL",
        }
    }

    /// 把任意整数映射到最近的合法级别.
    ///
    /// 参数：`value` 是外部传入的整数（典型来源是 Python logger 的 int level）.
    /// 返回值：合法 `Level`.
    /// 规则：
    /// - 小于 INFO（20）一律 Debug；
    /// - 其他区间向上取整到下一个标准级别；
    /// - 大于等于 CRITICAL（50）一律 Critical.
    ///
    /// 这样不会因为自定义数值产生无法表示的级别，也不会让阈值过滤出现空洞.
    pub fn from_i32(value: i32) -> Self {
        if value < Level::Info.as_i32() {
            Level::Debug
        } else if value < Level::Warning.as_i32() {
            Level::Info
        } else if value < Level::Error.as_i32() {
            Level::Warning
        } else if value < Level::Critical.as_i32() {
            Level::Error
        } else {
            Level::Critical
        }
    }

    /// 解析级别名.
    ///
    /// 参数：`name` 可带首尾空白，大小写不敏感.
    /// 返回值：无法识别时返回 `None`.
    /// 别名：`WARN` → Warning，`FATAL` → Critical，兼容 Python 与常见配置写法.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.trim().to_ascii_uppercase().as_str() {
            "DEBUG" => Some(Level::Debug),
            "INFO" => Some(Level::Info),
            "WARNING" | "WARN" => Some(Level::Warning),
            "ERROR" => Some(Level::Error),
            "CRITICAL" | "FATAL" => Some(Level::Critical),
            _ => None,
        }
    }

    /// 返回计数器数组使用的稠密下标.
    ///
    /// 参数：`self`.
    /// 返回值：0..4.不能直接用 `as_i32()` 当下标，否则 `[u64; 5]` 需要100 个槽位；该映射保持数组紧
    /// 凑.
    pub const fn index(self) -> usize {
        match self {
            Level::Debug => 0,
            Level::Info => 1,
            Level::Warning => 2,
            Level::Error => 3,
            Level::Critical => 4,
        }
    }
}

impl fmt::Display for Level {
    /// 输出大写标签（`DEBUG`/`INFO`/...
    /// ）.
    ///
    /// 参数：`formatter` 是调用方的格式化器.
    /// 返回值：写入结果.
    /// 实现只写静态字符串，不分配，适合日志/错误消息.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for Level {
    /// 解析失败时返回带原始输入的字符串错误.
    type Err = String;

    /// 从字符串解析级别.
    ///
    /// 参数：`value` 是候选级别名，规则同 [`Level::from_name`].
    /// 返回值：成功为级别，失败为 `unknown level: ...`，便于配置校验直接展示.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_name(value).ok_or_else(|| format!("unknown level: {value:?}"))
    }
}
