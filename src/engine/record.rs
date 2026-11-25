//! 记录类型：生产者和 sink 之间传递的不可变单元.
//!
//! 设计原理：
//! - 记录一旦入队就不再修改，sink 只读，因此 writer 不需要对单条记录加锁；
//! - `message` 是**已经渲染好的最终文本**（文本模板或 JSON 行），文件 sink 直接写字节；
//! - `fields` 只给结构化 sink/JSON 使用，文本模板不自动追加字段；
//! - message/target/字段 key 使用 `Arc<str>`，clone 只增加引用计数，不复制字符串；
//! - `fields` 内联容量 2，常见 tracing 事件不产生堆分配，同时控制队列槽大小.
//!
//! 参数/接口总原则：所有构造器都返回拥有所有权的 `Record`，调用方用 builder 风格链式补充
//! target/location/field；这些方法只改变元数据，不会触发 I/O.

use std::sync::Arc;
use std::time::SystemTime;

use smallvec::SmallVec;

use crate::engine::level::Level;

/// 结构化字段值，供 tracing 适配器、JSON 渲染和自定义 sink 使用.
///
/// 设计取舍：字符串用 `Arc<str>` 共享；数值保留原始类型而不是统一转字符串，让 JSON能输出真正的
/// number；`F64` 允许 NaN/Inf，JSON 渲染时再决定降级方式.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum FieldValue {
    /// 字符串值；来自 tracing `&str`/`String` 或内部 interning.
    Str(Arc<str>),
    /// 有符号整数.
    I64(i64),
    /// 无符号整数.
    U64(u64),
    /// 浮点数；JSON 输出时非有限值需要特殊处理.
    F64(f64),
    /// 布尔值.
    Bool(bool),
}

impl std::fmt::Display for FieldValue {
    /// 以人类可读文本输出字段值.
    ///
    /// 参数：`formatter` 是目标格式化器.
    /// 返回值：写入结果.
    /// 字符串直接写，数值用标准`Display`；该实现不保证 JSON 语义（字符串不带引号），JSON 渲染不要复
    /// 用它.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::Str(value) => formatter.write_str(value),
            FieldValue::I64(value) => write!(formatter, "{value}"),
            FieldValue::U64(value) => write!(formatter, "{value}"),
            FieldValue::F64(value) => write!(formatter, "{value}"),
            FieldValue::Bool(value) => write!(formatter, "{value}"),
        }
    }
}

/// 一个结构化字段：key 与值.
///
/// 参数/字段说明：
/// - `key` 用 `Arc<str>`：tracing 字段名来自静态 callsite 元数据，layer 会做线程内缓存，
///   多个记录共享同一份 key；
/// - `value` 是拥有所有权的 `FieldValue`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Field {
    /// 字段名；通常是 tracing 宏里的字面量.
    pub key: Arc<str>,
    /// 字段值.
    pub value: FieldValue,
}

impl Field {
    /// 构造一个字段.
    ///
    /// 参数：
    /// - `key`：任何可转成 `Arc<str>` 的类型（`&str`/`String`/`Arc<str>`）；
    /// - `value`：任何可转成 `FieldValue` 的类型（见下方 `From` 实现）.
    ///
    /// 返回值：拥有所有权的 `Field`；不会做额外校验（字段名长度由 layer 的`MAX_FIELDS` 与文本渲染转
    /// 义共同兜底）.
    pub fn new(key: impl Into<Arc<str>>, value: impl Into<FieldValue>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl From<&str> for FieldValue {
    /// 借用字符串转字段值.
    ///
    /// 参数：`value` 在调用栈内有效，因此必须复制进 `Arc<str>`，让记录跨线程/跨队列存活；这是字段值
    /// 路径上唯一不可避免的分配之一.
    fn from(value: &str) -> Self {
        FieldValue::Str(Arc::from(value))
    }
}

impl From<String> for FieldValue {
    /// 拥有字符串转字段值.
    ///
    /// 参数：`value` 由调用方交出所有权；转成 `Arc<str>` 时可能复制（取决于容量），但调用方不再需要
    /// 它.
    fn from(value: String) -> Self {
        FieldValue::Str(Arc::from(value))
    }
}

impl From<Arc<str>> for FieldValue {
    /// 已共享字符串直接复用，不发生复制.
    ///
    /// 参数：`value` 是引用计数字符串；返回值直接持有它，适合 layer 的 interning 路径.
    fn from(value: Arc<str>) -> Self {
        FieldValue::Str(value)
    }
}

impl From<i64> for FieldValue {
    /// 有符号整数转字段值；参数 `value` 原样保存.
    fn from(value: i64) -> Self {
        FieldValue::I64(value)
    }
}

impl From<u64> for FieldValue {
    /// 无符号整数转字段值；参数 `value` 原样保存.
    fn from(value: u64) -> Self {
        FieldValue::U64(value)
    }
}

impl From<f64> for FieldValue {
    /// 浮点数转字段值；参数 `value` 原样保存（包括 NaN/Inf）.
    fn from(value: f64) -> Self {
        FieldValue::F64(value)
    }
}

impl From<bool> for FieldValue {
    /// 布尔值转字段值；参数 `value` 原样保存.
    fn from(value: bool) -> Self {
        FieldValue::Bool(value)
    }
}

/// 调用点元数据.
///
/// 设计原理：
/// - `target` 总是存在，用于路由/自定义 sink；用 `Arc<str>` 让 layer 的 target 缓存可复用；
/// - `module_path`/`file`/`line` 可选：不是所有事件都有定位信息，缺省不分配；
/// - `timestamp` 在事件发生时采样一次，文本头与结构化 sink 共用同一时刻.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Metadata {
    /// 路由目标（service 名或 tracing target）.
    pub target: Arc<str>,
    /// 调用方模块路径；没有调用点信息时为 `None`.
    pub module_path: Option<Arc<str>>,
    /// 调用方源文件；没有调用点信息时为 `None`.
    pub file: Option<Arc<str>>,
    /// 调用方行号；没有调用点信息时为 `None`.
    pub line: Option<u32>,
    /// 事件时间戳，由 layer 统一采样，保证文本头与记录一致.
    pub timestamp: SystemTime,
}

impl Metadata {
    /// 用当前时间创建元数据.
    ///
    /// 参数：`target` 任何可转 `Arc<str>` 的路由目标.
    /// 返回值：不含定位信息的元数据.
    /// 该构造器会调用一次 `SystemTime::now()`；热路径更推荐 [`Metadata::at`] 共享layer 已采样的时间
    /// .
    pub fn new(target: impl Into<Arc<str>>) -> Self {
        Self::at(target, SystemTime::now())
    }

    /// 用调用方提供的时间戳创建元数据.
    ///
    /// 参数：
    /// - `target`：路由目标；
    /// - `timestamp`：事件发生时刻，由 layer 采样一次后同时用于文本渲染与本记录.
    ///
    /// 设计原因：避免每条事件读取两次时钟，并保证文本头与自定义 sink 看到同一时刻.
    pub fn at(target: impl Into<Arc<str>>, timestamp: SystemTime) -> Self {
        Self {
            target: target.into(),
            module_path: None,
            file: None,
            line: None,
            timestamp,
        }
    }

    /// 补充调用点定位信息.
    ///
    /// 参数：
    /// - `module_path`：调用方模块路径（可选）；
    /// - `file`：源文件（可选）；
    /// - `line`：行号（可选）.
    ///
    /// 返回值：链式返回自身.
    /// 三个参数都允许 `None`，表示该事件没有可靠定位信息.
    pub fn with_location(
        mut self,
        module_path: Option<Arc<str>>,
        file: Option<Arc<str>>,
        line: Option<u32>,
    ) -> Self {
        self.module_path = module_path;
        self.file = file;
        self.line = line;
        self
    }
}

/// 一条日志记录.
///
/// 大小约 184 字节（64 位平台，字段内联容量 2）.
/// 不装箱是为了避免生产者热路径多一次堆分配；队列满时单槽移动成本仍可控.
/// 尺寸上限由 `record.rs` 的测试守护.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Record {
    /// 核心严重级别；sink 用它在普通/错误文件之间分流.
    pub level: Level,
    /// 已渲染的最终文本（文本模板或 JSON 行），文件 sink 直接写字节.
    ///
    /// 使用可直接移动的 `String`，layer 可以从复用缓冲 `mem::take` 后零复制入队；
    /// 只有多 handler fan-out 时才 clone.
    pub message: String,
    /// 调用点/时间元数据.
    pub metadata: Metadata,
    /// 结构化字段.
    /// 内联容量 2 覆盖大多数 tracing 事件；超出后 `SmallVec` 才分配堆内存.
    /// 纯文本记录不会为四个空字段槽付出搬移成本.
    pub fields: SmallVec<[Field; 2]>,
    /// 文本是否已经过模板层控制字符转义（或者受信任的堆栈文本）.
    ///
    /// `FlareLayer` 走 `TextFormat` 后设为 true；批量 API 默认 false，文件/终端 sink 会在落盘前
    /// 再做一次控制字符转义. 该字段不公开，避免业务绕过最后一道物理行防线.
    pub(crate) sanitized: bool,
    /// 文本是否已经是完整 JSON 行.
    ///
    /// `json_lines(true)` 的服务用它拒绝普通批量文本，避免污染 JSON Lines 采集管道；
    /// layer 的 JSON 快路径会自动标记. 批量 API 用 [`Self::with_json`] 显式标记.
    pub(crate) json: bool,
}

impl Record {
    /// 用当前时间构造一条无字段记录.
    ///
    /// 参数：
    /// - `level`：核心级别；
    /// - `message`：最终文本（`&str`/`String`）；内部直接拥有 `String`.
    ///
    /// 返回值：`target` 为空、无定位信息的记录；调用方通常随后链式 `.with_target(...)`.
    pub fn new(level: Level, message: impl Into<String>) -> Self {
        Self::at(level, message, SystemTime::now())
    }

    /// 用共享事件时间构造记录（见 [`Metadata::at`]）.
    ///
    /// 参数：
    /// - `level`：核心级别；
    /// - `message`：最终文本；
    /// - `timestamp`：layer 采样的事件时间.
    ///
    /// 设计原因：文本头与 `Record.metadata.timestamp` 必须描述同一时刻；共享时间避免第二次时钟读取.
    pub fn at(level: Level, message: impl Into<String>, timestamp: SystemTime) -> Self {
        Self {
            level,
            message: message.into(),
            metadata: Metadata::at("", timestamp),
            fields: SmallVec::new(),
            sanitized: false,
            json: false,
        }
    }

    /// 标记该记录已经是单行 JSON；参数 `self`，返回值：链式自身.
    ///
    /// 批量 API 配合 `FlareConfig::json_lines(true)` 时必须调用，否则引擎会按格式错误丢弃.
    #[must_use]
    pub fn with_json(mut self) -> Self {
        self.json = true;
        self
    }

    /// 返回记录是否标记为 JSON 行；参数无.
    pub fn is_json(&self) -> bool {
        self.json
    }

    /// 标记文本已由模板层转义.
    ///
    /// 参数：`self`；返回值：链式返回自身.
    /// 只允许 crate 内调用；业务批量记录保持 false，让 sink 执行最终的物理行转义.
    pub(crate) fn mark_sanitized(mut self) -> Self {
        self.sanitized = true;
        self
    }

    /// 标记 layer 已生成 JSON 行；参数 `self`，返回值：链式自身.
    pub(crate) fn mark_json(mut self) -> Self {
        self.json = true;
        self
    }

    /// 设置路由 target.
    ///
    /// 参数：`target` 可转 `Arc<str>`.
    /// 返回值：链式返回自身.
    /// 若 layer 已做 target 缓存，传入 `Arc` 可只增加引用计数.
    pub fn with_target(mut self, target: impl Into<Arc<str>>) -> Self {
        self.metadata.target = target.into();
        self
    }

    /// 设置调用点定位信息.
    ///
    /// 参数：`module_path`/`file`/`line` 均可为 `None`；返回值：链式返回自身.
    /// 该方法是纯元数据写入，不分配（除非传入的 `String` 需要转 `Arc`）.
    pub fn with_location(
        mut self,
        module_path: Option<Arc<str>>,
        file: Option<Arc<str>>,
        line: Option<u32>,
    ) -> Self {
        self.metadata.module_path = module_path;
        self.metadata.file = file;
        self.metadata.line = line;
        self
    }

    /// 追加一个结构化字段.
    ///
    /// 参数：`field` 拥有所有权的字段；返回值：链式返回自身.
    /// 内联容量 2 内不分配；超过后 `SmallVec` 扩容.
    /// 字段数量上限由 layer 的 `MAX_FIELDS` 在采集阶段控制.
    pub fn with_field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 守护队列单条记录尺寸.
    ///
    /// 参数：无.
    /// 断言：`Record` 不超过 256 字节.
    /// 原因：队列按值存储记录，尺寸增长会线性放大每个 logger 的常驻内存；256 字节为第三个内联字段留
    /// 出余量.
    #[test]
    fn record_stays_small_enough_for_the_queue_budget() {
        let size = std::mem::size_of::<Record>();
        assert!(size <= 256, "Record grew to {size} bytes");
    }
}
