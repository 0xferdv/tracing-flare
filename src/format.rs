//! 纯 Rust 文本格式化器.
//!
//! 模板语法沿用 Flare/Python 风格：`[%(asctime)s-%(msecs)03d] [%(process)d:%(thread)d] ...`.
//! 不依赖 Python 或 PyO3.
//!
//! 设计原理：
//! - 模板在 `TextFormat::new` 中编译成 `Piece` 列表，运行期不再解析字符串；
//! - `used` 位掩码记录模板实际引用哪些字段，`Values` 只计算这些字段，避免每条事件
//!   都取 PID/线程名/文件路径；
//! - 默认模板与 `%(message)s` 有专用快路径；其他模板走通用 piece 循环；
//! - 所有输出在返回前统一做控制字符转义，防止日志注入/多行伪造.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use chrono::format::{Item, StrftimeItems};
use chrono::Local;

use crate::error::FlareError;
use crate::level::FlareLevel;

/// 默认行格式，与 Flare core/Python 侧字段集合一致.
///
/// 参数/接口说明：这是 `FlareConfig::new` 使用的默认模板；修改它会影响所有未显式设置`format` 的用户
/// ，必须同步 README/USAGE 与日志分析脚本.
pub const DEFAULT_FORMAT: &str = "[%(asctime)s-%(msecs)03d] [%(process)d:%(thread)d] [%(module_chain)s::%(lineno)d::%(levelname)s] > %(message)s";
/// `%(asctime)s` 的默认日期格式.
///
/// 设计原因：秒级精度 + 毫秒字段分开渲染；日期格式化是昂贵操作，秒级缓存可以复用.
pub const DEFAULT_DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// 进程启动时刻，用于 `%(relativeCreated)f`.
///
/// 懒初始化：只有模板引用该字段时才读取 `Instant::now`，避免为不使用的字段付成本.
static START: OnceLock<Instant> = OnceLock::new();

/// 单线程缓存的默认模板 header.
///
/// 设计原理：默认模板的 header（asctime/pid/thread/module/line/level）在“同一秒 + 同一调用点 + 同一
/// 级别”内不变，只有毫秒和消息变化.
/// 每秒/每调用点重建一次 header 比每条记录走通用 piece 循环便宜得多.
///
/// 字段说明：
/// - `second`：缓存对应的墙钟秒；
/// - `level`：缓存对应的级别（级别标签是 header 的一部分）；
/// - `file`/`line`/`module_key`：调用点键；`module_key` 在无文件时是 module_path/target；
/// - `date_fmt`：防止同一线程上不同 formatter 复用错误前缀；
/// - `prefix`：前缀 `[asctime-`；`suffix`：后缀 `] [pid:thread] [module::line::LEVEL] > `.
struct DefaultHeaderCache {
    second: i64,
    level: FlareLevel,
    file_ptr: usize,
    file_len: usize,
    line: u32,
    module_ptr: usize,
    module_len: usize,
    date_fmt_hash: u64,
    prefix: String,
    suffix: String,
}

impl Default for DefaultHeaderCache {
    /// 构造一个必然失效的初始缓存.
    ///
    /// 参数：无.`second = i64::MIN` 保证第一次渲染一定重建；其他字段为空避免误命中.
    fn default() -> Self {
        Self {
            second: i64::MIN,
            level: FlareLevel::Info,
            file_ptr: 0,
            file_len: 0,
            line: 0,
            module_ptr: 0,
            module_len: 0,
            date_fmt_hash: 0,
            prefix: String::new(),
            suffix: String::new(),
        }
    }
}

thread_local! {
    /// 每个线程独立缓存，避免跨线程锁；header 只在该线程的事件序列中复用.
    static DEFAULT_HEADER: RefCell<DefaultHeaderCache> =
        RefCell::new(DefaultHeaderCache::default());
}

/// 模板宽度/精度硬上限.
///
/// 设计原因：配置笔误（如 `%.*s` 配一个巨大宽度）不能让每条日志变成 GB 级分配；超过上限在编译期直接
/// 报配置错误.
const MAX_FORMAT_WIDTH: usize = 1_000_000;
/// 精度硬上限，理由同 [`MAX_FORMAT_WIDTH`].
const MAX_FORMAT_PRECISION: usize = 1_000_000;

/// `%` 风格格式化的 flags.
///
/// 字段与 printf/Python logging 对齐：`-` 左对齐、`0` 零填充、`+` 强制符号、空格在正数前留空格、`#`
/// 进制前缀.
/// 只保存布尔值，避免为每条记录分配结构.
#[derive(Clone, Copy, Debug, Default)]
struct Flags {
    left: bool,
    zero: bool,
    plus: bool,
    space: bool,
    alt: bool,
}

/// 模板支持的字段.
///
/// 设计原理：用 enum 而不是字符串在运行期匹配；`mask()` 取位掩码，`used` 用 OR 汇总，`Values` 通过
/// 位掩码判断是否需要计算该字段.
#[derive(Clone, Copy, Debug)]
enum Field {
    /// 本地时间文本.
    Asctime,
    /// 当前秒内毫秒（0..999）.
    Msecs,
    /// 进程 ID.
    Process,
    /// 操作系统线程 ID.
    Thread,
    /// 线程名（main 渲染为 MainThread）.
    ThreadName,
    /// 调用方模块路径最后 3 段，`::` 连接.
    ModuleChain,
    /// 调用方行号.
    Lineno,
    /// 级别名.
    LevelName,
    /// 最终消息正文.
    Message,
    /// service/logger 名.
    Name,
    /// 调用方文件名.
    Filename,
    /// 调用方模块名（文件名去扩展名）.
    Module,
    /// 调用方完整路径.
    Pathname,
    /// 调用方函数名；当前固定为 `-`，因为 tracing 元数据不提供可靠函数名.
    FuncName,
    /// 级别数值.
    LevelNo,
    /// UNIX 时间戳（秒，浮点）.
    Created,
    /// 相对进程启动的毫秒数.
    RelativeCreated,
    /// 进程名（可执行文件名）.
    ProcessName,
    /// 第一个 span 名；没有 span 时为 `-`.
    TaskName,
}

impl Field {
    /// 把模板字段名解析为 [`Field`].
    ///
    /// 参数：`name` 是 `%(` 与 `)` 之间的字段名.
    /// 返回值：未知字段返回 `None`，由编译阶段转成配置错误；大小写敏感，与模板/文档保持一致.
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "asctime" => Self::Asctime,
            "msecs" => Self::Msecs,
            "process" => Self::Process,
            "thread" => Self::Thread,
            "threadName" => Self::ThreadName,
            "module_chain" => Self::ModuleChain,
            "lineno" => Self::Lineno,
            "levelname" => Self::LevelName,
            "message" => Self::Message,
            "name" => Self::Name,
            "filename" => Self::Filename,
            "module" => Self::Module,
            "pathname" => Self::Pathname,
            "funcName" => Self::FuncName,
            "levelno" => Self::LevelNo,
            "created" => Self::Created,
            "relativeCreated" => Self::RelativeCreated,
            "processName" => Self::ProcessName,
            "taskName" => Self::TaskName,
            _ => return None,
        })
    }

    /// 返回该字段在 `used` 位掩码中的位.
    ///
    /// 参数：`self`.
    /// 返回值：`1 << enum discriminant`.
    /// 依赖 enum 的稳定顺序；新增字段必须追加在末尾，避免改变已有位.
    const fn mask(self) -> u32 {
        1 << (self as u32)
    }
}

/// 编译后的模板片段.
///
/// `Text(String)` 是字面量；`Field` 保存字段、flags、宽度、精度和转换符.
/// 运行时只遍历这个列表，不再解析原始模板.
#[derive(Clone, Debug)]
enum Piece {
    /// 原样输出的字面量.
    Text(String),
    /// 需要渲染的字段.
    Field {
        /// 字段种类.
        field: Field,
        /// printf 风格 flags.
        flags: Flags,
        /// 最小宽度；`None` 表示不填充.
        width: Option<usize>,
        /// 字符串最大字符数/浮点精度；`None` 表示默认.
        precision: Option<usize>,
        /// 转换符：`s/d/i/u/f/F/e/E/g/G/x/X/o/c`.
        conv: char,
    },
}

/// 编译后不可变的文本格式，可被多个 layer clone 共享.
///
/// 字段说明：
/// - `pieces`：按模板顺序排列的片段；
/// - `date_fmt`：`%(asctime)s` 的日期格式；
/// - `fmt`：原始模板，用于诊断/`template()`；
/// - `used`：模板实际引用字段的位掩码；
/// - `fast_default`：是否可走默认 header 缓存快路径；
/// - `message_only`：是否可走 `%(message)s` 快路径.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TextFormat {
    pieces: Vec<Piece>,
    date_fmt: String,
    fmt: String,
    used: u32,
    /// 默认模板 header 形状固定，可按秒、调用点和级别缓存，不必走通用 piece 列表.
    fast_default: bool,
    /// `%(message)s` 常见于高吞吐 sink，不需要任何 header 工作.
    message_only: bool,
    /// `date_fmt` 的稳定哈希，用于 header 缓存比较，避免每事件比较字符串.
    date_fmt_hash: u64,
}

impl TextFormat {
    /// 编译模板并校验日期格式.
    ///
    /// 参数：
    /// - `fmt`：行模板；空字符串非法；
    /// - `date_fmt`：`asctime` 的 chrono 格式；空或含 `Item::Error` 非法.
    ///
    /// 返回值：成功为编译后的格式；失败为 [`FlareError::Config`]，错误信息包含模板/字段/转换符上下
    /// 文，便于配置阶段直接修正.
    ///
    /// 设计原因：所有非法模板在 `build()` 阶段失败，而不是等到第一条日志才 panic/丢日志.
    pub fn new(fmt: impl Into<String>, date_fmt: impl Into<String>) -> Result<Self, FlareError> {
        let fmt = fmt.into();
        let date_fmt = date_fmt.into();
        if fmt.is_empty() {
            return Err(FlareError::Config("fmt must not be empty".to_string()));
        }
        if date_fmt.is_empty() {
            return Err(FlareError::Config("date_fmt must not be empty".to_string()));
        }
        if StrftimeItems::new(&date_fmt).any(|item| matches!(item, Item::Error)) {
            return Err(FlareError::Config(format!(
                "invalid date_fmt: {date_fmt:?}"
            )));
        }
        let pieces = compile(&fmt)?;
        let used = pieces.iter().fold(0u32, |mask, piece| match piece {
            Piece::Field { field, .. } => mask | field.mask(),
            Piece::Text(_) => mask,
        });
        // 亚秒日期格式不能按墙钟秒缓存.
        let fast_default =
            fmt == DEFAULT_FORMAT && !date_fmt.contains("%f") && !date_fmt.contains("%.");
        let message_only = fmt == "%(message)s";
        let mut hasher = DefaultHasher::new();
        date_fmt.hash(&mut hasher);
        let date_fmt_hash = hasher.finish();
        Ok(Self {
            pieces,
            date_fmt,
            fmt,
            used,
            fast_default,
            message_only,
            date_fmt_hash,
        })
    }

    /// 判断模板是否恰好为 `%(message)s`.
    ///
    /// 参数：`self`.
    /// 返回值：`true` 表示 layer 可跳过 `Values` 与 piece 循环，直接把消息写入复用缓冲并做转义.
    /// 该快路径不改变输出语义，只是省掉 header/字段计算.
    #[inline]
    pub(crate) fn is_message_only(&self) -> bool {
        self.message_only
    }

    /// 判断模板是否为默认模板且日期格式可按秒缓存.
    ///
    /// 参数：`self`.
    /// 返回值：`true` 时 layer 使用 header 秒级缓存；子秒日期格式（`%f`/`%.`）必须返回 `false`，否
    /// 则同一秒内不同毫秒会复用错误前缀.
    #[inline]
    pub(crate) fn is_default_template(&self) -> bool {
        self.fast_default
    }

    /// 返回原始模板，用于诊断/日志.
    ///
    /// 参数：`self`.
    /// 返回值：编译时保存的模板字符串引用；不会分配.
    pub fn template(&self) -> &str {
        &self.fmt
    }

    /// 渲染一条事件并返回独立 `String`.
    ///
    /// 参数：`input` 包含 service/target/level/message/metadata/spans；返回值：最终文本.
    /// 结构化字段不会自动追加到文本，它们通过 `Record.fields` 交给结构化 sink；如果模板显式引用字段
    /// ，`input` 不包含任意字段值，只有固定元数据字段.
    ///
    /// 说明：该公共入口会自行采样一次时间；热路径应使用内部 `render_*_into_at` 复用缓冲并共享 layer
    /// 已采样的时间.
    pub fn render(&self, input: RenderInput<'_>) -> String {
        let mut out = String::with_capacity(160 + input.message.len());
        if self.fast_default {
            self.render_default_into(input, None, SystemTime::now(), &mut out);
        } else if self.message_only {
            self.render_message_into(input.message, &mut out);
        } else {
            self.render_with_location_into(input, None, &mut out);
        }
        out
    }

    /// 判断模板是否引用 `%(taskName)s`.
    ///
    /// 参数：`self`.
    /// 返回值：`used` 掩码中是否包含 TaskName 位.
    /// 设计原因：默认格式不使用taskName，layer 可据此跳过 span 作用域遍历与每个 span 名称的 String
    /// 分配，这在async 服务中是可观测的开销.
    pub(crate) fn needs_task_name(&self) -> bool {
        self.used & Field::TaskName.mask() != 0
    }

    /// 渲染到调用方提供的缓冲，复用一次分配.
    ///
    /// 参数：
    /// - `input`：渲染输入；
    /// - `caller`：`#[track_caller]` 捕获的调用点 `(file, line)`，`None` 时回退 metadata；
    /// - `out`：复用缓冲，函数会先 `clear` 再写入.
    ///
    /// 返回值：无；结果在 `out` 中.
    /// 该入口自行采样时间，适合不需要共享时间戳的调用方.
    pub(crate) fn render_with_location_into(
        &self,
        input: RenderInput<'_>,
        caller: Option<(&str, u32)>,
        out: &mut String,
    ) {
        self.render_into(input, caller, None, out);
    }

    /// 用调用方采样的时间渲染到复用缓冲.
    ///
    /// 参数：
    /// - `input`：渲染输入；
    /// - `caller`：调用点，覆盖 metadata 的 file/line；
    /// - `timestamp`：事件时间，保证文本头与 `Record.metadata.timestamp` 一致；
    /// - `out`：复用输出缓冲.
    ///
    /// 返回值：无.
    /// 设计原因：layer 每条事件只读一次时钟，避免文本与结构化记录出现毫秒级偏差.
    pub(crate) fn render_with_location_into_at(
        &self,
        input: RenderInput<'_>,
        caller: Option<(&str, u32)>,
        timestamp: SystemTime,
        out: &mut String,
    ) {
        self.render_into(input, caller, Some(timestamp), out);
    }

    /// 通用 piece 循环渲染.
    ///
    /// 参数：
    /// - `input`：渲染输入；
    /// - `caller`：调用点；
    /// - `timestamp`：`Some` 时复用调用方时间，`None` 时由 `Values` 采样；
    /// - `out`：复用输出缓冲.
    ///
    /// 返回值：无.
    /// 实现按模板顺序遍历 `pieces`，只通过 `Values::get` 取实际引用字段；结束后统一做控制字符转义.
    fn render_into(
        &self,
        input: RenderInput<'_>,
        caller: Option<(&str, u32)>,
        timestamp: Option<SystemTime>,
        out: &mut String,
    ) {
        out.clear();
        // 复用缓冲已足够大时 `reserve` 是空操作；它避免后续更长记录反复扩容.
        out.reserve(160 + input.message.len());
        let values = Values::new(input, &self.date_fmt, self.used, caller, timestamp);
        for piece in &self.pieces {
            match piece {
                Piece::Text(text) => out.push_str(text),
                Piece::Field {
                    field,
                    flags,
                    width,
                    precision,
                    conv,
                } => {
                    let value = values.get(*field);
                    render_value_into(out, *conv, &value, *flags, *width, *precision);
                }
            }
        }
        sanitize_output(out);
    }

    /// 默认模板快路径：按秒/调用点/级别缓存 header.
    ///
    /// 参数：
    /// - `input`：渲染输入；
    /// - `caller`：调用点，优先于 metadata 的 file/line；
    /// - `timestamp`：事件时间；
    /// - `out`：复用输出缓冲.
    ///
    /// 返回值：无.
    /// 只有毫秒和消息逐条变化；header 命中时不再查 asctime/module_chain 缓存，也不取 PID/线程 ID，
    /// 从而把默认格式的每条成本压到接近纯字符串拼接.
    pub(crate) fn render_default_into(
        &self,
        input: RenderInput<'_>,
        caller: Option<(&str, u32)>,
        timestamp: SystemTime,
        out: &mut String,
    ) {
        out.clear();
        out.reserve(160 + input.message.len());
        let created = timestamp
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs_f64())
            .unwrap_or(0.0);
        let whole = created.floor();
        let second = whole as i64;
        let msecs = ((created - whole) * 1000.0).clamp(0.0, 999.0) as u32;
        let file = caller
            .map(|(file, _)| file)
            .or_else(|| input.metadata.file());
        let line = caller
            .map(|(_, line)| line)
            .or_else(|| input.metadata.line())
            .unwrap_or(0);
        let module_key =
            file.unwrap_or_else(|| input.metadata.module_path().unwrap_or(input.target));
        let level = input.level;
        let file = file.unwrap_or("");
        let file_ptr = if file.is_empty() {
            0
        } else {
            file.as_ptr() as usize
        };
        let module_ptr = if module_key.is_empty() {
            0
        } else {
            module_key.as_ptr() as usize
        };

        DEFAULT_HEADER.with(|cache| {
            let mut cache = cache.borrow_mut();
            if cache.second != second
                || cache.level != level
                || cache.line != line
                || cache.file_ptr != file_ptr
                || cache.file_len != file.len()
                || cache.module_ptr != module_ptr
                || cache.module_len != module_key.len()
                || cache.date_fmt_hash != self.date_fmt_hash
            {
                // 只有 header 变化时才支付 asctime/module-chain 缓存查找成本.
                let asctime = cached_asctime(created, &self.date_fmt);
                let module_chain = if file.is_empty() {
                    cached_module_chain(
                        input.metadata.module_path().unwrap_or(input.target),
                        input.target,
                    )
                } else {
                    cached_module_chain_from_file(file)
                };
                cache.second = second;
                cache.level = level;
                cache.line = line;
                cache.file_ptr = file_ptr;
                cache.file_len = file.len();
                cache.module_ptr = module_ptr;
                cache.module_len = module_key.len();
                cache.date_fmt_hash = self.date_fmt_hash;
                cache.prefix.clear();
                cache.prefix.push('[');
                cache.prefix.push_str(&asctime);
                cache.prefix.push('-');
                cache.suffix.clear();
                cache.suffix.push_str("] [");
                let mut number = itoa::Buffer::new();
                cache
                    .suffix
                    .push_str(number.format(u64::from(process_id())));
                cache.suffix.push(':');
                cache.suffix.push_str(number.format(thread_id()));
                cache.suffix.push_str("] [");
                cache.suffix.push_str(&module_chain);
                cache.suffix.push_str("::");
                cache.suffix.push_str(number.format(u64::from(line)));
                cache.suffix.push_str("::");
                cache.suffix.push_str(level.label());
                cache.suffix.push_str("] > ");
                // header 每秒/每调用点只构建一次；在这里转义一次，避免每条记录重扫整个 header.
                sanitize_output(&mut cache.prefix);
                sanitize_output(&mut cache.suffix);
            }
            out.push_str(&cache.prefix);
            if msecs < 10 {
                out.push_str("00");
            } else if msecs < 100 {
                out.push('0');
            }
            let mut number = itoa::Buffer::new();
            out.push_str(number.format(msecs));
            out.push_str(&cache.suffix);
        });
        let message = sanitize_text(input.message);
        out.push_str(message.as_ref());
    }

    /// `%(message)s` 快路径：直接复制消息并转义.
    ///
    /// 参数：
    /// - `message`：原始消息文本；
    /// - `out`：复用输出缓冲.
    ///
    /// 返回值：无.
    /// 不构造 `Values`、不取任何元数据，适合高吞吐纯消息日志.
    pub(crate) fn render_message_into(&self, message: &str, out: &mut String) {
        out.clear();
        out.reserve(message.len());
        let message = sanitize_text(message);
        out.push_str(message.as_ref());
    }
}

/// 就地对输出做日志注入防护.
///
/// 参数：`out` 是完整渲染结果（header + message）.
/// 规则：
/// - `\n`/`\r` 转义为字面量 `\n`/`\r`，防止一条日志伪造多行；
/// - `\t` 保留；
/// - 其他 C0/C1 控制字符转成 `\u{..}`；
/// - 先做字节预检（C0/DEL 是单字节，C1 以 0xC2 开头），无控制字符时避免逐字符解码.
fn sanitize_output(out: &mut String) {
    // header 注入防护：message/字段中的换行不能制造另一条物理日志行.其他 C0 控制字符
    // （NUL、backspace、ESC 等）同样转义；tab 为结构化文本保留.
    //
    // 先做字节预检：C0 控制字符和 DEL 是单字节 ASCII；C1（U+0080..=U+009F）恰好是
    // C2 80..9F 两字节序列.检查续字节可避免在常见路径上解码每个合法的 C2 前缀字符
    // （例如 U+00A2）.
    if !needs_sanitize(out) && out.len() <= MAX_SANITIZED_BYTES {
        return;
    }
    // 复用 batch 的可截断转义实现；避免先生成 6× 临时 String.
    let mut sanitized = Vec::with_capacity(out.len().min(MAX_SANITIZED_BYTES));
    append_sanitized(out, &mut sanitized);
    *out = String::from_utf8(sanitized).expect("sanitizer only emits valid UTF-8");
}

/// 判断输出是否包含需要转义的控制字符.
///
/// 参数：`out` 是完整输出.
/// 返回值：存在 C0/DEL/C1 控制字符时为 true；普通 tab 不算需要转义.
/// 用字节窗口避免对合法 UTF-8 字符做全量解码.
fn needs_sanitize(out: &str) -> bool {
    let bytes = out.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if (byte < 0x20 && byte != b'\t') || byte == 0x7f {
            return true;
        }
        if byte == 0xc2
            && bytes
                .get(index + 1)
                .is_some_and(|next| (0x80..=0x9f).contains(next))
        {
            return true;
        }
        index += 1;
    }
    false
}

/// 把控制字符追加为一个 ASCII 转义序列.
///
/// 参数：`ch` 是待转义控制字符；`out` 是字节缓冲.
/// 返回值：无.
/// 输出与 [`sanitize_output`] 完全一致（`\u{hex}` 小写），但直接写 `Vec<u8>`，避免为批量路径分配
/// 临时 String.
fn push_escaped_char(ch: char, out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.extend_from_slice(b"\\u{");
    let mut value = ch as u32;
    let mut digits = [0u8; 8];
    let mut count = 0usize;
    if value == 0 {
        digits[0] = b'0';
        count = 1;
    } else {
        while value != 0 {
            digits[count] = HEX[(value & 0xf) as usize];
            count += 1;
            value >>= 4;
        }
    }
    while count != 0 {
        count -= 1;
        out.push(digits[count]);
    }
    out.push(b'}');
}

/// 单条文本转义后的硬上限.
///
/// C0/C1 控制字符会扩张为 `\u{..}`；没有上限时 16 MiB 输入可放大数倍，形成日志量/内存放大攻击.
/// 超过上限的文本截断并追加可见标记，保证单条记录和 sink scratch 有界.
pub(crate) const MAX_SANITIZED_BYTES: usize = 32 * 1024 * 1024;
/// 截断标记；故意保持 ASCII，避免再次引入控制字符.
const TRUNCATED_MARKER: &[u8] = b"...[truncated]";

/// 追加截断标记；参数 `out` 是目标缓冲.
fn push_truncated_marker(out: &mut Vec<u8>) {
    out.extend_from_slice(TRUNCATED_MARKER);
}

/// 追加一个字符的转义字节.
///
/// 参数：`ch` 是字符；`out` 是目标缓冲.
/// 返回值：无.与 [`sanitize_output`] 的规则保持一致，tab 原样保留.
fn push_sanitized_char(ch: char, out: &mut Vec<u8>) {
    match ch {
        '\n' => out.extend_from_slice(b"\\n"),
        '\r' => out.extend_from_slice(b"\\r"),
        '\t' => out.push(b'\t'),
        other if other.is_control() => push_escaped_char(other, out),
        other => {
            let mut buffer = [0u8; 4];
            out.extend_from_slice(other.encode_utf8(&mut buffer).as_bytes());
        }
    }
}

/// 返回一个字符转义后的字节长度.
fn sanitized_char_len(ch: char) -> usize {
    match ch {
        '\n' | '\r' => 2,
        '\t' => 1,
        other if other.is_control() => {
            let mut value = other as u32;
            let mut digits = 1usize;
            while value >= 16 {
                value >>= 4;
                digits += 1;
            }
            4 + digits
        }
        other => other.len_utf8(),
    }
}

/// 把文本按与文本模板相同的规则转义后追加到字节缓冲.
///
/// 参数：`text` 是待写文本；`out` 是目标字节缓冲.
/// 返回值：无.
/// 批量 API 接收最终文本，不经 `TextFormat`；这里是最后一道物理行注入防线.
/// 无控制字符时只做一次字节预检；超过 [`MAX_SANITIZED_BYTES`] 时截断并写标记，避免转义放大.
pub(crate) fn append_sanitized(text: &str, out: &mut Vec<u8>) {
    let start = out.len();
    let limit = start.saturating_add(MAX_SANITIZED_BYTES);
    if !needs_sanitize(text) {
        if text.len() <= MAX_SANITIZED_BYTES {
            out.extend_from_slice(text.as_bytes());
            return;
        }
        for ch in text.chars() {
            let encoded = ch.len_utf8();
            if out.len().saturating_add(encoded) > limit {
                push_truncated_marker(out);
                return;
            }
            let mut buffer = [0u8; 4];
            out.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
        }
        return;
    }
    for ch in text.chars() {
        let encoded = sanitized_char_len(ch);
        if out.len().saturating_add(encoded) > limit {
            push_truncated_marker(out);
            return;
        }
        push_sanitized_char(ch, out);
    }
}

/// 返回转义后的字节长度.
///
/// 参数：`text` 是待写文本.
/// 返回值：与 [`append_sanitized`] 输出长度一致；超过上限时包含截断标记长度.
/// 轮转阈值必须按实际落盘字节判断；直接用原文长度会在大量控制字符时低估并跳过轮转.
pub(crate) fn sanitized_len(text: &str) -> usize {
    if !needs_sanitize(text) && text.len() <= MAX_SANITIZED_BYTES {
        return text.len();
    }
    let mut total = 0usize;
    for ch in text.chars() {
        let encoded = sanitized_char_len(ch);
        if total.saturating_add(encoded) > MAX_SANITIZED_BYTES {
            return total.saturating_add(TRUNCATED_MARKER.len());
        }
        total = total.saturating_add(encoded);
    }
    total
}

/// 返回按控制字符规则转义后的文本.
///
/// 参数：`text` 是原始文本.
/// 返回值：无控制字符时借用原文本；否则返回转义后的 `String`.
/// 仅用于已经构造 String 的终端输出；文件批量路径使用 [`append_sanitized`] 避免额外分配.
pub(crate) fn sanitize_text(text: &str) -> Cow<'_, str> {
    if !needs_sanitize(text) && text.len() <= MAX_SANITIZED_BYTES {
        return Cow::Borrowed(text);
    }
    let mut bytes = Vec::with_capacity(text.len());
    append_sanitized(text, &mut bytes);
    Cow::Owned(String::from_utf8(bytes).expect("sanitizer only emits valid UTF-8"))
}

impl Default for TextFormat {
    /// 默认模板；参数无，返回 `DEFAULT_FORMAT`/`DEFAULT_DATE_FORMAT`.
    fn default() -> Self {
        Self::new(DEFAULT_FORMAT, DEFAULT_DATE_FORMAT).expect("default format must compile")
    }
}

/// 文本渲染输入.
///
/// 参数/字段说明：
/// - `service`：service/logger 名，对应 `%(name)s`；
/// - `target`：路由 target；没有 caller file 时作为 `module_chain` 回退；
/// - `level`：Flare 级别，决定 `levelname/levelno`；
/// - `message`：已格式化的消息正文；
/// - `metadata`：tracing 元数据，提供 module_path/file/line 回退；
/// - `spans`：span 名列表，只有模板引用 `%(taskName)s` 时才需要.
#[non_exhaustive]
pub struct RenderInput<'a> {
    /// service/logger 名.
    pub service: &'a str,
    /// tracing target/路由目标.
    pub target: &'a str,
    /// Flare 级别.
    pub level: FlareLevel,
    /// 消息正文.
    pub message: &'a str,
    /// tracing 元数据.
    pub metadata: &'a tracing::Metadata<'a>,
    /// span 名列表（按从根到当前）.
    pub spans: &'a [String],
}

/// JSON Lines 渲染输入.
///
/// 参数/字段说明：
/// - `service/target/level/message`：同文本输入；
/// - `metadata`：提供 file/line/module_path 回退；
/// - `fields`：结构化字段，排除内部 `flare_logger`/`flare_level` 后写入 `fields` 对象；
/// - `timestamp`：RFC3339 时间；
/// - `caller`：`#[track_caller]` 调用点，优先于 metadata；
/// - `backtrace`：可选堆栈文本，写入 `backtrace` 字段.
#[cfg(feature = "json")]
pub(crate) struct JsonInput<'a> {
    /// service/logger 名.
    pub service: &'a str,
    /// tracing target/路由目标.
    pub target: &'a str,
    /// Flare 级别.
    pub level: FlareLevel,
    /// 消息正文.
    pub message: &'a str,
    /// tracing 元数据.
    pub metadata: &'a tracing::Metadata<'a>,
    /// 结构化字段.
    pub fields: &'a [(Arc<str>, crate::engine::record::FieldValue)],
    /// 事件时间.
    pub timestamp: SystemTime,
    /// 调用点（文件、行号）.
    pub caller: Option<(&'a str, u32)>,
    /// 可选堆栈.
    pub backtrace: Option<&'a str>,
}

/// 渲染一条 JSON Lines 记录（不含行尾换行）.
///
/// 参数：
/// - `input`：JSON 输入（见 [`JsonInput`]）；
/// - `out`：复用输出缓冲，函数会先 `clear`.
///
/// 返回值：无.
/// 输出 schema v1：
/// `schema/time/level/service/target/message/fields/pid/thread/file/line/module_chain`，错误
/// /CRITICAL 时可选 `backtrace`.
/// 内部路由字段不暴露；序列化失败时写入一个最小错误对象而不是 panic.
#[cfg(feature = "json")]
pub(crate) fn render_json_into(input: JsonInput<'_>, out: &mut String) {
    use crate::engine::record::FieldValue;
    use serde_json::{Map, Number, Value};

    /// 把内部字段值转成 serde_json 值.
    ///
    /// 参数：`value` 是结构化字段值.
    /// 返回值：JSON 值；非有限浮点无法表示时降级为 `Null`，保证整条日志仍可序列化.
    fn value_of(value: &FieldValue) -> Value {
        match value {
            FieldValue::Str(text) => Value::String(text.as_ref().to_string()),
            FieldValue::I64(number) => Value::Number(Number::from(*number)),
            FieldValue::U64(number) => Value::Number(Number::from(*number)),
            FieldValue::F64(number) => Number::from_f64(*number)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            FieldValue::Bool(value) => Value::Bool(*value),
        }
    }

    out.clear();
    let mut object = Map::new();
    object.insert("schema".to_string(), Value::from(1));
    let time = chrono::DateTime::<chrono::Utc>::from(input.timestamp)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    object.insert("time".to_string(), Value::String(time));
    object.insert(
        "level".to_string(),
        Value::String(input.level.label().to_string()),
    );
    object.insert(
        "service".to_string(),
        Value::String(input.service.to_string()),
    );
    object.insert(
        "target".to_string(),
        Value::String(input.target.to_string()),
    );
    object.insert(
        "message".to_string(),
        Value::String(input.message.to_string()),
    );

    let mut fields = Map::new();
    for (key, value) in input.fields {
        if key.as_ref() == crate::logger::LOGGER_FIELD
            || key.as_ref() == crate::logger::CRITICAL_FIELD
        {
            continue;
        }
        fields.insert(key.as_ref().to_string(), value_of(value));
    }
    if !fields.is_empty() {
        object.insert("fields".to_string(), Value::Object(fields));
    }
    object.insert(
        "pid".to_string(),
        Value::Number(Number::from(u64::from(process_id()))),
    );
    object.insert(
        "thread".to_string(),
        Value::Number(Number::from(thread_id())),
    );

    let file = input
        .caller
        .map(|(file, _)| file)
        .or_else(|| input.metadata.file());
    if let Some(file) = file {
        if !file.is_empty() {
            object.insert("file".to_string(), Value::String(file.to_string()));
            let chain = module_chain_from_file(file);
            if !chain.is_empty() {
                object.insert("module_chain".to_string(), Value::String(chain));
            }
        }
    }
    let line = input
        .caller
        .map(|(_, line)| line)
        .or_else(|| input.metadata.line());
    if let Some(line) = line {
        object.insert("line".to_string(), Value::Number(Number::from(line)));
    }
    if let Some(backtrace) = input.backtrace {
        object.insert(
            "backtrace".to_string(),
            Value::String(backtrace.to_string()),
        );
    }

    match serde_json::to_string(&Value::Object(object)) {
        Ok(rendered) => out.push_str(&rendered),
        Err(_) => out.push_str("{\"schema\":1,\"error\":\"json render failed\"}"),
    }
}

/// 把模板编译成片段列表.
///
/// 参数：`fmt` 是完整模板字符串.
/// 返回值：成功为 `Piece` 列表；失败为[`FlareError::Config`]，覆盖缺右括号、未知字段、非法宽度/精度
/// 、未知转换符、孤立 `%`.
/// 设计原因：所有语法检查在构造期完成，运行期只做线性渲染，不需要错误分支.
fn compile(fmt: &str) -> Result<Vec<Piece>, FlareError> {
    let mut pieces = Vec::new();
    let mut literal = String::new();
    let mut chars = fmt.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            literal.push(ch);
            continue;
        }
        match chars.next() {
            Some('%') => literal.push('%'),
            Some('(') => {
                let mut name = String::new();
                loop {
                    match chars.next() {
                        Some(')') => break,
                        Some(ch) => name.push(ch),
                        None => {
                            return Err(FlareError::Config(format!(
                                "log format is missing ')' after %({name}: {fmt:?}"
                            )))
                        }
                    }
                }
                let field = Field::from_name(&name).ok_or_else(|| {
                    FlareError::Config(format!(
                        "log format references unknown field %({name})s; available: asctime, msecs, process, thread, threadName, module_chain, lineno, levelname, message, name, filename, module, pathname, funcName, levelno, created, relativeCreated, processName, taskName"
                    ))
                })?;
                let mut flags = Flags::default();
                while let Some(&ch) = chars.peek() {
                    match ch {
                        '-' => flags.left = true,
                        '0' => flags.zero = true,
                        '+' => flags.plus = true,
                        ' ' => flags.space = true,
                        '#' => flags.alt = true,
                        _ => break,
                    }
                    chars.next();
                }
                let mut width_digits = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_ascii_digit() {
                        width_digits.push(ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let width = if width_digits.is_empty() {
                    None
                } else {
                    let width = width_digits.parse::<usize>().map_err(|_| {
                        FlareError::Config(format!("invalid log format width: {width_digits:?}"))
                    })?;
                    if width > MAX_FORMAT_WIDTH {
                        return Err(FlareError::Config(format!(
                            "log format width {width} exceeds limit {MAX_FORMAT_WIDTH}: {fmt:?}"
                        )));
                    }
                    Some(width)
                };
                let mut precision = None;
                if chars.peek() == Some(&'.') {
                    chars.next();
                    let mut digits = String::new();
                    while let Some(&ch) = chars.peek() {
                        if ch.is_ascii_digit() {
                            digits.push(ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if digits.is_empty() {
                        return Err(FlareError::Config(format!(
                            "log format precision is missing digits: {fmt:?}"
                        )));
                    }
                    let value = digits.parse::<usize>().map_err(|_| {
                        FlareError::Config(format!("invalid log format precision: {digits:?}"))
                    })?;
                    if value > MAX_FORMAT_PRECISION {
                        return Err(FlareError::Config(format!(
                            "log format precision {value} exceeds limit {MAX_FORMAT_PRECISION}: {fmt:?}"
                        )));
                    }
                    precision = Some(value);
                }
                let Some(conv) = chars.next() else {
                    return Err(FlareError::Config(format!(
                        "log format is missing a conversion after %({name}): {fmt:?}"
                    )));
                };
                if !matches!(
                    conv,
                    's' | 'd'
                        | 'i'
                        | 'u'
                        | 'f'
                        | 'F'
                        | 'e'
                        | 'E'
                        | 'g'
                        | 'G'
                        | 'x'
                        | 'X'
                        | 'o'
                        | 'c'
                ) {
                    return Err(FlareError::Config(format!(
                        "unsupported log format conversion %({name}){conv}; supported: s/d/i/u/f/F/e/E/g/G/x/X/o/c"
                    )));
                }
                if !literal.is_empty() {
                    pieces.push(Piece::Text(std::mem::take(&mut literal)));
                }
                pieces.push(Piece::Field {
                    field,
                    flags,
                    width,
                    precision,
                    conv,
                });
            }
            Some(other) => {
                return Err(FlareError::Config(format!(
                    "log format: % must be followed by a field name or %: %{other}; full format: {fmt:?}"
                )))
            }
            None => {
                return Err(FlareError::Config(format!(
                    "log format ends with a lone %: {fmt:?}"
                )))
            }
        }
    }
    if !literal.is_empty() {
        pieces.push(Piece::Text(literal));
    }
    Ok(pieces)
}

/// 一条事件实际需要渲染的字段值.
///
/// 设计原理：`used` 位掩码决定哪些字段需要计算；未引用字段保持空字符串/0，不分配.
/// 字段说明：
/// - `asctime`：缓存的日期文本；
/// - `msecs/process/thread/lineno/level_no/created/relative_created`：数值字段；
/// - `thread_name`/`filename`/`module`/`pathname`/`task_name`：按需分配字符串；
/// - `module_chain`：缓存的模块链 `Arc`；
/// - `level_name`/`message`/`name`/`func_name`/`process_name`：借用或静态文本.
struct Values<'a> {
    asctime: Option<Arc<str>>,
    msecs: i64,
    process: i64,
    thread: u64,
    thread_name: String,
    module_chain: Option<Arc<str>>,
    lineno: i64,
    level_name: &'static str,
    message: &'a str,
    name: &'a str,
    filename: String,
    module: String,
    pathname: String,
    func_name: &'static str,
    level_no: i32,
    created: f64,
    relative_created: f64,
    process_name: &'static str,
    task_name: String,
}

impl<'a> Values<'a> {
    /// 按 `used` 掩码按需计算字段.
    ///
    /// 参数：
    /// - `input`：渲染输入（消息/服务名/目标/级别/元数据/span）；
    /// - `date_fmt`：asctime 日期格式；
    /// - `used`：模板引用字段位掩码；
    /// - `caller`：`#[track_caller]` 调用点，优先于 metadata 的 file/line；
    /// - `timestamp`：`Some` 时复用调用方时间，`None` 时按需读取当前时间.
    ///
    /// 返回值：拥有所有字段值的 `Values`.
    /// 未引用字段不计算，避免为 `%(message)s` 这类模板付 PID/时间/文件路径成本.
    fn new(
        input: RenderInput<'a>,
        date_fmt: &str,
        used: u32,
        caller: Option<(&str, u32)>,
        timestamp: Option<SystemTime>,
    ) -> Self {
        let need = |field: Field| used & field.mask() != 0;
        // 模板不引用时间/PID/线程 ID 时，采样这些值是浪费（例如热循环中的 `%(message)s`）.
        // 默认格式需要它们，但自定义高吞吐模板通常不需要.
        let need_time = need(Field::Asctime)
            || need(Field::Msecs)
            || need(Field::Created)
            || need(Field::RelativeCreated);
        let created = if need_time {
            timestamp
                .unwrap_or_else(SystemTime::now)
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs_f64())
                .unwrap_or(0.0)
        } else {
            0.0
        };
        let start = if need(Field::RelativeCreated) {
            Some(START.get_or_init(Instant::now))
        } else {
            None
        };
        let caller_file = caller.map(|(file, _)| file);
        let caller_line = caller.map(|(_, line)| line);

        // 只计算编译后模板引用的字段.默认 tracing 格式使用 asctime/module_chain/thread，
        // 不使用 threadName/filename/taskName；提前创建这些字符串会变成每事件四次分配.
        let asctime = if need(Field::Asctime) {
            Some(cached_asctime(created, date_fmt))
        } else {
            None
        };
        let thread_name = if need(Field::ThreadName) {
            thread_name()
        } else {
            String::new()
        };
        let module_chain = if need(Field::ModuleChain) {
            if let Some(file) = caller_file {
                Some(cached_module_chain_from_file(file))
            } else {
                let module_path = input.metadata.module_path().unwrap_or(input.target);
                Some(cached_module_chain(module_path, input.target))
            }
        } else {
            None
        };
        let file_used = need(Field::Filename) || need(Field::Module) || need(Field::Pathname);
        let (filename, module, pathname) = if file_used {
            let file = caller_file.or_else(|| input.metadata.file()).unwrap_or("-");
            let filename = Path::new(file)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(file)
                .to_string();
            let module = filename
                .rsplit_once('.')
                .map(|(stem, _)| stem.to_string())
                .unwrap_or_else(|| filename.clone());
            (filename, module, file.to_string())
        } else {
            (String::new(), String::new(), String::new())
        };
        let task_name = if need(Field::TaskName) {
            input
                .spans
                .first()
                .cloned()
                .unwrap_or_else(|| "-".to_string())
        } else {
            String::new()
        };
        let lineno = if need(Field::Lineno) {
            caller_line
                .map(i64::from)
                .unwrap_or_else(|| i64::from(input.metadata.line().unwrap_or(0)))
        } else {
            0
        };

        Self {
            asctime,
            msecs: if need(Field::Msecs) {
                (created.fract() * 1000.0) as i64
            } else {
                0
            },
            process: if need(Field::Process) {
                i64::from(process_id())
            } else {
                0
            },
            thread: if need(Field::Thread) { thread_id() } else { 0 },
            thread_name,
            module_chain,
            lineno,
            level_name: input.level.label(),
            message: input.message,
            name: input.service,
            filename,
            module,
            pathname,
            func_name: "-",
            level_no: if need(Field::LevelNo) {
                input.level.levelno()
            } else {
                0
            },
            created,
            relative_created: start
                .map(|start| start.elapsed().as_secs_f64() * 1000.0)
                .unwrap_or(0.0),
            process_name: if need(Field::ProcessName) {
                process_name()
            } else {
                ""
            },
            task_name,
        }
    }

    /// 按字段种类取出用于渲染的值.
    ///
    /// 参数：`field` 是模板编译阶段的字段枚举.
    /// 返回值：`Value`，字符串尽量借用`Values` 内部缓存的 `Arc<str>`/`&str`，数值直接复制；不会分配
    /// （除非后续格式化需要 pad/精度）.
    fn get(&self, field: Field) -> Value<'_> {
        match field {
            Field::Asctime => {
                Value::Str(Cow::Borrowed(self.asctime.as_deref().unwrap_or_default()))
            }
            Field::Msecs => Value::Int(self.msecs),
            Field::Process => Value::Int(self.process),
            Field::Thread => Value::UInt(self.thread),
            Field::ThreadName => Value::Str(Cow::Borrowed(&self.thread_name)),
            Field::ModuleChain => Value::Str(Cow::Borrowed(
                self.module_chain.as_deref().unwrap_or_default(),
            )),
            Field::Lineno => Value::Int(self.lineno),
            Field::LevelName => Value::Str(Cow::Borrowed(self.level_name)),
            Field::Message => Value::Str(Cow::Borrowed(self.message)),
            Field::Name => Value::Str(Cow::Borrowed(self.name)),
            Field::Filename => Value::Str(Cow::Borrowed(&self.filename)),
            Field::Module => Value::Str(Cow::Borrowed(&self.module)),
            Field::Pathname => Value::Str(Cow::Borrowed(&self.pathname)),
            Field::FuncName => Value::Str(Cow::Borrowed(self.func_name)),
            Field::LevelNo => Value::Int(i64::from(self.level_no)),
            Field::Created => Value::Float(self.created),
            Field::RelativeCreated => Value::Float(self.relative_created),
            Field::ProcessName => Value::Str(Cow::Borrowed(self.process_name)),
            Field::TaskName => Value::Str(Cow::Borrowed(&self.task_name)),
        }
    }
}

/// 从模块路径生成最后 3 段的 `::` 链.
///
/// 参数：
/// - `module_path`：Rust/tracing 模块路径（`a::b::c`）或带点的路径；
/// - `fallback`：`module_path` 为空时使用的 target.
///
/// 返回值：最多 3 段、用 `::` 连接；不足 3 段取全部.
/// 设计原因：保留定位信息，又不让深层包名把日志行撑得过长；点路径先统一成点再拆分，兼容 Python 风格
/// 元数据.
fn module_chain(module_path: &str, fallback: &str) -> String {
    let source = if module_path.is_empty() {
        fallback
    } else {
        module_path
    };
    // Rust 模块路径使用 `::`，Python 风格模块元数据可能使用 `.`.
    let source = source.replace("::", ".");
    let parts: Vec<&str> = source.split('.').filter(|part| !part.is_empty()).collect();
    let start = parts.len().saturating_sub(3);
    parts[start..].join("::")
}

/// 从调用方源文件路径推导模块链.
///
/// 参数：`file` 是 `#[track_caller]` 或 tracing metadata 给出的源文件路径.
/// 返回值：类似 `app::service::main` 的链.
/// 设计原因：`#[track_caller]` 只有文件/行号，没有`module_path!`，因此按 `.../src/a/b.rs` 约定推导
/// ；`mod.rs` 去掉尾部 `mod`；Windows 反斜杠先统一成正斜杠.
#[inline]
fn module_chain_from_file(file: &str) -> String {
    let normalized = file.replace('\\', "/");
    let components: Vec<&str> = normalized
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
    let start = components
        .iter()
        .rposition(|part| *part == "src")
        .map_or(0, |index| index + 1);
    let mut parts = components[start..].to_vec();
    if let Some(last) = parts.last_mut() {
        let stem = last.strip_suffix(".rs").unwrap_or(last);
        *last = stem;
    }
    if parts.last() == Some(&"mod") {
        parts.pop();
    }
    if parts.is_empty() {
        return file.to_string();
    }
    let start = parts.len().saturating_sub(3);
    parts[start..].join("::")
}

/// 返回当前线程名的展示文本.
///
/// 参数：无.
/// 返回值：线程名；主线程渲染为 `MainThread`（与 Python 一致），未命名线程为`unnamed`.
/// 该函数会分配 String，只有模板引用 `%(threadName)s` 时才调用.
fn thread_name() -> String {
    let name = std::thread::current()
        .name()
        .unwrap_or("unnamed")
        .to_string();
    if name == "main" {
        "MainThread".to_string()
    } else {
        name
    }
}

/// 返回当前线程的操作系统线程 ID.
///
/// 参数：无.
/// 返回值：平台线程 ID.
/// 设计原因：用 `thread_local!` 缓存，避免每条记录重复syscall/哈希；Linux 用 `gettid`，Windows 用
/// `GetCurrentThreadId`，其他平台退化为`ThreadId` 哈希.
#[inline]
fn thread_id() -> u64 {
    thread_local! {
        static ID: u64 = os_thread_id();
    }
    ID.with(|id| *id)
}

/// 读取平台原生线程 ID.
///
/// 参数：无.
/// 返回值：OS 线程 ID.
/// 实现按平台分支：
/// - Windows：`GetCurrentThreadId`，无前置条件；
/// - Linux/Android：只读 `SYS_gettid` syscall；
/// - 其他：对 `std::thread::ThreadId` 哈希，保证进程内稳定但非 OS ID.
fn os_thread_id() -> u64 {
    #[cfg(windows)]
    {
        // SAFETY: GetCurrentThreadId 无前置条件，返回调用线程 id.
        u64::from(unsafe { windows_sys::Win32::System::Threading::GetCurrentThreadId() })
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // SAFETY: SYS_gettid 是只读 syscall，返回内核线程 id.
        unsafe { libc::syscall(libc::SYS_gettid) as u64 }
    }
    #[cfg(not(any(windows, target_os = "linux", target_os = "android")))]
    {
        let mut hasher = DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        hasher.finish()
    }
}

/// 线程内缓存 `(module_path, target)` 对应的模块链.
///
/// 参数：
/// - `module_path`：tracing 模块路径；
/// - `target`：模块路径为空时的回退 target.
///
/// 返回值：共享的 `Arc<str>`.
/// 缓存最多 16 项，满了整体清空；典型服务只有少量调用点，命中后每条记录只增加一次引用计数，
/// 不重新 split/join.
fn cached_module_chain(module_path: &str, target: &str) -> Arc<str> {
    // 一个 service 通常从多个模块写日志；单条缓存会在交替调用点之间每次都 miss.
    // 每线程保留一个有界小集合，让热调用点只支付一次 `Arc` clone.
    const MAX_CACHED_CHAINS: usize = 16;
    thread_local! {
        static CACHE: RefCell<Vec<(String, String, Arc<str>)>> = const { RefCell::new(Vec::new()) };
    }
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some((_, _, value)) = cache
            .iter()
            .find(|(module, cached_target, _)| module == module_path && cached_target == target)
        {
            return Arc::clone(value);
        }
        let value: Arc<str> = Arc::from(module_chain(module_path, target));
        if cache.len() >= MAX_CACHED_CHAINS {
            cache.clear();
        }
        cache.push((
            module_path.to_string(),
            target.to_string(),
            Arc::clone(&value),
        ));
        value
    })
}

/// 线程内缓存调用方文件对应的模块链.
///
/// 参数：`file` 是调用方源文件路径.
/// 返回值：共享 `Arc<str>`.
/// 默认格式每条记录都要渲染module_chain；按文件缓存后，同一调用点只在上次未命中时做一次路径拆分/拼
/// 接.
#[inline]
fn cached_module_chain_from_file(file: &str) -> Arc<str> {
    const MAX_CACHED_CHAINS: usize = 16;
    thread_local! {
        static CACHE: RefCell<Vec<(String, Arc<str>)>> = const { RefCell::new(Vec::new()) };
    }
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some((_, value)) = cache.iter().find(|(cached_file, _)| cached_file == file) {
            return Arc::clone(value);
        }
        let value: Arc<str> = Arc::from(module_chain_from_file(file));
        if cache.len() >= MAX_CACHED_CHAINS {
            cache.clear();
        }
        cache.push((file.to_string(), Arc::clone(&value)));
        value
    })
}

/// 按秒缓存本地时间文本.
///
/// 参数：
/// - `created`：事件 UNIX 秒（浮点）；
/// - `date_fmt`：日期格式.
///
/// 返回值：共享 `Arc<str>`.
/// 设计原因：`Local::now().format()` 需要时区查询与格式化，每条记录做一次成本明显；默认格式只有秒级
/// 精度，因此以 `(格式, 秒)` 为键缓存.
/// 含子秒格式（`%f`/`%.`）时绕过缓存，避免同一秒内不同毫秒复用错误文本.
fn cached_asctime(created: f64, date_fmt: &str) -> Arc<str> {
    let cacheable = !date_fmt.contains("%f") && !date_fmt.contains("%.");
    if !cacheable {
        return Arc::from(Local::now().format(date_fmt).to_string());
    }
    let seconds = created.floor() as i64;
    thread_local! {
        static CACHE: RefCell<Option<(String, i64, Arc<str>)>> = const { RefCell::new(None) };
    }
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some((fmt, cached_seconds, value)) = cache.as_ref() {
            if *cached_seconds == seconds && fmt == date_fmt {
                return Arc::clone(value);
            }
        }
        let value: Arc<str> = Arc::from(Local::now().format(date_fmt).to_string());
        *cache = Some((date_fmt.to_string(), seconds, Arc::clone(&value)));
        value
    })
}

/// 返回缓存的进程 ID.
///
/// 参数：无.
/// 返回值：当前进程 ID.`OnceLock` 保证只调用一次 `std::process::id()`；进程内 PID 不会变化，缓存没
/// 有失效问题.
fn process_id() -> u32 {
    static PROCESS_ID: OnceLock<u32> = OnceLock::new();
    *PROCESS_ID.get_or_init(std::process::id)
}

/// 返回缓存的进程名.
///
/// 参数：无.
/// 返回值：当前可执行文件名（不含扩展名）；无法获取时为 `unknown`.
/// 设计原因：`current_exe()` 可能涉及系统调用/路径处理，只应在模板引用`%(processName)s` 时初始化一
/// 次.
fn process_name() -> &'static str {
    static PROCESS_NAME: OnceLock<String> = OnceLock::new();
    PROCESS_NAME
        .get_or_init(|| {
            std::env::current_exe()
                .ok()
                .and_then(|path| {
                    path.file_stem()
                        .and_then(|name| name.to_str())
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_else(|| "unknown".to_string())
        })
        .as_str()
}

/// 渲染期的临时值.
///
/// 设计原理：`Values::get` 返回借用/复制值，避免统一转 `String`；`Cow` 让字符串字段命中缓存时零分配
/// ，数值字段只在需要文本时（`%s`/pad）才分配.
enum Value<'a> {
    /// 借用或拥有的文本.
    Str(Cow<'a, str>),
    /// 有符号整数.
    Int(i64),
    /// 无符号整数.
    UInt(u64),
    /// 浮点数.
    Float(f64),
}

impl Value<'_> {
    /// 转成文本形式.
    ///
    /// 参数：`self`.
    /// 返回值：字符串借用或拥有；数值会分配一个 `String`，因此整数`d/i/u` 转换在`render_value_into`
    /// 中走 itoa 快路径，不调用本方法.
    fn as_text(&self) -> Cow<'_, str> {
        match self {
            Self::Str(value) => value.clone(),
            Self::Int(value) => Cow::Owned(value.to_string()),
            Self::UInt(value) => Cow::Owned(value.to_string()),
            Self::Float(value) => Cow::Owned(value.to_string()),
        }
    }

    /// 尝试按有符号整数解释.
    ///
    /// 参数：`self`.
    /// 返回值：整数视图；字符串返回 `None`，超出 `i64` 的无符号值返回`None`，浮点直接截断（沿用
    /// printf 风格）.
    fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            Self::UInt(value) => i64::try_from(*value).ok(),
            Self::Float(value) => Some(*value as i64),
            Self::Str(_) => None,
        }
    }

    /// 尝试按浮点解释.
    ///
    /// 参数：`self`.
    /// 返回值：浮点视图；字符串返回 `None`；整数可能损失精度（与 printf的整数→浮点转换一致）.
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(value) => Some(*value as f64),
            Self::UInt(value) => Some(*value as f64),
            Self::Float(value) => Some(*value),
            Self::Str(_) => None,
        }
    }
}

/// 按宽度/flags 对文本做 padding.
///
/// 参数：
/// - `text`：原始文本；
/// - `width`：最小字符宽度，`None` 直接返回；
/// - `flags`：左对齐/零填充等；
/// - `sign_len`：零填充时保留在前的符号长度（用于负数/正号）.
///
/// 返回值：原 `Cow` 或新分配的填充文本.
/// 宽度按字符数计算，避免中文/emoji 被截断.
fn pad(text: Cow<'_, str>, width: Option<usize>, flags: Flags, sign_len: usize) -> Cow<'_, str> {
    let Some(width) = width else {
        return text;
    };
    let len = text.chars().count();
    if len >= width {
        return text;
    }
    let fill = width - len;
    if flags.left {
        return Cow::Owned(format!("{text}{}", " ".repeat(fill)));
    }
    if flags.zero {
        let split = sign_len.min(text.len());
        let (prefix, rest) = text.split_at(split);
        return Cow::Owned(format!("{prefix}{}{rest}", "0".repeat(fill)));
    }
    Cow::Owned(format!("{}{text}", " ".repeat(fill)))
}

/// 把符号 + 数字写入输出并按宽度/flags 填充.
///
/// 参数：
/// - `out`：目标缓冲；
/// - `sign`：空串/`-`/`+`/空格；
/// - `digits`：itoa 生成的数字文本；
/// - `width`：最小总宽度；
/// - `flags`：左对齐/零填充.
///
/// 返回值：无.
/// 该函数是整数渲染快路径，不分配临时 String，只做 push/extend.
fn push_padded_number(
    out: &mut String,
    sign: &str,
    digits: &str,
    width: Option<usize>,
    flags: Flags,
) {
    let len = sign.len().saturating_add(digits.len());
    let fill = width
        .filter(|width| *width > len)
        .map_or(0, |width| width - len);
    if fill == 0 {
        out.push_str(sign);
        out.push_str(digits);
        return;
    }
    if flags.left {
        out.push_str(sign);
        out.push_str(digits);
        out.extend(std::iter::repeat_n(' ', fill));
    } else if flags.zero {
        out.push_str(sign);
        out.extend(std::iter::repeat_n('0', fill));
        out.push_str(digits);
    } else {
        out.extend(std::iter::repeat_n(' ', fill));
        out.push_str(sign);
        out.push_str(digits);
    }
}

/// 把一个值按转换符写入输出.
///
/// 参数：
/// - `out`：目标缓冲；
/// - `conv`：转换符；
/// - `value`：渲染值；
/// - `flags`：printf 风格 flags；
/// - `width`：最小宽度；
/// - `precision`：字符串精度/浮点精度.
///
/// 返回值：无.
/// 整数 `d/i/u` 走 itoa + `push_padded_number`，避免 `to_string`；其他转换交给 `render_value` 生成
/// `Cow` 再追加.
fn render_value_into(
    out: &mut String,
    conv: char,
    value: &Value<'_>,
    flags: Flags,
    width: Option<usize>,
    precision: Option<usize>,
) {
    if matches!(conv, 'd' | 'i' | 'u') {
        let (negative, magnitude) = match value {
            Value::UInt(number) => (false, *number),
            Value::Int(number) => (number.is_negative(), number.unsigned_abs()),
            Value::Float(number) => {
                let number = *number as i64;
                (number.is_negative(), number.unsigned_abs())
            }
            Value::Str(_) => return,
        };
        let sign = if negative {
            "-"
        } else if flags.plus {
            "+"
        } else if flags.space {
            " "
        } else {
            ""
        };
        let mut buffer = itoa::Buffer::new();
        push_padded_number(out, sign, buffer.format(magnitude), width, flags);
        return;
    }
    let rendered = render_value(conv, value, flags, width, precision);
    out.push_str(&rendered);
}

/// 按转换符生成文本.
///
/// 参数：
/// - `conv`：转换符 `s/d/i/u/f/F/e/E/g/G/x/X/o/c`；
/// - `value`：渲染值；
/// - `flags`：printf 风格 flags；
/// - `width`：最小宽度；
/// - `precision`：字符串最大字符数/浮点精度.
///
/// 返回值：借用或拥有的文本.
/// 该函数处理需要分配/特殊格式的转换；整数快路径在`render_value_into` 中提前返回.
fn render_value<'a>(
    conv: char,
    value: &'a Value<'a>,
    flags: Flags,
    width: Option<usize>,
    precision: Option<usize>,
) -> Cow<'a, str> {
    match conv {
        's' => {
            let mut text = value.as_text();
            if let Some(precision) = precision {
                text = Cow::Owned(text.chars().take(precision).collect());
            }
            pad(text, width, flags, 0)
        }
        'd' | 'i' | 'u' => {
            let text = match value {
                Value::UInt(number) => {
                    if flags.plus {
                        format!("+{number}")
                    } else if flags.space {
                        format!(" {number}")
                    } else {
                        number.to_string()
                    }
                }
                _ => {
                    let Some(number) = value.as_i64() else {
                        return Cow::Borrowed("");
                    };
                    if flags.plus && number >= 0 {
                        format!("+{number}")
                    } else if flags.space && number >= 0 {
                        format!(" {number}")
                    } else {
                        number.to_string()
                    }
                }
            };
            let sign = usize::from(matches!(text.as_bytes().first(), Some(b'+' | b'-' | b' ')));
            pad(Cow::Owned(text), width, flags, sign)
        }
        'x' | 'X' | 'o' => {
            let Some(number) = value.as_i64() else {
                return Cow::Borrowed("");
            };
            let negative = number < 0;
            let magnitude = number.unsigned_abs();
            let mut digits = match conv {
                'x' => format!("{magnitude:x}"),
                'X' => format!("{magnitude:X}"),
                _ => format!("{magnitude:o}"),
            };
            if let Some(precision) = precision {
                if digits.len() < precision {
                    digits.insert_str(0, &"0".repeat(precision - digits.len()));
                }
            }
            let prefix = if flags.alt {
                match conv {
                    'o' => "0o",
                    'x' => "0x",
                    _ => "0X",
                }
            } else {
                ""
            };
            let sign = if negative {
                "-"
            } else if flags.plus {
                "+"
            } else if flags.space {
                " "
            } else {
                ""
            };
            let text = format!("{sign}{prefix}{digits}");
            pad(Cow::Owned(text), width, flags, sign.len() + prefix.len())
        }
        'f' | 'F' => {
            let Some(number) = value.as_f64() else {
                return Cow::Borrowed("");
            };
            let precision = precision.unwrap_or(6);
            let mut text = if number.is_finite() {
                format!("{:.*}", precision, number)
            } else if number.is_nan() {
                if conv == 'F' { "NAN" } else { "nan" }.to_string()
            } else if number.is_sign_negative() {
                if conv == 'F' { "-INF" } else { "-inf" }.to_string()
            } else if conv == 'F' {
                "INF".to_string()
            } else {
                "inf".to_string()
            };
            if flags.plus && !text.starts_with('-') {
                text.insert(0, '+');
            } else if flags.space && !text.starts_with('-') {
                text.insert(0, ' ');
            }
            let sign = usize::from(matches!(text.as_bytes().first(), Some(b'+' | b'-' | b' ')));
            pad(Cow::Owned(text), width, flags, sign)
        }
        'e' | 'E' | 'g' | 'G' => {
            let Some(number) = value.as_f64() else {
                return Cow::Borrowed("");
            };
            let precision = precision.unwrap_or(6);
            let mut text = match conv {
                'e' | 'E' => format!("{:.*e}", precision, number),
                _ => format!("{:.*}", precision, number),
            };
            if conv == 'E' || conv == 'G' {
                text = text.to_uppercase();
            }
            if flags.plus && !text.starts_with('-') {
                text.insert(0, '+');
            } else if flags.space && !text.starts_with('-') {
                text.insert(0, ' ');
            }
            let sign = usize::from(matches!(text.as_bytes().first(), Some(b'+' | b'-' | b' ')));
            pad(Cow::Owned(text), width, flags, sign)
        }
        'c' => {
            let text = match value {
                Value::Str(text) => text
                    .chars()
                    .next()
                    .map(|ch| ch.to_string())
                    .unwrap_or_default(),
                Value::Int(_) | Value::UInt(_) => value
                    .as_i64()
                    .and_then(|number| u32::try_from(number).ok())
                    .and_then(char::from_u32)
                    .map(|ch| ch.to_string())
                    .unwrap_or_default(),
                Value::Float(_) => String::new(),
            };
            pad(Cow::Owned(text), width, flags, 0)
        }
        _ => value.as_text(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证模板宽度/精度硬上限.
    ///
    /// 参数：无.
    /// 断言：超过 1,000,000 的宽度/精度被拒绝，合法组合可编译；防止配置笔误造成每条日志 GB 级分配.
    #[test]
    fn rejects_excessive_template_width_and_precision() {
        assert!(TextFormat::new("%(message)1000001s", DEFAULT_DATE_FORMAT).is_err());
        assert!(TextFormat::new("%(message).1000001s", DEFAULT_DATE_FORMAT).is_err());
        assert!(TextFormat::new("%(message)80.120s", DEFAULT_DATE_FORMAT).is_ok());
    }

    /// 验证精度解析不会把空/溢出数字静默当成 0.
    ///
    /// 参数：无.
    /// 断言：`.` 后缺少数字、数字超出 `usize` 都返回配置错误；`.0s` 是合法精度.
    /// 旧实现的 `unwrap_or(0)` 会让 `%(message).s` 静默输出空串，并绕过精度上限检查.
    #[test]
    fn rejects_empty_or_overflowing_template_precision() {
        assert!(TextFormat::new("%(message).s", DEFAULT_DATE_FORMAT).is_err());
        assert!(TextFormat::new(
            "%(message).999999999999999999999999999999s",
            DEFAULT_DATE_FORMAT
        )
        .is_err());
        assert!(TextFormat::new("%(message).0s", DEFAULT_DATE_FORMAT).is_ok());
    }

    /// 验证从源文件推导模块链的规则.
    ///
    /// 参数：无.
    /// 断言：`src/a/b/main.rs` → `a::b::main`，`mod.rs` 去掉尾部 `mod`，显式 module_path 优先于
    /// 回退 target.
    #[test]
    fn caller_module_chain_uses_rust_source_layout() {
        assert_eq!(
            module_chain_from_file("src/app/ocr_service/main.rs"),
            "app::ocr_service::main"
        );
        assert_eq!(
            module_chain_from_file("src/infra/runtime/mod.rs"),
            "infra::runtime"
        );
        assert_eq!(
            module_chain("app::ocr_service::main", "fallback"),
            "app::ocr_service::main"
        );
    }

    /// 验证多调用点缓存不会互相驱逐.
    ///
    /// 参数：无.
    /// 断言：两个不同文件/模块交替访问后，第一次的 `Arc` 仍被复用（指针相等），防止单条缓存导致每个
    /// 事件都重新拼接.
    #[test]
    fn module_chain_cache_keeps_multiple_callsites_warm() {
        let first = cached_module_chain_from_file("src/a/one.rs");
        let second = cached_module_chain_from_file("src/b/two.rs");
        let first_again = cached_module_chain_from_file("src/a/one.rs");
        assert_eq!(first.as_ref(), "a::one");
        assert_eq!(second.as_ref(), "b::two");
        assert!(Arc::ptr_eq(&first, &first_again));

        let path_a = cached_module_chain("crate::a", "a");
        let path_b = cached_module_chain("crate::b", "b");
        let path_a_again = cached_module_chain("crate::a", "a");
        assert!(Arc::ptr_eq(&path_a, &path_a_again));
        assert_eq!(path_b.as_ref(), "crate::b");
    }

    /// 验证进程名只初始化一次.
    ///
    /// 参数：无.
    /// 断言：两次返回的静态字符串指针相同，说明 `OnceLock` 缓存生效.
    #[test]
    fn process_name_is_cached_static_text() {
        let first = process_name();
        let second = process_name();
        assert!(std::ptr::eq(first.as_ptr(), second.as_ptr()));
    }

    /// 验证整数 padding 与 printf flags 一致.
    ///
    /// 参数：无.
    /// 断言：零填充保留符号（`-007`）、空格填充在正号前留位（` +12`）.
    #[test]
    fn integer_padding_matches_printf_flags() {
        let mut out = String::new();
        push_padded_number(
            &mut out,
            "-",
            "7",
            Some(4),
            Flags {
                zero: true,
                ..Flags::default()
            },
        );
        assert_eq!(out, "-007");

        out.clear();
        push_padded_number(&mut out, "+", "12", Some(5), Flags::default());
        assert_eq!(out, "  +12");

        out.clear();
        let value = Value::Int(-42);
        render_value_into(
            &mut out,
            'd',
            &value,
            Flags {
                zero: true,
                ..Flags::default()
            },
            Some(6),
            None,
        );
        assert_eq!(out, "-00042");
    }

    /// 验证注入防护只转义真正的控制字符，不误伤合法 C2 前缀字符.
    ///
    /// 参数：无.
    /// 断言：`é`/`¢` 保持不变；换行和 C1 控制符被转义.
    #[test]
    fn sanitize_escapes_controls_without_false_positive_c2() {
        let mut out = String::from("é ¢");
        sanitize_output(&mut out);
        assert_eq!(out, "é ¢");

        let mut out = String::from("a\nb");
        sanitize_output(&mut out);
        assert_eq!(out, "a\\nb");

        let mut out = String::from("a\u{85}b");
        sanitize_output(&mut out);
        assert_eq!(out, "a\\u{85}b");
    }

    /// 验证零分配字节追加与 String 转义结果一致.
    ///
    /// 参数：无.
    /// 断言：`append_sanitized`/`sanitized_len` 与 `sanitize_output` 对换行、ESC、tab、C1 产生
    /// 完全相同输出，且长度预估等于实际字节数.
    #[test]
    fn append_sanitized_matches_string_sanitizer() {
        for raw in [
            "plain",
            "A\nB",
            "A\r\nB",
            "A\u{1b}[31mRED",
            "tab\tkept",
            "c1\u{85}control",
            "é ¢",
        ] {
            let mut bytes = Vec::new();
            append_sanitized(raw, &mut bytes);
            assert_eq!(
                sanitized_len(raw),
                bytes.len(),
                "length mismatch for {raw:?}"
            );
            let mut expected = raw.to_string();
            sanitize_output(&mut expected);
            assert_eq!(String::from_utf8(bytes).unwrap(), expected);
            assert_eq!(sanitize_text(raw).as_ref(), expected);
        }
    }

    /// 验证转义/原文都受单条输出上限约束.
    ///
    /// 参数：无.
    /// 断言：超长控制字符文本不会无限放大，输出带截断标记，长度预估与真实字节一致.
    #[test]
    fn sanitizer_caps_amplification() {
        let mut bytes = Vec::new();
        let huge = "x".repeat(MAX_SANITIZED_BYTES + 64);
        append_sanitized(&huge, &mut bytes);
        assert!(bytes.ends_with(b"...[truncated]"));
        assert_eq!(sanitized_len(&huge), bytes.len());

        let escaping = "\n".repeat(MAX_SANITIZED_BYTES / 2 + 64);
        let mut bytes = Vec::new();
        append_sanitized(&escaping, &mut bytes);
        assert!(bytes.ends_with(b"...[truncated]"));
        assert!(bytes.len() <= MAX_SANITIZED_BYTES + 16);
        assert_eq!(sanitized_len(&escaping), bytes.len());
    }
}
