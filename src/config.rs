//! tracing-flare 的配置与路径规则.
//!
//! 设计原理：
//! - 所有配置在 `build()/install()` 阶段校验，非法组合立即返回 `FlareError`，不等到运行期；
//! - `ServiceConfig` 是保守默认，`LoggerConfig` 是生产模板；两者可互相转换；
//! - `FlareConfig` 是唯一根构建器，负责 base_path/格式/默认 service/动态 service 策略；
//! - 路径组件在配置阶段拒绝穿越/分隔符/NUL/控制字符/Windows 保留名，
//!   避免“看起来写对了，实际落到别处”.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::engine::rotation::{Rotation as CoreRotation, SizeRotation, TimeRotation};
use crate::engine::runtime::{MAX_QUEUE_BYTES, MAX_QUEUE_RECORDS, MAX_SHARDS};

use tracing_subscriber::prelude::*;

use crate::error::FlareError;
use crate::format::{TextFormat, DEFAULT_DATE_FORMAT, DEFAULT_FORMAT};
use crate::level::FlareLevel;

/// 队列满策略.
///
/// 参数/变体：`Drop` 立即丢弃并计 overload；`Block(Duration)` 最多等待给定时间.
/// 设计原因：tracing 事件路径必须非阻塞，所以只有直接 `FlareLogger` 能看到 Block 语义.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlareOverflow {
    /// 丢弃新记录并计 overload（默认；tracing layer 始终走非阻塞路径）.
    #[default]
    Drop,
    /// 最多阻塞生产者 `Duration`；仅直接 `FlareLogger` 调用可观察，tracing 事件永不阻塞.
    Block(Duration),
}

impl FlareOverflow {
    /// 转成引擎内部策略.
    ///
    /// 参数：`self`；返回值：引擎 `Overflow`.
    /// 保持配置层与引擎层解耦，公共类型不依赖内部模块.
    pub(crate) fn to_engine(self) -> crate::engine::runtime::Overflow {
        match self {
            Self::Drop => crate::engine::runtime::Overflow::Drop,
            Self::Block(timeout) => crate::engine::runtime::Overflow::Block(timeout),
        }
    }
}

/// 未匹配任何 service 的 target 的处理策略.
///
/// 参数/变体：`Fallback` 走默认 service；`Drop` 丢弃并计 no_route；`Create` 允许
/// `FlareGuard::logger(name)` 按需创建 service.
/// 注意：`Create` 不会让手工 `Logger::new(name)` 自动创建，事件路径绝不做文件系统 I/O.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum UnknownTarget {
    /// 路由到 fallback/default service（默认）.
    #[default]
    Fallback,
    /// 丢弃并计 `no_route`.
    Drop,
    /// 允许显式 guard API 按需创建；事件路径仍不会 I/O.
    Create,
}

/// 归档压缩算法.
///
/// 参数/变体：`Gzip`/`Zstd`/`Lz4`；压缩必须有轮转配置，否则 build 拒绝.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlareCompression {
    /// gzip 流式压缩，分块可并行.
    Gzip,
    /// zstd，帧内可按大小启用多线程.
    Zstd,
    /// lz4 frame，低 CPU 成本.
    Lz4,
}

impl FlareCompression {
    /// 返回该算法对应的 Cargo feature.
    ///
    /// 参数：`self`；返回值：`compression-gzip`/`compression-zstd`/`compression-lz4`.
    pub const fn feature_name(self) -> &'static str {
        match self {
            Self::Gzip => "compression-gzip",
            Self::Zstd => "compression-zstd",
            Self::Lz4 => "compression-lz4",
        }
    }

    /// 返回当前构建是否启用了该算法.
    ///
    /// 参数：`self`；返回值：对应 feature 是否开启.
    /// 配置校验用它把“未编译 codec”变成构建期可见的配置错误，而不是轮转时才失败.
    pub const fn is_available(self) -> bool {
        match self {
            Self::Gzip => cfg!(feature = "compression-gzip"),
            Self::Zstd => cfg!(feature = "compression-zstd"),
            Self::Lz4 => cfg!(feature = "compression-lz4"),
        }
    }

    /// 转成引擎压缩算法.
    ///
    /// 参数：`self`；返回值：内部 `Compression`.
    pub(crate) fn to_engine(self) -> crate::engine::compress::Compression {
        match self {
            Self::Gzip => crate::engine::compress::Compression::Gzip,
            Self::Zstd => crate::engine::compress::Compression::Zstd,
            Self::Lz4 => crate::engine::compress::Compression::Lz4,
        }
    }
}

/// 单个 service 的轮转预设.
///
/// 参数/变体：`Never` 单文件；`Size` 按大小；`Time` 按时间；`Composite` 时间或大小先到先切.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlareRotation {
    /// 单文件，不轮转（默认）.
    #[default]
    Never,
    /// 大小轮转：`.1` 最新，旧归档依次后移.
    Size {
        /// 单文件最大字节数，必须 > 0.
        max_bytes: u64,
        /// 保留归档数量，必须 > 0.
        keep: usize,
    },
    /// 时间轮转（`S/M/H/D/MIDNIGHT/W0..W6`）.
    Time(TimeSpec),
    /// 时间 + 大小，先到先切.
    Composite {
        /// 大小条件.
        size: SizeSpec,
        /// 时间条件.
        time: TimeSpec,
    },
}

/// 时间轮转参数.
///
/// 字段：`when` 是粒度；`interval` 是间隔；`at_time` 是本地时间 `(时,分,秒)`；`utc` 是否按 UTC；
/// `keep` 保留数量.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TimeSpec {
    /// 粒度：`S/M/H/D/MIDNIGHT/W0..W6`.
    pub when: String,
    /// 每隔多少个粒度轮转一次；`MIDNIGHT/W*` 必须为 1.
    pub interval: i64,
    /// 本地轮转时刻；`None` 用粒度默认值.
    pub at_time: Option<(u8, u8, u8)>,
    /// 是否使用 UTC.
    pub utc: bool,
    /// 保留归档数量.
    pub keep: usize,
}

/// 大小轮转参数.
///
/// 字段：`max_bytes` 是阈值；`keep` 是保留归档数.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SizeSpec {
    /// 单文件最大字节数.
    pub max_bytes: u64,
    /// 保留归档数量.
    pub keep: usize,
}

impl FlareRotation {
    /// 每天午夜（本地时间）轮转.
    ///
    /// 参数：`keep` 保留归档数；返回值：`Time(MIDNIGHT)` 预设.
    pub fn daily(keep: usize) -> Self {
        Self::Time(TimeSpec {
            when: "MIDNIGHT".to_string(),
            interval: 1,
            at_time: None,
            utc: false,
            keep,
        })
    }

    /// 每小时轮转.
    ///
    /// 参数：`keep` 保留归档数；返回值：`Time(H)` 预设.
    pub fn hourly(keep: usize) -> Self {
        Self::Time(TimeSpec {
            when: "H".to_string(),
            interval: 1,
            at_time: None,
            utc: false,
            keep,
        })
    }

    /// 每分钟轮转.
    ///
    /// 参数：`keep` 保留归档数；返回值：`Time(M)` 预设.
    pub fn minutely(keep: usize) -> Self {
        Self::Time(TimeSpec {
            when: "M".to_string(),
            interval: 1,
            at_time: None,
            utc: false,
            keep,
        })
    }

    /// 按大小轮转.
    ///
    /// 参数：`max_bytes` 单文件阈值；`keep` 保留归档数；返回值：`Size` 预设.
    /// 两者都必须在build 时 > 0.
    pub fn size(max_bytes: u64, keep: usize) -> Self {
        Self::Size { max_bytes, keep }
    }

    /// 时间 + 大小组合轮转.
    ///
    /// 参数：`size` 大小条件；`time` 时间条件；返回值：`Composite` 预设，任一条件满足即切.
    pub fn composite(size: SizeSpec, time: TimeSpec) -> Self {
        Self::Composite { size, time }
    }

    /// 转成引擎轮转配置.
    ///
    /// 参数：`self`；返回值：`None` 表示不轮转，`Some` 为引擎 `Rotation`；非法大小/时间参数返回配置
    /// 错误.
    /// 校验在 build 阶段完成，writer 运行期不再判断合法性.
    pub(crate) fn to_core(&self) -> Result<Option<CoreRotation>, FlareError> {
        match self {
            Self::Never => Ok(None),
            Self::Size { max_bytes, keep } => {
                validate_size(*max_bytes, *keep)?;
                Ok(Some(CoreRotation::Size(SizeRotation {
                    max_bytes: *max_bytes,
                    keep: *keep,
                })))
            }
            Self::Time(spec) => Ok(Some(CoreRotation::Time(to_time(spec)?))),
            Self::Composite { size, time } => {
                validate_size(size.max_bytes, size.keep)?;
                Ok(Some(CoreRotation::Composite {
                    time: Some(to_time(time)?),
                    size: Some(SizeRotation {
                        max_bytes: size.max_bytes,
                        keep: size.keep,
                    }),
                    triggered: None,
                }))
            }
        }
    }
}

/// 校验大小轮转参数.
///
/// 参数：`max_bytes` 阈值；`keep` 保留数.
/// 返回值：合法 `Ok(())`，否则配置错误.
/// 两者为 0都会导致“每条记录轮转/立即清理”等病态行为，必须在构建期拒绝.
fn validate_size(max_bytes: u64, keep: usize) -> Result<(), FlareError> {
    if max_bytes == 0 {
        return Err(FlareError::Config("max_bytes must be > 0".to_string()));
    }
    if keep == 0 {
        return Err(FlareError::Config("keep must be > 0".to_string()));
    }
    Ok(())
}

/// 把配置层时间规格转成引擎轮转.
///
/// 参数：`spec` 是 `TimeSpec`；返回值：引擎 `TimeRotation` 或配置错误.
/// 校验 `keep`、粒度/间隔/本地时间组合，避免 writer 运行期才发现非法配置.
fn to_time(spec: &TimeSpec) -> Result<TimeRotation, FlareError> {
    if spec.keep == 0 {
        return Err(FlareError::Config("keep must be > 0".to_string()));
    }
    TimeRotation::new(
        &spec.when,
        spec.interval,
        spec.at_time,
        spec.utc,
        spec.keep,
        None,
    )
    .map_err(FlareError::Config)
}

/// 每 service 的生产者侧限流.
///
/// 固定一秒窗口：每墙钟秒最多 `max_per_second` 条记录入队；超限在入队前丢弃并计`rate_limited`.
/// 它保护磁盘不被失控生产者刷爆，不是分布式限流器.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct RateLimit {
    /// 每秒允许入队的记录数.
    pub max_per_second: u32,
}

impl RateLimit {
    /// 创建每秒上限.
    ///
    /// 参数：`max_per_second` 必须 > 0（0 在 service 校验阶段被拒绝）；返回值：限流配置.
    pub const fn per_second(max_per_second: u32) -> Self {
        Self { max_per_second }
    }
}

/// 单个 service/logger 的配置.
///
/// 字段说明：`name` 决定目录/文件名；`target` 是 tracing 路由前缀；`level` 是阈值；
/// `rotation/compression` 控制归档；`console/files` 选择 sink；`folder/name_as_folder`控制目录布局
/// ；`queue_size/flush_each/file_buffer_size/sync_on_flush` 控制吞吐与持久性；
/// `follow_symlinks/instance_lock` 是安全/运维开关；`overflow/rate_limit` 控制过载；
/// `backtrace_on_error` 控制错误堆栈；`shards` 控制 writer/队列/文件分片.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ServiceConfig {
    /// service 名，同时决定默认 target/目录/文件名.
    pub name: String,
    /// tracing 路由前缀；可含 `.`、`::`、`/`，不是文件名.
    pub target: String,
    /// 级别阈值.
    pub level: FlareLevel,
    /// 轮转策略.
    pub rotation: FlareRotation,
    /// 归档压缩；`None` 表示不压缩.
    pub compression: Option<FlareCompression>,
    /// 是否输出到 stderr.
    pub console: bool,
    /// 是否写文件.
    pub files: bool,
    /// base_path 下的可选二级目录.
    pub folder: Option<String>,
    /// 是否把文件放到 `<base>/<name>/<name>.log`.
    pub name_as_folder: bool,
    /// 队列记录预算.
    pub queue_size: usize,
    /// 队列字节预算；`None` 表示按 `queue_size * 256` 自动推导.
    ///
    /// 记录数预算无法约束 16 MiB 大消息的聚合内存；生产环境应显式设置该值.
    pub max_queued_bytes: Option<usize>,
    /// 每个 writer 批次结束是否 flush.
    pub flush_each: bool,
    /// BufWriter 容量.
    pub file_buffer_size: usize,
    /// 显式 flush/close 时是否 fsync.
    pub sync_on_flush: bool,
    /// 每个 writer 批次 flush 后是否立即 fsync.
    ///
    /// 这是“批次级持久化”开关：比 `sync_on_flush` 更强，但吞吐会降到磁盘 fsync 能力.
    pub sync_each_batch: bool,
    /// 活动日志路径是否允许跟随符号链接；默认关闭.
    pub follow_symlinks: bool,
    /// 是否启用单实例锁.
    pub instance_lock: bool,
    /// 队列满策略.
    pub overflow: FlareOverflow,
    /// 可选生产者侧每秒上限.
    pub rate_limit: Option<RateLimit>,
    /// 是否为 ERROR/CRITICAL 捕获堆栈.
    pub backtrace_on_error: bool,
    /// writer/队列分片数；默认 1.
    pub shards: usize,
}

impl ServiceConfig {
    /// 创建保守默认配置.
    ///
    /// 参数：`name` 是 service 名；返回值：`Info`、不轮转、不压缩、只写文件、队列 65,536、每批flush
    /// 的配置.
    /// 生产场景通常改用 [`LoggerConfig::new`].
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            target: name.clone(),
            name,
            level: FlareLevel::Info,
            rotation: FlareRotation::Never,
            compression: None,
            console: false,
            files: true,
            folder: None,
            name_as_folder: true,
            queue_size: 65_536,
            max_queued_bytes: None,
            flush_each: true,
            file_buffer_size: 64 * 1024,
            sync_on_flush: false,
            sync_each_batch: false,
            follow_symlinks: false,
            instance_lock: false,
            overflow: FlareOverflow::Drop,
            rate_limit: None,
            backtrace_on_error: false,
            shards: 1,
        }
    }

    /// 设置 tracing 路由前缀.
    ///
    /// 参数：`target` 是路由前缀；返回值：链式自身.
    /// 只影响 tracing 事件路由，不改变文件名.
    pub fn target(mut self, target: impl Into<String>) -> Self {
        self.target = target.into();
        self
    }

    /// 设置级别阈值.
    ///
    /// 参数：`level` 可为 `FlareLevel`、整型或级别名（通过 Into）；返回值：链式自身.
    pub fn level(mut self, level: impl Into<FlareLevel>) -> Self {
        self.level = level.into();
        self
    }

    /// 设置轮转策略.
    ///
    /// 参数：`rotation` 是预设或自定义规格；返回值：链式自身.
    pub fn rotation(mut self, rotation: FlareRotation) -> Self {
        self.rotation = rotation;
        self
    }

    /// 设置归档压缩.
    ///
    /// 参数：`compression` 是压缩算法；返回值：链式自身.
    /// 必须同时有轮转配置，否则 build 拒绝.
    pub fn compression(mut self, compression: FlareCompression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// 设置是否输出 stderr.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    /// 高吞吐路径不建议开启 console.
    pub fn console(mut self, enabled: bool) -> Self {
        self.console = enabled;
        self
    }

    /// 设置是否写文件.
    ///
    /// 参数：`enabled`；返回值：链式自身.`files=false && console=false` 会在 build 拒绝.
    pub fn files(mut self, enabled: bool) -> Self {
        self.files = enabled;
        self
    }

    /// 设置 base_path 下的二级目录.
    ///
    /// 参数：`folder` 是合法路径组件；返回值：链式自身.
    /// 非法组件在 build 拒绝.
    pub fn folder(mut self, folder: impl Into<String>) -> Self {
        self.folder = Some(folder.into());
        self
    }

    /// 设置是否按 service 名建目录.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    /// 默认 true，文件落 `<base>/[folder]/<name>/`.
    pub fn name_as_folder(mut self, enabled: bool) -> Self {
        self.name_as_folder = enabled;
        self
    }

    /// 设置队列记录预算.
    ///
    /// 参数：`size` 是记录条数上限，必须 > 0；返回值：链式自身.
    /// 设置队列记录预算.
    ///
    /// 参数：`size` 是记录条数上限，必须 > 0；返回值：链式自身.
    pub fn queue_size(mut self, size: usize) -> Self {
        self.queue_size = size;
        self
    }

    /// 设置队列字节预算.
    ///
    /// 参数：`bytes` 必须 > 0；返回值：链式自身.
    /// 未设置时按 `queue_size * 256` 推导；显式设置可约束大消息的聚合内存.
    pub fn max_queued_bytes(mut self, bytes: usize) -> Self {
        self.max_queued_bytes = Some(bytes);
        self
    }

    /// 设置 writer/队列分片数.
    ///
    /// 参数：`shards` 必须为 `1..=16`；返回值：链式自身.
    /// 大于 1 时每个 shard 使用独立队列和 writer，文件路径带 shard 后缀；跨 shard 不保证
    /// 全局行序. 分片预算由 `queue_size` 平均拆分.
    #[must_use]
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }

    /// 设置每个 writer 批次结束是否 flush.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    /// 关闭后吞吐更高，但需要业务屏障 flush.
    pub fn flush_each(mut self, enabled: bool) -> Self {
        self.flush_each = enabled;
        self
    }

    /// 设置 BufWriter 容量.
    ///
    /// 参数：`size` 是字节数，必须 > 0；返回值：链式自身.
    pub fn file_buffer_size(mut self, size: usize) -> Self {
        self.file_buffer_size = size;
        self
    }

    /// 设置显式 flush/close 时是否 fsync.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    /// 写批次本身不会 fsync.
    pub fn sync_on_flush(mut self, enabled: bool) -> Self {
        self.sync_on_flush = enabled;
        self
    }

    /// 设置每个 writer 批次 flush 后是否 fsync.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    /// 开启后每批日志在返回可见前等待 `fsync`，属于审计/交易级持久化，吞吐会显著下降.
    pub fn sync_each_batch(mut self, enabled: bool) -> Self {
        self.sync_each_batch = enabled;
        self
    }

    /// 低延迟可见性预设：每批 flush、8 KiB 文件缓冲.
    ///
    /// 参数：无；返回值：链式自身.
    /// 只保证尽快进入 OS 页缓存，不保证掉电持久；适合 tail/实时排障.
    pub fn low_latency(mut self) -> Self {
        self.flush_each = true;
        self.file_buffer_size = 8 * 1024;
        self
    }

    /// 审计级持久化预设：低延迟 + 每批 fsync.
    ///
    /// 参数：无；返回值：链式自身.
    /// 只应用于关键审计/交易日志；普通高吞吐日志不要开启.
    pub fn durable(mut self) -> Self {
        self.flush_each = true;
        self.sync_each_batch = true;
        self.sync_on_flush = true;
        self.file_buffer_size = 8 * 1024;
        self
    }

    /// 设置是否允许活动日志路径跟随符号链接.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    /// 安全敏感环境保持 false.
    pub fn follow_symlinks(mut self, enabled: bool) -> Self {
        self.follow_symlinks = enabled;
        self
    }

    /// 设置是否启用单实例锁.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    /// 要求 `files=true`，否则 build 拒绝.
    pub fn instance_lock(mut self, enabled: bool) -> Self {
        self.instance_lock = enabled;
        self
    }

    /// 设置队列满策略.
    ///
    /// 参数：`overflow`；返回值：链式自身.`Block` 仅对直接 logger 调用生效.
    pub fn overflow(mut self, overflow: FlareOverflow) -> Self {
        self.overflow = overflow;
        self
    }

    /// 设置每秒限流.
    ///
    /// 参数：`rate` 是每秒上限；返回值：链式自身.
    pub fn rate_limit(mut self, rate: RateLimit) -> Self {
        self.rate_limit = Some(rate);
        self
    }

    /// 移除限流.
    ///
    /// 参数：无；返回值：链式自身.
    pub fn no_rate_limit(mut self) -> Self {
        self.rate_limit = None;
        self
    }

    /// 设置是否为 ERROR/CRITICAL 捕获堆栈.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    /// 文本模式会追加多行堆栈，JSON 写入 backtrace 字段.
    pub fn backtrace_on_error(mut self, enabled: bool) -> Self {
        self.backtrace_on_error = enabled;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), FlareError> {
        validate_component(&self.name, "service name")?;
        // target 只是 tracing 路由前缀，可以包含 `.`、`::`、`/`，不是文件名.
        if self.target.is_empty() || self.target.contains('\0') {
            return Err(FlareError::Config(
                "target must not be empty or contain NUL".to_string(),
            ));
        }
        if self.target.chars().any(|ch| ch.is_control()) {
            return Err(FlareError::Config(format!(
                "target must not contain control characters: {:?}",
                self.target
            )));
        }
        if let Some(folder) = &self.folder {
            validate_component(folder, "folder")?;
        }
        if !self.files && !self.console {
            return Err(FlareError::Config(format!(
                "service {:?}: files=false and console=false leaves no sink",
                self.name
            )));
        }
        if let Some(compression) = self.compression {
            if !compression.is_available() {
                return Err(FlareError::Config(format!(
                    "service {:?}: compression codec {} is not compiled; enable feature {:?}",
                    self.name,
                    compression.feature_name(),
                    compression.feature_name()
                )));
            }
        }
        if self.queue_size == 0 {
            return Err(FlareError::Config(format!(
                "service {:?}: queue_size must be > 0",
                self.name
            )));
        }
        if self.max_queued_bytes == Some(0) {
            return Err(FlareError::Config(format!(
                "service {:?}: max_queued_bytes must be > 0",
                self.name
            )));
        }
        if self.queue_size > MAX_QUEUE_RECORDS {
            return Err(FlareError::Config(format!(
                "service {:?}: queue_size {} exceeds limit {MAX_QUEUE_RECORDS}",
                self.name, self.queue_size
            )));
        }
        if self
            .max_queued_bytes
            .is_some_and(|bytes| bytes > MAX_QUEUE_BYTES)
        {
            return Err(FlareError::Config(format!(
                "service {:?}: max_queued_bytes exceeds limit {MAX_QUEUE_BYTES}",
                self.name
            )));
        }
        if self.shards == 0 || self.shards > MAX_SHARDS {
            return Err(FlareError::Config(format!(
                "service {:?}: shards must be in 1..={MAX_SHARDS}, got {}",
                self.name, self.shards
            )));
        }
        if self.shards > self.queue_size {
            return Err(FlareError::Config(format!(
                "service {:?}: shards({}) must not exceed queue_size({})",
                self.name, self.shards, self.queue_size
            )));
        }
        if self.file_buffer_size == 0 {
            return Err(FlareError::Config(format!(
                "service {:?}: file_buffer_size must be > 0",
                self.name
            )));
        }
        if self.instance_lock && !self.files {
            return Err(FlareError::Config(format!(
                "service {:?}: instance_lock requires files=true",
                self.name
            )));
        }
        if let Some(limit) = self.rate_limit {
            if limit.max_per_second == 0 {
                return Err(FlareError::Config(format!(
                    "service {:?}: rate_limit.max_per_second must be > 0",
                    self.name
                )));
            }
            if limit.max_per_second < self.shards as u32 {
                return Err(FlareError::Config(format!(
                    "service {:?}: rate_limit({}) must be >= shards({})",
                    self.name, limit.max_per_second, self.shards
                )));
            }
        }
        Ok(())
    }

    /// 计算日志目录.
    ///
    /// 参数：`base` 是根目录；返回值：`base/[folder]/[name]`.
    /// 设计原因：路径规则集中在配置层，fs 侧只负责创建/打开，避免两处逻辑不一致.
    pub(crate) fn log_dir(&self, base: &Path) -> PathBuf {
        let root = self
            .folder
            .as_ref()
            .map_or_else(|| base.to_path_buf(), |folder| base.join(folder));
        if self.name_as_folder {
            root.join(&self.name)
        } else {
            root
        }
    }

    /// 计算常规日志文件路径.
    ///
    /// 参数：`base` 是根目录；返回值：`<log_dir>/<name>.log`.
    pub(crate) fn log_path(&self, base: &Path) -> PathBuf {
        self.log_dir(base).join(format!("{}.log", self.name))
    }

    /// 计算错误日志文件路径.
    ///
    /// 参数：`base` 是根目录；返回值：`<log_dir>/<name>_error.log`.ERROR/CRITICAL 进入该文件，与常
    /// 规文件分流.
    pub(crate) fn error_log_path(&self, base: &Path) -> PathBuf {
        self.log_dir(base).join(format!("{}_error.log", self.name))
    }

    /// 计算某个 shard 的常规日志路径.
    ///
    /// 参数：`base` 是根目录；`shard` 是分片序号.
    /// 返回值：`shards <= 1` 时保持原 `<name>.log`；多 shard 时为 `<name>.<shard:02>.log`.
    /// 每个 shard 独立文件，才能让多个 writer 真正并行写盘.
    pub(crate) fn log_path_for_shard(&self, base: &Path, shard: usize) -> PathBuf {
        if self.shards <= 1 {
            self.log_path(base)
        } else {
            self.log_dir(base)
                .join(format!("{}.{shard:02}.log", self.name))
        }
    }

    /// 计算某个 shard 的错误日志路径.
    ///
    /// 参数：`base` 是根目录；`shard` 是分片序号.
    /// 返回值：`shards <= 1` 时保持原 `<name>_error.log`；多 shard 时为
    /// `<name>_error.<shard:02>.log`.
    pub(crate) fn error_log_path_for_shard(&self, base: &Path, shard: usize) -> PathBuf {
        if self.shards <= 1 {
            self.error_log_path(base)
        } else {
            self.log_dir(base)
                .join(format!("{}_error.{shard:02}.log", self.name))
        }
    }
}

/// 高层生产 logger 配置模板.
///
/// `LoggerConfig::new(name)` 从生产向配置开始（更大队列、256 KiB 文件缓冲、128 MiB/10 份大小轮转，
/// 启用 `compression-zstd` 时默认 zstd 压缩），调用方可逐项覆盖.
/// 用 [`Self::into_service`] 转换，或直接传给 [`FlareConfig::service`] / [`FlareConfig::fallback`].
///
/// 它与 [`ServiceConfig`] 刻意分离：`ServiceConfig` 是保守的底层默认值（`Never` 轮转、64 KiB
/// 缓冲），而 `LoggerConfig` 是带有生产取舍的起点.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LoggerConfig {
    name: String,
    target: String,
    level: FlareLevel,
    rotation: FlareRotation,
    compression: Option<FlareCompression>,
    console: bool,
    files: bool,
    folder: Option<String>,
    name_as_folder: bool,
    queue_size: usize,
    max_queued_bytes: Option<usize>,
    flush_each: bool,
    file_buffer_size: usize,
    sync_on_flush: bool,
    sync_each_batch: bool,
    follow_symlinks: bool,
    instance_lock: bool,
    overflow: FlareOverflow,
    rate_limit: Option<RateLimit>,
    backtrace_on_error: bool,
    shards: usize,
}

/// 生产模板的默认压缩算法.
///
/// 参数：无.
/// 返回值：启用 `compression-zstd` 时为 `Some(Zstd)`，否则为 `None`.
/// 原因：压缩依赖是可选 feature，默认模板必须在不引入 zstd/native 依赖时仍可构建；
/// 需要压缩的部署应启用 feature 并显式选择算法.
fn default_compression() -> Option<FlareCompression> {
    #[cfg(feature = "compression-zstd")]
    {
        Some(FlareCompression::Zstd)
    }
    #[cfg(not(feature = "compression-zstd"))]
    {
        None
    }
}

impl Default for LoggerConfig {
    /// 默认生产模板；参数无，返回 `LoggerConfig::new("flare")`.
    fn default() -> Self {
        Self::new("flare")
    }
}

impl LoggerConfig {
    /// 创建生产模板.
    ///
    /// 参数：`name` 是 service 名；返回值：`Info`、128 MiB/10 份大小轮转、zstd 压缩、262,144 队列、
    /// 256 KiB 文件缓冲、`flush_each=false` 的生产配置.
    /// 调用方可逐项覆盖.
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            target: name.clone(),
            name,
            level: FlareLevel::Info,
            rotation: FlareRotation::size(128 * 1024 * 1024, 10),
            compression: default_compression(),
            console: false,
            files: true,
            folder: None,
            name_as_folder: true,
            queue_size: 262_144,
            max_queued_bytes: None,
            flush_each: false,
            file_buffer_size: 256 * 1024,
            sync_on_flush: false,
            sync_each_batch: false,
            follow_symlinks: false,
            instance_lock: false,
            overflow: FlareOverflow::Drop,
            rate_limit: None,
            backtrace_on_error: false,
            shards: 1,
        }
    }

    /// [`Self::new`] 的别名，明确表达“生产模板”.
    ///
    /// 参数：`name` 是 service 名；返回值：生产配置.
    pub fn production(name: impl Into<String>) -> Self {
        Self::new(name)
    }

    /// 从低层 `ServiceConfig` 复制全部字段.
    ///
    /// 参数：`service` 是保守配置；返回值：字段一一对应的 `LoggerConfig`，不再应用生产默认值.
    pub fn from_service(service: ServiceConfig) -> Self {
        Self {
            name: service.name,
            target: service.target,
            level: service.level,
            rotation: service.rotation,
            compression: service.compression,
            console: service.console,
            files: service.files,
            folder: service.folder,
            name_as_folder: service.name_as_folder,
            queue_size: service.queue_size,
            max_queued_bytes: service.max_queued_bytes,
            flush_each: service.flush_each,
            file_buffer_size: service.file_buffer_size,
            sync_on_flush: service.sync_on_flush,
            sync_each_batch: service.sync_each_batch,
            follow_symlinks: service.follow_symlinks,
            instance_lock: service.instance_lock,
            overflow: service.overflow,
            rate_limit: service.rate_limit,
            backtrace_on_error: service.backtrace_on_error,
            shards: service.shards,
        }
    }

    /// 返回 service 名.
    ///
    /// 参数：无；返回值：名字引用.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 设置 tracing 路由前缀；参数 `target`，返回链式自身.
    pub fn target(mut self, target: impl Into<String>) -> Self {
        self.target = target.into();
        self
    }

    /// 设置级别阈值；参数 `level`，返回链式自身.
    pub fn level(mut self, level: impl Into<FlareLevel>) -> Self {
        self.level = level.into();
        self
    }

    /// 设置轮转策略；参数 `rotation`，返回链式自身.
    pub fn rotation(mut self, rotation: FlareRotation) -> Self {
        self.rotation = rotation;
        self
    }

    /// 设置压缩算法；参数 `compression`，返回链式自身.
    pub fn compression(mut self, compression: FlareCompression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// 关闭归档压缩；参数无，返回链式自身.
    pub fn no_compression(mut self) -> Self {
        self.compression = None;
        self
    }

    /// 设置是否输出 stderr；参数 `enabled`，返回链式自身.
    pub fn console(mut self, enabled: bool) -> Self {
        self.console = enabled;
        self
    }

    /// 设置是否写文件；参数 `enabled`，返回链式自身.
    pub fn files(mut self, enabled: bool) -> Self {
        self.files = enabled;
        self
    }

    /// 设置二级目录；参数 `folder`，返回链式自身.
    pub fn folder(mut self, folder: impl Into<String>) -> Self {
        self.folder = Some(folder.into());
        self
    }

    /// 设置是否按 service 名建目录；参数 `enabled`，返回链式自身.
    pub fn name_as_folder(mut self, enabled: bool) -> Self {
        self.name_as_folder = enabled;
        self
    }

    /// 设置队列记录预算；参数 `size`，返回链式自身.
    /// 设置队列记录预算.
    ///
    /// 参数：`size` 是记录条数上限，必须 > 0；返回值：链式自身.
    pub fn queue_size(mut self, size: usize) -> Self {
        self.queue_size = size;
        self
    }

    /// 设置队列字节预算.
    ///
    /// 参数：`bytes` 必须 > 0；返回值：链式自身.
    /// 未设置时按 `queue_size * 256` 推导；显式设置可约束大消息的聚合内存.
    pub fn max_queued_bytes(mut self, bytes: usize) -> Self {
        self.max_queued_bytes = Some(bytes);
        self
    }

    /// 设置 writer/队列分片数.
    ///
    /// 参数：`shards` 必须为 `1..=16`；返回值：链式自身.
    /// 大于 1 时每个 shard 使用独立队列、writer 和文件路径；跨 shard 不保证全局行序.
    #[must_use]
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }

    /// 设置每批结束是否 flush；参数 `enabled`，返回链式自身.
    pub fn flush_each(mut self, enabled: bool) -> Self {
        self.flush_each = enabled;
        self
    }

    /// 设置 BufWriter 容量；参数 `size`，返回链式自身.
    pub fn file_buffer_size(mut self, size: usize) -> Self {
        self.file_buffer_size = size;
        self
    }

    /// 设置显式 flush/close 是否 fsync；参数 `enabled`，返回链式自身.
    pub fn sync_on_flush(mut self, enabled: bool) -> Self {
        self.sync_on_flush = enabled;
        self
    }

    /// 设置每个 writer 批次 flush 后是否 fsync；参数 `enabled`，返回链式自身.
    pub fn sync_each_batch(mut self, enabled: bool) -> Self {
        self.sync_each_batch = enabled;
        self
    }

    /// 低延迟可见性预设；参数无，返回链式自身.
    pub fn low_latency(mut self) -> Self {
        self.flush_each = true;
        self.file_buffer_size = 8 * 1024;
        self
    }

    /// 审计级持久化预设；参数无，返回链式自身.
    pub fn durable(mut self) -> Self {
        self.flush_each = true;
        self.sync_each_batch = true;
        self.sync_on_flush = true;
        self.file_buffer_size = 8 * 1024;
        self
    }

    /// 设置是否跟随符号链接；参数 `enabled`，返回链式自身.
    pub fn follow_symlinks(mut self, enabled: bool) -> Self {
        self.follow_symlinks = enabled;
        self
    }

    /// 设置是否启用实例锁；参数 `enabled`，返回链式自身.
    pub fn instance_lock(mut self, enabled: bool) -> Self {
        self.instance_lock = enabled;
        self
    }

    /// 设置队列满策略；参数 `overflow`，返回链式自身.
    pub fn overflow(mut self, overflow: FlareOverflow) -> Self {
        self.overflow = overflow;
        self
    }

    /// 设置每秒限流；参数 `rate`，返回链式自身.
    pub fn rate_limit(mut self, rate: RateLimit) -> Self {
        self.rate_limit = Some(rate);
        self
    }

    /// 移除限流；参数无，返回链式自身.
    pub fn no_rate_limit(mut self) -> Self {
        self.rate_limit = None;
        self
    }

    /// 设置错误堆栈捕获；参数 `enabled`，返回链式自身.
    pub fn backtrace_on_error(mut self, enabled: bool) -> Self {
        self.backtrace_on_error = enabled;
        self
    }

    /// 转成低层 `ServiceConfig`；参数：`self`；返回值：拥有所有权的 service 配置.
    pub fn into_service(self) -> ServiceConfig {
        ServiceConfig {
            name: self.name,
            target: self.target,
            level: self.level,
            rotation: self.rotation,
            compression: self.compression,
            console: self.console,
            files: self.files,
            folder: self.folder,
            name_as_folder: self.name_as_folder,
            queue_size: self.queue_size,
            max_queued_bytes: self.max_queued_bytes,
            flush_each: self.flush_each,
            file_buffer_size: self.file_buffer_size,
            sync_on_flush: self.sync_on_flush,
            sync_each_batch: self.sync_each_batch,
            follow_symlinks: self.follow_symlinks,
            instance_lock: self.instance_lock,
            overflow: self.overflow,
            rate_limit: self.rate_limit,
            backtrace_on_error: self.backtrace_on_error,
            shards: self.shards,
        }
    }

    /// 克隆并转成 `ServiceConfig`.
    ///
    /// 参数：`self`；返回值：新的 service 配置，调用方仍保留 `LoggerConfig`.
    pub fn to_service(&self) -> ServiceConfig {
        self.clone().into_service()
    }
}

impl From<LoggerConfig> for ServiceConfig {
    /// 便捷转换：消费 `LoggerConfig` 生成 `ServiceConfig`.
    ///
    /// 参数：`config`；返回值：service 配置.
    fn from(config: LoggerConfig) -> Self {
        config.into_service()
    }
}

impl From<ServiceConfig> for LoggerConfig {
    /// 便捷转换：从 `ServiceConfig` 复制字段.
    ///
    /// 参数：`service`；返回值：`LoggerConfig`.
    fn from(service: ServiceConfig) -> Self {
        Self::from_service(service)
    }
}

/// 根构建器.
///
/// 字段：`base_path` 日志根目录；`fmt/date_fmt` 文本模板；`default` 默认/fallback service；
/// `services` 显式 service；`unknown_target` 未匹配策略；`max_dynamic_services` 动态上限；
/// `json_lines` 是否 JSON Lines.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct FlareConfig {
    base_path: PathBuf,
    fmt: String,
    date_fmt: String,
    default: ServiceConfig,
    services: Vec<ServiceConfig>,
    unknown_target: UnknownTarget,
    max_dynamic_services: usize,
    json_lines: bool,
}

impl Default for FlareConfig {
    /// 默认根配置；参数无，返回 `FlareConfig::new()`.
    fn default() -> Self {
        Self::new()
    }
}

impl FlareConfig {
    /// 创建默认根配置.
    ///
    /// 参数：无；返回值：base_path=`logs`、默认模板、fallback service `flare`、无显式 service、
    /// `Fallback` 策略、动态上限 1024、文本输出的构建器.
    pub fn new() -> Self {
        Self {
            base_path: PathBuf::from("logs"),
            fmt: DEFAULT_FORMAT.to_string(),
            date_fmt: DEFAULT_DATE_FORMAT.to_string(),
            default: ServiceConfig::new("flare"),
            services: Vec::new(),
            unknown_target: UnknownTarget::Fallback,
            max_dynamic_services: 1_024,
            json_lines: false,
        }
    }

    /// 创建开箱即用的生产配置.
    ///
    /// 参数：`base_path` 是日志根目录.
    /// 返回值：默认模板使用 [`LoggerConfig::new`] 的生产参数、允许
    /// [`crate::Logger::get_logger`] 按名字创建动态 service、`Fallback` 关闭.
    /// 设计原因：把“队列/缓冲区/flush/轮转/压缩/动态 service”一次性设好，业务只需要
    /// `Logger::get_logger("name")`；逐条 `info` 仍是 tracing 路径，批量用 `log_many`.
    pub fn production(base_path: impl Into<PathBuf>) -> Self {
        let mut config = Self::new()
            .base_path(base_path)
            .unknown_target(UnknownTarget::Create)
            .max_dynamic_services(1_024);
        config.default = LoggerConfig::new("flare").into_service();
        config.default.shards = 1;
        // 动态 service 可能成千上万；生产模板的 262k 队列会按 service 放大到数十 GiB.
        // 动态模板使用更小的有界队列，静态高吞吐 service 仍可使用 LoggerConfig 的 262k.
        config.default.queue_size = 8_192;
        config.default.max_queued_bytes = Some(8 * 1024 * 1024);
        config
    }

    /// 设置日志根目录.
    ///
    /// 参数：`path` 是目录路径；返回值：链式自身.
    /// 目录不存在时由落盘侧递归创建.
    pub fn base_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.base_path = path.into();
        self
    }

    /// 设置文本模板.
    ///
    /// 参数：`fmt` 是模板字符串；返回值：链式自身.
    /// 非法模板在 build 阶段返回配置错误.
    pub fn format(mut self, fmt: impl Into<String>) -> Self {
        self.fmt = fmt.into();
        self
    }

    /// 设置 asctime 日期格式.
    ///
    /// 参数：`date_fmt` 是 chrono 格式；返回值：链式自身.
    /// 非法格式在 build 阶段拒绝.
    pub fn date_format(mut self, date_fmt: impl Into<String>) -> Self {
        self.date_fmt = date_fmt.into();
        self
    }

    /// 设置 JSON Lines 输出.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    /// 需要 `json` feature；当前是进程级模式，显式 service 也使用同一输出编码.
    #[cfg(feature = "json")]
    pub fn json_lines(mut self, enabled: bool) -> Self {
        self.json_lines = enabled;
        self
    }

    /// 设置 default service 阈值；参数 `level`，返回链式自身.
    pub fn level(mut self, level: impl Into<FlareLevel>) -> Self {
        self.default.level = level.into();
        self
    }

    /// 设置 default service 轮转；参数 `rotation`，返回链式自身.
    pub fn rotation(mut self, rotation: FlareRotation) -> Self {
        self.default.rotation = rotation;
        self
    }

    /// 设置 default service 压缩；参数 `compression`，返回链式自身.
    pub fn compression(mut self, compression: FlareCompression) -> Self {
        self.default.compression = Some(compression);
        self
    }

    /// 设置 default service 是否输出 stderr；参数 `enabled`，返回链式自身.
    pub fn console(mut self, enabled: bool) -> Self {
        self.default.console = enabled;
        self
    }

    /// 设置 default service 队列预算；参数 `size`，返回链式自身.
    pub fn queue_size(mut self, size: usize) -> Self {
        self.default.queue_size = size;
        self
    }

    /// 设置 default/dynamic service 模板的队列字节预算.
    ///
    /// 参数：`bytes` 必须 > 0；返回值：链式自身.
    pub fn max_queued_bytes(mut self, bytes: usize) -> Self {
        self.default.max_queued_bytes = Some(bytes);
        self
    }

    /// 设置 default service 的 writer/队列分片数.
    ///
    /// 参数：`shards` 必须为 `1..=16`；返回值：链式自身.
    /// 只影响 default/fallback service；显式 service 用 [`ServiceConfig::shards`].
    #[must_use]
    pub fn shards(mut self, shards: usize) -> Self {
        self.default.shards = shards;
        self
    }

    /// 设置 default service 是否写文件；参数 `enabled`，返回链式自身.
    pub fn files(mut self, enabled: bool) -> Self {
        self.default.files = enabled;
        self
    }

    /// 设置 default service 二级目录；参数 `folder`，返回链式自身.
    pub fn folder(mut self, folder: impl Into<String>) -> Self {
        self.default.folder = Some(folder.into());
        self
    }

    /// 设置 default service 是否按名建目录；参数 `enabled`，返回链式自身.
    pub fn name_as_folder(mut self, enabled: bool) -> Self {
        self.default.name_as_folder = enabled;
        self
    }

    /// 设置每批结束是否 flush；参数 `enabled`，返回链式自身.
    pub fn flush_each(mut self, enabled: bool) -> Self {
        self.default.flush_each = enabled;
        self
    }

    /// 设置 BufWriter 容量；参数 `size`，返回链式自身.
    pub fn file_buffer_size(mut self, size: usize) -> Self {
        self.default.file_buffer_size = size;
        self
    }

    /// 设置显式 flush/close 是否 fsync；参数 `enabled`，返回链式自身.
    pub fn sync_on_flush(mut self, enabled: bool) -> Self {
        self.default.sync_on_flush = enabled;
        self
    }

    /// 设置每批 flush 后是否 fsync；参数 `enabled`，返回链式自身.
    pub fn sync_each_batch(mut self, enabled: bool) -> Self {
        self.default.sync_each_batch = enabled;
        self
    }

    /// 低延迟可见性预设；参数无，返回链式自身.
    pub fn low_latency(mut self) -> Self {
        self.default = self.default.low_latency();
        self
    }

    /// 审计级持久化预设；参数无，返回链式自身.
    pub fn durable(mut self) -> Self {
        self.default = self.default.durable();
        self
    }

    /// 设置未匹配 target 的策略；参数 `behavior`，返回链式自身.
    pub fn unknown_target(mut self, behavior: UnknownTarget) -> Self {
        self.unknown_target = behavior;
        self
    }

    /// 设置动态 service 上限.
    ///
    /// 参数：`max` 是上限，内部至少为 1；返回值：链式自身.
    /// 每个 service 有独立 writer/文件句柄，因此这是硬资源边界.
    pub fn max_dynamic_services(mut self, max: usize) -> Self {
        self.max_dynamic_services = max.max(1);
        self
    }

    /// 设置 default service 实例锁；参数 `enabled`，返回链式自身.
    pub fn instance_lock(mut self, enabled: bool) -> Self {
        self.default.instance_lock = enabled;
        self
    }

    /// 设置 default service 是否跟随符号链接；参数 `enabled`，返回链式自身.
    pub fn follow_symlinks(mut self, enabled: bool) -> Self {
        self.default.follow_symlinks = enabled;
        self
    }

    /// 设置 default service 队列满策略；参数 `overflow`，返回链式自身.
    pub fn overflow(mut self, overflow: FlareOverflow) -> Self {
        self.default.overflow = overflow;
        self
    }

    /// 设置 default service 每秒限流；参数 `rate`，返回链式自身.
    pub fn rate_limit(mut self, rate: RateLimit) -> Self {
        self.default.rate_limit = Some(rate);
        self
    }

    /// 移除 default service 限流；参数无，返回链式自身.
    pub fn no_rate_limit(mut self) -> Self {
        self.default.rate_limit = None;
        self
    }

    /// 设置 default service 错误堆栈捕获；参数 `enabled`，返回链式自身.
    pub fn backtrace_on_error(mut self, enabled: bool) -> Self {
        self.default.backtrace_on_error = enabled;
        self
    }

    /// 添加显式 service.
    ///
    /// 参数：`service` 可为 `ServiceConfig`（保守默认）或 `LoggerConfig`（生产模板）；返回值：链式
    /// 自身.
    /// 显式 service 使用自己的配置，不受 default 字段影响.
    pub fn service(mut self, service: impl Into<ServiceConfig>) -> Self {
        self.services.push(service.into());
        self
    }

    /// 设置 fallback service，同时作为 `UnknownTarget::Create` 的模板.
    ///
    /// 参数：`service` 接受两种配置类型；返回值：链式自身.
    pub fn fallback(mut self, service: impl Into<ServiceConfig>) -> Self {
        self.default = service.into();
        self
    }

    /// 构建 layer 与 guard，不安装全局 subscriber.
    ///
    /// 参数：`self`；返回值：`(FlareLayer, FlareGuard)` 或配置/构建错误.
    /// 所有模板、service、路径、轮转先在 build 阶段校验；成功后才启动 writer.
    pub fn build(self) -> Result<(crate::layer::FlareLayer, crate::guard::FlareGuard), FlareError> {
        let format = TextFormat::new(self.fmt, self.date_fmt)?;
        crate::router::build(
            self.base_path,
            format,
            self.default,
            self.services,
            self.unknown_target,
            self.max_dynamic_services,
            self.json_lines,
        )
    }

    /// 构建并安装进程级 subscriber.
    ///
    /// 参数：`self`；返回值：拥有 runtime 的 guard.
    /// 重复 install 会返回 `FlareError::Subscriber`；guard drop 时 flush/close.
    /// 只有 install 后 `Logger::get_logger` 才具备动态创建能力.
    pub fn install(self) -> Result<crate::guard::FlareGuard, FlareError> {
        let (layer, guard) = self.build()?;
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::set_global_default(subscriber)
            .map_err(|error| FlareError::Subscriber(error.to_string()))?;
        guard.register_default_router();
        Ok(guard)
    }
}

/// 校验路径组件.
///
/// 参数：`value` 是 service 名/folder；`what` 是错误上下文.
/// 返回值：合法为 `Ok(())`，否则配置错误.
/// 规则：非空、不含 `/`/`\`/NUL、不是 `.`/`..`、不含控制字符；Windows 额外拒绝`:`、尾随点/空格和保
/// 留设备名，防止越出日志目录或产生意外文件.
fn validate_component(value: &str, what: &str) -> Result<(), FlareError> {
    if value.is_empty() {
        return Err(FlareError::Config(format!("{what} must not be empty")));
    }
    if value.chars().any(|ch| matches!(ch, '/' | '\\' | '\0')) {
        return Err(FlareError::Config(format!(
            "{what} must not contain path separators/NUL: {value:?}"
        )));
    }
    if value == "." || value == ".." {
        return Err(FlareError::Config(format!("{what} must not be {value:?}")));
    }
    if value.chars().any(|ch| ch.is_control()) {
        return Err(FlareError::Config(format!(
            "{what} must not contain control characters: {value:?}"
        )));
    }
    // Windows 有额外路径语义：`C:foo` 是盘符相对路径，`name:stream` 指向备用数据流，末尾
    // 点/空格会被 Win32 静默去掉.必须在它们逃出日志目录或创建意外文件之前拒绝.
    // 设备名也按健壮性要求拒绝.
    #[cfg(windows)]
    {
        if value.contains(':') {
            return Err(FlareError::Config(format!(
                "{what} must not contain ':' on Windows: {value:?}"
            )));
        }
        if value.ends_with('.') || value.ends_with(' ') {
            return Err(FlareError::Config(format!(
                "{what} must not end with a dot or space on Windows: {value:?}"
            )));
        }
        let stem = value
            .split('.')
            .next()
            .unwrap_or(value)
            .to_ascii_uppercase();
        const RESERVED: [&str; 22] = [
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7",
            "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
        ];
        if RESERVED.contains(&stem.as_str()) {
            return Err(FlareError::Config(format!(
                "{what} is a reserved Windows device name: {value:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证跨平台路径组件拒绝规则.
    ///
    /// 参数：无；断言：空、`.`、`..`、分隔符、NUL、控制字符都拒绝；合法 service 名通过.
    #[test]
    fn path_components_reject_traversal_and_separators() {
        for name in ["", ".", "..", "a/b", "a\\b", "a\0b", "a\nb"] {
            assert!(
                validate_component(name, "service name").is_err(),
                "accepted unsafe component {name:?}"
            );
        }
        assert!(validate_component("billing-api_1", "service name").is_ok());
    }

    /// 验证低延迟/持久化预设的字段组合.
    ///
    /// 参数：无.
    /// 断言：low_latency 只保证每批 flush；durable 额外开启每批 fsync；queue 字节预算可显式设置.
    #[test]
    fn latency_and_durability_profiles_are_explicit() {
        let visible = ServiceConfig::new("visible").low_latency();
        assert!(visible.flush_each);
        assert!(!visible.sync_each_batch);
        assert_eq!(visible.file_buffer_size, 8 * 1024);

        let durable = LoggerConfig::new("durable").durable().into_service();
        assert!(durable.flush_each);
        assert!(durable.sync_each_batch);
        assert!(durable.sync_on_flush);
        assert_eq!(durable.file_buffer_size, 8 * 1024);

        let budgeted = ServiceConfig::new("budgeted").max_queued_bytes(12 * 1024 * 1024);
        assert_eq!(budgeted.max_queued_bytes, Some(12 * 1024 * 1024));
    }

    /// 验证 Windows 特有路径语义.
    ///
    /// 参数：无；断言：盘符/ADS/保留设备名/尾随点空格被拒绝；普通点号文件名通过.
    #[cfg(windows)]
    #[test]
    fn windows_path_components_reject_drive_and_ads_forms() {
        for name in [
            "C:foo",
            "name:stream",
            "CON",
            "con.txt",
            "LPT1.log",
            "trailing.",
            "trailing ",
        ] {
            assert!(
                validate_component(name, "service name").is_err(),
                "accepted unsafe Windows component {name:?}"
            );
        }
        assert!(validate_component("billing.api-1", "service name").is_ok());
    }
}
