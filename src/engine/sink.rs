//! 文件落点：懒打开、UTF-8 直接写、大小/时间轮转、归档清理和后台压缩.
//!
//! 线程模型：
//! - `FileSinkInner` 由 `FileSink` 的 `Mutex` 保护，可被多个线程直接调用；
//! - runtime 使用时只有 writer 线程会进入，因此热路径没有跨线程争用；
//! - `FileSink` 本身不创建消费线程，顺序由调用方保证.
//!
//! 写入策略：
//! - 无轮转：一批记录编码进复用的 `scratch`，一次 `write_all`；
//! - 有轮转：逐条判断大小/时间，但整批只采样一次时钟、整批 flush 一次；
//! - `flush_each` 控制的是“每个 writer 批次结束 flush”，不是每条记录一次系统调用.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::engine::compress::{
    try_acquire_compression_slot, Compression, MAX_GLOBAL_COMPRESSION_JOBS,
};
use crate::engine::level::Level;
use crate::engine::record::Record;
use crate::engine::rotation::Rotation;
use crate::engine::runtime::{Sink, SinkError, SinkHealth};
use crate::format::{append_sanitized, sanitized_len};

#[cfg(windows)]
const LINE_BREAK: &str = "\r\n";
#[cfg(not(windows))]
const LINE_BREAK: &str = "\n";

/// 单个文件 sink 允许同时运行的后台压缩任务数.
///
/// gzip 任务内部还会使用 rayon，zstd 任务会开 2–4 个 worker.
/// 若不设上限，短轮转间隔会让线程数随归档数增长；达到上限时保留未压缩归档，数据不丢.
const MAX_COMPRESSION_JOBS: usize = 2;
/// 告警队列上限.
/// 坏磁盘/权限错误可能每次轮转都失败，不能无限增长.
const MAX_WARNINGS: usize = 64;
/// 崩溃残留 `.tmp` 的清理间隔：每 60 秒最多扫描一次目录，避免每次轮转全目录遍历.
const TEMP_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
/// 有轮转时，多条记录先在内存中拼到该大小再写一次.
/// 大小轮转仍逐条判断阈值，但不再每条记录两次 `BufWriter::write_all`；256 KiB 足以摊薄调用开销，又
/// 不会形成大内存峰值.
const ROTATION_BUFFER_MAX: usize = 256 * 1024;
/// 轮转失败后的重试间隔.
/// 失败期间继续写当前文件，不能让“归档改名失败”演变成“之后每条记录都失败”：磁盘/权限/杀软锁文件可能
/// 在数秒内自行恢复，而日志不能停.
const ROTATION_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// 文件 sink 的无锁健康计数.
///
/// 字段：`rollovers/rollover_errors/warnings/compression_jobs` 与 writer 线程的状态同步更新.
/// 健康检查可能由监控线程在任意时刻调用；如果读取需要拿 writer 的 `inner` 锁，就会在磁盘慢、
/// 压缩 join 或写入大批次时阻塞监控线程. 这里用原子量提供近似快照，锁只保护真正的可变状态.
/// 所有读改写都用 `Relaxed`：只观测，不发布其他内存状态.
#[derive(Default)]
struct FileSinkHealth {
    /// 成功轮转次数.
    rollovers: AtomicU64,
    /// 轮转失败次数.
    rollover_errors: AtomicU64,
    /// 告警总数（包含因队列上限被省略的告警）.
    warnings: AtomicU64,
    /// 当前后台压缩任务数.
    compression_jobs: AtomicUsize,
}

impl FileSinkHealth {
    /// 生成健康快照；参数无，返回近似一致的健康统计.
    fn snapshot(&self) -> SinkHealth {
        SinkHealth {
            rollovers: self.rollovers.load(Ordering::Relaxed),
            rollover_errors: self.rollover_errors.load(Ordering::Relaxed),
            warnings: self.warnings.load(Ordering::Relaxed),
            compression_jobs: self.compression_jobs.load(Ordering::Relaxed),
        }
    }
}

/// 文件 sink 的内部可变状态.
///
/// 字段：`path/rotation/line_break` 决定落点与命名；`min_level/max_level` 是可选的直接调用过滤；
/// `flush_each/sync_on_flush/buffer_size` 控制持久性；`compression/jobs/sources` 管理后台压缩；
/// `writer/scratch/size` 是写路径状态；`warnings/*` 是有界告警；`forked` 防止子进程重复 flush.
pub struct FileSinkInner {
    /// 当前活动文件路径.
    pub path: PathBuf,
    /// `None` 表示单文件追加，不轮转.
    rotation: Option<Rotation>,
    /// 平台行尾：Windows 为 `\r\n`，其他平台为 `\n`.
    /// 按字节写入，避免重复编码.
    line_break: Vec<u8>,
    /// 可选的级别过滤，供直接调用 `write_at` 的调用方使用；runtime 侧还会在 SinkConfig再过滤一次.
    pub min_level: Option<i32>,
    pub max_level: Option<i32>,
    /// 每个 writer 批次结束是否 flush.
    /// 不是每条记录一次 flush.
    flush_each: bool,
    /// 显式 `flush()/close()` 时是否 fsync.
    /// 写批次本身不 fsync.
    sync_on_flush: bool,
    /// 每个 writer 批次 flush 后是否立即 fsync.
    sync_each_batch: bool,
    /// `BufWriter` 容量；小值减少内存，大值减少系统调用.
    buffer_size: usize,
    /// 打开活动日志路径时是否允许跟随符号链接.
    /// 默认关闭：能写日志目录的攻击者不能把进程的追加流重定向到任意文件.
    #[cfg_attr(not(unix), allow(dead_code))]
    follow_symlinks: bool,
    /// 归档压缩算法；`None` 表示轮转后保留明文归档.
    compression: Option<Compression>,
    /// 后台压缩任务.
    /// 时间轮转可能同时有多个；大小轮转在移动归档名前会 join 它们.
    compression_jobs: Vec<std::thread::JoinHandle<(PathBuf, io::Result<PathBuf>)>>,
    /// 与 `compression_jobs` 下标一一对应的源归档路径.
    /// 用于在清理 stale 归档时跳过仍在压缩的文件，避免在 Windows 上删除打开中的文件，或让压缩线程最
    /// 后 rename 失败.
    compression_sources: Vec<PathBuf>,
    /// 与 `FileSink` 共享的无锁健康计数.
    /// `FileSink::health` 只读这些原子量，不获取 `inner` 锁.
    health: Arc<FileSinkHealth>,
    /// 当前打开的缓冲写入器；懒打开，首次写入前为 None.
    writer: Option<BufWriter<File>>,
    /// 无轮转路径的复用编码缓冲，避免每批分配.
    scratch: Vec<u8>,
    /// 当前活动文件已写字节数（含行尾）；用于大小轮转判断.
    size: u64,
    /// 轮转次数，供健康检查.
    pub rollover_count: u64,
    /// 轮转失败次数（改名/刷新失败）.
    /// 失败不会中止后续写入，但会进入退避窗口.
    pub rollover_errors: u64,
    /// 轮转退避截止时间：失败后在这之前不再尝试轮转，避免每条记录都重复一次失败的 rename（Windows上
    /// rename 可能被 AV 锁定数分钟）.
    rotation_retry_at: Option<Instant>,
    /// 最近告警，最多 `MAX_WARNINGS` 条.
    warnings: Vec<String>,
    /// 超过告警上限后被省略的条数.
    warnings_dropped: u64,
    /// 上次临时文件清理时间，用于节流目录扫描.
    last_temp_cleanup: Option<Instant>,
    /// 与 `FileSink` 共享：进程 fork 且 sink 被丢弃后为 true.
    /// 即使 fork 时 sink 互斥锁被其他线程持有、`try_lock` 失败，`Drop` 也会检查它；
    /// 否则子进程 drop sink 时会把父进程的缓冲 flush 进真实日志文件.
    forked: Arc<AtomicBool>,
}

impl FileSinkInner {
    /// 创建文件 sink，支持显式 `flush()`/`close()` 时 fsync.
    ///
    /// 参数：
    /// - `path`/`rotation`/`min_level`/`max_level`/`flush_each`/`buffer_size`/`compression`/
    ///   `line_break` 同 [`Self::with_sync`]；
    /// - `sync_on_flush` 是否在显式 flush/close 时 `sync_data`；
    /// - `follow_symlinks` 是否允许活动路径跟随符号链接（安全敏感，默认 false）.
    ///
    /// 返回值：sink 或配置字符串错误.
    /// 校验 buffer 非 0、路径非空、行尾非空、轮转合法.
    #[allow(clippy::too_many_arguments)]
    pub fn with_sync(
        path: PathBuf,
        rotation: Option<Rotation>,
        min_level: Option<i32>,
        max_level: Option<i32>,
        flush_each: bool,
        buffer_size: usize,
        compression: Option<Compression>,
        sync_on_flush: bool,
        sync_each_batch: bool,
        follow_symlinks: bool,
        line_break: Vec<u8>,
    ) -> Result<Self, String> {
        if buffer_size == 0 {
            return Err("buffer_size must be > 0".to_string());
        }
        if path.as_os_str().is_empty() {
            return Err("log path must not be empty".to_string());
        }
        if line_break.is_empty() {
            return Err(
                "line_break must not be empty (records would be concatenated without separators)"
                    .to_string(),
            );
        }
        validate_rotation(rotation.as_ref())?;
        Ok(Self {
            path,
            rotation,
            line_break,
            min_level,
            max_level,
            flush_each,
            sync_on_flush,
            sync_each_batch,
            buffer_size,
            follow_symlinks,
            compression,
            compression_jobs: Vec::new(),
            compression_sources: Vec::new(),
            health: Arc::new(FileSinkHealth::default()),
            writer: None,
            scratch: Vec::new(),
            size: 0,
            rollover_count: 0,
            rollover_errors: 0,
            rotation_retry_at: None,
            warnings: Vec::new(),
            warnings_dropped: 0,
            last_temp_cleanup: None,
            forked: Arc::new(AtomicBool::new(false)),
        })
    }

    /// 判断级别是否落在 sink 区间.
    ///
    /// 参数：`level_no` 是级别数值；返回值：`[min, max)` 是否匹配.
    pub fn matches(&self, level_no: i32) -> bool {
        if let Some(min) = self.min_level {
            if level_no < min {
                return false;
            }
        }
        if let Some(max) = self.max_level {
            if level_no >= max {
                return false;
            }
        }
        true
    }

    /// 写一条文本（自动追加平台行尾）.
    ///
    /// 参数：`text` 是最终文本；返回值：IO 结果.
    /// 内部走 `write_many` 的批量路径.
    #[cfg(all(test, unix))]
    pub fn write(&mut self, text: &str) -> std::io::Result<()> {
        self.write_many(&[text])
    }

    /// 批量写入.
    ///
    /// 分支选择：
    /// - 无轮转：所有行编码进 `scratch`，一次 `write_all`；这是吞吐最高的路径.
    /// - 有轮转：每行都要按“当前大小 + 本行字节数”判断是否切文件，因此逐行写；
    ///   但时钟每批只取一次，`flush` 每批只做一次.
    ///
    /// 错误语义：返回 `Err` 表示至少一条记录未完整写入；调用方（runtime）会把本批计为 write_error.
    /// 已经写出的前几行不会回滚.
    /// 批量写文本.
    ///
    /// 参数：`texts` 是行文本切片；返回值：IO 结果.
    /// 无轮转时整批编码进 scratch 后一次 write；有轮转时逐条判断阈值但整批只取一次时钟/ flush 一次.
    #[cfg(all(test, unix))]
    pub fn write_many(&mut self, texts: &[&str]) -> std::io::Result<()> {
        if texts.is_empty() {
            return Ok(());
        }
        self.write_iter(texts.iter().map(|text| (*text, false)))
    }

    /// 批量写记录引用，避免调用方构造 `Vec<&str>`.
    ///
    /// 参数：`records` 是记录引用切片；返回值：IO 结果.
    pub fn write_records(&mut self, records: &[&Record]) -> std::io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        self.write_iter(
            records
                .iter()
                .map(|record| (&*record.message, record.sanitized)),
        )
    }

    /// 批量写连续记录切片（整批快路径）.
    ///
    /// 参数：`records` 是连续 `Record` 切片；返回值：IO 结果.
    /// 避免构造引用切片.
    pub fn write_records_all(&mut self, records: &[Record]) -> std::io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        self.write_iter(
            records
                .iter()
                .map(|record| (&*record.message, record.sanitized)),
        )
    }

    /// 写一个预格式化多行块.
    ///
    /// 参数：`block` 是以 `\n` 分隔的最终文本.
    /// 返回值：IO 结果.
    /// 实现复用 `write_iter`：`str::lines()` 保留行结构（不会把换行转义成字面量），每行仍走控制字符
    /// 转义与轮转判断. 空块不产生输出.
    pub fn write_preformatted_block(&mut self, block: &str) -> std::io::Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        self.write_iter(block.lines().map(|line| (line, false)))
    }

    /// 批量写入的公共实现.
    ///
    /// 分支选择：
    /// - 无轮转：所有行编码进 `scratch`，一次 `write_all`；这是吞吐最高的路径.
    /// - 有轮转：每行都要按“当前大小 + 本行字节数”判断是否切文件，因此逐行写；
    ///   但时钟每批只取一次，`flush` 每批只做一次.
    ///
    /// 错误语义：返回 `Err` 表示至少一条记录未完整写入；调用方（runtime）会把本批计为 write_error.
    /// 已经写出的前几行不会回滚.
    ///
    /// 参数：`texts` 是可克隆的 `(文本, 是否已转义)` 迭代器（无轮转路径需遍历两次：预估容量 +
    /// 编码）；返回值：IO 结果.
    fn write_iter<'a, I>(&mut self, texts: I) -> std::io::Result<()>
    where
        I: Iterator<Item = (&'a str, bool)> + Clone,
    {
        if self.forked.load(Ordering::Acquire) {
            return Err(io::Error::other("file sink was discarded after fork"));
        }
        // 顺手回收已结束的压缩任务，不阻塞 writer；压缩失败/成功通过 warnings 上报.
        self.collect_compression(false);
        self.open()?;
        let flush = self.flush_each;

        if self.rotation.is_none() {
            self.scratch.clear();
            // 容量预估用原文长度下界即可；未转义文本可能扩张，Vec 会按需增长.
            // 这样普通/已转义文本只在实际 append 时扫描一次，而不是先算精确长度再写一次.
            let expected: usize = texts
                .clone()
                .map(|(text, _)| text.len().saturating_add(self.line_break.len()))
                .sum();
            self.scratch.reserve(expected);
            for (text, sanitized) in texts {
                if sanitized {
                    self.scratch.extend_from_slice(text.as_bytes());
                } else {
                    append_sanitized(text, &mut self.scratch);
                }
                self.scratch.extend_from_slice(&self.line_break);
            }
            let written = self.scratch.len() as u64;
            let result = match self.writer.as_mut() {
                Some(writer) => writer.write_all(&self.scratch),
                None => Ok(()),
            };
            if let Err(error) = result {
                // 部分写失败时 size 已不可信；按磁盘实际长度重算，避免后续轮转阈值偏移.
                self.resync_size();
                return Err(error);
            }
            self.size = self.size.saturating_add(written);
            self.finish_writer_batch(flush)?;
            return Ok(());
        }

        // 轮转路径：每批只取一次时钟；大小阈值仍逐条精确判断，但把连续可写的行先攒进
        // 复用缓冲，达到 256 KiB 或需要轮转时再一次写出.原先每条记录两次 write_all，
        // 在 1M+ 条/秒时 BufWriter 调用开销会明显拖慢 writer.
        let now = now_secs();
        let now_instant = Instant::now();
        // 上一次轮转失败后先写当前文件；退避窗口内不再尝试 rename.否则大小/时间轮转
        // 的“仍然该切”判断会让每条记录都重复一次失败的 rename.
        let mut rotation_allowed = self
            .rotation_retry_at
            .is_none_or(|retry_at| now_instant >= retry_at);
        let mut buffer = std::mem::take(&mut self.scratch);
        buffer.clear();
        let result = (|| -> std::io::Result<()> {
            for (text, sanitized) in texts {
                // 按转义后的真实落盘长度判断轮转；直接用原文长度会在控制字符较多时低估.
                let text_len = if sanitized {
                    text.len()
                } else {
                    sanitized_len(text)
                };
                let payload = text_len.saturating_add(self.line_break.len());
                let payload_u64 = payload as u64;
                let logical = self.size.saturating_add(buffer.len() as u64);
                let should_roll = rotation_allowed
                    && logical > 0
                    && self
                        .rotation
                        .as_mut()
                        .map(|rotation| {
                            rotation.should_rollover(logical.saturating_add(payload_u64), now)
                        })
                        .unwrap_or(false);
                if should_roll {
                    self.write_buffer(&mut buffer)?;
                    self.rollover()?;
                    // rollover 因“目标已存在/rename 失败”跳过时会设置退避窗口.
                    // 本批剩余记录不再逐条重试，否则一次失败会放大成每条一次 rename.
                    if self.rotation_retry_at.is_some() {
                        rotation_allowed = false;
                    }
                } else if buffer.len().saturating_add(payload) > ROTATION_BUFFER_MAX {
                    self.write_buffer(&mut buffer)?;
                }
                if sanitized {
                    buffer.extend_from_slice(text.as_bytes());
                } else {
                    append_sanitized(text, &mut buffer);
                }
                buffer.extend_from_slice(&self.line_break);
                if buffer.len() >= ROTATION_BUFFER_MAX {
                    self.write_buffer(&mut buffer)?;
                }
            }
            self.write_buffer(&mut buffer)?;
            self.finish_writer_batch(flush)?;
            Ok(())
        })();
        self.scratch = buffer;
        result
    }

    /// 把轮转路径的临时缓冲写入当前文件并更新 size.
    ///
    /// 参数：`buffer` 是待写字节；返回值：IO 结果.
    /// 成功后清空缓冲；失败时按磁盘实际长度重算 size.
    fn write_buffer(&mut self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        let written = buffer.len() as u64;
        let result = match self.writer.as_mut() {
            Some(writer) => writer.write_all(buffer),
            None => return Err(io::Error::other("log file is not open")),
        };
        if let Err(error) = result {
            // 部分写失败：文件可能已有半行，size 以磁盘实际长度为准.
            self.resync_size();
            return Err(error);
        }
        self.size = self.size.saturating_add(written);
        buffer.clear();
        Ok(())
    }

    /// 写失败后按文件实际长度重算 size（部分写会使增量记账失真）.
    /// 写失败后按文件实际长度重算 size.
    ///
    /// 参数：无；返回值：无.
    /// 部分写会让增量记账失真；metadata 失败时保留旧值.
    fn resync_size(&mut self) {
        if let Ok(metadata) = fs::metadata(&self.path) {
            self.size = metadata.len();
        }
    }

    /// flush BufWriter；参数无，返回 IO 结果.
    fn flush_writer(&mut self) -> std::io::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    /// 结束一个 writer 批次时执行配置的 flush/fsync 策略.
    ///
    /// 参数：`flush` 是 `flush_each` 快照；返回值：IO 结果.
    /// `flush_each` 只做 BufWriter→OS；`sync_each_batch` 进一步等待存储设备.
    fn finish_writer_batch(&mut self, flush: bool) -> std::io::Result<()> {
        if flush {
            self.flush_writer()?;
            if self.sync_each_batch {
                self.sync_writer_now()?;
            }
        }
        Ok(())
    }

    /// 可选持久化屏障.
    /// 只在显式 `flush()`/`close()` 调用；每批 fsync 会把吞吐压到磁盘延迟，不是普通文件日志该有的行
    /// 为.
    /// 可选持久化屏障.
    ///
    /// 参数：无；返回值：IO 结果.
    /// 只有 `sync_on_flush=true` 才 `sync_data`；每批 fsync 会把吞吐压到磁盘延迟，不属于普通文件日
    /// 志行为.
    fn sync_writer(&mut self) -> std::io::Result<()> {
        if self.sync_on_flush {
            self.sync_writer_now()
        } else {
            Ok(())
        }
    }

    /// 立即 fsync 当前文件，不受 `sync_on_flush` 开关影响.
    ///
    /// 参数：无；返回值：IO 结果.
    /// `sync_each_batch` 的批次级持久化路径使用；没有打开 writer 时是 no-op.
    fn sync_writer_now(&mut self) -> std::io::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    /// 懒打开文件.
    ///
    /// 打开时读取现有文件长度，供大小轮转使用；如果配置了时间轮转，用文件 mtime 作为调度锚点，使进
    /// 程重启后不会把轮转周期重置为“从当前时刻重新计时”.
    /// 懒打开活动文件.
    ///
    /// 参数：无；返回值：IO 结果.
    /// 首次写入前 writer 为 None；打开时读取现有长度供大小轮转，时间轮转用 mtime 作为调度锚点；Unix
    /// 默认 O_NOFOLLOW、0600.
    fn open(&mut self) -> std::io::Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                create_private_dir(parent)?;
            }
        }
        let metadata = fs::metadata(&self.path).ok();
        self.size = metadata.as_ref().map(|meta| meta.len()).unwrap_or(0);
        if let (Some(metadata), Some(rotation)) = (metadata.as_ref(), self.rotation.as_mut()) {
            let started_at = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs_f64())
                .unwrap_or(0.0);
            rotation.on_open(started_at);
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true).read(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // 创建时和 open 后都设置严格权限；否则新文件会在下面 chmod 前按进程 umask 短暂可读.
            options.mode(0o600);
            if !self.follow_symlinks {
                options.custom_flags(libc::O_NOFOLLOW);
            }
        }
        let file = options.open(&self.path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // 尽力而为：日志可能含客户数据，默认 0666&umask 在多用户主机上常常全局可读.
            let _ = fs::set_permissions(&self.path, fs::Permissions::from_mode(0o600));
        }
        self.writer = Some(BufWriter::with_capacity(self.buffer_size.max(1024), file));
        Ok(())
    }

    /// 刷新屏障.
    ///
    /// 参数：无；返回值：IO 结果.
    /// 先等待压缩结束，再 flush 文件缓冲；`sync_on_flush` 时再 fsync.
    /// 写路径不会走到 fsync.
    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.forked.load(Ordering::Acquire) {
            self.discard_after_fork();
            return Ok(());
        }
        // `flush()` 是用户和测试依赖的屏障：归档压缩仍在写/rename 时不能返回.写路径本身不阻塞.
        self.collect_compression(true);
        self.flush_writer()?;
        self.sync_writer()
    }

    /// 关闭文件并等待所有归档压缩线程结束.
    ///
    /// 压缩线程持有归档临时文件，进程退出前必须 join，否则可能留下半成品 `.tmp`.
    /// 与旧版不同，这里返回最终 flush 的错误：否则磁盘满时“关停成功”会掩盖数据丢失.
    /// 关闭文件、等待压缩并返回 flush 错误.
    ///
    /// 参数：无；返回值：最终 flush/close 的 IO 结果.
    /// 失败不静默：关停成功不能掩盖数据丢失.
    pub fn close_checked(&mut self) -> io::Result<()> {
        if self.forked.load(Ordering::Acquire) {
            self.discard_after_fork();
            return Ok(());
        }
        let flushed = self.close_stream();
        // 关停不能留下压缩线程：它们持有归档临时文件，不 join 可能留下半成品.
        self.collect_compression(true);
        flushed
    }

    /// 外部轮转后重开活动文件.
    ///
    /// 参数：无；返回值：IO 结果.
    /// 先 flush/关闭当前句柄，等待压缩，再重新打开配置路径；任一步失败都返回错误，下一次写入仍会重
    /// 试 open.
    pub fn reopen(&mut self) -> io::Result<()> {
        if self.forked.load(Ordering::Acquire) {
            self.discard_after_fork();
            return Ok(());
        }
        let flushed = self.close_stream();
        self.collect_compression(true);
        let opened = self.open();
        flushed.and(opened)
    }

    /// 只 flush 并关闭文件句柄，不等待归档压缩（供 rollover 内部使用）.
    /// 只 flush 并关闭文件句柄，不等待压缩（供 rollover 内部使用）.
    ///
    /// 参数：无；返回值：flush/sync 结果.
    /// 失败写 warning；drop BufWriter 会再尝试一次 flush 并关闭 fd.
    fn close_stream(&mut self) -> io::Result<()> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };
        let mut result = writer.flush();
        if result.is_ok() && self.sync_on_flush {
            result = writer.get_ref().sync_data();
        }
        // drop BufWriter 会再尝试一次 flush（忽略错误）并关闭 fd.
        drop(writer);
        if let Err(error) = &result {
            self.warn(format!(
                "failed to flush log file on close: {} - {error}",
                self.path.display()
            ));
        }
        result
    }

    /// 回收后台压缩任务.
    ///
    /// `blocking=false` 用于写路径：只 join 已经结束的任务，不能因为某个大归档还在压缩就阻塞
    /// writer.`blocking=true` 用于 flush/close/大小轮转前，必须等全部任务结束.
    /// 回收后台压缩任务.
    ///
    /// 参数：`blocking` 为 true 时 join 全部任务（flush/close/大小轮转前）；false 时只回收已结束任
    /// 务，避免写路径被大归档压缩阻塞.
    fn collect_compression(&mut self, blocking: bool) {
        if blocking {
            let jobs = std::mem::take(&mut self.compression_jobs);
            self.compression_sources.clear();
            self.health.compression_jobs.store(0, Ordering::Relaxed);
            for handle in jobs {
                self.reap_compression(handle);
            }
            return;
        }
        let mut index = 0;
        while index < self.compression_jobs.len() {
            if self.compression_jobs[index].is_finished() {
                let handle = self.compression_jobs.swap_remove(index);
                self.compression_sources.swap_remove(index);
                self.reap_compression(handle);
            } else {
                index += 1;
            }
        }
        self.health
            .compression_jobs
            .store(self.compression_jobs.len(), Ordering::Relaxed);
    }

    /// 记录一条有界告警.
    ///
    /// 参数：`message` 是告警文本；返回值：无.
    /// 超过 `MAX_WARNINGS` 后只增加省略计数，防止坏磁盘每次轮转失败把告警队列撑爆.
    fn warn(&mut self, message: String) {
        self.health.warnings.fetch_add(1, Ordering::Relaxed);
        if self.warnings.len() < MAX_WARNINGS {
            self.warnings.push(message);
        } else {
            self.warnings_dropped = self.warnings_dropped.saturating_add(1);
        }
    }

    /// join 一个压缩任务并记录失败.
    ///
    /// 参数：`handle` 是压缩线程句柄；返回值：无.
    /// 成功/失败都只记 warning；线程 panic 也保留源归档.
    fn reap_compression(
        &mut self,
        handle: std::thread::JoinHandle<(PathBuf, io::Result<PathBuf>)>,
    ) {
        match handle.join() {
            Ok((_, Ok(_))) => {}
            Ok((archive, Err(error))) => self.warn(format!(
                "archive compression failed (keeping original): {} - {error}",
                archive.display()
            )),
            Err(_) => self.warn("archive compression thread panicked (keeping original; retried before next rotation)".to_string()),
        }
    }

    /// 把本次轮转产生的归档交给后台线程压缩.
    ///
    /// 时间轮转的归档名带新时间戳，可以同时压缩多个；大小轮转在移动 `.1/.2` 前会
    /// `collect_compression(true)`，避免压缩线程把 `.1` 改名成 `.1.gz` 与归档移位竞争.
    /// 把本次轮转归档交给后台线程压缩.
    ///
    /// 参数：`archive` 是明文归档路径；返回值：无.
    /// 受 per-sink 与全局并发上限约束；线程创建失败时保留明文归档，不在 writer 内同步压缩.
    fn start_compression(&mut self, archive: PathBuf) {
        let codec = match self.compression {
            Some(codec) => codec,
            None => return,
        };
        self.collect_compression(false);
        if self.compression_jobs.len() >= MAX_COMPRESSION_JOBS {
            // 每个压缩线程还可能使用 rayon/zstd 的内部线程；轮转速度超过压缩速度时
            // 不能无限加线程.保留明文归档不丢数据，交给 keep 清理策略自然淘汰.
            self.warn(format!(
                "compression jobs limit reached ({MAX_COMPRESSION_JOBS}); keeping uncompressed archive: {}",
                archive.display()
            ));
            return;
        }
        let thread_path = archive.clone();
        let result_path = archive.clone();
        let Some(slot) = try_acquire_compression_slot() else {
            self.warn(format!(
                "global compression jobs limit reached ({MAX_GLOBAL_COMPRESSION_JOBS}); keeping uncompressed archive: {}",
                archive.display()
            ));
            return;
        };
        let spawned = std::thread::Builder::new()
            .name("flare-log-compress".to_string())
            .stack_size(512 * 1024)
            .spawn(move || {
                let _slot = slot;
                let result = codec.compress_file(&thread_path);
                (result_path, result)
            });
        match spawned {
            Ok(handle) => {
                self.compression_jobs.push(handle);
                self.compression_sources.push(archive);
                self.health
                    .compression_jobs
                    .store(self.compression_jobs.len(), Ordering::Relaxed);
            }
            Err(error) => {
                // 线程创建失败时不在 writer 线程里同步压缩：大归档会把 writer 阻塞数秒，
                // 队列迅速打满并开始丢弃.保留明文归档，至少数据完整.
                self.warn(format!(
                    "failed to start compression thread (keeping uncompressed archive): {} - {error}",
                    archive.display()
                ));
            }
        }
    }

    /// 执行一次轮转.
    ///
    /// 顺序：计算归档改名步骤 -> 必要时等待压缩 -> 关闭当前文件 -> rename -> 启动新归档压缩 -> 清理
    /// 过期归档 -> 重新打开活动文件.
    ///
    /// 失败语义（企业环境里 rename 失败很常见：杀软锁文件、只读盘、目标已存在）：
    /// - 刷新失败：当前批数据可能没落盘，返回 Err 让 runtime 计入 write_error；
    /// - 目标已存在 / rename 失败：**不覆盖、不中止日志**.记录继续写入当前文件，
    ///   并进入 1 秒退避，避免每条记录都重试一次失败的 syscall.
    ///
    /// 旧实现直接 `?` 返回，且不推进轮转状态，导致一次 rename 失败之后所有记录都会重新判定“该轮转”
    /// 并再次失败，文件 sink 永久停写.
    /// 执行一次轮转.
    ///
    /// 参数：无；返回值：IO 结果.
    /// 顺序：计算归档步骤 → 必要时等待压缩 → 关闭当前文件 → rename →启动压缩 → 清理过期归档 → 重开
    /// 活动文件.
    /// 失败语义见模块文档：不覆盖、不中止日志、进入退避.
    fn rollover(&mut self) -> std::io::Result<()> {
        let shifts = match self.rotation.as_ref() {
            Some(rotation) => rotation.shifts_archives(),
            None => return Ok(()),
        };
        // 大小轮转要移动已有归档，在途压缩必须在 archive_steps 扫描目录前结束；否则计划
        // 按明文 `.1` 构造，而压缩线程稍后把它改名成 `.1.gz`，移位会漏掉它，归档序列
        // 少一代.时间轮转写新的时间戳名，不需要等待.
        if shifts {
            self.collect_compression(true);
        }
        let steps = match self.rotation.as_ref() {
            Some(rotation) => rotation.archive_steps(&self.path),
            None => return Ok(()),
        };
        // 先整体预检，再执行 rename：否则多步移动中途遇到“目标已存在”会留下编号空洞，
        // 下一次轮转还可能把已经移动过的归档再移一次.预检必须按执行顺序模拟：
        // `.1 -> .2` 的 `.2` 可能正是前一步 `.2 -> .3` 的来源，不能在“当前磁盘”
        // 上直接判冲突.
        let mut occupied: std::collections::HashSet<PathBuf> = steps
            .iter()
            .filter(|(source, _)| source.exists())
            .map(|(source, _)| source.clone())
            .collect();
        let mut conflict = None;
        for (source, target) in steps.iter() {
            if !occupied.contains(source) {
                continue;
            }
            if occupied.contains(target) {
                conflict = Some(target.clone());
                break;
            }
            occupied.remove(source);
            occupied.insert(target.clone());
        }
        if let Some(target) = conflict {
            self.backoff_rotation();
            self.warn(format!(
                "rotation target already exists; skipping rotation (continuing to current file): {}",
                target.display()
            ));
            return Ok(());
        }

        // 先关闭（flush）当前文件；失败说明缓冲里的数据没有可靠落盘，此时 rename 会把
        // “半写”的文件变成归档.当前批按写失败上报，但 sink 本身保持可用.
        if let Err(error) = self.close_stream() {
            self.backoff_rotation();
            self.warn(format!(
                "rotation pre-flush failed (current batch counted as write failure; continuing to current file): {} - {error}",
                self.path.display()
            ));
            self.open()?;
            return Err(error);
        }

        // 绝不覆盖已有归档：Unix 的 rename 会原子替换目标，时间戳撞名（时钟回拨/重启/
        // 多进程）会静默删掉旧归档；Windows 则 rename 失败.两种都改成“跳过本次轮转”.
        for (source, target) in steps.iter() {
            if !source.exists() {
                continue;
            }
            if target.exists() {
                self.backoff_rotation();
                self.warn(format!(
                    "rotation target already exists; skipping rotation (continuing to current file): {}",
                    target.display()
                ));
                self.open()?;
                return Ok(());
            }
            if let Err(error) = fs::rename(source, target) {
                self.backoff_rotation();
                self.warn(format!(
                    "rotation rename failed; skipping rotation (continuing to current file): {} -> {} - {error}",
                    source.display(),
                    target.display()
                ));
                self.open()?;
                return Ok(());
            }
        }

        let new_archive = steps.last().map(|(_, target)| target.clone());
        if let (Some(_), Some(archive)) = (self.compression, new_archive) {
            if archive.exists() {
                self.start_compression(archive);
            }
        }
        self.cleanup_temps();
        if let Some(rotation) = self.rotation.as_mut() {
            let archives = rotation.archives(&self.path);
            let stale = rotation.stale(&self.path, &archives);
            rotation.advance(now_secs());
            self.rollover_count = self.rollover_count.saturating_add(1);
            self.health
                .rollovers
                .store(self.rollover_count, Ordering::Relaxed);
            for path in stale {
                // 正在压缩的归档不能删：Linux 上压缩线程还能读到已打开的文件，但最后
                // remove/rename 会失败并留下孤儿归档；Windows 上删除打开中的文件会直接失败.
                // 本轮先保留，下一次轮转再按 keep 清理.
                if self
                    .compression_sources
                    .iter()
                    .any(|source| source == &path)
                {
                    continue;
                }
                if let Err(error) = fs::remove_file(&path) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        self.warn(format!(
                            "failed to clean archive (skipped): {} - {error}",
                            path.display()
                        ));
                    }
                }
            }
        }
        self.rotation_retry_at = None;
        if let Err(error) = self.open() {
            self.backoff_rotation();
            return Err(error);
        }
        Ok(())
    }

    /// 轮转失败后进入退避窗口：期间继续写当前文件，不做任何 rename.
    /// 轮转失败后进入退避窗口.
    ///
    /// 参数：无；返回值：无.
    /// 计数增加，并在 `ROTATION_RETRY_INTERVAL` 内不再尝试 rename，避免每条记录重复一次失败syscall.
    fn backoff_rotation(&mut self) {
        self.rollover_errors = self.rollover_errors.saturating_add(1);
        self.health
            .rollover_errors
            .store(self.rollover_errors, Ordering::Relaxed);
        self.rotation_retry_at = Some(Instant::now() + ROTATION_RETRY_INTERVAL);
    }

    /// 清理进程被 kill 后残留的压缩临时文件.
    ///
    /// 只删除超过 1 小时的 `.tmp`，避免误删另一个仍在写临时文件的压缩线程；目录扫描本身按
    /// `TEMP_CLEANUP_INTERVAL` 节流.
    /// 清理进程被 kill 后残留的压缩临时文件.
    ///
    /// 参数：无；返回值：无.
    /// 只删超过 1 小时的 `.tmp`，避免误删仍在写的压缩任务；目录扫描按`TEMP_CLEANUP_INTERVAL` 节流.
    fn cleanup_temps(&mut self) {
        if self
            .last_temp_cleanup
            .is_some_and(|last| last.elapsed() < TEMP_CLEANUP_INTERVAL)
        {
            return;
        }
        let Some(directory) = self.path.parent() else {
            return;
        };
        let Some(base) = self.path.file_name().and_then(|name| name.to_str()) else {
            return;
        };
        self.last_temp_cleanup = Some(Instant::now());
        let prefix = format!("{base}.");
        let now = SystemTime::now();
        let Ok(entries) = fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.starts_with(&prefix) || !name.ends_with(".tmp") || !path.is_file() {
                continue;
            }
            let stale = fs::metadata(&path)
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|modified| now.duration_since(modified).ok())
                .map(|age| age.as_secs() >= 3600)
                .unwrap_or(false);
            if stale {
                if let Err(error) = fs::remove_file(&path) {
                    self.warn(format!(
                        "failed to clean compression temp file (skipped): {} - {error}",
                        path.display()
                    ));
                }
            }
        }
    }

    /// fork 子进程：把继承的活动 fd 重定向到 `/dev/null`，不 flush 父进程缓冲.
    ///
    /// 参数：无；返回值：无.
    /// 重定向失败时宁可泄漏句柄，也不能把父进程未落盘数据重复写进真实文件；同时丢弃继承的压缩
    /// JoinHandle（子进程不存在这些线程）.
    /// （Windows 无 fork，此实现不参与编译.
    /// ）
    #[cfg(unix)]
    pub fn discard_after_fork(&mut self) {
        self.forked.store(true, Ordering::Release);
        // fork 子进程里压缩线程不存在；直接丢弃 JoinHandle，不能 join 一个永远不会结束的线程.
        self.compression_jobs.clear();
        self.compression_sources.clear();
        self.health.compression_jobs.store(0, Ordering::Relaxed);
        use std::os::unix::io::AsRawFd;
        let Some(writer) = self.writer.take() else {
            return;
        };
        let raw_fd = writer.get_ref().as_raw_fd();
        match OpenOptions::new().write(true).open("/dev/null") {
            Ok(null) => {
                let null_fd = null.as_raw_fd();
                let redirected = if null_fd != raw_fd {
                    // SAFETY: 两个描述符都有效；dup2 原子地把 raw_fd 替换为 /dev/null 描述.
                    // 此时子进程是单线程的.
                    unsafe { libc::dup2(null_fd, raw_fd) >= 0 }
                } else {
                    std::mem::forget(null);
                    true
                };
                if redirected {
                    // fd 已重定向到 /dev/null；drop 时会关闭重定向后的描述符.
                    drop(writer);
                } else {
                    // dup2 失败：flush 会把父进程缓冲重复写入真实文件，因此改为泄漏句柄.
                    std::mem::forget(writer);
                }
            }
            Err(_) => {
                // 重定向失败时宁可泄漏句柄，也不能 flush 到父进程文件而重复写数据.
                std::mem::forget(writer);
            }
        }
    }

    /// 非 Unix 的 fork 丢弃钩子；参数无.
    #[cfg(not(unix))]
    pub fn discard_after_fork(&mut self) {
        self.forked.store(true, Ordering::Release);
        // Windows 没有 fork，正常不会走到这里；仍保持字段状态一致.
        self.compression_jobs.clear();
        self.compression_sources.clear();
        self.health.compression_jobs.store(0, Ordering::Relaxed);
    }
}

impl Drop for FileSinkInner {
    /// drop 时避免 fork 子进程 flush 继承缓冲.
    ///
    /// 参数：`self`；返回值：无.
    /// 非 fork 情况由 `close`/`close_checked` 显式收尾；这里不隐式 flush，避免在析构中 panic/阻塞.
    fn drop(&mut self) {
        if self.forked.load(Ordering::Acquire) {
            // fork 子进程绝不能 flush 继承的缓冲.丢弃 JoinHandle 是安全的（子进程里这些线程
            // 不存在），且不能阻塞.
            self.compression_jobs.clear();
            self.compression_sources.clear();
            self.health.compression_jobs.store(0, Ordering::Relaxed);
            if let Some(writer) = self.writer.take() {
                std::mem::forget(writer);
            }
        }
    }
}

/// 读取当前 UNIX 秒（浮点）；参数无.
fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

/// 创建缺失的父目录.
///
/// 参数：`path` 目录；返回值：IO 结果.
/// Unix 0700；已存在目录保持原权限（可能是共享挂载）.
fn create_private_dir(path: &std::path::Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(path)
    }
}

/// 校验大小轮转参数.
///
/// 参数：`rotation` 是可选轮转；返回值：合法 `Ok(())`，否则错误字符串.
/// `max_bytes=0` 会让第一条之后每条都轮转并快速删光历史；`keep=0` 同理，必须在构造 sink 时拒绝.
fn validate_rotation(rotation: Option<&Rotation>) -> Result<(), String> {
    /// 校验单个大小轮转子配置；参数 `value`，返回合法/错误.
    fn size(value: &crate::engine::rotation::SizeRotation) -> Result<(), String> {
        if value.max_bytes == 0 {
            return Err("max_bytes must be > 0".to_string());
        }
        if value.keep == 0 {
            return Err("keep must be > 0".to_string());
        }
        Ok(())
    }
    match rotation {
        None | Some(Rotation::Time(_)) => Ok(()),
        Some(Rotation::Size(value)) => size(value),
        Some(Rotation::Composite { size: value, .. }) => {
            if let Some(value) = value {
                size(value)?;
            }
            Ok(())
        }
    }
}

/// 获取 mutex，poisoning 时取回内部值.
///
/// 参数：`mutex`；返回值：guard.
/// 日志落点不因某个线程 panic 而永久不可用.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 文件 sink 配置.
///
/// 默认值面向库使用者：不轮转、UTF-8、每批 flush、64 KiB 缓冲.
/// 高吞吐服务通常关掉`flush_each`、增大 `buffer_size`，并用显式 `flush()`/`close()` 做业务屏障.
#[derive(Clone, Debug)]
pub struct FileSinkOptions {
    /// 轮转策略；`None` 表示单文件一直追加.
    pub rotation: Option<Rotation>,
    /// 直接调用 `write_at` 时的最小级别（包含）.
    pub min_level: Option<i32>,
    /// 直接调用 `write_at` 时的最大级别（不包含）.
    pub max_level: Option<i32>,
    /// 每个 writer 批次结束是否 flush，默认 true.
    pub flush_each: bool,
    /// `BufWriter` 容量，默认 64 KiB.
    pub buffer_size: usize,
    /// 显式 `flush()`/`close()` 时是否 fsync，默认 false.
    /// 写批次不 fsync；开启后flush 返回即代表数据已到磁盘，代价是等待存储设备.
    pub sync_on_flush: bool,
    /// 每个 writer 批次 flush 后是否立即 fsync，默认 false.
    /// 批次级持久化；开启后每批等待存储设备.
    pub sync_each_batch: bool,
    /// 是否允许活动日志路径跟随符号链接.
    /// Unix 默认 false（防御可写日志目录中被预置的 symlink）.
    /// Windows 上目前只是建议值；reparse point 防护仍属后续加固项.
    pub follow_symlinks: bool,
    /// 轮转归档的压缩算法；压缩成功后删除明文归档.
    pub compression: Option<Compression>,
    /// 行尾；默认 Windows `\r\n`，其他平台 `\n`.
    pub line_break: Option<String>,
}

impl Default for FileSinkOptions {
    /// 返回默认配置.
    ///
    /// 参数：无；返回值：不轮转、无级别过滤、每批 flush、64 KiB 缓冲、不 fsync、不跟随symlink、无压
    /// 缩、平台默认行尾.
    fn default() -> Self {
        Self {
            rotation: None,
            min_level: None,
            max_level: None,
            flush_each: true,
            buffer_size: 64 * 1024,
            sync_on_flush: false,
            sync_each_batch: false,
            follow_symlinks: false,
            compression: None,
            line_break: None,
        }
    }
}

/// 线程安全的文件落点句柄.
///
/// 多个线程可以直接调用；每次调用由内部 `Mutex` 串行.
/// 跨线程顺序不保证；需要确定性顺序时使用 runtime 的单 writer.`Clone` 共享同一文件句柄与轮转状态.
#[derive(Clone)]
pub struct FileSink {
    inner: Arc<Mutex<FileSinkInner>>,
    /// 共享 fork 标志，同时存入 `FileSinkInner`，使 `Drop` 在 fork 时内层互斥锁不可用的情况下
    /// 仍能抑制继承的 `BufWriter` flush.
    forked: Arc<AtomicBool>,
    /// 无锁健康计数；与 `FileSinkInner` 共享同一组原子量.
    health: Arc<FileSinkHealth>,
}

impl FileSink {
    /// 创建线程安全文件 sink.
    ///
    /// 参数：`path` 活动路径；`options` 配置.
    /// 返回值：sink 或配置字符串错误.
    /// 构造阶段只校验并保存配置，不打开文件；首次写入时才创建目录/句柄.
    pub fn new(path: impl Into<PathBuf>, options: FileSinkOptions) -> Result<Self, String> {
        let line_break = options
            .line_break
            .unwrap_or_else(|| LINE_BREAK.to_string())
            .into_bytes();
        let inner = FileSinkInner::with_sync(
            path.into(),
            options.rotation,
            options.min_level,
            options.max_level,
            options.flush_each,
            options.buffer_size,
            options.compression,
            options.sync_on_flush,
            options.sync_each_batch,
            options.follow_symlinks,
            line_break,
        )?;
        let forked = Arc::clone(&inner.forked);
        let health = Arc::clone(&inner.health);
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            forked,
            health,
        })
    }

    /// 追加一条记录，不做级别过滤.
    #[cfg(all(test, unix))]
    pub fn write(&self, text: &str) -> io::Result<()> {
        lock(&self.inner).write(text)
    }

    /// 刷新文件缓冲，并等待在途归档压缩结束.
    pub fn flush(&self) -> io::Result<()> {
        lock(&self.inner).flush()
    }

    /// flush、关闭文件句柄并 join 所有压缩线程；返回最终 flush 的错误.
    pub fn close_checked(&self) -> io::Result<()> {
        lock(&self.inner).close_checked()
    }

    /// 外部轮转后重开活动文件.
    ///
    /// 参数：无；返回值：IO 结果.
    /// 见 [`FileSinkInner::reopen`].
    pub fn reopen(&self) -> io::Result<()> {
        lock(&self.inner).reopen()
    }

    /// 返回健康统计；参数无.
    /// 读共享原子量，不获取 `inner` 锁：磁盘慢、压缩 join 或 writer 正在写大批次时，监控线程也不会
    /// 被阻塞.
    pub fn health(&self) -> SinkHealth {
        self.health.snapshot()
    }

    /// fork 子进程专用：把继承的文件描述符重定向到 /dev/null，不 flush 父进程缓冲，避免把父进程未落
    /// 盘的数据重复写入.Unix 有效，Windows 为空操作.
    ///
    /// 使用 try_lock：fork 瞬间可能有其他线程正持有 sink 锁，子进程里这把锁永远不会释放；此时跳过资
    /// 源而不是死锁.
    pub fn discard_after_fork(&self) {
        // 先设置共享标志：fork 时若有其他线程持有内层互斥锁，下面的 try_lock 会失败，
        // 但 Drop 仍必须避免 flush 继承缓冲.
        self.forked.store(true, Ordering::Release);
        if let Ok(mut inner) = self.inner.try_lock() {
            inner.discard_after_fork();
        }
    }
}

impl Sink for FileSink {
    /// 写单条记录；参数 `record`，返回 Sink 结果.
    fn write(&self, record: &Record) -> Result<(), SinkError> {
        self.write_batch(&[record])
    }

    /// 批量写记录并做级别过滤.
    ///
    /// 参数：`records` 是记录引用切片；返回值：Sink 结果.
    /// 整批都匹配时直接写，避免构造引用 Vec；混合级别时才过滤.
    fn write_batch(&self, records: &[&Record]) -> Result<(), SinkError> {
        let mut inner = lock(&self.inner);
        // FileSinkOptions 的 min/max 是落点自己的过滤条件；runtime 可能通过
        // SinkConfig 再配一份级别区间.这里必须同时遵守，否则纯 Rust 用户把
        // FileSink 直接包进 SinkConfig::new 时，低级别记录会绕过文件落点配置.
        //
        // 常见配置下 runtime 已经按同一区间过滤过，整批都匹配；此时直接写入，
        // 避免每批构造一个 `Vec<&Record>`.
        if records
            .iter()
            .all(|record| inner.matches(record.level.as_i32()))
        {
            return inner.write_records(records).map_err(SinkError::from);
        }
        let selected: Vec<&Record> = records
            .iter()
            .copied()
            .filter(|record| inner.matches(record.level.as_i32()))
            .collect();
        if selected.is_empty() {
            return Ok(());
        }
        inner.write_records(&selected).map_err(SinkError::from)
    }

    /// 连续记录整批快路径.
    ///
    /// 参数：`records` 是连续 `Record` 切片；返回值：Sink 结果.
    /// 整批匹配时直接写，否则过滤.
    fn write_batch_all(&self, records: &[Record]) -> Result<(), SinkError> {
        let mut inner = lock(&self.inner);
        if records
            .iter()
            .all(|record| inner.matches(record.level.as_i32()))
        {
            return inner.write_records_all(records).map_err(SinkError::from);
        }
        // 混合级别：退回过滤路径.引用切片在快路径检查之后才构造，普通文件 sink 因此不会分配它.
        let selected: Vec<&Record> = records
            .iter()
            .filter(|record| inner.matches(record.level.as_i32()))
            .collect();
        if selected.is_empty() {
            return Ok(());
        }
        inner.write_records(&selected).map_err(SinkError::from)
    }

    /// 写预格式化多行块；参数 `level`/`block`，返回 Sink 结果.
    ///
    /// FileSinkOptions 的级别区间在锁内再检查一次，防止直接包装 FileSink 的调用方绕过过滤.
    fn write_preformatted_block(&self, level: Level, block: &str) -> Result<(), SinkError> {
        let mut inner = lock(&self.inner);
        if !inner.matches(level.as_i32()) {
            return Ok(());
        }
        inner
            .write_preformatted_block(block)
            .map_err(SinkError::from)
    }

    /// flush barrier；参数无，返回 Sink 结果.
    fn flush(&self) -> Result<(), SinkError> {
        FileSink::flush(self).map_err(SinkError::from)
    }

    /// close barrier；参数无，返回 Sink 结果.
    fn close(&self) -> Result<(), SinkError> {
        FileSink::close_checked(self).map_err(SinkError::from)
    }

    /// reopen；参数无，返回 Sink 结果.
    fn reopen(&self) -> Result<(), SinkError> {
        FileSink::reopen(self).map_err(SinkError::from)
    }

    /// fork 子进程丢弃继承句柄；参数无，返回无.
    fn discard_after_fork(&self) {
        FileSink::discard_after_fork(self);
    }

    /// 返回健康统计；参数无，返回 `Some` 统计.
    fn health(&self) -> Option<SinkHealth> {
        Some(FileSink::health(self))
    }
}

#[cfg(all(test, unix))]
mod fork_tests {
    use super::*;

    /// 验证 fork 子进程 drop 不会重复写父进程缓冲.
    ///
    /// 参数：无；断言：父进程缓冲只落盘一次，子进程退出码 0.
    #[test]
    #[ignore = "fork tests must run in a single-threaded process (Linux CI: --test-threads=1)"]
    fn fork_child_drop_does_not_flush_parent_buffer() {
        let dir = std::env::temp_dir().join(format!("flare-sink-fork-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("app.log");
        let sink = FileSink::new(
            path.clone(),
            FileSinkOptions {
                flush_each: false,
                buffer_size: 64 * 1024,
                ..Default::default()
            },
        )
        .unwrap();
        sink.write("parent-buffered").unwrap();

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // 子进程继承未 flush 的 BufWriter，必须丢弃它，不能把父进程字节 flush 进共享文件.
            sink.discard_after_fork();
            drop(sink);
            unsafe { libc::_exit(0) };
        }
        let mut status = 0;
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
        assert_eq!(waited, pid);
        assert!(libc::WIFEXITED(status));
        assert_eq!(libc::WEXITSTATUS(status), 0);

        sink.close_checked().unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(
            text.matches("parent-buffered").count(),
            1,
            "child must not duplicate parent buffer: {text}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// 验证默认拒绝跟随日志路径 symlink.
    ///
    /// 参数：无；断言：预置 symlink 时打开/写入失败.
    #[test]
    fn symlink_log_path_is_rejected_by_default() {
        use std::os::unix::fs::symlink;

        let dir = std::env::temp_dir().join(format!("flare-sink-symlink-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let victim = dir.join("victim.txt");
        fs::write(&victim, b"do-not-touch").unwrap();
        let log_path = dir.join("app.log");
        symlink(&victim, &log_path).unwrap();

        let sink = FileSink::new(
            log_path.clone(),
            FileSinkOptions {
                follow_symlinks: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(sink.write("must-not-follow").is_err());
        drop(sink);
        assert_eq!(fs::read(&victim).unwrap(), b"do-not-touch");

        let allowed = FileSink::new(
            log_path,
            FileSinkOptions {
                follow_symlinks: true,
                ..Default::default()
            },
        )
        .unwrap();
        allowed.write("intentional-follow").unwrap();
        allowed.close_checked().unwrap();
        assert!(fs::read_to_string(&victim)
            .unwrap()
            .contains("intentional-follow"));
        let _ = fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证健康检查不等待 writer 的 `inner` 锁.
    ///
    /// 参数：无.
    /// 断言：测试线程持有 `inner` 锁时，另一线程调用 `FileSink::health` 仍能在超时内返回.
    /// 这防止磁盘慢、压缩 join 或大批次写入把监控线程一起卡住.
    #[test]
    fn health_does_not_block_on_inner_mutex() {
        let dir = std::env::temp_dir().join(format!("flare-sink-health-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let sink =
            Arc::new(FileSink::new(dir.join("app.log"), FileSinkOptions::default()).unwrap());
        let held = lock(&sink.inner);

        let probe = Arc::clone(&sink);
        let (sender, receiver) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let _ = sender.send(probe.health());
        });
        let observed = receiver.recv_timeout(Duration::from_secs(2));
        drop(held);
        let health = observed.expect("health blocked on the inner sink mutex");
        assert_eq!(health.rollovers, 0);
        assert_eq!(health.rollover_errors, 0);
        assert_eq!(health.warnings, 0);
        assert_eq!(health.compression_jobs, 0);
        handle.join().expect("health probe thread");

        let _ = fs::remove_dir_all(&dir);
    }
}
