//! 归档压缩.
//!
//! 所有编码器都从源文件流式读到临时文件，峰值内存与归档大小无关：
//! - gzip：每 1 MiB 一个 member，窗口内用 rayon 并行，多 member 仍是合法 gzip；
//! - zstd：单帧，写入 content size；1 MiB 以上才启用原生多线程；
//! - lz4：单帧（Python `lz4.frame.decompress` 只读第一帧）.
//!
//! 写入顺序：临时文件 -> flush -> fsync -> rename -> 删除源文件.rename 前不覆盖已有目标（Windows 上
//! 先删除旧目标），保证失败时至少保留一份可读归档.
//!
//! Unix 在发布目标时使用 `hard_link` 而不是 `rename`：目标已存在时 `hard_link` 返回 `EEXIST`，
//! 可以关闭“检查后目标被替换”的 TOCTOU 窗口. 该做法要求文件系统支持硬链接；FAT/exFAT、部分
//! SMB/NFS 挂载和只读 overlay 会返回不支持. 此时压缩任务失败并保留明文归档，数据不丢；writer 线程
//! 不会被同步压缩阻塞.
//!
//! 压缩由 [`crate::engine::sink::FileSinkInner`] 的后台线程调用；本模块不启动线程，也不依赖writer线
//! 程以外的状态.

#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-lz4"
))]
use std::fs::{self, File};
use std::io::{self, ErrorKind};
#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-lz4"
))]
use std::io::{BufReader, BufWriter};
#[cfg(feature = "compression-gzip")]
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[cfg(feature = "compression-gzip")]
use flate2::write::GzEncoder;
#[cfg(feature = "compression-gzip")]
use flate2::Compression as GzipLevel;
#[cfg(feature = "compression-gzip")]
use rayon::prelude::*;

#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-lz4"
))]
const CHUNK_BYTES: usize = 1 << 20;
/// 一次并行 gzip 处理的输入窗口；内存上限由窗口大小决定，不随归档大小增长.
#[cfg(feature = "compression-gzip")]
const WINDOW_BYTES: usize = 16 << 20;
#[cfg(feature = "compression-zstd")]
const ZSTD_LEVEL: i32 = 3;

/// 全进程并发压缩任务上限.
///
/// 每个 FileSink 各自限制 2 个任务，但 N 个 logger 同时轮转时仍会产生 2N 个 OS 线程，再叠加
/// rayon/zstd 的内部线程会把 CPU 打满.
/// 这里再做一层进程级上限；拿不到槽位时保留明文归档（数据不丢），交给后续轮转/keep 清理.
pub(crate) const MAX_GLOBAL_COMPRESSION_JOBS: usize = 8;

static ACTIVE_COMPRESSION_JOBS: OnceLock<Mutex<usize>> = OnceLock::new();

/// 一个全局压缩槽位；drop 时归还.
/// 线程启动失败时闭包被 drop，槽位同样会释放.
pub(crate) struct CompressionSlot;

impl Drop for CompressionSlot {
    /// 归还槽位.
    ///
    /// 参数：`self`；返回值：无.
    /// 使用 saturating_sub，防御重复归还/计数异常.
    fn drop(&mut self) {
        let slots = ACTIVE_COMPRESSION_JOBS.get_or_init(|| Mutex::new(0));
        let mut active = slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *active = active.saturating_sub(1);
    }
}

/// 非阻塞申请一个全局压缩槽位.
///
/// 参数：无；返回值：`Some(slot)` 表示成功，`None` 表示达到进程上限.
/// 调用方在 `None` 时保留明文归档，不阻塞 writer.
pub(crate) fn try_acquire_compression_slot() -> Option<CompressionSlot> {
    let slots = ACTIVE_COMPRESSION_JOBS.get_or_init(|| Mutex::new(0));
    let mut active = slots
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if *active >= MAX_GLOBAL_COMPRESSION_JOBS {
        return None;
    }
    *active += 1;
    Some(CompressionSlot)
}

/// 支持的归档压缩算法.
///
/// 变体：`Gzip` 多 member 可并行；`Zstd` 单帧可原生多线程；`Lz4` 单帧低 CPU.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    /// gzip 多 member 流.
    Gzip,
    /// zstd 单帧.
    Zstd,
    /// lz4 frame 单帧.
    Lz4,
}

impl Compression {
    /// 返回归档文件后缀.
    ///
    /// 参数：`self`；返回值：`.gz`/`.zst`/`.lz4`.
    pub fn extension(self) -> &'static str {
        match self {
            Self::Gzip => ".gz",
            Self::Zstd => ".zst",
            Self::Lz4 => ".lz4",
        }
    }

    /// 把 `path` 压缩为 `<path><extension>`，成功后删除源文件.
    ///
    /// 失败保证：
    /// - 编码/fsync/rename 失败：删除临时文件，保留源文件；
    /// - rename 成功但删除源文件失败：保留压缩目标，返回错误，避免丢唯一归档.
    pub fn compress_file(self, path: &Path) -> io::Result<PathBuf> {
        #[cfg(any(
            feature = "compression-gzip",
            feature = "compression-zstd",
            feature = "compression-lz4"
        ))]
        {
            self.compress_file_impl(path)
        }
        #[cfg(not(any(
            feature = "compression-gzip",
            feature = "compression-zstd",
            feature = "compression-lz4"
        )))]
        {
            let feature = match self {
                Self::Gzip => "compression-gzip",
                Self::Zstd => "compression-zstd",
                Self::Lz4 => "compression-lz4",
            };
            let _ = path;
            Err(unsupported_codec(feature))
        }
    }

    /// 实际压缩实现；仅在至少启用一个 codec feature 时编译.
    ///
    /// 参数：`path` 是待压缩明文归档.
    /// 返回值：压缩目标路径或 IO 错误.
    #[cfg(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-lz4"
    ))]
    fn compress_file_impl(self, path: &Path) -> io::Result<PathBuf> {
        let target = with_extension(path, self);
        let temporary = temporary_path(&target);
        let result = (|| -> io::Result<()> {
            let source = File::open(path)?;
            #[cfg(feature = "compression-zstd")]
            let size = source.metadata()?.len();
            let mut input = BufReader::with_capacity(CHUNK_BYTES, source);
            let output = BufWriter::with_capacity(CHUNK_BYTES, create_private(&temporary)?);
            let output: BufWriter<File> = match self {
                Self::Gzip => {
                    #[cfg(feature = "compression-gzip")]
                    {
                        let mut output = output;
                        // `gzip_stream` 自己写出完整 member；再包一层 `GzEncoder` 会把已压缩的
                        // 分块二次压缩.
                        gzip_stream(&mut input, &mut output)?;
                        output
                    }
                    #[cfg(not(feature = "compression-gzip"))]
                    {
                        let _ = (&mut input, output);
                        return Err(unsupported_codec("compression-gzip"));
                    }
                }
                Self::Zstd => {
                    #[cfg(feature = "compression-zstd")]
                    {
                        let mut encoder = zstd::stream::Encoder::new(output, ZSTD_LEVEL)?;
                        // 小归档开多线程反而增加调度开销；1 MiB 以上再启用原生多线程.
                        if size >= (1 << 20) {
                            let workers = std::thread::available_parallelism()
                                .map(|value| value.get().min(4) as u32)
                                .unwrap_or(2)
                                .max(2);
                            encoder.multithread(workers)?;
                        }
                        // 写入源大小，一次性解码器不需要额外提示.
                        encoder.include_contentsize(true)?;
                        encoder.set_pledged_src_size(Some(size))?;
                        io::copy(&mut input, &mut encoder)?;
                        encoder.finish()?
                    }
                    #[cfg(not(feature = "compression-zstd"))]
                    {
                        let _ = (&mut input, output);
                        return Err(unsupported_codec("compression-zstd"));
                    }
                }
                Self::Lz4 => {
                    #[cfg(feature = "compression-lz4")]
                    {
                        let mut encoder = lz4_flex::frame::FrameEncoder::new(output);
                        io::copy(&mut input, &mut encoder)?;
                        encoder
                            .finish()
                            .map_err(|error| io::Error::other(error.to_string()))?
                    }
                    #[cfg(not(feature = "compression-lz4"))]
                    {
                        let _ = (&mut input, output);
                        return Err(unsupported_codec("compression-lz4"));
                    }
                }
            };
            // `into_inner` 会 flush BufWriter；rename 前先 sync，避免崩溃后留下已改名但内容为空的
            // 归档.
            let file = output.into_inner().map_err(|error| error.into_error())?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        // rename 之前不要提前删除已有目标：写入/rename 失败时旧归档必须保留.Unix 的
        // fs::rename 原子替换目标；Windows 在目标存在时 rename 会失败，所以先备份旧目标，
        // rename 失败时恢复——避免“旧归档已删、新归档没生成”的空窗.
        #[cfg(windows)]
        {
            let backup = if target.exists() {
                let backup = free_side_path(&target, ".bak");
                // 备份失败则直接放弃本次压缩；源明文仍然完整，清理临时文件.
                if let Err(error) = fs::rename(&target, &backup) {
                    let _ = fs::remove_file(&temporary);
                    return Err(error);
                }
                Some(backup)
            } else {
                None
            };
            if let Err(error) = fs::rename(&temporary, &target) {
                if let Some(backup) = backup.as_ref() {
                    let _ = fs::rename(backup, &target);
                }
                let _ = fs::remove_file(&temporary);
                return Err(error);
            }
            if let Some(backup) = backup {
                let _ = fs::remove_file(backup);
            }
        }
        #[cfg(not(windows))]
        {
            // `rename` 会原子替换已有目标.改用与临时文件同目录的硬链接：目标在检查后出现时
            // `hard_link` 返回 `EEXIST`，从而关闭 TOCTOU 覆盖窗口.
            if let Err(error) = fs::hard_link(&temporary, &target) {
                let _ = fs::remove_file(&temporary);
                return Err(error);
            }
            // 目标现在是完整且已 fsync 的 inode.删除临时名失败无害：数据已以 `target` 持久化，
            // 遗留 `.tmp` 会由周期性临时文件清理处理.
            let _ = fs::remove_file(&temporary);
        }
        // 压缩目标已经完整写入并 rename 成功.此刻删除源文件失败时保留目标，
        // 宁可短暂出现“明文 + 压缩”两份，也不要把唯一可用的压缩归档删掉；
        // 下一次轮转的 stale 清理会按 keep 规则处理重复文件.
        fs::remove_file(path)?;
        Ok(target)
    }
}

/// 构造“codec 未启用”的 IO 错误.
///
/// 参数：`feature` 是应启用的 Cargo feature 名.
/// 返回值：`Unsupported` 错误；调用方保留明文归档并记录 sink 写错误.
/// 这是配置阶段漏检的兜底：正常路径会在 `ServiceConfig::validate` 拒绝未启用 codec.
#[cfg(not(all(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-lz4"
)))]
fn unsupported_codec(feature: &str) -> io::Error {
    io::Error::new(
        ErrorKind::Unsupported,
        format!("compression codec disabled; enable the {feature} feature"),
    )
}

/// 计算压缩临时文件名.
///
/// 参数：`target` 是最终归档路径；返回值：`<target>.tmp`.
/// 使用可预测名字是为了崩溃后能清理；创建时配合 O_NOFOLLOW/0600 防攻击.
#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-lz4"
))]
fn temporary_path(target: &Path) -> PathBuf {
    let mut name = target.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

/// 以 0600 创建压缩临时文件.
///
/// 日志正文可能含客户数据.
/// 日志文件本身以 0600 打开（`sink.rs::open`），压缩临时文件与最终归档不能退化为 `0666 & umask`；
/// Unix 上在创建时指定 mode，并对崩溃遗留的旧临时文件再 chmod 一次（open 的 mode 只对新建文件生效）
/// .
/// 以 0600 创建压缩临时文件.
///
/// 参数：`path` 是临时文件路径.
/// 返回值：打开的 File 或 IO 错误.
/// 设计原因：日志可能含客户数据；Unix 创建时指定 mode 并再次 chmod，避免旧 .tmp 权限过宽；
/// O_NOFOLLOW 防止被预置 symlink 利用.
#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-lz4"
))]
fn create_private(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            // 临时文件名可预测.不要跟随被预置的 symlink 去截断任意目标；失败时保留明文归档.
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        // 旧 .tmp 可能由其他版本/进程创建；权限失败时宁可中止压缩，也不落一份可读归档.
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }
}

/// Windows 上为覆盖已有归档找一个不冲突的备份名.
///
/// 参数：`target` 是目标归档；`tag` 是首选后缀（如 `.bak`）.
/// 返回值：不存在的侧路径.
/// 最多尝试 1000 个编号，避免无限循环；全部冲突时返回首选名，由后续 rename 报错.
#[cfg(windows)]
#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-lz4"
))]
fn free_side_path(target: &Path, tag: &str) -> PathBuf {
    let build = |suffix: &str| {
        let mut name = target.as_os_str().to_os_string();
        name.push(suffix);
        PathBuf::from(name)
    };
    for index in 0..1000 {
        let candidate = if index == 0 {
            build(tag)
        } else {
            build(&format!("{tag}.{index}"))
        };
        if !candidate.exists() {
            return candidate;
        }
    }
    build(tag)
}

/// 把输入编码为多个连续 gzip member.
///
/// 仅在启用 `compression-gzip` 时编译.
///
/// 参数：`input` 源数据；`output` 目标写入器.
/// 返回值：IO 结果.
/// 每个窗口最多 `WINDOW_BYTES` 输入；窗口内按 1 MiB 分块并行压缩.
/// 多 member 是 gzip 规范允许的拼接格式，`gzip.decompress`/`MultiGzDecoder` 都能完整解出；空输入也
/// 写一个合法空 member.
#[cfg(feature = "compression-gzip")]
fn gzip_stream(input: &mut impl Read, output: &mut impl Write) -> io::Result<()> {
    let mut wrote_any = false;
    loop {
        let mut window: Vec<Vec<u8>> = Vec::new();
        let mut window_bytes = 0usize;
        let mut eof = false;
        while window_bytes < WINDOW_BYTES {
            let mut chunk = vec![0u8; CHUNK_BYTES];
            let read = read_filled(input, &mut chunk)?;
            if read == 0 {
                eof = true;
                break;
            }
            chunk.truncate(read);
            window_bytes += read;
            window.push(chunk);
            if read < CHUNK_BYTES {
                eof = true;
                break;
            }
        }
        if window.is_empty() {
            break;
        }
        // 只有一个 1 MiB 分块时，rayon 的调度开销大于并行收益，直接顺序压缩.
        let parts: Vec<io::Result<Vec<u8>>> = if window.len() == 1 {
            vec![gzip_chunk(&window[0])]
        } else {
            window.par_iter().map(|chunk| gzip_chunk(chunk)).collect()
        };
        for part in parts {
            output.write_all(&part?)?;
        }
        wrote_any = true;
        if eof {
            break;
        }
    }
    if !wrote_any {
        // 空归档也必须写一个合法的空 gzip member.
        output.write_all(&gzip_chunk(&[])?)?;
    }
    Ok(())
}

/// 压缩一个不超过 1 MiB 的 gzip 分块.
///
/// 参数：`chunk` 是输入字节；返回值：完整 gzip member 字节.rayon 并行窗口内每块调用一次，每块独立完
/// 成 header/trailer.
#[cfg(feature = "compression-gzip")]
fn gzip_chunk(chunk: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), GzipLevel::default());
    encoder.write_all(chunk)?;
    encoder.finish()
}

/// 读满 `buf` 或遇到 EOF.
///
/// 参数：`input` 源；`buf` 目标切片.
/// 返回值：实际读取字节数.
/// 单次 `read` 可能短读；不补满会让并行分块变小、压缩率下降，因此循环到满/EOF；EINTR 重试.
#[cfg(feature = "compression-gzip")]
fn read_filled(input: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match input.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(read) => total += read,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(total)
}

/// 在完整文件名后追加压缩后缀.
///
/// 参数：`path` 原归档路径；`codec` 算法.
/// 返回值：`app.log.1` -> `app.log.1.zst` 形式的新路径.
pub fn with_extension(path: &Path, codec: Compression) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    name.push(codec.extension());
    path.with_file_name(name)
}

/// 拆分归档名.
///
/// 参数：`name` 是文件名（不含目录）.
/// 返回值：`(base, Some(codec))` 或 `(name, None)`.
/// 从长后缀到短后缀匹配，返回去掉压缩后缀的基名与算法.
pub fn strip_suffix(name: &str) -> (&str, Option<Compression>) {
    for codec in [Compression::Gzip, Compression::Zstd, Compression::Lz4] {
        if let Some(base) = name.strip_suffix(codec.extension()) {
            return (base, Some(codec));
        }
    }
    (name, None)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// 验证已有压缩目标不会被覆盖.
    ///
    /// 参数：无；断言：目标已存在时压缩失败，旧目标内容不变，源明文保留.
    #[test]
    fn existing_compressed_target_is_not_overwritten() {
        let dir = std::env::temp_dir().join(format!("flare-compress-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let source = dir.join("app.log.1");
        fs::write(&source, b"new archive").unwrap();
        let target = with_extension(&source, Compression::Gzip);
        fs::write(&target, b"old archive").unwrap();

        let result = Compression::Gzip.compress_file(&source);
        assert!(result.is_err(), "existing archives must not be overwritten");
        assert_eq!(fs::read(&target).unwrap(), b"old archive");
        assert!(
            source.exists(),
            "existing archive must be preserved on failure"
        );

        let _ = fs::remove_dir_all(&dir);
    }
}
