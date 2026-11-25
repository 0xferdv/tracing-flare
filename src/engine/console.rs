//! 终端落点.
//!
//! 与 Python 包保持同一套级别配色：DEBUG 青、INFO 白、WARNING 亮黄、ERROR 亮红整行；CRITICAL 沿用
//! ERROR 的亮红整行，避免同一严重级别出现两种视觉编码.
//! 非 ERROR 级别只给级别标签着色，避免整行转义序列影响复制.
//!
//! 输出按批聚合：一批一次 `lock + write_all + flush`.
//! 这样既保留终端实时可见性，又不会把高吞吐变成每行两次系统调用.

use std::env;
use std::io::{self, IsTerminal, Write};

use crate::engine::level::Level;
use crate::engine::record::Record;
use crate::engine::runtime::{Sink, SinkError};
use crate::format::{append_sanitized, sanitize_text};

/// 控制台 sink.
///
/// 字段：`enabled` 是构造时解析后的最终是否着色，避免每条记录重复读环境变量/TTY.
#[derive(Clone, Debug)]
pub struct ConsoleSink {
    enabled: bool,
}

impl ConsoleSink {
    /// 创建自动颜色策略的 stderr 控制台 sink.
    ///
    /// 参数：无.
    /// 返回值：已解析颜色状态的 sink.
    /// 着色开启时在 Windows 上尝试启用 VT 处理；失败只影响颜色，不影响日志.
    pub fn stderr() -> Self {
        let enabled = if env::var_os("NO_COLOR").is_some() {
            false
        } else if env::var_os("FORCE_COLOR").is_some_and(|value| !value.is_empty() && value != "0")
        {
            true
        } else {
            io::stderr().is_terminal()
        };
        if enabled {
            enable_windows_vt();
        }
        Self { enabled }
    }
}

impl Sink for ConsoleSink {
    /// 写单条记录；参数 `record`，返回写入结果.
    fn write(&self, record: &Record) -> Result<(), SinkError> {
        self.write_batch(&[record])
    }

    /// 批量写控制台.
    ///
    /// 参数：`records` 是记录引用切片.
    /// 返回值：写入/flush 结果.
    /// 每批只做一次 lock/write/flush，批末 flush 保证 tail 可见；颜色开启时逐条生成带 ANSI 的行.
    fn write_batch(&self, records: &[&Record]) -> Result<(), SinkError> {
        if records.is_empty() {
            return Ok(());
        }
        // 每批只做一次 lock/write/flush，而不是逐行；批末仍然 flush，控制台 tail 立刻
        // 可见.这样只去掉高吞吐下中间每一行的锁和系统调用开销.
        let mut output =
            Vec::with_capacity(records.iter().map(|record| record.message.len() + 2).sum());
        if self.enabled {
            for record in records {
                // 模板路径已转义（可能包含受信任的多行堆栈）；批量路径未转义，必须在着色前处理，
                // 否则裸 `\n`/ESC 可伪造终端行或注入 ANSI 序列.
                if record.sanitized {
                    output.extend_from_slice(colorize(&record.message, record.level).as_bytes());
                } else {
                    let clean = sanitize_text(&record.message);
                    output.extend_from_slice(colorize(clean.as_ref(), record.level).as_bytes());
                }
                output.push(b'\n');
            }
        } else {
            for record in records {
                if record.sanitized {
                    output.extend_from_slice(record.message.as_bytes());
                } else {
                    append_sanitized(&record.message, &mut output);
                }
                output.push(b'\n');
            }
        }
        let mut stream = io::stderr().lock();
        stream.write_all(&output)?;
        stream.flush()?;
        Ok(())
    }

    /// 写预格式化多行块.
    ///
    /// 参数：`level` 是块内统一级别；`block` 是以 `\n` 分隔的最终文本.
    /// 返回值：写入/flush 结果.
    /// 每批一次 lock/write/flush；颜色开启时逐行着色，关闭时逐行转义并保留换行结构.
    fn write_preformatted_block(&self, level: Level, block: &str) -> Result<(), SinkError> {
        if block.is_empty() {
            return Ok(());
        }
        let mut output = Vec::with_capacity(block.len() + block.lines().count() + 16);
        if self.enabled {
            for line in block.lines() {
                let clean = sanitize_text(line);
                output.extend_from_slice(colorize(clean.as_ref(), level).as_bytes());
                output.push(b'\n');
            }
        } else {
            for line in block.lines() {
                append_sanitized(line, &mut output);
                output.push(b'\n');
            }
        }
        let mut stream = io::stderr().lock();
        stream.write_all(&output)?;
        stream.flush()?;
        Ok(())
    }

    /// 刷新目标流；参数无，返回 flush 结果.
    fn flush(&self) -> Result<(), SinkError> {
        io::stderr().flush()?;
        Ok(())
    }

    /// close 等价于 flush；参数无，返回 flush 结果.
    fn close(&self) -> Result<(), SinkError> {
        self.flush()
    }
}

const RESET: &str = "\x1b[0m";

/// 返回级别对应的 ANSI 样式.
///
/// 参数：`level` 核心级别.
/// 返回值：ANSI 前缀；CRITICAL 与 ERROR 相同，避免同一严重级别出现两种视觉编码.
fn style(level: Level) -> &'static str {
    match level {
        Level::Debug => "\x1b[36m",
        Level::Info => "\x1b[37m",
        Level::Warning => "\x1b[1;33m",
        // CRITICAL 在 tracing 中是 ERROR 级别事件.控制台颜色保持一致，运维无需额外视觉编码
        // 就能按严重级别过滤.
        Level::Error | Level::Critical => "\x1b[1;31m",
    }
}

/// 给一行文本着色.
///
/// 参数：`text` 是已格式化行；`level` 是级别.
/// 返回值：带 ANSI 的 String.
/// ERROR/CRITICAL 整行着色；其他级别优先给 header 中的 `::LEVEL]` 标签着色，找不到标签时在行首补一
/// 个彩色标签，避免整行转义影响复制.
fn colorize(text: &str, level: Level) -> String {
    let style = style(level);
    if matches!(level, Level::Error | Level::Critical) {
        return format!("{style}{text}{RESET}");
    }
    let marker = format!("::{}]", level.as_str());
    if text.contains(&marker) {
        return text.replacen(&marker, &format!("::{style}{}{RESET}]", level.as_str()), 1);
    }
    format!("{style}[{}]{RESET} {text}", level.as_str())
}

/// 尝试启用 Windows 虚拟终端处理.
///
/// 参数：无；返回值：无.
/// 失败被忽略：颜色只是可选增强，不能让日志初始化失败.
/// 只处理标准输出/标准错误句柄.
#[cfg(windows)]
fn enable_windows_vt() {
    use windows_sys::Win32::System::Console::{
        GetConsoleMode, GetStdHandle, SetConsoleMode, ENABLE_VIRTUAL_TERMINAL_PROCESSING,
        STD_ERROR_HANDLE,
    };

    // SAFETY: Win32 控制台 API 接受标准句柄与有效 mode 指针；VT 只是可选增强，因此失败被
    // 有意忽略.
    unsafe {
        let handle = GetStdHandle(STD_ERROR_HANDLE);
        let mut mode = 0u32;
        if GetConsoleMode(handle, &mut mode) != 0 {
            let _ = SetConsoleMode(handle, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
        }
    }
}

/// 非 Windows 平台的空实现；参数无.
#[cfg(not(windows))]
fn enable_windows_vt() {}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证 CRITICAL 与 ERROR 颜色一致.
    ///
    /// 参数：无；断言：样式字符串相同，着色行以 ERROR 样式开头.
    #[test]
    fn critical_uses_the_same_color_as_error() {
        assert_eq!(style(Level::Critical), style(Level::Error));
        let rendered = colorize("boom", Level::Critical);
        assert!(rendered.starts_with(style(Level::Error)), "{rendered}");
    }
}
