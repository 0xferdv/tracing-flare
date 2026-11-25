//! 测试专用观测辅助：延迟分位与 RSS.
#![allow(dead_code)]

/// 当前进程 RSS 字节数；不支持的平台返回 `None`.
#[cfg(target_os = "linux")]
pub fn rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    // Linux 页面通常 4 KiB；用 sysconf 避免大页/架构差异.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    pages.checked_mul(page_size as u64)
}

/// 当前进程 RSS 字节数；Windows 使用 WorkingSetSize.
#[cfg(windows)]
pub fn rss_bytes() -> Option<u64> {
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    // SAFETY: `PROCESS_MEMORY_COUNTERS` 是只含整数字段的普通 C 结构，零值是合法初始状态；
    // `GetCurrentProcess` 返回当前进程的有效伪句柄.
    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
    counters.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
    let ok = unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) };
    (ok != 0).then_some(counters.WorkingSetSize as u64)
}

/// 当前进程 RSS 字节数；其他平台暂不采集.
#[cfg(not(any(target_os = "linux", windows)))]
pub fn rss_bytes() -> Option<u64> {
    None
}

/// 返回排序后的最近秩分位数.
pub fn percentile(sorted: &[u64], ratio: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 * ratio).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

/// 把纳秒转成便于阅读的微秒/毫秒文本.
pub fn format_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.2}us", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}
