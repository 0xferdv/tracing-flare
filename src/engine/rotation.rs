//! 轮转策略：何时切、归档叫什么、保留哪些.
//!
//! - 时间轮转：S/M/H/D/MIDNIGHT/W0..W6，支持 at_time、UTC 和本地 DST 修正；
//! - 大小轮转：`.1` 最新，向 `.N` 顺移，同时清理历史 `.N.1`；
//! - 组合轮转：时间或大小先到先切，`triggered` 记录本次由谁触发.
//!
//! 所有路径计算都使用同一套 `Path` 组件，避免字符串拼接在带点目录名/压缩后缀时误判归档序号.

use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{
    Datelike, Days, Local, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone,
    Timelike, Utc,
};

use crate::engine::compress::{strip_suffix, with_extension};

const DAY_SECONDS: i64 = 86_400;

/// 时间轮转粒度.
///
/// `Second/Minute/Hour/Day/Midnight/Week`；`Midnight/Week` 是墙钟边界模式，其余是“当前 + step”.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TimeVariant {
    /// 秒.
    Second,
    /// 分.
    Minute,
    /// 小时.
    Hour,
    /// 天（相对间隔）.
    Day,
    /// 每天午夜（本地/UTC 边界）.
    Midnight,
    /// 每周指定星期（边界）.
    Week,
}

impl TimeVariant {
    /// 解析粒度字符串.
    ///
    /// 参数：`when` 已大写去空白；返回值：`(variant, weekday)` 或 None；W0..W6 的日期必须 0..6.
    fn parse(when: &str) -> Option<(Self, Option<u32>)> {
        Some(match when {
            "S" => (Self::Second, None),
            "M" => (Self::Minute, None),
            "H" => (Self::Hour, None),
            "D" => (Self::Day, None),
            "MIDNIGHT" => (Self::Midnight, None),
            other if other.starts_with('W') && other.len() == 2 => {
                let day = other[1..].parse::<u32>().ok()?;
                if day > 6 {
                    return None;
                }
                (Self::Week, Some(day))
            }
            _ => return None,
        })
    }

    /// 返回粒度对应的秒步长.
    ///
    /// 参数：`interval` 是间隔倍数；返回值：秒数.
    /// 周固定 7 天，午夜按 1 天.
    fn step_seconds(self, interval: i64) -> i64 {
        match self {
            Self::Second => interval,
            Self::Minute => 60 * interval,
            Self::Hour => 3_600 * interval,
            Self::Day | Self::Midnight => DAY_SECONDS * interval,
            Self::Week => DAY_SECONDS * 7 * interval,
        }
    }

    /// 是否为墙钟边界模式；参数无，返回 bool.
    fn is_boundary(self) -> bool {
        matches!(self, Self::Midnight | Self::Week)
    }

    /// 返回归档时间戳格式；参数无.
    fn suffix(self) -> &'static str {
        match self {
            Self::Second => "%Y-%m-%d_%H-%M-%S",
            Self::Minute => "%Y-%m-%d_%H-%M",
            Self::Hour => "%Y-%m-%d_%H",
            Self::Day | Self::Midnight | Self::Week => "%Y-%m-%d",
        }
    }
}

/// 时间轮转状态.
///
/// `rollover_at` 是绝对 UNIX 秒，重启时可由文件 mtime 锚定，避免“每次启动都从当前时间重新计时”导致
/// 轮转周期漂移.`MIDNIGHT`/`W*` 是边界模式，其余是“当前 + step”.
#[derive(Clone, Debug)]
pub struct TimeRotation {
    pub at_time: (u8, u8, u8),
    pub utc: bool,
    pub keep: usize,
    variant: TimeVariant,
    weekday: Option<u32>,
    step: i64,
    anchored: bool,
    /// 本进程观察到的最新墙钟秒；系统时钟回拨时用它阻止轮转计划倒退.
    last_now: Cell<f64>,
    pub rollover_at: f64,
}

impl TimeRotation {
    /// 解析并构建时间轮转.
    ///
    /// 参数：`when` 粒度；`interval` 间隔（>0）；`at_time` 本地时刻；`utc` 是否 UTC；`keep` 保留数
    /// （>0）；`started_at` 可选文件 mtime 锚点.
    /// 返回值：轮转或错误字符串.
    /// 边界模式 interval 必须 1，at_time 必须合法.
    pub fn new(
        when: &str,
        interval: i64,
        at_time: Option<(u8, u8, u8)>,
        utc: bool,
        keep: usize,
        started_at: Option<f64>,
    ) -> Result<Self, String> {
        if interval <= 0 {
            return Err(format!(
                "interval must be a positive integer, got {interval}"
            ));
        }
        if keep == 0 {
            return Err("keep must be a positive integer, got 0".to_string());
        }
        let normalized = when.trim().to_ascii_uppercase();
        let (variant, weekday) = TimeVariant::parse(&normalized)
            .ok_or_else(|| format!("unsupported rotation granularity: {when:?}"))?;
        if variant.is_boundary() && interval != 1 {
            return Err(format!(
                "MIDNIGHT / W0..W6 interval must be 1, got {interval}; use when='D' with interval for daily/weekly multiples"
            ));
        }
        let at_time = at_time.unwrap_or((0, 0, 0));
        if at_time.0 > 23 || at_time.1 > 59 || at_time.2 > 59 {
            return Err("at_time hour/minute/second out of range".to_string());
        }
        let mut rotation = Self {
            at_time,
            utc,
            keep,
            variant,
            weekday,
            step: variant.step_seconds(interval),
            anchored: false,
            last_now: Cell::new(0.0),
            rollover_at: 0.0,
        };
        rotation.rollover_at = rotation.initial_rollover(started_at.unwrap_or_else(now_secs));
        Ok(rotation)
    }

    /// 判断是否到时间点.
    ///
    /// 参数：`_size` 忽略大小；`now` 当前 UNIX 秒；返回值：`now >= rollover_at`.
    pub fn should_rollover(&self, _size: u64, now: f64) -> bool {
        self.effective_now(now) >= self.rollover_at
    }

    /// 返回本进程观察到的单调不回退时间.
    ///
    /// 参数：`now` 是本次读取的墙钟秒.
    /// 返回值：`max(now, last_now)`；系统时钟回拨时保持上次观察值，避免轮转计划和归档
    /// 时间戳倒退. 这不是跨重启的 monotonic clock，重启仍以文件 mtime/当前墙钟锚定.
    fn effective_now(&self, now: f64) -> f64 {
        let last = self.last_now.get();
        let effective = now.max(last);
        self.last_now.set(effective);
        effective
    }

    /// 把墙钟边界折算到真实的 UNIX 时刻.
    ///
    /// 参数：`current` 当前/锚点 UNIX 秒；`next` 按本地墙钟算出的候选边界.
    /// 返回值：修正 DST 偏移后的边界.
    /// `compute_rollover` 用“当前本地时间 + 剩余墙钟秒”计算；如果区间内发生夏令时切换，候选值会
    /// 相差一小时. 用两端的本地偏移差修正，保证 MIDNIGHT/W* 在切换日仍是当地目标时刻.
    fn adjust_for_dst(&self, current: f64, next: f64) -> f64 {
        if self.utc {
            return next;
        }
        let offset_now = local_offset_seconds(current);
        let offset_next = local_offset_seconds(next);
        if offset_now != offset_next {
            next + (offset_now - offset_next) as f64
        } else {
            next
        }
    }

    /// 计算首次打开的轮转计划.
    ///
    /// 参数：`current` 是文件 mtime 或当前墙钟.
    /// 返回值：下一次边界/间隔时间.
    /// 只有墙钟边界模式需要 DST 修正；S/M/H/D 是固定秒数间隔，不能因为跨 DST 被拉长或缩短.
    /// 修正后若不再前进（极端回拨），退化为一个完整 step，避免首次写入立即触发轮转.
    fn initial_rollover(&self, current: f64) -> f64 {
        let candidate = self.compute_rollover(current);
        if !self.variant.is_boundary() {
            return candidate;
        }
        let adjusted = self.adjust_for_dst(current, candidate);
        if candidate > current && adjusted <= current {
            current + self.step as f64
        } else {
            adjusted
        }
    }

    /// 用文件 mtime 锚定轮转计划.
    ///
    /// 参数：`started_at` 是文件创建/上次修改时间；返回值：无.
    /// 只在首次打开且值 >0 时生效，避免进程重启把周期重置为“从当前时刻重新计时”.
    pub fn on_open(&mut self, started_at: f64) {
        if self.anchored || started_at <= 0.0 {
            return;
        }
        self.anchored = true;
        self.rollover_at = self.initial_rollover(started_at);
    }

    /// 生成时间轮转的归档改名步骤.
    ///
    /// 参数：`file_path` 活动文件；返回值：单步 `(活动文件, 带时间戳归档)`.
    /// 时间轮转不移动历史归档.
    pub fn archive_steps(&self, file_path: &Path) -> Vec<(PathBuf, PathBuf)> {
        let start = self.interval_start();
        let stamp = self.format_stamp(start);
        vec![(file_path.to_path_buf(), with_suffix(file_path, &stamp))]
    }

    /// 当前间隔的起点，用于归档命名.
    ///
    /// 非边界粒度（S/M/H/D）的边界是“起点 + step”的绝对时间，直接减 step 就是真实起点；MIDNIGHT/W*
    /// 是墙钟边界，夏令时切换日的间隔是 23/25 小时，不能直接减`step`，否则春季会把归档标成前一天、
    /// S/M/H 会差一小时.
    /// 这里按本地墙钟回退一天/一周再定位 at_time.
    fn interval_start(&self) -> f64 {
        if !self.variant.is_boundary() || self.utc {
            return self.rollover_at - self.step as f64;
        }
        let seconds = self.rollover_at.floor() as i64;
        let nanos = ((self.rollover_at - self.rollover_at.floor()) * 1_000_000_000.0) as u32;
        let Some(current) = Local.timestamp_opt(seconds, nanos).single() else {
            return self.rollover_at - self.step as f64;
        };
        let days = if self.weekday.is_some() { 7_u64 } else { 1_u64 };
        let previous_date = current.date_naive() - Days::new(days);
        let Some(at_time) = NaiveTime::from_hms_opt(
            u32::from(self.at_time.0),
            u32::from(self.at_time.1),
            u32::from(self.at_time.2),
        ) else {
            return self.rollover_at - self.step as f64;
        };
        let previous = previous_date.and_time(at_time);
        match Local.from_local_datetime(&previous) {
            LocalResult::Single(datetime) => datetime.timestamp() as f64,
            LocalResult::Ambiguous(earliest, _) => earliest.timestamp() as f64,
            // 春季 02:30 这类本地时间不存在：退回绝对减法，至少不 panic.
            LocalResult::None => self.rollover_at - self.step as f64,
        }
    }

    /// 计算下一次轮转时间并折算 DST 偏移变化.
    ///
    /// 参数：`now` 当前 UNIX 秒；返回值：无.
    /// 非边界模式用 `now + step`；边界模式按本地/UTC时分秒计算，并修正 DST.
    /// 只有真正越过边界才重排，避免大小轮转把时间轮转不断推后.
    pub fn advance(&mut self, now: f64) {
        let now = self.effective_now(now);
        // 组合轮转里由“大小”触发时, FileSink 也会调用 advance(now).如果现在还没到
        // 当前时间边界, 重排计划会把下一次时间轮转推后一个完整 step; 大小轮转比时间
        // 粒度更频繁时, 时间轮转将永远不会触发.只有真正越过边界才允许重排.
        if now < self.rollover_at {
            return;
        }
        let mut next = self.compute_rollover(now);
        while next <= now {
            next += self.step as f64;
        }
        if self.utc {
            self.rollover_at = next;
            return;
        }
        // 本地墙钟边界必须跨过 DST 跳变.旧逻辑只对 W0..W6 修正，导致每天/MIDNIGHT
        // 的触发点在春季前拨/秋季回拨后漂移一小时.
        let mut next = self.adjust_for_dst(now, next);
        if next <= now {
            // DST 回拨可能把修正后的 next 拉回到 now 之前；保证至少前进一个周期，
            // 否则下一次写入会立刻再次触发轮转（同一小时反复切文件）.
            next = now + self.step as f64;
        }
        self.rollover_at = next;
    }

    /// 计算下一次轮转时间.
    ///
    /// 参数：`current` 起始 UNIX 秒；返回值：下一次边界/间隔时间.
    /// 边界模式按本地/UTC 时分秒计算，并折算 DST 偏移变化.
    pub fn compute_rollover(&self, current: f64) -> f64 {
        if !self.variant.is_boundary() {
            return current + self.step as f64;
        }
        let (current_hour, current_minute, current_second, current_day) =
            self.local_components(current);
        let target_seconds =
            (self.at_time.0 as i64 * 60 + self.at_time.1 as i64) * 60 + self.at_time.2 as i64;
        let now_seconds =
            (current_hour as i64 * 60 + current_minute as i64) * 60 + current_second as i64;
        let mut remaining = target_seconds - now_seconds;
        let mut day = current_day;
        if remaining < 0 {
            remaining += DAY_SECONDS;
            day = (day + 1) % 7;
        }
        let result = current + remaining as f64;
        let Some(target_weekday) = self.weekday else {
            return result;
        };
        if day as u32 == target_weekday {
            return result;
        }
        let days_to_wait = if (day as u32) < target_weekday {
            target_weekday - day as u32
        } else {
            6 - day as u32 + target_weekday + 1
        };
        result + f64::from(days_to_wait) * DAY_SECONDS as f64
    }

    /// 返回本地/UTC 的 (时, 分, 秒, 星期几)；参数 `timestamp` 是 UNIX 秒.
    fn local_components(&self, timestamp: f64) -> (u32, u32, u32, i64) {
        let seconds = timestamp.floor() as i64;
        let nanos = ((timestamp - timestamp.floor()) * 1_000_000_000.0) as u32;
        if self.utc {
            match Utc.timestamp_opt(seconds, nanos) {
                LocalResult::Single(dt) => (
                    dt.hour(),
                    dt.minute(),
                    dt.second(),
                    i64::from(dt.weekday().num_days_from_monday()),
                ),
                _ => (0, 0, 0, 0),
            }
        } else {
            match Local.timestamp_opt(seconds, nanos) {
                LocalResult::Single(dt) => (
                    dt.hour(),
                    dt.minute(),
                    dt.second(),
                    i64::from(dt.weekday().num_days_from_monday()),
                ),
                _ => (0, 0, 0, 0),
            }
        }
    }

    /// 格式化归档时间戳；参数 `timestamp` 是 UNIX 秒，返回按粒度格式化的文本.
    fn format_stamp(&self, timestamp: f64) -> String {
        let seconds = timestamp.floor() as i64;
        let nanos = ((timestamp - timestamp.floor()) * 1_000_000_000.0) as u32;
        let pattern = self.variant.suffix();
        if self.utc {
            Utc.timestamp_opt(seconds, nanos)
                .single()
                .map(|dt| dt.format(pattern).to_string())
                .unwrap_or_else(|| "1970-01-01".to_string())
        } else {
            Local
                .timestamp_opt(seconds, nanos)
                .single()
                .map(|dt| dt.format(pattern).to_string())
                .unwrap_or_else(|| "1970-01-01".to_string())
        }
    }

    /// 解析任意受支持粒度的归档时间戳.
    ///
    /// 只按当前 `variant` 解析会让配置从 `H` 改成 `D`（或 `M` 改成 `H`）之后的旧归档全部“隐身”：
    /// `archives()` 看不到它们，`stale()` 也就永远不会按 `keep` 清理，磁盘占用无上限增长.
    /// 这里按从细到粗依次尝试.
    fn parse_any_stamp(&self, suffix: &str) -> Option<f64> {
        const PATTERNS: [&str; 3] = ["%Y-%m-%d_%H-%M-%S", "%Y-%m-%d_%H-%M", "%Y-%m-%d_%H"];
        for pattern in PATTERNS {
            if let Ok(naive) = NaiveDateTime::parse_from_str(suffix, pattern) {
                return self.naive_to_timestamp(naive);
            }
        }
        let date = NaiveDate::parse_from_str(suffix, "%Y-%m-%d").ok()?;
        self.naive_to_timestamp(date.and_hms_opt(0, 0, 0)?)
    }

    /// 把本地/UTC `NaiveDateTime` 转 UNIX 秒.
    ///
    /// 参数：`naive` 无时区时间；返回值：时间戳或 None（本地时间不存在）.
    fn naive_to_timestamp(&self, naive: NaiveDateTime) -> Option<f64> {
        if self.utc {
            return Some(naive.and_utc().timestamp() as f64);
        }
        match Local.from_local_datetime(&naive) {
            LocalResult::Single(dt) => Some(dt.timestamp() as f64),
            LocalResult::Ambiguous(earliest, _) => Some(earliest.timestamp() as f64),
            LocalResult::None => None,
        }
    }

    /// 返回时间轮转归档列表.
    ///
    /// 参数：`file_path` 活动文件；返回值：匹配任意受支持时间戳格式的归档路径.
    pub fn archives(&self, file_path: &Path) -> Vec<PathBuf> {
        let (directory, base) = split_path(file_path);
        let Some(directory) = directory else {
            return Vec::new();
        };
        let prefix = format!("{base}.");
        let mut found = Vec::new();
        let Ok(entries) = fs::read_dir(directory) else {
            return found;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(suffix) = name.strip_prefix(&prefix) else {
                continue;
            };
            let (stamp, _) = strip_suffix(suffix);
            if self.parse_any_stamp(stamp).is_some() {
                found.push(path);
            }
        }
        found
    }

    /// 计算应删除的过期归档.
    ///
    /// 参数：`_file_path` 活动文件（未用）；`existing` 现有归档；返回值：超过 `keep` 的最旧文件.
    pub fn stale(&self, _file_path: &Path, existing: &[PathBuf]) -> Vec<PathBuf> {
        let (_, base) = split_path(_file_path);
        let prefix = format!("{base}.");
        let mut stamped: Vec<(f64, PathBuf)> = existing
            .iter()
            .filter_map(|path| {
                let name = path.file_name()?.to_str()?;
                let suffix = name.strip_prefix(&prefix)?;
                let (stamp, _) = strip_suffix(suffix);
                Some((self.parse_any_stamp(stamp)?, path.clone()))
            })
            .collect();
        stamped.sort_by(|left, right| {
            right
                .0
                .partial_cmp(&left.0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        stamped
            .into_iter()
            .skip(self.keep)
            .map(|(_, path)| path)
            .collect()
    }
}

/// 返回本地时区偏移秒数；参数 `timestamp`，无法解析时 0.
fn local_offset_seconds(timestamp: f64) -> i64 {
    let seconds = timestamp.floor() as i64;
    Local
        .timestamp_opt(seconds, 0)
        .single()
        .map(|dt| i64::from(dt.offset().fix().local_minus_utc()))
        .unwrap_or(0)
}

/// 返回当前 UNIX 秒；参数无.
fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

/// 拆分路径为父目录与文件名.
///
/// 参数：`path`；返回值：`(parent, file_name)`；无文件名时返回空串.
fn split_path(path: &Path) -> (Option<&Path>, String) {
    (
        path.parent(),
        path.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default(),
    )
}

/// 追加点号后缀；参数 `path`/`suffix`，返回新路径.
fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    name.push(".");
    name.push(suffix);
    path.with_file_name(name)
}

/// 追加数字后缀；参数 `path`/`index`，返回 `.N` 路径.
fn with_index(path: &Path, index: u64) -> PathBuf {
    with_suffix(path, &index.to_string())
}

/// 计算归档改名目标并保留压缩后缀.
///
/// 参数：`source` 原归档；`file_path` 活动文件；`index` 新编号.
/// 返回值：`app.log.1.zst` -> `app.log.2.zst` 形式路径.
fn with_index_like(source: &Path, file_path: &Path, index: u64) -> PathBuf {
    let target = with_index(file_path, index);
    match source
        .file_name()
        .and_then(|name| name.to_str())
        .map(strip_suffix)
        .and_then(|(_, codec)| codec)
    {
        Some(codec) => with_extension(&target, codec),
        None => target,
    }
}

/// 大小轮转配置.
///
/// 字段：`max_bytes` 单文件阈值；`keep` 保留归档数.
/// 达到阈值后活动文件切到 `.1`，旧归档后移.
#[derive(Clone, Debug)]
pub struct SizeRotation {
    /// 单文件最大字节数.
    pub max_bytes: u64,
    /// 保留归档数量.
    pub keep: usize,
}

impl SizeRotation {
    /// 判断是否达到大小阈值.
    ///
    /// 参数：`size` 当前逻辑大小；`_now` 忽略；返回值：`size >= max_bytes`.
    pub fn should_rollover(&self, size: u64, _now: f64) -> bool {
        size >= self.max_bytes
    }

    /// 生成大小轮转的改名步骤.
    ///
    /// 参数：`file_path` 活动文件；返回值：按编号从大到小的 `(源, 目标)`，最后一步是活动文件 →
    /// `.1`.
    pub fn archive_steps(&self, file_path: &Path) -> Vec<(PathBuf, PathBuf)> {
        let mut steps = Vec::new();
        let mut archives = self.archives(file_path);
        archives.sort_by_key(|path| self.index_of(path).unwrap_or(0));
        for archive in archives.into_iter().rev() {
            if let Some(number) = self.index_of(&archive) {
                // 目标基于原始文件名构造：`<file>.<n+1>`，不能变成 `<file>.<n>.<n+1>`；
                // 压缩归档保留自己的后缀.
                steps.push((
                    archive.clone(),
                    with_index_like(&archive, file_path, number + 1),
                ));
            }
        }
        steps.push((file_path.to_path_buf(), with_index(file_path, 1)));
        steps
    }

    /// 返回大小轮转归档列表.
    ///
    /// 参数：`file_path` 活动文件；返回值：`.N` 与历史 `.N.1` 形式的归档.
    pub fn archives(&self, file_path: &Path) -> Vec<PathBuf> {
        self.scan(file_path, false)
    }

    /// 扫描历史 `.N.1` 旧格式归档；参数 `file_path` 活动文件.
    fn legacy(&self, file_path: &Path) -> Vec<PathBuf> {
        self.scan(file_path, true)
    }

    /// 扫描同目录下匹配的归档.
    ///
    /// 参数：`file_path` 活动文件；`legacy` 是否匹配旧 `.N.1` 格式；返回值：归档路径列表.
    fn scan(&self, file_path: &Path, legacy: bool) -> Vec<PathBuf> {
        let (directory, base) = split_path(file_path);
        let Some(directory) = directory else {
            return Vec::new();
        };
        let prefix = format!("{base}.");
        let mut found = Vec::new();
        let Ok(entries) = fs::read_dir(directory) else {
            return found;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(rest) = name.strip_prefix(&prefix) else {
                continue;
            };
            let matches = if legacy {
                is_legacy_suffix(rest)
            } else {
                is_size_suffix(rest)
            };
            if matches {
                found.push(path);
            }
        }
        found
    }

    /// 计算大小轮转过期归档.
    ///
    /// 参数：`file_path` 活动文件；`existing` 现有归档；返回值：按编号/旧格式排序后应删除的文件.
    pub fn stale(&self, file_path: &Path, existing: &[PathBuf]) -> Vec<PathBuf> {
        let mut valid: Vec<(u64, PathBuf)> = existing
            .iter()
            .filter_map(|path| Some((self.index_of(path)?, path.clone())))
            .collect();
        valid.sort_by_key(|(number, _)| *number);
        let survivors = valid.len().min(self.keep);
        let mut stale: Vec<PathBuf> = valid
            .into_iter()
            .skip(self.keep)
            .map(|(_, path)| path)
            .collect();
        let slots = self.keep.saturating_sub(survivors);
        let mut legacy = self.legacy(file_path);
        legacy.sort_by(|left, right| {
            mtime(right)
                .partial_cmp(&mtime(left))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        stale.extend(legacy.into_iter().skip(slots));
        stale
    }

    /// 解析 `.N` 归档编号.
    ///
    /// 参数：`path` 归档路径；返回值：编号或 None（非数字/无后缀）.
    pub fn index_of(&self, path: &Path) -> Option<u64> {
        let name = path.file_name()?.to_str()?;
        let (name, _) = strip_suffix(name);
        let (_, rest) = name.rsplit_once('.')?;
        if rest.is_empty() || !rest.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        rest.parse().ok()
    }
}

/// 判断后缀是否为大小轮转编号（允许压缩后缀）；参数 `rest` 是文件名点号后的部分.
fn is_size_suffix(rest: &str) -> bool {
    let (rest, _) = strip_suffix(rest);
    !rest.is_empty() && rest.bytes().all(|byte| byte.is_ascii_digit())
}

/// 判断后缀是否为旧 `.N.1` 格式；参数 `rest` 是文件名点号后的部分.
fn is_legacy_suffix(rest: &str) -> bool {
    let (rest, _) = strip_suffix(rest);
    let Some(number) = rest.strip_suffix(".1") else {
        return false;
    };
    !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
}

/// 读取文件 mtime（UNIX 秒）；参数 `path`，失败返回 0.
fn mtime(path: &Path) -> f64 {
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

/// 组合轮转本次由谁触发.
///
/// `Time` 表示时间边界触发；`Size` 表示大小阈值触发.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// 时间触发.
    Time,
    /// 大小触发.
    Size,
}

/// 轮转策略组合.
///
/// `Composite` 的 `triggered` 必须由 `should_rollover` 写入：`archive_steps` 依据它决定用时间戳命名
/// 还是数字序号命名，二者不能混用，否则归档清理会认错文件.
#[derive(Clone, Debug)]
pub enum Rotation {
    /// 纯时间轮转.
    Time(TimeRotation),
    /// 纯大小轮转.
    Size(SizeRotation),
    /// 时间 + 大小组合.
    Composite {
        /// 时间子策略.
        time: Option<TimeRotation>,
        /// 大小子策略.
        size: Option<SizeRotation>,
        /// 本次实际触发者；决定归档命名格式.
        triggered: Option<Trigger>,
    },
}

impl Rotation {
    /// 判断本批记录是否触发轮转，并记录组合策略的触发者.
    ///
    /// 参数：`size` 当前逻辑大小；`now` 当前 UNIX 秒；返回值：是否轮转.
    /// 组合策略优先判断时间，再判断大小，并写入 `triggered`.
    pub fn should_rollover(&mut self, size: u64, now: f64) -> bool {
        match self {
            Self::Time(rotation) => rotation.should_rollover(size, now),
            Self::Size(rotation) => rotation.should_rollover(size, now),
            Self::Composite {
                time,
                size: size_rotation,
                triggered,
            } => {
                if let Some(rotation) = time.as_ref() {
                    if rotation.should_rollover(size, now) {
                        *triggered = Some(Trigger::Time);
                        return true;
                    }
                }
                if let Some(rotation) = size_rotation.as_ref() {
                    if rotation.should_rollover(size, now) {
                        *triggered = Some(Trigger::Size);
                        return true;
                    }
                }
                *triggered = None;
                false
            }
        }
    }

    /// 首次打开时锚定时间子策略.
    ///
    /// 参数：`started_at` 是文件 mtime；返回值：无.
    /// 大小/组合策略只把时间转发给子策略.
    pub fn on_open(&mut self, started_at: f64) {
        match self {
            Self::Time(rotation) => rotation.on_open(started_at),
            Self::Size(_) => {}
            Self::Composite { time, .. } => {
                if let Some(rotation) = time {
                    rotation.on_open(started_at);
                }
            }
        }
    }

    /// 生成归档改名步骤.
    ///
    /// 参数：`file_path` 活动文件；返回值：`(源, 目标)` 列表，按策略分派.
    pub fn archive_steps(&self, file_path: &Path) -> Vec<(PathBuf, PathBuf)> {
        match self {
            Self::Time(rotation) => rotation.archive_steps(file_path),
            Self::Size(rotation) => rotation.archive_steps(file_path),
            Self::Composite {
                time,
                size,
                triggered,
            } => match triggered {
                Some(Trigger::Size) => size
                    .as_ref()
                    .map(|rotation| rotation.archive_steps(file_path))
                    .unwrap_or_default(),
                _ => time
                    .as_ref()
                    .map(|rotation| rotation.archive_steps(file_path))
                    .unwrap_or_else(|| {
                        size.as_ref()
                            .map(|rotation| rotation.archive_steps(file_path))
                            .unwrap_or_default()
                    }),
            },
        }
    }

    /// 返回 true 表示 `archive_steps` 会移动已有归档.
    /// 只有这种移动才需要等待在途压缩；纯时间轮转每次写新的时间戳名，不需要等.
    /// 是否移动已有归档.
    ///
    /// 参数：无；返回值：大小/组合-大小触发为 true，纯时间轮转为 false.
    /// 只有移动已有归档时才需要等待在途压缩，避免 rename 竞争.
    pub fn shifts_archives(&self) -> bool {
        match self {
            Self::Time(_) => false,
            Self::Size(_) => true,
            Self::Composite {
                time,
                size,
                triggered,
            } => match triggered {
                Some(Trigger::Size) => true,
                Some(Trigger::Time) => false,
                // triggered 为空时 archive_steps 会优先用时间策略命名.
                None => time.is_none() && size.is_some(),
            },
        }
    }

    /// 计算组合策略过期归档并去重.
    ///
    /// 参数：`file_path` 活动文件；`existing` 现有归档；返回值：应删除文件.
    pub fn stale(&self, file_path: &Path, existing: &[PathBuf]) -> Vec<PathBuf> {
        let mut stale = match self {
            Self::Time(rotation) => rotation.stale(file_path, existing),
            Self::Size(rotation) => rotation.stale(file_path, existing),
            Self::Composite { time, size, .. } => {
                let mut stale = Vec::new();
                if let Some(rotation) = time {
                    stale.extend(rotation.stale(file_path, existing));
                }
                if let Some(rotation) = size {
                    stale.extend(rotation.stale(file_path, existing));
                }
                stale
            }
        };
        // 组合策略下时间/大小两个子策略可能对同一路径都给出过期判断；去重后避免
        // 同一文件被删除两次（第二次必然 NotFound）和重复告警.
        stale.sort();
        stale.dedup();
        stale
    }

    /// 返回当前策略的归档列表.
    ///
    /// 参数：`file_path` 活动文件；返回值：按策略分派到时间/大小/组合实现.
    pub fn archives(&self, file_path: &Path) -> Vec<PathBuf> {
        match self {
            Self::Time(rotation) => rotation.archives(file_path),
            Self::Size(rotation) => rotation.archives(file_path),
            Self::Composite { time, size, .. } => {
                let mut found = Vec::new();
                if let Some(rotation) = time {
                    for path in rotation.archives(file_path) {
                        if !found.contains(&path) {
                            found.push(path);
                        }
                    }
                }
                if let Some(rotation) = size {
                    for path in rotation.archives(file_path) {
                        if !found.contains(&path) {
                            found.push(path);
                        }
                    }
                }
                found
            }
        }
    }

    /// 推进时间子策略.
    ///
    /// 参数：`now` 当前 UNIX 秒；返回值：无.
    /// 大小触发时不推进时间表.
    pub fn advance(&mut self, now: f64) {
        match self {
            Self::Time(rotation) => rotation.advance(now),
            Self::Size(_) => {}
            Self::Composite {
                time, triggered, ..
            } => {
                // 只有时间策略自己触发时才推进时间表；大小触发只影响文件编号，
                // 不能顺带重排时间边界（否则组合轮转的时间轮转会一直被后推）.
                if !matches!(triggered, Some(Trigger::Size)) {
                    if let Some(rotation) = time {
                        rotation.advance(now);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证大小轮转在时间边界前触发不会推后计划；参数无.
    #[test]
    fn time_advance_before_boundary_keeps_schedule() {
        let now = 1_700_000_000.0_f64;
        let mut rotation = TimeRotation::new("D", 1, None, true, 7, Some(now)).unwrap();
        let planned = rotation.rollover_at;
        // 模拟大小轮转在时间边界之前触发：不能把下一次时间轮转推后.
        rotation.advance(now + 3_600.0);
        assert_eq!(rotation.rollover_at, planned);
    }

    /// 验证 MIDNIGHT 归档使用前一个本地日；参数无.
    #[test]
    fn midnight_archive_uses_previous_local_day() {
        use chrono::Local;
        let today = Local::now().date_naive();
        let midnight = today.and_hms_opt(0, 0, 0).unwrap();
        let started_at = Local
            .from_local_datetime(&midnight)
            .single()
            .unwrap()
            .timestamp() as f64;
        let rotation = TimeRotation::new("MIDNIGHT", 1, None, false, 7, Some(started_at)).unwrap();
        let steps = rotation.archive_steps(Path::new("app.log"));
        let expected = (today - Days::new(1)).format("%Y-%m-%d").to_string();
        assert_eq!(
            steps[0].1,
            std::path::PathBuf::from(format!("app.log.{expected}"))
        );
    }

    /// 验证组合策略中大小触发不会推迟时间轮转；参数无.
    #[test]
    fn composite_size_trigger_does_not_postpone_time_rotation() {
        let now = 1_700_000_000.0_f64;
        let time = TimeRotation::new("D", 1, None, true, 7, Some(now)).unwrap();
        let planned = time.rollover_at;
        let mut rotation = Rotation::Composite {
            time: Some(time),
            size: Some(SizeRotation {
                max_bytes: 1024,
                keep: 7,
            }),
            triggered: Some(Trigger::Size),
        };
        rotation.advance(now + 3_600.0);
        match &rotation {
            Rotation::Composite {
                time: Some(time), ..
            } => assert_eq!(time.rollover_at, planned),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod clock_tests {
    use super::*;

    /// 验证进程内观察时间不会因系统时钟回拨而倒退.
    #[test]
    fn effective_time_never_moves_backwards() {
        let rotation = TimeRotation::new("S", 1, None, true, 7, Some(100.0)).unwrap();
        assert_eq!(rotation.effective_now(100.0), 100.0);
        assert_eq!(rotation.effective_now(80.0), 100.0);
        assert_eq!(rotation.effective_now(101.0), 101.0);
    }
}
