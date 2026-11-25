//! runtime 的 writer 线程实现.
//!
//! 本模块只负责：
//! - 把队列消息 drain 成批；
//! - 按 sink 级别区间分发；
//! - flush/close 屏障；
//! - Stop/关停收尾.
//!
//! 生产者、配置和公开句柄在 `runtime/mod.rs`；这里不启动线程，也不决定何时关停，只执行
//! [`run_worker`] 的循环.

use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};

use super::{DropReason, Shared, Sink, BATCH_BYTES_MAX, BATCH_MAX, WRITER_ID};
use crate::engine::level::Level;
use crate::engine::record::Record;

// Record 直接内联在 Msg 里：逐条装箱会给生产者热路径增加一次堆分配.控制消息很少，
// 枚举只在两个线程间移动，大枚举带来的栈拷贝成本低于每记录一次 malloc.
//
// `cost` 由生产者在入队前计算一次并随消息携带；writer 不再重新扫描字段计算大小.
// 记录入队后不可变，因此这个值在整个生命周期内保持精确.
#[allow(clippy::large_enum_variant)]
pub(super) enum Msg {
    /// 单条记录.`record` 拥有最终文本，`cost` 是生产者已计算的预算成本，writer 不再扫描字段.
    Record { record: Record, cost: usize },
    /// 生产者批量入队；一次 channel 操作最多携带 `BATCH_MAX` 条，`cost` 是本批累计估算字节数.
    Batch { records: Vec<Record>, cost: usize },
    /// 预格式化多行块；一次 channel 操作携带整块文本，`lines` 是逻辑记录数.
    PreformattedBlock {
        /// 块内所有行的统一级别.
        level: Level,
        /// 以 `\n` 分隔的最终文本.
        block: String,
        /// `block.lines().count()`，生产者计算一次.
        lines: usize,
        /// 队列预算成本（字节 + 行开销）.
        cost: usize,
    },
    /// flush 屏障：writer 处理完此消息之前的记录后 flush 所有 sink，并通过 ack 返回结果.
    Flush(Sender<bool>),
    /// reopen 控制：writer 处理完此前记录后重开所有 sink，并通过 ack 返回结果.
    Reopen(Sender<bool>),
    /// 关停哨兵：writer 排空队列、flush/close 全部 sink 后通过 ack 返回最终结果.
    Stop(Sender<bool>),
    /// 由 `set_sinks_with_close_removed` 投递：在当前批次写完后再 close 被移除的 sink，避免与在途写
    /// 并发.
    CloseSink(Arc<dyn Sink>),
}

/// writer 线程复用的批处理临时缓冲.
///
/// 每批最多 512 条记录；如果每批都新建状态数组/索引数组，100 万条/秒时会产生数千次小分配.
/// 这里按批清空复用，容量只增不减.`selected` 仍按批创建，因为 `&Record` 的生命周期绑定当前批次，不
/// 能安全跨批缓存.
///
/// 字段：`outcomes` 与批次一一对应，记录 Pending/Delivered/Dropped；`selected_indices` 保存当前sink
/// 命中的批次下标.
pub(super) struct WriteScratch {
    outcomes: Vec<Outcome>,
    selected_indices: Vec<usize>,
}

/// 单条记录在本批次内的投递状态.
///
/// `Pending` 表示还没有匹配 sink 处理；`Delivered` 表示至少一个 sink 成功；`Dropped` 保存第一次失败
/// 原因，后续 sink 失败不重复计数.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// 尚未被任何匹配 sink 处理.
    Pending,
    /// 至少一个 sink 成功.
    Delivered,
    /// 所有匹配 sink 都失败，保存第一次失败原因.
    Dropped(DropReason),
}

impl WriteScratch {
    /// 创建空的复用缓冲；参数无.
    pub(super) fn new() -> Self {
        Self {
            outcomes: Vec::new(),
            selected_indices: Vec::new(),
        }
    }

    /// 为一批记录重置状态.
    ///
    /// 参数：`len` 是本批记录数.
    /// 返回值：无.`outcomes` 扩到 len 并全部设为 Pending；`selected_indices` 清空但保留容量.
    fn reset(&mut self, len: usize) {
        self.outcomes.clear();
        self.outcomes.resize(len, Outcome::Pending);
        self.selected_indices.clear();
    }
}

/// 把一批记录分发给所有 sink.
///
/// 参数：`shared` 是 logger 共享状态；`batch` 是本批连续记录；`scratch` 是复用的投递状态.
/// 返回值：无.
/// 每个 sink 只收到满足级别区间的记录；sink 错误/panic 被隔离，writer 继续处理后续 sink，最终按“是
/// 否至少一个 sink 成功”更新 delivered/dropped 计数.
fn write_batch(shared: &Shared, batch: &[Record], cost: usize, scratch: &mut WriteScratch) {
    if batch.is_empty() {
        return;
    }
    let sinks = shared.sinks.load_full();
    if sinks.is_empty() {
        shared.drop_after_enqueue(DropReason::NoSink, batch.len());
        shared.release_records(batch.len(), cost);
        return;
    }
    // 按记录而不是按 sink 跟踪投递.一条记录只要被至少一个 sink 写出就不算 dropped，
    // 即使另一个 sink 失败；所有匹配 sink 都失败的记录只按第一个失败原因计一次.
    scratch.reset(batch.len());
    // `selected` 借用当前批次，不能跨批缓存；只有 sink 需要过滤部分批次时才分配.
    let mut selected: Vec<&Record> = Vec::new();
    for config in sinks.iter() {
        let all_match = batch.iter().all(|record| config.matches(record.level));
        let result = if all_match {
            panic::catch_unwind(AssertUnwindSafe(|| config.sink.write_batch_all(batch)))
        } else {
            scratch.selected_indices.clear();
            selected.clear();
            if selected.capacity() == 0 {
                selected.reserve(batch.len());
            }
            for (index, record) in batch.iter().enumerate() {
                if config.matches(record.level) {
                    scratch.selected_indices.push(index);
                    selected.push(record);
                }
            }
            if selected.is_empty() {
                continue;
            }
            panic::catch_unwind(AssertUnwindSafe(|| config.sink.write_batch(&selected)))
        };
        let written = if all_match {
            batch.len()
        } else {
            selected.len()
        };
        let failure = match result {
            Ok(Ok(())) => {
                config.writes.fetch_add(written as u64, Ordering::Relaxed);
                if all_match {
                    scratch.outcomes.fill(Outcome::Delivered);
                } else {
                    for &index in &scratch.selected_indices {
                        scratch.outcomes[index] = Outcome::Delivered;
                    }
                }
                None
            }
            Ok(Err(error)) => {
                config.errors.fetch_add(1, Ordering::Relaxed);
                shared.write_errors.add(1);
                let _ = error;
                Some(DropReason::WriteError)
            }
            Err(_) => {
                config.panics.fetch_add(1, Ordering::Relaxed);
                shared.sink_panics.add(1);
                Some(DropReason::Panic)
            }
        };
        if let Some(failure) = failure {
            if all_match {
                for outcome in &mut scratch.outcomes {
                    if *outcome == Outcome::Pending {
                        *outcome = Outcome::Dropped(failure);
                    }
                }
            } else {
                for &index in &scratch.selected_indices {
                    // 保留第一个失败原因；后续 sink 失败不能重复计数.
                    if scratch.outcomes[index] == Outcome::Pending {
                        scratch.outcomes[index] = Outcome::Dropped(failure);
                    }
                }
            }
        }
    }
    let mut delivered_count = 0usize;
    let mut write_errors = 0usize;
    let mut panics = 0usize;
    let mut no_sink = 0usize;
    for outcome in scratch.outcomes.iter().copied() {
        match outcome {
            Outcome::Delivered => delivered_count += 1,
            Outcome::Dropped(DropReason::WriteError) => write_errors += 1,
            Outcome::Dropped(DropReason::Panic) => panics += 1,
            // Pending 表示没有任何匹配 sink 处理过该记录.
            Outcome::Pending | Outcome::Dropped(_) => no_sink += 1,
        }
    }
    shared.record_delivered(delivered_count);
    shared.drop_after_enqueue(DropReason::WriteError, write_errors);
    shared.drop_after_enqueue(DropReason::Panic, panics);
    shared.drop_after_enqueue(DropReason::NoSink, no_sink);
    // 队列预算覆盖从入队到 writer 尝试完所有 sink 的整个区间.只在 fan-out 之后释放，
    // 保证 I/O 期间内存仍然有界.
    shared.release_records(batch.len(), cost);
}

/// 把预格式化块分发给所有匹配级别的 sink.
///
/// 参数：`shared` 是 logger 状态；`level` 是统一级别；`block` 是整块文本；`lines` 是逻辑行数；
/// `cost` 是生产者预留的预算.
/// 返回值：无.
/// 语义：整块要么至少被一个 sink 成功接收（全部行计 delivered），要么整块计一次丢弃原因；
/// 与逐条记录的多 sink 语义一致，不会因为第二个 sink 失败重复计数.
fn write_preformatted_block(shared: &Shared, level: Level, block: &str, lines: usize, cost: usize) {
    let sinks = shared.sinks.load_full();
    if sinks.is_empty() {
        shared.drop_after_enqueue(DropReason::NoSink, lines);
        shared.release_records(lines, cost);
        return;
    }
    let mut matched = false;
    let mut delivered = false;
    let mut failed = false;
    let mut panicked = false;
    for config in sinks.iter() {
        if !config.matches(level) {
            continue;
        }
        matched = true;
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            config.sink.write_preformatted_block(level, block)
        }));
        match result {
            Ok(Ok(())) => {
                delivered = true;
                config.writes.fetch_add(lines as u64, Ordering::Relaxed);
            }
            Ok(Err(_)) => {
                config.errors.fetch_add(1, Ordering::Relaxed);
                shared.write_errors.add(1);
                failed = true;
            }
            Err(_) => {
                config.errors.fetch_add(1, Ordering::Relaxed);
                config.panics.fetch_add(1, Ordering::Relaxed);
                shared.write_errors.add(1);
                shared.sink_panics.add(1);
                panicked = true;
            }
        }
    }
    if delivered {
        shared.record_delivered(lines);
    } else if !matched {
        shared.drop_after_enqueue(DropReason::NoSink, lines);
    } else if panicked {
        shared.drop_after_enqueue(DropReason::Panic, lines);
    } else if failed {
        shared.drop_after_enqueue(DropReason::WriteError, lines);
    } else {
        shared.drop_after_enqueue(DropReason::NoSink, lines);
    }
    shared.release_records(lines, cost);
}

/// 依次 flush 所有 sink；一个失败不阻止后面的 sink.
///
/// 参数：`shared` 提供 sink 表与计数.
/// 返回值：全部成功为 true；任一 sink 返回错误或 panic 为 false.flush 中的 panic 也计入
/// `sink_panics`，与 `write_batch` 统计口径一致.
fn flush_sinks(shared: &Shared) -> bool {
    let sinks = shared.sinks.load_full();
    let mut ok = true;
    for config in sinks.iter() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| config.sink.flush()));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                config.errors.fetch_add(1, Ordering::Relaxed);
                shared.write_errors.add(1);
                ok = false;
            }
            Err(_) => {
                config.errors.fetch_add(1, Ordering::Relaxed);
                config.panics.fetch_add(1, Ordering::Relaxed);
                shared.write_errors.add(1);
                shared.sink_panics.add(1);
                ok = false;
            }
        }
    }
    ok
}

/// 重开所有 sink.
///
/// 参数：`shared` 提供 sink 表与计数.
/// 返回值：全部成功为 true.
/// 用于外部 logrotate；错误/panic 被隔离并计错，但不阻止后续 sink 重开.
fn reopen_sinks(shared: &Shared) -> bool {
    let sinks = shared.sinks.load_full();
    let mut ok = true;
    for config in sinks.iter() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| config.sink.reopen()));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                config.errors.fetch_add(1, Ordering::Relaxed);
                shared.write_errors.add(1);
                ok = false;
            }
            Err(_) => {
                config.errors.fetch_add(1, Ordering::Relaxed);
                config.panics.fetch_add(1, Ordering::Relaxed);
                shared.write_errors.add(1);
                shared.sink_panics.add(1);
                ok = false;
            }
        }
    }
    ok
}

/// 依次 close 所有 sink.
///
/// 参数：`shared` 提供 sink 表与计数.
/// 返回值：全部成功为 true.close 必须幂等：writer 退出、显式 close、重复 close 都可能走到这里；错误
/// /panic 被隔离，不会阻止其他 sink 收尾.
fn close_sinks(shared: &Shared) -> bool {
    let sinks = shared.sinks.load_full();
    let mut ok = true;
    for config in sinks.iter() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| config.sink.close()));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                config.errors.fetch_add(1, Ordering::Relaxed);
                shared.write_errors.add(1);
                ok = false;
            }
            Err(_) => {
                config.errors.fetch_add(1, Ordering::Relaxed);
                config.panics.fetch_add(1, Ordering::Relaxed);
                shared.write_errors.add(1);
                shared.sink_panics.add(1);
                ok = false;
            }
        }
    }
    ok
}

/// 尽量排空队列直到空/断开，期间处理 Flush/Reopen/Stop/CloseSink 控制消息.
///
/// 参数：`shared` 是 logger 状态；`rx` 是 writer 接收端；`defer_stop` 表示当前是 stop_requested 触
/// 发的自动收尾.
/// 返回值：如果收尾期间收到 Stop ack，返回它以便在 flush/close 完成后回真实结果；否则
/// None.`defer_stop=true` 时不会立刻回 Stop，避免调用方在最终收尾前拿到假结果.
fn drain_remaining(shared: &Shared, rx: &Receiver<Msg>, defer_stop: bool) -> Option<Sender<bool>> {
    let mut batch = Vec::with_capacity(BATCH_MAX);
    let mut scratch = WriteScratch::new();
    let mut bytes = 0usize;
    let mut pending_stop = None;
    loop {
        match rx.try_recv() {
            Ok(Msg::Record { record, cost }) => {
                if !batch.is_empty()
                    && (batch.len() >= BATCH_MAX || bytes.saturating_add(cost) > BATCH_BYTES_MAX)
                {
                    write_batch(shared, &batch, bytes, &mut scratch);
                    batch.clear();
                    bytes = 0;
                }
                bytes = bytes.saturating_add(cost);
                batch.push(record);
            }
            Ok(Msg::Batch {
                mut records,
                cost: records_bytes,
            }) => {
                if !batch.is_empty()
                    && (batch.len().saturating_add(records.len()) > BATCH_MAX
                        || bytes.saturating_add(records_bytes) > BATCH_BYTES_MAX)
                {
                    write_batch(shared, &batch, bytes, &mut scratch);
                    batch.clear();
                    bytes = 0;
                }
                bytes = bytes.saturating_add(records_bytes);
                batch.append(&mut records);
                shared.recycle_batch(records);
                if batch.len() >= BATCH_MAX || bytes >= BATCH_BYTES_MAX {
                    write_batch(shared, &batch, bytes, &mut scratch);
                    batch.clear();
                    bytes = 0;
                }
            }
            Ok(Msg::PreformattedBlock {
                level,
                block,
                lines,
                cost,
            }) => {
                if !batch.is_empty() {
                    write_batch(shared, &batch, bytes, &mut scratch);
                    batch.clear();
                    bytes = 0;
                }
                write_preformatted_block(shared, level, &block, lines, cost);
            }
            Ok(Msg::Flush(ack)) => {
                if !batch.is_empty() {
                    write_batch(shared, &batch, bytes, &mut scratch);
                    batch.clear();
                    bytes = 0;
                }
                let _ = ack.send(flush_sinks(shared));
            }
            Ok(Msg::Reopen(ack)) => {
                if !batch.is_empty() {
                    write_batch(shared, &batch, bytes, &mut scratch);
                    batch.clear();
                    bytes = 0;
                }
                let _ = ack.send(reopen_sinks(shared));
            }
            Ok(Msg::Stop(ack)) => {
                if defer_stop && pending_stop.is_none() {
                    // 自动收尾路径：先保存 ack，等 flush/close 完成后回真实结果.
                    pending_stop = Some(ack);
                } else {
                    // 正常 Stop 分支已消费首条；这里只会是重复哨兵.
                    let _ = ack.send(false);
                }
            }
            Ok(Msg::CloseSink(sink)) => {
                // 先把已攒的记录写完，再 close 被移除的 sink，保证 close 之后不会再写它.
                if !batch.is_empty() {
                    write_batch(shared, &batch, bytes, &mut scratch);
                    batch.clear();
                    bytes = 0;
                }
                let _ = panic::catch_unwind(AssertUnwindSafe(|| sink.close()));
            }
            Err(_) => break,
        }
    }
    if !batch.is_empty() {
        write_batch(shared, &batch, bytes, &mut scratch);
    }
    pending_stop
}

/// 队列已经不会再产生新记录时执行最终收尾.
///
/// 参数：`shared` 是 logger 状态；`rx` 用于排空剩余消息；`defer_stop` 同 [`drain_remaining`].
/// 返回值：flush 与 close 是否都成功.
/// 设计原因：`flush_sinks() && close_sinks()` 会在 flush 失败时短路跳过 close，导致文件句柄/压缩线
/// 程泄漏；这里必须先执行两者再合并结果.
fn drain_and_finish(shared: &Shared, rx: &Receiver<Msg>, defer_stop: bool) -> bool {
    shared.closed.store(true, Ordering::Release);
    let stop_ack = drain_remaining(shared, rx, defer_stop);
    let flushed = flush_sinks(shared);
    let closed = close_sinks(shared);
    let ok = flushed && closed;
    // `close()` 在 Stop 投不进满队列时轮询 `alive`，看到 false 后读取 `finished_ok`.
    // 必须先发布结果再发布 alive：release-store alive 会把它之前的所有写入一起发布，
    // 反过来（旧顺序）会让轮询方读到初始化时的 true，把失败的关停误报为成功.
    shared.finished_ok.store(ok, Ordering::Release);
    shared.alive.store(false, Ordering::Release);
    if let Some(ack) = stop_ack {
        let _ = ack.send(ok);
    }
    ok
}

/// writer 线程入口.
///
/// 参数：`shared` 是 logger 共享状态；`rx` 是队列接收端.
/// 返回值：无.
/// 线程本地写入当前 logger id，供 `Logger::flush/close` 识别“自己的 writer”；内部 panic 必须收敛
/// alive/closed 并尽量 close sink，不能留下假健康状态.
pub(super) fn run_worker(shared: Arc<Shared>, rx: Receiver<Msg>) {
    WRITER_ID.with(|flag| flag.set(Some(shared.id)));
    // writer 内部 panic 不能留下 alive=true 的假状态；同时尽量把已打开的 sink 收尾，
    // 否则 Logger 仍被其他线程持有时，文件句柄和压缩线程会一直存活.
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        run_worker_inner(Arc::clone(&shared), &rx)
    }));
    if result.is_err() {
        shared.closed.store(true, Ordering::Release);
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            let flushed = flush_sinks(&shared);
            let closed = close_sinks(&shared);
            flushed && closed
        }));
        // writer 已死，队列里剩余记录不会再被写.必须显式计数，否则 close 返回失败时
        // 监控只看得到 dropped=0；迟到的 Flush/Stop ack 回 false，避免调用方等满超时.
        let mut lost = 0usize;
        loop {
            match rx.try_recv() {
                Ok(Msg::Record { .. }) => lost += 1,
                Ok(Msg::Batch { records, .. }) => lost += records.len(),
                Ok(Msg::PreformattedBlock { lines, .. }) => lost += lines,
                Ok(Msg::Flush(ack)) => {
                    let _ = ack.send(false);
                }
                Ok(Msg::Reopen(ack)) => {
                    let _ = ack.send(false);
                }
                Ok(Msg::Stop(ack)) => {
                    let _ = ack.send(false);
                }
                Ok(Msg::CloseSink(sink)) => {
                    let _ = panic::catch_unwind(AssertUnwindSafe(|| sink.close()));
                }
                Err(_) => break,
            }
        }
        shared.drop_after_enqueue(DropReason::WorkerDead, lost);
        // 即使 sink 收尾成功，writer panic 也意味着队列中可能已有记录未写；close 必须
        // 报告失败，不能把不完整关停伪装成成功.
        shared.finished_ok.store(false, Ordering::Release);
        shared.alive.store(false, Ordering::Release);
    }
    WRITER_ID.with(|flag| flag.set(None));
}

/// 主消费循环.
///
/// 参数：`shared` 是 logger 状态；`rx` 是队列接收端.
/// 返回值：无.
/// 每轮先取一条消息，再非阻塞 drain 到 BATCH_MAX/BATCH_BYTES_MAX 或遇到控制消息；控制消息在前面的记
/// 录之后处理，保证 Flush/Stop 的 FIFO 屏障语义.stop_requested 路径用于“Stop 投不进满队列”的关停：
/// 队列排空后自行 flush/close，不再依赖哨兵唤醒.
fn run_worker_inner(shared: Arc<Shared>, rx: &Receiver<Msg>) {
    // 跨迭代复用批次缓冲和逐记录状态向量.writer 每秒可处理数千批；每批新建 512 条记录
    // 和多个记账向量会在 1M+/s 时形成分配器压力.
    let mut batch = Vec::with_capacity(BATCH_MAX);
    let mut scratch = WriteScratch::new();
    loop {
        // close() 未能把 Stop 投进满队列时，stop_requested 是唯一的关停信号.
        // 队列排空后在这里收尾；队列还有数据时继续按 FIFO 消费，保证已入队记录不丢.
        if shared.stop_requested.load(Ordering::Acquire) && rx.is_empty() {
            let _ = drain_and_finish(&shared, rx, true);
            return;
        }
        let first = match rx.recv() {
            Ok(message) => message,
            Err(_) => break,
        };
        batch.clear();
        let mut batch_bytes = 0usize;
        let mut control = None;
        match first {
            Msg::Record { record, cost } => {
                batch_bytes = cost;
                batch.push(record);
            }
            Msg::Batch { mut records, cost } => {
                batch_bytes = cost;
                batch.append(&mut records);
                shared.recycle_batch(records);
            }
            other => control = Some(other),
        }
        while batch.len() < BATCH_MAX && batch_bytes < BATCH_BYTES_MAX && control.is_none() {
            match rx.try_recv() {
                Ok(Msg::Record { record, cost }) => {
                    batch_bytes = batch_bytes.saturating_add(cost);
                    batch.push(record);
                }
                Ok(Msg::Batch { mut records, cost }) => {
                    // 不允许连续 Batch 消息无界扩大单个写批次；否则队列中塞满 log_many 批次时，
                    // 内存会被放大到 BATCH_MAX×queue_size.
                    if !batch.is_empty()
                        && (batch.len().saturating_add(records.len()) > BATCH_MAX
                            || batch_bytes.saturating_add(cost) > BATCH_BYTES_MAX)
                    {
                        write_batch(&shared, &batch, batch_bytes, &mut scratch);
                        batch.clear();
                        batch_bytes = 0;
                    }
                    batch_bytes = batch_bytes.saturating_add(cost);
                    batch.append(&mut records);
                    shared.recycle_batch(records);
                    if batch.len() >= BATCH_MAX || batch_bytes >= BATCH_BYTES_MAX {
                        write_batch(&shared, &batch, batch_bytes, &mut scratch);
                        batch.clear();
                        batch_bytes = 0;
                    }
                }
                Ok(other) => control = Some(other),
                Err(_) => break,
            }
        }
        if !batch.is_empty() {
            write_batch(&shared, &batch, batch_bytes, &mut scratch);
            // 在阻塞等待下一次 `recv` 前释放记录负载；否则空闲 logger 会无限期保留最多一整批
            // 记录及其消息分配.
            batch.clear();
        }
        if let Some(control) = control {
            match control {
                Msg::Flush(ack) => {
                    let _ = ack.send(flush_sinks(&shared));
                }
                Msg::Reopen(ack) => {
                    let _ = ack.send(reopen_sinks(&shared));
                }
                Msg::Stop(ack) => {
                    let ok = drain_and_finish(&shared, rx, false);
                    let _ = ack.send(ok);
                    return;
                }
                Msg::CloseSink(sink) => {
                    let _ = panic::catch_unwind(AssertUnwindSafe(|| sink.close()));
                }
                Msg::PreformattedBlock {
                    level,
                    block,
                    lines,
                    cost,
                } => {
                    write_preformatted_block(&shared, level, &block, lines, cost);
                }
                Msg::Record { .. } | Msg::Batch { .. } => unreachable!(),
            }
        }
    }
    // 所有 Logger 句柄都已释放：排空、flush、close，不能把缓冲区里的数据丢掉.
    let _ = drain_and_finish(&shared, rx, false);
}
