//! 有界队列日志运行时.
//!
//! 并发模型：
//! 1. 生产者只做级别过滤、格式化，然后入队；不打开文件、不持有 sink 锁.
//! 2. 每个 logger 有且只有一个 writer 线程；sink 的 write/flush/close 只在该线程执行.
//! 3. 队列容量固定.`Overflow::Drop` 直接丢新记录并计数；`Overflow::Block` 最多等待
//!    `timeout`，超时同样丢弃并计数.
//! 4. `flush` 是队列屏障：写入 Flush 控制消息，writer 处理完它之前的记录后 flush sink，
//!    再回 ack.`close` 是关停屏障：拒绝新记录，排空队列，flush 并 close sink.
//! 5. 配置更新（增删 sink、改级别）用原子发布；reader（writer）不拿写锁.
//!
//! 子模块：
//! - `worker`：writer 循环、批量分发、flush/close/Stop 收尾；
//! - 本文件：公开类型、生产者入队、配置更新、计数与健康快照.
//!
//! 关键不变量：
//! - 同一个 logger 内记录顺序与入队顺序一致；
//! - 任何 sink 的 panic 或返回错误都不能让 writer 退出；
//! - 自己不等待自己：writer 内部的 sink 调用同 logger 的 flush/close 会立即返回 false；
//! - `alive` 只在 writer 完成收尾后置 false；`stopped` 只在 close 观测到它之后置 true.

mod worker;

use worker::{run_worker, Msg};

use std::cell::Cell;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// 每个 logger 运行时的唯一 id.writer TLS 保存 id 而不是 bool，这样控制操作能区分“自己的 writer”（
/// 等待会死锁）和“另一个 logger 的 writer”（允许，例如 sink 管理第二个 logger）.
static NEXT_LOGGER_ID: AtomicU64 = AtomicU64::new(1);

use arc_swap::ArcSwap;
use crossbeam_channel::{bounded, RecvTimeoutError, SendTimeoutError, Sender, TrySendError};

use crate::config::RateLimit;
use crate::engine::level::Level;
use crate::engine::record::{FieldValue, Record};

/// 单个 writer 批次最多携带的记录条数.
///
/// 参数/设计：批次越大，channel 与 sink 调用次数越少；但一次 `write_batch` 会同时持有这些记录，过大
/// 会放大内存峰值与单次写延迟.512 是吞吐与延迟的折中.
pub(crate) const BATCH_MAX: usize = 512;
/// 单个 writer 批次的估算字节上限（4 MiB）.
///
/// 设计原因：只按条数限制无法约束超大消息；字节上限保证一次批量写入不会形成过大的临时缓冲，同时仍允
/// 许单条记录接近 [`MAX_RECORD_BYTES`].
const BATCH_BYTES_MAX: usize = 4 * 1024 * 1024;
/// 单条记录硬上限（消息 + 字段 + 记录开销）.
///
/// 没有上限时，一个异常 tracing 事件（巨大 payload/stack 字符串）可以分配任意内存并长期占用队列预算
/// ；超大内容应写入 blob 存储，再按引用记录.
pub const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// 单次预格式化块的最大字节数（8 MiB）.
///
/// 参数/设计：块通过一次 channel 操作携带多行文本，避免每行一次记录/队列 CAS；上限防止单个调用
/// 长期占用队列预算，也限制 writer 的转义临时缓冲. 超限按 `oversize` 丢弃，并计入块内行数.
pub const MAX_PREFORMATTED_BLOCK_BYTES: usize = 8 * 1024 * 1024;

/// 预格式化块内每行的估算记录开销.
///
/// 队列预算同时约束逻辑记录数与字节数；块内没有独立 `Record`，用较小的固定开销近似行元数据.
const PREFORMATTED_LINE_OVERHEAD: usize = 32;

/// writer 处理完一个 `Msg::Batch` 后，空 `Vec` 进入复用小池的最大数量.
///
/// 批次向量按 `BATCH_MAX` 个记录预留，频繁 `log_many` 时每次重新分配会进入生产者/分配器热路径.
/// 池只缓存空向量，不持有记录；上限防止长进程因异常大批次长期占用内存.
const BATCH_POOL_MAX: usize = 32;

/// 单个 logger 允许的最大分片数.
///
/// 分片会成倍增加 writer 线程、队列和文件句柄；16 足够覆盖常见多核部署，同时防止配置
/// 误写成极大值把进程资源耗尽.
/// 分片数超过 1 时必须显式配置，默认保持单 writer.
pub const MAX_SHARDS: usize = 16;

/// 单 logger 队列记录预算上限.
///
/// 记录数打包进 `QueueBudget` 的高 20 位；1,048,575 条已远高于默认 65k/262k 队列.
pub(crate) const MAX_QUEUE_RECORDS: usize = (1 << 20) - 1;

/// 单 logger 队列字节预算上限（44 位，约 16 TiB）.
pub(crate) const MAX_QUEUE_BYTES: usize = (1 << 44) - 1;

/// 估算记录在队列/批次中的成本.
///
/// 参数：`record` 是待入队记录.
/// 返回值：`Record` 结构体本身 + message + 每个字段的key/value 估算字节数.
/// 设计原因：队列预算是按内存近似值而不是条数；该值由生产者在入队前计算一次并随消息携带，writer 不
/// 再重复扫描.
#[inline]
fn record_cost(record: &Record) -> usize {
    let mut cost = std::mem::size_of::<Record>().saturating_add(record.message.len());
    for field in &record.fields {
        cost = cost
            .saturating_add(std::mem::size_of_val(field))
            .saturating_add(field.key.len());
        if let FieldValue::Str(value) = &field.value {
            cost = cost.saturating_add(value.len());
        }
    }
    cost
}

thread_local! {
    /// 当前线程正在执行的 writer runtime id（如果有）.
    /// 同时用于 tracing 递归防火墙，以及拒绝 logger 自己的 writer 调用 flush/close（否则会给自己入
    /// 队并等待永远不来的 ack）.
    static WRITER_ID: Cell<Option<u64>> = const { Cell::new(None) };
}

/// 判断当前线程是否是任意 Flare writer.
///
/// 参数：无.
/// 返回值：当前线程 TLS 中是否记录了 writer runtime id.
/// 用途：tracing layer 用它做递归防火墙；logger 控制操作用它拒绝“自己的 writer 等待自己”.
pub fn is_writer_thread() -> bool {
    WRITER_ID.with(|cell| cell.get().is_some())
}

/// 判断当前线程是否是指定 logger 的 writer.
///
/// 参数：`id` 是目标 runtime id.
/// 返回值：是否匹配.
/// 按 id 而不是 bool 判断，允许 sink 在writer 线程内操作另一个 logger 的 flush/close（不会自锁），
/// 但拒绝操作自己的.
fn is_writer_for(id: u64) -> bool {
    WRITER_ID.with(|cell| cell.get() == Some(id))
}

/// 计算距离 deadline 的剩余时间.
///
/// 参数：`deadline` 是统一的截止时刻.
/// 返回值：剩余 `Duration`；已过期时为 0.设计原因：flush/close 的“发送等待”和“ack 等待”必须共享同一
/// 个 deadline，否则最坏等待会翻倍.
fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// 为当前 shard 分配公平预算.
///
/// 参数：`deadline` 是整体截止时刻；`shards_left` 是包含当前 shard 在内的剩余 shard 数.
/// 返回值：`remaining(deadline) / shards_left`；最后一个 shard 获得全部剩余时间.
/// 不能把同一个 deadline 直接传给所有 shard：第一个慢 shard 可能耗尽预算，后续 shard 拿到 0 后
/// `recv_timeout(0)` 立即超时，看起来像关停失败，实际只是没有等待排水.
fn shard_budget(deadline: Instant, shards_left: usize) -> Duration {
    let rem = remaining(deadline);
    if shards_left <= 1 {
        rem
    } else {
        rem / u32::try_from(shards_left).unwrap_or(u32::MAX)
    }
}

/// 安全地把 timeout 加到当前时刻.
///
/// 参数：`timeout` 是调用方给的超时.
/// 返回值：deadline.
/// 平台 `Instant` 在极端`Duration::MAX` 下可能溢出 panic；这里用 `checked_add`，溢出时退化为一个足
/// 够长的固定预算，SDK 调用方不应因为传入巨大超时把进程打崩.
fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout)
        .unwrap_or_else(|| now + Duration::from_secs(10 * 365 * 24 * 3600))
}

/// 读取当前 UNIX 秒.
///
/// 参数：无.
/// 返回值：墙钟秒；系统时间早于 epoch 时回退 0.仅用于限流窗口，不用于持久化.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// 所有生产者共享的固定一秒窗口限流器.
///
/// `state` 打包 `(window_seconds << 32) | accepted_in_window`；CAS 循环让“换窗口”和“计数 +1”保持原
/// 子，热路径不需要 Mutex.
/// 窗口按墙钟秒切换，因此允许在秒边界出现最多2×max 的瞬时突发，这是固定窗口算法的已知取舍.
#[repr(align(64))]
struct RateLimitState {
    /// 每秒允许的记录数.
    max_per_second: u32,
    /// 打包后的窗口与计数.
    state: AtomicU64,
}

impl RateLimitState {
    /// 根据配置创建限流器.
    ///
    /// 参数：`limit` 是 service 配置中的每秒上限.
    /// 返回值：初始窗口为 0 的限流状态；第一次 `allow` 会自然进入当前秒.
    fn new(limit: RateLimit) -> Self {
        Self {
            max_per_second: limit.max_per_second,
            state: AtomicU64::new(0),
        }
    }

    /// 尝试为一条记录获取本秒配额.
    ///
    /// 参数：`now` 是当前墙钟秒.
    /// 返回值：`true` 表示允许入队；`false` 表示本秒配额已满.
    /// 实现用 CAS 循环：窗口不同则从 1 开始，窗口相同则递增；计数超过上限直接拒绝，不修改状态.
    #[inline]
    fn allow(&self, now: u64) -> bool {
        self.allow_n(now, 1)
    }

    /// 尝试为一批记录获取本秒配额.
    ///
    /// 参数：`now` 是当前墙钟秒；`count` 是本次要入队的记录数.
    /// 返回值：整批配额足够时为 true；配额不足时整批拒绝且不修改计数.
    /// 原因：预格式化块必须原子准入，部分接受会造成块内行数与 emitted 计数不一致.
    fn allow_n(&self, now: u64, count: usize) -> bool {
        if count == 0 {
            return true;
        }
        let now = now & 0xffff_ffff;
        let max = u64::from(self.max_per_second);
        let count = count as u64;
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            let window = current >> 32;
            let used = current & 0xffff_ffff;
            let (next_window, next_used) = if window == now {
                (window, used.saturating_add(count))
            } else {
                (now, count)
            };
            if next_used > max {
                return false;
            }
            match self.state.compare_exchange_weak(
                current,
                (next_window << 32) | next_used,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}

/// 可复用的预格式化记录批次.
///
/// 设计原因：`log_many` 每次调用都会分配一个临时 `Vec`；高频小批量场景下，分配器开销会
/// 进入生产者热路径. 调用方可以跨业务循环复用同一个 `RecordBatch`，由 [`Logger::log_batch`]
/// 在提交时清空并保留容量.
///
/// 参数/字段说明：
/// - `records`：待提交记录；
/// - `bytes`：当前批次的累计估算字节数，仅用于诊断和容量提示；
/// - `min_level`/`max_cost`：批次内级别最小值和成本最大值，供提交时的 O(1) 快路径判断.
///
/// 约束：记录在 `push` 时计算一次成本，提交后不可修改；批次容量没有硬上限，但
/// `log_batch` 会按 `BATCH_MAX`/`BATCH_BYTES_MAX` 切分，不会把整批一次性放进队列.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct RecordBatch {
    records: Vec<Record>,
    bytes: usize,
    min_level: Option<Level>,
    max_cost: usize,
    /// 关联 logger 的批次池；`None` 表示调用方自建批次，不参与池化.
    pool: Option<Arc<BatchPool>>,
}

impl RecordBatch {
    /// 创建空批次.
    ///
    /// 参数：无.
    /// 返回值：没有预留容量的批次；适合一次性使用.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// 创建带预留容量的批次.
    ///
    /// 参数：`capacity` 是预计记录条数.
    /// 返回值：已预留 `capacity` 个槽位的批次；容量只增不减，`clear` 会保留它.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
            bytes: 0,
            min_level: None,
            max_cost: 0,
            pool: None,
        }
    }

    /// 从 logger 的批次池创建批次.
    ///
    /// 参数：`pool` 是 logger 共享的批次池；`capacity` 是期望容量.
    /// 返回值：优先复用池中的空向量；池为空时新建.
    fn with_pool(pool: Arc<BatchPool>, capacity: usize) -> Self {
        Self {
            records: pool.take(capacity),
            bytes: 0,
            min_level: None,
            max_cost: 0,
            pool: Some(pool),
        }
    }

    /// 追加一条记录.
    ///
    /// 参数：`record` 是已格式化记录；返回值：无.
    /// 成本在入队前计算一次，后续快路径不再扫描字段.
    pub fn push(&mut self, record: Record) {
        let cost = record_cost(&record);
        self.bytes = self.bytes.saturating_add(cost);
        self.min_level = Some(match self.min_level {
            Some(level) => level.min(record.level),
            None => record.level,
        });
        self.max_cost = self.max_cost.max(cost);
        // 上一次提交把 records 交给 channel 后容量为 0；从池里取回 writer 回收的向量.
        if self.records.capacity() == 0 {
            if let Some(pool) = &self.pool {
                self.records = pool.take(0);
            }
        }
        self.records.push(record);
    }

    /// 当前待提交条数.
    ///
    /// 参数：无；返回值：记录条数.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// 是否没有待提交记录.
    ///
    /// 参数：无；返回值：`len() == 0`.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// 当前批次的累计估算字节数.
    ///
    /// 参数：无；返回值：所有待提交记录的估算成本之和.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// 清空记录并保留容量.
    ///
    /// 参数：无；返回值：无.
    /// 用于提交后复用；容量不会被释放.
    pub fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
        self.min_level = None;
        self.max_cost = 0;
    }
}

impl Extend<Record> for RecordBatch {
    /// 逐条追加记录.
    ///
    /// 参数：`records` 是记录迭代器；返回值：无.
    fn extend<T: IntoIterator<Item = Record>>(&mut self, records: T) {
        for record in records {
            self.push(record);
        }
    }
}

/// writer 回收的空批次向量池.
///
/// 字段：`free` 只保存已清空、容量不超过 `BATCH_MAX` 的向量；不持有记录.
/// 设计原因：`log_many`/`log_batch` 每个批次都需要把 `Vec<Record>` 交给 channel，writer
/// 消费后如果直接丢弃，生产者会为每个批次重新分配. 池把这次分配摊销到整个进程生命周期.
#[derive(Debug, Default)]
struct BatchPool {
    free: Mutex<Vec<Vec<Record>>>,
}

impl BatchPool {
    /// 取出一个空向量.
    ///
    /// 参数：`capacity` 是期望容量.
    /// 返回值：已清空并至少预留 `capacity` 个槽位的向量；池为空时新建.
    fn take(&self, capacity: usize) -> Vec<Record> {
        let mut pool = self
            .free
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut batch = pool.pop().unwrap_or_default();
        batch.clear();
        if batch.capacity() < capacity {
            batch.reserve(capacity - batch.capacity());
        }
        batch
    }

    /// 回收一个空向量.
    ///
    /// 参数：`batch` 是已清空的向量.
    /// 返回值：无.
    /// 只缓存容量不超过 `BATCH_MAX` 的向量，避免异常大批次长期占用内存.
    fn recycle(&self, mut batch: Vec<Record>) {
        batch.clear();
        if batch.capacity() == 0 || batch.capacity() > BATCH_MAX {
            return;
        }
        let mut pool = self
            .free
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pool.len() < BATCH_POOL_MAX {
            pool.push(batch);
        }
    }
}

impl FromIterator<Record> for RecordBatch {
    /// 从记录迭代器构造批次.
    ///
    /// 参数：`records` 是记录迭代器；返回值：已填充的批次.
    fn from_iter<T: IntoIterator<Item = Record>>(records: T) -> Self {
        let mut batch = Self::new();
        batch.extend(records);
        batch
    }
}

/// 队列满时的处理策略.
#[derive(Clone, Copy, Debug)]
pub enum Overflow {
    /// 丢弃新记录并计入 `overload`；生产者不等待.
    /// 默认值.
    Drop,
    /// 最多等待 `timeout` 获取空位，超时后丢弃新记录.
    ///
    /// 不要在异步执行器线程上使用；tracing layer 和 Python async 路径始终走非阻塞入口.
    Block(Duration),
}

/// sink 写入失败的错误类型.
///
/// 这里只保留文本，不强制 sink 依赖具体错误库；runtime 只做计数和一次性告警.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct SinkError {
    message: String,
}

impl SinkError {
    /// 创建只含文本的 sink 错误.
    ///
    /// 参数：`message` 是面向人的失败说明.
    /// 返回值：可跨线程传递的错误；不保存`io::Error` 的 source，因为 runtime 只做计数/告警，不把它
    /// 返回给业务热路径.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl From<io::Error> for SinkError {
    /// 把 IO 错误转成 sink 错误文本.
    ///
    /// 参数：`error` 是底层 IO 错误；返回值：`SinkError`.
    /// 实现保留错误展示文本，但runtime 不会把它向上抛，而是计入 `write_errors`/健康快照.
    fn from(error: io::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<String> for SinkError {
    /// 拥有字符串直接转成 sink 错误；参数 `message` 原样保存.
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for SinkError {
    /// 借用字符串转成 sink 错误；参数 `message` 在调用栈内有效，因此复制为 String.
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

/// 日志落点.
///
/// 实现必须 `Send + Sync`：同一 sink 可能被多个 logger clone 共享，也可能被直接调用.
/// 单个 shard 内，runtime 保证同一 logger 的 sink 调用只在 writer 线程串行发生；
/// `shards > 1` 且多个 shard 共享同一 sink 时，调用会并发发生，自定义 sink 必须自己保证内部
/// 状态安全，或为每个 shard 提供独立 sink.
pub trait Sink: Send + Sync {
    /// 写单条记录.
    ///
    /// 参数：`record` 是不可变记录引用.
    /// 返回值：成功 `Ok(())`，失败 `Err(SinkError)`.
    /// runtime 会捕获错误、计入 `write_errors` 并继续后续记录；实现不应 panic.
    fn write(&self, record: &Record) -> Result<(), SinkError>;

    /// 批量写入口.
    ///
    /// 参数：`records` 是同一批内的记录引用切片.
    /// 返回值：同 [`Sink::write`].
    /// 默认逐条调用 `write`；内置文件 sink 覆盖成一次缓冲写，减少 syscall.
    fn write_batch(&self, records: &[&Record]) -> Result<(), SinkError> {
        for record in records {
            self.write(record)?;
        }
        Ok(())
    }

    /// 整批都满足 sink 级别区间时的快路径.
    ///
    /// 参数：`records` 是连续记录切片.
    /// 返回值：同 [`Sink::write`].
    /// 默认构造临时引用切片再调用 `write_batch`；内置 sink 覆盖以避免每批分配.
    fn write_batch_all(&self, records: &[Record]) -> Result<(), SinkError> {
        let refs: Vec<&Record> = records.iter().collect();
        self.write_batch(&refs)
    }

    /// 写一个已由调用方拼好的多行文本块.
    ///
    /// 参数：
    /// - `level`：块内所有行的统一级别；
    /// - `block`：以 `\n` 分隔的最终文本，行内控制字符由 sink 转义.
    ///
    /// 返回值：同 [`Sink::write`].
    /// 默认实现逐行构造 `Record` 并调用 [`Sink::write`]，适合自定义 sink；内置文件/控制台 sink
    /// 覆盖为一次批量写，避免每行分配与多次锁/syscall.
    fn write_preformatted_block(&self, level: Level, block: &str) -> Result<(), SinkError> {
        for line in block.lines() {
            self.write(&Record::new(level, line))?;
        }
        Ok(())
    }

    /// 刷新缓冲.
    ///
    /// 参数：无.
    /// 返回值：失败会让 `flush()` 的 ack 为 false；runtime 计入 `write_errors`，
    /// 并继续 flush 其他 sink.
    fn flush(&self) -> Result<(), SinkError> {
        Ok(())
    }

    /// 关闭 sink.
    ///
    /// 参数：无.
    /// 返回值：关闭结果.
    /// 实现必须幂等：writer 退出、显式 close、重复 close 都会调用；失败时 runtime 计错但不会 panic.
    fn close(&self) -> Result<(), SinkError> {
        Ok(())
    }

    /// 外部轮转后重开资源.
    ///
    /// 参数：无.
    /// 返回值：重开结果.
    /// 默认空实现（不拥有文件句柄）；文件 sink 会 flush/关闭当前句柄并重新打开活动路径.
    fn reopen(&self) -> Result<(), SinkError> {
        Ok(())
    }

    /// fork 子进程钩子.
    ///
    /// 参数：无.
    /// 返回值：无.
    /// 实现应丢弃继承的文件句柄/缓冲，不能 flush 父进程数据.
    fn discard_after_fork(&self) {}

    /// 返回 sink 自己的健康细节.
    ///
    /// 参数：无.
    /// 返回值：轮转/压缩/告警计数；默认 `None` 表示没有额外状态.
    fn health(&self) -> Option<SinkHealth> {
        None
    }
}

/// sink 配置：名字、落点、级别区间和统计计数.
///
/// 级别区间为 `[min_level, max_level)`：
/// - `min_level=None` 表示不设下界；
/// - `max_level=None` 表示不设上界；
/// - 与 Python 的常规日志/错误日志分流使用同一约定.
#[derive(Clone)]
pub struct SinkConfig {
    /// sink 名，用于统计/健康/重名校验.
    pub name: String,
    /// 实际落点实现；`Arc<dyn Sink>` 允许同一 sink 被多个 logger 共享.
    pub sink: Arc<dyn Sink>,
    /// 下界（含）；`None` 不设下界.
    pub min_level: Option<Level>,
    /// 上界（不含）；`None` 不设上界.
    pub max_level: Option<Level>,
    /// 成功交给 sink 的累计条数.
    writes: Arc<AtomicU64>,
    /// sink 返回错误的次数（按调用，不按条）.
    errors: Arc<AtomicU64>,
    /// sink panic 次数.
    panics: Arc<AtomicU64>,
}

impl SinkConfig {
    /// 创建不限级别的 sink 配置.
    ///
    /// 参数：
    /// - `name`：sink 名；
    /// - `sink`：共享落点实现.
    ///
    /// 返回值：`min_level/max_level` 均为 `None` 的配置；通常随后 `.levels(...)` 设置区间.
    pub fn new(name: impl Into<String>, sink: Arc<dyn Sink>) -> Self {
        Self {
            name: name.into(),
            sink,
            min_level: None,
            max_level: None,
            writes: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            panics: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 设置 sink 的级别区间 `[min, max)`.
    ///
    /// 参数：`min_level` 含下界；`max_level` 不含上界；`None` 表示无界.
    /// 返回值：链式返回自身.
    /// 区间为空（`min >= max`）会在构建时被拒绝.
    pub fn levels(mut self, min_level: Option<Level>, max_level: Option<Level>) -> Self {
        self.min_level = min_level;
        self.max_level = max_level;
        self
    }

    /// 判断某级别是否落在该 sink 的区间.
    ///
    /// 参数：`level` 是记录级别.
    /// 返回值：`min <= level < max`；任一边界为 `None` 表示该侧无限制.
    /// 使用半开区间让普通/错误文件分流不重叠.
    pub fn matches(&self, level: Level) -> bool {
        if self.min_level.is_some_and(|min| level < min) {
            return false;
        }
        if self.max_level.is_some_and(|max| level >= max) {
            return false;
        }
        true
    }

    /// 生成该 sink 的统计快照.
    ///
    /// 参数：无.
    /// 返回值：名字、writes/errors/panics，以及 sink 自报的轮转/压缩/告警计数.
    /// 计数用 `Relaxed` 读取，只用于观测，不参与同步.
    pub fn stats(&self) -> SinkStat {
        let health = self.sink.health().unwrap_or_default();
        SinkStat {
            name: self.name.clone(),
            writes: self.writes.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            panics: self.panics.load(Ordering::Relaxed),
            rollovers: health.rollovers,
            rollover_errors: health.rollover_errors,
            warnings: health.warnings,
            compression_jobs: health.compression_jobs,
        }
    }
}

/// 运行期/构造期共用的 sink 配置校验.
///
/// 空名字会让统计与排障出现空标签；`min_level >= max_level` 的区间为空，会静默丢弃该 sink 的全部记
/// 录.
/// 两者都属于“看起来配了、实际没写”，在入口直接拒绝.
/// 校验 sink 配置.
///
/// 参数：`config` 是待校验的 sink 配置.
/// 返回值：`None` 表示合法；`Some(message)` 是面向人的错误.
/// 拒绝空/控制字符名字和空级别区间，避免“看起来配了、实际不写”.
fn sink_config_error(config: &SinkConfig) -> Option<String> {
    let name = config.name.trim();
    if name.is_empty() {
        return Some("sink name must not be empty".to_string());
    }
    if name.chars().any(char::is_control) {
        return Some(format!(
            "sink name must not contain control characters: {:?}",
            config.name
        ));
    }
    if let (Some(min), Some(max)) = (config.min_level, config.max_level) {
        if min >= max {
            return Some(format!(
                "sink {:?}: min_level({min}) must be less than max_level({max})",
                config.name
            ));
        }
    }
    None
}

/// 单个 sink 的累计计数.
///
/// 参数/字段说明：所有计数都是 runtime/sink 写入、健康检查读取；用于定位是哪个落点出错/轮转/压缩，
/// 而不是把所有问题混成一个全局错误.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct SinkStat {
    /// sink 名.
    pub name: String,
    /// 成功交给 sink 的记录条数（按批累加）.
    pub writes: u64,
    /// 写入/刷新/关闭失败的次数（按调用，不是记录条数）.
    pub errors: u64,
    /// sink panic 次数.
    pub panics: u64,
    /// 文件类 sink 完成的轮转次数.
    pub rollovers: u64,
    /// 轮转失败并退避的次数.
    pub rollover_errors: u64,
    /// sink 自报的告警数（有上限）.
    pub warnings: u64,
    /// 该 sink 当前拥有的压缩任务数.
    pub compression_jobs: usize,
}

/// sink 自报的可选健康细节.
///
/// 参数/字段说明：文件 sink 实现 `Sink::health` 时填充；默认 sink 没有这些状态.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct SinkHealth {
    /// 成功轮转次数.
    pub rollovers: u64,
    /// 轮转失败次数.
    pub rollover_errors: u64,
    /// 告警数量.
    pub warnings: u64,
    /// 当前压缩任务数.
    pub compression_jobs: usize,
}

/// 记录被丢弃的原因.
///
/// 统计口径（按记录，不按 sink 尝试）：
/// - `Overload`：队列/记录预算满或 Block 超时；
/// - `RateLimited`：service 级每秒上限已满；
/// - `Shutdown`：close 已开始后仍尝试写入；
/// - `WriteError`：记录没有被任何匹配的 sink 成功写入（至少一个 sink 返回错误）；
/// - `NoSink`：当前没有匹配该记录级别的 sink；
/// - `Panic`：匹配 sink 全部 panic/失败；
/// - `WorkerDead`：writer 已退出或 channel 断开；
/// - `Reentrant`：sink 在 writer 内回写同一个 logger；
/// - `Oversize`：单条记录超过 [`MAX_RECORD_BYTES`].
///
/// 一条记录写入 A 成功、B 失败时不计 drop；只有所有匹配 sink 都失败才计一次.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum DropReason {
    /// 队列/记录预算满，或 `Block` 超时.
    Overload,
    /// 超过 service 每秒限流配额.
    RateLimited,
    /// close 已开始后仍尝试写入.
    Shutdown,
    /// 记录没有被任何匹配 sink 成功写入（至少一个 sink 返回错误）.
    WriteError,
    /// 当前没有匹配该记录级别的 sink.
    NoSink,
    /// 匹配 sink 全部 panic/失败.
    Panic,
    /// writer 已退出或 channel 断开.
    WorkerDead,
    /// sink 在 writer 内回写同一个 logger.
    Reentrant,
    /// 单条记录超过 [`MAX_RECORD_BYTES`].
    Oversize,
    /// JSON Lines 服务收到未标记为 JSON 的批量文本.
    Format,
}

impl DropReason {
    /// 丢弃原因数量；必须与变体数一致.
    ///
    /// `drop_reasons: [Counter; DropReason::COUNT]` 和快照数组都使用该常量；新增变体时数组
    /// 长度/解构会立即编译失败，不会出现漏算.
    pub(crate) const COUNT: usize = 10;

    /// 返回计数器数组下标.
    ///
    /// 参数：`self`.
    /// 返回值：`0..DropReason::COUNT`，对应 `drop_reasons: [Counter; DropReason::COUNT]`.
    /// `#[repr(usize)]` 让变体声明顺序直接成为下标，新增原因只需追加变体并同步数组长度，
    /// 不再维护一份容易漏改的 match 映射.
    const fn index(self) -> usize {
        self as usize
    }
}

/// 一个独占 cache line 的计数 stripe.
///
/// 只包一个 `AtomicU64` 却按 64 字节对齐，确保相邻 stripe 不会落在同一 cache line.
/// 这是分片计数器避免 false sharing 的必要条件：仅给整个数组按 cache line 对齐
/// 仍会让 8 个 stripe 挤在一条线上.
#[repr(align(64))]
struct CounterStripe(AtomicU64);

/// 按线程分片的运行时计数器.
///
/// 生产者更新 emitted/dropped，writer 更新 write_errors/sink_panics，健康检查读取全部；若这些
/// 原子量挤在同一 cache line，会形成跨核 cache line 乒乓.
/// 每个线程固定使用一个 stripe，读取时求和.
///
/// 字段：`stripes` 是每线程独立更新的计数槽；每个槽独占一条 cache line.
/// 代价是每个逻辑计数器占 `COUNTER_STRIPES * 64` 字节，但服务数受
/// `max_dynamic_services` 限制，换来高并发下无跨核 cache line 争用.
struct Counter {
    stripes: [CounterStripe; COUNTER_STRIPES],
}

/// 单个计数器的 stripe 数.
///
/// 8 个足以覆盖常见生产者线程数；每个 stripe 独占 cache line.
const COUNTER_STRIPES: usize = 8;

#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<Counter>() == COUNTER_STRIPES * 64);

thread_local! {
    /// 当前线程使用的计数器 stripe；`usize::MAX` 表示尚未分配.
    static COUNTER_STRIPE: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// 分配 stripe 的全局游标.
///
/// 只在每个线程第一次计数时访问一次；之后线程从 TLS 读取，不再走全局原子.
static NEXT_COUNTER_STRIPE: AtomicUsize = AtomicUsize::new(0);

/// 返回当前线程的计数器 stripe.
///
/// 参数：无.
/// 返回值：`0..COUNTER_STRIPES`.
/// 首次调用用全局游标轮转分配，避免所有线程落到同一个 stripe；后续调用只读 TLS.
#[inline]
fn counter_stripe() -> usize {
    COUNTER_STRIPE
        .try_with(|cell| {
            let stripe = cell.get();
            if stripe != usize::MAX {
                return stripe;
            }
            let stripe = NEXT_COUNTER_STRIPE.fetch_add(1, Ordering::Relaxed) % COUNTER_STRIPES;
            cell.set(stripe);
            stripe
        })
        // TLS 正在销毁时不再访问线程局部状态；落到 0 号 stripe，计数仍保持有界.
        .unwrap_or(0)
}

impl Counter {
    /// 创建 0 值计数器；参数无.
    const fn new() -> Self {
        Self {
            stripes: [const { CounterStripe(AtomicU64::new(0)) }; COUNTER_STRIPES],
        }
    }

    /// 增加计数.
    ///
    /// 参数：`value` 为增量；0 直接返回，避免无意义的原子写.
    /// 使用 `Relaxed`：计数器只用于观测，不发布其他内存状态.
    #[inline]
    fn add(&self, value: u64) {
        if value != 0 {
            self.stripes[counter_stripe()]
                .0
                .fetch_add(value, Ordering::Relaxed);
        }
    }

    /// 读取所有 stripe 的和.
    ///
    /// 参数：无；返回值：累计计数.
    /// 逐项 `Relaxed` 读取，不要求跨 stripe 的原子一致；健康快照本来就是近似值.
    fn get(&self) -> u64 {
        self.stripes.iter().fold(0u64, |total, stripe| {
            total.saturating_add(stripe.0.load(Ordering::Relaxed))
        })
    }
}

/// 生产者门使用的 cache-line 对齐计数器.
///
/// 每个生产者和 close 都会触碰它；不能与 `closed`/`alive` 或指标计数器共享 cache line.
/// 所有操作都用 `SeqCst`：门的正确性依赖它与 `closed` 存储之间的单一全序.
#[repr(align(64))]
struct ProducerCounter(AtomicUsize);

impl ProducerCounter {
    /// 创建 0 值生产者门计数器；参数无.
    const fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    /// 生产者进入临界区.
    ///
    /// 参数：无.
    /// 使用 `SeqCst`：必须与 close 的 `closed` 存储建立单一全序，否则生产者可能读到 `closed=false`
    /// 后在 Stop 之后入队，导致关停丢记录.
    #[inline]
    fn add(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    /// 生产者离开临界区.
    ///
    /// 参数：无.`SeqCst` 与 [`Self::add`] 配对，保证 close 观察 active==0 时所有在途channel 操作已
    /// 经完成.
    #[inline]
    fn sub(&self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }

    /// 读取当前在途生产者数；参数无；`SeqCst` 保证与 close 的关停顺序一致.
    #[inline]
    fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// 队列预算：用一个 64 位原子同时约束记录数和估算字节数.
///
/// 字段：`state` 高 20 位是记录数，低 44 位是字节数；`peak_*` 是进程内观测到的峰值.
/// 一次 CAS 同时预留两个维度，避免“先占记录、再占字节”的中间状态；记录数上限由
/// [`MAX_QUEUE_RECORDS`] 保证，字节上限由 [`MAX_QUEUE_BYTES`] 保证.
/// 记录数预算无法约束 16 MiB 大消息的聚合内存，因此生产路径必须同时检查字节.
#[repr(align(64))]
struct QueueBudget {
    state: AtomicU64,
    peak_records: AtomicUsize,
    peak_bytes: AtomicUsize,
}

/// 字节字段在打包预算中的位数.
const QUEUE_BYTE_BITS: u32 = 44;
/// 字节字段掩码.
const QUEUE_BYTE_MASK: u64 = (1 << QUEUE_BYTE_BITS) - 1;

impl QueueBudget {
    /// 创建 0 预算；参数无.
    const fn new() -> Self {
        Self {
            state: AtomicU64::new(0),
            peak_records: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
        }
    }

    /// 尝试一次 CAS 同时预留记录和字节预算.
    ///
    /// 参数：`records`/`bytes` 是本次预留；`max_records`/`max_bytes` 是上限.
    /// 返回值：成功 true；任一维度不足 false.
    /// 打包更新保证两个维度始终一起变化，不会留下半预留.
    fn try_reserve(
        &self,
        records: usize,
        bytes: usize,
        max_records: usize,
        max_bytes: usize,
    ) -> bool {
        if records > max_records || bytes > max_bytes {
            return false;
        }
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            let current_records = (current >> QUEUE_BYTE_BITS) as usize;
            let current_bytes = (current & QUEUE_BYTE_MASK) as usize;
            if records > max_records.saturating_sub(current_records)
                || bytes > max_bytes.saturating_sub(current_bytes)
            {
                return false;
            }
            let next_records = current_records.saturating_add(records);
            let next_bytes = current_bytes.saturating_add(bytes);
            let next =
                ((next_records as u64) << QUEUE_BYTE_BITS) | (next_bytes as u64 & QUEUE_BYTE_MASK);
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        true
    }

    /// 归还记录和字节预算.
    ///
    /// 参数：`records`/`bytes` 是已处理/已丢弃的数量；返回值：无.
    /// 只对成功预留过的量调用，字段之间不会借位；`AcqRel` 与预留形成可见性顺序.
    fn release(&self, records: usize, bytes: usize) {
        if records == 0 && bytes == 0 {
            return;
        }
        let packed = ((records as u64) << QUEUE_BYTE_BITS) | (bytes as u64 & QUEUE_BYTE_MASK);
        self.state.fetch_sub(packed, Ordering::AcqRel);
    }

    /// 读取当前记录/字节占用与峰值.
    ///
    /// 峰值在快照/查询时采样，而不是每次入队更新：入队热路径不能为观测指标多付两个原子写.
    fn get(&self) -> (usize, usize, usize, usize) {
        let state = self.state.load(Ordering::Acquire);
        let records = (state >> QUEUE_BYTE_BITS) as usize;
        let bytes = (state & QUEUE_BYTE_MASK) as usize;
        let peak_records = self
            .peak_records
            .fetch_max(records, Ordering::Relaxed)
            .max(records);
        let peak_bytes = self
            .peak_bytes
            .fetch_max(bytes, Ordering::Relaxed)
            .max(bytes);
        (records, bytes, peak_records, peak_bytes)
    }
}

/// 按原因拆分的丢弃计数快照.
///
/// 参数/字段说明：每个字段对应 [`DropReason`] 的一个原因；总和应等于 `dropped`.
/// 该拆分让监控能区分“过载”“关停”“写失败”“没有 sink”等完全不同的处理动作.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct DropReasonSnapshot {
    /// 队列/预算满或 Block 超时.
    pub overload: u64,
    /// 超过每秒限流.
    pub rate_limited: u64,
    /// close 后写入.
    pub shutdown: u64,
    /// 所有匹配 sink 都返回错误.
    pub write_error: u64,
    /// 没有匹配 sink.
    pub no_sink: u64,
    /// sink panic 次数.
    pub panic: u64,
    /// writer 已死/channel 断开.
    pub worker_dead: u64,
    /// 重入.
    pub reentrant: u64,
    /// 超过单条上限.
    pub oversize: u64,
    /// JSON Lines 模式下未标记为 JSON 的批量记录.
    pub format: u64,
}

impl DropReasonSnapshot {
    /// 把每个 shard 的计数器数组转成快照.
    ///
    /// 参数：`counters` 是按 [`DropReason::index`] 排列的计数器.
    /// 返回值：命名字段快照.
    fn from_counters(counters: &[Counter; DropReason::COUNT]) -> Self {
        Self {
            overload: counters[DropReason::Overload.index()].get(),
            rate_limited: counters[DropReason::RateLimited.index()].get(),
            shutdown: counters[DropReason::Shutdown.index()].get(),
            write_error: counters[DropReason::WriteError.index()].get(),
            no_sink: counters[DropReason::NoSink.index()].get(),
            panic: counters[DropReason::Panic.index()].get(),
            worker_dead: counters[DropReason::WorkerDead.index()].get(),
            reentrant: counters[DropReason::Reentrant.index()].get(),
            oversize: counters[DropReason::Oversize.index()].get(),
            format: counters[DropReason::Format.index()].get(),
        }
    }

    /// 转成与 [`DropReason`] 下标一致的数组.
    ///
    /// 参数：无；返回值：长度 `DropReason::COUNT` 的计数数组.
    /// 数组字面量长度由编译器校验；新增原因时若忘记补字段会直接编译失败.
    fn to_array(&self) -> [u64; DropReason::COUNT] {
        [
            self.overload,
            self.rate_limited,
            self.shutdown,
            self.write_error,
            self.no_sink,
            self.panic,
            self.worker_dead,
            self.reentrant,
            self.oversize,
            self.format,
        ]
    }

    /// 从数组还原快照.
    ///
    /// 参数：`values` 是长度 `DropReason::COUNT` 的数组.
    /// 返回值：命名字段快照；解构长度由编译器校验.
    fn from_array(values: [u64; DropReason::COUNT]) -> Self {
        let [overload, rate_limited, shutdown, write_error, no_sink, panic, worker_dead, reentrant, oversize, format] =
            values;
        Self {
            overload,
            rate_limited,
            shutdown,
            write_error,
            no_sink,
            panic,
            worker_dead,
            reentrant,
            oversize,
            format,
        }
    }

    /// 合并另一个 shard 的丢弃原因.
    ///
    /// 参数：`from` 是下一个 shard 的快照.
    /// 返回值：无.
    /// 按数组遍历求和，新增原因只要更新 to_array/from_array 就会自动参与合并，不再需要手写
    /// 9 个字段的易漏代码.
    fn merge(&mut self, from: &Self) {
        let mut into = self.to_array();
        let from = from.to_array();
        for (target, value) in into.iter_mut().zip(from) {
            *target = (*target).saturating_add(value);
        }
        *self = Self::from_array(into);
    }
}

/// [`Logger::close_with_report`] 的结果.
///
/// 只看 `ok=false` 无法区分“截止时间到达时 writer 仍忙”与“某个 sink 返回错误”.
/// 运维需要这个区分来决定是重试关停还是排查 flush 失败.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct CloseReport {
    /// 所有 sink 的 flush 与 close 都成功.
    pub ok: bool,
    /// 截止时间到达时关停尚未完成；logger 仍在后台收尾，后续 `close` 可以重试.
    pub timed_out: bool,
}

impl CloseReport {
    /// 构造“已完成”报告.
    ///
    /// 参数：`ok` 表示 flush/close 是否全部成功；返回值：`timed_out=false` 的报告.
    /// 注意：sink 写记录失败不算关停失败，只进 `dropped_post/write_errors`.
    fn from_ok(ok: bool) -> Self {
        Self {
            ok,
            timed_out: false,
        }
    }

    /// 构造超时报告.
    ///
    /// 参数：无.
    /// 返回值：`ok=false, timed_out=true`；调用方可以稍后重试 close.
    fn timeout() -> Self {
        Self {
            ok: false,
            timed_out: true,
        }
    }
}

/// logger 的只读健康快照.
///
/// `pending` 是 channel 中的消息数：普通记录为 1 条/消息，`log_many` 的 Batch 为1 条/批，因此批量写
/// 入时它不等于待写记录条数；`queued_records` 是队列/在途批次的记录预算占用.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct LoggerSnapshot {
    /// service/logger 名.
    pub name: String,
    /// 当前阈值.
    pub level: Level,
    /// 是否已开始关停.
    pub closed: bool,
    /// writer 是否仍存活（完成收尾后为 false）.
    pub alive: bool,
    /// 队列容量（以记录数计）.
    pub queue_capacity: usize,
    /// 队列字节预算上限.
    pub queue_byte_capacity: usize,
    /// 队列 + writer 在途批次占用的记录预算.
    pub queued_records: usize,
    /// 队列 + writer 在途批次占用的字节预算.
    pub queued_bytes: usize,
    /// 观测到的记录预算峰值.
    pub peak_queued_records: usize,
    /// 观测到的字节预算峰值.
    pub peak_queued_bytes: usize,
    /// channel 中消息数（Batch 只算一条消息）.
    pub pending: usize,
    /// 按级别统计的成功入队数.
    pub emitted: [u64; 5],
    /// 至少被一个 sink 成功写出的记录数.
    pub delivered: u64,
    /// 总丢弃；等于 dropped_pre + dropped_post.
    pub dropped: u64,
    /// 入队前拒绝（overload/rate_limited/shutdown/oversize/...
    /// ）.
    pub dropped_pre: u64,
    /// 入队后未写成功（write_error/panic/no_sink/worker_dead）.
    pub dropped_post: u64,
    /// 按原因拆分的丢弃.
    pub drop_reasons: DropReasonSnapshot,
    /// sink 级写/刷新/关闭失败次数.
    pub write_errors: u64,
    /// sink panic 次数.
    pub sink_panics: u64,
    /// 每个 sink 的统计.
    pub sinks: Vec<SinkStat>,
}

/// logger 的共享状态.
///
/// 生命周期：`Logger` 持有 `Arc<Shared>` 和 `Sender`；writer 线程持有 `Arc<Shared>` 和`Receiver`.
/// 最后一个 `Logger` 释放后 `Sender` 断开，writer 排空队列后退出.
///
/// 字段说明（按职责分组）：
/// - 身份/配置：`id`、`name`、`level`、`overflow`、`queue_capacity`、`rate_limit`；
/// - 生命周期：`closed`、`stop_requested`、`active_producers`、`alive`、`stopped`、
///   `finished_ok`、`stop_lock` 等字段；
/// - sink 表：`sinks_lock`、`sinks`；
/// - 计数：`emitted/dropped/dropped_pre/dropped_post/delivered` 与 `drop_reasons/`
///   `write_errors/sink_panics` 等字段.
struct Shared {
    /// logger group id；同一 logger 的所有 shard 共用，用于 writer TLS 自识别和重入拒绝.
    id: u64,
    /// service/logger 名；同时作为默认 target.
    name: Arc<str>,
    /// 是否要求批量记录已标记为 JSON 行.
    json_lines: bool,
    /// 当前阈值（核心级别 int）.
    level: AtomicI32,
    /// 队列满策略.
    overflow: Overflow,
    /// 是否已拒绝新记录.
    closed: AtomicBool,
    /// `close()` 未能把 Stop 哨兵投进满队列时置位；writer 在队列排空后据此自行收尾，避免“关停依赖一
    /// 条投不进去的消息”.
    stop_requested: AtomicBool,
    /// 已通过 `closed` 检查、尚未完成 channel 发送的生产者数.
    ///
    /// close 先置 `closed`，再等待该计数归零后投递 Stop；否则生产者可能在 Stop 之后入队，writer 提
    /// 前退出造成静默丢失.
    active_producers: ProducerCounter,
    /// writer 是否仍存活；完成收尾后置 false.
    alive: AtomicBool,
    /// close 是否已经观测到 writer 收尾.
    stopped: AtomicBool,
    /// writer 最终 flush/close 的结果.Stop ack 丢失时 close 用它判断是否真的成功，避免把“worker 已
    /// 退出但 flush 失败”误报为成功.
    finished_ok: AtomicBool,
    /// 串行化 close，保证重复 close 返回第一次的真实结果.
    stop_lock: Mutex<()>,
    /// 串行化 sink 表的“写者”.`rcu` 保证单次更新原子；这把锁保证 add_sink 与 set_sinks之间有明确顺
    /// 序，而不是后写覆盖前写.
    sinks_lock: Mutex<()>,
    /// 队列容量（记录数）.
    queue_capacity: usize,
    /// 队列字节预算上限.
    max_queued_bytes: usize,
    /// 当前已预留的记录+字节预算.
    queue_budget: QueueBudget,
    /// writer 回收的空批次向量；只存 `Vec` 容量，不持有记录.
    batch_pool: Arc<BatchPool>,
    /// 可选限流器；`None` 表示不限流.
    rate_limit: Option<RateLimitState>,
    /// sink 表；读路径无锁，写路径在 `sinks_lock` 下 RCU 发布.
    sinks: ArcSwap<Vec<SinkConfig>>,
    /// 按级别统计的成功入队数.
    emitted: [Counter; 5],
    /// 总丢弃.
    dropped: Counter,
    /// 入队前丢弃.
    dropped_pre: Counter,
    /// 入队后未写成功.
    dropped_post: Counter,
    /// 至少被一个 sink 成功写出的记录数.
    delivered: Counter,
    /// 按原因拆分的丢弃.
    drop_reasons: [Counter; DropReason::COUNT],
    /// sink 级写/刷新/关闭失败次数.
    write_errors: Counter,
    /// sink panic 次数.
    sink_panics: Counter,
}

impl Shared {
    /// 记录一条“入队前”丢弃.
    ///
    /// 参数：`reason` 是丢弃原因；`count` 是记录数，0 直接返回.
    /// 返回值：无.
    /// 同时增加`dropped`、`dropped_pre` 和对应原因计数，保证对账.
    fn drop_before_enqueue(&self, reason: DropReason, count: usize) {
        if count == 0 {
            return;
        }
        self.dropped.add(count as u64);
        self.dropped_pre.add(count as u64);
        self.drop_reasons[reason.index()].add(count as u64);
    }

    /// 记录一条“入队后”丢弃.
    ///
    /// 参数：`reason` 是丢弃原因；`count` 是记录数，0 直接返回.
    /// 返回值：无.
    /// 与[`Self::drop_before_enqueue`] 的区别是计入 `dropped_post`.
    fn drop_after_enqueue(&self, reason: DropReason, count: usize) {
        if count == 0 {
            return;
        }
        self.dropped.add(count as u64);
        self.dropped_post.add(count as u64);
        self.drop_reasons[reason.index()].add(count as u64);
    }

    /// 记录成功写出的记录数.
    ///
    /// 参数：`count` 是本批至少被一个 sink 写成功的记录数；0 直接返回.
    /// 多 sink fan-out时每条记录只计一次，由 writer 的 `Outcome` 状态保证.
    fn record_delivered(&self, count: usize) {
        if count != 0 {
            self.delivered.add(count as u64);
        }
    }

    /// 查询限流是否允许当前记录.
    ///
    /// 参数：无.
    /// 返回值：`true` 表示未配置限流或本秒仍有配额；`false` 表示应丢弃.
    /// 未配置时是编译期可内联的 `None` 分支，不读时钟.
    #[inline]
    fn rate_allows(&self) -> bool {
        match &self.rate_limit {
            Some(limit) => limit.allow(now_secs()),
            None => true,
        }
    }

    /// 查询一批记录是否都能获得本秒限流配额.
    ///
    /// 参数：`count` 是本次提交的逻辑记录数.
    /// 返回值：未配置限流或整批配额足够时为 true.
    /// 整批准入避免块内部分行入队、部分行丢弃导致计数与文件内容不一致.
    #[inline]
    fn rate_allows_n(&self, count: usize) -> bool {
        match &self.rate_limit {
            Some(limit) => limit.allow_n(now_secs(), count),
            None => true,
        }
    }

    /// 记录一条成功入队.
    ///
    /// 参数：`level` 是记录级别；返回值：无.`emitted` 表示“成功进入队列”，不是“已写盘”.
    fn record_emitted(&self, level: Level) {
        self.emitted[level.index()].add(1);
    }

    /// 尝试预留记录预算.
    ///
    /// 参数：`count` 是记录数；返回值：是否成功.
    /// 预算不足时生产者应丢弃并计数.
    fn reserve_records(&self, count: usize, bytes: usize) -> bool {
        self.queue_budget
            .try_reserve(count, bytes, self.queue_capacity, self.max_queued_bytes)
    }

    /// 在 `Overflow::Block` 下等待记录+字节预算.
    ///
    /// 参数：`count`/`bytes` 是需要的预算；`timeout` 是总等待预算.
    /// 返回值：成功预留为 true；超时或请求本身超过上限为 false.
    /// 只在慢路径做有界睡眠轮询，避免给正常入队加锁/条件变量成本.
    fn reserve_records_blocking(&self, count: usize, bytes: usize, timeout: Duration) -> bool {
        if self.reserve_records(count, bytes) {
            return true;
        }
        if count > self.queue_capacity || bytes > self.max_queued_bytes {
            return false;
        }
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            thread::sleep(remaining.min(Duration::from_millis(1)));
            if self.reserve_records(count, bytes) {
                return true;
            }
        }
    }

    /// 归还记录+字节预算.
    ///
    /// 参数：`count`/`bytes` 是已处理或已丢弃的量；返回值：无.
    /// 必须在 writer 处理完批次或发送失败后调用，否则预算会泄漏.
    fn release_records(&self, count: usize, bytes: usize) {
        self.queue_budget.release(count, bytes);
    }

    /// 取出一个可复用的空批次向量.
    ///
    /// 参数：`capacity` 是期望容量.
    /// 返回值：已清空并至少预留 `capacity` 个槽位的向量.
    /// 池为空时新建；池里的向量容量只增不减，但最多缓存 [`BATCH_POOL_MAX`] 个.
    fn take_batch_vec(&self, capacity: usize) -> Vec<Record> {
        self.batch_pool.take(capacity)
    }

    /// 回收 writer 已消费的空批次向量.
    ///
    /// 参数：`batch` 是已经 `append`/处理完的向量；返回值：无.
    /// 只缓存容量不超过 `BATCH_MAX` 的向量，避免异常大批次长期占用内存.
    fn recycle_batch(&self, batch: Vec<Record>) {
        self.batch_pool.recycle(batch);
    }

    /// 进入生产者临界区.
    ///
    /// 参数：无.
    /// 返回值：`true` 表示可以继续发送，`false` 表示已关停.
    /// 调用方在 `true`后必须调用 [`Self::end_produce`]；`false` 时函数内部已回滚计数.
    ///
    /// `SeqCst` 是刻意选择：生产者的 increment/closed load 与 close 的 closed store/ active load 必
    /// 须形成单一全序，否则可能在 Stop 之后入队.
    #[inline]
    fn begin_produce(&self) -> bool {
        self.active_producers.add();
        if self.closed.load(Ordering::SeqCst) {
            self.active_producers.sub();
            false
        } else {
            true
        }
    }

    /// 离开生产者临界区.
    ///
    /// 参数：无；返回值：无.
    /// 必须与 [`Self::begin_produce`] 配对.
    #[inline]
    fn end_produce(&self) {
        self.active_producers.sub();
    }

    /// 等待所有在途生产者离开临界区.
    ///
    /// 参数：`deadline` 是关停截止时刻；返回值：是否在截止前归零.
    /// 实现以 1ms 轮询；关停不是热路径，轮询比引入条件变量更简单且不会被 fork 继承锁卡死.
    fn wait_for_producers(&self, deadline: Instant) -> bool {
        if self.active_producers.get() == 0 {
            return true;
        }
        while Instant::now() < deadline {
            if self.active_producers.get() == 0 {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        self.active_producers.get() == 0
    }

    /// 生成健康快照.
    ///
    /// 参数：`pending` 是调用方从 channel 读取的当前消息数；返回值：只读快照.
    /// 计数逐项读取，不是原子一致快照；严格对账应在停止写入后进行.
    fn snapshot(&self, pending: usize) -> LoggerSnapshot {
        let level = Level::from_i32(self.level.load(Ordering::Acquire));
        let snapshot = DropReasonSnapshot::from_counters(&self.drop_reasons);
        let (queued_records, queued_bytes, peak_queued_records, peak_queued_bytes) =
            self.queue_budget.get();
        LoggerSnapshot {
            name: self.name.to_string(),
            level,
            closed: self.closed.load(Ordering::Acquire),
            alive: self.alive.load(Ordering::Acquire),
            queue_capacity: self.queue_capacity,
            queue_byte_capacity: self.max_queued_bytes,
            queued_records,
            queued_bytes,
            peak_queued_records,
            peak_queued_bytes,
            pending,
            emitted: std::array::from_fn(|index| self.emitted[index].get()),
            delivered: self.delivered.get(),
            dropped: self.dropped.get(),
            dropped_pre: self.dropped_pre.get(),
            dropped_post: self.dropped_post.get(),
            drop_reasons: snapshot,
            write_errors: self.write_errors.get(),
            sink_panics: self.sink_panics.get(),
            sinks: self
                .sinks
                .load_full()
                .iter()
                .map(SinkConfig::stats)
                .collect(),
        }
    }
}

/// 核心 logger 构造失败原因.
///
/// 参数/变体说明：覆盖没有 sink、队列为 0、sink 重名、sink 配置非法、writer 线程启动失败.
/// 所有变体都发生在构建期，运行期不会返回这些错误.
#[derive(Clone, Debug, thiserror::Error)]
pub enum BuildError {
    /// 没有配置任何 sink.
    #[error("at least one sink is required")]
    NoSink,
    /// `queue_size == 0`，无法提供有界队列.
    #[error("queue_size must be > 0")]
    ZeroQueue,
    /// 两个 sink 使用同一个名字.
    #[error("duplicate sink name: {0:?}")]
    DuplicateSink(String),
    /// sink 名字/级别区间非法.
    #[error("invalid sink: {0}")]
    InvalidSink(String),
    /// 分片配置非法（0 或超过 [`MAX_SHARDS`]）.
    #[error("invalid sharding: {0}")]
    InvalidSharding(String),
    /// writer 线程创建失败（资源耗尽等）.
    #[error("cannot start writer thread: {0}")]
    Spawn(String),
}

/// 核心 logger 构造器.
///
/// 构造阶段只分配队列和启动 writer，不打开文件；文件首次写入时才创建目录和句柄.
/// 字段说明：`name` 是 service 名/默认 target；`level` 阈值；`queue_size`/`max_queued_bytes`
/// 是队列记录/字节预算；`overflow` 满队列策略；`rate_limit` 可选限流；`sinks` 落点列表；`shards`
/// 是 writer/队列分片数.
pub struct LoggerBuilder {
    name: String,
    level: Level,
    queue_size: usize,
    max_queued_bytes: Option<usize>,
    json_lines: bool,
    overflow: Overflow,
    rate_limit: Option<RateLimit>,
    #[cfg(test)]
    sinks: Vec<SinkConfig>,
    shards: usize,
}

impl LoggerBuilder {
    /// 创建构造器.
    ///
    /// 参数：`name` 是 service/logger 名.
    /// 返回值：默认 `Info` 阈值、65,536 记录预算、`Overflow::Drop`、无限流、无 sink 的构造器；必须
    /// 至少 `.add_sink` 后才能 build.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            level: Level::Info,
            // 默认 65,536 条：按 1.4M 条/秒大约可吸收 45ms 的 writer 抖动.10k 只能吸收
            // 约 7ms，轮转/磁盘短停顿就会触发丢新；内存代价约 12 MiB/logger.
            queue_size: 65_536,
            max_queued_bytes: None,
            json_lines: false,
            overflow: Overflow::Drop,
            rate_limit: None,
            #[cfg(test)]
            sinks: Vec::new(),
            shards: 1,
        }
    }

    /// 设置阈值.
    ///
    /// 参数：`level` 是核心级别；返回值：链式返回自身.
    /// 低于阈值的记录在入队前被过滤.
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    /// 设置队列记录预算.
    ///
    /// 参数：`queue_size` 是记录条数上限；返回值：链式返回自身.
    /// 容量按记录而不是 channel消息计算；批量消息会按记录数预留预算.
    pub fn queue_size(mut self, queue_size: usize) -> Self {
        // 容量按记录而不是 channel 消息计算；批量消息即使只占一个 channel 槽，也按记录数预留预算.
        self.queue_size = queue_size;
        self
    }

    /// 设置队列字节预算.
    ///
    /// 参数：`bytes` 必须 > 0；返回值：链式返回自身.
    /// `None` 时按 `queue_size * 256` 自动推导；显式值用于约束大消息聚合内存.
    pub fn max_queued_bytes(mut self, bytes: usize) -> Self {
        self.max_queued_bytes = Some(bytes);
        self
    }

    /// 设置是否要求批量记录标记为 JSON 行.
    ///
    /// 参数：`enabled`；返回值：链式自身.
    pub fn json_lines(mut self, enabled: bool) -> Self {
        self.json_lines = enabled;
        self
    }

    /// 设置队列满策略.
    ///
    /// 参数：`overflow` 是 `Drop` 或 `Block(timeout)`；返回值：链式返回自身.`Block` 只应用于直接
    /// logger 调用，tracing/async 路径始终使用非阻塞入口.
    pub fn overflow(mut self, overflow: Overflow) -> Self {
        self.overflow = overflow;
        self
    }

    /// 设置每秒限流.
    ///
    /// 参数：`rate` 是本秒上限；返回值：链式返回自身.
    /// 超限记录在入队前丢弃并计`rate_limited`.
    pub fn rate_limit(mut self, rate: RateLimit) -> Self {
        self.rate_limit = Some(rate);
        self
    }

    /// 追加一个 sink 配置.
    ///
    /// 参数：`sink` 是已构造的 sink 配置；返回值：链式返回自身.
    /// 重名/非法区间会在`build()` 拒绝.
    /// 测试专用：追加一个 sink.
    ///
    /// 参数：`sink` 是 sink 配置；返回值：链式返回自身.
    #[cfg(test)]
    pub fn add_sink(mut self, sink: SinkConfig) -> Self {
        self.sinks.push(sink);
        self
    }

    /// 设置 writer/队列分片数.
    ///
    /// 参数：`shards` 是分片数，必须为 `1..=MAX_SHARDS`.
    /// 返回值：链式返回自身.
    /// 默认 1，保持单 writer 和全局 FIFO；大于 1 时同一线程固定写一个 shard，跨线程不保证
    /// 全局顺序. `build()` 会把同一组 sink 克隆到每个 shard，因此 sink 可能被多个 writer 并发
    /// 调用；需要 I/O 并行时应使用每 shard 独立的 sink（router 的文件 sink 已这样做）.
    #[must_use]
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }

    /// 测试专用：用同一组 sink 构建 logger.
    ///
    /// 参数：`self` 是已完成配置的构造器.
    /// 返回值：成功为 [`Logger`]；失败为 [`BuildError`].
    #[cfg(test)]
    pub fn build(self) -> Result<Logger, BuildError> {
        if self.queue_size == 0 {
            return Err(BuildError::ZeroQueue);
        }
        if self.queue_size > MAX_QUEUE_RECORDS {
            return Err(BuildError::InvalidSharding(format!(
                "queue_size {} exceeds limit {MAX_QUEUE_RECORDS}",
                self.queue_size
            )));
        }
        if self
            .max_queued_bytes
            .is_some_and(|bytes| bytes > MAX_QUEUE_BYTES)
        {
            return Err(BuildError::InvalidSharding(format!(
                "max_queued_bytes exceeds limit {MAX_QUEUE_BYTES}"
            )));
        }
        self.validate_sharding()?;
        if self.sinks.is_empty() {
            return Err(BuildError::NoSink);
        }
        validate_sink_list(&self.sinks)?;
        let mut shard_sinks = Vec::with_capacity(self.shards);
        for _ in 0..self.shards {
            shard_sinks.push(self.sinks.clone());
        }
        self.build_with_shard_sinks(shard_sinks)
    }

    /// 用每 shard 独立的 sink 集合构建 logger.
    ///
    /// 参数：`shard_sinks` 长度必须等于 `self.shards`，每个 shard 至少一个 sink.
    /// 返回值：包含单 shard 或多 shard 的 [`Logger`] 包装.
    /// 该入口由 router 在需要每 shard 独立文件 sink 时使用；每 shard 的 sink 配置必须独立，
    /// 否则共享 sink 会承担并发调用.
    pub(crate) fn build_with_shard_sinks(
        self,
        shard_sinks: Vec<Vec<SinkConfig>>,
    ) -> Result<Logger, BuildError> {
        if self.queue_size == 0 {
            return Err(BuildError::ZeroQueue);
        }
        if self.queue_size > MAX_QUEUE_RECORDS {
            return Err(BuildError::InvalidSharding(format!(
                "queue_size {} exceeds limit {MAX_QUEUE_RECORDS}",
                self.queue_size
            )));
        }
        if self
            .max_queued_bytes
            .is_some_and(|bytes| bytes > MAX_QUEUE_BYTES)
        {
            return Err(BuildError::InvalidSharding(format!(
                "max_queued_bytes exceeds limit {MAX_QUEUE_BYTES}"
            )));
        }
        self.validate_sharding()?;
        if shard_sinks.len() != self.shards {
            return Err(BuildError::InvalidSharding(format!(
                "expected {} shard sink sets, got {}",
                self.shards,
                shard_sinks.len()
            )));
        }
        for sinks in &shard_sinks {
            if sinks.is_empty() {
                return Err(BuildError::NoSink);
            }
            validate_sink_list(sinks)?;
        }
        let queue_base = self.queue_size / self.shards;
        let queue_extra = self.queue_size % self.shards;
        let queue_bytes = self
            .max_queued_bytes
            .unwrap_or_else(|| self.queue_size.saturating_mul(256).max(1));
        if queue_bytes == 0 {
            return Err(BuildError::InvalidSharding(
                "max_queued_bytes must be > 0".to_string(),
            ));
        }
        let bytes_base = queue_bytes / self.shards;
        let bytes_extra = queue_bytes % self.shards;
        let rate_base = self
            .rate_limit
            .map(|limit| limit.max_per_second / self.shards as u32)
            .unwrap_or(0);
        let rate_extra = self
            .rate_limit
            .map(|limit| limit.max_per_second % self.shards as u32)
            .unwrap_or(0);
        // 所有 shard 共享一个 logger group id.
        // writer TLS 用它在任一 shard 内识别同一 logger 的重入.
        let group_id = NEXT_LOGGER_ID.fetch_add(1, Ordering::Relaxed);
        let mut cores = Vec::with_capacity(self.shards);
        for (shard, sinks) in shard_sinks.into_iter().enumerate() {
            let queue_size = queue_base + if shard < queue_extra { 1 } else { 0 };
            let queue_bytes = bytes_base + if shard < bytes_extra { 1 } else { 0 };
            let rate_limit = self.rate_limit.map(|_| RateLimit {
                max_per_second: rate_base + if (shard as u32) < rate_extra { 1 } else { 0 },
            });
            cores.push(self.build_core(
                sinks,
                queue_size,
                queue_bytes.max(1),
                rate_limit,
                shard,
                group_id,
            )?);
        }
        Ok(Logger::from_cores(cores))
    }

    /// 校验分片数与队列/限流预算是否匹配.
    ///
    /// 参数：无.
    /// 返回值：合法时 `Ok(())`；分片为 0、超过上限、或队列/限流不足以分片时返回错误.
    /// 说明：每个 shard 至少要有 1 条队列预算；限流也按 shard 拆分，避免总限额被放大.
    fn validate_sharding(&self) -> Result<(), BuildError> {
        if self.shards == 0 || self.shards > MAX_SHARDS {
            return Err(BuildError::InvalidSharding(format!(
                "shards must be in 1..={MAX_SHARDS}, got {}",
                self.shards
            )));
        }
        if self.shards > self.queue_size {
            return Err(BuildError::InvalidSharding(format!(
                "shards({}) must not exceed queue_size({})",
                self.shards, self.queue_size
            )));
        }
        if let Some(limit) = self.rate_limit {
            if limit.max_per_second < self.shards as u32 {
                return Err(BuildError::InvalidSharding(format!(
                    "rate_limit({}) must be >= shards({})",
                    limit.max_per_second, self.shards
                )));
            }
        }
        Ok(())
    }

    /// 构建单个 shard 的 core logger.
    ///
    /// 参数：`sinks` 是该 shard 的 sink 配置；`queue_size`/`queue_bytes` 是该 shard 的队列预算；
    /// `rate_limit` 是该 shard 的限流；`shard` 是分片序号，仅用于线程名；`group_id` 是所有
    /// shard 共享的 logger id.
    /// 返回值：已启动 writer 的 core logger.
    fn build_core(
        &self,
        sinks: Vec<SinkConfig>,
        queue_size: usize,
        queue_bytes: usize,
        rate_limit: Option<RateLimit>,
        shard: usize,
        group_id: u64,
    ) -> Result<CoreLogger, BuildError> {
        let (tx, rx) = bounded::<Msg>(queue_size);
        let shared = Arc::new(Shared {
            id: group_id,
            name: Arc::from(self.name.as_str()),
            json_lines: self.json_lines,
            level: AtomicI32::new(self.level.as_i32()),
            overflow: self.overflow,
            closed: AtomicBool::new(false),
            stop_requested: AtomicBool::new(false),
            active_producers: ProducerCounter::new(),
            alive: AtomicBool::new(true),
            stopped: AtomicBool::new(false),
            // fail-safe：writer 尚未发布结果前，任何并发读取都应看到“未成功”.
            finished_ok: AtomicBool::new(false),
            stop_lock: Mutex::new(()),
            sinks_lock: Mutex::new(()),
            queue_capacity: queue_size,
            max_queued_bytes: queue_bytes,
            queue_budget: QueueBudget::new(),
            batch_pool: Arc::new(BatchPool::default()),
            rate_limit: rate_limit.map(RateLimitState::new),
            sinks: ArcSwap::from_pointee(sinks),
            emitted: std::array::from_fn(|_| Counter::new()),
            dropped: Counter::new(),
            dropped_pre: Counter::new(),
            dropped_post: Counter::new(),
            delivered: Counter::new(),
            drop_reasons: std::array::from_fn(|_| Counter::new()),
            write_errors: Counter::new(),
            sink_panics: Counter::new(),
        });
        let worker_shared = Arc::clone(&shared);
        let thread_name = if self.shards == 1 {
            format!("flare-{}", shared.name)
        } else {
            format!("flare-{}-{shard}", shared.name)
        };
        thread::Builder::new()
            .name(thread_name)
            .stack_size(1 << 20)
            .spawn(move || run_worker(worker_shared, rx))
            .map_err(|error| BuildError::Spawn(error.to_string()))?;
        Ok(CoreLogger { shared, tx })
    }
}

/// 校验一组 sink 配置.
///
/// 参数：`sinks` 是同一 shard 的 sink 列表.
/// 返回值：合法时 `Ok(())`；空列表、重名或非法区间返回错误.
fn validate_sink_list(sinks: &[SinkConfig]) -> Result<(), BuildError> {
    if sinks.is_empty() {
        return Err(BuildError::NoSink);
    }
    let mut names = std::collections::HashSet::new();
    for sink in sinks {
        if let Some(message) = sink_config_error(sink) {
            return Err(BuildError::InvalidSink(message));
        }
        if !names.insert(sink.name.clone()) {
            return Err(BuildError::DuplicateSink(sink.name.clone()));
        }
    }
    Ok(())
}

/// 有界队列 logger 的单分片实现.
///
/// `Clone` 共享同一个队列和 writer.
/// 最后一个句柄释放时，writer 会排空队列、flush/close所有 sink 后退出；也可以显式调用
/// [`CoreLogger::close`].
#[derive(Clone)]
pub(crate) struct CoreLogger {
    shared: Arc<Shared>,
    tx: Sender<Msg>,
}

impl CoreLogger {
    /// 返回 logger/service 名.
    ///
    /// 参数：无.
    /// 返回值：共享名字的引用；不分配.
    pub fn name(&self) -> &str {
        &self.shared.name
    }

    /// 读取当前阈值.
    ///
    /// 参数：无.
    /// 返回值：核心级别；`Acquire` 保证看到最近一次 `set_level`.
    pub fn level(&self) -> Level {
        Level::from_i32(self.shared.level.load(Ordering::Acquire))
    }

    /// 原子更新阈值.
    ///
    /// 参数：`level` 是新阈值；返回值：无.
    /// 热路径读取用 `Acquire`，写入用 `Release`；更新只影响后续记录，不重排已入队记录.
    pub fn set_level(&self, level: Level) {
        self.shared.level.store(level.as_i32(), Ordering::Release);
    }

    /// 记录一次 layer 层识别出的重入丢弃.
    ///
    /// 参数：无；返回值：无.
    /// layer 在进入 `on_event` 前就拒绝了 writer 回写/自身递归，此时没有 Record 可走正常入队路径；
    /// 仍计入引擎 `DropReason::Reentrant`，让 `dropped = pre + post` 与按原因拆分保持可对账.
    pub(crate) fn count_reentrant(&self) {
        self.shared.drop_before_enqueue(DropReason::Reentrant, 1);
    }

    /// 判断某级别当前是否会被接受.
    ///
    /// 参数：`level` 是候选级别；返回值：`level >= 阈值 && 未关停`.
    /// 该检查是生产者快速路径，真正入队时还会再检查一次关闭状态.
    pub fn enabled(&self, level: Level) -> bool {
        level >= self.level() && !self.shared.closed.load(Ordering::Acquire)
    }

    /// 是否已开始关停.
    ///
    /// 参数：无.
    /// 返回值：`closed` 原子位；开始 close 后为 true，即使 writer 尚未完全退出.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// writer 是否仍存活.
    ///
    /// 参数：无.
    /// 返回值：`alive` 原子位；writer 完成收尾后为 false.
    pub fn alive(&self) -> bool {
        self.shared.alive.load(Ordering::Acquire)
    }

    /// 返回 channel 中当前消息数.
    ///
    /// 参数：无.
    /// 返回值：`Sender::len()`；包含控制消息，`Batch` 只算一条.
    /// 不额外维护pending 计数器，避免每条记录多两次原子操作.
    pub fn pending(&self) -> usize {
        self.tx.len()
    }

    /// 返回队列 + 在途批次占用的记录预算.
    ///
    /// 参数：无.
    /// 返回值：记录数（不是消息数）；用于健康快照与背压判断.
    pub fn queued_records(&self) -> usize {
        self.shared.queue_budget.get().0
    }

    /// 返回队列 + 在途批次占用的字节预算.
    ///
    /// 参数：无；返回值：估算字节数.
    pub fn queued_bytes(&self) -> usize {
        self.shared.queue_budget.get().1
    }

    /// 追加一条文本记录，target 自动使用 logger 名.
    ///
    /// 参数：`level` 是级别；`message` 是最终文本（不做模板渲染）；返回值：是否成功入队.
    /// 等价于 `log_record(Record::new(...).with_target(name))`.
    pub fn log(&self, level: Level, message: impl Into<String>) -> bool {
        self.log_record(Record::new(level, message).with_target(Arc::clone(&self.shared.name)))
    }

    /// 入队一条已构造记录.
    ///
    /// 参数：`record` 是拥有所有权的记录；返回值：级别过滤、丢弃、已关停时返回 false.
    /// 使用 logger 配置的 overflow 策略（可能是 `Block`）.
    pub fn log_record(&self, record: Record) -> bool {
        self.enqueue_record(record, self.shared.overflow)
    }

    /// 入队且永不阻塞，即使配置了 blocking overflow 策略.
    ///
    /// 参数：`record` 是记录；返回值：是否成功入队.tracing/async 适配器必须走这个入口：在运行时
    /// worker 上等待满队列会阻塞无关任务，单线程执行器还可能死锁.
    pub fn try_log_record(&self, record: Record) -> bool {
        self.enqueue_record(record, Overflow::Drop)
    }

    /// 入队一个已拼好的多行文本块.
    ///
    /// 参数：
    /// - `level`：块内所有行的统一级别；
    /// - `block`：以 `\n` 分隔的最终文本；不做模板渲染.
    ///
    /// 返回值：成功入队的行数；级别过滤、限流、超限、关停或队列满时为 0.
    ///
    /// 设计原因：逐条 `log_record` 每行至少一次队列 CAS/channel 操作；预格式化块把整块作为一条
    /// channel 消息，生产者成本由“行数 × 记录开销”降为一次原子预算 + 一次发送.
    /// 块内行数由 `str::lines()` 计算，因此末尾换行不会产生额外空行；空块返回 0.
    /// 行内控制字符仍由 sink 逐行转义，输出不会因为块而绕过物理行防线；JSON Lines 服务拒绝该入口，
    /// 应使用 [`Record::with_json`] 批量 API.
    pub fn log_preformatted_block(&self, level: Level, block: impl Into<String>) -> usize {
        self.enqueue_preformatted_block(block.into(), level, self.shared.overflow)
    }

    /// 预格式化块入队的完整路径.
    ///
    /// 参数：`block` 拥有整块文本；`level` 是统一级别；`overflow` 是满队列策略.
    /// 返回值：成功入队行数.
    /// 顺序与 [`Self::enqueue_record`] 一致：级别 → 尺寸 → 限流 → JSON 格式 → 重入 → 生产者门 →
    /// 预算 → channel 发送；整块原子准入，避免部分行落盘.
    fn enqueue_preformatted_block(&self, block: String, level: Level, overflow: Overflow) -> usize {
        let lines = block.lines().count();
        if lines == 0 || level < self.level() {
            return 0;
        }
        if block.len() > MAX_PREFORMATTED_BLOCK_BYTES {
            self.shared.drop_before_enqueue(DropReason::Oversize, lines);
            return 0;
        }
        if !self.shared.rate_allows_n(lines) {
            self.shared
                .drop_before_enqueue(DropReason::RateLimited, lines);
            return 0;
        }
        if self.shared.json_lines {
            self.shared.drop_before_enqueue(DropReason::Format, lines);
            return 0;
        }
        if is_writer_for(self.shared.id) {
            self.shared
                .drop_before_enqueue(DropReason::Reentrant, lines);
            return 0;
        }
        let cost = block
            .len()
            .saturating_add(lines.saturating_mul(PREFORMATTED_LINE_OVERHEAD));
        if !self.shared.begin_produce() {
            self.shared.drop_before_enqueue(DropReason::Shutdown, lines);
            return 0;
        }
        let reserve_started = Instant::now();
        let reserved = match overflow {
            Overflow::Drop => self.shared.reserve_records(lines, cost),
            Overflow::Block(timeout) => self.shared.reserve_records_blocking(lines, cost, timeout),
        };
        if !reserved {
            self.shared.end_produce();
            self.shared.drop_before_enqueue(DropReason::Overload, lines);
            return 0;
        }
        let message = Msg::PreformattedBlock {
            level,
            block,
            lines,
            cost,
        };
        let sent = match overflow {
            Overflow::Drop => match self.tx.try_send(message) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(_)) => Err(SendFailure::Overload),
                Err(TrySendError::Disconnected(_)) => Err(SendFailure::Dead),
            },
            Overflow::Block(timeout) => {
                let remaining = timeout.saturating_sub(reserve_started.elapsed());
                match self.tx.send_timeout(message, remaining) {
                    Ok(()) => Ok(()),
                    Err(SendTimeoutError::Timeout(_)) => Err(SendFailure::Overload),
                    Err(SendTimeoutError::Disconnected(_)) => Err(SendFailure::Dead),
                }
            }
        };
        self.shared.end_produce();
        match sent {
            Ok(()) => {
                self.shared.emitted[level.index()].add(lines as u64);
                lines
            }
            Err(SendFailure::Overload) => {
                self.shared.release_records(lines, cost);
                self.shared.drop_before_enqueue(DropReason::Overload, lines);
                0
            }
            Err(SendFailure::Dead) => {
                self.shared.release_records(lines, cost);
                self.shared
                    .drop_before_enqueue(DropReason::WorkerDead, lines);
                0
            }
        }
    }

    /// 单条记录入队的完整路径.
    ///
    /// 参数：`record` 是记录；`overflow` 是本次调用使用的满队列策略.
    /// 返回值：是否成功入队.
    /// 顺序：级别过滤 → 记录尺寸 → 限流 → 重入检查 → 生产者门 → 记录预算 → channel 发送.
    /// 每一步失败都走对应 `drop_before_enqueue`，确保每条失败记录可归因.
    fn enqueue_record(&self, record: Record, overflow: Overflow) -> bool {
        if record.level < self.level() {
            return false;
        }
        // 在这里只计算一次大小；writer 随消息收到该值，不再重新扫描每个字段.
        let cost = record_cost(&record);
        if cost > MAX_RECORD_BYTES {
            self.shared.drop_before_enqueue(DropReason::Oversize, 1);
            return false;
        }
        if !self.shared.rate_allows() {
            self.shared.drop_before_enqueue(DropReason::RateLimited, 1);
            return false;
        }
        if self.shared.json_lines && !record.json {
            self.shared.drop_before_enqueue(DropReason::Format, 1);
            return false;
        }
        if is_writer_for(self.shared.id) {
            // sink 在 writer 内回写同一个 logger.入队只在队列未满时安全；一旦满，
            // writer 会等待自己.直接丢弃并计入 reentrant.
            self.shared.drop_before_enqueue(DropReason::Reentrant, 1);
            return false;
        }
        // 先进入生产者临界区再检查 `closed`：close 会等待所有观察到 `closed=false` 的生产者
        // 离开该区后才发送 Stop，从而关闭“Stop 之后仍入队”的丢记录窗口.
        if !self.shared.begin_produce() {
            self.shared.drop_before_enqueue(DropReason::Shutdown, 1);
            return false;
        }
        let reserve_started = Instant::now();
        let reserved = match overflow {
            Overflow::Drop => self.shared.reserve_records(1, cost),
            Overflow::Block(timeout) => self.shared.reserve_records_blocking(1, cost, timeout),
        };
        if !reserved {
            self.shared.end_produce();
            self.shared.drop_before_enqueue(DropReason::Overload, 1);
            return false;
        }
        let level = record.level;
        let accepted = match overflow {
            Overflow::Drop => match self.tx.try_send(Msg::Record { record, cost }) {
                Ok(()) => {
                    self.shared.record_emitted(level);
                    true
                }
                Err(TrySendError::Full(_)) => {
                    self.shared.drop_before_enqueue(DropReason::Overload, 1);
                    false
                }
                Err(TrySendError::Disconnected(_)) => {
                    self.shared.drop_before_enqueue(DropReason::WorkerDead, 1);
                    false
                }
            },
            Overflow::Block(timeout) => {
                let remaining = timeout.saturating_sub(reserve_started.elapsed());
                match self
                    .tx
                    .send_timeout(Msg::Record { record, cost }, remaining)
                {
                    Ok(()) => {
                        self.shared.record_emitted(level);
                        true
                    }
                    Err(SendTimeoutError::Timeout(_)) => {
                        self.shared.drop_before_enqueue(DropReason::Overload, 1);
                        false
                    }
                    Err(SendTimeoutError::Disconnected(_)) => {
                        self.shared.drop_before_enqueue(DropReason::WorkerDead, 1);
                        false
                    }
                }
            }
        };
        if !accepted {
            self.shared.release_records(1, cost);
        }
        self.shared.end_produce();
        accepted
    }

    /// 创建一个可复用的预格式化批次.
    ///
    /// 参数：无.
    /// 返回值：从当前 logger 的批次池取出的空 [`RecordBatch`].
    /// 提交后继续 `push` 会优先复用 writer 回收的向量；适合高频循环中反复构造小批次.
    pub fn batch(&self) -> RecordBatch {
        RecordBatch::with_pool(Arc::clone(&self.shared.batch_pool), BATCH_MAX)
    }

    /// 提交一个可复用的预格式化批次.
    ///
    /// 参数：`batch` 是待提交批次；返回值：实际入队条数.
    /// 使用 logger 配置的队列满策略；可能按 `Overflow::Block` 阻塞.
    pub fn log_batch(&self, batch: &mut RecordBatch) -> usize {
        self.log_batch_with_overflow(batch, self.shared.overflow)
    }

    /// 提交一个批次，且永不按 `Overflow::Block` 阻塞.
    ///
    /// 参数：`batch` 是待提交批次；返回值：实际入队条数.
    /// 队列满时按 `Overflow::Drop` 语义立即丢弃并计数；适合 async/执行器线程.
    pub fn try_log_batch(&self, batch: &mut RecordBatch) -> usize {
        self.log_batch_with_overflow(batch, Overflow::Drop)
    }

    /// 提交一个可复用的预格式化批次.
    ///
    /// 参数：`batch` 是待提交批次；返回值：实际入队条数.
    /// 返回值：被级别过滤、关闭、限流、超大或队列预算不足的记录不计入.
    /// 提交后 `batch` 会被清空但保留容量；调用方可以继续 `push` 下一条记录.
    /// 设计原因：把“收集记录”和“按 `BATCH_MAX`/`BATCH_BYTES_MAX` 切批发送”分开，
    /// 高频调用方无需每次重新分配 `Vec`，也无需重复计算记录成本.
    fn log_batch_with_overflow(&self, batch: &mut RecordBatch, overflow: Overflow) -> usize {
        if batch.is_empty() {
            batch.bytes = 0;
            return 0;
        }
        let level = self.level();
        let closed = self.shared.closed.load(Ordering::Acquire);
        // 常见路径：批次已能放进一个队列消息，且没有逐条限流/过滤.
        // 直接把 `records` 交给 channel，避免再构造一个 `pending` Vec.
        if !closed
            && self.shared.rate_limit.is_none()
            && batch.records.len() <= BATCH_MAX
            && batch.bytes <= BATCH_BYTES_MAX
            && batch.min_level.is_some_and(|min_level| min_level >= level)
            && batch.max_cost <= MAX_RECORD_BYTES
        {
            let bytes = batch.bytes;
            let records = std::mem::take(&mut batch.records);
            batch.bytes = 0;
            batch.min_level = None;
            batch.max_cost = 0;
            return self.enqueue_batch_with_overflow(records, bytes, overflow);
        }

        let mut accepted = 0usize;
        let mut pending = self.shared.take_batch_vec(batch.len().min(BATCH_MAX));
        let mut bytes = 0usize;
        for record in batch.records.drain(..) {
            if record.level < level {
                continue;
            }
            if closed {
                self.shared.drop_before_enqueue(DropReason::Shutdown, 1);
                continue;
            }
            if self.shared.json_lines && !record.json {
                self.shared.drop_before_enqueue(DropReason::Format, 1);
                continue;
            }
            let cost = record_cost(&record);
            if cost > MAX_RECORD_BYTES {
                self.shared.drop_before_enqueue(DropReason::Oversize, 1);
                continue;
            }
            if !self.shared.rate_allows() {
                self.shared.drop_before_enqueue(DropReason::RateLimited, 1);
                continue;
            }
            if !pending.is_empty()
                && (pending.len() >= BATCH_MAX || bytes.saturating_add(cost) > BATCH_BYTES_MAX)
            {
                accepted +=
                    self.enqueue_batch_with_overflow(std::mem::take(&mut pending), bytes, overflow);
                pending = self.shared.take_batch_vec(BATCH_MAX);
                bytes = 0;
            }
            bytes = bytes.saturating_add(cost);
            pending.push(record);
        }
        if !pending.is_empty() {
            accepted += self.enqueue_batch_with_overflow(pending, bytes, overflow);
        } else {
            self.shared.recycle_batch(pending);
        }
        batch.clear();
        accepted
    }

    /// 批量入队，按 `BATCH_MAX`/`BATCH_BYTES_MAX` 切批.
    ///
    /// 参数：`records` 是任意记录迭代器.
    /// 返回值：实际入队条数；被级别过滤/关闭/限流/超大/预算不足的记录不计入.
    /// 使用 logger 配置的队列满策略；可能按 `Overflow::Block` 阻塞.
    pub fn log_many(&self, records: impl IntoIterator<Item = Record>) -> usize {
        self.log_many_with_overflow(records, self.shared.overflow)
    }

    /// 批量入队，且永不按 `Overflow::Block` 阻塞.
    ///
    /// 参数：`records` 是任意记录迭代器.
    /// 返回值：实际入队条数；队列满时按 `Overflow::Drop` 丢弃并计数.
    /// 适合 async/执行器线程或不能承受阻塞的生产者.
    pub fn try_log_many(&self, records: impl IntoIterator<Item = Record>) -> usize {
        self.log_many_with_overflow(records, Overflow::Drop)
    }

    /// 批量入队，按 `BATCH_MAX`/`BATCH_BYTES_MAX` 切批.
    ///
    /// 参数：`records` 是任意记录迭代器.
    /// 返回值：实际入队条数；被级别过滤/关闭/限流/超大/预算不足的记录不计入.
    /// 设计原因：一次 `log_many` 只做少量 channel 操作，是高吞吐/预格式化路径的核心；`cost` 随批次
    /// 携带，writer 不重复扫描字段.
    fn log_many_with_overflow(
        &self,
        records: impl IntoIterator<Item = Record>,
        overflow: Overflow,
    ) -> usize {
        let records = records.into_iter();
        let hint = records.size_hint().0;
        let mut accepted = 0usize;
        let mut batch = self.shared.take_batch_vec(hint.min(BATCH_MAX));
        let mut bytes = 0usize;
        // 与 log_batch_with_overflow 对齐：阈值/关闭状态在循环外各读一次。关闭的最终判定仍由
        // enqueue_batch_with_overflow 的生产者门完成，长迭代期间新到的 close 不会被漏掉.
        let level = self.level();
        let closed = self.shared.closed.load(Ordering::Acquire);
        for record in records {
            if record.level < level {
                continue;
            }
            if closed {
                self.shared.drop_before_enqueue(DropReason::Shutdown, 1);
                continue;
            }
            if self.shared.json_lines && !record.json {
                self.shared.drop_before_enqueue(DropReason::Format, 1);
                continue;
            }
            let cost = record_cost(&record);
            if cost > MAX_RECORD_BYTES {
                self.shared.drop_before_enqueue(DropReason::Oversize, 1);
                continue;
            }
            if !self.shared.rate_allows() {
                self.shared.drop_before_enqueue(DropReason::RateLimited, 1);
                continue;
            }
            if !batch.is_empty()
                && (batch.len() >= BATCH_MAX || bytes.saturating_add(cost) > BATCH_BYTES_MAX)
            {
                accepted +=
                    self.enqueue_batch_with_overflow(std::mem::take(&mut batch), bytes, overflow);
                batch = self.shared.take_batch_vec(BATCH_MAX);
                bytes = 0;
            }
            bytes = bytes.saturating_add(cost);
            batch.push(record);
        }
        if !batch.is_empty() {
            accepted += self.enqueue_batch_with_overflow(batch, bytes, overflow);
        } else {
            self.shared.recycle_batch(batch);
        }
        accepted
    }

    /// 发送一个已切好的批次.
    ///
    /// 参数：`batch` 是本批记录；`cost` 是生产者累计的估算字节数.
    /// 返回值：成功入队条数.
    /// 调用方保证批次非空且每条记录已通过级别/尺寸/限流检查；这里负责重入检查、生产者门、记录预算与
    /// channel 发送.
    fn enqueue_batch_with_overflow(
        &self,
        batch: Vec<Record>,
        cost: usize,
        overflow: Overflow,
    ) -> usize {
        let count = batch.len();
        if count == 0 {
            return 0;
        }
        if is_writer_for(self.shared.id) {
            self.shared
                .drop_before_enqueue(DropReason::Reentrant, count);
            self.shared.recycle_batch(batch);
            return 0;
        }
        if !self.shared.begin_produce() {
            self.shared.drop_before_enqueue(DropReason::Shutdown, count);
            self.shared.recycle_batch(batch);
            return 0;
        }
        let reserve_started = Instant::now();
        let reserved = match overflow {
            Overflow::Drop => self.shared.reserve_records(count, cost),
            Overflow::Block(timeout) => self.shared.reserve_records_blocking(count, cost, timeout),
        };
        if !reserved {
            self.shared.end_produce();
            self.shared.drop_before_enqueue(DropReason::Overload, count);
            self.shared.recycle_batch(batch);
            return 0;
        }
        let mut levels = [0u64; 5];
        for record in &batch {
            levels[record.level.index()] += 1;
        }
        let message = Msg::Batch {
            records: batch,
            cost,
        };
        let sent = match overflow {
            Overflow::Drop => match self.tx.try_send(message) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(message)) => {
                    self.recycle_batch_message(message);
                    Err(SendFailure::Overload)
                }
                Err(TrySendError::Disconnected(message)) => {
                    self.recycle_batch_message(message);
                    Err(SendFailure::Dead)
                }
            },
            Overflow::Block(timeout) => {
                let remaining = timeout.saturating_sub(reserve_started.elapsed());
                match self.tx.send_timeout(message, remaining) {
                    Ok(()) => Ok(()),
                    Err(SendTimeoutError::Timeout(message)) => {
                        self.recycle_batch_message(message);
                        Err(SendFailure::Overload)
                    }
                    Err(SendTimeoutError::Disconnected(message)) => {
                        self.recycle_batch_message(message);
                        Err(SendFailure::Dead)
                    }
                }
            }
        };
        self.shared.end_produce();
        match sent {
            Ok(()) => {
                for (index, value) in levels.into_iter().enumerate() {
                    if value > 0 {
                        self.shared.emitted[index].add(value);
                    }
                }
                count
            }
            Err(SendFailure::Overload) => {
                self.shared.release_records(count, cost);
                self.shared.drop_before_enqueue(DropReason::Overload, count);
                0
            }
            Err(SendFailure::Dead) => {
                self.shared.release_records(count, cost);
                self.shared
                    .drop_before_enqueue(DropReason::WorkerDead, count);
                0
            }
        }
    }

    /// 回收发送失败时 channel 返回的批次向量.
    ///
    /// 参数：`message` 是 `try_send`/`send_timeout` 返回给调用方的消息.
    /// 返回值：无.
    /// 只有 `Msg::Batch` 持有可复用的 `Vec`；控制消息直接丢弃.
    fn recycle_batch_message(&self, message: Msg) {
        if let Msg::Batch { records, .. } = message {
            self.shared.recycle_batch(records);
        }
    }

    /// fork 子进程：标记 runtime 惰性并丢弃继承的 sink 句柄.
    ///
    /// 参数：无.
    /// 返回值：无.
    /// 子进程不存在 writer 线程；调用 flush/close 会重复父进程缓冲数据，因此这里只设置终止状态并让
    /// sink 关闭 fd 而不写出.
    pub fn discard_after_fork(&self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.stop_requested.store(true, Ordering::Release);
        self.shared.alive.store(false, Ordering::Release);
        self.shared.stopped.store(true, Ordering::Release);
        // 子进程没有自己的待处理记录；flush/close 都是空操作.
        self.shared.finished_ok.store(true, Ordering::Release);
        for config in self.shared.sinks.load_full().iter() {
            config.sink.discard_after_fork();
        }
    }

    /// 刷新所有 sink.
    ///
    /// 返回 true 表示 Flush 消息已入队且 writer 在同一个 deadline 内回 ack.timeout 同时覆盖“入队等
    /// 待”和“等待 ack”，不会出现最坏 2×timeout.
    /// 刷新所有 sink（队列屏障）.
    ///
    /// 参数：`timeout` 是入队等待与 ack 等待共享的总预算.
    /// 返回值：true 表示 Flush 消息已入队且 writer 在 deadline 内回 ack；false 表示超时/关停 writer
    /// 已死/调用线程是自己的 writer.
    pub fn flush(&self, timeout: Duration) -> bool {
        if is_writer_for(self.shared.id) {
            return false;
        }
        if self.shared.stopped.load(Ordering::Acquire) {
            // 关停已经走过最终 flush/close；如实返回那次关停的结果，而不是无条件成功
            // （否则一次失败的 close 之后，所有健康检查 flush 都会被误报为成功）.
            return self.shared.finished_ok.load(Ordering::Acquire);
        }
        let deadline = deadline_after(timeout);
        let (ack_tx, ack_rx) = bounded(1);
        if self
            .tx
            .send_timeout(Msg::Flush(ack_tx), remaining(deadline))
            .is_err()
        {
            return false;
        }
        ack_rx.recv_timeout(remaining(deadline)).unwrap_or(false)
    }

    /// 重开所有 sink 的活动资源.
    ///
    /// 参数：`timeout` 是发送与 ack 的总预算.
    /// 返回值：true 表示 writer 已处理 Reopen；false表示已关停、writer 已死或超时.
    /// 设计原因：外部 logrotate 后需要关闭旧 fd 再打开新路径，该操作必须由 writer 串行执行，避免与
    /// 在途批次并发.
    pub fn reopen(&self, timeout: Duration) -> bool {
        if is_writer_for(self.shared.id)
            || self.shared.closed.load(Ordering::Acquire)
            || !self.shared.alive.load(Ordering::Acquire)
        {
            return false;
        }
        let deadline = deadline_after(timeout);
        let (ack_tx, ack_rx) = bounded(1);
        if self
            .tx
            .send_timeout(Msg::Reopen(ack_tx), remaining(deadline))
            .is_err()
        {
            return false;
        }
        ack_rx.recv_timeout(remaining(deadline)).unwrap_or(false)
    }

    /// 停止接收新记录、排空队列、flush/close 所有 sink，并等待 writer 退出.
    ///
    /// 参数：`timeout` 是“等待在途生产者 + 投递 Stop + 等待 ack”共享的总预算.
    /// 返回值：结构化 `CloseReport`；`timed_out=true` 表示可以稍后重试，`ok=false, timed_out=false`
    /// 表示 flush/close 真正失败.
    /// 设计原因：只返回 bool 无法区分“writer 太忙”和“sink 出错”，运维需要不同处置.
    pub fn close_with_report(&self, timeout: Duration) -> CloseReport {
        if is_writer_for(self.shared.id) {
            return CloseReport::from_ok(false);
        }
        let _guard = self
            .shared
            .stop_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.shared.stopped.load(Ordering::Acquire) {
            // 幂等调用必须返回第一次关停的真实结果；否则失败过一次的 close 会被后续
            // 调用粉饰成成功.
            return CloseReport::from_ok(self.shared.finished_ok.load(Ordering::Acquire));
        }
        self.shared.closed.store(true, Ordering::SeqCst);
        let deadline = deadline_after(timeout);
        // 在所有可能观察到 `closed=false` 的生产者离开发送临界区之前，不发布
        // stop_requested / Stop；否则 writer 可能已排空退出，而生产者正准备发送记录.
        if !self.shared.wait_for_producers(deadline) {
            return CloseReport::timeout();
        }
        self.shared.stop_requested.store(true, Ordering::Release);
        let (ack_tx, ack_rx) = bounded(1);
        if self.tx.try_send(Msg::Stop(ack_tx)).is_err() {
            // 队列满时不要阻塞在 send 上：把完整 timeout 留给 writer 排空队列并自行收尾.
            loop {
                if !self.shared.alive.load(Ordering::Acquire) {
                    self.shared.stopped.store(true, Ordering::Release);
                    return CloseReport::from_ok(self.shared.finished_ok.load(Ordering::Acquire));
                }
                if Instant::now() >= deadline {
                    return CloseReport::timeout();
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
        match ack_rx.recv_timeout(remaining(deadline)) {
            Ok(ok) => {
                // ack 已到：ok=false 表示 flush/close 失败，必须如实返回 false；
                // 但 writer 已经不会再接受记录，stopped 仍置位，后续 close 返回同一结果.
                self.shared.stopped.store(true, Ordering::Release);
                CloseReport::from_ok(ok)
            }
            Err(RecvTimeoutError::Timeout) => {
                // ack 丢失但 writer 已退出：用 finished_ok 判断它是否真的完成 flush/close，
                // 不能仅凭 alive=false 就报成功.
                if !self.shared.alive.load(Ordering::Acquire) {
                    self.shared.stopped.store(true, Ordering::Release);
                    CloseReport::from_ok(self.shared.finished_ok.load(Ordering::Acquire))
                } else {
                    CloseReport::timeout()
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                // writer 丢弃了 ack sender 却没设置 alive=false（不应发生，但按失败而不是超时
                // 上报）.
                if !self.shared.alive.load(Ordering::Acquire) {
                    self.shared.stopped.store(true, Ordering::Release);
                }
                CloseReport::from_ok(false)
            }
        }
    }

    /// 动态添加 sink；名字重复时返回 false，不改变现有 sink 表.
    ///
    /// 参数：`config` 是待添加配置.
    /// 返回值：是否成功.
    /// 构造期的 `LoggerBuilder` 已拒绝重名；运行期必须保持同样约束，否则统计/健康/排障会出现两个同
    /// 名 sink.
    pub fn try_add_sink(&self, config: SinkConfig) -> bool {
        if is_writer_for(self.shared.id) {
            // 与 flush/close 一致：writer 线程里不能再改 sink 表，否则可能在写 sink 的
            // 同时 close/替换它，甚至对不可重入的 Mutex 自死锁.
            return false;
        }
        if sink_config_error(&config).is_some() {
            return false;
        }
        // 与 close 串行化.否则 sink 可能在 writer 已为 close_sinks() 拍下表快照之后发布，
        // 永远得不到 close.
        let _stop_guard = self
            .shared
            .stop_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _guard = self
            .shared
            .sinks_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // 已经进入 close 后不能挂新 sink：writer 可能已经做过 close 快照，新 sink 永远
        // 不会收到 write/flush/close，等于泄漏资源.
        if self.shared.closed.load(Ordering::Acquire) || !self.shared.alive.load(Ordering::Acquire)
        {
            return false;
        }
        if self
            .shared
            .sinks
            .load()
            .iter()
            .any(|existing| existing.name == config.name)
        {
            return false;
        }
        self.shared.sinks.rcu(move |current| {
            let mut next = current.as_ref().clone();
            next.push(config.clone());
            Arc::new(next)
        });
        true
    }

    /// 替换 sink 表；被移除的 sink **不会**被关闭.
    ///
    /// 参数：`sinks` 是新表.
    /// 返回值：表是否已替换；writer 线程内调用、已关停或重名时 false.
    /// 共享 sink 的生命周期归创建者，库不擅自 close 仍被其他 logger 使用的 sink.
    pub fn set_sinks(&self, sinks: Vec<SinkConfig>) -> bool {
        self.replace_sinks(sinks)
    }

    /// 替换 sink 表的公共实现.
    ///
    /// 参数：`sinks` 是新表.
    /// 返回值：是否成功替换.
    /// 实现持有 `stop_lock → sinks_lock`，与 close/并发替换串行化；校验重名和空区间，发布用 ArcSwap
    /// 保证读路径无锁.
    fn replace_sinks(&self, sinks: Vec<SinkConfig>) -> bool {
        if is_writer_for(self.shared.id) {
            return false;
        }
        let _stop_guard = self
            .shared
            .stop_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _guard = self
            .shared
            .sinks_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // close 后替换 sink 表会让新 sink 逃过 close 快照，直接拒绝.
        if self.shared.closed.load(Ordering::Acquire) || !self.shared.alive.load(Ordering::Acquire)
        {
            return false;
        }
        // 运行期替换必须维持构造期同样的重名约束；否则统计/健康检查/运维排障会出现
        // 两个同名 sink，而且 try_add_sink 后续无法再补回这个名字.
        let mut names = std::collections::HashSet::with_capacity(sinks.len());
        for sink in &sinks {
            if sink_config_error(sink).is_some() || !names.insert(sink.name.clone()) {
                return false;
            }
        }
        self.shared.sinks.store(Arc::new(sinks));
        true
    }

    /// 读取当前 sink 表快照，供多 shard 事务回滚.
    ///
    /// 参数：无；返回值：共享 sink 表.
    pub(crate) fn sinks_snapshot(&self) -> Arc<Vec<SinkConfig>> {
        self.shared.sinks.load_full()
    }

    /// 回滚多 shard add_sink 已经应用的部分.
    ///
    /// 参数：`name` 是待移除 sink 名.
    /// 返回值：是否移除了该项.
    /// 只用于事务回滚，不 close sink；新 sink 仍由调用方持有，writer 可能已见过它并完成当前批次.
    pub(crate) fn rollback_added_sink(&self, name: &str) -> bool {
        if is_writer_for(self.shared.id) {
            return false;
        }
        let _stop_guard = self
            .shared
            .stop_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _guard = self
            .shared
            .sinks_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self
            .shared
            .sinks
            .load()
            .iter()
            .any(|sink| sink.name == name)
        {
            return false;
        }
        let name = name.to_string();
        self.shared.sinks.rcu(move |current| {
            let next: Vec<SinkConfig> = current
                .iter()
                .filter(|sink| sink.name != name)
                .cloned()
                .collect();
            Arc::new(next)
        });
        true
    }

    /// 恢复一个 shard 的旧 sink 表，供多 shard 事务回滚.
    ///
    /// 参数：`previous` 是变更前的共享表.
    /// 返回值：无.
    /// 不 close 任何 sink: 回滚目标是让所有 shard 回到同一旧状态；被替换掉的新 sink 仍由调用方持有.
    pub(crate) fn restore_sinks(&self, previous: Arc<Vec<SinkConfig>>) {
        let _stop_guard = self
            .shared
            .stop_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _guard = self
            .shared
            .sinks_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.shared.sinks.store(previous);
    }

    /// 在 sink 表成功替换后，异步 close 被移除的 sink.
    ///
    /// 参数：`previous` 是替换前的表.
    /// 返回值：无.
    /// 交给 writer 在当前批次之后 close；
    /// 队列满且 writer 仍存活时跳过，资源由调用方/后续 flush 处理.
    /// writer 已退出时调用线程直接 close 是安全的.
    pub(crate) fn close_removed_sinks(&self, previous: &[SinkConfig]) {
        let current = self.shared.sinks.load_full();
        for old in previous {
            if current
                .iter()
                .any(|kept| Arc::ptr_eq(&kept.sink, &old.sink))
            {
                continue;
            }
            if self
                .tx
                .try_send(Msg::CloseSink(Arc::clone(&old.sink)))
                .is_err()
                && !self.shared.alive.load(Ordering::Acquire)
            {
                let _ = old.sink.close();
            }
        }
    }

    /// 读取当前健康快照.
    ///
    /// 参数：无.
    /// 返回值：只做原子 load + sink 表快照，不阻塞 writer；`pending` 包含控制消息，`queued_records`
    /// 才是记录预算占用.
    pub fn snapshot(&self) -> LoggerSnapshot {
        self.shared.snapshot(self.tx.len())
    }
}

/// 批量发送失败分类.
///
/// `Overload` 队列满/超时；`Dead` channel 断开/writer 死亡.
#[derive(Clone, Copy)]
enum SendFailure {
    /// 队列满或 Block 超时.
    Overload,
    /// channel 断开/writer 死亡.
    Dead,
}

/// 为线程分配 shard 的全局游标.
///
/// 只在每个线程第一次写日志时访问；之后从 TLS 读取，保证同一线程固定写同一个 shard，
/// 从而保留线程内 FIFO.
static NEXT_SHARD_HINT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// 当前线程的 shard 提示值；`usize::MAX` 表示尚未分配.
    static SHARD_HINT: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// 返回当前线程应使用的 shard 下标.
///
/// 参数：`shards` 是 shard 数量.
/// 返回值：`0..shards`；同一线程对同一 `shards` 稳定.
/// 线程首次调用用全局游标轮转分配，避免所有线程集中到一个 shard；TLS 销毁期间回退到 0.
fn thread_shard(shards: usize) -> usize {
    SHARD_HINT
        .try_with(|cell| {
            let hint = cell.get();
            if hint != usize::MAX {
                return hint % shards;
            }
            let hint = NEXT_SHARD_HINT.fetch_add(1, Ordering::Relaxed);
            cell.set(hint);
            hint % shards
        })
        .unwrap_or(0)
}

/// 多 shard logger 句柄.
///
/// 字段：`cores` 至少一个 core；默认单 shard 时长度为 1，保持原单 writer 行为.
/// 生产者按线程固定选 shard；`flush`/`close`/`reopen` 使用共享 deadline，但按剩余 shard 数均分
/// 预算，避免第一个慢 shard 让后续 shard 立即超时；快照和关停报告按 shard 聚合.
#[derive(Clone)]
#[non_exhaustive]
pub struct Logger {
    cores: Arc<[CoreLogger]>,
    /// 串行化跨 shard 的 sink 表变更.
    ///
    /// 单 core 有 `stop_lock → sinks_lock`；多 shard 还需要外层锁，否则两个并发 add/set 可以在
    /// shard 之间交错，留下“半新半旧”的表. writer 线程检查在拿锁之前完成，避免 sink 回调里改表时
    /// 与关停互相等待.
    sink_ops: Arc<Mutex<()>>,
}

impl Logger {
    /// 由已构建的 core 列表创建包装.
    ///
    /// 参数：`cores` 非空；返回值：logger 句柄.
    fn from_cores(cores: Vec<CoreLogger>) -> Self {
        debug_assert!(!cores.is_empty());
        Self {
            cores: cores.into(),
            sink_ops: Arc::new(Mutex::new(())),
        }
    }

    /// 返回当前线程对应的 core.
    ///
    /// 参数：无；返回值：单 shard 直接返回 0，多 shard 按线程稳定选择.
    #[inline]
    fn current(&self) -> &CoreLogger {
        if self.cores.len() == 1 {
            &self.cores[0]
        } else {
            &self.cores[thread_shard(self.cores.len())]
        }
    }

    /// 当前线程是否是本 logger 任一 shard 的 writer.
    ///
    /// 参数：无；返回值：writer 线程内为 true.
    /// 所有 shard 共享 group id；在拿 `sink_ops` 前检查，避免 writer 内 sink 回调改表时与关停互等.
    #[inline]
    fn is_writer_thread(&self) -> bool {
        self.cores
            .first()
            .is_some_and(|core| is_writer_for(core.shared.id))
    }

    /// 返回 shard 数量.
    ///
    /// 参数：无；返回值：`1` 表示单 writer.
    pub fn shard_count(&self) -> usize {
        self.cores.len()
    }

    /// 测试专用：访问第一个 shard 的共享状态.
    ///
    /// 参数：无；返回值：第一个 core 的 `Shared`.
    /// 只用于同模块生命周期测试；生产代码应通过公开方法读取状态.
    #[cfg(test)]
    fn shared(&self) -> &Shared {
        &self.cores[0].shared
    }

    /// 返回 logger/service 名.
    ///
    /// 参数：无；返回值：共享名字引用.
    pub fn name(&self) -> &str {
        self.cores[0].name()
    }

    /// 读取当前阈值.
    ///
    /// 参数：无；返回值：所有 shard 共享的级别值.
    pub fn level(&self) -> Level {
        self.cores[0].level()
    }

    /// 原子更新所有 shard 的阈值.
    ///
    /// 参数：`level` 是新阈值；返回值：无.
    pub fn set_level(&self, level: Level) {
        for core in self.cores.iter() {
            core.set_level(level);
        }
    }

    /// 记录一次 layer 层识别出的重入丢弃.
    ///
    /// 参数：无；返回值：无.
    /// 计到当前线程对应 shard；重入事件本身没有可携带的记录/目标 shard 信息.
    pub(crate) fn count_reentrant(&self) {
        self.current().count_reentrant();
    }

    /// 判断某级别当前是否会被接受.
    ///
    /// 参数：`level` 是候选级别；返回值：级别达到阈值且未关停.
    pub fn enabled(&self, level: Level) -> bool {
        self.current().enabled(level)
    }

    /// 是否已开始关停.
    ///
    /// 参数：无；返回值：任一 shard 已关停即返回 true.
    pub fn is_closed(&self) -> bool {
        self.cores.iter().any(|core| core.is_closed())
    }

    /// writer 是否仍存活.
    ///
    /// 参数：无；返回值：任一 shard writer 存活为 true.
    /// 用于实例锁释放：必须等所有 writer 退出后才允许其他进程写同一路径.
    pub fn alive(&self) -> bool {
        self.cores.iter().any(|core| core.alive())
    }

    /// 返回 channel 中当前消息数.
    ///
    /// 参数：无；返回值：所有 shard 消息数之和；`Batch` 只算一条.
    pub fn pending(&self) -> usize {
        self.cores.iter().map(|core| core.pending()).sum()
    }

    /// 返回队列与在途批次的记录预算占用.
    ///
    /// 参数：无；返回值：所有 shard 预算占用之和.
    pub fn queued_records(&self) -> usize {
        self.cores.iter().map(|core| core.queued_records()).sum()
    }

    /// 返回队列与在途批次的字节预算占用.
    ///
    /// 参数：无；返回值：所有 shard 字节预算占用之和.
    pub fn queued_bytes(&self) -> usize {
        self.cores.iter().map(|core| core.queued_bytes()).sum()
    }

    /// 追加一条文本记录.
    ///
    /// 参数：`level` 是级别；`message` 是最终文本；返回值：是否成功入队.
    pub fn log(&self, level: Level, message: impl Into<String>) -> bool {
        self.current().log(level, message)
    }

    /// 入队一条已构造记录.
    ///
    /// 参数：`record` 是拥有所有权的记录；返回值：级别过滤/丢弃/关停时 false.
    pub fn log_record(&self, record: Record) -> bool {
        self.current().log_record(record)
    }

    /// 入队且永不阻塞.
    ///
    /// 参数：`record` 是记录；返回值：是否成功入队.
    pub fn try_log_record(&self, record: Record) -> bool {
        self.current().try_log_record(record)
    }

    /// 创建一个可复用的预格式化批次.
    ///
    /// 参数：无；返回值：当前 shard 的批次；提交时应由同一线程调用 [`Self::log_batch`].
    pub fn batch(&self) -> RecordBatch {
        self.current().batch()
    }

    /// 提交一个可复用的预格式化批次.
    ///
    /// 参数：`batch` 是待提交批次；返回值：实际入队条数.
    pub fn log_batch(&self, batch: &mut RecordBatch) -> usize {
        self.current().log_batch(batch)
    }

    /// 提交一个批次，且永不按 `Overflow::Block` 阻塞.
    ///
    /// 参数：`batch` 是待提交批次；返回值：实际入队条数.
    pub fn try_log_batch(&self, batch: &mut RecordBatch) -> usize {
        self.current().try_log_batch(batch)
    }

    /// 批量入队.
    ///
    /// 参数：`records` 是任意记录迭代器；返回值：实际入队条数.
    /// 当前线程固定选择一个 shard；跨线程不保证全局 FIFO.
    pub fn log_many(&self, records: impl IntoIterator<Item = Record>) -> usize {
        self.current().log_many(records)
    }

    /// 批量入队，且永不按 `Overflow::Block` 阻塞.
    ///
    /// 参数：`records` 是任意记录迭代器；返回值：实际入队条数.
    pub fn try_log_many(&self, records: impl IntoIterator<Item = Record>) -> usize {
        self.current().try_log_many(records)
    }

    /// 入队一个已拼好的多行文本块（当前 shard）.
    ///
    /// 参数：`level` 是块内统一级别；`block` 是以 `\n` 分隔的最终文本.
    /// 返回值：成功入队的行数.
    /// 适合把批处理结果/外部采集器输出直接落盘；JSON Lines 服务请使用 `Record::with_json` 批量 API.
    pub fn log_preformatted_block(&self, level: Level, block: impl Into<String>) -> usize {
        self.current().log_preformatted_block(level, block)
    }

    /// fork 子进程：丢弃所有 shard 的继承句柄.
    ///
    /// 参数：无；返回值：无.
    pub fn discard_after_fork(&self) {
        for core in self.cores.iter() {
            core.discard_after_fork();
        }
    }

    /// 刷新所有 shard.
    ///
    /// 参数：`timeout` 是所有 shard 共享的总预算；返回值：全部成功才 true.
    /// 每个 shard 只拿到“剩余时间 / 剩余 shard 数”，避免第一个慢 shard 耗尽全部预算.
    pub fn flush(&self, timeout: Duration) -> bool {
        let deadline = deadline_after(timeout);
        let mut ok = true;
        let shards = self.cores.len();
        for (index, core) in self.cores.iter().enumerate() {
            ok &= core.flush(shard_budget(deadline, shards - index));
        }
        ok
    }

    /// 重开所有 shard 的 sink.
    ///
    /// 参数：`timeout` 是所有 shard 共享的总预算；返回值：全部成功才 true.
    /// 每个 shard 只拿到公平份额，避免前面的 shard 让后续 shard 立即超时.
    pub fn reopen(&self, timeout: Duration) -> bool {
        let deadline = deadline_after(timeout);
        let mut ok = true;
        let shards = self.cores.len();
        for (index, core) in self.cores.iter().enumerate() {
            ok &= core.reopen(shard_budget(deadline, shards - index));
        }
        ok
    }

    /// 停止接收新记录并关闭所有 shard.
    ///
    /// 参数：`timeout` 是总预算；返回值：全部 shard 成功关停才 true.
    pub fn close(&self, timeout: Duration) -> bool {
        self.close_with_report(timeout).ok
    }

    /// 停止接收新记录并返回结构化关停报告.
    ///
    /// 参数：`timeout` 是所有 shard 共享的总预算.
    /// 返回值：任一 shard 超时则 `timed_out=true`；任一 sink 失败则 `ok=false`.
    /// 每个 shard 只获得公平份额；前面 shard 未用完的时间会留给后续 shard，而不是让后续
    /// `recv_timeout(0)` 立即超时.
    pub fn close_with_report(&self, timeout: Duration) -> CloseReport {
        if self.is_writer_thread() {
            return CloseReport::from_ok(false);
        }
        // 与 sink 表变更串行化: 否则 add/set 可能把 sink 发布到已经做过 close 快照的 shard.
        let _ops = self
            .sink_ops
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let deadline = deadline_after(timeout);
        let mut report = CloseReport::from_ok(true);
        let shards = self.cores.len();
        for (index, core) in self.cores.iter().enumerate() {
            let shard = core.close_with_report(shard_budget(deadline, shards - index));
            report.ok &= shard.ok;
            report.timed_out |= shard.timed_out;
        }
        report
    }

    /// 尝试给所有 shard 增加一个 sink.
    ///
    /// 参数：`config` 是 sink 配置；返回值：全部 shard 接受才 true.
    /// 任一 shard 拒绝时回滚已应用的 shard，避免 sink 表“半新半旧”.
    pub fn try_add_sink(&self, config: SinkConfig) -> bool {
        if self.is_writer_thread() {
            return false;
        }
        let _ops = self
            .sink_ops
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut applied = 0usize;
        for core in self.cores.iter() {
            if core.try_add_sink(config.clone()) {
                applied += 1;
                continue;
            }
            for prior in &self.cores[..applied] {
                let _ = prior.rollback_added_sink(&config.name);
            }
            return false;
        }
        true
    }

    /// 给所有 shard 增加一个 sink.
    ///
    /// 参数：`config` 是 sink 配置；返回值：无.
    /// 使用事务化的 [`Self::try_add_sink`]，失败时不会留下部分 shard.
    pub fn add_sink(&self, config: SinkConfig) {
        let _ = self.try_add_sink(config);
    }

    /// 替换所有 shard 的 sink 表.
    ///
    /// 参数：`sinks` 是新的 sink 列表；返回值：全部 shard 成功才 true.
    /// 失败时恢复已替换 shard 的旧表，避免 shard 之间 sink 列表不一致.
    pub fn set_sinks(&self, sinks: Vec<SinkConfig>) -> bool {
        self.replace_all_sinks(sinks, false)
    }

    /// 替换 sink 表并关闭被移除的 sink.
    ///
    /// 参数：`sinks` 是新的 sink 列表；返回值：全部 shard 成功才 true.
    /// 先在所有 shard 发布新表；只有全部成功后才异步 close 被移除 sink，避免回滚时关闭仍被旧表
    /// 引用的资源.
    pub fn set_sinks_with_close_removed(&self, sinks: Vec<SinkConfig>) -> bool {
        self.replace_all_sinks(sinks, true)
    }

    /// 多 shard sink 表替换事务.
    ///
    /// 参数：`sinks` 是新表；`close_removed` 表示成功后是否 close 旧表独有的 sink.
    /// 返回值：是否全部替换成功.
    fn replace_all_sinks(&self, sinks: Vec<SinkConfig>, close_removed: bool) -> bool {
        if self.is_writer_thread() {
            return false;
        }
        let _ops = self
            .sink_ops
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous: Vec<Arc<Vec<SinkConfig>>> =
            self.cores.iter().map(CoreLogger::sinks_snapshot).collect();
        let mut applied = 0usize;
        for core in self.cores.iter() {
            if core.set_sinks(sinks.clone()) {
                applied += 1;
                continue;
            }
            for (prior, old) in self.cores[..applied].iter().zip(&previous[..applied]) {
                prior.restore_sinks(Arc::clone(old));
            }
            return false;
        }
        if close_removed {
            for (core, old) in self.cores.iter().zip(previous.iter()) {
                core.close_removed_sinks(old);
            }
        }
        true
    }

    /// 返回所有 shard 的独立快照.
    ///
    /// 参数：无.
    /// 返回值：按 shard 顺序排列的快照；单 shard 时长度为 1.
    /// 用于确认负载是否真的分散到多个 writer，以及定位某个 shard 的丢弃/写失败.
    pub fn shard_snapshots(&self) -> Vec<LoggerSnapshot> {
        self.cores.iter().map(|core| core.snapshot()).collect()
    }

    /// 读取聚合健康快照.
    ///
    /// 参数：无；返回值：所有 shard 的计数和 sink 统计求和；`sinks` 按名字合并.
    pub fn snapshot(&self) -> LoggerSnapshot {
        if self.cores.len() == 1 {
            return self.cores[0].snapshot();
        }
        let mut snapshot = self.cores[0].snapshot();
        for core in self.cores.iter().skip(1) {
            merge_snapshot(&mut snapshot, core.snapshot());
        }
        snapshot.closed = self.cores.iter().any(|core| core.is_closed());
        snapshot.alive = self.cores.iter().any(|core| core.alive());
        snapshot
    }
}

/// 把一个 shard 的快照合并到聚合快照.
///
/// 参数：`into` 是累计快照；`from` 是下一个 shard 的快照.
/// 返回值：无.
/// 计数按记录语义相加；同名 sink 的 writes/errors 等相加，`compression_jobs` 求和.
fn merge_snapshot(into: &mut LoggerSnapshot, from: LoggerSnapshot) {
    into.queue_capacity = into.queue_capacity.saturating_add(from.queue_capacity);
    into.queue_byte_capacity = into
        .queue_byte_capacity
        .saturating_add(from.queue_byte_capacity);
    into.queued_records = into.queued_records.saturating_add(from.queued_records);
    into.queued_bytes = into.queued_bytes.saturating_add(from.queued_bytes);
    into.peak_queued_records = into
        .peak_queued_records
        .saturating_add(from.peak_queued_records);
    into.peak_queued_bytes = into
        .peak_queued_bytes
        .saturating_add(from.peak_queued_bytes);
    into.pending = into.pending.saturating_add(from.pending);
    for (index, value) in from.emitted.into_iter().enumerate() {
        into.emitted[index] = into.emitted[index].saturating_add(value);
    }
    into.delivered = into.delivered.saturating_add(from.delivered);
    into.dropped = into.dropped.saturating_add(from.dropped);
    into.dropped_pre = into.dropped_pre.saturating_add(from.dropped_pre);
    into.dropped_post = into.dropped_post.saturating_add(from.dropped_post);
    into.write_errors = into.write_errors.saturating_add(from.write_errors);
    into.sink_panics = into.sink_panics.saturating_add(from.sink_panics);
    into.drop_reasons.merge(&from.drop_reasons);
    for sink in from.sinks {
        if let Some(existing) = into.sinks.iter_mut().find(|item| item.name == sink.name) {
            existing.writes = existing.writes.saturating_add(sink.writes);
            existing.errors = existing.errors.saturating_add(sink.errors);
            existing.panics = existing.panics.saturating_add(sink.panics);
            existing.rollovers = existing.rollovers.saturating_add(sink.rollovers);
            existing.rollover_errors = existing
                .rollover_errors
                .saturating_add(sink.rollover_errors);
            existing.warnings = existing.warnings.saturating_add(sink.warnings);
            existing.compression_jobs = existing
                .compression_jobs
                .saturating_add(sink.compression_jobs);
        } else {
            into.sinks.push(sink);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::record::Record;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    /// 可配置失败/panic 的测试 sink.
    ///
    /// 字段：`writes` 计数；`fail` 返回错误；`panic` 触发 panic.
    struct TestSink {
        writes: Arc<AtomicUsize>,
        fail: bool,
        panic: bool,
    }

    /// flush 必然失败的 sink，用于关停报告测试.
    struct FlushFailSink;

    impl Sink for FlushFailSink {
        /// 写单条记录；参数 `_record`，返回 Ok.
        fn write(&self, _record: &Record) -> Result<(), SinkError> {
            Ok(())
        }

        /// flush 失败；参数无.
        fn flush(&self) -> Result<(), SinkError> {
            Err("flush failed".into())
        }
    }

    /// flush 必然 panic 的 sink，用于 panic 隔离测试.
    struct PanicFlushSink;

    impl Sink for PanicFlushSink {
        /// 写单条记录；参数 `_record`，返回 Ok.
        fn write(&self, _record: &Record) -> Result<(), SinkError> {
            Ok(())
        }

        /// flush panic；参数无.
        fn flush(&self) -> Result<(), SinkError> {
            panic!("flush panic");
        }
    }

    /// 阻塞直到 release 的 sink，用于队列满测试.
    ///
    /// 字段：`entered` 表示已进入写；`release` 控制放行.
    struct BlockingSink {
        entered: Arc<AtomicUsize>,
        release: Arc<(Mutex<bool>, std::sync::Condvar)>,
    }

    impl Sink for BlockingSink {
        /// 写单条记录；参数 `record`，返回 Ok.
        fn write(&self, record: &Record) -> Result<(), SinkError> {
            self.write_batch(&[record])
        }

        /// 阻塞写批次；参数 `_records`，放行后返回 Ok.
        fn write_batch(&self, _records: &[&Record]) -> Result<(), SinkError> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let (lock, condvar) = &*self.release;
            let mut released = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            while !*released {
                released = condvar
                    .wait(released)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            Ok(())
        }
    }

    impl Sink for TestSink {
        /// 写单条记录；参数 `record`，返回结果.
        fn write(&self, record: &Record) -> Result<(), SinkError> {
            self.write_batch(&[record])
        }

        /// 批量写并按配置失败/panic；参数 `records`.
        fn write_batch(&self, records: &[&Record]) -> Result<(), SinkError> {
            self.writes.fetch_add(records.len(), Ordering::SeqCst);
            if self.panic {
                panic!("test sink panic");
            }
            if self.fail {
                Err("test sink failure".into())
            } else {
                Ok(())
            }
        }
    }

    /// 构造测试 sink.
    ///
    /// 参数：`name` 名；`writes` 计数；`fail` 是否返回错误；`panic` 是否 panic.
    /// 返回值：sink 配置.
    fn sink(name: &str, writes: &Arc<AtomicUsize>, fail: bool, panic: bool) -> SinkConfig {
        SinkConfig::new(
            name,
            Arc::new(TestSink {
                writes: Arc::clone(writes),
                fail,
                panic,
            }),
        )
    }

    /// 用测试 sink 构建 logger.
    ///
    /// 参数：`sinks` 是 sink 配置；返回值：队列 64 的 logger.
    fn build(sinks: Vec<SinkConfig>) -> Logger {
        let mut builder = LoggerBuilder::new("accounting").queue_size(64);
        for sink in sinks {
            builder = builder.add_sink(sink);
        }
        builder.build().unwrap()
    }

    /// 断言会计不变量.
    ///
    /// 参数：`snapshot` 是静止后的快照；断言：`dropped = pre + post`，`emitted = delivered + post`.
    fn assert_accounting(snapshot: &LoggerSnapshot) {
        assert_eq!(
            snapshot.dropped,
            snapshot.dropped_pre + snapshot.dropped_post,
            "dropped must equal pre + post: {snapshot:?}"
        );
        assert_eq!(
            snapshot.emitted.iter().sum::<u64>(),
            snapshot.delivered + snapshot.dropped_post,
            "accepted records must be delivered or dropped after enqueue: {snapshot:?}"
        );
    }

    /// 验证所有 sink 失败时每条记录只计一次 drop；参数无.
    #[test]
    fn failed_sink_is_counted_once_per_undelivered_record() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = build(vec![
            sink("a", &writes, true, false),
            sink("b", &writes, true, false),
        ]);
        assert!(logger.log(Level::Info, "record"));
        assert!(logger.close(Duration::from_secs(2)));
        let snapshot = logger.snapshot();
        assert_accounting(&snapshot);
        assert_eq!(snapshot.dropped, 1, "{snapshot:?}");
        assert_eq!(snapshot.dropped_pre, 0, "{snapshot:?}");
        assert_eq!(snapshot.dropped_post, 1, "{snapshot:?}");
        assert_eq!(snapshot.delivered, 0, "{snapshot:?}");
        assert_eq!(snapshot.drop_reasons.write_error, 1, "{snapshot:?}");
        assert_eq!(snapshot.write_errors, 2, "{snapshot:?}");
        assert_eq!(writes.load(Ordering::SeqCst), 2);
    }

    /// 验证一个 sink 成功即不算丢记录；参数无.
    #[test]
    fn one_successful_sink_means_the_record_is_not_dropped() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = build(vec![
            sink("ok", &writes, false, false),
            sink("fail", &writes, true, false),
        ]);
        assert!(logger.log(Level::Info, "record"));
        assert!(logger.close(Duration::from_secs(2)));
        let snapshot = logger.snapshot();
        assert_accounting(&snapshot);
        assert_eq!(snapshot.dropped, 0, "{snapshot:?}");
        assert_eq!(snapshot.delivered, 1, "{snapshot:?}");
        assert_eq!(snapshot.drop_reasons.write_error, 0, "{snapshot:?}");
        assert_eq!(snapshot.write_errors, 1, "{snapshot:?}");
        assert_eq!(writes.load(Ordering::SeqCst), 2);
    }

    /// 验证 sink panic 被隔离并计 panic；参数无.
    #[test]
    fn sink_panic_is_isolated_and_counted() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = build(vec![sink("panic", &writes, false, true)]);
        assert!(logger.log(Level::Info, "record"));
        assert!(logger.close(Duration::from_secs(2)));
        let snapshot = logger.snapshot();
        assert_accounting(&snapshot);
        assert_eq!(snapshot.dropped, 1, "{snapshot:?}");
        assert_eq!(snapshot.drop_reasons.panic, 1, "{snapshot:?}");
        assert_eq!(snapshot.sink_panics, 1, "{snapshot:?}");
    }

    /// 验证无匹配级别 sink 计 no_sink；参数无.
    #[test]
    fn unmatched_sink_level_is_reported_as_no_sink() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = build(vec![SinkConfig::new(
            "errors-only",
            Arc::new(TestSink {
                writes: Arc::clone(&writes),
                fail: false,
                panic: false,
            }),
        )
        .levels(Some(Level::Error), None)]);
        assert!(logger.log(Level::Info, "record"));
        assert!(logger.close(Duration::from_secs(2)));
        let snapshot = logger.snapshot();
        assert_accounting(&snapshot);
        assert_eq!(snapshot.dropped, 1, "{snapshot:?}");
        assert_eq!(snapshot.drop_reasons.no_sink, 1, "{snapshot:?}");
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }

    /// 验证多 shard 预算按剩余 shard 均分.
    ///
    /// 参数：无.
    /// 断言：4 个 shard 时首个分到约 1/4，最后一个拿到全部剩余；不会让后续 shard 直接得到 0.
    #[test]
    fn shard_budget_splits_remaining_time() {
        let deadline = Instant::now() + Duration::from_secs(3_600);
        let first = shard_budget(deadline, 4);
        let last = shard_budget(deadline, 1);
        assert!(first >= Duration::from_secs(850), "{first:?}");
        assert!(last > first, "first={first:?} last={last:?}");
    }

    /// 验证分片快照合并覆盖全部 DropReason 变体.
    ///
    /// 参数：无.
    /// 断言：数组顺序与 `DropReason` 下标一致，合并后每个变体都相加；新增变体时数组长度/解构
    /// 会编译失败，防止 `merge_snapshot` 静默漏算.
    #[test]
    fn drop_reason_snapshot_merges_every_variant() {
        let left = DropReasonSnapshot::from_array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let right = DropReasonSnapshot::from_array([10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
        let mut merged = left;
        merged.merge(&right);
        assert_eq!(merged.to_array(), [11, 22, 33, 44, 55, 66, 77, 88, 99, 110]);
        assert_eq!(
            merged.overload,
            merged.to_array()[DropReason::Overload.index()]
        );
        assert_eq!(
            merged.oversize,
            merged.to_array()[DropReason::Oversize.index()]
        );
    }

    /// 验证多 shard sink 变更在部分失败时回滚.
    ///
    /// 参数：无.
    /// 断言：关闭 shard 1 后，`try_add_sink`/`set_sinks` 失败，但 shard 0 不会留下半应用的 sink 表.
    #[test]
    fn multi_shard_sink_mutation_rolls_back_on_partial_failure() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = LoggerBuilder::new("rollback")
            .queue_size(64)
            .shards(2)
            .add_sink(sink("base", &writes, false, false))
            .build()
            .unwrap();
        assert!(logger.cores[1].close_with_report(Duration::from_secs(1)).ok);

        let new_writes = Arc::new(AtomicUsize::new(0));
        assert!(!logger.try_add_sink(sink("new", &new_writes, false, false)));
        for core in logger.cores.iter() {
            assert!(
                !core.snapshot().sinks.iter().any(|sink| sink.name == "new"),
                "partial add_sink must be rolled back"
            );
        }

        let replacement_writes = Arc::new(AtomicUsize::new(0));
        assert!(!logger.set_sinks(vec![sink("replacement", &replacement_writes, false, false)]));
        for core in logger.cores.iter() {
            let snapshot = core.snapshot();
            assert!(
                snapshot.sinks.iter().any(|sink| sink.name == "base"),
                "set_sinks rollback must restore the old table"
            );
            assert!(
                !snapshot.sinks.iter().any(|sink| sink.name == "replacement"),
                "partial set_sinks must be rolled back"
            );
        }
        let _ = logger.close(Duration::from_secs(1));
    }

    /// 验证超过队列预算的批次整体按 overload 丢弃；参数无.
    #[test]
    fn batch_larger_than_queue_budget_is_rejected_as_a_unit() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = LoggerBuilder::new("budget")
            .queue_size(2)
            .add_sink(sink("ok", &writes, false, false))
            .build()
            .unwrap();
        let records: Vec<Record> = (0..3)
            .map(|index| Record::new(Level::Info, format!("record-{index}")))
            .collect();
        assert_eq!(logger.log_many(records), 0);
        let snapshot = logger.snapshot();
        assert_accounting(&snapshot);
        assert_eq!(snapshot.drop_reasons.overload, 3, "{snapshot:?}");
        assert_eq!(snapshot.dropped_pre, 3, "{snapshot:?}");
        assert!(logger.close(Duration::from_secs(2)));
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }

    /// 验证超大记录在入队前拒绝；参数无.
    #[test]
    fn oversize_record_is_dropped_before_queueing() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = build(vec![sink("ok", &writes, false, false)]);
        let message = "x".repeat(MAX_RECORD_BYTES + 1);
        assert!(!logger.log(Level::Info, message));
        let snapshot = logger.snapshot();
        assert_accounting(&snapshot);
        assert_eq!(snapshot.dropped, 1, "{snapshot:?}");
        assert_eq!(snapshot.drop_reasons.oversize, 1, "{snapshot:?}");
        assert!(logger.close(Duration::from_secs(2)));
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }

    /// 验证 close 等待在途生产者且不丢已接受记录；参数无.
    #[test]
    fn close_does_not_lose_accepted_records() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = build(vec![sink("ok", &writes, false, false)]);
        let mut handles = Vec::new();
        for producer in 0..4 {
            let logger = logger.clone();
            handles.push(thread::spawn(move || {
                for index in 0..20_000 {
                    let _ = logger.log(Level::Info, format!("p{producer}-{index}"));
                }
            }));
        }
        thread::sleep(Duration::from_millis(5));
        assert!(logger.close(Duration::from_secs(10)));
        for handle in handles {
            handle.join().unwrap();
        }
        let snapshot = logger.snapshot();
        assert_accounting(&snapshot);
        let emitted: u64 = snapshot.emitted.iter().sum();
        assert_eq!(
            emitted,
            writes.load(Ordering::SeqCst) as u64,
            "every accepted record must reach the sink: {snapshot:?}"
        );
        assert_eq!(snapshot.queued_records, 0, "{snapshot:?}");
    }

    /// 1M 条 `log_many` + NullSink 吞吐基准；参数无，需 `--ignored --nocapture` 运行.
    #[test]
    #[ignore = "throughput benchmark; run explicitly in release mode"]
    fn benchmark_one_million_log_many_null_sink() {
        /// 空 sink：只消费记录，不落盘，用于隔离队列/入队吞吐.
        struct NullSink;
        impl Sink for NullSink {
            /// 丢弃记录；参数 `_record`，返回 Ok.
            fn write(&self, _record: &Record) -> Result<(), SinkError> {
                Ok(())
            }
        }
        let logger = LoggerBuilder::new("null-batch-bench")
            .queue_size(1_000_000)
            .add_sink(SinkConfig::new("null", Arc::new(NullSink)))
            .build()
            .unwrap();
        let records = 1_000_000usize;
        let template = Record::new(Level::Info, "bench");
        let start = Instant::now();
        let accepted = logger.log_many((0..records).map(|_| template.clone()));
        let enqueue = start.elapsed();
        assert_eq!(accepted, records);
        assert!(logger.close(Duration::from_secs(60)));
        let total = start.elapsed();
        let snapshot = logger.snapshot();
        assert_eq!(snapshot.dropped, 0, "{snapshot:?}");
        eprintln!(
            "null-batch: enqueue={enqueue:?} total={total:?} rate={:.0}/s",
            records as f64 / total.as_secs_f64()
        );
    }

    /// 1M 条逐条 `log` + NullSink 吞吐基准；参数无，需 `--ignored --nocapture` 运行.
    #[test]
    #[ignore = "throughput benchmark; run explicitly in release mode"]
    fn benchmark_one_million_null_sink() {
        /// 空 sink：只消费记录，不落盘，用于隔离队列/入队吞吐.
        struct NullSink;
        impl Sink for NullSink {
            /// 丢弃记录；参数 `_record`，返回 Ok.
            fn write(&self, _record: &Record) -> Result<(), SinkError> {
                Ok(())
            }
        }
        let logger = LoggerBuilder::new("null-bench")
            .queue_size(1_000_000)
            .add_sink(SinkConfig::new("null", Arc::new(NullSink)))
            .build()
            .unwrap();
        let records = 1_000_000usize;
        let start = Instant::now();
        for index in 0..records {
            assert!(logger.log(Level::Info, format!("bench {index}")));
        }
        let enqueue = start.elapsed();
        assert!(logger.close(Duration::from_secs(60)));
        let total = start.elapsed();
        let snapshot = logger.snapshot();
        assert_accounting(&snapshot);
        assert_eq!(snapshot.dropped, 0, "{snapshot:?}");
        eprintln!(
            "null-sink: enqueue={enqueue:?} total={total:?} rate={:.0}/s queued={}",
            records as f64 / total.as_secs_f64(),
            snapshot.queued_records
        );
    }

    /// 验证关停报告区分超时与 sink 失败；参数无.
    #[test]
    fn close_report_distinguishes_timeout_from_sink_failure() {
        let writes = Arc::new(AtomicUsize::new(0));
        let failing = build(vec![sink("fail", &writes, true, false)]);
        assert!(failing.log(Level::Info, "record"));
        let report = failing.close_with_report(Duration::from_secs(2));
        // sink 写失败不等于关停失败：flush/close 仍可能成功；失败必须体现在计数里.
        assert!(report.ok, "{report:?}");
        assert!(!report.timed_out, "a completed close is not a timeout");
        let snapshot = failing.snapshot();
        assert_eq!(snapshot.dropped_post, 1, "{snapshot:?}");
        assert_accounting(&snapshot);

        let stuck = build(vec![sink("ok", &writes, false, false)]);
        stuck.shared().active_producers.add();
        let report = stuck.close_with_report(Duration::from_millis(20));
        assert!(!report.ok, "{report:?}");
        assert!(
            report.timed_out,
            "stuck producer must be reported as timeout: {report:?}"
        );
        stuck.shared().active_producers.sub();
        let retry = stuck.close_with_report(Duration::from_secs(2));
        assert!(retry.ok, "{retry:?}");
        assert!(!retry.timed_out, "{retry:?}");

        let flush_fail = build(vec![SinkConfig::new("flush-fail", Arc::new(FlushFailSink))]);
        let report = flush_fail.close_with_report(Duration::from_secs(2));
        assert!(!report.ok, "failed flush must be reported: {report:?}");
        assert!(!report.timed_out, "{report:?}");
    }

    /// 验证限流窗口原子重置与上限；参数无.
    #[test]
    fn rate_limit_window_is_atomic_and_resets_each_second() {
        let limit = RateLimitState::new(RateLimit::per_second(2));
        assert!(limit.allow(100));
        assert!(limit.allow(100));
        assert!(
            !limit.allow(100),
            "third record in the same second must drop"
        );
        assert!(limit.allow(101), "a new second resets the window");
        assert!(limit.allow(101));
        assert!(!limit.allow(101));
    }

    /// 验证预格式化块走自定义 sink 的默认逐行实现.
    ///
    /// 参数：无.
    /// 断言：块内每行都被写成独立记录，顺序不变；计数按行累加.
    /// 原因：`Sink::write_preformatted_block` 是有默认实现的新 trait 方法，必须证明自定义 sink
    /// 不覆盖也能正确接收块，且 writer 的投递/计数语义与逐条记录一致.
    #[test]
    fn preformatted_block_uses_custom_sink_default() {
        /// 记录收到的每行文本.
        struct Capture(Arc<Mutex<Vec<String>>>);
        impl Sink for Capture {
            /// 追加一行；参数 `record`，返回 Ok.
            fn write(&self, record: &Record) -> Result<(), SinkError> {
                self.0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(record.message.clone());
                Ok(())
            }
        }
        let lines = Arc::new(Mutex::new(Vec::new()));
        let logger = build(vec![SinkConfig::new(
            "capture",
            Arc::new(Capture(Arc::clone(&lines))),
        )]);
        assert_eq!(
            logger.log_preformatted_block(
                Level::Info,
                "alpha
beta"
            ),
            2
        );
        assert!(logger.flush(Duration::from_secs(5)));
        let captured = lines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(captured, vec!["alpha".to_string(), "beta".to_string()]);
        let snapshot = logger.snapshot();
        assert_eq!(snapshot.emitted.iter().sum::<u64>(), 2, "{snapshot:?}");
        assert_eq!(snapshot.delivered, 2, "{snapshot:?}");
        assert_eq!(snapshot.dropped, 0, "{snapshot:?}");
    }

    /// 验证多行块限流按整批原子准入.
    ///
    /// 参数：无.
    /// 断言：配额不足时整批拒绝，且失败不会部分消耗配额.
    /// 原因：预格式化块如果部分行被限流，emitted 计数与文件内容会不一致.
    #[test]
    fn rate_limit_batch_allowance_is_all_or_nothing() {
        let limit = RateLimitState::new(RateLimit::per_second(5));
        assert!(limit.allow_n(200, 3));
        assert!(
            !limit.allow_n(200, 3),
            "remaining 2 slots cannot accept a 3-line block"
        );
        assert!(
            limit.allow_n(200, 2),
            "a rejected block must not consume the remaining slots"
        );
        assert!(!limit.allow_n(200, 1), "window is now exhausted");
    }

    /// 验证 flush panic 被隔离并让关停报告失败；参数无.
    #[test]
    fn flush_panic_is_isolated_and_reported() {
        let logger = build(vec![SinkConfig::new(
            "panic-flush",
            Arc::new(PanicFlushSink),
        )]);
        assert!(logger.log(Level::Info, "record"));
        let report = logger.close_with_report(Duration::from_secs(2));
        assert!(!report.ok, "flush panic must fail shutdown: {report:?}");
        assert!(!report.timed_out, "{report:?}");
        let snapshot = logger.snapshot();
        assert!(snapshot.sink_panics >= 1, "{snapshot:?}");
        assert!(snapshot.write_errors >= 1, "{snapshot:?}");
    }

    /// 验证队列满时不阻塞生产者并计 overload；参数无.
    #[test]
    fn queue_full_drops_without_blocking_the_producer() {
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let logger = LoggerBuilder::new("overload")
            .queue_size(1)
            .add_sink(SinkConfig::new(
                "block",
                Arc::new(BlockingSink {
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                }),
            ))
            .build()
            .unwrap();
        assert!(logger.log(Level::Info, "first"));
        let deadline = Instant::now() + Duration::from_secs(2);
        while entered.load(Ordering::Acquire) == 0 {
            assert!(
                Instant::now() < deadline,
                "writer never entered the blocking sink"
            );
            thread::yield_now();
        }
        // writer 被阻塞，队列最多只能容纳配置的一个槽位.
        let _ = logger.log(Level::Info, "second");
        let accepted = logger.log(Level::Info, "overflow");
        assert!(!accepted, "full queue must drop instead of blocking");
        let snapshot = logger.snapshot();
        assert!(snapshot.dropped_pre >= 1, "{snapshot:?}");
        assert!(snapshot.drop_reasons.overload >= 1, "{snapshot:?}");

        {
            let (lock, condvar) = &*release;
            *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
            condvar.notify_all();
        }
        assert!(logger.close(Duration::from_secs(2)));
        assert_accounting(&logger.snapshot());
    }

    /// 验证队列预算同时约束记录数和字节数.
    ///
    /// 参数：无.
    /// 断言：任一维度超限都会拒绝；失败不会留下半预留；释放后峰值仍保留.
    #[test]
    fn queue_budget_enforces_records_and_bytes() {
        let budget = QueueBudget::new();
        assert!(budget.try_reserve(2, 100, 3, 200));
        assert!(!budget.try_reserve(2, 0, 3, 200));
        assert!(!budget.try_reserve(0, 150, 3, 200));
        assert_eq!(budget.get(), (2, 100, 2, 100));
        budget.release(2, 100);
        assert_eq!(budget.get(), (0, 0, 2, 100));
    }

    /// 验证 layer 重入计数进入引擎 dropped 口径.
    ///
    /// 参数：无.
    /// 断言：`count_reentrant` 只增加 dropped_pre/Reentrant，不增加 emitted；会计不变量成立.
    #[test]
    fn count_reentrant_updates_engine_drop_reasons() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = build(vec![sink("ok", &writes, false, false)]);
        logger.count_reentrant();
        let snapshot = logger.snapshot();
        assert_eq!(snapshot.drop_reasons.reentrant, 1, "{snapshot:?}");
        assert_eq!(snapshot.dropped_pre, 1, "{snapshot:?}");
        assert_eq!(snapshot.dropped, 1, "{snapshot:?}");
        assert_eq!(snapshot.emitted, [0; 5], "{snapshot:?}");
        assert_accounting(&snapshot);
        assert!(logger.close(Duration::from_secs(2)));
    }

    /// 验证 `Overflow::Block` 超时会丢弃且不泄漏记录预算.
    ///
    /// 参数：无.
    /// 断言：队列满时调用至少等待配置 timeout，随后计 overload；放行 writer 后仍可继续入队，
    /// 关停后会计不变量成立.
    #[test]
    fn block_overflow_times_out_without_leaking_budget() {
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let timeout = Duration::from_millis(50);
        let logger = LoggerBuilder::new("block-timeout")
            .queue_size(1)
            .overflow(Overflow::Block(timeout))
            .add_sink(SinkConfig::new(
                "block",
                Arc::new(BlockingSink {
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                }),
            ))
            .build()
            .unwrap();
        assert!(logger.log(Level::Info, "first"));
        let deadline = Instant::now() + Duration::from_secs(2);
        while entered.load(Ordering::Acquire) == 0 {
            assert!(
                Instant::now() < deadline,
                "writer never entered the blocking sink"
            );
            thread::yield_now();
        }

        let started = Instant::now();
        let accepted = logger.log(Level::Info, "times out");
        let elapsed = started.elapsed();
        assert!(!accepted, "full queue must time out under Block");
        assert!(
            elapsed >= timeout,
            "Block returned before timeout: {elapsed:?}"
        );
        let snapshot = logger.snapshot();
        assert!(snapshot.drop_reasons.overload >= 1, "{snapshot:?}");

        {
            let (lock, condvar) = &*release;
            *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
            condvar.notify_all();
        }
        assert!(logger.log(Level::Info, "after release"));
        assert!(logger.close(Duration::from_secs(2)));
        let snapshot = logger.snapshot();
        assert_eq!(snapshot.queued_records, 0, "{snapshot:?}");
        assert_accounting(&snapshot);
    }

    /// 验证 `try_log_many` 在 Block 配置下也不会阻塞；参数无.
    #[test]
    fn try_log_many_does_not_block_when_queue_full() {
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let logger = LoggerBuilder::new("try-many")
            .queue_size(1)
            .overflow(Overflow::Block(Duration::from_secs(5)))
            .add_sink(SinkConfig::new(
                "block",
                Arc::new(BlockingSink {
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                }),
            ))
            .build()
            .unwrap();
        assert!(logger.log(Level::Info, "first"));
        let deadline = Instant::now() + Duration::from_secs(2);
        while entered.load(Ordering::Acquire) == 0 {
            assert!(
                Instant::now() < deadline,
                "writer never entered the blocking sink"
            );
            thread::yield_now();
        }
        // writer 阻塞时第一条记录仍占用记录预算；try_log_many 必须立即丢弃.
        let started = Instant::now();
        let accepted = logger.try_log_many(vec![Record::new(Level::Info, "third")]);
        assert_eq!(accepted, 0, "full queue must drop under try_log_many");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "try_log_many must not wait for Overflow::Block"
        );
        {
            let (lock, condvar) = &*release;
            *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
            condvar.notify_all();
        }
        assert!(logger.close(Duration::from_secs(2)));
        assert_accounting(&logger.snapshot());
    }

    /// 验证 close 等待在途生产者；参数无.
    #[test]
    fn close_waits_for_inflight_producers() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = build(vec![sink("ok", &writes, false, false)]);
        logger.shared().active_producers.add();

        let closer = logger.clone();
        let handle = thread::spawn(move || closer.close(Duration::from_secs(2)));
        thread::sleep(Duration::from_millis(20));
        assert!(!handle.is_finished(), "close must wait for producer gate");
        logger.shared().active_producers.sub();
        assert!(handle.join().unwrap());
        assert!(logger.is_closed());
    }

    /// 验证卡住生产者使 close 超时且可重试；参数无.
    #[test]
    fn close_times_out_while_a_producer_is_stuck_then_can_retry() {
        let writes = Arc::new(AtomicUsize::new(0));
        let logger = build(vec![sink("ok", &writes, false, false)]);
        logger.shared().active_producers.add();
        let start = Instant::now();
        assert!(!logger.close(Duration::from_millis(20)));
        assert!(start.elapsed() >= Duration::from_millis(15));
        logger.shared().active_producers.sub();
        assert!(logger.close(Duration::from_secs(2)));
        assert!(logger.is_closed());
    }
}
