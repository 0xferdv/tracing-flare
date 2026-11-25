//! Layer 级计数器，与每个 service 的 runtime 计数器分开.
//!
//! 设计原理：
//! - layer 计数回答“tracing 事件进入 Flare 前后发生了什么”（seen/filtered/no_route/...），
//!   runtime 计数回答“进入某个 service 后队列与 sink 的行为”；
//! - 两者分开可以定位问题在路由层还是落点层，也避免一个巨大的全局结构体产生 cache 争用；
//! - `seen`/`forwarded` 是每条事件都会更新的高频计数，使用按线程分片的计数槽；
//!   其余低频计数器仍是单个 `AtomicU64`；
//! - 所有原子操作都使用 `Relaxed`：只用于观测，不参与同步，且不会为热路径引入额外内存屏障.
//!
//! 一致性说明：快照是逐字段读取的，不是原子一致快照；在多线程同时写入时，各字段之间可能属于不同瞬间
//! ，因此不变量（如 seen >= forwarded）只在静止/低频采样时严格成立.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// layer 计数器使用的 stripe 数.
///
/// `seen`/`forwarded` 每条事件都会更新；单原子会让所有生产者争同一 cache line. 分成 8 个
/// stripe 后，每个线程写自己的槽，健康快照时再求和.
const STRIPES: usize = 8;

thread_local! {
    /// 当前线程的 layer 计数 stripe；`usize::MAX` 表示尚未分配.
    static STRIPE: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// 分配 stripe 的全局游标；只在线程第一次计数时访问.
static NEXT_STRIPE: AtomicUsize = AtomicUsize::new(0);

/// 返回当前线程的 layer 计数 stripe.
///
/// 参数：无；返回值：`0..STRIPES`.
#[inline]
fn stripe() -> usize {
    STRIPE
        .try_with(|cell| {
            let value = cell.get();
            if value != usize::MAX {
                return value;
            }
            let value = NEXT_STRIPE.fetch_add(1, Ordering::Relaxed) % STRIPES;
            cell.set(value);
            value
        })
        // TLS 正在销毁时回退到 0 号 stripe，避免日志计数路径 panic.
        .unwrap_or(0)
}

/// 一个独占 cache line 的 layer 计数 stripe.
///
/// 只包一个 `AtomicU64` 却按 64 字节对齐；若整个数组只做外层对齐，8 个 stripe 仍会共用
/// 一条 cache line，多核同时更新时 cache line 会在核间来回弹跳.
#[repr(align(64))]
struct Stripe(AtomicU64);

/// 缓存行隔离的分片计数器.
///
/// 字段：`stripes` 是每线程独立更新的 8 个计数槽；每个槽独占一条 cache line.
/// 快照求和只在健康检查时执行，热路径只有一次 TLS 读取和一次 `Relaxed` 原子加.
pub(crate) struct StripedCounter {
    stripes: [Stripe; STRIPES],
}

impl Default for StripedCounter {
    /// 创建 0 值分片计数器；参数无.
    fn default() -> Self {
        Self {
            stripes: [const { Stripe(AtomicU64::new(0)) }; STRIPES],
        }
    }
}

impl StripedCounter {
    /// 增加计数.
    ///
    /// 参数：`value` 是增量；0 直接返回.
    /// 使用 `Relaxed`：只用于观测，不发布其他状态.
    #[inline]
    pub(crate) fn add(&self, value: u64) {
        if value != 0 {
            self.stripes[stripe()].0.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// 读取所有 stripe 的和.
    ///
    /// 参数：无；返回值：累计值；使用饱和加法，避免监控计数回绕.
    fn get(&self) -> u64 {
        self.stripes.iter().fold(0u64, |total, stripe| {
            total.saturating_add(stripe.0.load(Ordering::Relaxed))
        })
    }
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<StripedCounter>() == STRIPES * 64);

/// 运行期的 layer 计数器集合，只由 layer 写、健康检查读.
#[derive(Default)]
pub(crate) struct FlareStats {
    /// 进入 `on_event` 的事件总数（包含后续被过滤/无路由的事件）.
    pub(crate) seen: StripedCounter,
    /// 因 service 级别阈值被过滤的事件数.
    pub(crate) filtered: AtomicU64,
    /// 找不到 target/service 路由的事件数.
    pub(crate) no_route: AtomicU64,
    /// 成功交给 service runtime 队列的事件数.
    pub(crate) forwarded: StripedCounter,
    /// 交给 runtime 但被其拒绝（队列满/已关停等）的事件数.
    pub(crate) rejected: AtomicU64,
    /// layer 格式化/路由过程中的 panic 次数（被 `catch_unwind` 隔离）.
    pub(crate) layer_panics: AtomicU64,
    /// 重入被拒绝的次数（writer 线程内回写、或 layer 自身递归）.
    pub(crate) reentrant: AtomicU64,
    /// 格式化错误次数；当前格式化器在编译期校验，运行期主要为预留口径.
    pub(crate) format_errors: AtomicU64,
}

/// layer 计数器的只读快照.
///
/// 参数/字段说明：每个字段对应 `FlareStats` 的同名计数.
/// `Copy` 让健康检查可以低成本传递，不持有任何原子引用，避免调用方长期借用内部状态.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct FlareStatsSnapshot {
    /// 进入 layer 的事件总数.
    pub seen: u64,
    /// 级别过滤的数量.
    pub filtered: u64,
    /// 无路由的数量.
    pub no_route: u64,
    /// 成功转交 runtime 的数量.
    pub forwarded: u64,
    /// runtime 拒绝的数量.
    pub rejected: u64,
    /// layer panic 次数.
    pub layer_panics: u64,
    /// 重入拒绝次数.
    pub reentrant: u64,
    /// 格式化错误次数.
    pub format_errors: u64,
}

impl FlareStats {
    /// 生成一次非原子一致的只读快照.
    ///
    /// 参数：无.
    /// 返回值：所有计数器的当前值.`Relaxed` 读取足够用于监控；如果需要严格对账，应在停止写入（
    /// shutdown 后）再取快照.
    pub(crate) fn snapshot(&self) -> FlareStatsSnapshot {
        FlareStatsSnapshot {
            seen: self.seen.get(),
            filtered: self.filtered.load(Ordering::Relaxed),
            no_route: self.no_route.load(Ordering::Relaxed),
            forwarded: self.forwarded.get(),
            rejected: self.rejected.load(Ordering::Relaxed),
            layer_panics: self.layer_panics.load(Ordering::Relaxed),
            reentrant: self.reentrant.load(Ordering::Relaxed),
            format_errors: self.format_errors.load(Ordering::Relaxed),
        }
    }
}
