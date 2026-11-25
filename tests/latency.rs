//! 逐条写入门面的延迟分布（`#[ignore]`）.
//!
//! 运行：
//! `cargo test -p tracing-flare --release --features mimalloc --test latency --
//! --ignored --nocapture`
//!
//! 这里测的是生产者侧 `Logger::info` 调用（含 tracing、格式化、入队）的墙钟延迟，
//! 不是 writer 的 I/O 延迟；flush 耗时单独输出.

mod support;

use std::time::{Duration, Instant};

use tracing_subscriber::prelude::*;

use tracing_flare::{FlareConfig, FlareRotation, Logger, ServiceConfig};

/// 单线程延迟分布；参数无.
#[test]
#[ignore = "latency measurement; run explicitly in release mode"]
fn logger_enqueue_latency_distribution() {
    let dir = std::env::temp_dir().join(format!("flare-latency-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("latency")
                .target("latency")
                .rotation(FlareRotation::Never)
                .queue_size(1_000_000)
                .flush_each(false)
                .file_buffer_size(256 * 1024),
        )
        .build()
        .unwrap();
    guard.register_default_router();
    let log = Logger::get_logger("latency");
    let subscriber = tracing_subscriber::registry().with(_layer);
    let samples = 200_000usize;

    let rss_before = support::rss_bytes();
    let start = Instant::now();
    tracing::subscriber::with_default(subscriber, || {
        for index in 0..10_000 {
            log.info(format_args!("warmup {index}"));
        }
        for _ in 0..samples {
            let begin = Instant::now();
            log.info("latency probe");
            // 保留每次调用墙钟时间；这是测量工具，不是库热路径.
            let elapsed = begin.elapsed().as_nanos() as u64;
            // 用黑盒累加防止优化；本地收集到线程局部静态会引入额外同步，因此直接写入
            // 一个预分配缓冲（见下方 `LATENCIES`）。
            push_sample(elapsed);
        }
    });
    let enqueue_elapsed = start.elapsed();
    let flush_start = Instant::now();
    assert!(guard.flush(Some(Duration::from_secs(30))));
    let flush_elapsed = flush_start.elapsed();
    let rss_after = support::rss_bytes();
    let mut sorted = take_samples();
    sorted.sort_unstable();

    eprintln!(
        "latency: samples={} enqueue_total={enqueue_elapsed:?} flush={flush_elapsed:?} \
         p50={} p95={} p99={} p999={} max={}",
        sorted.len(),
        support::format_ns(support::percentile(&sorted, 0.50)),
        support::format_ns(support::percentile(&sorted, 0.95)),
        support::format_ns(support::percentile(&sorted, 0.99)),
        support::format_ns(support::percentile(&sorted, 0.999)),
        support::format_ns(*sorted.last().unwrap_or(&0)),
    );
    if let (Some(before), Some(after)) = (rss_before, rss_after) {
        eprintln!(
            "latency: rss_before={} MiB rss_after={} MiB",
            before / 1024 / 1024,
            after / 1024 / 1024
        );
    }
    assert_eq!(sorted.len(), samples);
    let _ = guard.shutdown(Some(Duration::from_secs(10)));
    let _ = std::fs::remove_dir_all(&dir);
}

thread_local! {
    /// 延迟采样暂存；按线程收集，避免测量工具自身引入跨线程同步.
    static LATENCIES: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// 追加一次延迟样本.
fn push_sample(ns: u64) {
    LATENCIES.with(|samples| samples.borrow_mut().push(ns));
}

/// 取走当前线程样本.
fn take_samples() -> Vec<u64> {
    LATENCIES.with(|samples| std::mem::take(&mut *samples.borrow_mut()))
}
