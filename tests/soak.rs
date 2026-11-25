//! 短时长稳定性探针（`#[ignore]`）.
//!
//! 运行：
//! `FLARE_SOAK_SECS=60 cargo test -p tracing-flare --release --features mimalloc
//! --test soak -- --ignored --nocapture`
//!
//! 默认 10 秒；CI 专用 runner 可设 3600/86400. 输出 RSS 峰值、队列峰值、丢弃计数和
//! 每次采样间隔的吞吐，不把结果写成硬断言（共享机器只作 advisory）.

mod support;

use std::time::{Duration, Instant};

use tracing_subscriber::prelude::*;

use tracing_flare::{FlareConfig, FlareRotation, Logger, ServiceConfig};

/// 运行环境指定的短 soak；参数无.
#[test]
#[ignore = "soak measurement; run explicitly in release mode"]
fn short_soak_reports_rss_and_drops() {
    let seconds = std::env::var("FLARE_SOAK_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10)
        .max(1);
    let dir = std::env::temp_dir().join(format!("flare-soak-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("soak")
                .target("soak")
                .rotation(FlareRotation::Never)
                .queue_size(1_000_000)
                .flush_each(false)
                .file_buffer_size(256 * 1024),
        )
        .build()
        .unwrap();
    guard.register_default_router();
    let log = Logger::get_logger("soak");
    let subscriber = tracing_subscriber::registry().with(_layer);
    let dispatch = tracing::Dispatch::new(subscriber);
    let started = Instant::now();
    let deadline = started + Duration::from_secs(seconds);
    let mut peak_rss = support::rss_bytes().unwrap_or(0);
    let mut peak_queued_bytes = 0usize;
    let mut next_sample = started + Duration::from_secs(1);
    let mut accepted = 0usize;
    while Instant::now() < deadline {
        let begin = Instant::now();
        tracing::dispatcher::with_default(&dispatch, || {
            for index in 0..10_000 {
                log.info(format_args!("soak {index} {}", "x".repeat(8)));
            }
        });
        accepted = accepted.saturating_add(10_000);
        if Instant::now() >= next_sample {
            if let Some(rss) = support::rss_bytes() {
                peak_rss = peak_rss.max(rss);
            }
            let health = guard.health();
            let queued: usize = health
                .services
                .iter()
                .map(|service| service.snapshot.peak_queued_bytes)
                .sum();
            peak_queued_bytes = peak_queued_bytes.max(queued);
            next_sample += Duration::from_secs(1);
        }
        // 让 writer 有机会追上，同时保持生产侧压力.
        std::thread::sleep(Duration::from_millis(1).saturating_sub(begin.elapsed()));
    }
    // flush 必须排空生产期间积累的队列.固定 30s 在 Windows/AV 或慢盘上的多 GB 运行后太小；
    // 随时长缩放，但保持有界，使 writer 真死时测试仍会失败.
    let flush_budget = Duration::from_secs(seconds.clamp(30, 300));
    assert!(guard.flush(Some(flush_budget)));
    let health = guard.health();
    let dropped: u64 = health
        .services
        .iter()
        .map(|service| service.snapshot.dropped)
        .sum();
    peak_queued_bytes = peak_queued_bytes.max(
        health
            .services
            .iter()
            .map(|service| service.snapshot.peak_queued_bytes)
            .sum(),
    );
    eprintln!(
        "soak: seconds={seconds} accepted={accepted} dropped={dropped} \
         peak_rss={} MiB peak_queued_bytes={} elapsed={:?}",
        peak_rss / 1024 / 1024,
        peak_queued_bytes,
        started.elapsed()
    );
    let _ = guard.shutdown(Some(Duration::from_secs(10)));
    let _ = std::fs::remove_dir_all(&dir);
}
