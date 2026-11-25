//! 并发冒烟测试：一个全局 subscriber、多个 service、多个生产者.
//! 本文件只有一个测试，因为它会安装进程级 subscriber.

use std::sync::Arc;
use std::time::Duration;

use tracing_flare::{flare_info, FlareConfig, FlareRotation, ServiceConfig};

const SERVICES: [&str; 4] = ["alpha", "beta", "gamma", "delta"];
const PRODUCERS: usize = 8;
const PER_PRODUCER: usize = 20_000;

/// 验证跨 service 多生产者并发.
///
/// 参数：无；断言：各文件行数/内容符合预期，无 panic.
#[test]
fn concurrent_producers_across_services() {
    let dir = std::env::temp_dir().join(format!("flare-concurrency-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mut config = FlareConfig::new()
        .base_path(&dir)
        .unknown_target(tracing_flare::UnknownTarget::Drop);
    for service in SERVICES {
        config = config.service(
            ServiceConfig::new(service)
                .target(service)
                .rotation(FlareRotation::Never)
                .queue_size(500_000),
        );
    }
    let guard = config.install().expect("install global subscriber");
    let guard = Arc::new(guard);

    let mut threads = Vec::new();
    for producer in 0..PRODUCERS {
        threads.push(std::thread::spawn(move || {
            for index in 0..PER_PRODUCER {
                match SERVICES[(producer + index) % SERVICES.len()] {
                    "alpha" => flare_info!(target: "alpha", producer, index, "concurrent record"),
                    "beta" => flare_info!(target: "beta", producer, index, "concurrent record"),
                    "gamma" => flare_info!(target: "gamma", producer, index, "concurrent record"),
                    _ => flare_info!(target: "delta", producer, index, "concurrent record"),
                }
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(guard.flush(Some(Duration::from_secs(30))));

    let health = guard.health();
    let total: usize = health
        .services
        .iter()
        .map(|service| service.snapshot.emitted.iter().sum::<u64>() as usize)
        .sum();
    assert_eq!(total, PRODUCERS * PER_PRODUCER, "{health:?}");
    let dropped: u64 = health
        .services
        .iter()
        .map(|service| service.snapshot.dropped)
        .sum();
    assert_eq!(dropped, 0, "{health:?}");

    assert!(guard.shutdown(Some(Duration::from_secs(30))));
    let mut written = 0usize;
    for service in SERVICES {
        let path = dir.join(service).join(format!("{service}.log"));
        let text = std::fs::read_to_string(&path).unwrap();
        written += text.lines().count();
    }
    assert_eq!(written, PRODUCERS * PER_PRODUCER);
    let _ = std::fs::remove_dir_all(&dir);
}
