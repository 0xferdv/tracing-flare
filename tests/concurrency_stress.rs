//! 多线程持续写入与关停压力测试.

use std::time::Duration;

use tracing_flare::{FlareConfig, FlareRotation, Logger, ServiceConfig};

/// 32 个生产者持续写入后 flush，验证无死锁、计数和文件行数一致.
#[test]
fn many_producers_flush_without_deadlock_or_loss() {
    let dir = std::env::temp_dir().join(format!("flare-stress-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let guard = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("stress")
                .target("stress")
                .rotation(FlareRotation::Never)
                .queue_size(8_192)
                .flush_each(false),
        )
        .install()
        .unwrap();
    let producers = 32usize;
    let per_producer = 200usize;
    let mut threads = Vec::with_capacity(producers);
    for producer in 0..producers {
        threads.push(std::thread::spawn(move || {
            let log = Logger::new("stress");
            for index in 0..per_producer {
                log.info(format_args!("stress p{producer} {index}"));
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(guard.flush(Some(Duration::from_secs(30))));
    let text = std::fs::read_to_string(dir.join("stress/stress.log")).unwrap();
    assert_eq!(text.lines().count(), producers * per_producer);
    let dropped: u64 = guard
        .health()
        .services
        .iter()
        .map(|service| service.snapshot.dropped)
        .sum();
    assert_eq!(dropped, 0);
    assert!(guard.shutdown(Some(Duration::from_secs(10))));
    let _ = std::fs::remove_dir_all(&dir);
}
