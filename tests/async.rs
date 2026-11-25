//! 异步门面集成测试.
//!
//! 验证 sync/async Logger 在 tokio 多线程 runtime 上安全可用、事件按 service 路由.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tracing_flare::{FlareConfig, FlareRotation, Logger, ServiceConfig};

static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);

/// 创建隔离临时目录；参数无，返回路径.
fn temp_dir() -> std::path::PathBuf {
    let serial = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("flare-async-test-{}-{serial}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 在多线程 Tokio runtime 上验证异步门面.
///
/// 参数：无；断言：任意 worker 线程上的 task 都能把事件送入同一全局 subscriber，文件包含预期消息且
/// 进程不死锁.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_logger_is_safe_on_tokio_multi_thread() {
    let dir = temp_dir();
    let guard = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("async-bench")
                .target("async-bench")
                .rotation(FlareRotation::Never)
                .queue_size(65_536)
                .flush_each(false),
        )
        .install()
        .unwrap();

    let log = Logger::new("async-bench");
    let mut tasks = Vec::new();
    let producers = 8usize;
    let per_producer = 1_000usize;
    for producer in 0..producers {
        let log = log.clone();
        tasks.push(tokio::spawn(async move {
            for index in 0..per_producer {
                log.ainfo(format!("async p{producer} {index}")).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    assert!(guard.flush(Some(Duration::from_secs(10))));
    let path = dir.join("async-bench/async-bench.log");
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        text.lines().count(),
        producers * per_producer,
        "expected one line per record"
    );
    assert!(text.lines().all(|line| line.contains("async p")));
    let health = guard.health();
    let dropped: u64 = health
        .services
        .iter()
        .map(|service| service.snapshot.dropped)
        .sum();
    assert_eq!(dropped, 0, "{health:?}");
    assert!(guard.shutdown(Some(Duration::from_secs(10))));
    let _ = std::fs::remove_dir_all(&dir);
}
