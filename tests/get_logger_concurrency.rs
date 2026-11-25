//! `get_logger` 动态 service 创建并发测试.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing_flare::{FlareConfig, Logger, UnknownTarget};

/// 这些测试共享进程级 default router，必须串行执行.
static TEST_GUARD: Mutex<()> = Mutex::new(());

/// 不同名字并发首次获取时，不重复创建同一个 service，也不发生死锁.
#[test]
fn concurrent_get_logger_different_names_completes() {
    let _test_guard = TEST_GUARD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = std::env::temp_dir().join(format!(
        "flare-get-logger-concurrent-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .queue_size(64)
        .unknown_target(UnknownTarget::Create)
        .max_dynamic_services(64)
        .build()
        .unwrap();
    guard.register_default_router();
    let logger = Arc::new(Logger::get_logger("concurrent-root"));
    let threads: Vec<_> = (0..32)
        .map(|index| {
            let logger = Arc::clone(&logger);
            std::thread::spawn(move || {
                let first = Logger::get_logger(format!("concurrent-{index}"));
                let second = Logger::get_logger(format!("concurrent-{index}"));
                assert!(first.is_same(&second));
                assert_eq!(first.name(), format!("concurrent-{index}"));
                logger.name().to_string()
            })
        })
        .collect();
    for thread in threads {
        assert_eq!(thread.join().unwrap(), "concurrent-root");
    }
    assert_eq!(
        Logger::get_logger("concurrent-root").name(),
        "concurrent-root"
    );
    assert!(guard.flush(Some(Duration::from_secs(10))));
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 同名首次获取在并发下只创建一个 service.
#[test]
fn concurrent_get_logger_same_name_creates_one_service() {
    let _test_guard = TEST_GUARD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = std::env::temp_dir().join(format!("flare-get-logger-same-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .queue_size(64)
        .unknown_target(UnknownTarget::Create)
        .max_dynamic_services(8)
        .build()
        .unwrap();
    guard.register_default_router();
    let handles: Vec<_> = (0..32)
        .map(|_| std::thread::spawn(|| Logger::get_logger("same-concurrent")))
        .collect();
    let mut first: Option<Logger> = None;
    for handle in handles {
        let logger = handle.join().unwrap();
        match &first {
            Some(first) => assert!(first.is_same(&logger)),
            None => first = Some(logger),
        }
    }
    let first = first.unwrap();
    assert!(first.is_same(&Logger::get_logger("same-concurrent")));
    let count = guard
        .health()
        .services
        .iter()
        .filter(|service| service.name == "same-concurrent")
        .count();
    assert_eq!(count, 1);
    assert!(guard.flush(Some(Duration::from_secs(10))));
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 动态 service 上限在并发/失败路径下仍然严格生效.
#[test]
fn dynamic_service_limit_is_strict_and_releases_reservations() {
    let _test_guard = TEST_GUARD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = std::env::temp_dir().join(format!("flare-get-logger-limit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .queue_size(64)
        .unknown_target(UnknownTarget::Create)
        .max_dynamic_services(1)
        .build()
        .unwrap();
    guard.register_default_router();
    assert!(Logger::try_get_logger("limit-a").is_ok());
    assert!(Logger::try_get_logger("limit-b").is_err());
    let count = guard.health().services.len();
    assert_eq!(count, 1);
    assert!(guard.flush(Some(Duration::from_secs(10))));
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}
