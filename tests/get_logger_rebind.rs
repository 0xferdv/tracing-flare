//! router 替换后 facade 必须重新绑定到新 service.

use std::sync::Mutex;
use std::time::Duration;

use tracing_flare::{FlareConfig, FlareCoreLevel, FlareRecord, Logger, UnknownTarget};

/// 测试共享默认 router；串行执行.
static TEST_GUARD: Mutex<()> = Mutex::new(());

/// 重新注册默认 router 后，同名 facade 不能继续使用旧 service 绑定.
#[test]
fn get_logger_rebinds_after_router_replacement() {
    let _test_guard = TEST_GUARD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let root = std::env::temp_dir().join(format!("flare-rebind-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let first = root.join("first");
    let second = root.join("second");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();

    let (_layer1, guard1) = FlareConfig::new()
        .base_path(&first)
        .queue_size(64)
        .unknown_target(UnknownTarget::Create)
        .build()
        .unwrap();
    guard1.register_default_router();
    let log = Logger::get_logger("rebind");
    assert!(log.log_many(vec![FlareRecord::new(FlareCoreLevel::Info, "first-router")]) == 1);
    assert!(guard1.flush(Some(Duration::from_secs(10))));

    let (_layer2, guard2) = FlareConfig::new()
        .base_path(&second)
        .queue_size(64)
        .unknown_target(UnknownTarget::Create)
        .build()
        .unwrap();
    guard2.register_default_router();
    // get_logger 负责在新 router 中确保 service 并重新绑定；同一个 facade 仍然复用.
    let log = Logger::get_logger("rebind");
    assert!(
        log.log_many(vec![FlareRecord::new(
            FlareCoreLevel::Info,
            "second-router"
        )]) == 1
    );
    assert!(guard2.flush(Some(Duration::from_secs(10))));

    let second_text = std::fs::read_to_string(second.join("rebind/rebind.log")).unwrap();
    assert!(second_text.contains("second-router"), "{second_text}");
    assert!(!second_text.contains("first-router"), "{second_text}");
    drop((guard1, guard2));
    let _ = std::fs::remove_dir_all(&root);
}

/// 已有 service 绑定在 shutdown 后不能绕过 closed 检查.
#[test]
fn bound_service_does_not_mask_shutdown_error() {
    let _test_guard = TEST_GUARD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = std::env::temp_dir().join(format!("flare-bound-shutdown-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .queue_size(64)
        .unknown_target(UnknownTarget::Create)
        .build()
        .unwrap();
    guard.register_default_router();
    let _log = Logger::get_logger("bound-shutdown");
    assert!(guard.shutdown(Some(Duration::from_secs(10))));
    assert!(Logger::try_get_logger("bound-shutdown").is_err());
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}
