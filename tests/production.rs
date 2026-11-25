//! 生产预设 + `Logger::get_logger` 的批量入口集成测试.

use std::time::Duration;

use tracing_flare::{FlareConfig, FlareCoreLevel, FlareRecord, Logger};

/// 验证 production 预设会自动创建动态 service，并且 `Logger::log_many` 走底层批量路径.
#[test]
fn production_preset_supports_get_logger_log_many() {
    let dir = std::env::temp_dir().join(format!("flare-production-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let (_layer, guard) = FlareConfig::production(&dir).build().unwrap();
    guard.register_default_router();
    let log = Logger::get_logger("billing");
    let records: Vec<FlareRecord> = (0..1_000)
        .map(|index| FlareRecord::new(FlareCoreLevel::Info, format!("line {index}")))
        .collect();
    assert_eq!(log.log_many(records), 1_000);
    assert!(guard.flush(Some(Duration::from_secs(10))));

    let text = std::fs::read_to_string(dir.join("billing/billing.log")).unwrap();
    assert_eq!(text.lines().count(), 1_000);
    assert!(text.contains("line 999"), "{text}");
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}
