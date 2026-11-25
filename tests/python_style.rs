//! Python 风格注册表/动态创建集成测试.
//!
//! 验证 get_logger 同名复用、UnknownTarget::Create 按需创建 service.

use std::time::Duration;

use tracing_flare::{FlareConfig, FlareLevel, FlareRotation, Logger, LoggerConfig, UnknownTarget};

/// 验证注册表复用与自动创建.
///
/// 参数：无；断言：同名对象相同、事件落到动态 service 文件.
#[test]
fn python_style_logger_registry_and_auto_creation() {
    let dir = std::env::temp_dir().join(format!("flare-python-style-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let guard = FlareConfig::new()
        .base_path(&dir)
        .unknown_target(UnknownTarget::Create)
        .install()
        .unwrap();

    let first = Logger::get_logger("python-service");
    let second = Logger::get_logger("python-service");
    let handler = Logger::get_handler("python-service");
    assert!(first.is_same(&second));
    assert!(first.is_same(&handler));

    first.info("python style line");
    assert!(guard.flush(Some(Duration::from_secs(10))));
    assert!(
        std::fs::read_to_string(dir.join("python-service/python-service.log"))
            .unwrap()
            .contains("python style line")
    );

    let configured = Logger::get_logger_with_config(
        "python-config",
        LoggerConfig::new("python-config")
            .level(FlareLevel::Debug)
            .rotation(FlareRotation::Never)
            .no_compression(),
    );
    configured.debug("configured line");
    assert!(guard.flush(Some(Duration::from_secs(10))));
    assert!(
        std::fs::read_to_string(dir.join("python-config/python-config.log"))
            .unwrap()
            .contains("configured line")
    );

    let health = guard.health();
    assert!(health
        .services
        .iter()
        .any(|service| service.name == "python-service"));
    assert!(health
        .services
        .iter()
        .any(|service| service.name == "python-config"));

    let first_concurrent = Logger::get_logger("python-concurrent");
    let handles: Vec<_> = (0..16)
        .map(|producer| {
            std::thread::spawn(move || {
                let log = Logger::get_logger("python-concurrent");
                for index in 0..100 {
                    log.info(format!("concurrent {producer} {index}"));
                }
                log
            })
        })
        .collect();
    for handle in handles {
        let log = handle.join().unwrap();
        assert!(first_concurrent.is_same(&log));
    }
    assert!(guard.flush(Some(Duration::from_secs(10))));
    let concurrent =
        std::fs::read_to_string(dir.join("python-concurrent/python-concurrent.log")).unwrap();
    assert_eq!(concurrent.lines().count(), 16 * 100);

    assert!(guard.shutdown(Some(Duration::from_secs(10))));
    assert!(Logger::try_get_logger("after-shutdown").is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
