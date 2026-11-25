//! 低延迟/可见性预设的端到端测试.

use std::time::{Duration, Instant};

use tracing_flare::{FlareConfig, FlareCoreLevel, FlareRotation, ServiceConfig};

/// 验证 `low_latency` 的每批 flush 能让文件在不显式 `guard.flush` 时可见.
#[test]
fn low_latency_batch_is_visible_without_explicit_flush() {
    let dir = std::env::temp_dir().join(format!("flare-realtime-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("visible")
                .target("visible")
                .rotation(FlareRotation::Never)
                .low_latency(),
        )
        .build()
        .unwrap();
    let handle = guard.handle("visible").unwrap();
    assert!(handle
        .logger()
        .log(FlareCoreLevel::Info, "visible without flush"));

    let path = dir.join("visible/visible.log");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if std::fs::read_to_string(&path)
            .unwrap_or_default()
            .contains("visible without flush")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "low_latency record did not reach the file in time"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let _ = guard.shutdown(Some(Duration::from_secs(10)));
    let _ = std::fs::remove_dir_all(&dir);
}
