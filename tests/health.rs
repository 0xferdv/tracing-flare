//! 健康快照与关停报告测试（`serde` feature）.
//!
//! 验证快照 JSON 字段与 `shutdown_report` 的逐 service 结果.

#![cfg(feature = "serde")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tracing_subscriber::prelude::*;

use tracing_flare::{FlareConfig, FlareRotation, ServiceConfig, UnknownTarget};

static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);

/// 创建隔离临时目录；参数无，返回路径.
fn temp_dir() -> std::path::PathBuf {
    let serial = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "flare-serde-health-{}-{serial}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 验证 shutdown_report 返回逐 service 结果与最终计数.
///
/// 参数：无；断言：ok/timed_out 正确，service 快照 delivered >= 1.
#[test]
fn shutdown_report_contains_per_service_outcomes() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("shutdown")
                .target("shutdown")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "shutdown", "before shutdown");
    });
    let report = guard.shutdown_report(Some(Duration::from_secs(10)));
    assert!(report.ok, "{report:?}");
    assert!(!report.timed_out, "{report:?}");
    let service = report
        .services
        .iter()
        .find(|service| service.name == "shutdown")
        .unwrap_or_else(|| panic!("missing shutdown service: {report:?}"));
    assert!(service.report.ok, "{service:?}");
    assert!(!service.report.timed_out, "{service:?}");
    assert!(service.snapshot.delivered >= 1, "{service:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证健康快照可序列化.
///
/// 参数：无；断言：service 名、dropped、layer.forwarded 字段正确.
#[test]
fn health_snapshot_serializes_to_json() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .unknown_target(UnknownTarget::Drop)
        .service(
            ServiceConfig::new("health")
                .target("health")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "health", "health line");
    });
    assert!(guard.flush(Some(Duration::from_secs(10))));
    let json = serde_json::to_value(guard.health()).unwrap();
    assert_eq!(json["services"][0]["name"], "health");
    assert_eq!(json["services"][0]["snapshot"]["dropped"], 0);
    assert!(json["layer"]["forwarded"].as_u64().unwrap_or(0) >= 1);
    let _ = std::fs::remove_dir_all(&dir);
}
