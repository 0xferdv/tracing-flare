//! JSON Lines 输出集成测试（`json` feature）.
//!
//! 验证换行被 JSON 转义、schema 字段、结构化字段与 PID/线程字段.

#![cfg(feature = "json")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tracing_subscriber::prelude::*;

use tracing_flare::{
    flare_info, FlareConfig, FlareCoreLevel, FlareRecord, FlareRotation, ServiceConfig,
};

static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);

/// 创建隔离临时目录；参数无，返回路径.
fn temp_dir() -> std::path::PathBuf {
    let serial = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("flare-json-{}-{serial}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 验证每条记录是一个 JSON 对象.
///
/// 参数：无；断言：文件只有一行、message 中的换行被转义、level/service/target/fields/pid/thread 正
/// 确.
#[test]
fn json_lines_are_one_object_per_record() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .json_lines(true)
        .service(
            ServiceConfig::new("json")
                .target("json")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        flare_info!(target: "json", order = 42, "hello\nworld");
    });
    assert!(guard.flush(Some(Duration::from_secs(10))));

    let text = std::fs::read_to_string(dir.join("json/json.log")).unwrap();
    assert_eq!(
        text.lines().count(),
        1,
        "newline must be JSON-escaped: {text}"
    );
    let value: serde_json::Value = serde_json::from_str(text.trim_end()).unwrap();
    assert_eq!(value["schema"], 1);
    assert_eq!(value["level"], "INFO");
    assert_eq!(value["service"], "json");
    assert_eq!(value["target"], "json");
    assert_eq!(value["message"], "hello\nworld");
    assert_eq!(value["fields"]["order"], 42);
    assert!(value["pid"].as_u64().unwrap_or(0) > 0);
    assert!(value["thread"].as_u64().is_some());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证 JSON 服务拒绝未标记 JSON 的批量文本.
///
/// 参数：无.
/// 断言：普通文本记录被计入 `drop_reasons.format`；只有显式 `with_json()` 的记录写入文件.
/// 原因：批量 API 不经过模板，JSON Lines 采集管道不能被半结构化文本污染.
#[test]
fn json_service_rejects_unmarked_batch_text() {
    let dir = temp_dir();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .json_lines(true)
        .service(
            ServiceConfig::new("json-guard")
                .target("json-guard")
                .rotation(FlareRotation::Never)
                .flush_each(false),
        )
        .build()
        .unwrap();
    let handle = guard.handle("json-guard").unwrap();
    let records = [
        FlareRecord::new(FlareCoreLevel::Info, r#"{"message":"ok","n":1}"#).with_json(),
        FlareRecord::new(FlareCoreLevel::Info, "not json"),
    ];
    assert_eq!(handle.logger().log_many(records), 1);
    assert_eq!(
        handle.logger().log_preformatted_block(
            FlareCoreLevel::Info,
            "{\"message\":\"block\"}
",
        ),
        0,
        "preformatted blocks cannot be validated as JSON Lines"
    );
    assert!(handle.flush(Some(Duration::from_secs(10))));

    let text = std::fs::read_to_string(dir.join("json-guard/json-guard.log")).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 1, "{text}");
    assert!(lines[0].contains(r#""n":1"#), "{text}");
    assert!(!text.contains("not json"), "{text}");

    let health = guard.health();
    let service = health
        .services
        .iter()
        .find(|service| service.name == "json-guard")
        .expect("service snapshot");
    assert_eq!(service.snapshot.drop_reasons.format, 2, "{health:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
