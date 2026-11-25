//! appender-lite 集成测试（`appender-lite` feature）.
//!
//! 验证 tracing-appender 组合、每日轮转、guard flush 与非法前缀拒绝.

//! appender-lite 集成测试（`appender-lite` feature）.
//!
//! 验证 tracing-appender 组合、每日轮转、guard flush 与非法前缀拒绝.

#![cfg(feature = "appender-lite")]

use std::sync::atomic::{AtomicUsize, Ordering};

use tracing_flare::appender::{AppenderLiteConfig, AppenderRotation};
use tracing_subscriber::prelude::*;

static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);

/// 创建隔离临时目录；参数无，返回路径.
fn temp_dir() -> std::path::PathBuf {
    let serial = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "flare-appender-test-{}-{serial}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 验证 daily appender 写入与 guard flush.
///
/// 参数：无；断言：日志文件出现且包含消息.
#[test]
fn daily_appender_writes_and_guard_flushes() {
    let dir = temp_dir();
    let (layer, guard) = AppenderLiteConfig::new(&dir, "app")
        .rotation(AppenderRotation::Daily)
        .max_log_files(3)
        .layer()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!("appender-lite line");
    });
    guard.shutdown();

    let path = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("app.") && name.ends_with(".log"))
        })
        .expect("appender should create a daily log file");
    let text = std::fs::read_to_string(path).unwrap();
    assert!(text.contains("appender-lite line"), "{text}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证非法文件名前缀被拒绝.
///
/// 参数：无；断言：`../escape` 构建 layer 返回错误.
#[test]
fn appender_rejects_unsafe_prefix() {
    assert!(AppenderLiteConfig::new("logs", "../escape")
        .layer::<tracing_subscriber::Registry>()
        .is_err());
}
