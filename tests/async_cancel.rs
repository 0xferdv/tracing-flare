//! 异步 Logger 取消语义.

use std::time::Duration;

use tracing_subscriber::prelude::*;

use tracing_flare::{FlareConfig, FlareRotation, Logger, ServiceConfig};

/// 未 poll 的 `ainfo` future 不产生日志；poll 后只产生一条.
#[test]
fn unpolled_ainfo_is_noop_and_polled_emits_once() {
    let dir = std::env::temp_dir().join(format!("flare-async-cancel-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("cancel")
                .target("cancel")
                .rotation(FlareRotation::Never)
                .queue_size(1_024)
                .flush_each(false),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing::Dispatch::new(subscriber);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::dispatcher::with_default(&dispatch, || {
        runtime.block_on(async {
            let log = Logger::new("cancel");
            let never = log.ainfo("never-polled");
            drop(never);
            log.ainfo("polled-once").await;
        });
    });
    assert!(guard.flush(Some(Duration::from_secs(10))));
    let text = std::fs::read_to_string(dir.join("cancel/cancel.log")).unwrap();
    assert!(!text.contains("never-polled"), "{text}");
    assert_eq!(text.matches("polled-once").count(), 1, "{text}");
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}
