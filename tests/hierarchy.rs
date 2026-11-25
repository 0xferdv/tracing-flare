//! Python 风格层级/handler 的 opt-in 集成测试.
//!
//! 测试串行化：root/global handler 是进程级状态，多个测试并行会互相污染.

#![cfg(feature = "python-hierarchy")]

use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use tracing_flare::{
    FlareConfig, FlareCoreLevel, FlareLevel, FlareRecord, FlareRotation, Handler, Logger,
    ServiceConfig,
};
use tracing_subscriber::prelude::*;

/// 进程级测试锁；保护 global handler/root 状态.
fn test_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 测试结束时清空 global handler，避免 panic 后污染其他测试.
struct GlobalCleanup;

impl Drop for GlobalCleanup {
    /// 清空 root handler/level；参数无.
    fn drop(&mut self) {
        Logger::clear_global_handlers();
        Logger::root().set_level(None);
    }
}

/// 创建独立临时目录.
fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("flare-hierarchy-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 验证 Handler/Logger 可跨线程共享；参数无.
#[test]
fn handler_is_send_sync() {
    /// 编译期断言；参数无.
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Handler>();
    assert_send_sync::<Logger>();
    assert!(Logger::get_logger("").is_same(&Logger::root()));
}

/// 验证 `propagate=false` 只停止 handler 传播，不掐断级别继承.
#[test]
fn propagate_false_keeps_level_inheritance_and_stops_handlers() {
    let _guard = test_guard();
    let _cleanup = GlobalCleanup;
    let dir = temp_dir("inherit");
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("inh.parent.child")
                .target("inh.parent.child")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("inh.audit")
                .target("inh.audit")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    guard.register_default_router();

    let child = Logger::get_logger("inh.parent.child");
    let parent = child.parent().expect("parent logger");
    parent.set_level(Some(FlareLevel::Warn));
    parent.add_handler(Handler::new("inh.audit").unwrap());
    child.set_propagate(false);
    assert_eq!(child.effective_level(), FlareLevel::Warn);

    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        child.info("filtered by inherited parent level");
        child.warn("accepted by inherited parent level");
    });
    parent.set_level(None);
    parent.clear_handlers();
    child.set_propagate(true);
    assert!(guard.flush(Some(Duration::from_secs(10))));

    let child_text = std::fs::read_to_string(dir.join("inh.parent.child/inh.parent.child.log"))
        .unwrap_or_default();
    assert!(child_text.contains("accepted by inherited parent level"));
    assert!(!child_text.contains("filtered by inherited parent level"));
    let audit = std::fs::read_to_string(dir.join("inh.audit/inh.audit.log")).unwrap_or_default();
    assert!(
        audit.is_empty(),
        "propagate=false must not reach parent handler"
    );

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证多 handler fan-out 的头部 service 名按落点分别渲染.
#[test]
fn multi_handler_headers_use_each_service_name() {
    let _guard = test_guard();
    let _cleanup = GlobalCleanup;
    let dir = temp_dir("headers");
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .format("%(name)s|%(levelname)s|%(message)s")
        .service(
            ServiceConfig::new("hdr.a")
                .target("hdr.a")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("hdr.b")
                .target("hdr.b")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("hdr.child")
                .target("hdr.child")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    guard.register_default_router();
    let child = Logger::get_logger("hdr.child");
    assert!(child.add_handler(Handler::new("hdr.a").unwrap()));
    assert!(child.add_handler(Handler::new("hdr.b").unwrap()));
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || child.info("fanout"));
    child.clear_handlers();
    assert!(guard.flush(Some(Duration::from_secs(10))));

    let a = std::fs::read_to_string(dir.join("hdr.a/hdr.a.log")).unwrap_or_default();
    let b = std::fs::read_to_string(dir.join("hdr.b/hdr.b.log")).unwrap_or_default();
    assert!(a.contains("hdr.a|INFO|fanout"), "{a}");
    assert!(b.contains("hdr.b|INFO|fanout"), "{b}");
    assert!(!a.contains("hdr.b|"), "{a}");
    assert!(!b.contains("hdr.a|"), "{b}");
    let child_text =
        std::fs::read_to_string(dir.join("hdr.child/hdr.child.log")).unwrap_or_default();
    assert!(child_text.is_empty());

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证父链、effective level 继承和预过滤.
#[test]
fn parent_chain_and_effective_level_inheritance() {
    let _guard = test_guard();
    let _cleanup = GlobalCleanup;
    let dir = temp_dir("level");
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("lev")
                .target("lev")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("lev.parent.child")
                .target("lev.parent.child")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    guard.register_default_router();

    let child = Logger::get_logger("lev.parent.child");
    let parent = child.parent().expect("parent logger");
    assert_eq!(parent.name(), "lev.parent");
    assert!(child.effective_level() == FlareLevel::Trace);

    parent.set_level(Some(FlareLevel::Warn));
    assert_eq!(child.effective_level(), FlareLevel::Warn);
    assert_eq!(parent.effective_level(), FlareLevel::Warn);

    let subscriber = tracing_subscriber::registry().with(layer.clone());
    tracing::subscriber::with_default(subscriber, || {
        child.debug("filtered by inherited level");
        child.warn("accepted by inherited level");
    });
    parent.set_level(None);
    assert_eq!(child.effective_level(), FlareLevel::Trace);

    // root 级别是全局默认值；未显式设置的整条父链都继承它.
    Logger::root().set_level(Some(FlareLevel::Warn));
    assert_eq!(child.effective_level(), FlareLevel::Warn);
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        child.info("filtered by root level");
        child.warn("accepted by root level");
    });
    Logger::root().set_level(None);

    assert!(guard.flush(Some(Duration::from_secs(10))));
    let text = std::fs::read_to_string(dir.join("lev.parent.child/lev.parent.child.log"))
        .unwrap_or_default();
    assert!(text.contains("accepted by inherited level"));
    assert!(!text.contains("filtered by inherited level"));
    assert!(text.contains("accepted by root level"));
    assert!(!text.contains("filtered by root level"));

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证 parent handler 传播、handler 级别、propagate=false 回退与去重.
#[test]
fn handler_propagation_level_and_dedup() {
    let _guard = test_guard();
    let _cleanup = GlobalCleanup;
    let dir = temp_dir("propagate");
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("app")
                .target("app")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("app.child")
                .target("app.child")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("audit")
                .target("audit")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    guard.register_default_router();

    let child = Logger::get_logger("app.child");
    let parent = child.parent().expect("parent logger");
    let audit = Handler::new("audit").expect("audit handler");
    audit.set_level(Some(FlareLevel::Info));
    assert!(parent.add_handler(audit.clone()));

    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        child.info("via parent handler");
        child.debug("filtered by handler level");
        child.set_propagate(false);
        child.info("own service fallback");
        child.add_handler(audit.clone());
        child.info("own handler");
    });
    child.set_propagate(true);
    child.clear_handlers();
    parent.clear_handlers();
    assert!(guard.flush(Some(Duration::from_secs(10))));

    let audit_text = std::fs::read_to_string(dir.join("audit/audit.log")).unwrap_or_default();
    assert_eq!(audit_text.matches("via parent handler").count(), 1);
    assert!(!audit_text.contains("filtered by handler level"));
    assert!(!audit_text.contains("own service fallback"));
    assert_eq!(audit_text.matches("own handler").count(), 1);

    let child_text =
        std::fs::read_to_string(dir.join("app.child/app.child.log")).unwrap_or_default();
    assert!(child_text.contains("own service fallback"));
    assert!(!child_text.contains("via parent handler"));

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证 global/root handler 以及同一 service 多 handler 的去重.
#[test]
fn global_handlers_are_deduplicated_by_service() {
    let _guard = test_guard();
    let _cleanup = GlobalCleanup;
    let dir = temp_dir("global");
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("g.child")
                .target("g.child")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("audit")
                .target("audit")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    guard.register_default_router();

    let first = Handler::new("audit").expect("audit handler 1");
    let second = Handler::new("audit").expect("audit handler 2");
    assert!(Logger::add_global_handler(first.clone()));
    assert!(!Logger::add_global_handler(first));
    assert!(Logger::add_global_handler(second));

    let child = Logger::get_logger("g.child");
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        child.info("global once");
    });
    Logger::clear_global_handlers();
    assert!(guard.flush(Some(Duration::from_secs(10))));

    let audit_text = std::fs::read_to_string(dir.join("audit/audit.log")).unwrap_or_default();
    assert_eq!(audit_text.matches("global once").count(), 1);

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证 handler 在 router 替换后按 service 名重绑定.
#[test]
fn handlers_rebind_after_router_replacement() {
    let _guard = test_guard();
    let _cleanup = GlobalCleanup;
    let first_dir = temp_dir("rebind-old");
    let (layer, guard) = FlareConfig::new()
        .base_path(&first_dir)
        .service(
            ServiceConfig::new("rebind.child")
                .target("rebind.child")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("rebind.audit")
                .target("rebind.audit")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    guard.register_default_router();
    let child = Logger::get_logger("rebind.child");
    assert!(child.add_handler(Handler::new("rebind.audit").unwrap()));
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || child.info("before replace"));
    assert!(guard.flush(Some(Duration::from_secs(10))));
    drop(guard);

    let second_dir = temp_dir("rebind-new");
    let (layer, guard) = FlareConfig::new()
        .base_path(&second_dir)
        .service(
            ServiceConfig::new("rebind.child")
                .target("rebind.child")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("rebind.audit")
                .target("rebind.audit")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    guard.register_default_router();
    let rebound = Logger::get_logger("rebind.child");
    assert!(child.is_same(&rebound));
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || child.info("after replace"));
    assert!(guard.flush(Some(Duration::from_secs(10))));

    let old = std::fs::read_to_string(first_dir.join("rebind.audit/rebind.audit.log"))
        .unwrap_or_default();
    let new = std::fs::read_to_string(second_dir.join("rebind.audit/rebind.audit.log"))
        .unwrap_or_default();
    assert!(old.contains("before replace"));
    assert!(!old.contains("after replace"));
    assert!(new.contains("after replace"));
    assert!(!new.contains("before replace"));

    child.clear_handlers();
    drop(guard);
    let _ = std::fs::remove_dir_all(&first_dir);
    let _ = std::fs::remove_dir_all(&second_dir);
}

/// 验证 handler 批量路径按流式输入投递大量记录.
#[test]
fn handler_log_many_streams_large_input() {
    let _guard = test_guard();
    let _cleanup = GlobalCleanup;
    let dir = temp_dir("stream");
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("stream.child")
                .target("stream.child")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never)
                .flush_each(false),
        )
        .service(
            ServiceConfig::new("stream.audit")
                .target("stream.audit")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never)
                .flush_each(false),
        )
        .build()
        .unwrap();
    guard.register_default_router();
    let child = Logger::get_logger("stream.child");
    assert!(child.add_handler(Handler::new("stream.audit").unwrap()));
    let count = 10_000usize;
    let records = (0..count)
        .map(|index| FlareRecord::new(FlareCoreLevel::Info, format!("stream line {index}")));
    let _subscriber = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    assert_eq!(child.log_many(records), count);
    child.clear_handlers();
    assert!(guard.flush(Some(Duration::from_secs(10))));

    let text =
        std::fs::read_to_string(dir.join("stream.audit/stream.audit.log")).unwrap_or_default();
    assert_eq!(text.lines().count(), count);
    assert!(text.contains("stream line 9999"));
    assert!(
        std::fs::read_to_string(dir.join("stream.child/stream.child.log"))
            .unwrap_or_default()
            .is_empty()
    );

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证批量路径也遵守 handler 投递.
#[test]
fn log_many_routes_to_handlers() {
    let _guard = test_guard();
    let _cleanup = GlobalCleanup;
    let dir = temp_dir("batch");
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("batch.child")
                .target("batch.child")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("audit")
                .target("audit")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    guard.register_default_router();

    let child = Logger::get_logger("batch.child");
    let audit = Handler::new("audit").expect("audit handler");
    assert!(child.add_handler(audit));
    let parent = child.parent().expect("parent logger");
    parent.set_level(Some(FlareLevel::Warn));
    let records = [
        FlareRecord::new(FlareCoreLevel::Info, "batch one"),
        FlareRecord::new(FlareCoreLevel::Warning, "batch two"),
        FlareRecord::new(FlareCoreLevel::Error, "batch three"),
    ];
    let _subscriber = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    assert_eq!(child.log_many(records), 2);
    parent.set_level(None);
    assert!(guard.flush(Some(Duration::from_secs(10))));

    let mut audit_text = std::fs::read_to_string(dir.join("audit/audit.log")).unwrap_or_default();
    audit_text
        .push_str(&std::fs::read_to_string(dir.join("audit/audit_error.log")).unwrap_or_default());
    assert!(!audit_text.contains("batch one"));
    assert!(audit_text.contains("batch two"));
    assert!(audit_text.contains("batch three"));
    let child_text =
        std::fs::read_to_string(dir.join("batch.child/batch.child.log")).unwrap_or_default();
    assert!(child_text.is_empty());

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}
