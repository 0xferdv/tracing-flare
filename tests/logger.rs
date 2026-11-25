//! Logger 门面与配置集成测试.
//!
//! 覆盖同步/异步方法、名称优先、生产默认值、Python 风格注册表、动态 service、级别过滤.

use std::future::Future;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use tracing_subscriber::prelude::*;

use tracing_flare::{
    FlareConfig, FlareLevel, FlareRotation, Logger, LoggerConfig, ServiceConfig, UnknownTarget,
};

static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);

/// 创建隔离临时目录；参数无，返回路径.
fn temp_dir() -> std::path::PathBuf {
    let serial = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("flare-logger-test-{}-{serial}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 空 waker：仅用于同步驱动 future，不实现唤醒逻辑.
struct NoopWaker;

impl Wake for NoopWaker {
    /// 空唤醒；参数 `self`，返回无.
    fn wake(self: Arc<Self>) {}
}

/// 在单线程/tokio 之外阻塞执行 future.
///
/// 参数：`future` 是待执行 future；返回值：其输出.
/// 仅用于测试同步等待异步门面.
#[allow(clippy::manual_noop_waker)]
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWaker));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("logger future must be immediately ready"),
    }
}

/// 验证 Logger 门面同步/异步方法按名称路由.
///
/// 参数：无；断言：各方法事件进入对应 service 文件.
#[test]
fn logger_facade_sync_and_async_methods_route_by_name() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("billing")
                .target("billing")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("inventory")
                .target("inventory")
                .level(FlareLevel::Trace)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();

    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let billing = Logger::new("billing");
        let inventory = Logger::new("inventory");
        assert_eq!(billing.name(), "billing");

        billing.trace("billing trace");
        billing.debug("billing debug");
        billing.info("billing info");
        billing.warning("billing warning");
        billing.error("billing error");
        billing.critical("billing critical");
        billing.fatal("billing fatal");

        inventory.info("inventory info");
        block_on(inventory.ainfo("inventory async info"));
        block_on(billing.ainfo("billing async info"));
        block_on(billing.awarn("billing async warning"));
        block_on(billing.aerror("billing async error"));
        block_on(billing.acritical("billing async critical"));
        block_on(billing.afatal("billing async fatal"));
        block_on(billing.alog(FlareLevel::Debug, "billing explicit level"));
    });

    assert!(guard.flush(Some(Duration::from_secs(5))));
    let normal = std::fs::read_to_string(dir.join("billing/billing.log")).unwrap();
    let errors = std::fs::read_to_string(dir.join("billing/billing_error.log")).unwrap();
    let inventory_normal = std::fs::read_to_string(dir.join("inventory/inventory.log")).unwrap();
    let inventory_errors =
        std::fs::read_to_string(dir.join("inventory/inventory_error.log")).unwrap_or_default();

    for line in [
        "billing trace",
        "billing debug",
        "billing info",
        "billing warning",
        "billing async info",
        "billing async warning",
        "billing explicit level",
    ] {
        assert!(normal.contains(line), "missing {line:?} in {normal}");
    }
    for line in [
        "billing error",
        "billing critical",
        "billing fatal",
        "billing async error",
        "billing async critical",
        "billing async fatal",
    ] {
        assert!(errors.contains(line), "missing {line:?} in {errors}");
    }
    assert!(!normal.contains("billing error"), "{normal}");
    assert!(!errors.contains("billing info"), "{errors}");
    assert!(
        inventory_normal.contains("inventory info"),
        "{inventory_normal}"
    );
    assert!(
        inventory_normal.contains("inventory async info"),
        "{inventory_normal}"
    );
    assert!(
        !inventory_errors.contains("inventory info"),
        "{inventory_errors}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证 logger 名优先于其他 service 的 target 别名.
///
/// 参数：无；断言：同名 logger 路由到自己的 service，不被 target 前缀抢走.
#[test]
fn logger_name_wins_over_a_target_alias_of_another_service() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("alpha")
                .target("shared")
                .rotation(FlareRotation::Never),
        )
        .service(
            ServiceConfig::new("shared")
                .target("beta")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        Logger::new("shared").info("name-priority line");
    });
    assert!(guard.flush(Some(Duration::from_secs(5))));
    let beta = std::fs::read_to_string(dir.join("shared/shared.log")).unwrap();
    let alpha = std::fs::read_to_string(dir.join("alpha/alpha.log")).unwrap_or_default();
    assert!(beta.contains("name-priority line"), "{beta}");
    assert!(!alpha.contains("name-priority line"), "{alpha}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证生产模板默认值可逐项覆盖.
///
/// 参数：无；断言：默认队列/轮转/压缩符合文档，覆盖字段生效.
#[test]
fn logger_config_has_production_defaults_and_overrides() {
    let profile = LoggerConfig::new("production");
    assert_eq!(profile.name(), "production");
    let defaults = profile.clone().to_service();
    assert_eq!(
        defaults.rotation,
        FlareRotation::size(128 * 1024 * 1024, 10)
    );
    #[cfg(feature = "compression-zstd")]
    assert_eq!(
        defaults.compression,
        Some(tracing_flare::FlareCompression::Zstd)
    );
    #[cfg(not(feature = "compression-zstd"))]
    assert_eq!(defaults.compression, None);
    assert_eq!(defaults.queue_size, 262_144);
    assert!(!defaults.flush_each);
    assert_eq!(defaults.file_buffer_size, 256 * 1024);

    let custom = LoggerConfig::new("custom")
        .level(FlareLevel::Debug)
        .rotation(FlareRotation::Never)
        .no_compression()
        .queue_size(1_024)
        .flush_each(true);
    let service = custom.into_service();
    assert_eq!(service.level, FlareLevel::Debug);
    assert_eq!(service.rotation, FlareRotation::Never);
    assert_eq!(service.compression, None);
    assert_eq!(service.queue_size, 1_024);
    assert!(service.flush_each);
}

/// 验证 get_logger/get_handler 是同一门面别名.
///
/// 参数：无；断言：两对象 `is_same` 为 true.
#[test]
fn python_style_logger_accessors_are_aliases() {
    let first = Logger::get_logger("registry-svc");
    let second = Logger::get_logger("registry-svc");
    let handler = Logger::get_handler("registry-svc");
    assert!(first.is_same(&second));
    assert!(first.is_same(&handler));
    assert_eq!(first.name(), "registry-svc");
}

/// 验证配置可同时配置静态与动态 service.
///
/// 参数：无；断言：静态 service 正常写，动态 service 按 Create 策略创建.
#[test]
fn logger_config_can_configure_static_and_dynamic_services() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .unknown_target(UnknownTarget::Create)
        .service(
            LoggerConfig::new("static")
                .level(FlareLevel::Debug)
                .rotation(FlareRotation::Never)
                .no_compression(),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        Logger::get_logger("static").debug("static debug");
        let dynamic = guard
            .logger_with_config(
                "dynamic-configured",
                LoggerConfig::new("dynamic-configured")
                    .level(FlareLevel::Debug)
                    .rotation(FlareRotation::Never)
                    .no_compression(),
            )
            .unwrap();
        dynamic.debug("dynamic debug");
        assert_eq!(
            guard.handle("dynamic-configured").unwrap().name(),
            "dynamic-configured"
        );
    });
    assert!(guard.flush(Some(Duration::from_secs(10))));
    assert!(std::fs::read_to_string(dir.join("static/static.log"))
        .unwrap()
        .contains("static debug"));
    assert!(
        std::fs::read_to_string(dir.join("dynamic-configured/dynamic-configured.log"))
            .unwrap()
            .contains("dynamic debug")
    );
    let _ = guard.shutdown(Some(Duration::from_secs(10)));
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证动态 service 只能通过 guard 显式创建.
///
/// 参数：无；断言：事件路径不会自动创建 service，guard API 可以.
#[test]
fn dynamic_service_is_created_only_through_the_guard() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .unknown_target(UnknownTarget::Create)
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let log = guard.logger("dynamic-service").unwrap();
        log.info("created on demand");
        // 复用同名 logger 返回同一个运行中的 service.
        let _ = guard.logger("dynamic-service").unwrap();
        assert!(guard.handle("dynamic-service").is_some());
        assert!(guard.logger("../escape").is_err());
    });
    assert!(guard.flush(Some(Duration::from_secs(10))));
    let text = std::fs::read_to_string(dir.join("dynamic-service/dynamic-service.log")).unwrap();
    assert!(text.contains("created on demand"), "{text}");
    let health = guard.health();
    assert!(
        health
            .services
            .iter()
            .any(|service| service.name == "dynamic-service"),
        "{health:?}"
    );
    assert!(guard.shutdown(Some(Duration::from_secs(10))));
    assert!(guard.logger("after-shutdown").is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证动态创建需要 UnknownTarget::Create.
///
/// 参数：无；断言：非 Create 策略下 `guard.logger` 返回错误.
#[test]
fn dynamic_service_requires_create_policy() {
    let dir = temp_dir();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .unknown_target(UnknownTarget::Drop)
        .service(
            ServiceConfig::new("static")
                .target("static")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    assert!(guard.logger("not-configured").is_err());
    assert!(guard.shutdown(Some(Duration::from_secs(5))));
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证未知名称在 Drop 策略下被丢弃.
///
/// 参数：无；断言：不产生 fallback 文件，事件被计 no_route/rejected.
#[test]
fn logger_unknown_name_is_dropped_without_fallback() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .unknown_target(UnknownTarget::Drop)
        .service(
            ServiceConfig::new("known")
                .target("known")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        Logger::new("known").info("known line");
        Logger::new("missing").error("missing line");
    });
    assert!(guard.flush(Some(Duration::from_secs(5))));
    let known = std::fs::read_to_string(dir.join("known/known.log")).unwrap();
    assert!(known.contains("known line"), "{known}");
    assert!(!known.contains("missing line"), "{known}");
    assert!(guard.stats().no_route >= 1, "{:?}", guard.stats());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证未知名称在 Fallback 策略下走默认 service.
///
/// 参数：无；断言：默认 service 文件出现消息.
#[test]
fn logger_unknown_name_uses_fallback() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("known")
                .target("known")
                .rotation(FlareRotation::Never),
        )
        .fallback(
            ServiceConfig::new("app")
                .target("app")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        Logger::new("missing").critical("fallback critical");
    });
    assert!(guard.flush(Some(Duration::from_secs(5))));
    let errors = std::fs::read_to_string(dir.join("app/app_error.log")).unwrap();
    assert!(errors.contains("fallback critical"), "{errors}");
    assert!(errors.contains("CRITICAL"), "{errors}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证门面遵守 service 级别阈值.
///
/// 参数：无；断言：低于阈值的事件不落盘.
#[test]
fn logger_respects_service_level_filtering() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("critical")
                .target("critical")
                .level(FlareLevel::Critical)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let log = Logger::new("critical");
        log.error("plain error and async plain error");
        block_on(log.aerror("async plain error"));
        log.critical("only critical");
    });
    assert!(guard.flush(Some(Duration::from_secs(5))));
    let text = std::fs::read_to_string(dir.join("critical/critical_error.log")).unwrap();
    assert!(!text.contains("plain error"), "{text}");
    assert!(!text.contains("async plain error"), "{text}");
    assert!(text.contains("only critical"), "{text}");
    assert!(guard.stats().filtered >= 2, "{:?}", guard.stats());
    let _ = std::fs::remove_dir_all(&dir);
}
