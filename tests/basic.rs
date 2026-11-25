//! 基础端到端集成测试.
//!
//! 覆盖目录布局、普通/错误文件分流、级别阈值、模板/轮转、taskName、轮转冲突退避、错误堆栈、reopen、
//! 快路径与通用渲染等价、配置校验、压缩、实例锁、fork 与 unknown target 策略.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tracing_subscriber::prelude::*;

use tracing_flare::{
    flare_critical, flare_error, flare_info, FlareConfig, FlareLevel, FlareRotation, RateLimit,
    ServiceConfig, UnknownTarget, DEFAULT_FORMAT,
};

static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);

/// 创建隔离临时目录.
///
/// 参数：无；返回值：已清空并创建的目录路径.
/// 用进程 ID + 原子序号避免并行测试冲突.
fn temp_dir() -> std::path::PathBuf {
    let serial = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("flare-test-{}-{serial}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 验证目录布局与 ERROR/CRITICAL 分流.
///
/// 参数：无；断言：普通文件含 INFO、错误文件含 ERROR/CRITICAL、CRITICAL 渲染、fallback 生效.
#[test]
fn writes_to_service_folders_and_splits_errors() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("billing")
                .target("billing")
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
        flare_info!(target: "billing", order = 42, "order created");
        flare_info!(target: "billing", "line with\nnewline");
        flare_info!(target: "billing", "c1\u{85}control");
        flare_error!(target: "billing", "boom");
        flare_critical!(target: "billing", "critical failure");
        flare_info!(target: "unknown", "fallback line");
    });

    assert!(guard.flush(Some(Duration::from_secs(30))));
    let normal = std::fs::read_to_string(dir.join("billing/billing.log")).unwrap();
    let errors = std::fs::read_to_string(dir.join("billing/billing_error.log")).unwrap();
    assert!(normal.contains("order created"), "{normal}");
    assert!(normal.contains("line with\\nnewline"), "{normal}");
    assert!(normal.contains("c1\\u{85}control"), "{normal}");
    assert!(!normal.contains("boom"), "{normal}");
    assert!(!normal.contains("critical failure"), "{normal}");
    assert!(errors.contains("boom"), "{errors}");
    assert!(errors.contains("critical failure"), "{errors}");
    assert!(errors.contains("CRITICAL"), "{errors}");
    let fallback = std::fs::read_to_string(dir.join("app/app.log")).unwrap();
    assert!(fallback.contains("fallback line"), "{fallback}");

    let stats = guard.stats();
    assert!(stats.seen >= 4, "{stats:?}");
    assert!(stats.forwarded >= 4, "{stats:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证 CRITICAL 阈值会过滤普通 ERROR.
///
/// 参数：无；断言：普通 error 不落错误文件，flare_critical 落盘.
#[test]
fn critical_threshold_filters_plain_errors() {
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
        flare_error!(target: "critical", "plain error");
        flare_critical!(target: "critical", "only critical");
    });
    assert!(guard.flush(Some(Duration::from_secs(30))));
    let text = std::fs::read_to_string(dir.join("critical/critical_error.log")).unwrap();
    assert!(!text.contains("plain error"), "{text}");
    assert!(text.contains("only critical"), "{text}");
    assert!(guard.shutdown(Some(Duration::from_secs(5))));
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证自定义模板与大小轮转.
///
/// 参数：无；断言：首行模板前缀正确、生成 `.1` 归档、健康计数有 rollover.
#[test]
fn custom_format_and_rotation_are_applied() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .format("%(levelname)s|%(name)s|%(module_chain)s|%(message)s")
        .service(
            ServiceConfig::new("fmt")
                .target("fmt")
                .rotation(FlareRotation::size(64, 2)),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        for index in 0..20 {
            flare_info!(target: "fmt", "record {} {}", index, "x".repeat(20));
        }
    });
    assert!(guard.flush(Some(Duration::from_secs(30))));
    let text = std::fs::read_to_string(dir.join("fmt/fmt.log")).unwrap();
    assert!(
        text.lines().next().unwrap().starts_with("INFO|fmt|"),
        "{text}"
    );
    assert!(
        dir.join("fmt/fmt.log.1").exists(),
        "size rotation should create an archive"
    );
    let health = guard.health();
    let service = health
        .services
        .iter()
        .find(|service| service.name == "fmt")
        .expect("fmt service health");
    let file_sink = service
        .snapshot
        .sinks
        .iter()
        .find(|sink| sink.name == "file")
        .expect("file sink health");
    assert!(file_sink.rollovers > 0, "{health:?}");
    assert_eq!(file_sink.compression_jobs, 0, "{health:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证 `%(taskName)s` 使用第一个 span 名.
///
/// 参数：无；断言：span 内外层日志包含 `request|inside`.
#[test]
fn task_name_renders_the_first_span() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .format("%(taskName)s|%(message)s")
        .service(
            ServiceConfig::new("span")
                .target("span")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::info_span!("request").in_scope(|| {
            flare_info!(target: "span", "inside");
        });
    });
    assert!(guard.flush(Some(Duration::from_secs(30))));
    let text = std::fs::read_to_string(dir.join("span/span.log")).unwrap();
    assert!(text.contains("request|inside"), "{text}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证轮转目标冲突时退避并继续写.
///
/// 参数：无；断言：活动文件包含冲突后记录，健康计数 rollover_errors/warnings 增加.
#[test]
fn rotation_conflict_backs_off_and_keeps_writing() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("conflict")
                .target("conflict")
                .rotation(FlareRotation::size(64, 2)),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    let active = dir.join("conflict/conflict.log");
    let blocked_target = dir.join("conflict/conflict.log.1");

    tracing::subscriber::with_default(subscriber, || {
        flare_info!(target: "conflict", "first line");
        let handle = guard.handle("conflict").expect("conflict service");
        assert!(handle.flush(Some(Duration::from_secs(10))));
        // 归档路径上是目录会让 rename 目标被占用.轮转必须退避并继续追加到活动文件，
        // 而不是让每条记录都失败.
        std::fs::create_dir_all(&blocked_target).unwrap();
        flare_info!(target: "conflict", "after conflict line");
        assert!(handle.flush(Some(Duration::from_secs(10))));
    });

    let text = std::fs::read_to_string(&active).unwrap();
    assert!(text.contains("after conflict line"), "{text}");
    let health = guard.health();
    let service = health
        .services
        .iter()
        .find(|service| service.name == "conflict")
        .expect("conflict health");
    let sink = service
        .snapshot
        .sinks
        .iter()
        .find(|sink| sink.name == "file")
        .expect("file sink");
    assert!(sink.rollover_errors >= 1, "{health:?}");
    assert!(sink.warnings >= 1, "{health:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证 `backtrace_on_error` 为错误记录追加堆栈.
///
/// 参数：无；断言：错误文件多行、普通文件无堆栈.
#[test]
fn backtrace_on_error_appends_frames_to_error_records() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("bt")
                .target("bt")
                .rotation(FlareRotation::Never)
                .backtrace_on_error(true),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        flare_info!(target: "bt", "normal line");
        flare_error!(target: "bt", "error with backtrace");
    });
    assert!(guard.flush(Some(Duration::from_secs(10))));
    let normal = std::fs::read_to_string(dir.join("bt/bt.log")).unwrap();
    let errors = std::fs::read_to_string(dir.join("bt/bt_error.log")).unwrap();
    assert!(normal.contains("normal line"), "{normal}");
    assert!(!normal.contains("backtrace"), "{normal}");
    assert!(errors.contains("error with backtrace"), "{errors}");
    assert!(
        errors.lines().count() > 2,
        "error record should include stack frames: {errors}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证外部 rename 后 reopen 切换新文件.
///
/// 参数：无；断言：旧归档只含 before，新活动文件只含 after.
#[test]
fn reopen_switches_to_the_new_active_file_after_external_rename() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("reopen")
                .target("reopen")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let handle = guard.handle("reopen").expect("reopen service");
    let subscriber = tracing_subscriber::registry().with(layer);
    let active = dir.join("reopen/reopen.log");
    let rotated = dir.join("reopen/reopen.log.old");

    tracing::subscriber::with_default(subscriber, || {
        flare_info!(target: "reopen", "before rotation");
        assert!(handle.flush(Some(Duration::from_secs(10))));

        // 第一次 reopen 释放句柄，使文件在 Windows 上也能被 rename.
        assert!(handle.reopen(Some(Duration::from_secs(10))));
        std::fs::rename(&active, &rotated).expect("rename active log");
        // 第二次 reopen 从零重新打开已不存在的活动路径.
        assert!(handle.reopen(Some(Duration::from_secs(10))));

        flare_info!(target: "reopen", "after rotation");
        assert!(handle.flush(Some(Duration::from_secs(10))));
    });

    let rotated_text = std::fs::read_to_string(&rotated).unwrap();
    let active_text = std::fs::read_to_string(&active).unwrap();
    assert!(rotated_text.contains("before rotation"), "{rotated_text}");
    assert!(!rotated_text.contains("after rotation"), "{rotated_text}");
    assert!(active_text.contains("after rotation"), "{active_text}");
    assert!(!active_text.contains("before rotation"), "{active_text}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 在固定调用点发一条消息.
///
/// 参数：`log` 是目标门面；返回值：无.
/// 用于快路径/通用渲染对比，保证行号一致.
fn emit_same_line(log: &tracing_flare::Logger) {
    log.info("same message");
}

/// 去掉首行时间戳与可选前缀，便于比较其余字段.
///
/// 参数：`line` 是日志行；返回值：`[TIME]` 加剩余内容.
fn strip_timestamp(line: &str) -> String {
    let line = line.strip_prefix('X').unwrap_or(line);
    match line.find(']') {
        Some(end) => format!("[TIME]{}", &line[end + 1..]),
        None => line.to_string(),
    }
}

/// 验证默认模板快路径与通用渲染输出一致.
///
/// 参数：无；断言：去掉时间戳后，快路径与“X + 通用模板”输出相同.
#[test]
fn fast_default_format_matches_generic_renderer() {
    let fast_dir = temp_dir();
    let (fast_layer, fast_guard) = FlareConfig::new()
        .base_path(&fast_dir)
        .service(
            ServiceConfig::new("cmp")
                .target("cmp")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let fast_subscriber = tracing_subscriber::registry().with(fast_layer);
    let fast_log = tracing_flare::Logger::new("cmp");
    tracing::subscriber::with_default(fast_subscriber, || emit_same_line(&fast_log));
    assert!(fast_guard.flush(Some(Duration::from_secs(10))));
    let fast_line = std::fs::read_to_string(fast_dir.join("cmp/cmp.log"))
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_string();

    let generic_dir = temp_dir();
    let generic_format = format!("X{DEFAULT_FORMAT}");
    let (generic_layer, generic_guard) = FlareConfig::new()
        .base_path(&generic_dir)
        .format(generic_format)
        .service(
            ServiceConfig::new("cmp")
                .target("cmp")
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let generic_subscriber = tracing_subscriber::registry().with(generic_layer);
    let generic_log = tracing_flare::Logger::new("cmp");
    tracing::subscriber::with_default(generic_subscriber, || emit_same_line(&generic_log));
    assert!(generic_guard.flush(Some(Duration::from_secs(10))));
    let generic_line = std::fs::read_to_string(generic_dir.join("cmp/cmp.log"))
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_string();

    assert_eq!(
        strip_timestamp(&fast_line),
        strip_timestamp(&generic_line),
        "fast={fast_line}\ngeneric={generic_line}"
    );
    let _ = std::fs::remove_dir_all(&fast_dir);
    let _ = std::fs::remove_dir_all(&generic_dir);
}

/// 验证配置错误在构建期返回.
///
/// 参数：无；断言：非法模板、无 sink、零限流均 `build().is_err()`.
#[test]
fn config_errors_are_reported() {
    let dir = temp_dir();
    let bad_format = FlareConfig::new()
        .base_path(&dir)
        .format("%(nope)s")
        .build();
    assert!(bad_format.is_err());
    let no_sink = FlareConfig::new()
        .base_path(&dir)
        .service(ServiceConfig::new("empty").files(false).console(false))
        .build();
    assert!(no_sink.is_err());
    let zero_rate = FlareConfig::new()
        .base_path(&dir)
        .service(ServiceConfig::new("rate").rate_limit(RateLimit::per_second(0)))
        .build();
    assert!(zero_rate.is_err(), "rate limit 0 must be rejected");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证压缩归档生成.
///
/// 参数：无；断言：轮转后存在 `.gz` 归档，健康计数 compression_jobs 归零.
#[cfg(feature = "compression-gzip")]
#[test]
fn compression_is_applied_to_rotated_archives() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("gz")
                .target("gz")
                // 2048 字节只制造 1 个归档：本测试验证压缩链路，不需要压力测试.
                // Windows 上每次归档 fsync 可能被 AV/文件系统放大到秒级；更小的轮转阈值会让
                // 多次压缩的累计延迟超过 flush 预算，产生与环境相关的假失败.
                .rotation(FlareRotation::size(2048, 2))
                .compression(tracing_flare::FlareCompression::Gzip),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        for index in 0..30 {
            flare_info!(target: "gz", "compress {index} {}", "z".repeat(20));
        }
    });
    assert!(guard.flush(Some(Duration::from_secs(30))));
    assert!(
        dir.join("gz/gz.log.1.gz").exists(),
        "rotated archive should be compressed"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证未启用的压缩 codec 在构建期被拒绝.
///
/// 参数：无.
/// 断言：未启用任何压缩 feature 时，选择 gzip 返回配置错误且不创建 service 文件.
/// 原因：可选依赖不应在轮转时才失败；配置校验必须给出可操作的 feature 提示.
#[cfg(not(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-lz4"
)))]
#[test]
fn compression_codec_requires_matching_feature() {
    let dir = temp_dir();
    let result = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("gz-disabled")
                .target("gz-disabled")
                .rotation(FlareRotation::size(2048, 2))
                .compression(tracing_flare::FlareCompression::Gzip),
        )
        .build();
    let error = match result {
        Ok(_) => panic!("compression without its feature must be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("compression-gzip"), "{error}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证实例锁随 guard 生命周期获取/释放.
///
/// 参数：无；断言：锁文件存在、第二次构建被拒绝、guard drop 后可重新构建.
#[test]
fn instance_lock_is_acquired_and_released_with_the_guard() {
    let dir = temp_dir();
    let config = || {
        FlareConfig::new().base_path(&dir).service(
            ServiceConfig::new("locked")
                .target("locked")
                .instance_lock(true)
                .rotation(FlareRotation::Never),
        )
    };
    let (_layer, guard) = config().build().unwrap();
    let lock_path = dir.join("locked/locked.log.lock");
    assert!(lock_path.exists(), "instance lock file must exist");
    // 同进程内可重入.
    let (_layer2, guard2) = config().build().unwrap();
    assert!(guard.shutdown(Some(Duration::from_secs(5))));
    assert!(guard2.shutdown(Some(Duration::from_secs(5))));
    // 关停后锁被释放，新的构建可以重新获取它.
    let (_layer3, guard3) = config().build().unwrap();
    assert!(guard3.shutdown(Some(Duration::from_secs(5))));
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
/// 验证 fork 子进程丢弃 runtime 不死锁.
///
/// 参数：无；断言：子进程调用 `after_fork_child` 后能正常退出.
#[test]
fn after_fork_child_discards_runtime_without_deadlock() {
    let dir = temp_dir();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("fork")
                .target("fork")
                .instance_lock(true)
                .rotation(FlareRotation::Never),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        flare_info!(target: "fork", "parent-before-fork");
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            // 子进程：绝不能解锁父进程的实例锁，不能 flush 继承的 writer 缓冲；
            // 日志调用仍应退化为空操作.
            guard.after_fork_child();
            flare_info!(target: "fork", "child-must-not-write");
            let ok = guard.flush(Some(Duration::from_secs(1)));
            unsafe { libc::_exit(if ok { 0 } else { 3 }) };
        }
        let mut status = 0;
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
        assert_eq!(waited, pid);
        assert!(libc::WIFEXITED(status));
        assert_ne!(
            libc::WEXITSTATUS(status),
            3,
            "child flush must not hang/error"
        );
        flare_info!(target: "fork", "parent-after-fork");
    });
    assert!(guard.flush(Some(Duration::from_secs(30))));
    let text = std::fs::read_to_string(dir.join("fork/fork.log")).unwrap();
    assert!(text.contains("parent-before-fork"), "{text}");
    assert!(text.contains("parent-after-fork"), "{text}");
    assert!(!text.contains("child-must-not-write"), "{text}");
    assert!(guard.shutdown(Some(Duration::from_secs(5))));
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证 `UnknownTarget::Drop` 不创建 fallback 文件.
///
/// 参数：无；断言：无匹配 target 时没有落盘文件.
#[test]
fn unknown_target_drop_does_not_create_a_fallback_file() {
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
        flare_info!(target: "known", "known line");
        flare_info!(target: "unknown", "dropped line");
    });
    assert!(guard.flush(Some(Duration::from_secs(30))));
    let known = std::fs::read_to_string(dir.join("known/known.log")).unwrap();
    assert!(known.contains("known line"), "{known}");
    assert!(!known.contains("dropped line"), "{known}");
    assert!(
        !dir.join("flare").exists(),
        "no fallback service should be built"
    );
    assert!(guard.stats().no_route >= 1, "{:?}", guard.stats());
    let _ = std::fs::remove_dir_all(&dir);
}
