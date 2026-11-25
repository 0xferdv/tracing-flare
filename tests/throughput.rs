//! 端到端吞吐基准（`#[ignore]`，需显式 release 运行）.
//!
//! 通过公开 tracing → FlareLayer → 队列 → 文件路径测量 1M 条记录；`benchmark` 支持自定义模板以隔离
//! 格式化成本.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing_subscriber::prelude::*;

use tracing_flare::{
    FlareConfig, FlareCoreLevel, FlareRecord, FlareRotation, Logger, ServiceConfig,
};

/// 跑固定 1M 条记录并输出吞吐.
///
/// 参数：`label` 是场景名；`format` 可选模板（None 用默认）。返回值：无；断言：无丢弃、队列排空.
fn benchmark(label: &str, format: Option<&str>) {
    let dir = std::env::temp_dir().join(format!("flare-throughput-{}-{label}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mut config = FlareConfig::new().base_path(&dir);
    if let Some(format) = format {
        config = config.format(format);
    }
    let (layer, guard) = config
        .service(
            ServiceConfig::new("bench")
                .target("bench")
                .rotation(FlareRotation::Never)
                .queue_size(1_000_000)
                .flush_each(false)
                .file_buffer_size(256 * 1024),
        )
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(layer);
    let records = 1_000_000usize;
    let start = Instant::now();
    tracing::subscriber::with_default(subscriber, || {
        let log = Logger::new("bench");
        for index in 0..records {
            log.info(format_args!("throughput record {index}"));
        }
    });
    let enqueue_elapsed = start.elapsed();
    assert!(guard.flush(Some(Duration::from_secs(60))));
    let total = start.elapsed();

    let health = guard.health();
    let dropped: u64 = health
        .services
        .iter()
        .map(|service| service.snapshot.dropped)
        .sum();
    let forwarded = health.layer.forwarded;
    let queued: usize = health
        .services
        .iter()
        .map(|service| service.snapshot.queued_records)
        .sum();
    assert_eq!(dropped, 0, "benchmark must not drop: {health:?}");
    assert_eq!(queued, 0, "queue should be drained: {health:?}");
    assert!(forwarded >= records as u64, "{health:?}");

    let rate = records as f64 / total.as_secs_f64();
    eprintln!(
        "{label}: records={records} enqueue={enqueue_elapsed:?} total={total:?} rate={rate:.0}/s forwarded={forwarded} dropped={dropped}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// 1,000,000 条记录走 tracing -> FlareLayer -> engine -> 文件路径，使用默认生产格式.
///
/// 运行：
/// ```bash
/// cargo test -p tracing-flare --release --test throughput -- --ignored --nocapture
/// ```
/// 默认格式吞吐；参数无.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn one_million_records_to_file() {
    benchmark("default", None);
}

/// 同一路径只保留 `%(message)s`；把格式化/元数据成本与 I/O、路由成本隔离开.
/// `%(message)s` only 吞吐；参数无.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn one_million_records_message_only() {
    benchmark("message-only", Some("%(message)s"));
}

/// asctime 模板吞吐；参数无.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn one_million_records_asctime() {
    benchmark("asctime", Some("%(asctime)s %(message)s"));
}

/// module_chain 模板吞吐；参数无.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn one_million_records_module_chain() {
    benchmark("module-chain", Some("%(module_chain)s %(message)s"));
}

/// thread 模板吞吐；参数无.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn one_million_records_thread() {
    benchmark("thread", Some("%(thread)d %(message)s"));
}

/// 8 生产者聚合吞吐；参数无.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn one_million_records_eight_producers() {
    let dir = std::env::temp_dir().join(format!("flare-throughput-8p-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("bench")
                .target("bench")
                .rotation(FlareRotation::Never)
                .queue_size(1_000_000)
                .flush_each(false)
                .file_buffer_size(256 * 1024),
        )
        .build()
        .unwrap();
    let records = 1_000_000usize;
    let producers = 8usize;
    let start = Instant::now();
    let mut handles = Vec::with_capacity(producers);
    for producer in 0..producers {
        let layer = layer.clone();
        handles.push(std::thread::spawn(move || {
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                let log = Logger::new("bench");
                for index in 0..records / producers {
                    log.info(format_args!("p{producer} record {index}"));
                }
            });
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let enqueue_elapsed = start.elapsed();
    assert!(guard.flush(Some(Duration::from_secs(60))));
    let total = start.elapsed();
    let health = guard.health();
    let dropped: u64 = health
        .services
        .iter()
        .map(|service| service.snapshot.dropped)
        .sum();
    assert_eq!(dropped, 0, "{health:?}");
    let rate = records as f64 / total.as_secs_f64();
    eprintln!(
        "eight-producers: records={records} enqueue={enqueue_elapsed:?} total={total:?} rate={rate:.0}/s forwarded={} dropped={dropped}",
        health.layer.forwarded
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 4 shard × 8 生产者吞吐；参数无.
///
/// 每个 shard 独立队列、writer 和文件 sink；验证分片后 flush/close 聚合仍不丢记录.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn one_million_records_eight_producers_sharded() {
    let dir =
        std::env::temp_dir().join(format!("flare-throughput-8p-shard-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("bench")
                .target("bench")
                .rotation(FlareRotation::Never)
                .queue_size(1_000_000)
                .flush_each(false)
                .file_buffer_size(256 * 1024)
                .shards(4),
        )
        .build()
        .unwrap();
    let records = 1_000_000usize;
    let producers = 8usize;
    let start = Instant::now();
    let mut handles = Vec::with_capacity(producers);
    for producer in 0..producers {
        let layer = layer.clone();
        handles.push(std::thread::spawn(move || {
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                let log = Logger::new("bench");
                for index in 0..records / producers {
                    log.info(format_args!("p{producer} record {index}"));
                }
            });
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let enqueue_elapsed = start.elapsed();
    assert!(guard.flush(Some(Duration::from_secs(60))));
    let total = start.elapsed();
    let health = guard.health();
    let dropped: u64 = health
        .services
        .iter()
        .map(|service| service.snapshot.dropped)
        .sum();
    assert_eq!(dropped, 0, "{health:?}");
    let rate = records as f64 / total.as_secs_f64();
    eprintln!(
        "eight-producers-sharded(4): records={records} enqueue={enqueue_elapsed:?} total={total:?} rate={rate:.0}/s forwarded={} dropped={dropped}",
        health.layer.forwarded
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 8 线程预格式化批量提交，验证分片 writer 在 I/O 路径上的并行能力.
///
/// 每线程 50,000 条，共 400,000 条；记录在计时前构造，计时只包含 `log_many`/队列/写盘.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn sharded_log_many_parallel_writers() {
    let dir = std::env::temp_dir().join(format!(
        "flare-throughput-shard-batch-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("batchshard")
                .target("batchshard")
                .rotation(FlareRotation::Never)
                .queue_size(1_000_000)
                .flush_each(false)
                .file_buffer_size(256 * 1024)
                .shards(4),
        )
        .build()
        .unwrap();
    let handle = guard.handle("batchshard").unwrap();
    let logger = Arc::new(handle.logger().clone());
    let producers = 8usize;
    let per_producer = 50_000usize;
    let mut batches = Vec::with_capacity(producers);
    for producer in 0..producers {
        batches.push(
            (0..per_producer)
                .map(|index| {
                    FlareRecord::new(FlareCoreLevel::Info, format!("p{producer} batch {index}"))
                })
                .collect::<Vec<_>>(),
        );
    }
    let start = Instant::now();
    let mut threads = Vec::with_capacity(producers);
    for records in batches {
        let logger = Arc::clone(&logger);
        threads.push(std::thread::spawn(move || logger.log_many(records)));
    }
    let accepted: usize = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .sum();
    let enqueue_elapsed = start.elapsed();
    assert_eq!(accepted, producers * per_producer);
    assert!(handle.flush(Some(Duration::from_secs(60))));
    let total = start.elapsed();
    let rate = (producers * per_producer) as f64 / total.as_secs_f64();
    eprintln!(
        "sharded-log_many: records={} enqueue={enqueue_elapsed:?} total={total:?} rate={rate:.0}/s",
        producers * per_producer
    );
    assert_eq!(
        handle.snapshot().emitted.iter().sum::<u64>(),
        (producers * per_producer) as u64
    );
    assert!(handle.close(Some(Duration::from_secs(10))));
    let _ = std::fs::remove_dir_all(&dir);
}

/// 生产预设下各日志级别的逐条门面吞吐.
///
/// 使用 `FlareConfig::production` + `Logger::get_logger`；每级 200,000 条，批末 flush.
/// `debug`/`trace` 在默认 Info 级 service 下会被过滤，不产生文件输出，单独测的是过滤成本.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn production_logger_levels_throughput() {
    let dir = std::env::temp_dir().join(format!("flare-levels-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (layer, guard) = FlareConfig::production(&dir).build().unwrap();
    guard.register_default_router();
    let subscriber = tracing_subscriber::registry().with(layer);
    let dispatch = tracing::Dispatch::new(subscriber);
    let log = Logger::get_logger("levels");
    let count = 200_000usize;
    let measure = |label: &str, call: &dyn Fn(&Logger, usize)| {
        let start = Instant::now();
        tracing::dispatcher::with_default(&dispatch, || {
            for index in 0..count {
                call(&log, index);
            }
        });
        let enqueue = start.elapsed();
        assert!(guard.flush(Some(Duration::from_secs(60))));
        let total = start.elapsed();
        eprintln!(
            "level={label} records={count} enqueue={enqueue:?} total={total:?} rate={:.0}/s",
            count as f64 / total.as_secs_f64()
        );
    };
    measure("info", &|log, index| log.info(format_args!("line {index}")));
    measure("warn", &|log, index| log.warn(format_args!("line {index}")));
    measure("error", &|log, index| {
        log.error(format_args!("line {index}"))
    });
    measure("critical", &|log, index| {
        log.critical(format_args!("line {index}"))
    });
    measure("debug-filtered", &|log, index| {
        log.debug(format_args!("line {index}"))
    });
    measure("trace-filtered", &|log, index| {
        log.trace(format_args!("line {index}"))
    });
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 预格式化块吞吐：1,000,000 行，每次提交 10,000 行.
///
/// 参数：无.
/// 断言：无丢弃、队列排空、健康计数等于写入行数.
/// 原因：块路径把 10k 次记录入队降为 1 次 channel/CAS；它与 tracing 逐条路径的差距就是生产侧
/// 分发/格式化/记录构造成本的下界.
#[test]
#[ignore = "throughput benchmark; run explicitly in release mode"]
fn one_million_lines_preformatted_block() {
    let dir = std::env::temp_dir().join(format!("flare-throughput-block-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("block-bench")
                .target("block-bench")
                .rotation(FlareRotation::Never)
                .queue_size(1_000_000)
                .flush_each(false)
                .file_buffer_size(256 * 1024),
        )
        .build()
        .unwrap();
    let handle = guard.handle("block-bench").unwrap();
    let mut block = String::with_capacity(10_000 * 48);
    for index in 0..10_000 {
        use std::fmt::Write as _;
        let _ = writeln!(block, "preformatted block line {index}");
    }
    let records = 1_000_000usize;
    let start = Instant::now();
    for _ in 0..100 {
        assert_eq!(
            handle
                .logger()
                .log_preformatted_block(FlareCoreLevel::Info, block.as_str()),
            10_000
        );
    }
    let enqueue_elapsed = start.elapsed();
    assert!(guard.flush(Some(Duration::from_secs(60))));
    let total = start.elapsed();

    let health = guard.health();
    let service = health
        .services
        .iter()
        .find(|service| service.name == "block-bench")
        .expect("service snapshot");
    assert_eq!(service.snapshot.dropped, 0, "{health:?}");
    assert_eq!(
        service.snapshot.emitted.iter().sum::<u64>(),
        records as u64,
        "{health:?}"
    );
    let rate = records as f64 / total.as_secs_f64();
    eprintln!(
        "preformatted-block: records={records} enqueue={enqueue_elapsed:?} total={total:?} rate={rate:.0}/s"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
