//! 可选分片模式集成测试.
//!
//! 验证 `ServiceConfig::shards(n)` 会创建 n 个独立 writer/文件，flush/close 聚合后不丢记录.

use std::sync::Arc;
use std::time::Duration;

use tracing_flare::{FlareConfig, FlareCoreLevel, FlareRecord, FlareRotation, ServiceConfig};

/// 生成隔离的临时目录.
///
/// 参数：`tag` 是测试名；返回值：已清理的目录.
fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("flare-sharding-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 统计目录下普通日志文件的行数.
///
/// 参数：`dir` 是 service 目录；返回值：所有 `name.<shard>.log` 文件的行数之和.
fn count_lines(dir: &std::path::Path) -> usize {
    let mut total = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".log") && !name.contains("_error") {
            let text = std::fs::read_to_string(entry.path()).unwrap();
            total += text.lines().count();
        }
    }
    total
}

/// 多线程写入四个 shard，验证总记录数、快照和关停结果.
#[test]
fn sharded_service_writes_each_shard_and_closes_without_loss() {
    let dir = temp_dir("threads");
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("shardbench")
                .target("shardbench")
                .rotation(FlareRotation::Never)
                .queue_size(4_096)
                .flush_each(false)
                .shards(4),
        )
        .build()
        .unwrap();
    let handle = guard.handle("shardbench").unwrap();
    assert_eq!(handle.logger().shard_count(), 4);
    let logger = Arc::new(handle.logger().clone());
    let producers = 8usize;
    let per_producer = 100usize;
    let mut threads = Vec::with_capacity(producers);
    for producer in 0..producers {
        let logger = Arc::clone(&logger);
        threads.push(std::thread::spawn(move || {
            for index in 0..per_producer {
                assert!(logger.log(FlareCoreLevel::Info, format!("p{producer} line {index}")));
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(handle.flush(Some(Duration::from_secs(10))));
    let snapshot = handle.snapshot();
    assert_eq!(
        snapshot.emitted.iter().sum::<u64>(),
        (producers * per_producer) as u64
    );
    assert_eq!(snapshot.queue_capacity, 4_096);
    assert_eq!(snapshot.queued_records, 0);
    let shards = handle.logger().shard_snapshots();
    assert_eq!(shards.len(), 4);
    assert_eq!(
        shards
            .iter()
            .map(|snapshot| snapshot.emitted.iter().sum::<u64>())
            .sum::<u64>(),
        (producers * per_producer) as u64
    );
    assert_eq!(
        count_lines(&dir.join("shardbench")),
        producers * per_producer
    );
    assert!(handle.close(Some(Duration::from_secs(10))));
    let _ = std::fs::remove_dir_all(&dir);
}

/// 批量路径在分片 logger 上也能提交，并由 writer 回收批次向量.
#[test]
fn sharded_log_many_and_reusable_batch_are_forwarded() {
    let dir = temp_dir("batch");
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("shardbatch")
                .target("shardbatch")
                .rotation(FlareRotation::Never)
                .queue_size(2_048)
                .flush_each(false)
                .shards(2),
        )
        .build()
        .unwrap();
    let handle = guard.handle("shardbatch").unwrap();
    let records: Vec<FlareRecord> = (0..500)
        .map(|index| FlareRecord::new(FlareCoreLevel::Info, format!("batch {index}")))
        .collect();
    assert_eq!(handle.logger().log_many(records), 500);
    let mut batch = handle.logger().batch();
    for index in 0..500 {
        batch.push(FlareRecord::new(
            FlareCoreLevel::Info,
            format!("reuse {index}"),
        ));
    }
    assert_eq!(handle.logger().log_batch(&mut batch), 500);
    assert!(handle.flush(Some(Duration::from_secs(10))));
    assert_eq!(count_lines(&dir.join("shardbatch")), 1_000);
    assert!(handle.close(Some(Duration::from_secs(10))));
    let _ = std::fs::remove_dir_all(&dir);
}

/// 队列预算不能被 shard 数整除时，余数分配给前面的 shard，总和保持不变.
#[test]
fn sharded_queue_budget_is_split_without_losing_capacity() {
    let dir = temp_dir("budget");
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("shardbudget")
                .target("shardbudget")
                .rotation(FlareRotation::Never)
                .queue_size(30)
                .flush_each(false)
                .shards(3),
        )
        .build()
        .unwrap();
    let handle = guard.handle("shardbudget").unwrap();
    let logger = Arc::new(handle.logger().clone());
    let mut threads = Vec::new();
    for producer in 0..3 {
        let logger = Arc::clone(&logger);
        threads.push(std::thread::spawn(move || {
            for index in 0..3 {
                assert!(logger.log(FlareCoreLevel::Info, format!("p{producer} line {index}")));
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(handle.flush(Some(Duration::from_secs(10))));
    let snapshot = handle.snapshot();
    assert_eq!(snapshot.queue_capacity, 30);
    assert_eq!(snapshot.emitted.iter().sum::<u64>(), 9);
    assert_eq!(count_lines(&dir.join("shardbudget")), 9);
    assert!(handle.close(Some(Duration::from_secs(10))));
    let _ = std::fs::remove_dir_all(&dir);
}

/// 分片数不能超过队列预算，也不能为 0.
#[test]
fn sharding_config_rejects_impossible_budgets() {
    let dir = temp_dir("invalid");
    let too_many = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("bad")
                .target("bad")
                .queue_size(2)
                .shards(3),
        )
        .build();
    assert!(too_many.is_err());
    let zero = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("bad")
                .target("bad")
                .queue_size(2)
                .shards(0),
        )
        .build();
    assert!(zero.is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 队列预算不能整除时，shard 容量之和仍等于配置值.
#[test]
fn sharded_queue_capacity_preserves_total_with_remainder() {
    let dir = temp_dir("capacity");
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("shardcapacity")
                .target("shardcapacity")
                .rotation(FlareRotation::Never)
                .queue_size(10)
                .shards(3),
        )
        .build()
        .unwrap();
    let handle = guard.handle("shardcapacity").unwrap();
    let capacities: Vec<usize> = handle
        .logger()
        .shard_snapshots()
        .iter()
        .map(|snapshot| snapshot.queue_capacity)
        .collect();
    assert_eq!(capacities.iter().sum::<usize>(), 10);
    assert!(capacities.iter().all(|capacity| *capacity > 0));
    assert!(handle.close(Some(Duration::from_secs(10))));
    let _ = std::fs::remove_dir_all(&dir);
}
