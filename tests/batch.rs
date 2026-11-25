//! 公开批量路径集成测试.
//!
//! 验证 `FlareHandle::logger().log_many` + `FlareRecord` 能绕过逐条 tracing/模板，把预格式化记录直
//! 接写入文件 sink；每轮 1,000 条并 flush.

use std::time::Duration;

use tracing_flare::{FlareConfig, FlareCoreLevel, FlareRecord, FlareRotation, ServiceConfig};

/// 验证公开批量路径写入与计数.
///
/// 参数：无；断言：`log_many` 返回 1,000，文件恰好 1,000 行且包含最后一条.
#[test]
fn public_batch_path_enqueues_preformatted_records() {
    let dir = std::env::temp_dir().join(format!("flare-batch-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("batch")
                .target("batch")
                .rotation(FlareRotation::Never)
                .flush_each(false),
        )
        .build()
        .unwrap();
    let handle = guard.handle("batch").unwrap();
    let records: Vec<FlareRecord> = (0..1_000)
        .map(|index| FlareRecord::new(FlareCoreLevel::Info, format!("batch line {index}")))
        .collect();
    assert_eq!(handle.logger().log_many(records), 1_000);
    assert!(handle.flush(Some(Duration::from_secs(10))));

    let text = std::fs::read_to_string(dir.join("batch/batch.log")).unwrap();
    assert_eq!(text.lines().count(), 1_000);
    assert!(text.contains("batch line 999"), "{text}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证可复用批次提交后清空并保留容量.
///
/// 参数：无；断言：两次提交都成功，第二次未重新分配记录槽位，文件行数正确.
#[test]
fn reusable_record_batch_can_be_submitted_twice() {
    let dir = std::env::temp_dir().join(format!("flare-batch-reuse-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("batch-reuse")
                .target("batch-reuse")
                .rotation(FlareRotation::Never)
                .flush_each(false),
        )
        .build()
        .unwrap();
    let handle = guard.handle("batch-reuse").unwrap();
    let mut batch = handle.logger().batch();
    for index in 0..1_000 {
        batch.push(FlareRecord::new(
            FlareCoreLevel::Info,
            format!("first line {index}"),
        ));
    }
    assert_eq!(handle.logger().log_batch(&mut batch), 1_000);
    assert!(batch.is_empty());
    assert_eq!(batch.bytes(), 0);

    for index in 0..1_000 {
        batch.push(FlareRecord::new(
            FlareCoreLevel::Info,
            format!("second line {index}"),
        ));
    }
    assert_eq!(handle.logger().log_batch(&mut batch), 1_000);
    assert!(handle.flush(Some(Duration::from_secs(10))));

    let text = std::fs::read_to_string(dir.join("batch-reuse/batch-reuse.log")).unwrap();
    assert_eq!(text.lines().count(), 2_000);
    assert!(text.contains("first line 999"), "{text}");
    assert!(text.contains("second line 999"), "{text}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证批量 API 在写文件前转义控制字符.
///
/// 参数：无.
/// 断言：两条记录仍只产生两个物理行，裸换行/ESC 不落盘，转义后的文本可见.
/// 批量 API 接收最终文本、不经过 `TextFormat`；sink 必须做最后一道物理行注入防线.
#[test]
fn batch_api_escapes_control_characters() {
    let dir = std::env::temp_dir().join(format!("flare-batch-escape-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("escape")
                .target("escape")
                .rotation(FlareRotation::Never)
                .flush_each(false),
        )
        .build()
        .unwrap();
    let handle = guard.handle("escape").unwrap();
    let records = [
        FlareRecord::new(FlareCoreLevel::Info, "BATCH_A\nBATCH_B"),
        FlareRecord::new(FlareCoreLevel::Info, "BATCH_C\u{1b}[31mRED"),
    ];
    assert_eq!(handle.logger().log_many(records), 2);
    assert!(handle.flush(Some(Duration::from_secs(10))));

    let bytes = std::fs::read(dir.join("escape/escape.log")).unwrap();
    assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 2);
    assert!(!bytes.contains(&0x1b), "raw ESC must not reach the file");
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("BATCH_A\\nBATCH_B"), "{text}");
    assert!(text.contains("BATCH_C\\u{1b}[31mRED"), "{text}");
    assert_eq!(text.lines().count(), 2);

    let _ = std::fs::remove_dir_all(&dir);
}

/// 验证预格式化多行块一次入队、按行落盘.
///
/// 参数：无.
/// 断言：返回行数等于逻辑行数；文件恰好同样多行；末尾换行不产生额外空行；控制字符被转义.
/// 原因：块路径必须保持与逐条记录相同的物理行防线，同时把多条记录的队列开销摊到一次调用.
#[test]
fn preformatted_block_writes_lines_with_one_enqueue() {
    let dir = std::env::temp_dir().join(format!("flare-block-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("block")
                .target("block")
                .rotation(FlareRotation::Never)
                .flush_each(false),
        )
        .build()
        .unwrap();
    let handle = guard.handle("block").unwrap();
    assert_eq!(
        handle
            .logger()
            .log_preformatted_block(FlareCoreLevel::Info, ""),
        0
    );
    let block = "first line\nsecond\tline\nthird\u{1b}[31mred\n";
    assert_eq!(
        handle
            .logger()
            .log_preformatted_block(FlareCoreLevel::Info, block),
        3
    );
    assert!(handle.flush(Some(Duration::from_secs(10))));

    let bytes = std::fs::read(dir.join("block/block.log")).unwrap();
    assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 3);
    assert!(!bytes.contains(&0x1b), "raw ESC must not reach the file");
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("first line"), "{text}");
    assert!(text.contains(r"third\u{1b}[31mred"), "{text}");

    let health = guard.health();
    let service = health
        .services
        .iter()
        .find(|service| service.name == "block")
        .expect("service snapshot");
    assert_eq!(service.snapshot.emitted.iter().sum::<u64>(), 3);
    assert_eq!(service.snapshot.dropped, 0);
    let _ = std::fs::remove_dir_all(&dir);
}
