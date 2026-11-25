//! 端到端吞吐基准.
//!
//! 这些基准刻意走公开路径（tracing 事件 → FlareLayer → 有界队列 → writer → 文件 sink），
//! 而不是内部 helper 实现.
//! 门禁比较同一台机器上的相对变化；绝对数字依赖硬件.
//!
//! 设计：每轮 10,000 条，批末 flush；覆盖默认/纯消息/module_chain/大小轮转/gzip 轮转/预格式化批量六
//! 类.
//! 磁盘相关基准在共享开发机方差大，由 `bench/policy.json` 标为 advisory.

use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tracing_subscriber::prelude::*;

use tracing_flare::{
    FlareCompression, FlareConfig, FlareCoreLevel, FlareGuard, FlareHandle, FlareRecord,
    FlareRotation, Logger, ServiceConfig,
};

/// 每个 criterion 迭代写入的记录数.
const RECORDS_PER_ITER: usize = 10_000;
/// 批末 flush 的超时预算.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);
/// criterion 采样数.
///
/// 100 比 30 给出更紧的置信区间；对 10k 批次仍可在可接受时间内完成.
const SAMPLE_SIZE: usize = 100;

/// 一组基准资源：临时目录、guard、dispatcher、handle 与门面.
///
/// 字段：`_dispatch` 必须存活，否则默认 dispatcher 被恢复，所有基准事件会被静默丢弃.
struct BenchFixture {
    dir: PathBuf,
    guard: Option<FlareGuard>,
    _dispatch: tracing::dispatcher::DefaultGuard,
    handle: FlareHandle,
    logger: Logger,
}

impl BenchFixture {
    /// 创建不压缩 fixture.
    ///
    /// 参数：`label` 场景名；`format` 可选模板；`rotation` 轮转；返回值：fixture.
    fn new(label: &str, format: Option<&str>, rotation: FlareRotation) -> Self {
        Self::with_options(label, format, rotation, None)
    }

    /// 创建带压缩选项的 fixture.
    ///
    /// 参数：`label`；`format`；`rotation`；`compression`.
    /// 返回值：fixture，使用 1M 队列、`flush_each=false`、256 KiB 文件缓冲以贴近吞吐测试配置.
    fn with_options(
        label: &str,
        format: Option<&str>,
        rotation: FlareRotation,
        compression: Option<FlareCompression>,
    ) -> Self {
        let dir = std::env::temp_dir().join(format!("flare-bench-{}-{label}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create bench dir");

        let mut config = FlareConfig::new().base_path(&dir);
        if let Some(format) = format {
            config = config.format(format);
        }
        let mut service = ServiceConfig::new("bench")
            .target("bench")
            .rotation(rotation)
            .queue_size(1_000_000)
            .flush_each(false)
            .file_buffer_size(256 * 1024);
        if let Some(compression) = compression {
            service = service.compression(compression);
        }
        let (layer, guard) = config.service(service).build().expect("build bench config");
        let handle = guard.handle("bench").expect("bench service handle");
        let subscriber = tracing_subscriber::registry().with(layer);
        // 必须让 guard 保持存活：立即 drop 会恢复之前的 dispatcher，并静默丢弃所有基准事件.
        let dispatch = tracing::Dispatch::new(subscriber);
        let dispatch_guard = tracing::dispatcher::set_default(&dispatch);
        Self {
            dir,
            guard: Some(guard),
            _dispatch: dispatch_guard,
            handle,
            logger: Logger::new("bench"),
        }
    }

    /// 批末 flush，失败时打印健康快照并 panic.
    ///
    /// 参数：无；返回值：无.
    fn flush(&self) {
        if !self.handle.flush(Some(FLUSH_TIMEOUT)) {
            if let Some(guard) = self.guard.as_ref() {
                eprintln!("bench flush failed: {:#?}", guard.health());
            }
            panic!("flush must succeed during benchmark");
        }
    }

    /// 写一轮记录并 flush.
    ///
    /// 参数：`prefix` 是消息前缀；返回值：无.
    fn write_batch(&self, prefix: &str) {
        for index in 0..RECORDS_PER_ITER {
            self.logger.info(format_args!("{prefix} record {index}"));
        }
        self.flush();
    }
}

impl Drop for BenchFixture {
    /// 关闭 guard 并清理临时目录；参数 `self`.
    fn drop(&mut self) {
        drop(self.guard.take());
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// 通用端到端基准函数.
///
/// 参数：`c` criterion 上下文；`id` 基准名；`format` 可选模板；`rotation` 轮转.
/// 返回值：无.
/// 每组吞吐按 10,000 元素计.
fn bench_case(c: &mut Criterion, id: &str, format: Option<&str>, rotation: FlareRotation) {
    let fixture = BenchFixture::new(id, format, rotation);
    let mut group = c.benchmark_group("e2e");
    group.throughput(Throughput::Elements(RECORDS_PER_ITER as u64));
    group.sample_size(SAMPLE_SIZE);
    group.bench_function(id, |b| {
        b.iter(|| {
            fixture.write_batch(id);
            black_box(());
        });
    });
    group.finish();
}

/// 默认格式基准；参数 `c`，返回无.
fn default_format(c: &mut Criterion) {
    bench_case(c, "default", None, FlareRotation::Never);
}

/// 纯消息格式基准；参数 `c`，返回无.
fn message_only(c: &mut Criterion) {
    bench_case(c, "message_only", Some("%(message)s"), FlareRotation::Never);
}

/// module_chain 格式基准；参数 `c`，返回无.
fn module_chain(c: &mut Criterion) {
    bench_case(
        c,
        "module_chain",
        Some("%(module_chain)s %(message)s"),
        FlareRotation::Never,
    );
}

/// 128 KiB 大小轮转基准；参数 `c`，返回无.
fn size_rotation(c: &mut Criterion) {
    bench_case(c, "size_rotation", None, FlareRotation::size(128 * 1024, 2));
}

/// 轮转 + gzip 压缩基准；参数 `c`，返回无.
///
/// 只在启用 `compression-gzip` 时注册；默认 feature 不引入 flate2/rayon.
#[cfg(feature = "compression-gzip")]
fn rotation_compression(c: &mut Criterion) {
    let fixture = BenchFixture::with_options(
        "rotation_compression",
        None,
        FlareRotation::size(128 * 1024, 2),
        Some(FlareCompression::Gzip),
    );
    let mut group = c.benchmark_group("e2e");
    group.throughput(Throughput::Elements(RECORDS_PER_ITER as u64));
    // 压缩慢得多且噪声更大；增加采样只会让门禁运行数分钟，却不会明显改善信号.
    group.sample_size(10);
    group.bench_function("rotation_compression", |b| {
        b.iter(|| {
            fixture.write_batch("rotation-compression");
            black_box(());
        });
    });
    group.finish();
}

/// 预格式化批量路径基准.
///
/// 参数：`c` criterion 上下文；返回值：无.
/// 使用 `log_many` 一次提交 10,000 条预构造记录，绕过 tracing 与文本模板，测量高吞吐上限.
fn batch_preformatted(c: &mut Criterion) {
    let dir = std::env::temp_dir().join(format!("flare-bench-batch-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("batchbench")
                .target("batchbench")
                .rotation(FlareRotation::Never)
                .queue_size(1_000_000)
                .flush_each(false)
                .file_buffer_size(256 * 1024),
        )
        .build()
        .unwrap();
    let handle = guard.handle("batchbench").unwrap();
    let template: Vec<FlareRecord> = (0..RECORDS_PER_ITER)
        .map(|index| FlareRecord::new(FlareCoreLevel::Info, format!("batch {index}")))
        .collect();
    let mut group = c.benchmark_group("e2e");
    group.throughput(Throughput::Elements(RECORDS_PER_ITER as u64));
    group.sample_size(SAMPLE_SIZE);
    group.bench_function("batch_preformatted", |b| {
        b.iter(|| {
            let records = template.iter().cloned();
            assert_eq!(handle.logger().log_many(records), RECORDS_PER_ITER);
            assert!(handle.flush(Some(FLUSH_TIMEOUT)));
            black_box(());
        });
    });
    group.finish();
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 可复用 `RecordBatch` 基准.
///
/// 参数：`c` criterion 上下文；返回值：无.
/// 每轮先填充批次再提交；与 `log_many` 对比，验证批次复用没有引入额外分配/扫描.
fn batch_reused(c: &mut Criterion) {
    let dir = std::env::temp_dir().join(format!("flare-bench-batch-reuse-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .service(
            ServiceConfig::new("batchreuse")
                .target("batchreuse")
                .rotation(FlareRotation::Never)
                .queue_size(1_000_000)
                .flush_each(false)
                .file_buffer_size(256 * 1024),
        )
        .build()
        .unwrap();
    let handle = guard.handle("batchreuse").unwrap();
    let template: Vec<FlareRecord> = (0..RECORDS_PER_ITER)
        .map(|index| FlareRecord::new(FlareCoreLevel::Info, format!("batch {index}")))
        .collect();
    let mut batch = handle.logger().batch();
    let mut group = c.benchmark_group("e2e");
    group.throughput(Throughput::Elements(RECORDS_PER_ITER as u64));
    group.sample_size(SAMPLE_SIZE);
    group.bench_function("batch_reused", |b| {
        b.iter(|| {
            for record in &template {
                batch.push(record.clone());
            }
            assert_eq!(handle.logger().log_batch(&mut batch), RECORDS_PER_ITER);
            assert!(handle.flush(Some(FLUSH_TIMEOUT)));
            black_box(());
        });
    });
    group.finish();
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(
    benches,
    default_format,
    message_only,
    module_chain,
    size_rotation,
    batch_preformatted,
    batch_reused
);
#[cfg(feature = "compression-gzip")]
criterion_group!(compression_benches, rotation_compression);
#[cfg(feature = "compression-gzip")]
criterion_main!(benches, compression_benches);
#[cfg(not(feature = "compression-gzip"))]
criterion_main!(benches);
