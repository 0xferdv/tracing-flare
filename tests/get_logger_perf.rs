//! `get_logger` 缓存命中微基准（默认忽略）.

use std::hint::black_box;
use std::time::{Duration, Instant};

use tracing_flare::{FlareConfig, Logger};

/// 测量生产预设下 `get_logger` 的缓存命中开销.
///
/// 该测试只验证热路径数量级，不作为 CI 硬门禁；运行方式见文件头.
#[test]
#[ignore = "microbenchmark; run explicitly in release mode"]
fn get_logger_cache_hit_is_cheap() {
    let dir = std::env::temp_dir().join(format!("flare-get-logger-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::production(&dir).build().unwrap();
    guard.register_default_router();
    let _first = Logger::get_logger("hit");
    let iterations = 1_000_000usize;
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(Logger::get_logger("hit"));
    }
    let elapsed = started.elapsed();
    let ns = elapsed.as_secs_f64() * 1_000_000_000.0 / iterations as f64;
    eprintln!("get_logger hit: {iterations} calls in {elapsed:?}, {ns:.1} ns/call");
    // Release 实测约 60–70ns/call；1s 已是约 15× 余量，足够挡住数量级退化，
    // 又不会把共享开发机的正常抖动当失败.
    assert!(
        elapsed < Duration::from_secs(1),
        "get_logger hit regressed: {elapsed:?} for {iterations} calls"
    );
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}
