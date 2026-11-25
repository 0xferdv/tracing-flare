# tracing-flare 性能基线与回归门禁（T0.3）

## 基准内容

`benches/flare.rs` 使用 criterion 跑公开路径（tracing 事件 → FlareLayer → 有界队列 →
writer → 文件 sink），每轮 10,000 条记录，批末 `flush`：

| ID | 场景 |
|---|---|
| `e2e/default` | 默认格式，无轮转 |
| `e2e/message_only` | `%(message)s` |
| `e2e/module_chain` | `%(module_chain)s %(message)s` |
| `e2e/size_rotation` | 128 KiB 大小轮转，保留 2 份 |
| `e2e/rotation_compression` | 128 KiB 轮转 + gzip 压缩 |
| `e2e/batch_preformatted` | `log_many` + 预格式化记录 + 文件 sink（批量路径） |
| `e2e/batch_reused` | `Logger::batch()` 复用 `RecordBatch` + 文件 sink |

压缩场景会显著慢于其它场景（大量小归档），这是预期行为；它用于守住压缩路径的回归，
不要把它当作整体吞吐指标。

## 运行与门禁

```bash
# 串行采样 throughput.rs（默认 3 次，输出中位数/P95）
python bench/perf_report.py

# 延迟分布（p50/p95/p99/p999 + RSS）
cargo test --release --features mimalloc --test latency -- --ignored --nocapture --test-threads=1

# 短/长 soak；FLARE_SOAK_SECS 默认 10，可设 3600/86400
FLARE_SOAK_SECS=3600 cargo test --release --features mimalloc --test soak -- --ignored --nocapture --test-threads=1

# 必须在包根目录执行；脚本按自身位置解析 baseline/policy 与 target/criterion.
# compression-gzip 用于生成 baseline 中的 rotation_compression 结果，否则门禁会报缺失.
cargo bench --features compression-gzip --bench flare -- --noplot --warm-up-time 2 --measurement-time 5

# 10% 回归门禁（默认阈值 0.10）
python bench/gate.py --threshold 0.10

# 只在点估计超过阈值时失败（不做置信区间过滤）
python bench/gate.py --threshold 0.10 --point-only

# 确认是预期性能变化后，刷新基线（必须包含全部已定义基准）
python bench/gate.py --update
```

`gate.py` 读取 `target/criterion/**/new/estimates.json` 的均值与 95% 置信区间，
与 `bench/baseline.json` 比较。默认只在“点估计超过阈值 **且** 新旧置信区间不重叠”时失败，
避免把开发机抖动当成回归；`--point-only` 用于专用 runner 上的严格模式。
基线机器指纹（OS/架构/CPU/核数）不匹配时默认失败；确需跨机器比较时显式传
`--allow-machine-mismatch`。脚本还会从 `benches/flare.rs` 提取全部 `e2e/*` 基准，
发现“已定义但未入基线”时失败，避免死配置和漏检。
任一基线基准没跑出来，同样以非 0 退出。

磁盘相关基准（`size_rotation`、`rotation_compression`、`batch_preformatted`）在共享开发机上方差远大于 10%，
因此 `bench/policy.json` 把它们标为 advisory：仍会运行和展示，但不阻断门禁。专用 runner
上可以把 `advisory` 清空，恢复全量阻断。

## 基线机器

当前入库基线来自：

- Windows 10，AMD64，8 逻辑核（Intel64 Family 6 Model 158）
- release + criterion bench profile
- 基线生成时间：2026-11-24（含全部 7 个 `e2e/*` 基准，见 `baseline.json`）

绝对时间依赖机器、磁盘、后台负载与电源策略。**门禁只在同一台参考机上有效**；共享 CI
runner 的抖动会超过 10%，建议：

1. PR 上只跑基准并上传报告，不阻断合并；
2. 专用 runner/夜间任务跑 `gate.py --threshold 0.10` 阻断；
3. 更换参考机后，用 `--update` 重建基线并在 PR 中说明。

## 维护规则

- 只允许在“确认性能变化是预期”的 PR 中执行 `--update`，并在 PR 描述中贴出前后对比。
- 新增基准时，先加 benchmark，再刷新基线；否则门禁会提示新基准不在基线中。
- 不要为了过门禁放宽阈值；先定位回归，或证明是参考机负载问题后重跑。
