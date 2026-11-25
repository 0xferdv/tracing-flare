# tracing-flare

tracing-flare 是一个纯 Rust 日志库，以 `tracing` 事件为输入，以 `tracing-subscriber`
Layer 组合，内置有界队列、单 writer、文件 sink、轮转和压缩。它不依赖 Python、PyO3 或
`tracing-appender`。

当前版本为 `0.1.0`。公共枚举和结构体标记为 `#[non_exhaustive]`，0.x 阶段可以新增
字段或变体，外部 `match` 需要保留 `_` 分支。

- crates.io：`tracing-flare = "0.1"`
- 文档：https://docs.rs/tracing-flare
- 最低支持 Rust 版本（MSRV）：1.82
- 仓库开发工具链：1.100.0
- 许可证：MIT

## 特性

- tracing 原生：实现 `Layer`，可以与 `EnvFilter`、`fmt`、OpenTelemetry 等 layer 组合。
- 非阻塞生产路径：事件线程只做过滤、格式化和有界入队，文件 I/O 由 writer 线程完成。
- 资源有界：队列同时限制记录数和估算字节数，单批、单条记录、压缩并发和分片数量都有上限。
- 可观测：健康快照提供入队、写出、丢弃、队列占用、轮转、压缩和 sink 错误计数。
- 安全默认值：限制路径字符、Unix 下使用 0700/0600、默认不跟随日志符号链接、转义控制字符。
- 可选的异步友好设计：event 路径不打开文件、不等待 writer；显式 flush 和 shutdown 提供屏障。

## 安装

```toml
[dependencies]
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["registry", "std"] }
tracing-flare = "0.1"
```

如果在本地开发，也可以使用 path 依赖：

```toml
tracing-flare = { path = "../tracing-flare" }
```

## 快速开始

### 使用生产预设

`Logger::init_production` 安装全局 subscriber，并允许 `Logger::get_logger(name)`
按名称创建和复用 logger：

```rust
use tracing_flare::Logger;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = Logger::init_production("logs")?;

    let log = Logger::get_logger("billing");
    log.info("order created");
    log.warn("inventory is low");

    Ok(())
}
```

guard 必须存活到进程退出。guard 被 drop 时会 flush 并关闭所有 writer。第一次
`get_logger("billing")` 会创建 `logs/billing/billing.log` 和对应的错误日志文件；
后续同名调用返回同一个缓存对象。

### 配置单个 service

```rust
use tracing_flare::{flare_info, FlareConfig, FlareLevel, FlareRotation, ServiceConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = FlareConfig::new()
        .base_path("logs")
        .rotation(FlareRotation::daily(7))
        .service(
            ServiceConfig::new("billing")
                .target("billing")
                .level(FlareLevel::Info)
                .console(true),
        )
        .install()?;

    flare_info!(target: "billing", order = 42, "order created");
    Ok(())
}
```

`install()` 会设置进程级 subscriber。这个示例会写入 `logs/billing/billing.log`，
错误级别写入 `logs/billing/billing_error.log`，同时输出到 stderr。

### 嵌入现有 subscriber

```rust
use tracing_subscriber::prelude::*;
use tracing_flare::{flare_info, FlareConfig, ServiceConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (layer, guard) = FlareConfig::new()
        .base_path("logs")
        .service(ServiceConfig::new("app").target("app"))
        .build()?;

    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        flare_info!(target: "app", "service started");
    });

    guard.flush(Some(std::time::Duration::from_secs(5)));
    Ok(())
}
```

`build()` 不安装全局 subscriber，适合库、测试、CLI 或已有 subscriber 的应用。

## 运行模型

```text
业务线程 / async task
  │ tracing::event! / flare_* / Logger::info
  ▼
FlareLayer
  │ 路由、级别过滤、span 与字段收集、格式化
  ▼
有界 channel
  │ 记录数和估算字节预算
  ▼
每个 service 一个 writer 线程
  │ 批量取出，最多 512 条或 4 MiB
  ├── 普通文件 sink（低于 ERROR）
  ├── 错误文件 sink（ERROR 及以上）
  ├── stderr sink（可选）
  └── 自定义 sink
  ▼
轮转 → 归档 → 可选压缩 → 按 keep 清理
```

生产路径的关键约束：

- 事件线程不做文件系统操作，也不获取 sink 锁。
- 每个 service 内保持 FIFO；开启分片后，只保证单个 shard 内有序。
- `flush` 是队列屏障，writer 处理完之前的记录后才返回。
- 关停顺序为：拒绝新记录、等待在途生产者、排空队列、flush、close、停止 writer。

## 路由

每个 service 有两个用于路由的名称：

| 字段 | 作用 |
|---|---|
| `name` | 日志目录、文件名、健康信息和动态 logger 名 |
| `target` | tracing target 前缀，也参与 `Logger::new(name)` 的名称匹配 |

路由顺序：

1. target 精确匹配；
2. 按 `.`、`::`、`/` 边界前缀匹配；
3. fallback service；
4. `UnknownTarget::Drop`：丢弃并计入 `no_route`；
5. `UnknownTarget::Create`：只允许通过显式 API 创建，事件路径不会做文件 I/O。

service 名称优先于其他 service 的 target 别名，配置顺序不会改变已注册名称的路由结果。

默认目录布局为：

```text
logs/
  billing/
    billing.log
    billing_error.log
  inventory/
    inventory.log
    inventory_error.log
```

使用 `name_as_folder(false)` 和 `folder("services")` 可以把文件放到
`logs/services/billing.log`。

## Logger API

`Logger` 是同步和异步的按名称入口，事件仍然经过 tracing、Layer 和 writer：

```rust
use tracing_flare::{FlareLevel, Logger};

let log = Logger::get_logger("billing");

log.trace("trace");
log.debug("debug");
log.info("info");
log.warn("warn");
log.error("error");
log.critical("critical");
log.fatal("fatal");
log.log(FlareLevel::Info, "explicit level");

// 异步方法在 future 首次 poll 时发出事件，不阻塞 executor。
// log.ainfo("async info").await;
```

`Logger::get_logger` 和 `Logger::get_handler` 使用进程级注册表，同一个名称返回同一个
对象。`Logger::new(name)` 不进入注册表，适合明确知道 service 已存在的场景。
注册表最多缓存 65,536 个名称，超出后返回未缓存的 facade，避免名称增长造成无界内存占用。

默认文本格式：

```text
[%(asctime)s-%(msecs)03d] [%(process)d:%(thread)d] [%(module_chain)s::%(lineno)d::%(levelname)s] > %(message)s
```

`Logger` 使用 `#[track_caller]` 获取业务调用点的文件与行号。`module_chain` 从源文件路径
推导；需要与 `module_path!` 完全一致时，使用 `flare_*` 或原生 `tracing` 宏。

### 动态服务

```rust
use tracing_subscriber::prelude::*;
use tracing_flare::{FlareConfig, UnknownTarget};

let (layer, guard) = FlareConfig::new()
    .base_path("logs")
    .unknown_target(UnknownTarget::Create)
    .build()?;

let subscriber = tracing_subscriber::registry().with(layer);
tracing::subscriber::with_default(subscriber, || {
    let log = guard.logger("tenant-42").unwrap();
    log.info("tenant created");
});

guard.flush(None);
```

`guard.logger(name)` 会同步打开文件并启动 writer，只应在启动阶段或低频管理路径调用。
`UnknownTarget::Create` 不会让事件路径自动创建 service。动态 service 默认上限为
1,024 个，可通过 `max_dynamic_services` 调整。

## 配置

`ServiceConfig` 的主要字段：

| 字段 | 默认值 | 说明 |
|---|---|---|
| `level` | `Info` | service 级别阈值 |
| `rotation` | `Never` | 大小、时间或组合轮转 |
| `compression` | `None` | gzip、zstd 或 lz4；需要 rotation |
| `files` / `console` | `true` / `false` | 文件与 stderr 输出 |
| `queue_size` | 65,536 | 队列记录数预算 |
| `max_queued_bytes` | `queue_size × 256` | 队列字节预算 |
| `flush_each` | `true` | 每个 writer 批次后 flush 到 OS |
| `file_buffer_size` | 64 KiB | 文件 BufWriter 容量 |
| `sync_on_flush` | `false` | 显式 flush/close 时执行 fsync |
| `sync_each_batch` | `false` | 每批 flush 后执行 fsync |
| `overflow` | `Drop` | 队列满时的处理策略 |
| `rate_limit` | `None` | 每秒入队上限 |
| `shards` | `1` | writer、队列和文件的并行分片数 |
| `instance_lock` | `false` | 单实例锁文件 |
| `follow_symlinks` | `false` | 是否允许日志路径跟随符号链接 |

`LoggerConfig` 提供偏生产的默认值：262,144 条队列、256 KiB 文件缓冲、128 MiB × 10
轮转，启用 `compression-zstd` 时使用 zstd。各字段可以在构建前覆盖：

```rust
use tracing_flare::{FlareConfig, FlareLevel, FlareRotation, LoggerConfig};

let _guard = FlareConfig::new()
    .base_path("logs")
    .service(
        LoggerConfig::new("billing")
            .level(FlareLevel::Debug)
            .queue_size(512 * 1024)
            .rotation(FlareRotation::size(256 * 1024 * 1024, 10)),
    )
    .install()?;
```

需要低延迟可见性时使用 `ServiceConfig::low_latency()`：每批 flush，8 KiB 缓冲。
需要持久化保证时使用 `durable()`：每批 flush 后执行 fsync，代价是吞吐下降。
`flush_each` 只把数据交给操作系统，不等同于 fsync。

## 轮转与压缩

```rust
use tracing_flare::{FlareCompression, FlareRotation, ServiceConfig};

let daily = ServiceConfig::new("daily").rotation(FlareRotation::daily(7));
let hourly = ServiceConfig::new("hourly").rotation(FlareRotation::hourly(24));
let sized = ServiceConfig::new("sized").rotation(FlareRotation::size(64 * 1024 * 1024, 10));
let zstd = ServiceConfig::new("zstd")
    .rotation(FlareRotation::size(128 * 1024 * 1024, 10))
    .compression(FlareCompression::Zstd);
```

时间轮转支持 `S`、`M`、`H`、`D`、`MIDNIGHT` 和 `W0..W6`。`MIDNIGHT` 和按周轮转的
interval 必须为 1。时间规则使用本地时间，除非显式启用 UTC。

大小轮转从 `.1` 开始编号，达到 keep 后删除最旧归档。组合轮转在时间或大小任一条件
满足时切换。

压缩在后台线程执行，每个 sink 和整个进程都有并发上限。达到上限时保留明文归档，不会
阻塞 writer。压缩临时文件和最终归档在 Unix 上使用 0600；`flush` 和 `close` 会等待在途
压缩结束。

压缩 codec 由 feature 控制：

- `compression-gzip`
- `compression-zstd`
- `compression-lz4`
- `compression`（同时启用三种）

选择未启用的 codec 会在配置校验阶段返回错误。

## JSON Lines

启用 `json` feature 后，每条日志输出为一个 JSON 对象：

```toml
tracing-flare = { version = "0.1", features = ["json"] }
```

```rust
use tracing_flare::{FlareConfig, ServiceConfig};

let (layer, guard) = FlareConfig::new()
    .base_path("logs")
    .json_lines(true)
    .service(ServiceConfig::new("billing").target("billing"))
    .build()?;
```

字段包括 `schema`、`time`、`level`、`service`、`target`、`message`、`pid`、`thread`，
以及可选的 `fields`、`file`、`line`、`module_chain` 和 `backtrace`。消息中的换行由
JSON 转义，不会产生多行日志。

批量 API 接收最终文本，不会为调用方执行 JSON 序列化。JSON Lines 服务要求批量记录
显式标记为 JSON：

```rust
use tracing_flare::{FlareCoreLevel, FlareRecord};

let core = guard.handle("billing").unwrap().logger();
let record = FlareRecord::new(FlareCoreLevel::Info, r#"{"event":"order_created"}"#).with_json();
core.log_many([record]);
```

未标记的批量文本会计入 `drop_reasons.format` 并被拒绝。

## 生命周期与关停

```rust
use std::time::Duration;

// 等待所有 service 处理完已入队记录。
guard.flush(Some(Duration::from_secs(10)));

// 幂等关停，返回 flush/close 的真实结果。
guard.shutdown(Some(Duration::from_secs(10)));

// 需要区分超时和 sink 错误时使用结构化报告。
let report = guard.shutdown_report(Some(Duration::from_secs(10)));
```

单个 service 也可以通过 `guard.handle(name)` 获取 `FlareHandle`，执行
`flush`、`reopen`、`close` 和健康查询。外部 logrotate 后调用 `reopen`，库本身不会
安装信号处理器。

Unix fork 子进程在继续使用日志前必须调用 `guard.after_fork_child()`。该调用会丢弃继承
的 writer 缓冲和 sink 句柄，并且不会释放父进程的实例锁。子进程应重新构建配置。

## 安全

- 路径组件拒绝 `..`、路径分隔符、NUL 和控制字符。Windows 额外拒绝 `:`、尾随点和空格、
  保留设备名。
- Unix 新建日志目录使用 0700，日志、锁文件和压缩临时文件使用 0600。
- 活动日志默认使用 `O_NOFOLLOW`，确实需要符号链接时显式设置 `follow_symlinks(true)`。
- 压缩文件使用 `O_NOFOLLOW`；Unix 通过硬链接发布归档，避免覆盖已有文件。
- 文本输出会转义 CR、LF、ESC 以及其他 C0/C1 控制字符，防止日志行注入。
- 单条转义结果上限为 32 MiB，超过时截断并附加 `...[truncated]`。
- 静态和动态 service 共用路径占用表，拒绝两个 writer 写同一个文件。

## 性能

以下数据来自 Windows 10、8 逻辑核、release 构建、未启用 mimalloc。每轮写入
1,000,000 条记录，批末执行一次 flush。共享开发机的波动约为 ±20%。

| 场景 | 吞吐 |
|---|---:|
| 单生产者，默认文本格式 | 1.2–1.7M 条/s |
| 8 生产者，默认文本格式 | 3.6–4.3M 条/s |
| 单生产者，`%(message)s` | 1.0–1.8M 条/s |
| 批量 `log_many`，预格式化记录 | 12–17M 条/s |
| `log_preformatted_block`，10k 行/块 | 8.5–11.4M 行/s |
| 已绑定 service 的过滤后 DEBUG/TRACE | 约 25M 条/s |

逐条 `Logger::info` 的成本主要在 tracing 分发、字段收集、格式化和单条入队。需要更高
吞吐时应使用批量 API：

```rust
use tracing_flare::{FlareCoreLevel, FlareRecord};

let log = guard.handle("billing").unwrap().logger();
log.log_many(
    (0..10_000).map(|index| FlareRecord::new(FlareCoreLevel::Info, format!("line {index}"))),
);
```

已经按行拼好的文本可以使用 `log_preformatted_block`：

```rust
let block = lines.join("\n");
let accepted = log.log_preformatted_block(FlareCoreLevel::Info, block);
```

单块上限为 8 MiB。块内每一行仍会执行控制字符转义；JSON Lines 模式拒绝该入口。

默认每个 service 只有一个 writer。需要并行写多个文件时可以设置 `shards(n)`，但不同
shard 之间不保证顺序，归档数量也会按 shard 成倍增加。

单生产者 100M/s 不在当前架构范围内。100M/s 意味着每条约 10ns 的预算，低于一次记录
格式化、入队和写盘的成本；多核聚合还需要多 writer、内存 sink 或更高的磁盘带宽。

## 平台与限制

- tracing 只有五个级别。TRACE 在引擎内部映射为 DEBUG，但显示仍为 TRACE。
- CRITICAL 是 tracing ERROR 加 `flare_level = "CRITICAL"` 的扩展。
- `FlareOverflow::Block` 只影响直接调用 `FlareLogger` 的路径，tracing 事件路径始终非阻塞。
- `module_chain` 从文件路径推导，极端目录布局下可能与 `module_path!` 不一致。
- Windows reparse point 防护目前仍以显式配置和路径校验为主。
- `appender-lite` 是独立兼容后端，不提供轮转、压缩、实例锁和 fork 语义。
- 仓库提供的最低支持版本为 1.82，开发和基准使用 Rust 1.100。

## 开发

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features -- --test-threads=1
cargo test -- --test-threads=1
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

性能基准：

```bash
cargo bench --features compression-gzip --bench flare -- --noplot
python bench/gate.py --threshold 0.10
python bench/perf_report.py --runs 3
```

基准结果依赖机器与磁盘，门禁只应在同一台参考机上比较。刷新基线前需要确认性能变化
符合预期。

## 许可证

MIT，见 `LICENSE`。
