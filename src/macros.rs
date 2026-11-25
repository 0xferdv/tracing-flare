//! `flare_*` 便捷宏.
//!
//! 设计原理：
//! - 语法完全沿用 `tracing` 原生宏（`target:`、字段、`{}` 格式化），减少学习与迁移成本；
//! - 不在宏里做级别过滤/格式化，全部交给 `FlareLayer`，保证与直接使用 tracing 的事件
//!   走同一条路径、同一套路由与 sink 语义；
//! - `flare_critical!` 是唯一的扩展：tracing 没有 CRITICAL，因此用 ERROR 事件 +
//!   `flare_level = "CRITICAL"` 字段表达，layer 在 `on_event` 中恢复为 CRITICAL.
//!
//! 参数约定：`$($arg:tt)*` 原样转发给对应的 tracing 宏，因此支持 tracing 的全部事件语法（`target:`
//! 、字段、`message`、`parent:` 等）；`flare_critical!` 额外支持可选`target: $target:expr` 分支，未
//! 提供 target 时使用默认 target.

/// 发出 TRACE 事件.
///
/// 参数：`$($arg:tt)*` 与 `tracing::trace!` 完全一致；展开为 `$crate::__tracing::trace!`，使用crate
/// 内部 re-export，避免调用方必须自己依赖 tracing.
#[macro_export]
macro_rules! flare_trace {
    ($($arg:tt)*) => { $crate::__tracing::trace!($($arg)*) };
}

/// 发出 DEBUG 事件.
///
/// 参数：`$($arg:tt)*` 与 `tracing::debug!` 一致；TRACE/DEBUG 在引擎内部共用 DEBUG 过滤槽.
#[macro_export]
macro_rules! flare_debug {
    ($($arg:tt)*) => { $crate::__tracing::debug!($($arg)*) };
}

/// 发出 INFO 事件.
///
/// 参数：`$($arg:tt)*` 与 `tracing::info!` 一致.
#[macro_export]
macro_rules! flare_info {
    ($($arg:tt)*) => { $crate::__tracing::info!($($arg)*) };
}

/// 发出 WARN 事件.
///
/// 参数：`$($arg:tt)*` 与 `tracing::warn!` 一致；渲染级别名为 `WARNING`.
#[macro_export]
macro_rules! flare_warn {
    ($($arg:tt)*) => { $crate::__tracing::warn!($($arg)*) };
}

/// 发出 ERROR 事件.
///
/// 参数：`$($arg:tt)*` 与 `tracing::error!` 一致；ERROR 及 CRITICAL 默认进入错误文件 sink.
#[macro_export]
macro_rules! flare_error {
    ($($arg:tt)*) => { $crate::__tracing::error!($($arg)*) };
}

/// 发出一个 Flare 按 CRITICAL 格式化和过滤的 ERROR 事件.
///
/// 设计原理：tracing 的五级模型没有 CRITICAL；为了同时兼容 tracing 生态与 Flare 的六级表面，这里仍
/// 发出 ERROR（保证其他 tracing layer 能识别严重级别），并附带`flare_level = "CRITICAL"` 供
/// `FlareLayer` 还原.
/// 不要用普通 `flare_error!` + 字段伪造 CRITICAL，否则服务级别阈值与错误文件分流会按 ERROR 处理.
///
/// 参数：
/// - `target:` 可选；提供时作为 tracing target 路由，未提供时使用默认 target；
/// - `$($arg:tt)*` 与 `tracing::error!` 一致（字段、消息、格式化参数）.
#[macro_export]
macro_rules! flare_critical {
    (target: $target:expr, $($arg:tt)*) => {
        $crate::__tracing::error!(target: $target, flare_level = "CRITICAL", $($arg)*)
    };
    ($($arg:tt)*) => {
        $crate::__tracing::error!(flare_level = "CRITICAL", $($arg)*)
    };
}
