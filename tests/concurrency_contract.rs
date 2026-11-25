//! 公共并发契约：关键句柄必须可跨线程共享.

use tracing_flare::{
    FlareGuard, FlareHandle, FlareLayer, FlareLogger, FlareRecord, FlareRecordBatch, FlareSinkStat,
    FlareSnapshot, Logger,
};

/// 编译期断言 `T: Send + Sync`.
fn assert_send_sync<T: Send + Sync>() {}

/// 公共句柄和记录类型满足跨线程共享要求.
#[test]
fn public_types_are_send_sync() {
    assert_send_sync::<Logger>();
    assert_send_sync::<FlareLogger>();
    assert_send_sync::<FlareGuard>();
    assert_send_sync::<FlareHandle>();
    assert_send_sync::<FlareLayer>();
    assert_send_sync::<FlareRecord>();
    assert_send_sync::<FlareRecordBatch>();
    assert_send_sync::<FlareSnapshot>();
    assert_send_sync::<FlareSinkStat>();
}
