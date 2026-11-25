//! 动态 service 名额协议的 loom 模型.
//!
//! 这是锁协议模型，不是生产 Router 的直接 loom 复刻；生产代码仍使用 std/crossbeam/arc-swap.
//! 目标：在 reserve -> build -> publish/cancel 的并发交错下，动态名额不会超卖。

use loom::sync::{Arc, Mutex, RwLock};
use loom::thread;

const LIMIT: usize = 1;

/// 与 Router 动态创建关键字段同构的最小状态.
#[derive(Default)]
struct Model {
    build_gate: RwLock<()>,
    publish: Mutex<()>,
    reserved: Mutex<usize>,
    dynamic: Mutex<usize>,
}

impl Model {
    /// 预留名额，成功后再发布；失败时释放名额.
    fn try_build(&self) -> bool {
        let _gate = self.build_gate.read().unwrap();
        let _publish = self.publish.lock().unwrap();
        let mut reserved = self.reserved.lock().unwrap();
        let dynamic = self.dynamic.lock().unwrap();
        if dynamic.saturating_add(*reserved) >= LIMIT {
            return false;
        }
        *reserved = reserved.saturating_add(1);
        drop((dynamic, reserved));
        drop(_publish);

        // 模拟构建耗时；其他线程可以尝试预留/发布.
        let _publish = self.publish.lock().unwrap();
        let mut reserved = self.reserved.lock().unwrap();
        *reserved = reserved.saturating_sub(1);
        drop(reserved);
        let mut dynamic = self.dynamic.lock().unwrap();
        *dynamic = dynamic.saturating_add(1);
        true
    }

    /// 关闭时等待所有构建结束.
    fn close(&self) {
        let _gate = self.build_gate.write().unwrap();
        let _publish = self.publish.lock().unwrap();
        assert!(*self.reserved.lock().unwrap() == 0);
    }
}

/// 两个并发构建在 limit=1 下最多成功一个，关闭时没有残留预留.
#[test]
fn reservation_protocol_never_oversells() {
    loom::model(|| {
        let model = Arc::new(Model::default());
        let a = Arc::clone(&model);
        let b = Arc::clone(&model);
        let first = thread::spawn(move || a.try_build());
        let second = thread::spawn(move || b.try_build());
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        assert!(first || second, "at least one build must succeed");
        assert!(*model.dynamic.lock().unwrap() <= LIMIT);
        model.close();
    });
}
