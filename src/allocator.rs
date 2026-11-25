//! 可选的进程级分配器.
//!
//! 设计原则：**日志库不得静默替换宿主进程的 allocator**.
//! 宿主可能已经使用jemalloc/snmalloc/mimalloc，或出于安全/统计目的安装了自定义分配器；如果库在链接
//! 阶段强行接管，会改变整个进程的内存行为，属于不可接受的隐式副作用.
//!
//! 因此这里只在用户显式启用 `mimalloc` feature 时注册全局分配器：
//! - 需要高并发分配性能的应用可以开启；
//! - 已经自行安装分配器的应用保持默认 feature，不受影响；
//! - 该 feature 只影响本 crate 链接进的最终二进制，不改变库的公共 API.
//!
//! 参数/接口说明：`GLOBAL_ALLOCATOR` 是 `#[global_allocator]` 静态变量，Rust 运行时会把它作为
//! `GlobalAlloc` 实现使用；`MiMalloc` 是无状态的零大小类型，静态实例只是一个符号，不占运行时内存.

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
