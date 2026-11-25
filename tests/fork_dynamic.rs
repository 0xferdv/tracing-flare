//! fork 子进程不会继承动态 service 创建能力.

#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use tracing_flare::{FlareConfig, Logger, UnknownTarget};

/// 子进程执行 after_fork_child 后拒绝创建新 service，父进程继续可用.
#[cfg(unix)]
#[test]
#[ignore = "fork tests must run in a single-threaded process (Linux CI: --test-threads=1)"]
fn forked_child_cannot_create_dynamic_service() {
    let dir = std::env::temp_dir().join(format!("flare-fork-dynamic-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (_layer, guard) = FlareConfig::new()
        .base_path(&dir)
        .queue_size(64)
        .unknown_target(UnknownTarget::Create)
        .max_dynamic_services(8)
        .build()
        .unwrap();
    guard.register_default_router();
    let parent = Logger::get_logger("fork-parent");
    parent.info("parent-before");
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        guard.after_fork_child();
        let code = match guard.service("fork-child") {
            Ok(_) => 1,
            Err(_) => 0,
        };
        unsafe { libc::_exit(code) };
    }
    let mut status = 0;
    let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
    assert_eq!(waited, pid);
    assert!(libc::WIFEXITED(status));
    assert_eq!(libc::WEXITSTATUS(status), 0);
    parent.info("parent-after");
    assert!(guard.flush(Some(Duration::from_secs(10))));
    let text = std::fs::read_to_string(dir.join("fork-parent/fork-parent.log")).unwrap();
    assert!(text.contains("parent-before"), "{text}");
    assert!(text.contains("parent-after"), "{text}");
    assert!(!text.contains("fork-child"), "{text}");
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}
