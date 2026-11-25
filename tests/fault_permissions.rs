//! Unix 权限故障注入：目录只读时 writer 报错但不退出/不死锁.

#[cfg(unix)]
mod unix {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::Duration;

    use tracing_subscriber::prelude::*;

    use tracing_flare::{FlareConfig, FlareRotation, Logger, ServiceConfig};

    /// 目录恢复权限，避免测试失败时残留不可删除目录.
    struct RestoreDir(PathBuf);

    impl Drop for RestoreDir {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
        }
    }

    /// 只读日志目录下写入失败会计入 write_errors/dropped，writer 仍可关停.
    #[test]
    fn read_only_log_directory_reports_write_failure() {
        let dir = std::env::temp_dir().join(format!("flare-readonly-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let _restore = RestoreDir(dir.clone());

        let (layer, guard) = FlareConfig::new()
            .base_path(&dir)
            .service(
                ServiceConfig::new("readonly")
                    .target("readonly")
                    .rotation(FlareRotation::Never)
                    .queue_size(64)
                    .flush_each(false),
            )
            .build()
            .unwrap();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            let log = Logger::new("readonly");
            log.info("must-fail-open");
        });
        assert!(guard.flush(Some(Duration::from_secs(10))));
        let health = guard.health();
        let write_errors: u64 = health
            .services
            .iter()
            .map(|service| service.snapshot.write_errors)
            .sum();
        let dropped: u64 = health
            .services
            .iter()
            .map(|service| service.snapshot.dropped)
            .sum();
        assert!(
            write_errors > 0 || dropped > 0,
            "read-only directory must be observable: {health:?}"
        );
        assert!(guard.shutdown(Some(Duration::from_secs(10))));
        drop(_restore);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
