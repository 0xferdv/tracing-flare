//! 日志文件的进程级单实例锁.
//!
//! Unix 用 `flock`，Windows 用 `LockFileEx`；进程退出时内核自动释放.
//! 同进程内通过全局路径注册表实现可重入租约：同一路径被多个 service 共享时，只有最后一个租约释放才
//! 真正解锁，避免关闭一个 service 就让另一个 writer 失去锁.
//!
//! fork 子进程只关闭继承的文件描述符、绝不 `LOCK_UN`：内核锁属于父进程的 open file description，子
//! 进程解锁会同时释放父进程的锁.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use crate::error::FlareError;

#[cfg(windows)]
const LOCK_OFFSET: u64 = 4096;
const OWNER_READ_ATTEMPTS: usize = 5;
const OWNER_READ_DELAY: std::time::Duration = std::time::Duration::from_millis(20);

/// 一个路径的进程内锁实例.
///
/// 字段：`path` 是规范化绝对路径；`file` 保存内核锁句柄；`forked` 表示已在子进程丢弃；`leases` 是进
/// 程内租约计数，最后一个租约释放时才真正解锁.
pub(crate) struct InstanceLock {
    path: PathBuf,
    file: Mutex<Option<File>>,
    forked: AtomicBool,
    leases: AtomicUsize,
}

static HELD: OnceLock<Mutex<HashMap<PathBuf, Arc<InstanceLock>>>> = OnceLock::new();

/// 返回全局路径注册表；参数无.
fn registry() -> &'static Mutex<HashMap<PathBuf, Arc<InstanceLock>>> {
    HELD.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 获取 mutex，poisoning 时取回内部值.
///
/// 参数：`mutex`；返回值：guard.
/// 日志库不应因为某个线程 panic 就让后续锁永久失败.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 获取路径的实例锁，或返回被其他进程占用的错误.
///
/// 参数：`path` 是锁文件路径.
/// 返回值：共享 `InstanceLock` 或 `FlareError::Lock`.
/// 同进程同路径返回同一句柄并增加租约；并发首次获取通过 `acquiring` 条件变量串行化，不会在持锁状态
/// 打开第二个描述符.
pub(crate) fn acquire_instance_lock(path: &Path) -> Result<Arc<InstanceLock>, FlareError> {
    let key = normalize(path);
    loop {
        {
            let held = lock(registry());
            if let Some(existing) = held.get(&key) {
                existing.leases.fetch_add(1, Ordering::AcqRel);
                return Ok(Arc::clone(existing));
            }
        }
        let (acquiring, ready) = acquiring_state();
        let mut in_progress = lock(acquiring);
        if in_progress.contains(&key) {
            // 另一个线程正在打开/锁定同一文件；等待时不持有全局注册表，也不执行 I/O.
            let _wait = ready
                .wait(in_progress)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            continue;
        }
        in_progress.insert(key.clone());
        drop(in_progress);

        let instance = Arc::new(InstanceLock {
            path: key.clone(),
            file: Mutex::new(None),
            forked: AtomicBool::new(false),
            leases: AtomicUsize::new(1),
        });
        let result = instance.acquire();
        if result.is_ok() {
            // 先发布再唤醒等待者：否则等待者可能醒来却看不到条目，从而为同一路径打开第二个描述符.
            let mut held = lock(registry());
            held.entry(key.clone())
                .or_insert_with(|| Arc::clone(&instance));
        }
        {
            let mut in_progress = lock(acquiring);
            in_progress.remove(&key);
            ready.notify_all();
        }
        result?;

        let held = lock(registry());
        return Ok(Arc::clone(held.get(&key).expect("lock published")));
    }
}

/// 返回“正在获取”集合与条件变量.
///
/// 参数：无；返回值：静态 `(Mutex<HashSet<PathBuf>>, Condvar)`.
/// 用于让并发首次获取同一路径的线程等待，而不是重复 open/加锁.
fn acquiring_state() -> &'static (Mutex<HashSet<PathBuf>>, Condvar) {
    static ACQUIRING: OnceLock<(Mutex<HashSet<PathBuf>>, Condvar)> = OnceLock::new();
    ACQUIRING.get_or_init(|| (Mutex::new(HashSet::new()), Condvar::new()))
}

/// 释放一个租约.
///
/// 参数：`path` 是锁文件路径；返回值：无.
/// 只有租约计数降到 0 才从注册表移除并真正解锁；这样多个 service 共享同一日志路径时，关闭一个不会解
/// 锁另一个仍在写的文件.
pub(crate) fn release_instance_lock(path: &Path) {
    let key = normalize(path);
    let mut held = lock(registry());
    let Some(instance) = held.get(&key).cloned() else {
        return;
    };
    let leases = instance.leases.load(Ordering::Acquire);
    if leases == 0 {
        // 防御性：旧调用方重复释放时，租约计数绝不能下溢.
        return;
    }
    if instance.leases.fetch_sub(1, Ordering::AcqRel) == 1 {
        // 先释放内核锁，再从注册表移除：若先 remove，另一个线程可在旧句柄仍持有文件锁时
        // 通过注册表检查并尝试打开第二个描述符，造成短暂的“同进程两个 writer”窗口.
        // 持有注册表锁期间释放句柄，新的 acquire 会等到移除完成后再创建实例.
        instance.release();
        held.remove(&key);
    }
}

/// fork 子进程：关闭继承的描述符但不解锁.
///
/// 参数：无；返回值：无.
/// 注册表锁可能被 fork 时的其他线程持有，因此用 `try_lock`；拿不到就直接返回，进程退出会关闭 fd.
/// 绝不对继承句柄调用 `LOCK_UN`，否则会释放父进程的锁.
pub(crate) fn close_all_after_fork() {
    // fork 时全局注册表互斥锁可能被其他线程持有；不能让子进程在继承锁上永久阻塞.
    // 进程退出仍会关闭 fd.
    let Ok(mut held) = registry().try_lock() else {
        return;
    };
    let instances: Vec<Arc<InstanceLock>> = held.drain().map(|(_, value)| value).collect();
    drop(held);
    for instance in instances {
        instance.close_after_fork();
    }
}

impl InstanceLock {
    /// 返回锁文件路径；参数无，主要用于诊断.
    #[allow(dead_code)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// 判断当前是否持有文件句柄.
    ///
    /// 参数：无；返回值：句柄是否存在；锁正被其他线程持有时保守返回 true.
    #[allow(dead_code)]
    pub(crate) fn held(&self) -> bool {
        match self.file.try_lock() {
            Ok(slot) => slot.is_some(),
            Err(_) => true,
        }
    }

    /// 打开锁文件并尝试内核加锁.
    ///
    /// 参数：无；返回值：成功持有为 `Ok(())`，被其他进程占用/IO 错误为 `FlareError::Lock`.
    /// 先创建私有目录、打开 0600 锁文件，再尝试非阻塞独占锁；成功后把 PID 写入文件仅供诊断，写失败
    /// 不影响锁.
    fn acquire(&self) -> Result<(), FlareError> {
        let mut slot = lock(&self.file);
        if slot.is_some() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                create_private_dir(parent).map_err(|error| FlareError::Lock(error.to_string()))?;
            }
        }
        let mut file = open_lock_file(&self.path)?;
        match try_lock(&file) {
            Ok(true) => {}
            Ok(false) => {
                let owner = read_owner(&self.path);
                return Err(FlareError::Lock(format!(
                    "log file is already locked by another process{}{}: {}",
                    if owner.is_empty() { "" } else { " (pid=" },
                    if owner.is_empty() {
                        String::new()
                    } else {
                        format!("{owner})")
                    },
                    self.path.display()
                )));
            }
            Err(error) => return Err(FlareError::Lock(error.to_string())),
        }
        // PID 文本是尽力而为；不能因此让加锁失败.
        let _ = file.set_len(0);
        let _ = file.seek(SeekFrom::Start(0));
        let _ = file.write_all(std::process::id().to_string().as_bytes());
        let _ = file.flush();
        let _ = file.sync_all();
        *slot = Some(file);
        Ok(())
    }

    /// 释放内核锁并关闭句柄.
    ///
    /// 参数：无；返回值：无.
    /// fork 子进程只关闭句柄，不 `LOCK_UN`.
    fn release(&self) {
        let mut slot = lock(&self.file);
        if let Some(file) = slot.take() {
            if !self.forked.load(Ordering::Acquire) {
                unlock(&file);
            }
        }
    }

    /// fork 子进程：标记并关闭继承句柄.
    ///
    /// 参数：无；返回值：无.
    /// 使用 `try_lock`，避免继承锁卡死；只 drop 文件，不调用 unlock.
    pub(crate) fn close_after_fork(&self) {
        self.forked.store(true, Ordering::Release);
        if let Ok(mut slot) = self.file.try_lock() {
            if let Some(file) = slot.take() {
                // 只关闭：子进程执行 LOCK_UN 会连带释放父进程的锁.
                drop(file);
            }
        }
    }
}

impl Drop for InstanceLock {
    /// 最后一个引用 drop 时释放锁（fork 子进程除外）.
    ///
    /// 参数：`self`；返回值：无.
    /// 使用 `get_mut` 避免额外加锁；forked 时只关闭句柄.
    fn drop(&mut self) {
        if let Some(file) = self.file.get_mut().ok().and_then(Option::take) {
            if !self.forked.load(Ordering::Acquire) {
                unlock(&file);
            }
        }
    }
}

/// 把路径规范化为绝对路径.
///
/// 参数：`path`；返回值：绝对路径；失败时回退原路径.
/// 同一文件的不同相对形式必须映射到同一 key，否则会打开两个描述符.
fn normalize(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

/// 创建私有目录.
///
/// 参数：`path` 是目录；返回值：IO 结果.
/// Unix 用 0700，其他平台用普通递归创建.
fn create_private_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(path)
    }
}

/// Unix 打开锁文件.
///
/// 参数：`path`；返回值：File 或锁错误.
/// 使用 0600、O_NOFOLLOW，创建后再 chmod，防止权限过宽或被预置符号链接利用.
#[cfg(unix)]
fn open_lock_file(path: &Path) -> Result<File, FlareError> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        // 锁文件名可预测；绝不跟随其他用户预置的符号链接.
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| FlareError::Lock(error.to_string()))?;
    let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
    Ok(file)
}

/// 非 Unix 打开锁文件.
///
/// 参数：`path`；返回值：File 或锁错误.
/// 读写创建、不截断.
#[cfg(not(unix))]
fn open_lock_file(path: &Path) -> Result<File, FlareError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|error| FlareError::Lock(error.to_string()))
}

/// 读取锁文件中记录的持有者 PID.
///
/// 参数：`path` 是锁文件；返回值：PID 文本或空串.
/// 最多重试 5 次，因为刚获取锁的进程可能还没写完；纯诊断用途，失败不影响锁语义.
fn read_owner(path: &Path) -> String {
    for attempt in 0..OWNER_READ_ATTEMPTS {
        if let Ok(mut file) = File::open(path) {
            let mut text = String::new();
            if file.read_to_string(&mut text).is_ok() {
                let text = text.trim().to_string();
                if !text.is_empty() {
                    return text;
                }
            }
        }
        if attempt + 1 < OWNER_READ_ATTEMPTS {
            std::thread::sleep(OWNER_READ_DELAY);
        }
    }
    String::new()
}

/// Unix 非阻塞独占锁.
///
/// 参数：`file` 是已打开锁文件；返回值：`Ok(true)` 成功，`Ok(false)` 被占用，`Err` 其他错误.
/// `flock(LOCK_EX|LOCK_NB)`；EAGAIN/EWOULDBLOCK/EACCES 视为占用.
#[cfg(unix)]
fn try_lock(file: &File) -> std::io::Result<bool> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: 调用期间 `file` 拥有有效的打开描述符.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK || code == libc::EACCES => {
            Ok(false)
        }
        _ => Err(error),
    }
}

/// Unix 解锁.
///
/// 参数：`file`；返回值：无.
/// `flock(LOCK_UN)`，错误被忽略.
#[cfg(unix)]
fn unlock(file: &File) {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `file` 拥有有效打开描述符；LOCK_UN 不访问内存.
    unsafe {
        libc::flock(file.as_raw_fd(), libc::LOCK_UN);
    }
}

/// Windows 非阻塞独占锁.
///
/// 参数：`file` 是锁文件；返回值：`Ok(true)` 成功，`Ok(false)` 被占用，`Err` 其他错误.
/// 使用 `LockFileEx` 在固定 offset 锁 1 字节，失败立即返回；`ERROR_LOCK_VIOLATION` 视为占用.
#[cfg(windows)]
fn try_lock(file: &File) -> std::io::Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{GetLastError, ERROR_LOCK_VIOLATION, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

    // 直接构造 union 字段，而不是从零值读取.访问 union 字段需要 `unsafe`；初始化 union
    // 变体是安全的，并可跨 windows-sys 版本工作.
    let mut overlapped = OVERLAPPED {
        Internal: 0,
        InternalHigh: 0,
        Anonymous: OVERLAPPED_0 {
            Anonymous: OVERLAPPED_0_0 {
                Offset: LOCK_OFFSET as u32,
                OffsetHigh: 0,
            },
        },
        hEvent: std::ptr::null_mut(),
    };
    let handle = file.as_raw_handle() as HANDLE;
    // SAFETY: `handle` 是有效文件句柄；`overlapped` 是有效的零初始化 OVERLAPPED 结构，
    // 且调用是同步/非阻塞的.
    let ok = unsafe {
        LockFileEx(
            handle,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut overlapped,
        )
    };
    if ok != 0 {
        return Ok(true);
    }
    let code = unsafe { GetLastError() };
    if code == ERROR_LOCK_VIOLATION {
        Ok(false)
    } else {
        Err(std::io::Error::from_raw_os_error(code as i32))
    }
}

/// Windows 解锁.
///
/// 参数：`file` 是锁文件；返回值：无.
/// 使用与加锁相同的 offset/长度调用 `UnlockFileEx`.
#[cfg(windows)]
fn unlock(file: &File) {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::UnlockFileEx;
    use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

    // 保持与 `try_lock` 相同的字节范围；构造 union 变体可避免在所有支持的 windows-sys
    // 版本上访问 unsafe union 字段.
    let mut overlapped = OVERLAPPED {
        Internal: 0,
        InternalHigh: 0,
        Anonymous: OVERLAPPED_0 {
            Anonymous: OVERLAPPED_0_0 {
                Offset: LOCK_OFFSET as u32,
                OffsetHigh: 0,
            },
        },
        hEvent: std::ptr::null_mut(),
    };
    // SAFETY: `handle` 是有效文件句柄，`overlapped` 描述与 `try_lock` 相同的字节范围；
    // UnlockFileEx 不会保留该指针.
    unsafe {
        UnlockFileEx(file.as_raw_handle() as HANDLE, 0, 1, 0, &mut overlapped);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static TEST_GUARD: Mutex<()> = Mutex::new(());

    /// 创建隔离的测试锁文件路径.
    ///
    /// 参数：`tag` 是测试名；返回值：临时目录下的锁文件路径.
    /// 先清理旧目录，保证测试可重复.
    fn temp_path(tag: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("flare-lock-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.join("service.log.lock")
    }

    /// 验证同进程可重入与租约语义.
    ///
    /// 参数：无；断言：同路径返回同一 `Arc`，释放一个租约仍保持锁，全部释放后可重新获取.
    #[test]
    fn same_process_acquire_is_reentrant_and_release_reopens() {
        let _guard = TEST_GUARD.lock().unwrap();
        let path = temp_path("reentrant");
        let first = acquire_instance_lock(&path).unwrap();
        let second = acquire_instance_lock(&path).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(first.held());
        // 两个所有者持有两个租约；释放一个后文件必须仍保持锁定.
        release_instance_lock(&path);
        assert!(first.held());
        release_instance_lock(&path);
        assert!(!first.held());
        let third = acquire_instance_lock(&path).unwrap();
        assert!(!Arc::ptr_eq(&first, &third));
        release_instance_lock(&path);
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    /// 验证 Unix 锁文件权限为 0600.
    ///
    /// 参数：无；断言：创建后的锁文件 mode 为 0600.
    #[test]
    fn lock_file_is_private_on_unix() {
        #[cfg(unix)]
        {
            let _guard = TEST_GUARD.lock().unwrap();
            use std::os::unix::fs::PermissionsExt;
            let path = temp_path("mode");
            let _lock = acquire_instance_lock(&path).unwrap();
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
            release_instance_lock(&path);
            let _ = fs::remove_dir_all(path.parent().unwrap());
        }
    }
}

#[cfg(all(test, unix))]
mod fork_tests {
    use super::*;
    use std::sync::Mutex;

    static FORK_GUARD: Mutex<()> = Mutex::new(());

    /// 验证 fork 子进程不能重新获取父进程锁.
    ///
    /// 参数：无；断言：子进程 `close_all_after_fork` 后再次 acquire 失败，父进程锁不受影响.
    #[test]
    #[ignore = "fork tests must run in a single-threaded process (Linux CI: --test-threads=1)"]
    fn fork_child_cannot_reacquire_parent_lock() {
        let _guard = FORK_GUARD.lock().unwrap();
        let mut dir = std::env::temp_dir();
        dir.push(format!("flare-lock-fork-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("service.log.lock");
        let parent = acquire_instance_lock(&path).unwrap();

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // 子进程：丢弃继承句柄后，必须无法再锁定该文件.
            close_all_after_fork();
            let code = match acquire_instance_lock(&path) {
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
        assert!(parent.held(), "child must not release parent's kernel lock");
        release_instance_lock(&path);
        let _ = fs::remove_dir_all(&dir);
    }
}
