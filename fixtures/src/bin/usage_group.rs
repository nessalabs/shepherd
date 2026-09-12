//! Single-threaded Unix membership fixture; child always expires within 15 seconds.
#[cfg(unix)]
fn main() {
    let args: Vec<_> = std::env::args().collect();
    // SAFETY: no runtime or user threads exist; fixture owns both fork branches.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        if args[2] == "detach" {
            assert!(unsafe { libc::setsid() } >= 0);
        }
        std::fs::write(&args[1], std::process::id().to_string()).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(15));
        return;
    }
    if args[2] == "orphan" {
        return;
    }
    // Parent reaps the detached test child normally when it expires.
    assert_eq!(unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) }, pid);
}
#[cfg(not(unix))]
fn main() {}
