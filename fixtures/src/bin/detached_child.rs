//! Fork a detached descendant; optionally let the tracked root exit first.
#[cfg(unix)]
fn main() {
    let args: Vec<_> = std::env::args().collect();
    // SAFETY: fixture is single-threaded, no runtime or inherited locks exist at fork.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        assert!(unsafe { libc::setsid() } >= 0);
        std::fs::write(&args[1], std::process::id().to_string()).unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    }
    if args.get(2).is_some_and(|a| a == "orphan") {
        return;
    }
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
#[cfg(not(unix))]
fn main() {}
