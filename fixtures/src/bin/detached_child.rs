//! Start a detached descendant; optionally let the tracked root exit first.
#![forbid(unsafe_code)]
#[cfg(unix)]
#[allow(clippy::zombie_processes)] // Orphan mode intentionally transfers the live child to the OS/subreaper.
fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.get(2).is_some_and(|a| a == "child") {
        nix::unistd::setsid().unwrap();
        std::fs::write(&args[1], std::process::id().to_string()).unwrap();
    } else {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg(&args[1])
            .arg("child")
            .spawn()
            .unwrap();
        if args.get(2).is_some_and(|a| a == "orphan") {
            return;
        }
        child.wait().unwrap();
        return;
    }
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
#[cfg(not(unix))]
fn main() {}
