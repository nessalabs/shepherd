//! Unix membership fixture; child always expires within 15 seconds.
#![forbid(unsafe_code)]
#[cfg(unix)]
#[allow(clippy::zombie_processes)] // Orphan mode intentionally exits; the OS adopts the bounded child.
fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(3).is_some_and(|a| a == "child") {
        if args[2] == "detach" {
            nix::unistd::setsid().unwrap();
        }
        std::fs::write(&args[1], std::process::id().to_string()).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(15));
    } else {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(&args[1..])
            .arg("child")
            .spawn()
            .unwrap();
        if args[2] != "orphan" {
            child.wait().unwrap();
        }
    }
}
#[cfg(not(unix))]
fn main() {}
