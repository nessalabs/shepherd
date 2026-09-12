//! A root that creates a descendant before optionally exiting.
fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.get(1).is_some_and(|a| a == "child") {
        std::fs::write(&args[2], std::process::id().to_string()).unwrap();
    } else {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("child")
            .arg(&args[1])
            .spawn()
            .unwrap();
        // The kernel Job Object owns containment; the fixture intentionally does not reap.
        drop(child);
        if args.get(2).is_some_and(|a| a == "orphan") {
            return;
        }
    }
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
