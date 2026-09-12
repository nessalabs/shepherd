//! Spawns N child processes (default 3) that each sleep, then sleeps itself. Used to test
//! descendant cleanup: killing the scope's process group must take the children too.

use std::process::Command;
use std::time::Duration;

fn main() {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let mut children = Vec::new();
    for _ in 0..count {
        if let Ok(child) = Command::new("sleep").arg("3600").spawn() {
            children.push(child);
        }
    }
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
