//! Portable, bounded-lifetime process chain for read-only observation tests.
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    let stop = PathBuf::from(&args[1]);
    let depth: u32 = args[2].to_str().unwrap().parse().unwrap();
    let mut child = (depth > 0).then(|| {
        std::process::Command::new(std::env::current_exe().unwrap())
            .arg(&stop)
            .arg((depth - 1).to_string())
            .spawn()
            .unwrap()
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    // Keep a user thread alive: Linux enumeration must not render it as a child process.
    let thread_stop = stop.clone();
    let thread = std::thread::spawn(move || {
        while !thread_stop.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
    });
    while !stop.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    thread.join().unwrap();
    if let Some(child) = child.as_mut() {
        child.wait().unwrap();
    }
}
