//! Bounded helper churn for the package-owned application soak.
#![forbid(unsafe_code)]
use std::{
    io::Write,
    time::{Duration, Instant},
};
fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    if mode == "worker" {
        let memory = vec![std::hint::black_box(17u8); 4 * 1024 * 1024];
        let end = Instant::now() + Duration::from_millis(60);
        let mut n = 1u64;
        while Instant::now() < end {
            n = std::hint::black_box(n.wrapping_mul(1664525).wrapping_add(1013904223));
        }
        std::hint::black_box(memory);
        return;
    }
    let end = Instant::now() + Duration::from_secs(10);
    while Instant::now() < end {
        let mut worker = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("worker")
            .spawn()
            .unwrap();
        std::io::stdout().write_all(&[17; 4096]).unwrap();
        std::io::stderr().write_all(&[23; 4096]).unwrap();
        worker.wait().unwrap();
    }
}
