//! Deterministic resource workloads for interval observation tests.
use std::time::Duration;
fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "idle".into());
    match mode.as_str() {
        "cpu" => {
            let mut x = 1_u64;
            loop {
                x = std::hint::black_box(x.wrapping_mul(6364136223846793005).wrapping_add(1));
            }
        }
        "rss" => {
            let mut pages = Vec::new();
            loop {
                pages.push(vec![std::hint::black_box(42u8); 1024 * 1024]);
                std::hint::black_box(&pages);
                std::thread::sleep(Duration::from_millis(30));
            }
        }
        "io" => {
            use std::io::Write;
            let path = std::env::args_os().nth(2).expect("output file");
            let mut file = std::fs::File::create(path).unwrap();
            loop {
                file.write_all(&[42; 65536]).unwrap();
                file.sync_data().unwrap();
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        _ => loop {
            std::thread::sleep(Duration::from_secs(1));
        },
    }
}
