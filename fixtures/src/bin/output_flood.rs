//! Finite binary output or unbounded stdout/stderr pressure.
use std::io::Write;
fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "both".into());
    if mode == "binary" {
        std::io::stdout()
            .write_all(&[0, 255, 128, 10, 13, 0])
            .unwrap();
        std::io::stderr().write_all(&[254, 0, 42]).unwrap();
        return;
    }
    let bytes = [255; 8192];
    loop {
        if mode != "stderr" {
            std::io::stdout().write_all(&bytes).unwrap();
        }
        if mode != "stdout" {
            std::io::stderr().write_all(&bytes).unwrap();
        }
    }
}
