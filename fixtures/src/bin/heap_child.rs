//! Finite captured-output child for allocation lifetime checks.
#![forbid(unsafe_code)]
use std::io::Write;
fn main() {
    std::io::stdout().write_all(&[17; 4096]).unwrap();
    std::io::stderr().write_all(&[23; 4096]).unwrap();
}
