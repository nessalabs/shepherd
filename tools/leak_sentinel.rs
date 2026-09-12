//! Detector control, compiled directly by the LeakSanitizer CI job.
#![forbid(unsafe_code)]
#[inline(never)]
fn allocate(leak: bool) {
    let value = std::hint::black_box(vec![23u8; 37].into_boxed_slice());
    if leak { std::mem::forget(value); }
}
fn main() {
    allocate(std::env::args().nth(1).as_deref() == Some("leak"));
}
