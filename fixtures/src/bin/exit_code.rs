//! Exits immediately with the exit code given as the first argument (default 0). Used to test
//! natural-exit reporting.

fn main() {
    let code: i32 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    std::process::exit(code);
}
