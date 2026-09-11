//! A well-behaved long-running process: sleeps until signalled. Exits promptly on SIGTERM
//! (default disposition), so it exercises the graceful-termination path.

use std::time::Duration;

fn main() {
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
