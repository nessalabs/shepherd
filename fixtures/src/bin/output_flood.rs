//! Finite binary output or unbounded stdout/stderr pressure.
use std::io::Write;
fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "both".into());
    if mode == "inherit-pipes" {
        #[cfg(unix)]
        // SAFETY: SIG_IGN is a valid signal disposition, not a Rust callback;
        // the single-threaded fixture changes only its own SIGTERM behavior.
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
        // This descendant keeps both inherited pipes open after the root is killed.
        // The enclosing scope owns its eventual cleanup.
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("hold-pipes")
            .spawn()
            .unwrap();
        std::io::stdout().write_all(b"ready").unwrap();
        std::io::stdout().flush().unwrap();
        child.wait().unwrap();
        return;
    }
    if mode == "hold-pipes" {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    }
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
