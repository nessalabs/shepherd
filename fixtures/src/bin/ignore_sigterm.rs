//! A stubborn process that ignores graceful termination (SIGTERM), forcing an escalation to
//! SIGKILL. Used to exercise the force path.

use std::time::Duration;

fn main() {
    #[cfg(unix)]
    // SAFETY: installing SIG_IGN for SIGTERM is a simple, valid libc call.
    unsafe {
        libc::signal(libc::SIGTERM, libc::SIG_IGN);
    }
    loop {
        std::thread::sleep(Duration::from_millis(200));
    }
}
