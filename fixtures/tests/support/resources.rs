//! Native resource observations shared by the stress harnesses.
#[cfg(unix)]
pub fn handles() -> usize {
    #[cfg(target_os = "linux")]
    let path = "/proc/self/fd";
    #[cfg(not(target_os = "linux"))]
    let path = "/dev/fd";
    std::fs::read_dir(path).unwrap().count()
}
#[cfg(windows)]
pub fn handles() -> usize {
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessHandleCount};
    let mut count = 0;
    // SAFETY: pseudo-handle identifies this process and count is a writable u32;
    // GetProcessHandleCount retains no pointer and its return value is checked.
    let result = unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) };
    assert_ne!(result, 0);
    count as usize
}
#[cfg(unix)]
pub fn assert_no_owned_zombies() {
    let output = std::process::Command::new("ps")
        .args(["-axo", "ppid=,stat=,comm="])
        .output()
        .unwrap();
    assert!(output.status.success());
    let own = std::process::id().to_string();
    for line in String::from_utf8(output.stdout).unwrap().lines() {
        let mut fields = line.split_whitespace();
        if fields.next() == Some(own.as_str()) {
            assert!(
                !fields.next().unwrap_or("").starts_with('Z'),
                "owned zombie: {line}"
            );
        }
    }
}
