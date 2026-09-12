//! One-process resource accounting: warm runtime, repeated lifecycles, then compare.
#![cfg(any(unix, windows))]
use shepherd::{GracePeriod, OutputMode, ProcessSpec, SupervisorBuilder, TerminateOptions};
use std::time::Duration;
#[cfg(unix)]
fn handles() -> usize {
    #[cfg(target_os = "linux")]
    let path = "/proc/self/fd";
    #[cfg(not(target_os = "linux"))]
    let path = "/dev/fd";
    std::fs::read_dir(path).unwrap().count()
}
#[cfg(windows)]
fn handles() -> usize {
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessHandleCount};
    let mut count = 0;
    assert_ne!(
        unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) },
        0
    );
    count as usize
}
#[cfg(unix)]
fn assert_no_owned_zombies() {
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
async fn run(iterations: usize) {
    let sup = SupervisorBuilder::new()
        .stats_interval(Duration::from_millis(10))
        .build();
    let options = TerminateOptions {
        grace: GracePeriod::new(Duration::ZERO),
        force_timeout: Some(Duration::from_secs(3)),
    };
    let mut baseline = None;
    for i in 0..iterations + 8 {
        if i == 8 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            baseline = Some(handles());
        }
        let scope = sup.create_scope();
        let pid = sup
            .spawn(
                scope,
                ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")).output(OutputMode::Capture {
                    buffer_bytes: 64,
                    tail_bytes: 32,
                }),
            )
            .await
            .unwrap();
        let output = sup.take_output(pid).unwrap();
        assert!(sup
            .terminate_scope(scope, options)
            .await
            .unwrap()
            .all_verified());
        assert!(sup.wait(pid).await.unwrap().outcome.is_verified());
        assert!(sup.processes(scope).is_none());
        drop(output);
    }
    sup.shutdown().await.unwrap();
    drop(sup);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = handles();
    let before = baseline.unwrap();
    eprintln!("resource inspection: {iterations} create/capture/kill/reap cycles; handles before={before}, after={after}");
    assert!(
        after <= before,
        "FD/handle growth: before={before}, after={after}"
    );
    #[cfg(unix)]
    assert_no_owned_zombies();
}
#[tokio::test]
async fn bounded_lifecycle_has_no_fd_handle_or_zombie_growth() {
    run(32).await;
}
#[tokio::test]
#[ignore = "long lifecycle stress; workflow_dispatch runs explicitly"]
async fn long_create_kill_capture_and_reap_loops() {
    let iterations = std::env::var("SHEPHERD_STRESS_ITERATIONS")
        .unwrap_or_else(|_| "2000".into())
        .parse()
        .unwrap();
    run(iterations).await;
}
