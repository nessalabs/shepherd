//! Real-process integration tests on Unix, driving the default `UnixProcessBackend` against
//! the pathological fixture binaries. These prove genuine OS process lifecycle: real spawn,
//! real signals, real reaping.
#![cfg(unix)]

use std::time::Duration;

use shepherd::{GracePeriod, ProcessSpec, SupervisorBuilder, TerminateOptions, TerminationOutcome};

fn short_opts() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_millis(300)),
        force_timeout: Some(Duration::from_secs(10)),
    }
}

#[tokio::test]
async fn graceful_termination_of_a_real_process() {
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .unwrap();
    // sleep_forever exits on the default SIGTERM disposition.
    let exit = sup
        .terminate(pid, TerminateOptions::default())
        .await
        .unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::GracefulSuccess);
    assert!(!exit.forced);
}

#[tokio::test]
async fn stubborn_process_is_force_killed() {
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let ready = std::env::temp_dir().join(format!("shepherd-stubborn-{}", std::process::id()));
    let _ = std::fs::remove_file(&ready);
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_ignore_sigterm")).arg(ready.as_os_str()),
        )
        .await
        .unwrap();
    // Readiness is emitted only after installing SIG_IGN, independent of host load.
    let _ = read_os_pid(&ready).await;
    std::fs::remove_file(ready).unwrap();
    let exit = sup.terminate(pid, short_opts()).await.unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::ForcedRequired);
    assert!(exit.forced);
}

#[tokio::test]
async fn natural_exit_reports_exit_code() {
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_exit_code")).arg("7"),
        )
        .await
        .unwrap();
    let exit = sup.wait(pid).await.unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::ExitedNaturally);
    assert_eq!(exit.code, Some(7));
}

#[tokio::test]
async fn terminating_one_scope_leaves_another_running() {
    // The flagship isolation guarantee, with real processes.
    let sup = SupervisorBuilder::new().build();
    let scope_a = sup.create_scope();
    let scope_b = sup.create_scope();
    let _a = sup
        .spawn(
            scope_a,
            ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")),
        )
        .await
        .unwrap();
    let b = sup
        .spawn(
            scope_b,
            ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")),
        )
        .await
        .unwrap();

    let report = sup.terminate_scope(scope_a, short_opts()).await.unwrap();
    assert!(report.all_verified());

    // Scope B is untouched.
    assert_eq!(sup.processes(scope_b).unwrap(), vec![b]);

    let report_b = sup.terminate_scope(scope_b, short_opts()).await.unwrap();
    assert!(report_b.all_verified());
}

#[tokio::test]
async fn scope_termination_cleans_up_descendants() {
    // spawn_children forks 3 `sleep` children into the scope's process group; terminating the
    // scope must take the whole group (roots verified, group swept for descendants).
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let _pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_spawn_children")).arg("3"),
        )
        .await
        .unwrap();
    // Give the fixture a moment to fork its children.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let report = sup.terminate_scope(scope, short_opts()).await.unwrap();
    assert!(report.all_verified());
}

fn process_alive(os_pid: u32) -> bool {
    // kill(pid, 0) is a liveness probe; ESRCH means the pid is gone.
    unsafe { libc::kill(os_pid as i32, 0) == 0 }
}

fn spawn_sleep_writing_pid(pid_file: &std::path::Path) -> ProcessSpec {
    ProcessSpec::new("sh").args([
        "-c",
        &format!("echo $$ > {}; exec sleep 3600", pid_file.display()),
    ])
}

async fn read_os_pid(pid_file: &std::path::Path) -> u32 {
    for _ in 0..250 {
        if let Ok(text) = std::fs::read_to_string(pid_file) {
            if let Ok(pid) = text.trim().parse::<u32>() {
                return pid;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for pid file {}", pid_file.display());
}

#[tokio::test]
async fn dropping_supervisor_hard_kills_running_children() {
    let dir = std::env::temp_dir();
    let pid_file = dir.join(format!("shepherd-drop-{}.pid", std::process::id()));
    let _ = std::fs::remove_file(&pid_file);

    let os_pid = {
        let sup = SupervisorBuilder::new().build();
        let scope = sup.create_scope();
        let _pid = sup
            .spawn(scope, spawn_sleep_writing_pid(&pid_file))
            .await
            .unwrap();
        let os_pid = read_os_pid(&pid_file).await;
        assert!(process_alive(os_pid), "child should be running before drop");
        os_pid
        // last supervisor handle drops here → CleanupGuard hard-kills
    };

    let mut gone = false;
    for _ in 0..50 {
        if !process_alive(os_pid) {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = std::fs::remove_file(&pid_file);
    assert!(gone, "dropped supervisor must SIGKILL remaining children");
}

#[tokio::test]
async fn concurrent_first_spawns_share_one_process_group() {
    let dir = std::env::temp_dir();
    let a_file = dir.join(format!("shepherd-conc-a-{}.pid", std::process::id()));
    let b_file = dir.join(format!("shepherd-conc-b-{}.pid", std::process::id()));
    let _ = std::fs::remove_file(&a_file);
    let _ = std::fs::remove_file(&b_file);

    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let (ra, rb) = tokio::join!(
        sup.spawn(scope, spawn_sleep_writing_pid(&a_file)),
        sup.spawn(scope, spawn_sleep_writing_pid(&b_file)),
    );
    ra.unwrap();
    rb.unwrap();

    #[cfg_attr(not(target_os = "linux"), expect(unused_variables))]
    let os_a = read_os_pid(&a_file).await;
    #[cfg_attr(not(target_os = "linux"), expect(unused_variables))]
    let os_b = read_os_pid(&b_file).await;
    #[cfg(target_os = "linux")]
    {
        let pg_a = process_group(os_a);
        let pg_b = process_group(os_b);
        assert_eq!(
            pg_a, pg_b,
            "concurrent first spawns into an empty scope must share one process group"
        );
    }

    let report = sup.terminate_scope(scope, short_opts()).await.unwrap();
    assert!(report.all_verified());
    let _ = std::fs::remove_file(&a_file);
    let _ = std::fs::remove_file(&b_file);
}

#[cfg(target_os = "linux")]
fn process_group(os_pid: u32) -> i32 {
    let stat = std::fs::read_to_string(format!("/proc/{os_pid}/stat")).expect("stat");
    let after = stat.rsplit_once(')').expect("comm").1;
    after
        .split_whitespace()
        .nth(3)
        .expect("pgrp")
        .parse()
        .expect("pgrp int")
}

#[tokio::test]
async fn scope_can_be_reused_after_natural_exit() {
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let first = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_exit_code")).arg("0"),
        )
        .await
        .unwrap();
    let exit = sup.wait(first).await.unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::ExitedNaturally);

    // The process group is now empty and must be forgotten so the next spawn can create one.
    let second = sup
        .spawn(scope, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .expect("reusing an open scope after natural exit must succeed");
    let exit = sup.terminate(second, short_opts()).await.unwrap();
    assert!(exit.outcome.is_verified());
}

#[tokio::test]
async fn roots_can_exit_and_reuse_scope_without_losing_original_descendants() {
    use std::sync::Arc;
    let sup = SupervisorBuilder::new()
        .backend(Arc::new(shepherd::UnixProcessBackend::new()))
        .build();
    assert_eq!(
        sup.capabilities().descendant_containment,
        shepherd::Containment::ProcessGroup
    );
    let scope = sup.create_scope();
    let file = std::env::temp_dir().join(format!("shepherd-orphan-group-{}", std::process::id()));
    let _ = std::fs::remove_file(&file);
    let root = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_job_tree"))
                .arg(file.as_os_str())
                .arg("orphan"),
        )
        .await
        .unwrap();
    let descendant = read_os_pid(&file).await;
    assert_eq!(sup.wait(root).await.unwrap().code, Some(0));
    let replacement = sup
        .spawn(scope, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .unwrap();
    assert!(sup
        .terminate_scope(scope, short_opts())
        .await
        .unwrap()
        .all_verified());
    assert!(sup.wait(replacement).await.unwrap().outcome.is_verified());
    tokio::time::timeout(Duration::from_secs(5), async {
        while process_alive(descendant) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("original descendant was lost when scope was reused");
    std::fs::remove_file(file).unwrap();
}
