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
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_ignore_sigterm")),
        )
        .await
        .unwrap();
    // Let the fixture install its SIGTERM-ignoring handler before we signal it.
    tokio::time::sleep(Duration::from_millis(300)).await;
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
