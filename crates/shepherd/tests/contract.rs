//! Portable contract tests over the deterministic `NullBackend`. These exercise the domain
//! and application logic (state machine, verified outcomes, scope isolation, idempotence)
//! without spawning real processes.

use std::sync::Arc;
use std::time::Duration;

use shepherd::{
    GracePeriod, NullBackend, ProcessSpec, SpawnError, SupervisorBuilder, TerminateOptions,
    TerminationOutcome,
};

fn supervisor() -> shepherd::ProcessSupervisor {
    SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .build()
}

fn short_opts() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_millis(50)),
        force_timeout: Some(Duration::from_secs(5)),
    }
}

#[tokio::test(start_paused = true)]
async fn graceful_termination_reports_graceful_success() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .await
        .unwrap();
    let exit = sup.terminate(pid, short_opts()).await.unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::GracefulSuccess);
}

#[tokio::test(start_paused = true)]
async fn ignoring_graceful_escalates_to_force() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("ignore-graceful"))
        .await
        .unwrap();
    let exit = sup.terminate(pid, short_opts()).await.unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::ForcedRequired);
    assert!(exit.forced);
}

#[tokio::test(start_paused = true)]
async fn natural_exit_is_reported() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("exit-immediately"))
        .await
        .unwrap();
    let exit = sup.wait(pid).await.unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::ExitedNaturally);
}

#[tokio::test(start_paused = true)]
async fn terminate_scope_cannot_touch_another_scope() {
    // The flagship isolation test (invariant #5).
    let sup = supervisor();
    let scope_a = sup.create_scope();
    let scope_b = sup.create_scope();
    let _a = sup
        .spawn(scope_a, ProcessSpec::new("respect-graceful"))
        .await
        .unwrap();
    let b = sup
        .spawn(scope_b, ProcessSpec::new("ignore-graceful"))
        .await
        .unwrap();

    let report = sup.terminate_scope(scope_a, short_opts()).await.unwrap();
    assert!(report.all_verified());

    // Scope B is completely untouched: its process is still live.
    let live_b = sup.processes(scope_b).expect("scope b exists");
    assert_eq!(live_b, vec![b], "terminating A must not affect B");

    // And B can still be cleaned up on its own.
    let report_b = sup.terminate_scope(scope_b, short_opts()).await.unwrap();
    assert!(report_b.all_verified());
}

#[tokio::test(start_paused = true)]
async fn repeated_terminate_is_idempotent() {
    // invariant #6
    let sup = supervisor();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("ignore-graceful"))
        .await
        .unwrap();
    let first = sup.terminate(pid, short_opts()).await.unwrap();
    let second = sup.terminate(pid, short_opts()).await.unwrap();
    assert_eq!(first.outcome, TerminationOutcome::ForcedRequired);
    assert_eq!(second.outcome, TerminationOutcome::ForcedRequired);
}

#[tokio::test(start_paused = true)]
async fn draining_scope_rejects_new_spawns() {
    // invariant #3
    let sup = supervisor();
    let scope = sup.create_scope();
    // Terminating an empty scope leaves it draining (no processes to reap and close it).
    sup.terminate_scope(scope, short_opts()).await.unwrap();
    let err = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .await
        .unwrap_err();
    assert!(matches!(err, SpawnError::ScopeClosed(_)));
}

#[tokio::test(start_paused = true)]
async fn shutdown_is_idempotent() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let _ = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .await
        .unwrap();
    sup.shutdown().await.unwrap();
    // A second shutdown is a no-op that still succeeds.
    sup.shutdown().await.unwrap();
}
