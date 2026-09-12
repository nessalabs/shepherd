//! Portable contract tests over the deterministic `NullBackend`. These exercise the domain
//! and application logic (state machine, verified outcomes, scope isolation, idempotence)
//! without spawning real processes.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use shepherd::{
    Capabilities, Containment, GracePeriod, NullBackend, ProcessBackend, ProcessSpec, SpawnError,
    Spawned, StatsError, SupervisorBuilder, Support, TerminateError, TerminateOptions,
    TerminationOutcome, UnverifiedReason, WaitError,
};
use shepherd_domain::{ProcessScopeId, RawExit, RawStats, Signal};

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

/// Backend whose `wait` always fails, to prove we never manufacture a verified exit.
struct WaitFailsBackend {
    inner: NullBackend,
}

#[async_trait]
impl ProcessBackend for WaitFailsBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        self.inner.spawn(scope, spec).await
    }

    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }

    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }

    async fn wait(&self, _target: &Spawned) -> Result<RawExit, WaitError> {
        Err(WaitError::Backend("lost child handle".into()))
    }

    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        self.inner.sample(target).await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            descendant_containment: Containment::None,
            cpu: Support::Unsupported,
            rss: Support::Unsupported,
            peak_rss: Support::Unsupported,
            io: Support::Unsupported,
            force_termination: true,
        }
    }

    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }

    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

#[tokio::test(start_paused = true)]
async fn wait_failure_is_cleanup_unverified() {
    let sup = SupervisorBuilder::new()
        .backend(Arc::new(WaitFailsBackend {
            inner: NullBackend::new(),
        }))
        .build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .await
        .unwrap();
    let exit = sup.wait(pid).await.unwrap();
    assert_eq!(
        exit.outcome,
        TerminationOutcome::CleanupUnverified(UnverifiedReason::ReapFailed)
    );
    for _ in 0..2 {
        let report = sup.terminate_scope(scope, short_opts()).await.unwrap();
        assert!(!report.all_verified());
        assert_eq!(report.outcomes, vec![(pid, exit.outcome)]);
        assert!(matches!(
            sup.shutdown().await,
            Err(shepherd::ShutdownError::Unverified(1))
        ));
    }
}

struct CleanupFailsBackend {
    inner: NullBackend,
    failing: std::sync::atomic::AtomicBool,
    attempts: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl ProcessBackend for CleanupFailsBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        self.inner.spawn(scope, spec).await
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn cleanup_scope(&self, scope: ProcessScopeId) -> Result<(), TerminateError> {
        use std::sync::atomic::Ordering;
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) {
            Err(TerminateError::Signal(
                "injected containment failure".into(),
            ))
        } else {
            self.inner.cleanup_scope(scope).await
        }
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        self.inner.sample(target).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_retries_failed_containment_for_empty_and_reaped_scopes() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    for populated in [false, true] {
        let backend = Arc::new(CleanupFailsBackend {
            inner: NullBackend::new(),
            failing: AtomicBool::new(true),
            attempts: AtomicUsize::new(0),
        });
        let sup = SupervisorBuilder::new().backend(backend.clone()).build();
        let scope = sup.create_scope();
        if populated {
            sup.spawn(scope, ProcessSpec::new("respect-graceful"))
                .await
                .unwrap();
        }
        for expected in 1..=2 {
            assert!(matches!(
                sup.shutdown().await,
                Err(shepherd::ShutdownError::Unverified(1))
            ));
            assert_eq!(backend.attempts.load(Ordering::SeqCst), expected);
            assert_eq!(sup.processes(scope), Some(Vec::new()));
        }
        backend.failing.store(false, Ordering::SeqCst);
        let report = sup.shutdown().await.unwrap();
        assert_eq!(report.scopes.len(), 1);
        assert!(report.scopes[0].all_verified());
        assert_eq!(sup.processes(scope), None);
        assert!(sup
            .terminate_scope(scope, short_opts())
            .await
            .unwrap()
            .all_verified());
        assert!(sup.shutdown().await.unwrap().scopes.is_empty());
        assert_eq!(backend.attempts.load(Ordering::SeqCst), 3);
        assert!(matches!(sup.spawn(scope, ProcessSpec::new("unused")).await,
            Err(SpawnError::ScopeClosed(id)) if id == scope));
    }
}

#[tokio::test]
async fn completed_scope_reports_are_bounded() {
    let sup = supervisor();
    let oldest = sup.create_scope();
    sup.terminate_scope(oldest, short_opts()).await.unwrap();
    for _ in 0..256 {
        let scope = sup.create_scope();
        assert!(sup
            .terminate_scope(scope, short_opts())
            .await
            .unwrap()
            .all_verified());
        assert!(sup
            .terminate_scope(scope, short_opts())
            .await
            .unwrap()
            .all_verified());
    }
    assert!(matches!(sup.terminate_scope(oldest, short_opts()).await,
        Err(TerminateError::UnknownScope(id)) if id == oldest));
}
