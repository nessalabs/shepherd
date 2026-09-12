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

/// Backend whose monitor fails, with explicit controls for a later retry.
#[derive(Default)]
struct WaitFailsBackend {
    inner: NullBackend,
    recover: std::sync::atomic::AtomicBool,
    hang: std::sync::atomic::AtomicBool,
    recover_on_kill: std::sync::atomic::AtomicBool,
    signals: std::sync::Mutex<Vec<Signal>>,
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
        self.signals.lock().unwrap().push(signal);
        if signal == Signal::Kill
            && self
                .recover_on_kill
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.recover
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.signal(target, signal).await
    }

    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }

    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        use std::sync::atomic::Ordering;
        if self.hang.load(Ordering::SeqCst) {
            return std::future::pending().await;
        }
        if self.recover.load(Ordering::SeqCst) {
            return self.inner.wait(target).await;
        }
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

    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

#[tokio::test(start_paused = true)]
async fn wait_failure_is_cleanup_unverified() {
    let sup = SupervisorBuilder::new()
        .backend(Arc::new(WaitFailsBackend {
            inner: NullBackend::new(),
            ..Default::default()
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

#[derive(Default)]
struct RecordingPublisher(std::sync::Mutex<Vec<shepherd::IntegrationEvent>>);
#[async_trait]
impl shepherd::IntegrationEventPublisher for RecordingPublisher {
    async fn publish(&self, event: shepherd::IntegrationEvent) {
        self.0.lock().unwrap().push(event);
    }
}
impl RecordingPublisher {
    fn scope_count(&self, scope: ProcessScopeId) -> usize {
        self.0.lock().unwrap().iter().filter(|event|
            matches!(event, shepherd::IntegrationEvent::ScopeTerminated { scope: id } if *id == scope)
        ).count()
    }
    async fn wait_for_scope(&self, scope: ProcessScopeId) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.scope_count(scope) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
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
    for mode in ["empty", "respect-graceful", "exit-immediately"] {
        let backend = Arc::new(CleanupFailsBackend {
            inner: NullBackend::new(),
            failing: AtomicBool::new(true),
            attempts: AtomicUsize::new(0),
        });
        let publisher = Arc::new(RecordingPublisher::default());
        let sup = SupervisorBuilder::new()
            .backend(backend.clone())
            .integration_publisher(publisher.clone())
            .build();
        let scope = sup.create_scope();
        let pid = if mode != "empty" {
            let pid = sup.spawn(scope, ProcessSpec::new(mode)).await.unwrap();
            if mode == "exit-immediately" {
                sup.wait(pid).await.unwrap();
            }
            Some(pid)
        } else {
            None
        };
        for expected in 1..=2 {
            assert!(matches!(
                sup.shutdown().await,
                Err(shepherd::ShutdownError::Unverified(1))
            ));
            assert_eq!(backend.attempts.load(Ordering::SeqCst), expected);
            assert_eq!(sup.processes(scope), Some(Vec::new()));
            assert_eq!(
                publisher.scope_count(scope),
                0,
                "published before verified cleanup"
            );
        }
        backend.failing.store(false, Ordering::SeqCst);
        let report = sup.shutdown().await.unwrap();
        assert_eq!(report.scopes.len(), 1);
        assert!(report.scopes[0].all_verified());
        if mode == "respect-graceful" {
            assert_eq!(
                report.scopes[0].outcomes,
                vec![(pid.unwrap(), TerminationOutcome::GracefulSuccess)]
            );
        }
        assert_eq!(sup.processes(scope), None);
        assert!(sup
            .terminate_scope(scope, short_opts())
            .await
            .unwrap()
            .all_verified());
        assert!(sup.shutdown().await.unwrap().scopes.is_empty());
        assert_eq!(backend.attempts.load(Ordering::SeqCst), 3);
        publisher.wait_for_scope(scope).await;
        assert_eq!(publisher.scope_count(scope), 1);
        assert!(matches!(sup.spawn(scope, ProcessSpec::new("unused")).await,
            Err(SpawnError::ScopeClosed(id)) if id == scope));
    }
}

#[tokio::test]
async fn cancellation_before_workers_complete_preserves_reaped_outcomes() {
    use std::future::Future;
    use std::task::Poll;
    let backend = Arc::new(NullBackend::new());
    let publisher = Arc::new(RecordingPublisher::default());
    let sup = SupervisorBuilder::new()
        .backend(backend.clone())
        .integration_publisher(publisher.clone())
        .build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .await
        .unwrap();
    let mut termination = Box::pin(sup.terminate_scope(scope, short_opts()));
    // One poll begins draining and queues workers; this current-thread runtime cannot
    // run them before we cancel, so there is no local JoinSet outcome to preserve.
    std::future::poll_fn(|cx| {
        assert!(termination.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(termination);
    backend.signal_scope(scope, Signal::Kill).await.unwrap();
    let exit = sup.wait(pid).await.unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::ForcedRequired);
    assert_eq!(publisher.scope_count(scope), 0);
    let report = sup.terminate_scope(scope, short_opts()).await.unwrap();
    assert_eq!(report.outcomes, vec![(pid, exit.outcome)]);
    assert_eq!(
        sup.terminate_scope(scope, short_opts())
            .await
            .unwrap()
            .outcomes,
        report.outcomes
    );
    publisher.wait_for_scope(scope).await;
    assert_eq!(publisher.scope_count(scope), 1);
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

#[tokio::test(start_paused = true)]
async fn quarantined_root_retries_signals_and_accepts_only_fresh_verified_reap() {
    use std::sync::atomic::Ordering;
    for program in ["respect-graceful", "ignore-graceful"] {
        let backend = Arc::new(WaitFailsBackend::default());
        let sup = SupervisorBuilder::new().backend(backend.clone()).build();
        let scope = sup.create_scope();
        let pid = sup.spawn(scope, ProcessSpec::new(program)).await.unwrap();
        assert!(!sup.wait(pid).await.unwrap().outcome.is_verified());
        backend.recover.store(true, Ordering::SeqCst);
        let recovered = sup.terminate(pid, short_opts()).await.unwrap();
        assert!(recovered.outcome.is_verified());
        assert_eq!(sup.wait(pid).await.unwrap(), recovered);
        assert_eq!(sup.terminate(pid, short_opts()).await.unwrap(), recovered);
        assert_eq!(
            *backend.signals.lock().unwrap(),
            if program == "respect-graceful" {
                vec![Signal::Term]
            } else {
                vec![Signal::Term, Signal::Kill]
            }
        );
        assert!(sup.processes(scope).unwrap().is_empty());
        assert!(sup
            .terminate_scope(scope, short_opts())
            .await
            .unwrap()
            .all_verified());
    }
}

#[tokio::test(start_paused = true)]
async fn quarantined_retry_still_forces_when_wait_errors_or_times_out() {
    use std::sync::atomic::Ordering;
    for hangs in [false, true] {
        let backend = Arc::new(WaitFailsBackend::default());
        let sup = SupervisorBuilder::new().backend(backend.clone()).build();
        let scope = sup.create_scope();
        let pid = sup
            .spawn(scope, ProcessSpec::new("ignore-graceful"))
            .await
            .unwrap();
        assert!(!sup.wait(pid).await.unwrap().outcome.is_verified());
        backend.hang.store(hangs, Ordering::SeqCst);
        let retry = sup.terminate(pid, short_opts()).await.unwrap();
        assert!(!retry.outcome.is_verified());
        assert_eq!(
            *backend.signals.lock().unwrap(),
            vec![Signal::Term, Signal::Kill]
        );
        assert!(!sup.wait(pid).await.unwrap().outcome.is_verified());
        backend.hang.store(false, Ordering::SeqCst);
        backend.recover.store(true, Ordering::SeqCst);
        let recovered = sup.terminate(pid, short_opts()).await.unwrap();
        assert_eq!(recovered.outcome, TerminationOutcome::ForcedRequired);
        assert!(sup.shutdown().await.is_ok());
    }
}

#[tokio::test(start_paused = true)]
async fn fresh_retry_wait_error_still_forces_then_records_recovered_scope_outcome() {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(WaitFailsBackend::default());
    let sup = SupervisorBuilder::new().backend(backend.clone()).build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("ignore-graceful"))
        .await
        .unwrap();
    assert!(!sup.wait(pid).await.unwrap().outcome.is_verified());
    backend.recover_on_kill.store(true, Ordering::SeqCst);
    let report = sup.terminate_scope(scope, short_opts()).await.unwrap();
    assert_eq!(
        report.outcomes,
        vec![(pid, TerminationOutcome::ForcedRequired)]
    );
    assert_eq!(
        *backend.signals.lock().unwrap(),
        vec![Signal::Term, Signal::Kill]
    );
    assert!(sup.wait(pid).await.unwrap().outcome.is_verified());
    assert!(sup.shutdown().await.is_ok());
}
