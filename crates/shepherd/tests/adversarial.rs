use async_trait::async_trait;
use shepherd::*;
use shepherd_domain::{RawExit, RawStats};
use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

const SPAWN_PAUSE: u8 = 1;
const SPAWN_FAIL: u8 = 2;
const SIGNAL_FAIL: u8 = 3;
const WAIT_FAIL: u8 = 4;
const SAMPLE_FAIL: u8 = 5;
const EXIT_IN_SAMPLE: u8 = 6;
const EXIT_IN_SIGNAL: u8 = 7;
const SCOPE_FAIL: u8 = 8;
const SAMPLE_PANIC: u8 = 9;
const WAIT_PANIC: u8 = 10;
struct Faults {
    inner: NullBackend,
    mode: AtomicU8,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl Faults {
    fn new(mode: u8) -> Arc<Self> {
        Arc::new(Self {
            inner: NullBackend::new(),
            mode: AtomicU8::new(mode),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        })
    }
}
#[async_trait]
impl ProcessBackend for Faults {
    async fn spawn(&self, s: ProcessScopeId, p: &ProcessSpec) -> Result<Spawned, SpawnError> {
        match self.mode.load(Ordering::SeqCst) {
            SPAWN_FAIL => return Err(SpawnError::Os("injected spawn failure".into())),
            SPAWN_PAUSE => {
                self.entered.notify_one();
                self.release.notified().await;
            }
            _ => {}
        }
        self.inner.spawn(s, p).await
    }
    async fn signal(&self, p: &Spawned, s: Signal) -> Result<(), TerminateError> {
        match self.mode.load(Ordering::SeqCst) {
            SIGNAL_FAIL => return Err(TerminateError::Signal("injected signal failure".into())),
            EXIT_IN_SIGNAL => self.inner.signal(p, Signal::Term).await?,
            _ => {}
        }
        self.inner.signal(p, s).await
    }
    async fn signal_scope(&self, p: ProcessScopeId, s: Signal) -> Result<(), TerminateError> {
        if self.mode.load(Ordering::SeqCst) == SCOPE_FAIL {
            return Err(TerminateError::Signal(
                "injected scope sweep failure".into(),
            ));
        }
        self.inner.signal_scope(p, s).await
    }
    async fn wait(&self, p: &Spawned) -> Result<RawExit, WaitError> {
        assert_ne!(
            self.mode.load(Ordering::SeqCst),
            WAIT_PANIC,
            "injected waiter panic"
        );
        if self.mode.load(Ordering::SeqCst) == WAIT_FAIL {
            return Err(WaitError::Backend("injected lost waiter".into()));
        }
        self.inner.wait(p).await
    }
    async fn sample(&self, p: &Spawned) -> Result<RawStats, StatsError> {
        assert_ne!(
            self.mode.load(Ordering::SeqCst),
            SAMPLE_PANIC,
            "injected sampler panic"
        );
        match self.mode.load(Ordering::SeqCst) {
            SAMPLE_FAIL => return Err(StatsError::Backend("injected sampler failure".into())),
            EXIT_IN_SAMPLE => self.inner.signal(p, Signal::Term).await.unwrap(),
            _ => {}
        }
        self.inner.sample(p).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all()
    }
    fn hard_kill_scope(&self, s: ProcessScopeId) {
        self.inner.hard_kill_scope(s)
    }
}
fn sup(backend: Arc<Faults>) -> ProcessSupervisor {
    SupervisorBuilder::new()
        .backend(backend)
        .stats_interval(Duration::from_millis(5))
        .build()
}
fn opts() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_millis(5)),
        force_timeout: Some(Duration::from_millis(30)),
    }
}
#[tokio::test]
async fn terminate_waits_for_admitted_spawn_even_when_spawn_caller_is_aborted() {
    let backend = Faults::new(SPAWN_PAUSE);
    let supervisor = sup(backend.clone());
    let scope = supervisor.create_scope();
    let worker = supervisor.clone();
    let spawn = tokio::spawn(async move { worker.spawn(scope, ProcessSpec::new("owned")).await });
    backend.entered.notified().await;
    spawn.abort();
    let _ = spawn.await;
    let worker = supervisor.clone();
    let cleanup = tokio::spawn(async move { worker.terminate_scope(scope, opts()).await });
    tokio::task::yield_now().await;
    assert!(!cleanup.is_finished());
    backend.release.notify_one();
    let report = cleanup.await.unwrap().unwrap();
    assert_eq!(report.outcomes.len(), 1);
    assert!(report.all_verified());
}
#[tokio::test]
async fn signal_failure_never_fakes_exit_and_retry_can_reap() {
    let backend = Faults::new(SIGNAL_FAIL);
    let supervisor = sup(backend.clone());
    let scope = supervisor.create_scope();
    let pid = supervisor
        .spawn(scope, ProcessSpec::new("owned"))
        .await
        .unwrap();
    assert!(!supervisor
        .terminate(pid, opts())
        .await
        .unwrap()
        .outcome
        .is_verified());
    assert_eq!(supervisor.processes(scope).unwrap(), vec![pid]);
    backend.mode.store(0, Ordering::SeqCst);
    assert!(supervisor
        .terminate_scope(scope, opts())
        .await
        .unwrap()
        .all_verified());
}
#[tokio::test]
async fn lost_waiter_stays_unverified_in_scope_and_shutdown() {
    let supervisor = sup(Faults::new(WAIT_FAIL));
    let scope = supervisor.create_scope();
    let pid = supervisor
        .spawn(scope, ProcessSpec::new("owned"))
        .await
        .unwrap();
    assert!(matches!(
        supervisor.wait(pid).await.unwrap().outcome,
        TerminationOutcome::CleanupUnverified(UnverifiedReason::ReapFailed)
    ));
    assert!(!supervisor
        .terminate_scope(scope, opts())
        .await
        .unwrap()
        .all_verified());
    assert!(supervisor.shutdown().await.is_err());
    assert!(supervisor.shutdown().await.is_err());
}
#[tokio::test]
async fn spawn_and_sampler_failures_keep_ownership_honest() {
    let backend = Faults::new(SPAWN_FAIL);
    let supervisor = sup(backend.clone());
    let scope = supervisor.create_scope();
    assert!(supervisor
        .spawn(scope, ProcessSpec::new("owned"))
        .await
        .is_err());
    assert!(supervisor.processes(scope).unwrap().is_empty());
    backend.mode.store(SAMPLE_FAIL, Ordering::SeqCst);
    let pid = supervisor
        .spawn(scope, ProcessSpec::new("owned"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(supervisor.stats(pid).await, Err(StatsError::Backend(_))) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(supervisor.processes(scope).unwrap(), vec![pid]);
    assert!(supervisor
        .terminate_scope(scope, opts())
        .await
        .unwrap()
        .all_verified());
}
#[tokio::test]
async fn exit_during_sample_does_not_resurrect_cache() {
    let supervisor = sup(Faults::new(EXIT_IN_SAMPLE));
    let scope = supervisor.create_scope();
    let pid = supervisor
        .spawn(scope, ProcessSpec::new("owned"))
        .await
        .unwrap();
    supervisor.wait(pid).await.unwrap();
    assert!(matches!(
        supervisor.stats(pid).await,
        Err(StatsError::UnknownProcess(_))
    ));
    supervisor.terminate_scope(scope, opts()).await.unwrap();
}
#[tokio::test]
async fn exit_between_lookup_and_signal_and_shutdown_races_are_idempotent() {
    let supervisor = sup(Faults::new(EXIT_IN_SIGNAL));
    let scope = supervisor.create_scope();
    let pid = supervisor
        .spawn(scope, ProcessSpec::new("owned"))
        .await
        .unwrap();
    let (exit, report, shutdown) = tokio::join!(
        supervisor.terminate(pid, opts()),
        supervisor.terminate_scope(scope, opts()),
        supervisor.shutdown()
    );
    assert!(exit.unwrap().outcome.is_verified());
    assert!(report.unwrap().all_verified());
    assert!(shutdown.is_ok());
}
#[tokio::test]
async fn failed_final_sweep_is_not_a_verified_shutdown() {
    let backend = Faults::new(SCOPE_FAIL);
    let supervisor = sup(backend.clone());
    let scope = supervisor.create_scope();
    supervisor
        .spawn(scope, ProcessSpec::new("owned"))
        .await
        .unwrap();
    assert!(supervisor.terminate_scope(scope, opts()).await.is_err());
    assert!(supervisor.shutdown().await.is_err());
    backend.mode.store(0, Ordering::SeqCst);
    assert!(supervisor.shutdown().await.is_ok());
}
struct StalledPublisher;
#[async_trait]
impl IntegrationEventPublisher for StalledPublisher {
    async fn publish(&self, _: IntegrationEvent) {
        std::future::pending::<()>().await
    }
}
#[tokio::test]
async fn stalled_observer_cannot_stall_cleanup_or_pruning() {
    let supervisor = SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .integration_publisher(Arc::new(StalledPublisher))
        .build();
    let scope = supervisor.create_scope();
    for _ in 0..100 {
        supervisor
            .spawn(scope, ProcessSpec::new("owned"))
            .await
            .unwrap();
    }
    let report = tokio::time::timeout(
        Duration::from_secs(1),
        supervisor.terminate_scope(scope, opts()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(report.all_verified());
    assert!(supervisor.processes(scope).is_none());
}

#[tokio::test]
async fn hundreds_of_simultaneous_exits_do_not_outlive_registered_waiters() {
    let supervisor = sup(Faults::new(0));
    let scope = supervisor.create_scope();
    for _ in 0..600 {
        supervisor
            .spawn(scope, ProcessSpec::new("owned"))
            .await
            .unwrap();
    }
    let report = tokio::time::timeout(
        Duration::from_secs(5),
        supervisor.terminate_scope(scope, opts()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.outcomes.len(), 600);
    assert!(report.all_verified());
}

#[tokio::test]
async fn worker_panics_become_typed_unverified_or_sampling_errors() {
    let backend = Faults::new(SAMPLE_PANIC);
    let supervisor = sup(backend.clone());
    let scope = supervisor.create_scope();
    let pid = supervisor
        .spawn(scope, ProcessSpec::new("owned"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(supervisor.stats(pid).await, Err(StatsError::Backend(_))) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    backend.mode.store(0, Ordering::SeqCst);
    assert!(supervisor
        .terminate_scope(scope, opts())
        .await
        .unwrap()
        .all_verified());
    let supervisor = sup(Faults::new(WAIT_PANIC));
    let scope = supervisor.create_scope();
    let pid = supervisor
        .spawn(scope, ProcessSpec::new("owned"))
        .await
        .unwrap();
    assert!(matches!(
        supervisor.wait(pid).await.unwrap().outcome,
        TerminationOutcome::CleanupUnverified(UnverifiedReason::ReapFailed)
    ));
    assert!(!supervisor
        .terminate_scope(scope, opts())
        .await
        .unwrap()
        .all_verified());
}

#[tokio::test]
async fn scope_guard_preserves_unverified_per_process_report() {
    let supervisor = sup(Faults::new(WAIT_FAIL));
    let result = supervisor
        .with_scope_options(
            vec![ProcessSpec::new("owned")],
            opts(),
            |scope| async move {
                scope.wait(scope.processes()[0]).await.unwrap();
                42
            },
        )
        .await;
    assert_eq!(result.result.unwrap(), 42);
    let report = result.termination.unwrap();
    assert_eq!(report.outcomes.len(), 1);
    assert!(!report.all_verified());
}
