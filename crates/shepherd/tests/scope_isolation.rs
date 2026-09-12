//! Deterministic regressions for shutdown admission and scoped observation boundaries.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use shepherd::{
    Capabilities, NullBackend, ProcessBackend, ProcessOutput, ProcessSpec, SpawnError, Spawned,
    StatsError, SupervisorBuilder, TerminateError, TerminationOutcome, WaitError,
};
use shepherd_app::output::{OutputSink, OutputSnapshot, OutputStream};
use shepherd_domain::{ProcessScopeId, RawExit, RawStats, Signal};
use tokio::sync::Notify;

struct EmptyOutput;
impl OutputSink for EmptyOutput {
    fn push(&self, _: OutputStream, _: &[u8]) {}
    fn close(&self, _: OutputStream, _: Option<String>) {}
    fn read(&self) -> OutputSnapshot {
        OutputSnapshot {
            chunks: vec![],
            tail: vec![],
            dropped_bytes: 0,
            stdout_closed: true,
            stderr_closed: true,
            errors: vec![],
        }
    }
}

#[derive(Default)]
struct GatedBackend {
    inner: NullBackend,
    admitted: Notify,
    release: Notify,
    reaped: Notify,
    global_kills: AtomicUsize,
    descendant: Mutex<Option<Spawned>>,
}
#[async_trait]
impl ProcessBackend for GatedBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        if spec.program == "delayed" {
            self.admitted.notify_one();
            self.release.notified().await;
            // Simulate a descendant that is only visible to containment cleanup.
            *self.descendant.lock().unwrap() = Some(
                self.inner
                    .spawn(scope, &ProcessSpec::new("ignore-graceful"))
                    .await?,
            );
        }
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
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        let exit = self.inner.wait(target).await;
        self.reaped.notify_one();
        exit
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        self.inner.sample(target).await
    }
    fn output(&self, _: &Spawned) -> Option<ProcessOutput> {
        Some(ProcessOutput(Arc::new(EmptyOutput)))
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.global_kills.fetch_add(1, Ordering::SeqCst);
        self.inner.hard_kill_all();
    }
}

#[tokio::test(start_paused = true)]
async fn admitted_spawn_during_shutdown_preserves_sibling_grace_and_sweeps_descendants() {
    let backend = Arc::new(GatedBackend::default());
    let supervisor = SupervisorBuilder::new().backend(backend.clone()).build();
    let first = supervisor.create_scope();
    let sibling = supervisor.create_scope();
    let sibling_pid = supervisor
        .spawn(sibling, ProcessSpec::new("respect-graceful"))
        .await
        .unwrap();
    let worker = supervisor.clone();
    let spawn = tokio::spawn(async move { worker.spawn(first, ProcessSpec::new("delayed")).await });
    backend.admitted.notified().await;
    let mut shutdown = Box::pin(supervisor.shutdown());
    // Poll shutdown through setting its intent and waiting for the admitted spawn's lock.
    tokio::select! { biased;
        result = &mut shutdown => panic!("shutdown bypassed admitted spawn: {result:?}"),
        () = std::future::ready(()) => {}
    }
    backend.release.notify_one();
    let pid = spawn.await.unwrap().unwrap();
    assert!(shutdown
        .await
        .unwrap()
        .scopes
        .iter()
        .all(|r| r.all_verified()));
    assert_eq!(backend.global_kills.load(Ordering::SeqCst), 0);
    assert_eq!(
        supervisor.wait(sibling_pid).await.unwrap().outcome,
        TerminationOutcome::GracefulSuccess
    );
    assert_eq!(
        supervisor.wait(pid).await.unwrap().outcome,
        TerminationOutcome::GracefulSuccess
    );
    let descendant = backend.descendant.lock().unwrap().unwrap();
    assert_eq!(
        backend.inner.wait(&descendant).await.unwrap().signal,
        Some(Signal::Kill)
    );
}

#[tokio::test(start_paused = true)]
async fn admitted_spawn_after_last_owner_drop_still_kills_and_reaps() {
    let backend = Arc::new(GatedBackend::default());
    let supervisor = SupervisorBuilder::new().backend(backend.clone()).build();
    let scope = supervisor.create_scope();
    let worker = supervisor.clone();
    let spawn = tokio::spawn(async move { worker.spawn(scope, ProcessSpec::new("delayed")).await });
    backend.admitted.notified().await;
    spawn.abort();
    assert!(spawn.await.unwrap_err().is_cancelled());
    drop(supervisor);
    assert_eq!(backend.global_kills.load(Ordering::SeqCst), 1);
    backend.release.notify_one();
    backend.reaped.notified().await;
    assert_eq!(backend.global_kills.load(Ordering::SeqCst), 2);
    let descendant = backend.descendant.lock().unwrap().unwrap();
    assert_eq!(
        backend.inner.wait(&descendant).await.unwrap().signal,
        Some(Signal::Kill)
    );
}

#[tokio::test(start_paused = true)]
async fn scoped_observers_reject_live_and_completed_siblings_but_keep_dynamic_outputs() {
    let supervisor = SupervisorBuilder::new()
        .backend(Arc::new(GatedBackend::default()))
        .build();
    let sibling = supervisor.create_scope();
    let live = supervisor
        .spawn(sibling, ProcessSpec::new("respect-graceful"))
        .await
        .unwrap();
    let completed = supervisor
        .spawn(sibling, ProcessSpec::new("exit-immediately"))
        .await
        .unwrap();
    supervisor.wait(completed).await.unwrap();
    let result = supervisor.with_scope(vec![ProcessSpec::new("exit-immediately")], |scope| async move {
        for pid in [live, completed] {
            assert!(scope.take_output(pid).is_none());
            assert!(matches!(scope.wait(pid).await, Err(WaitError::UnknownProcess(id)) if id == pid));
        }
        let initial = scope.processes()[0];
        scope.wait(initial).await.unwrap();
        assert!(scope.take_output(initial).is_some());
        let dynamic = scope.spawn(ProcessSpec::new("exit-immediately")).await.unwrap();
        scope.wait(dynamic).await.unwrap();
        assert!(scope.take_output(dynamic).is_some());
        assert!(scope.take_output(dynamic).is_none());
    }).await;
    assert!(result.termination.unwrap().all_verified());
    assert!(supervisor.take_output(live).is_some());
    assert!(supervisor.take_output(completed).is_some());
    supervisor.shutdown().await.unwrap();
}
