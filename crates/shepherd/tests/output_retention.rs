//! Portable retention regression with fake capture observers and no OS processes.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use shepherd::{
    Capabilities, NullBackend, ProcessBackend, ProcessOutput, ProcessSpec, SpawnError, Spawned,
    StatsError, SupervisorBuilder, TerminateError, WaitError,
};
use shepherd_app::output::{OutputSink, OutputSnapshot, OutputStream};
use shepherd_domain::{ProcessScopeId, RawExit, RawStats, Signal};

struct Capture(Arc<AtomicUsize>);
impl Drop for Capture {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl OutputSink for Capture {
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
struct CapturingBackend {
    inner: NullBackend,
    retained: Arc<AtomicUsize>,
}
#[async_trait]
impl ProcessBackend for CapturingBackend {
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
    fn output(&self, _: &Spawned) -> Option<ProcessOutput> {
        self.retained.fetch_add(1, Ordering::SeqCst);
        Some(ProcessOutput(Arc::new(Capture(self.retained.clone()))))
    }
}

#[tokio::test(start_paused = true)]
async fn unclaimed_capture_is_bounded_without_evicting_live_or_transferred_observers() {
    let backend = Arc::new(CapturingBackend::default());
    let sup = SupervisorBuilder::new().backend(backend.clone()).build();
    let scope = sup.create_scope();
    let live = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .await
        .unwrap();
    let transferred = sup
        .spawn(scope, ProcessSpec::new("exit-immediately"))
        .await
        .unwrap();
    let observer = sup.take_output(transferred).unwrap();
    sup.wait(transferred).await.unwrap();
    let mut completed = Vec::new();
    for _ in 0..320 {
        let pid = sup
            .spawn(scope, ProcessSpec::new("exit-immediately"))
            .await
            .unwrap();
        sup.wait(pid).await.unwrap();
        completed.push(pid);
    }
    // Exit notification precedes eviction; let the last monitor finish its bookkeeping.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(backend.retained.load(Ordering::SeqCst), 256 + 2);
    for pid in &completed[..64] {
        assert!(sup.take_output(*pid).is_none());
    }
    for pid in &completed[64..] {
        assert!(sup.take_output(*pid).is_some());
    }
    assert!(sup.take_output(live).is_some());
    assert_eq!(backend.retained.load(Ordering::SeqCst), 1);
    assert!(observer.read().errors.is_empty());
    drop(observer);
    assert_eq!(backend.retained.load(Ordering::SeqCst), 0);
    sup.shutdown().await.unwrap();
}
