//! Portable retention regression with fake capture observers and no OS processes.
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use shepherd::{
    Capabilities, NullBackend, ProcessBackend, ProcessOutput, ProcessSpec, SpawnError, Spawned,
    StatsError, SupervisorBuilder, TerminateError, WaitError,
};
use shepherd_app::output::{OutputSink, OutputSnapshot, OutputStream};
use shepherd_domain::{OutputMode, ProcessScopeId, RawExit, RawStats, Signal};

fn capture_spec(program: &str) -> ProcessSpec {
    ProcessSpec::new(program).output(OutputMode::Capture {
        buffer_bytes: 32,
        tail_bytes: 8,
    })
}

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
    failed_reaps: Mutex<HashSet<u32>>,
    captures: Mutex<HashSet<u32>>,
}
#[async_trait]
impl ProcessBackend for CapturingBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        let spawned = self.inner.spawn(scope, spec).await?;
        if matches!(spec.output, OutputMode::Capture { .. }) {
            self.captures.lock().unwrap().insert(spawned.os.pid);
        }
        if spec.program == "wait-fails" {
            self.failed_reaps.lock().unwrap().insert(spawned.os.pid);
        }
        Ok(spawned)
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
        if self.failed_reaps.lock().unwrap().contains(&target.os.pid) {
            return Err(WaitError::Backend("injected reap failure".into()));
        }
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        self.inner.sample(target).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
    fn output(&self, spawned: &Spawned) -> Option<ProcessOutput> {
        if !self.captures.lock().unwrap().remove(&spawned.os.pid) {
            return None;
        }
        self.retained.fetch_add(1, Ordering::SeqCst);
        Some(ProcessOutput(Arc::new(Capture(self.retained.clone()))))
    }
}

#[tokio::test(start_paused = true)]
async fn unclaimed_capture_preserves_live_unverified_and_transferred_observers() {
    let backend = Arc::new(CapturingBackend::default());
    let sup = SupervisorBuilder::new().backend(backend.clone()).build();
    let scope = sup.create_scope();
    let live = sup
        .spawn(scope, capture_spec("respect-graceful"))
        .await
        .unwrap();
    let unverified = sup.spawn(scope, capture_spec("wait-fails")).await.unwrap();
    assert!(!sup.wait(unverified).await.unwrap().outcome.is_verified());
    let transferred = sup
        .spawn(scope, capture_spec("exit-immediately"))
        .await
        .unwrap();
    let observer = sup.take_output(transferred).unwrap();
    sup.wait(transferred).await.unwrap();
    let mut completed = Vec::new();
    for _ in 0..320 {
        let pid = sup
            .spawn(scope, capture_spec("exit-immediately"))
            .await
            .unwrap();
        sup.wait(pid).await.unwrap();
        completed.push(pid);
    }
    // Eviction precedes the exit notification, so wait completion proves the bound.
    assert_eq!(backend.retained.load(Ordering::SeqCst), 256 + 3);
    for pid in &completed[..64] {
        assert!(sup.take_output(*pid).is_none());
    }
    for pid in &completed[64..] {
        assert!(sup.take_output(*pid).is_some());
    }
    assert!(sup.take_output(live).is_some());
    assert_eq!(backend.retained.load(Ordering::SeqCst), 2);
    assert!(
        sup.take_output(unverified).is_some(),
        "failed reap capture must remain available"
    );
    assert_eq!(backend.retained.load(Ordering::SeqCst), 1);
    assert!(observer.read().errors.is_empty());
    drop(observer);
    assert_eq!(backend.retained.load(Ordering::SeqCst), 0);
    // The injected reap failure cannot become verified cleanup; owner Drop supplies
    // the synchronous kill backstop for the fake process still tracked by the backend.
    drop(sup);
}

struct GatedPublisher {
    gate: tokio::sync::Semaphore,
    calls: AtomicUsize,
}
#[async_trait]
impl shepherd::IntegrationEventPublisher for GatedPublisher {
    async fn publish(&self, _: shepherd_domain::IntegrationEvent) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _permit = self.gate.acquire().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_publication_cannot_bypass_completed_capture_bound() {
    let backend = Arc::new(CapturingBackend::default());
    let publisher = Arc::new(GatedPublisher {
        gate: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
    });
    let sup = SupervisorBuilder::new()
        .backend(backend.clone())
        .integration_publisher(publisher.clone())
        .build();
    let scope = sup.create_scope();
    let mut first = None;
    for _ in 0..320 {
        let pid = sup
            .spawn(scope, capture_spec("exit-immediately"))
            .await
            .unwrap();
        first.get_or_insert(pid);
        sup.wait(pid).await.unwrap();
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(publisher.calls.load(Ordering::SeqCst) > 0);
    // Keep the publisher blocked while checking actual observer destruction.
    assert_eq!(backend.retained.load(Ordering::SeqCst), 256);
    assert!(sup.take_output(first.unwrap()).is_none());
    publisher.gate.add_permits(1);
    sup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn discard_and_claimed_output_do_not_consume_unclaimed_capture_history() {
    let backend = Arc::new(CapturingBackend::default());
    let sup = SupervisorBuilder::new().backend(backend.clone()).build();
    let scope = sup.create_scope();
    let first = sup
        .spawn(scope, capture_spec("exit-immediately"))
        .await
        .unwrap();
    sup.wait(first).await.unwrap();
    for _ in 0..650 {
        let discard = sup
            .spawn(scope, ProcessSpec::new("exit-immediately"))
            .await
            .unwrap();
        sup.wait(discard).await.unwrap();
    }
    assert_eq!(
        backend.retained.load(Ordering::SeqCst),
        1,
        "discard traffic evicted an unclaimed capture"
    );
    for _ in 0..320 {
        let before = sup
            .spawn(scope, capture_spec("exit-immediately"))
            .await
            .unwrap();
        drop(sup.take_output(before).unwrap());
        sup.wait(before).await.unwrap();
        let after = sup
            .spawn(scope, capture_spec("exit-immediately"))
            .await
            .unwrap();
        sup.wait(after).await.unwrap();
        drop(sup.take_output(after).unwrap());
    }
    assert_eq!(
        backend.retained.load(Ordering::SeqCst),
        1,
        "claimed captures consumed unclaimed history"
    );
    // Fill the remaining quota with actual retained captures. The oldest survives
    // until the 257th unclaimed observer, independently of all the traffic above.
    for _ in 0..255 {
        let pid = sup
            .spawn(scope, capture_spec("exit-immediately"))
            .await
            .unwrap();
        sup.wait(pid).await.unwrap();
    }
    assert_eq!(backend.retained.load(Ordering::SeqCst), 256);
    let next = sup
        .spawn(scope, capture_spec("exit-immediately"))
        .await
        .unwrap();
    sup.wait(next).await.unwrap();
    assert_eq!(backend.retained.load(Ordering::SeqCst), 256);
    assert!(sup.take_output(first).is_none());
    sup.shutdown().await.unwrap();
}
