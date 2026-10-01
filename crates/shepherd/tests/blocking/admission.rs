//! Run's observation must survive a caller that is not polled after attachment.
use super::*;
use crate::{NullBackend, OutputMode, OutputStream, ProcessBackend, Spawned};
use async_trait::async_trait;
use shepherd_app::output::OutputSink;
use shepherd_domain::{RawExit, RawStats, Signal};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

struct AdmissionBackend {
    inner: NullBackend,
    first: AtomicBool,
    release: tokio::sync::Notify,
}
#[async_trait]
impl ProcessBackend for AdmissionBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        if self.first.swap(false, Ordering::SeqCst) {
            self.release.notified().await;
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
    async fn cleanup_scope(&self, scope: ProcessScopeId) -> Result<(), TerminateError> {
        self.inner.cleanup_scope(scope).await
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        self.inner.sample(target).await
    }
    fn output(&self, _: &Spawned) -> Option<ProcessOutput> {
        Some(ProcessOutput(Arc::new(Capture)))
    }
    fn capabilities(&self) -> crate::Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}
struct Capture;
impl OutputSink for Capture {
    fn push(&self, _: OutputStream, _: &[u8]) {}
    fn close(&self, _: OutputStream, _: Option<String>) {}
    fn read(&self) -> OutputSnapshot {
        OutputSnapshot {
            chunks: vec![crate::OutputChunk {
                stream: OutputStream::Stdout,
                bytes: b"original admission capture".to_vec(),
            }],
            tail: vec![],
            dropped_bytes: 0,
            stdout_closed: true,
            stderr_closed: true,
            errors: vec![],
        }
    }
}

#[test]
fn run_keeps_admission_when_caller_poll_is_delayed() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let backend = Arc::new(AdmissionBackend {
        inner: NullBackend::new(),
        first: AtomicBool::new(true),
        release: tokio::sync::Notify::new(),
    });
    let inner = SupervisorBuilder::new().backend(backend.clone()).build();
    let supervisor = BlockingSupervisor::compose(inner, Driver::Handle(runtime.handle().clone()));
    let options = RunOptions::with_deadline(Duration::from_secs(5));
    let spec = ProcessSpec::new("exit-immediately").output(OutputMode::Capture {
        buffer_bytes: 64,
        tail_bytes: 16,
    });
    let mut run = std::pin::pin!(supervisor.run_async(spec, options));
    runtime.block_on(async {
        // Poll the actual run once. Its first owned spawn waits on the injected
        // backend gate, so the caller is definitely pending before attachment.
        std::future::poll_fn(|cx| {
            assert!(run.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        }).await;
        backend.release.notify_one();
        let first = ProcessId::new(1);
        tokio::time::timeout(Duration::from_secs(2), async {
            while supervisor.inner.os_pid(first).is_none() { tokio::task::yield_now().await; }
        }).await.expect("first attachment");
        assert!(supervisor.inner.wait(first).await.unwrap().outcome.is_verified());
        // Keep the run future unpolled while the real worker and monitor finish
        // and both 256-entry histories turn over on the same supervisor.
        for _ in 0..257 {
            let scope = supervisor.inner.create_scope();
            let pid = supervisor.inner.spawn(scope, ProcessSpec::new("exit-immediately")).await.unwrap();
            assert!(supervisor.inner.wait(pid).await.unwrap().outcome.is_verified());
            assert!(supervisor.inner.terminate_scope(scope, TerminateOptions::default()).await.unwrap().all_verified());
        }
        assert!(supervisor.inner.take_output(first).is_none(), "unclaimed capture history was not turned over");
        assert!(matches!(supervisor.inner.wait(first).await, Err(WaitError::UnknownProcess(id)) if id == first));
        let outcome = tokio::time::timeout(Duration::from_secs(2), &mut run).await
            .expect("run resumed").expect("admitted exit remains owned by run");
        assert!(outcome.all_verified());
        let output = outcome.output().expect("admitted capture remains owned by run");
        assert_eq!(output.chunks[0].bytes, b"original admission capture");
        supervisor.inner.shutdown().await.unwrap();
    });
}
