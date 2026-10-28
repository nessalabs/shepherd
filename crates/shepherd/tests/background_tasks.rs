//! Background ownership survives observers, but never upgrades cleanup evidence.
#[cfg(unix)]
use std::num::NonZeroUsize;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use shepherd::{
    BackgroundTask, BackgroundTaskCause, BackgroundTaskOptions, BackgroundTaskStatus, Capabilities,
    NullBackend, ProcessBackend, ProcessOutput, ProcessSpec, SpawnError, Spawned, StatsError,
    SupervisorBuilder, SystemClock, TerminateError, WaitError,
};
use shepherd_app::{
    output::{OutputChunk, OutputSink, OutputSnapshot, OutputStream},
    ports::Clock,
};
use shepherd_domain::{ProcessScopeId, RawExit, RawStats, Signal};

fn options() -> BackgroundTaskOptions {
    BackgroundTaskOptions {
        timeout: Duration::from_secs(30),
        ..Default::default()
    }
}
fn start(supervisor: &shepherd::ProcessSupervisor, program: &str) -> BackgroundTask {
    BackgroundTask::start(
        supervisor.clone(),
        ProcessSpec::new(program),
        options(),
        Arc::new(SystemClock),
    )
    .unwrap()
}
async fn attached(task: &BackgroundTask) -> shepherd::ProcessId {
    let mut state = task.subscribe();
    loop {
        match state.borrow_and_update().clone() {
            BackgroundTaskStatus::Running { process, .. } => return process,
            BackgroundTaskStatus::Finished(_) => {
                panic!("task finished before attachment observation")
            }
            BackgroundTaskStatus::Starting => (),
        }
        state.changed().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn start_returns_while_command_runs_and_cancelled_wait_does_not_cancel() {
    let supervisor = SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .build();
    let task = start(&supervisor, "respect-graceful");
    assert!(matches!(task.status(), BackgroundTaskStatus::Starting));
    let process = attached(&task).await;
    assert!(tokio::time::timeout(Duration::from_millis(1), task.wait())
        .await
        .is_err());
    assert!(matches!(
        task.status(),
        BackgroundTaskStatus::Running { .. }
    ));
    task.cancel();
    let result = task.wait().await;
    assert!(matches!(result.cause, BackgroundTaskCause::Cancelled));
    assert_eq!(result.process, Some(process));
    assert!(result.cleanup_verified());
    assert!(result.exit.as_ref().unwrap().outcome.is_verified());
    assert!(Arc::ptr_eq(&result, &task.wait().await));
    supervisor.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn dropping_observers_does_not_stop_task() {
    let supervisor = SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .build();
    let task = start(&supervisor, "respect-graceful");
    let process = attached(&task).await;
    drop(task);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), supervisor.wait(process))
            .await
            .is_err()
    );
    supervisor.shutdown().await.unwrap();
    assert!(supervisor
        .wait(process)
        .await
        .unwrap()
        .outcome
        .is_verified());
}

#[tokio::test(start_paused = true)]
async fn fast_exit_keeps_its_outcome() {
    let supervisor = SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .build();
    let task = start(&supervisor, "exit-immediately");
    let result = task.wait().await;
    assert!(matches!(result.cause, BackgroundTaskCause::Exited));
    assert_eq!(result.exit.as_ref().unwrap().code, Some(0));
    assert!(result.cleanup_verified());
    task.cancel();
    assert!(Arc::ptr_eq(&result, &task.wait().await));
    supervisor.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancellation_before_spawn_runs_no_command() {
    let backend = Arc::new(InjectedBackend::new(Mode::Ordinary));
    let supervisor = SupervisorBuilder::new().backend(backend.clone()).build();
    let task = start(&supervisor, "exit-immediately");
    task.cancel();
    let result = task.wait().await;
    assert!(matches!(result.cause, BackgroundTaskCause::Cancelled));
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 0);
    assert!(result.process.is_none());
    assert!(result.cleanup_verified());
    supervisor.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn timeout_keeps_cause_and_cleanup() {
    let supervisor = SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .build();
    let task = start(&supervisor, "ignore-graceful");
    let result = task.wait().await;
    assert!(matches!(result.cause, BackgroundTaskCause::TimedOut));
    assert_eq!(result.exit.as_ref().unwrap().signal, Some(Signal::Kill));
    assert!(result.cleanup_verified());
    supervisor.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_settles_background_tasks_and_closes_admission() {
    let supervisor = SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .build();
    let task = start(&supervisor, "respect-graceful");
    attached(&task).await;
    supervisor.shutdown().await.unwrap();
    assert!(task.wait().await.cleanup_verified());
    assert!(BackgroundTask::start(
        supervisor.clone(),
        ProcessSpec::new("exit-immediately"),
        options(),
        Arc::new(SystemClock)
    )
    .is_err());
}

#[derive(Clone, Copy)]
enum Mode {
    Ordinary,
    SpawnFailure,
    CleanupFailure,
    ReadPanic,
    GatedSpawn,
    LateOutput,
}
struct InjectedBackend {
    inner: NullBackend,
    mode: Mode,
    spawns: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl InjectedBackend {
    fn new(mode: Mode) -> Self {
        Self {
            inner: NullBackend::new(),
            mode,
            spawns: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
}
#[async_trait]
impl ProcessBackend for InjectedBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        self.spawns.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        match self.mode {
            Mode::SpawnFailure => return Err(SpawnError::Os("injected spawn failure".into())),
            Mode::GatedSpawn => self.release.notified().await,
            _ => (),
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
        if matches!(self.mode, Mode::CleanupFailure) {
            return Err(TerminateError::Signal("injected uncertainty".into()));
        }
        self.inner.cleanup_scope(scope).await
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
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
    fn output(&self, _: &Spawned) -> Option<ProcessOutput> {
        match self.mode {
            Mode::ReadPanic => Some(ProcessOutput(Arc::new(PanicOutput))),
            Mode::LateOutput => Some(ProcessOutput(Arc::new(LateOutput(AtomicUsize::new(0))))),
            _ => None,
        }
    }
}
struct PanicOutput;
impl OutputSink for PanicOutput {
    fn push(&self, _: OutputStream, _: &[u8]) {}
    fn close(&self, _: OutputStream, _: Option<String>) {}
    fn read(&self) -> OutputSnapshot {
        panic!("injected output read panic");
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_spawn_keeps_ownership() {
    let backend = Arc::new(InjectedBackend::new(Mode::GatedSpawn));
    let supervisor = SupervisorBuilder::new().backend(backend.clone()).build();
    let task = start(&supervisor, "respect-graceful");
    backend.entered.notified().await;
    task.cancel();
    assert!(tokio::time::timeout(Duration::from_millis(1), task.wait())
        .await
        .is_err());
    backend.release.notify_one();
    let result = task.wait().await;
    assert!(matches!(result.cause, BackgroundTaskCause::Cancelled));
    assert!(result.cleanup_verified());
    assert!(result.process.is_some());
    assert!(result.exit.as_ref().unwrap().outcome.is_verified());
    supervisor.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn spawn_failure_still_cleans_scope() {
    let backend = Arc::new(InjectedBackend::new(Mode::SpawnFailure));
    let supervisor = SupervisorBuilder::new().backend(backend).build();
    let result = start(&supervisor, "exit-immediately").wait().await;
    assert!(matches!(result.cause, BackgroundTaskCause::SpawnFailed(_)));
    assert!(result.process.is_none());
    assert!(result.cleanup_verified());
    supervisor.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn uncertain_cleanup_is_not_success() {
    let backend = Arc::new(InjectedBackend::new(Mode::CleanupFailure));
    let supervisor = SupervisorBuilder::new().backend(backend).build();
    let result = start(&supervisor, "exit-immediately").wait().await;
    assert!(matches!(result.cause, BackgroundTaskCause::Exited));
    assert_eq!(result.exit.as_ref().unwrap().code, Some(0));
    assert!(!result.cleanup_verified());
}

#[tokio::test(start_paused = true)]
async fn final_output_panic_preserves_prior_exit_and_cleanup_and_settles_wait() {
    let backend = Arc::new(InjectedBackend::new(Mode::ReadPanic));
    let supervisor = SupervisorBuilder::new().backend(backend).build();
    let task = start(&supervisor, "exit-immediately");
    let result = tokio::time::timeout(Duration::from_secs(2), task.wait())
        .await
        .unwrap();
    assert!(matches!(result.cause, BackgroundTaskCause::Exited));
    assert_eq!(result.exit.as_ref().unwrap().code, Some(0));
    assert!(result.cleanup_verified());
    assert!(!result.output.errors.is_empty());
    assert_eq!(task.output().errors, result.output.errors);
    supervisor.shutdown().await.unwrap();
}

struct PanicClock;
#[async_trait]
impl Clock for PanicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    async fn sleep(&self, _: Duration) {
        panic!("injected clock panic");
    }
}
#[tokio::test(start_paused = true)]
async fn worker_failure_retains_cleanup() {
    let supervisor = SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .build();
    let task = BackgroundTask::start(
        supervisor.clone(),
        ProcessSpec::new("respect-graceful"),
        options(),
        Arc::new(PanicClock),
    )
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), task.wait())
        .await
        .unwrap();
    assert!(matches!(result.cause, BackgroundTaskCause::WorkerFailed));
    assert!(result.cleanup_verified());
    supervisor.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn output_tail_is_bounded_across_reads() {
    let supervisor = SupervisorBuilder::new().build();
    let task = BackgroundTask::start(
        supervisor.clone(),
        ProcessSpec::new("/bin/sh")
            .arg("-c")
            .arg("printf 12345678901234567890; printf abcdefghij >&2"),
        BackgroundTaskOptions {
            output_bytes: NonZeroUsize::new(8).unwrap(),
            ..options()
        },
        Arc::new(SystemClock),
    )
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), task.wait())
        .await
        .unwrap();
    assert!(result.cleanup_verified());
    assert!(result.output.stdout_closed && result.output.stderr_closed);
    assert_eq!(
        result
            .output
            .tail
            .iter()
            .map(|chunk| chunk.bytes.len())
            .sum::<usize>(),
        8
    );
    assert_eq!(result.output.omitted_bytes, 22);
    for _ in 0..3 {
        assert_eq!(task.output().tail, result.output.tail);
        assert_eq!(task.output().omitted_bytes, 22);
    }
    supervisor.shutdown().await.unwrap();
}

struct LateOutput(AtomicUsize);
impl OutputSink for LateOutput {
    fn push(&self, _: OutputStream, _: &[u8]) {}
    fn close(&self, _: OutputStream, _: Option<String>) {}
    fn read(&self) -> OutputSnapshot {
        let read = self.0.fetch_add(1, Ordering::SeqCst);
        let late = read > 0;
        OutputSnapshot {
            chunks: if read > 1 {
                vec![]
            } else {
                vec![OutputChunk {
                    stream: OutputStream::Stdout,
                    bytes: if late {
                        b"late".to_vec()
                    } else {
                        b"early".to_vec()
                    },
                }]
            },
            tail: vec![OutputChunk {
                stream: OutputStream::Stdout,
                bytes: if late {
                    b"earlylate".to_vec()
                } else {
                    b"early".to_vec()
                },
            }],
            dropped_bytes: 0,
            stdout_closed: late,
            stderr_closed: late,
            errors: vec![],
        }
    }
}
struct ObservingClock {
    task: Mutex<Option<BackgroundTask>>,
    seen: Mutex<Vec<u8>>,
}
#[async_trait]
impl Clock for ObservingClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    async fn sleep(&self, duration: Duration) {
        if duration == Duration::from_secs(1) {
            let output = self.task.lock().unwrap().as_ref().unwrap().output();
            *self.seen.lock().unwrap() = output
                .tail
                .into_iter()
                .flat_map(|chunk| chunk.bytes)
                .collect();
        } else {
            std::future::pending::<()>().await;
        }
    }
}
#[tokio::test]
async fn final_publication_keeps_newer_observer_output() {
    let supervisor = SupervisorBuilder::new()
        .backend(Arc::new(InjectedBackend::new(Mode::LateOutput)))
        .build();
    let clock = Arc::new(ObservingClock {
        task: Mutex::new(None),
        seen: Mutex::new(vec![]),
    });
    let task = BackgroundTask::start(
        supervisor.clone(),
        ProcessSpec::new("exit-immediately"),
        options(),
        clock.clone(),
    )
    .unwrap();
    *clock.task.lock().unwrap() = Some(task.clone());
    let result = tokio::time::timeout(Duration::from_secs(5), task.wait())
        .await
        .unwrap();
    let bytes: Vec<_> = result
        .output
        .tail
        .iter()
        .flat_map(|chunk| chunk.bytes.clone())
        .collect();
    assert_eq!(bytes, b"earlylate");
    assert_eq!(bytes, *clock.seen.lock().unwrap());
    assert!(result.output.stdout_closed && result.output.stderr_closed);
    assert_eq!(result.output.omitted_bytes, 0);
    clock.task.lock().unwrap().take();
    supervisor.shutdown().await.unwrap();
}

struct AdmissionClock {
    origin: Instant,
    elapsed_ms: AtomicUsize,
}
#[async_trait]
impl Clock for AdmissionClock {
    fn now(&self) -> Instant {
        self.origin + Duration::from_millis(self.elapsed_ms.load(Ordering::SeqCst) as u64)
    }
    async fn sleep(&self, _: Duration) {
        std::future::pending::<()>().await;
    }
}
#[tokio::test]
async fn expired_admission_budget_does_not_spawn_after_scheduler_delay() {
    let backend = Arc::new(InjectedBackend::new(Mode::Ordinary));
    let supervisor = SupervisorBuilder::new().backend(backend.clone()).build();
    let clock = Arc::new(AdmissionClock {
        origin: Instant::now(),
        elapsed_ms: AtomicUsize::new(0),
    });
    let task = BackgroundTask::start(
        supervisor.clone(),
        ProcessSpec::new("exit-immediately"),
        BackgroundTaskOptions {
            timeout: Duration::from_millis(10),
            ..options()
        },
        clock.clone(),
    )
    .unwrap();
    clock.elapsed_ms.store(50, Ordering::SeqCst);
    let result = tokio::time::timeout(Duration::from_secs(5), task.wait())
        .await
        .unwrap();
    assert!(matches!(result.cause, BackgroundTaskCause::TimedOut));
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 0);
    assert!(result.cleanup_verified());
    supervisor.shutdown().await.unwrap();
}
