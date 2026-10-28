use crate::output::{OutputChunk, ProcessOutput};
use crate::ports::Clock;
use crate::{
    ObservedScope, ObservedSpawn, ProcessSupervisor, ScopeCreationError, ScopeTerminationReport,
    SpawnError, SpawnObservation, TerminateError, TerminateOptions,
};
use futures_util::FutureExt;
use shepherd_domain::{OutputMode, ProcessExit, ProcessId, ProcessScopeId, ProcessSpec};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Bounds for one background command. The caller owns admission across tasks.
#[derive(Clone, Debug)]
pub struct BackgroundTaskOptions {
    /// Maximum time from admission through spawning and execution.
    pub timeout: Duration,
    /// Combined stdout/stderr tail bytes retained by this task.
    pub output_bytes: NonZeroUsize,
    /// How its process scope is stopped, including after natural root exit.
    pub termination: TerminateOptions,
}

impl Default for BackgroundTaskOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(3600),
            output_bytes: NonZeroUsize::new(65_536).expect("nonzero output bound"),
            termination: TerminateOptions::default(),
        }
    }
}

/// Why the task stopped waiting. Physical exit and cleanup remain separate facts.
#[derive(Debug)]
pub enum BackgroundTaskCause {
    /// The process reported its exit.
    Exited,
    /// An explicit task cancellation was observed.
    Cancelled,
    /// Its execution budget elapsed.
    TimedOut,
    /// Its original spawn failed.
    SpawnFailed(SpawnError),
    /// An injected effect panicked. Cleanup is still attempted.
    WorkerFailed,
}

/// A repeatable, bounded output tail. Bytes retain their original stream order.
#[derive(Clone, Debug, Default)]
pub struct BackgroundTaskOutput {
    /// Latest bytes, bounded across both streams by `output_bytes`.
    pub tail: Vec<OutputChunk>,
    /// Bytes produced but omitted from this tail, including earlier reads.
    pub omitted_bytes: u64,
    /// Whether stdout reached EOF; false is not proof of complete output.
    pub stdout_closed: bool,
    /// Whether stderr reached EOF.
    pub stderr_closed: bool,
    /// Pipe errors reported by the capture adapter.
    pub errors: Vec<String>,
}

/// Settled execution, cleanup and output of a background task.
#[derive(Debug)]
pub struct BackgroundTaskResult {
    /// Scope identity minted when the task was admitted.
    pub scope: ProcessScopeId,
    /// Original process identity, if attachment occurred.
    pub process: Option<ProcessId>,
    /// OS PID for diagnostics; never authority for cancellation.
    pub os_pid: Option<u32>,
    /// The first reason the worker stopped waiting.
    pub cause: BackgroundTaskCause,
    /// The observed physical exit, including forced/unverified outcomes.
    pub exit: Option<ProcessExit>,
    /// Original scope cleanup. An `Ok` report can still be unverified.
    pub cleanup: Result<ScopeTerminationReport, TerminateError>,
    /// Last bounded output observation after cleanup and the pipe-close budget.
    pub output: BackgroundTaskOutput,
}

impl BackgroundTaskResult {
    /// True only when the owning scope confirmed cleanup.
    #[must_use]
    pub fn cleanup_verified(&self) -> bool {
        self.cleanup
            .as_ref()
            .is_ok_and(ScopeTerminationReport::all_verified)
    }
}

/// An observation of one task; completed data is shared, not copied on a watch.
#[derive(Clone, Debug)]
pub enum BackgroundTaskStatus {
    /// Accepted; spawning has not yet reported attachment.
    Starting,
    /// Attached to the task's original scope.
    Running {
        /// Shepherd's process identity.
        process: ProcessId,
        /// Diagnostic OS PID.
        os_pid: Option<u32>,
    },
    /// Execution and cleanup have settled, possibly unsuccessfully.
    Finished(Arc<BackgroundTaskResult>),
}

struct Output {
    source: Option<ProcessOutput>,
    drained: u64,
    current: BackgroundTaskOutput,
}

impl Output {
    fn read(&mut self) -> BackgroundTaskOutput {
        if let Some(source) = &self.source {
            let snapshot = source.read();
            self.drained = self.drained.saturating_add(
                snapshot
                    .chunks
                    .iter()
                    .map(|chunk| chunk.bytes.len() as u64)
                    .sum::<u64>(),
            );
            let retained = snapshot
                .tail
                .iter()
                .map(|chunk| chunk.bytes.len() as u64)
                .sum::<u64>();
            self.current = BackgroundTaskOutput {
                tail: snapshot.tail,
                omitted_bytes: self
                    .drained
                    .saturating_add(snapshot.dropped_bytes)
                    .saturating_sub(retained),
                stdout_closed: snapshot.stdout_closed,
                stderr_closed: snapshot.stderr_closed,
                errors: snapshot.errors,
            };
        }
        self.current.clone()
    }
}

struct Shared {
    // The worker publishes lifecycle; readers clone the current watch value.
    state: watch::Sender<BackgroundTaskStatus>,
    // The first cancellation is monotonic; observer drop is not cancellation.
    stop: watch::Sender<bool>,
    // Worker attachment/finalization and observer reads serialize this capture.
    output: Mutex<Output>,
}

/// Observation and cancellation of a command owned by its supervisor.
///
/// Dropping all handles or cancelling `wait` leaves the worker running. Retain
/// the supervisor and call its `shutdown` when the host stops. Runtime teardown
/// only has the existing observed-scope hard-kill backstop; it cannot promise
/// asynchronous reap. Results are retained only by these handles, not a registry.
#[derive(Clone)]
pub struct BackgroundTask {
    admitted_at: Instant,
    scope: ProcessScopeId,
    shared: Arc<Shared>,
}

impl BackgroundTask {
    /// Admit a scope and start its worker without waiting for the command.
    ///
    /// Requires a running Tokio runtime. Capture replaces `spec.output` with a
    /// bounded queue and tail. The injected clock owns timeout and pipe-close
    /// waits. The caller bounds the number of admitted tasks. Scope admission
    /// fails once supervisor shutdown starts; no worker is then created.
    pub fn start(
        supervisor: ProcessSupervisor,
        spec: ProcessSpec,
        options: BackgroundTaskOptions,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ScopeCreationError> {
        let admitted_at = clock.now();
        let owner = supervisor.try_create_observed_scope()?;
        let scope = owner.id();
        let (state, _) = watch::channel(BackgroundTaskStatus::Starting);
        let (stop, stopped) = watch::channel(false);
        let shared = Arc::new(Shared {
            state,
            stop,
            output: Mutex::new(Output {
                source: None,
                drained: 0,
                current: BackgroundTaskOutput::default(),
            }),
        });
        let task = Self {
            admitted_at,
            scope,
            shared,
        };
        let worker = task.clone();
        tokio::spawn(async move {
            worker
                .run(supervisor, owner, spec, options, clock, stopped)
                .await
        });
        Ok(task)
    }

    /// The task's existing process scope identity.
    #[must_use]
    pub fn id(&self) -> ProcessScopeId {
        self.scope
    }

    /// Observe status without waiting or changing its lifecycle.
    #[must_use]
    pub fn status(&self) -> BackgroundTaskStatus {
        self.shared.state.borrow().clone()
    }

    /// Subscribe to lifecycle changes. Register before reading to avoid a gap.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<BackgroundTaskStatus> {
        self.shared.state.subscribe()
    }

    /// Latest combined output tail. Repeated reads do not remove retained bytes.
    #[must_use]
    pub fn output(&self) -> BackgroundTaskOutput {
        self.shared
            .output
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .read()
    }

    /// Request cancellation. It does not claim that processes have stopped;
    /// `wait` returns their cleanup evidence. Repeated requests are harmless.
    pub fn cancel(&self) {
        self.shared.stop.send_replace(true);
    }

    /// Await the same settled result on every call. Cancelling this await only
    /// drops its observer and does not cancel the process.
    pub async fn wait(&self) -> Arc<BackgroundTaskResult> {
        let mut state = self.subscribe();
        loop {
            if let BackgroundTaskStatus::Finished(result) = state.borrow_and_update().clone() {
                return result;
            }
            // This handle holds the sender, so it cannot close while we wait.
            state
                .changed()
                .await
                .expect("background task retains its state");
        }
    }

    fn attached(&self, supervisor: &ProcessSupervisor, observation: &mut SpawnObservation) {
        self.shared
            .output
            .lock()
            .expect("background output mutex")
            .source = observation.output.take();
        self.shared
            .state
            .send_replace(BackgroundTaskStatus::Running {
                process: observation.pid,
                os_pid: supervisor.os_pid(observation.pid),
            });
    }

    async fn run(
        self,
        supervisor: ProcessSupervisor,
        owner: ObservedScope,
        spec: ProcessSpec,
        options: BackgroundTaskOptions,
        clock: Arc<dyn Clock>,
        mut stopped: watch::Receiver<bool>,
    ) {
        let mut spawn: Option<ObservedSpawn> = None;
        let mut observation: Option<SpawnObservation> = None;
        let mut exit = None;
        let mut os_pid = None;
        let activity = AssertUnwindSafe(async {
            if *stopped.borrow() { return BackgroundTaskCause::Cancelled; }
            let remaining = options.timeout.saturating_sub(clock.now().saturating_duration_since(self.admitted_at));
            if remaining.is_zero() { return BackgroundTaskCause::TimedOut; }
            let timeout = clock.sleep(remaining);
            tokio::pin!(timeout);
            let bytes = options.output_bytes.get();
            spawn = Some(supervisor.spawn_observed(self.scope, spec.output(OutputMode::Capture {
                buffer_bytes: bytes, tail_bytes: bytes,
            })));
            let spawned = spawn.as_mut().expect("spawn was started");
            let mut attached = tokio::select! {
                result = spawned.complete() => match result {
                    Ok(observed) => observed,
                    Err(error) => return BackgroundTaskCause::SpawnFailed(error),
                },
                _ = stopped.changed() => return BackgroundTaskCause::Cancelled,
                () = &mut timeout => return BackgroundTaskCause::TimedOut,
            };
            os_pid = supervisor.os_pid(attached.pid);
            self.attached(&supervisor, &mut attached);
            observation = Some(attached);
            let observed = observation.as_mut().expect("process attached");
            tokio::select! {
                biased;
                ended = &mut observed.exit => { exit = Some(ended); BackgroundTaskCause::Exited },
                _ = async { if !*stopped.borrow() { let _ = stopped.changed().await; } } => BackgroundTaskCause::Cancelled,
                () = &mut timeout => BackgroundTaskCause::TimedOut,
            }
        }).catch_unwind().await;
        let cause = activity.unwrap_or(BackgroundTaskCause::WorkerFailed);
        // Scope cleanup stays outside the entire effect-bearing activity. It
        // joins any in-flight spawn even when cancellation won before admission.
        let cleanup = AssertUnwindSafe(owner.finish(options.termination))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                Err(TerminateError::Signal(
                    "background cleanup worker panicked".into(),
                ))
            });
        let finalized = AssertUnwindSafe(async {
            if observation.is_none() {
                if let Some(mut recovered) = spawn.as_mut().and_then(ObservedSpawn::take_admission)
                {
                    os_pid = supervisor.os_pid(recovered.pid);
                    self.attached(&supervisor, &mut recovered);
                    observation = Some(recovered);
                }
            }
            if exit.is_none() {
                if let Some(observed) = observation.as_mut() {
                    exit = (&mut observed.exit).now_or_never();
                }
            }
            // Pipe readers can publish their final bytes just after cleanup.
            // A deadline stays visible as open pipes rather than invented EOF.
            let close = clock.sleep(Duration::from_secs(1));
            tokio::pin!(close);
            let mut output = self.output();
            if self
                .shared
                .output
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .source
                .is_some()
            {
                while !output.stdout_closed || !output.stderr_closed {
                    tokio::select! {
                        () = &mut close => break,
                        () = clock.sleep(Duration::from_millis(5)) => output = self.output(),
                    }
                }
            }
            output
        })
        .catch_unwind()
        .await;
        // Publication has a retained fallback even when the capture or clock
        // panics during finalization. Surviving observers must still settle.
        let mut retained = self
            .shared
            .output
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // An observer may have read newer bytes while finalization waited on
        // the pipe-close budget. Freeze the current observation under this lock.
        let mut output = retained.current.clone();
        if finalized.is_err() {
            output
                .errors
                .push("background output collection failed".into());
        }
        retained.current = output.clone();
        let capture = retained.source.take();
        drop(retained);
        // Destruction of a substituted capture is also outside publication's
        // critical path; its panic cannot strand task observers.
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| drop(capture)));
        self.shared
            .state
            .send_replace(BackgroundTaskStatus::Finished(Arc::new(
                BackgroundTaskResult {
                    scope: self.scope,
                    process: observation.as_ref().map(|observed| observed.pid),
                    os_pid,
                    cause,
                    exit,
                    cleanup,
                    output,
                },
            )));
    }
}
