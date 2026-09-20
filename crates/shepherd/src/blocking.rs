//! Synchronous Shepherd entry points for callers that do not own an async context.
//!
//! Tokio panics if `block_on` is called from a thread that is already driving a
//! runtime. This module never does that. An owned multi-thread runtime, or a
//! caller-supplied handle, drives every future. When the caller is already
//! inside Tokio:
//!
//! * same runtime (multi-thread): `block_in_place` + `Handle::block_on`
//! * a different runtime: a scoped helper thread calls `block_on` outside Tokio
//!
//! A current-thread handle used *from that same runtime* cannot block safely
//! and will panic inside Tokio. Use [`BlockingSupervisor::new`] (owned
//! multi-thread runtime) or call from a thread that is not driving that
//! current-thread runtime. [`BlockingSupervisor::from_runtime`] is the supported
//! way to wrap a current-thread runtime from ordinary synchronous code.
//!
//! Dropping an owned runtime from inside another Tokio context calls
//! `shutdown_background` so Tokio does not panic. Call `shutdown()` first when
//! you need verified cleanup.
//!
//! The async `ProcessSupervisor` surface is unchanged. Domain code is not
//! involved.

use std::future::Future;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::{Handle, Runtime};

use crate::{
    ObservationError, OutputSnapshot, ProcessExit, ProcessId, ProcessOutput, ProcessScopeId,
    ProcessSpec, ProcessStats, ProcessSupervisor, ScopeCreationError, ScopeTerminationReport,
    ScopeUsage, ShutdownError, ShutdownReport, SpawnError, StatsError, SupervisorBuilder,
    TerminateError, TerminateOptions, WaitError, WithScopeResult,
};

/// Owns or borrows a Tokio runtime and exposes the supervisor synchronously.
///
/// Cheaply cloneable: clones share the supervisor and the runtime. The last
/// user-facing supervisor handle still issues the documented Drop hard-kill.
#[derive(Clone)]
pub struct BlockingSupervisor {
    inner: ProcessSupervisor,
    driver: Driver,
}

#[derive(Clone)]
enum Driver {
    Owned(Arc<OwnedRuntime>),
    Handle(Handle),
}

/// Tokio panics if a `Runtime` is dropped on a worker (`blocking_pool.shutdown`
/// waits). `shutdown_background` is the supported way to drop a runtime from
/// inside another runtime; a normal drop is used when the caller is synchronous.
struct OwnedRuntime {
    runtime: Option<Runtime>,
}

impl OwnedRuntime {
    fn new(runtime: Runtime) -> Self {
        Self {
            runtime: Some(runtime),
        }
    }

    fn get(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("Shepherd blocking runtime already shut down")
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        if Handle::try_current().is_ok() {
            runtime.shutdown_background();
        }
    }
}

impl Driver {
    fn handle(&self) -> Handle {
        match self {
            Self::Owned(runtime) => runtime.get().handle().clone(),
            Self::Handle(handle) => handle.clone(),
        }
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        match self {
            Self::Owned(runtime) => runtime.get().block_on(future),
            Self::Handle(handle) => handle.block_on(future),
        }
    }
}

impl std::fmt::Debug for BlockingSupervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingSupervisor")
            .field("supervisor", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Default for BlockingSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockingSupervisor {
    /// Builds a supervisor with default adapters and an owned two-worker runtime.
    ///
    /// # Panics
    /// Panics if the owned Tokio runtime cannot be constructed.
    #[must_use]
    pub fn new() -> Self {
        Self::builder()
            .build()
            .expect("failed to construct the Shepherd blocking runtime")
    }

    /// Starts a builder for adapters and the owned runtime.
    #[must_use]
    pub fn builder() -> BlockingSupervisorBuilder {
        BlockingSupervisorBuilder::new()
    }

    /// Wraps an existing runtime handle and default adapters.
    ///
    /// The handle's runtime must outlive this supervisor. Prefer a multi-thread
    /// runtime. A current-thread handle only works when this thread is not
    /// already driving that runtime; otherwise Tokio panics. See the module
    /// docs.
    #[must_use]
    pub fn from_handle(handle: Handle) -> Self {
        Self::from_handle_and_builder(handle, SupervisorBuilder::new())
    }

    /// Wraps an existing runtime handle and a configured async supervisor builder.
    #[must_use]
    pub fn from_handle_and_builder(handle: Handle, builder: SupervisorBuilder) -> Self {
        Self {
            inner: builder.build(),
            driver: Driver::Handle(handle),
        }
    }

    /// Takes ownership of a Tokio runtime. `Runtime::block_on` is used when the
    /// caller is not inside Tokio, so a current-thread runtime is valid here.
    #[must_use]
    pub fn from_runtime(runtime: Runtime) -> Self {
        Self::from_runtime_and_builder(runtime, SupervisorBuilder::new())
    }

    /// Takes ownership of a Tokio runtime and a configured async supervisor builder.
    #[must_use]
    pub fn from_runtime_and_builder(runtime: Runtime, builder: SupervisorBuilder) -> Self {
        Self {
            inner: builder.build(),
            driver: Driver::Owned(Arc::new(OwnedRuntime::new(runtime))),
        }
    }

    /// The underlying async supervisor. Do not detach a clone past this wrapper's
    /// runtime lifetime if you still intend to drive futures on that runtime.
    #[must_use]
    pub fn supervisor(&self) -> &ProcessSupervisor {
        &self.inner
    }

    fn drive<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        drive_with(&self.driver, future)
    }

    /// Creates a new, open scope.
    ///
    /// # Panics
    /// Panics if shutdown has started. Use [`Self::try_create_scope`] when
    /// rejection must be handled.
    #[must_use]
    pub fn create_scope(&self) -> ProcessScopeId {
        self.inner.create_scope()
    }

    /// Creates a scope only while this supervisor accepts new work.
    ///
    /// # Errors
    /// Returns [`ScopeCreationError::SupervisorClosed`] once shutdown starts.
    pub fn try_create_scope(&self) -> Result<ProcessScopeId, ScopeCreationError> {
        self.inner.try_create_scope()
    }

    /// Runtime capabilities of the selected backend.
    #[must_use]
    pub fn capabilities(&self) -> crate::Capabilities {
        self.inner.capabilities()
    }

    /// Live process ids in a scope, or `None` if the scope is unknown.
    #[must_use]
    pub fn processes(&self, scope: ProcessScopeId) -> Option<Vec<ProcessId>> {
        self.inner.processes(scope)
    }

    /// OS PID of a retained managed process, for read-only observation.
    #[must_use]
    pub fn os_pid(&self, pid: ProcessId) -> Option<u32> {
        self.inner.os_pid(pid)
    }

    /// Transfers the capture observer at most once per process.
    #[must_use]
    pub fn take_output(&self, pid: ProcessId) -> Option<ProcessOutput> {
        self.inner.take_output(pid)
    }

    /// Spawns a process into `scope`.
    ///
    /// # Errors
    /// Returns [`SpawnError`] if the scope is unknown/closed or the OS spawn fails.
    pub fn spawn(&self, scope: ProcessScopeId, spec: ProcessSpec) -> Result<ProcessId, SpawnError> {
        self.drive(self.inner.spawn(scope, spec))
    }

    /// Waits until `pid` is reaped.
    ///
    /// # Errors
    /// Returns [`WaitError::UnknownProcess`] if the process was never owned.
    pub fn wait(&self, pid: ProcessId) -> Result<ProcessExit, WaitError> {
        self.drive(self.inner.wait(pid))
    }

    /// Terminates one process with verified two-phase teardown.
    ///
    /// # Errors
    /// Returns [`TerminateError::UnknownProcess`] if the process is not owned.
    pub fn terminate(
        &self,
        pid: ProcessId,
        opts: TerminateOptions,
    ) -> Result<ProcessExit, TerminateError> {
        self.drive(self.inner.terminate(pid, opts))
    }

    /// Terminates every process in `scope` and confirms reap.
    ///
    /// # Errors
    /// Returns [`TerminateError::UnknownScope`] if the scope is unknown.
    pub fn terminate_scope(
        &self,
        scope: ProcessScopeId,
        opts: TerminateOptions,
    ) -> Result<ScopeTerminationReport, TerminateError> {
        self.drive(self.inner.terminate_scope(scope, opts))
    }

    /// Observes a `with_scope` cleanup report, including after a body panic.
    ///
    /// # Errors
    /// Returns [`TerminateError::UnknownScope`] if the scope was not a scoped block.
    pub fn wait_scope_cleanup(
        &self,
        scope: ProcessScopeId,
    ) -> Result<ScopeTerminationReport, TerminateError> {
        self.drive(self.inner.wait_scope_cleanup(scope))
    }

    /// Two-phase shutdown of every remaining scope.
    ///
    /// # Errors
    /// Returns [`ShutdownError::Unverified`] when cleanup could not be confirmed.
    pub fn shutdown(&self) -> Result<ShutdownReport, ShutdownError> {
        self.drive(self.inner.shutdown())
    }

    /// Last cached interval sample for a live process.
    ///
    /// # Errors
    /// Returns [`StatsError`] when the process is unknown or no sample exists yet.
    pub fn stats(&self, pid: ProcessId) -> Result<ProcessStats, StatsError> {
        self.drive(self.inner.stats(pid))
    }

    /// Read-only scope usage. Not a cleanup verdict.
    ///
    /// # Errors
    /// Returns [`ObservationError`] when the scope is unknown or the backend
    /// cannot account for it.
    pub fn scope_usage(&self, scope: ProcessScopeId) -> Result<ScopeUsage, ObservationError> {
        self.drive(self.inner.scope_usage(scope))
    }

    /// Runs a synchronous closure in a fresh scope and always waits for cleanup.
    ///
    /// The body runs on the calling thread (or a helper thread when already
    /// inside a different runtime). A panic in the body still completes
    /// verified cleanup, then the panic is resumed.
    ///
    /// # Panics
    /// Panics if shutdown has started before the scope is admitted, or if `body`
    /// panics (after cleanup).
    pub fn with_scope<T, F>(&self, specs: Vec<ProcessSpec>, body: F) -> WithScopeResult<T>
    where
        F: FnOnce(BlockingScopedProcesses) -> T + Send,
        T: Send,
    {
        self.with_scope_options(specs, TerminateOptions::default(), body)
    }

    /// [`Self::with_scope`] with explicit termination options.
    ///
    /// # Panics
    /// Panics if shutdown has started before the scope is admitted, or if `body`
    /// panics (after cleanup).
    pub fn with_scope_options<T, F>(
        &self,
        specs: Vec<ProcessSpec>,
        opts: TerminateOptions,
        body: F,
    ) -> WithScopeResult<T>
    where
        F: FnOnce(BlockingScopedProcesses) -> T + Send,
        T: Send,
    {
        enum Outcome<T> {
            Value(T),
            Panic(Box<dyn std::any::Any + Send>),
        }

        let driver = self.driver.clone();
        let result = self.drive(async {
            self.inner
                .with_scope_options(specs, opts, |scoped| async move {
                    let handle = BlockingScopedProcesses {
                        inner: scoped,
                        driver,
                    };
                    match catch_unwind(AssertUnwindSafe(|| body(handle))) {
                        Ok(value) => Outcome::Value(value),
                        Err(panic) => Outcome::Panic(panic),
                    }
                })
                .await
        });
        match result.result {
            Ok(Outcome::Value(value)) => WithScopeResult {
                scope: result.scope,
                result: Ok(value),
                termination: result.termination,
            },
            Ok(Outcome::Panic(panic)) => resume_unwind(panic),
            Err(error) => WithScopeResult {
                scope: result.scope,
                result: Err(error),
                termination: result.termination,
            },
        }
    }

    /// Runs one process with a deadline that covers spawn, wait, and cleanup.
    ///
    /// On expiry the whole scope is terminated (group / job kill) and reap is
    /// confirmed. Output is drained when the spec requested capture.
    ///
    /// # Errors
    /// Returns spawn, wait, scope-creation, or terminate failures. Deadline
    /// expiry is [`BlockingRun::TimedOut`], not an error.
    pub fn run(
        &self,
        spec: ProcessSpec,
        deadline: Duration,
    ) -> Result<BlockingRun, BlockingRunError> {
        self.run_with_options(spec, RunOptions::with_deadline(deadline))
    }

    /// [`Self::run`] with explicit grace, force, and output-drain budgets.
    ///
    /// # Errors
    /// Returns spawn, wait, scope-creation, or terminate failures.
    pub fn run_with_options(
        &self,
        spec: ProcessSpec,
        options: RunOptions,
    ) -> Result<BlockingRun, BlockingRunError> {
        self.drive(self.run_async(spec, options))
    }

    async fn run_async(
        &self,
        spec: ProcessSpec,
        options: RunOptions,
    ) -> Result<BlockingRun, BlockingRunError> {
        let started = Instant::now();
        let scope = self.inner.try_create_scope()?;
        let mut admitted = None;
        let attempt = async {
            let pid = self.inner.spawn(scope, spec).await?;
            admitted = Some(pid);
            let exit = self.inner.wait(pid).await?;
            Ok::<_, BlockingRunError>((pid, exit))
        };
        let outcome = tokio::time::timeout(options.deadline, attempt).await;
        let leftover = options.deadline.saturating_sub(started.elapsed());
        match outcome {
            Ok(Ok((pid, exit))) => {
                let output = drain_output(self.inner.take_output(pid), options.output_drain).await;
                let termination = self
                    .inner
                    .terminate_scope(scope, bound_terminate(options.terminate, leftover))
                    .await?;
                Ok(BlockingRun::Completed {
                    exit,
                    output,
                    termination,
                })
            }
            Ok(Err(error)) => {
                let _ = self
                    .inner
                    .terminate_scope(scope, bound_terminate(options.terminate, leftover))
                    .await;
                Err(error)
            }
            Err(_) => {
                let pid = admitted.or_else(|| {
                    self.inner
                        .processes(scope)
                        .and_then(|ids| ids.into_iter().next())
                });
                // Cleanup is not starved by a fully consumed deadline: the host
                // still needs a confirmed group kill and reap.
                let termination = self.inner.terminate_scope(scope, options.terminate).await?;
                let output = match pid {
                    Some(pid) => {
                        drain_output(self.inner.take_output(pid), options.output_drain).await
                    }
                    None => None,
                };
                Ok(BlockingRun::TimedOut {
                    output,
                    termination,
                })
            }
        }
    }
}

fn drive_with<F>(driver: &Driver, future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    match Handle::try_current() {
        Err(_) => driver.block_on(future),
        Ok(current) if current.id() == driver.handle().id() => {
            tokio::task::block_in_place(|| driver.handle().block_on(future))
        }
        Ok(_) => std::thread::scope(|scope| {
            scope
                .spawn(|| driver.block_on(future))
                .join()
                .unwrap_or_else(|payload| resume_unwind(payload))
        }),
    }
}

fn bound_terminate(mut opts: TerminateOptions, leftover: Duration) -> TerminateOptions {
    if leftover.is_zero() {
        return opts;
    }
    opts.grace = crate::GracePeriod::new(opts.grace.as_duration().min(leftover));
    opts.force_timeout = Some(opts.force_timeout.unwrap_or(leftover).min(leftover));
    opts
}

async fn drain_output(output: Option<ProcessOutput>, budget: Duration) -> Option<OutputSnapshot> {
    let output = output?;
    let deadline = Instant::now() + budget;
    let mut chunks = Vec::new();
    let mut snapshot = output.read();
    chunks.append(&mut snapshot.chunks);
    while !(snapshot.stdout_closed && snapshot.stderr_closed) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
        snapshot = output.read();
        chunks.append(&mut snapshot.chunks);
    }
    snapshot.chunks = chunks;
    Some(snapshot)
}

/// Builds a [`BlockingSupervisor`] with default or custom adapters.
pub struct BlockingSupervisorBuilder {
    inner: SupervisorBuilder,
    worker_threads: usize,
}

impl std::fmt::Debug for BlockingSupervisorBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingSupervisorBuilder")
            .field("worker_threads", &self.worker_threads)
            .finish_non_exhaustive()
    }
}

impl Default for BlockingSupervisorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockingSupervisorBuilder {
    /// Starts a builder with default adapters and two worker threads.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: SupervisorBuilder::new(),
            worker_threads: 2,
        }
    }

    /// Overrides the process backend (e.g. a `NullBackend` for tests).
    #[must_use]
    pub fn backend(mut self, backend: Arc<dyn crate::ProcessBackend>) -> Self {
        self.inner = self.inner.backend(backend);
        self
    }

    /// Sets the outbound integration-event publisher.
    #[must_use]
    pub fn integration_publisher(
        mut self,
        publisher: Arc<dyn crate::IntegrationEventPublisher>,
    ) -> Self {
        self.inner = self.inner.integration_publisher(publisher);
        self
    }

    /// Sets the shared sampling interval.
    #[must_use]
    pub fn stats_interval(mut self, interval: Duration) -> Self {
        self.inner = self.inner.stats_interval(interval);
        self
    }

    /// Sets the owned multi-thread worker count. Values below 2 become 2 so a
    /// `with_scope` body can `block_in_place` while monitors still run.
    #[must_use]
    pub fn worker_threads(mut self, threads: usize) -> Self {
        self.worker_threads = threads.max(2);
        self
    }

    /// Builds the supervisor and its owned runtime.
    ///
    /// # Errors
    /// Returns the Tokio runtime construction error.
    pub fn build(self) -> Result<BlockingSupervisor, std::io::Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.worker_threads)
            .enable_all()
            .thread_name("shepherd-blocking")
            .build()?;
        Ok(BlockingSupervisor {
            inner: self.inner.build(),
            driver: Driver::Owned(Arc::new(OwnedRuntime::new(runtime))),
        })
    }
}

/// Access to a blocking `with_scope` block. Does not extend supervisor ownership.
pub struct BlockingScopedProcesses {
    inner: crate::ScopedProcesses,
    driver: Driver,
}

impl BlockingScopedProcesses {
    /// The scope id for this block.
    #[must_use]
    pub fn id(&self) -> ProcessScopeId {
        self.inner.id()
    }

    /// Snapshot of process ids admitted with the block, plus later scoped spawns
    /// are tracked separately for wait/output authorization.
    #[must_use]
    pub fn processes(&self) -> &[ProcessId] {
        self.inner.processes()
    }

    /// Spawns into this block's scope.
    ///
    /// # Errors
    /// Returns [`SpawnError`] if the scope is closed or the OS spawn fails.
    pub fn spawn(&self, spec: ProcessSpec) -> Result<ProcessId, SpawnError> {
        drive_with(&self.driver, self.inner.spawn(spec))
    }

    /// Waits for a process owned by this block.
    ///
    /// # Errors
    /// Returns [`WaitError::UnknownProcess`] if `pid` is not authorized here.
    pub fn wait(&self, pid: ProcessId) -> Result<ProcessExit, WaitError> {
        drive_with(&self.driver, self.inner.wait(pid))
    }

    /// Transfers the capture observer for a process owned by this block.
    #[must_use]
    pub fn take_output(&self, pid: ProcessId) -> Option<ProcessOutput> {
        self.inner.take_output(pid)
    }
}

/// Options for [`BlockingSupervisor::run_with_options`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunOptions {
    /// Wall-clock bound covering spawn *and* wait, not only output reads.
    pub deadline: Duration,
    /// Grace and force budgets used for verified cleanup.
    pub terminate: TerminateOptions,
    /// How long to keep draining capture pipes after the process is reaped.
    pub output_drain: Duration,
}

impl RunOptions {
    /// A deadline with short, test-friendly cleanup defaults.
    #[must_use]
    pub fn with_deadline(deadline: Duration) -> Self {
        Self {
            deadline,
            terminate: TerminateOptions {
                grace: crate::GracePeriod::new(Duration::from_millis(100)),
                force_timeout: Some(Duration::from_secs(5)),
            },
            output_drain: Duration::from_secs(1),
        }
    }
}

/// Result of a deadline-bounded run.
#[derive(Debug, Clone)]
pub enum BlockingRun {
    /// The process exited before the deadline; the scope was then cleaned up.
    Completed {
        /// Verified process exit.
        exit: ProcessExit,
        /// Combined capture snapshot when the spec requested output.
        output: Option<OutputSnapshot>,
        /// Scope cleanup after the natural exit.
        termination: ScopeTerminationReport,
    },
    /// The deadline expired; Shepherd issued a confirmed group/job kill.
    TimedOut {
        /// Capture snapshot if anything was admitted and captured.
        output: Option<OutputSnapshot>,
        /// Verified (or explicitly unverified) scope cleanup.
        termination: ScopeTerminationReport,
    },
}

impl BlockingRun {
    /// Whether the attempt ended because the deadline expired.
    #[must_use]
    pub const fn timed_out(&self) -> bool {
        matches!(self, Self::TimedOut { .. })
    }

    /// Whether every process in the cleanup report reached a verified outcome.
    #[must_use]
    pub fn all_verified(&self) -> bool {
        self.termination().all_verified()
    }

    /// The scope cleanup report.
    #[must_use]
    pub const fn termination(&self) -> &ScopeTerminationReport {
        match self {
            Self::Completed { termination, .. } | Self::TimedOut { termination, .. } => termination,
        }
    }

    /// Captured output, if any.
    #[must_use]
    pub const fn output(&self) -> Option<&OutputSnapshot> {
        match self {
            Self::Completed { output, .. } | Self::TimedOut { output, .. } => output.as_ref(),
        }
    }
}

/// Failure of a deadline-bounded run other than timeout.
#[derive(Debug, thiserror::Error)]
pub enum BlockingRunError {
    /// Supervisor no longer admits scopes.
    #[error(transparent)]
    Scope(#[from] ScopeCreationError),
    /// Spawn failed; the scope is still cleaned up before this is returned.
    #[error(transparent)]
    Spawn(#[from] SpawnError),
    /// Waiting for the process failed.
    #[error(transparent)]
    Wait(#[from] WaitError),
    /// Scope cleanup could not be completed.
    #[error(transparent)]
    Terminate(#[from] TerminateError),
}
