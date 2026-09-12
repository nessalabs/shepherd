//! The `ProcessSupervisor` application service.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::future::{AbortHandle, Abortable};
use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::FutureExt;

use shepherd_domain::{
    ProcessExit, ProcessId, ProcessScopeId, ProcessSpec, ProcessStats, Signal, TerminationOutcome,
    UnverifiedReason,
};

use crate::dispatch::{
    EventDispatcher, IntegrationTranslator, RegistryPruneHandler, SharedRegistry,
    WaitNotifierHandler,
};
use crate::error::{
    ScopeCreationError, ShutdownError, SpawnError, StatsError, TerminateError, WaitError,
};
use crate::ports::{
    Clock, EventHandler, IntegrationEventPublisher, ProcessBackend, Spawned, TerminateOptions,
    Waiters,
};
use crate::registry::ScopeRegistry;

/// The aggregated result of terminating a scope.
#[derive(Debug, Clone)]
pub struct ScopeTerminationReport {
    /// The scope that was terminated.
    pub scope: ProcessScopeId,
    /// Per-process outcomes.
    pub outcomes: Vec<(ProcessId, TerminationOutcome)>,
}

impl ScopeTerminationReport {
    /// Whether every process reached a verified terminal outcome.
    #[must_use]
    pub fn all_verified(&self) -> bool {
        self.outcomes.iter().all(|(_, o)| o.is_verified())
    }
}

/// The aggregated result of a supervisor shutdown.
#[derive(Debug, Clone)]
pub struct ShutdownReport {
    /// Per-scope termination reports.
    pub scopes: Vec<ScopeTerminationReport>,
}

/// Coordinates spawning, monitoring, termination, waiting, and reaping across scopes.
///
/// Cheaply cloneable (shares one inner state). Never a global singleton — construct and own
/// it explicitly.
///
/// Cloning a supervisor keeps the same cleanup responsibility: only when the **last
/// user-facing handle** is dropped (and `shutdown` was not awaited) does the RAII guard
/// issue a synchronous hard-kill. Monitor tasks hold [`Inner`] only, so they cannot keep
/// that guard alive and silently orphan children.
pub struct ProcessSupervisor {
    inner: Arc<Inner>,
    cleanup: Option<Arc<CleanupGuard>>,
}

impl Clone for ProcessSupervisor {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            cleanup: self.cleanup.clone(),
        }
    }
}

/// Drops with the last user-facing [`ProcessSupervisor`] handle and hard-kills remaining
/// work. Shared with [`Inner::shutting_down`] so an explicit shutdown is not warned as a leak.
struct CleanupGuard {
    backend: Arc<dyn ProcessBackend>,
    shutting_down: Arc<AtomicBool>,
    owners_dropped: Arc<AtomicBool>,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if !self.shutting_down.load(Ordering::SeqCst) {
            tracing::warn!(
                "ProcessSupervisor dropped without shutdown; issuing unverified hard-kill"
            );
        }
        self.owners_dropped.store(true, Ordering::SeqCst);
        self.shutting_down.store(true, Ordering::SeqCst);
        self.backend.hard_kill_all();
    }
}

type ScopeOperation = tokio::sync::Mutex<Option<ScopeTerminationReport>>;

struct Inner {
    registry: SharedRegistry,
    backend: Arc<dyn ProcessBackend>,
    clock: Arc<dyn Clock>,
    waiters: Arc<dyn Waiters>,
    dispatcher: EventDispatcher,
    spawn_times: Mutex<HashMap<ProcessId, Instant>>,
    shutting_down: Arc<AtomicBool>,
    owners_dropped: Arc<AtomicBool>,
    scope_operations: Mutex<HashMap<ProcessScopeId, Arc<ScopeOperation>>>,
    samples: Mutex<HashMap<ProcessId, Result<ProcessStats, StatsError>>>,
    sampler_started: AtomicBool,
    outputs: Mutex<HashMap<ProcessId, crate::output::ProcessOutput>>,
    scope_results: Mutex<HashMap<ProcessScopeId, ScopeCleanupSender>>,
    completed_scope_results: Mutex<VecDeque<ProcessScopeId>>,
    completed: Mutex<VecDeque<ProcessId>>,
    sampler_task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    stats_interval: Duration,
    reports: Mutex<HashMap<ProcessScopeId, ScopeTerminationReport>>,
    pending_outcomes: Mutex<HashMap<ProcessScopeId, HashMap<ProcessId, TerminationOutcome>>>,
    completed_scopes: Mutex<VecDeque<ProcessScopeId>>,
    shutdown_serial: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for ProcessSupervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessSupervisor")
            .field("dispatcher", &self.inner.dispatcher)
            .finish_non_exhaustive()
    }
}

impl ProcessSupervisor {
    /// Constructs a supervisor from its injected ports.
    #[must_use]
    pub fn new(
        backend: Arc<dyn ProcessBackend>,
        clock: Arc<dyn Clock>,
        waiters: Arc<dyn Waiters>,
        publisher: Arc<dyn IntegrationEventPublisher>,
    ) -> Self {
        Self::with_stats_interval(backend, clock, waiters, publisher, Duration::from_secs(1))
    }

    /// Constructs a supervisor with one shared interval sampler, started on first spawn.
    /// Zero intervals are clamped to one millisecond.
    #[must_use]
    pub fn with_stats_interval(
        backend: Arc<dyn ProcessBackend>,
        clock: Arc<dyn Clock>,
        waiters: Arc<dyn Waiters>,
        publisher: Arc<dyn IntegrationEventPublisher>,
        stats_interval: Duration,
    ) -> Self {
        let registry: SharedRegistry = Arc::new(Mutex::new(ScopeRegistry::new()));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let owners_dropped = Arc::new(AtomicBool::new(false));
        let handlers: Vec<Arc<dyn EventHandler>> = vec![
            Arc::new(WaitNotifierHandler::new(waiters.clone())),
            Arc::new(RegistryPruneHandler::new(registry.clone())),
            Arc::new(IntegrationTranslator::new(publisher)),
        ];
        Self {
            inner: Arc::new(Inner {
                registry,
                backend: Arc::clone(&backend),
                clock,
                waiters,
                dispatcher: EventDispatcher::new(handlers),
                spawn_times: Mutex::new(HashMap::new()),
                scope_operations: Mutex::new(HashMap::new()),
                samples: Mutex::new(HashMap::new()),
                sampler_started: AtomicBool::new(false),
                outputs: Mutex::new(HashMap::new()),
                scope_results: Mutex::new(HashMap::new()),
                completed_scope_results: Mutex::new(VecDeque::new()),
                completed: Mutex::new(VecDeque::new()),
                sampler_task: tokio::sync::Mutex::new(None),
                stats_interval: stats_interval.max(Duration::from_millis(1)),
                reports: Mutex::new(HashMap::new()),
                pending_outcomes: Mutex::new(HashMap::new()),
                completed_scopes: Mutex::new(VecDeque::new()),
                shutdown_serial: tokio::sync::Mutex::new(()),
                shutting_down: Arc::clone(&shutting_down),
                owners_dropped: Arc::clone(&owners_dropped),
            }),
            cleanup: Some(Arc::new(CleanupGuard {
                backend,
                shutting_down,
                owners_dropped,
            })),
        }
    }

    // Infrastructure tasks must never extend the last user-facing handle's lifetime.
    fn worker(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cleanup: None,
        }
    }

    /// Creates a new, open scope and returns its id.
    ///
    /// # Panics
    /// Panics if shutdown has started. Use [`Self::try_create_scope`] when scope
    /// creation can race with shutdown or when rejection must be handled.
    pub fn create_scope(&self) -> ProcessScopeId {
        self.create_scope_before_publish(|_| {})
    }

    /// Creates a scope only while this supervisor accepts new work.
    ///
    /// # Errors
    /// Returns [`ScopeCreationError::SupervisorClosed`] once shutdown starts,
    /// without allocating an id, registry entry, or operation lock.
    pub fn try_create_scope(&self) -> Result<ProcessScopeId, ScopeCreationError> {
        self.try_create_scope_before_publish(|_| {})
    }

    // The callback lets the concurrency regression pause at the publication boundary.
    fn create_scope_before_publish(
        &self,
        before_publish: impl FnOnce(ProcessScopeId),
    ) -> ProcessScopeId {
        // The fallible helper has released its registry guard before this can panic.
        self.try_create_scope_before_publish(before_publish)
            .expect("cannot create a scope after supervisor shutdown starts")
    }

    fn try_create_scope_before_publish(
        &self,
        before_publish: impl FnOnce(ProcessScopeId),
    ) -> Result<ProcessScopeId, ScopeCreationError> {
        let mut registry = self.lock_registry();
        if self.inner.shutting_down.load(Ordering::SeqCst) {
            return Err(ScopeCreationError::SupervisorClosed);
        }
        let scope = registry.create_scope();
        before_publish(scope);
        // Registry -> operation map is the shared lock order. Do not expose the new
        // scope to shutdown before its serialization lock exists.
        self.inner
            .scope_operations
            .lock()
            .expect("scope operations mutex")
            .insert(scope, Arc::new(tokio::sync::Mutex::new(None)));
        Ok(scope)
    }

    /// Runtime capabilities of the selected backend.
    pub fn capabilities(&self) -> shepherd_domain::Capabilities {
        self.inner.backend.capabilities()
    }

    fn scope_operation(&self, scope: ProcessScopeId) -> Option<Arc<ScopeOperation>> {
        self.inner
            .scope_operations
            .lock()
            .expect("scope operations mutex")
            .get(&scope)
            .cloned()
    }

    /// The ids of live processes in a scope, or `None` if the scope is unknown.
    #[must_use]
    pub fn processes(&self, scope: ProcessScopeId) -> Option<Vec<ProcessId>> {
        self.lock_registry()
            .get(scope)
            .map(|s| s.live_process_ids())
    }

    /// Spawns a process into `scope`.
    ///
    /// # Errors
    /// Returns [`SpawnError`] if the scope is unknown/closed or the OS spawn fails. If the
    /// scope closes during the spawn (a spawn-vs-terminate race), the just-created OS process
    /// is killed and reaped rather than leaked.
    pub async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: ProcessSpec,
    ) -> Result<ProcessId, SpawnError> {
        let worker = self.worker();
        tokio::spawn(async move { worker.spawn_owned(scope, spec).await })
            .await
            .map_err(|e| SpawnError::Os(format!("spawn worker failed: {e}")))?
    }

    async fn spawn_owned(
        &self,
        scope: ProcessScopeId,
        spec: ProcessSpec,
    ) -> Result<ProcessId, SpawnError> {
        let operation = self.scope_operation(scope).ok_or_else(|| {
            if self
                .inner
                .reports
                .lock()
                .expect("reports mutex")
                .contains_key(&scope)
            {
                SpawnError::ScopeClosed(scope)
            } else {
                SpawnError::UnknownScope(scope)
            }
        })?;
        let serial = operation.lock().await;
        if serial.is_some() {
            return Err(SpawnError::ScopeClosed(scope));
        }
        if self.inner.shutting_down.load(Ordering::SeqCst) {
            return Err(SpawnError::ScopeClosed(scope));
        }
        if self
            .inner
            .reports
            .lock()
            .expect("reports mutex")
            .contains_key(&scope)
        {
            return Err(SpawnError::ScopeClosed(scope));
        }
        {
            let registry = self.lock_registry();
            let s = registry.get(scope).ok_or(SpawnError::UnknownScope(scope))?;
            if !s.is_open() {
                return Err(SpawnError::ScopeClosed(scope));
            }
        }

        let spawned = self.inner.backend.spawn(scope, &spec).await?;
        // Ordinary shutdown waits on this operation lock and cleans an admitted spawn
        // through its scope. Only last-owner Drop requires a synchronous late-spawn sweep.
        if self.inner.owners_dropped.load(Ordering::SeqCst) {
            self.inner.backend.hard_kill_all();
            self.kill_orphan(&spawned).await;
            return Err(SpawnError::ScopeClosed(scope));
        }

        // Attach under the lock, then release it *before* any await. If the scope closed
        // during the spawn (a spawn-vs-terminate race), kill the orphan outside the lock.
        enum Attach {
            Ok(ProcessId, Vec<shepherd_domain::DomainEvent>),
            Closed,
            Unknown,
        }
        let attach = {
            let mut registry = self.lock_registry();
            let pid = registry.next_process_id();
            match registry.get_mut(scope) {
                None => Attach::Unknown,
                Some(s) => match s.attach_spawned(pid, spawned.os, spec) {
                    Ok(event) => Attach::Ok(pid, vec![event]),
                    Err(_) => Attach::Closed,
                },
            }
        };
        let (pid, events) = match attach {
            Attach::Ok(pid, events) => (pid, events),
            Attach::Closed => {
                self.kill_orphan(&spawned).await;
                return Err(SpawnError::ScopeClosed(scope));
            }
            Attach::Unknown => {
                self.kill_orphan(&spawned).await;
                return Err(SpawnError::UnknownScope(scope));
            }
        };

        self.inner
            .spawn_times
            .lock()
            .expect("spawn_times mutex")
            .insert(pid, self.inner.clock.now());
        if let Some(output) = self.inner.backend.output(&spawned) {
            self.inner
                .outputs
                .lock()
                .expect("outputs mutex")
                .insert(pid, output);
        }
        // Start ownership monitoring before any cancellable dispatch.
        self.start_monitor(scope, pid, spawned);
        self.start_sampler();
        self.inner.dispatcher.dispatch(&events).await;
        Ok(pid)
    }

    /// Runs a closure in a fresh scope and always schedules two-phase cleanup.
    /// Closure errors are preserved in T; partial spawn errors are the outer result.
    /// Cancel/panic drops the closure but leaves cleanup running on the active runtime.
    /// # Panics
    /// Panics if shutdown has started before the scope is admitted.
    pub async fn with_scope<T, F, Fut>(
        &self,
        specs: Vec<ProcessSpec>,
        body: F,
    ) -> WithScopeResult<T>
    where
        F: FnOnce(ScopedProcesses) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        self.with_scope_options(specs, TerminateOptions::default(), body)
            .await
    }

    /// Variant allowing the caller to choose the grace period and force timeout.
    /// # Panics
    /// Panics if shutdown has started before the scope is admitted.
    pub async fn with_scope_options<T, F, Fut>(
        &self,
        specs: Vec<ProcessSpec>,
        opts: TerminateOptions,
        body: F,
    ) -> WithScopeResult<T>
    where
        F: FnOnce(ScopedProcesses) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let (finish, finished) = tokio::sync::oneshot::channel();
        let guard = ScopeExit {
            finish: Some(finish),
        };
        let (report_tx, mut report_rx) = tokio::sync::watch::channel(None);
        // Register observation before shutdown can discover this scope.
        let scope = self.create_scope_before_publish(|scope| {
            self.inner
                .scope_results
                .lock()
                .expect("scope results mutex")
                .insert(scope, report_tx.clone());
        });
        let worker = self.worker();
        let worker_report = report_tx.clone();
        let cleanup = tokio::spawn(async move {
            let _ = finished.await;
            worker
                .cleanup_and_publish_scope(scope, opts, worker_report)
                .await;
        });
        tokio::spawn(async move {
            // A normal worker publishes before returning. This observer only handles
            // failures, so runtime shutdown cannot lose an already completed result.
            if let Err(error) = cleanup.await {
                publish_scope_cleanup_result(
                    &report_tx,
                    Err(TerminateError::Signal(format!(
                        "scope cleanup worker failed: {error}"
                    ))),
                );
            }
        });
        let mut processes = Vec::new();
        let mut spawn_error = None;
        for spec in specs {
            match self.spawn(scope, spec).await {
                Ok(pid) => processes.push(pid),
                Err(error) => {
                    spawn_error = Some(error);
                    break;
                }
            }
        }
        let result = match spawn_error {
            Some(error) => Err(error),
            None => Ok(body(ScopedProcesses {
                scope,
                owned: Mutex::new(processes.iter().copied().collect()),
                processes,
                supervisor: self.worker(),
            })
            .await),
        };
        drop(guard); // same cleanup path for success, closure error, panic, and cancel
        let termination = loop {
            if let Some(report) = report_rx.borrow_and_update().clone() {
                break report;
            }
            if report_rx.changed().await.is_err() {
                break Err(TerminateError::Signal(
                    "scope cleanup worker stopped".into(),
                ));
            }
        };
        WithScopeResult {
            scope,
            result,
            termination,
        }
    }

    async fn cleanup_and_publish_scope(
        &self,
        scope: ProcessScopeId,
        opts: TerminateOptions,
        report_tx: ScopeCleanupSender,
    ) {
        let published = report_tx.borrow().clone();
        let result = if let Some(Ok(report)) =
            published.filter(|result| result.as_ref().is_ok_and(|report| report.all_verified()))
        {
            Ok(report)
        } else {
            let result = self.terminate_scope(scope, opts).await;
            // External cleanup can complete while this worker waits, then its general
            // report can expire. Our channel retains that verified result independently.
            match report_tx.borrow().clone() {
                Some(Ok(report)) if report.all_verified() => Ok(report),
                _ => result,
            }
        };
        // terminate_scope commits verified publication and bounded history together.
        // Publish errors here without replacing a concurrently verified external result.
        publish_scope_cleanup_result(&report_tx, result);
    }

    /// Waits for the retained report of a with_scope block, including after cancellation.
    pub async fn wait_scope_cleanup(
        &self,
        scope: ProcessScopeId,
    ) -> Result<ScopeTerminationReport, TerminateError> {
        let mut receiver = self
            .inner
            .scope_results
            .lock()
            .expect("scope results mutex")
            .get(&scope)
            .ok_or(TerminateError::UnknownScope(scope))?
            .subscribe();
        loop {
            if let Some(report) = receiver.borrow_and_update().clone() {
                return report;
            }
            receiver
                .changed()
                .await
                .map_err(|_| TerminateError::Signal("scope cleanup worker stopped".into()))?;
        }
    }

    /// Transfers the capture observer to the caller, at most once per process.
    /// Call after spawn, or after wait for post-mortem output. Unclaimed observers
    /// retain the most recent 256 unclaimed captures from verified completed processes.
    pub fn take_output(&self, pid: ProcessId) -> Option<crate::output::ProcessOutput> {
        // Serialize claims with completion so neither path leaves a stale history slot.
        let mut completed = self.inner.completed.lock().expect("completed mutex");
        let output = self
            .inner
            .outputs
            .lock()
            .expect("outputs mutex")
            .remove(&pid);
        completed.retain(|retained| *retained != pid);
        output
    }

    /// Returns the most recent interval sample, without performing backend I/O.
    /// CPU is a fraction of one core. Uptime is measured at sample time.
    /// Returns NotReady until the first observation, or the last sampling error.
    pub async fn stats(&self, pid: ProcessId) -> Result<ProcessStats, StatsError> {
        let registry = self.lock_registry();
        let live = registry
            .scope_of(pid)
            .and_then(|id| registry.get(id))
            .and_then(|s| s.get(pid))
            .is_some_and(|p| p.state().is_live());
        if !live {
            return Err(StatsError::UnknownProcess(pid));
        }
        self.inner
            .samples
            .lock()
            .expect("samples mutex")
            .get(&pid)
            .cloned()
            .unwrap_or(Err(StatsError::NotReady(pid)))
    }

    fn start_sampler(&self) {
        if self.inner.sampler_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = Arc::downgrade(&self.inner);
        let interval = self.inner.stats_interval;
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut pending = FuturesUnordered::new();
            let mut active = HashMap::<ProcessId, AbortHandle>::new();
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let Some(inner) = weak.upgrade() else { break; };
                        let targets = {
                            let registry = inner.registry.lock().expect("registry mutex");
                            registry.scope_ids().into_iter().flat_map(|id| {
                                let scope = registry.get(id).expect("scope exists");
                                scope.live_process_ids().into_iter().map(|pid| {
                                    (pid, Spawned {
                                        os: scope.get(pid).expect("process exists").os_identity(),
                                    })
                                }).collect::<Vec<_>>()
                            }).collect::<Vec<_>>()
                        };
                        let live: HashSet<_> = targets.iter().map(|(pid, _)| *pid).collect();
                        for (pid, abort) in &active {
                            if !live.contains(pid) {
                                abort.abort();
                            }
                        }
                        for (pid, target) in targets {
                            // One observation per root, with no queue of missed intervals.
                            if active.contains_key(&pid) { continue; }
                            let backend = Arc::clone(&inner.backend);
                            let (abort, registration) = AbortHandle::new_pair();
                            active.insert(pid, abort);
                            pending.push(async move {
                                let observed = Abortable::new(async {
                                    match tokio::time::timeout(
                                        Duration::from_secs(1),
                                        std::panic::AssertUnwindSafe(backend.sample(&target)).catch_unwind(),
                                    ).await {
                                        Ok(Ok(result)) => result,
                                        Ok(Err(_)) => Err(StatsError::Backend("sampler panicked".into())),
                                        Err(_) => Err(StatsError::Backend("sampler timeout".into())),
                                    }
                                }, registration).await;
                                (pid, observed)
                            }.boxed());
                        }
                    }
                    Some((pid, observed)) = pending.next(), if !pending.is_empty() => {
                        active.remove(&pid);
                        let Ok(observed) = observed else { continue; };
                        let Some(inner) = weak.upgrade() else { break; };
                        let sample = observed.map(|raw| {
                            let start = inner.spawn_times.lock().expect("spawn times mutex")
                                .get(&pid).copied();
                            let uptime = start.map(|s| inner.clock.now().saturating_duration_since(s))
                                .unwrap_or_default();
                            ProcessStats::from_raw(pid, raw, uptime)
                        });
                        // Recheck under registry -> samples lock order: a result after reap
                        // must never resurrect a cache entry.
                        let registry = inner.registry.lock().expect("registry mutex");
                        let live = registry.scope_of(pid).and_then(|id| registry.get(id))
                            .and_then(|s| s.get(pid)).is_some_and(|p| p.state().is_live());
                        if live {
                            inner.samples.lock().expect("samples mutex").insert(pid, sample);
                        }
                    }
                }
            }
        });
        // Startup holds its scope operation lock, so successful shutdown cannot yet
        // hold the sampler mutex while joining. No suspension during registration.
        *self
            .inner
            .sampler_task
            .try_lock()
            .expect("sampler startup serialized with shutdown") = Some(task);
    }

    /// Waits for a process to reach its reaped terminal state.
    ///
    /// # Errors
    /// Returns [`WaitError::UnknownProcess`] if the process was never owned.
    pub async fn wait(&self, pid: ProcessId) -> Result<ProcessExit, WaitError> {
        if let Some(exit) = self.inner.waiters.try_get(pid) {
            return Ok(exit);
        }
        {
            let registry = self.lock_registry();
            if registry.scope_of(pid).is_none() {
                return Err(WaitError::UnknownProcess(pid));
            }
        }
        Ok(self.inner.waiters.wait(pid).await)
    }

    /// Terminates a single process with a verified two-phase teardown.
    ///
    /// # Errors
    /// Returns [`TerminateError::UnknownProcess`] if the process is not owned.
    pub async fn terminate(
        &self,
        pid: ProcessId,
        opts: TerminateOptions,
    ) -> Result<ProcessExit, TerminateError> {
        // Idempotency: if the process already reached a terminal state (and was possibly
        // pruned from the registry), the recorded exit lives in the waiters.
        if let Some(exit) = self.inner.waiters.try_get(pid) {
            return Ok(exit);
        }

        let (scope, spawned, graceful, request_events) = {
            let mut registry = self.lock_registry();
            let scope = registry
                .scope_of(pid)
                .ok_or(TerminateError::UnknownProcess(pid))?;
            let s = registry.get_mut(scope).expect("scope exists");
            let process = s.get(pid).ok_or(TerminateError::UnknownProcess(pid))?;
            if let Some(exit) = process.exit() {
                return Ok(exit);
            }
            let spawned = Spawned {
                os: process.os_identity(),
            };
            let graceful = process.spec().graceful_signal;
            let events = s.request_termination(pid)?;
            (scope, spawned, graceful, events)
        };
        self.inner.dispatcher.dispatch(&request_events).await;

        if let Some(exit) = self.inner.waiters.try_get(pid) {
            return Ok(exit);
        }

        // Graceful phase.
        let _ = self.inner.backend.signal(&spawned, graceful).await;
        tokio::select! {
            exit = self.inner.waiters.wait(pid) => return Ok(exit),
            () = self.inner.clock.sleep(opts.grace.as_duration()) => {}
        }

        // Force phase.
        {
            let mut registry = self.lock_registry();
            if let Some(s) = registry.get_mut(scope) {
                let _ = s.escalate_termination(pid);
            }
        }
        let _ = self.inner.backend.signal(&spawned, Signal::Kill).await;

        let exit = match opts.force_timeout {
            Some(timeout) => tokio::select! {
                exit = self.inner.waiters.wait(pid) => exit,
                () = self.inner.clock.sleep(timeout) => ProcessExit {
                    pid,
                    code: None,
                    signal: None,
                    outcome: TerminationOutcome::CleanupUnverified(
                        UnverifiedReason::WaitTimedOut { waited: timeout },
                    ),
                    forced: true,
                },
            },
            None => self.inner.waiters.wait(pid).await,
        };
        Ok(exit)
    }

    /// Terminates every process in a scope. Structurally cannot affect another scope: it only
    /// ever iterates this scope's own processes.
    ///
    /// # Errors
    /// Returns [`TerminateError::UnknownScope`] if the scope is unknown.
    pub async fn terminate_scope(
        &self,
        scope: ProcessScopeId,
        opts: TerminateOptions,
    ) -> Result<ScopeTerminationReport, TerminateError> {
        if let Some(report) = self
            .inner
            .reports
            .lock()
            .expect("reports mutex")
            .get(&scope)
            .cloned()
        {
            return Ok(report);
        }
        let operation = match self.scope_operation(scope) {
            Some(operation) => operation,
            None => {
                // Cleanup may have published and removed its lock since the first read.
                return self
                    .inner
                    .reports
                    .lock()
                    .expect("reports mutex")
                    .get(&scope)
                    .cloned()
                    .ok_or(TerminateError::UnknownScope(scope));
            }
        };
        self.terminate_scope_operation(scope, opts, operation).await
    }

    async fn terminate_scope_operation(
        &self,
        scope: ProcessScopeId,
        opts: TerminateOptions,
        operation: Arc<ScopeOperation>,
    ) -> Result<ScopeTerminationReport, TerminateError> {
        let mut serial = operation.lock().await;
        if let Some(report) = serial.as_ref() {
            return Ok(report.clone());
        }
        if let Some(report) = self
            .inner
            .reports
            .lock()
            .expect("reports mutex")
            .get(&scope)
            .cloned()
        {
            return Ok(report);
        }
        let (mut events, live) = {
            let mut registry = self.lock_registry();
            let s = registry
                .get_mut(scope)
                .ok_or(TerminateError::UnknownScope(scope))?;
            let events = s.begin_scope_termination();
            let live = s.process_ids();
            // A monitor may have recorded a reap while Open but not dispatched its
            // prune yet. Capture those exits at the same lock boundary as Draining.
            // Later reaps persist themselves; already-pruned Open history is not kept.
            let exits: Vec<_> = live
                .iter()
                .filter_map(|pid| {
                    s.get(*pid)
                        .and_then(|process| process.exit())
                        .map(|exit| (*pid, exit.outcome))
                })
                .collect();
            if !exits.is_empty() {
                let mut pending = self
                    .inner
                    .pending_outcomes
                    .lock()
                    .expect("pending outcomes mutex");
                let retained = pending.entry(scope).or_default();
                for (pid, outcome) in exits {
                    retained.entry(pid).or_insert(outcome);
                }
            }
            (events, live)
        };
        events.retain(|event| !matches!(event, shepherd_domain::DomainEvent::ScopeClosed { .. }));
        self.inner.dispatcher.dispatch(&events).await;

        let mut set = tokio::task::JoinSet::new();
        for pid in live {
            let this = self.worker();
            set.spawn(async move { (pid, this.terminate(pid, opts).await) });
        }

        while let Some(joined) = set.join_next().await {
            let (pid, outcome) = match joined {
                Ok((pid, Ok(exit))) => (pid, exit.outcome),
                Ok((pid, Err(_))) => (
                    pid,
                    TerminationOutcome::CleanupUnverified(UnverifiedReason::ProcessDisappeared),
                ),
                Err(error) => {
                    return Err(TerminateError::Signal(format!(
                        "termination worker failed: {error}"
                    )))
                }
            };
            self.inner
                .pending_outcomes
                .lock()
                .expect("pending outcomes mutex")
                .entry(scope)
                .or_default()
                .entry(pid)
                .and_modify(|previous| {
                    if !previous.is_verified() {
                        *previous = outcome;
                    }
                })
                .or_insert(outcome);
        }

        // Sweep any descendants that outlived their roots: a whole-group kill catches
        // grandchildren the per-root path does not track individually.
        self.inner.backend.cleanup_scope(scope).await?;

        let mut outcomes: Vec<_> = self
            .inner
            .pending_outcomes
            .lock()
            .expect("pending outcomes mutex")
            .get(&scope)
            .into_iter()
            .flat_map(|outcomes| outcomes.iter().map(|(pid, outcome)| (*pid, *outcome)))
            .collect();
        outcomes.sort_by_key(|(pid, _)| *pid);
        let report = ScopeTerminationReport { scope, outcomes };
        if report.all_verified() {
            // In-flight callers own this state even after bounded lookup history expires.
            *serial = Some(report.clone());
            // Publish before removing the live entry so repeat callers cannot see a gap.
            let sender = self
                .inner
                .scope_results
                .lock()
                .expect("scope results mutex")
                .get(&scope)
                .cloned();
            if let Some(sender) = sender {
                publish_scope_cleanup_result(&sender, Ok(report.clone()));
                // Only scoped blocks consume scoped result history. The operation lock
                // makes verified completion enter this queue exactly once.
                let mut completed = self
                    .inner
                    .completed_scope_results
                    .lock()
                    .expect("completed scope results mutex");
                completed.push_back(scope);
                while completed.len() > 256 {
                    let old = completed.pop_front().expect("completed scoped result");
                    self.inner
                        .scope_results
                        .lock()
                        .expect("scope results mutex")
                        .remove(&old);
                }
            }
            self.inner
                .reports
                .lock()
                .expect("reports mutex")
                .insert(scope, report.clone());
            self.lock_registry().remove(scope);
            self.inner
                .scope_operations
                .lock()
                .expect("scope operations mutex")
                .remove(&scope);
            self.inner
                .pending_outcomes
                .lock()
                .expect("pending outcomes mutex")
                .remove(&scope);
            let mut completed = self
                .inner
                .completed_scopes
                .lock()
                .expect("completed scopes mutex");
            completed.push_back(scope);
            while completed.len() > 256 {
                if let Some(old) = completed.pop_front() {
                    self.inner
                        .reports
                        .lock()
                        .expect("reports mutex")
                        .remove(&old);
                }
            }
            drop(completed);
            // Commit the report before scheduling exactly one terminal notification.
            // This observer task holds no supervisor/backend/cleanup guard, and a caller
            // cancelling after cleanup cannot cancel its best-effort publication.
            let dispatcher = self.inner.dispatcher.clone();
            tokio::spawn(async move {
                dispatcher
                    .dispatch(&[shepherd_domain::DomainEvent::ScopeClosed { scope }])
                    .await;
            });
        }
        Ok(report)
    }

    /// Terminates all scopes and cleans up. Safe to call more than once.
    ///
    /// # Errors
    /// Returns [`ShutdownError::Unverified`] if any scope could not be verified as cleaned up.
    pub async fn shutdown(&self) -> Result<ShutdownReport, ShutdownError> {
        self.inner.shutting_down.store(true, Ordering::SeqCst);
        let _serial = self.inner.shutdown_serial.lock().await;
        // Pin every snapshotted operation before another terminator can retire it.
        let scopes = {
            let registry = self.lock_registry();
            let operations = self
                .inner
                .scope_operations
                .lock()
                .expect("scope operations mutex");
            registry
                .scope_ids()
                .into_iter()
                .map(|scope| {
                    (
                        scope,
                        operations
                            .get(&scope)
                            .expect("registered scope operation")
                            .clone(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let mut reports = Vec::new();
        let mut unverified = 0usize;
        for (scope, operation) in scopes {
            match self
                .terminate_scope_operation(scope, TerminateOptions::default(), operation)
                .await
            {
                Ok(report) => {
                    if !report.all_verified() {
                        unverified += 1;
                    }
                    reports.push(report);
                }
                Err(_) => unverified += 1,
            }
        }
        if unverified > 0 {
            return Err(ShutdownError::Unverified(unverified));
        }
        // No future spawn can be admitted, and admitted spawns registered their sampler
        // before releasing the scope operation lock. Stop and join the coordinator now,
        // even if its interval is long and the user retains the supervisor indefinitely.
        let mut sampler = self.inner.sampler_task.lock().await;
        if let Some(task) = sampler.as_mut() {
            task.abort();
            // Keep the handle in Inner until joining finishes. If this shutdown is
            // cancelled, the next caller must still join the pending destruction.
            let _ = task.await;
            *sampler = None;
        }
        Ok(ShutdownReport { scopes: reports })
    }

    fn lock_registry(&self) -> std::sync::MutexGuard<'_, ScopeRegistry> {
        self.inner.registry.lock().expect("registry mutex")
    }

    async fn kill_orphan(&self, spawned: &Spawned) {
        let _ = self.inner.backend.signal(spawned, Signal::Kill).await;
        let _ = self.inner.backend.wait(spawned).await;
    }

    fn start_monitor(&self, scope: ProcessScopeId, pid: ProcessId, spawned: Spawned) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let wait_result = inner.backend.wait(&spawned).await;
            let verified_reap = wait_result.is_ok();
            let events = {
                let mut registry = inner.registry.lock().expect("registry mutex");
                let Some(s) = registry.get_mut(scope) else {
                    return;
                };
                let Some(process) = s.get(pid) else {
                    return;
                };
                // A failed wait must never be reported as a verified exit: the process may
                // still be running. Record CleanupUnverified so callers can branch, and leave
                // OS-level bookkeeping to the backend / Drop hard-kill path.
                let (raw, wait_failed) = match wait_result {
                    Ok(raw) => (raw, false),
                    Err(_) => (
                        shepherd_domain::RawExit {
                            code: None,
                            signal: None,
                            core_dumped: false,
                        },
                        true,
                    ),
                };
                // Derive the outcome from what actually happened, not from whether an
                // escalation was *attempted*: a process may exit gracefully in the window
                // between the grace timer firing and this record, and a redundant SIGKILL to
                // an already-dead process is a no-op.
                let terminating = process.state().is_terminating();
                let killed = raw.signal == Some(Signal::Kill);
                let outcome = if wait_failed {
                    TerminationOutcome::CleanupUnverified(UnverifiedReason::ReapFailed)
                } else if !terminating {
                    TerminationOutcome::ExitedNaturally
                } else if killed {
                    TerminationOutcome::ForcedRequired
                } else {
                    TerminationOutcome::GracefulSuccess
                };
                let exit = ProcessExit {
                    pid,
                    code: raw.code,
                    signal: raw.signal,
                    outcome,
                    forced: killed,
                };
                let mut events = s.record_exit(pid).unwrap_or_default();
                events.extend(s.record_reaped(pid, exit).unwrap_or_default());
                // Persist before dispatch can prune the root or suspend on a publisher.
                // The operation's caller may already have cancelled its JoinSet.
                if !s.is_open() {
                    inner
                        .pending_outcomes
                        .lock()
                        .expect("pending outcomes mutex")
                        .entry(scope)
                        .or_default()
                        .insert(pid, outcome);
                }
                events.retain(|event| {
                    !matches!(event, shepherd_domain::DomainEvent::ScopeClosed { .. })
                });
                events
            };
            inner.samples.lock().expect("samples mutex").remove(&pid);
            // Evict before external publication: a stalled publisher must not bypass
            // the retention bound after waiters have observed the terminal exit.
            // A failed reap may leave a live producer behind this observer. Only
            // verified completions are eligible for bounded post-mortem eviction.
            if verified_reap {
                let mut completed = inner.completed.lock().expect("completed mutex");
                let mut outputs = inner.outputs.lock().expect("outputs mutex");
                // Discarded or already claimed output consumes no observer history.
                if outputs.contains_key(&pid) {
                    completed.push_back(pid);
                    while completed.len() > 256 {
                        if let Some(old) = completed.pop_front() {
                            outputs.remove(&old);
                        }
                    }
                }
            }
            inner.dispatcher.dispatch(&events).await;
            inner
                .spawn_times
                .lock()
                .expect("spawn_times mutex")
                .remove(&pid);
        });
    }
}

fn publish_scope_cleanup_result(
    sender: &ScopeCleanupSender,
    result: Result<ScopeTerminationReport, TerminateError>,
) {
    sender.send_if_modified(|current| {
        if current
            .as_ref()
            .is_some_and(|result| result.as_ref().is_ok_and(|report| report.all_verified()))
        {
            return false;
        }
        *current = Some(result);
        true
    });
}

type ScopeCleanupSender =
    tokio::sync::watch::Sender<Option<Result<ScopeTerminationReport, TerminateError>>>;
struct ScopeExit {
    finish: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Drop for ScopeExit {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(());
        }
    }
}

/// Closure value (including a caller's Result) and an independent cleanup report.
#[derive(Debug)]
pub struct WithScopeResult<T> {
    pub scope: ProcessScopeId,
    pub result: Result<T, SpawnError>,
    pub termination: Result<ScopeTerminationReport, TerminateError>,
}
/// Access to a with_scope block's fresh scope. Does not extend supervisor ownership.
pub struct ScopedProcesses {
    scope: ProcessScopeId,
    processes: Vec<ProcessId>,
    owned: Mutex<HashSet<ProcessId>>,
    supervisor: ProcessSupervisor,
}
impl ScopedProcesses {
    pub fn id(&self) -> ProcessScopeId {
        self.scope
    }
    pub fn processes(&self) -> &[ProcessId] {
        &self.processes
    }
    pub async fn spawn(&self, spec: ProcessSpec) -> Result<ProcessId, SpawnError> {
        let pid = self.supervisor.spawn(self.scope, spec).await?;
        self.owned
            .lock()
            .expect("scoped processes mutex")
            .insert(pid);
        Ok(pid)
    }
    pub async fn wait(&self, pid: ProcessId) -> Result<ProcessExit, WaitError> {
        if !self
            .owned
            .lock()
            .expect("scoped processes mutex")
            .contains(&pid)
        {
            return Err(WaitError::UnknownProcess(pid));
        }
        self.supervisor.wait(pid).await
    }
    pub fn take_output(&self, pid: ProcessId) -> Option<crate::output::ProcessOutput> {
        if !self
            .owned
            .lock()
            .expect("scoped processes mutex")
            .contains(&pid)
        {
            return None;
        }
        self.supervisor.take_output(pid)
    }
}

#[cfg(test)]
mod scope_operation_tests {
    use super::*;
    use async_trait::async_trait;
    use std::future::Future;
    use std::task::{Context, Poll};

    // Empty-scope cleanup must never invoke a root process operation.
    struct EmptyPorts;
    #[async_trait]
    impl ProcessBackend for EmptyPorts {
        async fn spawn(&self, _: ProcessScopeId, _: &ProcessSpec) -> Result<Spawned, SpawnError> {
            panic!("closed or unknown scopes must not reach backend spawn")
        }
        async fn signal(&self, _: &Spawned, _: Signal) -> Result<(), TerminateError> {
            unreachable!()
        }
        async fn signal_scope(&self, _: ProcessScopeId, _: Signal) -> Result<(), TerminateError> {
            Ok(())
        }
        async fn wait(&self, _: &Spawned) -> Result<shepherd_domain::RawExit, WaitError> {
            unreachable!()
        }
        async fn sample(&self, _: &Spawned) -> Result<shepherd_domain::RawStats, StatsError> {
            unreachable!()
        }
        fn capabilities(&self) -> shepherd_domain::Capabilities {
            unreachable!()
        }
        fn hard_kill_all(&self) {}
    }
    #[async_trait]
    impl Clock for EmptyPorts {
        fn now(&self) -> Instant {
            unreachable!()
        }
        async fn sleep(&self, _: Duration) {
            unreachable!()
        }
    }
    impl Waiters for EmptyPorts {
        fn signal_exit(&self, _: ProcessId, _: ProcessExit) {
            unreachable!()
        }
        fn try_get(&self, _: ProcessId) -> Option<ProcessExit> {
            unreachable!()
        }
        fn wait(&self, _: ProcessId) -> crate::ports::WaitFuture {
            unreachable!()
        }
    }
    #[async_trait]
    impl IntegrationEventPublisher for EmptyPorts {
        async fn publish(&self, _: shepherd_domain::IntegrationEvent) {}
    }
    fn supervisor() -> ProcessSupervisor {
        let ports = Arc::new(EmptyPorts);
        ProcessSupervisor::new(ports.clone(), ports.clone(), ports.clone(), ports)
    }

    #[tokio::test]
    async fn queued_operations_retain_results_after_lookup_history_eviction() {
        let supervisor = supervisor();
        let scope = supervisor.create_scope();
        let operation = supervisor.scope_operation(scope).unwrap();
        let held = operation.lock().await;
        let mut cleanup = Box::pin(supervisor.terminate_scope(scope, Default::default()));
        let mut queued = Box::pin(supervisor.terminate_scope(scope, Default::default()));
        let mut spawn = Box::pin(supervisor.spawn(scope, ProcessSpec::new("unused")));
        std::future::poll_fn(|cx| {
            assert!(cleanup.as_mut().poll(cx).is_pending());
            assert!(queued.as_mut().poll(cx).is_pending());
            assert!(spawn.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(held);
        assert!(cleanup.await.unwrap().all_verified());
        for _ in 0..650 {
            let old = supervisor.create_scope();
            supervisor
                .terminate_scope(old, Default::default())
                .await
                .unwrap();
        }
        assert!(!supervisor
            .inner
            .reports
            .lock()
            .unwrap()
            .contains_key(&scope));
        assert!(queued.await.unwrap().all_verified());
        assert!(matches!(spawn.await, Err(SpawnError::ScopeClosed(id)) if id == scope));
        assert!(supervisor.inner.scope_operations.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn shutdown_snapshot_pins_operations_before_it_reaches_each_scope() {
        let supervisor = supervisor();
        for _ in 0..652 {
            supervisor.create_scope();
        }
        let scopes = supervisor.lock_registry().scope_ids();
        let first = supervisor.scope_operation(scopes[0]).unwrap();
        let held = first.lock().await;
        let mut shutdown = Box::pin(supervisor.shutdown());
        std::future::poll_fn(|cx| {
            assert!(shutdown.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        for scope in &scopes[1..] {
            supervisor
                .terminate_scope(*scope, Default::default())
                .await
                .unwrap();
        }
        assert!(!supervisor
            .inner
            .reports
            .lock()
            .unwrap()
            .contains_key(&scopes[1]));
        drop(held);
        let result = shutdown.await.unwrap();
        assert_eq!(result.scopes.len(), 652);
        assert!(result
            .scopes
            .iter()
            .all(ScopeTerminationReport::all_verified));
    }

    #[tokio::test]
    async fn draining_captures_reap_recorded_open_before_monitor_prune() {
        let supervisor = supervisor();
        let scope = supervisor.create_scope();
        // Pause the monitor at its registry unlock, before dispatching ProcessReaped.
        let (pid, event) = {
            let mut registry = supervisor.lock_registry();
            let pid = registry.next_process_id();
            let s = registry.get_mut(scope).unwrap();
            s.attach_spawned(
                pid,
                shepherd_domain::OsIdentity::new(1, shepherd_domain::ReuseToken::Unavailable),
                ProcessSpec::new("unused"),
            )
            .unwrap();
            let exit = ProcessExit {
                pid,
                code: Some(0),
                signal: None,
                outcome: TerminationOutcome::ExitedNaturally,
                forced: false,
            };
            s.record_exit(pid).unwrap();
            let events = s.record_reaped(pid, exit).unwrap();
            assert!(s.is_open());
            (
                pid,
                events
                    .into_iter()
                    .find(|event| {
                        matches!(event, shepherd_domain::DomainEvent::ProcessReaped { .. })
                    })
                    .unwrap(),
            )
        };
        assert!(supervisor.inner.pending_outcomes.lock().unwrap().is_empty());
        let mut cleanup = Box::pin(supervisor.terminate_scope(scope, Default::default()));
        std::future::poll_fn(|cx| {
            assert!(cleanup.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        // The JoinSet has not run. Cancel cleanup, then resume the monitor's prune.
        drop(cleanup);
        RegistryPruneHandler::new(supervisor.inner.registry.clone())
            .handle(&event)
            .await
            .unwrap();
        assert!(supervisor
            .lock_registry()
            .get(scope)
            .unwrap()
            .process_ids()
            .is_empty());
        let retry = supervisor
            .terminate_scope(scope, Default::default())
            .await
            .unwrap();
        assert_eq!(
            retry.outcomes,
            vec![(pid, TerminationOutcome::ExitedNaturally)]
        );
        assert!(supervisor.inner.pending_outcomes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejected_creation_does_not_allocate_or_poison_shutdown_state() {
        let supervisor = supervisor();
        let admitted = supervisor.try_create_scope().unwrap();
        assert!(supervisor.scope_operation(admitted).is_some());
        supervisor.shutdown().await.unwrap();
        // Probe the ID sequence before/after rejection. Only these two probes may
        // consume IDs; rejected public calls must not advance the sequence.
        let before = supervisor.lock_registry().next_scope_id();
        let report_count = supervisor.inner.reports.lock().unwrap().len();
        for _ in 0..1000 {
            assert_eq!(
                supervisor.try_create_scope(),
                Err(ScopeCreationError::SupervisorClosed)
            );
        }
        assert_eq!(
            supervisor.try_create_scope_before_publish(|_| panic!("rejection invoked publication")),
            Err(ScopeCreationError::SupervisorClosed)
        );
        let misuse =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| supervisor.create_scope()));
        assert!(misuse.is_err());
        let after = supervisor.lock_registry().next_scope_id();
        assert_eq!(after.get(), before.get() + 1);
        assert!(supervisor.lock_registry().scope_ids().is_empty());
        assert!(supervisor.inner.scope_operations.lock().unwrap().is_empty());
        assert!(supervisor.inner.pending_outcomes.lock().unwrap().is_empty());
        assert_eq!(supervisor.inner.reports.lock().unwrap().len(), report_count);
        assert_eq!(
            supervisor.inner.completed_scopes.lock().unwrap().len(),
            report_count
        );
        // Catching the infallible wrapper's panic must not poison the registry.
        assert!(supervisor.shutdown().await.unwrap().scopes.is_empty());
    }

    #[test]
    fn worker_error_publication_cannot_downgrade_verified_external_cleanup() {
        let supervisor = supervisor();
        let scope = supervisor.create_scope();
        let report = ScopeTerminationReport {
            scope,
            outcomes: Vec::new(),
        };
        let (sender, receiver) = tokio::sync::watch::channel(Some(Ok(report)));
        // Both normal cleanup errors and the JoinError observer use this atomic helper.
        publish_scope_cleanup_result(&sender, Err(TerminateError::UnknownScope(scope)));
        publish_scope_cleanup_result(
            &sender,
            Err(TerminateError::Signal("worker panicked".into())),
        );
        assert!(receiver
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .all_verified());
    }

    #[tokio::test]
    async fn active_block_retains_external_cleanup_after_report_eviction() {
        let supervisor = supervisor();
        let external = supervisor.clone();
        let result = supervisor
            .with_scope(Vec::new(), move |scope| async move {
                let report = external
                    .terminate_scope(scope.id(), Default::default())
                    .await
                    .unwrap();
                assert!(report.all_verified());
                for _ in 0..650 {
                    let old = external.create_scope();
                    external
                        .terminate_scope(old, Default::default())
                        .await
                        .unwrap();
                }
                assert!(!external
                    .inner
                    .reports
                    .lock()
                    .unwrap()
                    .contains_key(&scope.id()));
                // Match F's global history eviction as well: both active participants
                // must use their own channel, independently of lookup retention.
                let receiver = external
                    .inner
                    .scope_results
                    .lock()
                    .unwrap()
                    .remove(&scope.id())
                    .unwrap()
                    .subscribe();
                (42, receiver)
            })
            .await;
        let (value, receiver) = result.result.unwrap();
        assert_eq!(value, 42);
        assert!(result.termination.unwrap().all_verified());
        // Wait for the worker's history update, proving it also retained success.
        tokio::time::timeout(Duration::from_secs(1), async {
            while !supervisor
                .inner
                .completed_scope_results
                .lock()
                .unwrap()
                .contains(&result.scope)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(receiver
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .all_verified());
    }

    #[test]
    fn completed_cleanup_is_published_without_join_observer_before_runtime_drop() {
        let supervisor = supervisor();
        let scope = supervisor.create_scope();
        let (report, _) = tokio::sync::watch::channel(None);
        supervisor
            .inner
            .scope_results
            .lock()
            .unwrap()
            .insert(scope, report.clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Drive the worker body to completion without ever starting its JoinHandle
        // observer, then destroy the runtime that performed cleanup.
        runtime.block_on(supervisor.worker().cleanup_and_publish_scope(
            scope,
            Default::default(),
            report,
        ));
        drop(runtime);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), supervisor.wait_scope_cleanup(scope))
                .await
                .expect("completed worker left report publication to an unpolled observer")
        });
        assert!(result.unwrap().all_verified());
        assert_eq!(
            supervisor
                .inner
                .completed_scope_results
                .lock()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn shutdown_cannot_observe_scope_before_its_operation_lock_is_published() {
        let supervisor = supervisor();
        let creator = supervisor.clone();
        let (inserted, insertion) = std::sync::mpsc::sync_channel(0);
        let (release, released) = std::sync::mpsc::sync_channel(0);
        let creation = std::thread::spawn(move || {
            creator.create_scope_before_publish(|_| {
                inserted.send(()).unwrap();
                released.recv().unwrap();
            })
        });
        insertion.recv_timeout(Duration::from_secs(5)).unwrap();
        // The new registry entry exists, but readers must be blocked until its
        // operation lock is present. Check this before allowing creation to continue.
        let registry_hidden = matches!(
            supervisor.inner.registry.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        );
        let operation_unpublished = supervisor.inner.scope_operations.lock().unwrap().is_empty();
        let owner = supervisor.clone();
        let shutdown = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(owner.shutdown())
        });
        // Shutdown sets its intent immediately before taking the registry snapshot.
        // The channel, rather than a sleep, controls the creator's critical section.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !supervisor.inner.shutting_down.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let shutdown_started = supervisor.inner.shutting_down.load(Ordering::SeqCst);
        release.send(()).unwrap();
        let scope = creation.join().unwrap();
        let result = shutdown.join().unwrap();
        assert!(
            registry_hidden,
            "published a scope before its operation lock"
        );
        assert!(operation_unpublished);
        assert!(
            shutdown_started,
            "shutdown thread did not reach its registry snapshot"
        );
        let report = result.expect("shutdown observed a half-created scope");
        assert_eq!(report.scopes.len(), 1);
        assert_eq!(report.scopes[0].scope, scope);
        assert!(report.scopes[0].all_verified());
        assert!(supervisor.processes(scope).is_none());
        assert!(supervisor.scope_operation(scope).is_none());
    }

    #[tokio::test]
    async fn completed_scopes_and_unknown_lookups_do_not_retain_operation_locks() {
        let supervisor = supervisor();
        let mut last = None;
        for _ in 0..600 {
            let scope = supervisor.create_scope();
            assert_eq!(supervisor.inner.scope_operations.lock().unwrap().len(), 1);
            assert!(supervisor
                .terminate_scope(scope, TerminateOptions::default())
                .await
                .unwrap()
                .all_verified());
            assert!(supervisor.inner.scope_operations.lock().unwrap().is_empty());
            last = Some(scope);
        }
        let scope = last.unwrap();
        assert!(supervisor
            .terminate_scope(scope, TerminateOptions::default())
            .await
            .unwrap()
            .all_verified());
        assert!(
            matches!(supervisor.spawn(scope, ProcessSpec::new("unused")).await, Err(SpawnError::ScopeClosed(id)) if id == scope)
        );
        for id in 1000..1600 {
            let unknown = ProcessScopeId::new(id);
            assert!(
                matches!(supervisor.spawn(unknown, ProcessSpec::new("unused")).await, Err(SpawnError::UnknownScope(id)) if id == unknown)
            );
            assert!(
                matches!(supervisor.terminate_scope(unknown, TerminateOptions::default()).await, Err(TerminateError::UnknownScope(id)) if id == unknown)
            );
        }
        assert!(supervisor.inner.scope_operations.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn spawn_queued_behind_cleanup_reports_scope_closed_without_recreating_lock() {
        let supervisor = supervisor();
        let scope = supervisor.create_scope();
        let operation = supervisor.scope_operation(scope).unwrap();
        let held = operation.lock().await;
        let mut cleanup =
            std::pin::pin!(supervisor.terminate_scope(scope, TerminateOptions::default()));
        let mut spawn = std::pin::pin!(supervisor.spawn_owned(scope, ProcessSpec::new("unused")));
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        assert!(cleanup.as_mut().poll(&mut cx).is_pending());
        assert!(spawn.as_mut().poll(&mut cx).is_pending());
        drop(held);
        assert!(cleanup.await.unwrap().all_verified());
        assert!(supervisor.scope_operation(scope).is_none());
        assert!(matches!(spawn.await, Err(SpawnError::ScopeClosed(id)) if id == scope));
        assert!(supervisor.scope_operation(scope).is_none());
    }
}

#[cfg(test)]
mod sampler_shutdown_tests {
    use super::*;
    use async_trait::async_trait;

    struct Ports {
        fail_cleanup: AtomicBool,
    }
    #[async_trait]
    impl ProcessBackend for Ports {
        async fn spawn(&self, _: ProcessScopeId, _: &ProcessSpec) -> Result<Spawned, SpawnError> {
            unreachable!()
        }
        async fn signal(&self, _: &Spawned, _: Signal) -> Result<(), TerminateError> {
            unreachable!()
        }
        async fn signal_scope(&self, _: ProcessScopeId, _: Signal) -> Result<(), TerminateError> {
            if self.fail_cleanup.load(Ordering::SeqCst) {
                Err(TerminateError::Signal("injected cleanup failure".into()))
            } else {
                Ok(())
            }
        }
        async fn wait(&self, _: &Spawned) -> Result<shepherd_domain::RawExit, WaitError> {
            unreachable!()
        }
        async fn sample(&self, _: &Spawned) -> Result<shepherd_domain::RawStats, StatsError> {
            unreachable!()
        }
        fn capabilities(&self) -> shepherd_domain::Capabilities {
            unreachable!()
        }
        fn hard_kill_all(&self) {}
    }
    #[async_trait]
    impl Clock for Ports {
        fn now(&self) -> Instant {
            Instant::now()
        }
        async fn sleep(&self, duration: Duration) {
            tokio::time::sleep(duration).await;
        }
    }
    impl Waiters for Ports {
        fn signal_exit(&self, _: ProcessId, _: ProcessExit) {
            unreachable!()
        }
        fn try_get(&self, _: ProcessId) -> Option<ProcessExit> {
            unreachable!()
        }
        fn wait(&self, _: ProcessId) -> crate::ports::WaitFuture {
            unreachable!()
        }
    }
    #[async_trait]
    impl IntegrationEventPublisher for Ports {
        async fn publish(&self, _: shepherd_domain::IntegrationEvent) {}
    }

    #[tokio::test]
    async fn recovered_scoped_cleanups_enter_bounded_history_once() {
        let ports = Arc::new(Ports {
            fail_cleanup: AtomicBool::new(true),
        });
        let sup =
            ProcessSupervisor::new(ports.clone(), ports.clone(), ports.clone(), ports.clone());
        let mut first_receiver = None;
        let mut first_scope = None;
        for index in 0..300 {
            ports.fail_cleanup.store(true, Ordering::SeqCst);
            let failed = sup.with_scope(Vec::new(), |_| async {}).await;
            assert!(failed.result.is_ok());
            assert!(failed.termination.is_err());
            let scope = failed.scope;
            if index == 0 {
                first_scope = Some(scope);
                first_receiver = Some(
                    sup.inner
                        .scope_results
                        .lock()
                        .unwrap()
                        .get(&scope)
                        .unwrap()
                        .subscribe(),
                );
            }
            ports.fail_cleanup.store(false, Ordering::SeqCst);
            assert!(sup
                .terminate_scope(scope, TerminateOptions::default())
                .await
                .unwrap()
                .all_verified());
            assert!(sup.wait_scope_cleanup(scope).await.unwrap().all_verified());
            // Repeated successful cleanup reads the cached report, without consuming
            // another history position or evicting unrelated retained scoped results.
            for _ in 0..3 {
                assert!(sup
                    .terminate_scope(scope, TerminateOptions::default())
                    .await
                    .unwrap()
                    .all_verified());
            }
            let completed = sup.inner.completed_scope_results.lock().unwrap();
            assert_eq!(completed.len(), (index + 1).min(256));
            assert_eq!(completed.iter().filter(|id| **id == scope).count(), 1);
            assert_eq!(
                sup.inner.scope_results.lock().unwrap().len(),
                completed.len()
            );
        }
        let first_scope = first_scope.unwrap();
        assert!(matches!(sup.wait_scope_cleanup(first_scope).await,
            Err(TerminateError::UnknownScope(id)) if id == first_scope));
        // Evicting lookup history cannot invalidate an already registered observer.
        let receiver = first_receiver.unwrap();
        assert!(receiver
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .all_verified());
        assert!(sup.shutdown().await.unwrap().scopes.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_shutdown_preserves_sampler_join_for_retry() {
        use std::future::Future;
        use std::task::{Context, Poll};
        let ports = Arc::new(Ports {
            fail_cleanup: AtomicBool::new(false),
        });
        let sup = ProcessSupervisor::new(ports.clone(), ports.clone(), ports.clone(), ports);
        sup.start_sampler();
        let sampler = sup
            .inner
            .sampler_task
            .lock()
            .await
            .as_ref()
            .unwrap()
            .abort_handle();
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut shutdown = Box::pin(sup.shutdown());
        // Do not yield to the runtime: abort is requested, but task destruction has
        // not run. Cancelling this caller must leave the join handle available.
        assert!(matches!(
            shutdown.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(shutdown);
        assert!(!sampler.is_finished());
        let mut retry = Box::pin(sup.shutdown());
        assert!(
            matches!(retry.as_mut().poll(&mut context), Poll::Pending),
            "retry reported success before the aborted sampler finished"
        );
        retry.await.unwrap();
        assert!(sampler.is_finished());
    }

    #[tokio::test(start_paused = true)]
    async fn successful_shutdown_joins_sampler_but_failed_cleanup_keeps_it_running() {
        let ports = Arc::new(Ports {
            fail_cleanup: AtomicBool::new(true),
        });
        let sup = ProcessSupervisor::with_stats_interval(
            ports.clone(),
            ports.clone(),
            ports.clone(),
            ports.clone(),
            Duration::from_secs(3600),
        );
        sup.create_scope();
        // Start the same coordinator that spawn starts, with no roots to distract from
        // the idle-timer bug. Observe the task itself, not whether sample() was called.
        sup.start_sampler();
        let sampler = sup
            .inner
            .sampler_task
            .lock()
            .await
            .as_ref()
            .unwrap()
            .abort_handle();
        tokio::task::yield_now().await;
        assert!(!sampler.is_finished());
        assert!(matches!(
            sup.shutdown().await,
            Err(ShutdownError::Unverified(1))
        ));
        assert!(!sampler.is_finished(), "failed cleanup stopped observation");
        ports.fail_cleanup.store(false, Ordering::SeqCst);
        let before = tokio::time::Instant::now();
        sup.shutdown().await.unwrap();
        assert!(
            sampler.is_finished(),
            "successful shutdown left the coordinator alive"
        );
        assert_eq!(
            tokio::time::Instant::now(),
            before,
            "shutdown waited for a sampler tick"
        );
        // A retained supervisor and repeated shutdown must not restart its timer.
        tokio::time::advance(Duration::from_secs(7200)).await;
        sup.shutdown().await.unwrap();
        assert!(sampler.is_finished());
    }
}
