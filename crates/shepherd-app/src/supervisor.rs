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
use crate::error::{ShutdownError, SpawnError, StatsError, TerminateError, WaitError};
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

struct Inner {
    registry: SharedRegistry,
    backend: Arc<dyn ProcessBackend>,
    clock: Arc<dyn Clock>,
    waiters: Arc<dyn Waiters>,
    dispatcher: EventDispatcher,
    spawn_times: Mutex<HashMap<ProcessId, Instant>>,
    shutting_down: Arc<AtomicBool>,
    owners_dropped: Arc<AtomicBool>,
    scope_operations: Mutex<HashMap<ProcessScopeId, Arc<tokio::sync::Mutex<()>>>>,
    samples: Mutex<HashMap<ProcessId, Result<ProcessStats, StatsError>>>,
    sampler_started: AtomicBool,
    outputs: Mutex<HashMap<ProcessId, crate::output::ProcessOutput>>,
    scope_results: Mutex<HashMap<ProcessScopeId, ScopeCleanupSender>>,
    stats_interval: Duration,
    completed: Mutex<VecDeque<ProcessId>>,
    reports: Mutex<HashMap<ProcessScopeId, ScopeTerminationReport>>,
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
                stats_interval: stats_interval.max(Duration::from_millis(1)),
                completed: Mutex::new(VecDeque::new()),
                reports: Mutex::new(HashMap::new()),
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
    pub fn create_scope(&self) -> ProcessScopeId {
        let scope = self.lock_registry().create_scope();
        self.inner
            .scope_operations
            .lock()
            .expect("scope operations mutex")
            .insert(scope, Arc::new(tokio::sync::Mutex::new(())));
        scope
    }

    /// Runtime capabilities of the selected backend.
    pub fn capabilities(&self) -> shepherd_domain::Capabilities {
        self.inner.backend.capabilities()
    }

    fn scope_operation(&self, scope: ProcessScopeId) -> Option<Arc<tokio::sync::Mutex<()>>> {
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
        let _serial = operation.lock().await;
        if self.inner.shutting_down.load(Ordering::SeqCst) {
            return Err(SpawnError::ScopeClosed(scope));
        }
        // Cleanup can publish its report and remove the scope while this spawn
        // waits on the old operation lock. Preserve the closed-scope error then.
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
        let scope = self.create_scope();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let guard = ScopeExit {
            finish: Some(finish),
        };
        let (report_tx, mut report_rx) = tokio::sync::watch::channel(None);
        self.inner
            .scope_results
            .lock()
            .expect("scope results mutex")
            .insert(scope, report_tx.clone());
        let worker = self.worker();
        let backstop = ScopeCleanupBackstop {
            backend: self.inner.backend.clone(),
            scope,
            report: report_tx.clone(),
            armed: true,
        };
        let cleanup = tokio::spawn(async move {
            let mut backstop = backstop;
            let _ = finished.await;
            let result = worker.terminate_scope(scope, opts).await;
            if !result.as_ref().is_ok_and(|r| r.all_verified()) {
                backstop.backend.hard_kill_scope(scope);
            }
            // A completed worker returns its actual report, even when unverified.
            // Only interruption/panic publishes the generic Drop-backstop error.
            backstop.armed = false;
            result
        });
        tokio::spawn(async move {
            let result = cleanup.await.unwrap_or_else(|e| {
                Err(TerminateError::Signal(format!(
                    "scope cleanup worker failed: {e}"
                )))
            });
            report_tx.send_replace(Some(result));
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
    /// Call after spawn, or after wait for post-mortem output.
    pub fn take_output(&self, pid: ProcessId) -> Option<crate::output::ProcessOutput> {
        self.inner
            .outputs
            .lock()
            .expect("outputs mutex")
            .remove(&pid)
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
        tokio::spawn(async move {
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
    }

    /// Waits for a process to reach its reaped terminal state.
    ///
    /// # Errors
    /// Returns [`WaitError::UnknownProcess`] if the process was never owned.
    pub async fn wait(&self, pid: ProcessId) -> Result<ProcessExit, WaitError> {
        if let Some(exit) = self.inner.waiters.try_get(pid) {
            return Ok(exit);
        }
        let future = {
            let registry = self.lock_registry();
            let Some(scope) = registry.scope_of(pid).and_then(|id| registry.get(id)) else {
                return self
                    .inner
                    .waiters
                    .try_get(pid)
                    .ok_or(WaitError::UnknownProcess(pid));
            };
            if let Some(exit) = scope.get(pid).and_then(|p| p.exit()) {
                return Ok(exit);
            }
            // Register under ownership lock before a completion can be pruned/evicted.
            self.inner.waiters.wait(pid)
        };
        Ok(future.await)
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

        let (scope, spawned, graceful, request_events, mut exit_future) = {
            let mut registry = self.lock_registry();
            let Some(scope) = registry.scope_of(pid) else {
                return self
                    .inner
                    .waiters
                    .try_get(pid)
                    .ok_or(TerminateError::UnknownProcess(pid));
            };
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
            (
                scope,
                spawned,
                graceful,
                events,
                self.inner.waiters.wait(pid),
            )
        };
        self.inner.dispatcher.dispatch(&request_events).await;

        if let Some(exit) = self.inner.waiters.try_get(pid) {
            return Ok(exit);
        }

        // Graceful phase.
        let _ = self.inner.backend.signal(&spawned, graceful).await;
        tokio::select! {
            exit = &mut exit_future => return Ok(exit),
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
                exit = &mut exit_future => exit,
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
            None => exit_future.await,
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
                // Cleanup may have published and removed its lock since our first read.
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
        let _serial = operation.lock().await;
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
        let (events, live) = {
            let mut registry = self.lock_registry();
            let s = registry
                .get_mut(scope)
                .ok_or(TerminateError::UnknownScope(scope))?;
            {
                let events = s.begin_scope_termination();
                let processes = s
                    .process_ids()
                    .into_iter()
                    .map(|pid| {
                        let observer: crate::ports::WaitFuture =
                            match s.get(pid).and_then(|p| p.exit()) {
                                Some(exit) => Box::pin(std::future::ready(exit)),
                                None => self.inner.waiters.wait(pid),
                            };
                        (pid, observer)
                    })
                    .collect::<Vec<_>>();
                (events, processes)
            }
        };
        self.inner.dispatcher.dispatch(&events).await;

        let mut set = tokio::task::JoinSet::new();
        for (pid, observer) in live {
            let this = self.worker();
            set.spawn(async move {
                let result = tokio::select! { biased;
                    exit = observer => Ok(exit),
                    result = this.terminate(pid, opts) => result,
                };
                (pid, result)
            });
        }

        let mut outcomes = Vec::new();
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((pid, Ok(exit))) => outcomes.push((pid, exit.outcome)),
                Ok((pid, Err(_))) => outcomes.push((
                    pid,
                    TerminationOutcome::CleanupUnverified(UnverifiedReason::ProcessDisappeared),
                )),
                Err(error) => {
                    return Err(TerminateError::Signal(format!(
                        "termination worker failed: {error}"
                    )))
                }
            }
        }

        // Sweep any descendants that outlived their roots: a whole-group kill catches
        // grandchildren the per-root path does not track individually.
        self.inner.backend.cleanup_scope(scope).await?;
        // A containment sweep can finish roots whose individual signal failed or timed out.
        // Re-observe their monitor outcome; a sweep itself is never evidence of reap.
        for (pid, outcome) in &mut outcomes {
            if !outcome.is_verified() {
                let waited = match opts.force_timeout {
                    Some(timeout) => tokio::time::timeout(timeout, self.wait(*pid)).await.ok(),
                    None => Some(self.wait(*pid).await),
                };
                if let Some(Ok(exit)) = waited {
                    *outcome = exit.outcome;
                }
            }
        }

        let report = ScopeTerminationReport { scope, outcomes };
        if report.all_verified() {
            // Publish before removing the live entry so repeat callers cannot see a gap.
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
                    self.inner
                        .scope_results
                        .lock()
                        .expect("scope results mutex")
                        .remove(&old);
                }
            }
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
        let scopes = self.lock_registry().scope_ids();
        let mut reports = Vec::new();
        let mut unverified = 0usize;
        for scope in scopes {
            match self
                .terminate_scope(scope, TerminateOptions::default())
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
            let wait_result = std::panic::AssertUnwindSafe(inner.backend.wait(&spawned))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(WaitError::Backend("waiter panicked".into())));
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
                events
            };
            inner.samples.lock().expect("samples mutex").remove(&pid);
            // Evict before external publication: a stalled publisher must not bypass
            // the retention bound after waiters have observed the terminal exit.
            // A failed reap may leave a live producer behind this observer. Only
            // verified completions are eligible for bounded post-mortem eviction.
            if verified_reap {
                let mut completed = inner.completed.lock().expect("completed mutex");
                completed.push_back(pid);
                while completed.len() > 256 {
                    if let Some(old) = completed.pop_front() {
                        inner.outputs.lock().expect("outputs mutex").remove(&old);
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
    // Retain authorization while any owned process or bounded observation remains.
    // The lock order extends the supervisor's registry -> outputs/waiters order; no
    // supervisor operation takes the per-handle owned lock in the other direction.
    fn observable_owned(&self) -> std::sync::MutexGuard<'_, HashSet<ProcessId>> {
        let mut owned = self.owned.lock().expect("scoped processes mutex");
        let registry = self.supervisor.lock_registry();
        let scope = registry.get(self.scope);
        let outputs = self.supervisor.inner.outputs.lock().expect("outputs mutex");
        owned.retain(|pid| {
            scope.is_some_and(|s| s.contains(*pid))
                || outputs.contains_key(pid)
                || self.supervisor.inner.waiters.try_get(*pid).is_some()
        });
        owned
    }

    pub async fn spawn(&self, spec: ProcessSpec) -> Result<ProcessId, SpawnError> {
        let pid = self.supervisor.spawn(self.scope, spec).await?;
        self.observable_owned().insert(pid);
        Ok(pid)
    }
    pub async fn wait(&self, pid: ProcessId) -> Result<ProcessExit, WaitError> {
        if !self.observable_owned().contains(&pid) {
            return Err(WaitError::UnknownProcess(pid));
        }
        let result = self.supervisor.wait(pid).await;
        drop(self.observable_owned());
        result
    }
    pub fn take_output(&self, pid: ProcessId) -> Option<crate::output::ProcessOutput> {
        if !self.observable_owned().contains(&pid) {
            return None;
        }
        let output = self.supervisor.take_output(pid);
        drop(self.observable_owned());
        output
    }
}

struct ScopeCleanupBackstop {
    backend: Arc<dyn ProcessBackend>,
    scope: ProcessScopeId,
    report: ScopeCleanupSender,
    armed: bool,
}
impl Drop for ScopeCleanupBackstop {
    fn drop(&mut self) {
        if self.armed {
            self.backend.hard_kill_scope(self.scope);
            self.report.send_replace(Some(Err(TerminateError::Signal(
                "scope cleanup interrupted or unverified; synchronous backstop issued".into(),
            ))));
        }
    }
}

#[cfg(test)]
mod spawn_cleanup_tests {
    use super::*;
    use async_trait::async_trait;
    use std::future::Future;
    use std::task::Context;

    // This regression closes an empty scope: no process operation should be reached.
    struct EmptyPorts;
    #[async_trait]
    impl ProcessBackend for EmptyPorts {
        async fn spawn(&self, _: ProcessScopeId, _: &ProcessSpec) -> Result<Spawned, SpawnError> {
            panic!("a queued spawn must not reach the backend after scope cleanup")
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
        fn hard_kill_scope(&self, _: ProcessScopeId) {}
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

    #[tokio::test]
    async fn spawn_queued_behind_cleanup_reports_scope_closed() {
        let ports = Arc::new(EmptyPorts);
        let sup = ProcessSupervisor::new(ports.clone(), ports.clone(), ports.clone(), ports);
        let scope = sup.create_scope();
        let operation = sup.scope_operation(scope).unwrap();
        let held = operation.lock().await;
        let mut cleanup = std::pin::pin!(sup.terminate_scope(scope, TerminateOptions::default()));
        let mut spawn = std::pin::pin!(sup.spawn_owned(scope, ProcessSpec::new("unused")));
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        // Explicit polls establish FIFO lock order, with spawn retaining the old lock
        // while cleanup will remove both the registry entry and its lock-map entry.
        assert!(cleanup.as_mut().poll(&mut cx).is_pending());
        assert!(spawn.as_mut().poll(&mut cx).is_pending());
        drop(held);
        assert!(cleanup.await.unwrap().all_verified());
        assert!(sup.processes(scope).is_none());
        assert!(matches!(spawn.await, Err(SpawnError::ScopeClosed(id)) if id == scope));
    }
}

#[cfg(test)]
mod scoped_history_tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::AtomicU32;
    use tokio::sync::watch;

    #[derive(Default)]
    struct ExitHistory {
        exits: HashMap<ProcessId, watch::Sender<Option<ProcessExit>>>,
        completed: VecDeque<ProcessId>,
    }
    // Immediate roots and a bounded waiter adapter exercise the real supervisor's
    // monitor, output eviction and scoped APIs without an infrastructure dependency.
    #[derive(Default)]
    struct HistoryPorts {
        next: AtomicU32,
        history: Mutex<ExitHistory>,
    }
    #[async_trait]
    impl ProcessBackend for HistoryPorts {
        async fn spawn(&self, _: ProcessScopeId, _: &ProcessSpec) -> Result<Spawned, SpawnError> {
            let pid = self.next.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Spawned {
                os: shepherd_domain::OsIdentity::new(
                    pid,
                    shepherd_domain::ReuseToken::StartTime(u64::from(pid)),
                ),
            })
        }
        async fn signal(&self, _: &Spawned, _: Signal) -> Result<(), TerminateError> {
            Ok(())
        }
        async fn signal_scope(&self, _: ProcessScopeId, _: Signal) -> Result<(), TerminateError> {
            Ok(())
        }
        async fn wait(&self, target: &Spawned) -> Result<shepherd_domain::RawExit, WaitError> {
            if target.os.pid == 1 {
                return Err(WaitError::Backend("quarantined root".into()));
            }
            Ok(shepherd_domain::RawExit {
                code: Some(0),
                signal: None,
                core_dumped: false,
            })
        }
        async fn sample(&self, _: &Spawned) -> Result<shepherd_domain::RawStats, StatsError> {
            Err(StatsError::Backend("unused sampler".into()))
        }
        fn output(&self, _: &Spawned) -> Option<crate::output::ProcessOutput> {
            Some(crate::output::ProcessOutput(Arc::new(EmptyOutput)))
        }
        fn capabilities(&self) -> shepherd_domain::Capabilities {
            unreachable!("this regression does not query backend capabilities")
        }
        fn hard_kill_all(&self) {}
        fn hard_kill_scope(&self, _: ProcessScopeId) {}
    }
    #[async_trait]
    impl Clock for HistoryPorts {
        fn now(&self) -> Instant {
            Instant::now()
        }
        async fn sleep(&self, duration: Duration) {
            tokio::time::sleep(duration).await;
        }
    }
    impl Waiters for HistoryPorts {
        fn signal_exit(&self, pid: ProcessId, exit: ProcessExit) {
            let mut history = self.history.lock().unwrap();
            history
                .exits
                .entry(pid)
                .or_insert_with(|| watch::channel(None).0)
                .send_replace(Some(exit));
            history.completed.push_back(pid);
            while history.completed.len() > 128 {
                let old = history.completed.pop_front().unwrap();
                history.exits.remove(&old);
            }
        }
        fn try_get(&self, pid: ProcessId) -> Option<ProcessExit> {
            self.history
                .lock()
                .unwrap()
                .exits
                .get(&pid)
                .and_then(|exit| *exit.borrow())
        }
        fn wait(&self, pid: ProcessId) -> crate::ports::WaitFuture {
            let mut receiver = self
                .history
                .lock()
                .unwrap()
                .exits
                .entry(pid)
                .or_insert_with(|| watch::channel(None).0)
                .subscribe();
            Box::pin(async move {
                loop {
                    if let Some(exit) = *receiver.borrow_and_update() {
                        return exit;
                    }
                    receiver.changed().await.unwrap();
                }
            })
        }
    }
    #[async_trait]
    impl IntegrationEventPublisher for HistoryPorts {
        async fn publish(&self, _: shepherd_domain::IntegrationEvent) {}
    }
    struct EmptyOutput;
    impl crate::output::OutputSink for EmptyOutput {
        fn push(&self, _: crate::output::OutputStream, _: &[u8]) {}
        fn close(&self, _: crate::output::OutputStream, _: Option<String>) {}
        fn read(&self) -> crate::output::OutputSnapshot {
            unreachable!("the regression transfers output handles without reading bytes")
        }
    }

    #[tokio::test]
    async fn scoped_dynamic_membership_expires_history_but_keeps_quarantine_and_observations() {
        let ports = Arc::new(HistoryPorts::default());
        let supervisor = ProcessSupervisor::new(ports.clone(), ports.clone(), ports.clone(), ports);
        let result = supervisor
            .with_scope(vec![], |scope| async move {
                let quarantined = scope.spawn(ProcessSpec::new("unverified")).await.unwrap();
                assert!(!scope.wait(quarantined).await.unwrap().outcome.is_verified());
                let mut first_completed = None;
                let mut latest = quarantined;
                let mut output_only = None;
                for index in 0..650 {
                    latest = scope.spawn(ProcessSpec::new("immediate")).await.unwrap();
                    first_completed.get_or_insert(latest);
                    if index == 450 {
                        output_only = Some(latest);
                    }
                    assert!(scope.wait(latest).await.unwrap().outcome.is_verified());
                    assert!(
                        scope.owned.lock().unwrap().len() <= 257,
                        "dynamic membership retained expired completed IDs"
                    );
                }
                let expired = first_completed.unwrap();
                assert!(matches!(scope.wait(expired).await, Err(WaitError::UnknownProcess(id)) if id == expired));
                assert!(scope.take_output(expired).is_none());
                let output_only = output_only.unwrap();
                assert!(scope.supervisor.inner.waiters.try_get(output_only).is_none());
                assert!(scope.take_output(output_only).is_some());
                assert!(scope.take_output(latest).is_some());
                assert!(scope.wait(latest).await.unwrap().outcome.is_verified());
                // Both completed caches have evicted this root; registry quarantine
                // must still authorize observing its unverified outcome.
                assert!(!scope.wait(quarantined).await.unwrap().outcome.is_verified());
            })
            .await;
        assert!(result.result.is_ok());
        assert!(!result.termination.unwrap().all_verified());
    }
}
