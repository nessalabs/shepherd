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
    sampler_task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    outputs: Mutex<HashMap<ProcessId, crate::output::ProcessOutput>>,
    scope_results: Mutex<HashMap<ProcessScopeId, ScopeCleanupSender>>,
    completed_scope_results: Mutex<VecDeque<ProcessScopeId>>,
    stats_interval: Duration,
    completed: Mutex<VecDeque<ProcessId>>,
    reports: Mutex<HashMap<ProcessScopeId, ScopeTerminationReport>>,
    pending_outcomes: Mutex<HashMap<ProcessScopeId, HashMap<ProcessId, TerminationOutcome>>>,
    completed_scopes: Mutex<VecDeque<ProcessScopeId>>,
    shutdown_serial: tokio::sync::Mutex<()>,
    os_pids: Mutex<OsPidHistory>,
}

/// Last 256 attached OS identities. Bound is enforced at `remember`, not on
/// verified reap: the monitor often returns after `terminate_scope` has already
/// dropped the registry entry, and a failed wait never reached the old evictor.
const OS_PID_HISTORY_BOUND: usize = 256;

/// `os_pid` must survive an immediate natural exit: spawn attaches, the monitor
/// reaps, and prune can run before the caller looks up the OS identity.
struct OsPidHistory {
    by_pid: HashMap<ProcessId, u32>,
    order: VecDeque<ProcessId>,
}

impl Default for OsPidHistory {
    fn default() -> Self {
        Self {
            // Headroom so a full 256-entry table does not rehash after warmup.
            by_pid: HashMap::with_capacity(OS_PID_HISTORY_BOUND * 2),
            order: VecDeque::with_capacity(OS_PID_HISTORY_BOUND),
        }
    }
}

impl OsPidHistory {
    fn remember(&mut self, pid: ProcessId, os_pid: u32) {
        if self.by_pid.insert(pid, os_pid).is_some() {
            return;
        }
        self.order.push_back(pid);
        while self.order.len() > OS_PID_HISTORY_BOUND {
            if let Some(old) = self.order.pop_front() {
                self.by_pid.remove(&old);
            }
        }
    }

    fn get(&self, pid: ProcessId) -> Option<u32> {
        self.by_pid.get(&pid).copied()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        debug_assert_eq!(self.by_pid.len(), self.order.len());
        self.by_pid.len()
    }
}

#[cfg(test)]
mod os_pid_history_tests {
    use super::*;

    #[test]
    fn remember_evicts_oldest_without_a_verified_reap() {
        let mut history = OsPidHistory::default();
        let ids: Vec<_> = (0..300).map(ProcessId::new).collect();
        for (index, pid) in ids.iter().copied().enumerate() {
            history.remember(pid, u32::try_from(index).expect("os pid"));
            assert!(history.len() <= OS_PID_HISTORY_BOUND);
        }
        assert_eq!(history.len(), OS_PID_HISTORY_BOUND);
        assert_eq!(history.get(ids[0]), None);
        assert_eq!(history.get(ids[43]), None);
        assert_eq!(history.get(ids[44]), Some(44));
        assert_eq!(history.get(ids[299]), Some(299));
    }
}

// Claims and completion share this lock order; only retained captures consume history.
fn retain_completed_output(inner: &Inner, pid: ProcessId) {
    let mut completed = inner.completed.lock().expect("completed mutex");
    let mut outputs = inner.outputs.lock().expect("outputs mutex");
    if outputs.contains_key(&pid) && !completed.contains(&pid) {
        completed.push_back(pid);
        while completed.len() > 256 {
            if let Some(old) = completed.pop_front() {
                outputs.remove(&old);
            }
        }
    }
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
            Arc::new(WaitNotifierHandler::with_registry(
                waiters.clone(),
                registry.clone(),
            )),
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
                sampler_task: tokio::sync::Mutex::new(None),
                outputs: Mutex::new(HashMap::new()),
                scope_results: Mutex::new(HashMap::new()),
                completed_scope_results: Mutex::new(VecDeque::new()),
                stats_interval: stats_interval.max(Duration::from_millis(1)),
                completed: Mutex::new(VecDeque::new()),
                reports: Mutex::new(HashMap::new()),
                pending_outcomes: Mutex::new(HashMap::new()),
                completed_scopes: Mutex::new(VecDeque::new()),
                shutdown_serial: tokio::sync::Mutex::new(()),
                os_pids: Mutex::new(OsPidHistory::default()),
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

    /// Observe current scope usage without holding cleanup locks. Cleanup may
    /// remove the scope during collection; errors are not cleanup verdicts.
    pub async fn scope_usage(
        &self,
        scope: ProcessScopeId,
    ) -> Result<crate::ScopeUsage, crate::ObservationError> {
        if self.processes(scope).is_none() {
            return Err(crate::ObservationError::Backend("unknown scope".into()));
        }
        self.inner.backend.scope_usage(scope).await
    }

    /// Returns the OS PID of a managed process, for read-only observation.
    /// Shepherd's ProcessId is a logical ID and must not be passed as an OS PID.
    /// This lookup is not proof the process is still alive; it may exit immediately.
    /// The identity stays available after an immediate natural exit until the
    /// same 256-entry post-mortem bound as waiter history. The bound is the last
    /// 256 attachments, including cases where the monitor never records a
    /// verified reap because the registry was already pruned.
    #[must_use]
    pub fn os_pid(&self, pid: ProcessId) -> Option<u32> {
        let registry = self.lock_registry();
        if let Some(os) = registry
            .scope_of(pid)
            .and_then(|scope| registry.get(scope))
            .and_then(|scope| scope.get(pid))
            .map(|process| process.os_identity().pid)
        {
            return Some(os);
        }
        drop(registry);
        self.inner.os_pids.lock().expect("os_pids mutex").get(pid)
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
        self.inner
            .os_pids
            .lock()
            .expect("os_pids mutex")
            .remember(pid, spawned.os.pid);
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
        let backstop = ScopeCleanupBackstop {
            backend: self.inner.backend.clone(),
            scope,
            report: report_tx.clone(),
            armed: true,
        };
        let cleanup = tokio::spawn(async move {
            let mut backstop = backstop;
            let _ = finished.await;
            let retained = backstop.verified_report();
            let result = match retained {
                Some(report) => Ok(report),
                None => worker.terminate_scope(scope, opts).await,
            };
            let result = backstop.verified_report().map(Ok).unwrap_or(result);
            if !result.as_ref().is_ok_and(|r| r.all_verified()) {
                backstop.backend.hard_kill_scope(scope);
            }
            // Publish within this task: the runtime may stop before its observer runs.
            backstop.complete(result);
        });
        tokio::spawn(async move {
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
        if let Some(exit) = self
            .inner
            .waiters
            .try_get(pid)
            .filter(|exit| exit.outcome.is_verified())
        {
            return Ok(exit);
        }

        let (scope, spawned, graceful, request_events, quarantined, mut exit_future) = {
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
                if exit.outcome.is_verified() {
                    return Ok(exit);
                }
            }
            let quarantined = process.exit().is_some();
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
                quarantined,
                self.inner.waiters.wait(pid),
            )
        };
        if quarantined {
            return self
                .retry_quarantined(scope, pid, spawned, graceful, opts)
                .await;
        }
        self.inner.dispatcher.dispatch(&request_events).await;

        if let Some(exit) = self.inner.waiters.try_get(pid) {
            if exit.outcome.is_verified() {
                return Ok(exit);
            }
            return self
                .retry_quarantined(scope, pid, spawned, graceful, opts)
                .await;
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

    // A failed monitor has no future verified waiter notification to await. Retry
    // through the retained backend identity, with at most two fresh wait attempts.
    async fn retry_quarantined(
        &self,
        scope: ProcessScopeId,
        pid: ProcessId,
        spawned: Spawned,
        graceful: Signal,
        opts: TerminateOptions,
    ) -> Result<ProcessExit, TerminateError> {
        let _ = self.inner.backend.signal(&spawned, graceful).await;
        let observe = || {
            std::panic::AssertUnwindSafe(self.inner.backend.wait(&spawned))
                .catch_unwind()
                .map(|result| {
                    result
                        .unwrap_or_else(|_| Err(WaitError::Backend("retry waiter panicked".into())))
                })
                .boxed()
        };
        let mut observation = observe();
        let grace = self.inner.clock.sleep(opts.grace.as_duration());
        tokio::pin!(grace);
        let observed = tokio::select! {
            result = &mut observation => Some(result),
            () = &mut grace => None,
        };
        if let Some(Ok(raw)) = observed {
            return self.record_recovered_exit(scope, pid, raw).await;
        }
        if observed.is_some() {
            // An immediate wait error is not completion and must not skip force.
            grace.await;
        }
        let _ = self.inner.backend.signal(&spawned, Signal::Kill).await;
        if observed.is_some() {
            observation = observe();
        }
        let result = match opts.force_timeout {
            Some(timeout) => tokio::select! {
                result = observation => Some(result),
                () = self.inner.clock.sleep(timeout) => None,
            },
            None => Some(observation.await),
        };
        match result {
            Some(Ok(raw)) => self.record_recovered_exit(scope, pid, raw).await,
            other => Ok(ProcessExit {
                pid,
                code: None,
                signal: None,
                forced: true,
                outcome: TerminationOutcome::CleanupUnverified(match other {
                    Some(Err(_)) => UnverifiedReason::ReapFailed,
                    None => UnverifiedReason::WaitTimedOut {
                        waited: opts.force_timeout.expect("bounded wait"),
                    },
                    Some(Ok(_)) => unreachable!(),
                }),
            }),
        }
    }

    async fn record_recovered_exit(
        &self,
        scope: ProcessScopeId,
        pid: ProcessId,
        raw: shepherd_domain::RawExit,
    ) -> Result<ProcessExit, TerminateError> {
        let killed = raw.signal == Some(Signal::Kill);
        let mut exit = ProcessExit {
            pid,
            code: raw.code,
            signal: raw.signal,
            forced: killed,
            outcome: if killed {
                TerminationOutcome::ForcedRequired
            } else {
                TerminationOutcome::GracefulSuccess
            },
        };
        let events = {
            let mut registry = self.lock_registry();
            let mut events = Vec::new();
            if let Some(s) = registry.get_mut(scope) {
                if let Some(previous) = s
                    .get(pid)
                    .and_then(|process| process.exit())
                    .filter(|exit| exit.outcome.is_verified())
                {
                    exit = previous;
                } else if s.get(pid).is_some() {
                    events = s.record_reaped(pid, exit)?;
                    if !s.is_open() {
                        self.inner
                            .pending_outcomes
                            .lock()
                            .expect("pending outcomes mutex")
                            .entry(scope)
                            .or_default()
                            .insert(pid, exit.outcome);
                    }
                }
            }
            retain_completed_output(&self.inner, pid);
            // Publish the correction before cancellation can interrupt dispatch/prune.
            // The Waiters contract prevents a delayed old failure from downgrading it.
            self.inner.waiters.signal_exit(pid, exit);
            events
                .retain(|event| !matches!(event, shepherd_domain::DomainEvent::ScopeClosed { .. }));
            events
        };
        self.inner.dispatcher.dispatch(&events).await;
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
            let processes = s
                .process_ids()
                .into_iter()
                .map(|pid| {
                    let observer: crate::ports::WaitFuture = match s.get(pid).and_then(|p| p.exit())
                    {
                        Some(exit) => Box::pin(std::future::ready(exit)),
                        None => self.inner.waiters.wait(pid),
                    };
                    (pid, observer)
                })
                .collect::<Vec<_>>();
            (events, processes)
        };
        events.retain(|event| !matches!(event, shepherd_domain::DomainEvent::ScopeClosed { .. }));
        self.inner.dispatcher.dispatch(&events).await;

        let mut set = tokio::task::JoinSet::new();
        for (pid, observer) in live {
            let this = self.worker();
            set.spawn(async move {
                let termination = this.terminate(pid, opts);
                tokio::pin!(termination);
                let result = tokio::select! { biased;
                    exit = observer => {
                        if exit.outcome.is_verified() {
                            Ok(exit)
                        } else {
                            termination.await
                        }
                    },
                    result = &mut termination => result,
                };
                (pid, result)
            });
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

        // Keep a verified monitor result if it arrived while a retry observer waited.
        {
            let mut pending = self
                .inner
                .pending_outcomes
                .lock()
                .expect("pending outcomes mutex");
            let retained = pending.entry(scope).or_default();
            for (pid, outcome) in outcomes {
                retained
                    .entry(pid)
                    .and_modify(|previous| {
                        if !previous.is_verified() {
                            *previous = outcome;
                        }
                    })
                    .or_insert(outcome);
            }
            outcomes = retained
                .iter()
                .map(|(pid, outcome)| (*pid, *outcome))
                .collect();
        }
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
            // `os_pid` history is bounded at attach: this path often runs after
            // terminate_scope has already dropped the registry entry.
            if verified_reap {
                retain_completed_output(&inner, pid);
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
impl ScopeCleanupBackstop {
    fn verified_report(&self) -> Option<ScopeTerminationReport> {
        self.report
            .borrow()
            .as_ref()
            .and_then(|result| result.as_ref().ok())
            .filter(|report| report.all_verified())
            .cloned()
    }
    fn complete(&mut self, result: Result<ScopeTerminationReport, TerminateError>) {
        // No suspension point may separate result publication from disarming.
        publish_scope_cleanup_result(&self.report, result);
        self.armed = false;
    }
}
impl Drop for ScopeCleanupBackstop {
    fn drop(&mut self) {
        if self.armed && self.verified_report().is_none() {
            self.backend.hard_kill_scope(self.scope);
            publish_scope_cleanup_result(
                &self.report,
                Err(TerminateError::Signal(
                    "scope cleanup interrupted or unverified; synchronous backstop issued".into(),
                )),
            );
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
    #[derive(Default)]
    struct RecoveringBackend {
        panic_attempts: u32,
        waits: AtomicU32,
        signals: Mutex<Vec<Signal>>,
        released: tokio::sync::Notify,
    }
    #[async_trait]
    impl ProcessBackend for RecoveringBackend {
        async fn spawn(&self, _: ProcessScopeId, _: &ProcessSpec) -> Result<Spawned, SpawnError> {
            Ok(Spawned {
                os: shepherd_domain::OsIdentity::new(
                    42,
                    shepherd_domain::ReuseToken::StartTime(42),
                ),
            })
        }
        async fn signal(&self, _: &Spawned, signal: Signal) -> Result<(), TerminateError> {
            self.signals.lock().unwrap().push(signal);
            self.released.notify_one();
            Ok(())
        }
        async fn signal_scope(&self, _: ProcessScopeId, _: Signal) -> Result<(), TerminateError> {
            Ok(())
        }
        async fn wait(&self, _: &Spawned) -> Result<shepherd_domain::RawExit, WaitError> {
            if self.waits.fetch_add(1, Ordering::SeqCst) < self.panic_attempts {
                panic!("injected backend wait panic");
            }
            self.released.notified().await;
            Ok(shepherd_domain::RawExit {
                code: None,
                signal: Some(Signal::Term),
                core_dumped: false,
            })
        }
        async fn sample(&self, _: &Spawned) -> Result<shepherd_domain::RawStats, StatsError> {
            Err(StatsError::Backend("unused".into()))
        }
        fn capabilities(&self) -> shepherd_domain::Capabilities {
            unreachable!()
        }
        fn hard_kill_all(&self) {}
        fn hard_kill_scope(&self, _: ProcessScopeId) {}
    }

    async fn run_scope_retry(panic_attempts: u32) {
        let backend = Arc::new(RecoveringBackend {
            panic_attempts,
            ..Default::default()
        });
        let ports = Arc::new(HistoryPorts::default());
        let supervisor =
            ProcessSupervisor::new(backend.clone(), ports.clone(), ports.clone(), ports);
        let scope = supervisor.create_scope();
        let pid = supervisor
            .spawn(scope, ProcessSpec::new("recovering"))
            .await
            .unwrap();
        let initial = supervisor.wait(pid).await.unwrap();
        assert_eq!(
            initial.outcome,
            TerminationOutcome::CleanupUnverified(UnverifiedReason::ReapFailed)
        );
        let report = tokio::time::timeout(
            Duration::from_secs(2),
            supervisor.terminate_scope(
                scope,
                TerminateOptions {
                    grace: shepherd_domain::GracePeriod::new(Duration::from_millis(1)),
                    force_timeout: Some(Duration::from_millis(100)),
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.all_verified(), panic_attempts < 3);
        assert!(backend.waits.load(Ordering::SeqCst) >= 2);
        let signals = backend.signals.lock().unwrap().clone();
        assert_eq!(signals.first(), Some(&Signal::Term));
        if panic_attempts > 1 {
            assert!(
                signals.contains(&Signal::Kill),
                "panic skipped forced attempt"
            );
        }
        assert_eq!(
            supervisor.wait(pid).await.unwrap().outcome.is_verified(),
            panic_attempts < 3
        );
    }

    #[tokio::test]
    async fn scope_retry_does_not_short_circuit_on_cached_failed_reap() {
        run_scope_retry(1).await;
    }

    #[tokio::test]
    async fn scope_retry_catches_panics_in_both_fresh_wait_attempts() {
        run_scope_retry(2).await;
        run_scope_retry(3).await;
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
                latest
            })
            .await;
        let latest = result.result.expect("scoped body returned the latest pid");
        assert!(!result.termination.unwrap().all_verified());
        assert!(
            supervisor.inner.os_pids.lock().unwrap().len() <= OS_PID_HISTORY_BOUND,
            "os_pid history must stay bounded across 650 immediate roots"
        );
        assert!(supervisor.os_pid(latest).is_some());
    }

    #[tokio::test]
    async fn os_pid_history_stays_bounded_when_monitor_loses_the_registry() {
        let ports = Arc::new(HistoryPorts::default());
        let supervisor = ProcessSupervisor::new(ports.clone(), ports.clone(), ports.clone(), ports);
        // HistoryPorts treats OS pid 1 as a quarantined wait failure.
        let discarded = supervisor.create_scope();
        let _ = supervisor
            .spawn(discarded, ProcessSpec::new("unverified"))
            .await
            .unwrap();
        let _ = supervisor
            .terminate_scope(discarded, TerminateOptions::default())
            .await;
        let mut first = None;
        let mut latest = None;
        for _ in 0..300 {
            let scope = supervisor.create_scope();
            let pid = supervisor
                .spawn(scope, ProcessSpec::new("immediate"))
                .await
                .unwrap();
            first.get_or_insert(pid);
            latest = Some(pid);
            assert!(supervisor.os_pid(pid).is_some());
            assert!(supervisor.wait(pid).await.unwrap().outcome.is_verified());
            assert!(supervisor
                .terminate_scope(scope, TerminateOptions::default())
                .await
                .unwrap()
                .all_verified());
            assert!(
                supervisor.inner.os_pids.lock().unwrap().len() <= OS_PID_HISTORY_BOUND,
                "os_pid history grew past the attach bound"
            );
        }
        assert!(supervisor.os_pid(latest.unwrap()).is_some());
        assert_eq!(supervisor.os_pid(first.unwrap()), None);
        assert_eq!(
            supervisor.inner.os_pids.lock().unwrap().len(),
            OS_PID_HISTORY_BOUND
        );
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
        fn hard_kill_scope(&self, _: ProcessScopeId) {}
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
        let mut completed = ScopeCleanupBackstop {
            backend: supervisor.inner.backend.clone(),
            scope,
            report: sender.clone(),
            armed: true,
        };
        completed.complete(Err(TerminateError::UnknownScope(scope)));
        drop(completed);
        drop(ScopeCleanupBackstop {
            backend: supervisor.inner.backend.clone(),
            scope,
            report: sender.clone(),
            armed: true,
        });
        assert!(receiver
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .all_verified());
    }

    #[tokio::test]
    async fn ordinary_scope_history_does_not_evict_scoped_cleanup_results() {
        let supervisor = supervisor();
        let first = supervisor.with_scope(Vec::new(), |_| async {}).await;
        assert!(first.termination.unwrap().all_verified());
        for _ in 0..650 {
            let scope = supervisor.create_scope();
            supervisor
                .terminate_scope(scope, Default::default())
                .await
                .unwrap();
        }
        assert!(supervisor
            .wait_scope_cleanup(first.scope)
            .await
            .unwrap()
            .all_verified());
        assert_eq!(
            supervisor
                .inner
                .completed_scope_results
                .lock()
                .unwrap()
                .len(),
            1
        );
        for _ in 0..256 {
            assert!(supervisor
                .with_scope(Vec::new(), |_| async {})
                .await
                .termination
                .unwrap()
                .all_verified());
        }
        assert!(
            matches!(supervisor.wait_scope_cleanup(first.scope).await, Err(TerminateError::UnknownScope(id)) if id == first.scope)
        );
        assert_eq!(supervisor.inner.scope_results.lock().unwrap().len(), 256);
        assert_eq!(
            supervisor
                .inner
                .completed_scope_results
                .lock()
                .unwrap()
                .len(),
            256
        );
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
                let receiver = external
                    .inner
                    .scope_results
                    .lock()
                    .unwrap()
                    .get(&scope.id())
                    .unwrap()
                    .subscribe();
                for _ in 0..650 {
                    assert!(external
                        .with_scope(Vec::new(), |_| async {})
                        .await
                        .termination
                        .unwrap()
                        .all_verified());
                }
                assert!(!external
                    .inner
                    .reports
                    .lock()
                    .unwrap()
                    .contains_key(&scope.id()));
                assert!(!external
                    .inner
                    .scope_results
                    .lock()
                    .unwrap()
                    .contains_key(&scope.id()));
                (42, receiver)
            })
            .await;
        let (value, receiver) = result.result.unwrap();
        assert_eq!(value, 42);
        assert!(result.termination.unwrap().all_verified());
        // The worker owns Inner until it finishes; no lookup entry remains to help it.
        tokio::time::timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&supervisor.inner) != 1 {
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
    fn completed_cleanup_is_observable_after_runtime_drop_without_join_observer() {
        let supervisor = supervisor();
        let scope = supervisor.create_scope();
        let (sender, _) = tokio::sync::watch::channel(None);
        supervisor
            .inner
            .scope_results
            .lock()
            .unwrap()
            .insert(scope, sender.clone());
        let worker = supervisor.worker();
        let mut backstop = ScopeCleanupBackstop {
            backend: supervisor.inner.backend.clone(),
            scope,
            report: sender,
            armed: true,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Run the worker completion operation without ever scheduling a join observer.
        runtime.block_on(async move {
            tokio::spawn(async move {
                let result = worker.terminate_scope(scope, Default::default()).await;
                backstop.complete(result);
            })
            .await
            .unwrap();
        });
        drop(runtime);
        let next = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = next.block_on(async {
            tokio::time::timeout(
                Duration::from_millis(50),
                supervisor.wait_scope_cleanup(scope),
            )
            .await
            .expect("worker completed without publishing its result")
            .unwrap()
        });
        assert_eq!(report.scope, scope);
        assert!(report.all_verified());
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
        fn hard_kill_scope(&self, _: ProcessScopeId) {}
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

    #[tokio::test]
    async fn shutdown_releases_supervisor_sampler_and_backend_owners() {
        let ports = Arc::new(Ports {
            fail_cleanup: AtomicBool::new(false),
        });
        let backend = Arc::downgrade(&ports);
        let sup = ProcessSupervisor::new(ports.clone(), ports.clone(), ports.clone(), ports);
        let inner = Arc::downgrade(&sup.inner);
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
        let clone = sup.clone();
        sup.shutdown().await.unwrap();
        assert!(sampler.is_finished());
        drop(sampler);
        drop(sup);
        assert!(
            inner.upgrade().is_some(),
            "positive control must retain the owner"
        );
        drop(clone);
        assert!(
            inner.upgrade().is_none(),
            "sampler retained supervisor state"
        );
        assert!(
            backend.upgrade().is_none(),
            "shutdown retained backend ownership"
        );
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
