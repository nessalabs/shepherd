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
    cleanup: Arc<CleanupGuard>,
}

impl Clone for ProcessSupervisor {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            cleanup: Arc::clone(&self.cleanup),
        }
    }
}

/// Drops with the last user-facing [`ProcessSupervisor`] handle and hard-kills remaining
/// work. Shared with [`Inner::shutting_down`] so an explicit shutdown is not warned as a leak.
struct CleanupGuard {
    backend: Arc<dyn ProcessBackend>,
    shutting_down: Arc<AtomicBool>,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if !self.shutting_down.load(Ordering::SeqCst) {
            tracing::warn!(
                "ProcessSupervisor dropped without shutdown; issuing unverified hard-kill"
            );
        }
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
    scope_operations: Mutex<HashMap<ProcessScopeId, Arc<tokio::sync::Mutex<()>>>>,
    samples: Mutex<HashMap<ProcessId, Result<ProcessStats, StatsError>>>,
    sampler_started: AtomicBool,
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
                sampler_task: tokio::sync::Mutex::new(None),
                stats_interval: stats_interval.max(Duration::from_millis(1)),
                reports: Mutex::new(HashMap::new()),
                pending_outcomes: Mutex::new(HashMap::new()),
                completed_scopes: Mutex::new(VecDeque::new()),
                shutdown_serial: tokio::sync::Mutex::new(()),
                shutting_down: Arc::clone(&shutting_down),
            }),
            cleanup: Arc::new(CleanupGuard {
                backend,
                shutting_down,
            }),
        }
    }

    /// Creates a new, open scope and returns its id.
    pub fn create_scope(&self) -> ProcessScopeId {
        self.create_scope_before_publish(|| {})
    }

    // The callback lets the concurrency regression pause at the publication boundary.
    fn create_scope_before_publish(&self, before_publish: impl FnOnce()) -> ProcessScopeId {
        let mut registry = self.lock_registry();
        let scope = registry.create_scope();
        before_publish();
        // Registry -> operation map is the shared lock order. Do not expose the new
        // scope to shutdown before its serialization lock exists.
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
        // Start ownership monitoring before any cancellable dispatch.
        self.start_monitor(scope, pid, spawned);
        self.start_sampler();
        self.inner.dispatcher.dispatch(&events).await;
        Ok(pid)
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
        let (mut events, live) = {
            let mut registry = self.lock_registry();
            let s = registry
                .get_mut(scope)
                .ok_or(TerminateError::UnknownScope(scope))?;
            (s.begin_scope_termination(), s.process_ids())
        };
        events.retain(|event| !matches!(event, shepherd_domain::DomainEvent::ScopeClosed { .. }));
        self.inner.dispatcher.dispatch(&events).await;

        let mut set = tokio::task::JoinSet::new();
        for pid in live {
            let this = self.clone();
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
            inner.dispatcher.dispatch(&events).await;
            inner
                .spawn_times
                .lock()
                .expect("spawn_times mutex")
                .remove(&pid);
        });
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

#[cfg(test)]
mod scope_operation_tests {
    use super::*;
    use async_trait::async_trait;
    use std::future::Future;
    use std::task::Poll;
    use std::time::Duration;

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

    #[test]
    fn shutdown_cannot_observe_scope_before_its_operation_lock_is_published() {
        let supervisor = supervisor();
        let creator = supervisor.clone();
        let (inserted, insertion) = std::sync::mpsc::sync_channel(0);
        let (release, released) = std::sync::mpsc::sync_channel(0);
        let creation = std::thread::spawn(move || {
            creator.create_scope_before_publish(|| {
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
        let mut spawn = std::pin::pin!(supervisor.spawn(scope, ProcessSpec::new("unused")));
        std::future::poll_fn(|cx| {
            assert!(cleanup.as_mut().poll(cx).is_pending());
            assert!(spawn.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(held);
        assert!(cleanup.await.unwrap().all_verified());
        assert!(supervisor.scope_operation(scope).is_none());
        assert!(matches!(spawn.await, Err(SpawnError::ScopeClosed(id)) if id == scope));
        assert!(supervisor.scope_operation(scope).is_none());
    }
}
