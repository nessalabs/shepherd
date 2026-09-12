//! The `ProcessScope` aggregate root — the unit of ownership and consistency boundary.

use std::collections::BTreeMap;

use crate::error::DomainError;
use crate::event::DomainEvent;
use crate::exit::ProcessExit;
use crate::ids::{ProcessId, ProcessScopeId};
use crate::lifecycle::ScopeState;
use crate::os::OsIdentity;
use crate::process::Process;
use crate::spec::ProcessSpec;

/// The aggregate root for a set of supervised processes sharing one lifetime.
///
/// All mutation of the contained [`Process`] entities flows through this root, which
/// enforces the ownership invariants:
///
/// * a process belongs to exactly one scope and cannot move;
/// * a draining/closed scope never accepts new processes;
/// * terminating this scope can never affect another scope (a scope only ever touches
///   processes in its own map).
///
/// Methods **return** [`DomainEvent`]s rather than dispatching them, keeping the domain pure.
#[derive(Debug)]
pub struct ProcessScope {
    id: ProcessScopeId,
    state: ScopeState,
    processes: BTreeMap<ProcessId, Process>,
}

impl ProcessScope {
    /// Creates a new, open scope.
    #[must_use]
    pub fn new(id: ProcessScopeId) -> Self {
        Self {
            id,
            state: ScopeState::Open,
            processes: BTreeMap::new(),
        }
    }

    /// The scope's identity.
    #[must_use]
    pub fn id(&self) -> ProcessScopeId {
        self.id
    }

    /// The current scope state.
    #[must_use]
    pub fn state(&self) -> ScopeState {
        self.state
    }

    /// Whether the scope will accept new processes.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.state.accepts_processes()
    }

    /// Whether the scope is closed (terminal).
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(self.state, ScopeState::Closed)
    }

    /// Borrows a contained process, if present.
    #[must_use]
    pub fn get(&self, pid: ProcessId) -> Option<&Process> {
        self.processes.get(&pid)
    }

    /// Whether the scope owns `pid`.
    #[must_use]
    pub fn contains(&self, pid: ProcessId) -> bool {
        self.processes.contains_key(&pid)
    }

    /// All process ids currently tracked (including terminal ones not yet pruned).
    #[must_use]
    pub fn process_ids(&self) -> Vec<ProcessId> {
        self.processes.keys().copied().collect()
    }

    /// Ids of processes that are still alive.
    #[must_use]
    pub fn live_process_ids(&self) -> Vec<ProcessId> {
        self.processes
            .values()
            .filter(|p| p.state().is_live())
            .map(Process::id)
            .collect()
    }

    /// Whether any tracked process is still alive.
    #[must_use]
    pub fn has_live_processes(&self) -> bool {
        self.processes.values().any(|p| p.state().is_live())
    }

    /// Attaches a freshly spawned process to this scope.
    ///
    /// # Errors
    /// Returns [`DomainError::ScopeClosed`] if the scope is not open (invariant #3).
    pub fn attach_spawned(
        &mut self,
        pid: ProcessId,
        os: OsIdentity,
        spec: ProcessSpec,
    ) -> Result<DomainEvent, DomainError> {
        if !self.state.accepts_processes() {
            return Err(DomainError::ScopeClosed(self.id));
        }
        if self.processes.contains_key(&pid) {
            return Err(crate::InvalidTransition {
                from: "owned",
                event: "attach duplicate process",
            }
            .into());
        }
        self.processes.insert(pid, Process::running(pid, os, spec));
        Ok(DomainEvent::ProcessSpawned {
            scope: self.id,
            pid,
        })
    }

    /// Requests graceful termination of a single owned process.
    ///
    /// Returns the event only when this call actually initiated termination (idempotent).
    ///
    /// # Errors
    /// Returns [`DomainError::UnknownProcess`] if `pid` is not owned by this scope.
    pub fn request_termination(&mut self, pid: ProcessId) -> Result<Vec<DomainEvent>, DomainError> {
        let scope = self.id;
        let process = self
            .processes
            .get_mut(&pid)
            .ok_or(DomainError::UnknownProcess(pid))?;
        Ok(if process.request_graceful() {
            vec![DomainEvent::TerminationRequested { scope, pid }]
        } else {
            Vec::new()
        })
    }

    /// Escalates a single owned process to forceful termination.
    ///
    /// # Errors
    /// Returns [`DomainError::UnknownProcess`] if `pid` is not owned by this scope.
    pub fn escalate_termination(&mut self, pid: ProcessId) -> Result<(), DomainError> {
        let process = self
            .processes
            .get_mut(&pid)
            .ok_or(DomainError::UnknownProcess(pid))?;
        process.escalate_to_force();
        Ok(())
    }

    /// Begins terminating the whole scope: transitions to draining and requests graceful
    /// termination of every live process. This only ever touches this scope's own processes.
    pub fn begin_scope_termination(&mut self) -> Vec<DomainEvent> {
        if matches!(self.state, ScopeState::Open) {
            self.state = ScopeState::Draining;
        }
        let scope = self.id;
        let mut events = Vec::new();
        for process in self.processes.values_mut() {
            if process.request_graceful() {
                events.push(DomainEvent::TerminationRequested {
                    scope,
                    pid: process.id(),
                });
            }
        }
        events
    }

    /// Records that an owned process was observed to have exited.
    ///
    /// # Errors
    /// Returns [`DomainError::UnknownProcess`] if `pid` is not owned by this scope.
    pub fn record_exit(&mut self, pid: ProcessId) -> Result<Vec<DomainEvent>, DomainError> {
        let scope = self.id;
        let process = self
            .processes
            .get_mut(&pid)
            .ok_or(DomainError::UnknownProcess(pid))?;
        Ok(if process.mark_exited() {
            vec![DomainEvent::ProcessExited { scope, pid }]
        } else {
            Vec::new()
        })
    }

    /// Records that an owned process was reaped, storing its terminal exit. Closes the scope
    /// if it is draining and no live processes remain.
    ///
    /// # Errors
    /// Returns [`DomainError::UnknownProcess`] if `pid` is not owned by this scope.
    pub fn record_reaped(
        &mut self,
        pid: ProcessId,
        exit: ProcessExit,
    ) -> Result<Vec<DomainEvent>, DomainError> {
        let scope = self.id;
        let process = self
            .processes
            .get_mut(&pid)
            .ok_or(DomainError::UnknownProcess(pid))?;
        let mut events = Vec::new();
        if process.mark_reaped(exit) {
            events.push(DomainEvent::ProcessReaped { scope, pid, exit });
            if matches!(self.state, ScopeState::Draining)
                && !self.has_live_processes()
                && self
                    .processes
                    .values()
                    .all(|p| p.exit().is_some_and(|e| e.outcome.is_verified()))
            {
                self.state = ScopeState::Closed;
                events.push(DomainEvent::ScopeClosed { scope });
            }
        }
        Ok(events)
    }

    /// Removes a reaped process from bookkeeping (invariant #9). Callers prune only after
    /// the terminal outcome has been observed/delivered.
    pub fn prune(&mut self, pid: ProcessId) {
        if self.processes.get(&pid).is_some_and(|p| {
            p.state().is_terminal() && p.exit().is_some_and(|e| e.outcome.is_verified())
        }) {
            self.processes.remove(&pid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exit::TerminationOutcome;
    use crate::os::ReuseToken;

    fn os(pid: u32) -> OsIdentity {
        OsIdentity::new(pid, ReuseToken::Unavailable)
    }

    fn spec() -> ProcessSpec {
        ProcessSpec::new("dummy")
    }

    fn exit(pid: ProcessId, outcome: TerminationOutcome) -> ProcessExit {
        ProcessExit {
            pid,
            code: Some(0),
            signal: None,
            outcome,
            forced: false,
        }
    }

    #[test]
    fn attach_emits_spawned_and_tracks_process() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        let event = scope.attach_spawned(pid, os(100), spec()).unwrap();
        assert_eq!(
            event,
            DomainEvent::ProcessSpawned {
                scope: scope.id(),
                pid
            }
        );
        assert!(scope.contains(pid));
        assert_eq!(scope.live_process_ids(), vec![pid]);
    }

    #[test]
    fn draining_scope_rejects_new_processes() {
        // invariant #3
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        scope.attach_spawned(pid, os(100), spec()).unwrap();
        scope.begin_scope_termination();
        let err = scope
            .attach_spawned(ProcessId::new(11), os(101), spec())
            .unwrap_err();
        assert_eq!(err, DomainError::ScopeClosed(scope.id()));
    }

    #[test]
    fn repeated_termination_is_idempotent() {
        // invariant #6
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        scope.attach_spawned(pid, os(100), spec()).unwrap();
        let first = scope.request_termination(pid).unwrap();
        assert_eq!(first.len(), 1);
        let second = scope.request_termination(pid).unwrap();
        assert!(second.is_empty());
    }

    #[test]
    fn reaping_last_draining_process_closes_scope() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        scope.attach_spawned(pid, os(100), spec()).unwrap();
        scope.begin_scope_termination();
        scope.record_exit(pid).unwrap();
        let events = scope
            .record_reaped(pid, exit(pid, TerminationOutcome::GracefulSuccess))
            .unwrap();
        assert!(events.contains(&DomainEvent::ProcessReaped {
            scope: scope.id(),
            pid,
            exit: exit(pid, TerminationOutcome::GracefulSuccess)
        }));
        assert!(events.contains(&DomainEvent::ScopeClosed { scope: scope.id() }));
        assert!(scope.is_closed());
    }

    #[test]
    fn reaped_process_can_be_pruned_but_live_cannot() {
        // invariant #9
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        scope.attach_spawned(pid, os(100), spec()).unwrap();
        scope.prune(pid);
        assert!(scope.contains(pid), "must not prune a live process");
        scope.record_exit(pid).unwrap();
        scope
            .record_reaped(pid, exit(pid, TerminationOutcome::ExitedNaturally))
            .unwrap();
        scope.prune(pid);
        assert!(!scope.contains(pid), "reaped process should be pruned");
    }

    #[test]
    fn unknown_process_is_rejected() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let missing = ProcessId::new(999);
        assert_eq!(
            scope.request_termination(missing).unwrap_err(),
            DomainError::UnknownProcess(missing)
        );
        assert_eq!(
            scope.escalate_termination(missing).unwrap_err(),
            DomainError::UnknownProcess(missing)
        );
        assert_eq!(
            scope.record_exit(missing).unwrap_err(),
            DomainError::UnknownProcess(missing)
        );
        assert_eq!(
            scope
                .record_reaped(missing, exit(missing, TerminationOutcome::Failed))
                .unwrap_err(),
            DomainError::UnknownProcess(missing)
        );
    }

    #[test]
    fn new_scope_is_open_and_empty() {
        let scope = ProcessScope::new(ProcessScopeId::new(1));
        assert!(scope.is_open());
        assert!(!scope.is_closed());
        assert!(scope.process_ids().is_empty());
        assert!(scope.live_process_ids().is_empty());
        assert!(!scope.has_live_processes());
        assert_eq!(scope.get(ProcessId::new(1)), None);
    }

    #[test]
    fn begin_scope_termination_is_idempotent() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        scope.attach_spawned(pid, os(100), spec()).unwrap();
        let first = scope.begin_scope_termination();
        assert_eq!(first.len(), 1);
        let second = scope.begin_scope_termination();
        assert!(second.is_empty());
        assert_eq!(scope.state(), ScopeState::Draining);
    }

    #[test]
    fn terminating_empty_scope_leaves_it_draining() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let events = scope.begin_scope_termination();
        assert!(events.is_empty());
        assert_eq!(scope.state(), ScopeState::Draining);
        assert!(!scope.is_open());
    }

    #[test]
    fn record_exit_is_idempotent() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        scope.attach_spawned(pid, os(100), spec()).unwrap();
        assert_eq!(scope.record_exit(pid).unwrap().len(), 1);
        assert!(scope.record_exit(pid).unwrap().is_empty());
    }

    #[test]
    fn reap_event_is_emitted_at_most_once() {
        // invariant #7
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        scope.attach_spawned(pid, os(100), spec()).unwrap();
        let first = scope
            .record_reaped(pid, exit(pid, TerminationOutcome::ForcedRequired))
            .unwrap();
        assert_eq!(first.len(), 1);
        let second = scope
            .record_reaped(pid, exit(pid, TerminationOutcome::ForcedRequired))
            .unwrap();
        assert!(second.is_empty(), "reaping twice emits no duplicate event");
    }

    #[test]
    fn escalate_termination_succeeds_for_owned_process() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        scope.attach_spawned(pid, os(100), spec()).unwrap();
        scope.escalate_termination(pid).unwrap();
        assert!(scope.get(pid).unwrap().was_forced());
    }

    #[test]
    fn terminating_a_scope_never_touches_another_scope() {
        // invariant #5 at the aggregate level: a scope only ever holds its own processes.
        let mut a = ProcessScope::new(ProcessScopeId::new(1));
        let mut b = ProcessScope::new(ProcessScopeId::new(2));
        let pa = ProcessId::new(10);
        let pb = ProcessId::new(20);
        a.attach_spawned(pa, os(100), spec()).unwrap();
        b.attach_spawned(pb, os(200), spec()).unwrap();

        a.begin_scope_termination();
        // B is entirely unaffected: still open, still owns its live process.
        assert!(b.is_open());
        assert_eq!(b.live_process_ids(), vec![pb]);
        assert!(!a.contains(pb));
        assert!(!b.contains(pa));
    }

    #[test]
    fn draining_scope_with_two_processes_closes_only_after_both_reaped() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let p1 = ProcessId::new(10);
        let p2 = ProcessId::new(11);
        scope.attach_spawned(p1, os(100), spec()).unwrap();
        scope.attach_spawned(p2, os(101), spec()).unwrap();
        scope.begin_scope_termination();

        let e1 = scope
            .record_reaped(p1, exit(p1, TerminationOutcome::GracefulSuccess))
            .unwrap();
        assert!(!e1
            .iter()
            .any(|e| matches!(e, DomainEvent::ScopeClosed { .. })));
        assert!(!scope.is_closed());

        let e2 = scope
            .record_reaped(p2, exit(p2, TerminationOutcome::GracefulSuccess))
            .unwrap();
        assert!(e2
            .iter()
            .any(|e| matches!(e, DomainEvent::ScopeClosed { .. })));
        assert!(scope.is_closed());
    }

    #[test]
    fn closed_scope_rejects_new_spawns() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(10);
        scope.attach_spawned(pid, os(100), spec()).unwrap();
        scope.begin_scope_termination();
        scope
            .record_reaped(pid, exit(pid, TerminationOutcome::GracefulSuccess))
            .unwrap();
        assert!(scope.is_closed());
        assert_eq!(
            scope
                .attach_spawned(ProcessId::new(11), os(101), spec())
                .unwrap_err(),
            DomainError::ScopeClosed(scope.id())
        );
    }
}

#[cfg(test)]
mod adversarial_tests {
    use super::*;
    use crate::{ReuseToken, TerminationOutcome, UnverifiedReason};
    #[test]
    fn duplicate_attachment_cannot_replace_a_live_process() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(1);
        let os = OsIdentity::new(1, ReuseToken::StartTime(1));
        scope
            .attach_spawned(pid, os, ProcessSpec::new("original"))
            .unwrap();
        assert!(scope
            .attach_spawned(pid, os, ProcessSpec::new("replacement"))
            .is_err());
        assert_eq!(scope.get(pid).unwrap().spec().program, "original");
    }
    #[test]
    fn unverified_exit_is_quarantined_not_pruned_or_closed() {
        let mut scope = ProcessScope::new(ProcessScopeId::new(1));
        let pid = ProcessId::new(1);
        scope
            .attach_spawned(
                pid,
                OsIdentity::new(1, ReuseToken::Unavailable),
                ProcessSpec::new("owned"),
            )
            .unwrap();
        scope.begin_scope_termination();
        scope
            .record_reaped(
                pid,
                ProcessExit {
                    pid,
                    code: None,
                    signal: None,
                    forced: false,
                    outcome: TerminationOutcome::CleanupUnverified(UnverifiedReason::ReapFailed),
                },
            )
            .unwrap();
        scope.prune(pid);
        assert!(scope.contains(pid));
        assert!(!scope.is_closed());
    }
}
