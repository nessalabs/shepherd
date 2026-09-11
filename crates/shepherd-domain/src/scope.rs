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
            if matches!(self.state, ScopeState::Draining) && !self.has_live_processes() {
                self.state = ScopeState::Closed;
                events.push(DomainEvent::ScopeClosed { scope });
            }
        }
        Ok(events)
    }

    /// Removes a reaped process from bookkeeping (invariant #9). Callers prune only after
    /// the terminal outcome has been observed/delivered.
    pub fn prune(&mut self, pid: ProcessId) {
        if self
            .processes
            .get(&pid)
            .is_some_and(|p| p.state().is_terminal())
        {
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
        assert!(
            second.is_empty(),
            "second request must not re-emit an event"
        );
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
    }
}
