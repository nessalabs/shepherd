//! The `ScopeRegistry` repository — collection-style access to scope aggregates.

use std::collections::BTreeMap;

use shepherd_domain::{ProcessId, ProcessScope, ProcessScopeId};

/// In-memory repository of [`ProcessScope`] aggregates held by a supervisor.
///
/// Not thread-safe on its own; the supervisor guards it with a mutex that is never held
/// across an `.await`.
#[derive(Debug, Default)]
pub struct ScopeRegistry {
    scopes: BTreeMap<ProcessScopeId, ProcessScope>,
    next_scope: u64,
    next_process: u64,
}

impl ScopeRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mints a fresh, unique scope id.
    pub fn next_scope_id(&mut self) -> ProcessScopeId {
        self.next_scope += 1;
        ProcessScopeId::new(self.next_scope)
    }

    /// Mints a fresh, unique process id.
    pub fn next_process_id(&mut self) -> ProcessId {
        self.next_process += 1;
        ProcessId::new(self.next_process)
    }

    /// Inserts a new scope and returns its id.
    pub fn create_scope(&mut self) -> ProcessScopeId {
        let id = self.next_scope_id();
        self.scopes.insert(id, ProcessScope::new(id));
        id
    }

    /// Borrows a scope.
    #[must_use]
    pub fn get(&self, id: ProcessScopeId) -> Option<&ProcessScope> {
        self.scopes.get(&id)
    }

    /// Mutably borrows a scope.
    pub fn get_mut(&mut self, id: ProcessScopeId) -> Option<&mut ProcessScope> {
        self.scopes.get_mut(&id)
    }

    /// Removes a scope from the registry.
    pub fn remove(&mut self, id: ProcessScopeId) {
        self.scopes.remove(&id);
    }

    /// All scope ids currently tracked.
    #[must_use]
    pub fn scope_ids(&self) -> Vec<ProcessScopeId> {
        self.scopes.keys().copied().collect()
    }

    /// Finds the scope that owns `pid`, if any.
    #[must_use]
    pub fn scope_of(&self, pid: ProcessId) -> Option<ProcessScopeId> {
        self.scopes
            .iter()
            .find(|(_, scope)| scope.contains(pid))
            .map(|(id, _)| *id)
    }
}

#[cfg(test)]
mod loom_tests {
    use super::*;
    use loom::sync::{Arc, Mutex};
    use shepherd_domain::{OsIdentity, ProcessSpec, ReuseToken};
    #[test]
    fn loom_registry_spawn_vs_close_and_unique_ids() {
        loom::model(|| {
            let mut registry = ScopeRegistry::new();
            let scope = registry.create_scope();
            let registry = Arc::new(Mutex::new(registry));
            let first = registry.clone();
            let spawn = loom::thread::spawn(move || {
                let mut r = first.lock().unwrap();
                let pid = r.next_process_id();
                let s = r.get_mut(scope).unwrap();
                let result = s.attach_spawned(
                    pid,
                    OsIdentity::new(1, ReuseToken::StartTime(1)),
                    ProcessSpec::new("model"),
                );
                if result.is_err() {
                    assert!(!s.is_open());
                }
                pid
            });
            let second = registry.clone();
            let close = loom::thread::spawn(move || {
                let mut r = second.lock().unwrap();
                r.get_mut(scope).unwrap().begin_scope_termination();
                r.next_process_id()
            });
            assert_ne!(spawn.join().unwrap(), close.join().unwrap());
            assert!(!registry.lock().unwrap().get(scope).unwrap().is_open());
        });
    }
}
