//! Domain events (in-process facts) and integration events (boundary-crossing facts).

use crate::exit::{ProcessExit, TerminationOutcome};
use crate::ids::{ProcessId, ProcessScopeId};

/// A fact that happened inside the Process Supervision bounded context.
///
/// Domain events are **returned** by aggregate transitions (the aggregate never dispatches)
/// and are handled **in-process** by same-context handlers. Emitted at most once logically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainEvent {
    /// A process was successfully spawned into a scope.
    ProcessSpawned {
        /// The owning scope.
        scope: ProcessScopeId,
        /// The spawned process.
        pid: ProcessId,
    },
    /// Termination was requested for a process.
    TerminationRequested {
        /// The owning scope.
        scope: ProcessScopeId,
        /// The process being terminated.
        pid: ProcessId,
    },
    /// A process was observed to have exited (before reaping).
    ProcessExited {
        /// The owning scope.
        scope: ProcessScopeId,
        /// The exited process.
        pid: ProcessId,
    },
    /// An exited process's resources were reaped; its terminal state is confirmed.
    ProcessReaped {
        /// The owning scope.
        scope: ProcessScopeId,
        /// The reaped process.
        pid: ProcessId,
        /// The verified terminal exit.
        exit: ProcessExit,
    },
    /// A scope reached the closed state; all its processes are reaped.
    ScopeClosed {
        /// The closed scope.
        scope: ProcessScopeId,
    },
}

/// A lifecycle fact published across Shepherd's boundary to another bounded context (the
/// consuming application's policy layer).
///
/// Decoupled from internal invariants; delivery is bounded and lossy-tolerant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrationEvent {
    /// A process reached a terminal, reaped state.
    ProcessTerminated {
        /// The owning scope.
        scope: ProcessScopeId,
        /// The terminated process.
        pid: ProcessId,
        /// The verified outcome.
        outcome: TerminationOutcome,
    },
    /// A whole scope was terminated and closed.
    ScopeTerminated {
        /// The terminated scope.
        scope: ProcessScopeId,
    },
}
