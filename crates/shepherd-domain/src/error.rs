//! Domain-level errors (pure; no I/O errors here).

use crate::ids::{ProcessId, ProcessScopeId};

/// Errors produced by the pure domain when an operation is not valid for the current state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    /// Attempted to spawn into a scope that is draining or closed.
    #[error("scope {0} is not accepting new processes")]
    ScopeClosed(ProcessScopeId),

    /// Referenced a process that the scope does not own.
    #[error("unknown process {0}")]
    UnknownProcess(ProcessId),

    /// Referenced a scope that does not exist.
    #[error("unknown scope {0}")]
    UnknownScope(ProcessScopeId),

    /// A lifecycle transition was requested that is illegal for the current state.
    #[error(transparent)]
    InvalidTransition(#[from] InvalidTransition),
}

/// An illegal lifecycle transition was attempted.
///
/// These are guarded internally; a returned value indicates a bug in the caller of the
/// aggregate, not a normal runtime condition.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid transition from {from} on event '{event}'")]
pub struct InvalidTransition {
    /// The state the process/scope was in.
    pub from: &'static str,
    /// The event/operation that was rejected.
    pub event: &'static str,
}
