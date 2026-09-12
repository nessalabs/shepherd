//! Typed application errors. Each operation has its own error so callers can branch.

use shepherd_domain::{DomainError, ProcessId, ProcessScopeId};

/// Error admitting a new process scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ScopeCreationError {
    /// Shutdown has started, so no new scopes can be admitted.
    #[error("supervisor is no longer accepting scopes")]
    SupervisorClosed,
}

/// Error creating a process.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    /// The target scope does not exist.
    #[error("unknown scope {0}")]
    UnknownScope(ProcessScopeId),
    /// The scope is draining/closed and cannot accept new processes.
    #[error("scope {0} is not accepting new processes")]
    ScopeClosed(ProcessScopeId),
    /// The OS failed to create the process.
    #[error("failed to spawn process: {0}")]
    Os(String),
    /// A domain rule rejected the operation.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

/// Error terminating a process or scope.
#[derive(Debug, thiserror::Error)]
pub enum TerminateError {
    /// The referenced process is unknown.
    #[error("unknown process {0}")]
    UnknownProcess(ProcessId),
    /// The referenced scope is unknown.
    #[error("unknown scope {0}")]
    UnknownScope(ProcessScopeId),
    /// Sending the signal failed.
    #[error("failed to signal process: {0}")]
    Signal(String),
    /// A domain rule rejected the operation.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

/// Error collecting statistics.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StatsError {
    /// The sampler has not completed its first observation.
    #[error("no sample available yet for process {0}")]
    NotReady(ProcessId),
    /// The referenced process is unknown or no longer live.
    #[error("unknown or exited process {0}")]
    UnknownProcess(ProcessId),
    /// The backend failed to sample.
    #[error("failed to sample process: {0}")]
    Backend(String),
}

/// Error waiting for a process.
#[derive(Debug, thiserror::Error)]
pub enum WaitError {
    /// The referenced process is unknown.
    #[error("unknown process {0}")]
    UnknownProcess(ProcessId),
    /// The backend failed while waiting.
    #[error("failed to wait for process: {0}")]
    Backend(String),
}

/// Error during supervisor shutdown.
#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    /// One or more scopes could not be verified as fully cleaned up.
    #[error("shutdown could not verify cleanup of {0} scope(s)")]
    Unverified(usize),
}

/// Error surfaced by an [`EventHandler`](crate::ports::EventHandler). Never swallowed.
#[derive(Debug, thiserror::Error)]
#[error("event handler '{handler}' failed: {message}")]
pub struct HandlerError {
    /// The handler that failed.
    pub handler: &'static str,
    /// A human-readable message.
    pub message: String,
}

impl HandlerError {
    /// Creates a handler error.
    pub fn new(handler: &'static str, message: impl Into<String>) -> Self {
        Self {
            handler,
            message: message.into(),
        }
    }
}
