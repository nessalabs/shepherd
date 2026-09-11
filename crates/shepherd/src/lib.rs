//! # shepherd
//!
//! A reusable, production-quality **process supervision** library. Shepherd provides
//! *mechanism* — spawning, ownership by explicit scopes, verified termination, reaping, and
//! resource observation — and leaves *policy* to the caller.
//!
//! ```no_run
//! use shepherd::{ProcessSpec, SupervisorBuilder, TerminateOptions};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let supervisor = SupervisorBuilder::new().build();
//! let scope = supervisor.create_scope();
//! let pid = supervisor.spawn(scope, ProcessSpec::new("sleep").arg("30")).await?;
//! let report = supervisor.terminate_scope(scope, TerminateOptions::default()).await?;
//! assert!(report.all_verified());
//! # let _ = pid;
//! # Ok(())
//! # }
//! ```
//!
//! See `docs/DESIGN.md`, `docs/DIAGRAMS.md`, and `docs/GLOSSARY.md`.
#![forbid(unsafe_code)]

use std::sync::Arc;

pub use shepherd_app::ports::{IntegrationEventPublisher, ProcessBackend, Spawned};
pub use shepherd_app::{
    HandlerError, ProcessSupervisor, ScopeTerminationReport, ShutdownError, ShutdownReport,
    SpawnError, StatsError, TerminateError, TerminateOptions, WaitError,
};
pub use shepherd_domain::{
    Capabilities, Containment, DomainEvent, EnvPolicy, GracePeriod, IntegrationEvent, ProcessExit,
    ProcessId, ProcessScopeId, ProcessSpec, ProcessState, ProcessStats, Signal, Support,
    TerminationOutcome, UnverifiedReason,
};
pub use shepherd_infra::NullBackend;
#[cfg(unix)]
pub use shepherd_infra::UnixProcessBackend;
pub use shepherd_infra::{
    BroadcastIntegrationPublisher, InMemoryWaiters, NoopIntegrationPublisher, SystemClock,
};

/// Builds a [`ProcessSupervisor`] wired with default or custom infrastructure adapters.
pub struct SupervisorBuilder {
    backend: Option<Arc<dyn ProcessBackend>>,
    publisher: Option<Arc<dyn IntegrationEventPublisher>>,
}

impl std::fmt::Debug for SupervisorBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisorBuilder").finish_non_exhaustive()
    }
}

impl Default for SupervisorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SupervisorBuilder {
    /// Starts a builder with default adapters.
    #[must_use]
    pub fn new() -> Self {
        Self {
            backend: None,
            publisher: None,
        }
    }

    /// Overrides the process backend (e.g. a [`NullBackend`] for tests).
    #[must_use]
    pub fn backend(mut self, backend: Arc<dyn ProcessBackend>) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Sets the outbound integration-event publisher.
    #[must_use]
    pub fn integration_publisher(mut self, publisher: Arc<dyn IntegrationEventPublisher>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Builds the supervisor.
    #[must_use]
    pub fn build(self) -> ProcessSupervisor {
        let backend = self.backend.unwrap_or_else(default_backend);
        let clock = Arc::new(SystemClock);
        let waiters = Arc::new(InMemoryWaiters::new());
        let publisher = self
            .publisher
            .unwrap_or_else(|| Arc::new(NoopIntegrationPublisher));
        ProcessSupervisor::new(backend, clock, waiters, publisher)
    }
}

#[cfg(unix)]
fn default_backend() -> Arc<dyn ProcessBackend> {
    Arc::new(UnixProcessBackend::new())
}

#[cfg(not(unix))]
fn default_backend() -> Arc<dyn ProcessBackend> {
    Arc::new(NullBackend::new())
}
