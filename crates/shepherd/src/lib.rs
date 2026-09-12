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

pub use shepherd_app::output::{OutputChunk, OutputSnapshot, OutputStream, ProcessOutput};
pub use shepherd_app::ports::{IntegrationEventPublisher, ProcessBackend, Spawned};
pub use shepherd_app::{
    HandlerError, ProcessSupervisor, ScopeTerminationReport, ShutdownError, ShutdownReport,
    SpawnError, StatsError, TerminateError, TerminateOptions, WaitError,
};
pub use shepherd_domain::{
    Capabilities, Containment, DomainEvent, EnvPolicy, GracePeriod, IntegrationEvent, OutputMode,
    ProcessExit, ProcessId, ProcessScopeId, ProcessSpec, ProcessState, ProcessStats, Signal,
    Support, TerminationOutcome, UnverifiedReason,
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
    stats_interval: std::time::Duration,
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
            stats_interval: std::time::Duration::from_secs(1),
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

    /// Sets the shared sampling interval (default one second; minimum one millisecond).
    #[must_use]
    pub fn stats_interval(mut self, interval: std::time::Duration) -> Self {
        self.stats_interval = interval;
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
            .unwrap_or_else(|| Arc::new(NoopIntegrationPublisher)); // ADR 0007
        ProcessSupervisor::with_stats_interval(
            backend,
            clock,
            waiters,
            publisher,
            self.stats_interval,
        )
    }
}

#[cfg(target_os = "linux")]
fn default_backend() -> Arc<dyn ProcessBackend> {
    Arc::new(UnixProcessBackend::auto())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn default_backend() -> Arc<dyn ProcessBackend> {
    // Process-group Unix adapter; process-wrap / cgroups-rs are deferred (ADR 0006).
    Arc::new(UnixProcessBackend::new())
}

#[cfg(windows)]
fn default_backend() -> Arc<dyn ProcessBackend> {
    Arc::new(shepherd_infra::WindowsJobBackend::new())
}

#[cfg(not(any(unix, windows)))]
fn default_backend() -> Arc<dyn ProcessBackend> {
    // Honest default: no Job Object adapter yet (ADR 0006).
    Arc::new(NullBackend::new())
}

#[cfg(windows)]
pub use shepherd_infra::WindowsJobBackend;
