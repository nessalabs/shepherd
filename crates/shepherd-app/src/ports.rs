//! Application ports (driven interfaces) and their supporting value types.
//!
//! These are the seams between the application and infrastructure. Adapters live in
//! `shepherd-infra`; handlers and the supervisor depend only on these traits, never on
//! concrete implementations.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use shepherd_domain::{
    Capabilities, DomainEvent, GracePeriod, IntegrationEvent, OsIdentity, ProcessExit, ProcessId,
    ProcessScopeId, ProcessSpec, RawExit, RawStats, Signal,
};

use crate::error::{HandlerError, SpawnError, StatsError, TerminateError, WaitError};

/// A successfully spawned OS process, as seen by the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Spawned {
    /// The OS identity (pid + reuse token).
    pub os: OsIdentity,
}

/// Options controlling a termination request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminateOptions {
    /// How long to wait after the graceful signal before escalating to force.
    pub grace: GracePeriod,
    /// Maximum time to wait for exit after force before giving up (`None` = wait forever).
    pub force_timeout: Option<Duration>,
}

impl Default for TerminateOptions {
    fn default() -> Self {
        Self {
            grace: GracePeriod::default(),
            force_timeout: Some(Duration::from_secs(10)),
        }
    }
}

/// The platform abstraction: spawn, signal, wait, sample, and report capabilities.
///
/// Implemented per OS in `shepherd-infra`. All ownership of the raw child handle lives behind
/// this port; the application never touches a raw `Child`.
#[async_trait]
pub trait ProcessBackend: Send + Sync {
    /// Spawns `spec` into the containment resource for `scope`.
    async fn spawn(&self, scope: ProcessScopeId, spec: &ProcessSpec)
        -> Result<Spawned, SpawnError>;

    /// Sends `signal` to a single process.
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError>;

    /// Sends `signal` to every process in `scope`'s containment resource.
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError>;

    /// Resolves when the process has exited, returning its raw exit. Reaps the child.
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError>;

    /// Samples current resource usage.
    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError>;

    /// The runtime-detected guarantees of this backend.
    fn capabilities(&self) -> Capabilities;
}

/// Time source, so grace periods and uptime are deterministic in tests.
#[async_trait]
pub trait Clock: Send + Sync {
    /// The current instant.
    fn now(&self) -> Instant;
    /// Sleeps for `duration`.
    async fn sleep(&self, duration: Duration);
}

/// Lets `wait(pid)` callers block until a process reaches its reaped terminal state.
pub trait Waiters: Send + Sync {
    /// Records the terminal exit for `pid` and wakes any waiters.
    fn signal_exit(&self, pid: ProcessId, exit: ProcessExit);
    /// Returns the recorded exit if the process has already been reaped.
    fn try_get(&self, pid: ProcessId) -> Option<ProcessExit>;
    /// A future that resolves with the terminal exit once reaped.
    fn wait(&self, pid: ProcessId) -> WaitFuture;
}

/// Boxed future returned by [`Waiters::wait`].
pub type WaitFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ProcessExit> + Send>>;

/// Outbound port for publishing integration events across Shepherd's boundary.
///
/// Bounded and lossy-tolerant: a slow or absent publisher must never affect Shepherd's
/// internal invariants.
#[async_trait]
pub trait IntegrationEventPublisher: Send + Sync {
    /// Publishes an integration event.
    async fn publish(&self, event: IntegrationEvent);
}

/// A focused handler that reacts to specific in-process domain events using only injected
/// ports (dependency inversion).
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Handles a single domain event. Errors are surfaced (never swallowed).
    async fn handle(&self, event: &DomainEvent) -> Result<(), HandlerError>;
    /// A stable name for diagnostics.
    fn name(&self) -> &'static str;
}
