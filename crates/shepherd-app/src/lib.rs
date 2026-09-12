//! # shepherd-app
//!
//! The application layer for Shepherd. Orchestrates the pure domain, hosts the in-process
//! event dispatcher and handlers, and defines the driven **ports** that infrastructure
//! adapters implement. Owns async; depends only on `shepherd-domain`.
#![forbid(unsafe_code)]

pub mod dispatch;
pub mod error;
pub mod ports;
pub mod registry;
pub mod supervisor;

pub use dispatch::{
    EventDispatcher, IntegrationTranslator, RegistryPruneHandler, SharedRegistry,
    WaitNotifierHandler,
};
pub use error::{
    HandlerError, ScopeCreationError, ShutdownError, SpawnError, StatsError, TerminateError,
    WaitError,
};
pub use ports::{
    Clock, EventHandler, IntegrationEventPublisher, ProcessBackend, Spawned, TerminateOptions,
    WaitFuture, Waiters,
};
pub use registry::ScopeRegistry;
pub use supervisor::{ProcessSupervisor, ScopeTerminationReport, ShutdownReport};

pub mod output;
