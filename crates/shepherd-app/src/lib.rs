//! # shepherd-app
//!
//! The application layer for Shepherd. Orchestrates the pure domain, hosts the in-process
//! event dispatcher and handlers, and defines the driven **ports** that infrastructure
//! adapters implement. Owns async; depends only on `shepherd-domain`.
#![forbid(unsafe_code)]

pub mod dispatch;
pub mod error;
pub mod kept;
pub mod ports;
pub mod registry;
pub mod service;
pub mod supervisor;

pub use dispatch::{
    EventDispatcher, IntegrationTranslator, RegistryPruneHandler, SharedRegistry,
    WaitNotifierHandler,
};
pub use error::{
    HandlerError, ScopeCreationError, ShutdownError, SpawnError, StatsError, TerminateError,
    WaitError,
};
pub use kept::{
    KeptCommit, KeptEntry, KeptError, KeptExit, KeptLaunch, KeptLauncher, KeptLock, KeptProcess,
    KeptProcesses, KeptSpec, KeptStore, KeptTarget, KeptTermination,
};
pub use ports::{
    Clock, EventHandler, IntegrationEventPublisher, ProcessBackend, Spawned, TerminateOptions,
    WaitFuture, Waiters,
};
pub use registry::ScopeRegistry;
pub use service::{
    Service, ServiceFailure, ServiceFailureKind, ServiceHandle, ServiceInstance, ServiceLaunch,
    ServiceNameInUse, ServiceOptions, ServiceState, ServiceStatus, ServiceStop, ServiceStopped,
    ServiceSummary, ServiceSupervisor,
};
pub use supervisor::{
    ObservedScope, ObservedSpawn, ProcessSupervisor, ScopeTerminationReport, ScopedProcesses,
    ShutdownReport, SpawnObservation, WithScopeResult,
};

pub mod output;

pub mod observation;
pub use observation::{ObservationError, ProcessObservationBackend, ProcessObserver};

pub mod usage;
pub use usage::*;
