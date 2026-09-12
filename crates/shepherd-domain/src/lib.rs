//! # shepherd-domain
//!
//! The **pure** domain model for the Process Supervision bounded context.
//!
//! This crate contains only entities, value objects, aggregates, domain events, and domain
//! errors. It has **no** dependency on an async runtime, on OS facilities, or on any
//! infrastructure crate, and it performs no I/O. All behaviour here is deterministic and
//! unit-testable without spawning a process.
//!
//! See `docs/DESIGN.md` and `docs/GLOSSARY.md` for the architecture and ubiquitous
//! language.
#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]

mod capability;
mod error;
mod event;
mod exit;
mod ids;
mod lifecycle;
mod os;
mod process;
mod scope;
mod spec;
mod stats;

pub use capability::{Capabilities, Containment, Support};
pub use error::{DomainError, InvalidTransition};
pub use event::{DomainEvent, IntegrationEvent};
pub use exit::{ProcessExit, RawExit, TerminationOutcome, UnverifiedReason};
pub use ids::{ProcessId, ProcessScopeId};
pub use lifecycle::{ProcessLifecycle, ScopeState};
pub use os::{OsIdentity, ReuseToken};
pub use process::Process;
pub use scope::ProcessScope;
pub use spec::{EnvPolicy, GracePeriod, OutputMode, ProcessSpec, Signal};
pub use stats::{ProcessState, ProcessStats, RawStats};

mod observation;
pub use observation::{ObservedProcess, ObservedProcessIdentity, ProcessSnapshot, ProcessTree};
