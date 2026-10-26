//! # shepherd-infra
//!
//! Infrastructure adapters that implement the `shepherd-app` ports: platform process
//! backends, a system clock, an in-memory waiter set, and integration-event publishers.
//! This is the anti-corruption layer — it translates OS concepts into domain terms.

pub mod backend;
pub mod clock;
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub mod kept;
pub mod publisher;
pub mod waiters;

pub use backend::NullBackend;
#[cfg(unix)]
pub use backend::UnixProcessBackend;
pub use clock::SystemClock;
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub use kept::{DirectoryKeptStore, UnixKeptLauncher};
pub use publisher::{BroadcastIntegrationPublisher, NoopIntegrationPublisher};
pub use waiters::InMemoryWaiters;

mod output;

#[cfg(windows)]
pub use backend::WindowsJobBackend;

pub mod observation;
pub use observation::SystemProcessObserver;

mod usage;
