//! Platform backend adapters implementing [`ProcessBackend`](shepherd_app::ports::ProcessBackend).

mod null;
pub use null::NullBackend;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::UnixProcessBackend;
