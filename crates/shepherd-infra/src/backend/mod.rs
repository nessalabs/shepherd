//! Platform backend adapters implementing [`ProcessBackend`](shepherd_app::ports::ProcessBackend).

mod null;
pub use null::NullBackend;

#[cfg(unix)]
pub(crate) mod unix;
#[cfg(unix)]
pub use unix::UnixProcessBackend;

#[cfg(target_os = "linux")]
mod cgroup;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::WindowsJobBackend;
#[cfg(any(unix, windows))]
mod sampling;
