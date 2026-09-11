//! Resource observations. Observations only — never verdicts.

use std::time::Duration;

use crate::ids::ProcessId;

/// The observed OS run-state of a process at sample time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    /// Running or runnable.
    Running,
    /// Sleeping / waiting.
    Sleeping,
    /// Stopped (e.g. `SIGSTOP`).
    Stopped,
    /// Exited but not yet reaped.
    Zombie,
    /// State could not be determined.
    Unknown,
}

/// Raw resource sample produced by a backend (before the supervisor adds identity/uptime).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawStats {
    /// Fraction of one CPU core, `1.0` == one core fully used.
    pub cpu_usage: f32,
    /// Resident set size, in bytes.
    pub memory_rss_bytes: u64,
    /// Virtual memory size, in bytes, if available.
    pub virtual_memory_bytes: Option<u64>,
    /// Peak resident set size, in bytes, if available.
    pub peak_rss_bytes: Option<u64>,
    /// Bytes read, if available.
    pub io_read_bytes: Option<u64>,
    /// Bytes written, if available.
    pub io_write_bytes: Option<u64>,
    /// Number of descendants observed, if available.
    pub descendant_count: Option<u32>,
    /// Observed run-state.
    pub state: ProcessState,
}

/// A point-in-time observation of a supervised process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProcessStats {
    /// The supervised process the sample belongs to.
    pub pid: ProcessId,
    /// Fraction of one CPU core, `1.0` == one core fully used.
    pub cpu_usage: f32,
    /// Resident set size, in bytes.
    pub memory_rss_bytes: u64,
    /// Virtual memory size, in bytes, if available.
    pub virtual_memory_bytes: Option<u64>,
    /// Peak resident set size, in bytes, if available.
    pub peak_rss_bytes: Option<u64>,
    /// Bytes read, if available.
    pub io_read_bytes: Option<u64>,
    /// Bytes written, if available.
    pub io_write_bytes: Option<u64>,
    /// Number of descendants observed, if available.
    pub descendant_count: Option<u32>,
    /// How long the process has been alive.
    pub uptime: Duration,
    /// Observed run-state.
    pub state: ProcessState,
}

impl ProcessStats {
    /// Combines a [`RawStats`] sample with the supervised identity and uptime.
    #[must_use]
    pub fn from_raw(pid: ProcessId, raw: RawStats, uptime: Duration) -> Self {
        Self {
            pid,
            cpu_usage: raw.cpu_usage,
            memory_rss_bytes: raw.memory_rss_bytes,
            virtual_memory_bytes: raw.virtual_memory_bytes,
            peak_rss_bytes: raw.peak_rss_bytes,
            io_read_bytes: raw.io_read_bytes,
            io_write_bytes: raw.io_write_bytes,
            descendant_count: raw.descendant_count,
            uptime,
            state: raw.state,
        }
    }
}
