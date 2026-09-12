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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_raw_copies_every_field_and_adds_identity_and_uptime() {
        let raw = RawStats {
            cpu_usage: 0.5,
            memory_rss_bytes: 2048,
            virtual_memory_bytes: Some(4096),
            peak_rss_bytes: Some(3000),
            io_read_bytes: Some(10),
            io_write_bytes: Some(20),
            descendant_count: Some(2),
            state: ProcessState::Sleeping,
        };
        let pid = ProcessId::new(9);
        let stats = ProcessStats::from_raw(pid, raw, Duration::from_secs(3));
        assert_eq!(stats.pid, pid);
        assert_eq!(stats.cpu_usage, 0.5);
        assert_eq!(stats.memory_rss_bytes, 2048);
        assert_eq!(stats.virtual_memory_bytes, Some(4096));
        assert_eq!(stats.peak_rss_bytes, Some(3000));
        assert_eq!(stats.io_read_bytes, Some(10));
        assert_eq!(stats.io_write_bytes, Some(20));
        assert_eq!(stats.descendant_count, Some(2));
        assert_eq!(stats.uptime, Duration::from_secs(3));
        assert_eq!(stats.state, ProcessState::Sleeping);
    }

    #[test]
    fn process_states_are_distinct() {
        let all = [
            ProcessState::Running,
            ProcessState::Sleeping,
            ProcessState::Stopped,
            ProcessState::Zombie,
            ProcessState::Unknown,
        ];
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                assert_eq!(i == j, a == b);
            }
        }
    }
}
