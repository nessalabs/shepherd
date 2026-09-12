//! Read-only process observations, independent of supervised ownership.

/// An observed PID and its reported start time. Never a signaling/reaping authority.
/// Start times have second resolution; this is not a collision-free native identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObservedProcessIdentity {
    /// OS process ID, distinct from Shepherd's logical ProcessId.
    pub os_pid: u32,
    /// Seconds since Unix epoch; None when the OS does not report a start time.
    pub start_time_unix_seconds: Option<u64>,
}

/// One visible process in a best-effort snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedProcess {
    /// Observed identity; may change between snapshots when a PID is reused.
    pub identity: ObservedProcessIdentity,
    /// Reported OS parent PID. The parent need not be visible in this snapshot.
    pub parent_os_pid: Option<u32>,
    /// Display name (lossy Unicode), possibly empty when unavailable.
    /// Not an executable path or command line.
    pub name: String,
}

/// Visible processes sampled over an interval, not an atomic or exhaustive OS census.
/// Permissions, process churn and OS visibility can omit processes without diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSnapshot {
    /// Processes sorted by OS PID. No ownership is acquired by observing them.
    pub processes: Vec<ObservedProcess>,
}

/// A root and its currently observed descendants, in parent-before-child order.
/// Reparented/detached descendants and processes that exit between samples may be absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessTree {
    /// Identity of the selected root in this snapshot.
    pub root: ObservedProcessIdentity,
    /// Root followed by descendants. Use parent_os_pid to render the hierarchy.
    pub processes: Vec<ObservedProcess>,
}
