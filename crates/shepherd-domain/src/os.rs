//! OS-level identity as plain domain data (no handles, no I/O).

/// The OS-level identity of a supervised process.
///
/// Combines the OS pid with a [`ReuseToken`] so PID reuse can be detected. This is plain
/// data: the domain never holds a live handle or file descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OsIdentity {
    /// The operating-system process id.
    pub pid: u32,
    /// A discriminator that detects PID reuse.
    pub reuse_token: ReuseToken,
}

impl OsIdentity {
    /// Creates a new OS identity.
    #[must_use]
    pub const fn new(pid: u32, reuse_token: ReuseToken) -> Self {
        Self { pid, reuse_token }
    }
}

/// A value that distinguishes a specific process instance from a later reuse of the same
/// pid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReuseToken {
    /// Process start time (e.g. jiffies since boot on Linux).
    StartTime(u64),
    /// No reuse discriminator available on this platform/backend.
    Unavailable,
}
