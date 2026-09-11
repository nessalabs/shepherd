//! Runtime-detected platform guarantees, expressed as values (never prose).

/// The guarantees a platform backend actually provides at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// Strength of descendant containment.
    pub descendant_containment: Containment,
    /// CPU-usage statistics support.
    pub cpu: Support,
    /// Resident-set-size statistics support.
    pub rss: Support,
    /// Peak-RSS statistics support.
    pub peak_rss: Support,
    /// I/O byte-counter statistics support.
    pub io: Support,
    /// Whether forceful termination is available.
    pub force_termination: bool,
}

/// The mechanism enforcing "kill the whole tree", and thus how strong containment is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Containment {
    /// Linux cgroup v2 — contains descendants even if they call `setsid`.
    CgroupV2,
    /// Windows Job Object — contains the whole tree.
    JobObject,
    /// POSIX process group — best effort; a descendant that calls `setsid` escapes.
    ProcessGroup,
    /// No containment beyond the direct child.
    None,
}

impl Containment {
    /// Whether this mechanism contains descendants that deliberately detach (`setsid`).
    #[must_use]
    pub const fn contains_detached(self) -> bool {
        matches!(self, Self::CgroupV2 | Self::JobObject)
    }
}

/// Whether a particular statistic is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Support {
    /// The statistic is collected.
    Supported,
    /// The statistic is not available on this platform/backend.
    Unsupported,
}

impl Support {
    /// Returns `true` when supported.
    #[must_use]
    pub const fn is_supported(self) -> bool {
        matches!(self, Self::Supported)
    }
}
