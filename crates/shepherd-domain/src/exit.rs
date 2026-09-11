//! Exit information and verified termination outcomes.

use crate::ids::ProcessId;
use crate::spec::Signal;
use std::time::Duration;

/// Raw exit information reported by a backend after a process is reaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawExit {
    /// Exit code, if the process exited normally.
    pub code: Option<i32>,
    /// Terminating signal, if the process was killed by a signal.
    pub signal: Option<Signal>,
    /// Whether the OS reported a core dump.
    pub core_dumped: bool,
}

/// The verified result of a termination or a natural exit.
///
/// Success variants are only ever produced after Shepherd has confirmed the process is gone
/// **and** reaped. Sending a signal is never, by itself, a success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationOutcome {
    /// The process exited on its own before any termination was requested.
    ExitedNaturally,
    /// A graceful request was honoured and the process exited within the grace period.
    GracefulSuccess,
    /// The process had to be force-killed after ignoring the graceful request.
    ForcedRequired,
    /// Termination failed.
    Failed,
    /// Cleanup could not be verified (see [`UnverifiedReason`]).
    CleanupUnverified(UnverifiedReason),
}

impl TerminationOutcome {
    /// Whether this outcome represents verified cleanup.
    #[must_use]
    pub const fn is_verified(self) -> bool {
        matches!(
            self,
            Self::ExitedNaturally | Self::GracefulSuccess | Self::ForcedRequired
        )
    }
}

/// Why cleanup could not be verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnverifiedReason {
    /// A scope/supervisor handle was dropped without an explicit shutdown; the `Drop`
    /// backstop issued a synchronous hard kill it could not confirm.
    DroppedWithoutShutdown,
    /// The async runtime was shutting down, so the exit/reap could not be awaited.
    RuntimeShutdown,
    /// Waiting for the process to exit timed out.
    WaitTimedOut {
        /// How long Shepherd waited before giving up.
        waited: Duration,
    },
    /// Reaping the process failed.
    ReapFailed,
    /// The process disappeared before Shepherd could confirm the outcome (reuse-safe).
    ProcessDisappeared,
}

/// The recorded terminal result of a supervised process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessExit {
    /// The supervised process this exit belongs to.
    pub pid: ProcessId,
    /// Exit code, if any.
    pub code: Option<i32>,
    /// Terminating signal, if any.
    pub signal: Option<Signal>,
    /// The verified outcome.
    pub outcome: TerminationOutcome,
    /// Whether force was required.
    pub forced: bool,
}
