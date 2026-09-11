//! The `Process` entity — reachable only through its owning scope aggregate.

use crate::exit::ProcessExit;
use crate::ids::ProcessId;
use crate::lifecycle::ProcessLifecycle;
use crate::os::OsIdentity;
use crate::spec::ProcessSpec;

/// A single supervised OS process.
///
/// An entity (identity = [`ProcessId`]) that lives inside exactly one
/// [`ProcessScope`](crate::ProcessScope). Its fields are private: all mutation happens
/// through methods, and transitions are idempotent where repetition must be safe.
#[derive(Debug, Clone)]
pub struct Process {
    id: ProcessId,
    os: OsIdentity,
    spec: ProcessSpec,
    state: ProcessLifecycle,
    forced: bool,
    exit: Option<ProcessExit>,
}

impl Process {
    /// Creates a running process from a successful spawn.
    #[must_use]
    pub(crate) fn running(id: ProcessId, os: OsIdentity, spec: ProcessSpec) -> Self {
        Self {
            id,
            os,
            spec,
            state: ProcessLifecycle::Running,
            forced: false,
            exit: None,
        }
    }

    /// The supervised identity.
    #[must_use]
    pub fn id(&self) -> ProcessId {
        self.id
    }

    /// The OS identity (pid + reuse token).
    #[must_use]
    pub fn os_identity(&self) -> OsIdentity {
        self.os
    }

    /// The spec used to spawn this process.
    #[must_use]
    pub fn spec(&self) -> &ProcessSpec {
        &self.spec
    }

    /// The current lifecycle state.
    #[must_use]
    pub fn state(&self) -> ProcessLifecycle {
        self.state
    }

    /// Whether force was required during termination.
    #[must_use]
    pub fn was_forced(&self) -> bool {
        self.forced
    }

    /// The recorded terminal exit, if reaped.
    #[must_use]
    pub fn exit(&self) -> Option<ProcessExit> {
        self.exit
    }

    /// Requests graceful termination.
    ///
    /// Returns `true` if this call moved the process into the graceful phase (i.e. a signal
    /// should now be sent). Idempotent: repeated calls on an already-terminating or terminal
    /// process return `false`.
    pub(crate) fn request_graceful(&mut self) -> bool {
        match self.state {
            ProcessLifecycle::Running | ProcessLifecycle::Spawning => {
                self.state = ProcessLifecycle::GracefulRequested;
                true
            }
            _ => false,
        }
    }

    /// Escalates to forceful termination.
    ///
    /// Returns `true` if this call moved the process into the forcing phase. Idempotent.
    pub(crate) fn escalate_to_force(&mut self) -> bool {
        match self.state {
            ProcessLifecycle::Running
            | ProcessLifecycle::Spawning
            | ProcessLifecycle::GracefulRequested => {
                self.state = ProcessLifecycle::Forcing;
                self.forced = true;
                true
            }
            _ => false,
        }
    }

    /// Records that the process was observed to have exited.
    ///
    /// Returns `true` if this call transitioned a live process to exited. Idempotent for
    /// already-exited/terminal processes.
    pub(crate) fn mark_exited(&mut self) -> bool {
        if self.state.is_live() {
            self.state = ProcessLifecycle::ExitedUnreaped;
            true
        } else {
            false
        }
    }

    /// Records that the process was reaped, storing its terminal exit.
    ///
    /// Returns `true` if this call transitioned to reaped. Idempotent once reaped.
    pub(crate) fn mark_reaped(&mut self, exit: ProcessExit) -> bool {
        match self.state {
            ProcessLifecycle::ExitedUnreaped
            | ProcessLifecycle::Running
            | ProcessLifecycle::GracefulRequested
            | ProcessLifecycle::Forcing => {
                self.state = ProcessLifecycle::Reaped;
                self.exit = Some(exit);
                true
            }
            _ => false,
        }
    }
}
