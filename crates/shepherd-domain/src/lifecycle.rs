//! Lifecycle state machines for a process and a scope.

/// The supervised lifecycle state of a [`Process`](crate::Process).
///
/// ```text
/// Spawning ── ok ──▶ Running ── exit ──▶ ExitedUnreaped ── reap ──▶ Reaped
///    │                 │
///    │ err             │ terminate(graceful)
///    ▼                 ▼
/// SpawnFailed     GracefulRequested ── grace elapsed ──▶ Forcing ──▶ ExitedUnreaped
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessLifecycle {
    /// Being created (transient; modelled at the application boundary).
    Spawning,
    /// Alive and supervised.
    Running,
    /// A graceful termination has been requested.
    GracefulRequested,
    /// Being force-terminated.
    Forcing,
    /// Exited but not yet reaped (zombie).
    ExitedUnreaped,
    /// Exited and reaped; terminal.
    Reaped,
    /// Creation failed; terminal.
    SpawnFailed,
}

impl ProcessLifecycle {
    /// A stable name for diagnostics / transition errors.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Spawning => "Spawning",
            Self::Running => "Running",
            Self::GracefulRequested => "GracefulRequested",
            Self::Forcing => "Forcing",
            Self::ExitedUnreaped => "ExitedUnreaped",
            Self::Reaped => "Reaped",
            Self::SpawnFailed => "SpawnFailed",
        }
    }

    /// Whether the process is still alive from Shepherd's point of view.
    #[must_use]
    pub const fn is_live(self) -> bool {
        matches!(
            self,
            Self::Spawning | Self::Running | Self::GracefulRequested | Self::Forcing
        )
    }

    /// Whether the process has reached a terminal state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Reaped | Self::SpawnFailed)
    }

    /// Whether a termination has been requested (graceful or forceful).
    #[must_use]
    pub const fn is_terminating(self) -> bool {
        matches!(self, Self::GracefulRequested | Self::Forcing)
    }
}

/// The lifecycle state of a [`ProcessScope`](crate::ProcessScope).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeState {
    /// Accepting new processes.
    Open,
    /// Terminating; no new processes accepted.
    Draining,
    /// All processes reaped; terminal.
    Closed,
}

impl ScopeState {
    /// A stable name for diagnostics.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Open => "Open",
            Self::Draining => "Draining",
            Self::Closed => "Closed",
        }
    }

    /// Whether the scope will accept new processes.
    #[must_use]
    pub const fn accepts_processes(self) -> bool {
        matches!(self, Self::Open)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_PROCESS: [ProcessLifecycle; 7] = [
        ProcessLifecycle::Spawning,
        ProcessLifecycle::Running,
        ProcessLifecycle::GracefulRequested,
        ProcessLifecycle::Forcing,
        ProcessLifecycle::ExitedUnreaped,
        ProcessLifecycle::Reaped,
        ProcessLifecycle::SpawnFailed,
    ];

    #[test]
    fn process_state_names_are_stable_and_unique() {
        let names: Vec<_> = ALL_PROCESS.iter().map(|s| s.name()).collect();
        assert_eq!(
            names,
            vec![
                "Spawning",
                "Running",
                "GracefulRequested",
                "Forcing",
                "ExitedUnreaped",
                "Reaped",
                "SpawnFailed",
            ]
        );
    }

    #[test]
    fn liveness_classification() {
        use ProcessLifecycle::*;
        for state in ALL_PROCESS {
            let live = matches!(state, Spawning | Running | GracefulRequested | Forcing);
            assert_eq!(state.is_live(), live);
        }
    }

    #[test]
    fn terminal_classification() {
        use ProcessLifecycle::*;
        for state in ALL_PROCESS {
            let terminal = matches!(state, Reaped | SpawnFailed);
            assert_eq!(state.is_terminal(), terminal);
        }
    }

    #[test]
    fn terminating_classification() {
        use ProcessLifecycle::*;
        for state in ALL_PROCESS {
            let terminating = matches!(state, GracefulRequested | Forcing);
            assert_eq!(state.is_terminating(), terminating);
        }
    }

    #[test]
    fn scope_states() {
        assert_eq!(ScopeState::Open.name(), "Open");
        assert_eq!(ScopeState::Draining.name(), "Draining");
        assert_eq!(ScopeState::Closed.name(), "Closed");
        assert!(ScopeState::Open.accepts_processes());
        assert!(!ScopeState::Draining.accepts_processes());
        assert!(!ScopeState::Closed.accepts_processes());
    }
}
