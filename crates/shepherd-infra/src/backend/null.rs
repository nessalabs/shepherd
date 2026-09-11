//! A deterministic, in-memory backend for portable contract tests.
//!
//! No real processes are spawned. Behaviour is driven by the spec's program name so tests can
//! script graceful vs. force scenarios:
//!
//! * `"exit-immediately"` — exits (code 0) as soon as it is spawned (natural exit).
//! * `"ignore-graceful"` — ignores graceful signals; only a forceful kill ends it.
//! * anything else — exits (code 0) on a graceful signal, or killed on a forceful one.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use shepherd_app::error::{SpawnError, StatsError, TerminateError, WaitError};
use shepherd_app::ports::{ProcessBackend, Spawned};
use shepherd_domain::{
    Capabilities, Containment, OsIdentity, ProcessScopeId, ProcessSpec, ProcessState, RawExit,
    RawStats, ReuseToken, Signal, Support,
};
use tokio::sync::watch;

struct NullProc {
    scope: ProcessScopeId,
    ignores_graceful: bool,
    exit: watch::Sender<Option<RawExit>>,
    // Retain a receiver so `watch::Sender::send` never fails/drops a value for lack of
    // receivers before the monitor subscribes.
    _keep: watch::Receiver<Option<RawExit>>,
}

#[derive(Default)]
struct State {
    next_pid: u32,
    procs: HashMap<u32, NullProc>,
}

/// An in-memory fake [`ProcessBackend`].
#[derive(Clone, Default)]
pub struct NullBackend {
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for NullBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NullBackend").finish_non_exhaustive()
    }
}

impl NullBackend {
    /// Creates an empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn deliver(proc: &NullProc, exit: RawExit) {
        // A process that has already exited cannot exit again; ignore later signals so a
        // redundant kill can't overwrite an earlier graceful exit (matches real OS behaviour).
        if proc.exit.borrow().is_some() {
            return;
        }
        let _ = proc.exit.send(Some(exit));
    }
}

const GRACEFUL_EXIT: RawExit = RawExit {
    code: Some(0),
    signal: None,
    core_dumped: false,
};

const KILLED_EXIT: RawExit = RawExit {
    code: None,
    signal: Some(Signal::Kill),
    core_dumped: false,
};

#[async_trait]
impl ProcessBackend for NullBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        let program = spec.program.to_string_lossy().into_owned();
        let (exit_tx, keep_rx) = watch::channel(None);
        let mut state = self.state.lock().expect("null backend mutex");
        state.next_pid += 1;
        let pid = state.next_pid;
        let ignores_graceful = program == "ignore-graceful";
        if program == "exit-immediately" {
            let _ = exit_tx.send(Some(GRACEFUL_EXIT));
        }
        state.procs.insert(
            pid,
            NullProc {
                scope,
                ignores_graceful,
                exit: exit_tx,
                _keep: keep_rx,
            },
        );
        Ok(Spawned {
            os: OsIdentity::new(pid, ReuseToken::StartTime(u64::from(pid))),
        })
    }

    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        let state = self.state.lock().expect("null backend mutex");
        if let Some(proc) = state.procs.get(&target.os.pid) {
            apply_signal(proc, signal);
        }
        Ok(())
    }

    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        let state = self.state.lock().expect("null backend mutex");
        for proc in state.procs.values().filter(|p| p.scope == scope) {
            apply_signal(proc, signal);
        }
        Ok(())
    }

    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        let mut rx = {
            let state = self.state.lock().expect("null backend mutex");
            match state.procs.get(&target.os.pid) {
                Some(proc) => proc.exit.subscribe(),
                None => {
                    return Err(WaitError::UnknownProcess(shepherd_domain::ProcessId::new(
                        0,
                    )))
                }
            }
        };
        loop {
            if let Some(exit) = *rx.borrow_and_update() {
                return Ok(exit);
            }
            if rx.changed().await.is_err() {
                return Ok(KILLED_EXIT);
            }
        }
    }

    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        let state = self.state.lock().expect("null backend mutex");
        if !state.procs.contains_key(&target.os.pid) {
            return Err(StatsError::UnknownProcess(shepherd_domain::ProcessId::new(
                0,
            )));
        }
        Ok(RawStats {
            cpu_usage: 0.0,
            memory_rss_bytes: 1_048_576,
            virtual_memory_bytes: Some(4_194_304),
            peak_rss_bytes: None,
            io_read_bytes: None,
            io_write_bytes: None,
            descendant_count: Some(0),
            state: ProcessState::Running,
        })
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            descendant_containment: Containment::None,
            cpu: Support::Supported,
            rss: Support::Supported,
            peak_rss: Support::Unsupported,
            io: Support::Unsupported,
            force_termination: true,
        }
    }

    fn hard_kill_all(&self) {
        let state = self.state.lock().expect("null backend mutex");
        for proc in state.procs.values() {
            NullBackend::deliver(proc, KILLED_EXIT);
        }
    }
}

fn apply_signal(proc: &NullProc, signal: Signal) {
    match signal {
        Signal::Kill => NullBackend::deliver(proc, KILLED_EXIT),
        Signal::Term | Signal::Interrupt if !proc.ignores_graceful => {
            NullBackend::deliver(proc, GRACEFUL_EXIT);
        }
        _ => {}
    }
}
