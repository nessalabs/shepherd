//! A real Unix backend: process-group containment, signal-based termination, and
//! Tokio-based waiting/reaping. Linux resource stats come from `/proc`.
//!
//! This is the MVP Unix adapter. On Linux it uses a POSIX process group per scope (a cgroup
//! v2 layer is a later phase); on other Unix targets it is process-group only. Descendant
//! containment is therefore best-effort: a child that calls `setsid` can escape — reported
//! honestly via [`Capabilities`].

use std::collections::HashMap;
use std::os::unix::process::ExitStatusExt;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nix::sys::signal::{kill, killpg, Signal as NixSignal};
use nix::unistd::Pid;
use shepherd_app::error::{SpawnError, StatsError, TerminateError, WaitError};
use shepherd_app::ports::{ProcessBackend, Spawned};
use shepherd_domain::{
    Capabilities, Containment, EnvPolicy, OsIdentity, ProcessScopeId, ProcessSpec, ProcessState,
    RawExit, RawStats, ReuseToken, Signal, Support,
};
use tokio::sync::watch;

struct ChildSlot {
    sender: watch::Sender<Option<RawExit>>,
    // Retain a receiver so the waiter task's `send` is never lost if it fires before the
    // supervisor's monitor subscribes.
    _keep: watch::Receiver<Option<RawExit>>,
}

#[derive(Default)]
struct State {
    children: HashMap<u32, ChildSlot>,
    scope_groups: HashMap<ProcessScopeId, i32>,
}

/// A real Unix [`ProcessBackend`].
#[derive(Clone, Default)]
pub struct UnixProcessBackend {
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for UnixProcessBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixProcessBackend").finish_non_exhaustive()
    }
}

impl UnixProcessBackend {
    /// Creates an empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ProcessBackend for UnixProcessBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        let existing_pgid = self
            .state
            .lock()
            .expect("unix backend mutex")
            .scope_groups
            .get(&scope)
            .copied();
        // 0 => create a new process group led by the child; otherwise join the scope's group.
        let target_pgid = existing_pgid.unwrap_or(0);

        let mut cmd = tokio::process::Command::new(&spec.program);
        cmd.args(&spec.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        apply_env(&mut cmd, &spec.env);

        // SAFETY: `setpgid` is async-signal-safe and the closure touches only its captured
        // `target_pgid` copy, so it is sound to run between fork and exec.
        unsafe {
            cmd.pre_exec(move || {
                if libc::setpgid(0, target_pgid) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        cmd.kill_on_drop(false);

        let mut child = cmd.spawn().map_err(|e| SpawnError::Os(e.to_string()))?;
        let pid = child
            .id()
            .ok_or_else(|| SpawnError::Os("child has no pid".into()))?;

        let (exit_tx, keep_rx) = watch::channel(None);
        {
            let mut state = self.state.lock().expect("unix backend mutex");
            state.children.insert(
                pid,
                ChildSlot {
                    sender: exit_tx.clone(),
                    _keep: keep_rx,
                },
            );
            state
                .scope_groups
                .entry(scope)
                .or_insert_with(|| i32::try_from(pid).unwrap_or(0));
        }

        // Waiter task: awaits the child (which reaps it), then publishes the raw exit.
        tokio::spawn(async move {
            let status = child.wait().await;
            let raw = match status {
                Ok(status) => RawExit {
                    code: status.code(),
                    signal: status.signal().map(signal_from_raw),
                    core_dumped: status.core_dumped(),
                },
                Err(_) => RawExit {
                    code: None,
                    signal: None,
                    core_dumped: false,
                },
            };
            let _ = exit_tx.send(Some(raw));
        });

        let reuse_token =
            read_start_time(pid).map_or(ReuseToken::Unavailable, ReuseToken::StartTime);
        Ok(Spawned {
            os: OsIdentity::new(pid, reuse_token),
        })
    }

    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        let pid = Pid::from_raw(i32::try_from(target.os.pid).unwrap_or(0));
        match kill(pid, Some(to_nix(signal))) {
            Ok(()) => Ok(()),
            // ESRCH: the process already exited — treat as success.
            Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(e) => Err(TerminateError::Signal(e.to_string())),
        }
    }

    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        let pgid = self
            .state
            .lock()
            .expect("unix backend mutex")
            .scope_groups
            .get(&scope)
            .copied();
        let Some(pgid) = pgid else {
            return Ok(());
        };
        match killpg(Pid::from_raw(pgid), Some(to_nix(signal))) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(e) => Err(TerminateError::Signal(e.to_string())),
        }
    }

    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        let mut rx = {
            let state = self.state.lock().expect("unix backend mutex");
            match state.children.get(&target.os.pid) {
                Some(slot) => slot.sender.subscribe(),
                None => {
                    return Err(WaitError::Backend(format!(
                        "no child registered for pid {}",
                        target.os.pid
                    )))
                }
            }
        };
        loop {
            if let Some(exit) = *rx.borrow_and_update() {
                return Ok(exit);
            }
            if rx.changed().await.is_err() {
                return Err(WaitError::Backend("waiter channel closed".into()));
            }
        }
    }

    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        sample_process(target.os.pid)
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            descendant_containment: Containment::ProcessGroup,
            cpu: Support::Unsupported,
            rss: rss_support(),
            peak_rss: peak_support(),
            io: Support::Unsupported,
            force_termination: true,
        }
    }
}

fn apply_env(cmd: &mut tokio::process::Command, env: &EnvPolicy) {
    match env {
        EnvPolicy::Inherit => {}
        EnvPolicy::Overrides(entries) => {
            for (key, value) in entries {
                match value {
                    Some(value) => {
                        cmd.env(key, value);
                    }
                    None => {
                        cmd.env_remove(key);
                    }
                }
            }
        }
        EnvPolicy::Clear(entries) => {
            cmd.env_clear();
            for (key, value) in entries {
                cmd.env(key, value);
            }
        }
    }
}

fn to_nix(signal: Signal) -> NixSignal {
    match signal {
        Signal::Term => NixSignal::SIGTERM,
        Signal::Kill => NixSignal::SIGKILL,
        Signal::Interrupt => NixSignal::SIGINT,
        Signal::Custom(raw) => NixSignal::try_from(raw).unwrap_or(NixSignal::SIGTERM),
    }
}

fn signal_from_raw(raw: i32) -> Signal {
    match raw {
        libc::SIGTERM => Signal::Term,
        libc::SIGKILL => Signal::Kill,
        libc::SIGINT => Signal::Interrupt,
        other => Signal::Custom(other),
    }
}

#[cfg(target_os = "linux")]
fn rss_support() -> Support {
    Support::Supported
}
#[cfg(not(target_os = "linux"))]
fn rss_support() -> Support {
    Support::Unsupported
}

#[cfg(target_os = "linux")]
fn peak_support() -> Support {
    Support::Supported
}
#[cfg(not(target_os = "linux"))]
fn peak_support() -> Support {
    Support::Unsupported
}

#[cfg(target_os = "linux")]
fn read_start_time(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Field 22 (1-based) is starttime; it follows the ")" that closes comm.
    let after_comm = stat.rsplit_once(')')?.1;
    after_comm.split_whitespace().nth(19)?.parse().ok()
}
#[cfg(not(target_os = "linux"))]
fn read_start_time(_pid: u32) -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn sample_process(pid: u32) -> Result<RawStats, StatsError> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .map_err(|e| StatsError::Backend(e.to_string()))?;
    let mut rss = 0u64;
    let mut vsize = None;
    let mut peak = None;
    let mut state = ProcessState::Unknown;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            rss = parse_kb(rest);
        } else if let Some(rest) = line.strip_prefix("VmSize:") {
            vsize = Some(parse_kb(rest));
        } else if let Some(rest) = line.strip_prefix("VmHWM:") {
            peak = Some(parse_kb(rest));
        } else if let Some(rest) = line.strip_prefix("State:") {
            state = parse_state(rest.trim());
        }
    }
    Ok(RawStats {
        cpu_usage: 0.0,
        memory_rss_bytes: rss,
        virtual_memory_bytes: vsize,
        peak_rss_bytes: peak,
        io_read_bytes: None,
        io_write_bytes: None,
        descendant_count: None,
        state,
    })
}
#[cfg(not(target_os = "linux"))]
fn sample_process(_pid: u32) -> Result<RawStats, StatsError> {
    Ok(RawStats {
        cpu_usage: 0.0,
        memory_rss_bytes: 0,
        virtual_memory_bytes: None,
        peak_rss_bytes: None,
        io_read_bytes: None,
        io_write_bytes: None,
        descendant_count: None,
        state: ProcessState::Unknown,
    })
}

#[cfg(target_os = "linux")]
fn parse_kb(field: &str) -> u64 {
    field
        .split_whitespace()
        .next()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn parse_state(field: &str) -> ProcessState {
    match field.chars().next() {
        Some('R') => ProcessState::Running,
        Some('S' | 'D') => ProcessState::Sleeping,
        Some('T') => ProcessState::Stopped,
        Some('Z') => ProcessState::Zombie,
        _ => ProcessState::Unknown,
    }
}
