//! A real Unix backend: process-group containment, signal-based termination, and
//! Tokio-based waiting/reaping. Linux resource stats come from `/proc`.
//!
//! This is the MVP Unix adapter. On Linux it uses a POSIX process group per scope (a cgroup
//! v2 layer is a later phase); on other Unix targets it is process-group only. Descendant
//! containment is therefore best-effort: a child that calls `setsid` can escape — reported
//! honestly via [`Capabilities`].
//!
//! Decisions recorded in `docs/decisions/0002-drop-hard-kill.md`,
//! `docs/decisions/0003-serialized-scope-process-groups.md`,
//! `docs/decisions/0004-reuse-safe-signaling.md`, and
//! `docs/decisions/0010-retain-slot-until-wait.md`.

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

#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// `(os_pid, reuse_token)` so a recycled pid cannot overwrite an unreaped slot.
type ChildKey = (u32, u64);

struct ChildSlot {
    sender: watch::Sender<Option<RawExit>>,
    // Retain a receiver so the waiter task's `send` is never lost if it fires before the
    // supervisor's monitor subscribes.
    _keep: watch::Receiver<Option<RawExit>>,
    #[cfg(target_os = "linux")]
    pidfd: Option<OwnedFd>,
}

struct ScopeGroup {
    pgid: i32,
    live: usize,
}

#[derive(Default)]
struct State {
    children: HashMap<ChildKey, ChildSlot>,
    scope_groups: HashMap<ProcessScopeId, ScopeGroup>,
    /// Serializes spawns per scope so the first process creates exactly one process group.
    scope_locks: HashMap<ProcessScopeId, Arc<tokio::sync::Mutex<()>>>,
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

    fn scope_lock(&self, scope: ProcessScopeId) -> Arc<tokio::sync::Mutex<()>> {
        let mut state = self.state.lock().expect("unix backend mutex");
        state
            .scope_locks
            .entry(scope)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn existing_pgid(&self, scope: ProcessScopeId) -> Option<i32> {
        self.state
            .lock()
            .expect("unix backend mutex")
            .scope_groups
            .get(&scope)
            .map(|g| g.pgid)
    }

    /// Pgid to *join* on spawn. An empty group (live == 0) is treated as gone so a later
    /// spawn creates a fresh group; the stale pgid is still kept for `signal_scope` until then.
    fn joinable_pgid(&self, scope: ProcessScopeId) -> Option<i32> {
        self.state
            .lock()
            .expect("unix backend mutex")
            .scope_groups
            .get(&scope)
            .and_then(|g| (g.live > 0).then_some(g.pgid))
    }

    fn forget_group(&self, scope: ProcessScopeId) {
        self.state
            .lock()
            .expect("unix backend mutex")
            .scope_groups
            .remove(&scope);
    }

    fn register_child(
        &self,
        scope: ProcessScopeId,
        os: OsIdentity,
        sender: watch::Sender<Option<RawExit>>,
        keep: watch::Receiver<Option<RawExit>>,
        new_group: bool,
        #[cfg(target_os = "linux")] pidfd: Option<OwnedFd>,
    ) {
        let mut state = self.state.lock().expect("unix backend mutex");
        state.children.insert(
            child_key(&os),
            ChildSlot {
                sender,
                _keep: keep,
                #[cfg(target_os = "linux")]
                pidfd,
            },
        );
        let pgid = i32::try_from(os.pid).unwrap_or(0);
        if new_group {
            state
                .scope_groups
                .insert(scope, ScopeGroup { pgid, live: 1 });
        } else if let Some(group) = state.scope_groups.get_mut(&scope) {
            group.live = group.live.saturating_add(1);
        } else {
            state
                .scope_groups
                .insert(scope, ScopeGroup { pgid, live: 1 });
        }
    }

    /// The OS child has exited. Decrement `live` so the next spawn can create a fresh
    /// group, but **keep the slot** until [`wait`](ProcessBackend::wait) consumes the
    /// recorded exit. Removing it here races a monitor that has not subscribed yet
    /// (macOS CI: `CleanupUnverified(ReapFailed)`). See ADR 0010.
    fn note_os_exit(&self, scope: ProcessScopeId) {
        let mut state = self.state.lock().expect("unix backend mutex");
        if let Some(group) = state.scope_groups.get_mut(&scope) {
            group.live = group.live.saturating_sub(1);
            // Keep the pgid when live hits 0 so `signal_scope` can still sweep descendants
            // that outlived the last tracked root. The next spawn uses `joinable_pgid`
            // (None when live == 0) and creates a fresh group.
        }
    }

    async fn spawn_in_group(
        &self,
        spec: &ProcessSpec,
        target_pgid: i32,
    ) -> Result<tokio::process::Child, SpawnError> {
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
        // Kill-on-drop on `Child` would fire when the waiter task ends, not when the
        // supervisor is dropped. The supervisor's CleanupGuard calls `hard_kill_all`.
        cmd.kill_on_drop(false);

        cmd.spawn().map_err(|e| SpawnError::Os(e.to_string()))
    }
}

#[async_trait]
impl ProcessBackend for UnixProcessBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        // One spawn at a time per scope: the first child creates the group; later children
        // join it. Without this, two concurrent first-spawns both use pgid=0 and leak a group.
        let scope_lock = self.scope_lock(scope);
        let _serial = scope_lock.lock().await;

        let existing_pgid = self.joinable_pgid(scope);
        let (mut child, new_group) =
            match self.spawn_in_group(spec, existing_pgid.unwrap_or(0)).await {
                Ok(child) => (child, existing_pgid.is_none()),
                Err(_err) if existing_pgid.is_some() => {
                    // The recorded group is gone (last member exited; kernel recycled the pgid).
                    // Forget it and create a fresh group for this still-open scope.
                    self.forget_group(scope);
                    (self.spawn_in_group(spec, 0).await?, true)
                }
                Err(err) => return Err(err),
            };

        let pid = child
            .id()
            .ok_or_else(|| SpawnError::Os("child has no pid".into()))?;
        let reuse_token =
            read_start_time(pid).map_or(ReuseToken::Unavailable, ReuseToken::StartTime);
        let os = OsIdentity::new(pid, reuse_token);
        #[cfg(target_os = "linux")]
        let pidfd = open_pidfd(pid);

        let (exit_tx, keep_rx) = watch::channel(None);
        self.register_child(
            scope,
            os,
            exit_tx.clone(),
            keep_rx,
            new_group,
            #[cfg(target_os = "linux")]
            pidfd,
        );

        let this = self.clone();
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
            this.note_os_exit(scope);
        });

        Ok(Spawned { os })
    }

    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        #[cfg(target_os = "linux")]
        {
            // Hold the lock across the syscall so the pidfd cannot be closed underneath us.
            let state = self.state.lock().expect("unix backend mutex");
            if let Some(fd) = state
                .children
                .get(&child_key(&target.os))
                .and_then(|slot| slot.pidfd.as_ref())
            {
                return pidfd_kill(fd.as_raw_fd(), to_nix(signal));
            }
        }

        if !identity_still_matches(&target.os) {
            return Ok(());
        }
        let pid = Pid::from_raw(i32::try_from(target.os.pid).unwrap_or(0));
        match kill(pid, Some(to_nix(signal))) {
            Ok(()) => Ok(()),
            Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(e) => Err(TerminateError::Signal(e.to_string())),
        }
    }

    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        let pgid = self.existing_pgid(scope);
        let Some(pgid) = pgid else {
            return Ok(());
        };
        match killpg(Pid::from_raw(pgid), Some(to_nix(signal))) {
            Ok(()) => Ok(()),
            Err(nix::errno::Errno::ESRCH) => {
                self.forget_group(scope);
                Ok(())
            }
            Err(e) => Err(TerminateError::Signal(e.to_string())),
        }
    }

    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        let key = child_key(&target.os);
        let mut rx = {
            let state = self.state.lock().expect("unix backend mutex");
            match state.children.get(&key) {
                Some(slot) => slot.sender.subscribe(),
                None => {
                    return Err(WaitError::Backend(format!(
                        "no child registered for pid {}",
                        target.os.pid
                    )))
                }
            }
        };
        let exit = loop {
            if let Some(exit) = *rx.borrow_and_update() {
                break exit;
            }
            if rx.changed().await.is_err() {
                return Err(WaitError::Backend("waiter channel closed".into()));
            }
        };
        self.state
            .lock()
            .expect("unix backend mutex")
            .children
            .remove(&key);
        Ok(exit)
    }

    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        if !identity_still_matches(&target.os) {
            return Err(StatsError::Backend(
                "process identity no longer matches".into(),
            ));
        }
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

    fn hard_kill_all(&self) {
        let groups: Vec<i32> = self
            .state
            .lock()
            .expect("unix backend mutex")
            .scope_groups
            .values()
            .map(|g| g.pgid)
            .collect();
        for pgid in groups {
            let _ = killpg(Pid::from_raw(pgid), Some(NixSignal::SIGKILL));
        }
    }
}

fn child_key(os: &OsIdentity) -> ChildKey {
    let token = match os.reuse_token {
        ReuseToken::StartTime(t) => t,
        ReuseToken::Unavailable => 0,
    };
    (os.pid, token)
}

fn identity_still_matches(os: &OsIdentity) -> bool {
    match os.reuse_token {
        ReuseToken::Unavailable => true,
        ReuseToken::StartTime(expected) => match read_start_time(os.pid) {
            Some(actual) => actual == expected,
            None => false,
        },
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
fn open_pidfd(pid: u32) -> Option<OwnedFd> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0i32) };
    if fd < 0 {
        None
    } else {
        // SAFETY: `pidfd_open` returned a new file descriptor we now own.
        Some(unsafe { OwnedFd::from_raw_fd(fd as i32) })
    }
}

#[cfg(target_os = "linux")]
fn pidfd_kill(fd: i32, signal: NixSignal) -> Result<(), TerminateError> {
    let rc = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd,
            signal as i32,
            std::ptr::null::<libc::c_void>(),
            0i32,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(TerminateError::Signal(err.to_string()))
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The waiter task reaps as soon as the OS child exits. A late `wait()` — the
    /// monitor not yet scheduled, or a caller that subscribed after reap — must still
    /// observe the recorded exit instead of `WaitError` / `ReapFailed`.
    #[tokio::test]
    async fn late_wait_after_natural_exit_still_sees_status() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let spec = ProcessSpec::new("true");
        let spawned = backend.spawn(scope, &spec).await.expect("spawn true");
        tokio::time::sleep(Duration::from_millis(150)).await;
        let exit = backend
            .wait(&spawned)
            .await
            .expect("late wait must not lose a reaped child");
        assert_eq!(exit.code, Some(0));
    }
}
