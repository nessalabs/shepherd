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
use std::future::Future;
use std::os::unix::process::ExitStatusExt;
use std::process::Stdio;
use std::sync::{Arc, Mutex, Weak};
use std::task::Poll;

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
type ExitChannels = (
    watch::Sender<Option<Result<RawExit, String>>>,
    watch::Receiver<Option<Result<RawExit, String>>>,
);

struct ChildSlot {
    scope: ProcessScopeId,
    output: Option<shepherd_app::output::ProcessOutput>,
    sampling: Arc<std::sync::atomic::AtomicBool>,
    sender: watch::Sender<Option<Result<RawExit, String>>>,
    // Retain a receiver so the waiter task's `send` is never lost if it fires before the
    // supervisor's monitor subscribes.
    _keep: watch::Receiver<Option<Result<RawExit, String>>>,
    #[cfg(target_os = "linux")]
    pidfd: Option<OwnedFd>,
}

struct Anchor {
    _input: tokio::process::ChildStdin,
    exit: watch::Sender<Option<Result<(), String>>>,
    kill_issued: bool,
}

// Constructed before task dispatch: cancellation before its first poll still runs Drop.
struct AnchorWait {
    child: tokio::process::Child,
    state: Weak<Mutex<State>>,
    exit: watch::Sender<Option<Result<(), String>>>,
    scope: ProcessScopeId,
    pgid: i32,
    armed: bool,
}
impl AnchorWait {
    fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for AnchorWait {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let state = self.state.upgrade();
        let mut state = state
            .as_ref()
            .map(|s| s.lock().unwrap_or_else(|p| p.into_inner()));
        // Child still owns the PID here. Kill before dropping it (Tokio's Child Drop may
        // try_wait/reap), and publish pin loss before another caller can signal the group.
        let _ = killpg(Pid::from_raw(self.pgid), Some(NixSignal::SIGKILL));
        if let Some(state) = state.as_mut() {
            if state
                .scope_groups
                .get(&self.scope)
                .is_some_and(|g| g.pgid == self.pgid)
            {
                if let Some(anchor) = state.anchors.get_mut(&self.scope) {
                    anchor.kill_issued = true;
                }
            }
        }
        self.exit.send_replace(Some(Err(
            "anchor waiter interrupted; synchronous group kill issued, reap unverified".into(),
        )));
    }
}

struct ScopeGroup {
    pgid: i32,
    live: usize,
}

#[derive(Default)]
struct State {
    children: HashMap<ChildKey, ChildSlot>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    cpu_samples: HashMap<ChildKey, (std::time::Instant, u64)>,
    scope_groups: HashMap<ProcessScopeId, ScopeGroup>,
    anchors: HashMap<ProcessScopeId, Anchor>,
    /// Serializes spawns per scope so the first process creates exactly one process group.
    scope_locks: HashMap<ProcessScopeId, Arc<tokio::sync::Mutex<()>>>,
}

/// A real Unix [`ProcessBackend`].
#[derive(Clone, Default)]
pub struct UnixProcessBackend {
    sampling: super::sampling::SamplingPool,
    state: Arc<Mutex<State>>,
    #[cfg(target_os = "linux")]
    cgroups: Option<Arc<super::cgroup::Cgroups>>,
}

impl std::fmt::Debug for UnixProcessBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixProcessBackend").finish_non_exhaustive()
    }
}

impl UnixProcessBackend {
    fn sample_sync(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        if matches!(target.os.reuse_token, ReuseToken::Unavailable) {
            return Err(StatsError::Backend(
                "no safe sample identity available".into(),
            ));
        }
        if !identity_still_matches(&target.os) {
            return Err(StatsError::Backend(
                "process identity no longer matches".into(),
            ));
        }
        let raw = sample_process(target.os.pid)?;
        #[cfg(target_os = "linux")]
        let raw = {
            let stat = std::fs::read_to_string(format!("/proc/{}/stat", target.os.pid))
                .map_err(|e| StatsError::Backend(e.to_string()))?;
            let fields: Vec<_> = stat
                .rsplit_once(')')
                .ok_or_else(|| StatsError::Backend("invalid proc stat".into()))?
                .1
                .split_whitespace()
                .collect();
            let ticks = fields
                .get(11)
                .and_then(|s| s.parse::<u64>().ok())
                .zip(fields.get(12).and_then(|s| s.parse::<u64>().ok()))
                .map(|(u, s)| u.saturating_add(s))
                .ok_or_else(|| StatsError::Backend("invalid CPU counters".into()))?;
            let now = std::time::Instant::now();
            let mut state = self.state.lock().expect("unix backend mutex");
            if !state.children.contains_key(&child_key(&target.os)) {
                return Err(StatsError::Backend("child reaped during sample".into()));
            }
            let previous = state
                .cpu_samples
                .insert(child_key(&target.os), (now, ticks));
            let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
            if hz <= 0 {
                return Err(StatsError::Backend("invalid clock tick rate".into()));
            }
            let cpu_usage = previous
                .map(|(time, old)| {
                    ticks.saturating_sub(old) as f64
                        / hz as f64
                        / now.duration_since(time).as_secs_f64().max(1e-9)
                })
                .unwrap_or(0.0) as f32;
            RawStats { cpu_usage, ..raw }
        };
        #[cfg(target_os = "macos")]
        let raw = {
            let task =
                libproc::proc_pid::pidinfo::<libproc::task_info::TaskInfo>(target.os.pid as i32, 0)
                    .map_err(StatsError::Backend)?;
            let mut timebase = mach2::mach_time::mach_timebase_info_data_t { numer: 0, denom: 0 };
            // TaskInfo reports Mach absolute time units, not nanoseconds on ARM.
            if unsafe { mach2::mach_time::mach_timebase_info(&mut timebase) } != 0
                || timebase.denom == 0
            {
                return Err(StatsError::Backend("invalid Mach timebase".into()));
            }
            let ticks = ((task.pti_total_user as u128 + task.pti_total_system as u128)
                * timebase.numer as u128
                / timebase.denom as u128)
                .min(u64::MAX as u128) as u64;
            let now = std::time::Instant::now();
            let mut state = self.state.lock().expect("unix backend mutex");
            if !state.children.contains_key(&child_key(&target.os)) {
                return Err(StatsError::Backend("child reaped during sample".into()));
            }
            let previous = state
                .cpu_samples
                .insert(child_key(&target.os), (now, ticks));
            let cpu_usage = previous
                .map(|(time, old)| {
                    ticks.saturating_sub(old) as f64
                        / 1e9
                        / now.duration_since(time).as_secs_f64().max(1e-9)
                })
                .unwrap_or(0.0) as f32;
            RawStats {
                cpu_usage,
                memory_rss_bytes: task.pti_resident_size,
                virtual_memory_bytes: Some(task.pti_virtual_size),
                ..raw
            }
        };
        if !identity_still_matches(&target.os) {
            return Err(StatsError::Backend("child changed during sample".into()));
        }
        Ok(raw)
    }

    /// Creates an empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects cgroup v2 if a delegated ancestor permits creation and cgroup.kill.
    /// Otherwise returns an honest process-group backend.
    #[cfg(target_os = "linux")]
    pub fn auto() -> Self {
        super::cgroup::Cgroups::detect()
            .map(|cgroups| Self {
                sampling: super::sampling::SamplingPool::default(),
                state: Arc::default(),
                cgroups: Some(Arc::new(cgroups)),
            })
            .unwrap_or_else(|_| Self::new())
    }

    /// Requires a real writable cgroup v2 ancestor; never silently falls back.
    #[cfg(target_os = "linux")]
    pub fn with_cgroup_root(root: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self {
            sampling: super::sampling::SamplingPool::default(),
            state: Arc::default(),
            cgroups: Some(Arc::new(super::cgroup::Cgroups::new(root.as_ref())?)),
        })
    }

    fn containment(&self) -> Containment {
        #[cfg(target_os = "linux")]
        if self.cgroups.is_some() {
            return Containment::CgroupV2;
        }
        Containment::ProcessGroup
    }

    fn scope_lock(&self, scope: ProcessScopeId) -> Arc<tokio::sync::Mutex<()>> {
        let mut state = self.state.lock().expect("unix backend mutex");
        state
            .scope_locks
            .entry(scope)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn ensure_anchor(&self, scope: ProcessScopeId) -> Result<(), SpawnError> {
        if self.containment() != Containment::ProcessGroup {
            return Ok(());
        }
        {
            let mut state = self.state.lock().expect("unix backend mutex");
            if let Some(anchor) = state.anchors.get(&scope) {
                if anchor.kill_issued {
                    return Err(SpawnError::ScopeClosed(scope));
                }
                let exit = anchor.exit.borrow().clone();
                match exit {
                    None => return Ok(()),
                    Some(Err(error)) => {
                        return Err(SpawnError::Os(format!("anchor reap failed: {error}")))
                    }
                    Some(Ok(())) => {}
                }
                let group = state.scope_groups.get(&scope).expect("anchor group");
                // Reaping releases the PID pin. Never join or signal a possibly recycled
                // group. Only replace a group after proving that its number is absent.
                if group.live != 0
                    || killpg(Pid::from_raw(group.pgid), None) != Err(nix::errno::Errno::ESRCH)
                {
                    return Err(SpawnError::Os(
                        "scope anchor exited while its group may still exist; cleanup required"
                            .into(),
                    ));
                }
                state.anchors.remove(&scope);
                state.scope_groups.remove(&scope);
            }
        }
        // A shell blocked in its builtin read creates no extra child. Its stdin is private.
        // Keeping this group leader alive pins the PGID across natural root exits.
        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .args(["-c", "trap '' HUP INT TERM; read shepherd_scope_anchor"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        unsafe {
            command.pre_exec(|| {
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = libc::SIG_IGN;
                libc::sigemptyset(&mut action.sa_mask);
                for signal in [libc::SIGHUP, libc::SIGINT, libc::SIGTERM] {
                    if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().map_err(|e| SpawnError::Os(e.to_string()))?;
        let pgid = child.id().expect("new anchor pid") as i32;
        let input = child.stdin.take().expect("anchor stdin");
        let (exit, _) = watch::channel(None);
        let mut worker = AnchorWait {
            child,
            state: Arc::downgrade(&self.state),
            exit: exit.clone(),
            scope,
            pgid,
            armed: true,
        };
        tokio::spawn(async move {
            let state = worker.state.clone();
            let sender = worker.exit.clone();
            let mut wait = Box::pin(worker.child.wait());
            std::future::poll_fn(|cx| {
                // Serialize the syscall that reaps (and releases the PID) with group
                // signaling and publish the loss of the pin before unlocking.
                let state = state.upgrade();
                let _guard = state
                    .as_ref()
                    .map(|state| state.lock().expect("unix backend mutex"));
                match wait.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        sender.send_replace(Some(result.map(|_| ()).map_err(|e| e.to_string())));
                        Poll::Ready(())
                    }
                    Poll::Pending => Poll::Pending,
                }
            })
            .await;
            drop(wait);
            worker.disarm();
        });
        let mut state = self.state.lock().expect("unix backend mutex");
        state.anchors.insert(
            scope,
            Anchor {
                _input: input,
                exit,
                kill_issued: false,
            },
        );
        state
            .scope_groups
            .insert(scope, ScopeGroup { pgid, live: 0 });
        Ok(())
    }

    fn register_child(
        &self,
        scope: ProcessScopeId,
        os: OsIdentity,
        channels: ExitChannels,
        new_group: bool,
        output: Option<shepherd_app::output::ProcessOutput>,
        #[cfg(target_os = "linux")] pidfd: Option<OwnedFd>,
    ) {
        let mut state = self.state.lock().expect("unix backend mutex");
        state.children.insert(
            child_key(&os),
            ChildSlot {
                scope,
                output,
                sampling: Arc::default(),
                sender: channels.0,
                _keep: channels.1,
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

    fn publish_wait_result(
        &self,
        scope: ProcessScopeId,
        sender: &watch::Sender<Option<Result<RawExit, String>>>,
        raw: Result<RawExit, String>,
    ) {
        if raw.is_ok() {
            self.note_os_exit(scope);
        }
        let _ = sender.send(Some(raw));
    }

    fn spawn_in_group(
        &self,
        spec: &ProcessSpec,
        target_pgid: i32,
        #[cfg(target_os = "linux")] membership: Option<&std::fs::File>,
    ) -> Result<tokio::process::Child, SpawnError> {
        let mut cmd = tokio::process::Command::new(&spec.program);
        cmd.args(&spec.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if matches!(spec.output, shepherd_domain::OutputMode::Capture { .. }) {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        }
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        apply_env(&mut cmd, &spec.env);

        // SAFETY: `setpgid` is async-signal-safe and the closure touches only its captured
        // `target_pgid` copy, so it is sound to run between fork and exec.
        #[cfg(target_os = "linux")]
        let membership_fd = membership.map(AsRawFd::as_raw_fd);
        unsafe {
            cmd.pre_exec(move || {
                if libc::setpgid(0, target_pgid) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                #[cfg(target_os = "linux")]
                if let Some(fd) = membership_fd {
                    // Writing 0 moves the calling child before exec can fork descendants.
                    // write is async-signal-safe; the parent retains the opened file.
                    if libc::write(fd, b"0".as_ptr().cast(), 1) != 1 {
                        return Err(std::io::Error::last_os_error());
                    }
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

        #[cfg(target_os = "linux")]
        let membership = self
            .cgroups
            .as_ref()
            .map(|c| c.membership(scope))
            .transpose()
            .map_err(|e| SpawnError::Os(e.to_string()))?;
        let (mut child, new_group) = loop {
            self.ensure_anchor(scope)?;
            let mut state = self.state.lock().expect("unix backend mutex");
            if state
                .anchors
                .get(&scope)
                .is_some_and(|a| a.kill_issued || a.exit.borrow().is_some())
            {
                // The anchor reaped between ensure_anchor and this lock acquisition.
                // Recheck recovery before choosing a numeric group to join.
                drop(state);
                continue;
            }
            let existing_pgid = state
                .scope_groups
                .get(&scope)
                .and_then(|g| (g.live > 0 || state.anchors.contains_key(&scope)).then_some(g.pgid));
            // Keep the PID pin until setpgid has executed in the new child. The
            // anchor's wait task takes this same mutex before reaping it.
            let spawned = self.spawn_in_group(
                spec,
                existing_pgid.unwrap_or(0),
                #[cfg(target_os = "linux")]
                membership.as_ref(),
            );
            break match spawned {
                Ok(child) => (child, existing_pgid.is_none()),
                Err(_) if existing_pgid.is_some() && !state.anchors.contains_key(&scope) => {
                    // Cgroup containment also owns all descendants of the old group.
                    state.scope_groups.remove(&scope);
                    (
                        self.spawn_in_group(
                            spec,
                            0,
                            #[cfg(target_os = "linux")]
                            membership.as_ref(),
                        )?,
                        true,
                    )
                }
                Err(error) => return Err(error),
            };
        };

        let pid = child
            .id()
            .ok_or_else(|| SpawnError::Os("child has no pid".into()))?;
        let reuse_token =
            read_start_time(pid).map_or(ReuseToken::Unavailable, ReuseToken::StartTime);
        let os = OsIdentity::new(pid, reuse_token);
        #[cfg(target_os = "linux")]
        let pidfd = open_pidfd(pid);

        let output = match spec.output {
            shepherd_domain::OutputMode::Discard => None,
            shepherd_domain::OutputMode::Capture {
                buffer_bytes,
                tail_bytes,
            } => Some(crate::output::capture(buffer_bytes, tail_bytes)),
        };
        let mut readers = Vec::new();
        if let Some(output) = &output {
            use shepherd_app::output::OutputStream;
            if let Some(stdout) = child.stdout.take() {
                readers.push((
                    OutputStream::Stdout,
                    tokio::spawn(crate::output::drain(
                        stdout,
                        output.clone(),
                        OutputStream::Stdout,
                    )),
                ));
            }
            if let Some(stderr) = child.stderr.take() {
                readers.push((
                    OutputStream::Stderr,
                    tokio::spawn(crate::output::drain(
                        stderr,
                        output.clone(),
                        OutputStream::Stderr,
                    )),
                ));
            }
        }
        let (exit_tx, keep_rx) = watch::channel(None);
        self.register_child(
            scope,
            os,
            (exit_tx.clone(), keep_rx),
            new_group,
            output.clone(),
            #[cfg(target_os = "linux")]
            pidfd,
        );

        let this = self.clone();
        tokio::spawn(async move {
            let status = child.wait().await;
            let raw = match status {
                Ok(status) => Ok(RawExit {
                    code: status.code(),
                    signal: status.signal().map(signal_from_raw),
                    core_dumped: status.core_dumped(),
                }),
                Err(error) => Err(error.to_string()),
            };
            // Root reap is independent of inherited pipe lifetimes. Keep owning the
            // readers here, but let termination observers see the actual exit now.
            this.publish_wait_result(scope, &exit_tx, raw);
            if let Some(output) = output {
                crate::output::finish_readers(readers, output).await;
            }
        });

        Ok(Spawned { os })
    }

    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        if !self
            .state
            .lock()
            .expect("unix backend mutex")
            .children
            .contains_key(&child_key(&target.os))
        {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            // Hold the lock across the syscall so the pidfd cannot be closed underneath us.
            let state = self.state.lock().expect("unix backend mutex");
            if let Some(fd) = state
                .children
                .get(&child_key(&target.os))
                .and_then(|slot| slot.pidfd.as_ref())
            {
                return pidfd_kill(fd.as_raw_fd(), to_nix(signal)?);
            }
        }

        if matches!(target.os.reuse_token, ReuseToken::Unavailable) {
            return Err(TerminateError::Signal(
                "no safe process identity available".into(),
            ));
        }
        if !identity_still_matches(&target.os) {
            return Ok(());
        }
        let pid = Pid::from_raw(i32::try_from(target.os.pid).unwrap_or(0));
        match kill(pid, Some(to_nix(signal)?)) {
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
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups {
            if signal == Signal::Kill {
                return cgroups
                    .kill(scope)
                    .map_err(|e| TerminateError::Signal(e.to_string()));
            }
        }
        let mut state = self.state.lock().expect("unix backend mutex");
        signal_group(&mut state, scope, to_nix(signal)?)
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
            if let Some(exit) = rx.borrow_and_update().clone() {
                break exit;
            }
            if rx.changed().await.is_err() {
                return Err(WaitError::Backend("waiter channel closed".into()));
            }
        };
        // Failed wait is not reap evidence; retain exact identity for kill backstops.
        if exit.is_ok() {
            let mut state = self.state.lock().expect("unix backend mutex");
            state.children.remove(&key);
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            state.cpu_samples.remove(&key);
        }
        exit.map_err(WaitError::Backend)
    }

    async fn cleanup_scope(&self, scope: ProcessScopeId) -> Result<(), TerminateError> {
        self.signal_scope(scope, Signal::Kill).await?;
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups {
            cgroups
                .finish(scope)
                .await
                .map_err(|e| TerminateError::Signal(e.to_string()))?;
        }
        let anchor = self
            .state
            .lock()
            .expect("unix backend mutex")
            .anchors
            .get(&scope)
            .map(|a| a.exit.subscribe());
        if let Some(mut receiver) = anchor {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if let Some(result) = receiver.borrow_and_update().clone() {
                        return result.map_err(TerminateError::Signal);
                    }
                    receiver
                        .changed()
                        .await
                        .map_err(|_| TerminateError::Signal("anchor waiter closed".into()))?;
                }
            })
            .await
            .map_err(|_| TerminateError::Signal("anchor reap timed out".into()))??;
        }
        // Never expose a group entry without its anchor after the PID was reaped.
        // Synchronous owner Drop may signal scopes concurrently with this cleanup.
        let mut state = self.state.lock().expect("unix backend mutex");
        state.anchors.remove(&scope);
        state.scope_groups.remove(&scope);
        state.scope_locks.remove(&scope);
        Ok(())
    }

    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        let active = self
            .state
            .lock()
            .expect("unix backend mutex")
            .children
            .get(&child_key(&target.os))
            .ok_or_else(|| StatsError::Backend("process reaped".into()))?
            .sampling
            .clone();
        let backend = self.clone();
        let target = *target;
        self.sampling
            .run(active, move || backend.sample_sync(&target))
            .await
    }

    fn output(&self, target: &Spawned) -> Option<shepherd_app::output::ProcessOutput> {
        self.state
            .lock()
            .expect("unix backend mutex")
            .children
            .get(&child_key(&target.os))
            .and_then(|slot| slot.output.clone())
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            descendant_containment: self.containment(),
            cpu: cpu_support(),
            rss: rss_support(),
            peak_rss: peak_support(),
            io: io_support(),
            force_termination: true,
        }
    }

    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups {
            if let Err(error) = cgroups.kill(scope) {
                tracing::error!(%scope, %error, "cgroup hard kill failed; attempting registered roots only");
            }
            let state = self.state.lock().expect("unix backend mutex");
            kill_registered_roots(&state, Some(scope));
            return;
        }
        let mut state = self.state.lock().expect("unix backend mutex");
        if let Err(error) = signal_group(&mut state, scope, NixSignal::SIGKILL) {
            tracing::error!(%error, "cannot safely signal scope group after anchor loss");
        }
        // Cleanup may have retired the group while a failed reap retained a root.
        kill_registered_roots(&state, Some(scope));
    }

    fn hard_kill_all(&self) {
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups {
            let _ = cgroups.kill_all();
            let state = self.state.lock().expect("unix backend mutex");
            kill_registered_roots(&state, None);
            return;
        }
        let mut state = self.state.lock().expect("unix backend mutex");
        let scopes: Vec<_> = state.scope_groups.keys().copied().collect();
        for scope in scopes {
            if let Err(error) = signal_group(&mut state, scope, NixSignal::SIGKILL) {
                tracing::error!(%error, "cannot safely signal scope group after anchor loss");
            }
        }
        kill_registered_roots(&state, None);
    }
}

fn signal_group(
    state: &mut State,
    scope: ProcessScopeId,
    signal: NixSignal,
) -> Result<(), TerminateError> {
    let Some(group) = state.scope_groups.get(&scope) else {
        return Ok(());
    };
    let pgid = Pid::from_raw(group.pgid);
    if let Some(anchor) = state.anchors.get(&scope) {
        if anchor.kill_issued {
            return Ok(());
        }
        if anchor.exit.borrow().is_some() {
            if killpg(pgid, None) == Err(nix::errno::Errno::ESRCH) {
                return Ok(());
            }
            return Err(TerminateError::Signal(
                "scope anchor was reaped; refusing to signal an unpinned group".into(),
            ));
        }
    }
    match killpg(pgid, Some(signal)) {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => {
            if signal == NixSignal::SIGKILL {
                if let Some(anchor) = state.anchors.get_mut(&scope) {
                    anchor.kill_issued = true;
                }
            }
            Ok(())
        }
        Err(error) => Err(TerminateError::Signal(error.to_string())),
    }
}

fn kill_registered_roots(state: &State, scope: Option<ProcessScopeId>) {
    for ((pid, token), slot) in state
        .children
        .iter()
        .filter(|(_, slot)| scope.is_none_or(|scope| slot.scope == scope))
    {
        #[cfg(target_os = "linux")]
        if let Some(fd) = &slot.pidfd {
            let _ = pidfd_kill(fd.as_raw_fd(), NixSignal::SIGKILL);
            continue;
        }
        #[cfg(not(target_os = "linux"))]
        let _ = slot;
        if *token != 0 && read_start_time(*pid) == Some(*token) {
            let _ = kill(Pid::from_raw(*pid as i32), Some(NixSignal::SIGKILL));
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod cgroup_backstop_tests {
    use super::*;
    use std::fs::File;
    use std::time::Duration;

    async fn failed_kill_file_preserves_root_backstop(all: bool) {
        let delegated = std::env::var_os("SHEPHERD_CGROUP_ROOT")
            .expect("set a writable delegated cgroup v2 ancestor");
        let backend = UnixProcessBackend::with_cgroup_root(delegated).unwrap();
        let scope = ProcessScopeId::new(1);
        let other = ProcessScopeId::new(2);
        let spec = ProcessSpec::new("/bin/sleep").arg("30");
        let root = backend.spawn(scope, &spec).await.unwrap();
        let survivor = backend.spawn(other, &spec).await.unwrap();
        assert!(
            backend.state.lock().unwrap().children[&child_key(&root.os)]
                .pidfd
                .is_some(),
            "privileged Linux regression requires a real pidfd"
        );
        let cgroups = backend.cgroups.as_ref().unwrap();
        // Inject an actual write error into the retained descriptor; normal cgroup
        // creation, membership, the OS child, and its pidfd are all real.
        let saved = cgroups.replace_kill_file(scope, File::open("/dev/null").unwrap());
        assert_eq!(
            cgroups.kill(scope).unwrap_err().raw_os_error(),
            Some(libc::EBADF)
        );
        let sender = backend.state.lock().unwrap().children[&child_key(&root.os)]
            .sender
            .clone();
        backend.publish_wait_result(scope, &sender, Err("injected wait failure".into()));
        assert!(backend.wait(&root).await.is_err());
        assert!(backend.state.lock().unwrap().children[&child_key(&root.os)]
            .pidfd
            .is_some());
        let mut recovered = sender.subscribe();
        if all {
            backend.hard_kill_all();
        } else {
            backend.hard_kill_scope(scope);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(&*recovered.borrow_and_update(), Some(Ok(_))) {
                    break;
                }
                recovered.changed().await.unwrap();
            }
        })
        .await
        .expect("failed wait discarded root kill identity");
        let exit = tokio::time::timeout(Duration::from_secs(5), backend.wait(&root))
            .await
            .expect("failed cgroup write abandoned its registered root")
            .unwrap();
        assert_eq!(exit.signal, Some(Signal::Kill));
        // Root reap does not establish descendant containment. The failed cgroup
        // interface must still cause explicit cleanup to return an error.
        assert!(backend.cleanup_scope(scope).await.is_err());
        assert_eq!(
            backend.capabilities().descendant_containment,
            Containment::CgroupV2
        );
        if !all {
            assert!(
                tokio::time::timeout(Duration::from_millis(30), backend.wait(&survivor))
                    .await
                    .is_err(),
                "scope-only fallback signalled another scope"
            );
            backend.cleanup_scope(other).await.unwrap();
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), backend.wait(&survivor))
                .await
                .unwrap()
                .unwrap()
                .signal,
            Some(Signal::Kill)
        );
        if all {
            backend.cleanup_scope(other).await.unwrap();
        }
        drop(cgroups.replace_kill_file(scope, saved));
        backend.cleanup_scope(scope).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires delegated cgroup v2; privileged CI runs this fail-closed"]
    async fn scope_backstop_kills_roots_when_cgroup_write_fails() {
        failed_kill_file_preserves_root_backstop(false).await;
    }

    #[tokio::test]
    #[ignore = "requires delegated cgroup v2; privileged CI runs this fail-closed"]
    async fn all_backstop_kills_roots_when_one_cgroup_write_fails() {
        failed_kill_file_preserves_root_backstop(true).await;
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

fn to_nix(signal: Signal) -> Result<NixSignal, TerminateError> {
    Ok(match signal {
        Signal::Term => NixSignal::SIGTERM,
        Signal::Kill => NixSignal::SIGKILL,
        Signal::Interrupt => NixSignal::SIGINT,
        Signal::Custom(raw) => {
            NixSignal::try_from(raw).map_err(|e| TerminateError::Signal(e.to_string()))?
        }
    })
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

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn cpu_support() -> Support {
    Support::Supported
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn cpu_support() -> Support {
    Support::Unsupported
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rss_support() -> Support {
    Support::Supported
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn rss_support() -> Support {
    Support::Unsupported
}

#[cfg(target_os = "linux")]
fn io_support() -> Support {
    Support::Supported
}
#[cfg(not(target_os = "linux"))]
fn io_support() -> Support {
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
#[cfg(target_os = "macos")]
fn read_start_time(pid: u32) -> Option<u64> {
    let info = libproc::proc_pid::pidinfo::<libproc::bsd_info::BSDInfo>(pid as i32, 0).ok()?;
    Some(
        info.pbi_start_tvsec
            .saturating_mul(1_000_000)
            .saturating_add(info.pbi_start_tvusec),
    )
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
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
    let io = std::fs::read_to_string(format!("/proc/{pid}/io"))
        .map_err(|e| StatsError::Backend(e.to_string()))?;
    let io_field = |key: &str| {
        io.lines()
            .find_map(|line| line.strip_prefix(key))
            .and_then(|v| v.trim().parse().ok())
    };
    Ok(RawStats {
        cpu_usage: 0.0,
        memory_rss_bytes: rss,
        virtual_memory_bytes: vsize,
        peak_rss_bytes: peak,
        io_read_bytes: io_field("read_bytes:"),
        io_write_bytes: io_field("write_bytes:"),
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

#[cfg(test)]
mod identity_tests {
    use super::*;
    #[tokio::test]
    async fn mismatched_reuse_token_cannot_signal_a_live_child() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let child = backend
            .spawn(scope, &ProcessSpec::new("sleep").arg("30"))
            .await
            .unwrap();
        let wrong = Spawned {
            os: OsIdentity::new(child.os.pid, ReuseToken::StartTime(u64::MAX)),
        };
        backend.signal(&wrong, Signal::Kill).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), backend.wait(&child))
                .await
                .is_err()
        );
        backend.signal(&child, Signal::Kill).await.unwrap();
        assert_eq!(
            backend.wait(&child).await.unwrap().signal,
            Some(Signal::Kill)
        );
        backend.cleanup_scope(scope).await.unwrap();
    }
}

#[cfg(test)]
mod anchor_tests {
    use super::*;
    use std::time::Duration;

    async fn anchor_reaped(backend: &UnixProcessBackend, scope: ProcessScopeId) {
        let mut exit = backend.state.lock().unwrap().anchors[&scope]
            .exit
            .subscribe();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(result) = exit.borrow_and_update().clone() {
                    result.unwrap();
                    break;
                }
                exit.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn group_broadcast_does_not_poison_an_open_scope() {
        for signal in ["TERM", "KILL"] {
            let backend = UnixProcessBackend::new();
            let scope = ProcessScopeId::new(1);
            let child = backend
                .spawn(
                    scope,
                    &ProcessSpec::new("/bin/sh").args(["-c", &format!("kill -{signal} 0")]),
                )
                .await
                .unwrap();
            let exit = backend.wait(&child).await.unwrap();
            assert!(exit.signal.is_some());
            if signal == "KILL" {
                anchor_reaped(&backend, scope).await;
            }
            let next = backend
                .spawn(scope, &ProcessSpec::new("true"))
                .await
                .expect("an empty open scope remains reusable after a group broadcast");
            assert_eq!(backend.wait(&next).await.unwrap().code, Some(0));
            backend.cleanup_scope(scope).await.unwrap();
        }
    }

    #[tokio::test]
    async fn reaped_anchor_cannot_signal_a_recycled_group_number() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let other = ProcessScopeId::new(2);
        let child = backend
            .spawn(scope, &ProcessSpec::new("true"))
            .await
            .unwrap();
        backend.wait(&child).await.unwrap();
        let old_pgid = backend.state.lock().unwrap().scope_groups[&scope].pgid;
        kill(Pid::from_raw(old_pgid), Some(NixSignal::SIGKILL)).unwrap();
        anchor_reaped(&backend, scope).await;
        let survivor = backend
            .spawn(other, &ProcessSpec::new("/bin/sleep").arg("30"))
            .await
            .unwrap();
        {
            // Model reuse deterministically instead of exhausting the host PID namespace.
            let mut state = backend.state.lock().unwrap();
            state.scope_groups.get_mut(&scope).unwrap().pgid = state.scope_groups[&other].pgid;
        }
        assert!(backend.signal_scope(scope, Signal::Kill).await.is_err());
        assert!(backend
            .spawn(scope, &ProcessSpec::new("true"))
            .await
            .is_err());
        backend.hard_kill_scope(scope);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), backend.wait(&survivor))
                .await
                .is_err()
        );
        backend
            .state
            .lock()
            .unwrap()
            .scope_groups
            .get_mut(&scope)
            .unwrap()
            .pgid = old_pgid;
        backend.cleanup_scope(scope).await.unwrap();
        backend.signal(&survivor, Signal::Kill).await.unwrap();
        backend.wait(&survivor).await.unwrap();
        backend.cleanup_scope(other).await.unwrap();
    }

    #[tokio::test]
    async fn scope_kill_rejects_new_admission_before_and_after_anchor_reap() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let root = backend
            .spawn(scope, &ProcessSpec::new("/bin/sleep").arg("30"))
            .await
            .unwrap();
        backend.hard_kill_all();
        assert!(
            matches!(backend.spawn(scope, &ProcessSpec::new("true")).await, Err(SpawnError::ScopeClosed(id)) if id == scope)
        );
        backend.wait(&root).await.unwrap();
        anchor_reaped(&backend, scope).await;
        assert!(
            matches!(backend.spawn(scope, &ProcessSpec::new("true")).await, Err(SpawnError::ScopeClosed(id)) if id == scope)
        );
        backend.cleanup_scope(scope).await.unwrap();
    }

    #[test]
    fn runtime_drop_marks_anchor_pin_unverified_even_before_first_poll() {
        for poll_waiter in [false, true] {
            let backend = UnixProcessBackend::new();
            let scope = ProcessScopeId::new(1);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                backend.ensure_anchor(scope).unwrap();
                if poll_waiter {
                    tokio::task::yield_now().await;
                }
            });
            let pgid = backend.state.lock().unwrap().scope_groups[&scope].pgid;
            drop(runtime);
            {
                let state = backend.state.lock().unwrap();
                let anchor = &state.anchors[&scope];
                assert!(anchor.kill_issued);
                assert!(anchor.exit.borrow().as_ref().unwrap().is_err());
            }
            // A new runtime must receive an honest error, not trust a stale PID pin or hang.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                assert!(backend.cleanup_scope(scope).await.is_err());
                assert!(matches!(
                    backend.spawn(scope, &ProcessSpec::new("true")).await,
                    Err(SpawnError::ScopeClosed(_))
                ));
            });
            // The interrupted runtime cannot guarantee async reap. Reap this test's own
            // anchor explicitly if Tokio's orphan reaper has not already consumed it.
            let waited = unsafe { libc::waitpid(pgid, std::ptr::null_mut(), 0) };
            assert!(
                waited == pgid
                    || (waited == -1
                        && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD))
            );
        }
    }

    async fn removed_group_backstop(all: bool) {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let other = ProcessScopeId::new(2);
        let spec = ProcessSpec::new("/bin/sleep").arg("30");
        let root = backend.spawn(scope, &spec).await.unwrap();
        let sibling = backend.spawn(other, &spec).await.unwrap();
        let group = backend
            .state
            .lock()
            .unwrap()
            .scope_groups
            .remove(&scope)
            .unwrap();
        if all {
            backend.hard_kill_all();
        } else {
            backend.hard_kill_scope(scope);
        }
        let result = tokio::time::timeout(Duration::from_secs(2), backend.wait(&root)).await;
        // Restore the real anchor's group before cleanup, including on regression failure.
        backend
            .state
            .lock()
            .unwrap()
            .scope_groups
            .insert(scope, group);
        let sibling_alive = identity_still_matches(&sibling.os);
        backend.hard_kill_all();
        if result.is_err() {
            let _ = backend.wait(&root).await;
        }
        let sibling_exit = backend.wait(&sibling).await.unwrap();
        backend.cleanup_scope(scope).await.unwrap();
        backend.cleanup_scope(other).await.unwrap();
        assert_eq!(
            result
                .expect("retained root was hidden by missing group")
                .unwrap()
                .signal,
            Some(Signal::Kill)
        );
        assert_eq!(sibling_exit.signal, Some(Signal::Kill));
        if !all {
            assert!(sibling_alive, "scope backstop killed sibling scope");
        }
    }

    #[tokio::test]
    async fn scope_backstop_reaches_root_after_group_removal() {
        removed_group_backstop(false).await;
    }

    #[tokio::test]
    async fn global_backstop_reaches_root_after_group_removal() {
        removed_group_backstop(true).await;
    }

    #[tokio::test]
    async fn lost_anchor_backstop_still_kills_registered_roots() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let root = backend
            .spawn(scope, &ProcessSpec::new("/bin/sleep").arg("30"))
            .await
            .unwrap();
        let pgid = backend.state.lock().unwrap().scope_groups[&scope].pgid;
        kill(Pid::from_raw(pgid), Some(NixSignal::SIGKILL)).unwrap();
        anchor_reaped(&backend, scope).await;
        backend.hard_kill_all();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), backend.wait(&root))
                .await
                .unwrap()
                .unwrap()
                .signal,
            Some(Signal::Kill)
        );
        backend.cleanup_scope(scope).await.unwrap();
    }
}

#[cfg(test)]
mod failed_wait_tests {
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn failed_wait_retains_identity_until_verified_reap() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let root = backend
            .spawn(scope, &ProcessSpec::new("/bin/sleep").arg("30"))
            .await
            .unwrap();
        let key = child_key(&root.os);
        let sender = backend.state.lock().unwrap().children[&key].sender.clone();
        backend.publish_wait_result(scope, &sender, Err("injected wait failure".into()));
        assert!(backend.wait(&root).await.is_err());
        assert!(backend.state.lock().unwrap().children.contains_key(&key));
        assert_eq!(backend.state.lock().unwrap().scope_groups[&scope].live, 1);
        // The real OS waiter remains alive and replaces the injected failure only
        // after it has actually reaped this process.
        let mut recovered = sender.subscribe();
        backend.hard_kill_all();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(&*recovered.borrow_and_update(), Some(Ok(_))) {
                    break;
                }
                recovered.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            backend.wait(&root).await.unwrap().signal,
            Some(Signal::Kill)
        );
        assert!(!backend.state.lock().unwrap().children.contains_key(&key));
        backend.cleanup_scope(scope).await.unwrap();
    }
}
