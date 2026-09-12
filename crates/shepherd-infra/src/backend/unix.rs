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

struct WaitChannels {
    sender: watch::Sender<Option<Result<RawExit, String>>>,
    keep: watch::Receiver<Option<Result<RawExit, String>>>,
    retry: tokio::sync::mpsc::Sender<()>,
    #[cfg(test)]
    failures: Arc<std::sync::atomic::AtomicUsize>,
}

struct ChildSlot {
    output: Option<shepherd_app::output::ProcessOutput>,
    sampling: Arc<std::sync::atomic::AtomicBool>,
    scope: ProcessScopeId,
    retry: tokio::sync::mpsc::Sender<()>,
    #[cfg(test)]
    failures: Arc<std::sync::atomic::AtomicUsize>,
    sender: watch::Sender<Option<Result<RawExit, String>>>,
    // Retain a receiver so the waiter task's `send` is never lost if it fires before the
    // supervisor's monitor subscribes.
    _keep: watch::Receiver<Option<Result<RawExit, String>>>,
    #[cfg(target_os = "linux")]
    pidfd: Option<OwnedFd>,
}

struct ScopeGroup {
    pgid: i32,
    live: usize,
    wait_failed: bool,
}

#[derive(Default)]
struct State {
    children: HashMap<ChildKey, ChildSlot>,
    #[cfg(target_os = "linux")]
    cpu_samples: HashMap<ChildKey, (std::time::Instant, u64)>,
    scope_groups: HashMap<ProcessScopeId, ScopeGroup>,
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
        channels: WaitChannels,
        new_group: bool,
        output: Option<shepherd_app::output::ProcessOutput>,
        #[cfg(target_os = "linux")] pidfd: Option<OwnedFd>,
    ) {
        let mut state = self.state.lock().expect("unix backend mutex");
        state.children.insert(
            child_key(&os),
            ChildSlot {
                output,
                sampling: Arc::default(),
                scope,
                sender: channels.sender,
                _keep: channels.keep,
                retry: channels.retry,
                #[cfg(test)]
                failures: channels.failures,
                #[cfg(target_os = "linux")]
                pidfd,
            },
        );
        let pgid = i32::try_from(os.pid).unwrap_or(0);
        if new_group {
            state.scope_groups.insert(
                scope,
                ScopeGroup {
                    pgid,
                    live: 1,
                    wait_failed: false,
                },
            );
        } else if let Some(group) = state.scope_groups.get_mut(&scope) {
            group.live = group.live.saturating_add(1);
        } else {
            state.scope_groups.insert(
                scope,
                ScopeGroup {
                    pgid,
                    live: 1,
                    wait_failed: false,
                },
            );
        }
    }

    #[cfg(test)]
    fn publish_wait_result(
        &self,
        scope: ProcessScopeId,
        sender: &watch::Sender<Option<Result<RawExit, String>>>,
        raw: Result<RawExit, String>,
    ) {
        publish_wait_result(&Arc::downgrade(&self.state), scope, sender, raw);
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
        // Serialize quarantine publication with the actual fork/exec admission.
        let (mut child, new_group) = {
            let mut state = self.state.lock().expect("unix backend mutex");
            let existing_pgid = match state.scope_groups.get(&scope) {
                Some(group) if group.wait_failed => return Err(SpawnError::ScopeClosed(scope)),
                Some(group) if group.live > 0 => Some(group.pgid),
                _ => None,
            };
            match self.spawn_in_group(
                spec,
                existing_pgid.unwrap_or(0),
                #[cfg(target_os = "linux")]
                membership.as_ref(),
            ) {
                Ok(child) => (child, existing_pgid.is_none()),
                Err(_err) if existing_pgid.is_some() => {
                    // The recorded group is gone (last member exited; kernel recycled the pgid).
                    // Forget it and create a fresh group for this still-open scope.
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
                Err(err) => return Err(err),
            }
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
        let (retry_tx, mut retry_rx) = tokio::sync::mpsc::channel(1);
        #[cfg(test)]
        let failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        self.register_child(
            scope,
            os,
            WaitChannels {
                sender: exit_tx.clone(),
                keep: keep_rx,
                retry: retry_tx,
                #[cfg(test)]
                failures: failures.clone(),
            },
            new_group,
            output.clone(),
            #[cfg(target_os = "linux")]
            pidfd,
        );

        let state = Arc::downgrade(&self.state);
        tokio::spawn(async move {
            loop {
                let status = wait_os_child(
                    &mut child,
                    #[cfg(test)]
                    &failures,
                )
                .await;
                let raw = status
                    .map(|status| RawExit {
                        code: status.code(),
                        signal: status.signal().map(signal_from_raw),
                        core_dumped: status.core_dumped(),
                    })
                    .map_err(|error| error.to_string());
                let reaped = raw.is_ok();
                publish_wait_result(&state, scope, &exit_tx, raw);
                if reaped || retry_rx.recv().await.is_none() {
                    break;
                }
            }
            if let Some(output) = output {
                crate::output::finish_readers(readers, output).await;
            }
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

        if matches!(target.os.reuse_token, ReuseToken::Unavailable) {
            return Err(TerminateError::Signal(
                "no safe signal identity available".into(),
            ));
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
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups {
            if signal == Signal::Kill {
                return cgroups
                    .kill(scope)
                    .map_err(|e| TerminateError::Signal(e.to_string()));
            }
        }
        let mut state = self.state.lock().expect("unix backend mutex");
        if state
            .scope_groups
            .get(&scope)
            .is_some_and(|group| group.wait_failed)
        {
            if signal == Signal::Kill {
                kill_registered_roots(&state, scope);
            }
            return Err(TerminateError::Signal(
                "scope group identity unverified after wait failure".into(),
            ));
        }
        let pgid = state.scope_groups.get(&scope).map(|group| group.pgid);
        let Some(pgid) = pgid else {
            return Ok(());
        };
        match killpg(Pid::from_raw(pgid), Some(to_nix(signal))) {
            Ok(()) => Ok(()),
            Err(nix::errno::Errno::ESRCH) => {
                state.scope_groups.remove(&scope);
                Ok(())
            }
            Err(e) => Err(TerminateError::Signal(e.to_string())),
        }
    }

    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        let key = child_key(&target.os);
        let (mut rx, retry) = {
            let state = self.state.lock().expect("unix backend mutex");
            match state.children.get(&key) {
                Some(slot) => (slot.sender.subscribe(), slot.retry.clone()),
                None => {
                    return Err(WaitError::Backend(format!(
                        "no child registered for pid {}",
                        target.os.pid
                    )))
                }
            }
        };
        let mut retry_failed = matches!(&*rx.borrow_and_update(), Some(Err(_)));
        if retry_failed {
            match retry.try_send(()) {
                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                    // A competing retry can publish its final success and close
                    // the worker after we observed the old error. Read the latest
                    // observation below instead of downgrading that success.
                    retry_failed = false;
                }
            }
        }
        drop(retry);
        if retry_failed {
            rx.changed()
                .await
                .map_err(|_| WaitError::Backend("waiter channel closed".into()))?;
        }
        let exit = loop {
            if let Some(exit) = rx.borrow_and_update().clone() {
                break exit;
            }
            if rx.changed().await.is_err() {
                return Err(WaitError::Backend("waiter channel closed".into()));
            }
        };
        // A waiter error is not proof that the root exited or was reaped. Keep its
        // identity (including pidfd) available to the synchronous kill backstop.
        // A later successful wait can consume a recovered reap observation.
        if exit.is_ok() {
            self.state
                .lock()
                .expect("unix backend mutex")
                .children
                .remove(&key);
        }
        #[cfg(target_os = "linux")]
        self.state
            .lock()
            .expect("unix backend mutex")
            .cpu_samples
            .remove(&key);
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
        self.forget_group(scope);
        self.state
            .lock()
            .expect("unix backend mutex")
            .scope_locks
            .remove(&scope);
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
            io: cpu_support(),
            force_termination: true,
        }
    }

    fn hard_kill_all(&self) {
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups {
            let _ = cgroups.kill_all();
            let state = self.state.lock().expect("unix backend mutex");
            // Retained roots remain reachable even after a containment entry was
            // removed: containment cleanup alone does not prove root reap.
            for scope in state
                .children
                .values()
                .map(|slot| slot.scope)
                .collect::<std::collections::HashSet<_>>()
            {
                kill_registered_roots(&state, scope);
            }
            return;
        }
        let state = self.state.lock().expect("unix backend mutex");
        for group in state
            .scope_groups
            .values()
            .filter(|group| !group.wait_failed)
        {
            let _ = killpg(Pid::from_raw(group.pgid), Some(NixSignal::SIGKILL));
        }
        for scope in state
            .children
            .values()
            .map(|slot| slot.scope)
            .collect::<std::collections::HashSet<_>>()
        {
            kill_registered_roots(&state, scope);
        }
    }
}

fn publish_wait_result(
    state: &std::sync::Weak<Mutex<State>>,
    scope: ProcessScopeId,
    sender: &watch::Sender<Option<Result<RawExit, String>>>,
    raw: Result<RawExit, String>,
) {
    if let Some(state) = state.upgrade() {
        let mut state = state.lock().expect("unix backend mutex");
        if let Some(group) = state.scope_groups.get_mut(&scope) {
            if raw.is_ok() {
                group.live = group.live.saturating_sub(1);
            } else {
                group.wait_failed = true;
            }
        }
    }
    let _ = sender.send(Some(raw));
}

async fn wait_os_child(
    child: &mut tokio::process::Child,
    #[cfg(test)] failures: &std::sync::atomic::AtomicUsize,
) -> std::io::Result<std::process::ExitStatus> {
    #[cfg(test)]
    if failures
        .fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |count| count.checked_sub(1),
        )
        .is_ok()
    {
        return Err(std::io::Error::other("injected OS wait failure"));
    }
    child.wait().await
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
fn cpu_support() -> Support {
    Support::Supported
}
#[cfg(not(target_os = "linux"))]
fn cpu_support() -> Support {
    Support::Unsupported
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

fn kill_registered_roots(state: &State, scope: ProcessScopeId) {
    for ((pid, token), slot) in state
        .children
        .iter()
        .filter(|(_, slot)| slot.scope == scope)
    {
        #[cfg(target_os = "linux")]
        if let Some(fd) = &slot.pidfd {
            let _ = pidfd_kill(fd.as_raw_fd(), NixSignal::SIGKILL);
            // Buffer the reap request even if a concurrent failed observation has
            // not been published yet. A successful waiter ignores this token.
            let _ = slot.retry.try_send(());
            continue;
        }
        #[cfg(not(target_os = "linux"))]
        let _ = slot;
        if *token != 0 && read_start_time(*pid) == Some(*token) {
            let _ = kill(Pid::from_raw(*pid as i32), Some(NixSignal::SIGKILL));
        }
        let _ = slot.retry.try_send(());
    }
}

#[cfg(test)]
mod tests {
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
        assert!(matches!(&*sender.borrow(), Some(Err(_))));
        assert!(backend.state.lock().unwrap().children.contains_key(&key));
        assert_eq!(backend.state.lock().unwrap().scope_groups[&scope].live, 1);
        // The real OS waiter remains alive and replaces the injected failure only
        // after it has actually reaped this process.
        let mut recovered = sender.subscribe();
        // Model a containment entry already retired independently of root reap.
        #[cfg(target_os = "linux")]
        backend.forget_group(scope);
        backend.hard_kill_all();
        #[cfg(not(target_os = "linux"))]
        backend.signal(&root, Signal::Kill).await.unwrap();
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
        #[cfg(not(target_os = "linux"))]
        assert!(backend.cleanup_scope(scope).await.is_err());
        #[cfg(target_os = "linux")]
        backend.cleanup_scope(scope).await.unwrap();
    }

    #[tokio::test]
    async fn failed_wait_quarantines_recycled_group() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let other = ProcessScopeId::new(2);
        let spec = ProcessSpec::new("/bin/sleep").arg("30");
        let root = backend.spawn(scope, &spec).await.unwrap();
        let sibling = backend.spawn(other, &spec).await.unwrap();
        let sender = backend.state.lock().unwrap().children[&child_key(&root.os)]
            .sender
            .clone();
        backend.publish_wait_result(scope, &sender, Err("injected wait failure".into()));
        {
            let mut state = backend.state.lock().unwrap();
            state.scope_groups.get_mut(&scope).unwrap().pgid = sibling.os.pid as i32;
        }
        assert!(
            matches!(backend.spawn(scope, &spec).await, Err(SpawnError::ScopeClosed(id)) if id == scope)
        );
        assert!(backend.signal_scope(scope, Signal::Kill).await.is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), backend.wait(&sibling))
                .await
                .is_err()
        );
        // Remove the sibling group so the global sweep cannot legitimately signal it.
        // Its registered root is intentionally removed only within this test model.
        let sibling_slot = backend
            .state
            .lock()
            .unwrap()
            .children
            .remove(&child_key(&sibling.os))
            .unwrap();
        backend.forget_group(other);
        backend.hard_kill_all();
        let mut sibling_exit = sibling_slot.sender.subscribe();
        let sibling_alive = tokio::time::timeout(Duration::from_millis(30), async {
            loop {
                if sibling_exit.borrow_and_update().is_some() {
                    break;
                }
                sibling_exit.changed().await.unwrap();
            }
        })
        .await
        .is_err();
        backend
            .state
            .lock()
            .unwrap()
            .children
            .insert(child_key(&sibling.os), sibling_slot);
        backend.signal(&root, Signal::Kill).await.unwrap();
        backend.signal(&sibling, Signal::Kill).await.unwrap();
        let mut recovered = sender.subscribe();
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
        backend.wait(&root).await.unwrap();
        backend.wait(&sibling).await.unwrap();
        assert!(sibling_alive, "quarantined pgid killed another scope");
    }

    #[tokio::test]
    async fn actual_wait_failure_retries_and_reaps_child() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let root = backend
            .spawn(scope, &ProcessSpec::new("/bin/sleep").arg("30"))
            .await
            .unwrap();
        backend.state.lock().unwrap().children[&child_key(&root.os)]
            .failures
            .store(1, std::sync::atomic::Ordering::SeqCst);
        // The first OS wait operation really returns an error before touching Child.
        assert!(backend.wait(&root).await.is_err());
        backend.signal(&root, Signal::Kill).await.unwrap();
        let exit = tokio::time::timeout(Duration::from_secs(5), backend.wait(&root))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exit.signal, Some(Signal::Kill));
        assert!(!backend
            .state
            .lock()
            .unwrap()
            .children
            .contains_key(&child_key(&root.os)));
    }

    #[tokio::test]
    async fn hard_kill_retries_reap_with_backend_retained() {
        let backend = UnixProcessBackend::new();
        let root = backend
            .spawn(
                ProcessScopeId::new(1),
                &ProcessSpec::new("/bin/sleep").arg("30"),
            )
            .await
            .unwrap();
        let mut observation = {
            let state = backend.state.lock().unwrap();
            let slot = &state.children[&child_key(&root.os)];
            slot.failures.store(1, std::sync::atomic::Ordering::SeqCst);
            slot.sender.subscribe()
        };
        assert!(backend.wait(&root).await.is_err());
        backend.hard_kill_all();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(&*observation.borrow_and_update(), Some(Ok(_))) {
                    break;
                }
                observation.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            backend.wait(&root).await.unwrap().signal,
            Some(Signal::Kill)
        );
    }

    #[tokio::test]
    async fn hard_kill_before_first_wait_error_still_requests_reap() {
        let backend = UnixProcessBackend::new();
        let root = backend
            .spawn(
                ProcessScopeId::new(1),
                &ProcessSpec::new("/bin/sleep").arg("30"),
            )
            .await
            .unwrap();
        let mut observation = {
            let state = backend.state.lock().unwrap();
            let slot = &state.children[&child_key(&root.os)];
            slot.failures.store(1, std::sync::atomic::Ordering::SeqCst);
            slot.sender.subscribe()
        };
        // No yield: the wait task has not published its injected error yet.
        backend.hard_kill_all();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(&*observation.borrow_and_update(), Some(Ok(_))) {
                    break;
                }
                observation.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        backend.wait(&root).await.unwrap();
    }

    #[tokio::test]
    async fn failed_waiter_does_not_keep_backend_alive_or_spin() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let root = backend
            .spawn(scope, &ProcessSpec::new("/bin/sleep").arg("30"))
            .await
            .unwrap();
        let (failures, sender) = {
            let state = backend.state.lock().unwrap();
            let slot = &state.children[&child_key(&root.os)];
            (slot.failures.clone(), slot.sender.clone())
        };
        failures.store(10, std::sync::atomic::Ordering::SeqCst);
        assert!(backend.wait(&root).await.is_err());
        for _ in 0..3 {
            assert!(backend.wait(&root).await.is_err());
        }
        tokio::task::yield_now().await;
        assert_eq!(failures.load(std::sync::atomic::Ordering::SeqCst), 6);
        let weak = Arc::downgrade(&backend.state);
        backend.hard_kill_all();
        drop(backend);
        assert!(weak.upgrade().is_none());
        // Only the task's injection counter reference can remain now. Closing the
        // slot's retry sender wakes it and releases Child without another wait loop.
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&failures) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(sender);
    }

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

#[cfg(all(test, target_os = "linux"))]
mod cgroup_backstop_tests {
    use super::*;
    use std::fs::File;
    use std::time::Duration;

    async fn failed_kill_file_preserves_root_backstop() {
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
        sender.send_replace(Some(Err("injected wait failure".into())));
        assert!(matches!(&*sender.borrow(), Some(Err(_))));
        assert!(backend.state.lock().unwrap().children[&child_key(&root.os)]
            .pidfd
            .is_some());
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
        .expect("failed wait discarded the root kill identity");
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
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), backend.wait(&survivor))
                .await
                .unwrap()
                .unwrap()
                .signal,
            Some(Signal::Kill)
        );
        backend.cleanup_scope(other).await.unwrap();
        drop(cgroups.replace_kill_file(scope, saved));
        backend.cleanup_scope(scope).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires delegated cgroup v2; privileged CI runs this fail-closed"]
    async fn all_backstop_kills_roots_when_one_cgroup_write_fails() {
        failed_kill_file_preserves_root_backstop().await;
    }
}

#[cfg(test)]
mod signal_identity_tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn unavailable_identity_cannot_signal_unrelated_live_process() {
        let backend = UnixProcessBackend::new();
        let mut unrelated = std::process::Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let target = Spawned {
            os: OsIdentity::new(unrelated.id(), ReuseToken::Unavailable),
        };
        let result = backend.signal(&target, Signal::Kill).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let survived = unrelated.try_wait().unwrap().is_none();
        let _ = unrelated.kill();
        let _ = unrelated.wait();
        assert!(result.is_err(), "unverified raw PID signal was accepted");
        assert!(survived, "unverified identity killed unrelated process");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_start_identity_allows_owned_root_termination() {
        let backend = UnixProcessBackend::new();
        let scope = ProcessScopeId::new(1);
        let root = backend
            .spawn(scope, &ProcessSpec::new("/bin/sleep").arg("30"))
            .await
            .unwrap();
        let has_identity = matches!(root.os.reuse_token, ReuseToken::StartTime(_));
        let signalled = backend.signal(&root, Signal::Kill).await;
        backend.hard_kill_all();
        let exit = tokio::time::timeout(Duration::from_secs(2), backend.wait(&root))
            .await
            .unwrap()
            .unwrap();
        backend.cleanup_scope(scope).await.unwrap();
        assert!(has_identity);
        assert!(signalled.is_ok());
        assert_eq!(exit.signal, Some(Signal::Kill));
    }
}
