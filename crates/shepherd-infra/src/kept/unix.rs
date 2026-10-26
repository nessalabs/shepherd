//! Unix kept-process launcher (ADR 0023).
//!
//! **Launch.** The child calls `setsid` (so it leads its own session and process group
//! and outlives the owner's terminal and group), reports its pid over a private socket
//! and then blocks before `exec` until the owner sends one "go" byte. The owner sends it
//! only after the record is durable. If the owner dies first, the socket reaches EOF
//! and the child exits without running the program: no kept program ever runs without
//! a record. Launches are serialized process-wide so no gated child can hold another
//! gate's socket open.
//!
//! **Exit.** A kept process this process started (or inherited across `execve`) is
//! reaped here. Its exit is observed without reaping first (pidfd on Linux, kqueue on
//! macOS), so the process group is still pinned when it is swept with `SIGKILL`. A
//! kept process whose owner died belongs to init; its exit is observed the same way,
//! its group swept best-effort, and its status is unknown.

use std::collections::HashMap;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use nix::errno::Errno;
use nix::sys::signal::{kill, killpg, Signal as NixSignal};
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::Pid;
use shepherd_app::kept::{KeptCommit, KeptError, KeptExit, KeptLaunch, KeptLauncher, KeptTarget};
use shepherd_domain::{EnvPolicy, KeptObservation, KeptRecord, ProcessSpec, Signal};
use tokio::sync::watch;

use crate::backend::unix as native;

/// The byte that lets a gated child run its program.
const GO: u8 = b'G';
/// The byte that tells a gated child to exit without running it.
const ABORT: u8 = b'A';
/// How long the group may take to empty after the sweep.
const GROUP_SWEEP: Duration = Duration::from_secs(2);
/// How often liveness is polled where the OS offers no exit notification.
const POLL: Duration = Duration::from_millis(250);

/// One gate at a time: a gated child inherits every descriptor open at its fork until it
/// execs, so two overlapping gates could each keep the other's socket alive.
static GATE: Mutex<()> = Mutex::new(());

/// Kept processes this process must reap, by pid. An entry lives until the reap.
static CHILDREN: OnceLock<Mutex<HashMap<u32, Arc<Watched>>>> = OnceLock::new();

/// The Unix [`KeptLauncher`].
///
/// Its methods must run inside a Tokio runtime with IO and time enabled: exit watchers
/// wait on descriptors and time the group sweep.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnixKeptLauncher;

impl UnixKeptLauncher {
    /// Creates the launcher.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl KeptLauncher for UnixKeptLauncher {
    async fn launch(
        &self,
        spec: &ProcessSpec,
        log: Option<&Path>,
        commit: KeptCommit,
    ) -> Result<(KeptLaunch, Arc<dyn KeptTarget>), KeptError> {
        let spec = spec.clone();
        let log = log.map(Path::to_path_buf);
        let (launch, watched) =
            tokio::task::spawn_blocking(move || gated_spawn(&spec, log.as_deref(), commit))
                .await
                .map_err(|e| KeptError::Launch(e.to_string()))??;
        watched.launching.store(false, Ordering::SeqCst);
        watched.watch();
        Ok((launch, Arc::new(UnixKeptTarget { watched })))
    }

    async fn attach(&self, record: &KeptRecord) -> Result<Option<Arc<dyn KeptTarget>>, KeptError> {
        if let Some(known) = children().get(&record.os_pid).cloned() {
            if known.start_token == record.start_token {
                if known.ended() {
                    return Ok(None);
                }
                // Re-arm in case the runtime that watched it has gone away (a no-op
                // while its launch is still in progress).
                known.watch();
                return Ok(Some(Arc::new(UnixKeptTarget { watched: known })));
            }
        }
        if i32::try_from(record.os_pid).is_err() {
            return Ok(None);
        }
        // Pin the instance first (pidfd), then check it is the recorded one: a pid cannot
        // leave a process and come back, so a match proves the pin is that process.
        #[cfg(target_os = "linux")]
        let pidfd = native::open_pidfd(record.os_pid);
        let Some(observed) = observe(record.os_pid)? else {
            return Ok(None);
        };
        if !record.is_same_instance(&observed) {
            return Ok(None);
        }
        let ours = parent_of(record.os_pid) == Some(nix::unistd::getpid().as_raw());
        if is_zombie(record.os_pid) {
            if ours {
                // Inherited across execve and already exited: reap it rather than leave
                // a zombie. The pid cannot be reused before this reap.
                let _ = killpg(Pid::from_raw(record.os_pid as i32), NixSignal::SIGKILL);
                let _ = reap(Pid::from_raw(record.os_pid as i32));
            }
            return Ok(None);
        }
        let owner = if ours { Owner::Child } else { Owner::Foreign };
        let watched = Arc::new(Watched::new(
            record.os_pid,
            record.start_token,
            owner,
            read_exe(record.os_pid),
            #[cfg(target_os = "linux")]
            pidfd,
        ));
        if owner == Owner::Child {
            let watched = children().entry(record.os_pid).or_insert(watched).clone();
            watched.watch();
            return Ok(Some(Arc::new(UnixKeptTarget { watched })));
        }
        Ok(Some(Arc::new(UnixKeptTarget { watched })))
    }

    fn boot_id(&self) -> Option<String> {
        boot_id()
    }
}

fn children() -> MutexGuard<'static, HashMap<u32, Arc<Watched>>> {
    CHILDREN
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

fn forget_child(watched: &Watched) {
    let mut registry = children();
    if registry
        .get(&watched.os_pid)
        .is_some_and(|known| std::ptr::eq(Arc::as_ptr(known), watched))
    {
        registry.remove(&watched.os_pid);
    }
}

fn launch_error(context: &str, error: impl std::fmt::Display) -> KeptError {
    KeptError::Launch(format!("{context}: {error}"))
}

/// Forks the child, holds it before `exec`, commits, then lets it run. Blocking.
///
/// The child is registered for reaping before the record exists, so anyone who finds
/// the record (a concurrent `list`) also finds this one registration.
fn gated_spawn(
    spec: &ProcessSpec,
    log: Option<&Path>,
    commit: KeptCommit,
) -> Result<(KeptLaunch, Arc<Watched>), KeptError> {
    let _serial = GATE.lock().unwrap_or_else(|p| p.into_inner());
    // macOS sets close-on-exec after creating the pair; no Shepherd fork may run in
    // between, or that child would hold the gate open for its whole life.
    let (mut owner_end, child_end) =
        native::with_spawn_lock(UnixStream::pair).map_err(|e| launch_error("gate socket", e))?;
    let owner_fd = owner_end.as_raw_fd();
    let child_fd = child_end.as_raw_fd();

    let mut command = std::process::Command::new(&spec.program);
    command.args(&spec.args).stdin(Stdio::null());
    match log {
        Some(path) => {
            let file = open_log(path)?;
            let copy = file.try_clone().map_err(|e| launch_error("log file", e))?;
            command.stdout(file).stderr(copy);
        }
        None => {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }
    }
    if let Some(cwd) = &spec.cwd {
        command.current_dir(cwd);
    }
    apply_env(&mut command, &spec.env);
    // SAFETY: after fork the closure uses only the two copied descriptor numbers, a stack
    // buffer and async-signal-safe setsid/close/getpid/write/read; it allocates nothing
    // and takes no lock. Closing the owner's end in the child makes the owner's death
    // visible as EOF; the child's end is close-on-exec.
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            libc::close(owner_fd);
            let pid = libc::getpid().to_ne_bytes();
            if libc::write(child_fd, pid.as_ptr().cast(), pid.len()) != pid.len() as isize {
                return Err(std::io::Error::last_os_error());
            }
            let mut go = 0u8;
            loop {
                let read = libc::read(child_fd, (&mut go as *mut u8).cast(), 1);
                if read == 1 && go == GO {
                    return Ok(());
                }
                if read < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(std::io::Error::from_raw_os_error(libc::ECANCELED));
            }
        });
    }
    // `spawn` returns only once the child has exec'd or failed, so it runs on its own
    // thread while this one reads the pid, commits and opens the gate.
    let spawner = std::thread::Builder::new()
        .name("shepherd-kept-spawn".into())
        .spawn(move || {
            let spawned = native::spawn_native(&mut command);
            drop(child_end);
            spawned
        })
        .map_err(|e| launch_error("spawn thread", e))?;
    let join = |spawner: std::thread::JoinHandle<std::io::Result<std::process::Child>>| {
        spawner
            .join()
            .unwrap_or_else(|_| Err(std::io::Error::other("spawn thread panicked")))
    };
    // Refuse explicitly as well as by EOF: a descriptor copy leaked into a fork outside
    // Shepherd would otherwise keep the child waiting.
    let abort = |owner_end: UnixStream| {
        let _ = nix::sys::socket::send(owner_end.as_raw_fd(), &[ABORT], no_sigpipe());
        drop(owner_end);
    };

    let mut pid = [0u8; 4];
    if let Err(error) = owner_end.read_exact(&mut pid) {
        abort(owner_end);
        return Err(match join(spawner) {
            Err(spawn) => launch_error("spawn", spawn),
            Ok(_) => launch_error("child did not report", error),
        });
    }
    let os_pid = i32::from_ne_bytes(pid) as u32;
    let Some(start_token) = native::read_start_time(os_pid) else {
        abort(owner_end);
        let _ = join(spawner);
        return Err(KeptError::Launch("child start time unreadable".into()));
    };
    let launch = KeptLaunch {
        os_pid,
        start_token,
    };
    // The child is ours and unreaped, so its pid is pinned while we register it.
    let watched = Arc::new(Watched::new(
        os_pid,
        start_token,
        Owner::Child,
        None,
        #[cfg(target_os = "linux")]
        native::open_pidfd(os_pid),
    ));
    // No watcher may start (and reap) until the launch has succeeded: std reaps a
    // child that fails before exec.
    watched.launching.store(true, Ordering::SeqCst);
    children().insert(os_pid, watched.clone());
    if let Err(error) = commit(launch) {
        abort(owner_end);
        let _ = join(spawner);
        watched.abandon();
        return Err(error);
    }
    let opened = nix::sys::socket::send(owner_fd, &[GO], no_sigpipe());
    let spawned = join(spawner);
    drop(owner_end);
    let failure = match (opened, spawned) {
        // Not waited on through `Child`: the watcher reaps by pid after sweeping.
        (Ok(_), Ok(_child)) => None,
        (Err(error), _) => Some(launch_error("open gate", error)),
        (_, Err(error)) => Some(launch_error("exec", error)),
    };
    if let Some(error) = failure {
        watched.abandon();
        return Err(error);
    }
    *watched.exe.lock().unwrap_or_else(|p| p.into_inner()) = read_exe(os_pid);
    Ok((launch, watched))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn no_sigpipe() -> nix::sys::socket::MsgFlags {
    nix::sys::socket::MsgFlags::MSG_NOSIGNAL
}
#[cfg(target_vendor = "apple")]
fn no_sigpipe() -> nix::sys::socket::MsgFlags {
    nix::sys::socket::MsgFlags::from_bits_retain(libc::MSG_NOSIGNAL)
}
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn no_sigpipe() -> nix::sys::socket::MsgFlags {
    nix::sys::socket::MsgFlags::empty()
}

fn open_log(path: &Path) -> Result<std::fs::File, KeptError> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| launch_error("log file", e))
}

fn apply_env(command: &mut std::process::Command, env: &EnvPolicy) {
    match env {
        EnvPolicy::Inherit => {}
        EnvPolicy::Overrides(entries) => {
            for (key, value) in entries {
                match value {
                    Some(value) => command.env(key, value),
                    None => command.env_remove(key),
                };
            }
        }
        EnvPolicy::Clear(entries) => {
            command.env_clear();
            command.envs(entries.iter().map(|(k, v)| (k, v)));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owner {
    /// This process is its parent and must reap it.
    Child,
    /// Its parent is someone else (usually init after the owner died).
    Foreign,
}

/// Shared exit state for one kept process instance.
struct Watched {
    os_pid: u32,
    start_token: u64,
    owner: Owner,
    exe: Mutex<Option<PathBuf>>,
    #[cfg(target_os = "linux")]
    pidfd: Option<std::os::fd::OwnedFd>,
    /// Serializes group signals with the reap. Set when the leader is reaped (or known
    /// gone), before the sweep finishes, so nothing treats it as live meanwhile.
    ended: Mutex<Option<KeptExit>>,
    /// The published exit, once the sweep has finished.
    exit: watch::Sender<Option<KeptExit>>,
    /// Whether a watcher task is running (reset if its runtime drops it).
    started: AtomicBool,
    /// Set while `gated_spawn` has not yet finished: no watcher may start then.
    launching: AtomicBool,
}

/// Lets a later `watch` start a new watcher if this one is dropped before finishing.
struct Rearm(Arc<Watched>);

impl Drop for Rearm {
    fn drop(&mut self) {
        if !self.0.has_exited() {
            self.0.started.store(false, Ordering::SeqCst);
        }
    }
}

impl Watched {
    fn new(
        os_pid: u32,
        start_token: u64,
        owner: Owner,
        exe: Option<PathBuf>,
        #[cfg(target_os = "linux")] pidfd: Option<std::os::fd::OwnedFd>,
    ) -> Self {
        Self {
            os_pid,
            start_token,
            owner,
            exe: Mutex::new(exe),
            #[cfg(target_os = "linux")]
            pidfd,
            ended: Mutex::new(None),
            exit: watch::channel(None).0,
            started: AtomicBool::new(false),
            launching: AtomicBool::new(false),
        }
    }

    /// Whether the exit has been published.
    fn has_exited(&self) -> bool {
        self.exit.borrow().is_some()
    }

    /// Whether the leader has been reaped or is known gone (the sweep may still run).
    fn ended(&self) -> bool {
        self.ended
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    }

    /// Ends a registration whose launch failed: std has reaped (or will reap) the
    /// child, so it is forgotten and anyone already waiting is woken.
    fn abandon(&self) {
        *self.ended.lock().unwrap_or_else(|p| p.into_inner()) = Some(KeptExit::default());
        self.publish(KeptExit::default());
    }

    fn publish(&self, exit: KeptExit) {
        self.exit.send_replace(Some(exit));
        if self.owner == Owner::Child {
            forget_child(self);
        }
    }

    fn pid(&self) -> Pid {
        Pid::from_raw(self.os_pid as i32)
    }

    /// Starts the exit watcher unless one is running. Children are watched from launch
    /// or attach (they must be reaped); foreign processes when someone waits for them.
    fn watch(self: &Arc<Self>) {
        if self.launching.load(Ordering::SeqCst)
            || self.has_exited()
            || self.started.swap(true, Ordering::SeqCst)
        {
            return;
        }
        let watched = self.clone();
        tokio::spawn(async move {
            let rearm = Rearm(watched.clone());
            watched.wait_for_exit().await;
            watched.finish().await;
            drop(rearm);
        });
    }

    async fn wait_for_exit(&self) {
        #[cfg(target_os = "linux")]
        if let Some(pidfd) = &self.pidfd {
            if let Ok(fd) = tokio::io::unix::AsyncFd::with_interest(
                pidfd.as_raw_fd(),
                tokio::io::Interest::READABLE,
            ) {
                // A pidfd turns readable when the process exits, before it is reaped.
                if fd.readable().await.is_ok() {
                    return;
                }
            }
        }
        #[cfg(target_vendor = "apple")]
        if let Some(queue) = ExitQueue::register(self.os_pid) {
            // The registration is by number: if that number is now someone else, the
            // recorded instance is already gone.
            if self.gone() || queue.exited().await {
                return;
            }
        }
        // No notification available (for example a kernel without pidfds): poll.
        while !self.gone() {
            tokio::time::sleep(POLL).await;
        }
    }

    /// Positively the recorded instance, running. Required before any numeric signal.
    fn confirmed_alive(&self) -> bool {
        native::read_start_time(self.os_pid) == Some(self.start_token) && !is_zombie(self.os_pid)
    }

    /// Positively gone: no process at the pid, another instance there, or a zombie.
    /// An unreadable identity is not gone. Never reaps, so a child's group stays pinned.
    fn gone(&self) -> bool {
        match native::read_start_time(self.os_pid) {
            Some(token) if token != self.start_token => true,
            Some(_) => is_zombie(self.os_pid),
            None => kill(self.pid(), None) == Err(Errno::ESRCH) || is_zombie(self.os_pid),
        }
    }

    /// Sweeps the group while it is still pinned, reaps a child, waits for the group to
    /// empty and publishes. Reports stragglers unless the group is seen empty.
    async fn finish(&self) {
        let (mut exit, pinned) = {
            let mut ended = self.ended.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(exit) = *ended {
                // An earlier watcher reaped it and was dropped mid-sweep; the leader no
                // longer pins the group, so the sweep cannot safely be redone.
                drop(ended);
                self.publish(KeptExit {
                    stragglers: true,
                    ..exit
                });
                return;
            }
            // A child's exited leader pins its group until reaped. A foreign leader pins
            // it only while it is still a zombie of the same instance.
            let pinned = self.owner == Owner::Child
                || (native::read_start_time(self.os_pid) == Some(self.start_token)
                    && is_zombie(self.os_pid));
            if pinned {
                let _ = killpg(self.pid(), NixSignal::SIGKILL);
            }
            let exit = match self.owner {
                Owner::Child => reap(self.pid()),
                Owner::Foreign => KeptExit::default(),
            };
            *ended = Some(exit);
            (exit, pinned)
        };
        // Members still alive keep the group number in use, so it cannot be reused: a
        // group that still has a running member is the recorded one even after init
        // reaped its leader. A group that cannot be listed is never reported empty.
        exit.stragglers = if pinned {
            !group_empties(self.pid()).await
        } else {
            match group_members(self.os_pid as i32) {
                None => true,
                Some(members) if members.iter().all(|&pid| is_zombie(pid as u32)) => false,
                Some(_) => !group_empties(self.pid()).await,
            }
        };
        self.publish(exit);
    }
}

/// Re-sends SIGKILL to `pgid` until no member is running, for at most `GROUP_SWEEP`.
/// Zombies are dead already; they wait for their own parent (often init) to reap them.
async fn group_empties(pgid: Pid) -> bool {
    let deadline = tokio::time::Instant::now() + GROUP_SWEEP;
    loop {
        if killpg(pgid, NixSignal::SIGKILL) == Err(Errno::ESRCH) {
            return true;
        }
        if group_members(pgid.as_raw())
            .is_some_and(|members| members.iter().all(|&pid| is_zombie(pid as u32)))
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn reap(pid: Pid) -> KeptExit {
    loop {
        match waitpid(pid, None) {
            Ok(WaitStatus::Exited(_, code)) => {
                return KeptExit {
                    code: Some(code),
                    ..KeptExit::default()
                }
            }
            Ok(WaitStatus::Signaled(_, signal, _)) => {
                return KeptExit {
                    signal: Some(native::signal_from_raw(signal as i32)),
                    ..KeptExit::default()
                }
            }
            Ok(_) | Err(Errno::EINTR) => {}
            Err(_) => return KeptExit::default(),
        }
    }
}

struct UnixKeptTarget {
    watched: Arc<Watched>,
}

#[async_trait]
impl KeptTarget for UnixKeptTarget {
    fn os_pid(&self) -> u32 {
        self.watched.os_pid
    }

    fn exe(&self) -> Option<PathBuf> {
        self.watched
            .exe
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    async fn signal(&self, signal: Signal) -> Result<(), KeptError> {
        let signal = native::to_nix(signal).map_err(|e| KeptError::Signal(e.to_string()))?;
        let watched = &self.watched;
        let ended = watched.ended.lock().unwrap_or_else(|p| p.into_inner());
        if ended.is_some() {
            return Ok(());
        }
        let sent = |result: nix::Result<()>| match result {
            Ok(()) | Err(Errno::ESRCH) => Ok(()),
            Err(error) => Err(KeptError::Signal(error.to_string())),
        };
        // A child's group is pinned until `finish` reaps it under this lock. A foreign
        // leader must be positively re-identified before any numeric signal; on Linux
        // its pidfd signals exactly that instance.
        if watched.owner == Owner::Foreign {
            #[cfg(target_os = "linux")]
            if let Some(pidfd) = &watched.pidfd {
                native::pidfd_kill(pidfd.as_raw_fd(), signal)
                    .map_err(|e| KeptError::Signal(e.to_string()))?;
                return if watched.confirmed_alive() {
                    sent(killpg(watched.pid(), signal))
                } else {
                    Ok(())
                };
            }
            if !watched.confirmed_alive() {
                return Ok(());
            }
        }
        sent(killpg(watched.pid(), signal))?;
        // The leader may have left its group; it is pinned (child) or just re-identified.
        sent(kill(watched.pid(), signal))
    }

    async fn exited(&self) -> KeptExit {
        self.watched.watch();
        let mut exit = self.watched.exit.subscribe();
        let seen = exit.wait_for(Option::is_some).await.map(|exit| *exit);
        match seen {
            Ok(exit) => exit.unwrap_or_default(),
            // The sender lives in `Watched`, which this target holds: unreachable.
            Err(_) => std::future::pending().await,
        }
    }
}

/// What the OS reports now about `pid`, or `None` if no process is there.
///
/// An identity that exists but cannot be read is an error, never "gone": treating it
/// as gone would drop the record of a live process.
fn observe(pid: u32) -> Result<Option<KeptObservation>, KeptError> {
    if let Some(start_token) = native::read_start_time(pid) {
        return Ok(Some(KeptObservation {
            start_token,
            boot_id: boot_id(),
        }));
    }
    match kill(Pid::from_raw(pid as i32), None) {
        Err(Errno::ESRCH) => Ok(None),
        // A zombie has exited; it no longer needs a record.
        _ if is_zombie(pid) => Ok(None),
        _ => Err(KeptError::Signal(format!(
            "pid {pid} exists but its start time is unreadable"
        ))),
    }
}

#[cfg(target_os = "linux")]
fn boot_id() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|id| id.trim().to_string())
}
#[cfg(not(target_os = "linux"))]
fn boot_id() -> Option<String> {
    // macOS start tokens are wall-clock microseconds, already unique across boots.
    None
}

#[cfg(target_os = "linux")]
fn read_exe(pid: u32) -> Option<PathBuf> {
    let path = std::fs::read_link(format!("/proc/{pid}/exe")).ok()?;
    // A replaced binary still identifies the instance; drop the kernel's marker.
    let text = path.to_str()?;
    Some(PathBuf::from(
        text.strip_suffix(" (deleted)").unwrap_or(text),
    ))
}
#[cfg(target_vendor = "apple")]
fn read_exe(pid: u32) -> Option<PathBuf> {
    libproc::proc_pid::pidpath(pid as i32)
        .ok()
        .map(PathBuf::from)
}
#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn read_exe(_pid: u32) -> Option<PathBuf> {
    None
}

#[cfg(target_os = "linux")]
fn stat_field(pid: u32, index: usize) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the ")" that closes comm, starting with field 3 (state).
    stat.rsplit_once(')')?
        .1
        .split_whitespace()
        .nth(index)
        .map(str::to_string)
}

/// The pids in process group `pgid`, or `None` if they cannot be listed.
#[cfg(target_os = "linux")]
fn group_members(pgid: i32) -> Option<Vec<i32>> {
    let mut members = Vec::new();
    for entry in std::fs::read_dir("/proc").ok()?.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        // Field 5 (pgrp) is the third after comm; a process that just exited is skipped.
        if stat_field(pid, 2).and_then(|g| g.parse::<i32>().ok()) == Some(pgid) {
            members.push(pid as i32);
        }
    }
    Some(members)
}
#[cfg(target_vendor = "apple")]
fn group_members(pgid: i32) -> Option<Vec<i32>> {
    crate::usage::group_members(pgid).ok()
}

#[cfg(target_os = "linux")]
fn is_zombie(pid: u32) -> bool {
    matches!(stat_field(pid, 0).as_deref(), Some("Z" | "X"))
}
#[cfg(target_os = "linux")]
fn parent_of(pid: u32) -> Option<i32> {
    stat_field(pid, 1)?.parse().ok()
}

#[cfg(target_vendor = "apple")]
fn bsd_info(pid: u32) -> Option<libproc::bsd_info::BSDInfo> {
    libproc::proc_pid::pidinfo::<libproc::bsd_info::BSDInfo>(pid as i32, 0).ok()
}
#[cfg(target_vendor = "apple")]
fn is_zombie(pid: u32) -> bool {
    // SZOMB from <sys/proc.h>.
    bsd_info(pid).is_some_and(|info| info.pbi_status == 5)
}
#[cfg(target_vendor = "apple")]
fn parent_of(pid: u32) -> Option<i32> {
    bsd_info(pid).map(|info| info.pbi_ppid as i32)
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn is_zombie(_pid: u32) -> bool {
    false
}
#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn parent_of(_pid: u32) -> Option<i32> {
    None
}

/// A kqueue that reports one process's exit without reaping it.
#[cfg(target_vendor = "apple")]
struct ExitQueue {
    queue: tokio::io::unix::AsyncFd<QueueFd>,
}

#[cfg(target_vendor = "apple")]
struct QueueFd(nix::sys::event::Kqueue);

#[cfg(target_vendor = "apple")]
impl AsRawFd for QueueFd {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsFd;
        self.0.as_fd().as_raw_fd()
    }
}

#[cfg(target_vendor = "apple")]
impl ExitQueue {
    fn register(pid: u32) -> Option<Self> {
        use nix::sys::event::{EventFilter, EventFlag, FilterFlag, KEvent, Kqueue};
        let queue = Kqueue::new().ok()?;
        let change = KEvent::new(
            pid as usize,
            EventFilter::EVFILT_PROC,
            EventFlag::EV_ADD | EventFlag::EV_ONESHOT,
            FilterFlag::NOTE_EXIT,
            0,
            0,
        );
        // ESRCH here means it already exited; the liveness loop then sees that at once.
        queue.kevent(&[change], &mut [], None).ok()?;
        let queue =
            tokio::io::unix::AsyncFd::with_interest(QueueFd(queue), tokio::io::Interest::READABLE)
                .ok()?;
        Some(Self { queue })
    }

    /// Resolves true once the exit event arrives (false if the queue failed).
    async fn exited(&self) -> bool {
        use nix::sys::event::{EventFilter, EventFlag, FilterFlag, KEvent};
        loop {
            let Ok(mut ready) = self.queue.readable().await else {
                return false;
            };
            let mut events = [KEvent::new(
                0,
                EventFilter::EVFILT_PROC,
                EventFlag::empty(),
                FilterFlag::empty(),
                0,
                0,
            )];
            let zero = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            match self.queue.get_ref().0.kevent(&[], &mut events, Some(zero)) {
                Ok(0) => ready.clear_ready(),
                Ok(_) => return true,
                Err(Errno::EINTR) => {}
                Err(_) => return false,
            }
        }
    }
}
