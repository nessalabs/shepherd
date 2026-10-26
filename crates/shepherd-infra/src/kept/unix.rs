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
/// How often liveness is polled where the OS offers no exit notification.
const POLL: Duration = Duration::from_millis(250);

/// One gate at a time: a gated child inherits every descriptor open at its fork until it
/// execs, so two overlapping gates could each keep the other's socket alive.
static GATE: Mutex<()> = Mutex::new(());

/// Kept processes this process must reap, by pid. An entry lives until the reap.
static CHILDREN: OnceLock<Mutex<HashMap<u32, Arc<Watched>>>> = OnceLock::new();

/// The Unix [`KeptLauncher`].
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
        let launch =
            tokio::task::spawn_blocking(move || gated_spawn(&spec, log.as_deref(), commit))
                .await
                .map_err(|e| KeptError::Launch(e.to_string()))??;
        #[cfg(target_os = "linux")]
        let pidfd = native::open_pidfd(launch.os_pid);
        let watched = Arc::new(Watched::new(
            launch.os_pid,
            launch.start_token,
            Owner::Child,
            read_exe(launch.os_pid),
            #[cfg(target_os = "linux")]
            pidfd,
        ));
        children().insert(launch.os_pid, watched.clone());
        watched.watch();
        Ok((launch, Arc::new(UnixKeptTarget { watched })))
    }

    async fn attach(&self, record: &KeptRecord) -> Result<Option<Arc<dyn KeptTarget>>, KeptError> {
        if let Some(known) = children().get(&record.os_pid).cloned() {
            if known.start_token == record.start_token && !known.has_exited() {
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
            let mut registry = children();
            if let Some(known) = registry.get(&record.os_pid).cloned() {
                return Ok(Some(Arc::new(UnixKeptTarget { watched: known })));
            }
            registry.insert(record.os_pid, watched.clone());
            drop(registry);
            watched.watch();
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

fn launch_error(context: &str, error: impl std::fmt::Display) -> KeptError {
    KeptError::Launch(format!("{context}: {error}"))
}

/// Forks the child, holds it before `exec`, commits, then lets it run. Blocking.
fn gated_spawn(
    spec: &ProcessSpec,
    log: Option<&Path>,
    commit: KeptCommit,
) -> Result<KeptLaunch, KeptError> {
    let _serial = GATE.lock().unwrap_or_else(|p| p.into_inner());
    let (mut owner_end, child_end) =
        UnixStream::pair().map_err(|e| launch_error("gate socket", e))?;
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

    let mut pid = [0u8; 4];
    if let Err(error) = owner_end.read_exact(&mut pid) {
        drop(owner_end);
        return Err(match join(spawner) {
            Err(spawn) => launch_error("spawn", spawn),
            Ok(_) => launch_error("child did not report", error),
        });
    }
    let os_pid = i32::from_ne_bytes(pid) as u32;
    let committed = native::read_start_time(os_pid)
        .ok_or_else(|| KeptError::Launch("child start time unreadable".into()))
        .and_then(|start_token| {
            let launch = KeptLaunch {
                os_pid,
                start_token,
            };
            commit(launch).map(|()| launch)
        });
    let launch = match committed {
        Ok(launch) => launch,
        Err(error) => {
            // EOF on the gate: the child exits before exec and std reaps it.
            drop(owner_end);
            let _ = join(spawner);
            return Err(error);
        }
    };
    let opened = nix::sys::socket::send(owner_fd, &[GO], no_sigpipe());
    let spawned = join(spawner);
    drop(owner_end);
    if let Err(error) = opened {
        return Err(launch_error("open gate", error));
    }
    match spawned {
        // Not waited on through `Child`: the watcher reaps by pid after sweeping.
        Ok(_child) => Ok(launch),
        Err(error) => Err(launch_error("exec", error)),
    }
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
    exe: Option<PathBuf>,
    #[cfg(target_os = "linux")]
    pidfd: Option<std::os::fd::OwnedFd>,
    /// Serializes group signals with the exit sweep: true once exited.
    exited: Mutex<bool>,
    exit: watch::Sender<Option<KeptExit>>,
    started: std::sync::atomic::AtomicBool,
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
            exe,
            #[cfg(target_os = "linux")]
            pidfd,
            exited: Mutex::new(false),
            exit: watch::channel(None).0,
            started: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn has_exited(&self) -> bool {
        self.exit.borrow().is_some()
    }

    fn pid(&self) -> Pid {
        Pid::from_raw(self.os_pid as i32)
    }

    /// Starts the exit watcher once. Children are watched from launch or attach (they
    /// must be reaped); foreign processes only when someone waits for them.
    fn watch(self: &Arc<Self>) {
        if self.started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let watched = self.clone();
        tokio::spawn(async move {
            watched.wait_for_exit().await;
            watched.finish();
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
                let _ = fd.readable().await;
                return;
            }
        }
        #[cfg(target_vendor = "apple")]
        if let Some(queue) = ExitQueue::register(self.os_pid) {
            if queue.exited().await {
                return;
            }
        }
        // No notification available (for example a kernel without pidfds): poll.
        while self.alive() {
            tokio::time::sleep(POLL).await;
        }
    }

    /// Whether the instance is still running (same start token, not a zombie). Never
    /// reaps, so a child's group stays pinned.
    fn alive(&self) -> bool {
        native::read_start_time(self.os_pid) == Some(self.start_token) && !is_zombie(self.os_pid)
    }

    /// Sweeps the group while it is still pinned, then reaps and publishes.
    fn finish(&self) {
        let mut exited = self.exited.lock().unwrap_or_else(|p| p.into_inner());
        let _ = killpg(self.pid(), NixSignal::SIGKILL);
        let exit = match self.owner {
            Owner::Child => reap(self.pid()),
            Owner::Foreign => KeptExit::default(),
        };
        *exited = true;
        self.exit.send_replace(Some(exit));
        drop(exited);
        if self.owner == Owner::Child {
            let mut registry = children();
            if registry
                .get(&self.os_pid)
                .is_some_and(|known| std::ptr::eq(Arc::as_ptr(known), self))
            {
                registry.remove(&self.os_pid);
            }
        }
    }
}

fn reap(pid: Pid) -> KeptExit {
    loop {
        match waitpid(pid, None) {
            Ok(WaitStatus::Exited(_, code)) => {
                return KeptExit {
                    code: Some(code),
                    signal: None,
                }
            }
            Ok(WaitStatus::Signaled(_, signal, _)) => {
                return KeptExit {
                    code: None,
                    signal: Some(native::signal_from_raw(signal as i32)),
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
        self.watched.exe.clone()
    }

    async fn signal(&self, signal: Signal) -> Result<(), KeptError> {
        let signal = native::to_nix(signal).map_err(|e| KeptError::Signal(e.to_string()))?;
        let watched = &self.watched;
        let exited = watched.exited.lock().unwrap_or_else(|p| p.into_inner());
        if *exited {
            return Ok(());
        }
        // A child's group is pinned until `finish` reaps it under this lock. A foreign
        // leader is re-identified first; on Linux its pidfd signals exactly that instance.
        let sent = |result: nix::Result<()>| match result {
            Ok(()) | Err(Errno::ESRCH) => Ok(()),
            Err(error) => Err(KeptError::Signal(error.to_string())),
        };
        if watched.owner == Owner::Foreign {
            #[cfg(target_os = "linux")]
            if let Some(pidfd) = &watched.pidfd {
                native::pidfd_kill(pidfd.as_raw_fd(), signal)
                    .map_err(|e| KeptError::Signal(e.to_string()))?;
                return if watched.alive() {
                    sent(killpg(watched.pid(), signal))
                } else {
                    Ok(())
                };
            }
            if !watched.alive() {
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
