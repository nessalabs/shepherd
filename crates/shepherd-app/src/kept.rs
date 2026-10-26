//! Kept processes: opt-in processes that outlive their owner (ADR 0023).
//!
//! A kept process is owned by a durable [`KeptRecord`] instead of a scope. The record is
//! written before the program may run, so a live kept process always has one; it is
//! removed only after the process is confirmed gone. [`KeptProcesses::list`] shows every
//! live kept process, and [`KeptProcesses::adopt`] attaches a later owner to one.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use shepherd_domain::{
    KeptName, KeptRecord, OutputMode, ProcessSpec, Signal, SpecFingerprint, TerminationOutcome,
    UnverifiedReason,
};

use crate::ports::{Clock, TerminateOptions};

/// Error from a kept-process operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeptError {
    /// This platform has no kept-process adapter.
    #[error("kept processes are not supported on this platform")]
    Unsupported,
    /// Another live owner holds this name.
    #[error("kept process {0} is held by another owner")]
    Busy(KeptName),
    /// A live process already owns this name; adopt or terminate it first.
    #[error("kept process {name} is already running as pid {os_pid}")]
    AlreadyRunning {
        /// The name in use.
        name: KeptName,
        /// The pid of the process that owns it.
        os_pid: u32,
    },
    /// Kept processes cannot pipe output to an owner that may go away.
    #[error("kept processes cannot capture output; give them a log file")]
    CaptureUnsupported,
    /// The record store failed.
    #[error("kept record store failed: {0}")]
    Store(String),
    /// The process could not be started.
    #[error("failed to launch kept process: {0}")]
    Launch(String),
    /// Signaling the process failed.
    #[error("failed to signal kept process: {0}")]
    Signal(String),
}

/// An exclusive claim on one kept name, held by exactly one live owner.
///
/// Dropping it releases the claim. The OS also releases it when the owner dies, so a
/// crashed owner never blocks adoption.
pub struct KeptLock {
    name: KeptName,
    _guard: Box<dyn Send + Sync>,
}

impl KeptLock {
    /// Wraps an adapter's guard. The claim lasts exactly as long as `guard`.
    pub fn new(name: KeptName, guard: Box<dyn Send + Sync>) -> Self {
        Self {
            name,
            _guard: guard,
        }
    }

    /// The claimed name.
    #[must_use]
    pub fn name(&self) -> &KeptName {
        &self.name
    }
}

impl std::fmt::Debug for KeptLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeptLock")
            .field("name", &self.name)
            .finish()
    }
}

/// Durable storage of kept records, one per name. Blocking: the application calls it
/// off the async workers. Writes must be atomic and flushed before they return.
pub trait KeptStore: Send + Sync {
    /// Claims `name`, or returns `None` if another live owner holds it.
    ///
    /// # Errors
    /// [`KeptError::Store`] when the claim cannot be attempted.
    fn lock(&self, name: &KeptName) -> Result<Option<KeptLock>, KeptError>;
    /// Reads the record for `name`, if there is one.
    ///
    /// # Errors
    /// [`KeptError::Store`] when the record exists but cannot be read.
    fn read(&self, name: &KeptName) -> Result<Option<KeptRecord>, KeptError>;
    /// Durably replaces the record for `lock`'s name.
    ///
    /// # Errors
    /// [`KeptError::Store`] when the record was not made durable.
    fn write(&self, lock: &KeptLock, record: &KeptRecord) -> Result<(), KeptError>;
    /// Removes the record for `lock`'s name. Removing a missing record succeeds.
    ///
    /// # Errors
    /// [`KeptError::Store`] when the record could not be removed.
    fn remove(&self, lock: &KeptLock) -> Result<(), KeptError>;
    /// Every name that has a record.
    ///
    /// # Errors
    /// [`KeptError::Store`] when the store cannot be listed.
    fn names(&self) -> Result<Vec<KeptName>, KeptError>;
}

/// The identity of a launched process, handed to the commit step before it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeptLaunch {
    /// The OS pid (also its session and process-group id).
    pub os_pid: u32,
    /// The OS start token.
    pub start_token: u64,
}

/// Records the launched process durably. Runs before the program may execute: if it
/// fails, the program never runs.
pub type KeptCommit = Box<dyn FnOnce(KeptLaunch) -> Result<(), KeptError> + Send>;

/// Starts kept processes and reattaches to them. Implemented per OS.
#[async_trait]
pub trait KeptLauncher: Send + Sync {
    /// Starts `spec` in its own session, appending output to `log`. The child is held
    /// before `exec` until `commit` succeeds; if `commit` fails or the owner dies first,
    /// the child exits without running the program.
    async fn launch(
        &self,
        spec: &ProcessSpec,
        log: Option<&Path>,
        commit: KeptCommit,
    ) -> Result<(KeptLaunch, Arc<dyn KeptTarget>), KeptError>;

    /// Attaches to the process `record` owns, or `None` if that instance is gone.
    async fn attach(&self, record: &KeptRecord) -> Result<Option<Arc<dyn KeptTarget>>, KeptError>;

    /// The current boot's id, where the OS names one.
    fn boot_id(&self) -> Option<String>;
}

/// A live kept process.
#[async_trait]
pub trait KeptTarget: Send + Sync {
    /// The OS pid.
    fn os_pid(&self) -> u32;
    /// The executable it runs, when readable.
    fn exe(&self) -> Option<PathBuf>;
    /// Signals its process group. A no-op once it has exited.
    async fn signal(&self, signal: Signal) -> Result<(), KeptError>;
    /// Resolves once it has exited, its group was swept and (if it is this process's
    /// child) it was reaped. Cancel-safe and repeatable.
    async fn exited(&self) -> KeptExit;
}

/// How a kept process ended. Code and signal are known only when this process reaped
/// it; a process adopted after its parent died exits without telling anyone why.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct KeptExit {
    /// Exit code, if known.
    pub code: Option<i32>,
    /// Terminating signal, if known.
    pub signal: Option<Signal>,
}

/// What to start as a kept process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptSpec {
    /// The name that owns it; one live process per name.
    pub name: KeptName,
    /// What to run. Output must be [`OutputMode::Discard`]; use `log` instead.
    pub spec: ProcessSpec,
    /// The file stdout and stderr are appended to (created with owner-only access).
    pub log: Option<PathBuf>,
    /// A caller revision folded into the fingerprint.
    pub revision: Option<String>,
}

impl KeptSpec {
    /// A kept spec with no log and no revision.
    #[must_use]
    pub fn new(name: KeptName, spec: ProcessSpec) -> Self {
        Self {
            name,
            spec,
            log: None,
            revision: None,
        }
    }

    /// Appends stdout and stderr to `path`.
    #[must_use]
    pub fn log(mut self, path: impl Into<PathBuf>) -> Self {
        self.log = Some(path.into());
        self
    }

    /// Folds `revision` into the fingerprint (for inputs the spec does not carry).
    #[must_use]
    pub fn revision(mut self, revision: impl Into<String>) -> Self {
        self.revision = Some(revision.into());
        self
    }

    /// The fingerprint a process started from this spec records.
    #[must_use]
    pub fn fingerprint(&self) -> SpecFingerprint {
        SpecFingerprint::of(&self.spec, self.revision.as_deref())
    }
}

/// One live kept process in a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptEntry {
    /// Its record.
    pub record: KeptRecord,
    /// Whether a live owner holds it right now (otherwise it can be adopted).
    pub held: bool,
}

/// The result of terminating a kept process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeptTermination {
    /// The verified outcome. The record is removed only when this is verified.
    pub outcome: TerminationOutcome,
    /// How it ended, when it ended.
    pub exit: Option<KeptExit>,
}

/// Starts, lists and adopts kept processes. Cheap to clone.
#[derive(Clone)]
pub struct KeptProcesses {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<dyn KeptStore>,
    launcher: Arc<dyn KeptLauncher>,
    clock: Arc<dyn Clock>,
    owner: String,
}

impl std::fmt::Debug for KeptProcesses {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeptProcesses")
            .field("owner", &self.inner.owner)
            .finish_non_exhaustive()
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, KeptError> + Send + 'static,
) -> Result<T, KeptError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| KeptError::Store(error.to_string()))?
}

impl KeptProcesses {
    /// Wires a store, a launcher and a clock. `owner` labels the records this owner
    /// writes (for example the application's name).
    pub fn new(
        store: Arc<dyn KeptStore>,
        launcher: Arc<dyn KeptLauncher>,
        clock: Arc<dyn Clock>,
        owner: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                launcher,
                clock,
                owner: owner.into(),
            }),
        }
    }

    /// Starts `spec` as a kept process and holds it.
    ///
    /// # Errors
    /// [`KeptError::Busy`] if another owner holds the name, [`KeptError::AlreadyRunning`]
    /// if a live process already owns it, [`KeptError::CaptureUnsupported`] for captured
    /// output, or a store or launch failure. On any error no process is left running.
    pub async fn start(&self, spec: KeptSpec) -> Result<KeptProcess, KeptError> {
        if matches!(spec.spec.output, OutputMode::Capture { .. }) {
            return Err(KeptError::CaptureUnsupported);
        }
        let lock = Arc::new(self.claim(&spec.name).await?);
        if let Some(previous) = self.read(&spec.name).await? {
            if let Some(target) = self.inner.launcher.attach(&previous).await? {
                return Err(KeptError::AlreadyRunning {
                    name: spec.name,
                    os_pid: target.os_pid(),
                });
            }
        }
        let mut record = KeptRecord {
            name: spec.name.clone(),
            os_pid: 0,
            start_token: 0,
            boot_id: self.inner.launcher.boot_id(),
            exe: None,
            program: spec.spec.program.to_string_lossy().into_owned(),
            args: spec
                .spec
                .args
                .iter()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect(),
            fingerprint: spec.fingerprint(),
            revision: spec.revision.clone(),
            graceful_signal: spec.spec.graceful_signal,
            log: spec.log.clone(),
            owner: self.inner.owner.clone(),
            started_at_unix_ms: unix_ms(),
        };
        let commit: KeptCommit = {
            let lock = lock.clone();
            let mut record = record.clone();
            let store = self.inner.store.clone();
            Box::new(move |launch: KeptLaunch| {
                record.os_pid = launch.os_pid;
                record.start_token = launch.start_token;
                store.write(&lock, &record)
            })
        };
        let (launch, target) = match self
            .inner
            .launcher
            .launch(&spec.spec, spec.log.as_deref(), commit)
            .await
        {
            Ok(launched) => launched,
            Err(error) => {
                // The program never ran; drop a record written before exec failed.
                let store = self.inner.store.clone();
                let cleanup = lock.clone();
                let _ = blocking(move || store.remove(&cleanup)).await;
                return Err(error);
            }
        };
        record.os_pid = launch.os_pid;
        record.start_token = launch.start_token;
        record.exe = target.exe();
        let process = KeptProcess {
            inner: self.inner.clone(),
            lock,
            record,
            target,
        };
        // The record is already durable; adding the executable path only sharpens it.
        if let Err(error) = process.rewrite().await {
            tracing::warn!(%error, name = %process.record.name, "kept record kept without exe");
        }
        Ok(process)
    }

    /// Holds the live kept process named `name`. Returns `None` (and drops a stale
    /// record) when its process is gone.
    ///
    /// # Errors
    /// [`KeptError::Busy`] if another live owner holds it, or a store failure.
    pub async fn adopt(&self, name: &KeptName) -> Result<Option<KeptProcess>, KeptError> {
        let lock = self.claim(name).await?;
        let Some(record) = self.read(name).await? else {
            return Ok(None);
        };
        let lock = Arc::new(lock);
        match self.inner.launcher.attach(&record).await? {
            Some(target) => Ok(Some(KeptProcess {
                inner: self.inner.clone(),
                lock,
                record,
                target,
            })),
            None => {
                let store = self.inner.store.clone();
                blocking(move || store.remove(&lock)).await?;
                Ok(None)
            }
        }
    }

    /// Every live kept process, held or not, sorted by name. Records whose process is
    /// gone are removed when no owner holds them.
    ///
    /// # Errors
    /// A store failure.
    pub async fn list(&self) -> Result<Vec<KeptEntry>, KeptError> {
        let store = self.inner.store.clone();
        let mut names = blocking(move || store.names()).await?;
        names.sort();
        let mut live = Vec::new();
        for name in names {
            let Some(record) = self.read(&name).await? else {
                continue;
            };
            let alive = self.inner.launcher.attach(&record).await?.is_some();
            let store = self.inner.store.clone();
            let claim = name.clone();
            let lock = blocking(move || store.lock(&claim)).await?;
            match (alive, lock) {
                (true, lock) => live.push(KeptEntry {
                    record,
                    held: lock.is_none(),
                }),
                (false, Some(lock)) => {
                    // Reread under the claim: an owner may have restarted it meanwhile.
                    let current = self.read(&name).await?;
                    if current.as_ref() == Some(&record) {
                        let store = self.inner.store.clone();
                        blocking(move || store.remove(&lock)).await?;
                    } else if let Some(current) = current {
                        if self.inner.launcher.attach(&current).await?.is_some() {
                            live.push(KeptEntry {
                                record: current,
                                held: false,
                            });
                        }
                    }
                }
                (false, None) => {}
            }
        }
        Ok(live)
    }

    /// Claims `name`, retrying briefly: a listing probes each claim for a moment.
    async fn claim(&self, name: &KeptName) -> Result<KeptLock, KeptError> {
        for attempt in 0..CLAIM_ATTEMPTS {
            let store = self.inner.store.clone();
            let claim = name.clone();
            if let Some(lock) = blocking(move || store.lock(&claim)).await? {
                return Ok(lock);
            }
            if attempt + 1 < CLAIM_ATTEMPTS {
                self.inner.clock.sleep(CLAIM_RETRY).await;
            }
        }
        Err(KeptError::Busy(name.clone()))
    }

    async fn read(&self, name: &KeptName) -> Result<Option<KeptRecord>, KeptError> {
        let store = self.inner.store.clone();
        let name = name.clone();
        blocking(move || store.read(&name)).await
    }
}

const CLAIM_ATTEMPTS: u32 = 10;
const CLAIM_RETRY: Duration = Duration::from_millis(10);

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// A kept process held by this owner.
///
/// Dropping it (or calling [`release`](Self::release)) leaves the process running and
/// its record in place for a later [`KeptProcesses::adopt`]. Only
/// [`terminate`](Self::terminate) or an observed exit ends it.
pub struct KeptProcess {
    inner: Arc<Inner>,
    lock: Arc<KeptLock>,
    record: KeptRecord,
    target: Arc<dyn KeptTarget>,
}

impl std::fmt::Debug for KeptProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeptProcess")
            .field("record", &self.record)
            .finish_non_exhaustive()
    }
}

impl KeptProcess {
    /// Its record.
    #[must_use]
    pub fn record(&self) -> &KeptRecord {
        &self.record
    }

    /// Its OS pid.
    #[must_use]
    pub fn os_pid(&self) -> u32 {
        self.target.os_pid()
    }

    /// Resolves once the process has exited, then removes its record. Cancel-safe.
    pub async fn exited(&self) -> KeptExit {
        let exit = self.target.exited().await;
        let _ = self.forget().await;
        exit
    }

    /// Stops the process and its group: the graceful signal, then force after the grace
    /// period. The record is removed only after the exit is confirmed.
    pub async fn terminate(self, options: TerminateOptions) -> KeptTermination {
        let clock = self.inner.clock.clone();
        let target = self.target.clone();
        let exited = target.exited();
        tokio::pin!(exited);
        if let Some(exit) = futures_util::FutureExt::now_or_never(&mut exited) {
            return self
                .finish(TerminationOutcome::ExitedNaturally, Some(exit))
                .await;
        }
        if target.signal(self.record.graceful_signal).await.is_ok() {
            tokio::select! {
                exit = &mut exited => {
                    return self.finish(TerminationOutcome::GracefulSuccess, Some(exit)).await;
                }
                () = clock.sleep(options.grace.as_duration()) => {}
            }
        }
        if let Err(error) = target.signal(Signal::Kill).await {
            tracing::warn!(%error, name = %self.record.name, "kept force signal failed");
        }
        let exit = match options.force_timeout {
            None => Some(exited.await),
            Some(timeout) => tokio::select! {
                exit = &mut exited => Some(exit),
                () = clock.sleep(timeout) => None,
            },
        };
        match exit {
            Some(exit) => {
                self.finish(TerminationOutcome::ForcedRequired, Some(exit))
                    .await
            }
            None => KeptTermination {
                outcome: TerminationOutcome::CleanupUnverified(UnverifiedReason::WaitTimedOut {
                    waited: options
                        .grace
                        .as_duration()
                        .saturating_add(options.force_timeout.unwrap_or(Duration::ZERO)),
                }),
                exit: None,
            },
        }
    }

    /// Stops holding the process and leaves it running for a later owner.
    pub fn release(self) {}

    async fn finish(self, outcome: TerminationOutcome, exit: Option<KeptExit>) -> KeptTermination {
        match self.forget().await {
            Ok(()) => KeptTermination { outcome, exit },
            Err(error) => {
                tracing::warn!(%error, name = %self.record.name, "kept record not removed");
                KeptTermination {
                    outcome: TerminationOutcome::CleanupUnverified(UnverifiedReason::ReapFailed),
                    exit,
                }
            }
        }
    }

    async fn forget(&self) -> Result<(), KeptError> {
        let store = self.inner.store.clone();
        let lock = self.lock.clone();
        blocking(move || store.remove(&lock)).await
    }

    async fn rewrite(&self) -> Result<(), KeptError> {
        let store = self.inner.store.clone();
        let lock = self.lock.clone();
        let record = self.record.clone();
        blocking(move || store.write(&lock, &record)).await
    }
}
