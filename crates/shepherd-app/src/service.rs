//! Service mode: keep named processes running (ADR 0023).
//!
//! A [`ServiceSupervisor`] runs any number of named [`Service`]s. Each one is started,
//! polled until its readiness check returns a value, watched, and restarted with
//! exponential backoff when it exits or never becomes ready. Status is published on a
//! `watch` channel. By default each run lives in its own scope and stops with its
//! owner; with [`ServiceOptions::kept`] each run is a kept process that outlives the
//! owner and is adopted by the next one when its fingerprint still matches.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use shepherd_domain::{
    KeptName, KeptRecord, ProcessId, ProcessScopeId, ProcessSpec, RestartPolicy, SpecFingerprint,
    TerminationOutcome,
};
use tokio::sync::{mpsc, watch};

use crate::kept::{KeptError, KeptExit, KeptProcess, KeptProcesses, KeptSpec};
use crate::output::ProcessOutput;
use crate::ports::{Clock, TerminateOptions};
use crate::supervisor::ProcessSupervisor;

/// What a service runs and how it knows the run is ready.
#[async_trait]
pub trait Service: Send + Sync + 'static {
    /// What a ready run reports (for example the address it serves).
    type Ready: Clone + Send + Sync + 'static;

    /// Prepares and describes the next run. Called before every start, so it can fetch
    /// or verify a binary, pick a port, or read settings that changed.
    async fn launch(&self) -> Result<ServiceLaunch, ServiceFailure>;

    /// Checks the running instance. Polled until it returns a value; `None` means not
    /// ready yet and an error means the run failed.
    async fn ready(
        &self,
        instance: &ServiceInstance,
    ) -> Result<Option<Self::Ready>, ServiceFailure>;
}

/// One run of a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceLaunch {
    /// What to run. Kept runs must not capture output (use `log`).
    pub spec: ProcessSpec,
    /// The file a kept run appends its output to.
    pub log: Option<PathBuf>,
    /// A revision folded into a kept run's fingerprint, for inputs the spec does not
    /// carry (a token file named in the arguments, say). A changed revision restarts it.
    pub revision: Option<String>,
}

impl ServiceLaunch {
    /// A launch of `spec` with no log and no revision.
    #[must_use]
    pub fn new(spec: ProcessSpec) -> Self {
        Self {
            spec,
            log: None,
            revision: None,
        }
    }

    /// Appends a kept run's output to `path`.
    #[must_use]
    pub fn log(mut self, path: impl Into<PathBuf>) -> Self {
        self.log = Some(path.into());
        self
    }

    /// Sets the revision.
    #[must_use]
    pub fn revision(mut self, revision: impl Into<String>) -> Self {
        self.revision = Some(revision.into());
        self
    }

    fn fingerprint(&self) -> SpecFingerprint {
        SpecFingerprint::of(&self.spec, self.revision.as_deref())
    }
}

/// A failed launch, readiness check or run, in words.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ServiceFailure(pub String);

impl ServiceFailure {
    /// A failure described by `message`.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// The running instance a readiness check looks at.
#[derive(Clone)]
pub struct ServiceInstance {
    /// Its OS pid.
    pub os_pid: u32,
    /// Whether it was adopted from an earlier owner rather than started now.
    pub adopted: bool,
    /// Captured output, for a scoped run that asked for capture.
    pub output: Option<ProcessOutput>,
    /// The kept record, for a kept run.
    pub record: Option<KeptRecord>,
}

impl std::fmt::Debug for ServiceInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceInstance")
            .field("os_pid", &self.os_pid)
            .field("adopted", &self.adopted)
            .field("output", &self.output.is_some())
            .field("record", &self.record)
            .finish()
    }
}

/// How a [`ServiceSupervisor`] runs one service.
#[derive(Debug, Clone)]
pub struct ServiceOptions {
    /// Backoff between restarts.
    pub restart: RestartPolicy,
    /// How long a run may take to become ready before it is stopped and retried.
    pub ready_within: Duration,
    /// How often readiness is checked.
    pub ready_poll: Duration,
    /// How runs are stopped.
    pub terminate: TerminateOptions,
    /// Run as a kept process in this store, so a run outlives its owner.
    pub kept: Option<KeptProcesses>,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            restart: RestartPolicy::default(),
            ready_within: Duration::from_secs(30),
            ready_poll: Duration::from_millis(500),
            terminate: TerminateOptions::default(),
            kept: None,
        }
    }
}

/// The published state of a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceStatus<T> {
    /// Launching run number `attempt` (1-based) and waiting for it to be ready.
    Starting {
        /// Which start this is.
        attempt: u32,
    },
    /// The run is ready.
    Ready {
        /// What the readiness check returned.
        value: T,
        /// The run's OS pid.
        os_pid: u32,
        /// Whether the run was adopted from an earlier owner.
        adopted: bool,
    },
    /// Waiting before the next start.
    Backoff {
        /// Consecutive failures so far.
        failures: u32,
        /// How long until the next start.
        delay: Duration,
        /// Which step of the last run failed.
        kind: ServiceFailureKind,
        /// Why the last run failed, in words for a log.
        reason: String,
    },
    /// Stopping on request.
    Stopping,
    /// Stopped; nothing is running.
    Stopped,
    /// No longer supervised; the kept run with this pid was left running.
    Released {
        /// The pid left running.
        os_pid: u32,
    },
    /// Gave up after too many consecutive failures; nothing is running.
    Failed {
        /// Which step of the last run failed.
        kind: ServiceFailureKind,
        /// Why the last run failed, in words for a log.
        reason: String,
    },
}

/// Which step of a run failed. Callers branch on this; `reason` is for a log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceFailureKind {
    /// [`Service::launch`] returned an error.
    Launch,
    /// The run could not be started: spawning it, keeping its record, or ending a
    /// previous run that had to stop first failed.
    Start,
    /// [`Service::ready`] returned an error.
    ReadyCheck,
    /// It did not become ready within [`ServiceOptions::ready_within`].
    NotReadyInTime,
    /// It exited before it was ready.
    ExitedBeforeReady,
    /// It exited after it was ready.
    Exited,
}

/// A failed run: which step, and why in words.
struct RunFailure {
    kind: ServiceFailureKind,
    reason: String,
}

impl RunFailure {
    fn new(kind: ServiceFailureKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
}

impl<T> ServiceStatus<T> {
    /// Whether no further status will follow.
    #[must_use]
    pub fn is_final(&self) -> bool {
        matches!(
            self,
            Self::Stopped | Self::Released { .. } | Self::Failed { .. }
        )
    }

    fn summary(&self) -> ServiceState {
        match self {
            Self::Starting { attempt } => ServiceState::Starting { attempt: *attempt },
            Self::Ready {
                os_pid, adopted, ..
            } => ServiceState::Ready {
                os_pid: *os_pid,
                adopted: *adopted,
            },
            Self::Backoff {
                failures, delay, ..
            } => ServiceState::Backoff {
                failures: *failures,
                delay: *delay,
            },
            Self::Stopping => ServiceState::Stopping,
            Self::Stopped => ServiceState::Stopped,
            Self::Released { os_pid } => ServiceState::Released { os_pid: *os_pid },
            Self::Failed { .. } => ServiceState::Failed,
        }
    }
}

/// [`ServiceStatus`] without its value, for listings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceState {
    /// See [`ServiceStatus::Starting`].
    Starting {
        /// Which start this is.
        attempt: u32,
    },
    /// See [`ServiceStatus::Ready`].
    Ready {
        /// The run's OS pid.
        os_pid: u32,
        /// Whether the run was adopted.
        adopted: bool,
    },
    /// See [`ServiceStatus::Backoff`].
    Backoff {
        /// Consecutive failures so far.
        failures: u32,
        /// How long until the next start.
        delay: Duration,
    },
    /// See [`ServiceStatus::Stopping`].
    Stopping,
    /// See [`ServiceStatus::Stopped`].
    Stopped,
    /// See [`ServiceStatus::Released`].
    Released {
        /// The pid left running.
        os_pid: u32,
    },
    /// See [`ServiceStatus::Failed`].
    Failed,
}

/// How to stop a service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceStop {
    /// Stop the run and its process tree.
    Terminate,
    /// Stop supervising but leave a kept run running for the next owner to adopt. A
    /// scoped run cannot outlive its owner, so it is terminated.
    Leave,
}

/// What stopping a service did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceStopped {
    /// The terminated run's outcome, when one was running and was terminated.
    pub outcome: Option<TerminationOutcome>,
    /// The pid of a kept run left running.
    pub left_running: Option<u32>,
}

/// A service's name is already supervised here.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("service {0} is already running")]
pub struct ServiceNameInUse(pub KeptName);

/// One supervised service in a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSummary {
    /// Its name.
    pub name: KeptName,
    /// Its current state.
    pub state: ServiceState,
}

/// Runs any number of named services. Cheap to clone.
#[derive(Clone)]
pub struct ServiceSupervisor {
    supervisor: ProcessSupervisor,
    clock: Arc<dyn Clock>,
    services: Arc<Mutex<HashMap<KeptName, watch::Receiver<ServiceState>>>>,
}

impl std::fmt::Debug for ServiceSupervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceSupervisor").finish_non_exhaustive()
    }
}

impl ServiceSupervisor {
    /// Runs services on `supervisor`, timing backoff and readiness with `clock`.
    pub fn new(supervisor: ProcessSupervisor, clock: Arc<dyn Clock>) -> Self {
        Self {
            supervisor,
            clock,
            services: Arc::default(),
        }
    }

    /// Starts supervising `service` as `name`. Must be called inside a Tokio runtime.
    ///
    /// # Errors
    /// [`ServiceNameInUse`] if a service with this name is still supervised here.
    pub fn start<S: Service>(
        &self,
        name: KeptName,
        service: S,
        options: ServiceOptions,
    ) -> Result<ServiceHandle<S::Ready>, ServiceNameInUse> {
        let (summary_tx, summary_rx) = watch::channel(ServiceState::Starting { attempt: 1 });
        {
            let mut services = self.services.lock().expect("services mutex");
            if services
                .get(&name)
                .is_some_and(|state| !is_final(*state.borrow()))
            {
                return Err(ServiceNameInUse(name));
            }
            services.insert(name.clone(), summary_rx);
        }
        let (status_tx, status_rx) = watch::channel(ServiceStatus::Starting { attempt: 1 });
        let (stop_tx, stop_rx) = mpsc::channel(1);
        let runner = Runner {
            name: name.clone(),
            service,
            options,
            supervisor: self.supervisor.clone(),
            clock: self.clock.clone(),
            status: status_tx,
            summary: summary_tx,
            stop: stop_rx,
        };
        let task = tokio::spawn(runner.run());
        Ok(ServiceHandle {
            name,
            status: status_rx,
            stop: Some(stop_tx),
            task: Some(task),
        })
    }

    /// Every service supervised here (including finished ones until their name is
    /// reused), sorted by name.
    #[must_use]
    pub fn list(&self) -> Vec<ServiceSummary> {
        let services = self.services.lock().expect("services mutex");
        let mut list: Vec<_> = services
            .iter()
            .map(|(name, state)| ServiceSummary {
                name: name.clone(),
                state: *state.borrow(),
            })
            .collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
    }
}

fn is_final(state: ServiceState) -> bool {
    matches!(
        state,
        ServiceState::Stopped | ServiceState::Released { .. } | ServiceState::Failed
    )
}

/// Controls one supervised service.
///
/// Dropping the handle stops supervision as [`ServiceStop::Leave`] does: a kept run is
/// left running for the next owner, a scoped run is terminated.
#[derive(Debug)]
pub struct ServiceHandle<T> {
    name: KeptName,
    status: watch::Receiver<ServiceStatus<T>>,
    stop: Option<mpsc::Sender<ServiceStop>>,
    task: Option<tokio::task::JoinHandle<ServiceStopped>>,
}

impl<T: Clone> ServiceHandle<T> {
    /// The service's name.
    #[must_use]
    pub fn name(&self) -> &KeptName {
        &self.name
    }

    /// A receiver of the service's status.
    #[must_use]
    pub fn status(&self) -> watch::Receiver<ServiceStatus<T>> {
        self.status.clone()
    }

    /// Waits until the service is ready and returns what its readiness check returned,
    /// or `None` once it has stopped or failed for good.
    pub async fn ready(&self) -> Option<T> {
        let mut status = self.status.clone();
        let seen = status
            .wait_for(|s| matches!(s, ServiceStatus::Ready { .. }) || s.is_final())
            .await
            .ok()?
            .clone();
        match seen {
            ServiceStatus::Ready { value, .. } => Some(value),
            _ => None,
        }
    }

    /// Stops the service and waits until it has stopped.
    pub async fn stop(mut self, how: ServiceStop) -> ServiceStopped {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(how).await;
        }
        let task = self.task.take().expect("task present until stop");
        task.await.unwrap_or(ServiceStopped {
            outcome: None,
            left_running: None,
        })
    }
}

impl<T> Drop for ServiceHandle<T> {
    fn drop(&mut self) {
        // Closing the channel tells the runner to stop as `Leave`; it keeps running
        // until then, so its cleanup is not cut short.
        self.stop.take();
    }
}

enum Running {
    Scoped {
        scope: ProcessScopeId,
        pid: ProcessId,
    },
    Kept(Box<KeptProcess>),
}

/// How a step ended: the run finished (or failed), or a stop was requested.
enum Step<T> {
    Done(T),
    Stop(ServiceStop),
}

struct Runner<S: Service> {
    name: KeptName,
    service: S,
    options: ServiceOptions,
    supervisor: ProcessSupervisor,
    clock: Arc<dyn Clock>,
    status: watch::Sender<ServiceStatus<S::Ready>>,
    summary: watch::Sender<ServiceState>,
    stop: mpsc::Receiver<ServiceStop>,
}

/// Runs `work` unless a stop arrives first. A closed channel (dropped handle) is `Leave`.
async fn or_stop<T>(
    stop: &mut mpsc::Receiver<ServiceStop>,
    work: impl std::future::Future<Output = T>,
) -> Step<T> {
    tokio::select! {
        biased;
        stop = stop.recv() => Step::Stop(stop.unwrap_or(ServiceStop::Leave)),
        done = work => Step::Done(done),
    }
}

impl<S: Service> Runner<S> {
    fn publish(&self, status: ServiceStatus<S::Ready>) {
        self.summary.send_replace(status.summary());
        self.status.send_replace(status);
    }

    async fn run(mut self) -> ServiceStopped {
        let mut stop = std::mem::replace(&mut self.stop, mpsc::channel(1).1);
        let mut failures = 0u32;
        let mut attempt = 0u32;
        let mut last = RunFailure::new(ServiceFailureKind::Start, String::new());
        // Only a runner that has not yet started or adopted a run may adopt one: a
        // failed first attempt must not turn a matching earlier run into a stranger.
        let mut may_adopt = true;
        loop {
            if failures > 0 {
                if self.options.restart.gives_up(failures) {
                    self.publish(ServiceStatus::Failed {
                        kind: last.kind,
                        reason: last.reason,
                    });
                    return stopped(None, None);
                }
                self.publish(ServiceStatus::Backoff {
                    failures,
                    delay: self.options.restart.delay(failures),
                    kind: last.kind,
                    reason: last.reason.clone(),
                });
                let delay = self.options.restart.delay(failures);
                if let Step::Stop(_) = or_stop(&mut stop, self.clock.sleep(delay)).await {
                    self.publish(ServiceStatus::Stopped);
                    return stopped(None, None);
                }
            }
            attempt = attempt.saturating_add(1);
            self.publish(ServiceStatus::Starting { attempt });
            let launch = match or_stop(&mut stop, self.service.launch()).await {
                Step::Stop(how) => return self.finish(None, how).await,
                Step::Done(Ok(launch)) => launch,
                Step::Done(Err(failure)) => {
                    (failures, last) = self.failed(
                        failures,
                        RunFailure::new(ServiceFailureKind::Launch, format!("launch: {failure}")),
                    );
                    continue;
                }
            };
            // Starting is never interrupted: a half-started run would have no owner. A
            // stop request waits in the channel and is seen right after.
            let started = match self.options.kept.clone() {
                Some(kept) => self.start_kept(&kept, launch, may_adopt).await,
                None => self.start_scoped(launch).await,
            };
            let (running, instance) = match started {
                Ok(started) => {
                    may_adopt = false;
                    started
                }
                Err(failure) => {
                    (failures, last) = self.failed(
                        failures,
                        RunFailure::new(ServiceFailureKind::Start, failure),
                    );
                    continue;
                }
            };
            let value = match or_stop(&mut stop, self.until_ready(&running, &instance)).await {
                Step::Stop(how) => return self.finish(Some(running), how).await,
                Step::Done(Ok(value)) => value,
                Step::Done(Err(failure)) => {
                    let _ = self.end(running, ServiceStop::Terminate).await;
                    (failures, last) = self.failed(failures, failure);
                    continue;
                }
            };
            self.publish(ServiceStatus::Ready {
                value,
                os_pid: instance.os_pid,
                adopted: instance.adopted,
            });
            let ready_at = self.clock.now();
            let exit = match or_stop(&mut stop, exited(&self.supervisor, &running)).await {
                Step::Stop(how) => return self.finish(Some(running), how).await,
                Step::Done(exit) => exit,
            };
            // A natural exit still sweeps what the run left behind.
            let _ = self.end(running, ServiceStop::Terminate).await;
            if self.clock.now().saturating_duration_since(ready_at)
                >= self.options.restart.healthy_duration()
            {
                failures = 0;
            }
            (failures, last) =
                self.failed(failures, RunFailure::new(ServiceFailureKind::Exited, exit));
        }
    }

    fn failed(&self, failures: u32, failure: RunFailure) -> (u32, RunFailure) {
        tracing::warn!(
            service = %self.name,
            failures,
            kind = ?failure.kind,
            reason = %failure.reason,
            "service run failed"
        );
        (failures.saturating_add(1), failure)
    }

    async fn start_scoped(
        &self,
        launch: ServiceLaunch,
    ) -> Result<(Running, ServiceInstance), String> {
        let scope = self
            .supervisor
            .try_create_scope()
            .map_err(|e| e.to_string())?;
        let pid = match self.supervisor.spawn(scope, launch.spec).await {
            Ok(pid) => pid,
            Err(error) => {
                let _ = self
                    .supervisor
                    .terminate_scope(scope, self.options.terminate)
                    .await;
                return Err(format!("spawn: {error}"));
            }
        };
        let instance = ServiceInstance {
            os_pid: self.supervisor.os_pid(pid).unwrap_or(0),
            adopted: false,
            output: self.supervisor.take_output(pid),
            record: None,
        };
        Ok((Running::Scoped { scope, pid }, instance))
    }

    async fn start_kept(
        &self,
        kept: &KeptProcesses,
        launch: ServiceLaunch,
        may_adopt: bool,
    ) -> Result<(Running, ServiceInstance), String> {
        let fingerprint = launch.fingerprint();
        // Any live record under this name is this service's earlier run.
        if let Some(previous) = kept.adopt(&self.name).await.map_err(kept_failure)? {
            if may_adopt && previous.record().fingerprint == fingerprint {
                let instance = ServiceInstance {
                    os_pid: previous.os_pid(),
                    adopted: true,
                    output: None,
                    record: Some(previous.record().clone()),
                };
                return Ok((Running::Kept(Box::new(previous)), instance));
            }
            let ended = previous.terminate(self.options.terminate).await;
            if !ended.outcome.is_verified() {
                return Err(format!("previous run did not stop: {:?}", ended.outcome));
            }
        }
        let mut spec = KeptSpec::new(self.name.clone(), launch.spec);
        spec.log = launch.log;
        spec.revision = launch.revision;
        let process = kept.start(spec).await.map_err(kept_failure)?;
        let instance = ServiceInstance {
            os_pid: process.os_pid(),
            adopted: false,
            output: None,
            record: Some(process.record().clone()),
        };
        Ok((Running::Kept(Box::new(process)), instance))
    }

    /// Polls readiness until a value, an error, the deadline, or the run's exit.
    async fn until_ready(
        &self,
        running: &Running,
        instance: &ServiceInstance,
    ) -> Result<S::Ready, RunFailure> {
        let deadline = self.clock.now() + self.options.ready_within;
        let late = || RunFailure::new(ServiceFailureKind::NotReadyInTime, "not ready in time");
        let check = async {
            loop {
                match self.service.ready(instance).await {
                    Ok(Some(value)) => return Ok(value),
                    Ok(None) => {}
                    Err(failure) => {
                        return Err(RunFailure::new(
                            ServiceFailureKind::ReadyCheck,
                            format!("not ready: {failure}"),
                        ))
                    }
                }
                if self.clock.now() >= deadline {
                    return Err(late());
                }
                self.clock.sleep(self.options.ready_poll).await;
            }
        };
        // The deadline also bounds a readiness check that never returns.
        tokio::select! {
            result = check => result,
            exit = exited(&self.supervisor, running) => Err(RunFailure::new(
                ServiceFailureKind::ExitedBeforeReady,
                format!("exited before ready: {exit}"),
            )),
            () = self.clock.sleep(self.options.ready_within) => Err(late()),
        }
    }

    /// Ends a run as asked and reports what happened.
    async fn end(&self, running: Running, how: ServiceStop) -> ServiceStopped {
        match (running, how) {
            (Running::Kept(process), ServiceStop::Leave) => {
                let os_pid = process.os_pid();
                process.release();
                stopped(None, Some(os_pid))
            }
            (Running::Kept(process), ServiceStop::Terminate) => {
                let ended = process.terminate(self.options.terminate).await;
                stopped(Some(ended.outcome), None)
            }
            (Running::Scoped { scope, pid }, _) => {
                let report = self
                    .supervisor
                    .terminate_scope(scope, self.options.terminate)
                    .await;
                let outcome = report.ok().map(|report| {
                    report
                        .outcomes
                        .iter()
                        .find(|(id, _)| *id == pid)
                        .map_or(TerminationOutcome::ExitedNaturally, |(_, o)| *o)
                });
                stopped(outcome, None)
            }
        }
    }

    async fn finish(&self, running: Option<Running>, how: ServiceStop) -> ServiceStopped {
        self.publish(ServiceStatus::Stopping);
        let result = match running {
            Some(running) => self.end(running, how).await,
            None => stopped(None, None),
        };
        self.publish(match result.left_running {
            Some(os_pid) => ServiceStatus::Released { os_pid },
            None => ServiceStatus::Stopped,
        });
        result
    }
}

fn stopped(outcome: Option<TerminationOutcome>, left_running: Option<u32>) -> ServiceStopped {
    ServiceStopped {
        outcome,
        left_running,
    }
}

fn kept_failure(error: KeptError) -> String {
    format!("kept: {error}")
}

/// Resolves when the run exits, described in words.
async fn exited(supervisor: &ProcessSupervisor, running: &Running) -> String {
    match running {
        Running::Scoped { pid, .. } => match supervisor.wait(*pid).await {
            Ok(exit) => describe(exit.code, exit.signal.map(|s| format!("{s:?}"))),
            Err(error) => format!("wait failed: {error}"),
        },
        Running::Kept(process) => {
            let KeptExit { code, signal, .. } = process.exited().await;
            describe(code, signal.map(|s| format!("{s:?}")))
        }
    }
}

fn describe(code: Option<i32>, signal: Option<String>) -> String {
    match (code, signal) {
        (Some(code), _) => format!("exited with code {code}"),
        (None, Some(signal)) => format!("killed by {signal}"),
        (None, None) => "exited".into(),
    }
}
