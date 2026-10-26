//! Service mode against real processes: restart with backoff, readiness values, status,
//! listing, and kept services that are left running and adopted again (ADR 0023).
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use shepherd::{
    GracePeriod, KeptName, ProcessSpec, RestartPolicy, Service, ServiceFailure, ServiceFailureKind,
    ServiceInstance, ServiceLaunch, ServiceOptions, ServiceState, ServiceStatus, ServiceStop,
    SupervisorBuilder, TerminateOptions,
};

const SETTLE: Duration = Duration::from_secs(20);

fn scratch(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "shepherd-service-{label}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn running(pid: u32) -> bool {
    let output = Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    let stat = String::from_utf8_lossy(&output.stdout);
    let stat = stat.trim();
    !stat.is_empty() && !stat.starts_with('Z')
}

async fn until_gone(pid: u32) {
    let deadline = Instant::now() + SETTLE;
    while running(pid) {
        assert!(Instant::now() < deadline, "pid {pid} still running");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn fast() -> ServiceOptions {
    ServiceOptions {
        restart: RestartPolicy::new(Duration::from_millis(20), Duration::from_millis(100)),
        ready_within: Duration::from_secs(10),
        ready_poll: Duration::from_millis(20),
        terminate: TerminateOptions {
            grace: GracePeriod::new(Duration::from_secs(2)),
            force_timeout: Some(Duration::from_secs(10)),
        },
        kept: None,
    }
}

/// Writes its pid to a fresh file each run; ready once that file names the run's pid.
struct PidFile {
    dir: PathBuf,
    revision: String,
    launches: Arc<AtomicU32>,
    fail_launch: bool,
    hang_ready: bool,
    fail_ready: bool,
    exit_early: bool,
}

impl PidFile {
    fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            revision: "r1".into(),
            launches: Arc::default(),
            fail_launch: false,
            hang_ready: false,
            fail_ready: false,
            exit_early: false,
        }
    }
    fn file(&self) -> PathBuf {
        self.dir.join("pid")
    }
}

#[async_trait::async_trait]
impl Service for PidFile {
    type Ready = u32;

    async fn launch(&self) -> Result<ServiceLaunch, ServiceFailure> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        if self.fail_launch {
            return Err(ServiceFailure::new("no binary"));
        }
        let script = if self.exit_early {
            "exit 3".to_owned()
        } else {
            format!("echo $$ > '{}'; exec sleep 300", self.file().display())
        };
        Ok(
            ServiceLaunch::new(ProcessSpec::new("/bin/sh").args(["-c", &script]))
                .revision(self.revision.clone())
                .log(self.dir.join("service.log")),
        )
    }

    async fn ready(&self, instance: &ServiceInstance) -> Result<Option<u32>, ServiceFailure> {
        if self.hang_ready {
            std::future::pending::<()>().await;
        }
        if self.fail_ready {
            return Err(ServiceFailure::new("bad answer"));
        }
        let pid = std::fs::read_to_string(self.file())
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok());
        Ok(pid.filter(|pid| *pid == instance.os_pid))
    }
}

async fn status_where<T: Clone>(
    status: &mut tokio::sync::watch::Receiver<ServiceStatus<T>>,
    what: impl FnMut(&ServiceStatus<T>) -> bool,
) -> ServiceStatus<T> {
    tokio::time::timeout(SETTLE, status.wait_for(what))
        .await
        .expect("status in time")
        .expect("status channel open")
        .clone()
}

#[tokio::test]
async fn a_service_restarts_after_its_process_dies() {
    let dir = scratch("restart");
    let supervisor = SupervisorBuilder::new().build();
    let services = shepherd::service_supervisor(&supervisor);
    let handle = services
        .start(KeptName::new("pinger").unwrap(), PidFile::new(&dir), fast())
        .unwrap();
    let first = handle.ready().await.unwrap();
    assert!(running(first));
    assert!(matches!(
        services.list()[0].state,
        ServiceState::Ready { os_pid, adopted: false } if os_pid == first
    ));
    assert!(services
        .start(KeptName::new("pinger").unwrap(), PidFile::new(&dir), fast())
        .is_err());

    let mut status = handle.status();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(first as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    let backoff = status_where(&mut status, |s| matches!(s, ServiceStatus::Backoff { .. })).await;
    assert!(
        matches!(&backoff, ServiceStatus::Backoff { failures: 1, kind: ServiceFailureKind::Exited, reason, .. } if reason.contains("SIGKILL") || reason.contains("Kill")),
        "{backoff:?}"
    );
    let second = match status_where(&mut status, |s| matches!(s, ServiceStatus::Ready { .. })).await
    {
        ServiceStatus::Ready { value, .. } => value,
        _ => unreachable!(),
    };
    assert_ne!(first, second);
    until_gone(first).await;

    let stopped = handle.stop(ServiceStop::Terminate).await;
    assert!(stopped.outcome.unwrap().is_verified(), "{stopped:?}");
    assert_eq!(stopped.left_running, None);
    until_gone(second).await;
    assert_eq!(services.list()[0].state, ServiceState::Stopped);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_readiness_check_that_hangs_backs_off_and_gives_up() {
    let dir = scratch("never");
    let supervisor = SupervisorBuilder::new().build();
    let services = shepherd::service_supervisor(&supervisor);
    let mut service = PidFile::new(&dir);
    // A check that never returns is bounded by the readiness deadline.
    service.hang_ready = true;
    let launches = service.launches.clone();
    let mut options = fast();
    options.ready_within = Duration::from_millis(200);
    options.restart = options.restart.max_failures(Some(2));
    let handle = services
        .start(KeptName::new("stuck").unwrap(), service, options)
        .unwrap();
    let mut status = handle.status();
    let failed = status_where(&mut status, ServiceStatus::is_final).await;
    assert_eq!(
        failed,
        ServiceStatus::Failed {
            kind: ServiceFailureKind::NotReadyInTime,
            reason: "not ready in time".into()
        }
    );
    assert_eq!(launches.load(Ordering::SeqCst), 3);
    assert_eq!(handle.ready().await, None);
    let last = std::fs::read_to_string(dir.join("pid")).unwrap();
    until_gone(last.trim().parse().unwrap()).await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn launch_failures_back_off_and_a_stop_ends_the_wait() {
    let dir = scratch("launch");
    let supervisor = SupervisorBuilder::new().build();
    let services = shepherd::service_supervisor(&supervisor);
    let mut service = PidFile::new(&dir);
    service.fail_launch = true;
    let mut options = fast();
    options.restart = RestartPolicy::new(Duration::from_secs(600), Duration::from_secs(600));
    let handle = services
        .start(KeptName::new("broken").unwrap(), service, options)
        .unwrap();
    let mut status = handle.status();
    let backoff = status_where(&mut status, |s| matches!(s, ServiceStatus::Backoff { .. })).await;
    assert_eq!(
        backoff,
        ServiceStatus::Backoff {
            failures: 1,
            delay: Duration::from_secs(600),
            kind: ServiceFailureKind::Launch,
            reason: "launch: no binary".into()
        }
    );
    let stopped = tokio::time::timeout(SETTLE, handle.stop(ServiceStop::Terminate))
        .await
        .unwrap();
    assert_eq!(stopped.outcome, None);
    assert_eq!(*status.borrow(), ServiceStatus::Stopped);
    std::fs::remove_dir_all(dir).unwrap();
}

/// A caller tells a run that ended before it was ready from one whose check refused
/// it by the failure's kind, never by its wording.
#[tokio::test]
async fn a_failure_names_the_step_that_failed() {
    for (label, configure, kind) in [
        (
            "early",
            (|service: &mut PidFile| service.exit_early = true) as fn(&mut PidFile),
            ServiceFailureKind::ExitedBeforeReady,
        ),
        (
            "refused",
            |service: &mut PidFile| service.fail_ready = true,
            ServiceFailureKind::ReadyCheck,
        ),
    ] {
        let dir = scratch(label);
        let supervisor = SupervisorBuilder::new().build();
        let services = shepherd::service_supervisor(&supervisor);
        let mut service = PidFile::new(&dir);
        configure(&mut service);
        let mut options = fast();
        options.restart = options.restart.max_failures(Some(1));
        let handle = services
            .start(KeptName::new(label).unwrap(), service, options)
            .unwrap();
        let mut status = handle.status();
        let failed = status_where(&mut status, ServiceStatus::is_final).await;
        assert!(
            matches!(&failed, ServiceStatus::Failed { kind: found, .. } if *found == kind),
            "{label}: {failed:?}"
        );
        // A run killed while writing its pid may leave the file empty.
        let pid = std::fs::read_to_string(dir.join("pid"))
            .ok()
            .and_then(|pid| pid.trim().parse().ok());
        if let Some(pid) = pid {
            until_gone(pid).await;
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn a_kept_service_is_left_running_and_adopted_while_it_matches() {
    let dir = scratch("kept");
    let kept = shepherd::kept_processes(dir.join("records"), "tests").unwrap();
    let options = || ServiceOptions {
        kept: Some(kept.clone()),
        ..fast()
    };
    let name = || KeptName::new("tunnel").unwrap();
    let supervisor = SupervisorBuilder::new().build();

    // First owner: starts it, then lets go (the owner restarting itself).
    let services = shepherd::service_supervisor(&supervisor);
    let handle = services
        .start(name(), PidFile::new(&dir), options())
        .unwrap();
    let pid = handle.ready().await.unwrap();
    let left = handle.stop(ServiceStop::Leave).await;
    assert_eq!(left.left_running, Some(pid));
    assert!(running(pid));
    assert_eq!(kept.list().await.unwrap().len(), 1);

    // Next owner, same settings: adopts the running process instead of starting one.
    let services = shepherd::service_supervisor(&supervisor);
    let handle = services
        .start(name(), PidFile::new(&dir), options())
        .unwrap();
    let mut status = handle.status();
    let ready = status_where(&mut status, |s| matches!(s, ServiceStatus::Ready { .. })).await;
    assert_eq!(
        ready,
        ServiceStatus::Ready {
            value: pid,
            os_pid: pid,
            adopted: true
        }
    );
    // Dropping the handle also leaves it running.
    drop(handle);
    status_where(&mut status, ServiceStatus::is_final).await;
    assert!(running(pid));

    // Changed settings: the old run is stopped and a new one started.
    let mut changed = PidFile::new(&dir);
    changed.revision = "r2".into();
    let handle = services.start(name(), changed, options()).unwrap();
    let mut status = handle.status();
    let ready = status_where(&mut status, |s| matches!(s, ServiceStatus::Ready { .. })).await;
    let ServiceStatus::Ready {
        value,
        adopted: false,
        ..
    } = ready
    else {
        panic!("{ready:?}");
    };
    assert_ne!(value, pid);
    until_gone(pid).await;

    // Turning it off stops the process tree and drops the record.
    let stopped = handle.stop(ServiceStop::Terminate).await;
    assert!(stopped.outcome.unwrap().is_verified());
    until_gone(value).await;
    assert!(kept.list().await.unwrap().is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn dropping_a_scoped_service_handle_terminates_its_run() {
    let dir = scratch("drop");
    let supervisor = SupervisorBuilder::new().build();
    let services = shepherd::service_supervisor(&supervisor);
    let handle = services
        .start(KeptName::new("scoped").unwrap(), PidFile::new(&dir), fast())
        .unwrap();
    let pid = handle.ready().await.unwrap();
    let mut status = handle.status();
    drop(handle);
    assert_eq!(
        status_where(&mut status, ServiceStatus::is_final).await,
        ServiceStatus::Stopped
    );
    until_gone(pid).await;
    std::fs::remove_dir_all(dir).unwrap();
}
