//! Kept processes against real processes: they outlive their owner, are always listed,
//! can be adopted by a later owner, and never run without a durable record (ADR 0023).
#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use shepherd::{
    GracePeriod, KeptError, KeptName, KeptProcesses, KeptSpec, ProcessSpec, TerminateOptions,
    TerminationOutcome,
};

const SETTLE: Duration = Duration::from_secs(20);

fn scratch(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "shepherd-kept-{label}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn name(text: &str) -> KeptName {
    KeptName::new(text).unwrap()
}

fn kept(dir: &Path) -> KeptProcesses {
    shepherd::kept_processes(dir.join("records"), "tests").unwrap()
}

fn quick() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_secs(5)),
        force_timeout: Some(Duration::from_secs(10)),
    }
}

/// Whether `pid` is a running (not zombie) process, read with `ps` so the check is
/// independent of the code under test.
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

async fn read_pid(path: &Path) -> u32 {
    let deadline = Instant::now() + SETTLE;
    loop {
        if let Some(pid) = std::fs::read_to_string(path)
            .ok()
            .and_then(|text| text.trim().parse().ok())
        {
            return pid;
        }
        assert!(
            Instant::now() < deadline,
            "{} never written",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn sleeper() -> ProcessSpec {
    ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever"))
}

#[tokio::test]
async fn start_list_release_adopt_and_terminate() {
    let dir = scratch("cycle");
    let kept = kept(&dir);
    let log = dir.join("out.log");
    let process = kept
        .start(
            KeptSpec::new(
                name("cycle"),
                ProcessSpec::new("/bin/sh").args(["-c", "echo hello; exec sleep 300"]),
            )
            .log(&log)
            .revision("r1"),
        )
        .await
        .unwrap();
    let pid = process.os_pid();
    assert!(running(pid));
    let record = process.record().clone();
    assert_eq!(record.os_pid, pid);
    assert_eq!(record.owner, "tests");
    assert_eq!(record.log.as_deref(), Some(log.as_path()));
    assert_eq!(record.revision.as_deref(), Some("r1"));
    assert!(
        record.exe.is_some(),
        "the executable is recorded after exec"
    );

    let listed = kept.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert!(listed[0].held, "a live owner holds it");
    assert!(matches!(
        kept.adopt(&name("cycle")).await,
        Err(KeptError::Busy(_))
    ));

    process.release();
    assert!(running(pid), "releasing leaves it running");
    let listed = kept.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert!(!listed[0].held);
    assert!(matches!(
        kept.start(KeptSpec::new(name("cycle"), sleeper())).await,
        Err(KeptError::AlreadyRunning { os_pid, .. }) if os_pid == pid
    ));

    let adopted = kept.adopt(&name("cycle")).await.unwrap().unwrap();
    assert_eq!(adopted.os_pid(), pid);
    let deadline = Instant::now() + SETTLE;
    while !std::fs::read_to_string(&log)
        .unwrap_or_default()
        .contains("hello")
    {
        assert!(Instant::now() < deadline, "log never received output");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let ended = adopted.terminate(quick()).await;
    assert_eq!(ended.outcome, TerminationOutcome::GracefulSuccess);
    assert!(!running(pid));
    assert!(kept.list().await.unwrap().is_empty());
    assert!(!dir.join("records/cycle.json").exists());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_kept_process_outlives_its_owner_and_is_adopted_by_the_next() {
    let dir = scratch("survive");
    let records = dir.join("records");
    let output = Command::new(env!("CARGO_BIN_EXE_kept_owner"))
        .arg("start")
        .arg(&records)
        .arg("tunnel")
        .arg(dir.join("tunnel.log"))
        .arg(env!("CARGO_BIN_EXE_sleep_forever"))
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let pid: u32 = String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(running(pid), "the kept process outlived its owner");

    let kept = kept(&dir);
    let listed = kept.list().await.unwrap();
    assert_eq!(listed.len(), 1, "a kept process is never a hidden orphan");
    assert_eq!(listed[0].record.os_pid, pid);
    assert_eq!(listed[0].record.owner, "kept_owner");
    assert!(!listed[0].held, "its owner is gone");

    let adopted = kept.adopt(&name("tunnel")).await.unwrap().unwrap();
    assert_eq!(
        adopted.record().fingerprint,
        KeptSpec::new(name("tunnel"), sleeper()).fingerprint()
    );
    let ended = adopted.terminate(quick()).await;
    assert!(ended.outcome.is_verified(), "{ended:?}");
    until_gone(pid).await;
    assert!(kept.list().await.unwrap().is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn an_owner_that_dies_before_the_gate_opens_leaves_nothing_running() {
    let dir = scratch("gate");
    let records = dir.join("records");
    let marker = dir.join("ran");
    let output = Command::new(env!("CARGO_BIN_EXE_kept_owner"))
        .arg("crash")
        .arg(&records)
        .arg("gated")
        .arg("/bin/sh")
        .arg("-c")
        .arg(format!("echo ran > '{}'; exec sleep 300", marker.display()))
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let pid: u32 = String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    until_gone(pid).await;
    assert!(!marker.exists(), "the program ran without its gate opening");
    assert!(
        records.join("gated.json").exists(),
        "the record was durable first"
    );

    let kept = kept(&dir);
    assert!(kept.list().await.unwrap().is_empty());
    assert!(
        !records.join("gated.json").exists(),
        "a dead record is pruned"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn natural_exit_is_observed_with_its_code_and_clears_the_record() {
    let dir = scratch("exit");
    let kept = kept(&dir);
    let process = kept
        .start(KeptSpec::new(
            name("short"),
            ProcessSpec::new("/bin/sh").args(["-c", "sleep 0.2; exit 3"]),
        ))
        .await
        .unwrap();
    let exit = tokio::time::timeout(SETTLE, process.exited())
        .await
        .unwrap();
    assert_eq!(exit.code, Some(3));
    assert!(kept.list().await.unwrap().is_empty());
    assert!(!dir.join("records/short.json").exists());
    let ended = process.terminate(quick()).await;
    assert_eq!(ended.outcome, TerminationOutcome::ExitedNaturally);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn terminate_sweeps_the_kept_process_group() {
    let dir = scratch("group");
    let kept = kept(&dir);
    let child_pid = dir.join("child.pid");
    let process = kept
        .start(KeptSpec::new(
            name("group"),
            ProcessSpec::new("/bin/sh").args([
                "-c".to_string(),
                format!("sleep 300 & echo $! > '{}'; wait", child_pid.display()),
            ]),
        ))
        .await
        .unwrap();
    let grandchild = read_pid(&child_pid).await;
    assert!(running(grandchild));
    let ended = process.terminate(quick()).await;
    assert!(ended.outcome.is_verified(), "{ended:?}");
    until_gone(grandchild).await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn failures_leave_no_process_and_no_record() {
    let dir = scratch("fail");
    let kept = kept(&dir);
    let missing = kept
        .start(KeptSpec::new(
            name("missing"),
            ProcessSpec::new(dir.join("no-such-program")),
        ))
        .await;
    assert!(matches!(missing, Err(KeptError::Launch(_))), "{missing:?}");
    assert!(!dir.join("records/missing.json").exists());

    let captured = kept
        .start(KeptSpec::new(
            name("captured"),
            sleeper().output(shepherd::OutputMode::Capture {
                buffer_bytes: 16,
                tail_bytes: 16,
            }),
        ))
        .await;
    assert!(matches!(captured, Err(KeptError::CaptureUnsupported)));
    assert!(kept.list().await.unwrap().is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_record_never_grants_authority_over_a_different_process() {
    let dir = scratch("stranger");
    let kept = kept(&dir);
    // Start one, then point its record at this test process with a wrong start token.
    let process = kept
        .start(KeptSpec::new(name("stranger"), sleeper()))
        .await
        .unwrap();
    let real = process.record().clone();
    process.terminate(quick()).await;
    let store = shepherd::DirectoryKeptStore::open(dir.join("records")).unwrap();
    use shepherd::KeptStore;
    let lock = store.lock(&real.name).unwrap().unwrap();
    let forged = shepherd::KeptRecord {
        os_pid: std::process::id(),
        // No process started at the end of time, so this can never match.
        start_token: u64::MAX,
        ..real
    };
    store.write(&lock, &forged).unwrap();
    drop(lock);

    assert!(kept.adopt(&name("stranger")).await.unwrap().is_none());
    assert!(kept.list().await.unwrap().is_empty());
    assert!(!dir.join("records/stranger.json").exists());
    assert!(running(std::process::id()));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_process_that_execs_another_program_is_still_adopted() {
    let dir = scratch("exec");
    let kept = kept(&dir);
    let process = kept
        .start(KeptSpec::new(
            name("wrapper"),
            ProcessSpec::new("/bin/sh").args(["-c", "sleep 0.3; exec sleep 300"]),
        ))
        .await
        .unwrap();
    let pid = process.os_pid();
    process.release();
    tokio::time::sleep(Duration::from_millis(600)).await;
    // The executable changed under the same instance; it must not look gone.
    let adopted = kept.adopt(&name("wrapper")).await.unwrap();
    let adopted = adopted.expect("the same instance after exec");
    assert_eq!(adopted.os_pid(), pid);
    assert!(adopted.terminate(quick()).await.outcome.is_verified());
    until_gone(pid).await;
    std::fs::remove_dir_all(dir).unwrap();
}
