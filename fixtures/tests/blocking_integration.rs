//! Real-process blocking-API tests. These run on the ordinary workspace CI
//! matrix (Linux, macOS, Windows) without an async entry point on the caller.
#![cfg(any(unix, windows))]

use std::time::{Duration, Instant};

use shepherd::blocking::{BlockingRun, BlockingSupervisor, RunOptions};
use shepherd::{
    EnvPolicy, GracePeriod, OutputMode, OutputStream, ProcessSpec, TerminateOptions,
    TerminationOutcome,
};

fn supervisor() -> BlockingSupervisor {
    BlockingSupervisor::new()
}

fn short_opts() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_millis(300)),
        force_timeout: Some(Duration::from_secs(10)),
    }
}

fn run_opts(deadline: Duration) -> RunOptions {
    RunOptions {
        deadline,
        terminate: short_opts(),
        output_drain: Duration::from_secs(2),
    }
}

fn captured(spec: ProcessSpec) -> ProcessSpec {
    spec.output(OutputMode::Capture {
        buffer_bytes: 65_536,
        tail_bytes: 4_096,
    })
}

#[test]
fn natural_exit_code_is_reported() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_exit_code")).arg("7"),
        )
        .unwrap();
    let exit = sup.wait(pid).unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::ExitedNaturally);
    assert_eq!(exit.code, Some(7));
    assert!(sup
        .terminate_scope(scope, short_opts())
        .unwrap()
        .all_verified());
    sup.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
fn graceful_termination_of_a_real_process() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .unwrap();
    let exit = sup.terminate(pid, TerminateOptions::default()).unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::GracefulSuccess);
    assert!(!exit.forced);
    sup.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
fn stubborn_process_is_force_killed() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let ready =
        std::env::temp_dir().join(format!("shepherd-blocking-stubborn-{}", std::process::id()));
    let _ = std::fs::remove_file(&ready);
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_ignore_sigterm")).arg(ready.as_os_str()),
        )
        .unwrap();
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if ready.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = std::fs::remove_file(&ready);
    let exit = sup.terminate(pid, short_opts()).unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::ForcedRequired);
    assert!(exit.forced);
    sup.shutdown().unwrap();
}

#[test]
fn terminating_one_scope_leaves_another_running() {
    let sup = supervisor();
    let scope_a = sup.create_scope();
    let scope_b = sup.create_scope();
    let _a = sup
        .spawn(
            scope_a,
            ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")),
        )
        .unwrap();
    let b = sup
        .spawn(
            scope_b,
            ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")),
        )
        .unwrap();
    assert!(sup
        .terminate_scope(scope_a, short_opts())
        .unwrap()
        .all_verified());
    assert_eq!(sup.processes(scope_b).unwrap(), vec![b]);
    assert!(sup
        .terminate_scope(scope_b, short_opts())
        .unwrap()
        .all_verified());
}

#[test]
fn run_completes_with_cleared_environment_and_captured_output() {
    let sup = supervisor();
    let spec = captured(
        ProcessSpec::new(env!("CARGO_BIN_EXE_output_flood"))
            .arg("binary")
            .env(EnvPolicy::Clear(Vec::new())),
    );
    let run = sup
        .run_with_options(spec, run_opts(Duration::from_secs(10)))
        .unwrap();
    assert!(!run.timed_out(), "binary fixture must finish: {run:?}");
    assert!(run.all_verified());
    let output = run.output().expect("capture observer");
    let stdout: Vec<u8> = output
        .chunks
        .iter()
        .chain(output.tail.iter())
        .filter(|c| c.stream == OutputStream::Stdout)
        .flat_map(|c| c.bytes.iter().copied())
        .collect();
    assert!(
        stdout.windows(6).any(|w| w == [0, 255, 128, 10, 13, 0])
            || stdout == [0, 255, 128, 10, 13, 0],
        "captured stdout was {stdout:?}"
    );
}

#[test]
fn run_deadline_covers_wait_not_only_output_and_confirms_kill() {
    let sup = supervisor();
    let spec = captured(ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")));
    let started = Instant::now();
    let run = sup
        .run_with_options(spec, run_opts(Duration::from_millis(400)))
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(8),
        "deadline must bound the whole attempt, got {elapsed:?}"
    );
    assert!(run.timed_out());
    assert!(
        run.all_verified(),
        "group/job kill on expiry must be confirmed: {:?}",
        run.termination()
    );
    match run {
        BlockingRun::TimedOut { termination, .. } => {
            assert!(!termination.outcomes.is_empty());
            assert!(termination.outcomes.iter().all(|(_, o)| o.is_verified()));
        }
        other => panic!("expected timeout, got {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn run_deadline_still_kills_after_child_closes_stdio() {
    let sup = supervisor();
    // Echo one line, close both pipes, then hang. A deadline that only covered
    // reading output would return at EOF and then block forever on wait().
    let spec = captured(
        ProcessSpec::new("/bin/sh")
            .args([
                "-c",
                "echo closed-ready; exec >/dev/null 2>&1; while :; do sleep 1; done",
            ])
            .env(EnvPolicy::Clear(Vec::new())),
    );
    let started = Instant::now();
    let run = sup
        .run_with_options(spec, run_opts(Duration::from_millis(600)))
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "closed-stdio hang must not escape the deadline"
    );
    assert!(run.timed_out());
    assert!(run.all_verified(), "{:?}", run.termination());
    let output = run.output().expect("capture");
    let stdout: Vec<u8> = output
        .chunks
        .iter()
        .chain(output.tail.iter())
        .filter(|c| c.stream == OutputStream::Stdout)
        .flat_map(|c| c.bytes.iter().copied())
        .collect();
    let text = String::from_utf8_lossy(&stdout);
    assert!(
        text.contains("closed-ready"),
        "should observe bytes before the child closed stdout: {text:?}"
    );
}

#[cfg(unix)]
#[test]
fn deadline_kills_process_group_descendants() {
    let sup = supervisor();
    let spec = ProcessSpec::new(env!("CARGO_BIN_EXE_spawn_children")).arg("3");
    let run = sup
        .run_with_options(spec, run_opts(Duration::from_millis(500)))
        .unwrap();
    assert!(
        run.timed_out(),
        "spawn_children sleeps until the group is killed"
    );
    assert!(run.all_verified(), "{:?}", run.termination());
}

#[test]
fn with_scope_runs_real_process_and_nested_block() {
    let sup = supervisor();
    let outer = sup.with_scope_options(
        vec![ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever"))],
        short_opts(),
        |outer| {
            let pid = outer.processes()[0];
            let inner = sup.with_scope_options(
                vec![ProcessSpec::new(env!("CARGO_BIN_EXE_exit_code")).arg("0")],
                short_opts(),
                |inner| inner.wait(inner.processes()[0]).unwrap().code,
            );
            assert_eq!(inner.result.unwrap(), Some(0));
            assert!(inner.termination.unwrap().all_verified());
            // Inner cleanup must not reap the outer sleeper; the block's
            // terminate_scope does that after the body returns.
            assert_eq!(sup.processes(outer.id()).as_deref(), Some(&[pid][..]));
            outer.id()
        },
    );
    assert!(outer.termination.unwrap().all_verified());
}

#[test]
fn bounded_output_overflow_does_not_block_deadline_kill() {
    let sup = supervisor();
    let spec = ProcessSpec::new(env!("CARGO_BIN_EXE_output_flood"))
        .arg("stdout")
        .output(OutputMode::Capture {
            buffer_bytes: 64,
            tail_bytes: 32,
        });
    let run = sup
        .run_with_options(spec, run_opts(Duration::from_secs(8)))
        .unwrap();
    assert!(run.all_verified(), "{:?}", run.termination());
    if let Some(output) = run.output() {
        assert!(
            output.dropped_bytes > 0
                || output.chunks.iter().map(|c| c.bytes.len()).sum::<usize>() <= 64 + 32 + 4096,
            "byte cap must bound retained capture: dropped={} chunks={}",
            output.dropped_bytes,
            output.chunks.len()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_run_from_inside_application_runtime() {
    let sup = BlockingSupervisor::new();
    let run = sup
        .run_with_options(
            ProcessSpec::new(env!("CARGO_BIN_EXE_exit_code")).arg("3"),
            run_opts(Duration::from_secs(10)),
        )
        .unwrap();
    match run {
        BlockingRun::Completed {
            exit, termination, ..
        } => {
            assert_eq!(exit.code, Some(3));
            assert!(termination.all_verified());
        }
        other => panic!("expected completion, got {other:?}"),
    }
}

#[test]
fn run_deadline_kills_cleared_env_sleeper() {
    // Absolute fixture path: EnvPolicy::Clear must not depend on PATH (Windows
    // CI failed when cmd.exe could start but ping.exe could not).
    let sup = supervisor();
    let spec = captured(
        ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")).env(EnvPolicy::Clear(Vec::new())),
    );
    let started = Instant::now();
    let run = sup
        .run_with_options(spec, run_opts(Duration::from_millis(400)))
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "cleared-env sleeper must not escape the deadline, got {:?}",
        started.elapsed()
    );
    assert!(run.timed_out(), "{run:?}");
    assert!(run.all_verified(), "{:?}", run.termination());
}

#[test]
fn run_missing_program_fails_spawn_and_leaves_no_live_process() {
    let sup = supervisor();
    let spec = ProcessSpec::new(if cfg!(windows) {
        r"C:\shepherd-definitely-missing-bin.exe"
    } else {
        "/tmp/shepherd-definitely-missing-bin"
    });
    let err = sup
        .run_with_options(spec, run_opts(Duration::from_secs(5)))
        .expect_err("missing program must fail spawn");
    assert!(
        matches!(err, shepherd::blocking::BlockingRunError::Spawn(_)),
        "{err:?}"
    );
}

#[test]
fn repeated_runs_on_one_supervisor_stay_verified() {
    let sup = supervisor();
    for code in ["0", "1", "3"] {
        let run = sup
            .run_with_options(
                ProcessSpec::new(env!("CARGO_BIN_EXE_exit_code")).arg(code),
                run_opts(Duration::from_secs(10)),
            )
            .unwrap();
        assert!(run.all_verified(), "{run:?}");
        assert!(!run.timed_out());
    }
    sup.shutdown().unwrap();
}

#[test]
fn concurrent_runs_are_isolated() {
    let sup = std::sync::Arc::new(supervisor());
    std::thread::scope(|threads| {
        for _ in 0..4 {
            let sup = std::sync::Arc::clone(&sup);
            threads.spawn(move || {
                let sleeper = captured(ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")));
                let run = sup
                    .run_with_options(sleeper, run_opts(Duration::from_millis(350)))
                    .unwrap();
                assert!(run.timed_out(), "{run:?}");
                assert!(run.all_verified(), "{:?}", run.termination());
            });
        }
    });
}

#[test]
fn launch_probe_cleared_environment() {
    let dir = std::env::temp_dir().join(format!("shepherd-blocking-launch-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let values = vec![
        (
            shepherd_test_support::probe::VALUE.into(),
            "value with spaces 🐑".into(),
        ),
        (
            shepherd_test_support::probe::CWD.into(),
            dir.as_os_str().to_owned(),
        ),
    ];
    let sup = supervisor();
    let spec = ProcessSpec::new(env!("CARGO_BIN_EXE_launch_probe"))
        .args([
            "",
            "two words",
            "quote\"inside",
            "back\\slash\\",
            "日本語 🐑",
        ])
        .env(EnvPolicy::Clear(values))
        .cwd(&dir);
    let run = sup
        .run_with_options(spec, run_opts(Duration::from_secs(10)))
        .unwrap();
    match &run {
        BlockingRun::Completed { exit, .. } => {
            assert_eq!(
                exit.code,
                Some(0),
                "child launch assertions failed: {exit:?}"
            );
        }
        other => panic!("expected launch probe to finish: {other:?}"),
    }
    assert!(run.all_verified());
    let _ = std::fs::remove_dir_all(&dir);
}
