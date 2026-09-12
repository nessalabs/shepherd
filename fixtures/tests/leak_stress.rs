//! One-process resource accounting: warm runtime, repeated lifecycles, then compare.
#![cfg(any(unix, windows))]
use shepherd::{GracePeriod, OutputMode, ProcessSpec, SupervisorBuilder, TerminateOptions};
use shepherd_test_support::{RuntimeFlavor, StressConfig, StressMode, TestEnvironment};
use std::time::Duration;
#[cfg(windows)]
#[path = "support/windows_handles.rs"]
mod windows_handles;
#[cfg(unix)]
fn handles() -> usize {
    #[cfg(target_os = "linux")]
    let path = "/proc/self/fd";
    #[cfg(not(target_os = "linux"))]
    let path = "/dev/fd";
    std::fs::read_dir(path).unwrap().count()
}
#[cfg(windows)]
fn handles() -> usize {
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessHandleCount};
    let mut count = 0;
    assert_ne!(
        unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) },
        0
    );
    count as usize
}
#[cfg(unix)]
fn assert_no_owned_zombies() {
    let output = std::process::Command::new("ps")
        .args(["-axo", "ppid=,stat=,comm="])
        .output()
        .unwrap();
    assert!(output.status.success());
    let own = std::process::id().to_string();
    for line in String::from_utf8(output.stdout).unwrap().lines() {
        let mut fields = line.split_whitespace();
        if fields.next() == Some(own.as_str()) {
            assert!(
                !fields.next().unwrap_or("").starts_with('Z'),
                "owned zombie: {line}"
            );
        }
    }
}
fn options() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::ZERO),
        force_timeout: Some(Duration::from_secs(5)),
    }
}
fn captured(program: &str) -> ProcessSpec {
    ProcessSpec::new(program).output(OutputMode::Capture {
        buffer_bytes: 64,
        tail_bytes: 32,
    })
}
async fn drain(output: shepherd::ProcessOutput) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = output.read();
            assert!(snapshot.errors.is_empty());
            assert!(snapshot.chunks.iter().map(|c| c.bytes.len()).sum::<usize>() <= 64);
            assert!(snapshot.tail.iter().map(|c| c.bytes.len()).sum::<usize>() <= 32);
            if snapshot.stdout_closed && snapshot.stderr_closed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("output readers did not close before resource accounting");
}
async fn cycle(sup: &shepherd::ProcessSupervisor, kind: usize) {
    if kind == 3 {
        let worker = sup.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            worker
                .with_scope_options(
                    vec![captured(env!("CARGO_BIN_EXE_sleep_forever"))],
                    options(),
                    |scope| async move {
                        let pid = scope.processes()[0];
                        assert!(tx
                            .send((scope.id(), pid, scope.take_output(pid).unwrap()))
                            .is_ok());
                        std::future::pending::<()>().await;
                    },
                )
                .await
        });
        let (scope, pid, output) = rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(sup.wait_scope_cleanup(scope).await.unwrap().all_verified());
        assert!(sup.wait(pid).await.unwrap().outcome.is_verified());
        drain(output).await;
        return;
    }
    if kind == 4 {
        let result = sup
            .with_scope_options(
                vec![
                    ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")),
                    ProcessSpec::new("shepherd-stress-missing-program-8d3a"),
                ],
                options(),
                |_| async { panic!("partial spawn must not enter body") },
            )
            .await;
        assert!(result.result.is_err());
        let report = result.termination.unwrap();
        assert_eq!(report.outcomes.len(), 1);
        assert!(report.all_verified());
        return;
    }
    if kind == 5 {
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let sup = sup.clone();
            tasks.spawn(async move {
                let scope = sup.create_scope();
                let pid = sup
                    .spawn(scope, captured(env!("CARGO_BIN_EXE_sleep_forever")))
                    .await
                    .unwrap();
                let output = sup.take_output(pid).unwrap();
                tokio::task::yield_now().await;
                assert!(sup
                    .terminate_scope(scope, options())
                    .await
                    .unwrap()
                    .all_verified());
                assert!(sup.wait(pid).await.unwrap().outcome.is_verified());
                drain(output).await;
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        return;
    }
    let scope = sup.create_scope();
    let spec = match kind {
        1 => captured(env!("CARGO_BIN_EXE_output_flood")).arg("binary"),
        2 => captured(env!("CARGO_BIN_EXE_output_flood")).arg("both"),
        _ => captured(env!("CARGO_BIN_EXE_sleep_forever")),
    };
    let pid = sup.spawn(scope, spec).await.unwrap();
    let output = sup.take_output(pid).unwrap();
    if kind == 0 {
        let tree = shepherd::process_observer()
            .tree_usage(sup.os_pid(pid).unwrap())
            .await
            .unwrap();
        assert_eq!(tree.usage.totals(None).resident_bytes.contributors, 1);
        match sup.scope_usage(scope).await {
            Ok(_) => {}
            Err(shepherd::ObservationError::Unsupported) => assert_eq!(
                sup.capabilities().descendant_containment,
                shepherd::Containment::ProcessGroup
            ),
            Err(e) => panic!("scope usage failed during stable workload: {e}"),
        }
    }
    if kind == 1 {
        assert!(sup.wait(pid).await.unwrap().outcome.is_verified());
    }
    if kind == 2 {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if output.read().dropped_bytes > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("fixture did not exercise output overflow");
    }
    assert!(sup
        .terminate_scope(scope, options())
        .await
        .unwrap()
        .all_verified());
    assert!(sup.wait(pid).await.unwrap().outcome.is_verified());
    assert!(sup.processes(scope).is_none());
    drain(output).await;
}
async fn run(iterations: usize, seed: u64) {
    // Warm the whole bounded blocking pool concurrently, not just four short reads
    // that may all finish on one worker. Keep these runtime-owned threads alive for
    // the measurement so lazy pool growth/retirement cannot change the baseline.
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(5));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let barrier = barrier.clone();
        workers.push(tokio::task::spawn_blocking(move || {
            barrier.wait();
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.await.unwrap();
    }

    let sup = SupervisorBuilder::new()
        .stats_interval(Duration::from_millis(10))
        .build();
    // Warm every scenario, including the blocking/runtime machinery used by fanout.
    for kind in 0..6 {
        cycle(&sup, kind).await;
    }
    // Warm native sampling at the maximum fanout before counting persistent runtime
    // resources. Otherwise a late first blocking-pool allocation looks like a leak.
    let warm_scope = sup.create_scope();
    let mut warm_pids = Vec::new();
    for _ in 0..4 {
        warm_pids.push(
            sup.spawn(
                warm_scope,
                ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")),
            )
            .await
            .unwrap(),
        );
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut ready = true;
            for pid in &warm_pids {
                ready &= sup.stats(*pid).await.is_ok();
            }
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native sampler did not warm up");
    assert!(sup
        .terminate_scope(warm_scope, options())
        .await
        .unwrap()
        .all_verified());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let before = handles();
    #[cfg(windows)]
    windows_handles::describe("baseline");
    let mut counts = [0usize; 6];
    let mut state = seed;
    for i in 0..iterations {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        // First six guarantee coverage, then deterministic seeded scenario selection.
        let kind = if i < 6 {
            i
        } else {
            ((state >> 32) as usize) % 6
        };
        counts[kind] += 1;
        if i > 0 && i % 500 == 0 {
            eprintln!("resource checkpoint: iteration={i} handles={}", handles());
        }
        tokio::time::timeout(Duration::from_secs(20), cycle(&sup, kind))
            .await
            .unwrap_or_else(|_| {
                panic!("stress timeout: seed={seed} iteration={i} scenario={kind}")
            });
    }
    sup.shutdown().await.unwrap();
    drop(sup);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = handles();
    #[cfg(windows)]
    windows_handles::describe("after cleanup");
    eprintln!("resource inspection: {iterations} mixed cycles; seed={seed}; scenario counts={counts:?}; handles before={before}, after={after}");
    assert!(
        after <= before,
        "FD/handle growth: before={before}, after={after}"
    );
    #[cfg(unix)]
    assert_no_owned_zombies();
}
fn exercise(config: StressConfig) {
    let StressConfig {
        seed,
        runtimes,
        iterations,
    } = config;
    for flavor in runtimes {
        eprintln!("stress runtime={flavor:?}, iterations={iterations}, seed={seed}");
        let mut builder = if *flavor == RuntimeFlavor::Current {
            tokio::runtime::Builder::new_current_thread()
        } else {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.worker_threads(2);
            builder
        };
        builder
            .max_blocking_threads(4)
            .thread_keep_alive(Duration::from_secs(3600))
            .enable_all()
            .build()
            .unwrap()
            .block_on(run(iterations, seed));
    }
}
#[test]
fn bounded_lifecycle_has_no_fd_handle_or_zombie_growth() {
    exercise(TestEnvironment::from_env().stress(StressMode::Smoke));
}
#[test]
#[ignore = "long mixed lifecycle stress; workflow_dispatch runs explicitly"]
fn long_create_kill_capture_and_reap_loops() {
    exercise(TestEnvironment::from_env().stress(StressMode::Long));
}
