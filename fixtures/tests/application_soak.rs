//! Concurrent consumer behavior under repeated supervisor startup and shutdown.
#![deny(unsafe_code)]
use shepherd::{
    process_observer, GracePeriod, ObservationError, OutputMode, ProcessSpec, SupervisorBuilder,
    TerminateOptions,
};
use shepherd_test_support::{RuntimeFlavor, SoakConfig, StressMode, TestEnvironment};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
// Native counters are isolated in one audited support module.
#[allow(unsafe_code)]
#[path = "support/resources.rs"]
mod resources;
fn options() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::ZERO),
        force_timeout: Some(Duration::from_secs(5)),
    }
}
fn captured(program: &str) -> ProcessSpec {
    ProcessSpec::new(program).output(OutputMode::Capture {
        buffer_bytes: 32 * 1024,
        tail_bytes: 1024,
    })
}
#[derive(Default)]
struct Counts {
    samples: AtomicUsize,
    cancelled: AtomicUsize,
    reads: AtomicUsize,
    dropped: AtomicUsize,
    membership_changes: AtomicUsize,
}

async fn epoch(seed: u64, index: usize, counts: Arc<Counts>) {
    let sup = SupervisorBuilder::new()
        .stats_interval(Duration::from_millis(10))
        .build();
    let closing = Arc::new(AtomicBool::new(false));
    let mut consumers = tokio::task::JoinSet::new();
    let mut readiness = Vec::new();
    let width = 2 + ((seed as usize).wrapping_add(index) % 3);
    let mut scopes = Vec::new();
    for lane in 0..width {
        let scope = sup.create_scope();
        scopes.push(scope);
        let pid = sup
            .spawn(scope, captured(env!("CARGO_BIN_EXE_soak_workload")))
            .await
            .unwrap();
        let noisy = sup
            .spawn(
                scope,
                captured(env!("CARGO_BIN_EXE_output_flood")).arg("both"),
            )
            .await
            .unwrap();
        let mut outputs = vec![
            sup.take_output(pid).unwrap(),
            sup.take_output(noisy).unwrap(),
        ];
        let os_pid = sup.os_pid(pid).unwrap();
        let worker = sup.clone();
        let closing = closing.clone();
        let counts = counts.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        readiness.push(rx);
        consumers.spawn(async move {
            let observer = process_observer();
            let first = observer.tree_usage(os_pid).await.unwrap();
            assert!(first
                .usage
                .entries
                .iter()
                .any(|e| e.identity.os_pid == os_pid && e.measurement.is_ok()));
            counts.samples.fetch_add(1, Ordering::Relaxed);
            if lane % 3 == 0 {
                outputs.clear();
                counts.dropped.fetch_add(1, Ordering::Relaxed);
            }
            // Ensure every epoch includes an observation cancellation attempt,
            // independent of how quickly a particular machine runs the loop.
            let cancel_observer = observer.clone();
            let query = tokio::spawn(async move { cancel_observer.tree_usage(os_pid).await });
            tokio::task::yield_now().await;
            query.abort();
            match query.await {
                Err(e) if e.is_cancelled() => {
                    counts.cancelled.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Ok(_)) => {}
                other => panic!("unexpected cancellation result: {other:?}"),
            }
            tx.send(()).unwrap();
            let mut previous_members: Vec<_> =
                first.usage.entries.iter().map(|e| e.identity).collect();
            let mut turn = 0;
            while !closing.load(Ordering::SeqCst) {
                // Alternate completed snapshots with cancellation while the request
                // is eligible to run. The deterministic native-permit test proves
                // the already-running cancellation boundary independently.
                if turn % 3 == 1 {
                    let observer = observer.clone();
                    let query = tokio::spawn(async move { observer.tree_usage(os_pid).await });
                    tokio::task::yield_now().await;
                    query.abort();
                    match query.await {
                        Err(e) if e.is_cancelled() => {
                            counts.cancelled.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(Ok(_)) => {}
                        Ok(Err(_)) if closing.load(Ordering::SeqCst) => {}
                        Ok(Err(e)) => panic!("completed cancellation query failed: {e}"),
                        Err(e) => panic!("sampling task panicked: {e}"),
                    }
                } else {
                    match observer.tree_usage(os_pid).await {
                        Ok(snapshot) => {
                            let members: Vec<_> =
                                snapshot.usage.entries.iter().map(|e| e.identity).collect();
                            if members != previous_members {
                                counts.membership_changes.fetch_add(1, Ordering::Relaxed);
                            }
                            previous_members = members;
                            let totals = snapshot.usage.totals(Some(&first.usage));
                            assert!(
                                totals.resident_bytes.contributors <= snapshot.usage.entries.len()
                            );
                            counts.samples.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(
                            ObservationError::NotVisible(_) | ObservationError::IdentityChanged(_),
                        ) if closing.load(Ordering::SeqCst) => {}
                        Err(e) => panic!("unexpected tree error: {e}"),
                    }
                }
                match worker.scope_usage(scope).await {
                    Ok(_) => {}
                    Err(ObservationError::Unsupported) => assert_eq!(
                        worker.capabilities().descendant_containment,
                        shepherd::Containment::ProcessGroup
                    ),
                    Err(_) if closing.load(Ordering::SeqCst) => {}
                    Err(e) => panic!("unexpected scope error: {e}"),
                }
                // Some consumers drop observers; others drain slowly, while the
                // producer continues writing both streams faster than consumption.
                for output in &outputs {
                    let value = output.read();
                    assert!(value.errors.is_empty());
                    assert!(value.chunks.iter().map(|c| c.bytes.len()).sum::<usize>() <= 32 * 1024);
                    assert!(value.tail.iter().map(|c| c.bytes.len()).sum::<usize>() <= 1024);
                    counts.reads.fetch_add(1, Ordering::Relaxed);
                }
                turn += 1;
                tokio::time::sleep(Duration::from_millis(
                    1 + (seed.wrapping_add(lane as u64).wrapping_add(turn)) % 5,
                ))
                .await;
            }
        });
    }
    for ready in readiness {
        tokio::time::timeout(Duration::from_secs(10), ready)
            .await
            .unwrap()
            .unwrap();
    }
    // Partial admission failure while the other scopes remain active.
    let partial = sup
        .with_scope_options(
            vec![
                ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")),
                ProcessSpec::new("shepherd-soak-missing-executable-648a"),
            ],
            options(),
            |_| async { panic!("partial spawn entered body") },
        )
        .await;
    assert!(partial.result.is_err());
    assert!(partial.termination.unwrap().all_verified());
    // A real scope-body cancellation while unrelated consumers are still polling.
    let clone = sup.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let cancelled = tokio::spawn(async move {
        clone
            .with_scope_options(
                vec![captured(env!("CARGO_BIN_EXE_sleep_forever"))],
                options(),
                |scope| async move {
                    tx.send(scope.id()).unwrap();
                    std::future::pending::<()>().await
                },
            )
            .await
    });
    let cancelled_scope = rx.await.unwrap();
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert!(sup
        .wait_scope_cleanup(cancelled_scope)
        .await
        .unwrap()
        .all_verified());
    tokio::time::sleep(Duration::from_millis(
        60 + (seed.wrapping_add(index as u64)) % 40,
    ))
    .await;
    closing.store(true, Ordering::SeqCst);
    // Already admitted observations can still be running. Vary explicit per-scope
    // cleanup against supervisor-wide shutdown and then immediately create a new
    // supervisor in the next epoch using the same runtime.
    if index % 2 == 0 {
        let mut cleanup = tokio::task::JoinSet::new();
        for scope in &scopes {
            let scope = *scope;
            let s = sup.clone();
            cleanup.spawn(async move {
                assert!(s
                    .terminate_scope(scope, options())
                    .await
                    .unwrap()
                    .all_verified());
            });
        }
        while let Some(done) = cleanup.join_next().await {
            done.unwrap();
        }
    }
    if index % 2 == 1 {
        let (spawn, shutdown) = tokio::join!(
            sup.spawn(
                scopes[0],
                ProcessSpec::new(env!("CARGO_BIN_EXE_exit_code")).arg("0")
            ),
            sup.shutdown()
        );
        shutdown.unwrap();
        match spawn {
            Ok(pid) => assert!(sup.wait(pid).await.unwrap().outcome.is_verified()),
            Err(shepherd::SpawnError::ScopeClosed(_) | shepherd::SpawnError::UnknownScope(_)) => {}
            Err(e) => panic!("unexpected admitted-spawn shutdown error: {e}"),
        }
    } else {
        sup.shutdown().await.unwrap();
    }
    assert!(sup.try_create_scope().is_err());
    while let Some(done) = consumers.join_next().await {
        done.unwrap();
    }
    drop(sup);
}

async fn rss() -> u64 {
    let snapshot = process_observer()
        .tree_usage(std::process::id())
        .await
        .unwrap();
    snapshot
        .usage
        .entries
        .iter()
        .find(|e| e.identity.os_pid == std::process::id())
        .unwrap()
        .measurement
        .as_ref()
        .unwrap()
        .resident_bytes
}
async fn run(config: &SoakConfig) {
    // Warm all blocking workers and retain them for stable native-resource counts.
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let mut warm = Vec::new();
    for _ in 0..4 {
        let b = barrier.clone();
        warm.push(tokio::task::spawn_blocking(move || {
            b.wait();
        }));
    }
    barrier.wait();
    for task in warm {
        task.await.unwrap();
    }
    let counts = Arc::new(Counts::default());
    for i in 0..6 {
        tokio::time::timeout(
            Duration::from_secs(30),
            epoch(config.seed, i, counts.clone()),
        )
        .await
        .unwrap_or_else(|_| panic!("soak warm-up timeout seed={} epoch={i}", config.seed));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let baseline_rss = rss().await;
    let baseline_handles = resources::handles();
    let mut max_rss = baseline_rss;
    for i in 0..config.rounds {
        tokio::time::timeout(
            Duration::from_secs(30),
            epoch(config.seed, i, counts.clone()),
        )
        .await
        .unwrap_or_else(|_| panic!("soak timeout seed={} epoch={i}", config.seed));
        let resident = rss().await;
        max_rss = max_rss.max(resident);
        assert!(resident<=baseline_rss.saturating_add(config.rss_budget_bytes),"soak RSS envelope exceeded: seed={} epoch={i} baseline={baseline_rss} current={resident}",config.seed);
        if i % 10 == 0 {
            eprintln!(
                "soak checkpoint seed={} epoch={i} rss={resident} handles={}",
                config.seed,
                resources::handles()
            );
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = resources::handles();
    assert!(
        after <= baseline_handles,
        "soak handle growth: {baseline_handles} -> {after}"
    );
    #[cfg(unix)]
    resources::assert_no_owned_zombies();
    assert!(counts.samples.load(Ordering::Relaxed) > config.rounds);
    assert!(counts.membership_changes.load(Ordering::Relaxed) > 0);
    assert!(counts.reads.load(Ordering::Relaxed) > 0);
    assert!(counts.dropped.load(Ordering::Relaxed) > 0);
    assert!(counts.cancelled.load(Ordering::Relaxed) > 0);
    eprintln!("application soak passed: rounds={} seed={} handles={baseline_handles}->{after} rss_baseline={baseline_rss} rss_max={max_rss} samples={} cancelled={} reads={} dropped={}",config.rounds,config.seed,counts.samples.load(Ordering::Relaxed),counts.cancelled.load(Ordering::Relaxed),counts.reads.load(Ordering::Relaxed),counts.dropped.load(Ordering::Relaxed));
}
fn exercise(mode: StressMode) {
    let config = TestEnvironment::from_env().soak(mode);
    for flavor in config.runtimes {
        eprintln!("application soak runtime={flavor:?}");
        let mut builder = match flavor {
            RuntimeFlavor::Current => tokio::runtime::Builder::new_current_thread(),
            RuntimeFlavor::Multi => {
                let mut b = tokio::runtime::Builder::new_multi_thread();
                b.worker_threads(2);
                b
            }
        };
        builder
            .max_blocking_threads(4)
            .thread_keep_alive(Duration::from_secs(3600))
            .enable_all()
            .build()
            .unwrap()
            .block_on(run(&config));
    }
}
#[test]
fn bounded_application_soak() {
    exercise(StressMode::Smoke);
}
#[test]
#[ignore = "manual seeded application soak"]
fn long_application_soak() {
    exercise(StressMode::Long);
}
