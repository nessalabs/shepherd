//! Run alone: the allocator measures this entire process, including runtime tasks.
#![deny(unsafe_code)]
// Reuse the audited native counter; allocation measurement itself stays safe.
#[allow(unsafe_code)]
#[path = "support/resources.rs"]
mod resources;
use shepherd::{GracePeriod, OutputMode, ProcessSpec, SupervisorBuilder, TerminateOptions};
use shepherd_test_support::{HeapConfig, RuntimeFlavor, TestEnvironment};
use std::{sync::Arc, time::Duration};
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

// Bounded history hash tables may rehash after turnover even after filling once.
// Allow 64 KiB of table capacity growth, but only 16 additional live allocations:
// a tiny allocation retained per lifecycle still fails well before a full batch.
fn within(baseline: &dhat::HeapStats, current: &dhat::HeapStats) -> bool {
    current.curr_bytes <= baseline.curr_bytes + 64 * 1024
        && current.curr_blocks <= baseline.curr_blocks + 16
}

async fn cycle(sup: &shepherd::ProcessSupervisor) {
    let scope = sup.create_scope();
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_heap_child")).output(OutputMode::Capture {
                buffer_bytes: 8192,
                tail_bytes: 1024,
            }),
        )
        .await
        .unwrap();
    let output = sup.take_output(pid).unwrap();
    let weak_output = Arc::downgrade(&output.0);
    assert!(sup.wait(pid).await.unwrap().outcome.is_verified());
    assert!(sup
        .terminate_scope(
            scope,
            TerminateOptions {
                grace: GracePeriod::new(Duration::ZERO),
                force_timeout: Some(Duration::from_secs(5)),
            }
        )
        .await
        .unwrap()
        .all_verified());
    drop(output.read());
    drop(output);
    // Cleanup must release the backend's output owner, not merely remove a PID.
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak_output.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed process retained its output buffer");
}

async fn exercise(config: &HeapConfig) {
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let b = barrier.clone();
        workers.push(tokio::task::spawn_blocking(move || {
            b.wait();
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.await.unwrap();
    }
    drop(barrier);
    let sup = SupervisorBuilder::new()
        .stats_interval(Duration::from_millis(10))
        .build();
    // Fill and turn over the bounded 256-entry histories before taking a baseline.
    for _ in 0..config.warmup_cycles {
        cycle(&sup).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let baseline_handles = resources::handles();
    let baseline = dhat::HeapStats::get();
    for batch in 0..config.batches {
        for _ in 0..config.cycles_per_batch {
            cycle(&sup).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let current = dhat::HeapStats::get();
        eprintln!("heap batch={batch} baseline_bytes={} current_bytes={} baseline_blocks={} current_blocks={}", baseline.curr_bytes,current.curr_bytes,baseline.curr_blocks,current.curr_blocks);
        dhat::assert!(
            within(&baseline, &current),
            "live heap grew beyond 64 KiB or 16 blocks"
        );
    }
    sup.shutdown().await.unwrap();
    drop(sup);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let final_handles = resources::handles();
    assert!(
        final_handles <= baseline_handles,
        "native resource growth: {baseline_handles} -> {final_handles}"
    );
    #[cfg(unix)]
    resources::assert_no_owned_zombies();
    eprintln!("heap workload native handles: {baseline_handles} -> {final_handles}");
}

#[test]
#[ignore = "isolated allocation instrumented CI job"]
fn long_lived_supervisor_releases_heap() {
    let config = TestEnvironment::from_env().heap();
    eprintln!("heap config: {config:?}");
    let _profiler = dhat::Profiler::builder()
        .testing()
        .trim_backtraces(Some(4))
        .build();
    // Verify the same predicate rejects reachable retained allocations, which LSan
    // can legitimately consider live. Release the control before testing Shepherd.
    let baseline = dhat::HeapStats::get();
    let retained = std::hint::black_box(vec![7u8; 128 * 1024]);
    assert!(
        !within(&baseline, &dhat::HeapStats::get()),
        "retention detector was disabled"
    );
    drop(retained);
    assert!(within(&baseline, &dhat::HeapStats::get()));
    let baseline = dhat::HeapStats::get();
    let retained: Vec<_> = (0..32)
        .map(|_| std::hint::black_box(Box::new(7u8)))
        .collect();
    assert!(
        !within(&baseline, &dhat::HeapStats::get()),
        "small-allocation detector was disabled"
    );
    drop(retained);
    assert!(within(&baseline, &dhat::HeapStats::get()));
    for flavor in config.runtimes {
        let mut builder = if *flavor == RuntimeFlavor::Multi {
            let mut b = tokio::runtime::Builder::new_multi_thread();
            b.worker_threads(2);
            b
        } else {
            tokio::runtime::Builder::new_current_thread()
        };
        let runtime = builder
            .max_blocking_threads(4)
            .thread_keep_alive(config.timeout + Duration::from_secs(60))
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(config.timeout, exercise(&config))
                .await
                .unwrap();
        });
        drop(runtime);
    }
}
