//! Portable blocking-API contract tests over `NullBackend`.
//! These prove the synchronous surface keeps the ownership invariant without
//! requiring the caller to be inside Tokio, and that runtime nesting is handled.

#![cfg(feature = "blocking")]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use shepherd::blocking::{BlockingRunError, BlockingSupervisor, RunOptions};
use shepherd::{
    GracePeriod, NullBackend, OutputChunk, OutputMode, OutputSnapshot, OutputStream,
    ProcessBackend, ProcessOutput, ProcessSpec, ScopeCreationError, SpawnError, Spawned,
    SupervisorBuilder, TerminateError, TerminateOptions, TerminationOutcome, WaitError,
};
use shepherd_app::output::OutputSink;
use shepherd_domain::{ProcessScopeId, RawExit, RawStats, Signal};

fn supervisor() -> BlockingSupervisor {
    BlockingSupervisor::builder()
        .backend(Arc::new(NullBackend::new()))
        .build()
        .expect("blocking runtime")
}

fn short_opts() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_millis(50)),
        force_timeout: Some(Duration::from_secs(2)),
    }
}

fn run_opts(deadline: Duration) -> RunOptions {
    RunOptions {
        deadline,
        terminate: short_opts(),
        output_drain: Duration::from_millis(20),
    }
}

#[test]
fn spawn_wait_natural_exit_outside_async() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("exit-immediately"))
        .unwrap();
    let os = sup
        .os_pid(pid)
        .expect("OS pid remains after an immediate natural exit");
    let exit = sup.wait(pid).unwrap();
    assert_eq!(sup.os_pid(pid), Some(os));
    assert_eq!(exit.outcome, TerminationOutcome::ExitedNaturally);
    assert!(sup
        .terminate_scope(scope, short_opts())
        .unwrap()
        .all_verified());
    sup.shutdown().unwrap();
}

#[test]
fn graceful_and_forced_termination() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let polite = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .unwrap();
    let exit = sup.terminate(polite, short_opts()).unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::GracefulSuccess);

    let stubborn = sup
        .spawn(scope, ProcessSpec::new("ignore-graceful"))
        .unwrap();
    let exit = sup.terminate(stubborn, short_opts()).unwrap();
    assert_eq!(exit.outcome, TerminationOutcome::ForcedRequired);
    assert!(exit.forced);
    sup.shutdown().unwrap();
}

#[test]
fn terminate_scope_cannot_touch_another_scope() {
    let sup = supervisor();
    let scope_a = sup.create_scope();
    let scope_b = sup.create_scope();
    let _a = sup
        .spawn(scope_a, ProcessSpec::new("respect-graceful"))
        .unwrap();
    let b = sup
        .spawn(scope_b, ProcessSpec::new("ignore-graceful"))
        .unwrap();

    assert!(sup
        .terminate_scope(scope_a, short_opts())
        .unwrap()
        .all_verified());
    assert_eq!(sup.processes(scope_b).expect("scope b"), vec![b]);
    assert!(sup
        .terminate_scope(scope_b, short_opts())
        .unwrap()
        .all_verified());
}

#[test]
fn draining_scope_rejects_new_spawns() {
    let sup = supervisor();
    let scope = sup.create_scope();
    sup.terminate_scope(scope, short_opts()).unwrap();
    let err = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .unwrap_err();
    assert!(matches!(err, SpawnError::ScopeClosed(_)));
}

#[test]
fn shutdown_rejects_new_scopes_and_is_idempotent() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let _ = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .unwrap();
    sup.shutdown().unwrap();
    sup.shutdown().unwrap();
    assert_eq!(
        sup.try_create_scope(),
        Err(ScopeCreationError::SupervisorClosed)
    );
}

#[test]
fn with_scope_preserves_result_and_cleanup() {
    let sup = supervisor();
    for fail in [false, true] {
        let result = sup.with_scope_options(
            vec![ProcessSpec::new("ignore-graceful")],
            short_opts(),
            |scope| {
                assert_eq!(scope.processes().len(), 1);
                if fail {
                    Err("closure error")
                } else {
                    Ok::<_, &str>(42)
                }
            },
        );
        assert_eq!(
            result.result.unwrap(),
            if fail { Err("closure error") } else { Ok(42) }
        );
        assert!(result.termination.unwrap().all_verified());
    }
}

#[test]
fn with_scope_body_can_spawn_and_wait() {
    let sup = supervisor();
    let result = sup.with_scope_options(Vec::new(), short_opts(), |scope| {
        let pid = scope
            .spawn(ProcessSpec::new("exit-immediately"))
            .expect("scoped spawn");
        scope.wait(pid).expect("scoped wait").outcome
    });
    assert_eq!(result.result.unwrap(), TerminationOutcome::ExitedNaturally);
    assert!(result.termination.unwrap().all_verified());
}

#[test]
fn with_scope_panic_still_verifies_cleanup() {
    let sup = supervisor();
    let seen = std::sync::Mutex::new(None);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = sup.with_scope_options(
            vec![ProcessSpec::new("ignore-graceful")],
            short_opts(),
            |scope| {
                *seen.lock().expect("seen") = Some(scope.id());
                panic!("injected blocking body panic");
            },
        );
    }));
    assert!(panicked.is_err());
    let scope = seen.lock().expect("seen").expect("scope recorded");
    assert!(sup.wait_scope_cleanup(scope).unwrap().all_verified());
}

#[test]
fn nested_with_scope_blocks_are_independent() {
    let sup = supervisor();
    let outer = sup.with_scope_options(
        vec![ProcessSpec::new("ignore-graceful")],
        short_opts(),
        |outer| {
            let inner = sup.with_scope_options(
                vec![ProcessSpec::new("respect-graceful")],
                short_opts(),
                |inner| inner.id(),
            );
            assert_ne!(outer.id(), inner.result.unwrap());
            assert!(inner.termination.unwrap().all_verified());
            outer.id()
        },
    );
    assert!(outer.termination.unwrap().all_verified());
}

#[test]
fn run_completed_natural_exit() {
    let sup = supervisor();
    let run = sup
        .run_with_options(
            ProcessSpec::new("exit-immediately"),
            run_opts(Duration::from_secs(2)),
        )
        .unwrap();
    assert!(!run.timed_out());
    assert!(run.all_verified());
    match run {
        shepherd::blocking::BlockingRun::Completed { exit, .. } => {
            assert_eq!(exit.outcome, TerminationOutcome::ExitedNaturally);
        }
        other => panic!("expected completion, got {other:?}"),
    }
}

#[test]
fn run_deadline_kills_and_reaps() {
    let sup = supervisor();
    let run = sup
        .run_with_options(
            ProcessSpec::new("ignore-graceful"),
            run_opts(Duration::from_millis(80)),
        )
        .unwrap();
    assert!(run.timed_out());
    assert!(
        run.all_verified(),
        "deadline expiry must still confirm group cleanup: {:?}",
        run.termination()
    );
}

#[test]
fn concurrent_threads_share_one_supervisor() {
    let sup = Arc::new(supervisor());
    std::thread::scope(|threads| {
        for _ in 0..4 {
            let sup = Arc::clone(&sup);
            threads.spawn(move || {
                let scope = sup.create_scope();
                let pid = sup
                    .spawn(scope, ProcessSpec::new("exit-immediately"))
                    .unwrap();
                assert_eq!(
                    sup.wait(pid).unwrap().outcome,
                    TerminationOutcome::ExitedNaturally
                );
                assert!(sup
                    .terminate_scope(scope, short_opts())
                    .unwrap()
                    .all_verified());
            });
        }
    });
    sup.shutdown().unwrap();
}

#[test]
fn from_runtime_current_thread_from_sync_code() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let sup = BlockingSupervisor::from_runtime_and_builder(
        runtime,
        SupervisorBuilder::new().backend(Arc::new(NullBackend::new())),
    );
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("exit-immediately"))
        .unwrap();
    assert_eq!(
        sup.wait(pid).unwrap().outcome,
        TerminationOutcome::ExitedNaturally
    );
    assert!(sup
        .terminate_scope(scope, short_opts())
        .unwrap()
        .all_verified());
}

fn current_thread_blocking(backend: Arc<dyn ProcessBackend>) -> BlockingSupervisor {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    BlockingSupervisor::from_runtime_and_builder(runtime, SupervisorBuilder::new().backend(backend))
}

#[test]
fn from_runtime_current_thread_with_scope_body_can_spawn_and_wait() {
    // from_runtime is the documented current-thread path. The body must be able
    // to spawn/wait; those calls cannot nest block_on on that same runtime.
    let sup = current_thread_blocking(Arc::new(NullBackend::new()));
    let result = sup.with_scope_options(Vec::new(), short_opts(), |scope| {
        let pid = scope
            .spawn(ProcessSpec::new("exit-immediately"))
            .expect("scoped spawn on current-thread owned runtime");
        scope
            .wait(pid)
            .expect("scoped wait on current-thread")
            .outcome
    });
    assert_eq!(result.result.unwrap(), TerminationOutcome::ExitedNaturally);
    assert!(result.termination.unwrap().all_verified());
}

#[test]
fn with_scope_wait_rejects_foreign_process() {
    let sup = supervisor();
    let foreign_scope = sup.create_scope();
    let foreign = sup
        .spawn(foreign_scope, ProcessSpec::new("exit-immediately"))
        .unwrap();
    let result = sup.with_scope_options(Vec::new(), short_opts(), |scope| {
        scope
            .wait(foreign)
            .expect_err("must not wait a foreign pid")
    });
    assert!(matches!(
        result.result.unwrap(),
        WaitError::UnknownProcess(_)
    ));
    assert!(result.termination.unwrap().all_verified());
    assert!(sup
        .terminate_scope(foreign_scope, short_opts())
        .unwrap()
        .all_verified());
}

#[test]
fn from_handle_multi_thread_from_sync_code() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let sup = BlockingSupervisor::from_handle_and_builder(
        runtime.handle().clone(),
        SupervisorBuilder::new().backend(Arc::new(NullBackend::new())),
    );
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("exit-immediately"))
        .unwrap();
    assert_eq!(sup.wait(pid).unwrap().code, Some(0));
    drop(sup);
    drop(runtime);
}

#[test]
fn drop_without_shutdown_does_not_hang() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let _ = sup
        .spawn(scope, ProcessSpec::new("ignore-graceful"))
        .unwrap();
    drop(sup);
}

#[tokio::test(flavor = "current_thread")]
async fn owned_runtime_from_inside_current_thread_tokio() {
    let sup = supervisor();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("exit-immediately"))
        .unwrap();
    assert_eq!(
        sup.wait(pid).unwrap().outcome,
        TerminationOutcome::ExitedNaturally
    );
    assert!(sup
        .terminate_scope(scope, short_opts())
        .unwrap()
        .all_verified());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byo_handle_from_inside_same_multi_thread_runtime() {
    let sup = BlockingSupervisor::from_handle_and_builder(
        tokio::runtime::Handle::current(),
        SupervisorBuilder::new().backend(Arc::new(NullBackend::new())),
    );
    let run = sup
        .run_with_options(
            ProcessSpec::new("exit-immediately"),
            run_opts(Duration::from_secs(2)),
        )
        .unwrap();
    assert!(run.all_verified());
    assert!(!run.timed_out());
}

#[test]
fn run_zero_deadline_still_confirms_cleanup() {
    let sup = supervisor();
    let run = sup
        .run_with_options(
            ProcessSpec::new("ignore-graceful"),
            run_opts(Duration::ZERO),
        )
        .unwrap();
    assert!(run.timed_out());
    assert!(
        run.all_verified(),
        "deadline 0 must still terminate and reap: {:?}",
        run.termination()
    );
}

#[test]
fn run_near_deadline_success_does_not_starve_cleanup() {
    let sup = supervisor();
    let run = sup
        .run_with_options(
            ProcessSpec::new("exit-immediately"),
            run_opts(Duration::from_millis(1)),
        )
        .unwrap();
    // Either the wait won the race or the deadline did; both must verify reap.
    assert!(
        run.all_verified(),
        "a 1ms leftover must not clamp terminate to a failed reap: {:?}",
        run.termination()
    );
    run.into_verified().expect("verified");
}

#[test]
fn run_spawn_failure_still_terminates_the_admitted_scope() {
    let backend = FailSpawnThenCleanup::default();
    let sup = BlockingSupervisor::builder()
        .backend(Arc::new(backend))
        .build()
        .unwrap();
    let err = sup
        .run_with_options(
            ProcessSpec::new("anything"),
            run_opts(Duration::from_secs(2)),
        )
        .expect_err("spawn and cleanup both fail");
    assert!(
        matches!(err, BlockingRunError::Terminate(_)),
        "cleanup failure must not be hidden behind spawn: {err:?}"
    );
}

#[test]
fn run_spawn_failure_with_verified_cleanup_returns_spawn() {
    let backend = FailSpawnOnly::default();
    let sup = BlockingSupervisor::builder()
        .backend(Arc::new(backend))
        .build()
        .unwrap();
    let err = sup
        .run_with_options(
            ProcessSpec::new("anything"),
            run_opts(Duration::from_secs(2)),
        )
        .expect_err("spawn fails");
    assert!(
        matches!(err, BlockingRunError::Spawn(_)),
        "verified cleanup should preserve the spawn error: {err:?}"
    );
}

#[test]
fn from_handle_current_thread_from_sync_panics_instead_of_hanging() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let sup = BlockingSupervisor::from_handle_and_builder(
        runtime.handle().clone(),
        SupervisorBuilder::new().backend(Arc::new(NullBackend::new())),
    );
    let scope = sup.create_scope();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = sup.spawn(scope, ProcessSpec::new("exit-immediately"));
    }));
    assert!(panicked.is_err(), "must refuse current-thread from_handle");
}

#[tokio::test(flavor = "current_thread")]
async fn byo_current_thread_same_runtime_panics_instead_of_deadlocking() {
    let sup = BlockingSupervisor::from_handle_and_builder(
        tokio::runtime::Handle::current(),
        SupervisorBuilder::new().backend(Arc::new(NullBackend::new())),
    );
    let scope = sup.create_scope();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = sup.spawn(scope, ProcessSpec::new("exit-immediately"));
    }));
    assert!(
        panicked.is_err(),
        "same current-thread from_handle must panic, not deadlock"
    );
}

#[test]
fn run_after_shutdown_is_supervisor_closed() {
    let sup = supervisor();
    sup.shutdown().unwrap();
    let err = sup
        .run_with_options(
            ProcessSpec::new("exit-immediately"),
            run_opts(Duration::from_secs(1)),
        )
        .expect_err("closed supervisor");
    assert!(matches!(err, BlockingRunError::Scope(_)), "{err:?}");
}

#[test]
fn with_scope_partial_spawn_still_cleans() {
    let backend = FailSecondSpawn::default();
    let sup = BlockingSupervisor::builder()
        .backend(Arc::new(backend))
        .build()
        .unwrap();
    let result = sup.with_scope_options(
        vec![
            ProcessSpec::new("exit-immediately"),
            ProcessSpec::new("fail-this-one"),
        ],
        short_opts(),
        |_| panic!("body must not run after partial spawn failure"),
    );
    assert!(result.result.is_err(), "{:?}", result.result.err());
    assert!(result.termination.unwrap().all_verified());
}

#[test]
fn empty_with_scope_still_reports_cleanup() {
    let sup = supervisor();
    let result = sup.with_scope_options(Vec::new(), short_opts(), |scope| {
        assert!(scope.processes().is_empty());
        7
    });
    assert_eq!(result.result.unwrap(), 7);
    assert!(result.termination.unwrap().all_verified());
}

#[test]
fn clone_shares_runtime_and_survives_sibling_drop() {
    let a = supervisor();
    let b = a.clone();
    drop(a);
    let scope = b.create_scope();
    let pid = b
        .spawn(scope, ProcessSpec::new("exit-immediately"))
        .unwrap();
    assert_eq!(
        b.wait(pid).unwrap().outcome,
        TerminationOutcome::ExitedNaturally
    );
    assert!(b
        .terminate_scope(scope, short_opts())
        .unwrap()
        .all_verified());
}

#[test]
fn repeated_run_cycles_stay_verified() {
    let sup = supervisor();
    for i in 0..32 {
        let spec = if i % 2 == 0 {
            ProcessSpec::new("exit-immediately")
        } else {
            ProcessSpec::new("ignore-graceful")
        };
        let run = sup
            .run_with_options(spec, run_opts(Duration::from_millis(80)))
            .unwrap();
        assert!(
            run.all_verified(),
            "cycle {i} unverified: {:?}",
            run.termination()
        );
    }
    sup.shutdown().unwrap();
}

#[test]
fn slow_spawn_is_waited_then_cleaned() {
    let sup = BlockingSupervisor::builder()
        .backend(Arc::new(SlowSpawnBackend {
            inner: NullBackend::new(),
            delay: Duration::from_millis(80),
        }))
        .build()
        .unwrap();
    let started = Instant::now();
    let run = sup
        .run_with_options(
            ProcessSpec::new("ignore-graceful"),
            run_opts(Duration::from_millis(20)),
        )
        .unwrap();
    // Deadline fires during spawn; cleanup must still wait for admission and reap.
    assert!(run.timed_out() || run.all_verified(), "{run:?}");
    assert!(
        run.all_verified(),
        "in-flight spawn must not be abandoned: {:?}",
        run.termination()
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "slow spawn cleanup hung: {:?}",
        started.elapsed()
    );
}

#[test]
fn into_verified_rejects_unverified_report() {
    let run = shepherd::blocking::BlockingRun::TimedOut {
        output: None,
        termination: shepherd::ScopeTerminationReport {
            scope: shepherd::ProcessScopeId::new(1),
            outcomes: vec![(
                shepherd::ProcessId::new(1),
                TerminationOutcome::CleanupUnverified(shepherd::UnverifiedReason::ReapFailed),
            )],
        },
    };
    assert!(!run.all_verified());
    assert!(matches!(
        run.into_verified(),
        Err(BlockingRunError::Unverified { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_runtime_from_inside_foreign_multi_thread_runtime() {
    let sup = supervisor();
    let result = sup.with_scope_options(
        vec![ProcessSpec::new("exit-immediately")],
        short_opts(),
        |scope| scope.wait(scope.processes()[0]).unwrap().outcome,
    );
    assert_eq!(result.result.unwrap(), TerminationOutcome::ExitedNaturally);
    assert!(result.termination.unwrap().all_verified());
}

#[derive(Default)]
struct FailSpawnThenCleanup {
    inner: NullBackend,
}

#[async_trait]
impl ProcessBackend for FailSpawnThenCleanup {
    async fn spawn(
        &self,
        _scope: ProcessScopeId,
        _spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        Err(SpawnError::Os("injected spawn failure".into()))
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn cleanup_scope(&self, _scope: ProcessScopeId) -> Result<(), TerminateError> {
        Err(TerminateError::Signal(
            "injected containment failure".into(),
        ))
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, shepherd::StatsError> {
        self.inner.sample(target).await
    }
    fn capabilities(&self) -> shepherd::Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

#[derive(Default)]
struct FailSpawnOnly {
    inner: NullBackend,
}

#[async_trait]
impl ProcessBackend for FailSpawnOnly {
    async fn spawn(
        &self,
        _scope: ProcessScopeId,
        _spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        Err(SpawnError::Os("injected spawn failure".into()))
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, shepherd::StatsError> {
        self.inner.sample(target).await
    }
    fn capabilities(&self) -> shepherd::Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

#[derive(Default)]
struct FailSecondSpawn {
    inner: NullBackend,
    count: AtomicUsize,
}

#[async_trait]
impl ProcessBackend for FailSecondSpawn {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        let n = self.count.fetch_add(1, Ordering::SeqCst);
        if n >= 1 {
            return Err(SpawnError::Os("injected second spawn failure".into()));
        }
        self.inner.spawn(scope, spec).await
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, shepherd::StatsError> {
        self.inner.sample(target).await
    }
    fn capabilities(&self) -> shepherd::Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

/// Shared current-thread `from_runtime` must serialize `block_on` instead of
/// deadlocking, and `run` timers must still fire on that scheduler.
#[test]
fn current_thread_shared_runtime_stays_responsive() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BlockingSupervisor>();

    let sup = std::sync::Arc::new(current_thread_blocking(Arc::new(NullBackend::new())));
    let started = Instant::now();
    std::thread::scope(|threads| {
        for i in 0..3 {
            let sup = std::sync::Arc::clone(&sup);
            threads.spawn(move || {
                let scope = sup.create_scope();
                let pid = sup
                    .spawn(scope, ProcessSpec::new("exit-immediately"))
                    .unwrap_or_else(|e| panic!("thread {i} spawn: {e}"));
                assert_eq!(
                    sup.wait(pid).unwrap().outcome,
                    TerminationOutcome::ExitedNaturally
                );
                assert!(sup
                    .terminate_scope(scope, short_opts())
                    .unwrap()
                    .all_verified());
            });
        }
        let runner = std::sync::Arc::clone(&sup);
        threads.spawn(move || {
            let run = runner
                .run_with_options(
                    ProcessSpec::new("ignore-graceful"),
                    run_opts(Duration::from_millis(40)),
                )
                .unwrap();
            assert!(run.timed_out(), "{run:?}");
            assert!(run.all_verified(), "{:?}", run.termination());
        });
    });
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "current-thread shared runtime hung: {:?}",
        started.elapsed()
    );
}

#[test]
fn with_scope_terminate_then_block_cleanup_is_idempotent() {
    let sup = supervisor();
    let result = sup.with_scope_options(
        vec![ProcessSpec::new("ignore-graceful")],
        short_opts(),
        |scope| {
            sup.terminate_scope(scope.id(), short_opts())
                .unwrap()
                .all_verified()
        },
    );
    assert!(result.result.unwrap());
    let termination = result.termination.expect("outer cleanup");
    assert!(
        termination.all_verified(),
        "second terminate_scope after body must be idempotent: {termination:?}"
    );
}

#[test]
fn run_timeout_surfaces_containment_failure() {
    let sup = BlockingSupervisor::builder()
        .backend(Arc::new(TimeoutThenContainmentFail::default()))
        .build()
        .unwrap();
    let err = sup
        .run_with_options(
            ProcessSpec::new("ignore-graceful"),
            run_opts(Duration::from_millis(20)),
        )
        .expect_err("cleanup failure after timeout must surface");
    assert!(
        matches!(err, BlockingRunError::Terminate(_)),
        "must not return TimedOut that hides a failed reap: {err:?}"
    );
}

#[test]
fn run_wait_failure_is_not_a_verified_success() {
    let sup = BlockingSupervisor::builder()
        .backend(Arc::new(FailWaitAfterSpawn::default()))
        .build()
        .unwrap();
    let started = Instant::now();
    let result = sup.run_with_options(
        ProcessSpec::new("ignore-graceful"),
        run_opts(Duration::from_secs(2)),
    );
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "wait-error cleanup hung: {:?}",
        started.elapsed()
    );
    match result {
        Ok(run) => {
            assert!(
                !run.all_verified(),
                "a failed reap must not look successful: {run:?}"
            );
            assert!(
                run.into_verified().is_err(),
                "into_verified is the gate for hosts that refuse unverified reaps"
            );
        }
        Err(err) => assert!(
            matches!(
                err,
                BlockingRunError::Wait(_)
                    | BlockingRunError::Terminate(_)
                    | BlockingRunError::Unverified { .. }
            ),
            "wait-error path must be a typed failure, got {err:?}"
        ),
    }
}

#[test]
fn with_scope_panic_keeps_cleanup_error_visible() {
    let sup = BlockingSupervisor::builder()
        .backend(Arc::new(TimeoutThenContainmentFail::default()))
        .build()
        .unwrap();
    let seen = std::sync::Mutex::new(None);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = sup.with_scope_options(
            vec![ProcessSpec::new("ignore-graceful")],
            short_opts(),
            |scope| {
                *seen.lock().expect("seen") = Some(scope.id());
                panic!("body panic before failed cleanup");
            },
        );
    }));
    assert!(panicked.is_err());
    let scope = seen.lock().expect("seen").expect("scope");
    let cleanup = sup.wait_scope_cleanup(scope);
    assert!(
        cleanup.is_err(),
        "cleanup failure after panic must stay visible: {cleanup:?}"
    );
}

/// Timeout, panic, self-terminate, and natural exit on one supervisor at once.
/// Each outcome must be verified or a typed failure — not a hang or a silent leak.
#[test]
fn chaos_mixed_failures_on_one_supervisor() {
    let sup = std::sync::Arc::new(supervisor());
    let started = Instant::now();
    let panicked_scope = std::sync::Arc::new(std::sync::Mutex::new(None));
    std::thread::scope(|threads| {
        let timeout_sup = std::sync::Arc::clone(&sup);
        threads.spawn(move || {
            let run = timeout_sup
                .run_with_options(
                    ProcessSpec::new("ignore-graceful"),
                    run_opts(Duration::from_millis(50)),
                )
                .unwrap();
            assert!(run.timed_out(), "{run:?}");
            assert!(run.all_verified(), "{:?}", run.termination());
        });
        let natural_sup = std::sync::Arc::clone(&sup);
        threads.spawn(move || {
            let run = natural_sup
                .run_with_options(
                    ProcessSpec::new("exit-immediately"),
                    run_opts(Duration::from_secs(2)),
                )
                .unwrap();
            assert!(!run.timed_out(), "{run:?}");
            assert!(run.all_verified(), "{:?}", run.termination());
        });
        let panic_sup = std::sync::Arc::clone(&sup);
        let seen = std::sync::Arc::clone(&panicked_scope);
        threads.spawn(move || {
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = panic_sup.with_scope_options(
                    vec![ProcessSpec::new("ignore-graceful")],
                    short_opts(),
                    |scope| {
                        *seen.lock().expect("seen") = Some(scope.id());
                        panic!("chaos body panic");
                    },
                );
            }));
            assert!(panicked.is_err());
        });
        let self_term = std::sync::Arc::clone(&sup);
        threads.spawn(move || {
            let result = self_term.with_scope_options(
                vec![ProcessSpec::new("respect-graceful")],
                short_opts(),
                |scope| {
                    self_term
                        .terminate_scope(scope.id(), short_opts())
                        .unwrap()
                        .all_verified()
                },
            );
            assert!(result.result.unwrap());
            assert!(result.termination.unwrap().all_verified());
        });
    });
    let scope = panicked_scope.lock().expect("seen").expect("panic scope");
    assert!(sup.wait_scope_cleanup(scope).unwrap().all_verified());
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "mixed chaos hung: {:?}",
        started.elapsed()
    );
}

#[test]
fn scoped_cleanup_history_evicts_oldest() {
    let sup = supervisor();
    let first = sup
        .with_scope_options(Vec::new(), short_opts(), |scope| scope.id())
        .scope;
    let mut last = first;
    for _ in 0..256 {
        last = sup
            .with_scope_options(Vec::new(), short_opts(), |scope| scope.id())
            .scope;
    }
    assert!(
        sup.wait_scope_cleanup(first).is_err(),
        "history is bounded; the oldest scoped report must evict"
    );
    assert!(sup.wait_scope_cleanup(last).unwrap().all_verified());
}

#[test]
fn with_scope_prunes_authorization_after_observation_history_bound() {
    let sup = supervisor();
    let result = sup.with_scope_options(Vec::new(), short_opts(), |scope| {
        let mut first = None;
        let mut latest = None;
        for _ in 0..300 {
            let pid = scope
                .spawn(ProcessSpec::new("exit-immediately"))
                .expect("scoped spawn");
            first.get_or_insert(pid);
            latest = Some(pid);
            assert!(scope.wait(pid).unwrap().outcome.is_verified());
            assert!(
                scope.authorized_len() <= 257,
                "blocking scope retained every completed id: {}",
                scope.authorized_len()
            );
        }
        let first = first.expect("first pid");
        let latest = latest.expect("latest pid");
        assert!(
            matches!(scope.wait(first), Err(WaitError::UnknownProcess(id)) if id == first),
            "expired observation history must drop authorization"
        );
        assert!(scope.wait(latest).unwrap().outcome.is_verified());
        scope.authorized_len()
    });
    assert!(result.result.unwrap() <= 257);
    assert!(result.termination.unwrap().all_verified());
}

#[test]
fn wait_scope_cleanup_can_register_before_the_body_returns() {
    let sup = supervisor();
    let (scope_tx, scope_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let body = {
        let sup = sup.clone();
        std::thread::spawn(move || {
            sup.with_scope_options(Vec::new(), short_opts(), move |scope| {
                scope_tx.send(scope.id()).expect("scope");
                release_rx.recv().expect("release");
                7
            })
        })
    };
    let scope = scope_rx.recv().expect("admitted");
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let waiter = {
        let sup = sup.clone();
        std::thread::spawn(move || {
            let result = sup.wait_scope_cleanup(scope);
            done_tx.send(result).expect("done");
        })
    };
    assert!(
        done_rx.recv_timeout(Duration::from_millis(80)).is_err(),
        "waiter must block until cleanup, not return UnknownScope"
    );
    release_tx.send(()).expect("release body");
    let report = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("cleanup observed");
    assert!(report.unwrap().all_verified());
    assert_eq!(body.join().expect("body").result.unwrap(), 7);
    waiter.join().expect("waiter");
}

#[test]
fn wait_scope_cleanup_sees_recovered_terminate_after_initial_failure() {
    let sup = BlockingSupervisor::builder()
        .backend(Arc::new(FailFirstCleanup::default()))
        .build()
        .unwrap();
    let result = sup.with_scope_options(Vec::new(), short_opts(), |_| ());
    assert!(
        result.termination.is_err(),
        "first cleanup must fail: {:?}",
        result.termination
    );
    assert!(sup
        .terminate_scope(result.scope, short_opts())
        .unwrap()
        .all_verified());
    assert!(
        sup.wait_scope_cleanup(result.scope).unwrap().all_verified(),
        "shared channel must publish the recovered success"
    );
}

#[test]
fn run_keeps_capture_while_cleanup_is_gated() {
    let hold = Arc::new(tokio::sync::Notify::new());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let backend = Arc::new(GatedCaptureCleanup {
        inner: NullBackend::new(),
        hold: hold.clone(),
        started: Mutex::new(Some(started_tx)),
        labels: Mutex::new(std::collections::HashMap::new()),
    });
    let sup = BlockingSupervisor::builder()
        .backend(backend)
        .build()
        .unwrap();
    let runner = {
        let sup = sup.clone();
        std::thread::spawn(move || {
            sup.run_with_options(capture_spec("run-a"), run_opts(Duration::from_secs(2)))
        })
    };
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("cleanup gated");
    for _ in 0..257 {
        let scope = sup.create_scope();
        let pid = sup
            .spawn(scope, capture_spec("other"))
            .expect("flood spawn");
        assert!(sup.wait(pid).unwrap().outcome.is_verified());
        assert!(sup
            .terminate_scope(scope, short_opts())
            .unwrap()
            .all_verified());
    }
    hold.notify_waiters();
    let run = runner.join().expect("run").expect("completed");
    assert!(!run.timed_out(), "{run:?}");
    assert!(run.all_verified(), "{:?}", run.termination());
    let snapshot = run.output().expect("run kept its capture");
    let bytes: Vec<u8> = snapshot
        .chunks
        .iter()
        .flat_map(|chunk| chunk.bytes.iter().copied())
        .collect();
    assert!(
        bytes
            .windows(RUN_A_BYTES.len())
            .any(|window| window == RUN_A_BYTES),
        "delayed cleanup must not lose the run's capture: {snapshot:?}"
    );
}

#[test]
fn from_handle_inside_localset_on_multi_thread() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let sup = BlockingSupervisor::from_handle_and_builder(
            tokio::runtime::Handle::current(),
            SupervisorBuilder::new().backend(Arc::new(NullBackend::new())),
        );
        tokio::task::spawn_local(async move {
            let scope = sup.create_scope();
            let pid = sup
                .spawn(scope, ProcessSpec::new("exit-immediately"))
                .expect("localset spawn");
            assert_eq!(
                sup.wait(pid).unwrap().outcome,
                TerminationOutcome::ExitedNaturally
            );
            let run = sup
                .run_with_options(
                    ProcessSpec::new("exit-immediately"),
                    run_opts(Duration::from_secs(2)),
                )
                .unwrap();
            assert!(run.all_verified(), "{:?}", run.termination());
            assert!(sup
                .terminate_scope(scope, short_opts())
                .unwrap()
                .all_verified());
        })
        .await
        .expect("spawn_local");
    });
}

#[derive(Default)]
struct FailWaitAfterSpawn {
    inner: NullBackend,
}

#[async_trait]
impl ProcessBackend for FailWaitAfterSpawn {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        self.inner.spawn(scope, spec).await
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn wait(&self, _target: &Spawned) -> Result<RawExit, WaitError> {
        Err(WaitError::Backend("injected wait failure".into()))
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, shepherd::StatsError> {
        self.inner.sample(target).await
    }
    fn capabilities(&self) -> shepherd::Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

#[derive(Default)]
struct TimeoutThenContainmentFail {
    inner: NullBackend,
}

#[async_trait]
impl ProcessBackend for TimeoutThenContainmentFail {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        self.inner.spawn(scope, spec).await
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn cleanup_scope(&self, _scope: ProcessScopeId) -> Result<(), TerminateError> {
        Err(TerminateError::Signal(
            "injected containment failure after timeout".into(),
        ))
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, shepherd::StatsError> {
        self.inner.sample(target).await
    }
    fn capabilities(&self) -> shepherd::Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

const RUN_A_BYTES: &[u8] = b"run-a-bytes";

fn capture_spec(program: &str) -> ProcessSpec {
    ProcessSpec::new(program).output(OutputMode::Capture {
        buffer_bytes: 64,
        tail_bytes: 16,
    })
}

struct FixedOutput(&'static [u8]);
impl OutputSink for FixedOutput {
    fn push(&self, _: OutputStream, _: &[u8]) {}
    fn close(&self, _: OutputStream, _: Option<String>) {}
    fn read(&self) -> OutputSnapshot {
        OutputSnapshot {
            chunks: vec![OutputChunk {
                stream: OutputStream::Stdout,
                bytes: self.0.to_vec(),
            }],
            tail: vec![],
            dropped_bytes: 0,
            stdout_closed: true,
            stderr_closed: true,
            errors: vec![],
        }
    }
}

#[derive(Default)]
struct FailFirstCleanup {
    inner: NullBackend,
    failed: AtomicBool,
}

#[async_trait]
impl ProcessBackend for FailFirstCleanup {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        self.inner.spawn(scope, spec).await
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn cleanup_scope(&self, scope: ProcessScopeId) -> Result<(), TerminateError> {
        if !self.failed.swap(true, Ordering::SeqCst) {
            return Err(TerminateError::Signal(
                "injected first cleanup failure".into(),
            ));
        }
        self.inner.cleanup_scope(scope).await
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, shepherd::StatsError> {
        self.inner.sample(target).await
    }
    fn capabilities(&self) -> shepherd::Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

struct GatedCaptureCleanup {
    inner: NullBackend,
    hold: Arc<tokio::sync::Notify>,
    started: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    labels: Mutex<std::collections::HashMap<u32, &'static [u8]>>,
}

#[async_trait]
impl ProcessBackend for GatedCaptureCleanup {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        let spawned = self.inner.spawn(scope, spec).await?;
        let label = if spec.program == "run-a" {
            RUN_A_BYTES
        } else {
            b"other"
        };
        self.labels
            .lock()
            .expect("labels")
            .insert(spawned.os.pid, label);
        Ok(spawned)
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn cleanup_scope(&self, scope: ProcessScopeId) -> Result<(), TerminateError> {
        let started = self.started.lock().expect("started").take();
        if let Some(started) = started {
            let _ = started.send(());
            self.hold.notified().await;
        }
        self.inner.cleanup_scope(scope).await
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, shepherd::StatsError> {
        self.inner.sample(target).await
    }
    fn output(&self, target: &Spawned) -> Option<ProcessOutput> {
        let label = self
            .labels
            .lock()
            .expect("labels")
            .get(&target.os.pid)
            .copied()
            .unwrap_or(b"other");
        Some(ProcessOutput(Arc::new(FixedOutput(label))))
    }
    fn capabilities(&self) -> shepherd::Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}

struct SlowSpawnBackend {
    inner: NullBackend,
    delay: Duration,
}

#[async_trait]
impl ProcessBackend for SlowSpawnBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        tokio::time::sleep(self.delay).await;
        self.inner.spawn(scope, spec).await
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        self.inner.wait(target).await
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, shepherd::StatsError> {
        self.inner.sample(target).await
    }
    fn capabilities(&self) -> shepherd::Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.inner.hard_kill_all();
    }
}
