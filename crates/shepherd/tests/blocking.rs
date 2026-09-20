//! Portable blocking-API contract tests over `NullBackend`.
//! These prove the synchronous surface keeps the ownership invariant without
//! requiring the caller to be inside Tokio, and that runtime nesting is handled.

#![cfg(feature = "blocking")]

use std::sync::Arc;
use std::time::Duration;

use shepherd::blocking::{BlockingSupervisor, BlockingSupervisorBuilder, RunOptions};
use shepherd::{
    GracePeriod, NullBackend, ProcessSpec, ScopeCreationError, SpawnError, SupervisorBuilder,
    TerminateOptions, TerminationOutcome,
};

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
    let exit = sup.wait(pid).unwrap();
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

#[test]
fn builder_debug_and_accessors() {
    let builder = BlockingSupervisorBuilder::new()
        .stats_interval(Duration::from_millis(250))
        .worker_threads(1);
    assert!(format!("{builder:?}").contains("BlockingSupervisorBuilder"));
    let sup = builder
        .backend(Arc::new(NullBackend::new()))
        .build()
        .unwrap();
    assert!(format!("{sup:?}").contains("BlockingSupervisor"));
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new("respect-graceful"))
        .unwrap();
    assert!(sup.os_pid(pid).is_some());
    assert!(sup.take_output(pid).is_none());
    assert!(sup.capabilities().force_termination);
    let _ = sup.stats(pid);
    let _ = sup.scope_usage(scope);
    assert!(sup
        .terminate_scope(scope, short_opts())
        .unwrap()
        .all_verified());
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
