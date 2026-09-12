#![cfg(any(unix, windows))]
use shepherd::{GracePeriod, ProcessSpec, SupervisorBuilder, TerminateOptions};
use std::time::Duration;
fn opts() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_millis(30)),
        force_timeout: Some(Duration::from_secs(3)),
    }
}
fn spec() -> ProcessSpec {
    ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever"))
}
#[tokio::test]
async fn real_scope_success_error_abort_and_partial_spawn_failure_reap() {
    let sup = SupervisorBuilder::new().build();
    for fail in [false, true] {
        let out = sup
            .with_scope_options(vec![spec()], opts(), |scope| async move {
                let pid = scope.processes()[0];
                if fail {
                    return Err(pid);
                }
                Ok(pid)
            })
            .await;
        let pid = out.result.unwrap().unwrap_or_else(|pid| pid);
        assert!(out.termination.unwrap().all_verified());
        assert!(sup.wait(pid).await.unwrap().outcome.is_verified());
    }
    let failure = sup
        .with_scope_options(
            vec![spec(), ProcessSpec::new("shepherd-no-such-program-184822")],
            opts(),
            |_| async { panic!("closure must not run after partial spawn failure") },
        )
        .await;
    assert!(failure.result.is_err());
    let report = failure.termination.unwrap();
    assert_eq!(report.outcomes.len(), 1);
    assert!(report.all_verified());
    let worker = sup.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        worker
            .with_scope_options(vec![spec()], opts(), |scope| async move {
                tx.send((scope.id(), scope.processes()[0])).unwrap();
                std::future::pending::<()>().await;
            })
            .await
    });
    let (scope, pid) = rx.await.unwrap();
    task.abort();
    let _ = task.await;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), sup.wait_scope_cleanup(scope))
            .await
            .unwrap()
            .unwrap()
            .all_verified()
    );
    assert!(sup.wait(pid).await.unwrap().outcome.is_verified());
}
