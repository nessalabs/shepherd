use shepherd::{GracePeriod, NullBackend, ProcessSpec, SupervisorBuilder, TerminateOptions};
use std::sync::Arc;
use std::time::Duration;
fn sup() -> shepherd::ProcessSupervisor {
    SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .build()
}
fn opts() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_millis(20)),
        force_timeout: Some(Duration::from_secs(2)),
    }
}
#[tokio::test]
async fn success_and_question_mark_error_preserve_result_and_cleanup() {
    let supervisor = sup();
    for fail in [false, true] {
        let result = supervisor
            .with_scope_options(
                vec![ProcessSpec::new("ignore-graceful")],
                opts(),
                |scope| async move {
                    assert_eq!(scope.processes().len(), 1);
                    if fail {
                        Err("closure error")?;
                    }
                    Ok::<_, &str>(42)
                },
            )
            .await;
        assert_eq!(
            result.result.unwrap(),
            if fail { Err("closure error") } else { Ok(42) }
        );
        assert!(result.termination.unwrap().all_verified());
    }
}
#[tokio::test]
async fn abort_closure_still_yields_verified_cleanup_report() {
    let supervisor = sup();
    let worker = supervisor.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        worker
            .with_scope_options(
                vec![ProcessSpec::new("ignore-graceful")],
                opts(),
                |scope| async move {
                    tx.send((scope.id(), scope.processes()[0])).unwrap();
                    std::future::pending::<()>().await;
                },
            )
            .await
    });
    let (scope, pid) = rx.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let report = tokio::time::timeout(Duration::from_secs(2), supervisor.wait_scope_cleanup(scope))
        .await
        .unwrap()
        .unwrap();
    assert!(report.all_verified());
    assert!(supervisor.wait(pid).await.unwrap().outcome.is_verified());
}
// Virtual time orders cancellation before grace expiry even on a stalled CI runner.
#[tokio::test(start_paused = true)]
async fn dropping_with_scope_future_cleans_up_but_dropping_terminate_does_not() {
    let supervisor = sup();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut block = Box::pin(supervisor.with_scope_options(
        vec![ProcessSpec::new("ignore-graceful")],
        opts(),
        |scope| async move {
            tx.send((scope.id(), scope.processes()[0])).unwrap();
            std::future::pending::<()>().await;
        },
    ));
    let (scope, pid) = tokio::select! { value = rx => value.unwrap(), _ = &mut block => panic!("closure must be pending") };
    // An in-flight terminate future sends grace, but canceling it must not force.
    let mut terminate = Box::pin(supervisor.terminate(pid, opts()));
    tokio::select! { _ = tokio::time::sleep(Duration::from_millis(5)) => {}, _ = &mut terminate => panic!("ignored grace") }
    drop(terminate);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), supervisor.wait(pid))
            .await
            .is_err()
    );
    drop(block);
    assert!(supervisor
        .wait_scope_cleanup(scope)
        .await
        .unwrap()
        .all_verified());
}
#[tokio::test]
async fn nested_blocks_are_independent_scopes() {
    let supervisor = sup();
    let borrowed = &supervisor;
    let outer = supervisor
        .with_scope_options(
            vec![ProcessSpec::new("ignore-graceful")],
            opts(),
            |outer| async move {
                let inner = borrowed
                    .with_scope_options(
                        vec![ProcessSpec::new("respect-graceful")],
                        opts(),
                        |inner| async move { inner.id() },
                    )
                    .await;
                assert_ne!(outer.id(), inner.result.unwrap());
                assert!(inner.termination.unwrap().all_verified());
                assert!(tokio::time::timeout(
                    Duration::from_millis(20),
                    borrowed.wait(outer.processes()[0])
                )
                .await
                .is_err());
            },
        )
        .await;
    assert!(outer.termination.unwrap().all_verified());
}

#[tokio::test]
async fn panic_and_nested_cancellation_still_clean_both_scopes() {
    let supervisor = sup();
    let worker = supervisor.clone();
    let nested = supervisor.clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let task = tokio::spawn(async move {
        worker
            .with_scope_options(
                vec![ProcessSpec::new("ignore-graceful")],
                opts(),
                |outer| async move {
                    tx.send(outer.id()).await.unwrap();
                    nested
                        .with_scope_options(
                            vec![ProcessSpec::new("ignore-graceful")],
                            opts(),
                            |inner| async move {
                                tx.send(inner.id()).await.unwrap();
                                std::future::pending::<()>().await;
                            },
                        )
                        .await;
                },
            )
            .await
    });
    let outer = rx.recv().await.unwrap();
    let inner = rx.recv().await.unwrap();
    task.abort();
    let _ = task.await;
    assert!(supervisor
        .wait_scope_cleanup(inner)
        .await
        .unwrap()
        .all_verified());
    assert!(supervisor
        .wait_scope_cleanup(outer)
        .await
        .unwrap()
        .all_verified());
    let worker = supervisor.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        worker
            .with_scope_options(
                vec![ProcessSpec::new("owned")],
                opts(),
                |scope| async move {
                    tx.send(scope.id()).unwrap();
                    panic!("injected closure panic");
                },
            )
            .await
    });
    let scope = rx.await.unwrap();
    assert!(task.await.unwrap_err().is_panic());
    assert!(supervisor
        .wait_scope_cleanup(scope)
        .await
        .unwrap()
        .all_verified());
}

#[test]
fn runtime_shutdown_reports_unverified_instead_of_hanging_on_cleanup() {
    let supervisor = sup();
    let worker = supervisor.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scope = runtime.block_on(async {
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            worker
                .with_scope_options(
                    vec![ProcessSpec::new("ignore-graceful")],
                    opts(),
                    |scope| async move {
                        tx.send(scope.id()).unwrap();
                        std::future::pending::<()>().await;
                    },
                )
                .await;
        });
        rx.await.unwrap()
    });
    drop(runtime);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        assert!(
            tokio::time::timeout(Duration::from_secs(1), supervisor.wait_scope_cleanup(scope))
                .await
                .unwrap()
                .is_err()
        );
    });
}
