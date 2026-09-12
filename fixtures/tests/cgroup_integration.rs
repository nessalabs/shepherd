//! Run explicitly in a delegated cgroup v2 environment. These tests fail closed.
#![cfg(target_os = "linux")]
use shepherd::{
    Containment, GracePeriod, ProcessSpec, SupervisorBuilder, TerminateOptions, UnixProcessBackend,
};
use std::{sync::Arc, time::Duration};

fn opts() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_millis(50)),
        force_timeout: Some(Duration::from_secs(5)),
    }
}
async fn detached(orphan: bool, drop_owner: bool) {
    // Adopt orphan descendants in this test process so the test can verify reap as well
    // as absence of live cgroup members. Never reap unrelated children with waitpid(-1).
    assert_eq!(
        unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) },
        0
    );
    let root = shepherd_test_support::TestEnvironment::from_env().cgroup_root();
    let backend = UnixProcessBackend::with_cgroup_root(&root)
        .expect("real cgroup v2 with cgroup.kill required");
    let sup = SupervisorBuilder::new().backend(Arc::new(backend)).build();
    assert_eq!(
        sup.capabilities().descendant_containment,
        Containment::CgroupV2
    );
    let a = sup.create_scope();
    let b = sup.create_scope();
    let bpid = sup
        .spawn(b, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .unwrap();
    let file = std::env::temp_dir().join(format!(
        "shepherd-cgroup-{}-{orphan}-{drop_owner}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&file);
    let mut spec = ProcessSpec::new(env!("CARGO_BIN_EXE_detached_child")).arg(file.as_os_str());
    if orphan {
        spec = spec.arg("orphan");
    }
    let pid = sup.spawn(a, spec).await.unwrap();
    let child: i32 = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&file) {
                if let Ok(pid) = text.parse() {
                    break pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if orphan {
        assert_eq!(sup.wait(pid).await.unwrap().code, Some(0));
    }
    if drop_owner {
        drop(sup);
    } else {
        assert!(sup.terminate_scope(a, opts()).await.unwrap().all_verified());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), sup.wait(bpid))
                .await
                .is_err()
        );
        assert!(sup.terminate_scope(b, opts()).await.unwrap().all_verified());
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut status = 0;
            let result = unsafe { libc::waitpid(child, &mut status, libc::WNOHANG) };
            if result == child {
                assert!(libc::WIFSIGNALED(status));
                break;
            }
            // Drop signals synchronously, but adoption follows the root's actual exit.
            // ECHILD before reparenting is transient; success still requires waitpid(child).
            if result < 0 {
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::ECHILD)
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("detached descendant survived cleanup");
    assert_eq!(unsafe { libc::kill(child, 0) }, -1);
    std::fs::remove_file(file).unwrap();
}
#[tokio::test]
#[ignore = "requires delegated cgroup v2; privileged CI runs --ignored and fails closed"]
async fn detached_scope_kill_and_isolation() {
    detached(false, false).await;
}
#[tokio::test]
#[ignore = "requires delegated cgroup v2; privileged CI runs --ignored and fails closed"]
async fn parent_exits_before_detached_descendant() {
    detached(true, false).await;
}
#[tokio::test]
#[ignore = "requires delegated cgroup v2; privileged CI runs --ignored and fails closed"]
async fn drop_last_handle_kills_detached_descendant() {
    detached(false, true).await;
}

#[test]
fn ordinary_directory_never_claims_cgroup_capability() {
    assert!(UnixProcessBackend::with_cgroup_root(std::env::temp_dir()).is_err());
    use shepherd::ProcessBackend;
    assert_eq!(
        UnixProcessBackend::new()
            .capabilities()
            .descendant_containment,
        Containment::ProcessGroup
    );
}

#[tokio::test]
#[ignore = "requires delegated cgroup v2; privileged CI runs --ignored and fails closed"]
async fn nested_cgroups_are_removed_after_kill_and_reap() {
    use shepherd::{ProcessBackend, ProcessScopeId, Signal};
    let delegated = shepherd_test_support::TestEnvironment::from_env().cgroup_root();
    let ancestor = delegated.join(format!("nested-test-{}", std::process::id()));
    std::fs::create_dir(&ancestor).unwrap();
    let backend = UnixProcessBackend::with_cgroup_root(&ancestor).unwrap();
    let root = std::fs::read_dir(&ancestor)
        .unwrap()
        .map(Result::unwrap)
        .find(|entry| entry.file_type().unwrap().is_dir())
        .unwrap()
        .path();
    let scope = ProcessScopeId::new(1);
    let child = backend
        .spawn(
            scope,
            &ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")),
        )
        .await
        .unwrap();
    let scope_path = root.join(format!("scope-{scope}"));
    let nested = scope_path.join("nested");
    let leaf = nested.join("leaf");
    std::fs::create_dir(&nested).unwrap();
    std::fs::create_dir(&leaf).unwrap();
    std::fs::create_dir(scope_path.join("empty-sibling")).unwrap();
    std::fs::write(leaf.join("cgroup.procs"), child.os.pid.to_string()).unwrap();
    assert!(std::fs::read_to_string(scope_path.join("cgroup.events"))
        .unwrap()
        .lines()
        .any(|line| line == "populated 1"));

    backend.cleanup_scope(scope).await.unwrap();
    let exit = tokio::time::timeout(Duration::from_secs(5), backend.wait(&child))
        .await
        .expect("nested member was not reaped")
        .unwrap();
    assert_eq!(exit.signal, Some(Signal::Kill));
    assert!(!scope_path.exists(), "nested cgroups leaked after cleanup");
    // Cleanup remains idempotent after the complete tree has been removed.
    backend.cleanup_scope(scope).await.unwrap();
    drop(backend);
    tokio::time::timeout(Duration::from_secs(5), async {
        while root.exists() {
            // The OS waiter may still be releasing its backend clone after publication.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("supervisor cgroup leaked after cleanup");
    std::fs::remove_dir(ancestor).unwrap();
}

#[tokio::test]
#[ignore = "requires delegated cgroup v2; privileged CI runs explicitly"]
async fn scope_accounting_reads_its_own_cgroup_and_fails_after_cleanup() {
    use shepherd::{AccountingSource, ScopeUsage};
    let root = shepherd_test_support::TestEnvironment::from_env().cgroup_root();
    let backend = UnixProcessBackend::with_cgroup_root(root).unwrap();
    let sup = SupervisorBuilder::new().backend(Arc::new(backend)).build();
    let one = sup.create_scope();
    let two = sup.create_scope();
    let first = sup
        .spawn(
            one,
            ProcessSpec::new(env!("CARGO_BIN_EXE_resource_workload")).arg("cpu"),
        )
        .await
        .unwrap();
    sup.spawn(two, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .unwrap();
    let ScopeUsage::Accounting(a) = sup.scope_usage(one).await.unwrap() else {
        panic!("cgroup accounting required");
    };
    assert_eq!(a.source, AccountingSource::LinuxCgroup);
    assert!(
        a.cpu_time.is_some(),
        "cpu.stat must be available on the privileged runner"
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    let ScopeUsage::Accounting(b) = sup.scope_usage(one).await.unwrap() else {
        unreachable!()
    };
    assert!(b.cpu_time.unwrap() > a.cpu_time.unwrap());
    assert_eq!(b.peak_commit_bytes, None);
    assert!(sup
        .terminate_scope(one, TerminateOptions::default())
        .await
        .unwrap()
        .all_verified());
    assert!(sup.wait(first).await.is_ok());
    assert!(sup.scope_usage(one).await.is_err());
    assert!(sup.scope_usage(two).await.is_ok());
    sup.shutdown().await.unwrap();
}
