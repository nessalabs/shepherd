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
    let root = std::env::var_os("SHEPHERD_CGROUP_ROOT")
        .expect("set a writable delegated cgroup v2 ancestor");
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
            assert!(
                result >= 0,
                "detached child must remain reapable by subreaper"
            );
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
