#![cfg(windows)]
use shepherd::{Containment, GracePeriod, ProcessSpec, SupervisorBuilder, TerminateOptions};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::time::Duration;
use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
use windows_sys::Win32::System::Threading::{
    OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
};
fn opts() -> TerminateOptions {
    TerminateOptions {
        grace: GracePeriod::new(Duration::from_millis(30)),
        force_timeout: Some(Duration::from_secs(5)),
    }
}
async fn tree(orphan: bool, drop_owner: bool) {
    let sup = SupervisorBuilder::new().build();
    assert_eq!(
        sup.capabilities().descendant_containment,
        Containment::JobObject
    );
    let a = sup.create_scope();
    let b = sup.create_scope();
    let bpid = sup
        .spawn(b, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .unwrap();
    let path = std::env::temp_dir().join(format!(
        "shepherd-job-{}-{orphan}-{drop_owner}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let mut spec = ProcessSpec::new(env!("CARGO_BIN_EXE_job_tree")).arg(path.as_os_str());
    if orphan {
        spec = spec.arg("orphan");
    }
    let pid = sup.spawn(a, spec).await.unwrap();
    let child = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&path) {
                if let Ok(pid) = text.parse::<u32>() {
                    break pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, child) };
    assert!(!handle.is_null());
    let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
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
        while unsafe { WaitForSingleObject(handle.as_raw_handle(), 0) } != WAIT_OBJECT_0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Job Object descendant survived cleanup");
    std::fs::remove_file(path).unwrap();
}
#[tokio::test]
async fn job_tree_kill_and_isolation() {
    tree(false, false).await;
}
#[tokio::test]
async fn job_contains_orphan() {
    tree(true, false).await;
}
#[tokio::test]
async fn last_owner_drop_kills_job_tree() {
    tree(false, true).await;
}
