//! The same observation API covers supervised and externally spawned roots.
use shepherd::{
    process_observer, ProcessObserver, ProcessSpec, ProcessTree, SupervisorBuilder,
    TerminateOptions,
};
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

struct StopFile(PathBuf);
impl StopFile {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
                "shepherd-observe-{label}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )))
    }
    fn stop(&self) {
        std::fs::write(&self.0, b"stop").unwrap();
    }
}
impl Drop for StopFile {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, b"stop");
    }
}
async fn chain(observer: &ProcessObserver, pid: u32) -> ProcessTree {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(tree) = observer.tree(pid).await {
                if tree.processes.len() == 3 {
                    assert_eq!(tree.root.os_pid, pid);
                    assert_eq!(tree.processes[1].parent_os_pid, Some(pid));
                    assert_eq!(
                        tree.processes[2].parent_os_pid,
                        Some(tree.processes[1].identity.os_pid)
                    );
                    assert!(tree.processes.iter().all(|p| !p.name.is_empty()));
                    return tree;
                }
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("root, child and grandchild must become visible")
}

#[tokio::test]
async fn external_tree_is_read_only_and_refreshable() {
    let stop = StopFile::new("external");
    let mut root = std::process::Command::new(env!("CARGO_BIN_EXE_observation_tree"))
        .arg(&stop.0)
        .arg("2")
        .spawn()
        .unwrap();
    let observer = process_observer();
    let first = chain(&observer, root.id()).await;
    let usage = observer.tree_usage(root.id()).await.unwrap();
    assert_eq!(usage.usage.entries.len(), 3);
    assert_eq!(usage.usage.totals(None).resident_bytes.contributors, 3);
    assert_eq!(
        usage.usage.totals(None).resident_bytes.value,
        Some(
            usage
                .usage
                .entries
                .iter()
                .map(|e| e.measurement.as_ref().unwrap().resident_bytes)
                .sum()
        )
    );
    let refreshed = observer.refresh_tree(first.root).await.unwrap();
    assert_eq!(first, refreshed);
    // Snapshot enumeration and tree filtering are independent of any supervisor.
    let snapshot = observer.snapshot().await.unwrap();
    assert!(snapshot
        .processes
        .iter()
        .any(|p| p.identity.os_pid == root.id()));
    drop(observer);
    assert!(
        root.try_wait().unwrap().is_none(),
        "observation must not kill or reap"
    );
    stop.stop();
    assert!(root.wait().unwrap().success());
    assert!(matches!(
        process_observer().refresh_tree(first.root).await,
        Err(shepherd::ObservationError::NotVisible(_))
            | Err(shepherd::ObservationError::IdentityChanged(_))
    ));
    // The marker remains until every fixture has exited, including on failure.
    drop(stop);
}

#[tokio::test]
async fn managed_tree_uses_the_same_os_pid_api() {
    let stop = StopFile::new("managed");
    let supervisor = SupervisorBuilder::new().build();
    let scope = supervisor.create_scope();
    assert_eq!(supervisor.os_pid(shepherd::ProcessId::new(u64::MAX)), None);
    let id = supervisor
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_observation_tree"))
                .arg(&stop.0)
                .arg("2"),
        )
        .await
        .unwrap();
    let observer = process_observer();
    let tree = chain(&observer, supervisor.os_pid(id).unwrap()).await;
    drop(observer);
    // The observer neither adopts descendants nor changes the owned-root registry.
    assert_eq!(supervisor.processes(scope).unwrap(), vec![id]);
    assert_eq!(tree.processes.len(), 3);
    stop.stop();
    tokio::time::timeout(Duration::from_secs(10), supervisor.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert!(supervisor
        .terminate_scope(scope, TerminateOptions::default())
        .await
        .unwrap()
        .all_verified());
    supervisor.shutdown().await.unwrap();
}
