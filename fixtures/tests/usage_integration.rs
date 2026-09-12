#[cfg(any(target_os = "macos", windows))]
use shepherd::ScopeUsage;
// Real-process regressions for read-only resource observations.
use shepherd::{process_observer, ProcessSpec, SupervisorBuilder, TerminateOptions};
use std::{
    process::{Child, Command},
    time::Duration,
};

struct External(Child);
impl Drop for External {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn external_cpu_observation_has_no_ownership_and_reports_rates() {
    let mut child = External(
        Command::new(env!("CARGO_BIN_EXE_resource_workload"))
            .arg("cpu")
            .spawn()
            .unwrap(),
    );
    let observer = process_observer();
    let first = observer.tree_usage(child.0.id()).await.unwrap();
    assert_eq!(first.usage.totals(None).cpu_cores.value, None);
    let mut latest = first.clone();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            latest = observer.refresh_tree_usage(&first).await.unwrap();
            if latest
                .usage
                .totals(Some(&first.usage))
                .cpu_cores
                .value
                .is_some_and(|v| v > 0.01)
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    let totals = latest.usage.totals(Some(&first.usage));
    assert_eq!(totals.resident_bytes.contributors, 1);
    assert!(totals.resident_bytes.value.unwrap() > 0);
    #[cfg(target_os = "macos")]
    {
        let m = latest.usage.entries[0].measurement.as_ref().unwrap();
        assert!(m.physical_footprint_bytes.unwrap() > 0);
        assert!(m.peak_physical_footprint_bytes.unwrap() >= m.physical_footprint_bytes.unwrap());
        assert!(m.disk_read_bytes.is_some());
    }
    drop(observer);
    assert!(child.0.try_wait().unwrap().is_none());
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    assert!(process_observer()
        .refresh_tree_usage(&latest)
        .await
        .is_err());
}

#[tokio::test]
async fn managed_root_uses_identical_read_only_api_and_cleanup_survives_sampling() {
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .unwrap();
    let result = process_observer()
        .tree_usage(sup.os_pid(pid).unwrap())
        .await
        .unwrap();
    assert_eq!(result.usage.entries.len(), 1);
    assert!(result.usage.entries[0].measurement.is_ok());
    let (_, cleanup) = tokio::join!(
        sup.scope_usage(scope),
        sup.terminate_scope(scope, TerminateOptions::default())
    );
    assert!(cleanup.unwrap().all_verified());
    sup.shutdown().await.unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn macos_scope_excludes_anchor_and_other_scopes() {
    let sup = SupervisorBuilder::new().build();
    let one = sup.create_scope();
    let two = sup.create_scope();
    let a = sup
        .spawn(one, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .unwrap();
    let b = sup
        .spawn(two, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .unwrap();
    let ScopeUsage::Sampled(usage) = sup.scope_usage(one).await.unwrap() else {
        panic!("expected sampled scope");
    };
    assert_eq!(
        usage.entries.len(),
        1,
        "private anchor and other scope must be excluded"
    );
    assert_eq!(usage.entries[0].identity.os_pid, sup.os_pid(a).unwrap());
    assert_ne!(usage.entries[0].identity.os_pid, sup.os_pid(b).unwrap());
    assert!(usage.entries[0].measurement.is_ok());
    assert!(sup
        .terminate_scope(one, TerminateOptions::default())
        .await
        .unwrap()
        .all_verified());
    assert!(sup.scope_usage(one).await.is_err());
    assert!(sup.scope_usage(two).await.is_ok());
    sup.shutdown().await.unwrap();
}

#[cfg(windows)]
#[tokio::test]
async fn windows_job_accounting_is_named_and_scope_local() {
    let sup = SupervisorBuilder::new().build();
    let one = sup.create_scope();
    let two = sup.create_scope();
    for scope in [one, two] {
        sup.spawn(scope, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
            .await
            .unwrap();
    }
    let ScopeUsage::Accounting(usage) = sup.scope_usage(one).await.unwrap() else {
        panic!("expected job accounting");
    };
    assert_eq!(usage.source, shepherd::AccountingSource::WindowsJob);
    assert_eq!(usage.member_count, Some(1));
    assert!(usage.cpu_time.is_some());
    assert!(usage.peak_commit_bytes.is_some());
    assert_eq!(usage.memory_bytes, None);
    sup.shutdown().await.unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn process_group_fallback_does_not_invent_kernel_counters() {
    let sup = SupervisorBuilder::new()
        .backend(std::sync::Arc::new(shepherd::UnixProcessBackend::new()))
        .build();
    let scope = sup.create_scope();
    sup.spawn(scope, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
        .await
        .unwrap();
    assert!(matches!(
        sup.scope_usage(scope).await,
        Err(shepherd::ObservationError::Unsupported)
    ));
    sup.shutdown().await.unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn macos_group_keeps_orphans_but_excludes_detached_children() {
    for mode in ["orphan", "detach"] {
        let marker = std::env::temp_dir().join(format!(
            "shepherd-usage-group-{}-{mode}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&marker);
        let sup = SupervisorBuilder::new().build();
        let scope = sup.create_scope();
        let root = sup
            .spawn(
                scope,
                ProcessSpec::new(env!("CARGO_BIN_EXE_usage_group"))
                    .arg(&marker)
                    .arg(mode),
            )
            .await
            .unwrap();
        let child = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(text) = std::fs::read_to_string(&marker) {
                    if let Ok(pid) = text.parse::<u32>() {
                        break pid;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if mode == "orphan" {
            sup.wait(root).await.unwrap();
        }
        let ScopeUsage::Sampled(usage) = sup.scope_usage(scope).await.unwrap() else {
            panic!("sampled");
        };
        assert_eq!(
            usage.entries.iter().any(|e| e.identity.os_pid == child),
            mode == "orphan"
        );
        if mode == "detach" {
            let tree = process_observer()
                .tree_usage(sup.os_pid(root).unwrap())
                .await
                .unwrap();
            assert!(tree
                .usage
                .entries
                .iter()
                .any(|e| e.identity.os_pid == child));
            // Let the original parent reap its detached child. Fixture is bounded.
            tokio::time::timeout(Duration::from_secs(20), sup.wait(root))
                .await
                .unwrap()
                .unwrap();
        }
        assert!(sup
            .terminate_scope(scope, TerminateOptions::default())
            .await
            .unwrap()
            .all_verified());
        sup.shutdown().await.unwrap();
        std::fs::remove_file(marker).unwrap();
    }
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn macos_group_enumeration_grows_past_its_initial_buffer() {
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    for _ in 0..65 {
        sup.spawn(scope, ProcessSpec::new(env!("CARGO_BIN_EXE_sleep_forever")))
            .await
            .unwrap();
    }
    let ScopeUsage::Sampled(usage) = sup.scope_usage(scope).await.unwrap() else {
        panic!("sampled");
    };
    assert_eq!(usage.entries.len(), 65);
    assert_eq!(usage.totals(None).resident_bytes.contributors, 65);
    sup.shutdown().await.unwrap();
}
