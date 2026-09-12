use shepherd::{ProcessSpec, StatsError, SupervisorBuilder, TerminateOptions};
use std::time::Duration;

#[tokio::test]
async fn cached_samples_and_isolation() {
    let sup = SupervisorBuilder::new()
        .stats_interval(Duration::from_secs(60))
        .build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_resource_workload")),
        )
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match sup.stats(pid).await {
                Ok(s) => break s,
                Err(StatsError::NotReady(_)) => tokio::task::yield_now().await,
                Err(e) => panic!("{e}"),
            }
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        sup.stats(pid).await.unwrap(),
        first,
        "stats must return the cached observation"
    );
    sup.terminate_scope(scope, TerminateOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        sup.stats(pid).await,
        Err(StatsError::UnknownProcess(_))
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn real_cpu_and_rss_move() {
    let sup = SupervisorBuilder::new()
        .stats_interval(Duration::from_millis(40))
        .build();
    if !sup.capabilities().cpu.is_supported() {
        assert!(!sup.capabilities().rss.is_supported());
        return;
    }
    let a = sup.create_scope();
    let b = sup.create_scope();
    let cpu = sup
        .spawn(
            a,
            ProcessSpec::new(env!("CARGO_BIN_EXE_resource_workload")).arg("cpu"),
        )
        .await
        .unwrap();
    let rss = sup
        .spawn(
            b,
            ProcessSpec::new(env!("CARGO_BIN_EXE_resource_workload")).arg("rss"),
        )
        .await
        .unwrap();
    let mut initial_rss = None;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let (Ok(c), Ok(r)) = (sup.stats(cpu).await, sup.stats(rss).await) {
                let initial = *initial_rss.get_or_insert(r.memory_rss_bytes);
                if c.cpu_usage > 0.05 && r.memory_rss_bytes > initial + 2 * 1024 * 1024 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .expect("real CPU and increasing RSS observations required");
    assert!(sup
        .terminate_scope(a, TerminateOptions::default())
        .await
        .unwrap()
        .all_verified());
    assert!(sup.stats(rss).await.is_ok());
    assert!(sup
        .terminate_scope(b, TerminateOptions::default())
        .await
        .unwrap()
        .all_verified());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn real_io_counters_increase() {
    let sup = SupervisorBuilder::new()
        .stats_interval(Duration::from_millis(40))
        .build();
    assert!(sup.capabilities().io.is_supported());
    let scope = sup.create_scope();
    let path = std::env::temp_dir().join(format!("shepherd-io-{}", std::process::id()));
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_resource_workload"))
                .arg("io")
                .arg(path.as_os_str()),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(s) = sup.stats(pid).await {
                if s.io_write_bytes.unwrap_or(0) > 65536 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .expect("real write-byte counter required");
    assert!(sup
        .terminate_scope(scope, TerminateOptions::default())
        .await
        .unwrap()
        .all_verified());
    std::fs::remove_file(path).unwrap();
}
