#![cfg(unix)]
use shepherd::{
    GracePeriod, OutputMode, OutputSnapshot, OutputStream, ProcessOutput, ProcessSpec, Signal,
    SupervisorBuilder, TerminateOptions,
};
use std::time::Duration;

// Every consuming read is retained, including bytes arriving after root reap.
async fn finished_output(output: &ProcessOutput) -> Vec<OutputSnapshot> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut snapshots = Vec::new();
        loop {
            let snapshot = output.read();
            let closed = snapshot.stdout_closed && snapshot.stderr_closed;
            snapshots.push(snapshot);
            if closed {
                return snapshots;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("capture readers did not close after root reap")
}

#[tokio::test]
async fn binary_bytes_and_postmortem_tail_are_exact() {
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_output_flood"))
                .arg("binary")
                .output(OutputMode::Capture {
                    buffer_bytes: 64,
                    tail_bytes: 64,
                }),
        )
        .await
        .unwrap();
    assert_eq!(sup.wait(pid).await.unwrap().code, Some(0));
    let output = sup.take_output(pid).unwrap();
    assert!(sup.take_output(pid).is_none());
    let snapshots = finished_output(&output).await;
    let chunks: Vec<_> = snapshots
        .iter()
        .flat_map(|s| s.chunks.iter().cloned())
        .collect();
    let s = snapshots.last().unwrap();
    let stdout: Vec<_> = chunks
        .iter()
        .filter(|c| c.stream == OutputStream::Stdout)
        .flat_map(|c| c.bytes.clone())
        .collect();
    let stderr: Vec<_> = chunks
        .iter()
        .filter(|c| c.stream == OutputStream::Stderr)
        .flat_map(|c| c.bytes.clone())
        .collect();
    assert_eq!(stdout, [0, 255, 128, 10, 13, 0]);
    assert_eq!(stderr, [254, 0, 42]);
    assert_eq!(chunks, s.tail);
    assert_eq!(s.dropped_bytes, 0);
    assert!(s.stdout_closed && s.stderr_closed);
    assert!(s.errors.is_empty());
}

#[tokio::test]
async fn nonconsumer_flood_cannot_block_termination() {
    for mode in ["stdout", "stderr", "both"] {
        let sup = SupervisorBuilder::new().build();
        let scope = sup.create_scope();
        let pid = sup
            .spawn(
                scope,
                ProcessSpec::new(env!("CARGO_BIN_EXE_output_flood"))
                    .arg(mode)
                    .output(OutputMode::Capture {
                        buffer_bytes: 4096,
                        tail_bytes: 1024,
                    }),
            )
            .await
            .unwrap();
        let output = sup.take_output(pid).unwrap();
        // Readiness without consuming the queue: use a snapshot once, then allow a flood.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if output.read().dropped_bytes > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let report = tokio::time::timeout(
            Duration::from_secs(3),
            sup.terminate_scope(
                scope,
                TerminateOptions {
                    grace: GracePeriod::new(Duration::from_millis(50)),
                    force_timeout: Some(Duration::from_secs(2)),
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(report.all_verified());
        let snapshots = finished_output(&output).await;
        assert!(snapshots.last().unwrap().dropped_bytes > 0);
        for s in snapshots {
            assert!(s.chunks.iter().map(|c| c.bytes.len()).sum::<usize>() <= 4096);
            assert!(s.tail.iter().map(|c| c.bytes.len()).sum::<usize>() <= 1024);
        }
    }
}

#[tokio::test]
async fn discard_has_no_pipe_backpressure() {
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(scope, ProcessSpec::new(env!("CARGO_BIN_EXE_output_flood")))
        .await
        .unwrap();
    assert!(sup.take_output(pid).is_none());
    assert!(tokio::time::timeout(
        Duration::from_secs(3),
        sup.terminate_scope(scope, TerminateOptions::default())
    )
    .await
    .unwrap()
    .unwrap()
    .all_verified());
}

#[tokio::test]
async fn root_reap_is_verified_before_inherited_pipe_readers_finish() {
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    let pid = sup
        .spawn(
            scope,
            ProcessSpec::new(env!("CARGO_BIN_EXE_output_flood"))
                .arg("inherit-pipes")
                .output(OutputMode::Capture {
                    buffer_bytes: 64,
                    tail_bytes: 64,
                }),
        )
        .await
        .unwrap();
    let output = sup.take_output(pid).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if output
                .read()
                .tail
                .iter()
                .flat_map(|c| c.bytes.iter())
                .copied()
                .collect::<Vec<_>>()
                == b"ready"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("inherited-pipe descendant was not ready");
    let exit = sup
        .terminate(
            pid,
            TerminateOptions {
                grace: GracePeriod::new(Duration::from_millis(10)),
                force_timeout: Some(Duration::from_millis(150)),
            },
        )
        .await
        .unwrap();
    // Do not kill the descendant yet: both pipe readers must exhaust their own
    // bounded completion budget without changing the root's verified outcome.
    let snapshots = finished_output(&output).await;
    assert!(sup
        .terminate_scope(scope, TerminateOptions::default())
        .await
        .unwrap()
        .all_verified());
    assert!(
        exit.outcome.is_verified(),
        "root reap waited for output: {:?}",
        exit.outcome
    );
    assert_eq!(exit.signal, Some(Signal::Kill));
    let last = snapshots.last().unwrap();
    assert!(last.stdout_closed && last.stderr_closed);
    assert_eq!(
        last.errors.len(),
        2,
        "both inherited readers must be stopped explicitly"
    );
}
