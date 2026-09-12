#![cfg(unix)]
use shepherd::{
    GracePeriod, OutputMode, OutputStream, ProcessSpec, SupervisorBuilder, TerminateOptions,
};
use std::time::Duration;

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
    let s = output.read();
    let stdout: Vec<_> = s
        .chunks
        .iter()
        .filter(|c| c.stream == OutputStream::Stdout)
        .flat_map(|c| c.bytes.clone())
        .collect();
    let stderr: Vec<_> = s
        .chunks
        .iter()
        .filter(|c| c.stream == OutputStream::Stderr)
        .flat_map(|c| c.bytes.clone())
        .collect();
    assert_eq!(stdout, [0, 255, 128, 10, 13, 0]);
    assert_eq!(stderr, [254, 0, 42]);
    assert_eq!(s.chunks, s.tail);
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
        let s = output.read();
        assert!(s.dropped_bytes > 0);
        assert!(s.chunks.iter().map(|c| c.bytes.len()).sum::<usize>() <= 4096);
        assert!(s.tail.iter().map(|c| c.bytes.len()).sum::<usize>() <= 1024);
        assert!(s.stdout_closed && s.stderr_closed);
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
