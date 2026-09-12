//! Launch boundaries that mocks cannot verify: argv, environment, cwd and real OS errors.
use shepherd::{EnvPolicy, ProcessSpec, SupervisorBuilder, TerminateOptions};
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[tokio::test]
async fn launch_preserves_unicode_empty_arguments_and_recovers_from_bad_cwd() {
    let dir = Directory(std::env::temp_dir().join(format!(
            "shepherd space 日本語-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    std::fs::create_dir(&dir.0).unwrap();
    let sup = SupervisorBuilder::new().build();
    let scope = sup.create_scope();
    for _ in 0..3 {
        assert!(sup
            .spawn(
                scope,
                ProcessSpec::new(env!("CARGO_BIN_EXE_launch_probe")).cwd(dir.0.join("missing"))
            )
            .await
            .is_err());
        assert!(sup.processes(scope).unwrap().is_empty());
    }
    for clear in [false, true] {
        let values = vec![
            ("SHEPHERD_PROBE_VALUE".into(), "value with spaces 🐑".into()),
            ("SHEPHERD_PROBE_CWD".into(), dir.0.as_os_str().to_owned()),
        ];
        let env = if clear {
            EnvPolicy::Clear(values)
        } else {
            let mut overrides: Vec<_> = values.into_iter().map(|(k, v)| (k, Some(v))).collect();
            overrides.push(("SHEPHERD_PROBE_ABSENT".into(), None));
            EnvPolicy::Overrides(overrides)
        };
        let pid = sup
            .spawn(
                scope,
                ProcessSpec::new(env!("CARGO_BIN_EXE_launch_probe"))
                    .args([
                        "",
                        "two words",
                        "quote\"inside",
                        "back\\slash\\",
                        "日本語 🐑",
                    ])
                    .env(env)
                    .cwd(&dir.0),
            )
            .await
            .unwrap();
        let exit = tokio::time::timeout(Duration::from_secs(10), sup.wait(pid))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            exit.code,
            Some(0),
            "child-side launch assertions failed: {exit:?}"
        );
    }
    assert!(sup
        .terminate_scope(scope, TerminateOptions::default())
        .await
        .unwrap()
        .all_verified());
    sup.shutdown().await.unwrap();
}

#[cfg(unix)]
#[test]
fn real_descriptor_exhaustion_is_recoverable_in_an_isolated_process() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_fd_exhaustion_probe"))
        .arg(env!("CARGO_BIN_EXE_exit_code"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fd probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
