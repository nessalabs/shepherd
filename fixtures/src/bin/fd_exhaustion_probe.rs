//! Run separately so RLIMIT_NOFILE cannot contaminate concurrent test processes.
#[cfg(unix)]
fn main() {
    struct Limit(libc::rlimit);
    impl Drop for Limit {
        fn drop(&mut self) {
            unsafe {
                libc::setrlimit(libc::RLIMIT_NOFILE, &self.0);
            }
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let sup = shepherd::SupervisorBuilder::new()
            .backend(std::sync::Arc::new(shepherd::UnixProcessBackend::new()))
            .build();
        let scope = sup.create_scope();
        let mut original = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut original) },
            0
        );
        let restore = Limit(original);
        let reduced = libc::rlimit {
            rlim_cur: original.rlim_cur.min(64),
            rlim_max: original.rlim_max,
        };
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &reduced) }, 0);
        let mut files = Vec::new();
        loop {
            match std::fs::File::open("/dev/null") {
                Ok(file) => files.push(file),
                Err(e) => {
                    assert_eq!(e.raw_os_error(), Some(libc::EMFILE));
                    break;
                }
            }
        }
        let program = std::env::args_os().nth(1).unwrap();
        assert!(sup
            .spawn(scope, shepherd::ProcessSpec::new(&program).arg("0"))
            .await
            .is_err());
        assert!(sup.processes(scope).unwrap().is_empty());
        drop(files);
        drop(restore);
        let pid = sup
            .spawn(scope, shepherd::ProcessSpec::new(program).arg("0"))
            .await
            .unwrap();
        let exit = sup.wait(pid).await.unwrap();
        assert_eq!(exit.code, Some(0));
        assert!(sup
            .terminate_scope(scope, shepherd::TerminateOptions::default())
            .await
            .unwrap()
            .all_verified());
        sup.shutdown().await.unwrap();
    });
}
#[cfg(not(unix))]
fn main() {}
