//! Run separately so RLIMIT_NOFILE cannot contaminate concurrent test processes.
#[cfg(unix)]
fn main() {
    use nix::sys::resource::{getrlimit, setrlimit, Resource};
    struct Limit((libc::rlim_t, libc::rlim_t));
    impl Drop for Limit {
        fn drop(&mut self) {
            let _ = setrlimit(Resource::RLIMIT_NOFILE, self.0 .0, self.0 .1);
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
        let original = getrlimit(Resource::RLIMIT_NOFILE).unwrap();
        let restore = Limit(original);
        setrlimit(Resource::RLIMIT_NOFILE, original.0.min(64), original.1).unwrap();
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
