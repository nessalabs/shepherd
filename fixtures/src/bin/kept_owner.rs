//! An owner process for kept-process tests. It exits (or dies) while its kept process
//! lives on, so the test can check survival, listing and adoption from another owner.
//!
//! `kept_owner start <dir> <name> <log> <program> [args...]` starts a kept process,
//! prints its pid and exits normally.
//! `kept_owner crash <dir> <name> <program> [args...]` commits the record and then
//! exits before the gate opens, as a crash would.
#![forbid(unsafe_code)]

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::sync::Arc;

    use shepherd::{
        DirectoryKeptStore, KeptLauncher, KeptName, KeptRecord, KeptSpec, KeptStore, ProcessSpec,
        SpecFingerprint, UnixKeptLauncher,
    };

    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = &args[1];
    let name = KeptName::new(args[2].clone()).unwrap();
    match args[0].as_str() {
        "start" => {
            let kept = shepherd::kept_processes(dir, "kept_owner").unwrap();
            let spec = ProcessSpec::new(&args[4]).args(&args[5..]);
            let process = kept
                .start(KeptSpec::new(name, spec).log(&args[3]))
                .await
                .unwrap();
            println!("{}", process.os_pid());
            process.release();
        }
        "crash" => {
            let store = Arc::new(DirectoryKeptStore::open(dir).unwrap());
            let lock = store.lock(&name).unwrap().unwrap();
            let spec = ProcessSpec::new(&args[3]).args(&args[4..]);
            let fingerprint = SpecFingerprint::of(&spec, None);
            let commit = Box::new(move |launch: shepherd::KeptLaunch| {
                let record = KeptRecord {
                    name: name.clone(),
                    os_pid: launch.os_pid,
                    start_token: launch.start_token,
                    boot_id: None,
                    exe: None,
                    program: "crash".into(),
                    args: Vec::new(),
                    fingerprint,
                    revision: None,
                    graceful_signal: shepherd::Signal::Term,
                    log: None,
                    owner: "kept_owner".into(),
                    started_at_unix_ms: 0,
                };
                store.write(&lock, &record).unwrap();
                println!("{}", launch.os_pid);
                // Die with the record durable and the gate still closed.
                std::process::exit(0);
            });
            let _ = UnixKeptLauncher::new().launch(&spec, None, commit).await;
            unreachable!("the commit step exits the owner");
        }
        other => panic!("unknown mode {other}"),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn main() {}
