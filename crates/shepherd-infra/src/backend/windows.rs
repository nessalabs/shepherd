//! Job Object containment. Roots remain suspended until assignment succeeds.
use async_trait::async_trait;
use shepherd_app::output::{OutputStream, ProcessOutput};
use shepherd_app::ports::{ProcessBackend, Spawned};
use shepherd_app::{SpawnError, StatsError, TerminateError, WaitError};
use shepherd_domain::{
    Capabilities, Containment, EnvPolicy, OsIdentity, OutputMode, ProcessScopeId, ProcessSpec,
    ProcessState, RawExit, RawStats, ReuseToken, Signal, Support,
};
use std::collections::HashMap;
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use windows_sys::Win32::Foundation::{FILETIME, HANDLE, INVALID_HANDLE_VALUE, STILL_ACTIVE};
use windows_sys::Win32::System::{
    Diagnostics::ToolHelp::*, JobObjects::*, ProcessStatus::*, Threading::*,
};

const KILLED: u32 = 0xe000_0001;
type Key = (u32, u64);
fn key(target: &Spawned) -> Key {
    (
        target.os.pid,
        match target.os.reuse_token {
            ReuseToken::StartTime(t) => t,
            ReuseToken::Unavailable => 0,
        },
    )
}
fn owned(raw: HANDLE) -> io::Result<OwnedHandle> {
    if raw.is_null() || raw == INVALID_HANDLE_VALUE {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedHandle::from_raw_handle(raw) })
    }
}
fn raw(h: &OwnedHandle) -> HANDLE {
    h.as_raw_handle()
}
fn bool_result(result: i32) -> io::Result<()> {
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
fn ft(t: FILETIME) -> u64 {
    (u64::from(t.dwHighDateTime) << 32) | u64::from(t.dwLowDateTime)
}
fn times(handle: HANDLE) -> io::Result<(u64, u64)> {
    let mut creation = unsafe { std::mem::zeroed() };
    let mut exit = creation;
    let mut kernel = creation;
    let mut user = creation;
    bool_result(unsafe {
        GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user)
    })?;
    Ok((ft(creation), ft(kernel).saturating_add(ft(user))))
}
fn new_job() -> io::Result<OwnedHandle> {
    let job = owned(unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) })?;
    let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    bool_result(unsafe {
        SetInformationJobObject(
            raw(&job),
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            std::mem::size_of_val(&limits) as u32,
        )
    })?;
    Ok(job)
}
fn active(job: &OwnedHandle) -> io::Result<u32> {
    let mut info: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { std::mem::zeroed() };
    bool_result(unsafe {
        QueryInformationJobObject(
            raw(job),
            JobObjectBasicAccountingInformation,
            (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
            std::mem::size_of_val(&info) as u32,
            std::ptr::null_mut(),
        )
    })?;
    Ok(info.ActiveProcesses)
}
fn resume(pid: u32) -> io::Result<()> {
    // The root's process handle is retained and its primary thread has never run.
    // Enumerating that process's suspended thread avoids reopening by an unowned PID.
    let snapshot = owned(unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) })?;
    let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of_val(&entry) as u32;
    let mut found = false;
    let mut ok = unsafe { Thread32First(raw(&snapshot), &mut entry) };
    while ok != 0 {
        if entry.th32OwnerProcessID == pid {
            let thread =
                owned(unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) })?;
            if unsafe { ResumeThread(raw(&thread)) } == u32::MAX {
                return Err(io::Error::last_os_error());
            }
            found = true;
        }
        ok = unsafe { Thread32Next(raw(&snapshot), &mut entry) };
    }
    if found {
        Ok(())
    } else {
        Err(io::Error::other("suspended primary thread not found"))
    }
}
struct Slot {
    scope: ProcessScopeId,
    process: Arc<OwnedHandle>,
    sampling: Arc<AtomicBool>,
    exit: watch::Sender<Option<Result<RawExit, String>>>,
    output: Option<ProcessOutput>,
    killed: Arc<AtomicBool>,
    previous: Option<(Instant, u64)>,
}
#[derive(Default)]
struct State {
    jobs: HashMap<ProcessScopeId, OwnedHandle>,
    children: HashMap<Key, Slot>,
}
/// One Job Object per scope, with kernel KILL_ON_JOB_CLOSE protection.
#[derive(Clone, Default)]
pub struct WindowsJobBackend {
    sampling: super::sampling::SamplingPool,
    state: Arc<Mutex<State>>,
}
impl WindowsJobBackend {
    fn sample_sync(
        &self,
        target: &Spawned,
        process: Arc<OwnedHandle>,
    ) -> Result<RawStats, StatsError> {
        // Retain the exact handle independently of registry removal. Native calls
        // execute without the job mutex, so sampling cannot block signals or reap.
        let (_, ticks) = times(raw(&process)).map_err(|e| StatsError::Backend(e.to_string()))?;
        let now = Instant::now();
        let mut memory: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
        let mut io: IO_COUNTERS = unsafe { std::mem::zeroed() };
        bool_result(unsafe {
            K32GetProcessMemoryInfo(
                raw(&process),
                &mut memory,
                std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
            )
        })
        .map_err(|e| StatsError::Backend(e.to_string()))?;
        bool_result(unsafe { GetProcessIoCounters(raw(&process), &mut io) })
            .map_err(|e| StatsError::Backend(e.to_string()))?;
        let mut state = self.state.lock().expect("job mutex");
        let slot = state
            .children
            .get_mut(&key(target))
            .ok_or_else(|| StatsError::Backend("process reaped during sample".into()))?;
        let cpu_usage = slot
            .previous
            .map(|(time, old)| {
                ticks.saturating_sub(old) as f64
                    / 1e7
                    / now.duration_since(time).as_secs_f64().max(1e-9)
            })
            .unwrap_or(0.0) as f32;
        slot.previous = Some((now, ticks));
        Ok(RawStats {
            cpu_usage,
            memory_rss_bytes: memory.WorkingSetSize as u64,
            virtual_memory_bytes: None,
            peak_rss_bytes: Some(memory.PeakWorkingSetSize as u64),
            io_read_bytes: Some(io.ReadTransferCount),
            io_write_bytes: Some(io.WriteTransferCount),
            descendant_count: None,
            state: ProcessState::Unknown,
        })
    }
    pub fn new() -> Self {
        Self::default()
    }
}
#[async_trait]
impl ProcessBackend for WindowsJobBackend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        let mut cmd = tokio::process::Command::new(&spec.program);
        cmd.args(&spec.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .creation_flags(CREATE_SUSPENDED)
            .kill_on_drop(false);
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        match &spec.env {
            EnvPolicy::Inherit => {}
            EnvPolicy::Clear(entries) => {
                cmd.env_clear();
                cmd.envs(entries.iter().cloned());
            }
            EnvPolicy::Overrides(entries) => {
                for (k, v) in entries {
                    if let Some(v) = v {
                        cmd.env(k, v);
                    } else {
                        cmd.env_remove(k);
                    }
                }
            }
        }
        let output = match spec.output {
            OutputMode::Discard => None,
            OutputMode::Capture {
                buffer_bytes,
                tail_bytes,
            } => {
                cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
                Some(crate::output::capture(buffer_bytes, tail_bytes))
            }
        };
        let mut child = cmd.spawn().map_err(|e| SpawnError::Os(e.to_string()))?;
        let setup = (|| -> io::Result<_> {
            let pid = child
                .id()
                .ok_or_else(|| io::Error::other("missing child pid"))?;
            let child_handle = child
                .raw_handle()
                .ok_or_else(|| io::Error::other("missing process handle"))?;
            // Open only while Tokio owns the unreaped process, then retain this exact handle.
            let process = owned(unsafe {
                OpenProcess(
                    PROCESS_QUERY_INFORMATION
                        | PROCESS_VM_READ
                        | PROCESS_TERMINATE
                        | PROCESS_SYNCHRONIZE,
                    0,
                    pid,
                )
            })?;
            let token = times(raw(&process))?.0;
            let spawned = Spawned {
                os: OsIdentity::new(pid, ReuseToken::StartTime(token)),
            };
            let mut state = self.state.lock().expect("job mutex");
            if let std::collections::hash_map::Entry::Vacant(entry) = state.jobs.entry(scope) {
                entry.insert(new_job()?);
            }
            bool_result(unsafe {
                AssignProcessToJobObject(raw(&state.jobs[&scope]), child_handle)
            })?;
            resume(pid)?;
            let (tx, _) = watch::channel(None);
            let killed = Arc::new(AtomicBool::new(false));
            state.children.insert(
                key(&spawned),
                Slot {
                    scope,
                    process: Arc::new(process),
                    sampling: Arc::default(),
                    exit: tx.clone(),
                    output: output.clone(),
                    killed: killed.clone(),
                    previous: None,
                },
            );
            Ok((spawned, tx, killed))
        })();
        let (spawned, tx, killed) = match setup {
            Ok(value) => value,
            Err(error) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(SpawnError::Os(error.to_string()));
            }
        };
        let mut readers = Vec::new();
        if let Some(out) = &output {
            if let Some(pipe) = child.stdout.take() {
                readers.push((
                    OutputStream::Stdout,
                    tokio::spawn(crate::output::drain(
                        pipe,
                        out.clone(),
                        OutputStream::Stdout,
                    )),
                ));
            }
            if let Some(pipe) = child.stderr.take() {
                readers.push((
                    OutputStream::Stderr,
                    tokio::spawn(crate::output::drain(
                        pipe,
                        out.clone(),
                        OutputStream::Stderr,
                    )),
                ));
            }
        }
        tokio::spawn(async move {
            let exit = child
                .wait()
                .await
                .map(|status| RawExit {
                    code: status.code(),
                    signal: (killed.load(Ordering::SeqCst) && status.code() == Some(KILLED as i32))
                        .then_some(Signal::Kill),
                    core_dumped: false,
                })
                .map_err(|e| e.to_string());
            // A descendant's inherited pipes cannot delay the root's verified exit.
            // This same waiter task retains the readers through bounded finishing.
            tx.send_replace(Some(exit));
            if let Some(output) = output {
                crate::output::finish_readers(readers, output).await;
            }
        });
        Ok(spawned)
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        if signal != Signal::Kill {
            return Err(TerminateError::Signal(
                "Windows has no general graceful process signal".into(),
            ));
        }
        let state = self.state.lock().expect("job mutex");
        let Some(slot) = state.children.get(&key(target)) else {
            return Ok(());
        };
        let mut code = 0;
        bool_result(unsafe { GetExitCodeProcess(raw(&slot.process), &mut code) })
            .map_err(|e| TerminateError::Signal(e.to_string()))?;
        if code != STILL_ACTIVE as u32 {
            return Ok(());
        }
        slot.killed.store(true, Ordering::SeqCst);
        bool_result(unsafe { TerminateProcess(raw(&slot.process), KILLED) })
            .map_err(|e| TerminateError::Signal(e.to_string()))
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        if signal != Signal::Kill {
            return Err(TerminateError::Signal(
                "Job Objects support force termination only".into(),
            ));
        }
        let state = self.state.lock().expect("job mutex");
        for slot in state.children.values().filter(|s| s.scope == scope) {
            slot.killed.store(true, Ordering::SeqCst);
        }
        if let Some(job) = state.jobs.get(&scope) {
            bool_result(unsafe { TerminateJobObject(raw(job), KILLED) })
                .map_err(|e| TerminateError::Signal(e.to_string()))?;
        }
        Ok(())
    }
    async fn cleanup_scope(&self, scope: ProcessScopeId) -> Result<(), TerminateError> {
        self.signal_scope(scope, Signal::Kill).await?;
        for _ in 0..500 {
            {
                let mut state = self.state.lock().expect("job mutex");
                let Some(job) = state.jobs.get(&scope) else {
                    return Ok(());
                };
                if active(job).map_err(|e| TerminateError::Signal(e.to_string()))? == 0 {
                    state.jobs.remove(&scope);
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(TerminateError::Signal(
            "Job Object still has active processes".into(),
        ))
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        let mut rx = self
            .state
            .lock()
            .expect("job mutex")
            .children
            .get(&key(target))
            .ok_or_else(|| WaitError::Backend("unknown process handle".into()))?
            .exit
            .subscribe();
        let result = loop {
            if let Some(exit) = rx.borrow_and_update().clone() {
                break exit;
            }
            rx.changed()
                .await
                .map_err(|_| WaitError::Backend("waiter closed".into()))?;
        };
        self.state
            .lock()
            .expect("job mutex")
            .children
            .remove(&key(target));
        result.map_err(WaitError::Backend)
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        let (process, active) = {
            let state = self.state.lock().expect("job mutex");
            let slot = state
                .children
                .get(&key(target))
                .ok_or_else(|| StatsError::Backend("process reaped".into()))?;
            (slot.process.clone(), slot.sampling.clone())
        };
        let backend = self.clone();
        let target = *target;
        self.sampling
            .run(active, move || backend.sample_sync(&target, process))
            .await
    }
    fn output(&self, target: &Spawned) -> Option<ProcessOutput> {
        self.state
            .lock()
            .expect("job mutex")
            .children
            .get(&key(target))
            .and_then(|s| s.output.clone())
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            descendant_containment: Containment::JobObject,
            cpu: Support::Supported,
            rss: Support::Supported,
            peak_rss: Support::Supported,
            io: Support::Supported,
            force_termination: true,
        }
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        let state = self.state.lock().expect("job mutex");
        for slot in state.children.values().filter(|s| s.scope == scope) {
            slot.killed.store(true, Ordering::SeqCst);
        }
        if let Some(job) = state.jobs.get(&scope) {
            let _ = unsafe { TerminateJobObject(raw(job), KILLED) };
        }
    }

    fn hard_kill_all(&self) {
        let state = self.state.lock().expect("job mutex");
        for slot in state.children.values() {
            slot.killed.store(true, Ordering::SeqCst);
        }
        for job in state.jobs.values() {
            if let Err(error) = bool_result(unsafe { TerminateJobObject(raw(job), KILLED) }) {
                tracing::error!(%error, "Job Object hard kill failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closing_last_job_handle_is_a_kernel_kill_backstop() {
        use std::os::windows::process::CommandExt;
        let job = new_job().unwrap();
        let mut child = std::process::Command::new("cmd.exe")
            .args(["/C", "ping -n 60 127.0.0.1 >NUL"])
            .creation_flags(CREATE_SUSPENDED)
            .spawn()
            .unwrap();
        bool_result(unsafe { AssignProcessToJobObject(raw(&job), child.as_raw_handle()) }).unwrap();
        assert_eq!(active(&job).unwrap(), 1);
        // Keep the root suspended: it cannot exit naturally or run any user code.
        assert_eq!(
            unsafe { WaitForSingleObject(child.as_raw_handle(), 20) },
            windows_sys::Win32::Foundation::WAIT_TIMEOUT
        );
        drop(job); // no TerminateJobObject: exercise KILL_ON_JOB_CLOSE itself.
        assert_eq!(
            unsafe { WaitForSingleObject(child.as_raw_handle(), 5000) },
            windows_sys::Win32::Foundation::WAIT_OBJECT_0
        );
        let _ = child.wait().unwrap(); // Job close does not promise a nonzero exit code.
    }
}
