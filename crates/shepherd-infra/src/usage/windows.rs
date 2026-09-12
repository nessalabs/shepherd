use super::*;
use shepherd_app::ProcessUsage;
use std::{
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    time::Duration,
};
use windows_sys::Win32::{
    Foundation::FILETIME,
    System::{ProcessStatus::*, Threading::*},
};
fn ticks(t: FILETIME) -> u64 {
    (u64::from(t.dwHighDateTime) << 32) | u64::from(t.dwLowDateTime)
}
pub(super) fn sample(identity: ObservedProcessIdentity) -> Result<ProcessUsage, UsageFailure> {
    // SAFETY: read-only handle with RAII close, all output structures initialized.
    unsafe {
        let raw = OpenProcess(
            PROCESS_QUERY_INFORMATION | PROCESS_VM_READ,
            0,
            identity.os_pid,
        );
        if raw.is_null() {
            return Err(unavailable(io::Error::last_os_error()));
        }
        let handle = OwnedHandle::from_raw_handle(raw);
        let (mut creation, mut exit, mut kernel, mut user) = (
            std::mem::zeroed(),
            std::mem::zeroed(),
            std::mem::zeroed(),
            std::mem::zeroed(),
        );
        if GetProcessTimes(
            handle.as_raw_handle(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        ) == 0
        {
            return Err(unavailable(io::Error::last_os_error()));
        }
        let start = ticks(creation);
        let epoch = (start / 10_000_000)
            .checked_sub(11_644_473_600)
            .ok_or_else(|| unavailable("invalid process creation time"))?;
        if identity.start_time_unix_seconds.is_some_and(|s| s != epoch) {
            return Err(UsageFailure::IdentityChanged);
        }
        let mut memory: PROCESS_MEMORY_COUNTERS = std::mem::zeroed();
        let size = std::mem::size_of_val(&memory) as u32;
        if GetProcessMemoryInfo(handle.as_raw_handle(), &mut memory, size) == 0 {
            return Err(unavailable(io::Error::last_os_error()));
        }
        let nanos = ticks(user)
            .checked_add(ticks(kernel))
            .and_then(|v| v.checked_mul(100))
            .ok_or_else(|| unavailable("CPU overflow"))?;
        Ok(ProcessUsage {
            identity,
            native_start: start,
            sampled_at: Instant::now(),
            cpu_time: Duration::from_nanos(nanos),
            resident_bytes: memory.WorkingSetSize as u64,
            physical_footprint_bytes: None,
            peak_physical_footprint_bytes: None,
            disk_read_bytes: None,
            disk_write_bytes: None,
        })
    }
}
