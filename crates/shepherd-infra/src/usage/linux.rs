use super::*;
use shepherd_app::ProcessUsage;
use std::{fs, time::Duration};

fn stat(pid: u32) -> Result<(u64, u64), UsageFailure> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).map_err(unavailable)?;
    let fields: Vec<_> = text
        .rsplit_once(')')
        .ok_or_else(|| unavailable("invalid stat"))?
        .1
        .split_whitespace()
        .collect();
    let number = |index: usize| {
        fields
            .get(index)
            .ok_or_else(|| unavailable("short stat"))?
            .parse::<u64>()
            .map_err(unavailable)
    };
    Ok((
        number(19)?,
        number(11)?
            .checked_add(number(12)?)
            .ok_or_else(|| unavailable("CPU overflow"))?,
    ))
}
pub(super) fn sample(identity: ObservedProcessIdentity) -> Result<ProcessUsage, UsageFailure> {
    let pid = identity.os_pid;
    let (start, ticks) = stat(pid)?;
    let status = fs::read_to_string(format!("/proc/{pid}/status")).map_err(unavailable)?;
    let rss = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|s| s.split_whitespace().next())
        .ok_or_else(|| unavailable("resident memory unavailable"))?
        .parse::<u64>()
        .map_err(unavailable)?
        .checked_mul(1024)
        .ok_or_else(|| unavailable("RSS overflow"))?;
    // Identity epoch uses the same second-resolution convention as discovery.
    let boot = fs::read_to_string("/proc/stat")
        .map_err(unavailable)?
        .lines()
        .find_map(|l| l.strip_prefix("btime "))
        .ok_or_else(|| unavailable("boot time unavailable"))?
        .parse::<u64>()
        .map_err(unavailable)?;
    // SAFETY: read-only clock tick frequency query.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if hz <= 0 {
        return Err(unavailable("invalid clock frequency"));
    }
    if identity
        .start_time_unix_seconds
        .is_some_and(|s| s != boot + start / hz as u64)
        || stat(pid)?.0 != start
    {
        return Err(UsageFailure::IdentityChanged);
    }
    Ok(ProcessUsage {
        identity,
        native_start: start,
        sampled_at: Instant::now(),
        cpu_time: Duration::from_secs_f64(ticks as f64 / hz as f64),
        resident_bytes: rss,
        physical_footprint_bytes: None,
        peak_physical_footprint_bytes: None,
        disk_read_bytes: None,
        disk_write_bytes: None,
    })
}
