use super::*;
use shepherd_app::ProcessUsage;
use std::{io, time::Duration};

// Allocate the largest requested record; older versions write its common prefix.
// libc declares the Darwin API as pointer-to-rusage_info_t, although the argument
// is the record's address, not an additional level of pointer indirection.
fn read_with(
    mut call: impl FnMut(i32, &mut libc::rusage_info_v4) -> io::Result<()>,
) -> io::Result<(libc::rusage_info_v4, i32)> {
    for flavor in [
        libc::RUSAGE_INFO_V4,
        libc::RUSAGE_INFO_V2,
        libc::RUSAGE_INFO_V0,
    ] {
        // SAFETY: this C record consists only of integer fields and byte arrays.
        let mut raw = unsafe { std::mem::zeroed() };
        match call(flavor, &mut raw) {
            Ok(()) => return Ok((raw, flavor)),
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) && flavor != libc::RUSAGE_INFO_V0 => {}
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
fn read(pid: i32) -> io::Result<(libc::rusage_info_v4, i32)> {
    read_with(|flavor, raw| {
        // SAFETY: correctly aligned V4 storage accommodates every requested flavor.
        let rc = unsafe {
            libc::proc_pid_rusage(pid, flavor, (raw as *mut libc::rusage_info_v4).cast())
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    })
}
fn nanos(user: u64, system: u64, numer: u32, denom: u32) -> Result<u64, UsageFailure> {
    if denom == 0 {
        return Err(unavailable("invalid Mach timebase"));
    }
    u64::try_from((u128::from(user) + u128::from(system)) * u128::from(numer) / u128::from(denom))
        .map_err(unavailable)
}

pub(super) fn sample(identity: ObservedProcessIdentity) -> Result<ProcessUsage, UsageFailure> {
    let pid = i32::try_from(identity.os_pid).map_err(unavailable)?;
    if pid <= 0 {
        return Err(unavailable("invalid PID"));
    }
    let before =
        libproc::proc_pid::pidinfo::<libproc::bsd_info::BSDInfo>(pid, 0).map_err(unavailable)?;
    if identity
        .start_time_unix_seconds
        .is_some_and(|s| s != before.pbi_start_tvsec)
    {
        return Err(UsageFailure::IdentityChanged);
    }
    let (raw, flavor) = read(pid).map_err(unavailable)?;
    let sampled_at = Instant::now();
    let after =
        libproc::proc_pid::pidinfo::<libproc::bsd_info::BSDInfo>(pid, 0).map_err(unavailable)?;
    if (before.pbi_start_tvsec, before.pbi_start_tvusec)
        != (after.pbi_start_tvsec, after.pbi_start_tvusec)
    {
        return Err(UsageFailure::IdentityChanged);
    }
    let mut tb = mach2::mach_time::mach_timebase_info_data_t { numer: 0, denom: 0 };
    // SAFETY: valid writable timebase record.
    if unsafe { mach2::mach_time::mach_timebase_info(&mut tb) } != 0 {
        return Err(unavailable("Mach timebase query failed"));
    }
    Ok(ProcessUsage {
        identity,
        native_start: raw.ri_proc_start_abstime,
        sampled_at,
        cpu_time: Duration::from_nanos(nanos(
            raw.ri_user_time,
            raw.ri_system_time,
            tb.numer,
            tb.denom,
        )?),
        resident_bytes: raw.ri_resident_size,
        physical_footprint_bytes: Some(raw.ri_phys_footprint),
        peak_physical_footprint_bytes: (flavor >= libc::RUSAGE_INFO_V4)
            .then_some(raw.ri_lifetime_max_phys_footprint),
        disk_read_bytes: (flavor >= libc::RUSAGE_INFO_V2).then_some(raw.ri_diskio_bytesread),
        disk_write_bytes: (flavor >= libc::RUSAGE_INFO_V2).then_some(raw.ri_diskio_byteswritten),
    })
}
pub(super) fn group(pgid: i32) -> Result<UsageSnapshot, ObservationError> {
    // Grow on a full buffer, with a hard cap. Never silently truncate membership.
    let mut capacity = 64usize;
    let pids = loop {
        let mut pids = vec![0i32; capacity];
        // SAFETY: writable PID array and its exact byte capacity.
        let count = unsafe {
            libc::proc_listpgrppids(pgid, pids.as_mut_ptr().cast(), (capacity * 4) as i32)
        };
        if count < 0 {
            return Err(ObservationError::Backend(
                io::Error::last_os_error().to_string(),
            ));
        }
        // Unlike proc_listpids, this wrapper returns a PID count, not bytes.
        if (count as usize) < capacity {
            pids.truncate(count as usize);
            break pids;
        }
        capacity *= 2;
        if capacity > 1_048_576 {
            return Err(ObservationError::Backend(
                "PID list exceeds sampling bound".into(),
            ));
        }
    };
    let members = pids
        .into_iter()
        .filter(|p| *p > 0 && *p != pgid)
        .map(|p| ObservedProcessIdentity {
            os_pid: p as u32,
            start_time_unix_seconds: None,
        })
        .collect();
    let mut result = collect(members, UsageSelection::ProcessGroup);
    for entry in &mut result.entries {
        // SAFETY: getpgid is a read-only query. Exit/group changes invalidate this sample.
        if unsafe { libc::getpgid(entry.identity.os_pid as i32) } != pgid {
            entry.measurement = Err(UsageFailure::IdentityChanged);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn flavor_fallback_is_only_for_invalid_version_and_clears_storage() {
        let mut calls = Vec::new();
        let (r, v) = read_with(|v, r| {
            calls.push(v);
            assert_eq!(r.ri_user_time, 0);
            r.ri_user_time = 7;
            if v == 0 {
                Ok(())
            } else {
                Err(io::Error::from_raw_os_error(libc::EINVAL))
            }
        })
        .unwrap();
        assert_eq!(calls, vec![4, 2, 0]);
        assert_eq!(v, 0);
        assert_eq!(r.ri_user_time, 7);
        for errno in [libc::EPERM, libc::ESRCH] {
            let mut calls = 0;
            let e = read_with(|_, _| {
                calls += 1;
                Err(io::Error::from_raw_os_error(errno))
            })
            .err()
            .unwrap();
            assert_eq!(calls, 1);
            assert_eq!(e.raw_os_error(), Some(errno));
        }
    }
    #[test]
    fn cpu_conversion_includes_both_counters_without_overflow() {
        assert_eq!(nanos(6, 6, 125, 3).unwrap(), 500);
        assert_eq!(nanos(4, 7, 1, 1).unwrap(), 11);
        assert!(nanos(1, 1, 1, 0).is_err());
        assert!(nanos(u64::MAX, u64::MAX, 125, 3).is_err());
    }
}

#[cfg(test)]
mod native_tests {
    use super::*;
    #[test]
    fn cpu_units_match_getrusage_on_this_architecture() {
        let identity = ObservedProcessIdentity {
            os_pid: std::process::id(),
            start_time_unix_seconds: None,
        };
        let first = sample(identity).unwrap();
        // SAFETY: initialized rusage output record for the calling process.
        let mut before: libc::rusage = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut before) },
            0
        );
        let deadline = Instant::now() + Duration::from_millis(250);
        let mut x = 1u64;
        while Instant::now() < deadline {
            x = std::hint::black_box(x.wrapping_mul(1664525).wrapping_add(1013904223));
        }
        let mut after: libc::rusage = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut after) }, 0);
        let last = sample(identity).unwrap();
        let seconds = |r: libc::rusage| {
            (r.ru_utime.tv_sec + r.ru_stime.tv_sec) as f64
                + (r.ru_utime.tv_usec + r.ru_stime.tv_usec) as f64 / 1e6
        };
        let expected = seconds(after) - seconds(before);
        let actual = (last.cpu_time - first.cpu_time).as_secs_f64();
        assert!(expected > 0.01);
        assert!(
            (actual - expected).abs() < 0.05 + expected * 0.15,
            "Mach unit mismatch: expected {expected}, got {actual}"
        );
    }
    #[test]
    fn footprint_tracks_touched_memory_and_identity_mismatch_fails() {
        let identity = ObservedProcessIdentity {
            os_pid: std::process::id(),
            start_time_unix_seconds: None,
        };
        let before = sample(identity).unwrap();
        let memory = vec![std::hint::black_box(42u8); 32 * 1024 * 1024];
        std::hint::black_box(&memory);
        let after = sample(identity).unwrap();
        assert!(
            after.physical_footprint_bytes.unwrap()
                > before.physical_footprint_bytes.unwrap() + 16 * 1024 * 1024
        );
        assert!(sample(ObservedProcessIdentity {
            start_time_unix_seconds: Some(1),
            ..identity
        })
        .is_err());
        assert!(sample(ObservedProcessIdentity {
            os_pid: 0,
            ..identity
        })
        .is_err());
        drop(memory);
    }
}
