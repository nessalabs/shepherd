//! Resource observations, separate from lifecycle ownership and cleanup verdicts.
use shepherd_domain::{ObservedProcessIdentity, ProcessTree};
use std::time::{Duration, Instant};

/// Why one process could not be measured. Missing never means zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageFailure {
    Unavailable(String),
    IdentityChanged,
    Unsupported,
}

/// One native measurement. CPU is cumulative per-process time, not a rate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessUsage {
    pub identity: ObservedProcessIdentity,
    /// Native start token where supported; only compare tokens from the same adapter.
    pub native_start: u64,
    pub sampled_at: Instant,
    pub cpu_time: Duration,
    pub resident_bytes: u64,
    pub physical_footprint_bytes: Option<u64>,
    pub peak_physical_footprint_bytes: Option<u64>,
    /// OS disk counters, not all application transfers. Zero may mean no accounting.
    pub disk_read_bytes: Option<u64>,
    pub disk_write_bytes: Option<u64>,
}

/// A member and its measurement or explicit failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageEntry {
    pub identity: ObservedProcessIdentity,
    pub measurement: Result<ProcessUsage, UsageFailure>,
}

/// How members were selected. Neither form is an atomic census.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageSelection {
    Tree,
    ProcessGroup,
}

/// A sum and the number of processes contributing to it. None means no data.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageTotal<T> {
    pub value: Option<T>,
    pub contributors: usize,
}

/// Current sampled totals. No sum of per-process peaks is exposed as a scope peak.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageTotals {
    pub resident_bytes: UsageTotal<u64>,
    pub physical_footprint_bytes: UsageTotal<u64>,
    pub cpu_cores: UsageTotal<f64>,
}

/// Owned results; retaining or dropping these never changes process ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSnapshot {
    pub selection: UsageSelection,
    pub started_at: Instant,
    pub finished_at: Instant,
    /// Visible selected members only; invisible members cannot be counted here.
    pub entries: Vec<UsageEntry>,
}
impl UsageSnapshot {
    /// Sum each identity once. CPU rates require matching native identities and
    /// increasing timestamps/counters. Newly visible members have no rate yet.
    pub fn totals(&self, previous: Option<&Self>) -> UsageTotals {
        let previous: std::collections::HashMap<_, _> = previous
            .filter(|p| p.selection == self.selection)
            .into_iter()
            .flat_map(|p| &p.entries)
            .filter_map(|e| e.measurement.as_ref().ok().map(|m| (e.identity, m)))
            .collect();
        let mut seen = std::collections::HashSet::new();
        let mut rss = Vec::new();
        let mut footprint = Vec::new();
        let mut cpu = Vec::new();
        for entry in &self.entries {
            if !seen.insert(entry.identity.os_pid) {
                continue;
            }
            let Ok(now) = &entry.measurement else {
                continue;
            };
            rss.push(now.resident_bytes);
            footprint.extend(now.physical_footprint_bytes);
            let old = previous.get(&entry.identity).copied();
            if let Some(old) = old.filter(|old| old.native_start == now.native_start) {
                if let (Some(elapsed), Some(work)) = (
                    now.sampled_at.checked_duration_since(old.sampled_at),
                    now.cpu_time.checked_sub(old.cpu_time),
                ) {
                    if !elapsed.is_zero() {
                        cpu.push(work.as_secs_f64() / elapsed.as_secs_f64());
                    }
                }
            }
        }
        fn sum(values: Vec<u64>) -> UsageTotal<u64> {
            UsageTotal {
                value: if values.is_empty() {
                    None
                } else {
                    values.iter().try_fold(0u64, |a, b| a.checked_add(*b))
                },
                contributors: values.len(),
            }
        }
        UsageTotals {
            resident_bytes: sum(rss),
            physical_footprint_bytes: sum(footprint),
            cpu_cores: UsageTotal {
                value: (!cpu.is_empty()).then(|| cpu.iter().sum()),
                contributors: cpu.len(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeUsage {
    pub tree: ProcessTree,
    pub usage: UsageSnapshot,
}

/// Kernel counters retain exited members where the OS accounts for them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeAccounting {
    pub source: AccountingSource,
    pub sampled_at: Instant,
    pub cpu_time: Option<Duration>,
    /// Linux cgroup memory.current; different from RSS and Windows commit.
    pub memory_bytes: Option<u64>,
    /// Windows peak job commit charge; never called resident memory.
    pub peak_commit_bytes: Option<u64>,
    pub io_read_bytes: Option<u64>,
    pub io_write_bytes: Option<u64>,
    /// Linux pids.current includes threads; Windows active processes does not.
    pub member_count: Option<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountingSource {
    LinuxCgroup,
    WindowsJob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeUsage {
    Sampled(UsageSnapshot),
    Accounting(ScopeAccounting),
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> UsageSnapshot {
        let at = Instant::now();
        let identity = ObservedProcessIdentity {
            os_pid: 1,
            start_time_unix_seconds: Some(1),
        };
        UsageSnapshot {
            selection: UsageSelection::Tree,
            started_at: at,
            finished_at: at,
            entries: vec![UsageEntry {
                identity,
                measurement: Ok(ProcessUsage {
                    identity,
                    native_start: 7,
                    sampled_at: at,
                    cpu_time: Duration::from_secs(2),
                    resident_bytes: 10,
                    physical_footprint_bytes: Some(15),
                    peak_physical_footprint_bytes: Some(30),
                    disk_read_bytes: None,
                    disk_write_bytes: None,
                }),
            }],
        }
    }
    #[test]
    fn totals_use_matching_intervals_deduplicate_and_keep_missing_distinct_from_zero() {
        let old = snapshot();
        let mut next = old.clone();
        let now = next.entries[0].measurement.as_mut().unwrap();
        now.sampled_at += Duration::from_secs(2);
        now.cpu_time += Duration::from_secs(3);
        next.entries.push(next.entries[0].clone());
        next.entries.push(UsageEntry {
            identity: ObservedProcessIdentity {
                os_pid: 2,
                start_time_unix_seconds: None,
            },
            measurement: Err(UsageFailure::Unsupported),
        });
        let totals = next.totals(Some(&old));
        assert_eq!(
            totals.cpu_cores,
            UsageTotal {
                value: Some(1.5),
                contributors: 1
            }
        );
        assert_eq!(
            totals.resident_bytes,
            UsageTotal {
                value: Some(10),
                contributors: 1
            }
        );
        assert_eq!(totals.physical_footprint_bytes.value, Some(15));
        assert_eq!(next.totals(None).cpu_cores.value, None);
        next.entries.clear();
        assert_eq!(next.totals(None).resident_bytes.value, None);
    }
    #[test]
    fn reuse_counter_reset_and_nonincreasing_time_never_produce_a_cpu_rate() {
        let old = snapshot();
        for case in 0..4 {
            let mut next = old.clone();
            let n = next.entries[0].measurement.as_mut().unwrap();
            match case {
                0 => n.native_start += 1,
                1 => n.cpu_time = Duration::ZERO,
                2 => n.sampled_at -= Duration::from_secs(1),
                _ => {}
            }
            assert_eq!(next.totals(Some(&old)).cpu_cores.value, None);
        }
        let mut next = old.clone();
        next.entries[0].measurement.as_mut().unwrap().sampled_at += Duration::from_secs(1);
        assert_eq!(next.totals(Some(&old)).cpu_cores.value, Some(0.0));
    }
}
