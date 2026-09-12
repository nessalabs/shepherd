//! Bounded read-only native sampling. Never waits, signals, or adopts a process.
use shepherd_app::{ObservationError, UsageEntry, UsageFailure, UsageSelection, UsageSnapshot};
use shepherd_domain::ObservedProcessIdentity;
use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
mod windows;

pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, ObservationError> + Send + 'static,
) -> Result<T, ObservationError> {
    static PERMITS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    let permit = PERMITS
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(4)))
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| ObservationError::Backend(e.to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        f()
    })
    .await
    .map_err(|e| ObservationError::Backend(e.to_string()))?
}

pub(crate) fn collect(
    members: Vec<ObservedProcessIdentity>,
    selection: UsageSelection,
) -> UsageSnapshot {
    let started_at = Instant::now();
    let mut seen = std::collections::HashSet::new();
    let entries = members
        .into_iter()
        .filter(|m| seen.insert(m.os_pid))
        .map(|identity| {
            #[cfg(target_os = "macos")]
            let measurement = macos::sample(identity);
            #[cfg(target_os = "linux")]
            let measurement = linux::sample(identity);
            #[cfg(windows)]
            let measurement = windows::sample(identity);
            #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
            let measurement = Err(UsageFailure::Unsupported);
            UsageEntry {
                identity,
                measurement,
            }
        })
        .collect();
    UsageSnapshot {
        selection,
        started_at,
        finished_at: Instant::now(),
        entries,
    }
}

fn unavailable(error: impl std::fmt::Display) -> UsageFailure {
    UsageFailure::Unavailable(error.to_string())
}

#[cfg(target_os = "macos")]
pub(crate) fn group(pgid: i32) -> Result<UsageSnapshot, ObservationError> {
    macos::group(pgid)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn failed_members_are_preserved_and_duplicates_do_not_inflate_totals() {
        let missing = ObservedProcessIdentity {
            os_pid: u32::MAX,
            start_time_unix_seconds: Some(1),
        };
        let snapshot = blocking(move || Ok(collect(vec![missing, missing], UsageSelection::Tree)))
            .await
            .unwrap();
        assert_eq!(snapshot.entries.len(), 1);
        assert!(snapshot.entries[0].measurement.is_err());
        assert_eq!(snapshot.totals(None).resident_bytes.value, None);
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_keeps_native_work_permits_until_the_work_finishes() {
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut releases = Vec::new();
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            releases.push(tx);
            let started = started.clone();
            tasks.push(tokio::spawn(blocking(move || {
                started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = rx.recv();
                Ok(())
            })));
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(std::sync::atomic::Ordering::SeqCst) != 4 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        let mut fifth = Box::pin(blocking(|| Ok(())));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut fifth)
                .await
                .is_err()
        );
        drop(releases);
        tokio::time::timeout(std::time::Duration::from_secs(5), fifth)
            .await
            .unwrap()
            .unwrap();
    }
}
