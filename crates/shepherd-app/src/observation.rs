//! Read-only process discovery. This service never attaches processes to the registry.
use async_trait::async_trait;
use shepherd_domain::{ObservedProcessIdentity, ProcessSnapshot, ProcessTree};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

/// Failure to collect or select an observation.
#[derive(Debug, thiserror::Error)]
pub enum ObservationError {
    /// This target has no observation adapter.
    #[error("process observation is unsupported on this platform")]
    Unsupported,
    /// Missing may mean exited, inaccessible, or not present; never proof of exit.
    #[error("process {0} is not visible in the snapshot")]
    NotVisible(u32),
    /// A previously observed PID now has a different reported start time.
    #[error("process {0} has a different observed identity")]
    IdentityChanged(u32),
    /// Native observation failed.
    #[error("process observation failed: {0}")]
    Backend(String),
}

/// Read-only infrastructure port; implementations must not signal, wait or adopt processes.
#[async_trait]
pub trait ProcessObservationBackend: Send + Sync {
    /// Collect visible processes. Snapshots are always best-effort and non-atomic.
    async fn snapshot(&self) -> Result<ProcessSnapshot, ObservationError>;
}

/// A reusable observer for both managed and externally started OS processes.
/// Independent of supervisor ownership: dropping it never terminates a process.
#[derive(Clone)]
pub struct ProcessObserver {
    backend: Arc<dyn ProcessObservationBackend>,
}
impl ProcessObserver {
    /// Construct with a read-only platform adapter.
    pub fn new(backend: Arc<dyn ProcessObservationBackend>) -> Self {
        Self { backend }
    }
    /// Enumerate all processes visible to the current user.
    pub async fn snapshot(&self) -> Result<ProcessSnapshot, ObservationError> {
        self.backend.snapshot().await
    }
    /// Observe a root selected by OS PID, without acquiring ownership.
    pub async fn tree(&self, os_pid: u32) -> Result<ProcessTree, ObservationError> {
        select_tree(self.snapshot().await?, os_pid, None)
    }
    /// Refresh a tree, rejecting a changed reported start time. Second-resolution or
    /// missing start times cannot rule out all PID reuse; never use this for signaling.
    pub async fn refresh_tree(
        &self,
        root: ObservedProcessIdentity,
    ) -> Result<ProcessTree, ObservationError> {
        select_tree(self.snapshot().await?, root.os_pid, Some(root))
    }
}

fn select_tree(
    snapshot: ProcessSnapshot,
    pid: u32,
    expected: Option<ObservedProcessIdentity>,
) -> Result<ProcessTree, ObservationError> {
    let processes: BTreeMap<_, _> = snapshot
        .processes
        .into_iter()
        .map(|p| (p.identity.os_pid, p))
        .collect();
    let root = processes
        .get(&pid)
        .ok_or(ObservationError::NotVisible(pid))?
        .identity;
    if expected.is_some_and(|e| e != root) {
        return Err(ObservationError::IdentityChanged(pid));
    }
    let mut children: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for p in processes.values() {
        if let Some(parent) = p.parent_os_pid {
            // A newer process cannot be the parent of an older process. Reject known
            // stale parent-PID links; unavailable/coarse timestamps remain best-effort.
            if let Some(parent_process) = processes.get(&parent) {
                if matches!((parent_process.identity.start_time_unix_seconds, p.identity.start_time_unix_seconds), (Some(a), Some(b)) if a > b)
                {
                    continue;
                }
            }
            children.entry(parent).or_default().push(p.identity.os_pid);
        }
    }
    let mut queue = VecDeque::from([pid]);
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    while let Some(pid) = queue.pop_front() {
        if !seen.insert(pid) {
            continue;
        }
        if let Some(p) = processes.get(&pid) {
            selected.push(p.clone());
        }
        if let Some(ids) = children.get(&pid) {
            queue.extend(ids);
        }
    }
    Ok(ProcessTree {
        root,
        processes: selected,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use shepherd_domain::ObservedProcess;
    fn p(pid: u32, parent: Option<u32>, start: u64) -> ObservedProcess {
        ObservedProcess {
            identity: ObservedProcessIdentity {
                os_pid: pid,
                start_time_unix_seconds: Some(start),
            },
            parent_os_pid: parent,
            name: format!("p{pid}"),
        }
    }
    #[test]
    fn tree_orders_children_excludes_siblings_and_handles_cycles() {
        let s = ProcessSnapshot {
            processes: vec![
                p(4, Some(2), 1),
                p(3, Some(1), 1),
                p(2, Some(4), 1),
                p(1, None, 1),
            ],
        };
        let t = select_tree(s, 2, None).unwrap();
        assert_eq!(
            t.processes
                .iter()
                .map(|p| p.identity.os_pid)
                .collect::<Vec<_>>(),
            vec![2, 4]
        );
    }
    #[test]
    fn rejects_missing_reused_root_and_stale_parent_link() {
        let s = ProcessSnapshot {
            processes: vec![p(1, None, 5), p(2, Some(1), 4), p(3, Some(1), 6)],
        };
        assert!(matches!(
            select_tree(s.clone(), 9, None),
            Err(ObservationError::NotVisible(9))
        ));
        assert!(matches!(
            select_tree(s.clone(), 1, Some(p(1, None, 4).identity)),
            Err(ObservationError::IdentityChanged(1))
        ));
        assert_eq!(select_tree(s, 1, None).unwrap().processes.len(), 2);
    }
}
