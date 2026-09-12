//! Portable read-only process enumeration. Never uses sysinfo's control operations.
use async_trait::async_trait;
use shepherd_app::{ObservationError, ProcessObservationBackend};
use shepherd_domain::{ObservedProcess, ObservedProcessIdentity, ProcessSnapshot};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Serialized native enumeration. A canceled caller leaves its permit with the
/// blocking operation, so repeated cancellation cannot queue unbounded native work.
pub struct SystemProcessObserver {
    permit: Arc<Semaphore>,
}
impl Default for SystemProcessObserver {
    fn default() -> Self {
        Self {
            permit: Arc::new(Semaphore::new(1)),
        }
    }
}
#[async_trait]
impl ProcessObservationBackend for SystemProcessObserver {
    async fn usage(
        &self,
        members: Vec<ObservedProcessIdentity>,
    ) -> Result<shepherd_app::UsageSnapshot, ObservationError> {
        crate::usage::blocking(move || {
            Ok(crate::usage::collect(
                members,
                shepherd_app::UsageSelection::Tree,
            ))
        })
        .await
    }
    async fn snapshot(&self) -> Result<ProcessSnapshot, ObservationError> {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return Err(ObservationError::Unsupported);
        }
        let permit = self
            .permit
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| ObservationError::Backend(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            // A fresh table avoids carrying stale entries across observations.
            let mut system = sysinfo::System::new();
            system.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::All,
                true,
                sysinfo::ProcessRefreshKind::nothing(),
            );
            let mut processes: Vec<_> = system
                .processes()
                .iter()
                .filter(|(_, p)| p.thread_kind() != Some(sysinfo::ThreadKind::Userland))
                .map(|(pid, p)| {
                    let start = p.start_time();
                    ObservedProcess {
                        identity: ObservedProcessIdentity {
                            os_pid: pid.as_u32(),
                            start_time_unix_seconds: (start != 0).then_some(start),
                        },
                        parent_os_pid: p.parent().map(|p| p.as_u32()),
                        name: p.name().to_string_lossy().into_owned(),
                    }
                })
                .collect();
            processes.sort_by_key(|p| p.identity.os_pid);
            ProcessSnapshot { processes }
        })
        .await
        .map_err(|e| ObservationError::Backend(e.to_string()))
    }
}
