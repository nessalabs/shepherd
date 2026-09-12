//! An in-memory [`Waiters`] adapter using per-process `watch` channels.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use shepherd_app::ports::{WaitFuture, Waiters};
use shepherd_domain::{ProcessExit, ProcessId};
use tokio::sync::watch;

type Slots = HashMap<ProcessId, watch::Sender<Option<ProcessExit>>>;

/// Wakes `wait(pid)` callers when a process is reaped. A late waiter (after the exit was
/// already recorded) resolves immediately; an early waiter is woken on `signal_exit`.
#[derive(Debug, Default, Clone)]
pub struct InMemoryWaiters {
    slots: Arc<Mutex<Slots>>,
}

impl InMemoryWaiters {
    /// Creates an empty waiter set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn sender(&self, pid: ProcessId) -> watch::Sender<Option<ProcessExit>> {
        let mut slots = self.slots.lock().expect("waiters mutex");
        slots
            .entry(pid)
            .or_insert_with(|| watch::channel(None).0)
            .clone()
    }
}

impl Waiters for InMemoryWaiters {
    fn signal_exit(&self, pid: ProcessId, exit: ProcessExit) {
        self.sender(pid).send_if_modified(|current| {
            if current.is_some_and(|previous| previous.outcome.is_verified()) {
                return false;
            }
            *current = Some(exit);
            true
        });
    }

    fn try_get(&self, pid: ProcessId) -> Option<ProcessExit> {
        let slots = self.slots.lock().expect("waiters mutex");
        slots.get(&pid).and_then(|tx| *tx.borrow())
    }

    fn wait(&self, pid: ProcessId) -> WaitFuture {
        let sender = self.sender(pid);
        Box::pin(async move {
            let mut rx = sender.subscribe();
            loop {
                if let Some(exit) = *rx.borrow_and_update() {
                    return exit;
                }
                // The sender lives in the slots map, so this only errors if the map is
                // dropped, which cannot happen while a caller holds the supervisor.
                if rx.changed().await.is_err() {
                    // Fall back to a final read; if still empty the sender is gone.
                    if let Some(exit) = *rx.borrow() {
                        return exit;
                    }
                    std::future::pending::<()>().await;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shepherd_domain::TerminationOutcome;
    #[tokio::test]
    async fn verified_correction_survives_delayed_unverified_publication() {
        let waiters = InMemoryWaiters::new();
        let pid = ProcessId::new(1);
        let mut exit = ProcessExit {
            pid,
            code: None,
            signal: None,
            outcome: shepherd_domain::TerminationOutcome::CleanupUnverified(
                shepherd_domain::UnverifiedReason::ReapFailed,
            ),
            forced: false,
        };
        waiters.signal_exit(pid, exit);
        let failed = exit;
        exit.outcome = TerminationOutcome::GracefulSuccess;
        exit.code = Some(0);
        waiters.signal_exit(pid, exit);
        waiters.signal_exit(pid, failed);
        assert_eq!(waiters.try_get(pid), Some(exit));
        assert_eq!(waiters.wait(pid).await, exit);
    }

    #[tokio::test]
    async fn exit_before_first_subscriber_is_retained() {
        let waiters = InMemoryWaiters::new();
        let pid = ProcessId::new(1);
        let exit = ProcessExit {
            pid,
            code: Some(0),
            signal: None,
            outcome: TerminationOutcome::ExitedNaturally,
            forced: false,
        };
        waiters.signal_exit(pid, exit);
        assert_eq!(waiters.try_get(pid), Some(exit));
        assert_eq!(waiters.wait(pid).await, exit);
    }
}
