//! An in-memory [`Waiters`] adapter using per-process `watch` channels.

#[cfg(all(shepherd_loom, test))]
use loom::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
#[cfg(not(all(shepherd_loom, test)))]
use std::sync::{Arc, Mutex};

use shepherd_app::ports::{WaitFuture, Waiters};
use shepherd_domain::{ProcessExit, ProcessId};
use tokio::sync::watch;

#[derive(Debug, Default)]
struct Slots {
    senders: HashMap<ProcessId, watch::Sender<Option<ProcessExit>>>,
    completed: VecDeque<ProcessId>,
}

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
            .senders
            .entry(pid)
            .or_insert_with(|| watch::channel(None).0)
            .clone()
    }
}

impl Waiters for InMemoryWaiters {
    fn signal_exit(&self, pid: ProcessId, exit: ProcessExit) {
        let mut slots = self.slots.lock().expect("waiters mutex");
        let sender = slots
            .senders
            .entry(pid)
            .or_insert_with(|| watch::channel(None).0)
            .clone();
        let previous = *sender.borrow();
        if previous.is_some_and(|exit| exit.outcome.is_verified()) {
            return;
        }
        sender.send_replace(Some(exit));
        if previous.is_none() {
            slots.completed.push_back(pid);
        }
        while slots.completed.len() > 256 {
            if let Some(old) = slots.completed.pop_front() {
                slots.senders.remove(&old);
            }
        }
    }

    fn try_get(&self, pid: ProcessId) -> Option<ProcessExit> {
        let slots = self.slots.lock().expect("waiters mutex");
        slots.senders.get(&pid).and_then(|tx| *tx.borrow())
    }

    fn wait(&self, pid: ProcessId) -> WaitFuture {
        let sender = self.sender(pid);
        let mut rx = sender.subscribe();
        Box::pin(async move {
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

#[cfg(all(test, not(shepherd_loom)))]
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
        assert_eq!(waiters.slots.lock().unwrap().completed.iter().filter(|id| **id == pid).count(), 1);
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
        assert_eq!(waiters.slots.lock().unwrap().completed.iter().filter(|id| **id == pid).count(), 1);
    }
}

#[cfg(all(test, shepherd_loom))]
mod loom_tests {
    use super::*;
    use shepherd_domain::TerminationOutcome;
    #[test]
    fn loom_waiter_registration_vs_completion() {
        loom::model(|| {
            let waiters = InMemoryWaiters::new();
            let publisher = waiters.clone();
            let subscriber = waiters.clone();
            let pid = ProcessId::new(1);
            let exit = ProcessExit {
                pid,
                code: Some(0),
                signal: None,
                outcome: TerminationOutcome::ExitedNaturally,
                forced: false,
            };
            let publish = loom::thread::spawn(move || publisher.signal_exit(pid, exit));
            let subscribe = loom::thread::spawn(move || subscriber.wait(pid));
            publish.join().unwrap();
            let future = subscribe.join().unwrap();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            assert_eq!(runtime.block_on(future), exit);
            assert_eq!(waiters.try_get(pid), Some(exit));
        });
    }
}

#[cfg(all(test, not(shepherd_loom)))]
mod retention_tests {
    use super::*;
    use shepherd_domain::TerminationOutcome;
    #[tokio::test]
    async fn completed_history_is_bounded_without_invalidating_registered_waiters() {
        let waiters = InMemoryWaiters::new();
        let pending = waiters.wait(ProcessId::new(1));
        for id in 1..1000 {
            let pid = ProcessId::new(id);
            waiters.signal_exit(
                pid,
                ProcessExit {
                    pid,
                    code: Some(0),
                    signal: None,
                    outcome: TerminationOutcome::ExitedNaturally,
                    forced: false,
                },
            );
        }
        assert!(waiters.try_get(ProcessId::new(1)).is_none());
        assert_eq!(pending.await.pid, ProcessId::new(1));
        assert_eq!(waiters.slots.lock().unwrap().senders.len(), 256);
    }
}
