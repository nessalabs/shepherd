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
    entries: HashMap<ProcessId, Slot>,
    completed: VecDeque<ProcessId>,
}

#[derive(Debug)]
enum Slot {
    Pending(watch::Sender<Option<ProcessExit>>),
    Completed(ProcessExit),
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
}

impl Waiters for InMemoryWaiters {
    fn signal_exit(&self, pid: ProcessId, exit: ProcessExit) {
        let mut slots = self.slots.lock().expect("waiters mutex");
        let previously_published = match slots.entries.get(&pid) {
            Some(Slot::Completed(_)) => return,
            Some(Slot::Pending(sender)) => sender.borrow().is_some(),
            None => false,
        };
        if let Some(Slot::Pending(sender)) = slots.entries.get(&pid) {
            // Registered receivers retain this channel independently of history.
            sender.send_replace(Some(exit));
        }
        if exit.outcome.is_verified() {
            slots.entries.insert(pid, Slot::Completed(exit));
        } else {
            // A later verified correction must still reach registered receivers.
            slots
                .entries
                .entry(pid)
                .or_insert_with(|| Slot::Pending(watch::channel(Some(exit)).0));
        }
        if !previously_published {
            slots.completed.push_back(pid);
        }
        while slots.completed.len() > 256 {
            if let Some(old) = slots.completed.pop_front() {
                slots.entries.remove(&old);
            }
        }
    }

    fn try_get(&self, pid: ProcessId) -> Option<ProcessExit> {
        let slots = self.slots.lock().expect("waiters mutex");
        match slots.entries.get(&pid) {
            Some(Slot::Completed(exit)) => Some(*exit),
            Some(Slot::Pending(sender)) => *sender.borrow(),
            None => None,
        }
    }

    fn wait(&self, pid: ProcessId) -> WaitFuture {
        let mut slots = self.slots.lock().expect("waiters mutex");
        let entry = slots
            .entries
            .entry(pid)
            .or_insert_with(|| Slot::Pending(watch::channel(None).0));
        let mut rx = match entry {
            Slot::Completed(exit) => {
                let exit = *exit;
                return Box::pin(std::future::ready(exit));
            }
            Slot::Pending(sender) => sender.subscribe(),
        };
        drop(slots);
        Box::pin(async move {
            loop {
                if let Some(exit) = *rx.borrow_and_update() {
                    return exit;
                }
                // Verified publication releases the registry sender. Receivers still
                // own its final value, including across lookup-history eviction.
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
    async fn cancelled_wait_does_not_retain_waiter_registry() {
        let waiters = InMemoryWaiters::new();
        let slots = Arc::downgrade(&waiters.slots);
        let mut pending = waiters.wait(ProcessId::new(91));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), &mut pending)
                .await
                .is_err()
        );
        drop(pending);
        let clone = waiters.clone();
        drop(waiters);
        assert!(slots.upgrade().is_some());
        drop(clone);
        assert!(
            slots.upgrade().is_none(),
            "cancelled waiter retained the registry"
        );
    }

    #[tokio::test]
    async fn verified_correction_survives_delayed_unverified_publication() {
        let waiters = InMemoryWaiters::new();
        let pid = ProcessId::new(1);
        let pending = waiters.wait(pid);
        let channel = {
            let slots = waiters.slots.lock().unwrap();
            match slots.entries.get(&pid).unwrap() {
                Slot::Pending(sender) => sender.subscribe(),
                Slot::Completed(_) => panic!("not yet published"),
            }
        };
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
        assert!(
            channel.has_changed().is_ok(),
            "unverified corrections need a sender"
        );
        let failed = exit;
        exit.outcome = TerminationOutcome::GracefulSuccess;
        exit.code = Some(0);
        waiters.signal_exit(pid, exit);
        waiters.signal_exit(pid, failed);
        assert!(
            channel.has_changed().is_err(),
            "verified history must release its notification sender"
        );
        assert_eq!(waiters.try_get(pid), Some(exit));
        assert_eq!(pending.await, exit);
        assert_eq!(waiters.wait(pid).await, exit);
        assert_eq!(
            waiters
                .slots
                .lock()
                .unwrap()
                .completed
                .iter()
                .filter(|id| **id == pid)
                .count(),
            1
        );
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
        assert_eq!(
            waiters
                .slots
                .lock()
                .unwrap()
                .completed
                .iter()
                .filter(|id| **id == pid)
                .count(),
            1
        );
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
        assert_eq!(waiters.slots.lock().unwrap().entries.len(), 256);
    }
}
