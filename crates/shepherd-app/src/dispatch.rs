//! The in-process event dispatcher (a mediator) and the standard domain-event handlers.
//!
//! The domain returns events; this module routes them, after the state transition is
//! committed, to focused handlers that depend only on ports. Handler errors are logged via
//! `tracing` and never silently swallowed.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use shepherd_domain::{DomainEvent, IntegrationEvent};

use crate::error::HandlerError;
use crate::ports::{EventHandler, IntegrationEventPublisher, Waiters};
use crate::registry::ScopeRegistry;

/// Shared, mutex-guarded scope registry. The guard is never held across an `.await`.
pub type SharedRegistry = Arc<Mutex<ScopeRegistry>>;

/// Routes committed domain events to the registered handlers in deterministic order.
#[derive(Clone)]
pub struct EventDispatcher {
    handlers: Vec<Arc<dyn EventHandler>>,
}

impl std::fmt::Debug for EventDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventDispatcher")
            .field(
                "handlers",
                &self.handlers.iter().map(|h| h.name()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl EventDispatcher {
    /// Creates a dispatcher over an ordered set of handlers.
    #[must_use]
    pub fn new(handlers: Vec<Arc<dyn EventHandler>>) -> Self {
        Self { handlers }
    }

    /// Dispatches each event to every handler, in order. A handler error does not abort the
    /// others; it is surfaced via `tracing`.
    pub async fn dispatch(&self, events: &[DomainEvent]) {
        for event in events {
            for handler in &self.handlers {
                if let Err(error) = handler.handle(event).await {
                    tracing::error!(
                        handler = error.handler,
                        %error,
                        "domain event handler failed"
                    );
                }
            }
        }
    }
}

/// On [`DomainEvent::ProcessReaped`], records the terminal exit so `wait(pid)` callers wake.
pub struct WaitNotifierHandler {
    waiters: Arc<dyn Waiters>,
    registry: Option<SharedRegistry>,
}

impl WaitNotifierHandler {
    /// Creates the handler with an injected [`Waiters`] port.
    #[must_use]
    pub fn new(waiters: Arc<dyn Waiters>) -> Self {
        Self {
            waiters,
            registry: None,
        }
    }

    pub(crate) fn with_registry(waiters: Arc<dyn Waiters>, registry: SharedRegistry) -> Self {
        Self {
            waiters,
            registry: Some(registry),
        }
    }
}

#[async_trait]
impl EventHandler for WaitNotifierHandler {
    async fn handle(&self, event: &DomainEvent) -> Result<(), HandlerError> {
        if let DomainEvent::ProcessReaped { scope, pid, exit } = event {
            if !exit.outcome.is_verified() {
                if let Some(registry) = &self.registry {
                    let registry = registry
                        .lock()
                        .map_err(|_| HandlerError::new(self.name(), "registry mutex poisoned"))?;
                    // A failed reap must still own a quarantined registry entry. A
                    // verified correction can prune it and expire from waiter history
                    // before this older event dispatches; never resurrect that failure.
                    if registry
                        .get(*scope)
                        .and_then(|scope| scope.get(*pid))
                        .and_then(|process| process.exit())
                        .is_some_and(|current| !current.outcome.is_verified())
                    {
                        self.waiters.signal_exit(*pid, *exit);
                    }
                    return Ok(());
                }
            }
            self.waiters.signal_exit(*pid, *exit);
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "WaitNotifierHandler"
    }
}

/// On terminal events, prunes bookkeeping and releases closed scopes (invariant #9).
pub struct RegistryPruneHandler {
    registry: SharedRegistry,
}

impl RegistryPruneHandler {
    /// Creates the handler with an injected registry.
    #[must_use]
    pub fn new(registry: SharedRegistry) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl EventHandler for RegistryPruneHandler {
    async fn handle(&self, event: &DomainEvent) -> Result<(), HandlerError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| HandlerError::new(self.name(), "registry mutex poisoned"))?;
        match event {
            DomainEvent::ProcessReaped { scope, pid, exit } if exit.outcome.is_verified() => {
                if let Some(scope) = registry.get_mut(*scope) {
                    scope.prune(*pid);
                }
            }
            // Scope release requires containment verification. Keep failed reap evidence
            // as well, so retries cannot mistake an unverified root for an empty scope.
            DomainEvent::ScopeClosed { .. } => {}
            _ => {}
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "RegistryPruneHandler"
    }
}

/// Translates the externally-meaningful subset of domain events into integration events and
/// publishes them via the outbound port.
pub struct IntegrationTranslator {
    publisher: Arc<dyn IntegrationEventPublisher>,
    sender: tokio::sync::mpsc::Sender<IntegrationEvent>,
    receiver: Mutex<Option<tokio::sync::mpsc::Receiver<IntegrationEvent>>>,
}
impl IntegrationTranslator {
    #[must_use]
    pub fn new(publisher: Arc<dyn IntegrationEventPublisher>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        Self {
            publisher,
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }
}
#[async_trait]
impl EventHandler for IntegrationTranslator {
    async fn handle(&self, event: &DomainEvent) -> Result<(), HandlerError> {
        let event = match event {
            DomainEvent::ProcessReaped { scope, pid, exit } => {
                IntegrationEvent::ProcessTerminated {
                    scope: *scope,
                    pid: *pid,
                    outcome: exit.outcome,
                }
            }
            DomainEvent::ScopeClosed { scope } => {
                IntegrationEvent::ScopeTerminated { scope: *scope }
            }
            _ => return Ok(()),
        };
        if let Some(mut receiver) = self.receiver.lock().expect("publisher mutex").take() {
            let publisher = self.publisher.clone();
            tokio::spawn(async move {
                while let Some(event) = receiver.recv().await {
                    if tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        publisher.publish(event),
                    )
                    .await
                    .is_err()
                    {
                        tracing::warn!("integration publisher timed out; event dropped");
                    }
                }
            });
        }
        if self.sender.try_send(event).is_err() {
            tracing::warn!("integration publisher queue full or closed; event dropped");
        }
        Ok(())
    }
    fn name(&self) -> &'static str {
        "IntegrationTranslator"
    }
}

#[cfg(test)]
mod waiter_notification_tests {
    use super::*;
    use shepherd_domain::{
        OsIdentity, ProcessExit, ProcessId, ProcessScopeId, ProcessSpec, ReuseToken,
        TerminationOutcome, UnverifiedReason,
    };
    use std::collections::HashMap;

    #[derive(Default)]
    struct EvictableWaiters(Mutex<HashMap<ProcessId, ProcessExit>>);
    impl Waiters for EvictableWaiters {
        fn signal_exit(&self, pid: ProcessId, exit: ProcessExit) {
            self.0.lock().unwrap().insert(pid, exit);
        }
        fn try_get(&self, pid: ProcessId) -> Option<ProcessExit> {
            self.0.lock().unwrap().get(&pid).copied()
        }
        fn wait(&self, _: ProcessId) -> crate::ports::WaitFuture {
            unreachable!()
        }
    }
    fn failed_exit(pid: ProcessId) -> ProcessExit {
        ProcessExit {
            pid,
            code: None,
            signal: None,
            outcome: TerminationOutcome::CleanupUnverified(UnverifiedReason::ReapFailed),
            forced: false,
        }
    }

    #[tokio::test]
    async fn stale_failure_cannot_replace_correction_or_resurrect_evicted_history() {
        let registry = Arc::new(Mutex::new(ScopeRegistry::new()));
        let waiters = Arc::new(EvictableWaiters::default());
        let notifier = WaitNotifierHandler::with_registry(waiters.clone(), registry.clone());
        let (scope, pid, failed) = {
            let mut registry = registry.lock().unwrap();
            let scope = registry.create_scope();
            let pid = registry.next_process_id();
            let s = registry.get_mut(scope).unwrap();
            s.attach_spawned(
                pid,
                OsIdentity::new(1, ReuseToken::Unavailable),
                ProcessSpec::new("unused"),
            )
            .unwrap();
            let failed = failed_exit(pid);
            s.record_reaped(pid, failed).unwrap();
            (scope, pid, failed)
        };
        let stale = DomainEvent::ProcessReaped {
            scope,
            pid,
            exit: failed,
        };
        notifier.handle(&stale).await.unwrap();
        assert_eq!(waiters.try_get(pid), Some(failed));
        let verified = ProcessExit {
            code: Some(0),
            outcome: TerminationOutcome::GracefulSuccess,
            ..failed
        };
        registry
            .lock()
            .unwrap()
            .get_mut(scope)
            .unwrap()
            .record_reaped(pid, verified)
            .unwrap();
        let corrected = DomainEvent::ProcessReaped {
            scope,
            pid,
            exit: verified,
        };
        notifier.handle(&corrected).await.unwrap();
        notifier.handle(&stale).await.unwrap();
        assert_eq!(waiters.try_get(pid), Some(verified));
        RegistryPruneHandler::new(registry)
            .handle(&corrected)
            .await
            .unwrap();
        // Simulate bounded waiter eviction after other roots complete.
        waiters.0.lock().unwrap().clear();
        notifier.handle(&stale).await.unwrap();
        assert_eq!(waiters.try_get(pid), None);
        // A verified notification remains deliverable even after registry pruning.
        notifier.handle(&corrected).await.unwrap();
        assert_eq!(waiters.try_get(pid), Some(verified));
    }

    #[tokio::test]
    async fn compatibility_constructor_delivers_without_a_registry() {
        let waiters = Arc::new(EvictableWaiters::default());
        let notifier = WaitNotifierHandler::new(waiters.clone());
        let pid = ProcessId::new(1);
        let exit = failed_exit(pid);
        notifier
            .handle(&DomainEvent::ProcessReaped {
                scope: ProcessScopeId::new(1),
                pid,
                exit,
            })
            .await
            .unwrap();
        assert_eq!(waiters.try_get(pid), Some(exit));
    }
}
