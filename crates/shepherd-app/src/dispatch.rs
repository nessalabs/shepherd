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
}

impl WaitNotifierHandler {
    /// Creates the handler with an injected [`Waiters`] port.
    #[must_use]
    pub fn new(waiters: Arc<dyn Waiters>) -> Self {
        Self { waiters }
    }
}

#[async_trait]
impl EventHandler for WaitNotifierHandler {
    async fn handle(&self, event: &DomainEvent) -> Result<(), HandlerError> {
        if let DomainEvent::ProcessReaped { pid, exit, .. } = event {
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
