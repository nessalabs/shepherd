//! Integration-event publisher adapters.

use async_trait::async_trait;
use shepherd_app::ports::IntegrationEventPublisher;
use shepherd_domain::IntegrationEvent;

/// A publisher that drops every integration event.
///
/// The default outbound adapter (ADR 0007): the boundary port exists so consumers can
/// plug in their own bus, but nothing is emitted until they do.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopIntegrationPublisher;

#[async_trait]
impl IntegrationEventPublisher for NoopIntegrationPublisher {
    async fn publish(&self, _event: IntegrationEvent) {}
}

/// A bounded, lossy-tolerant publisher backed by a `tokio::sync::broadcast` channel.
///
/// A slow or absent subscriber cannot stall Shepherd: `send` never blocks and lagging
/// subscribers simply miss events.
#[derive(Debug, Clone)]
pub struct BroadcastIntegrationPublisher {
    sender: tokio::sync::broadcast::Sender<IntegrationEvent>,
}

impl BroadcastIntegrationPublisher {
    /// Creates a publisher with a bounded buffer of `capacity` events.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _rx) = tokio::sync::broadcast::channel(capacity);
        Self { sender }
    }

    /// Subscribes to the integration-event stream.
    #[must_use]
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<IntegrationEvent> {
        self.sender.subscribe()
    }
}

#[async_trait]
impl IntegrationEventPublisher for BroadcastIntegrationPublisher {
    async fn publish(&self, event: IntegrationEvent) {
        // Ignore the "no active receivers" error: publishing is best-effort by contract.
        let _ = self.sender.send(event);
    }
}
