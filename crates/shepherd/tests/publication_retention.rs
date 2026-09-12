//! A stalled integration publisher must not retain one task per completed scope.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use shepherd::{
    IntegrationEvent, IntegrationEventPublisher, NullBackend, SupervisorBuilder, TerminateOptions,
};
use tokio::sync::Semaphore;

struct GatedPublisher {
    gate: Semaphore,
    active: AtomicUsize,
    peak: AtomicUsize,
    started: AtomicUsize,
    completed: AtomicUsize,
}
impl GatedPublisher {
    fn new() -> Self {
        Self {
            gate: Semaphore::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            started: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
        }
    }
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl IntegrationEventPublisher for GatedPublisher {
    async fn publish(&self, _: IntegrationEvent) {
        self.started.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let _active = Active(&self.active);
        self.gate.acquire().await.unwrap().forget();
        self.completed.fetch_add(1, Ordering::SeqCst);
    }
}
async fn settle() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn completed_scopes_share_a_bounded_publisher_queue_and_release_on_drop() {
    let publisher = Arc::new(GatedPublisher::new());
    let sup = SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .integration_publisher(publisher.clone())
        .build();
    for _ in 0..600 {
        let scope = sup.create_scope();
        assert!(sup
            .terminate_scope(scope, TerminateOptions::default())
            .await
            .unwrap()
            .all_verified());
        settle().await;
    }
    assert_eq!(publisher.active.load(Ordering::SeqCst), 1);
    assert_eq!(publisher.started.load(Ordering::SeqCst), 1);
    assert_eq!(publisher.peak.load(Ordering::SeqCst), 1);

    // One publication was active and exactly 64 were queued; overflow was dropped.
    publisher.gate.add_permits(1000);
    settle().await;
    assert_eq!(publisher.started.load(Ordering::SeqCst), 65);
    assert_eq!(publisher.completed.load(Ordering::SeqCst), 65);
    assert_eq!(publisher.active.load(Ordering::SeqCst), 0);
    assert_eq!(publisher.peak.load(Ordering::SeqCst), 1);

    let weak = Arc::downgrade(&publisher);
    drop(publisher);
    drop(sup);
    settle().await;
    assert!(
        weak.upgrade().is_none(),
        "publisher worker retained its sender or owner"
    );
}

#[tokio::test(start_paused = true)]
async fn publication_timeout_releases_a_dropped_owner_without_opening_the_gate() {
    let publisher = Arc::new(GatedPublisher::new());
    let sup = SupervisorBuilder::new()
        .backend(Arc::new(NullBackend::new()))
        .integration_publisher(publisher.clone())
        .build();
    let scope = sup.create_scope();
    sup.terminate_scope(scope, TerminateOptions::default())
        .await
        .unwrap();
    settle().await;
    assert_eq!(publisher.active.load(Ordering::SeqCst), 1);
    let weak = Arc::downgrade(&publisher);
    drop(publisher);
    drop(sup);
    assert!(weak.upgrade().is_some());
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    settle().await;
    assert!(
        weak.upgrade().is_none(),
        "timed-out publication retained its owner"
    );
}
