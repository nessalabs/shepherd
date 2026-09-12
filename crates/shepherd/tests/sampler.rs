//! Deterministic scheduling regressions: sample latency cannot serialize other roots.
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use shepherd::{
    Capabilities, NullBackend, ProcessBackend, ProcessSpec, SpawnError, Spawned, StatsError,
    SupervisorBuilder, TerminateError, TerminateOptions, WaitError,
};
use shepherd_domain::{ProcessScopeId, RawExit, RawStats, Signal};

#[derive(Default)]
struct Observation {
    stalled: bool,
    calls: usize,
    active: usize,
    reaped: bool,
}
#[derive(Default)]
struct Backend {
    inner: NullBackend,
    observations: Mutex<HashMap<u32, Observation>>,
    kills: AtomicUsize,
}
struct Sampling<'a>(&'a Backend, u32);
impl Drop for Sampling<'_> {
    fn drop(&mut self) {
        self.0
            .observations
            .lock()
            .unwrap()
            .get_mut(&self.1)
            .unwrap()
            .active -= 1;
    }
}
#[async_trait]
impl ProcessBackend for Backend {
    async fn spawn(
        &self,
        scope: ProcessScopeId,
        spec: &ProcessSpec,
    ) -> Result<Spawned, SpawnError> {
        let target = self.inner.spawn(scope, spec).await?;
        self.observations.lock().unwrap().insert(
            target.os.pid,
            Observation {
                stalled: spec.program == "stalled",
                ..Observation::default()
            },
        );
        Ok(target)
    }
    async fn signal(&self, target: &Spawned, signal: Signal) -> Result<(), TerminateError> {
        self.inner.signal(target, signal).await
    }
    async fn signal_scope(
        &self,
        scope: ProcessScopeId,
        signal: Signal,
    ) -> Result<(), TerminateError> {
        self.inner.signal_scope(scope, signal).await
    }
    async fn wait(&self, target: &Spawned) -> Result<RawExit, WaitError> {
        let exit = self.inner.wait(target).await?;
        self.observations
            .lock()
            .unwrap()
            .get_mut(&target.os.pid)
            .unwrap()
            .reaped = true;
        Ok(exit)
    }
    async fn sample(&self, target: &Spawned) -> Result<RawStats, StatsError> {
        let (stalled, count) = {
            let mut observations = self.observations.lock().unwrap();
            let observation = observations.get_mut(&target.os.pid).unwrap();
            assert_eq!(observation.active, 0, "overlapping samples for one root");
            observation.calls += 1;
            observation.active += 1;
            (observation.stalled, observation.calls)
        };
        let _sampling = Sampling(self, target.os.pid);
        if stalled {
            std::future::pending::<()>().await;
        }
        let mut raw = self.inner.sample(target).await?;
        raw.memory_rss_bytes = count as u64;
        Ok(raw)
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn hard_kill_scope(&self, scope: ProcessScopeId) {
        self.inner.hard_kill_scope(scope);
    }
    fn hard_kill_all(&self) {
        self.kills.fetch_add(1, Ordering::SeqCst);
        self.inner.hard_kill_all();
    }
}
async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}
async fn tick() {
    tokio::time::advance(Duration::from_millis(100)).await;
    settle().await;
}

#[tokio::test(start_paused = true)]
async fn stalled_roots_do_not_delay_updates_or_new_roots() {
    let backend = Arc::new(Backend::default());
    let sup = SupervisorBuilder::new()
        .backend(backend.clone())
        .stats_interval(Duration::from_millis(100))
        .build();
    let scope = sup.create_scope();
    // Establish a pending sample before introducing any healthy root. This makes the
    // sequential implementation fail independently of registry iteration order.
    sup.spawn(scope, ProcessSpec::new("stalled")).await.unwrap();
    settle().await;
    for _ in 0..99 {
        sup.spawn(scope, ProcessSpec::new("stalled")).await.unwrap();
    }
    let healthy = sup.spawn(scope, ProcessSpec::new("healthy")).await.unwrap();
    tick().await;
    let first = sup.stats(healthy).await.unwrap().memory_rss_bytes;
    for _ in 0..3 {
        tick().await;
    }
    assert!(sup.stats(healthy).await.unwrap().memory_rss_bytes >= first + 3);
    let newcomer = sup.spawn(scope, ProcessSpec::new("healthy")).await.unwrap();
    tick().await;
    assert!(sup.stats(newcomer).await.is_ok());
    assert!(backend
        .observations
        .lock()
        .unwrap()
        .values()
        .filter(|s| s.stalled)
        .all(|s| s.calls == 1 && s.active == 1));

    assert!(sup
        .terminate_scope(scope, TerminateOptions::default())
        .await
        .unwrap()
        .all_verified());
    tick().await;
    assert!(matches!(
        sup.stats(healthy).await,
        Err(StatsError::UnknownProcess(_))
    ));
    assert!(backend
        .observations
        .lock()
        .unwrap()
        .values()
        .all(|s| s.reaped && s.active == 0));
}

#[tokio::test(start_paused = true)]
async fn timeouts_retry_without_overlap_and_sampling_cannot_keep_owner_alive() {
    let backend = Arc::new(Backend::default());
    let sup = SupervisorBuilder::new()
        .backend(backend.clone())
        .stats_interval(Duration::from_millis(100))
        .build();
    let scope = sup.create_scope();
    let stalled = sup.spawn(scope, ProcessSpec::new("stalled")).await.unwrap();
    settle().await;
    for _ in 0..11 {
        tick().await;
    }
    assert!(matches!(sup.stats(stalled).await,
        Err(StatsError::Backend(message)) if message == "sampler timeout"));
    assert!(backend
        .observations
        .lock()
        .unwrap()
        .values()
        .all(|s| s.calls == 2 && s.active == 1));
    drop(sup);
    assert_eq!(backend.kills.load(Ordering::SeqCst), 1);
    settle().await;
    tick().await;
    assert!(backend
        .observations
        .lock()
        .unwrap()
        .values()
        .all(|s| s.reaped && s.active == 0));
}
