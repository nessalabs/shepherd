//! Bounded native observations, independent of the async coordinator's lifetime.
use shepherd_app::StatsError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub(super) struct SamplingPool(Arc<Semaphore>);
impl Default for SamplingPool {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(16)))
    }
}
struct ActiveSample(Arc<AtomicBool>);
impl Drop for ActiveSample {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl SamplingPool {
    pub(super) async fn run<T, F>(&self, active: Arc<AtomicBool>, read: F) -> Result<T, StatsError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, StatsError> + Send + 'static,
    {
        active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| StatsError::Backend("previous native sample still running".into()))?;
        let active = ActiveSample(active);
        let capacity = self
            .0
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StatsError::Backend("native sampling pool closed".into()))?;
        tokio::task::spawn_blocking(move || {
            // Cancellation of the async waiter cannot release either permit while
            // its native read is still running, nor submit a duplicate read.
            let _active = active;
            let _capacity = capacity;
            read()
        })
        .await
        .map_err(|error| StatsError::Backend(format!("native sample failed: {error}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[tokio::test]
    async fn blocking_read_does_not_stall_runtime_or_duplicate_after_timeout() {
        let pool = SamplingPool::default();
        let active = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (release, blocked) = std::sync::mpsc::channel();
        let task = tokio::spawn({
            let pool = pool.clone();
            let active = active.clone();
            let entered = entered.clone();
            let calls = calls.clone();
            async move {
                tokio::time::timeout(
                    Duration::from_millis(50),
                    pool.run(active, move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        entered.notify_one();
                        blocked.recv().unwrap();
                        Ok(())
                    }),
                )
                .await
            }
        });
        entered.notified().await;
        // Both the timer and a different root can progress on a current-thread runtime.
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            pool.run(Arc::new(AtomicBool::new(false)), || Ok(42))
                .await
                .unwrap(),
            42
        );
        assert!(task.await.unwrap().is_err());
        assert!(active.load(Ordering::Acquire));
        assert_eq!(
            pool.0.available_permits(),
            15,
            "native capacity released before syscall finished"
        );
        for _ in 0..32 {
            assert!(pool
                .run::<(), _>(active.clone(), || panic!("duplicate native read"))
                .await
                .is_err());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while active.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pool.0.available_permits(), 16);
        assert_eq!(pool.run(active, || Ok(7)).await.unwrap(), 7);
    }

    #[tokio::test]
    async fn admission_is_bounded_and_queued_cancellation_releases_child_permit() {
        let pool = SamplingPool::default();
        // Reserve all capacity, representing sixteen outstanding native reads.
        let capacity = pool.0.clone().acquire_many_owned(16).await.unwrap();
        let active = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let pool = pool.clone();
            let active = active.clone();
            async move {
                pool.run::<(), _>(active, || panic!("read bypassed capacity limit"))
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(active.load(Ordering::Acquire));
        assert!(!task.is_finished());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!active.load(Ordering::Acquire));
        drop(capacity);
        assert_eq!(pool.run(active, || Ok(1)).await.unwrap(), 1);
    }
}
