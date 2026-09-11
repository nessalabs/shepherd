//! A Tokio-backed [`Clock`] adapter.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use shepherd_app::ports::Clock;

/// A [`Clock`] backed by the real system time and the Tokio timer.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}
