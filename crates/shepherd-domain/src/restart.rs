//! When a service restarts its process after a failure.

use std::time::Duration;

/// Exponential backoff between restarts of a service.
///
/// The first restart after a failure waits `initial`; each further consecutive failure
/// multiplies the wait by `factor`, up to `max`. A run that stayed ready for
/// `healthy_after` clears the failure count. `max_failures` (if set) is how many
/// consecutive failures the service tolerates before it gives up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestartPolicy {
    initial: Duration,
    max: Duration,
    factor: u32,
    healthy_after: Duration,
    max_failures: Option<u32>,
}

impl Default for RestartPolicy {
    /// 1 s doubling to 60 s, healthy after 60 s ready, never gives up.
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
            factor: 2,
            healthy_after: Duration::from_secs(60),
            max_failures: None,
        }
    }
}

impl RestartPolicy {
    /// A policy waiting `initial`, growing to at most `max` (raised to `initial` if
    /// smaller). Other settings take their defaults.
    #[must_use]
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max: max.max(initial),
            ..Self::default()
        }
    }

    /// Sets the growth factor (minimum 1, which means a constant delay).
    #[must_use]
    pub fn factor(mut self, factor: u32) -> Self {
        self.factor = factor.max(1);
        self
    }

    /// Sets how long a run must stay ready to clear the failure count.
    #[must_use]
    pub fn healthy_after(mut self, healthy_after: Duration) -> Self {
        self.healthy_after = healthy_after;
        self
    }

    /// Gives up after this many consecutive failures (`None` never gives up).
    #[must_use]
    pub fn max_failures(mut self, max_failures: Option<u32>) -> Self {
        self.max_failures = max_failures;
        self
    }

    /// How long a run must stay ready to clear the failure count.
    #[must_use]
    pub fn healthy_duration(&self) -> Duration {
        self.healthy_after
    }

    /// The wait before the restart that follows `failures` consecutive failures
    /// (`0` means the first start, which never waits).
    #[must_use]
    pub fn delay(&self, failures: u32) -> Duration {
        if failures == 0 {
            return Duration::ZERO;
        }
        let mut delay = self.initial;
        for _ in 1..failures {
            if delay >= self.max {
                break;
            }
            delay = delay.saturating_mul(self.factor);
        }
        delay.min(self.max)
    }

    /// Whether `failures` consecutive failures exhaust this policy.
    #[must_use]
    pub fn gives_up(&self, failures: u32) -> bool {
        self.max_failures.is_some_and(|max| failures > max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Duration = Duration::from_secs(1);

    #[test]
    fn default_doubles_from_one_to_sixty_seconds() {
        let policy = RestartPolicy::default();
        let delays: Vec<_> = (0..9).map(|n| policy.delay(n).as_secs()).collect();
        assert_eq!(delays, [0, 1, 2, 4, 8, 16, 32, 60, 60]);
        assert_eq!(policy.delay(u32::MAX), 60 * S);
        assert_eq!(policy.healthy_duration(), 60 * S);
        assert!(!policy.gives_up(u32::MAX));
    }

    #[test]
    fn builder_bounds_its_settings() {
        let policy = RestartPolicy::new(5 * S, S)
            .factor(0)
            .healthy_after(3 * S)
            .max_failures(Some(2));
        assert_eq!(policy.delay(1), 5 * S);
        assert_eq!(policy.delay(4), 5 * S, "max is raised to initial; factor 1 is flat");
        assert_eq!(policy.healthy_duration(), 3 * S);
        assert!(!policy.gives_up(2));
        assert!(policy.gives_up(3));
        let huge = RestartPolicy::new(Duration::MAX / 2, Duration::MAX).factor(u32::MAX);
        assert_eq!(huge.delay(3), Duration::MAX);
    }
}
