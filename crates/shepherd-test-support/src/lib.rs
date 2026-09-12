//! Shared test environment configuration. See docs/TEST_ENVIRONMENT.md for the inventory.
//! Read settings at the test boundary, then pass typed values to the test runner.
use std::{ffi::OsString, path::PathBuf};

pub const STRESS_SEED: &str = "SHEPHERD_STRESS_SEED";
pub const STRESS_RUNTIME: &str = "SHEPHERD_STRESS_RUNTIME";
pub const STRESS_ITERATIONS: &str = "SHEPHERD_STRESS_ITERATIONS";
pub const PROPERTY_CASES: &str = "PROPTEST_CASES";
pub const CGROUP_ROOT: &str = "SHEPHERD_CGROUP_ROOT";

/// Child-launch protocol names, not user-configurable test tuning.
pub mod probe {
    pub const VALUE: &str = "SHEPHERD_PROBE_VALUE";
    pub const ABSENT: &str = "SHEPHERD_PROBE_ABSENT";
    pub const CWD: &str = "SHEPHERD_PROBE_CWD";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFlavor {
    Current,
    Multi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StressMode {
    Smoke,
    Long,
}

#[derive(Debug, PartialEq, Eq)]
pub struct StressConfig {
    pub seed: u64,
    pub runtimes: &'static [RuntimeFlavor],
    pub iterations: usize,
}

/// Reads only the settings needed by the selected test, without mutating or caching
/// the process environment. Invalid settings panic with the variable's name.
pub struct TestEnvironment<F = fn(&str) -> Option<OsString>> {
    lookup: F,
}

impl TestEnvironment {
    pub fn from_env() -> Self {
        Self {
            lookup: |name| std::env::var_os(name),
        }
    }
}

impl<F: Fn(&str) -> Option<OsString>> TestEnvironment<F> {
    fn text(&self, name: &str) -> Option<String> {
        (self.lookup)(name).map(|value| {
            value
                .into_string()
                .unwrap_or_else(|_| panic!("{name} must contain valid Unicode"))
        })
    }

    fn number<T: std::str::FromStr>(&self, name: &str, default: T) -> T {
        self.text(name).map_or(default, |value| {
            value.parse().unwrap_or_else(|_| {
                panic!("{name} must be an unsigned integer in range; got {value:?}")
            })
        })
    }

    pub fn stress(&self, mode: StressMode) -> StressConfig {
        let runtimes: &'static [RuntimeFlavor] = match self.text(STRESS_RUNTIME).as_deref() {
            None | Some("both") => &[RuntimeFlavor::Current, RuntimeFlavor::Multi],
            Some("current") => &[RuntimeFlavor::Current],
            Some("multi") => &[RuntimeFlavor::Multi],
            Some(value) => {
                panic!("{STRESS_RUNTIME} must be current, multi, or both; got {value:?}")
            }
        };
        let iterations = match mode {
            StressMode::Smoke => 36,
            StressMode::Long => self.number(STRESS_ITERATIONS, 2_000_usize),
        };
        assert!(
            (6..=100_000).contains(&iterations),
            "{STRESS_ITERATIONS} must be 6..=100000"
        );
        StressConfig {
            seed: self.number(STRESS_SEED, 42),
            runtimes,
            iterations,
        }
    }

    pub fn property_cases(&self) -> u32 {
        let cases = self.number(PROPERTY_CASES, 64_u32);
        assert!(cases > 0, "{PROPERTY_CASES} must be a positive integer");
        cases
    }

    /// Preserve native paths, including non-Unicode paths on Unix.
    pub fn cgroup_root(&self) -> PathBuf {
        let root = (self.lookup)(CGROUP_ROOT)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                panic!("{CGROUP_ROOT} must name a writable delegated cgroup v2 ancestor")
            });
        PathBuf::from(root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(
        entries: &'a [(&'a str, &'a str)],
    ) -> TestEnvironment<impl Fn(&str) -> Option<OsString> + 'a> {
        TestEnvironment {
            lookup: move |name: &str| {
                entries
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| OsString::from(value))
            },
        }
    }

    #[test]
    fn defaults_and_runtime_selection() {
        let empty = env(&[]);
        assert_eq!(
            empty.stress(StressMode::Long),
            StressConfig {
                seed: 42,
                runtimes: &[RuntimeFlavor::Current, RuntimeFlavor::Multi],
                iterations: 2000
            }
        );
        assert_eq!(empty.property_cases(), 64);
        for (name, flavor) in [
            ("current", RuntimeFlavor::Current),
            ("multi", RuntimeFlavor::Multi),
        ] {
            assert_eq!(
                env(&[(STRESS_RUNTIME, name)])
                    .stress(StressMode::Smoke)
                    .runtimes,
                &[flavor]
            );
        }
    }

    #[test]
    fn boundaries_and_smoke_isolation() {
        for iterations in ["6", "100000"] {
            let config = env(&[
                (STRESS_ITERATIONS, iterations),
                (STRESS_SEED, "18446744073709551615"),
            ])
            .stress(StressMode::Long);
            assert_eq!(config.iterations.to_string(), iterations);
            assert_eq!(config.seed, u64::MAX);
        }
        assert_eq!(
            env(&[(STRESS_ITERATIONS, "invalid")])
                .stress(StressMode::Smoke)
                .iterations,
            36
        );
        assert_eq!(
            env(&[(PROPERTY_CASES, "4294967295")]).property_cases(),
            u32::MAX
        );
        assert_eq!(
            env(&[(CGROUP_ROOT, "/tmp/羊 space")]).cgroup_root(),
            PathBuf::from("/tmp/羊 space")
        );
    }

    #[test]
    fn invalid_settings_identify_the_variable() {
        for (key, value) in [
            (STRESS_ITERATIONS, "5"),
            (STRESS_ITERATIONS, "100001"),
            (STRESS_SEED, "18446744073709551616"),
            (STRESS_SEED, "-1"),
            (STRESS_RUNTIME, "typo"),
            (PROPERTY_CASES, "0"),
            (PROPERTY_CASES, "4294967296"),
            (PROPERTY_CASES, ""),
            (CGROUP_ROOT, ""),
        ] {
            let panic = std::panic::catch_unwind(|| {
                let entries = [(key, value)];
                let config = env(&entries);
                match key {
                    PROPERTY_CASES => {
                        config.property_cases();
                    }
                    CGROUP_ROOT => {
                        config.cgroup_root();
                    }
                    _ => {
                        config.stress(StressMode::Long);
                    }
                }
            })
            .expect_err("invalid configuration must fail");
            let message = panic.downcast_ref::<String>().unwrap();
            assert!(message.contains(key), "{message}");
        }
    }

    #[test]
    #[should_panic(expected = "SHEPHERD_CGROUP_ROOT")]
    fn privileged_tests_require_explicit_root() {
        env(&[]).cgroup_root();
    }

    #[cfg(unix)]
    #[test]
    fn native_paths_survive_but_numeric_non_unicode_fails() {
        use std::os::unix::ffi::OsStringExt;
        let raw = OsString::from_vec(vec![b'/', 0xff]);
        let config = TestEnvironment {
            lookup: |_: &str| Some(raw.clone()),
        };
        assert_eq!(config.cgroup_root(), PathBuf::from(&raw));
        assert!(std::panic::catch_unwind(|| config.property_cases()).is_err());
    }
}
