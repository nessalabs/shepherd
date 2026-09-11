//! The immutable specification for spawning a process.

use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

/// Immutable specification describing how to spawn a process.
///
/// Carries no product policy — only what the OS needs to start the process and how Shepherd
/// should handle its output and graceful termination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpec {
    /// The program to execute.
    pub program: OsString,
    /// Arguments passed to the program.
    pub args: Vec<OsString>,
    /// Environment-variable policy.
    pub env: EnvPolicy,
    /// Working directory, if overridden.
    pub cwd: Option<PathBuf>,
    /// Signal used for the graceful phase of termination.
    pub graceful_signal: Signal,
    /// How stdout/stderr are handled.
    pub output: OutputMode,
}

impl ProcessSpec {
    /// Starts building a spec for `program` with sensible defaults
    /// (graceful signal = SIGTERM, inherit env, discard output).
    #[must_use]
    pub fn new(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: EnvPolicy::Inherit,
            cwd: None,
            graceful_signal: Signal::Term,
            output: OutputMode::Discard,
        }
    }

    /// Appends a single argument.
    #[must_use]
    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Appends multiple arguments.
    #[must_use]
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Sets the environment policy.
    #[must_use]
    pub fn env(mut self, env: EnvPolicy) -> Self {
        self.env = env;
        self
    }

    /// Sets the working directory.
    #[must_use]
    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Sets the graceful-termination signal.
    #[must_use]
    pub fn graceful_signal(mut self, signal: Signal) -> Self {
        self.graceful_signal = signal;
        self
    }

    /// Sets the output mode.
    #[must_use]
    pub fn output(mut self, output: OutputMode) -> Self {
        self.output = output;
        self
    }
}

/// Environment-variable policy for a spawned process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvPolicy {
    /// Inherit the parent environment.
    Inherit,
    /// Inherit the parent environment, then apply these overrides (`None` removes a key).
    Overrides(Vec<(OsString, Option<OsString>)>),
    /// Start from an empty environment with exactly these entries.
    Clear(Vec<(OsString, OsString)>),
}

/// A termination signal.
///
/// On Windows these map onto Job Object soft-close / terminate semantics; only [`Signal::Kill`]
/// is universally deliverable there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// Graceful termination request (`SIGTERM` on Unix).
    Term,
    /// Forceful, uncatchable termination (`SIGKILL` on Unix).
    Kill,
    /// Interrupt (`SIGINT` on Unix).
    Interrupt,
    /// A raw platform signal number (Unix only).
    Custom(i32),
}

/// The grace period to wait after a graceful request before escalating to force.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GracePeriod(Duration);

impl GracePeriod {
    /// Creates a grace period.
    #[must_use]
    pub const fn new(duration: Duration) -> Self {
        Self(duration)
    }

    /// Returns the underlying duration.
    #[must_use]
    pub const fn as_duration(self) -> Duration {
        self.0
    }
}

impl Default for GracePeriod {
    fn default() -> Self {
        Self(Duration::from_secs(5))
    }
}

/// How stdout/stderr are handled. Bytes only; never assumes UTF-8.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Drain and discard output.
    Discard,
    /// Drain into a bounded queue (drop-oldest on overflow) the caller consumes, and keep a
    /// capped tail for post-mortem.
    Capture {
        /// Maximum bytes buffered for live consumption before dropping oldest.
        buffer_bytes: usize,
        /// Maximum bytes retained for the post-mortem tail.
        tail_bytes: usize,
    },
}

impl Default for OutputMode {
    fn default() -> Self {
        Self::Discard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_spec_has_sensible_defaults() {
        let spec = ProcessSpec::new("prog");
        assert_eq!(spec.program, OsString::from("prog"));
        assert!(spec.args.is_empty());
        assert_eq!(spec.env, EnvPolicy::Inherit);
        assert_eq!(spec.cwd, None);
        assert_eq!(spec.graceful_signal, Signal::Term);
        assert_eq!(spec.output, OutputMode::Discard);
    }

    #[test]
    fn builder_sets_every_field() {
        let spec = ProcessSpec::new("prog")
            .arg("one")
            .args(["two", "three"])
            .env(EnvPolicy::Clear(vec![(
                OsString::from("K"),
                OsString::from("V"),
            )]))
            .cwd("/tmp")
            .graceful_signal(Signal::Interrupt)
            .output(OutputMode::Capture {
                buffer_bytes: 10,
                tail_bytes: 5,
            });
        assert_eq!(
            spec.args,
            vec![
                OsString::from("one"),
                OsString::from("two"),
                OsString::from("three")
            ]
        );
        assert!(matches!(spec.env, EnvPolicy::Clear(_)));
        assert_eq!(spec.cwd, Some(std::path::PathBuf::from("/tmp")));
        assert_eq!(spec.graceful_signal, Signal::Interrupt);
        assert!(matches!(spec.output, OutputMode::Capture { .. }));
    }

    #[test]
    fn env_policy_variants() {
        let overrides = EnvPolicy::Overrides(vec![
            (OsString::from("A"), Some(OsString::from("1"))),
            (OsString::from("B"), None),
        ]);
        assert!(matches!(overrides, EnvPolicy::Overrides(ref v) if v.len() == 2));
    }

    #[test]
    fn signal_variants_are_distinct() {
        assert_ne!(Signal::Term, Signal::Kill);
        assert_ne!(Signal::Interrupt, Signal::Custom(9));
        assert_eq!(Signal::Custom(3), Signal::Custom(3));
    }

    #[test]
    fn grace_period_roundtrips_and_defaults() {
        let g = GracePeriod::new(Duration::from_millis(250));
        assert_eq!(g.as_duration(), Duration::from_millis(250));
        assert_eq!(GracePeriod::default().as_duration(), Duration::from_secs(5));
    }

    #[test]
    fn output_mode_default_is_discard() {
        assert_eq!(OutputMode::default(), OutputMode::Discard);
    }
}
