//! Kept processes: opt-in processes that outlive their owner, owned by a durable record.
//!
//! The core invariant gives every process exactly one owner. A kept process's owner is
//! its [`KeptRecord`], written before the program runs, instead of a scope that dies with
//! the supervisor. This module holds only the pure parts: names, records, fingerprints
//! and the identity rule. Storage and the OS live behind application ports.

use std::path::{Path, PathBuf};

use crate::spec::{EnvPolicy, ProcessSpec, Signal};

/// The caller-chosen name of a kept process: 1 to 64 ASCII letters, digits, `.`, `_` or
/// `-`, not starting with `.`. Safe to use as a file name on every platform.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeptName(String);

impl KeptName {
    /// The longest accepted name, in bytes.
    pub const MAX_LEN: usize = 64;

    /// Validates `name`.
    ///
    /// # Errors
    /// [`KeptNameError`] when the name is empty, too long, starts with `.`, or contains a
    /// character outside the allowed set.
    pub fn new(name: impl Into<String>) -> Result<Self, KeptNameError> {
        let name = name.into();
        let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-');
        if name.is_empty()
            || name.len() > Self::MAX_LEN
            || name.starts_with('.')
            || !name.chars().all(allowed)
        {
            return Err(KeptNameError(name));
        }
        Ok(Self(name))
    }

    /// The name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl core::fmt::Display for KeptName {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A rejected [`KeptName`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid kept process name {0:?}: use 1-64 of [A-Za-z0-9._-], not starting with '.'")]
pub struct KeptNameError(pub String);

/// A stable 64-bit digest of what a process was started with: program, arguments,
/// environment policy, working directory, graceful signal and a caller revision.
///
/// It decides whether a running kept process still matches what its owner would start
/// now. The digest is FNV-1a over a length-prefixed encoding, so it is the same across
/// builds, Rust versions and machines. It is not a security boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpecFingerprint(u64);

impl SpecFingerprint {
    /// Digests `spec` plus an optional caller `revision` (for inputs the spec does not
    /// carry, such as the contents of a file named in the arguments).
    #[must_use]
    pub fn of(spec: &ProcessSpec, revision: Option<&str>) -> Self {
        let mut hash = Fnv::new();
        hash.text(&spec.program.to_string_lossy());
        hash.number(spec.args.len() as u64);
        for arg in &spec.args {
            hash.text(&arg.to_string_lossy());
        }
        match &spec.env {
            EnvPolicy::Inherit => hash.number(0),
            EnvPolicy::Overrides(entries) => {
                hash.number(1);
                hash.number(entries.len() as u64);
                for (key, value) in entries {
                    hash.text(&key.to_string_lossy());
                    match value {
                        Some(value) => {
                            hash.number(1);
                            hash.text(&value.to_string_lossy());
                        }
                        None => hash.number(0),
                    }
                }
            }
            EnvPolicy::Clear(entries) => {
                hash.number(2);
                hash.number(entries.len() as u64);
                for (key, value) in entries {
                    hash.text(&key.to_string_lossy());
                    hash.text(&value.to_string_lossy());
                }
            }
        }
        match &spec.cwd {
            Some(cwd) => {
                hash.number(1);
                hash.text(&cwd.to_string_lossy());
            }
            None => hash.number(0),
        }
        match spec.graceful_signal {
            Signal::Term => hash.number(0),
            Signal::Kill => hash.number(1),
            Signal::Interrupt => hash.number(2),
            Signal::Custom(raw) => {
                hash.number(3);
                hash.number(u64::from(raw.unsigned_abs()) | (u64::from(raw < 0) << 32));
            }
        }
        match revision {
            Some(revision) => {
                hash.number(1);
                hash.text(revision);
            }
            None => hash.number(0),
        }
        Self(hash.0)
    }

    /// Rebuilds a fingerprint from its raw value (as stored in a record).
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for SpecFingerprint {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

struct Fnv(u64);

impl Fnv {
    const fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
    fn bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    fn number(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }
    fn text(&mut self, text: &str) {
        self.number(text.len() as u64);
        self.bytes(text.as_bytes());
    }
}

/// What the OS reports about the process currently holding a recorded pid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptObservation {
    /// The OS start token of the process now at that pid.
    pub start_token: u64,
    /// The current boot, where the OS names one.
    pub boot_id: Option<String>,
    /// The executable the process now runs, when readable.
    pub exe: Option<PathBuf>,
}

/// The durable owner of one kept process.
///
/// Written before the program can run, so a live kept process is never without one.
/// Removed only after its process is confirmed gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptRecord {
    /// The caller-chosen name; one live record per name.
    pub name: KeptName,
    /// The OS pid. Also the process-group and session id: kept processes lead their own.
    pub os_pid: u32,
    /// The OS start token read when the record was written (pid-reuse discriminator).
    pub start_token: u64,
    /// The boot the process started in, where the OS names one (start tokens restart
    /// with each boot on Linux).
    pub boot_id: Option<String>,
    /// The executable the process runs, read after `exec`. `None` only if the owner
    /// stopped between `exec` and the record update.
    pub exe: Option<PathBuf>,
    /// The program as given in the spec (lossy text, for display).
    pub program: String,
    /// The arguments as given in the spec (lossy text, for display).
    pub args: Vec<String>,
    /// Digest of the spec and revision it was started with.
    pub fingerprint: SpecFingerprint,
    /// The caller revision folded into the fingerprint, if any.
    pub revision: Option<String>,
    /// The signal that asks it to stop gracefully.
    pub graceful_signal: Signal,
    /// The file stdout and stderr are appended to, if any.
    pub log: Option<PathBuf>,
    /// A caller-chosen label for the owning application.
    pub owner: String,
    /// Wall-clock start, in milliseconds since the Unix epoch.
    pub started_at_unix_ms: u64,
}

impl KeptRecord {
    /// Whether `observed` is the same process instance this record owns.
    ///
    /// The start token must match. A boot id or executable path is compared only when
    /// both sides have one, so a missing reading never makes a stranger look owned: the
    /// start token alone already rejects a reused pid.
    #[must_use]
    pub fn is_same_instance(&self, observed: &KeptObservation) -> bool {
        let same = |a: Option<&str>, b: Option<&str>| a.zip(b).map_or(true, |(a, b)| a == b);
        observed.start_token == self.start_token
            && same(self.boot_id.as_deref(), observed.boot_id.as_deref())
            && self
                .exe
                .as_deref()
                .zip(observed.exe.as_deref())
                .map_or(true, |(a, b): (&Path, &Path)| a == b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn names_accept_the_portable_set_only() {
        assert_eq!(KeptName::new("tunnel-1.quick_A").unwrap().as_str(), "tunnel-1.quick_A");
        assert_eq!(KeptName::new("a").unwrap().to_string(), "a");
        assert!(KeptName::new("x".repeat(64)).is_ok());
        for bad in ["", ".hidden", "a/b", "a b", "ü", "a\\b", "..", &"x".repeat(65)] {
            let error = KeptName::new(bad).unwrap_err();
            assert_eq!(error.0, bad);
            assert!(error.to_string().contains("invalid kept process name"));
        }
    }

    fn base() -> ProcessSpec {
        ProcessSpec::new("/bin/prog").args(["--a", "b"])
    }

    #[test]
    fn fingerprint_is_stable_and_covers_every_input() {
        let same = SpecFingerprint::of(&base(), None);
        assert_eq!(same, SpecFingerprint::of(&base(), None));
        assert_eq!(SpecFingerprint::from_raw(same.get()), same);
        assert_eq!(same.to_string().len(), 16);
        let variants = [
            SpecFingerprint::of(&ProcessSpec::new("/bin/other").args(["--a", "b"]), None),
            SpecFingerprint::of(&ProcessSpec::new("/bin/prog").args(["--ab"]), None),
            SpecFingerprint::of(&ProcessSpec::new("/bin/prog").args(["--a", "b", ""]), None),
            SpecFingerprint::of(
                &base().env(EnvPolicy::Overrides(vec![(OsString::from("K"), None)])),
                None,
            ),
            SpecFingerprint::of(
                &base().env(EnvPolicy::Overrides(vec![(
                    OsString::from("K"),
                    Some(OsString::from("")),
                )])),
                None,
            ),
            SpecFingerprint::of(&base().env(EnvPolicy::Clear(Vec::new())), None),
            SpecFingerprint::of(
                &base().env(EnvPolicy::Clear(vec![(
                    OsString::from("K"),
                    OsString::from("V"),
                )])),
                None,
            ),
            SpecFingerprint::of(&base().cwd("/tmp"), None),
            SpecFingerprint::of(&base().graceful_signal(Signal::Kill), None),
            SpecFingerprint::of(&base().graceful_signal(Signal::Interrupt), None),
            SpecFingerprint::of(&base().graceful_signal(Signal::Custom(1)), None),
            SpecFingerprint::of(&base().graceful_signal(Signal::Custom(-1)), None),
            SpecFingerprint::of(&base(), Some("")),
            SpecFingerprint::of(&base(), Some("token-2")),
        ];
        for (index, variant) in variants.iter().enumerate() {
            assert_ne!(*variant, same, "variant {index} must change the fingerprint");
            for other in &variants[index + 1..] {
                assert_ne!(variant, other);
            }
        }
    }

    fn record() -> KeptRecord {
        KeptRecord {
            name: KeptName::new("tunnel").unwrap(),
            os_pid: 42,
            start_token: 7,
            boot_id: Some("boot-a".into()),
            exe: Some(PathBuf::from("/bin/prog")),
            program: "/bin/prog".into(),
            args: vec!["--a".into()],
            fingerprint: SpecFingerprint::of(&base(), None),
            revision: None,
            graceful_signal: Signal::Term,
            log: None,
            owner: "test".into(),
            started_at_unix_ms: 1,
        }
    }

    #[test]
    fn identity_requires_the_start_token_and_any_shared_readings() {
        let record = record();
        let seen = |start_token, boot: Option<&str>, exe: Option<&str>| KeptObservation {
            start_token,
            boot_id: boot.map(Into::into),
            exe: exe.map(PathBuf::from),
        };
        assert!(record.is_same_instance(&seen(7, Some("boot-a"), Some("/bin/prog"))));
        assert!(record.is_same_instance(&seen(7, None, None)));
        assert!(!record.is_same_instance(&seen(8, Some("boot-a"), Some("/bin/prog"))));
        assert!(!record.is_same_instance(&seen(7, Some("boot-b"), Some("/bin/prog"))));
        assert!(!record.is_same_instance(&seen(7, Some("boot-a"), Some("/bin/sh"))));
        let unread = KeptRecord {
            boot_id: None,
            exe: None,
            ..record
        };
        assert!(unread.is_same_instance(&seen(7, Some("boot-b"), Some("/bin/sh"))));
    }
}
