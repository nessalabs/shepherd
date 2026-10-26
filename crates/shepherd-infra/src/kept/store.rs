//! Kept records as files in one caller-chosen directory.
//!
//! `<name>.json` holds the record and `<name>.lock` the owner claim (an exclusive
//! `flock`, which the kernel drops when its owner dies). Records are replaced by
//! write-to-temporary, `fsync`, `rename`, `fsync` of the directory, so a reader sees the
//! old record or the new one, never part of one. Lock files are never deleted:
//! unlinking a file other owners may be locking would let two owners hold one name.

use std::fs::{DirBuilder, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use serde::{Deserialize, Serialize};
use shepherd_app::kept::{KeptError, KeptLock, KeptStore};
use shepherd_domain::{KeptName, KeptRecord, Signal, SpecFingerprint};

const RECORD: &str = ".json";
const LOCK: &str = ".lock";
const FORMAT: u32 = 1;

/// A [`KeptStore`] in one directory, created with owner-only access when missing.
#[derive(Debug, Clone)]
pub struct DirectoryKeptStore {
    dir: PathBuf,
}

impl DirectoryKeptStore {
    /// Opens (creating if needed) the store directory.
    ///
    /// # Errors
    /// The directory cannot be created.
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
        Ok(Self { dir })
    }

    /// The store directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn path(&self, name: &KeptName, suffix: &str) -> PathBuf {
        self.dir.join(format!("{name}{suffix}"))
    }
}

fn store_error(context: &str, error: impl std::fmt::Display) -> KeptError {
    KeptError::Store(format!("{context}: {error}"))
}

impl KeptStore for DirectoryKeptStore {
    fn lock(&self, name: &KeptName) -> Result<Option<KeptLock>, KeptError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(self.path(name, LOCK))
            .map_err(|e| store_error("open lock", e))?;
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => Ok(Some(KeptLock::new(name.clone(), Box::new(lock)))),
            Err((_, Errno::EWOULDBLOCK)) => Ok(None),
            Err((_, error)) => Err(store_error("lock", error)),
        }
    }

    fn read(&self, name: &KeptName) -> Result<Option<KeptRecord>, KeptError> {
        let bytes = match std::fs::read(self.path(name, RECORD)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(store_error("read record", error)),
        };
        let stored: StoredRecord =
            serde_json::from_slice(&bytes).map_err(|e| store_error("decode record", e))?;
        stored.into_record(name).map(Some)
    }

    fn write(&self, lock: &KeptLock, record: &KeptRecord) -> Result<(), KeptError> {
        if lock.name() != &record.name {
            return Err(KeptError::Store(format!(
                "claim for {} cannot write record {}",
                lock.name(),
                record.name
            )));
        }
        let bytes = serde_json::to_vec_pretty(&StoredRecord::from(record))
            .map_err(|e| store_error("encode record", e))?;
        // Names never start with '.', so a temporary can never shadow a record.
        let temporary = self.dir.join(format!(".{}{RECORD}.tmp", record.name));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|e| store_error("create record", e))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|e| store_error("write record", e))?;
        drop(file);
        std::fs::rename(&temporary, self.path(&record.name, RECORD))
            .map_err(|e| store_error("publish record", e))?;
        sync_dir(&self.dir)
    }

    fn remove(&self, lock: &KeptLock) -> Result<(), KeptError> {
        match std::fs::remove_file(self.path(lock.name(), RECORD)) {
            Ok(()) => sync_dir(&self.dir),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(store_error("remove record", error)),
        }
    }

    fn names(&self) -> Result<Vec<KeptName>, KeptError> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&self.dir).map_err(|e| store_error("list", e))? {
            let entry = entry.map_err(|e| store_error("list", e))?;
            let file = entry.file_name();
            let Some(stem) = file.to_str().and_then(|f| f.strip_suffix(RECORD)) else {
                continue;
            };
            if let Ok(name) = KeptName::new(stem) {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }
}

fn sync_dir(dir: &Path) -> Result<(), KeptError> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| store_error("sync directory", e))
}

#[derive(Serialize, Deserialize)]
struct StoredRecord {
    format: u32,
    name: String,
    os_pid: u32,
    start_token: u64,
    boot_id: Option<String>,
    exe: Option<PathBuf>,
    program: String,
    args: Vec<String>,
    fingerprint: String,
    revision: Option<String>,
    graceful_signal: StoredSignal,
    log: Option<PathBuf>,
    owner: String,
    started_at_unix_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredSignal {
    Term,
    Kill,
    Interrupt,
    Custom(i32),
}

impl From<&KeptRecord> for StoredRecord {
    fn from(record: &KeptRecord) -> Self {
        Self {
            format: FORMAT,
            name: record.name.to_string(),
            os_pid: record.os_pid,
            start_token: record.start_token,
            boot_id: record.boot_id.clone(),
            exe: record.exe.clone(),
            program: record.program.clone(),
            args: record.args.clone(),
            fingerprint: record.fingerprint.to_string(),
            revision: record.revision.clone(),
            graceful_signal: match record.graceful_signal {
                Signal::Term => StoredSignal::Term,
                Signal::Kill => StoredSignal::Kill,
                Signal::Interrupt => StoredSignal::Interrupt,
                Signal::Custom(raw) => StoredSignal::Custom(raw),
            },
            log: record.log.clone(),
            owner: record.owner.clone(),
            started_at_unix_ms: record.started_at_unix_ms,
        }
    }
}

impl StoredRecord {
    fn into_record(self, file_name: &KeptName) -> Result<KeptRecord, KeptError> {
        if self.format != FORMAT {
            return Err(KeptError::Store(format!(
                "record {file_name} has unknown format {}",
                self.format
            )));
        }
        let name = KeptName::new(self.name).map_err(|e| store_error("record name", e))?;
        if &name != file_name {
            return Err(KeptError::Store(format!(
                "record file {file_name} names {name}"
            )));
        }
        let fingerprint = u64::from_str_radix(&self.fingerprint, 16)
            .map_err(|e| store_error("record fingerprint", e))?;
        Ok(KeptRecord {
            name,
            os_pid: self.os_pid,
            start_token: self.start_token,
            boot_id: self.boot_id,
            exe: self.exe,
            program: self.program,
            args: self.args,
            fingerprint: SpecFingerprint::from_raw(fingerprint),
            revision: self.revision,
            graceful_signal: match self.graceful_signal {
                StoredSignal::Term => Signal::Term,
                StoredSignal::Kill => Signal::Kill,
                StoredSignal::Interrupt => Signal::Interrupt,
                StoredSignal::Custom(raw) => Signal::Custom(raw),
            },
            log: self.log,
            owner: self.owner,
            started_at_unix_ms: self.started_at_unix_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shepherd_domain::ProcessSpec;

    fn record(name: &str, signal: Signal) -> KeptRecord {
        KeptRecord {
            name: KeptName::new(name).unwrap(),
            os_pid: 41,
            start_token: 9,
            boot_id: Some("boot".into()),
            exe: Some("/bin/sleep".into()),
            program: "sleep".into(),
            args: vec!["30".into()],
            fingerprint: SpecFingerprint::of(&ProcessSpec::new("sleep"), Some("r")),
            revision: Some("r".into()),
            graceful_signal: signal,
            log: Some("/tmp/x.log".into()),
            owner: "test".into(),
            started_at_unix_ms: 5,
        }
    }

    fn scratch() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "shepherd-kept-store-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        dir.join("nested")
    }

    #[test]
    fn records_round_trip_and_list_by_name() {
        let dir = scratch();
        let store = DirectoryKeptStore::open(&dir).unwrap();
        assert_eq!(store.dir(), dir);
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let b = KeptName::new("b").unwrap();
        let a = KeptName::new("a").unwrap();
        let lock_b = store.lock(&b).unwrap().unwrap();
        let lock_a = store.lock(&a).unwrap().unwrap();
        for (lock, signal) in [
            (&lock_b, Signal::Custom(10)),
            (&lock_a, Signal::Interrupt),
        ] {
            let record = record(lock.name().as_str(), signal);
            store.write(lock, &record).unwrap();
            assert_eq!(store.read(lock.name()).unwrap(), Some(record));
        }
        for signal in [Signal::Term, Signal::Kill] {
            store.write(&lock_a, &record("a", signal)).unwrap();
            assert_eq!(store.read(&a).unwrap().unwrap().graceful_signal, signal);
        }
        std::fs::write(dir.join("not a name.json"), b"{}").unwrap();
        std::fs::write(dir.join("other.txt"), b"").unwrap();
        assert_eq!(store.names().unwrap(), vec![a.clone(), b.clone()]);
        let wrong = record("b", Signal::Term);
        assert!(matches!(store.write(&lock_a, &wrong), Err(KeptError::Store(_))));
        store.remove(&lock_a).unwrap();
        store.remove(&lock_a).unwrap();
        assert_eq!(store.read(&a).unwrap(), None);
        assert_eq!(store.names().unwrap(), vec![b]);
        std::fs::remove_dir_all(dir.parent().unwrap()).unwrap();
    }

    #[test]
    fn one_owner_per_name_until_the_claim_drops() {
        let dir = scratch();
        let store = DirectoryKeptStore::open(&dir).unwrap();
        let name = KeptName::new("tunnel").unwrap();
        let held = store.lock(&name).unwrap().unwrap();
        assert!(store.lock(&name).unwrap().is_none());
        assert!(DirectoryKeptStore::open(&dir)
            .unwrap()
            .lock(&name)
            .unwrap()
            .is_none());
        drop(held);
        assert!(store.lock(&name).unwrap().is_some());
        std::fs::remove_dir_all(dir.parent().unwrap()).unwrap();
    }

    #[test]
    fn damaged_or_foreign_records_are_errors_not_absences() {
        let dir = scratch();
        let store = DirectoryKeptStore::open(&dir).unwrap();
        let name = KeptName::new("a").unwrap();
        let lock = store.lock(&name).unwrap().unwrap();
        store.write(&lock, &record("a", Signal::Term)).unwrap();
        let path = dir.join("a.json");
        let good = std::fs::read_to_string(&path).unwrap();
        for bad in [
            "{".to_string(),
            good.replace("\"format\": 1", "\"format\": 2"),
            good.replace("\"name\": \"a\"", "\"name\": \"b\""),
            good.replace("\"name\": \"a\"", "\"name\": \".a\""),
            good.replace(
                &format!("\"{}\"", record("a", Signal::Term).fingerprint),
                "\"zz\"",
            ),
        ] {
            std::fs::write(&path, bad).unwrap();
            assert!(matches!(store.read(&name), Err(KeptError::Store(_))));
        }
        std::fs::remove_dir_all(dir.parent().unwrap()).unwrap();
    }
}
