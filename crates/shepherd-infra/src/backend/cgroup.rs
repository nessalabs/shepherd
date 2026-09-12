//! Direct cgroup v2 filesystem adapter. Only cgroup.kill is accepted: a PID sweep
//! cannot establish the same atomic containment guarantee during concurrent forks.
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use shepherd_domain::ProcessScopeId;

static NEXT: AtomicU64 = AtomicU64::new(1);
struct Group {
    path: PathBuf,
    kill: File,
}
impl Group {
    fn create(path: PathBuf) -> io::Result<Self> {
        fs::create_dir(&path)?;
        match OpenOptions::new()
            .write(true)
            .open(path.join("cgroup.kill"))
        {
            Ok(kill) => Ok(Self { path, kill }),
            Err(e) => {
                let _ = fs::remove_dir(path);
                Err(e)
            }
        }
    }
    fn kill(&self) -> io::Result<()> {
        (&self.kill).write_all(b"1")
    }
}
impl Drop for Group {
    fn drop(&mut self) {
        let _ = self.kill();
        let _ = fs::remove_dir(&self.path);
    }
}
pub(super) struct Cgroups {
    root: PathBuf,
    groups: Mutex<HashMap<ProcessScopeId, Group>>,
}
impl Cgroups {
    pub fn detect() -> io::Result<Self> {
        if let Some(root) = std::env::var_os("SHEPHERD_CGROUP_ROOT") {
            return Self::new(Path::new(&root));
        }
        let membership = fs::read_to_string("/proc/self/cgroup")?;
        let relative = membership
            .lines()
            .find_map(|l| l.strip_prefix("0::"))
            .ok_or_else(|| io::Error::other("not in cgroup v2"))?;
        // Check the current delegated ancestor first, then the conventional mount root.
        // Unusual mount layouts require the explicit constructor / environment override.
        Self::new(&Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/')))
            .or_else(|_| Self::new(Path::new("/sys/fs/cgroup")))
    }
    pub fn new(ancestor: &Path) -> io::Result<Self> {
        let probe = File::open(ancestor)?;
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        use std::os::fd::AsRawFd;
        // SAFETY: valid open descriptor and correctly sized output buffer.
        if unsafe { libc::fstatfs(probe.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { stat.assume_init() }.f_type != 0x6367_7270 {
            return Err(io::Error::other("ancestor is not cgroup v2"));
        }
        let root = ancestor.join(format!(
            "shepherd-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let group = Group::create(root.clone())?;
        // Exercise the actual kill interface while empty. This is a capability probe.
        group.kill()?;
        let child = Group::create(root.join("probe"))?;
        OpenOptions::new()
            .write(true)
            .open(child.path.join("cgroup.procs"))?;
        drop(child);
        // The supervisor root is an empty organizational directory, not a scope.
        drop(group);
        fs::create_dir(&root)?;
        Ok(Self {
            root,
            groups: Mutex::new(HashMap::new()),
        })
    }
    pub fn membership(&self, scope: ProcessScopeId) -> io::Result<File> {
        let mut groups = self.groups.lock().expect("cgroup mutex");
        if let std::collections::hash_map::Entry::Vacant(entry) = groups.entry(scope) {
            entry.insert(Group::create(self.root.join(format!("scope-{scope}")))?);
        }
        OpenOptions::new()
            .write(true)
            .open(groups[&scope].path.join("cgroup.procs"))
    }
    pub fn kill(&self, scope: ProcessScopeId) -> io::Result<()> {
        match self.groups.lock().expect("cgroup mutex").get(&scope) {
            Some(group) => group.kill(),
            None => Ok(()),
        }
    }
    pub fn kill_all(&self) {
        for group in self.groups.lock().expect("cgroup mutex").values() {
            if let Err(error) = group.kill() {
                tracing::error!(%error, "cgroup hard kill failed");
            }
        }
    }
    pub async fn finish(&self, scope: ProcessScopeId) -> io::Result<()> {
        for _ in 0..500 {
            {
                let mut groups = self.groups.lock().expect("cgroup mutex");
                let Some(group) = groups.get(&scope) else {
                    return Ok(());
                };
                let events = fs::read_to_string(group.path.join("cgroup.events"))?;
                if events.lines().any(|l| l == "populated 0") {
                    fs::remove_dir(&group.path)?;
                    groups.remove(&scope);
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "cgroup remains populated after kill",
        ))
    }
}
impl Drop for Cgroups {
    fn drop(&mut self) {
        self.kill_all();
        self.groups.get_mut().expect("cgroup mutex").clear();
        let _ = fs::remove_dir(&self.root);
    }
}
