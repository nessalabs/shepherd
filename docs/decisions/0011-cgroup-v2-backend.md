# 0011 — cgroup v2 before exec, atomic kill, verified emptiness

Accepted; supersedes the cgroup deferral in 0006.

The Linux default attempts a private directory beneath the current cgroup v2
ancestor, then `/sys/fs/cgroup`. `SHEPHERD_CGROUP_ROOT` or
`UnixProcessBackend::with_cgroup_root` selects a delegated ancestor explicitly.
`UnixProcessBackend::new` remains an explicit process-group backend. The required
constructor returns an error rather than silently falling back.

Use the kernel's documented filesystem interface directly. We need only creation,
membership, kill, and emptiness; cgroups-rs would add controller/policy abstractions
we do not use. References: https://docs.kernel.org/admin-guide/cgroup-v2.html.

Each scope has one cgroup. Open cgroup.procs in the parent and write `0` in the
child's async-signal-safe pre_exec hook before user code can fork or call setsid.
A post-spawn move would leave an escape window. Failed membership fails exec.
Application scope operations serialize spawn against scope cleanup.

Require cgroup.kill and exercise it on an empty probe. A PID sweep on older
kernels cannot make the same atomic guarantee against concurrent forks, so those
hosts use ProcessGroup. Once selected, later cgroup setup failures reject spawn;
capabilities never silently degrade while processes are owned. This refines §17.

Keep the kill descriptor open for synchronous Drop. Explicit scope cleanup checks
`cgroup.events` for `populated 0`, then removes the scope directory. Failure or a
five-second timeout returns an error; a successful signal alone is insufficient.
Empty nested cgroups are removed bottom-up before the scope directory. Traversal
uses pinned directory descriptors and refuses symlinks; cgroup control files are
never unlinked. Kernel directory removal still fails if a group becomes populated.
Root children are separately waited and reaped. Descendant zombies belong to their
OS parent/reaper; emptiness means no live descendant, not that Shepherd can wait
for arbitrary non-child processes. Tests use a subreaper to verify their reaping.

`Capabilities` is available on the supervisor. Stats support is unchanged in this
phase. Process-group cleanup remains best effort and cannot verify escaped trees.
Cgroups do not automatically kill on supervisor SIGKILL; no such backstop is claimed.

Prerequisite fixes: preserve backend wait errors, retain waiter exits even before
subscription, start monitors before dispatch, and propagate final sweep failures.
