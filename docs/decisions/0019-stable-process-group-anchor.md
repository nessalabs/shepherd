# 0019 — Pin a process group across root exits with a private anchor

Accepted; supersedes the group-recreation rule in 0003.

A scope must not forget descendants when its last tracked root exits and another
root is spawned. Nor may Drop signal a numeric PGID after the group has disappeared
and that number has been reused. Keep one private group leader alive for the scope.

On the process-group backend, /bin/sh runs a single builtin read from a private pipe.
It creates no subsidiary sleep process. Its PID pins the PGID; roots join the same
group even after all prior roots exit. The input pipe is never exposed. The anchor
has an independent Tokio wait task; explicit cleanup kills the group, verifies
anchor reap, then removes the group and spawn lock. The waiter holds only a Weak
state reference between polls, so it cannot retain its own input pipe indefinitely. Failed root spawn leaves the
anchor owned by its open scope until cleanup. Cgroup scopes do not need an anchor.

This costs one small process and pipe per open process-group scope. It preserves one
containment resource per scope and makes late group sweeps identity-safe under the
normal ownership contract. The anchor ignores HUP/INT/TERM before exec, so a child's
HUP/INT/TERM group broadcasts cannot destroy the pin. Its wait poll serializes reap
with group signaling and setpgid, publishing pin loss before releasing the lock.
A guard owns the anchor Child before task dispatch, including cancellation before
first poll: interruption kills its still-pinned group and publishes unverified pin
loss before Tokio Child Drop can reap. Anchor/group/lock removal is atomic. Once a
whole-group kill is issued, new admission is rejected before and after anchor reap.

After an uncatchable signal kills the anchor, a later spawn recreates it only when
all roots have exited and the old numeric group is absent. If an unpinned group
still exists, joining or signaling it could affect a reused group ID: those operations
return an explicit error. The synchronous backstop still targets registered roots
using their retained identities. A previously issued whole-group kill needs no
second signal after its anchor reaps. Cleanup may remain unverified when descendants
survive external anchor loss; process groups cannot solve that identity gap.

Tests exercise TERM and KILL broadcasts followed by scope reuse, simulate a recycled
PGID without exhausting the host PID namespace, and check that the lost-anchor
backstop still kills owned roots. setsid descendants still escape on process-group
hosts; no stronger capability is advertised.

Individual signaling requires a registered slot and uses pidfd on Linux. Without a
pidfd it requires a matching start time. Unavailable metadata returns an error, and
invalid custom signals are rejected rather than silently converted to SIGTERM.
The macOS check-to-signal race remains an explicit OS limit.
