# 0019 — Pin a process group across root exits with a private anchor

Accepted; supersedes the group-recreation rule in 0003.

A scope must not forget descendants when its last tracked root exits and another
root is spawned. Nor may Drop signal a numeric PGID after the group has disappeared
and that number has been reused. Keep one private group leader alive for the scope.

On the process-group backend, /bin/sh runs a single builtin read from a private pipe.
It creates no subsidiary sleep process. Its PID pins the PGID; roots join the same
group even after all prior roots exit. The input pipe is never exposed. The anchor
has an independent Tokio wait task; explicit cleanup kills the group, verifies
anchor reap, then removes the group and spawn lock. Failed root spawn leaves the
anchor owned by its open scope until cleanup. Cgroup scopes do not need an anchor.

This costs one small process and pipe per open process-group scope. It preserves one
containment resource per scope and makes late group sweeps identity-safe under the
normal ownership contract. External actors killing the anchor or migrating group
members are outside that contract. setsid descendants still escape on process-group
hosts; no stronger capability is advertised.

Individual signaling requires a registered slot and uses pidfd on Linux. Without a
pidfd it requires a matching start time. Unavailable metadata returns an error, and
invalid custom signals are rejected rather than silently converted to SIGTERM.
The macOS check-to-signal race remains an explicit OS limit.
