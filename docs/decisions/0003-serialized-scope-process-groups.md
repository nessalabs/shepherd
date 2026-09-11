# 0003 — Serialize process-group creation per scope

## Status

Accepted (implemented). Addresses two Codex P1s: concurrent first-spawn creating
two groups, and a stale pgid after the last member exits naturally.

## Context

A Unix scope is one POSIX process group. The first spawn used `setpgid(0, 0)`
and recorded the child's pid as the pgid; later spawns joined that group. Two
bugs followed:

1. Two concurrent first-spawns both observed "no group" and both created a
   group; only one pgid was stored, so `signal_scope` could not sweep the other.
2. After the last process exited naturally the kernel destroyed the group, but
   the pgid stayed registered. The next spawn called `setpgid(0, stale_pgid)`
   and failed, so an open scope could not be reused.

## Decision

- Hold a per-scope `tokio::sync::Mutex` across spawn so only one child creates
  or joins the group at a time.
- Track `live` members on the group. When the last *tracked root* exits, keep
  the pgid so `signal_scope` can still sweep descendants that outlived the root.
  The next spawn into that scope sees `live == 0`, does not join the old group,
  and creates a fresh one.
- If joining a recorded group fails (ESRCH / stale), forget it and retry as a
  new group.

## Consequences

- Concurrent first-spawns share one group (covered by an integration test).
- Sequential reuse of an open scope after a natural exit works (covered).
- Spawns into the same scope are serialized. That is acceptable: spawn is
  already an OS call, and correctness of containment outranks parallelism.
