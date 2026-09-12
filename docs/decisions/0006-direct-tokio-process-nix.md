# 0006 — Unix adapter is `tokio::process` + `nix`; `process-wrap` and `cgroups-rs` deferred

## Status

Accepted (implemented). Supersedes the "compose `process-wrap` first" sketch in
`docs/DESIGN.md` §2 for this phase only.

## Context

The design plan listed `process-wrap` (Tokio frontend, process groups, Job Objects,
kill-on-drop) and `cgroups-rs` (Linux cgroup v2) as the primitives to build on. Wiring
those crates before Shepherd's own ownership, wait, and Drop model existed would have
imported their kill-on-drop and wrapper semantics into a layer that is still settling
(see [0002](./0002-drop-hard-kill.md)).

Linux cgroup v2 is a later phase: hosted CI cannot yet run privileged `cgroup.kill`
tests, and the process-group path must be correct first.

## Decision

- The Unix `ProcessBackend` uses `tokio::process::Command` plus `nix` (`setpgid`,
  `kill`, `killpg`) and, on Linux, `pidfd_open` / `pidfd_send_signal`.
- `process-wrap` and `cgroups-rs` are **not** dependencies in this phase.
- The default facade backend is `UnixProcessBackend` on Unix and `NullBackend` on
  non-Unix (Windows included). We do not ship a stub Job Object adapter that would
  claim capabilities we have not implemented.
- `Capabilities` reports process-group containment, not cgroup / Job Object.

A later Linux cgroup v2 adapter may still compose `cgroups-rs` (or a direct
`/sys/fs/cgroup` fallback) as a `CommandWrapper` or a sibling backend. `process-wrap`
remains a candidate once Job Objects and kill-on-drop alignment are designed against
[0002](./0002-drop-hard-kill.md).

## Consequences

- Shepherd owns spawn, group membership, wait, and Drop kill. No hidden
  `Child::kill_on_drop` from a wrapper crate.
- Descendant containment is best-effort (`killpg`); a `setsid` child can escape. That
  is reported honestly and is the reason cgroup v2 is the next backend, not a silent
  upgrade of this one.
- Windows callers must inject a backend or accept the deterministic `NullBackend`
  until a Job Object adapter exists.
