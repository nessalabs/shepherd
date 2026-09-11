# 0004 — Reuse-safe signaling

## Status

Accepted (implemented). Addresses Codex P1: `kill(pid)` ignored `ReuseToken` and
could signal a recycled PID.

## Context

After `waitpid` the kernel may reuse the numeric PID before the supervisor
monitor records the exit. Signaling by bare pid in that window can kill an
unrelated process.

`OsIdentity` already carries a `ReuseToken` (start time). The domain must not
hold file descriptors, so the reuse-safe handle lives in the Unix adapter.

## Decision

- **Linux:** `pidfd_open` at spawn; `pidfd_send_signal` under the backend lock
  so the fd cannot be closed underneath the syscall. The pidfd refers to that
  process instance even if the number is reused.
- **Fallback (no pidfd / non-Linux):** re-read start time and compare to the
  recorded token before `kill`. If it does not match or the proc is gone, treat
  as already exited. A remaining TOCTOU exists on this fallback and is reported
  honestly via `Capabilities` (process-group containment, not cgroup/pidfd).
- Child slots are keyed by `(pid, reuse_token)` so a recycled pid cannot
  overwrite an unreaped slot.

## Consequences

- Linux signaling is race-free against PID reuse when pidfd is available.
- macOS remains best-effort (start-time check + process group), matching the
  platform guarantee table.
