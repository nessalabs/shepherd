# 0005 — Failed backend wait is `CleanupUnverified(ReapFailed)`

## Status

Accepted (implemented). Addresses Codex P1 on `supervisor.rs`: a failed
`ProcessBackend::wait` was turned into an empty `RawExit` and then reported as
`ExitedNaturally` / `GracefulSuccess`.

## Context

`wait` failing (lost child handle, waiter channel closed) does **not** mean the
process exited. Recording a verified outcome would prune ownership and lie to
callers.

## Decision

If `wait` returns `Err`, the monitor records
`TerminationOutcome::CleanupUnverified(UnverifiedReason::ReapFailed)`, wakes
`wait(pid)` callers with that outcome, and does not invent a verified exit.
OS-level bookkeeping stays with the backend / `hard_kill_all` Drop path.

## Consequences

- Callers can branch on `CleanupUnverified` instead of treating a lost handle as
  success.
- Covered by a portable contract test (`WaitFailsBackend`).
