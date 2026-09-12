# 0010 — Retain the child slot until `wait` consumes the exit

## Status

Accepted (implemented). Fixes macOS CI: `graceful_termination_of_a_real_process` and
`scope_can_be_reused_after_natural_exit` reported `CleanupUnverified(ReapFailed)`.

## Context

The Unix waiter task owns `tokio::process::Child` and records `RawExit` on a
`watch` channel. It then called `note_child_exited`, which **removed the slot**
(and dropped the sender) as soon as the OS child was reaped.

`ProcessSupervisor::spawn` returns after `tokio::spawn`ing the monitor. The
monitor is not guaranteed to have entered `ProcessBackend::wait` before the
caller signals the child. On a fast SIGTERM (macOS hosted runners especially)
the sequence was:

1. Waiter reaps, `send(Some(exit))`, remove slot.
2. Monitor looks up the slot → `WaitError::Backend("no child registered")`.
3. Per [0005](./0005-unverified-on-wait-failure.md) that becomes
   `CleanupUnverified(ReapFailed)` — a lie; the child *was* reaped.

Reproduced on Linux with a 150 ms delay between `spawn("true")` and `wait`.

## Decision

- On OS exit, decrement the scope's `live` count only (so the next spawn creates
  a fresh process group). Do **not** drop the slot.
- `wait` subscribes (or reads the already-sent value) and **then** removes the
  slot.
- A `wait` for an unknown key is still an error — that is a real lost handle.

## Consequences

- A late monitor still observes a verified exit.
- Slots for processes whose monitor never runs stay until the backend is
  dropped. The monitor is started on every successful spawn, so this is only
  the Drop / abandoned-backend path.
- Covered by `late_wait_after_natural_exit_still_sees_status` in
  `shepherd-infra`.
