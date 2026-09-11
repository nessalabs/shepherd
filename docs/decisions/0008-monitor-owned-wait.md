# 0008 — Wait and reap start at spawn, not in a `ReaperHandler`

## Status

Accepted (implemented). Refines the `ReaperHandler` sketch in `docs/DESIGN.md` §3.4.

## Context

The design listed a `ReaperHandler` that, on `ProcessExited`, would call
`ProcessBackend` to reap and then raise `ProcessReaped`. That inverts the real
dependency: `waitpid` / Tokio `Child::wait` is the *source* of the exit fact, not a
reaction to it. If no task is already waiting, Shepherd cannot observe the exit and
cannot emit `ProcessExited` in the first place.

`Child` is also moved into the backend waiter so the application never holds a raw
handle ([0002](./0002-drop-hard-kill.md)). The wait future must be polled from spawn
until the child exits.

## Decision

- `ProcessSupervisor::spawn` starts a per-process monitor task that owns
  `ProcessBackend::wait`.
- On `Ok(RawExit)` the monitor records exit + reap on the aggregate and dispatches
  the resulting domain events.
- On `Err` it records `CleanupUnverified(ReapFailed)` ([0005](./0005-unverified-on-wait-failure.md)).
- Domain-event handlers remain focused and I/O-light: `WaitNotifierHandler`,
  `RegistryPruneHandler`, `IntegrationTranslator`. There is no `ReaperHandler`.
- Monitor tasks clone `Inner` only, never the user-facing `CleanupGuard`.

## Consequences

- Every successful spawn has a waiter before the caller returns. Zombies cannot
  accumulate because nobody started waiting.
- Handlers stay fake-testable (ports only). The wait I/O stays behind
  `ProcessBackend`.
- A later shared reaper *task* (one loop, many children) is compatible with this
  decision as long as wait still begins at spawn and handlers do not perform OS I/O.
