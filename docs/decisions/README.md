# Architecture Decision Records

Lightweight records of implementation choices that are not obvious from the code
alone. Each file is numbered and named for the decision, not the ticket.

| ID | Decision |
| --- | --- |
| [0001](./0001-application-owned-ports.md) | Driven ports live in `shepherd-app`, not the pure domain |
| [0002](./0002-drop-hard-kill.md) | Last supervisor handle Drop issues a synchronous hard-kill |
| [0003](./0003-serialized-scope-process-groups.md) | One process group per scope, created under a per-scope spawn lock |
| [0004](./0004-reuse-safe-signaling.md) | Signal via pidfd (Linux) / start-time check, never a bare recycled PID |
| [0005](./0005-unverified-on-wait-failure.md) | A failed `ProcessBackend::wait` is `CleanupUnverified(ReapFailed)` |
