# Shepherd — Ubiquitous Language (Glossary)

This glossary is the agreed **ubiquitous language** of the Process Supervision bounded
context. It is **enforced**: the `ddd-architecture` CI job checks that every public
type/method in `shepherd-domain` appears here, and that every term here exists in code.
Drift in either direction fails the build. Keep this file in lockstep with the domain.

See [`DESIGN.md`](./DESIGN.md) for architecture and [`DIAGRAMS.md`](./DIAGRAMS.md) for
class/state diagrams.

## Building-block stereotypes

| Stereotype | Meaning in Shepherd |
| --- | --- |
| Aggregate Root | The consistency boundary; the only entry point for mutating what it owns. |
| Entity | Has identity and a lifecycle; only reachable through its aggregate root. |
| Value Object | Immutable, compared by value; carries no identity. |
| Domain Event | An immutable fact that happened in the domain; emitted at most once logically. |
| Repository | Collection-style access to aggregates. |
| Port | A trait the domain owns; implemented by an infrastructure adapter. |
| Adapter / ACL | Infrastructure implementation of a port; translates OS concepts to domain terms. |
| Application Service | Orchestrates the domain and drives ports; owns async. |

## Core terms

### ProcessSupervisor
**Application Service.** The top-level entry point an application constructs and owns.
Coordinates spawning, stats, termination, waiting, reaping, and shutdown across scopes.
Never a global singleton.

### ProcessScope
**Aggregate Root.** The unit of ownership and consistency boundary. Owns one or more root
`Process` entities and enforces the ownership invariants. `terminate_scope` acts on exactly
one scope and can never affect another.

### ProcessScopeId
**Value Object.** Opaque, supervisor-assigned identifier of a `ProcessScope`.

### Process
**Entity.** A single supervised OS process, identified by a `ProcessId`, living inside
exactly one `ProcessScope`. Never exposed as raw mutable child ownership.

### ProcessId
**Value Object.** Opaque, supervisor-assigned handle for a `Process`. Distinct from the OS
pid, to defeat PID reuse.

### OsIdentity
**Value Object.** The OS-level identity of a process: its pid plus a `ReuseToken`.

### ReuseToken
**Value Object.** A discriminator (pidfd / process start-time / OS handle) that detects OS
PID reuse so a recycled pid is never mistaken for a live owned process.

### ProcessSpec
**Value Object.** The immutable specification for spawning a process: program, args, env
policy, working directory, graceful signal, and output mode. Carries no product policy.

### EnvPolicy
**Value Object.** Explicit environment-variable policy for a spawned process.

### OutputMode
**Value Object.** How stdout/stderr are handled: bounded byte-chunk queue (drop-oldest with
a dropped-bytes counter) and/or capped tail capture. Bytes only; never assumes UTF-8.

### ProcessStats
**Value Object.** A point-in-time observation of a process: CPU usage, RSS, virtual memory,
peak RSS, I/O bytes, descendant count, uptime, state, and sample time. Observation only,
never a verdict.

### ProcessState
**Value Object.** The observed OS run state of a process at sample time (e.g. running,
sleeping, zombie).

### ProcessLifecycle
**Value Object (state machine).** The supervised lifecycle state of a `Process`: Spawning,
Running, GracefulRequested, Forcing, ExitedUnreaped, Reaped, SpawnFailed.

### ScopeState
**Value Object (state machine).** The lifecycle state of a `ProcessScope`: Open, Draining,
Closed. A non-Open scope rejects new processes.

### ProcessExit
**Value Object.** The recorded terminal result of a process: exit code, signal, whether
force was required, and the `TerminationOutcome`.

### TerminationOutcome
**Value Object (enum).** The verified result of a termination or exit:
`ExitedNaturally`, `GracefulSuccess`, `ForcedRequired`, `Failed`, `CleanupUnverified`.
Success is only reported after verified non-existence and reap.

### UnverifiedReason
**Value Object (enum).** Why cleanup could not be verified: `DroppedWithoutShutdown`,
`RuntimeShutdown`, `WaitTimedOut`, `ReapFailed`, `ProcessDisappeared`.

### Signal
**Value Object.** A termination signal (e.g. graceful SIGTERM, force SIGKILL). On Windows,
maps to Job Object soft-close / terminate semantics.

### GracePeriod
**Value Object.** The duration to wait after a graceful request before escalating to force.

### Capabilities
**Value Object.** The runtime-detected guarantees of a platform backend: descendant
containment strength, and support for CPU / RSS / peak RSS / I/O stats and force
termination. Guarantees are values, never prose.

### ScopeTerminationReport
**Value Object.** The aggregated per-process `TerminationOutcome`s produced by
`terminate_scope`.

### ShutdownReport
**Value Object.** The aggregated result of `shutdown` across all scopes.

## Domain events

### ProcessSpawned
**Domain Event.** A process was successfully spawned into a scope.

### TerminationRequested
**Domain Event.** Termination was requested for a process or scope.

### ProcessExited
**Domain Event.** A process was observed to have exited (before reaping).

### ProcessReaped
**Domain Event.** An exited process's OS resources were reaped; its terminal state is
confirmed.

### ScopeClosed
**Domain Event.** A scope reached the Closed state; all its processes are reaped.

## Ports (domain-owned traits)

### ProcessBackend
**Port.** The platform abstraction: spawn, sample, terminate, terminate_scope, reap, and
report capabilities. Implemented per OS in `shepherd-infra`.

### Clock
**Port.** Time source (now / sleep), so grace periods and sampling are deterministic in
tests.

### OutputSink
**Port.** Destination for drained stdout/stderr byte chunks.

## Repository

### ScopeRegistry
**Repository.** Collection-style access to `ProcessScope` aggregates held by a supervisor.

## Errors (typed)

### SpawnError
Typed error for failed process creation.

### TerminateError
Typed error for failed termination.

### StatsError
Typed error for failed stats collection.

### ReapError
Typed error for failed reaping.

### ShutdownError
Typed error for failed supervisor shutdown.

### ScopeClosed
Domain error returned when spawning into a Draining/Closed scope.

### UnknownProcess
Domain error for an unknown `ProcessId`.

### UnknownScope
Domain error for an unknown `ProcessScopeId`.
