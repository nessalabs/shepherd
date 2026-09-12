# Shepherd — implementation diagrams

## Dependencies and runtime ownership

```mermaid
flowchart TD
    F[Facade: SupervisorBuilder and public types] --> A[Application: ProcessSupervisor]
    F --> I[Infrastructure adapters]
    I --> P[Application-owned ports]
    A --> P
    A --> D[Pure domain: ProcessScope / Process / values / events]
    P --> D
    U[User-facing supervisor clones] --> G[Shared CleanupGuard]
    U --> N[Shared Inner]
    G --> K[Synchronous hard_kill_all on last Drop]
    N --> R[ScopeRegistry + per-scope operation locks]
    N --> M[Per-spawn wait monitor]
    N --> S[One shared interval sampler]
    N --> C[Bounded completed histories]
    M --> R
    S --> P
```

Workers retain Inner, never CleanupGuard. The sampler keeps a Weak reference between
intervals. Registry locks never span an await; scope-operation locks deliberately span
spawn and cleanup. The domain has no runtime, OS, filesystem, network or unsafe code.

## Scope lifecycle and verification

```mermaid
stateDiagram-v2
    [*] --> Open
    Open --> Open: spawn / natural root exit
    Open --> Draining: begin_scope_termination
    Draining --> Draining: failed reap quarantined
    Draining --> Closed: every root has verified terminal outcome
    Closed --> Released: successful containment sweep and root observation
    Draining --> Released: empty scope and verified containment cleanup
    Released --> [*]: completed report retained within bounded history
```

Root exit alone does not release containment: descendants can outlive their roots.
Process-group scopes retain a private anchor until explicit scope cleanup. Cgroup and
Job Object scopes retain their kernel resource until emptiness is verified.

## Block exit and cancellation

```mermaid
sequenceDiagram
    participant C as Caller future
    participant W as Scope cleanup worker
    participant S as Supervisor
    participant B as Backend
    participant M as Spawn monitor
    C->>S: with_scope(specs, closure)
    S->>W: await block-exit signal
    S->>B: serialized spawn into scope
    S->>M: start wait / reap
    S->>C: run closure with ScopedProcesses
    C-->>W: guard drops on return, error, panic or cancellation
    W->>S: terminate_scope
    S->>B: graceful per-root signals
    S->>B: force after grace
    M->>S: terminal outcome / honest reap failure
    S->>B: final scope sweep + containment verification
    S-->>W: ScopeTerminationReport or error
    W-->>C: retained report via normal result or wait_scope_cleanup
```

Dropping terminate() stops driving that invocation; it does not trigger an ownership
kill. Runtime shutdown of the scope worker invokes a scope-only synchronous backstop
and records an unverified error. A stopped runtime cannot prove asynchronous reap.

## Backend mechanisms

```mermaid
flowchart LR
    L[Linux default probe] -->|usable cgroup.kill| CG[cgroup per scope]
    L -->|unavailable| PG[Process group with private anchor]
    MAC[macOS] --> PG
    WIN[Windows] --> JOB[Job Object per scope]
    CG --> PRE[write cgroup.procs before exec]
    CG --> EMPTY[cgroup.kill then populated=0]
    PG --> GROUP[roots join stable PGID; killpg + anchor reap]
    JOB --> ASSIGN[create suspended; assign; resume]
    JOB --> ACTIVE[terminate job; ActiveProcesses=0; close]
```

Cgroups and Jobs contain detached descendants; groups do not. Cgroups do not have an
automatic creator-death kill. Job close protection starts at assignment.

## Observations and events

```mermaid
flowchart LR
    PIPE[stdout / stderr pipes] --> READ[Independent byte readers]
    READ --> QUEUE[Bounded combined queue + dropped-byte count]
    READ --> TAIL[Separately capped tail]
    QUEUE --> OUT[ProcessOutput.read]
    TAIL --> OUT
    SAMPLE[Interval backend samples] --> CACHE[Cache while root remains live]
    CACHE --> STATS[stats returns cache]
    REAP[Monitor terminal outcome] --> NOTIFY[WaitNotifierHandler]
    REAP --> PRUNE[RegistryPruneHandler: verified roots only]
    REAP --> TRANS[IntegrationTranslator]
    TRANS --> LOSS[Bounded queue + timeout]
    LOSS --> PUB[Optional external publisher; no-op default]
```

Capture never blocks on caller consumption. Discard uses the OS null device. Sample
and output errors do not relinquish process ownership. Inspect TerminationOutcome
before treating a terminal event as proof of reap.
