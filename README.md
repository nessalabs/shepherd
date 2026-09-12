# shepherd

A reusable, production-quality **process supervision** library for Rust. Shepherd provides
*mechanism* — spawning, ownership by explicit scopes, verified termination, reaping, and
resource observation — and leaves *policy* to the caller.

> Status: early implementation. The pure domain, application orchestration, a deterministic
> test backend, and a real Unix (process-group) backend are in place and tested. Linux
> cgroup v2 is implemented with privileged tests; macOS statistics and Windows backends, plus streaming stats and output plumbing, remain on
> the roadmap (see [`docs/DESIGN.md`](docs/DESIGN.md)).

## The core invariant

> Every process Shepherd starts belongs to exactly one explicit lifetime scope until
> Shepherd has *confirmed* that process has exited and its resources have been reaped.

## Example

```rust
use shepherd::{ProcessSpec, SupervisorBuilder, TerminateOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = SupervisorBuilder::new().build();
    let scope = supervisor.create_scope();

    let _pid = supervisor
        .spawn(scope, ProcessSpec::new("sleep").arg("30"))
        .await?;

    // Verified two-phase teardown: graceful, then force if needed, then reap.
    let report = supervisor.terminate_scope(scope, TerminateOptions::default()).await?;
    assert!(report.all_verified());
    Ok(())
}
```

Scope creation is allowed only before shutdown starts. The existing `create_scope()`
method panics after that point; use `try_create_scope()` to handle
`ScopeCreationError::SupervisorClosed`, including creation racing with shutdown.
Rejected creation allocates no scope id or retained state.

## Architecture (Domain-Driven Design)

Shepherd is one bounded context split into layers with a strict *dependencies point inward*
rule, enforced in CI:

| Crate | Role |
| --- | --- |
| `shepherd-domain` | Pure domain: entities, value objects, the `ProcessScope` aggregate, events. No async, no OS, no I/O. |
| `shepherd-app` | Application services: the `ProcessSupervisor`, the event dispatcher + handlers, and the driven ports. |
| `shepherd-infra` | Adapters: the Unix/null backends, clock, waiters, integration publishers. |
| `shepherd` | The public facade that wires it together. |

The domain's purity, the layering, and the ubiquitous language are enforced by the
`tools/ddd-arch-check` fitness functions in CI.

## Documentation

- [`docs/DESIGN.md`](docs/DESIGN.md) — architecture and implementation plan.
- [`docs/DIAGRAMS.md`](docs/DIAGRAMS.md) — class and state diagrams.
- [`docs/GLOSSARY.md`](docs/GLOSSARY.md) — the ubiquitous language (enforced in CI).
- [`docs/decisions/`](docs/decisions/) — implementation ADRs (ports, Drop kill, groups, signaling, wait failure, deferred crates, publishers, monitors, toolchain).

## Development

```sh
cargo test --workspace                                   # unit + contract + integration
cargo clippy --workspace --all-targets -- -D warnings    # lint
cargo fmt --all --check                                  # format
( cd tools/ddd-arch-check && node --experimental-strip-types check.ts )   # DDD boundaries
```

## Platform guarantees (current)

| Capability | Linux (cgroup v2 when usable; else process group) | macOS | Windows |
| --- | --- | --- | --- |
| Root process tracking | yes | yes | (null backend) |
| Descendant cleanup | `cgroup.kill` with emptiness verification; fallback best-effort (`killpg`) | best-effort | — |
| Force termination | yes | yes | — |
| RSS / peak RSS stats | yes (cached `/proc`) | — | — |
| CPU / I/O stats | yes (cached per-root counters) | — | — |

Guarantees are reported at runtime through the `Capabilities` type. See the design doc for
the full target matrix (including Linux cgroup v2, Windows Job Objects, and honest macOS
limitations).

## License

Apache-2.0.

Linux selects containment at construction. Use `supervisor.capabilities()` to inspect
it. Privileged test requirements and commands: [ADR 0012](docs/decisions/0012-privileged-cgroup-ci.md).
Cgroup containment does not imply cleanup after abrupt supervisor SIGKILL.

Configure sampling with `.stats_interval(Duration::from_millis(250))` on the builder.
`stats(pid).await` returns the last interval observation (`StatsError::NotReady`
before the first sample). CPU 1.0 means one fully used core; uptime is at sample time.
