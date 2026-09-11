# shepherd

A reusable, production-quality **process supervision** library for Rust. Shepherd provides
*mechanism* — spawning, ownership by explicit scopes, verified termination, reaping, and
resource observation — and leaves *policy* to the caller.

> Status: early implementation. The pure domain, application orchestration, a deterministic
> test backend, and a real Unix (process-group) backend are in place and tested. Linux
> cgroup v2, macOS, and Windows backends, plus streaming stats and output plumbing, are on
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

## Development

```sh
cargo test --workspace                                   # unit + contract + integration
cargo clippy --workspace --all-targets -- -D warnings    # lint
cargo fmt --all --check                                  # format
( cd tools/ddd-arch-check && node --experimental-strip-types check.ts )   # DDD boundaries
```

## Platform guarantees (current)

| Capability | Linux (process group) | macOS | Windows |
| --- | --- | --- | --- |
| Root process tracking | yes | yes | (null backend) |
| Descendant cleanup | best-effort (`killpg`) | best-effort | — |
| Force termination | yes | yes | — |
| RSS / peak RSS stats | yes (`/proc`) | — | — |

Guarantees are reported at runtime through the `Capabilities` type. See the design doc for
the full target matrix (including Linux cgroup v2, Windows Job Objects, and honest macOS
limitations).

## License

Apache-2.0.
