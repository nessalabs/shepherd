# shepherd

A reusable, production-quality **process supervision** library for Rust. Shepherd provides
*mechanism* — spawning, ownership by explicit scopes, verified termination, reaping, and
resource observation — and leaves *policy* to the caller.

> Status: platform adapters, cached statistics, and bounded byte capture are implemented.
> Cancellation guards and adversarial hardening are still in progress. See the design and
> phase PRs for evidence; platform limits are exposed explicitly.

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
- [`docs/decisions/`](docs/decisions/) — implementation ADRs (ports, Drop kill, groups, signaling, wait failure, deferred crates, publishers, monitors, toolchain).

## Development

```sh
cargo test --workspace                                   # unit + contract + integration
cargo clippy --workspace --all-targets -- -D warnings    # lint
cargo fmt --all --check                                  # format
( cd tools/ddd-arch-check && node --experimental-strip-types check.ts )   # DDD boundaries
```

## Platform guarantees (current)

| Capability | Linux cgroup v2 / fallback | macOS | Windows |
| --- | --- | --- | --- |
| Roots waited/reaped | yes | yes | yes |
| Detached containment | yes / no | no | yes (Job Object) |
| Force cleanup | cgroup.kill / best-effort killpg | best-effort killpg | Job Object |
| Cached CPU / RSS | yes | yes (libproc) | yes (process handles) |
| Cached peak RSS / I/O | yes | Unsupported | yes |


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

Capture bytes with `ProcessSpec::output(OutputMode::Capture { buffer_bytes: 65536,
tail_bytes: 4096 })`. Call `supervisor.take_output(pid)` once; its observer
`read()` drains the bounded queue and returns the tail, dropped-byte count, and
EOF/error status. Both pipes share the byte budget. Retaining the observer never
keeps a process alive.
