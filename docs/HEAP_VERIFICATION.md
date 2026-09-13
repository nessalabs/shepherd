# Checking heap memory lifetimes

Shepherd checks both lost memory and memory kept alive longer than needed. These
are different problems. A leak detector may consider a growing registry valid
because the application still has a reference to it.

## Lost allocations

The Linux LeakSanitizer CI job instruments Shepherd's real lifecycle, output,
cancellation, and observation tests. It checks for leaks when each test process
exits normally, after its runtimes and supervisors have been dropped. A reported
leak fails the job and includes the allocation stack.

Before testing Shepherd, CI runs a separate control program twice. The clean run
must succeed. The second run deliberately loses 37 bytes and must fail with a
LeakSanitizer report identifying those bytes. An unrelated crash or a disabled
detector cannot satisfy this check. No leak suppressions are configured.

This job uses a pinned nightly compiler on Linux. It does not claim equivalent
native leak-detector coverage on Windows or macOS, nor does it inspect memory in
children killed before their exit-time detector can run.

## Retained allocations

The isolated `heap_retention` test uses the development-only `dhat` allocator to
count live Rust heap bytes and allocations, rather than resident memory. Ordinary
allocator caching of freed memory does not count as a live allocation here.

By default, each Tokio runtime runs one supervisor through 512 warm-up process lifecycles,
then 1,024 measured lifecycles. Every child writes to both captured streams, exits,
and receives verified scope cleanup. The supervisor stays alive throughout. The
warm-up exceeds the 256-entry history limits; shutting down between cycles cannot
hide growing registries.

After every 256 measured cycles, live allocations must stay within 16 blocks and
64 KiB of the warm baseline. The byte allowance covers bounded history hash-table
rehashing: the initial investigation identified a later resize in the scope-report
table. The separate block limit catches even tiny per-cycle retained allocations.
These are fixed regression limits, not user settings or a promise of zero leaks.

The same predicate is checked against deliberately retained large storage and
32 tiny allocations. Both must be rejected. Releasing each control must bring it
back within the limits. These controls do not leak memory themselves.

Only this test executable uses `dhat`; consumers keep their own allocator. Native
allocations made outside Rust's global allocator are outside its measurements.
Failure profiles are retained as CI artifacts. Linux, macOS, and Windows run this
isolated test with both Tokio runtime flavors. It also requires final FD/handle
counts no higher than the warm baseline and no owned Unix zombies.

`TestEnvironment::heap()` centralizes workload sizing and runtime selection.
[Environment settings](TEST_ENVIRONMENT.md) document batch and timeout overrides;
the warm-up and batch sizes remain fixed to cover history eviction.

## Object ownership

Release checks test lifetimes directly:

- A captured output buffer must lose its final strong owner after verified cleanup
  and after the consumer drops its output handle.
- Successful supervisor shutdown must finish its sampler. Dropping the final
  supervisor clone must release its internal state and backend ownership.
- Cancelling a wait and dropping the waiter adapter must release its registry.

Weak references let tests observe release without keeping the object alive. Positive
controls retain a clone first, verifying that the check can distinguish a living
owner from a released one. Existing history-size and queue-bound tests still apply.

## Running locally

```sh
cargo test --locked -p shepherd-fixtures --release --test heap_retention -- --ignored --nocapture
cargo test --locked -p shepherd-app shutdown_releases_supervisor_sampler_and_backend_owners
cargo test --locked -p shepherd-infra cancelled_wait_does_not_retain_waiter_registry
```

The allocator test is ignored by the ordinary workspace run and explicitly required
by its own CI job. It must run alone because it counts the entire test process.

These checks provide evidence for the exercised paths, not a proof covering every
possible scheduling order. Prevent regressions by keeping histories bounded, avoiding
strong-reference cycles, joining background tasks, and testing failure and cancellation
cleanup. Keep the existing RSS, OS handle, descriptor, and zombie checks as well.

Tool references: [LeakSanitizer](https://clang.llvm.org/docs/LeakSanitizer.html),
[Rust sanitizer support](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html),
[dhat heap testing](https://docs.rs/dhat/0.3.3/dhat/).
