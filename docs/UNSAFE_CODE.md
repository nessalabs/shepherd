# Unsafe code in Shepherd

Most of Shepherd is safe Rust. The domain, application, public facade, and test
configuration crates forbid unsafe code. The platform adapters still need a small
set of native operations that Rust's standard library does not provide safely.

An `unsafe` block means we must uphold an extra contract the compiler cannot check.
It does not mean every operation in the library is unsafe to call. Our public
supervision and observation APIs remain safe Rust APIs.

## What we replaced

This audit reduced project-owned Rust unsafe sites from **82 to 56**:

- Detached and orphan helper fixtures now use `std::process::Command` instead of
  raw `fork`. Safe `nix::unistd::setsid` supplies the required new session.
- Unix filesystem identification, clock frequency, process-group lookup, resource
  limits, child-subreaper setup, signals used for liveness, and explicit test reaping
  now use safe `nix` APIs.
- A test-only pre-exec process-group hook now uses `Command::process_group`.
- The macOS CPU oracle now uses safe `nix::getrusage` accessors.

These wrappers contain their own native implementation. We have reduced the unsafe
contracts maintained in this repository, not removed native code from dependencies.

## Required exceptions and their safeguards

**Unix launch and pidfds.** The anchor must ignore selected signals before it can
run. Linux children must enter their cgroup before their program can create helpers.
These operations require `pre_exec`, whose post-fork restrictions are explicitly
unsafe. Its closures use only stack data and async-signal-safe calls: no allocation,
logging, or Rust locks. A copied cgroup descriptor stays open until spawn completes.
Pidfd syscalls provide identity-preserving signaling; the new descriptor is validated
and transferred once into `OwnedFd`. Plain numeric-PID signaling is not an equivalent
replacement. Existing scope-isolation, cancellation, anchor-reuse, and privileged
cgroup tests cover these paths.

**macOS measurements.** `proc_pid_rusage`, PID-group enumeration, and Mach timebase
queries require native ABI buffers. Records are initialized, correctly aligned and
sized for the requested version; errors are checked. PID lists grow within a fixed
limit rather than silently truncating. CPU conversion and identity checks happen
in safe code. Standard Rust has no equivalent resource-accounting API. Intel and
Apple Silicon CI checks version fallback, CPU units, memory, identity changes, and
membership growth.

**Windows jobs and process handles.** The standard library has no Job Object API.
The adapter needs create/assign/query/terminate operations, suspended-thread resume,
and process counters. Successful native handles are validated and owned by RAII;
queries retain a live handle and match output structures to their native byte sizes.
The child stays suspended until assignment succeeds. Safety comments identify the
contract at each call. Native Windows CI tests job isolation, suspended creation,
owner-drop cleanup, observations, and resource growth.

**Test-only probes.** Two output fixtures deliberately ignore SIGTERM to force the
escalation path; they install `SIG_IGN`, never a Rust signal callback. Windows tests
query owned process handles to independently verify exit. Resource helpers count
handles and inspect bounded native diagnostic tables. The diagnostic parser checks
entry counts, string bounds, UTF-16 length, and pointer alignment before making Rust
references. Those helpers observe handles; they do not close handles obtained from
a diagnostic snapshot.

## Rust inventory

The counts below are unsafe keywords in Rust code, excluding comments and literals.
They include zero-initializing native records and test-only code. Counts are a change
review aid, not a measure of risk. Every retained location belongs to an exception
above and also has a local safety comment.

| File | Sites | Justification and source |
| --- | ---: | --- |
| `crates/shepherd-infra/src/backend/unix.rs` | 6 | Pre-exec containment setup, owned pidfd syscalls, and Mach timebase conversion. [Source](../crates/shepherd-infra/src/backend/unix.rs) |
| `crates/shepherd-infra/src/backend/windows.rs` | 33 | Job/process/thread FFI, initialized ABI records, owned-handle transfer, and native lifecycle tests. [Source](../crates/shepherd-infra/src/backend/windows.rs) |
| `crates/shepherd-infra/src/usage/macos.rs` | 4 | Versioned rusage buffer, Mach timebase, and bounded PID-group enumeration. [Source](../crates/shepherd-infra/src/usage/macos.rs) |
| `crates/shepherd-infra/src/usage/windows.rs` | 1 | Read-only process handle and correctly sized time/memory outputs. [Source](../crates/shepherd-infra/src/usage/windows.rs) |
| `fixtures/src/bin/ignore_sigterm.rs` | 1 | Install SIG_IGN to provide a stubborn child for escalation tests. [Source](../fixtures/src/bin/ignore_sigterm.rs) |
| `fixtures/src/bin/output_flood.rs` | 1 | Install SIG_IGN so inherited-pipe cleanup tests exercise forced termination. [Source](../fixtures/src/bin/output_flood.rs) |
| `fixtures/tests/support/resources.rs` | 1 | Read-only process handle count for leak assertions; writable scalar output. [Source](../fixtures/tests/support/resources.rs) |
| `fixtures/tests/support/windows_handles.rs` | 6 | Bounded native handle/type tables, with alignment and range checks before dereference. [Source](../fixtures/tests/support/windows_handles.rs) |
| `fixtures/tests/windows_integration.rs` | 3 | Independently observe the test descendant through a retained native handle. [Source](../fixtures/tests/windows_integration.rs) |

The [C rusage probe](research/macos-rusage-probe.c) is an independent SDK/ABI check
compiled only for CI and research. It creates and reaps its own child, uses stack
records and checked allocations, and is not linked into the Rust library. Its C
operations are outside the Rust keyword counts above.

## Keeping this current

[AGENTS.md](../AGENTS.md) requires a safe alternative to be considered first. A needed
unsafe change must update this register, explain its invariants locally, and retain
or add a boundary test. Run:

```sh
python3 tools/check_unsafe.py
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

CI runs the inventory check and safety-comment lint, including native macOS and
Windows configurations. It also rejects implicit unsafe operations inside unsafe
functions. The inventory checker is a lexical change detector, not a Rust soundness
proof. Comments and passing tests cannot prove a foreign-function contract correct;
review must check the actual ownership, lifetimes, sizes, and platform behavior.
Dependencies' internal unsafe code is not audited by this register.
