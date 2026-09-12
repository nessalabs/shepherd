# Working on Shepherd

Prefer safe Rust. Use the standard library or an existing maintained safe wrapper
when it preserves the required behavior. Do not add an unsafe block merely for
convenience, performance guesses, or to silence a compiler error.

When an unsafe operation is strictly needed:

1. Keep it small and inside an infrastructure adapter or a narrowly scoped test
   helper. Domain, application, facade, and configuration code must stay safe.
2. Add a nearby `SAFETY:` comment explaining the actual invariants: pointer validity,
   buffer size/alignment, handle ownership/lifetime, or post-fork restrictions.
3. Add or update its entry in [the unsafe-code register](docs/UNSAFE_CODE.md), including
   why a safe alternative is insufficient and which tests exercise the boundary.
   This is the required justification document for every retained unsafe location.
4. Run the inventory check and platform Clippy jobs. Do not disable the safety-comment
   lint or broaden an unsafe block to avoid documenting a new operation.

The retained exceptions are native pre-exec/pidfd operations, macOS accounting,
Windows kernel handles, and specific test-only native probes. Their precise reasons
and file inventory are in the register. A safe wrapper moves the underlying unsafe
implementation into a dependency; it does not make OS behavior infallible.

For stress work, keep configurable values in `shepherd-test-support`, use bounded
fixtures, preserve strict FD/handle and owned-zombie assertions, and record seeds.
Do not treat an elapsed timeout as verified cleanup. Tests must cover the interaction
between observation, output, cancellation, admission, and shutdown, not only each
operation separately.
