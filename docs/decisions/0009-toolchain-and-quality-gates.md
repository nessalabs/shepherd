# 0009 — Toolchain and quality gates for this implementation

## Status

Accepted (implemented). Closes the open items in `docs/DESIGN.md` §16 that this
phase had to decide.

## Context

The design left edition (2021 vs 2024), fitness-function language (TypeScript vs
Rust `xtask`), and the exact CI bar unspecified. Newer stable clippy also started
failing the lint job on lints that 1.83 does not emit (`derivable_impls` on
`OutputMode`), which made CI non-reproducible against the declared MSRV.

The domain crate is the only place the nine invariants can be unit-tested without
an OS. A coverage hole there is a hole in the model.

## Decision

| Choice | Value |
| --- | --- |
| Edition | 2021 (workspace) |
| MSRV | 1.83 |
| Lint CI toolchain | pinned `dtolnay/rust-toolchain@1.83.0` (rustfmt + clippy) |
| Test / coverage CI toolchain | latest stable |
| Fitness functions | TypeScript at `tools/ddd-arch-check/` (`node --experimental-strip-types`, Node 22) |
| Domain coverage | `cargo llvm-cov -p shepherd-domain --fail-under-lines 100` |

Edition 2024 and a Rust `xtask` rewrite of the fitness functions remain possible
later; they are not required to keep the domain pure.

## Consequences

- Local `cargo clippy` on 1.83 matches CI. Developers on newer rustc may see extra
  lints; CI does not.
- Domain line coverage is a merge gate. New aggregate behavior needs tests in
  `shepherd-domain` or CI fails.
- The DDD check has no Rust toolchain dependency beyond `cargo metadata`.
