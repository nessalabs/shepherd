# 0001 — Application-owned driven ports

## Status

Accepted (implemented).

## Context

The original design sketched ports (`ProcessBackend`, `Clock`, `Waiters`, …) as
domain-owned traits. Those ports are inherently async and require trait objects
(`async-trait`), which would pull a runtime into `shepherd-domain`.

## Decision

Keep `shepherd-domain` a zero-async, `thiserror`-only crate. Define the driven
ports in `shepherd-app`. Infrastructure adapters implement them; the facade wires
them. The domain returns `Vec<DomainEvent>` from aggregate methods and never
performs I/O.

## Consequences

- Domain purity is a compile-time and CI fact (`ddd-architecture` job).
- This is a deliberate refinement of the "domain-owned ports" sketch in
  `docs/DESIGN.md` §3.1.
