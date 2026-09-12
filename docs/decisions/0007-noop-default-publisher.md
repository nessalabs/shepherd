# 0007 — Default integration publisher is a no-op

## Status

Accepted (implemented).

## Context

`docs/DESIGN.md` §3.4 requires an outbound `IntegrationEventPublisher` so a different
bounded context (the consuming application) can observe `ProcessTerminated` /
`ScopeTerminated`. It allowed either a bounded broadcast channel or a caller-supplied
publisher. An unused broadcast still allocates a channel and invites the assumption
that "events are going somewhere."

The hard rule is unchanged: an absent, slow, or failing consumer must never stall
termination or affect Shepherd's invariants.

## Decision

- `SupervisorBuilder` defaults to `NoopIntegrationPublisher` (drops every event).
- `BroadcastIntegrationPublisher` is an opt-in adapter (`tokio::sync::broadcast`,
  non-blocking `send`, lag/drop on the subscriber side).
- Callers who already have a bus inject their own `IntegrationEventPublisher`.
- The `IntegrationTranslator` handler still maps the externally-meaningful subset of
  domain events; only the default sink is silent.

## Consequences

- Library users who never subscribe pay nothing and cannot accidentally couple to an
  internal channel.
- Tests that need outbound events construct a broadcast (or a recording fake) and
  pass it to the builder.
