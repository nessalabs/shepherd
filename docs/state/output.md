---
id: "output"
title: "Read command output"
kind: "statechart"
status: "implemented"
summary: "Both streams share a bounded queue while a separate tail survives reads."
parent: "shepherd"
sources:
  [
    "crates/shepherd-app/src/output.rs",
    "crates/shepherd-infra/src/output.rs",
    "fixtures/tests/output_integration.rs"
  ]
sourceRevision: "e56c5e583793ad7d69b4dd4738857909ca8007ad"
diagramLinks: {}
model: { "spec": "tla/OutputBudget.tla", "config": "tla/OutputBudget.cfg" }
modelCheck:
  sourceRevision: "e56c5e583793ad7d69b4dd4738857909ca8007ad"
  checker: tlc-1.7.4
  date: 2026-11-03T08:39:20.291Z
  config: tla/OutputBudget.cfg
  modelDigest: 32dc6514d655e13950e4c1e649a9ec842654aad5c2144f2d4cff5d8f911a14c4
  states: 46
  held:
    - QueueBound
    - TailBound
    - ByteAccounting
  broke: []
  unchecked: []
diagramDetails:
  [
    {
        "diagram": 0,
        "node": "Queue",
        "section": "Queue and tail have different jobs"
      },
    {
        "diagram": 0,
        "node": "Status",
        "section": "Exit and EOF are different facts"
      }
  ]
---
# Read output without blocking the child

A command can print faster than the UI can read. Shepherd retains a fixed amount
of recent output and counts omitted bytes instead of letting memory grow without
bound. Both stdout and stderr consume the same queue budget; stream labels remain
attached to their bytes. Dropped output alone does not mean the command failed.

For ordinary spawns, capture is opt-in: configure
`ProcessSpec::output(OutputMode::Capture { buffer_bytes, tail_bytes })` before
spawning. The default is `Discard`, so later reads cannot recover those bytes.
[Background tasks](background-tasks.md) enable bounded capture automatically;
[kept processes](kept-processes.md) reject capture and use a log instead.
The diagram below describes capture mode.

```mermaid
flowchart LR
    Pipes[Stdout and stderr] --> Queue[Combined bounded queue]
    Pipes --> Tail[Independent bounded tail]
    Queue --> Read[Read drains queued chunks]
    Tail --> Read
    Status[Per-stream closed flags and errors] --> Read
```

## Queue and tail have different jobs

`take_output` transfers the available observer once. Cloned observers share one
consuming queue: they are not independent subscriptions. Reading drains the queue
but leaves the tail. On overflow, oldest bytes are removed, including partial
chunks; a single oversized write keeps only its suffix. The dropped counter is
cumulative for queue loss. Raw bytes may split UTF-8 characters; decoding policy
belongs to the consumer.

## Exit and EOF are different facts

A child can exit while a descendant still holds a pipe open. Reader shutdown is
bounded after root reap. The adapter waits up to 100 ms for each reader, then may
abort it and mark that stream closed with an error. Both closed flags can therefore
be true even though a pipe never reached normal EOF. Check `errors`,
`dropped_bytes`, and both closed flags; inspect process exit and cleanup separately.
For example, `dropped_bytes = 100` means queue overflow omitted 100 bytes, not that
the program exited unsuccessfully. A zero counter still cannot establish complete
output if another observer drained the shared queue or capture ended with errors. Output handles retain data but do not keep the process alive.
Background tasks add their own final pipe-close observation budget and retain
open flags if it expires. A finished task therefore need not have complete output.

## Model scope

`OutputBudget` reduces writes to one or two bytes, a queue capacity of two and a
tail capacity of one, over at most three writes. It checks queue and tail bounds
and conservation of produced bytes across read, queued and dropped counts. It
does not model byte values, Unicode, pipe scheduling or saturating u64 overflow.
Controls remove queue eviction and erase the dropped count during reads.
