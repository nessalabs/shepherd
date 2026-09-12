# 0014 — Bounded byte capture and transferable observation

Accepted. Output types and the OutputSink port belong to shepherd-app; infrastructure
implements a combined stdout/stderr byte queue and an independent capped tail.
OutputChunk carries the stream tag and raw bytes. No UTF-8 decoding is assumed.

Capture starts independent OS-pipe readers at spawn. Both streams share buffer_bytes;
overflow removes the oldest bytes, including a partial chunk when necessary, and
increments a saturating dropped_bytes counter. Partial eviction advances a chunk
offset without shifting retained bytes; a consuming read compacts that chunk only
once. Tail snapshots copy only the retained suffix. An oversized incoming chunk retains
its newest bytes. A zero-capacity queue still drains and counts discarded bytes.
The optional tail has its own byte cap and survives consuming reads.

ProcessSupervisor::take_output transfers the observer once. Unclaimed observers
are retained for only the most recent 256 unclaimed captures from verified completed processes, bounding aggregate
post-mortem retention. Eviction precedes event publication so a stalled external
publisher cannot defer that bound. Live outputs, unverified reap outputs, and transferred observer lifetimes are unaffected. ProcessOutput clones
share queue consumption. read() returns queued chunks, the current tail, cumulative
drops, per-stream closure flags and reader errors without awaiting new bytes. The
observer retains only output state, never the child or supervisor ownership guard.

Discard connects both streams to the OS null device: there is no pipe that can fill.
Capture readers continue even when no caller consumes output. The waiter publishes
the root exit immediately after OS reap, independently of pipe completion. A
completed wait/termination does not imply output EOF; callers needing all retained
bytes must continue consuming snapshots until both stream closure flags are true.
The existing waiter task still owns reader finishing. Following root reap,
each reader has a 100 ms completion budget; an inherited pipe still open afterwards
is aborted and reported explicitly. This can truncate descendant output, but avoids
an escaped or long-lived descendant holding cleanup open indefinitely. Reader failure
is observable separately from the verified process termination outcome.

Tests cover exact binary bytes, both stream tags, partial/oversized/zero-capacity
queue overflow, capped tails, failing readers, non-consuming callers, output floods,
discard and termination during flood.

Discarded output and captures claimed before or after completion consume no history
slots. Claim and completion serialize the output map and FIFO together so observer
transfer cannot leave stale IDs that prematurely evict unrelated captures.
