# Adversarial and reader review

Source reviewed: `e56c5e583793ad7d69b4dd4738857909ca8007ad`.
Reviewed on 2026-11-03. Independent subagents performed a docs-only newcomer pass,
a source-aware technical pass, and a model/source adversarial pass. This is
simulated reader evidence, not a study of real users.

## Reader tasks

Starting at the system page and following only doc links, the reader tried to:
run a background command then stop waiting and assess cleanup; keep a named
service across an owner restart; distinguish missing output from command failure;
and identify current test evidence and platform exclusions.

The first pass found the conceptual ownership descriptions useful but could not
safely infer cancellation-result call order or service-handle shutdown defaults.
It also found missing capture prerequisites and non-clickable evidence references.
The revision gives the exact cancel/wait/result order, a compile-checked usage
example, a service stop-outcome table and orderly restart recipe, capture-mode
prerequisites, and relative evidence links. Root diagrams now link owners; critical
state inspectors explain uncertainty without requiring the reader to switch views.

## Technical findings and changes

- Cancellation returns no result. The page now distinguishes `cancel()` from the
  retained result returned by `wait().await`.
- Ordinary output capture must be requested before spawning; defaults discard
  output. Background tasks override capture, while kept processes reject it.
- A closed stream flag can mean an aborted reader with an error, not actual EOF.
  Both the chart inspector and prose now require errors and loss counters too.
- Unverified kept termination has different record outcomes. A force wait without
  observed leader exit retains the record; an exited leader with stragglers can
  lose its record because that identity no longer safely addresses the group.
  Missing records are not presented as proof of descendant cleanup.
- The service diagram now shows stop during preparation, readiness and backoff,
  while retaining the uninterruptible ownership-establishment condition.

## Model findings and changes

- The initial KeptPublication model mixed unrelated launch and listing variables.
  It now chooses disjoint launch or listing scenarios. Prose explicitly assumes
  the durable acknowledgement belongs to the launch, the listed predecessor was
  truly absent, and claims serialize access. Combined live-record integrity is
  not claimed. The stale-generation counterexample remains meaningful.
- ServiceAdoption checks a supplied eligibility flag, not its worker-lifetime
  maintenance. The page now states this assumption and the source-reviewed flag
  evolution rather than claiming it was modeled.
- TaskFinalization initially lacked an independent missing-cause control. The
  added control settles a Starting task without assigning a cause and must break
  CauseBeforeFinished. Every configured semantic invariant now has a control.

The author inspected all saved expected counterexamples: early publication,
missing cause, execution before record commit, stale-record deletion, queue/tail
overflow, lost dropped-byte counts, unverified closure/pruning, evidence downgrade,
ineligible/mismatched adoption and replacement before verified cleanup.

The first model run also exposed an authoring error: an unparenthesized TLA IF
left successor variables unspecified. The expression was corrected before the
final receipt. A subsequent runner attempt refused a stale mutation pattern after
the scenario split; the pattern was corrected and the entire suite rerun. Neither
attempt was counted as successful verification.

## Completion and limits

Final TLC and negative-control counts are in [results.json](results.json).
Finite terminal states intentionally disable deadlock checking: this suite checks
safety properties, not eventual progress. No fairness/liveness, OS containment,
identity-token implementation or automatic Rust/model correspondence is proved.
The independent second source/model pass found T1–T4 and M1–M3 resolved.
It requested two final service-chart corrections: stop during launch preparation
must remain visible, and stop during backoff publishes Stopped directly rather
than Stopping. Both edges were corrected against Runner::run and or_stop.
The parent's follow-up docs-only task pass found all four sampled tasks answerable
from the revised pages. This sampled review does not establish every possible
reader journey; unmodeled implementation boundaries remain explicit above.

The final local browser pass rendered all 14 accepted pages and 10 diagrams with
zero page or diagram errors. Selective state details and root navigation were
inspected in the renderer. See [browser results](browser-results.json).
