# Background tasks (issue #21)

Status: implemented; validation covers the macOS host. Source: https://github.com/nessalabs/shepherd/issues/21.

## Decision

A `BackgroundTask` composes an observed process scope and observed spawn. Starting it returns an observation/control handle immediately; an owned worker retains the scope through process exit and verified cleanup. Dropping the handle or cancelling an await does not cancel the task. Explicit cancellation, the task timeout and supervisor shutdown remain available. The task does not restart commands or survive the supervisor's host process; kept processes have a different contract.

The task's identity is its existing scope identity. Shepherd owns process identity, output and cleanup. Callers own session association, authorization, durable records, concurrency budgets and delivery of completion. This primitive adds no agent concept and no second process registry.

`status`, bounded output and `wait` observe the same task. Terminal results retain cause, observed exit, cleanup report and output independently. The output is a capped tail of the combined streams, with an exact omitted-byte count, not an unbounded log. Observation copies are bounded. A clock port supplies timeout and output-close waits.

## Ordering contract

| Case | Behavior | Regression |
| --- | --- | --- |
| B1 Start while supervisor admits scopes | Return scope identity before command finishes | start_returns_while_command_runs_and_cancelled_wait_does_not_cancel |
| B2 Drop all task handles or cancel wait | Worker owns process until completion/timeout/shutdown | dropping_observers_does_not_stop_task |
| B3 Very fast exit | Original admission retains process and exit even if history is pruned | fast_exit_keeps_its_outcome |
| B4 Cancel before spawn starts | No process is spawned; empty scope is cleaned | cancellation_before_spawn_runs_no_command |
| B5 Cancel during spawn | Finish original spawn ownership then clean that scope | cancellation_during_spawn_keeps_ownership |
| B6 Cancel after terminal publication | Preserve the original exit and cleanup evidence | fast_exit_keeps_its_outcome |
| B7 Timeout | Stop owned scope and retain timed-out cause and physical exit | timeout_keeps_cause_and_cleanup |
| B8 Output exceeds cap / repeated reads | Keep bounded latest bytes and exact omitted count | output_tail_is_bounded_across_reads |
| B9 Spawn fails | Retain typed failure and cleanup result separately | spawn_failure_still_cleans_scope |
| B10 Cleanup uncertain | Report uncertainty; never claim resources released | uncertain_cleanup_is_not_success |
| B11 Worker panics | Scope backstop/cleanup still runs; report typed worker failure | worker_failure_retains_cleanup |
| B12 Supervisor shutdown | Existing scope cleanup stops task; task observers settle | shutdown_settles_background_tasks_and_closes_admission |
| B13 Output collection panics after cleanup | Publish prior exit and cleanup with an output error | final_output_panic_preserves_prior_exit_and_cleanup_and_settles_wait |
| B14 Observer reads newer bytes during the final pipe-close wait | Terminal publication freezes the newest retained observation | final_publication_keeps_newer_observer_output |
| B15 Budget expires before detached worker runs | Settle timed out without spawning | expired_admission_budget_does_not_spawn_after_scheduler_delay |

The existing backend remains the authority on containment and cleanup. No polling loop or background-task wrapper upgrades an unverified result. Caller runtime teardown retains the existing scope drop backstop, not a promise of asynchronous reap after the runtime is gone.
