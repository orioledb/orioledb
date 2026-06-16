# Trial 16 (`fixca00562d-b`): deferred rollback for empty oxid PANICs the replica

Build: `ca00562d` + `f071202a`. Hunt: 100×60s/6s streaming kill chaos. Trials 1–15
clean; trial 16 = BUG: primary fully consistent, but the **replica startup process
PANICked** and the node shut down → catchup + replica checks failed.

Evidence: `results/20260612_052707_fixca00562d-b_NONE_invariant_replica.log` (12M)
+ `..._invariant.log` (890M, primary).

## The crash chain (replica startup proc, PID 1644236)

```
05:26:09.866  FATAL: could not receive data from WAL stream  (primary kill-cycled)
05:26:14.877  WAL redo: XID (345 672 0); ROLLBACK (345 672 0 - xmin 345 csn 412)
              walk_undo_stack oxid=345 ... exit                       ← real undo (672), walks fine
05:26:14.877  WAL redo: XID (460 0 0); ROLLBACK (460 0 0 - xmin 345 csn 412)
              walk_undo_stack enter oxid=460 abortTrx=1 toLocation=NULL
              walk_undo_stack abort-branch oxid=460 location=0 newOnCommitLocation=0
              walk_undo_range enter oxid=460 location=0 toLoc=0x2000000000000000
              PANIC: undo_item_buf_read_item(): read of unexisting undo record
                     undoType=0, location=0, checkpointRetain[Start|End]Location=13600
05:26:15.237  startup process terminated by signal 6 → replica shut down
```

## Reading

- `XID (460 0 0)` is the **deferred-rollback** (#876) signature for a
  no-material-changes oxid: both undo-location fields are zero, and it shares its
  batch's `xmin 345 csn 412` with the preceding deferred rollbacks.
- Replaying its ROLLBACK calls `walk_undo_stack` with `location=0`; the walker takes
  0 as a real undo location instead of "nothing to walk" and tries to read undo at
  byte 0 — below `checkpointRetainStartLocation=13600` → PANIC.
- Pre-series builds replayed these empty deferred rollbacks without crashing (they
  caused the *livelock* instead). The crash is new with the drainer/teardown commits
  (`c64f55a3` tears down drained per-oxid state fully; a subsequent deferred
  ROLLBACK for the same/below-horizon oxid then sees zeroed locations), or with the
  re-admitted `WAL_REC_XID(460 0 0)` creating fresh state whose zero locations the
  abort path no longer guards.

## Tally for this fix series so far

- Assert crash-loop variant: killed by `f071202a`-era trial-1 evidence? No — that
  was the PRE-rebase build (`2b463f27`). Post-rebase (`ca00562d`): the Assert has
  not fired in 16 trials, but trial 16 shows the deferred-rollback path now
  PANICs the replica outright.
- Same root cause family as the livelock: **deferred rollbacks emitted for oxids
  below the horizon keep reaching consumers that have already passed them.** Every
  variant that doesn't suppress them at the emission site
  (`o_emit_recovery_finish_rollbacks`, skip `oxid < recovery_xmin`) has now failed
  three different ways: livelock (original), Assert crash-loop (`2b463f27`),
  undo-read PANIC (`ca00562d`, this trial).
- Also pending against this series: the ~3× writes/trial throughput regression
  (`perf_regression_writes_drop_ca00562d.md`) and one unexplained
  `plan-check FAIL` (trial 10).
