# replica-undo-cleanup-bgwriter-spof

## Status

**Open lead, not yet in the catalog before this pass.** This is a new property
written to fill a gap named in `sut-analysis.md` §5/§11 but never turned into
a catalog entry. Not a regression target for a fixed bug — it's a structural
single-point-of-responsibility observation about currently-shipping code at
`a975c702156cd449e9c0a8db6f8d9bf5bca4537d`.

## Task context

This evidence file was written to fill Task A, item 1, of a follow-up gap-filling
pass: "bgwriter's single point of responsibility for replica undo cleanup"
(`src/workers/bgwriter.c:200-214`, `BGWriterNum == 0`). The instruction was to
read the actual code, determine whether it's a legitimate distinct property
(checked carefully against `recovery-worker-stall-blocks-leader` and
`replica-globalxmin-catchup-lag`), and either write it up or explain why not.
Conclusion: **legitimate, distinct property** — different subsystem
(`src/workers/bgwriter.c`, not `src/recovery/recovery.c`), different oracle
(`orioledb_has_retained_undo()`, not `orioledb_get_xid_meta()`/`commitPtr`),
different failure mechanism (a designated-single-worker liveness gap, not a
leader/worker queue-backpressure gap or an xmin-horizon-tracking gap).

## The mechanism

`bgwriter_main()`'s per-cycle loop (`src/workers/bgwriter.c:172-217`) walks
each undo log type and decides between two branches:

```c
if (writeInProgressLocation + undo_circular_buffer_size <
    lastUsedLocation + undo_circular_buffer_size / 20)
{
    /* eviction needed: evict_undo_to_disk(...) */
}
else
{
    /*
     * Even when eviction is not needed, update min undo locations to
     * allow cleanup of undo files. Without this, minProcRetainLocation
     * set during recovery may never be advanced on a synced replica.
     * Only first bgwriter does this to avoid unnecessary concurrency.
     */
    Assert(BGWriterNum >= 0);

    if (BGWriterNum == 0)
        update_min_undo_locations((UndoLogType) j, false, true);
}
```
(`src/workers/bgwriter.c:200-214`)

This is the **"else" (no-eviction-needed) branch** — i.e., under ordinary,
non-memory-pressure conditions, the *only* place `update_min_undo_locations()`
gets called for a given undo log is here, gated to `BGWriterNum == 0`
specifically to avoid every bgwriter process doing redundant work
concurrently (the comment's own stated rationale).

**Confirmed via direct code reading that this is the sole path on a replica.**
`grep -n update_min_undo_locations src/recovery/recovery.c` returns zero hits
— `src/recovery/recovery.c` (the WAL-apply/replay machinery) never calls
`update_min_undo_locations()` directly. All the *other* call sites of
`update_min_undo_locations()` (`src/transam/undo.c:1574,1759,1930,3338,3651`,
`src/btree/print.c:134,136`) are reached from regular-backend
commit/rollback/eviction-retry paths — code that doesn't run on a replica
actively replaying WAL, since replay is driven by recovery workers applying
WAL records, not by ordinary transaction commit machinery. So **on a synced
replica in steady state (no local write traffic, no eviction pressure), bgwriter
process #0 specifically is the only thing in the entire process tree that
advances `minProcRetainLocation` / unblocks undo-file cleanup**, exactly as
`sut-analysis.md` §5/§11 states.

### Why the "crash" framing needs a caveat

`register_bgwriter()` (`src/workers/bgwriter.c:46-64`) sets
`worker.bgw_restart_time = 0`, so if bgwriter #0 genuinely crashes,
Postgres's own background-worker infrastructure respawns it near-immediately
(the same "crash-loop restart" pattern `sut-analysis.md` §6 documents for
S3 workers). A bare `SIGKILL` of just that one process is therefore likely
**not** the sharpest fault to inject — it would be auto-healed quickly. The
two conditions that plausibly do produce a sustained gap are:

1. **`orioledb.debug_disable_bgwriter = true`** (`src/orioledb.c:700-707`,
   `PGC_POSTMASTER`) — a global boolean, not per-worker-index. When set,
   *every* bgwriter process (including #0) logs "stopped:
   orioledb.debug_disable_bgwriter = True" and returns immediately
   (`bgwriter.c:107-114`) — but because `bgw_restart_time = 0`, the process
   is still respawned by the postmaster, checks the flag again, and returns
   again, in a tight respawn loop that never reaches the undo-cleanup code
   at all. This is a heavily-used flag in the deterministic test suite —
   confirmed via `grep -rn debug_disable_bgwriter test/` — set in
   `test/orioledb_regression.conf`, `test/orioledb_isolation.conf`, and at
   least 15 individual `test/t/*_test.py` files (e.g.
   `checkpoint_concurrent_test.py`, `eviction_test.py`,
   `replication_test.py:459` for the *master* side of
   `test_replication_root_eviction`) — but **confirmed absent from every
   `test/antithesis/config/**` file**, so it isn't set in the harness today.
2. **Bgwriter #0 alive but wedged** (not exited) — e.g. blocked in
   `ppool_run_maintenance()`'s eviction inner loop, or in `evict_undo_to_disk()`
   waiting on I/O, or lock-contended — the same "wedged, not crashed" shape
   `recovery-worker-stall-blocks-leader.md` already documents for recovery
   workers, but here applied to the one bgwriter process the whole
   replica-cleanup guarantee is pinned to. `bgwriter_num_workers` is
   `PGC_POSTMASTER` with a floor of 1 (`src/orioledb.c:797-807`), so there is
   always exactly one process wearing the `BGWriterNum == 0` badge at any
   given time (can't be configured to zero) — but "always exactly one" is
   precisely the single-point-of-failure shape: there is no backup, no
   second bgwriter that can pick up the `BGWriterNum == 0` role if #0 stalls
   without exiting.

## Existing deterministic coverage (relevant, but not adversarial)

`orioledb_has_retained_undo()` becoming `false` on a replica after DML is
already a well-established, deterministically-tested invariant — confirmed
via `grep -n orioledb_has_retained_undo test/t/replication_test.py`, hit in
**13 separate test methods** (`test_replication_simple`,
`test_replication_in_progress`, `test_replication_drop`,
`test_replication_create_drop_commit`, `test_replication_create_rollback`,
`test_replication_create_truncate_commit`,
`test_replication_drop_truncate_rollback`,
`test_replication_simple_truncate`, `test_replication_non_root_eviction`,
`test_replication_root_eviction`, `test_replica_checkpoint`,
`test_replication_column_ddl`, `test_tablespace_replication`,
`test_recreate_o_table_version_replication`, `test_replication_hot_read`).
None of these deterministic tests disable, kill, or otherwise fault-inject
the replica's own bgwriter #0 specifically — they all assume it runs
normally. This is exactly the Antithesis value-add shape: reuse a
well-established oracle, add a fault the deterministic suite structurally
never constructs.

## Property

| | |
|---|---|
| **Type** | Liveness |
| **Property** | On a streaming/synced replica under sustained write load with no eviction pressure, `minProcRetainLocation`/undo-file cleanup eventually advances (observable via `orioledb_has_retained_undo()` returning `false` once outstanding writes quiesce) within a bounded time — even when the replica's `BGWriterNum == 0` process specifically is disabled, wedged, or repeatedly killed, not merely absent from a clean run. |
| **Invariant** | `Always(orioledb_has_retained_undo_eventually_false_within_bound)` reusing the existing oracle from the 13 deterministic replication tests, run under fault injection targeting specifically the replica's bgwriter #0 process — the honest framing given today's code is that this invariant is **expected to fail** when bgwriter #0 is disabled/permanently wedged (there is no fallback path), so the more useful Antithesis-side signal is `Sometimes(bgwriter_0_stall_or_disable_injected)` paired with `Always(...)` to make the causal link between "bgwriter #0 unavailable" and "undo retention pinned forever" explicit and reproducible, rather than assuming it as a known limitation. |
| **Antithesis Angle** | Requires a primary+streaming-replica topology (the harness's largest documented gap, per `sut-analysis.md` §9). On the replica: (a) set `orioledb.debug_disable_bgwriter = true` and confirm `orioledb_has_retained_undo()` never clears despite quiescent writes and passing time — a config-mutation-style test, not a runtime fault; (b) more organically, inject a scheduling-delay/CPU-throttling/`SIGSTOP` fault targeting specifically the replica's bgwriter #0 PID while sustained DML with periodic commits/rollbacks runs on the primary, and confirm retained-undo cleanup stalls in proportion and does not resume via any alternate path once the stall clears. |
| **Why It Matters** | Undo retention pinned indefinitely on a replica is an availability/resource-exhaustion failure (unbounded undo-file growth, matching the framing already established for `replica-globalxmin-catchup-lag`'s "stuck low globalXmin" symptom) — but via a structurally different, single-named-process mechanism that has no documented or coded fallback. Unlike the globalXmin case (which is about the recovery leader's own xmin bookkeeping), this specific gap is entirely avoidable by design (any other bgwriter instance, or the leader/startup process itself, could in principle do this bookkeeping) — the code chose not to, purely "to avoid unnecessary concurrency," which is a reasonable efficiency tradeoff but leaves a real liveness gap if the chosen single worker is unavailable. |

**Open Questions:**

- Is `orioledb.debug_disable_bgwriter` ever set inside a real deployment (vs. only in the deterministic test suite), making the "disabled" branch of this property realistic outside of testing, or is this purely a test-harness-only risk? `(needs human input)`
- What is the realistic worst-case time for bgwriter #0 to become "wedged but alive" under organic write pressure alone (no fault injection) — e.g., does `ppool_run_maintenance()`'s eviction inner loop have a bound tight enough that this is rarely observable without deliberate fault injection? `(needs further investigation — the eviction loop's own bound, `bgwriter_lru_maxpages * (BLCKSZ / ORIOLEDB_BLCKSZ)`, was located but not measured against realistic workloads)`
- Does anything on the *primary* side (as opposed to the replica) also depend on this exact `BGWriterNum == 0` path, or does the primary have an independent bookkeeping path via ordinary backend commit flows that makes this a replica-only concern? `(partial: confirmed recovery.c never calls update_min_undo_locations directly, so the replica dependency is exclusive; did not separately confirm whether the primary's own reliance on this path is redundant with or additional to backend-commit-driven calls)`

## SUT-side instrumentation

`existing-assertions.md` confirms zero assertions in `src/`. No current signal
distinguishes "bgwriter #0 is progressing normally but cleanup is naturally
slow" from "bgwriter #0 is stalled/disabled and cleanup will never happen."
Suggested: a `Reachable`/timed marker at the `if (BGWriterNum == 0)` branch
(`bgwriter.c:212-213`) recording successful calls, so an external monitor can
distinguish "never called in this run" (suspicious) from "called periodically
but retained-undo still isn't clearing" (a different bug entirely).

## Investigation Log

#### Is bgwriter's crash actually a liveness risk, given Postgres's own bgworker restart infrastructure?

- Examined: `src/workers/bgwriter.c:46-64` (`register_bgwriter`,
  `bgw_restart_time = 0`), compared against the identical pattern documented
  for S3 workers in `sut-analysis.md` §6 ("S3 worker 'retries' only by
  crash-looping... `bgw_restart_time = 0`; a FATAL kills the worker, which is
  respawned immediately").
- Found: the same `bgw_restart_time = 0` setting applies to bgwriter, so a
  genuine crash (not a graceful `debug_disable_bgwriter`-triggered return) is
  auto-healed by the postmaster near-immediately, the same way S3 workers are.
- Conclusion: reframed the property away from "bgwriter #0 crashes and stays
  dead" (unlikely, given restart_time=0) toward the two conditions that
  plausibly do produce a sustained gap: the global `debug_disable_bgwriter`
  flag (which defeats the restart-heals-it argument, since the respawned
  process re-checks the same flag and exits again every time), and a
  live-but-wedged process (which restart-on-exit cannot help with at all,
  since the process never exits).
