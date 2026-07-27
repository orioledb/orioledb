# checkpointer-startup-lock-drain-progress

## Focus

Concurrency (attention focus 2).

## What led to this

`src/checkpoint/checkpoint.c:509-548`, function `acquire_chkp_lock_drain()`. The comment directly above the function is an explicit, self-documented deadlock scenario and its avoidance strategy — this is one of the clearest pieces of "this is a known hard concurrency seam" evidence in the whole codebase:

> "On a hot standby the startup process can hold one of OrioleDB's checkpoint-coordination LWLocks (`oTablesMetaLock`, `oSysTreesLock`, ...) SHARED while replaying a WAL window — e.g. between `WAL_REC_O_TABLES_META_LOCK` and `WAL_REC_O_TABLES_META_UNLOCK` for the meta lock. Concurrent (i.e. unrelated) transactions on the primary can sneak `XACT_COMMIT` records into that window; during their replay startup may call `DropRelationFiles -> register_forget_request -> RegisterSyncRequest`, which blocks on the checkpointer-served sync queue when the queue is full. If the checkpointer is at the same time waiting for the same LWLock EXCLUSIVE, we deadlock: startup holds the lock and waits for us to drain the queue, we wait for the lock."

The fix:

```c
static void
acquire_chkp_lock_drain(LWLock *lock)
{
	Assert(AmCheckpointerProcess());

	while (!LWLockConditionalAcquire(lock, LW_EXCLUSIVE))
	{
		AbsorbSyncRequests();
		/* Brief backoff so we don't pin a CPU while startup makes progress. */
		pg_usleep(1000L);
		CHECK_FOR_INTERRUPTS();
	}
}
```

This is called from `o_perform_checkpoint()` at `checkpoint.c:1418-1419` (`acquire_chkp_lock_drain(&checkpoint_state->oTablesMetaLock)` then `oSysTreesLock`), `checkpoint.c:1442`, `checkpoint.c:1555`, and `checkpoint.c:5227` — i.e. on essentially every checkpoint, not just an edge case.

The claimed liveness bound is explicit in the comment: "the worst case is a few extra iterations while startup keeps producing requests, after which it hits the matching unlock record and releases the lock" — this is a documented liveness guarantee, listed in `sut-analysis.md` §5, not yet independently stress-tested here.

## The property

**Type:** Liveness (progress) — the checkpointer and the startup process on a hot/warm standby both eventually make progress through this lock-vs-sync-queue cycle, under arbitrary relative scheduling.

**Property:** On a standby replaying WAL, if the startup process holds one of `oTablesMetaLock`/`oSysTreesLock` SHARED (mid-window between the WAL_REC lock/unlock pair) while blocked on `RegisterSyncRequest` because the checkpointer's sync-request queue is full, and the checkpointer concurrently wants the same LWLock EXCLUSIVE, both processes still make forward progress — the checkpointer's `AbsorbSyncRequests()` + retry loop drains the queue so startup unblocks, replays the matching unlock record, and releases the LWLock within a bounded number of iterations, not an unbounded stall.

**Invariant:** `Sometimes` for reaching the interesting state — `sometimes(checkpointer_entered_lock_drain_retry_loop)` confirms the contended path (not just the uncontended `LWLockConditionalAcquire` success on the first try) is actually exercised. Combined with `always(lock_drain_loop_terminates_within_bound)` — track iteration count or wall-clock time inside the loop and assert it stays under a generous bound (e.g., a few seconds under fault injection, since the comment's own claim is "a few extra iterations"); an assertion that never fires within the bound during a run flags the claimed liveness bound as violated.

**Antithesis Angle:** This requires a standby topology — noted in `sut-analysis.md` §9 as "the single largest and highest-value coverage gap": the existing Antithesis harness has no second Postgres node. To exercise this property at all, the harness needs: a primary generating a steady stream of DDL/relation-lock activity (to keep `oTablesMetaLock`/`oSysTreesLock` churn happening) plus enough concurrent unrelated `XACT_COMMIT` traffic to fill the sync-request queue during replay, and a standby under fault injection (CPU throttling / scheduling delay) to widen the window where startup is parked mid-WAL-window. Antithesis's value-add here is specifically making the "queue full + LWLock held + startup blocked" three-way alignment likely, which is otherwise a narrow timing window in an unfaulted run.

**Why It Matters:** If this drain loop's assumption is ever violated (e.g., a future change adds another kind of request `AbsorbSyncRequests()` doesn't drain, or the startup process can block on something else while holding the lock), replicas would freeze checkpointing indefinitely — and since checkpoints gate WAL retention/disk growth and (per `sut-analysis.md` §6) can already hang unboundedly in S3 mode, a second independent way to wedge the checkpointer on standbys is a serious availability risk that would be very hard to diagnose without a repro this specific.

**Open Questions:**

- Is there an existing isolation test (`test/specs/*.spec`) or Python test (`test/t/replication_test.py`) that already exercises this exact interleaving deterministically (not just via chance timing)? Not confirmed in this pass — `sut-analysis.md` calls out `replication_test.py` as the largest file (43 methods) but this pass did not grep it specifically for `acquire_chkp_lock_drain`-adjacent test names. `(partial: acknowledged the gap exists per §9's replication-topology finding, but didn't confirm zero overlap with `replication_test.py`)`.
- The comment's liveness claim ("a few extra iterations") is not backed by any numeric bound in code — `pg_usleep(1000L)` is a fixed 1ms backoff with no cap on retry count. Under adversarial fault injection that keeps regenerating sync requests indefinitely (not just "chance" DDL/commit traffic but a deliberately hostile workload), does the loop still terminate, or does the "worst case is a few extra iterations" framing implicitly assume bounded, not adversarial, request generation? This is the crux of what Antithesis should test and is currently unresolved.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** No assertion touches `acquire_chkp_lock_drain`, `oTablesMetaLock`/`oSysTreesLock` contention, or standby lock-drain behavior anywhere in `test/antithesis/`. This is a strong instrumentation candidate: the loop's iteration count is internal state not observable from a SQL client, so a SUT-side counter (e.g., exposed via a new stopevent or a lightweight stat) recording how many drain iterations a given checkpoint needed would let Antithesis's search prioritize toward wider contention windows.

## Cross-cutting pattern (added by evaluation pass, R14)

One of four properties sharing the "unbounded busy-wait" shape identified by
the Wildcard evaluation lens (the others: `sk-fixup-sentinel-spin-livelock`,
`recovery-worker-idxbuild-stall`, `recovery-worker-stall-blocks-leader` —
see `property-relationships.md` Cluster 11). This property is the odd one
out among the four: `acquire_chkp_lock_drain()`'s loop *does* call
`CHECK_FOR_INTERRUPTS()` each iteration (confirmed by direct reading — see
the code excerpt above), so the gap here is purely the missing outer retry
cap, not raw uninterruptibility. Because of that, a direct
`pg_cancel_backend()`/`statement_timeout` test against the startup process
while it's parked in this drain wait is a strictly cheaper *positive*
control — confirming the interruptibility contract holds — that should be
run before investing in the full standby+CPU-throttling adversarial-
regeneration scenario above, which is needed only to test the *bound*, not
the interruptibility.

### Investigation Log

#### Is there an existing isolation test or Python test that already exercises this exact interleaving deterministically?

- Examined: `sut-analysis.md` §9 (replication-topology coverage gap), `replication_test.py` (noted as the largest test file, 43 methods).
- Found: `sut-analysis.md` confirms the standby-topology gap generally exists.
- Not found: `replication_test.py` was not grepped specifically for `acquire_chkp_lock_drain`-adjacent test names, so overlap isn't ruled out.
- Conclusion: tagged `(partial: ...)` — the general gap is acknowledged, but zero overlap with `replication_test.py` isn't confirmed.
