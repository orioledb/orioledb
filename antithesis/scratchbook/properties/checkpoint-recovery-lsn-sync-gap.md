# checkpoint-recovery-lsn-sync-gap

## Merge note

Merges two independently-written files describing the same finding:
`end-of-recovery-checkpoint-lsn-sync-gap.md` and this file
(`checkpoint-recovery-lsn-sync-gap.md`, kept as the canonical slug — has the
firsthand code trace; the other file explicitly notes it carried the
evidence forward from `sut-analysis.md` §11 without independently re-reading
`checkpoint.c`).

## Focus

Concurrency / Lifecycle Transitions, extending `sut-analysis.md` §11. The
recovery-to-normal-operation boundary is squarely a lifecycle transition:
`CHECKPOINT_END_OF_RECOVERY` is, by definition, the exact moment a
promoted/recovering instance transitions from "replaying" to "serving."

## What led to this

`src/checkpoint/checkpoint.c:1322-1332`:

```c
static XLogRecPtr
get_checkpoint_xlog_ptr(void)
{
	if (is_recovery_in_progress())
	{
		/* FIXME: synchronize recovery workers */
		return GetCurrentReplayRecPtr(NULL);
	}
	else
		return GetXLogInsertRecPtr();
}
```

Verified directly (not just cited from the SUT analysis): this function is called three times inside `o_perform_checkpoint()`, each time assigning a boundary LSN that other subsystems treat as authoritative:

- `checkpoint.c:1421` — `checkpoint_state->replayStartPtr = get_checkpoint_xlog_ptr();`, immediately followed by `wait_finish_active_commits(checkpoint_state->replayStartPtr)`.
- `checkpoint.c:1436` — `checkpoint_state->sysTreesStartPtr = get_checkpoint_xlog_ptr();`, taken while holding `oTablesMetaLock`/`oSysTreesLock`, with a comment explaining this is meant to guarantee "no partial changes to tables and indices system trees" at the recovery-restart point.
- `checkpoint.c:1457` — `checkpoint_state->toastConsistentPtr = get_checkpoint_xlog_ptr();`, immediately followed by `checkpoint_write_pending_sk_fixups();` — this is the exact boundary the orioledb#855 fix (already the target of the existing `sk-recovery-race[-chaos]` harness) depends on for correctness.

Contrast with `finish_write_xids()` (`checkpoint.c:902-948`, called later in the same function at `checkpoint.c:1461`), which *does* wait for every recovery worker to actually catch up before snapshotting in-flight oxids — i.e., the codebase knows how to synchronize with parallel recovery workers correctly, and does so for the oxid snapshot, but not for the three LSN captures above.

`GetCurrentReplayRecPtr(NULL)` returns the **leader/startup process's own** replay pointer. During parallel WAL apply (`src/recovery/recovery.c`, `src/recovery/worker.c` distribute decoded records to N worker processes over `shm_mq`, per `sut-analysis.md` §1), the leader's replay pointer can be ahead of what every worker has actually finished applying — workers apply asynchronously and the leader's job is to dispatch and track its own read position, not the workers' completion. `finish_write_xids()`'s explicit worker-wait (contrasted above) exists precisely because the codebase's own authors recognize this ahead/behind gap needs closing for correctness-sensitive boundaries.

## Reachability of the recovery-in-progress branch

Confirmed reachable, not just theoretical: `CheckPoint_hook` (patched Postgres, `xlog.c:7549` per `sut-analysis.md` §1) fires from `CreateCheckPoint()`, including for `CHECKPOINT_END_OF_RECOVERY`, which by definition runs while `RecoveryInProgress()` is still returning true (end-of-recovery checkpoint happens before the recovery-complete transition). Beyond end-of-recovery, ordinary restartpoints on a streaming/warm standby also run with recovery in progress and can trigger the same `o_perform_checkpoint()` path.

Note: `o_recovery_finish_hook()` (`recovery.c:1259-1310`) calls `worker_wait_shutdown()` for every worker *before* calling `recovery_finish(-1)` — so by the time `recovery_finish()` itself runs, all workers have already joined. This means the specific window this property is about is checkpoints that run *while workers are still actively applying* (mid-recovery restartpoints, or an end-of-recovery checkpoint that races with the final worker-join sequence), not the final post-join cleanup. This refines (but does not invalidate) the framing that `CHECKPOINT_END_OF_RECOVERY` runs "while parallel recovery workers are active" — that claim needs to be read as "can run concurrently with worker activity, depending on exact ordering relative to `o_recovery_finish_hook`'s worker-join," which was not fully pinned down (see Open Questions).

## Why this matters, and why it's a lifecycle-transition finding too

The orioledb#855 PK/secondary-key checkpoint-boundary race is the single most concretely-tracked bug class in this codebase (sut-analysis §2, §8, §10) and is the origin of the existing `sk-recovery-race[-chaos]` Antithesis harness. That harness targets the race on the **live-DML path** (a checkpoint racing with ordinary concurrent inserts/updates/deletes) via a live `CHECKPOINT` command against a running instance (per its stopevent-pinned `pg_stopevent_set('sk_modify_pending', ...)` design, which requires the instance to already be up). This finding is the *same class of assumption* — an LSN captured without waiting for all parallel recovery workers to actually reach it — relocated to the **end-of-recovery / promotion path**, a structurally different lifecycle moment (crash recovery finishing, or a standby being promoted), which is **not** covered by the existing harness at all, since that harness never crashes the instance mid-DML and then restarts into a `CHECKPOINT_END_OF_RECOVERY`.

## The property

**Type:** Safety — this is the same failure shape as orioledb#855 (PK/secondary-index divergence across a checkpoint boundary), relocated from the live-DML path to the recovery/restartpoint/promotion path.

**Property:** A checkpoint or restartpoint taken while `RecoveryInProgress()` is true captures `replayStartPtr`/`sysTreesStartPtr`/`toastConsistentPtr` values that correctly reflect a point *no later than* what every parallel recovery worker has actually applied — not merely the leader's own replay position — so that the PK/SK fixup mechanism (`checkpoint_write_pending_sk_fixups()`/`apply_pending_sk_fixups()`, the orioledb#855 fix) and the "no partial system-tree changes" guarantee both hold across a crash immediately following such a checkpoint, including immediately after `CHECKPOINT_END_OF_RECOVERY` (a crash-then-recovery-then-immediate-checkpoint sequence, i.e. the promotion/end-of-recovery boundary specifically).

**Invariant:** `Always` — reuse the existing oracle from `sk-recovery-race`: after a crash/restart following a checkpoint taken during recovery, PK-row-count must equal distinct-SK-token-count (same check as `sk-recovery-race/driver.py:89-95`) plus `orioledb_tbl_check()` structural consistency. The novelty is *when* the checkpoint is forced: during an active multi-worker recovery replay (e.g., a cascading/lagging standby doing its own restartpoints while catching up, or immediately as part of promotion after a crash-during-DML), not during live primary DML. A `reachable()` confirming the specific scenario was actually hit — i.e. that recovery workers were still meaningfully behind the leader's replay pointer at the moment the checkpoint's LSN was captured — is valuable, since without deliberately slowing worker replay the race window may be too narrow to hit by chance.

**Antithesis Angle:** This needs a topology the existing harness doesn't have (a standby actively replaying, ideally lagging behind a busy primary so restartpoints land mid-replay) — same gap noted in `sut-analysis.md` §9 as the highest-value coverage hole. Antithesis's fault injection (scheduling delay on specific recovery worker processes) is the natural way to widen the gap between the leader's `GetCurrentReplayRecPtr(NULL)` and the slowest worker's actual apply position — the wider the gap, the more likely a restartpoint's captured LSN is stale relative to true worker progress, and the more likely the PK/SK fixup mechanism sees a boundary that doesn't match reality. A simpler variant needing no standby topology: crash the instance mid-DML (kill -9 during concurrent inserts/updates against a table with a secondary index), let it come back up through crash recovery, and check the invariant immediately after the automatic `CHECKPOINT_END_OF_RECOVERY` Postgres performs as part of promotion.

**Why It Matters:** orioledb#855 was exactly this failure mode (PK-applied/SK-pending window not correctly bounded relative to a checkpoint), already fixed once for the live-DML path and the target of dedicated Antithesis coverage. This FIXME is the same assumption class, unfixed, on a different path the existing harness structurally cannot reach. A silent PK/SK divergence surfacing only on index-scan queries is, per `sut-analysis.md` §10, one of the worst-case failure classes for this system (silent wrong query results). This is exactly what `property-catalog.md` means by "a similar but slightly different condition [that] could bypass the fix."

**Open Questions:**

- Does a real fault sequence exist where the gap between leader replay pointer and slowest-worker-applied position is large enough, at the exact moment a restartpoint's boundary LSN is captured, to actually flip the PK/SK fixup's correctness? This pass confirmed the FIXME and its reachability but did not construct or run a concrete repro. `(partial: mechanism and reachability confirmed via code reading; timing-window magnitude not measured)`.
- Exact ordering between `o_recovery_finish_hook`'s worker-join loop and the `CHECKPOINT_END_OF_RECOVERY` checkpoint's LSN capture — i.e., whether ordinary mid-recovery restartpoints are the only real-world reachable case for this property, or whether end-of-recovery itself can also race worker activity. Needs a closer read of the patched Postgres `xlog.c` `CreateCheckPoint()`/`StartupXLOG()` call ordering relative to where `o_recovery_finish_hook` fires. `(partial: hook-firing-during-recovery is confirmed; worker-lag-at-that-exact-moment is inferred, not directly observed)`
- Does this require a multi-recovery-worker configuration (`recovery_pool_size_guc > 0`) to be reachable at all, or can it also manifest in single-process recovery mode? Not checked — worth confirming before designing the workload.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** No existing assertion touches `get_checkpoint_xlog_ptr()`, restartpoints during recovery, or any recovery-time checkpoint boundary — the only existing PK/SK assertions (`sk-recovery-race[-chaos]`) target live-DML checkpoints exclusively. A SUT-side `always()`/`reachable()` pair analogous to the existing ones, but gated on "checkpoint was taken with `is_recovery_in_progress()` true," would give direct visibility into this specific path that a workload-only check cannot easily distinguish from the already-covered live-DML case. No stopevent currently exists at `get_checkpoint_xlog_ptr()` or in the parallel-recovery-worker apply loop that would let this be pinned deterministically the way `sk_modify_pending` pins the live-DML variant — this would need a new stopevent (added via `stopevents.txt`, per CLAUDE.md's guidance to edit source inputs) to become a high-precision test rather than a probabilistic one.

### Investigation Log

#### Was `get_checkpoint_xlog_ptr()` and the `FIXME` comment independently re-verified across both source passes?

- Examined: `src/checkpoint/checkpoint.c:1322-1332` and its three call sites (`checkpoint.c:1421,1436,1457`), plus `finish_write_xids()` (`checkpoint.c:902-948,1461`), by one focus pass (Concurrency) directly, with line numbers and full function bodies read.
- A second, independent focus pass (Lifecycle Transitions) carried the finding forward from `sut-analysis.md` §11 without re-reading `checkpoint.c` directly in that pass (explicitly flagged as such in that file's own Investigation Log, preserved here for transparency: "this property's evidence was carried forward from sut-analysis §11 rather than independently re-read... this is flagged honestly rather than presented as freshly re-verified").
- Conclusion: the finding is solid — confirmed by direct, firsthand code reading in the Concurrency pass, with a second pass converging on the same conclusion via the SUT analysis. Treat the Concurrency pass's line numbers and mechanism trace as the authoritative source for this merged file.
