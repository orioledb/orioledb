# Recovery livelock — root cause, proven (deferred recovery-finish rollback)

Status: **root cause confirmed and proven from logs.** OrioleDB streaming standby
(and, it turns out, the primary's own crash recovery) wedges a recovery worker in
`o_btree_..._handle_conflicts` after a SIGKILL. This documents the full causal
chain, the log evidence (trial 6, build `1f68070f` = `eb3b765f` + a `LIVELOCK-ROOT`
LOG marker; captures preserved at `rootcause_evidence/TIMEOUT_6_{primary,replica}.log`),
and the fix directions.

The single instrumented line that catches the violation, added to `update_run_xmin`:
```
LIVELOCK-ROOT invariant violation: update_run_xmin lowering globalXmin to oxid=196
  BELOW writtenXmin=423 (globalXmin_was=423 recovery_xmin=531 queue_empty=0 nextXid=537)
```

---

## 0. TL;DR

After a crash, the primary aborts its in-flight txns and **lifts its horizon**, then
ships the aborts to the standby as **deferred `WAL_REC_XID`+`WAL_REC_ROLLBACK`
pairs** — emitted *after* an **end-of-recovery checkpoint** that already carries the
lifted horizon. On replay, the horizon (`writtenXmin`) is therefore raised **past**
the aborted oxids *before* the rollbacks arrive; then each `WAL_REC_XID` re-admits a
**bare, already-frozen oxid** into `xmin_queue`. The **lowest** one (oxid 196 in the
capture) drives `globalXmin` **below `writtenXmin`**, which re-exposes FROZEN slots:
`oxid_get_csn` then reads long-committed oxids as **IN_PROGRESS**, and the recovery
conflict-resolver — which cannot wait during recovery — **retries forever**.

---

## 1. The watermark invariant being broken

`XidMeta` ordering: `writtenXmin ≤ globalXmin ≤ runXmin ≤ nextXid`.

- `writtenXmin` = **frozen watermark**: oxids `< writtenXmin` have had their `xidBuffer`
  circular-buffer slots **recycled** and stamped `COMMITSEQNO_FROZEN`. Reading
  `xidBuffer[oxid]` below it is invalid.
- The *only* thing that makes `oxid_get_csn` correct for an old oxid is the fast path
  `oxid < globalXmin → FROZEN`. It is sound **iff `globalXmin ≥ writtenXmin`**.

**The bug writes `globalXmin = 196 < writtenXmin = 423`** — so oxids in `[196, 423)`
miss the fast path *and* have recycled slots.

**Asymmetry that makes it possible:** `globalXmin` is **bidirectional** —
`update_run_xmin` lowers it (`if (xmin < globalXmin) globalXmin = xmin`), while
`advance_global_xmin` only raises it. `writtenXmin` is **monotonic-up** (raised to
`globalXmin` at oxid.c:1186, never lowered). So once `writtenXmin` ratchets to 423,
a later lowering of `globalXmin` lands *under* it.

---

## 2. The full sequence (with log evidence)

### Primary — abort, lift, checkpoint, then deferred rollbacks
```
17:24:48.293  redo done at 0/3015100                                   ← recovery_finish (redo over)
17:24:48      recovery-finish-abort-trace … aborting in-flight oxid=196  (29 oxids, min=196 max=513)
17:24:48      current_oxid_abort set_oxid_csn(ABORTED) oxid=196         ← ACTUAL (in-memory) abort
17:24:48.311  checkpoint starting: end-of-recovery immediate wait       ← the gap WAL record
17:24:48.901  checkpoint complete … lsn=0/3015198
17:24:48.950  emitting WAL_REC_ROLLBACK for in-flight oxid 303 …
17:24:49.009  emitting WAL_REC_ROLLBACK for in-flight oxid 196 …        ← deferred rollbacks shipped
```
Order inside `recovery_finish` (recovery.c): **abort loop (1659) → `free_run_xmin()`
(1754) → end-of-recovery checkpoint → `o_emit_recovery_finish_rollbacks()` (1786)**.
So the in-memory abort happens *first*; `free_run_xmin` lifts `runXmin` to `nextXid`;
the checkpoint raises `globalXmin`/`writtenXmin`; the rollbacks are emitted last.

Each deferred pair on the wire (primary redo CONTEXT) is:
```
XID (196 0 0); ROLLBACK (196 0 0 - xmin 530 csn 513)
```
- the **`WAL_REC_XID` carries a bare oxid** (`196 0 0`), empty modify;
- the **`WAL_REC_ROLLBACK` stamps `xmin = 530`** — the *lifted* `runXmin`, **not** the
  oxid. **All 29** rollbacks carry the same high `xmin=530`. Contrast a normal
  rollback: `ROLLBACK (1003 … - xmin 902)` → `902 < 1003`.

### Replica — horizon climbs, then the lowest oxid is re-admitted under it
1. `recovery_xmin` advances **only** via `finish.xmin` on COMMIT/ROLLBACK
   (recovery.c:3821) — i.e. the **second** record of each pair, value `530`. The
   `WAL_REC_XID` (first record) never touches `recovery_xmin`; the checkpoint never
   touches it either.
2. The 28 higher aborted oxids (all `> 196`, `iters=0`/empty undo → finalize fast)
   enter and **leave** the queue; `recovery_xmin` climbs to 531. During the
   drained-queue windows, `update_run_xmin`'s `Min(queue_min, recovery_xmin)` picks
   `recovery_xmin`, so `runXmin` rises and `advance_global_xmin` ratchets
   **`writtenXmin` up to 423** (`globalXmin == writtenXmin == 423`, top of the climb).
3. Then `WAL_REC_XID(196)` — the **lowest** oxid, re-admitted late — sets
   `queue_min=196`; `update_run_xmin` computes `Min(196, 531)=196` and writes
   `globalXmin = 196 < writtenXmin = 423`. **Violation** (the LIVELOCK-ROOT line).

The replica has **no other records for oxid 196** — only the rollback's abort
(`walk_undo_stack oxid=196 abortTrx=1`, `iters=0`). It never saw 196 in-flight (its
WAL never streamed), which is *why* `writtenXmin` was free to pass it.

---

## 3. Why `globalXmin < writtenXmin` *is* the livelock

```c
/* oxid_get_csn() */
if (oxid < globalXmin) return COMMITSEQNO_FROZEN;   // the ONLY voucher for an old oxid
map_oxid(oxid, &csn, …);                            // else read the (recycled) slot
```
With `globalXmin=196`, oxid 389 (committed long ago, `196 ≤ 389 < 423`) **misses the
fast path** and `map_oxid` reads its **recycled** slot → SPECIAL → returns
**`COMMITSEQNO_INPROGRESS`**. (Empirically the wedged worker reads 389/343/388 →
`result_csn=0`.)

```c
/* o_btree_modify_handle_conflicts() */
csn = oxid_get_csn(oxid /*389*/, false);   // → INPROGRESS (modify.c:80)
…
Assert(COMMITSEQNO_IS_INPROGRESS(csn));     // modify.c:141 — "conflicting txn in-progress"
if (context->callbackInfo->waitCallback) { … }   // NULL during recovery
return ConflictResolutionRetry;             // modify.c:181

/* o_btree_modify_internal() */
if (resolution == ConflictResolutionRetry) goto retry;   // modify.c:120
```
A recovery worker re-applying a modify finds an existing tuple stamped with oxid 389,
reads it as IN_PROGRESS, has **no `waitCallback`** (workers can't block in recovery),
so it **retries** — re-locks, re-finds 389, reads IN_PROGRESS again. 389 is a
**phantom** (committed; its slot is recycled; `globalXmin` is stuck at 196), so it
never flips → **infinite retry**.

Measured: `handle_conflicts` **enter 6,703,881 / exit 1,486**.

**Self-sustaining:** the wedged worker can't advance its `commitPtr` →
`get_workers_commit_ptr()` can't move → `finished_list` can't drain → nothing raises
`globalXmin` back → 389 stays mis-read → the worker stays wedged.

---

## 4. The primary hits it too (not replica-only)

Replaying its own deferred rollbacks during a later crash recovery, the **primary**
logged three LIVELOCK-ROOT violations:
```
17:24:56.214  lowering globalXmin to oxid=330 BELOW writtenXmin=530  (globalXmin_was=530)
17:24:56.215  lowering globalXmin to oxid=303 BELOW writtenXmin=530  (globalXmin_was=330)
17:24:56.216  lowering globalXmin to oxid=196 BELOW writtenXmin=530  (globalXmin_was=303)
```
`globalXmin` was **530** (`== writtenXmin`, the lifted post-recovery horizon) and was
dragged down `530 → 330 → 303 → 196` by the replayed `WAL_REC_XID`s. Confirms: the
horizon is moved past the aborted oxids (by `free_run_xmin` + the end-of-recovery
checkpoint, **after** the in-memory abort) *before* the rollbacks re-introduce them.

---

## 5. Key clarifications resolved during the investigation

- **The rollback's `xmin` is not the damage.** It's high (530) and `Min(196, 531)`
  ignores it; the lowering comes from `queue_min=196` (the re-admitted oxid). The
  high `xmin` is the *enabler*: it lets `recovery_xmin`→`runXmin`→`writtenXmin` climb
  past 196 during the drained-queue windows. Without it, `writtenXmin` couldn't pass
  196 and there'd be no regression.
- **The checkpoint does not carry `recovery_xmin`.** It's the structural WAL record
  in the gap (and on the replica it re-fires `advance_global_xmin` via the
  restartpoint), but the horizon *value* travels in the rollback records' `finish.xmin`.
- **`recovery_xmin` *is* consumed** — by `update_run_xmin`'s `Min(queue_min,
  recovery_xmin)`, winning whenever the queue is clear of lower oxids.
- **Ordering is the trap, not abort-vs-horizon.** The primary aborts first, then
  lifts the horizon (self-consistent). The defect is that the **horizon-advancing
  checkpoint ships ahead of the abort-carrying rollbacks**, so the consumer raises
  `writtenXmin` over an oxid it is about to be told to re-create.

---

## 6. Fix directions

1. **Consumer-side floor (airtight, order/timing-independent).** In `update_run_xmin`:
   `xmin = Max(Min(queue_min, recovery_xmin), writtenXmin)` — or refuse to admit an
   oxid `< writtenXmin` into `xmin_queue`. Directly enforces `globalXmin ≥ writtenXmin`.
   Must keep the normal in-flight path (oxid `≥ writtenXmin`) untouched.
2. **Producer-side ascending sort (mitigation).** Sort `recovery_finish_aborted_oxids`
   ascending before the emit loop so the lowest oxid is admitted first and the horizon
   advances monotonically. Helps the parallel-recovery reproduction, but is timing-
   dependent (a fast finalize-before-next-admit can still race, esp. single-process).
3. **Don't ship a horizon ahead of the aborts** (deeper): make the deferred rollbacks
   carry no horizon-advancing information for those oxids (e.g. `InvalidOXid` finish
   xmin) and/or emit them before the end-of-recovery checkpoint.

Empirical gate for any fix: the SK-stress hunt (100×60s/6s) must show **0 livelock
timeouts AND 0 SK-divergence**, and the `replication_test.py` recovery tests must stay
green — green targeted tests alone do not imply livelock-free.
