# Issue: streaming standby crashes on the deferred `WAL_REC_ROLLBACK` of a resurrected in-flight oxid

**Status:** root-caused **end-to-end** and reproduced **deterministically** on `orioledb/orioledb`
`main` + PostgreSQL `patches17_20`. Primary-side spurious emit and replica-side `location = 0` PANIC
are both proven from real logs; a consumer-side fix is verified to close it.
**Severity:** high — a streaming hot-standby is taken down (fatal PANIC) after the primary
crash-recovers, and stays down (the toxic record is durable, so every restart re-PANICs).
**Component:**
[`src/checkpoint/checkpoint.c`](https://github.com/orioledb/orioledb/blob/main/src/checkpoint/checkpoint.c)
(`finish_write_xids`, `wait_finish_active_commits`),
[`src/recovery/recovery.c`](https://github.com/orioledb/orioledb/blob/main/src/recovery/recovery.c)
(`recovery_switch_to_oxid`, `o_emit_recovery_finish_rollbacks`),
[`src/recovery/wal.c`](https://github.com/orioledb/orioledb/blob/main/src/recovery/wal.c)
(`flush_local_wal`),
[`src/transam/undo.c`](https://github.com/orioledb/orioledb/blob/main/src/transam/undo.c)
(`walk_undo_stack`).
**Deterministic repro:** `test/t/replication_test.py::test_checkpoint_abort_snapshot_resurrects_aborted_oxid`.

> Code links point to `orioledb/orioledb` `main`; line anchors are as of commit `8c2588d6`
> (*recovery: allocate xmin_queue only for the recovery leader*) and may drift as `main` advances.

> ### Supersedes the earlier "non-durable rollback (routes A/B)" theory
> Earlier drafts framed the trigger as *"the abort left no durable rollback record"* — either a
> `!has_material_changes` fast-path no-op (route A) or a slow-path `WAL_REC_ROLLBACK` not fsync'd
> before the SIGKILL (route B). **The deterministic repro disproves that framing.** The reproducing
> rollback is **fully durable**: a material `UPDATE` rolled back under `fsync=on`,
> `synchronous_commit=on`, `full_page_writes=on`. Durability of the rollback is **not** the defect.
> The defect is the **checkpoint capturing the in-flight `vxids` snapshot and `replayStartPtr` at
> points that are mutually inconsistent for aborts** (§2). The routes-A/B analysis is retired.

---

## 1. One-paragraph summary

A transaction aborts with **material changes**, so `wal_rollback` writes a **durable**
`WAL_REC_ROLLBACK`. But a concurrent checkpoint snapshots the in-flight xid set from
`oProcData[].vxids[]` **before** the aborting backend clears its vxids slot, so it records the oxid
as in-flight even though its rollback is already durable and sits **below** the checkpoint's
`replayStartPtr`. After a crash, recovery rebuilds in-flight state from the durable checkpoint xids
dump, **skips** the sub-`replayStartPtr` rollback on replay, re-discovers a *phantom* in-flight
oxid, re-aborts it in `recovery_finish`, and emits a **spurious deferred `WAL_REC_ROLLBACK`** that
streams to the standby. On the standby that deferred rollback re-finds a recovery-leader hash entry
whose undo location was never materialized (left `0`), and the undo walk reads location `0` →
`PANIC: undo_item_buf_read_item(): read of unexisting undo record … location = 0`, taking the
standby down.

---

## 2. Primary side — the checkpoint abort-snapshot race (durable rollback)

Independent of `fsync` / `synchronous_commit` / WAL transmission (all verified clean):

1. A backend aborts a transaction that **did** material work. `wal_rollback` writes a
   `WAL_REC_ROLLBACK` and (under `synchronous_commit=on`) flushes it — the rollback is **durable**.
2. The abort path does **not** advertise itself to the checkpoint's quiesce wait.
   [`flush_local_wal(isCommit, …)`](https://github.com/orioledb/orioledb/blob/main/src/recovery/wal.c#L727)
   sets `oProcData[].commitInProgressXlogLocation` **only when `isCommit`** — never on the rollback
   path. So
   [`wait_finish_active_commits(redo_pos)`](https://github.com/orioledb/orioledb/blob/main/src/checkpoint/checkpoint.c#L674),
   which spins until every proc's `commitInProgressXlogLocation > redo_pos`, waits for in-progress
   **commits only**, not aborts.
3. While the aborting backend is between "rollback durable" and "vxids slot cleared",
   [`finish_write_xids`](https://github.com/orioledb/orioledb/blob/main/src/checkpoint/checkpoint.c#L900)
   scans `vxids[]`, gates only on `OXidIsValid(vxids[].oxid)`, and records the oxid in the
   **durable** checkpoint xids dump as in-flight.
4. The checkpoint's `replayStartPtr` is captured **above** the durable rollback record.
5. After SIGKILL + restart, recovery reads the dump → re-discovers the oxid in-flight; replays
   orioledb containers from `replayStartPtr`, which is **above** the rollback → the rollback is
   **skipped** (no `recovery_switch_to_oxid` for it); `recovery_finish` re-aborts the phantom; and
   [`o_emit_recovery_finish_rollbacks`](https://github.com/orioledb/orioledb/blob/main/src/recovery/recovery.c#L1850)
   emits the spurious deferred `WAL_REC_ROLLBACK`, which streams to the standby.

> Decisive point: the dump (snapshotted from `vxids`, *past* the rollback) and `replayStartPtr`
> disagree about whether the oxid is settled — **for aborts**. Commits are drained by
> `wait_finish_active_commits`; aborts are not.

---

## 3. Replica side — the leader's never-materialized `undo_stacks = 0`

`recovery_xid_state_hash` and its `undo_stacks` are a process-local `static HTAB *`. The standby's
recovery **leader** (startup, `worker_id = -1`) dispatches data modifies to **workers** by row-key
hash (`GET_WORKER_ID(hash)`); `worker_send_modify` ships the op/oids/tuple but **never** an undo
location. So for a dispatched, data-only txn the leader **never accumulates undo** — its
`undo_stacks[i]` is whatever the `!found` `memset` left (`0`,
[`recovery_switch_to_oxid`](https://github.com/orioledb/orioledb/blob/main/src/recovery/recovery.c#L1948)).

(The leader is **not** a pure dispatcher: it applies **system-tree / catalog** modifies itself —
`apply_sys_tree_modify_record` — and owns that undo (`UndoLogSystem`). That is why the leader runs
`apply_undo_stack` on *every* finish. For a data-only oxid its own stacks are correctly empty; the
bug is purely how "empty" is *represented* after a RETAIN + re-find.)

Lifecycle of the culprit on the leader
([`recovery_switch_to_oxid`](https://github.com/orioledb/orioledb/blob/main/src/recovery/recovery.c#L1885)):

| step | leader behaviour |
|---|---|
| real rollback switch | `!found` → `memset undo_stacks = 0`; `reset_cur_undo_locations` writes the **SENTINEL** (`InvalidUndoLocation`) into the shared slot, **masking** the zero → `apply_undo_stack` is a safe no-op (the worker that owns the row walked the real undo) |
| finish bookkeeping | entry **RETAINed** on `finished_list` (deferred finalization) and **detached** (`cur_recovery_xid_state = NULL`), so the `0` is never re-saved / over-written with the sentinel |
| gap until deferred | the `finished_list` drain (`update_proc_retain_undo_location`, gated by `get_workers_commit_ptr()` = min worker `commitPtr`) has **not** yet removed it |
| deferred rollback switch | `found = 1` (still retained) → `set_cur_undo_locations` propagates the raw `undo_stacks[i] = 0` into the shared slot (**not** masked) |
| deferred rollback apply | `apply_undo_stack` → `walk_undo_stack` reads location `0` → [`undo_item_buf_read_item(): read of unexisting undo record … location = 0`](https://github.com/orioledb/orioledb/blob/main/src/transam/undo.c#L1087) **PANIC** |

**Decisive asymmetry — `!found` vs `found`:** `!found` masks the zeroed stack with the sentinel
(safe no-op); `found` propagates the raw `0` (crash). Whether the deferred rollback hits `found`
depends only on whether the drain removed the RETAINed entry first — i.e. on `min worker commitPtr`
vs the culprit's rollback LSN.

**Why a quiescent standby does *not* crash:** with no load the workers catch up, the drain DELETEs
the culprit, and the deferred rollback re-creates it `!found` → masked → safe. The crash needs the
workers to **lag** so the culprit is still RETAINed when the deferred rollback arrives.

---

## 4. Crash characterisation — single fatal PANIC, not a crash-loop or livelock

On `main` the standby hits a **single fatal PANIC** and shuts down:

```
PANIC:  undo_item_buf_read_item(): read of unexisting undo record with undoType = 0, location = 0, ...
CONTEXT:  WAL redo at <lsn> ... XID (54 0 0); ROLLBACK (54 0 0 - xmin 54 ...)
startup process (PID …) was terminated by signal 6: Aborted
shutting down due to startup process failure
database system is shut down
```

It is **not** an automatic crash-loop: a PANIC in a standby's **startup (recovery) process** is
unrecoverable — the postmaster shuts the cluster down rather than restarting it, **even with**
`restart_after_crash = on` (verified). It is effectively worse than a loop — the toxic deferred
`ROLLBACK` is durable in the replicated stream, so **every manual restart re-PANICs on the same
record**; the standby is permanently unrecoverable until that WAL is past.

In assert / `IS_DEV` builds where `checkpointRetainStartLocation == 0`, the garbage at undo offset 0
trips the undo-item-type `Assert` in `undo.c` *before* the explicit `location = 0` PANIC — same root
cause, same fatal outcome.

> An earlier "Mode 1 — `globalXmin` regression livelock" was a production-build / reader-induced
> horizon variant observed only under the chaos hunt; the deterministic repro produces the
> `location = 0` PANIC (historically "Mode 2"). Both are the same defect — a deferred rollback for an
> oxid the standby has already passed.

---

## 5. Deterministic reproduction

`test/t/replication_test.py::test_checkpoint_abort_snapshot_resurrects_aborted_oxid` reproduces the
whole chain on one primary + one streaming standby, using **two stop events** (`orioledb.enable_stopevents
= on`; **no injection points / no `IS_DEV`-only traces**). Both stop events are added by this change
(they are **not** in `orioledb/orioledb` `main` yet):

- **`before_abort_vxids_clear`** — added in
  [`current_oxid_abort`](https://github.com/orioledb/orioledb/blob/main/src/transam/oxid.c#L1492),
  right before the vxids slot is cleared. Forces §2: a material `UPDATE` is rolled back (durable
  `WAL_REC_ROLLBACK`), the backend parks with its vxids slot still advertising the oxid in-flight, a
  checkpoint runs to completion capturing it in the dump, then the cluster is SIGKILL'd and restarted
  → recovery emits the spurious deferred rollback.
- **`replay_on_record`** — added at the top of
  [`replay_on_record`](https://github.com/orioledb/orioledb/blob/main/src/recovery/recovery.c#L3981).
  Forces §3's RETAIN window via *backlog-then-burst*: park the standby's recovery leader so the
  walreceiver piles up a large committed bulk workload behind it, then release it. The leader bursts
  through the backlog far ahead of the workers (dispatch is cheap, apply is not; a small
  `orioledb.recovery_queue_size` stops workers reading ahead), so `min worker commitPtr` stays far
  below the culprit's rollback LSN, the drain cannot remove it, and the deferred rollback re-finds it
  (`found = 1`).

The test asserts **survival** (the standby stays `Running` and replays *past* the deferred rollback
to a probe LSN). It therefore **passes on a fixed build and fails on a buggy one** — a regression
guard.

Verified (PG `patches17_20`):

| build | primary | standby | test |
|---|---|---|---|
| **unfixed** | emits spurious `WAL_REC_ROLLBACK` for oxid 54 | `PANIC … location = 0` → shut down | **FAIL** |
| **fixed** | still emits it (now harmless) | replays it as a no-op, stays up | **PASS** |

---

## 6. Fix

**Verified (consumer side, §3).** In
[`recovery_switch_to_oxid`](https://github.com/orioledb/orioledb/blob/main/src/recovery/recovery.c#L1948)'s
`!found` branch, after the `memset`, mark each undo stack invalid so a re-find restores the sentinel
rather than the raw `0`:

```c
memset(cur_state->undo_stacks, 0, sizeof(cur_state->undo_stacks));
for (i = 0; i < (int) UndoLogsCount; i++)
    undo_stack_locations_set_invalid(&cur_state->undo_stacks[i]);
```

(`undo_stack_locations_set_invalid` is the existing helper at
[recovery.c#L856](https://github.com/orioledb/orioledb/blob/main/src/recovery/recovery.c#L856).) A
RETAINed, never-materialized entry re-found by the deferred rollback (`found = 1`) now yields
`InvalidUndoLocation` (the sentinel) instead of `0`, so `apply_undo_stack` no-ops — the same outcome
as the `!found` path. Verified: unfixed → FAIL with the `location = 0` PANIC; fixed → PASS with the
scenario still armed (the primary still emits the spurious rollback).

**Invariant:** re-finding a retained-but-never-materialized recovery xid-state entry must restore
`InvalidUndoLocation`, not `0` — the `!found` reset's masking must survive a re-find.

**Alternative (primary side, §2), not implemented.** Make the in-flight `vxids` snapshot consistent
with `replayStartPtr` for aborts: either drain in-flight **aborts** before the dump (not only
commits), or have `finish_write_xids` exclude an oxid whose `WAL_REC_ROLLBACK` is already durable
below `replayStartPtr`. Either side's fix alone stops the crash.

---

## 7. Why an emit-gate guard does not close it

A guard keyed on `checkpoint_xid && !wal_xid && oxid < recovery_xmin` at the emit site is
unreliable: the culprit arrives as / produces a `WAL_REC_XID` (so `!wal_xid` is false), and
`oxid < recovery_xmin` is case-dependent (it holds only when `recovery_xmin` happened to advance
past the specific culprit, which varies with how much WAL was fsync'd before the kill). The robust
fixes are the two in §6 — consumer-side masking, or primary-side snapshot consistency for aborts.

---

## 8. Enabling the test in CI

- Already discovered by `testgrescheck` (`replication_test.py` is in `TESTGRESCHECKS_PART_1`); no
  schedule edit needed.
- Needs committed: the two stop events (`stopevents.txt` + the `STOPEVENT` call sites in
  `recovery.c` and `oxid.c`) and the `wait_stopevent` import in `replication_test.py`
  (`wait_stopevent` itself already lives in `base_test.py`). **No injection points / no `IS_DEV`-only
  dependency.**
- The test asserts survival, so it must land **together with the §6 fix** — otherwise CI is red.
