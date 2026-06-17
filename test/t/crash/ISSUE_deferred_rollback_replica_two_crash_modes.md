# Issue: streaming standby crashes on the deferred WAL_REC_ROLLBACK of a resurrected in-flight oxid (two crash modes)

**Status:** root-caused **end-to-end** — primary-side emit (§4.1, checkpoint abort-snapshot race) and replica-side PANIC (§4.2, leader's never-materialized `undo_stacks=0` propagated `found`) both fully traced over multiple catches; two fix-relevant invariants identified (one per side). Committed fix series `f071202a` does not close it (see §6).
**Severity:** high — a streaming hot-standby PANICs (or, in production/non-assert builds, livelocks) after the primary crash-recovers
**Component:** `src/recovery/recovery.c` (read_xids, update_run_xmin, o_emit_recovery_finish_rollbacks), `src/recovery/wal.c` (wal_rollback), `src/transam/` (undo/CSN/horizon)
**Branch:** `add_stress_bank_account_test`
**Repro harness:** `test/t/crash/rr_stress_test.py` with `RR_REPLICA_MODE=streaming`, `RR_KILL_POSTMASTER=1`

---

## 1. One-paragraph summary

A transaction that is **in-flight at a primary crash** (SIGKILL) but whose **abort left no durable rollback record** is **rediscovered as in-flight by crash recovery** (it is still listed in the checkpoint's xids dump). Recovery then emits a **deferred `WAL_REC_ROLLBACK`** for it. The streaming standby has meanwhile advanced its horizon **past** that oxid (the primary streamed durable commits carrying a higher `runXmin`, and the standby froze the band). When the standby replays the deferred rollback — after a walreceiver reconnect, with its in-memory watermark persisted — it re-introduces that already-passed oxid, and **crashes**. The crash lands in one of **two distinct sites** depending on which the replay hits first (§2). The defect is the same in both: a per-instance, durable-only reconstruction of "who is in-flight" disagrees with the standby's already-published horizon.

There are **two ways** an abort leaves no durable rollback (§4): (A) the `!has_material_changes` **fast path** writes nothing; (B) a **slow-path** abort writes a `WAL_REC_ROLLBACK` that is **not fsync'd before the SIGKILL**. The reproducing catches in this session were predominantly route **B**.

---

## 2. The two crash modes

Both are triggered by the same toxic record (the deferred rollback of a resurrected in-flight oxid). They differ only in where the standby dies.

| | **Mode 1 — globalXmin regression / monotonicity assert** | **Mode 2 — undo-read PANIC** |
|---|---|---|
| crash site | `update_run_xmin()`: re-admitted oxid drags `runXmin` below the frozen `globalXmin` → `Assert(xmin >= globalXmin)` (assert build) or silent `globalXmin` lowering → re-exposed frozen band → conflict-retry **livelock** (production build) | `walk_undo_stack()` while *replaying* the empty `ROLLBACK (N 0 0)`: reads undo at `location=0` → `PANIC: undo_item_buf_read_item(): read of unexisting undo record ... location = 0` |
| when in the replay | **after** the rollback is applied (horizon recompute) | **during** the rollback's undo walk — earlier, so it wins the race and Mode 1's assert never fires |
| example oxid | **131** (`xidsdump` trial 1), 389 (`ddcatch`), 45 (`catchrepl3`) | **400** (`xmintrace` catch), 460 (`fixca00562d` trial 16) |
| internal label | the "recovery livelock" headline bug | "Bug #3" in the `wal.c` injection-point comments |

### Mode 1 — globalXmin regression (oxid 131, `xidsdump`)

Replica TRIGGER line at the crash:
```
update_run_xmin TRIGGER xmin=131 < globalXmin=155 (queue_min_oxid=131 recovery_xmin=155
    nextXid=242 writtenXmin=155) *** ASSERT-WILL-FIRE ***
TRAP: failed Assert("xmin >= globalXmin"), recovery.c
```
The standby had frozen `[87,155)` (covering 131); the deferred `ROLLBACK (131 0 0 - xmin 87 csn 226)` re-admitted 131 into `xmin_queue` below the frozen watermark; `Min(queue_min=131, recovery_xmin=155)=131 < globalXmin=155` → assert.

### Mode 2 — undo-read PANIC (oxid 400, `xmintrace`)

```
WAL redo ... XID (400 0 0); ROLLBACK (400 0 0 - xmin 350 csn 362)
PANIC: undo_item_buf_read_item(): read of unexisting undo record with undoType = 0, location = 0,
       transactionUndoRetainLocation = 0x2000000000000000, ...
```
`update_run_xmin TRIGGER` fired **0** times here and globalXmin only ever **rose** — the undo walk PANICs before any horizon recompute. The emitted record has `location 0` (empty) even though oxid 400's **checkpoint** entry had **real** undo (`kind=0 undoLoc=134808`); why the deferred record comes out empty for a material oxid is the Mode-2-specific question.

---

## 3. Evidence (verified by grep, this session)

For oxid 400 (`results/20260615_182611_xmintrace_NONE_replicatrap*`), primary side:

1. **In-flight per crash recovery:** `recovery-finish-abort-trace ... aborting in-flight oxid=400` @18:25:11.598.
2. **In the checkpoint xids file:** `read_xids REC oxid=400 kind=0 undoLoc=134808 ...` @18:25:11.598 (read straight from `orioledb_data/<n>.xid`; the saved primary datadir is preserved).
3. **Aborted in memory before the SIGKILL, but via the SLOW path:**
   ```
   18:25:10.046  o_btree_normal_modify action=2 oxid=400      (real modifies)
   18:25:10.601  undo_xact_callback event=2 (ABORT) -> abort -> wal_rollback begin
   18:25:10.609  wal_rollback end
   18:25:10.610  current_oxid_abort set_oxid_csn(ABORTED)
   ```
   `grep "wal_rollback FAST-PATH oxid=400"` = **0** → it had `has_material_changes=true` → slow path → a durable `WAL_REC_ROLLBACK` was written to the WAL buffer but **not fsync'd before the kill** (~18:25:11), so recovery never saw it. (All other emitted oxids 350/354/382/425/451 are the same: real undo, no fast-path.)
4. **Not drained by `update_run_xmin()`:** `grep -c "DRAIN oxid=400|DRAIN-BLOCKED oxid=400"` = **0** (the drain only targets `checkpoint_xid && !wal_xid`; a slow-path abort emits a `WAL_REC_XID` → `wal_xid=true` → outside the drain's reach).

For oxid 131 (`results/20260615_172739_xidsdump*`, **Mode 1**), primary side (relevant checkpoint = `checkpoint=3 count=231`, read at 17:26:42.008):

1. **In-flight per recovery:** `recovery-finish-abort-trace ... aborting in-flight oxid=131` @17:26:42.058.
2. **In the checkpoint xids file — EMPTY undo:** `read_xids REC oxid=131 kind={0,1,2} undoLoc=0x2000000000000000 (Invalid)` @17:26:42.009. (Contrast oxid 400's `kind=0 undoLoc=134808`, real.)
3. **Aborted in memory before the SIGKILL:** `o_btree_normal_modify action=2 oxid=131` @17:26:38.471 → `undo_xact_callback event=2 → wal_rollback begin/end → current_oxid_abort set_oxid_csn(ABORTED)` @17:26:39.920, with the SIGKILL burst at 17:26:40. The backend emitted **no** durable `WAL_REC_ROLLBACK` (the only one is recovery's, @17:26:42.664). Empty checkpoint undo + no backend rollback = the **route-A (fast-path) signature**. (Caveat: this build predates the `wal_rollback FAST-PATH` trace, so fast-path is *inferred* here, not trace-proven.)
4. **Not drained:** `grep -c "DRAIN oxid=131|DRAIN-BLOCKED oxid=131"` = 0.

### 3.1 The crash mode is NOT determined by the abort route

An earlier draft hypothesised "Mode 1 ↔ route A (empty/fast-path), Mode 2 ↔ route B
(material/slow-path)". Two full-trace Mode-1 catches **refute** it — Mode 1 occurs with
*both* routes, including a **trace-confirmed fast-path** culprit:

| culprit | mode | checkpoint undo | fast-path? (`wal_rollback FAST-PATH` trace) | route |
|---|---|---|---|---|
| 131 (`xidsdump`) | 1 (assert) | empty (`Invalid`) | inferred (build lacked the trace) | A |
| **382 (`fphunt` trial 34)** | **1 (assert)** | **empty (`Invalid`)** | **YES — `FAST-PATH oxid=382 (no material changes)`; ZERO `o_btree_normal_modify`** | **A (confirmed)** |
| **371 (`modeone`)** | **1 (assert)** | **real (`115032`)** | **NO (`FAST-PATH`=0; slow path)** | **B** |
| 400 (`xmintrace`), 369 (`walxidc`) | 2 (undo-read) | real | NO (slow path) | B |

Oxid **382** is the decisive case: a **pure `orioledb_get_current_oxid()` transaction with
no DML** (zero `o_btree_normal_modify`, empty checkpoint undo) whose abort took the
`wal_rollback` fast path (`no material changes`) — and it still produced a Mode-1
`TRIGGER xmin=382 < globalXmin=383` → `Assert recovery.c`. It was caught by a 6-hour hunt
running the **fast-path-only rollbacker** (`RR_ROLLBACKERS=10`, `8dd10b17`) **with readers**.

So **Mode 1 occurs with BOTH routes** — fast-path/empty (382, 131) *and* slow-path/material
(371) — and route B produces both modes. The crash mode is simply **whichever site the
replay reaches first** for the toxic record — `update_run_xmin` (Mode 1) vs the undo walk in
the rollback's replay (Mode 2) — independent of how the rollback lost durability. The single
necessary fact is only: "aborted in memory before the kill, leaving **no durable rollback**
that recovery replayed."

Why a *pure* fast-path oxid (which the drainer's `!wal_xid` clause is meant to catch)
nonetheless reaches Mode 1: it is drainable on the **primary** (no `WAL_REC_XID`), but on the
**standby** it arrives as the deferred rollback's *streamed* `WAL_REC_XID` → `wal_xid=true`
→ outside the drain → re-admitted below `globalXmin`. **Readers are required** for the
horizon geometry that freezes a band above it (a no-reader fast-path-rollbacker run,
`drainrace`, caught nothing — those oxids were drained before the standby froze past them).

### 3.2 Catches are often mis-classified as ERROR (harness note)

A replica PANIC leaves the postmaster in a state where `pg_ctl stop` times out (`server does not shut down`), and that teardown exception masks the trial as **ERROR**, not BUG (e.g. `xmintrace` trial 9, `walxidc` trial 15). The `_save_data_dirs`/`_replica_trapped` hook still fires first and preserves the catch (logs + both data dirs). So **scan saved `*_replicatrap_*` artifacts, not just BUG-status trials** — and subsequent trials in the same run cascade to ERROR because the stuck replica blocks setup.

---

## 4. Root cause

`runXmin`/`globalXmin` is a **per-instance, in-memory recyclability horizon**. The primary advances it when an oxid resolves (commit OR in-memory abort both call `advance_run_xmin`) and streams it; the standby follows and **freezes** the band, treating those oxids as settled/recyclable. But the resolving abort can leave **no durable record**, via either route:

- **Route A — fast path:** `wal_rollback` takes the `!has_material_changes` no-op (wal.c:425) and writes nothing. (A transaction can hold an oxid with no material changes — e.g. `orioledb_get_current_oxid()` with no DML — because oxid assignment and `has_material_changes` are independent.)
- **Route B — slow path, not fsync'd:** the abort has material changes, so `wal_rollback` writes a `WAL_REC_XID + WAL_REC_ROLLBACK`, but the SIGKILL lands before that WAL is fsync'd, so it is lost.

Either way the oxid remains in the **durable** checkpoint xids dump as in-flight. Crash recovery rebuilds in-flight state from durable artifacts only, **re-discovers** the oxid, and emits a **deferred `WAL_REC_ROLLBACK`** at a higher LSN than where the standby's horizon already passed it. The standby's watermark survives the walreceiver reconnect (it's shmem state), so the late record lands **below** the frozen watermark → Mode 1 or Mode 2 crash.

The deepest framing: the **primary** decides whether to emit/resurrect using its **own** recovery horizon, but the only horizon that makes the oxid *toxic* lives on the **standby** (which froze the band) — a cross-instance reference-frame mismatch the primary cannot see.

### 4.1 The checkpoint abort-snapshot race — verified primary-side mechanism (oxid 372, full durability)

Verified on the `gxminfix7` catch (`results/saved_catches/20260616_gxminfix7_mode2_oxid372_floor_0b2e5e33/`), where the cluster ran **fully durable**: `fsync=on`, `synchronous_commit=on`, `full_page_writes=on` — pinned by `rr_stress_test.py:164-174` when postmaster-kill is enabled (the testgres base `fsync=off` at `postgresql.conf` line 2 is **overridden** by the appended `fsync=on` at line 20; `synchronous_commit` is unset → default `on`). So Routes A/B (no durable record) do **not** apply to 372 — its rollback **was** durable:

- `pg_waldump` of the primary's *own* WAL shows 372's container at **`0/300D110`** (`custom129`, 173 bytes = `XID(372 320); UPDATE; UPDATE; ROLLBACK xmin=360`). It is fsync'd — the walsender ships only fsync'd WAL (`walsender.c:3208-3215`; `GetFlushRecPtr` = `LogwrtResult.Flush`, advanced **after** `issue_xlog_fsync`, `xlog.c:2489→2494`) — and the replica replays it (215×). **No WAL divergence**: primary and replica hold identical WAL, timeline 1. (Earlier "divergence" claims were a redo-trace-vs-WAL artifact and are retracted.)

So why does recovery still treat 372 as in-flight and emit a deferred rollback? Because the checkpoint snapshots the in-flight xids set and the recovery replay-start point **inconsistently with respect to aborts**. In `o_perform_checkpoint` (`checkpoint.c`):

- `:1428` `replayStartPtr = get_checkpoint_xlog_ptr()` — the WAL position **OrioleDB** replays its own container records *from* (this is **not** PG's checkpoint redo lsn — see the two-layer note after the race window).
- `:1429` `wait_finish_active_commits(replayStartPtr)` — drains only in-flight **commits**: it spins on `oProcData[i].commitInProgressXlogLocation`, which is set **only on the commit path** (`wal.c:900-923`, `if (isCommit) …commitInProgressXlogLocation = …`). An in-flight **abort never sets it**, so the checkpoint does **not** wait for in-flight aborts.
- `:1468` `finish_write_xids()` then snapshots in-flight oxids straight from `oProcData[i].vxids[j].oxid` (`checkpoint.c:931-947`) — recording whatever oxid still occupies a proc slot, with its `undoLocation`.

**Same-clock timing proof (one node, no skew) that the checkpoint snapshotted 372 mid-abort.** Checkpoint #4 ran `[10:21:30.184 starting "immediate force wait" → 10:21:30.827 complete]` (redo lsn `0/300C850`); 372's *entire* abort ran **inside** that window — `csn-trace abort wal_rollback begin 30.319 → end 30.350 → current_oxid_abort 30.355` (the `30.355` step is where `vxids[j].oxid` + `undoLocation` are cleared). So `finish_write_xids`, scanning during the window, read 372's proc slot **before** `30.355` cleared it, and the dump it wrote back lists 372 with a **live** `undoLoc=111904` (the real byte offset `0x1B520`, **not** the `0x2000000000000000` invalid sentinel that kinds 1/2 carry). The dump *contents* prove the mid-abort capture without needing the exact sub-scan timestamp.

Race window for a transaction **mid-abort** during the checkpoint:
1. its `wal_rollback` has already written `WAL_REC_ROLLBACK` **below** OrioleDB's `replayStartPtr` (372 → `0/300D110` < replay-start `0/300D1C0`);
2. `wait_finish_active_commits` does not wait for it (abort ≠ commit);
3. its `vxids[j].oxid` slot is not yet cleared, so `finish_write_xids` records it as in-flight (`undoLoc=111904`).

Result: the **durable** checkpoint dump says "372 in-flight," but 372's resolving rollback sits **before** `replayStartPtr` and is **skipped on replay** (OrioleDB's first replayed container is at `0/300D1C0`; no `recovery_switch_to_oxid(372)` occurs). Recovery re-discovers a phantom in-flight 372, `recovery_finish` aborts it (`recovery-finish-abort-trace … aborting in-flight oxid=372` @31.805, workers 0/1/2), and emits the spurious deferred `WAL_REC_ROLLBACK` at `0/300D858` (`xmin=372`, the floor → dodges both the drain and the `oxid >= recovery_xmin` emit-gate).

**Two replay layers — do not conflate them (this is the look-alike-evidence trap here).** PG's own redo for this recovery ran `redo starts at 0/300C850` → `redo done at 0/300D668`, so 372's rollback container (`0/300D110`) **is** inside PG's WAL-scan range. But OrioleDB applies its *own* container records only from its `replayStartPtr` = `0/300D1C0` (verified: the first `WAL redo … OrioleDB WAL container` is `XID (420 672 0)` at `0/300D1C0`, @31.788). Since `0/300D110 < 0/300D1C0`, OrioleDB **skips** 372's resolving rollback even though PG's redo passed over that LSN — which is why 372 stays in-flight *at the orioledb layer* and `recovery_finish` re-aborts it. Reading the PG redo lsn (`0/300C850`) as "the replay start" would falsely conclude the rollback was replayed; the orioledb `replayStartPtr` is the load-bearing one.

Evidence (verified this session, gxminfix7 catch):

| code prediction | artifact |
|---|---|
| checkpoint window straddles 372's abort | chkpt `30.184 → 30.827` ⊃ abort `30.319 → 30.355` (one clock) |
| dump snapshots mid-abort oxid from `vxids` | `read_xids checkpoint=4 … oxid=372 kind=0 undoLoc=111904` (live, all workers); kinds 1/2 = invalid sentinel |
| OrioleDB `replayStartPtr` ≠ PG redo lsn | PG redo `0/300C850 → 0/300D668`; OrioleDB first container `0/300D1C0` |
| abort's rollback is below OrioleDB `replayStartPtr` → skipped | 372 container at `0/300D110` (< `0/300D1C0`); no `recovery_switch_to_oxid(372)` |
| skipped → re-abort → deferred emit | `recovery-finish-abort 372` @31.805 → `emit_recovery_rollback oxid=372 recovery_xmin=372 runXmin=372 nextXid=423` @32.381 → record at `0/300D858` |

**This is a third route, distinct from A/B:** the rollback is *durable* but **below `replayStartPtr`**, so recovery skips it while the dump (snapshotted past it, from `vxids`) still lists the oxid in-flight. It refines §4's "the oxid remains in the dump because the abort left no durable record" — here the record exists and is durable; the defect is the checkpoint capturing the in-flight set and `replayStartPtr` at points inconsistent for aborts. (Route B — "slow-path, not fsync'd" — may in some earlier catches actually be this same race; not re-verified for 131/400/371.)

**Why the replica panics (not the primary):** the standby applies the real rollback, then the primary's **deferred** duplicate of the same oxid; replaying the duplicate walks undo at `location=0` (below the undo floor `checkpointRetainStartLocation=13600`) → `undo_item_buf_read_item` PANIC. *The precise per-process mechanism (it is NOT a simple "rebuild fresh on switch") is fully traced in §4.2 — the earlier one-line story here was incomplete.*

**Fix direction:** make the in-flight snapshot consistent with `replayStartPtr` for aborts too — either have the pre-dump quiesce drain in-flight **aborts** (not only commits), or have `finish_write_xids` exclude an oxid whose `WAL_REC_ROLLBACK` is already durable at `< replayStartPtr`. This is a **checkpoint/recovery** defect on the primary, independent of `fsync`, `synchronous_commit`, and WAL transmission (all verified clean).

### 4.2 Replica-side Mode-2 mechanism — fully traced (the leader's never-materialized `undo_stacks=0`)

Once the primary emits the spurious deferred `WAL_REC_ROLLBACK` (§4.1), the standby must replay it. Why that replay walks `location=0` was, until now, hand-waved ("rebuilt fresh with zeroed undo"). With three added recovery traces — `switch_to_oxid` (`found`/`checkpoint_xid`/`undoStack0`/`sharedLoc0`), `check_delete` (DELETE vs RETAIN + the three gating flags), and `switch_away` (the previous oxid's `undo_stacks[0]` saved on a switch) — it is now traced end-to-end. **Confirmed across 4 independent catches with identical signature: oxid 382 (`awayhunt`), 296 + 400 (`retainhunt2`), 229 (`multirepl5`).**

**The state is per-process and not shared.** `recovery_xid_state_hash` and its `undo_stacks` are a process-local `static HTAB *` (`recovery.c:222`). The leader (startup, `worker_id=-1`) dispatches modify records to workers **by row-key hash** (`o_btree_hash` → `GET_WORKER_ID(hash) = hash % recovery_pool_size_guc`, `recovery.c:5179-5186`, `internal.h:41`); `worker_send_modify` (`recovery.c:4789`) ships **only** the op type, the oxid (as a change-marker), the oids, and the tuple — **never an undo location/undo-stack**. Each worker rebuilds its own undo via `make_undo_record`. So the leader and every worker hold **independent, divergent** copies of an oxid's undo; the leader never receives the workers' undo locations.

For a dispatched txn the **leader never accumulates undo** — its `undo_stacks[oxid]` is whatever the `!found` `memset` left (`0`, `recovery.c:1949`). The finish (rollback) record is broadcast to all workers (`workers_send_oxid_finish`) **and** run by the leader (`recovery_finish_current_oxid`, `worker_id=-1`), which always calls `apply_undo_stack` → `walk_undo_stack` on the **leader's own** shared slot.

Lifecycle of the culprit on the **leader** (verified, oxid 229 / `multirepl5`; pid 3328527 = startup, pid 3328531 = worker 1):

| step | leader (startup, pid 3328527) | code | trace |
|---|---|---|---|
| real rollback switch | `!found` → `memset undo_stacks=0` → `reset_cur_undo_locations` sets shared=**SENTINEL** | recovery.c:1949, 1953 | `switch_to_oxid oxid=229 found=0 undoStack0=0 sharedLoc0=SENTINEL` |
| real rollback apply | `apply_undo_stack` walks **SENTINEL** → **no-op, safe** (leader owns no undo for 229; worker 1 walked the real undo `location=84336 iters=3`) | recovery.c:2148 | (worker-1 walk shows 84336) |
| finish bookkeeping | RETAIN on `finished_list` (`!sync` postpone) **and detach** `cur_recovery_xid_state = NULL` | recovery.c:2157, **2220** | `check_delete oxid=229 in_finished_list=1 -> RETAIN` |
| gap until deferred | entry retained; `undo_stacks=0` **never re-saved** (the `1900` switch-away save never fires for a detached oxid) | recovery.c:1900 | **zero** `switch_away prev_oxid=229` |
| deferred rollback switch | `found` (still on `finished_list`) → `set_cur_undo_locations(undo_stacks=0)` → shared=**0** (NOT masked) | recovery.c:1926 | `switch_to_oxid oxid=229 found=1 undoStack0=0 sharedLoc0=0` |
| deferred rollback apply | `apply_undo_stack` → `walk_undo_stack` reads **0** → `undo_item_buf_read location=0` < floor 13600 → **PANIC** | recovery.c:2148, undo.c:1353 | `walk_undo_stack abort-branch oxid=229 location=0` + PANIC |

**The decisive asymmetry is `!found` vs `found`:** the `!found` branch calls `reset_cur_undo_locations` which writes the **SENTINEL** into the shared slot (`undo.c:2038`), *masking* the zeroed `undo_stacks` → safe no-op. The `found` branch instead **propagates the raw `undo_stacks[0]=0`** (`set_cur_undo_locations`, `recovery.c:1926`) → shared `0` → crash. The chain `undo_stacks[0]=0 → set_cur_undo_locations → write_shared_undo_locations (to->location=from->location, undo.c:2013) → walk_undo_stack reads sharedLocations->location (undo.c:1353)` is proven.

**Why the leader takes `found` (and `undo_stacks` stays `0`):** `recovery_finish_current_oxid` RETAINs the entry on `finished_list` (deferred finalization) and sets `cur_recovery_xid_state = NULL` (`recovery.c:2220`). Because it is detached, no later `recovery_switch_to_oxid` ever runs the `1900` save against it, so the `!found`-memset `0` is **never overwritten with the sentinel**. The `finished_list` drain (`update_proc_retain_undo_location`, `recovery.c:2961`) is LSN-ordered/gated and had not removed it before the deferred rollback's switch re-attaches (`found`) and the walk PANICs.

**Why the minimal deterministic test (oxid 102) does NOT crash:** there, the entry is **DELETEd** (not retained) after the real rollback, so the deferred rollback re-creates it `!found` → masked to the sentinel → safe. RETAIN-vs-DELETE is the differentiator; the chaos hunt produces RETAIN (via `in_finished_list` / retain-undo heaps under reader-driven horizon geometry, §5), the minimal test produces DELETE.

**Fix-relevant invariant (consumer side):** for an oxid the leader never accumulated undo for, the finish-time `apply_undo_stack` must walk the **SENTINEL** (no-op), not `0`. The retained `finished_list` entry's zeroed `undo_stacks` must not be propagated unmasked by the `found` branch — i.e. re-finding a retained-but-never-materialized entry must restore `InvalidUndoLocation`, not `0` (equivalently: the `!found` reset's masking must survive re-find). This is independent of the §4.1 primary-side fix; either side's fix alone should stop the PANIC.

---

## 5. Reproduction

```bash
cd /home/user/work/orioledb && source ../venv/bin/activate
RR_REPLICA_MODE=streaming RR_DURATION=60 RR_KILL_POSTMASTER=1 RR_KILL_POSTMASTER_INTERVAL=3 \
  RR_WRITERS=20 RR_ACCOUNTS=100 RR_READERS_PK=6 RR_READERS_SK=6 RR_READERS_MIXED=6 \
  RR_INJECTION_POINTS=NONE RR_ASSERT_FIRINGS=0 RR_PANIC_FATAL=0 RR_INSTANCE=repro \
  python3 -m unittest test.t.crash.rr_stress_test.RrStressTest.test_bank_account_invariant
```
- **Readers are required** (~240 no-reader trials caught 0; with readers the rate is single-digit %). Readers' back-dated RR snapshots create the horizon geometry where the standby freezes a band *above* an oxid the primary will resurrect.
- `kill=3s` raises the rate vs `kill=6s`.
- Assert build (`IS_DEV=1`) → Mode 1 TRAPs / Mode 2 PANICs; production build → Mode 1 livelocks.

A **deterministic** test is not yet valid, but the obstacle is the *horizon geometry*, not the
abort route. Oxid 382 proves a **pure `get_current_oxid()` (route-A) transaction DOES reproduce
Mode 1** under chaos — so the existing `replication_test.py::test_crash_recovery_drops_fastpath_aborted_xids_silently`
and `test_streaming_standby_survives_fastpath_aborted_oxid_deferred_rollback` pass on the buggy
build **not** because route A can't reproduce, but because their setup (durable advancing COMMITs +
full standby catchup) lets the **primary** drain those oxids before the standby ever freezes a band
above them. A reproducing deterministic test (route A *or* B) must instead engineer: the oxid stays
in-flight across a checkpoint, the **standby** advances/freezes its horizon *past* the oxid (so on
the standby the deferred rollback's `WAL_REC_XID` re-admits it below `globalXmin`), and the
master's `recovery_xmin` does **not** drain it first. The chaos hunt achieves this via readers +
rapid kills; reproducing it deterministically is the open test problem.

---

## 6. Why the committed fix series (`f071202a`) does NOT close it

The series (`recovery_xmin` floor seed + `update_run_xmin` drainer + monotonicity `Assert`) attacks the **consumer** with predicate `checkpoint_xid && !wal_xid && oxid < recovery_xmin`, which fails on the reproducing oxids:

- **`!wal_xid` evaded:** the toxic oxid arrives as / produced a `WAL_REC_XID` (`DRAIN-BLOCKED oxid=77 ... checkpoint_xid=0 wal_xid=1`), so the drain skips it.
- **`oxid < recovery_xmin` at the emit site is CASE-DEPENDENT (unreliable):**
  - Mode-2 catches: `emit_recovery_rollback oxid=400 recovery_xmin=350 (>= → would NOT skip)`, `oxid=369 recovery_xmin=366 (>= → would NOT skip)` — `recovery_xmin` sat at the **floor**, so the guard skips nothing.
  - Mode-1 catch: `emit_recovery_rollback oxid=371 recovery_xmin=380 (< → WOULD skip)` — here `recovery_xmin` had advanced *past* the culprit, so the guard would suppress it.
  So `skip oxid < recovery_xmin` would have fixed **371** but **not 400/369**. Its effect depends on whether `recovery_xmin` (Max-bumped by durable finish records replayed during recovery) happened to advance past the *specific* culprit — which varies with how much WAL was fsync'd before the kill. (This also corrects earlier flip-flops: it is neither "always works" nor "never works" — it is non-deterministic.)
- The monotonicity `Assert` only converts the Mode-1 livelock into a hard crash.

So the fix must either (a) not emit a deferred rollback for any oxid lacking a durable in-flight proof, or (b) be enforced on the **standby** where the frozen-watermark knowledge lives — keyed off something other than the primary's emit-time `recovery_xmin`.

---

## 7. Open questions

- ~~Mode 2: why is a **material** oxid's deferred rollback emitted with `location=0` (empty)?~~ **FULLY ANSWERED (§4.2, traced over 4 catches):** the deferred record carries no undo location; on the standby the **leader** (which only dispatched the oxid's modifies, never accumulated its undo) holds `undo_stacks[oxid]=0` from the `!found` `memset`. `recovery_finish_current_oxid` RETAINs the entry on `finished_list` and detaches (`cur_recovery_xid_state=NULL`, recovery.c:2220), so the `0` is never overwritten; the deferred rollback re-finds it (`found`) and `set_cur_undo_locations` propagates `0` unmasked (the `!found` reset's SENTINEL masking is lost) → walk `location=0` → PANIC.
- ~~How does a *durable*, fsync'd rollback (oxid 372) still get re-emitted as a deferred rollback?~~ **ANSWERED (§4.1):** the checkpoint abort-snapshot race — `wait_finish_active_commits` drains only commits, so a mid-abort oxid is captured in-flight from `vxids` while its rollback sits below `replayStartPtr` and is skipped on replay.
- Are Mode-1 culprits ever genuinely route-A (fast-path/empty), or also route-B? (The `xidsdump` build lacked the `wal_rollback FAST-PATH` trace, so 131's route is unconfirmed; a fresh assert-variant catch with the full trace set would settle it.)
- What is the correct, durable, cross-instance-safe signal for "this in-flight oxid was already resolved-and-passed on the standby"?

---

## 8. Related files

- `livelock_investigation/behavior_model_current.md` — tagged PROVEN/INFERRED/OPEN model.
- `livelock_investigation/why_f071202a_fix_series_fails.md` — the four-commit critique.
- `livelock_investigation/trigger_oxid_captured_trial4.md`, `recovery_livelock_deferred_rollback_root_cause.md`.
- `ISSUE_streaming_standby_recovery_livelock.md` — earlier (INPROGRESS-spin) framing of Mode 1, superseded by this file's deferred-rollback model.
- Evidence (Mode 1): `results/20260615_172739_xidsdump_*` (oxid 131).
- Evidence (Mode 2, §4.1 primary-side, live-undoLoc): `results/saved_catches/20260616_mode2hunt_mode2_oxid432_xmin379_nonfloor/` incl. `INVESTIGATION.md`.
- Evidence (Mode 2, §4.2 replica-side, full `switch_to_oxid`/`check_delete`/`switch_away` traces): 4 catches with identical signature — oxid 382 (`awayhunt`), 296 + 400 (`retainhunt2_*`), 229 (`20260617_095615_multirepl5_*`). All show: real rollback `found=0 sharedLoc0=SENTINEL` (safe) → `check_delete -> RETAIN (in_finished_list=1)` → deferred rollback `found=1 undoStack0=0 sharedLoc0=0` → `walk_undo_stack location=0` PANIC; zero `switch_away prev_oxid=<culprit>`.

### Diagnostic traces + harness used (this investigation)

- `recovery.c` (all `USE_INJECTION_POINTS`): `switch_to_oxid` (post-branch: `found`/`checkpoint_xid`/`wal_xid`/`undoStack0`/`sharedLoc0`), `check_delete` (DELETE-vs-RETAIN + the three gating flags), `switch_away` (prev oxid's `undo_stacks[0]` on a switch). `oxid.c` carries a `before_abort_vxids_clear` stop event (§4.1 primary race) and `recovery.c::replay_on_record` carries a `replay_on_record` stop event (§4.2 replica RETAIN-forcing) — both used by the deterministic test, runtime-gated by `orioledb.enable_stopevents`.
- **Deterministic test (now reproduces the crash):** `test/t/replication_test.py::test_checkpoint_abort_snapshot_resurrects_inflight_oxid` forces the §4.1 race via `before_abort_vxids_clear`, then forces the §4.2 RETAIN window via `replay_on_record`: it parks the standby's recovery leader so a large committed bulk workload piles up as a backlog (walreceiver keeps writing while the leader is frozen), then releases the leader. The leader bursts through the backlog far ahead of the workers (small `recovery_queue_size` so workers can't read ahead), so `get_workers_commit_ptr()` stays far below the culprit's rollback LSN → the drain can't remove it → the deferred rollback re-finds it (`found=1`, `undoStack0=0`, `sharedLoc0=0`, `worker_id=-1`) → `walk_undo_stack` reads `location=0` → the standby's startup process aborts (`undo.c:391` undo-item-type `Assert` in the IS_DEV build; the production `undo_item_buf_read_item ... location = 0` PANIC otherwise) and the postmaster shuts down. **4/4 runs reproduce.** (The *minimal* workload still lands on the safe `!found` branch — the quiescent gap drains the culprit first, §4.2 — which is exactly the window the backlog-burst defeats.)
- Multi-replica hunt: `RR_REPLICAS=N` (new) spawns N independent streaming standbys per primary (port pool widened in `base_test.py::TestPortManager`; extras tracked + torn down via `getReplicas`/`tearDown`). Each standby replays the deferred rollback independently → ~N× per-trial Mode-2 reproduction. `multirepl5` (5 replicas) caught at trial 2.
