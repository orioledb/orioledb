# Issue: streaming standby crashes on the deferred WAL_REC_ROLLBACK of a resurrected in-flight oxid (two crash modes)

**Status:** root-caused; reproduced many times this session with full tracing; fix direction NOT settled (the committed fix series `f071202a` does not close it — see §6)
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

- Mode 2: why is a **material** oxid's deferred rollback emitted with `location=0` (empty), causing the undo-read PANIC, when its checkpoint entry has real undo?
- Are Mode-1 culprits ever genuinely route-A (fast-path/empty), or also route-B? (The `xidsdump` build lacked the `wal_rollback FAST-PATH` trace, so 131's route is unconfirmed; a fresh assert-variant catch with the full trace set would settle it.)
- What is the correct, durable, cross-instance-safe signal for "this in-flight oxid was already resolved-and-passed on the standby"?

---

## 8. Related files

- `livelock_investigation/behavior_model_current.md` — tagged PROVEN/INFERRED/OPEN model.
- `livelock_investigation/why_f071202a_fix_series_fails.md` — the four-commit critique.
- `livelock_investigation/trigger_oxid_captured_trial4.md`, `recovery_livelock_deferred_rollback_root_cause.md`.
- `ISSUE_streaming_standby_recovery_livelock.md` — earlier (INPROGRESS-spin) framing of Mode 1, superseded by this file's deferred-rollback model.
- Evidence: `results/20260615_172739_xidsdump_*` (Mode 1, oxid 131), `results/20260615_182611_xmintrace_*` incl. saved datadirs (Mode 2, oxid 400).
