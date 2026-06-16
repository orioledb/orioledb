# Streaming-standby recovery bug — current behavior model

Status anchor: `xidsdump` trial 1 (2026-06-15), oxid **131**, build with the
`read_xids`/`TRIGGER`/`DRAIN` traces. Evidence:
`results/20260615_172739_xidsdump_NONE_invariant{,_replica}.log`. Each claim is
tagged **[PROVEN]** (direct log evidence), **[INFERRED]** (logic from proven
facts), or **[OPEN]** (not yet measured — the `xmindump` hunt is running to
settle these).

This supersedes earlier drafts where I (a) called oxid 316 a "modify-then-abort
slow-path" case and (b) claimed the primary's `recovery_xmin` was "stuck at 87."
Both were wrong; see §8 Corrections.

---

## 1. Conceptual foundations

- **Oxid ≠ material change.** A transaction acquires an oxid lazily, but acquiring
  one does **not** require modifying data. `has_material_changes` (wal.c:301) is
  set only when an insert/update/delete record is buffered — not by oxid
  assignment nor by the `WAL_REC_XID` record. `orioledb_get_current_oxid()` is the
  SQL handle that takes an oxid with zero material changes. **[PROVEN — code]**
- **Fast-path `wal_rollback`** (wal.c:421): if `!has_material_changes`, it discards
  the private buffer and writes **nothing** to WAL. So an abort can leave **no
  durable rollback record**. **[PROVEN — code]**
- **In-memory abort advances the horizon.** Both commit and abort call
  `advance_run_xmin`, so an in-memory abort raises `runXmin` past the aborted oxid.
  **[PROVEN — code + behavior]**
- **Checkpoint xids dump.** A checkpoint records the set of in-flight oxids to
  `orioledb_data/<n>.xid`, each with an `undoLocation` that is either a real offset
  (material undo captured) or `InvalidUndoLocation` = `0x2000000000000000` (empty).
  Crash recovery's `read_xids()` reads this set and stamps each entry
  `COMMITSEQNO_INPROGRESS`. **[PROVEN — code + read_xids trace]**
- **Deferred rollback (#876).** At end of crash recovery,
  `o_emit_recovery_finish_rollbacks()` emits a `WAL_REC_XID + WAL_REC_ROLLBACK`
  for each in-flight oxid that recovery couldn't otherwise resolve. These are
  stamped with the **checkpoint-era `runXmin`** (the floor), because `free_run_xmin`
  is deliberately deferred until after emission (recovery.c:1854, #889).
  **[PROVEN — code]**
- **The standby watermark is in-memory and survives a walreceiver reconnect.**
  `globalXmin`/`writtenXmin` are shmem state on the standby; a streaming break +
  reconnect does not reset them. **[PROVEN — behavior]**

## 2. The cast (oxid 131, this trial)

| fact | value | tag |
|---|---|---|
| culprit oxid | 131 | [PROVEN] |
| 131 in primary checkpoint=3 dump | yes, `undoLoc=Invalid` (empty), count=231 | [PROVEN] |
| 131 on primary: did one `action=2` modify @17:26:38.471, then ABORTED in memory @17:26:39.920 | [PROVEN] |
| 131 abort was fast-path (empty undo, `iters=0`, no backend rollback record) | [PROVEN] |
| SIGKILL @17:26:40.456 (≈0.5 s after 131's abort) | [PROVEN] |
| primary produced **durable** WAL with `runXmin=155` pre-kill (2652 `xmin=155` finish recs @LSN 0/3005xxx, before truncation 0/3007A40) | [PROVEN] |
| replica raised `globalXmin 87→155` (srcXid=190) @17:26:39.983, froze band `[87,155)` | [PROVEN] |
| replica re-streamed from 0/3000000 after the break; `writtenXmin=155` persisted | [PROVEN] |
| deferred rollback `XID (131 0 0); ROLLBACK (131 0 0 - xmin 87 csn 226)` @LSN 0/3007CB8 | [PROVEN] |
| replica TRIGGER: `xmin=131 < globalXmin=155 (queue_min_oxid=131, recovery_xmin=155, nextXid=242, writtenXmin=155)` → TRAP recovery.c:2666 | [PROVEN] |
| `DRAIN` lines on primary AND replica | **zero** | [PROVEN] |

## 3. End-to-end sequence

1. **Primary, normal op:** oxid 131 acquires an oxid, does one modify with an empty
   undo footprint. A checkpoint (cp=3) captures 131 (and 230 others) as in-flight
   with `undoLoc=Invalid`. **[PROVEN]**
2. **Primary:** 131 aborts in memory (fast-path, no durable rollback). This + other
   commits advance `runXmin` to 155; those `xmin=155` finish records are **flushed**
   (durable) and streamed. **[PROVEN]**
3. **Standby (pass 1):** replays the durable stream, raises `globalXmin/writtenXmin`
   to 155, frozen-stamping `[87,155)` — which covers 131. **[PROVEN]**
4. **SIGKILL** the primary (no checkpoint after step 2; cp=3 still lists 131
   in-flight with empty undo). **[PROVEN]**
5. **Primary crash recovery:** `read_xids` loads the 231 in-flight oxids; replays
   durable WAL → `recovery_xmin = Max(.., 155) = 155` **[INFERRED from §5]**. 131 has
   no durable resolution, so recovery aborts it and `o_emit_recovery_finish_rollbacks`
   emits the empty `WAL_REC_ROLLBACK(131)` stamped with the floor `xmin=87`. **[PROVEN]**
6. **Standby (pass 2):** walreceiver reconnects, re-streams 0/3000000 forward;
   `writtenXmin=155` persists in shmem. It replays the deferred `ROLLBACK(131)` at
   0/3007CB8, re-admitting 131 into `xmin_queue` **below** the frozen 155. **[PROVEN]**
7. **Crash:** `update_run_xmin` computes `Min(queue_min=131, recovery_xmin=155)=131
   < globalXmin=155` → `Assert(xmin>=globalXmin)` → standby down. **[PROVEN]**

## 4. The defect, in one sentence

A fast-path in-memory abort advances the horizon **without leaving a durable
record**; the checkpoint still lists the oxid as in-flight (empty undo); crash
recovery therefore resurrects it and emits a deferred rollback at a **higher LSN
than where the standby's (reconnect-surviving, in-memory) watermark already passed
and froze it** → the standby re-admits a frozen oxid below the watermark → assert.

## 5. Why the horizon legitimately reached 155 (correcting the durability-asymmetry error)

A streaming standby only replays WAL the primary **flushed**. The replica reached
`globalXmin=155`, replaying 1758 `xmin=155` finish records (e.g. `COMMIT (188 … -
xmin 155)` @0/3005220). Those LSNs precede the crash truncation (0/3007A40), so they
were durable. The **same** durable records are in the primary log (2652 of them),
so the primary's own crash recovery replays them and `recovery_xmin` reaches ~155.
**[PROVEN]** → My earlier "primary recovery_xmin stuck at 87 / durability asymmetry"
was false. The `xmin=87` on the deferred rollback is the **checkpoint-era floor**,
stamped deliberately (§1), not the live `recovery_xmin`.

## 6. The two unresolved questions (the running `xmindump` hunt will measure these)

**Q1 — Why zero drains, given `recovery_xmin≈155` and `131<155` and 131 is
checkpoint-only?** The drain loop breaks either on `oxid >= recovery_xmin` (now ruled
out for 131, since 155>131) or on `!checkpoint_xid || wal_xid`. So the drain must be
breaking on **`wal_xid=true`** (or `checkpoint_xid=false`), OR `recovery_xmin` had
not yet risen to 155 at the moments `update_run_xmin` actually ran with 131/87 at the
queue head (a **timing** question — recovery_xmin rises mid-replay, and the drain
only runs via `check_delete_xid_state` when *other* oxids finish). **[OPEN]**
Competing hypotheses:
  - (a) `wal_xid=true`: 131's modify produced a durable `WAL_REC_XID` replayed during
    recovery — but this is in tension with 131 showing empty undo / fast-path abort.
  - (b) timing: `recovery_xmin` was still ≤ queue-min (87) at every `update_run_xmin`
    call, so the first break (`oxid >= recovery_xmin`) always fired before 131 could
    be considered.
  The new `DRAIN-BLOCKED` trace fires only when `oxid < recovery_xmin` but the drain
  still breaks, printing `checkpoint_xid`/`wal_xid` — distinguishing (a) from (b).

**Q2 — Would the emission-site fix actually skip 131?** The fix is "skip the deferred
rollback for `oxid < recovery_xmin` in `o_emit_recovery_finish_rollbacks`." If the
**live** `recovery_xmin` at that site is ~155, then `131<155` → skipped → fixed.
**[INFERRED: likely yes]**, pending the new `emit_recovery_rollback` trace that prints
`oxid` vs live `recovery_xmin` at emit. (I earlier doubted this on the wrong
`recovery_xmin=87`; retracted.)

## 7. Why draining is the wrong layer regardless (independent of Q1)

`xmin_queue` is reconstructed by replaying WAL; the deferred-rollback record
re-creates the entry on every replay pass (and survives walreceiver reconnect via the
in-memory watermark). Even if the drain caught 131 on pass 1, pass 2 re-admits it.
And once 131 arrives as a streamed `WAL_REC_XID`, it is indistinguishable from a
genuinely in-flight rollback. The decision belongs at the **emission site** (don't
write the record) — and the only horizon that proves 131 is safe to drop lives on the
**standby** (it froze `[87,155)`), which the primary can't see. That cross-instance
reference-frame gap is the root: a per-instance in-memory watermark over-promises
when serialized across the boundary. **[INFERRED — consistent with all evidence]**

## 8. Corrections log (claims I retracted under review)

1. "oxid 316 was a modify-then-abort slow-path case." — Wrong framing; the checkpoint
   captures these oxids **empty** regardless of later modifies, and 131 is cleanly
   fast-path/empty. The *deferred rollback* is empty because it's built from the
   checkpoint snapshot.
2. "Primary `recovery_xmin` stuck at 87 → durability asymmetry." — Wrong; the 87 is
   the floor stamped on the rollback by design, and the primary demonstrably has the
   durable `xmin=155` records (§5). recovery_xmin reaches ~155.
3. "The emission-site fix wouldn't catch 131 (131 > recovery_xmin=87)." — Retracted;
   built on #2. With recovery_xmin≈155, it likely does catch it (Q2).
