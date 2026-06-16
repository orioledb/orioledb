# Crash-recovery investigation: how OrioleDB classifies oxids (INPROGRESS / COMMITTED / ABORTED)

Scope: the checkpoint + WAL-replay logic by which OrioleDB crash recovery reconstructs
each transaction's verdict, and why a WAL record `COMMIT (635 - xmin 618)` (implying
"the in-flight floor is 618, so 557 < 618 is settled") can coexist with recovery
classifying oxid **557 as in-progress**. Built from four parallel sub-investigations
(§1 checkpoint dump, §2 WAL-replay state machine, §3 watermark/`finish.xmin` semantics,
§4 the trial-6 empirical walkthrough).

---

## Summary

**Recovery derives an oxid's fate from TWO independent durable inputs that can — and at
the crash instant did — disagree:**

- **(A) The horizon watermark.** Every COMMIT/ROLLBACK WAL record carries
  `finish.xmin = the producer's runXmin at build time` (§3). On replay it advances
  `recovery_xmin = Max(recovery_xmin, finish.xmin)` and thence `runXmin`/`globalXmin`.
  `finish.xmin = 618` asserts only *"the producer's **in-memory** oldest-in-flight oxid
  was 618."* It says nothing durable about any specific oxid < 618.

- **(B) The per-oxid recovery state** (`recovery_xid_state_hash`, §2), reconstructed from
  the **checkpoint xids dump** (`read_xids`) plus **replayed WAL finish records**. An oxid
  is INPROGRESS until a durable `WAL_REC_COMMIT`/`WAL_REC_ROLLBACK` is replayed for it;
  any survivor still INPROGRESS at `recovery_finish` is force-aborted.

**Why the two disagree for 557 (§4):**

1. `runXmin` (hence `finish.xmin`) is advanced by `advance_run_xmin`, which is called by
   **both** `current_oxid_commit` *and* `current_oxid_abort` (§3.3). So the moment 557
   aborted **in memory** on the primary (`set_oxid_csn(ABORTED)` at 05:38:49.806),
   `runXmin` walked past it to 618 — and streamed commits stamped `finish.xmin=618`
   onto the wire **before** the crash. Input (A) now says "557 settled."
2. But 557's abort took the `wal_rollback` `!has_material_changes` **no-op** — it wrote
   **no durable `WAL_REC_ROLLBACK`** (§3.3, §4 step 2). And the checkpoint that was
   running when 557 aborted **started at 05:38:49.483, *before* the abort** (§4 step 4),
   so its xids dump captured 557's `vxids[]` slot as an **active in-flight oxid** — the
   dump has **no field to record "aborted"** (§1.1/§1.4). Input (B) now says "557 INPROGRESS."
3. After the SIGKILL, recovery rebuilds (B) from durable artifacts only — it never sees
   the primary's in-memory `ABORTED` (§2.5). It finds 557 in the xids dump, finds **no**
   replayed finish record for it (proven: every `XID (557 …)` in the log is the *bare
   deferred* `(557 0 0)`, never an original-life record — §4 step 2/5), and therefore
   keeps 557 INPROGRESS and `recovery_finish` re-aborts it, emitting a **deferred
   `WAL_REC_ROLLBACK(557, xmin 557)`**.

**The damage.** Recovery trusts (B) over (A): the deferred `ROLLBACK(557)` re-admits 557
into the standby's `xmin_queue`; `update_run_xmin` writes `globalXmin = Min(queue_min=557,
recovery_xmin=648) = 557`, **below `writtenXmin = 648`**. The frozen band `[557, 648)` —
containing committed-and-streamed oxids 568/620/622 — is re-exposed; `oxid_get_csn` reads
their recycled slots as INPROGRESS (csn=0); the recovery conflict-resolver retries forever
(§4 step 6).

**So the contradiction is not a paradox — it is the bug.** `finish.xmin=618` is a watermark
summarising the producer's *in-memory* resolution; "557 in-progress" is recovery's verdict
from *durable artifacts*. 557 aborted in RAM (advancing the watermark) but left no durable
finish record (so the per-oxid state stays INPROGRESS), and the straddling checkpoint
persisted it as in-flight. The watermark crossing an oxid is **not evidence the oxid was
durably finalized** (§3.4) — it only means the producer had stopped treating it as
in-flight. Crash recovery, which can see only durable artifacts, is right to distrust the
watermark and re-resolve 557 — but on a streaming standby that re-resolution arrives as a
late rollback below an already-advanced horizon, which is the regression.

**Fix implication.** Any fix that only constrains *where the horizon value comes from*
(`81e8edcc` seed-from-checkpoint, `618396d2` pin recovery_xmin) cannot help, because the
lowering enters through `queue_min` from the deferred `ROLLBACK(557)`, not through
`recovery_xmin`. The two robust options act on the *seam itself*: (1) **producer-side** —
make `wal_rollback` always emit a durable rollback marker (remove the no-op), so input (B)
can never lack a finish record (verified 0/100 in a hunt; costs WAL); or (2)
**consumer-side floor** — `globalXmin = Max(Min(queue_min, recovery_xmin), writtenXmin)` in
`update_run_xmin`, so input (B) can never drag `globalXmin` below the frozen watermark
regardless of what re-admits an oxid (no added WAL).

---

## 1. Checkpoint dump: what is (and isn't) persisted per oxid

### 1.1 The per-oxid `XidFileRec` — presence + undo locations, no status

Each in-flight transaction is dumped to the `<chkpnum>.xid` file as one `XidFileRec` per
checkpointable undo log. The struct has exactly four fields and **no CSN / commit-or-abort
flag** (`include/checkpoint/checkpoint.h:160-166`):

```c
typedef struct
{
    OXid        oxid;
    XidRecKind  kind;              /* which undo log / rewind / SkFixup — NOT a status */
    UndoStackLocations undoLocation;
    UndoLocation retainLocation;
} XidFileRec;
```

`kind` (`XidRecKind`) only distinguishes the *undo-log category* (`XidRecUndoRegular`,
`XidRecUndoSystem`, rewind variants, `XidRecPendingSkFixup`) — it is **not** a
committed/aborted tag. No field anywhere encodes "this oxid committed" vs "aborted".

`finish_write_xids` (`src/checkpoint/checkpoint.c:909-955`) samples the in-flight set by
walking every proc slot's `vxids[]` and dumping a record **only for slots whose
`vxids[j].oxid` is valid at sample time** (`checkpoint.c:932-947`):

```c
xidRec.oxid = oProcData[i].vxids[j].oxid;
if (OXidIsValid(xidRec.oxid)) {        /* only present oxids are dumped */
    read_shared_undo_locations(&xidRec.undoLocation, ...);
    xidRec.retainLocation = ...transactionUndoRetainLocation;
    if (xidRec.oxid != oProcData[i].vxids[j].oxid) continue;  /* torn-read guard only */
    write_to_xids_queue(&xidRec);
}
```

### 1.2 Control-file watermarks

`src/checkpoint/checkpoint.c:1531-1548`:
```c
control.lastCSN              = nextCommitSeqNo;          // :1531
control.lastXid              = xid_meta->nextXid;        // :1532
control.checkpointRetainXmin = checkpoint_xmin;          // :1547  = runXmin @ start (:1411)
control.checkpointRetainXmax = checkpoint_xmax;          // :1548  = nextXid @ end   (:1479)
```
These are range/counter watermarks, **not per-oxid status**.

### 1.3 The xids snapshot is NOT a single instant

- **Open:** `start_write_xids` (`checkpoint.c:1433`) sets `flushUndoLocations=true` for every
  proc; from then on backends self-insert an `XidFileRec` when they commit/abort
  (`src/transam/undo.c:1391-1400`).
- **Close:** `finish_write_xids` (`checkpoint.c:1468`) runs much later (after sys-trees,
  table callbacks, SK-fixup pass) and only then sweeps `vxids[]`.
- `checkpoint_xmin = runXmin` is sampled at `:1411` — *before* both. The per-record re-check
  at `:944` only guards a torn single-slot read; it does **not** make the snapshot atomic
  with respect to concurrent commits/aborts.

### 1.4 Key consequence

The dump records an oxid as **"present / in-flight"** but has **no way to record "aborted."**
An oxid that aborts mid-checkpoint — after its `vxid` slot was sampled, before checkpoint
completion — is persisted **indistinguishably from one still running**. The checkpoint
contributes *presence + undo-chain locations*; **recovery alone decides the verdict.**

---

## 2. WAL replay: the oxid classification state machine

Recovery rebuilds every transaction's verdict into `recovery_xid_state_hash`
(`RecoveryXidState` keyed by `OXid`, `recovery.c:222`, created at `recovery.c:1503`). Each
entry's `csn` walks **INPROGRESS → {COMMITTED | ABORTED}**, with a `wal_xid` flag marking
"seen as a live WAL transaction" vs "merely a checkpoint carry-over."

### 2.1 The two INPROGRESS entry points

**(a) Checkpoint xids dump — `read_xids` (`recovery.c:837`, called from `recovery_init`
`:1548`).** For each dumped record: `HASH_ENTER`, then
```c
state->csn = COMMITSEQNO_INPROGRESS;   // :884
state->checkpoint_xid = true;          // :901
state->wal_xid = false;                // :902  -- not (yet) seen in live WAL
pairingheap_add(xmin_queue, &state->xmin_ph_node);   // :896
```

**(b) Replayed `WAL_REC_XID` — `recovery_switch_to_oxid` (`recovery.c:1854`), from the
dispatch `case WAL_REC_XID` (`recovery.c:3847`).** `HASH_ENTER`, set `wal_xid = true`
(`:1887`); if new: `csn = COMMITSEQNO_INPROGRESS` (`:1906`) + `pairingheap_add` (`:1925`).
If the oxid already existed (a `read_xids` carry-over), its state is restored and `wal_xid`
flips to `true` — the WAL now owns it.

### 2.2 Transition to COMMITTED / ABORTED

`WAL_REC_COMMIT` and `WAL_REC_ROLLBACK` share one dispatch arm (`recovery.c:3856-3910`):
```c
recovery_xmin = Max(recovery_xmin, rec->u.finish.xmin);            // :3865
recovery_finish_current_oxid(commit ? COMMITSEQNO_MAX_NORMAL-1
                                    : COMMITSEQNO_ABORTED, ...);    // :3898
```
`recovery_finish_current_oxid` (`recovery.c:2010`) stamps the terminal CSN: COMMIT assigns a
real CSN via `nextCommitSeqNo` and applies undo as committed; ROLLBACK calls
`apply_undo_stack` then `set_oxid_csn(oxid, COMMITSEQNO_ABORTED)`. `check_delete_xid_state`
(`:2108`) then removes the entry from `xmin_queue` and the hash. `WAL_REC_JOINT_COMMIT`
(`:3912`) is the commit variant carrying the PG xid; it likewise bumps `recovery_xmin`.

### 2.3 Terminal step — `recovery_finish` force-aborts survivors

`recovery_finish` (`recovery.c:1656`) sweeps the hash; any entry **still INPROGRESS** never
got a finish record and is force-aborted in memory (`:1686-1712`), emitting **no inline WAL**.
On the main process the oxid is buffered (`recovery_finish_aborted_oxids`, `:1738`, #876) and
`o_emit_recovery_finish_rollbacks` (`:1819`) → `wal_emit_recovery_finish_rollback`
(`wal.c:486`) later writes the **deferred `WAL_REC_ROLLBACK`**. Implicit rule:
**no finish record ⇒ ABORTED.**

### 2.4 The lookup — `recovery_map_oxid_csn` (`recovery.c:1474`)

```c
if (*found) {
    if (!state->wal_xid) return COMMITSEQNO_ABORTED;  // dump carry-over never re-seen in WAL
    return state->csn;                                 // INPROGRESS / real CSN / ABORTED
}
return 0;                                              // not found
```

### 2.5 The crucial asymmetry — durable artifacts only

Recovery's verdict comes **only** from (1) the checkpoint xids dump and (2) replayed WAL
finish records. It **never sees the primary's pre-crash in-memory CSN**. So an oxid in the
dump with no replayed finish record stays INPROGRESS for the whole replay regardless of what
it reached in RAM on the primary. On the primary's own recovery this is benign —
`recovery_finish` force-aborts it. But a **live streaming standby never calls
`recovery_finish`** (`recovery.c:1690-1697`), so such an oxid stays INPROGRESS indefinitely —
the root asymmetry of the whole livelock.

---

## 3. Watermarks and finish.xmin: a horizon, not a per-oxid guarantee

### 3.1 The watermarks and invariant

`writtenXmin <= globalXmin <= runXmin <= nextXid`, plus recovery-local `recovery_xmin`
(`recovery.c:650`, the redo-side mirror of a producer's `runXmin`).
- `runXmin` — oldest still-in-flight oxid; everything below is resolved **in memory**.
- `globalXmin` — `advance_global_xmin` seeds it `= runXmin`, lowers by per-proc `xmin`,
  raises the stored value only (`oxid.c:1128-1162`).
- `writtenXmin` — below it, CSNs are FROZEN-stamped out of `xidBuffer` into on-disk
  `o_buffers` (`oxid.c:1183-1193`). Powers the `oxid_get_csn` fast path
  `oxid < globalXmin → COMMITSEQNO_FROZEN`.

### 3.2 What `finish.xmin` is

`src/recovery/wal.c:354 / 452`:
```c
add_finish_wal_record(WAL_REC_COMMIT,   pg_atomic_read_u64(&xid_meta->runXmin));
add_finish_wal_record(WAL_REC_ROLLBACK, pg_atomic_read_u64(&xid_meta->runXmin));
```
`finish.xmin` = the producer's `runXmin` at build time = **the oldest oxid still in-flight on
the producer at that instant** — a horizon, not a fact about the record's own oxid. Replayed:
`recovery_xmin = Max(recovery_xmin, finish.xmin)` (`recovery.c:3865`). It asserts only:
*"the oldest unresolved oxid is ≥ finish.xmin"* ⟺ *"every oxid < finish.xmin has left the
in-flight set."*

### 3.3 Leaving the in-flight set ≠ having a durable finish record

`runXmin` is advanced by `advance_run_xmin` (`oxid.c:1472-1492`), a CAS loop walking it up
past resolved oxids. **Both** lifecycle exits call it:
- `current_oxid_commit → advance_run_xmin(curOxid)` (`oxid.c:1545`)
- `current_oxid_abort  → advance_run_xmin(curOxid)` (`oxid.c:1599`)

So `runXmin` advances past an oxid the moment its CSN is set **in memory** (a real CSN *or*
`COMMITSEQNO_ABORTED`) — independent of any durable WAL. The decoupling is explicit on the
abort path: `wal_rollback` (`wal.c:425-431`) short-circuits when no logged changes:
```c
if (!local_wal.has_material_changes) { local_wal.buffer_offset = 0; ...; return; }  // NO WAL_REC_ROLLBACK
```
`current_oxid_abort` still runs and still calls `advance_run_xmin` — **`runXmin` moves past an
oxid for which zero durable finish bytes exist.** Therefore `finish.xmin = 618` means *"the
producer's in-memory oldest-in-flight was 618"* — **not** that every oxid < 618 has a durable
COMMIT/ROLLBACK record.

### 3.4 Consequence: horizon and per-oxid state are independent inputs

Redo has two sources of truth: the **horizon** (`recovery_xmin` from `finish.xmin`, → `runXmin`
/`globalXmin`) saying *"everything below is settled,"* and the **per-oxid state** (§2) from
actual durable artifacts. Because the horizon is derived from the producer's *in-memory*
`runXmin`, the two **can disagree** — the horizon can declare 557 settled (`557 < 618`) while
no durable finish record for 557 ever existed. **The watermark crossing an oxid is not evidence
the oxid was durably finalized.**

---

## 4. Empirical walkthrough: trial-6 (oxid 557)

Logs: `results/CAUGHT_livelock_{primary,replica}.log`. Crash boundary: primary "not properly
shut down" 05:38:51.284, redo done at `0/3020E40` (05:38:51.379).

**Step 1 — 557 fully aborted in memory ~1.5 s before SIGKILL, never committed.** Primary
(pid 1470152), all at 05:38:49.806: `undo_xact_callback enter event=2 oxid=557` → `abort enter`
→ `wal_rollback begin/end` → `current_oxid_abort … set_oxid_csn(ABORTED) begin/end`. Zero
precommit/commit traces for 557.

**Step 2 — no durable `WAL_REC_ROLLBACK(557)` during its life.** Every `XID (557 …)` in the
primary log is the bare deferred `XID (557 0 0); ROLLBACK (557 0 0 - xmin 557 csn 661)`
(27×, redo at `0/3021250` > redo-done `0/3020E40`). **No** original-life `XID (557 <nonzero>)`
exists — 557's modifies lived only in `local_wal`, never on the durable wire.

**Step 3 — the horizon climbed past 557 before the crash, via a streamed commit.** Replica,
redo at `0/301D3D8` (05:38:49.848): `XID (635 544 0); COMMIT (635 544 0 - xmin 618 csn 541)`.
`0/301D3D8 < 0/3020E40`, so `finish.xmin=618` was streamed/replayed **pre-crash**.

**Step 4 — the straddling checkpoint + recovery seed.** Primary: `checkpoint starting`
05:38:49.483 (**before** 557's abort at .806) → `checkpoint complete` 05:38:50.299 (**after**).
Crash-recovery seed 05:38:51.062: `SEED … checkpointRetainXmin=438 … nextXid=784`. `438 ≤ 557`,
so 557 is inside the retained band the dump replays as in-flight.

**Step 5 — recovery sourced "557 in-flight" from the xids dump, not WAL.** No replayed
original `XID(557)` (Step 2), yet all workers abort it:
`recovery-finish-abort-trace … oxid=557` (05:38:51.354–.375). The only possible source is
`read_xids` (the straddling checkpoint's dump), as INPROGRESS.

**Step 6 — the deferred `ROLLBACK(557)` drags globalXmin down → spin on committed oxids.**
Replica redo of `XID (557 0 0); ROLLBACK (557 0 0 - xmin 557 csn 661)` at `0/3021250`:
```
GXMIN-TRACE update_run_xmin LOWER globalXmin 648 -> 557 (recovery_xmin=648 writtenXmin=648 queue_empty=0) *** BELOW-writtenXmin INVARIANT-VIOLATION ***
```
Top-spun oxids 622 / 620 / 568 (~1.52M each) all sit in `[557, 648)` and read `csn=0`
(INPROGRESS) — yet were streamed-and-committed pre-crash (e.g. `COMMIT (568 320 0 - xmin 391
csn 425)` at `0/3016360`). Committed transactions whose CSN can no longer be resolved once the
horizon is dragged back to 557 ⇒ `handle_conflicts` retries forever.

**Conclusion.** `finish.xmin=618` = the streamed *commit horizon* (in-memory floor), under
which 557 was settled. "557 in-progress" = the *checkpoint xids dump*, which started a hair
before 557's in-memory abort completed and so snapshotted it INPROGRESS — and 557's abort left
no durable `WAL_REC_ROLLBACK` to retract it. Recovery re-aborts the long-dead 557 via a
deferred rollback whose `xmin 557` pulls the standby's `globalXmin` below `writtenXmin`,
stranding committed oxids 568/620/622 and producing the livelock.
