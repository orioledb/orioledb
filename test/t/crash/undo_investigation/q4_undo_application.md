# Q4: Undo application on transaction COMMIT vs ABORT

## Summary

OrioleDB has no heap; every row modification pushes a per-transaction **undo
record** onto an in-memory undo stack (one stack per `UndoLogType`, count =
`UndoLogsCount`). The single `XactCallback` `undo_xact_callback`
(`src/transam/undo.c:2110`, registered at `src/orioledb.c:1255`) drives the
end-of-transaction handling for the three events `XACT_EVENT_PRE_COMMIT`,
`XACT_EVENT_COMMIT`, `XACT_EVENT_ABORT`.

The crucial asymmetry: **the undo records that reverse row changes are applied
ONLY on ABORT.** On COMMIT they are *not* replayed at all — the row versions
stay in place; the commit path only flips the CSN (so the new versions become
visible) and runs the small subset of records that explicitly opted into a
commit-time callback (`callOnCommit`, e.g. dropping obsolete relation files).
The records are then retained for as long as some snapshot might still need the
*old* version, and discarded later by undo-location advancement, never
"applied".

The same `apply_undo_stack()` machinery is reused during crash recovery to roll
back transactions that were aborted (or left in-flight) in the WAL.

---

## The callback and its three events

`undo_xact_callback(XactEvent event, void *arg)` — `src/transam/undo.c:2110`.

Common preamble:
- `oxid = get_current_oxid_if_any()` (`undo.c:2112`).
- If no valid oxid or parallel worker, it only resets per-command undo
  bookkeeping (`undo.c:2148-2170`).
- Otherwise asserts `!RecoveryInProgress()` and dispatches on `event`
  (`undo.c:2219-2220`).

### XACT_EVENT_PRE_COMMIT — `undo.c:2222`
Only meaningful when an Oriole txn is acting as a sub-transaction of a heap txn
(SWITCH_LOGICAL_XID). It runs the **pre-commit** stage of the undo stack —
durability work that must precede the commit WAL record:
```
for (i = 0; i < UndoLogsCount; i++)
    precommit_undo_stack((UndoLogType) i, oxid, true);   // undo.c:2235-2236
```
`precommit_undo_stack()` (`undo.c:1419`) walks only the `onCommitLocation`
chain with stage `OUndoCallbackStagePreCommit`. Then, if there's a heap xid, it
issues `current_oxid_xlog_precommit()` and possibly `wal_joint_commit()`
(`undo.c:2238-2251`).

### XACT_EVENT_COMMIT — `undo.c:2255` (see COMMIT section below)

### XACT_EVENT_ABORT — `undo.c:2415` (see ABORT section below)

After the switch, for both COMMIT and ABORT it releases reserved undo size and
page-pool buffers (`undo.c:2521-2524`).

---

## COMMIT path (`XACT_EVENT_COMMIT`, undo.c:2255-2413)

This whole case runs under **`HOLD_INTERRUPTS()` (set by PostgreSQL's commit
machinery), NOT inside a full `START_CRIT_SECTION`** — see the comment at
`undo.c:2334` ("XACT_EVENT_COMMIT runs under HOLD_INTERRUPTS, so a normal
CHECK_FOR_INTERRUPTS() is suppressed here"). That is why a `palloc`-ing
`elog(LOG)` is legal here but would TRAP inside the injection-point crit bracket
at `undo.c:2320-2322`. The only crit section in this path is the 3-line
`START_CRIT_SECTION(); INJECTION_POINT("orioledb-commit-assert");
END_CRIT_SECTION();` stress-test bracket (`undo.c:2320-2322`).

Step by step:

1. **WAL / flush of the commit record.** For an independent Oriole txn
   (`!heapXid`) it assigns the xidless commit LSN, flushes WAL and waits for
   sync replication (`undo.c:2262-2285`); for a heap txn it records the xlog ptr
   (`undo.c:2286-2295`).

2. **CSN transition — phase 1 (COMMITTING).**
   `current_oxid_precommit()` (`undo.c:2329`, defined `src/transam/oxid.c:1417`)
   sets the oxid's CSN to a special value flagged
   `COMMITSEQNO_STATUS_CSN_COMMITTING` (`oxid.c:1424-1427`). Concurrent readers
   that touch a tuple modified by this oxid now see "committing" and spin until
   the final CSN lands.

3. **Acquire the global CSN.**
   `csn = GetCurrentCSN()`; if still `COMMITSEQNO_INPROGRESS`, atomically
   `pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1)`
   (`undo.c:2350-2352`). The window between the global increment and the
   per-oxid flip is the `orioledb-csn-incremented` injection point
   (`undo.c:2368`).

4. **CSN transition — phase 2 (final).**
   `current_oxid_commit(csn)` (`undo.c:2376`, defined `oxid.c:1507`) writes the
   real CSN for the oxid. Now the txn's new row versions are visible to anyone
   with a CSN >= this one.

5. **Commit-time undo callbacks (the only "undo" work done at commit).**
   ```
   INJECTION_POINT("oriole-before-on-commit-undo-stack");        // undo.c:2391
   for (i = 0; i < UndoLogsCount; i++)
       on_commit_undo_stack((UndoLogType) i, oxid, true);        // undo.c:2393-2396
   ```
   `on_commit_undo_stack()` (`undo.c:1431`) calls
   `walk_undo_stack(..., abortTrx=false)`. In the `!abortTrx` branch
   (`undo.c:1327-1346`) it walks **only the `onCommitLocation` chain** (the
   subset of records whose descriptor set `callOnCommit`, e.g. relfilenode-drop
   items), invoking each callback with stage `OUndoCallbackStageCommit`. The
   ordinary row-modification undo records (type `modify_undo_callback`) are
   **never visited** — `modify_undo_callback` even asserts `stage ==
   OUndoCallbackStageAbort` (`src/btree/undo.c:474`). So committed row changes
   are left exactly as written.

6. **Cleanup.** `wal_after_commit(); reset_cur_undo_locations();
   reset_command_undo_locations(); ...` (`undo.c:2399-2404`).

**Key point:** on COMMIT the abort-undo records are NOT applied. They remain in
the undo log so that older snapshots can still reach the previous row versions,
and are only physically reclaimed later when no snapshot can see them (retain
logic below).

### Recovery analogue of the commit path
`recovery_finish_current_oxid()` with a non-aborted csn mirrors this:
`set_oxid_csn(COMMITTING)` → `precommit_undo_stack` → `on_commit_undo_stack` →
new csn from `nextCommitSeqNo` → `set_oxid_csn(csn)` (`recovery.c:1897-1926`).

---

## ABORT path (`XACT_EVENT_ABORT`, undo.c:2415-2514)

1. **WAL rollback record.** If not in recovery,
   `wal_rollback(oxid, logicalXidContext.xid, false)` (`undo.c:2425-2436`).

2. **Release CSN spinners first.**
   `current_oxid_clear_committing()` (`undo.c:2461`, defined `oxid.c:1601`).
   If precommit had already set the `CSN_COMMITTING` bit but we never reached
   `current_oxid_commit()`, every other backend that saw a tuple modified by us
   is busy-spinning in `oxid_get_csn()` waiting for that bit to clear. The
   comment at `undo.c:2438-2460` explains the deadlock this prevents: we must
   revert the bit to IN_PROGRESS *before* `apply_undo_stack()` goes after page
   locks those spinners hold. The final flip to `COMMITSEQNO_ABORTED` happens
   later in `current_oxid_abort()`.

3. **Apply the undo stack — the actual rollback.**
   ```
   for (i = 0; i < UndoLogsCount; i++)
       apply_undo_stack((UndoLogType) i, oxid, NULL, true);      // undo.c:2467-2468
   ```
   `apply_undo_stack()` (`undo.c:1412`) → `walk_undo_stack(..., abortTrx=true)`.
   In the abort branch (`undo.c:1347-1366`) it starts from
   `sharedLocations->location` (the *full* stack top, not just the onCommit
   subset) and calls `walk_undo_range_with_buf(..., OUndoCallbackStageAbort,
   ...)`. `walk_undo_range()` (`undo.c:1162`) iterates **every** record, walking
   the `item->prev` chain (`undo.c:1219-1222` — abort follows `prev`, commit
   follows `onCommitLocation`), and invokes `descr->callback(... stage=Abort)`
   for each (`undo.c:1188`).

4. **Per-record row reversal.** For a row change the callback is
   `modify_undo_callback` (`src/btree/undo.c:456`; registered in the descriptor
   table at `undo.c:126`). It:
   - reconstructs the tuple/key from the undo record payload
     (`btree/undo.c:492-493`),
   - re-finds the page via `refind_page()` (`btree/undo.c:516`),
   - verifies the live tuple still belongs to this oxid
     (`XACT_INFO_OXID_EQ`, `btree/undo.c:561`) and that the undo chain matches
     (`btree/undo.c:576-577`),
   - calls `page_item_rollback(desc, p, loc, ...)` (`btree/undo.c:583`) which
     restores the previous tuple version (re-inserts a deleted/updated row,
     removes an inserted row) in place, then may merge/unlock the page.
   Idempotent guards (`cmp != 0` → already undone, `OFindPageResultFailure` →
   subtree already dropped) make re-application during recovery safe.

5. **Final CSN flip + cleanup.** `wal_after_commit()`,
   `reset_cur_undo_locations()`, then `current_oxid_abort()` (`undo.c:2486`,
   `oxid.c:1541`) writes `COMMITSEQNO_ABORTED`. Registered snapshots are drained
   and `snapshotRetainUndoLocation` reset (`undo.c:2502-2512`).

`apply_undo_branches()` (`undo.c:1271`) is a recovery-only helper that re-walks
sub-chains already aborted before a checkpoint, with the same Abort stage.

---

## Retain / release of undo (when records become discardable)

Undo is reclaimed by **advancing the minimum retained location**, not by
applying records. While a txn holds undo, `reserve_undo_size` /
record-add code sets `transactionUndoRetainLocation` per-process
(`undo.c:917-959`), and snapshots set `snapshotRetainUndoLocation`
(`undo.c:994-1022`). `update_min_undo_locations()` (`undo.c:430`) sweeps all
procs, taking the `Min` of every proc's `reservedUndoLocation`,
`transactionUndoRetainLocation` and `snapshotRetainUndoLocation`
(`undo.c:476-498`) to compute `minProcRetainLocation` /
`minProcTransactionRetainLocation` (`undo.c:501-502`), then advances
`cleanedLocation` (`undo.c:540-546`). Records below the new
`minRetainLocation` are no longer reachable by any snapshot and their backing
undo files become reclaimable — that is the moment a committed txn's old row
versions (and an aborted txn's now-dead records) are truly discarded. The macro
`UNDO_REC_EXISTS` (`include/transam/undo.h:356`) encodes the "still retained"
test against `minProcRetainLocation` / checkpoint retain window.

On abort the per-txn retain is dropped explicitly at `undo.c:2509-2510`
(`snapshotRetainUndoLocation = InvalidUndoLocation`); on commit the txn's
records linger until `update_min_undo_locations` sweeps past them.

---

## Recovery-side undo application

Recovery replay (`src/recovery/recovery.c`, `worker.c`) reconstructs each
in-progress oxid's undo stack and finishes it via
`recovery_finish_current_oxid(csn, ptr, worker_id, sync)` (`recovery.c:1886`):

- **Replay of a COMMIT WAL record** → `recovery_finish_current_oxid(COMMITSEQNO_MAX_NORMAL - 1, ...)`
  (`recovery.c:3753`, also `recovery.c:4197`; worker side `worker.c:588`). Non-aborted
  branch runs `precommit_undo_stack` + `on_commit_undo_stack` and assigns a real
  CSN (`recovery.c:1897-1926`) — same "don't replay row undo" semantics as the
  live commit path.

- **Replay of a ROLLBACK WAL record** → `recovery_finish_current_oxid(COMMITSEQNO_ABORTED, ...)`
  (`recovery.c:3753` else-branch / `worker.c:601`). The aborted branch
  (`recovery.c:1927-1934`) calls
  `apply_undo_stack((UndoLogType) i, oxid, NULL, true)` for every undo type —
  the identical row-reversal path used at runtime — then `set_oxid_csn(ABORTED)`.

- **In-flight transactions at recovery end.** `recovery_finish(worker_id)`
  (`recovery.c:1620`) seq-scans `recovery_xid_state_hash`; for every entry still
  `COMMITSEQNO_IS_INPROGRESS` (`recovery.c:1650`) it restores that oxid's undo
  stack locations and calls
  `apply_undo_stack((UndoLogType) i, recovery_oxid, NULL, true)`
  (`recovery.c:1668-1673`), then `walk_checkpoint_stacks(..., COMMITSEQNO_ABORTED, ...)`.
  This is an **in-memory abort with no WAL emitted** — the
  `recovery-finish-abort-trace` instrumentation logging the aborted oxid is at
  `recovery.c:1663` (gated by `USE_INJECTION_POINTS`). The comment
  (`recovery.c:1653-1662`) notes that a live streaming standby never reaches
  `recovery_finish()`, so such an oxid stays INPROGRESS forever and a later
  replayed modify of its row livelocks — the streaming-replica bug this branch
  is chasing.

---

## Code references

- `src/orioledb.c:1255` — `RegisterXactCallback(undo_xact_callback, NULL)`
- `src/transam/undo.c:2110` — `undo_xact_callback` definition
- `src/transam/undo.c:2148-2170` — no-oxid / parallel-worker fast path
- `src/transam/undo.c:2222-2253` — `XACT_EVENT_PRE_COMMIT` (precommit_undo_stack, joint commit)
- `src/transam/undo.c:2255-2413` — `XACT_EVENT_COMMIT` body
- `src/transam/undo.c:2320-2322` — `orioledb-commit-assert` crit-section injection bracket
- `src/transam/undo.c:2329` / `oxid.c:1417` — `current_oxid_precommit` (CSN → COMMITTING)
- `src/transam/undo.c:2334-2347` — HOLD_INTERRUPTS note + stopevent RESUME/HOLD
- `src/transam/undo.c:2350-2352` — global `nextCommitSeqNo` fetch-add
- `src/transam/undo.c:2376` / `oxid.c:1507` — `current_oxid_commit` (final CSN)
- `src/transam/undo.c:2391-2396` — `on_commit_undo_stack` loop (commit-stage callbacks only)
- `src/transam/undo.c:2415-2514` — `XACT_EVENT_ABORT` body
- `src/transam/undo.c:2425-2436` — `wal_rollback`
- `src/transam/undo.c:2461` / `oxid.c:1601` — `current_oxid_clear_committing` (release spinners)
- `src/transam/undo.c:2467-2468` — `apply_undo_stack` loop (the rollback)
- `src/transam/undo.c:2486` / `oxid.c:1541` — `current_oxid_abort` (CSN → ABORTED)
- `src/transam/undo.c:1162-1230` — `walk_undo_range` (per-record dispatch; prev vs onCommit chain)
- `src/transam/undo.c:1298-1410` — `walk_undo_stack` (commit vs abort branches)
- `src/transam/undo.c:1412-1434` — `apply_undo_stack` / `precommit_undo_stack` / `on_commit_undo_stack`
- `src/transam/undo.c:1271-1290` — `apply_undo_branches` (recovery sub-chain abort)
- `src/transam/undo.c:430-602` — `update_min_undo_locations` (retain/release sweep)
- `src/transam/undo.c:917-1022` — transaction/snapshot retain-location setting
- `include/transam/undo.h:340-345` — `OUndoCallbackStage` enum (Abort/PreCommit/Commit)
- `include/transam/undo.h:356-359` — `UNDO_REC_EXISTS` / `UNDO_REC_XACT_RETAIN` macros
- `include/transam/undo.h:406` — `undo_xact_callback` prototype
- `src/btree/undo.c:126` — descriptor table entry `modify_undo_callback`
- `src/btree/undo.c:456-614` — `modify_undo_callback` (row reversal; asserts Abort stage; `page_item_rollback` at 583)
- `src/recovery/recovery.c:1620-1689` — `recovery_finish` (in-flight oxid abort; trace at 1663)
- `src/recovery/recovery.c:1886-1945` — `recovery_finish_current_oxid` (commit vs abort branches; apply_undo_stack at 1932)
- `src/recovery/recovery.c:3753`, `4197` — WAL-replay dispatch to commit/abort csn
- `src/recovery/worker.c:588`, `601` — worker-side commit/abort finish
- `src/recovery/wal.c:338` — "in-flight -> discard" comment (no explicit WAL_REC_ROLLBACK)
