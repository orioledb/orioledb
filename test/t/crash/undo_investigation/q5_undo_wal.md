# Q5 — Does the undo record, or undo application, emit WAL?

## Short summary

- **Creating/adding an undo record emits NO WAL.** Undo is purely in-memory
  shared-memory bookkeeping. `get_undo_record()`
  (`src/transam/undo.c:1840`) just allocates space in the reserved undo
  buffer; `add_new_undo_stack_item()` (`src/transam/undo.c:1923`) links it
  into the stack. Neither calls any `wal_*` / `o_wal_*` / `add_*_wal_record`
  function. The row-level WAL records are produced *separately* by the
  modify path (`o_wal_insert/update/delete` in `src/btree/modify.c`,
  `src/tableam/operations.c`, `src/tuple/toast.c`,
  `src/catalog/o_sys_cache.c`), NOT by undo. **Hypothesis CONFIRMED.**

- **Undo APPLICATION (the act of walking/applying the undo stack) emits NO
  WAL.** `apply_undo_stack()` (`src/transam/undo.c:1413`) →
  `walk_undo_stack()` contains only in-memory page operations and
  `csn-trace` `elog(LOG, ...)` markers — no WAL-emitting call. The WAL
  record that *records* "this transaction rolled back" is a *separate*
  finish record written by `wal_rollback()`, only on the **normal backend
  abort** path.

- **The relationship**: undo lifecycle events (record creation, commit-time
  `on_commit_undo_stack`, abort-time `apply_undo_stack`) are WAL-silent.
  The transaction *boundary* records — `WAL_REC_XID`, `WAL_REC_COMMIT`,
  `WAL_REC_ROLLBACK`, `WAL_REC_JOINT_COMMIT` — are emitted by the
  surrounding `wal_commit()` / `wal_rollback()` / `wal_joint_commit()`
  wrappers in `src/recovery/wal.c`, which are called from the
  `undo_xact_callback` (`src/transam/undo.c`), and **only when NOT in
  recovery**.

## WAL record types (`include/recovery/wal_record.h:42`)

The full set is defined by the `ORIOLE_WAL_RECORDS(X)` X-macro
(`include/recovery/wal_record.h:42-60`):

| # | Type | Emitter (`src/recovery/wal.c`) | Undo-lifecycle related? |
|---|------|--------------------------------|--------------------------|
| 1 | `WAL_REC_XID` | `add_xid_wal_record` (`wal.c:550`) | Transaction boundary — prefixes a txn's records; not the undo log itself |
| 2 | `WAL_REC_COMMIT` | `add_finish_wal_record(WAL_REC_COMMIT,...)` via `wal_commit` (`wal.c:303,348`) | Txn boundary: emitted on **commit**, after `on_commit_undo_stack` runs |
| 3 | `WAL_REC_ROLLBACK` | `add_finish_wal_record(WAL_REC_ROLLBACK,...)` via `wal_rollback` (`wal.c:410,441`) | Txn boundary: emitted on **backend abort only** |
| 4 | `WAL_REC_RELATION` | `add_rel_wal_record` | No — relation context for modify records |
| 5 | `WAL_REC_INSERT` | `add_modify_wal_record` / `o_wal_insert` | Row modify (separate from undo) |
| 6 | `WAL_REC_UPDATE` | `add_modify_wal_record` / `o_wal_update` | Row modify (separate from undo) |
| 7 | `WAL_REC_DELETE` | `add_modify_wal_record` / `o_wal_delete` | Row modify (separate from undo) |
| 8 | `WAL_REC_O_TABLES_META_LOCK` | `add_o_tables_meta_lock_wal_record` | No — DDL meta |
| 9 | `WAL_REC_O_TABLES_META_UNLOCK` | `add_o_tables_meta_unlock_wal_record` | No — DDL meta |
| 10 | `WAL_REC_SAVEPOINT` | `add_savepoint_wal_record` (`undo.c:2740`) | Subtxn boundary |
| 11 | `WAL_REC_ROLLBACK_TO_SAVEPOINT` | `add_rollback_to_savepoint_wal_record` (`undo.c:2768`) | Subtxn partial undo boundary |
| 12 | `WAL_REC_JOINT_COMMIT` | `add_joint_commit_wal_record` via `wal_joint_commit` (`wal.c:522`, `undo.c:2248`) | Txn boundary (heap+oriole joint commit) |
| 13 | `WAL_REC_TRUNCATE` | `add_truncate_wal_record` | No — truncate |
| 14 | `WAL_REC_BRIDGE_ERASE` | `add_bridge_erase_wal_record` | No — bridge index |
| 15 | `WAL_REC_REINSERT` | `add_modify_wal_record_extended` / `o_wal_reinsert` | Row modify (PK-changing update) |
| 16 | `WAL_REC_REPLAY_FEEDBACK` | written inside `add_finish_wal_record` (`wal.c:506`) | Replication feedback on commit |
| 17 | `WAL_REC_SWITCH_LOGICAL_XID` | `add_switch_logical_xid_wal_record` (`undo.c:2187`) | Logical-decoding xid linkage |
| 18 | `WAL_REC_RELREPLIDENT` | `add_relreplident_wal_record` | No — replica identity context |

**Undo-lifecycle → WAL mapping (the core answer):**

| Undo lifecycle event | Code site | WAL record emitted? |
|----------------------|-----------|---------------------|
| Create/add undo record | `get_undo_record` `undo.c:1840`, `add_new_undo_stack_item` `undo.c:1923` | **NONE** (pure in-memory) |
| Commit-time undo walk (`on_commit_undo_stack`) | `undo.c:1432`, called `undo.c:2395` | **NONE** by the walk; the surrounding commit path emits `WAL_REC_COMMIT` via `wal_commit` |
| Backend abort undo walk (`apply_undo_stack`) | `undo.c:1413`, called `undo.c:2468` | **NONE** by the walk; the surrounding abort path emits `WAL_REC_ROLLBACK` via `wal_rollback` (`undo.c:2431`) — **but only if `!RecoveryInProgress()`** |
| Subtransaction rollback | `undo.c:2768` | `WAL_REC_ROLLBACK_TO_SAVEPOINT` (boundary, not the undo data) |

So WAL records are the row-level modify records + transaction-boundary
records produced separately in `wal.c`. The undo log itself is never
serialised to WAL.

## Backend abort vs. recovery-finish in-memory abort (the bug-relevant part)

### Normal backend ABORT — DOES write a WAL ROLLBACK record

In `undo_xact_callback`'s `XACT_EVENT_ABORT` case
(`src/transam/undo.c:2415`):

```
2425   if (!RecoveryInProgress())
2426   {
2431       wal_rollback(oxid, logicalXidContext.xid, false);
2436   }
...
2461   current_oxid_clear_committing();
2467   for (i = 0; i < (int) UndoLogsCount; i++)
2468       apply_undo_stack((UndoLogType) i, oxid, NULL, true);
2486   current_oxid_abort();
```

`wal_rollback()` (`src/recovery/wal.c:410`) appends `WAL_REC_XID` (if
needed, `wal.c:439`) + `WAL_REC_ROLLBACK` (`add_finish_wal_record`,
`wal.c:441`) and flushes (`flush_local_wal`, `wal.c:443`). It is explicitly
guarded `Assert(!is_recovery_process())` (`wal.c:422`). **So a live backend
abort produces a `WAL_REC_ROLLBACK` in the WAL stream**, which a standby
replays so it learns the transaction aborted.

Note the ordering: `wal_rollback()` (WAL emission) runs FIRST, then
`apply_undo_stack()` (the actual in-memory undo). The WAL record and the
undo application are two independent steps; the WAL record is a finish
*marker*, not a serialisation of the undo records.

### Recovery-finish in-memory abort — does NOT write any WAL (CONFIRMED in code)

`recovery_finish()` (`src/recovery/recovery.c:1620`) walks every leftover
transaction state; for those still `INPROGRESS` at recovery end
(`recovery.c:1650`):

```
1663   elog(LOG, "recovery-finish-abort-trace pid=%d worker=%d aborting in-flight oxid=%lu", ...);
1668   for (i = 0; i < (int) UndoLogsCount; i++)
1669       set_cur_undo_locations((UndoLogType) i, cur_state->undo_stacks[i]);
1671       flush_current_undo_stack();
1672   for (i = 0; i < (int) UndoLogsCount; i++)
1673       apply_undo_stack((UndoLogType) i, recovery_oxid, NULL, true);
1674   walk_checkpoint_stacks(cur_state, COMMITSEQNO_ABORTED, ...);
```

This path calls **only** `apply_undo_stack()` (WAL-free, see above) and
`walk_checkpoint_stacks()`. It does **NOT** call `wal_rollback()`,
`add_finish_wal_record()`, `wal_commit()`, or any other `wal.c` emitter.
There is **no `WAL_REC_ROLLBACK` (or any WAL) produced** for these
recovery-finish aborts. The in-code comment at `recovery.c:1652-1662`
states this directly:

> "In-flight transactions left INPROGRESS at recovery end are aborted
> here, in memory, with no WAL emitted. On the primary's crash recovery
> this runs and cleans them up; a live streaming standby never reaches
> recovery_finish(), so the oxid stays INPROGRESS forever and a later
> replayed modify of its row deadlocks in conflict resolution."

This is corroborated structurally elsewhere in `recovery.c`:
`recovery_finish_current_oxid()` (`recovery.c:1886`) — the replay-side
commit/abort applied while consuming a replayed `WAL_REC_COMMIT`/
`WAL_REC_ROLLBACK` — likewise only runs `on_commit_undo_stack` /
`precommit_undo_stack` / `walk_checkpoint_stacks` and sets the CSN directly
in shared memory (`set_oxid_csn`, `recovery.c:1910`). The recovery side is
a pure WAL *consumer*; it never re-emits WAL. There is no WAL-emitting call
anywhere in the recovery finish/abort code.

### Why this matches the bug theory

- The primary's **crash recovery** runs `recovery_finish()` at the end and
  in-memory-aborts the dangling oxids — fine for the primary, because it
  then transitions to normal operation with those oxids cleaned up.
- A **streaming standby** stays in perpetual recovery and **never reaches
  `recovery_finish()`** for those in-flight oxids (it only ever applies the
  WAL it receives). Because the recovery-finish abort emits **no
  `WAL_REC_ROLLBACK`**, there is nothing in the WAL stream telling the
  standby those transactions aborted. Their CSN therefore stays
  `INPROGRESS` forever, and any later replayed modify touching such a row
  wedges in `oxid_get_csn()` returning `INPROGRESS`.

## What is CONFIRMED vs. NOT fully confirmed

**Confirmed from code:**
- Undo record creation/addition emits no WAL (`undo.c:1840`, `undo.c:1923`).
- Undo application (`apply_undo_stack`/`walk_undo_stack`) emits no WAL.
- Row WAL is produced separately by the modify path, not by undo.
- Backend abort path DOES call `wal_rollback` → `WAL_REC_ROLLBACK`, gated by
  `!RecoveryInProgress()` (`undo.c:2425`) and `Assert(!is_recovery_process())`
  (`wal.c:422`).
- The recovery-finish abort path (`recovery.c:1650-1677`) calls only
  `apply_undo_stack` + `walk_checkpoint_stacks` and emits **no WAL** — both
  by code inspection (no `wal_*`/`add_*_wal_record` call in that block) and
  by the explicit in-source comment.

**Not independently verified here (theory-level, would need a runtime
repro):**
- The *runtime claim* that a specific standby actually wedges on a specific
  oxid (e.g. 480) because of this — that requires the harness logs
  (`recovery-finish-abort-trace` + the standby's `oxid_get_csn` spin), not
  static code. The static code is fully consistent with the theory, but the
  static analysis alone cannot prove the standby observably hangs; it proves
  only the *necessary precondition* (recovery-finish abort emits no WAL).

## Code references

- `include/recovery/wal_record.h:42-60` — `ORIOLE_WAL_RECORDS` X-macro, all 18 WAL record types.
- `include/recovery/wal.h:197-226` — emitter prototypes (`wal_commit`, `wal_rollback`, `o_wal_*`, `add_modify_wal_record`, etc.).
- `src/recovery/wal.c:303` — `wal_commit()` → appends `WAL_REC_XID` + `WAL_REC_COMMIT` (`wal.c:323,348`).
- `src/recovery/wal.c:410` — `wal_rollback()`; `Assert(!is_recovery_process())` at `wal.c:422`; `WAL_REC_ROLLBACK` at `wal.c:441`.
- `src/recovery/wal.c:457` — `add_finish_wal_record()` (writes COMMIT or ROLLBACK finish record).
- `src/recovery/wal.c:550` — `add_xid_wal_record()` (`WAL_REC_XID`).
- `src/recovery/wal.c:116/130` — `add_modify_wal_record[_extended]()` (`WAL_REC_INSERT/UPDATE/DELETE/REINSERT`, the row-level modify records).
- `src/transam/undo.c:1840` — `get_undo_record()` (in-memory undo allocation, no WAL).
- `src/transam/undo.c:1923` — `add_new_undo_stack_item()` (in-memory, no WAL).
- `src/transam/undo.c:1413` — `apply_undo_stack()` → `walk_undo_stack()` (no WAL; only `csn-trace` elogs).
- `src/transam/undo.c:1432` — `on_commit_undo_stack()` (no WAL).
- `src/transam/undo.c:2415-2492` — `XACT_EVENT_ABORT` case; `wal_rollback` gated by `!RecoveryInProgress()` at `undo.c:2425`, called `undo.c:2431`; `apply_undo_stack` at `undo.c:2468`.
- `src/transam/undo.c:225` — commit path calls `wal_commit()`.
- `src/recovery/recovery.c:1620` — `recovery_finish()`.
- `src/recovery/recovery.c:1650-1677` — recovery-finish in-memory abort of INPROGRESS oxids: `apply_undo_stack` + `walk_checkpoint_stacks` only, **no WAL**.
- `src/recovery/recovery.c:1652-1662` — in-source comment asserting "no WAL emitted" + standby-wedge theory.
- `src/recovery/recovery.c:1663` — `recovery-finish-abort-trace` instrumentation.
- `src/recovery/recovery.c:1886` — `recovery_finish_current_oxid()` (replay-side commit/abort; sets CSN in shared memory, no WAL re-emission).
- Row-WAL emitter call sites (not undo): `src/btree/modify.c`, `src/tableam/operations.c`, `src/tuple/toast.c`, `src/catalog/o_sys_cache.c`.
