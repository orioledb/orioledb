# Topic 2 — The PG ↔ OrioleDB Interface Boundary During WAL Recovery

> Scope: the *interface boundary* between PostgreSQL and the OrioleDB extension
> during crash / WAL recovery. What hooks orioledb registers, how vanilla PG
> finds its way into orioledb's redo code, what stays in PG-land vs.
> orioledb-land, and what the patches17_6 fork of PostgreSQL adds to make the
> bridge possible.
>
> Sibling topics (handled by other agents): Topic 1 (baseline PG recovery),
> Topic 3 (orioledb-specific recovery internals — undo, csn, page-state),
> Topic 4 (WAL-sequence edge cases). This document deliberately stops at the
> boundary; it does not narrate what happens inside orioledb's per-record
> handlers beyond identifying them.

## Three categories, one rule for every citation

Every concrete claim below is tagged as one of:

- **[VANILLA]** — code that exists in upstream PostgreSQL with no orioledb
  awareness. The custom-rmgr mechanism, the `RmgrData` struct, the
  `XLogReadRecord` loop. This part of the bridge is generic.
- **[PATCH17_6]** — code added to PostgreSQL by the orioledb fork's
  `patches17_6` branch. These are extension-point hooks (function pointers,
  callbacks) that vanilla PG does not expose. The fork keeps them
  source-compatible — none of these *require* orioledb, they just allow it.
- **[ORIOLEDB]** — code inside the `orioledb` extension (the `.so` loaded via
  `shared_preload_libraries`). This is where orioledb registers callbacks
  against the [VANILLA] and [PATCH17_6] hooks.

The full list of patches17_6 commits above PG 17.0 (tag `d7ec59a63d`) is
56 commits — see Appendix B for the relevant subset.

## H2 — Custom Resource Manager: how PG knows about orioledb during redo

OrioleDB uses PostgreSQL's [VANILLA] **custom WAL resource manager**
mechanism. The mechanism predates orioledb (PG 15 introduced it). Every WAL
record carries an `xl_rmid` byte; PG looks up `RmgrTable[xl_rmid]` and
calls `rm_redo` on the entry.

### Reserved ID

OrioleDB hard-codes its custom rmgr ID:

- `/home/user/work/orioledb/include/orioledb.h:109` —
  `#define ORIOLEDB_RMGR_ID (129)`

Vanilla PG defines the legal range:

- `/home/user/work/postgres/src/include/access/rmgr.h:35-36` —
  `RM_MIN_CUSTOM_ID = 128`, `RM_MAX_CUSTOM_ID = UINT8_MAX (255)`
- `/home/user/work/postgres/src/include/access/rmgr.h:60` — `RM_EXPERIMENTAL_ID = 128`

`129` is one past `RM_EXPERIMENTAL_ID`, i.e. it is a **reserved** ID — orioledb
has gone through the wiki-coordinated reservation process referenced in
`/home/user/work/postgres/src/backend/access/transam/rmgr.c:100-104`.

### Registration call

The orioledb extension registers its rmgr inside `_PG_init`:

- `/home/user/work/orioledb/src/orioledb.c:392-402` — `static RmgrData rmgr` declaration
- `/home/user/work/orioledb/src/orioledb.c:1262` —
  `RegisterCustomRmgr(ORIOLEDB_RMGR_ID, &rmgr);`

The struct shape is dictated by [VANILLA] PG:

- `/home/user/work/postgres/src/include/access/xlog_internal.h:349-360` — full
  `RmgrData` definition (8 function-pointer slots).

OrioleDB populates seven of the eight slots (no `rm_mask` — see below):

| Slot          | OrioleDB callback                          | When PG calls it                                          |
|---------------|--------------------------------------------|------------------------------------------------------------|
| `rm_name`     | `"OrioleDB resource manager"` (string)     | At log/diagnostic time                                     |
| `rm_startup`  | `o_recovery_start_hook` (recovery.c:1025)  | Once, from `RmgrStartup()` at top of `PerformWalRecovery`  |
| `rm_cleanup`  | `o_recovery_cleanup` (orioledb.c:387)      | Once, from `RmgrCleanup()` at the end of `PerformWalRecovery` |
| `rm_redo`     | `orioledb_redo` (recovery.c:1106)          | Per WAL record whose `xl_rmid == 129`                      |
| `rm_desc`     | `orioledb_rm_desc`                         | pg_waldump, log lines                                      |
| `rm_identify` | `orioledb_rm_identify`                     | pg_waldump, log lines                                      |
| `rm_mask`     | `NULL`                                     | Only used by `wal_consistency_checking`; orioledb opts out |
| `rm_decode`   | `orioledb_decode` (logical.c:1310)         | Logical decoding (snapbuild)                               |

`rm_mask = NULL` is deliberate: vanilla `wal_consistency_checking` compares
*pages* between primary and standby after replaying a record. OrioleDB does
not log page images — its WAL is row-level (see Topic 4) — so there is nothing
to mask and the feature is N/A.

### Registration constraints

[VANILLA] `RegisterCustomRmgr` (rmgr.c:107-146) enforces: (1) non-empty
`rm_name`, (2) ID in the custom range, and (3)
`process_shared_preload_libraries_in_progress == true` — registration is
only legal from `_PG_init`. Hence orioledb's `_PG_init` early-out at
`orioledb.c:468-469`. Without preloading, the rmgr is never registered,
and PG's redo loop hits `RmgrNotFound` + `ereport(ERROR)` on the first
orioledb record (rmgr.c:91-95).

### Where redo dispatch happens

[VANILLA] redo loop is in `xlogrecovery.c`:

- `/home/user/work/postgres/src/backend/access/transam/xlogrecovery.c:1662` —
  `PerformWalRecovery()`
- Line 1747 — `RmgrStartup()` — calls every registered rmgr's `rm_startup`
  callback. **For orioledb this is the first time control crosses the boundary**:
  vanilla PG → `o_recovery_start_hook()` (sets up worker pool, opens checkpoint
  state).
- Lines 1760-1843 — the main redo apply loop calls `ApplyWalRecord` per record.
- `xlogrecovery.c:2020` — the actual dispatch line:
  `GetRmgr(record->xl_rmid).rm_redo(xlogreader);`
- For orioledb records (`xl_rmid == 129`) this calls
  `orioledb_redo(XLogReaderState *record)` at
  `/home/user/work/orioledb/src/recovery/recovery.c:1106`.
- Line 1901 — `RmgrCleanup()` — calls `o_recovery_cleanup` →
  `o_recovery_finish_hook(cleanup=true)`.

So the *entire interaction* between PG's main redo loop and orioledb's
recovery code, in steady state, is: PG hands an `XLogReaderState*` to
`orioledb_redo()` and gets nothing back. All side effects (state machine
transitions, worker dispatch, undo replay) happen inside orioledb's process
memory.

### The WAL container format

When orioledb emits a record, the payload is a self-describing **container** —
multiple internal "WAL_REC_*" sub-records concatenated. The header carries
`ORIOLEDB_WAL_VERSION` and per-container flags. PG sees only the `xl_rmid` and
the opaque data buffer.

Container build & emit site:

- `/home/user/work/orioledb/src/recovery/wal.c:864-912` —
  `log_logical_wal_container()`. Calls vanilla `XLogBeginInsert()`,
  `XLogRegisterData(...)`, `XLogInsert(ORIOLEDB_RMGR_ID, ORIOLEDB_XLOG_CONTAINER)`.
- `/home/user/work/orioledb/include/orioledb.h:110` —
  `#define ORIOLEDB_XLOG_CONTAINER (0x00)` — orioledb's `xl_info` discriminator
  (only one value is used today, leaving the other 7 bits for future
  expansion).
- `/home/user/work/orioledb/src/recovery/recovery.c:1112` — recovery-side
  assert that the info byte matches.

Inner record-type list:

- `/home/user/work/orioledb/include/recovery/wal_record.h:42-60` — the X-macro
  `ORIOLE_WAL_RECORDS(X)` enumerates every per-tx-event record type
  (`WAL_REC_XID=1`, `WAL_REC_COMMIT=2`, `WAL_REC_ROLLBACK=3`, ...,
  `WAL_REC_RELREPLIDENT=18`).

## H2 — Table Access Method (TableAM): what gets called *outside* recovery

This is the section most likely to be misunderstood. OrioleDB does register a
full `TableAmRoutine` so that `CREATE TABLE ... USING orioledb` works and so
that normal-time queries can touch orioledb relations. **But almost none of
the `TableAmRoutine` slots are exercised during WAL redo.**

### Where the TableAM is wired

`tableam/handler.c:2480-2542` — `static const TableAmRoutine
orioledb_am_methods` (60+ slots populated); `handler.c:2553` —
`orioledb_tableam_handler` (the function exported by `CREATE ACCESS METHOD
orioledb TYPE TABLE HANDLER ...`).

### TableAM is producer-only; the redo path bypasses it entirely

The redo path does **not** loop over `RmgrTable` *and* `TableAmRoutine`. The
only entry into the extension during redo is via `rm_redo`:
`ApplyWalRecord` calls `GetRmgr(xl_rmid).rm_redo(record)` (xlogrecovery.c:2020)
— there is no `table_am->redo_modify(...)` slot in the upstream contract.

Inside `orioledb_redo` → `replay_container` → `replay_on_record`
(recovery.c:3678-3829), dispatch is by inner `WAL_REC_*` type, calling
orioledb-private primitives (`recovery_switch_to_oxid`, `apply_modify_record`,
`apply_sys_tree_modify_record`, `spread_idx_modify`, `o_truncate_table`,
`o_tables_meta_lock_no_wal`, `recovery_savepoint`). None of these go through
the `TableAmRoutine` vtable.

TableAM slots **never** called during redo include:
`orioledb_tuple_insert/update/delete` (handler.c:480+) — they *produce* the
WAL on the normal-ops side; `orioledb_index_build_range_scan` (handler.c:1138)
— recovery-side index builds use the worker pool instead; all `scan_*` slots
— no SQL scans run during redo; `orioledb_relation_size` (handler.c:1282).

The TableAM-to-rmgr mapping is **producer-side only**:

```
Normal ops:    SQL → executor → TableAm.tuple_insert → orioledb_tuple_insert
                                                          ↓
                                                      o_wal_insert (wal.c:917)
                                                          ↓
                                                      add_modify_wal_record(WAL_REC_INSERT, ...)
                                                          ↓
                                                      flush_local_wal → XLogInsert(rmid=129)

Crash & redo:  PG main loop → GetRmgr(129).rm_redo → orioledb_redo
                                                          ↓
                                                      replay_container → replay_on_record
                                                          ↓
                                                      case WAL_REC_INSERT: apply_modify_record (internal)
```

The TableAM slot **does not appear on the recovery side at all**.

### Why this split exists

The TableAM is a per-backend, per-relation, per-tuple API tied to live SQL
execution (`Relation*`, `TupleTableSlot*`, `Snapshot*`). The redo path runs
in the startup process before any backend exists, before
`reachedConsistency` is true, before backends can open relations. The only
contract the redo path has with the extension is "here is a byte buffer
tagged with your rmid, apply it." Note also that
`tuple_complete_modification` ([PATCH17_6], commit `15967dbe43`) is a
*normal-ops* slot despite living in `TableAmRoutine` — it hooks the
executor's tuple-completion phase, not redo.

## H2 — patches17_6 hooks orioledb relies on

These are the additions PostgreSQL needs to carry to make the extension work.
They split into roughly four buckets: recovery-side, transaction-side,
checkpoint-side, and general extensibility (snapshot, locale, etc.). I list
the recovery-relevant ones first.

### Recovery-side hooks (this is the Topic 2 core)

#### `RedoShutdownHook` — [PATCH17_6] (commit `c13889ae21`)

Decl `xlog.h:322`; called from `xlogrecovery.c:1886-1887`; orioledb binds
at `orioledb.c:1263` → `o_recovery_shutdown_hook` (orioledb.c:381) →
`o_recovery_finish_hook(false)`. Fires *only* on
`RECOVERY_TARGET_ACTION_SHUTDOWN` — orioledb drains its worker pool and
writes its undo-position checkpoint before PG's `proc_exit(3)`.
Distinct from `rm_cleanup` (which fires on every `PerformWalRecovery` exit).

#### `GetReplayXlogPtrHook` — [PATCH17_6] (commit `0a0631719b`)

Decl `xlogrecovery.h:51,100`; called at `xlogrecovery.c:4632-4633`;
orioledb binds at `orioledb.c:1280` → `recovery_get_effective_replay_ptr`
(recovery.c:1316). Overrides the LSN walsenders report — default is
`lastReplayedEndRecPtr`; orioledb returns `min(worker_ptrs[i].commitPtr)`
across parallel recovery workers, so walsenders don't claim a record is
replayed before all workers have consumed it.

#### `RecoveryStopsBeforeHook` — [PATCH17_6] (commit `85db5468bb`)

Decl `xlogrecovery.h:106`; called at `xlogrecovery.c:2647-2666`; orioledb
binds at `orioledb.c:1284` → `orioledb_recovery_stops_before_hook`
(recovery.c:1378). PG vanilla `recoveryStopsBefore` only understands
`RM_XACT_ID` as PITR stop boundaries; this hook is invoked *only* for
`RmgrIdIsCustom(xl_rmid)` records (xlogrecovery.c:2648). OrioleDB parses
its container header (`WAL_CONTAINER_HAS_XACT_INFO`) and returns true if
`xactTime` crosses `recoveryTargetTime`.

#### `RecoveryTargetReachedHook` — [PATCH17_6] (commit `e11d3deedc`)

Decl `xlogrecovery.h:113`; called at `xlogrecovery.c:1867-1876`. orioledb
does not currently register this — listed for completeness as the
symmetric "after stop" hook with `recoveryStopAfter/recordPtr/recordEndPtr`
metadata.

#### `xact_redo_hook` — [PATCH17_6] (commits `c13889ae21` + `2e2347e9db`)

Decl `xact.h:543-544`; called at `xact.c:6126-6127` (commit) and
`6280-6281` (abort); orioledb binds at `orioledb.c:1273` →
`o_xact_redo_hook` (recovery.c:4139). The hook for **joint commits**: a
heap-and-orioledb transaction emits both a vanilla `XLOG_XACT_COMMIT` and
a `WAL_REC_JOINT_COMMIT`; redo of the former triggers this hook, which
matches the pending oxid on `joint_commit_list` and finishes the
orioledb-side undo.

#### `HandleStartupProcInterrupts_hook` and `base_init_startup_hook` — [PATCH17_6] (commit `f316acfe92`)

The interrupt hook (registered at `recovery.c:1570`) piggy-backs
worker-queue draining onto the startup process's interrupt poll. The
base-init hook (registered at `orioledb.c:1275-1276`; called from
`postinit.c:662-663`) fires from `BaseInit()` for every backend type; for
`MyBackendType == B_STARTUP` orioledb cleans up stale `*.tmp`/`*.map`
checkpoint temp files before recovery sees them (orioledb.c:1293-1310).

### Checkpoint-side hooks (recovery-adjacent)

#### `CheckPoint_hook` — [PATCH17_6] (commit `c13889ae21` + ordering fix `7563680371`)

Decl `xlog.h:314-315`; called from `xlog.c:7548-7549` inside
`CreateCheckPoint` *after* `CheckPointBuffers`; orioledb binds at
`orioledb.c:1259` → `o_perform_checkpoint` (src/checkpoint/checkpoint.c).
Drives orioledb's CoW checkpoint (atomic page-pointer swap, undo
truncation, file-extent flush). Writes `controlReplayStartPtr` and
`controlToastConsistentPtr`, which `orioledb_redo` (recovery.c:1171-1199)
consumes on the next startup.

#### `after_checkpoint_cleanup_hook` — [PATCH17_6] (same commit)

Decl `xlog.h:317-320`; called at `xlog.c:6139-6140` (end-of-recovery) and
`xlog.c:7370-7371` (per-checkpoint); orioledb binds at `orioledb.c:1260`.
The end-of-recovery site is **the only PG → orioledb call between "WAL
replay finished" and "cluster opens for writes"** — orioledb flushes
post-recovery state and (if S3 mode is on) signals replay caught up.

### Transaction-side hooks

#### `get_xidless_commit_lsn_hook` — [PATCH17_6] (commit `66616e11b1`)

Decl `xact.h:160-162`; called at `xact.c:1403-1405` (xidless branch of
`CommitTransaction` — orioledb-only transactions without a top heap xid
that participated in logical replication). Orioledb binds at
`orioledb.c:1257` → `orioledb_get_xidless_commit_lsn` (undo.c:238).

#### `getRunningTransactionsExtension` / `waitSnapshotHook` — [PATCH17_6] (commit `8aae2edc37`)

Decls `standby.h:105` / `snapbuild.h`; called at `procarray.c:2884-2886`
and `snapbuild.c:1627-1628`. Let orioledb attach its CSN-snapshot state to
the `xl_running_xacts` stream used by hot-standby and logical decoding.

#### `RegisterXactCallback(undo_xact_callback, NULL)` — [VANILLA] API

`xact.h`; called at `xact.c:2401-2402` inside `CommitTransaction`.
Orioledb registers at `orioledb.c:1255`. This is the load-bearing piece
for the commit-time SK-leak investigation: WAL emission (`wal_commit`,
`add_finish_wal_record`, the `WAL_REC_COMMIT` write) happens in
`XACT_EVENT_PRE_COMMIT`; undo cleanup happens in `XACT_EVENT_COMMIT`.
Impl in `src/transam/undo.c`.

### General extensibility (not redo-time; consolidated)

Other [PATCH17_6] hooks orioledb registers in `_PG_init` but which are
not called from the redo loop: `AcceptInvalidationMessagesHook`
(inval.c:906-907), `CustomErrorCleanupHook` (elog.c:3789-3790),
`snapshot_register_hook` / `snapshot_deregister_hook` (snapmgr.c),
`IndexAMRoutineHook` (amapi.c:33-35), `skip_tree_height_hook`
(plancat.c:489), `database_size_hook`, `set_plain_rel_pathlist_hook`,
`VacuumHorizonHook` (heapam_visibility.c:132-134, procarray.c),
`pg_newlocale_from_collation_hook`. See Appendix A for the full
registration/call-site table and Appendix B for commit IDs.

### A non-hook: PG's smgr / md.c is NOT modified by patches17_6 for orioledb

Grep of `src/backend/storage/smgr/` and `src/include/storage/smgr.h` finds
**zero** orioledb references. OrioleDB does not register a custom smgr; it
bypasses the buffer manager and manages its own files under
`orioledb_data/` and `orioledb_undo/` (`include/orioledb.h:107-108`). PG-side
`varatt.h` does carry `VARTAG_ORIOLEDB = 34` (varatt.h:109,122,309,330) for
toast-pointer-format recognition, but that's data-format extension, not smgr.
The closest smgr-adjacent change is the checkpointer patch
(`checkpointer.c:218-232`, comment line 67) which adds
`InitializeTimeouts`, `InitDeadLockChecking`, `RelationCacheInitialize`,
`InitCatalogCache`, `SharedInvalBackendInit` to `CheckpointerMain` so
orioledb's CoW-checkpoint code can use PG-level locks and the catalog cache
from inside the checkpointer.

## H2 — Custom WAL container / overflow record types

OrioleDB does **not** use PG's main/FSM/VM-fork smgr overflow record types.
All orioledb WAL is a single `xl_rmid=129`, `xl_info=ORIOLEDB_XLOG_CONTAINER=0x00`
record carrying an opaque payload of inner `WAL_REC_*` sub-records (Topic 4
covers wire format). Container header (in record data, not PG's
`XLogRecord` header): `wal_version` (uint16) =
`ORIOLEDB_WAL_VERSION=17` (wal.h:31); `flags` (uint8) =
`WAL_CONTAINER_HAS_XACT_INFO (1<<0)` / `WAL_CONTAINER_HAS_ORIGIN_INFO (1<<1)`
(wal.h:19-20); optional `WALRecXactInfo { xactTime, xid }` (needed for PITR
stop-time); optional `WALRecOriginInfo { origin_id, origin_lsn }` (logical
origin tracking); then concatenated inner records walked by the X-macro
`ORIOLE_WAL_RECORDS` (wal_record.h:42-60). When an inner buffer fills up
mid-transaction, orioledb emits a fresh PG-level record with a fresh
container; the next transaction's `WAL_REC_XID` re-establishes the active
oxid (`flush_local_wal_if_needed` at wal.c:830-862).

## H2 — Shared-memory startup wiring

Sequence at process startup (from `shared_preload_libraries='orioledb.so'`):

1. Postmaster's `process_shared_preload_libraries()` ([VANILLA]) sets
   `process_shared_preload_libraries_in_progress = true`, `dlopen`s
   `orioledb.so`, and calls its `_PG_init` symbol.
2. `_PG_init` (orioledb.c:462) early-outs if not preloading (line 468).
3. Declares GUCs, opens `orioledb_data/` and `orioledb_undo/` (lines 471-473).
4. Saves previous hook-chain pointers, installs its own hooks into the
   global function-pointer slots (lines 1244-1290) — most are [PATCH17_6].
5. Registers bgworkers via vanilla `RegisterBackgroundWorker`:
   `register_bgwriter` × N (workers/bgwriter.c:46);
   `register_s3worker` × N if `orioledb_s3_mode` (s3/worker.c:114);
   `register_rewind_worker` if `enable_rewind` (rewind/rewind.c:973).
6. `RegisterCustomRmgr(ORIOLEDB_RMGR_ID, &rmgr)` (line 1262) — plugs
   orioledb into the redo loop.
7. `RegisterXactCallback(undo_xact_callback, NULL)` (line 1255).
8. Catalog cache callbacks via `CacheRegisterUsercacheCallback` (line 1258).

When the postmaster forks any backend (including the startup process),
vanilla `BaseInit()` calls `base_init_startup_hook` (postinit.c:662-663)
and the `shmem_startup_hook` chain runs — orioledb's link is
`orioledb_shmem_startup` (orioledb.c:1454).

### Recovery worker pool startup

`o_recovery_start_hook` (the rmgr's `rm_startup` slot) at recovery.c:1025
is where the recovery worker pool comes into existence:
`recovery_worker_register` (worker.c:165) builds a `BackgroundWorker`
struct with `bgw_function_name = "recovery_worker_main"` and calls vanilla
`RegisterDynamicBackgroundWorker`; each worker attaches to its shm queue
(`shm_mq_attach`) and the startup process dispatches modify ops by table-OID
hash via `worker_send_msg`. **Recovery workers are bgworkers, started
lazily from inside the rmgr startup callback**, unlike the bgwriter workers
(registered in `_PG_init`, started at postmaster start). Recovery workers
shut down when redo completes.

## H2 — WAL replay dispatch: the full call graph

End-to-end, from PG's startup process calling `PerformWalRecovery()` down to
orioledb's per-record handler:

```
PerformWalRecovery                              [VANILLA]   xlogrecovery.c:1662
  RmgrStartup                                   [VANILLA]   rmgr.c:58
    rm_startup (rmid=129)  → o_recovery_start_hook          recovery.c:1025
                              ├ RegisterDynamicBackgroundWorker × N         (worker.c:165)
                              └ recovery_init(-1)                           (recovery.c:1476)

  MAIN REDO LOOP                                [VANILLA]   xlogrecovery.c:1760-1843
    ApplyWalRecord                              [VANILLA]   xlogrecovery.c:1937
      GetRmgr(xl_rmid).rm_redo                              xlogrecovery.c:2020
        ├ rmid=RM_XACT_ID  → xact_redo_commit/abort         xact.c:6117 / 6275
        │                      └ xact_redo_hook             xact.c:6126 / 6280   [PATCH17_6]
        │                         └ o_xact_redo_hook        recovery.c:4139      [ORIOLEDB]
        │
        └ rmid=129         → orioledb_redo                  recovery.c:1106      [ORIOLEDB]
                              ├ if past controlToastConsistentPtr:
                              │   workers_synchronize + apply_pending_sk_fixups
                              │   workers_notify_toast_consistent
                              └ replay_container                                 (recovery.c:4103)
                                  └ replay_on_record                             (recovery.c:3678)
                                      switch (rec->type):
                                        WAL_REC_XID, _COMMIT, _ROLLBACK,
                                        _JOINT_COMMIT, _RELATION,
                                        _INSERT/UPDATE/DELETE/REINSERT,
                                        _O_TABLES_META_LOCK/_UNLOCK,
                                        _TRUNCATE, _SAVEPOINT,
                                        _ROLLBACK_TO_SAVEPOINT, _BRIDGE_ERASE
                                        — dispatches via apply_modify_record /
                                          spread_idx_modify (to recovery workers)

  if reachedRecoveryTarget:
    RecoveryTargetReachedHook                              [PATCH17_6, unused by orioledb]
    if SHUTDOWN action: RedoShutdownHook                   [PATCH17_6]
                          └ o_recovery_shutdown_hook        (orioledb.c:381)
                              └ o_recovery_finish_hook(false)

  RmgrCleanup                                  [VANILLA]   rmgr.c:74
    rm_cleanup (rmid=129)  → o_recovery_cleanup             (orioledb.c:387)
                              └ o_recovery_finish_hook(true) (recovery.c:1209)
                                  ├ workers_send_finish, worker_wait_shutdown × N
                                  └ recovery_finish(-1)
```

## H2 — End-of-recovery handshake

After `PerformWalRecovery()` returns, vanilla `StartupXLOG` continues:
`PerformRecoveryXLogAction()` writes `XLOG_END_OF_RECOVERY` or a
shutdown-checkpoint, then `CleanupAfterArchiveRecovery` runs (xlog.c:6120-6131).
`xlog.c:6139` then calls **`after_checkpoint_cleanup_hook(EndOfLog, 0)`** —
the *one* PG → orioledb call between "WAL replay finished" and "cluster
opens for writes" (`o_after_checkpoint_cleanup_hook` in
`src/checkpoint/checkpoint.c` handles S3 reconciliation and on-disk sync).
Finally `ControlFile->state = DB_IN_PRODUCTION` at xlog.c:6157-6158.

OrioleDB's *internal* end-of-recovery work (draining post-toast-consistent
worker queues, applying remaining `joint_commit_list`, flushing undo
positions) happens earlier, inside `rm_cleanup` / `o_recovery_finish_hook`.
S3-mode reconciliation also runs early: `s3_check_control` at
`orioledb.c:1213` is invoked from `_PG_init` before recovery starts; S3
bgworkers (registered via `register_s3worker`) handle uploads asynchronously
once the cluster is open.

## H2 — ASCII sequence diagram: PG postmaster crash → recovery → orioledb hooks

```
TIME ────────────────────────────────────────────────────────────────────►

 T0  postmaster sends SIGQUIT to all backends [VANILLA postmaster.c quickdie()].
     orioledb backends die WITHOUT finishing undo cleanup.
       │
       ▼
 T1  postmaster restarts (restart_after_crash=on), forks startup process.   [VANILLA]
       │
       ▼
 T2  BaseInit() → base_init_startup_hook                                    [PATCH17_6]
       └─ o_base_init_startup_hook  (orioledb.c:1293)                       [ORIOLEDB]
            → recovery_cleanup_old_files (stale .tmp / .map removal)
       │
       ▼
 T3  shmem_startup_hook chain                                               [VANILLA+chain]
       └─ orioledb_shmem_startup  (orioledb.c:1454)                         [ORIOLEDB]
            → ShmemInitStruct("orioledb_enigne", ...) + LWLock tranches
       │
       ▼
 T4  InitWalRecovery → PerformWalRecovery → RmgrStartup                     [VANILLA]
       └─ RmgrTable[129].rm_startup = o_recovery_start_hook                 [ORIOLEDB]
            (recovery.c:1025)
            → RegisterDynamicBackgroundWorker × recovery_pool_size_guc
            → recovery_init(-1) (xid hash, syscache hooks, read_xids)
            → recovery_worker_main per worker  (worker.c:200)
       │
       ▼
 T5  main redo loop:  do { ApplyWalRecord(...); } while (record);           [VANILLA]
       per record:
         xl_rmid==RM_XACT_ID → xact_redo_commit/abort → xact_redo_hook      [PATCH17_6]
                                  → o_xact_redo_hook (joint commits)        [ORIOLEDB]
         xl_rmid==129        → orioledb_redo  (recovery.c:1106)             [ORIOLEDB]
                                  → replay_container → replay_on_record
                                  → workers apply INSERT/UPDATE/DELETE in parallel
       │
       ▼
 T6  redo loop ends → RmgrCleanup                                           [VANILLA]
       └─ RmgrTable[129].rm_cleanup = o_recovery_cleanup                    [ORIOLEDB]
            → o_recovery_finish_hook(true)  (recovery.c:1209)
              workers_send_finish + worker_wait_shutdown × N
              recovery_finish(-1) (flush undo retain locations)
       │
       ▼
 T7  PG writes XLOG_END_OF_RECOVERY / shutdown checkpoint                   [VANILLA]
       └─ after_checkpoint_cleanup_hook(EndOfLog, 0)                        [PATCH17_6]
            → o_after_checkpoint_cleanup_hook  (checkpoint.c)               [ORIOLEDB]
              post-recovery file sync, S3 reconciliation start
       │
       ▼
 T8  ControlFile->state = DB_IN_PRODUCTION → cluster opens for writes.      [VANILLA]
```

The hooks that fire are, in order:
`base_init_startup_hook` → `shmem_startup_hook` → `rm_startup` →
(per-XACT record) `xact_redo_hook` + (per-orioledb record) `rm_redo` →
`rm_cleanup` → `after_checkpoint_cleanup_hook`. Six bridge points total.

## H2 — Summary

- **Single redo entry point**: `rm_redo` at `xlogrecovery.c:2020`. Everything
  else is (a) lifecycle hooks ([VANILLA] `rm_startup`/`rm_cleanup`; [PATCH17_6]
  `RedoShutdownHook`, `xact_redo_hook`, `GetReplayXlogPtrHook`,
  `RecoveryStopsBeforeHook`, `RecoveryTargetReachedHook`), (b) the
  end-of-recovery handshake `after_checkpoint_cleanup_hook`, or (c)
  general-purpose hooks (`base_init_startup_hook`, etc.) that also fire in
  the startup process.
- **TableAM is producer-only** — it generates WAL via `o_wal_insert/...`,
  but is not consulted on the replay side.
- **No smgr modifications** in patches17_6 for orioledb — orioledb manages
  its own storage outside the `RelFileLocator` namespace.
- **No custom WAL overflow record types** — orioledb uses one
  `xl_rmid=129`/`xl_info=0x00` record carrying a self-describing container.
- **rmgr ID `129` is globally reserved**; orioledb *must* be in
  `shared_preload_libraries` to recover a cluster containing any orioledb
  tables (otherwise `RmgrNotFound` ereport at first orioledb record).

## Appendix A — The full hooks table (registration site → call site)

| Hook                                  | Registered at (orioledb.c)  | Vanilla call site                                                       | Category   |
|---------------------------------------|-----------------------------|--------------------------------------------------------------------------|------------|
| `RegisterCustomRmgr(129, &rmgr)`      | orioledb.c:1262             | xlogrecovery.c:2020 (`rm_redo`), rmgr.c:65/81 (`rm_startup`/`_cleanup`) | [VANILLA]  |
| `RedoShutdownHook`                    | orioledb.c:1263             | xlogrecovery.c:1886-1887                                                | [PATCH17_6]|
| `GetReplayXlogPtrHook`                | orioledb.c:1280             | xlogrecovery.c:4632-4633                                                | [PATCH17_6]|
| `RecoveryStopsBeforeHook`             | orioledb.c:1284             | xlogrecovery.c:2647-2666                                                | [PATCH17_6]|
| `xact_redo_hook`                      | orioledb.c:1273             | xact.c:6126-6127 (commit), 6280-6281 (abort)                            | [PATCH17_6]|
| `get_xidless_commit_lsn_hook`         | orioledb.c:1257             | xact.c:1403-1405                                                        | [PATCH17_6]|
| `CheckPoint_hook`                     | orioledb.c:1259             | xlog.c:7548-7549                                                        | [PATCH17_6]|
| `after_checkpoint_cleanup_hook`       | orioledb.c:1260             | xlog.c:6139-6140 (end-of-recovery), 7370-7371 (per-ckpt)                | [PATCH17_6]|
| `AcceptInvalidationMessagesHook`      | orioledb.c:1252             | inval.c:906-907                                                         | [PATCH17_6]|
| `CustomErrorCleanupHook`              | orioledb.c:1265             | elog.c:3789-3790                                                        | [PATCH17_6]|
| `snapshot_register/deregister_hook`   | orioledb.c:1266-1267        | snapmgr.c (multiple sites)                                              | [PATCH17_6]|
| `IndexAMRoutineHook`                  | orioledb.c:1277             | amapi.c:33-35                                                           | [PATCH17_6]|
| `getRunningTransactionsExtension`     | orioledb.c:1278             | procarray.c:2884-2886                                                   | [PATCH17_6]|
| `waitSnapshotHook`                    | orioledb.c:1279             | snapbuild.c:1627-1628                                                   | [PATCH17_6]|
| `skip_tree_height_hook`               | orioledb.c:1271-1272        | plancat.c:489                                                           | [PATCH17_6]|
| `VacuumHorizonHook` (if rewind)       | orioledb.c:1287             | heapam_visibility.c:132-134, procarray.c:2032-2034                      | [PATCH17_6]|
| `pg_newlocale_from_collation_hook`    | orioledb.c:1274             | (patch-added in `pg_locale.c`)                                          | [PATCH17_6]|
| `base_init_startup_hook`              | orioledb.c:1275-1276        | postinit.c:662-663                                                      | [PATCH17_6]|
| `HandleStartupProcInterrupts_hook`    | recovery.c:1570             | (patch-added in startup.c)                                              | [PATCH17_6]|
| `RegisterXactCallback(undo_xact_cb)`  | orioledb.c:1255             | xact.c:2401-2402                                                        | [VANILLA]  |
| `CacheRegisterUsercacheCallback`      | orioledb.c:1258             | (patch-added)                                                           | [PATCH17_6]|

## Appendix B — Patches17_6 commits this report depends on

Filtered from `git log --oneline patches17_6 ^d7ec59a63d` (above PG 17.0).
Recovery-relevant:

- `c13889ae21` Recovery and checkpointer hooks (RedoShutdownHook, CheckPoint_hook, after_checkpoint_cleanup_hook, xact_redo_hook)
- `0a0631719b` GetReplayXlogPtrHook
- `85db5468bb` RecoveryStopsHook for custom WAL records
- `e11d3deedc` Recovery target reached hook with stop-boundary metadata
- `2e2347e9db` Call xact_redo_hook on replaying transactions
- `66616e11b1` Replication origin advance for xid-less commits
- `7563680371` Move CheckPoint_hook() call after CheckPointBuffers() (ordering fix)
- `8aae2edc37` Rework handling of running transactions by extensions

Startup / lifecycle / misc:
- `f316acfe92` base_init_startup_hook + HandleStartupProcInterrupts_hook
- `21d97b84ce` AcceptInvalidationMessagesHook
- `9e8e7d2259` Hook for custom error cleanup
- `a19e20cb7e` Allow locks in checkpointer (InitDeadLockChecking patch)
- `118de6f329` Don't cancel recovery processes because of deadlocks
- `ae09f7059c` Snapshot extension and hooks; `cb493eb6df` SnapshotData split
- `188c810728` pg_newlocale_from_collation_hook
- `6f82958259` database_size hook; `6b6eb10f5c` set_plain_rel_pathlist_hook
- `803fc3febc` added injection points (crash-harness dependency)

TableAM / IndexAM (producer-side):
- `b9a4b5d93f` Improvements to TableAM API
- `15967dbe43` tuple_complete_modification TableAM callback
- `7d93efcb98` Extend tableam->relation_size()
- `6a6e9ffa7e` Hook to override index AM routine
- `db45c025b1` Methods for index update and delete
- `333f7e3fd3` Make index insert compatible with outside callers
