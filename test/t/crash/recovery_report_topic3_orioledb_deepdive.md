# Topic 3: OrioleDB Internal Recovery Deep Dive

This topic picks up where Topic 2 leaves off: PostgreSQL's startup process has
opened the WAL stream, identified an `RM_ORIOLEDB_ID` record, and dispatched it
to the orioledb resource-manager handler `orioledb_redo`. From that call onward
the heap-side machinery is irrelevant — every byte of state that has to land in
a B-tree leaf, an undo record, an oxid/CSN map entry, or a TOAST tree, is the
responsibility of orioledb's own code under `src/recovery/`, `src/btree/`,
`src/transam/`, and `src/checkpoint/`.

This deep dive maps that code path: the on-disk structures redo must mutate,
the row-level WAL record types it consumes, how a single UPDATE record fans out
into one PK mutation plus N SK deltas, how page LSNs decide whether a record
is a no-op, how oxid/CSN/undo state is rebuilt, how parallel recovery workers
shard work by oxid hash, and how the consistent point is reached for both the
main trees and TOAST.

## 1. On-disk model recap (what redo is mutating)

Before tracing the redo path it helps to fix the storage objects redo touches.
OrioleDB has no PostgreSQL heap; every relation is a set of B-trees rooted in
orioledb's own catalog and described by `OTableDescr` /`OIndexDescr`.

The trees a single user table can have, in dependency order:

- **PK tree** (`oIndexPrimary`): leaf tuples are the row payload, indexed by
  the user's PRIMARY KEY (or a synthetic ctid if no PK was declared).
  This tree IS the table. Any seqscan of the table is a walk of this
  B-tree.
- **SK trees** (`oIndexRegular` / `oIndexUnique`): leaf tuples contain the
  indexed columns followed by the PK key. The PK key is the row's
  "location"; there is no `ctid` indirection layer.
- **TOAST tree** (`oIndexToast`): rows whose values exceeded the inline
  threshold are sliced into chunks under a synthetic key. The TOAST
  tree is loaded into the standard PG TOAST mechanism via orioledb's
  wrappers.
- **Bridge tree** (`oIndexBridge`): present when the table opted into
  `ctid` emulation; it maps emulated `ctid` -> PK key for callers that
  need a stable item pointer (e.g. SP-GiST). Bridge trees consume
  WAL_REC_BRIDGE_ERASE records and have their own per-bridge `recovery_rec_*`
  callback path in `src/recovery/recovery.c:2235` (`replay_erase_bridge_item`).

Aside from the per-table trees, recovery also touches a small fleet of
**system trees** (`IS_SYS_TREE_OIDS()` in `src/recovery/recovery.c:2168`).
These are persistent btrees under fixed pseudo-OIDs (`SYS_TREES_O_TABLES`,
`SYS_TREES_O_INDICES`, typecache subtrees, etc.). They share the redo
machinery but are *always* applied by the leader, never sharded onto
workers — see `apply_sys_tree_modify_record` at
`src/recovery/recovery.c:4662`. The redo leader holds the OTablesMetaLock
across the records that mutate them, just as the primary does at runtime.

Per-page header on disk is `OrioleDBPageHeader` (defined in
`include/orioledb.h:356`), wrapping a transient `OrioleDBOndiskPageHeader` of
the same size. Notably:

```c
typedef struct {
    pg_atomic_uint64 state;
    uint32           pageChangeCount;
    uint32           checkpointNum;       /* <-- crucial for redo */
} OrioleDBPageHeader;
```

**There is no per-page `lsn` field stored on disk.** This is critical: unlike
the heap, orioledb does **not** carry a `pd_lsn`-equivalent in its page
header. The redo loop therefore cannot ask "has this page already absorbed
this WAL record?" by simple LSN comparison. Instead, idempotence is enforced
*through the row-level identity of the change itself*: the modify path always
goes through `o_btree_modify()` with a tuple key, and the
`recovery_*_callback`s under `src/recovery/recovery.c:2015-2156` use
`xactInfo` and `deleted` flags to decide whether the same change is already
present. The `checkpointNum` field above is the *checkpoint generation* the
page belongs to — used by I/O and CoW, not by redo skip logic. See
`src/orioledb.c:1396` and `src/utils/ucm.c:251` for the few sites that
re-stamp it.

Page-level resilience to a re-applied record is therefore a property of the
**tuple model**, not of an `lsn>=record_lsn` test. The per-tree
`lastCheckpointLSN` (Section 6) controls *whether the tree is replayed at
all*, not which individual records skip.

Underpinning all trees:

- **Per-tree data file** (`<prefix>/<datoid>_<reloid>.<chkpnum>`), produced by
  the CoW checkpointer in `src/checkpoint/checkpoint.c`. Once a tree finishes
  checkpoint N, its file is closed at version N+1 (or freed if obsoleted).
- **Free space map** (`SeqBufDescShared freeBuf` inside `BTreeMetaPage`,
  `include/btree/page_contents.h:24-64`) — a per-tree append-only journal of
  free extents, walked by writers and rebuilt on recovery from the saved
  sequence buffer.
- **Undo log** (`src/transam/undo.c`) — an in-memory ring of undo records
  keyed by `UndoLocation`; the per-row tuple header's `undoLocation` field
  (`BTreeLeafTuphdr.undoLocation`, `include/btree/page_contents.h:283`)
  is the back-pointer into it. Section 7 details how the undo log is
  re-populated during redo.
- **xidBuffer** (`src/transam/xidbuf.c`, opaque to redo logic) — the on-disk
  CSN map for committed/aborted oxids. Recovery rebuilds it lazily by
  observing WAL_REC_XID/COMMIT/ROLLBACK records and replaying them through
  `current_oxid_*` helpers — Section 7.
- **Checkpoint metadata files** under `ORIOLEDB_DATA_DIR` — `chkp_<n>.xid`,
  `chkp_<n>.bmp`, `chkp_<n>.tmp`, `chkp_<n>.meta` plus per-tree segments.
  Recovery loads `chkp_<n>.xid` via `read_xids()` at
  `src/recovery/recovery.c:820` to seed any oxids that committed before the
  last completed checkpoint but whose WAL stream may still mention them.

## 2. CoW checkpoint architecture and its consequences for redo

### 2.1 What a checkpoint actually writes

OrioleDB's checkpoints are **copy-on-write**: dirty pages are *not* overwritten
in place. The checkpointer (`o_perform_checkpoint`,
`src/checkpoint/checkpoint.c:1305`) walks every tree, snapshots each dirty
page's contents into a *new* file segment for the next checkpoint generation
(`<file>.<chkpNum+1>` or, internally, an extent inside the per-tree segment),
then **atomically rewires the downlink in the parent page** to point at the
new on-disk extent (`MAKE_ON_DISK_DOWNLINK`,
`include/btree/page_contents.h:301`). The previous-generation file is held
until the new control file is fsynced (Section 2.3), then deleted by
`recovery_cleanup_old_files()` (`src/orioledb.c:1300-1303`,
`src/recovery/recovery.c:recovery_cleanup_old_files`).

Two consequences:

1. **No torn pages.** After a crash the old extent is fully intact because it
   was never overwritten; the new extent is either fully written or it never
   existed (its downlink was never published). PG-style "full page image"
   records (FPI) are therefore unnecessary and orioledb's WAL contains *only*
   row-level records (Section 3).
2. **No per-page LSN.** Because each page extent on disk belongs to exactly
   one checkpoint generation, recovery decides "do I have to replay onto this
   tree?" at *tree granularity*, by checking when this tree's pages were last
   stable — not by per-page LSN. Section 6 details this.

### 2.2 The control file as the recovery anchor

After the CoW phase finishes for every tree, the checkpointer fsyncs the
new-generation tree segments, fsyncs the undo log range, fsyncs the xidmap,
and only *then* writes the new `CheckpointControl` to
`orioledb_data/control` (`src/checkpoint/checkpoint.c:1474-1517`). The
control file write is itself wrapped (`write_checkpoint_control` in
`src/checkpoint/control.c`) so a torn write is detectable.

On startup, `checkpoint_shmem_init` reads the previous-good control file
(`src/checkpoint/checkpoint.c:325-368`) and rehydrates
`checkpoint_state->control{Replay,SysTrees,ToastConsistent}Ptr`, the per-undo
log positions, and the xid water-marks. From this point everything redo does
is anchored against the LSN positions in this control file.

### 2.3 The three checkpoint-control consistent points

The control file (`CheckpointControl`, `include/checkpoint/control.h:42-63`)
publishes three WAL positions. These are the **only** consistent points
recovery has to anchor on:

| Field             | Meaning                                                              | Set at                                                                |
|-------------------|----------------------------------------------------------------------|-----------------------------------------------------------------------|
| `replayStartPtr`  | LSN of WAL at the moment checkpoint snapshotted the sys/table state. | `src/checkpoint/checkpoint.c:1392` (under oTablesMetaLock + oSysTreesLock). |
| `sysTreesStartPtr`| LSN at which checkpointer finished checkpointing system trees and unlocked oTablesMetaLock. | `src/checkpoint/checkpoint.c:1407`. |
| `toastConsistentPtr` | LSN at which the checkpointer crossed from PK trees to SK trees. | `src/checkpoint/checkpoint.c:1428` / `src/checkpoint/checkpoint.c:5114`. |

The control file copies of these are loaded back into
`checkpoint_state->controlReplayStartPtr` etc. at startup
(`src/checkpoint/checkpoint.c:328-332`), and the redo loop reads them
directly (`src/recovery/recovery.c:1171-1199`).

Why three? Because the checkpointer crosses three internally distinct
phases:

1. **Sys-tree phase.** While `oTablesMetaLock` is held, system trees are
   walked and written. `replayStartPtr` = "WAL position at which the
   checkpointer began this phase". The sys-tree image on disk is
   consistent up to `sysTreesStartPtr`.
2. **PK-tree phase.** Lock released, every user table's PK tree is
   checkpointed in turn. Each tree's on-disk image absorbs its own
   in-memory state but the in-progress oxids' UNDO log still references
   tuples in older positions, so the *next* recovery has to apply WAL
   records from `replayStartPtr` forward to those PK trees too.
3. **SK-tree phase.** When the checkpointer hits its first SK
   (`td->type >= oIndexUnique`), it captures `toastConsistentPtr` and
   simultaneously snapshots every backend's PK-applied / SK-pending
   marker (`checkpoint_write_pending_sk_fixups()`,
   `src/checkpoint/checkpoint.c:976`). The PendingSkFixup records
   written to the xids file tell the *next* recovery: "between
   `replayStartPtr` and `toastConsistentPtr` there were N in-progress
   PK inserts whose SK side may not have been emitted to the on-disk SK
   tree — when SK replay reaches `toastConsistentPtr` you must apply
   these fix-ups."

### 2.4 The recovery-side view

In `orioledb_redo` (`src/recovery/recovery.c:1106`) these three fields
control the dispatch:

```c
if (record->ReadRecPtr >= checkpoint_state->controlToastConsistentPtr
    && !toast_consistent)
{
    if (!recovery_single)
        workers_synchronize(record->ReadRecPtr, true);  // drain PK redo
    apply_pending_sk_fixups();                          // emit missing SK rows
    toast_consistent = true;
    if (!recovery_single)
        workers_notify_toast_consistent();              // tell workers
}

if (record->ReadRecPtr >= checkpoint_state->controlReplayStartPtr)
{
    if (!replay_container(msg_start, msg_start + msg_len, ...))
        elog(ERROR, "...");
}
```

So:
- Records with `ReadRecPtr < controlReplayStartPtr` are **skipped entirely**
  — the on-disk image already absorbed them.
- Records between `controlReplayStartPtr` and `controlToastConsistentPtr` go
  through `replay_container` and reach PK trees, but the workers
  short-circuit SK trees in `apply_modify_record`
  (`src/recovery/worker.c:683`):

  ```c
  if (descr && toast_consistent)
      apply_tbl_modify_record(descr, ...);    // PK + all SKs
  else
      apply_btree_modify_record(&id->desc, ...);  // PK only
  ```

  i.e. before TOAST consistency, only the index identified in the
  immediately preceding `WAL_REC_RELATION` is touched — not the full SK
  fanout.
- At the boundary, workers drain (`workers_synchronize`), the leader walks
  the saved `PendingSkFixup` xids-file records and emits the missing SK
  rows (`apply_pending_sk_fixups()`,
  `src/recovery/recovery.c:600`), then flips `toast_consistent` and
  tells workers (`workers_notify_toast_consistent()`,
  `src/recovery/recovery.c:4619`).
- Records past `controlToastConsistentPtr` exercise the full PK + SK
  fanout in `apply_tbl_insert/update/delete` (`src/recovery/worker.c:818`).

For sys-tree mutations the leader applies them only when
`ctx->xlogRecPtr >= checkpoint_state->controlSysTreesStartPtr`
(`src/recovery/recovery.c:3941`).

## 3. WAL record taxonomy and the redo dispatch table

All orioledb records are emitted inside a single PG WAL container of resource
manager `RM_ORIOLEDB_ID`. The bytes that follow the container header are an
orioledb-private stream of records. The full set is enumerated by the X-macro
`ORIOLE_WAL_RECORDS(X)` at `include/recovery/wal_record.h:42`; the write-side
emitter lives in `src/recovery/wal.c`, the read/parse side in
`src/recovery/wal_reader.c`, and the redo-side dispatcher is
`replay_on_record()` at `src/recovery/recovery.c:3677`.

The record types in payload order:

| Record                          | Struct (`include/recovery/wal.h`)        | Payload                                                                                                             |
|---------------------------------|------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `WAL_REC_XID` (1)               | `WALRecXid` (line 67)                    | `oxid`, `logicalXid`, `heapXid`. Sets recovery's current oxid for subsequent records.                               |
| `WAL_REC_COMMIT` (2)            | `WALRecFinish` (line 159)                | `xmin`, `csn`. Closes current oxid as committed.                                                                    |
| `WAL_REC_ROLLBACK` (3)          | `WALRecFinish` (same)                    | `xmin`, `csn`. Closes current oxid as aborted.                                                                      |
| `WAL_REC_RELATION` (4)          | `WALRecRelation` (line 83)               | `treeType`, `oids` (datoid/reloid/relnode), `xmin`/`csn`/`cid`, `version`, `baseVersion`. Selects the target tree.  |
| `WAL_REC_INSERT/UPDATE/DELETE` (5/6/7) | `WALRecModify1` (line 115)        | `tupleFormatFlags`, `length`, tuple bytes. The PK or SK key/tuple. See payload notes below.                         |
| `WAL_REC_O_TABLES_META_LOCK` (8) | `WALRec` (line 62)                      | None. Acquires the meta lock at the leader.                                                                         |
| `WAL_REC_O_TABLES_META_UNLOCK` (9) | `WALRecOTablesUnlockMeta` (line 105)  | `oids`, `oldRelnode`. Releases meta lock and triggers table-shape changes.                                          |
| `WAL_REC_SAVEPOINT` (10)        | `WALRecSavepoint` (line 135)             | `parentSubid`, `logicalXid`, `parentLogicalXid`.                                                                    |
| `WAL_REC_ROLLBACK_TO_SAVEPOINT` (11) | `WALRecRollbackToSavepoint` (line 143) | `parentSubid`, `xmin`, `csn`.                                                                                  |
| `WAL_REC_JOINT_COMMIT` (12)     | `WALRecJointCommit` (line 151)           | `xid` (PG xid), `xmin`, `csn`. Defers final commit to `o_xact_redo_hook`.                                           |
| `WAL_REC_TRUNCATE` (13)         | `WALRecTruncate` (line 166)              | `oids` to truncate.                                                                                                 |
| `WAL_REC_BRIDGE_ERASE` (14)     | `WALRecBridgeErase` (line 174)           | `iptr` to erase from the bridge index.                                                                              |
| `WAL_REC_REINSERT` (15)         | `WALRecModify2` (line 124)               | Old tuple bytes + new tuple bytes. UPDATEs that change the PK get demoted to DELETE+INSERT on disk.                 |
| `WAL_REC_REPLAY_FEEDBACK` (16)  | `WALRec`                                 | None. Marks the tx as wanting synchronous-replication feedback.                                                     |
| `WAL_REC_SWITCH_LOGICAL_XID` (17) | `WALRecSwitchLogicalXid` (line 76)     | `topXid`, `subXid`. Logical-decoding xid binding. Ignored by redo.                                                  |
| `WAL_REC_RELREPLIDENT` (18)     | `WALRecRelReplident` (line 98)           | `relreplident`, `relreplident_ix_oid`. Currently no-op in redo (`src/recovery/recovery.c:3830`).                    |

### 3.1 What the modify records carry

A `WAL_REC_INSERT`/`UPDATE`/`DELETE`/`REINSERT` record carries **only the
target tree's tuple bytes**, not a redo image of a page. The target tree was
selected by the most-recent `WAL_REC_RELATION` (the leader caches it in
`ctx->descr` / `ctx->indexDescr`, `src/recovery/recovery.c:3773-3828`).

- For INSERT, the payload is the new leaf tuple.
- For UPDATE, the payload is the new leaf tuple. (Old tuple is reconstructed
  by callbacks at redo time via lookup against the PK.)
- For DELETE, the payload is the **key** (`OKeyLength`), or the **full
  tuple** if `relreplident == REPLICA_IDENTITY_FULL`. See
  `recovery_rec_delete()` at `src/recovery/recovery.c:2314` for the writer's
  view and `replay_on_record` (case `WAL_REC_DELETE`,
  `src/recovery/recovery.c:4046-4072`) for the redo's view.
- For REINSERT (UPDATE that changed the PK), the payload is **both** tuples
  in a `WALRecModify2`; redo splits it into `DELETE` (old) + `INSERT` (new)
  via `apply_modify_record(... RecoveryMsgTypeDelete ...)` then
  `apply_modify_record(... RecoveryMsgTypeInsert ...)`
  (`src/recovery/recovery.c:4034-4035`).

### 3.2 What modify records do NOT carry

Two omissions are load-bearing for the redo design:

1. **No per-record undo location.** The undo back-pointer in the
   `BTreeLeafTuphdr.undoLocation` field is re-derived during redo by
   *re-inserting* through `o_btree_modify()` — the same modify path used at
   runtime. The undo log is therefore re-populated as a side effect of redo
   applying each record (Section 7).
2. **No SK deltas.** A WAL record targets one tree; the leader/worker that
   replays an UPDATE against the PK is the same one responsible for
   computing and applying the corresponding SK deltas (Section 5).

## 4. The redo loop in `orioledb_redo`

PostgreSQL's startup process dispatches each `RM_ORIOLEDB_ID` container by
calling `orioledb_redo(XLogReaderState *record)` at
`src/recovery/recovery.c:1106`. The function is small and reads top-down:

```
orioledb_redo:
    [bounds check]    record info == ORIOLEDB_XLOG_CONTAINER
    [PITR check]      record->ReadRecPtr vs orioledb.replay_until_lsn (line 1115)
    [TOAST boundary]  if record->ReadRecPtr >= controlToastConsistentPtr
                          && !toast_consistent:                      (line 1171)
                          workers_synchronize(...)
                          apply_pending_sk_fixups()
                          toast_consistent = true
                          workers_notify_toast_consistent()
    [replay]          if record->ReadRecPtr >= controlReplayStartPtr:    (line 1191)
                          replay_container(msg_start, msg_start+msg_len,
                                            recovery_single, ReadRecPtr, EndRecPtr)
    [crash check]     if unexpected_worker_detach: ERROR
```

`replay_container` (`src/recovery/recovery.c:4103`) wraps the orioledb
container in a `WalReaderState` (declared in `include/recovery/wal_reader.h`)
and walks records via `wal_parse_container`. Three callbacks are wired:

- `replay_check_version`: rejects WAL of newer-than-supported binary version
  (`src/recovery/recovery.c:3631`).
- `replay_on_container`: harvests the optional `WAL_CONTAINER_XACT_INFO`
  (PG xid) into `recoveryHeapTransactionId` for logical decoding seeding.
- `replay_on_record`: the main dispatcher. One huge `switch (rec->type)`
  ladder (`src/recovery/recovery.c:3677-4097`) covering every WAL record.

### 4.1 Single-process vs parallel

The redo loop is single-threaded when `*recovery_single_process == true`,
parallel otherwise. The branch is taken at most leaves of the dispatcher
ladder:

```c
if (ctx->single) {
    recovery_switch_to_oxid(rec->oxid, -1);
    apply_modify_record(ctx->descr, ctx->indexDescr, type, tuple1.tuple);
} else {
    spread_idx_modify(&ctx->indexDescr->desc, type, tuple1.tuple);
}
```
(`src/recovery/recovery.c:4078-4086`)

`spread_idx_modify` (`src/recovery/recovery.c:4680`) hashes the tuple's
B-tree key and routes the message to one of the
`recovery_pool_size_guc` workers via `worker_send_modify`. Single-process
just runs `apply_modify_record` inline against the leader process. Section 8
expands on the parallel side.

### 4.2 Ordering invariants the dispatcher enforces

`replay_on_record` keeps a `ReplayWalDescCtx` (`src/recovery/recovery.c:3663`)
across records inside one container. The dispatcher relies on the writer
having emitted records in this canonical order:

1. `WAL_REC_XID` once per oxid before any tuple records for it.
2. `WAL_REC_RELATION` once per (oids, tree-type) before any modify against
   that tree (resets `ctx->descr` / `ctx->indexDescr`).
3. Zero or more `WAL_REC_INSERT/UPDATE/DELETE/REINSERT` for the active
   (oxid, tree).
4. Eventually `WAL_REC_COMMIT` or `WAL_REC_ROLLBACK` (or `JOINT_COMMIT`).

The write side enforces this via the `local_oids`/`local_type` cache in
`src/recovery/wal.c:36-37`: `add_modify_wal_record_extended` checks
`ORelOidsIsEqual(local_oids, oids)` and emits a `WAL_REC_RELATION` whenever
it differs (`src/recovery/wal.c:181-198`). Crash-time, the recovery code
defends against a *truncated* container — every flush (`flush_local_wal`)
emits a self-contained prefix-bounded block.

## 5. From WAL record to btree mutation: `apply_modify_record` and the SK fanout

This is the crux of orioledb redo. A *single* WAL UPDATE record describing one
PK-tree mutation has to result in: one PK row updated, and N SK rows
re-keyed (when any SK-indexed columns changed). The fanout happens
**locally**, on the redo side, in `apply_tbl_*` helpers in
`src/recovery/worker.c`. It is not driven by additional WAL records.

### 5.1 Top of the call graph

For a non-sys-tree modify, `replay_on_record` ends in either
`apply_modify_record` (single-process) or `spread_idx_modify` (parallel).
`spread_idx_modify` (`src/recovery/recovery.c:4680`) computes
`hash = o_btree_hash(desc, rec, ...)`, picks the worker, and forwards the
tuple via a shm_mq message. The worker, in
`recovery_worker_main`'s message loop (`src/recovery/worker.c:471`), calls
`apply_modify_record` itself.

`apply_modify_record` (`src/recovery/worker.c:670`) is two lines of logic:

```c
if (descr && toast_consistent)
    apply_tbl_modify_record(descr, type, p, oxid, COMMITSEQNO_INPROGRESS);
else
    apply_btree_modify_record(&id->desc, type, p, oxid, COMMITSEQNO_INPROGRESS);
```

(`src/recovery/worker.c:683`). The branch:

- **Before TOAST consistent**, or **for trees without a parent `OTableDescr`**
  (sys trees, bridge, isolated indexes): write *only* to the tree named in the
  WAL_REC_RELATION header. The reason: SK tuples may depend on TOASTed
  values whose chunks have not been replayed yet, so synthesising a SK row now
  could be wrong. The skipped SK side is captured at checkpoint time as
  `PendingSkFixup` records (Section 9) and re-emitted at the boundary.
- **After TOAST consistent**: walk all indices, computing the SK keys from
  the new tuple, and applying inserts/deletes to each.

### 5.2 The PK + SK walk for an UPDATE

`apply_tbl_update` (`src/recovery/worker.c:1021`) handles UPDATEs. The key
loop is `for (i = 0; i < descr->nIndices; i++)`. Index 0 is the PK
(`isPrimary = i == PrimaryIndexNumber`). The pseudocode:

```
for each index i in descr->indices:
    if isPrimary:
        o_btree_modify(&PK->desc, BTreeOperationUpdate, new_tuple, ...,
                       modifyCallback = o_update_copy_callback,
                       arg = &tupCopy);
        // o_update_copy_callback writes the *old* tuple into tupCopy
        if modify_result != Updated:
            return;                  // nothing to undo on SK side
        if nIndices == 1:
            return;                  // no SK to update
        tts_orioledb_store_tuple(new_slot, new_tuple, ...);
        tts_orioledb_store_tuple(old_slot, tupCopy.tuple, ...);
    else:
        fill SK keyBound from new_slot -> new_key
        fill SK keyBound from old_slot -> old_key
        if cmp(new_key, old_key) != 0:
            if predicate_satisfied(old_slot):
                o_btree_modify(&SK->desc, Delete, old_key, ...);
            if predicate_satisfied(new_slot):
                build new_stup from new_slot
                o_btree_modify(&SK->desc, Insert, new_stup, ...);
```

Two critical details:

1. **The old PK tuple is captured by callback** during the PK modify.
   `o_update_copy_callback` runs *inside* `o_btree_modify` while it holds
   the PK leaf lock, snapshotting the pre-update tuple into `tupCopy`.
   The redo path then uses `tupCopy.tuple` to recompute the *old* SK key
   for the comparison `cmp(new_key, old_key)`.
2. **The new and old SK keys are compared.** If the SK key is unchanged
   (the UPDATE didn't touch any column the SK indexes), the SK row is
   left alone — no delete-then-insert churn. This is essential for
   replay performance and matches the runtime path.

### 5.3 INSERT and DELETE fanout

INSERT (`src/recovery/worker.c:818`):

```
tts_orioledb_store_tuple(slot, new_tuple, ...);
o_btree_load_shmem(&PK->desc);
btree_ctid_update_if_needed(...);     // for ctid-PK tables
for each index i:
    if isPrimary:
        callbackInfo = recovery_insert_primary_callback;
        callbackInfo.postUndoRecorded = set_pending_sk_marker_from_descr;
        o_btree_modify(&PK->desc, Insert, new_tuple, ...);
    else:
        stuple = tts_orioledb_make_secondary_tuple(slot, SK, true);
        if !predicate_satisfied(SK, slot): continue;
        if o_btree_len(SK, stuple) > max: continue;   // truncated runtime
        o_btree_modify(&SK->desc, Insert, stuple, ...);
```

DELETE (`src/recovery/worker.c:941`):

```
for each index i:
    if isPrimary:
        callbackInfo.modifyCallback = o_delete_copy_callback;
        callbackInfo.arg = &tupCopy;
        o_btree_modify(&PK->desc, Delete, key, ...);
        if modify_result != Deleted: return;
        tts_orioledb_store_tuple(slot, tupCopy.tuple, ...);
    else:
        if !predicate_satisfied(SK, slot): continue;
        fill SK keyBound -> keyBound
        o_btree_modify(&SK->desc, Delete, ..., keyBound, ...);
```

### 5.4 Why the "pending SK marker" matters at redo

The PK callback set `callbackInfo.postUndoRecorded` to
`set_pending_sk_marker_from_descr` (INSERT) or
`set_pending_sk_marker_from_tup_copy` (UPDATE/DELETE). On the primary, these
publish a per-backend `pendingSkUndoLoc` (in `ODBProcData`) for the window
between "PK undo record written" and "SK btree modifications complete".

At redo time the workers run the same callbacks, so `pendingSkUndoLoc` is
populated the same way. If the worker is interrupted between the PK insert
and the SK insert (e.g. recovery is shut down, or a parallel worker is
killed), the in-progress oxid is rolled back via the per-worker undo log,
exactly as a runtime crash would.

This mirrors the runtime PK->SK ordering exactly, which is why a *recovery*
SK desynchronization bug shows the same symptoms as a runtime one — and why
the active investigation under `test/t/crash/` instruments both paths
with the same `csn-trace` / `wal-trace` markers.

### 5.5 The leader-only sys-tree path

System trees are never sharded. The leader applies them inline via
`apply_sys_tree_modify_record` (`src/recovery/recovery.c:4662`), which calls
`apply_btree_modify_record` directly. The leader holds the meta lock for the
duration. See Section 4 above for ordering — system tree records can only
appear when `ctx->xlogRecPtr >= controlSysTreesStartPtr`
(`src/recovery/recovery.c:3941`), because the checkpoint phase ordering
guarantees the on-disk sys-tree image is otherwise complete.

## 6. PK/SK page-LSN checks: how orioledb avoids double-applying records

This section's heading mirrors the original investigation's terminology, but
the answer here is *non-obvious and important to get right* because orioledb's
mechanism does **not** look like PostgreSQL's heap `pd_lsn`-vs-record-LSN
comparison.

### 6.1 There is no per-page LSN

`OrioleDBPageHeader` (`include/orioledb.h:356`) carries `state`,
`pageChangeCount`, `checkpointNum` — and *no* `lsn` field. The page on disk
(`OrioleDBOndiskPageHeader`, `include/orioledb.h:367`) carries
`checkpointNum`, compression info, and version. Again, no LSN. The closest
the page comes to "version" information is the `checkpointNum`, which is the
generation in which the page was last *written*, not the WAL position it was
last *modified at*.

### 6.2 The actual skip ladder, in three layers

Redo idempotence is enforced by a layered set of checks, not a single
per-page test:

#### Layer 1 — container-level skip (cheap, common)

In `orioledb_redo` (`src/recovery/recovery.c:1191`):

```c
if (record->ReadRecPtr >= checkpoint_state->controlReplayStartPtr)
    replay_container(...);
```

If `ReadRecPtr` is below the last checkpoint's `replayStartPtr`, the entire
container is dropped. This is the dominant case: every record produced
*before* the last checkpoint completed its sys-tree phase is skipped at the
container level, because the on-disk sys-tree/PK pages already absorbed it
via the CoW write. No per-record or per-page logic runs.

#### Layer 2 — boundary skip for SK trees

Between `controlReplayStartPtr` and `controlToastConsistentPtr`, the modify
records *do* run, but `apply_modify_record` short-circuits to PK-only
(`src/recovery/worker.c:683`):

```c
if (descr && toast_consistent)
    apply_tbl_modify_record(descr, type, p, oxid, COMMITSEQNO_INPROGRESS);
else
    apply_btree_modify_record(&id->desc, type, p, oxid, COMMITSEQNO_INPROGRESS);
```

SKs are skipped because their on-disk image already absorbed the records
that came before `controlToastConsistentPtr` (CoW). They will only start
being touched when `toast_consistent` flips to true at the boundary.

#### Layer 3 — tuple-level idempotence in `o_btree_modify`

For records that *do* execute, idempotence is a property of the modify
machinery itself, not of a "has this page seen this LSN?" check. The recovery
callbacks (`src/recovery/recovery.c:2015-2156`) inspect the existing tuple's
`OTupleXactInfo` and `deleted` flags:

- `recovery_insert_primary_callback`: if a tuple exists for this PK with the
  same `oxid`, the callback returns "OK, already there" — no second insert.
- `recovery_delete_primary_callback`: if no tuple exists, returns "already
  deleted".
- `recovery_*_deleted_*_callback`: handles the case where a tuple was
  inserted-then-deleted in the same oxid.

This is why redo *can* re-execute a record without corruption: the modify
path computes the actual state difference. Inserting a tuple that's already
there is a no-op; deleting a tuple that's already gone is a no-op.

#### Layer 4 — page-write generation (CoW boundary, not redo)

For completeness: `write_page_to_disk` (`src/btree/io.c:1777-1797`) compares
the in-memory page's `o_header.checkpointNum` against the *target* checkpoint
number. If less, the page is "dirty for this checkpoint" and gets written to
a new on-disk location; if equal it's the same generation and either skipped
or overwritten in place (compressed pages have additional rules). This logic
is about **checkpoint write coordination**, not redo, but it's the same
field that recovery later reads to validate the page is from the expected
generation.

### 6.3 The implication for the active SK-leak investigation

Because there is no per-page LSN, you cannot answer "did this SK page already
absorb this WAL record?" by looking at the page in isolation. The answer
depends on:

1. Where the record sits relative to `controlToastConsistentPtr`.
2. Whether the leader emitted a corresponding `PendingSkFixup` record at
   checkpoint time.
3. Whether the worker that owns the hashed PK key applied the fixup before
   the boundary flipped.

A bug that drops a single SK row therefore cannot be diagnosed by looking at
a page header. It has to be diagnosed by either (a) reconstructing the WAL
stream around `controlToastConsistentPtr` and confirming that a
`PendingSkFixup` should have been emitted and was, or (b) confirming that
the runtime PK->SK ordering (the `pendingSkUndoLoc` mechanism, Section 5.4)
correctly tracked the in-progress oxid through commit. The active
investigation focuses on the second path because the bug reproduces without
any crash near a checkpoint boundary.

### 6.4 Visual: PK/SK record-vs-state ladder

```
WAL record  R at LSN L  hits orioledb_redo:
        |
        v
   L < controlReplayStartPtr ?
        |yes
        +---> SKIP entire container.  Done.
        |no
        v
   L < controlToastConsistentPtr  &&  !toast_consistent ?
        |yes
        +---> dispatch into replay_on_record:
        |       - sys-tree records: applied only if L >= controlSysTreesStartPtr
        |       - INSERT/UPDATE/DELETE: ONLY against the tree named in
        |         the most-recent WAL_REC_RELATION (no SK fanout).
        |         apply_btree_modify_record(&id->desc, ...).
        |no  (toast became consistent OR L past boundary)
        v
   At boundary: workers_synchronize() drains queues,
                apply_pending_sk_fixups() runs,
                toast_consistent <- true,
                workers_notify_toast_consistent().
        |
        v
   Now apply_modify_record() takes the
   `descr && toast_consistent` branch ->
   apply_tbl_*() which walks descr->indices.
        |
        v
   For each index i in descr->indices:
        if isPrimary: o_btree_modify(PK, ...) with copy-callback.
        else:         build SK key from new/old slot,
                      o_btree_modify(SK, ...) only if key changed.
        |
        v
   Inside o_btree_modify, the recovery_*_callback inspects existing
   tuple's xactInfo/deleted flags to make the operation idempotent.
```

## 7. Recovery transaction state: oxid, CSN, undo log

Redo has to rebuild three pieces of transaction state that runtime carries in
shared memory: the oxid -> CSN map, the per-oxid undo-stack locations, and
the xmin/retain-undo bookkeeping. None of these are stored *on disk* in a
form ready to be loaded; they are *rederived from the WAL stream* and from
the saved per-checkpoint xids file.

### 7.1 The per-oxid state at the leader

The leader keeps a `recovery_xid_state_hash` (`src/recovery/recovery.c:1486`)
mapping `OXid -> RecoveryXidState` (defined at
`src/recovery/recovery.c:99-135`). A `RecoveryXidState` carries:

- `xid`: PG xid if this oxid joined a heap commit (joint commit, line 103).
- `retain_locs[UndoLogsCount]`: per-undo-log lowest location that this oxid
  still references (the undo retain pointer; line 107).
- `undo_stacks[UndoLogsCount]`: per-undo-log `UndoStackLocations` tuple
  (branch / subxact / on-commit; line 108). This is the same struct runtime
  backends carry in `cur_undo_locations`.
- `checkpoint_undo_stacks`: dlist of `CheckpointUndoStack` entries loaded
  from the previous-checkpoint xids file for this oxid; replayed at finish
  (line 109, struct at line 142).
- `csn`: COMMITSEQNO_INPROGRESS while in-flight, the real CSN after commit
  (line 110).
- `ptr`: WAL LSN of the commit/abort record (line 111).
- `in_finished_list`, `in_joint_commit_list`, `in_retain_undo_heaps[]`: list
  / heap membership flags so cleanup knows what to detach.
- `systree_modified`, `invalidate_typcache`, `o_tables_meta_locked`,
  `checkpoint_xid`, `wal_xid`: bookkeeping bits read by the dispatcher.
- `used_by[recovery_pool_size_guc + recovery_idx_pool_size_guc]`: which
  workers have seen messages for this oxid; used to coordinate finish.

### 7.2 The lifecycle of an oxid during redo

```
WAL_REC_XID(oxid)            -> recovery_switch_to_oxid(oxid, -1)
                                   - hash_search HASH_ENTER (new entry if first)
                                   - sets cur_recovery_xid_state
                                   - reset_cur_undo_locations() for the new
                                     oxid (or restores prior undo stacks
                                     if we'd seen this oxid before -- e.g.
                                     workload split across containers).
                                   - if new: adds to xmin_queue, sets csn =
                                     COMMITSEQNO_INPROGRESS.

WAL_REC_RELATION(oids,type)  -> sets ctx->descr / ctx->indexDescr; no state
                                change to the oxid hash.

WAL_REC_INSERT/UPDATE/DEL    -> spread_idx_modify(...) OR apply_modify_record(...).
                                Each successful o_btree_modify writes a NEW
                                UndoLocation into the leaf tuple header and
                                advances cur_undo_locations.{branch,subxact,
                                onCommit}.  The runtime path's
                                add_new_undo_stack_item is shared with redo:
                                no special "recovery" undo emission.

WAL_REC_SAVEPOINT(parent)    -> recovery_savepoint() -> add_subxact_undo_item().
                                Records a savepoint frame in the undo stack
                                so a future ROLLBACK_TO_SAVEPOINT can unwind
                                to it.

WAL_REC_ROLLBACK_TO_SAVEPOINT-> recovery_rollback_to_savepoint() ->
                                rollback_to_savepoint() walks the undo
                                stack tail to the matching subxact frame,
                                reverting in-progress modifications.

WAL_REC_COMMIT / ROLLBACK    -> recovery_finish_current_oxid(csn, ptr, -1, sync).
                                See 7.3.
```

### 7.3 `recovery_finish_current_oxid` — the per-record close

At `src/recovery/recovery.c:1871`. Three branches by csn/sync:

**Commit + sync** (`!COMMITSEQNO_IS_ABORTED && sync`, line 1883):

```
set_oxid_csn(oxid, COMMITSEQNO_COMMITTING);
if flush_undo_pos: flush_current_undo_stack();
for each undo log type:
    precommit_undo_stack(undoType, oxid, true);
for each undo log type:
    on_commit_undo_stack(undoType, oxid, true);
walk_checkpoint_stacks(state, csn, InvalidSubTransactionId, flush_undo_pos);
csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
set_oxid_csn(oxid, csn);
set_oxid_xlog_ptr(oxid, XLOG_PTR_ALIGN(ptr));
```

The CSN flip pattern (`COMMITTING` -> real CSN) is the same as the runtime
path in `undo_xact_callback`'s XACT_EVENT_COMMIT case (`src/transam/undo.c`
around line 2321-2386). Both call `current_oxid_precommit`/`current_oxid_commit`
on the runtime path; redo opens-codes the equivalents
(`precommit_undo_stack` / `on_commit_undo_stack` are the per-undo-log
helpers that consume the redo-rebuilt undo locations).

**Commit + async** (no sync; line 1899):

Same precommit/on_commit/walk steps, but defers the CSN assignment and the
`set_oxid_csn(...)` call until the leader catches up — the oxid is pushed
into `finished_list` and processed in batches. This is the parallel-recovery
optimisation: workers can report "I'm done" to the leader before the leader
has synthesised the actual CSN.

**Abort** (`COMMITSEQNO_IS_ABORTED`, line 1913):

```
if flush_undo_pos: flush_current_undo_stack();
for each undo log type:
    apply_undo_stack(undoType, oxid, NULL, true);
walk_checkpoint_stacks(state, csn, InvalidSubTransactionId, flush_undo_pos);
if worker_id < 0:
    if sync: set_oxid_csn(oxid, COMMITSEQNO_ABORTED);
    else: push into finished_list (so workers don't see runXmin advance
                                   past us prematurely).
```

`apply_undo_stack` is the *runtime* abort/rollback machinery
(`src/transam/undo.c:1408`): it walks the redo-rebuilt undo log and
inverse-applies each record. Because the undo log was rebuilt purely by
redo's earlier `o_btree_modify` calls, applying it during abort produces
exactly the inverse effect — undoing what redo just did. Net: an aborted
transaction in WAL is replayed by *inserting* the rows (rebuilding undo as
a side effect) and then *undoing* them (consuming that same undo). The
on-disk state ends up as it would have without the transaction.

### 7.4 The xids file and "checkpoint_xid"

When a checkpoint completes, `o_perform_checkpoint` writes a `chkp_N.xid`
file listing every oxid that was *in-flight* at the moment of checkpoint
(see `flush_current_undo_stack` at `src/recovery/recovery.c:1853` and
`write_to_xids_queue` for the format). Each entry includes the
`UndoStackLocations` for each undo log and a `retainLocation`.

At startup, `read_xids()` (`src/recovery/recovery.c:820`) reads back this
file. For every oxid in it, it pre-populates a `RecoveryXidState` with
`checkpoint_xid = true` and `csn = COMMITSEQNO_INPROGRESS`. This way, when
redo later sees a `WAL_REC_COMMIT` or `WAL_REC_ROLLBACK` for that oxid,
`recovery_switch_to_oxid` finds the existing entry (instead of creating a
fresh one) and the undo stacks the checkpoint saved are already loaded —
ready for `recovery_finish_current_oxid` to walk them.

If a WAL record references an oxid the xids file did not mention, that's a
new oxid that started *after* the checkpoint; redo creates the
`RecoveryXidState` on the fly from the WAL_REC_XID record. Crucially,
*either way* the oxid -> undo stack mapping is whole by the time the
COMMIT/ROLLBACK record arrives.

### 7.5 `recovery_finish` — final cleanup

At end of recovery (`src/recovery/recovery.c:1620`) the leader walks every
remaining oxid in `recovery_xid_state_hash`. Any oxid still
`COMMITSEQNO_IS_INPROGRESS` is treated as a crashed in-flight transaction:
its undo stacks are applied, and the runtime xid map records it as
`COMMITSEQNO_ABORTED`. Any oxid in the `finished_list` (async-commit case)
has its CSN synthesised here.

Net effect: by the time `recovery_finish` returns, the runtime CSN map is
identical to what would have existed if no crash had happened — all
committed oxids have real CSNs, all aborted (including in-flight) oxids
are marked `COMMITSEQNO_ABORTED`.

## 8. Recovery workers: parallel redo by oxid hash

`recovery_pool_size_guc` (default 3) controls how many background workers
the leader spawns to do parallel redo. Each is an
`orioledb recovery worker N` registered as a `BGWORKER_SHMEM_ACCESS`
worker by `recovery_worker_register` (`src/recovery/worker.c:164`). On
startup they all call into `recovery_worker_main`
(`src/recovery/worker.c:199`).

### 8.1 Why sharded — and what's *not* sharded

The orioledb engine's PK trees can be modified concurrently because each leaf
modify only locks the *one* PK leaf it's touching. Two transactions that
modify rows with different PK keys never block each other. The same property
makes redo safely parallelisable: as long as two records with the *same* PK
key go to the *same* worker, ordering is preserved.

Hash:
```c
hash = o_btree_hash(desc, rec, BTreeKeyLeafTuple);  // or BTreeKeyNonLeafKey
worker_id = hash % recovery_pool_size_guc;          // GET_WORKER_ID(hash)
```
(`src/recovery/recovery.c:4694, 41`)

The hash is over the tuple's *btree key* — the PK key for PK records, the
SK key for SK records. Net effect:

- All records for a single (tree, PK-key) end up on the same worker.
- All records for a single (tree, SK-key) end up on the same worker.
- Records on the **same row** but in different trees (PK record + SK record
  for the same row) generally land on **different** workers, because the
  hash inputs are different.

**Not sharded** (always leader):

- System-tree modifications (`apply_sys_tree_modify_record`,
  `src/recovery/recovery.c:4662`).
- `WAL_REC_RELATION`: pure ctx update; the leader caches `descr` then sends
  per-modify messages with `RECOVERY_MODIFY_OIDS` flag set so the worker
  re-fetches the same descr (`src/recovery/worker.c:413-446`).
- `WAL_REC_O_TABLES_META_LOCK/UNLOCK`, `WAL_REC_TRUNCATE`,
  `WAL_REC_BRIDGE_ERASE` (single-process path; in parallel, BRIDGE_ERASE
  is sharded by `o_hash_iptr`, line 3916).
- The toast-consistent boundary handshake (`workers_synchronize` +
  `apply_pending_sk_fixups` + `workers_notify_toast_consistent`).
- The final CSN assignment for sync commits.

### 8.2 Message format and the per-worker queue

Each worker has a fixed-size shm_mq queue
(`RECOVERY_QUEUE_BUF_SIZE = 8KB`, `include/recovery/internal.h:70`),
located by `GET_WORKER_QUEUE(worker_id)` from a global
`recovery_first_queue` base. Messages all start with a
`RecoveryMsgHeader { uint32 type }`. The header type encodes both the
operation (low byte: `RECOVERY_MSG_OPERATION_MASK = 0xFF`) and flags
(`RECOVERY_MODIFY_OXID = 0x100`, `RECOVERY_MODIFY_OIDS = 0x200`).

Message types (`include/recovery/internal.h:48-65`):

| Message                                    | Carries                                                                        |
|--------------------------------------------|--------------------------------------------------------------------------------|
| `RecoveryMsgTypeInsert/Update/Delete`      | header + (optional oxid) + (optional oids+type) + tuple_len + flags + tuple.   |
| `RecoveryMsgTypeBridgeErase`               | header + (optional oxid/oids) + ItemPointerData.                               |
| `RecoveryMsgTypeCommit/Rollback`           | `RecoveryMsgOXidPtr { oxid, ptr, needsFeedback }`.                             |
| `RecoveryMsgTypeSynchronize`               | `RecoveryMsgPtr { ptr }`.                                                      |
| `RecoveryMsgTypeFinished`                  | empty.                                                                         |
| `RecoveryMsgTypeToastConsistent`           | empty.                                                                         |
| `RecoveryMsgTypeSavepoint`                 | `RecoveryMsgSavepoint { oxid, parentSubId }`.                                  |
| `RecoveryMsgTypeRollbackToSavepointt`      | `RecoveryMsgRollbackToSavepoint { oxid, ptr, parentSubId }`.                   |
| `RecoveryMsgTypeLeaderParallelIndexBuild`  | OTable serialisation + ix_num + isrebuild + oxid.                              |
| `RecoveryMsgTypeWorkerParallelIndexBuild`  | DSM seg handle + oxid.                                                         |
| `RecoveryMsgTypeInit`                      | empty (first message per worker).                                              |

Messages are batched by the leader into a per-worker `queue_buf` and flushed
when full (`worker_queue_flush`, `src/recovery/recovery.c:4637`).

### 8.3 The worker's main loop

`recovery_queue_process` (`src/recovery/worker.c:362`) is a tight
`while (!finished)` over `recovery_queue_read` -> per-message dispatch.
On each modify, the worker:

1. Reads the optional oxid (`RECOVERY_MODIFY_OXID` flag) and switches its
   own `cur_recovery_xid_state` (`recovery_switch_to_oxid(oxid, id)`).
2. Reads the optional (oids, ix_type) (`RECOVERY_MODIFY_OIDS`) and refetches
   the table/index descriptor. If `ix_type == oIndexInvalid`, the message
   targets the PK / table-wide path (with SK fanout) — otherwise it targets
   a specific SK or bridge tree.
3. Reads the tuple bytes from the queue buffer.
4. Calls `apply_modify_record(descr, indexDescr, type, tuple)`.

The worker reports progress via `update_worker_ptr(id, ptr)`
(`src/recovery/worker.c:347`) which writes
`worker_ptrs[id].commitPtr = ptr` and bumps `worker_ptrs_changes`.
The leader can compute the "min commitPtr across workers" via
`get_workers_commit_ptr()` (`src/recovery/recovery.c:1259`).

### 8.4 Synchronization: `workers_synchronize`

When the leader needs all workers to drain to a given LSN
(`workers_synchronize`, declared at `src/recovery/recovery.c:677`), it
broadcasts a `RecoveryMsgTypeSynchronize { ptr }` message to each worker
and then spins reading `get_workers_commit_ptr() >= ptr`. This is the
single chokepoint between "PK records done up to LSN L" and "SK records
can start" (for the toast-consistent boundary), and between "txn applied
on all workers" and "leader can write the CSN" (for the sys-tree-modifying
commit path, `src/recovery/recovery.c:3723`).

### 8.5 Tail of recovery: `workers_send_finish` + join

At `o_recovery_finish_hook` (`src/recovery/recovery.c:1209`) the leader
sends `RecoveryMsgTypeFinished` to each worker, then `worker_wait_shutdown`
in a loop until each worker has incremented `worker_finish_count` (and the
optional `idx_worker_finish_count`). A worker that fails to ack the finish
message in time is treated as a recovery failure
(`src/recovery/recovery.c:1247-1250`).

The `index_build_leader` and the `recovery_idx_pool_size_guc` extra
workers handle parallel index builds during recovery (used when
`WAL_REC_RELATION` records describe a new SK whose data must be built
from the PK tree). They're listed at
`src/recovery/worker.c:479-582` (`Recovery{Leader,Worker}ParallelIndexBuild`).

## 9. TOAST and the `toastConsistentPtr` boundary

The TOAST tree is a per-table B-tree storing chunks of out-of-line values.
A row in the PK tree may carry a "toast pointer" pointing into the toast
tree; an SK on that row may also reference the toasted value (e.g. an
SK on a text column that gets toasted). On the primary, writers always
write the toast chunks *before* the PK tuple, and the PK tuple *before*
the SK tuple. The toast tree is therefore at-least-as-current as the PK,
and the PK is at-least-as-current as the SK.

This is the foundation of the per-checkpoint boundary `toastConsistentPtr`.

### 9.1 Two-phase checkpoint, the writer's view

`o_perform_checkpoint` (`src/checkpoint/checkpoint.c:1305`) walks trees in
a strict order:

1. Sys trees, holding `oTablesMetaLock` + `oSysTreesLock` (line 1389-1409).
2. All trees with `td->type < oIndexUnique` (PK + toast + plain indices),
   no global lock (line 1414).
3. First tree with `td->type >= oIndexUnique` (i.e. a unique/SK index)
   triggers the `toastConsistentPtr` capture and the `PendingSkFixup`
   snapshot:

   ```c
   if (td->type >= oIndexUnique &&
       XLogRecPtrIsInvalid(checkpoint_state->toastConsistentPtr))
   {
       checkpoint_state->toastConsistentPtr = get_checkpoint_xlog_ptr();
       checkpoint_write_pending_sk_fixups();
   }
   ```
   (`src/checkpoint/checkpoint.c:5105-5116`)
4. All remaining SK trees.

The fallback case (no SK trees exist at all) is at lines 1426-1430: the
checkpointer still sets the boundary and writes (an empty)
`PendingSkFixup` snapshot. This keeps the recovery boundary present even
for tables with no SKs.

### 9.2 The PendingSkFixup snapshot

`checkpoint_write_pending_sk_fixups()` (`src/checkpoint/checkpoint.c:976`)
walks `oProcData[i]` for every backend slot and harvests
`pg_atomic_read_u64(&oProcData[i].pendingSkUndoLoc)`. If that field holds a
valid undo location, the backend is in the **PK-applied / SK-pending
window** (Section 5.4): the PK row has been inserted/updated/deleted but
the SK side hasn't been emitted yet. The checkpointer writes one
`XidFileRec { kind = XidRecPendingSkFixup, oxid, undoLocation, ... }` per
such backend into the active xids file (chkp_N.xid).

A special sentinel value `WaitingSkUndoLoc` means "the backend will
shortly start the PK insert, on a self-created table" — the checkpointer
*spins* outside the proc's flushLock (`src/checkpoint/checkpoint.c:1006`)
until that sentinel clears, then captures the real undo location.

### 9.3 The recovery boundary handshake

In `orioledb_redo`, just before replaying the first WAL record at LSN
>= `controlToastConsistentPtr`:

```c
if (record->ReadRecPtr >= checkpoint_state->controlToastConsistentPtr &&
    !toast_consistent)
{
    if (!recovery_single)
        workers_synchronize(record->ReadRecPtr, true);   // drain workers
    apply_pending_sk_fixups();                           // emit missing SK
    toast_consistent = true;
    if (!recovery_single)
        workers_notify_toast_consistent();               // tell workers
}
```
(`src/recovery/recovery.c:1171-1189`)

`apply_pending_sk_fixups()` (`src/recovery/recovery.c:600`) walks the
in-memory `pending_sk_fixups_head` list (populated from the xids file by
`record_pending_sk_fixup`, `src/recovery/recovery.c:306`) and, for each
entry:

1. `recovery_switch_to_oxid(entry->oxid, -1)`.
2. `set_oxid_csn(entry->oxid, COMMITSEQNO_INPROGRESS)`.
3. `apply_one_pending_sk_fixup(entry)` — `src/recovery/recovery.c:325`:
   - Reads the PK undo record back via `undo_read(UndoLogRegular,
     itemLoc, ...)`.
   - Resolves the table descr through `o_fetch_index_descr(item.oids,
     oIndexPrimary, ...)`.
   - Looks up the current PK row on the PK tree (the row is still there
     even if `tuphdr->deleted` is set, because no vacuum has run yet).
   - For each SK in `descr->indices[1..]`, computes the old and new SK
     keys and applies DELETE/INSERT pairs through `o_btree_modify` with
     the same `recovery_*_overwrite_callback` as the worker path,
     making the operation idempotent against any later WAL records.

After all fix-ups, the leader flips `toast_consistent = true` and sends
`RecoveryMsgTypeToastConsistent` to each worker
(`src/recovery/recovery.c:4619`).

### 9.4 The race window

The window the boundary mechanism is designed to close:

1. Primary `T1` does PK INSERT, undo record written, PK leaf modified.
2. **Checkpointer crosses to SKs at this exact moment**, captures
   `toastConsistentPtr` = LSN_N, snapshots `T1.pendingSkUndoLoc`.
3. Primary `T1` does SK INSERT, undo record written, SK leaf modified.
   But the SK leaf write is at LSN_N+1, which is *past* the
   `toastConsistentPtr`, so the on-disk SK page snapshot misses it.
4. Crash. Primary restarts.
5. Recovery replays WAL from `controlReplayStartPtr`. The PK INSERT at
   LSN_N is applied (PK only, because `!toast_consistent`). The SK
   INSERT at LSN_N+1 *is* in WAL, but…
6. At LSN_N+1 the boundary trips: `apply_pending_sk_fixups()` runs first
   (using the snapshot from step 2), then the WAL stream resumes.

The fix-up at step 6 inserts the SK row using the undo-recorded PK key.
*Then* the WAL replay at step 6 inserts the same SK row again from the
WAL stream. Idempotence (`recovery_insert_overwrite_callback`) ensures
the second insert is a no-op.

### 9.5 The known failure mode (active investigation context)

The SK-leak under investigation does NOT appear to involve the boundary
mechanism (no crash near a checkpoint, no `PendingSkFixup` records
visible in failing trials). It instead reproduces with stress workload
that crashes via `orioledb-before-pre-commit-wal-finish`
(`src/recovery/wal.c:338`) — i.e. between PK undo write and
`WAL_REC_COMMIT` flush. The recovery path under suspicion is the one in
Section 7.3 (the rollback case for an in-flight oxid). The boundary
machinery here is documented for completeness; see Topic 4 for the WAL
sequence edge cases relevant to the active bug.

## 10. Recovery finish: cleanup, dropped tables, file cleanup

### 10.1 `o_recovery_start_hook` and `o_recovery_finish_hook`

The hooks are wired in `src/orioledb.c` near `_PG_init` via
`o_recovery_start_hook` / `o_recovery_finish_hook` pointers (declared at
`include/recovery/recovery.h:21,24`). They straddle the redo loop:

- `o_recovery_start_hook` (`src/recovery/recovery.c:1025`): runs once,
  *before* the first `orioledb_redo` call. Initialises
  `recovery_xid_state_hash`, `idxbuild_oids_hash`, spawns the worker pool
  if `recovery_pool_size_guc > 0`, reads the previous-checkpoint xids file
  via `read_xids()` (line 1042 calls `recovery_cleanup_old_files` first to
  drop unfinished checkpoint sidecars), and finally calls
  `apply_xids_branches()` to register the saved in-flight oxids.

- `o_recovery_finish_hook` (`src/recovery/recovery.c:1209`): runs once,
  after the *last* `orioledb_redo`. Sends `RecoveryMsgTypeFinished` to
  each worker, waits for them, calls `recovery_finish(-1)` (Section 7.5),
  and, when `cleanup && remove_old_checkpoint_files`, runs a second
  `recovery_cleanup_old_files(startup_chkp_num, false)` to drop the
  *previous* generation of per-tree files now that no recovery process
  can possibly reference them.

### 10.2 `recovery_cleanup_old_files`

At `src/recovery/recovery.c:3154`. Walks `orioledb_data/` (and every
tablespace's mirror) deleting files for checkpoint numbers earlier than
the parameter. Two modes:

- `before_recovery=true` (called at start): drop checkpoint sidecars (like
  `<chkpNum+1>.xid.tmp`) left half-written by a crash mid-checkpoint, and
  any per-tree file segments newer than `lastCheckpointNumber + 1` that
  could not possibly be referenced by the control file we're about to
  recover from.
- `before_recovery=false` (called at end): drop the *previous*
  checkpoint's per-tree files, now that the control file is durable.

### 10.3 Table drops, truncates, and replica index rebuilds

`WAL_REC_O_TABLES_META_UNLOCK` (line 3842) ends a meta-locked section.
Its replay path goes through `handle_o_tables_meta_unlock` which checks
whether the table's OIndexChunk was inserted or deleted in the meta-locked
section. If deleted, `add_undo_drop_relnode` (line 3968) defers the file
deletion until the undo log catches up; if inserted,
`add_undo_create_relnode` (line 3994) does the inverse. Either way the
*file deletion* doesn't happen synchronously inside redo; it's scheduled
through the same undo path that the runtime uses, so a roll-back during
or just after redo correctly undoes it.

`WAL_REC_TRUNCATE` (line 3864) handles `TRUNCATE TABLE` by calling
`o_truncate_table(oids, true)` after a `workers_synchronize` chokepoint
to drain pending modifications. The `true` second argument indicates
"during recovery" — `o_truncate_table` uses the standard runtime path
to drop and recreate per-tree state but skips WAL emission.

Index drops/creations seen mid-stream (e.g. `WAL_REC_O_TABLES_META_LOCK`
followed by `WAL_REC_O_TABLES_META_UNLOCK` and bracket modifications to
`SYS_TREES_O_INDICES`) trigger `rebuild_indices` /
`build_secondary_index` if the table's primary structure changed. The
leader may delegate the actual rebuild to the index-build pool via
`recovery_send_leader_oids` (`src/recovery/recovery.c:3551, 3590`,
declaration at `include/recovery/recovery.h:33`).

### 10.4 Promotion / "ready for new connections"

After `o_recovery_finish_hook` returns, the cluster is ready to accept
non-recovery connections. The flag mechanism for this is PG-native:
recovery completes, `recovery_target_*` or end-of-WAL is detected by
startup, startup signals postmaster, and orioledb's `iam_recovery` flag
goes false on the relevant processes. Backends that connect from this
point on enter the runtime path (`is_recovery_in_progress()` returns
false; see `include/recovery/recovery.h:41`).

## 11. ASCII diagrams

### 11.1 Redo call graph (parallel mode)

```
PG startup process
   |
   +-- xlog dispatch: rmgr==RM_ORIOLEDB_ID
       |
       v
   orioledb_redo(record)                 src/recovery/recovery.c:1106
       |
       +-- PITR check (orioledb.replay_until_lsn)
       |
       +-- if reach controlToastConsistentPtr:
       |       workers_synchronize(L, send_synchronize=true)
       |       |   for w in workers:
       |       |       send RecoveryMsgTypeSynchronize{ptr=L}
       |       |   spin until min(worker_ptrs[*].commitPtr) >= L
       |       apply_pending_sk_fixups()  src/recovery/recovery.c:600
       |       toast_consistent = true
       |       workers_notify_toast_consistent()
       |
       +-- if past controlReplayStartPtr:
               |
               v
           replay_container(start,end,single,readPtr,endPtr)
               |
               +-- wal_parse_container(WalReaderState{...})
                       |
                       v
                   replay_on_record(WalReaderState*, WalRecord*)  recovery.c:3677
                       |
                       +-- WAL_REC_XID:
                       |     advance_oxids() + recovery_switch_to_oxid(oxid,-1)
                       |
                       +-- WAL_REC_RELATION:
                       |     update ctx->descr / ctx->indexDescr
                       |
                       +-- WAL_REC_INSERT/UPDATE/DELETE:
                       |     if sys_tree_num > 0:
                       |         apply_sys_tree_modify_record(...) [leader]
                       |     if ctx->indexDescr != NULL:
                       |         if single:
                       |             apply_modify_record(descr, indexDescr, type, tuple)
                       |         else:
                       |             spread_idx_modify(&indexDescr->desc, type, tuple)
                       |                 hash = o_btree_hash(...)
                       |                 worker_send_modify(hash%pool, ...)
                       |
                       +-- WAL_REC_COMMIT / WAL_REC_ROLLBACK:
                       |     workers_send_oxid_finish(...)
                       |     if systree_modified: workers_synchronize(...)
                       |     recovery_finish_current_oxid(csn, ptr, -1, sync)
                       |
                       +-- WAL_REC_SAVEPOINT / WAL_REC_RB_TO_SP:
                       |     recovery_savepoint / recovery_rollback_to_savepoint
                       |     + workers_send_savepoint / workers_send_rollback_to_savepoint
                       |
                       +-- WAL_REC_O_TABLES_META_LOCK / UNLOCK:
                       |     leader-only; modifies system trees + invalidations
                       |
                       +-- WAL_REC_TRUNCATE / WAL_REC_BRIDGE_ERASE / etc.
```

Worker side:

```
recovery_worker_main(N)                  src/recovery/worker.c:199
    +-- shm_mq_attach(GET_WORKER_QUEUE(N))
    +-- recovery_queue_process(queue, N)
            loop:
                data = recovery_queue_read(queue, &size, N)
                for each message in data:
                    Insert/Update/Delete -> apply_modify_record(descr,indexDescr,type,tuple)
                                            -> if toast_consistent: apply_tbl_modify_record
                                                                      |
                                                                      +-- apply_tbl_insert/delete/update
                                                                          for i in descr->indices:
                                                                              o_btree_modify(&indices[i]->desc, ...)
                                               else: apply_btree_modify_record(&id->desc, ...)
                    Commit/Rollback -> recovery_finish_current_oxid(csn,ptr,N,false)
                                       update_worker_ptr(N, ptr)
                    Synchronize     -> update_worker_ptr(N, ptr)
                    ToastConsistent -> toast_consistent = true
                    Savepoint/RB    -> recovery_savepoint / recovery_rollback_to_savepoint
                    Finished        -> break
```

### 11.2 Page-LSN comparison ladder (the actual mechanism)

```
WAL record R at LSN L hits redo:

   |    +-- L < controlReplayStartPtr ---------------+
   |    |                                            |
   |    |   on-disk PK/SK pages already absorbed     |
   |    |   this via CoW write. Skip container.      |
   |    +--------------------------------------------+
   |
   |    +-- controlReplayStartPtr <= L                +
   |    |   < controlToastConsistentPtr,              |
   |    |   !toast_consistent                         |
   |    |                                             |
   |    |   Dispatch into replay_on_record:           |
   |    |   sys-tree:  applied if L >= sysTreesStartPtr
   |    |   PK record: apply_btree_modify_record(PK)  |
   |    |   SK record: apply_btree_modify_record(SK)  |
   |    |              one tree only (no fanout)      |
   |    +---------------------------------------------+
   |
   |    +-- At LSN_N == controlToastConsistentPtr      +
   |    |                                              |
   |    |   workers_synchronize(LSN_N, true)            |
   |    |   apply_pending_sk_fixups()                   |
   |    |      walk pending_sk_fixups_head:             |
   |    |          for each entry { oxid, undoLoc }:    |
   |    |              undo_read(...) -> recover undo   |
   |    |              fetch PK page, get current tuple |
   |    |              for each SK in descr:            |
   |    |                  o_btree_modify(SK, DELETE+INSERT)
   |    |   toast_consistent = true                     |
   |    |   workers_notify_toast_consistent()           |
   |    +-----------------------------------------------+
   |
   v    +-- L > controlToastConsistentPtr               +
        |   toast_consistent == true                    |
        |                                               |
        |   Dispatch into replay_on_record:             |
        |   PK record (or unified): apply_modify_record |
        |       -> apply_tbl_*: walk descr->indices,    |
        |          modify PK AND every SK with changed  |
        |          key.                                 |
        |                                               |
        |   Within o_btree_modify, recovery_*_callback  |
        |   inspects existing tuple's xactInfo/deleted: |
        |       if same oxid + same op: no-op           |
        |       else: apply                             |
        +-----------------------------------------------+
```

### 11.3 Consistent-point timeline

```
WAL time-axis: ----+-------+-------+-------+-------+----+-->
                   ^       ^       ^       ^
                   |       |       |       |
                   replayStartPtr  |       |
                   |               |       |
                   |        sysTreesStartPtr (= same in current code)
                   |               |
                   |        toastConsistentPtr
                   |               |
                                   |
                                   end-of-checkpoint (record durable)
                                   |
                                   redo_pos in PG control file


  Checkpointer phase:
     [   sys-tree image    ][  PK-tree image   ][  SK-tree image   ]
          |                       |                    |
          oTablesMetaLock         |                    |
          oSysTreesLock           |                    |
          held                   relased              relased
                                                       |
                                                  toastConsistentPtr
                                                  + PendingSkFixup snapshot

  Recovery dispatch from this control file:
     skip --------------------> replay PK only ---------> replay PK+SK
     (L < controlReplayStartPtr) (controlReplayStartPtr   (>= controlToastConsistentPtr)
                                  <= L
                                  < controlToastConsistentPtr)

     For sys-tree records:
     skip --------------------> applied
     (L < controlSysTreesStartPtr)  (>= controlSysTreesStartPtr)
```

Note that in practice `replayStartPtr == sysTreesStartPtr` whenever sys-tree
checkpointing finishes promptly: both are captured under the same
oTablesMetaLock/oSysTreesLock acquisition. The two fields exist for the
case where checkpoint sys-tree work is non-trivial and a record arrives
between them.

## 12. Open questions and pointers into the active investigation

This section anchors the SK-leak hypotheses (from
`feature/add_stress_bank_account_test`) onto the code surveyed above. None
of these are conclusions; they are the surface area to investigate.

### 12.1 The race window the harness is actually exercising

The crash injection point that reproduces the bug is
`orioledb-before-pre-commit-wal-finish` (`src/recovery/wal.c:336-340`).
This is *inside* `wal_commit`, *between* the `add_xid_wal_record_if_needed`
call and `add_finish_wal_record(WAL_REC_COMMIT, ...)` call. Concretely:

```
PRIMARY backend B1:
   ... earlier in transaction ...
   o_btree_modify(PK, INSERT, tuple) -> undo record, leaf tuple, PK row visible
                                        only as xactInfo.oxid=B1's.
   o_btree_modify(SK1, INSERT, ...)   -> similarly
   ... many more rows ...
   CommitTransaction:
       wal_commit:
           flush local WAL buffer  [PK INSERT, SK1 INSERT, ...]
                                   [...flushed to PG WAL stream...]
           add_xid_wal_record(B1.oxid, B1.logicalXid)  if needed
           --> START_CRIT_SECTION; INJECTION_POINT("orioledb-before-pre-commit-wal-finish"); ...
               PANIC here (error -> escalates to PANIC inside crit section).
           [WAL_REC_COMMIT is NEVER appended; never flushed.]
```

So the WAL stream contains the row records (PK and SK), an optional XID
record, but *no* COMMIT or ROLLBACK record for that oxid.

### 12.2 What recovery does with such an oxid

`recovery_switch_to_oxid` creates the `RecoveryXidState` entry on first
sighting (Section 7.1). Subsequent INSERT/UPDATE/DELETE records run
through `apply_modify_record`. The PK row is inserted; if past
`controlToastConsistentPtr`, the SK is also inserted via `apply_tbl_insert`
(Section 5.3).

When the WAL stream ends (or moves to a different oxid) without a
COMMIT or ROLLBACK, `recovery_finish` (`src/recovery/recovery.c:1620`) is
the catch-all. Its first inner loop (line 1641) iterates every entry in
the hash and, for any whose `csn == COMMITSEQNO_INPROGRESS`, runs:

```c
for each undo log type:
    set_cur_undo_locations(undoType, cur_state->undo_stacks[undoType]);
if flush_undo_pos:
    flush_current_undo_stack();
for each undo log type:
    apply_undo_stack(undoType, recovery_oxid, NULL, true);
walk_checkpoint_stacks(cur_state, COMMITSEQNO_ABORTED, ..., flush_undo_pos);
```

In other words: a never-committed in-flight oxid is *aborted* by walking
its undo log and inverse-applying every record. The SK INSERT we just
applied during redo should be inverted by an SK DELETE produced by the
undo walk — assuming the undo log is well-formed and the SK undo entry
is reachable from one of the four locations in `cur_state->undo_stacks`
(branch/subxact/onCommit/...).

The bug hypothesis is that, in some path through this code, the SK
undo entry **is not consumed** by `apply_undo_stack`, leaving an SK
row alive without a corresponding PK row.

### 12.3 Code surfaces that match the symptom

The harness's `sk_extra` set (SK rows whose PK key is absent in PK tree)
points at undo entries that didn't apply. Candidate sites where this can
happen:

1. **`apply_undo_stack` stops short.** If `cur_state->undo_stacks[i]` is
   incomplete — e.g. only the branch location is set, the subxact tail is
   not — `apply_undo_stack` will not reach the SK undo item. Check
   `set_cur_undo_locations` paths in `src/transam/undo.c:2023` for any
   case where a stack is restored with `InvalidUndoLocation` even though
   the records actually were emitted.

2. **`recovery_finish` vs `recovery_finish_current_oxid` divergence.**
   The "leaf cleanup" loop in `recovery_finish` (line 1641-1672) does
   `set_cur_undo_locations` from `cur_state->undo_stacks` then
   `apply_undo_stack`. The normal commit/abort path
   (`recovery_finish_current_oxid`) takes the same steps. If
   `recovery_switch_to_oxid` was called for this oxid but
   `recovery_finish_current_oxid` was *not* (because the WAL stream ran
   out), the cleanup loop is the only safety net. Cross-check whether
   `cur_state->undo_stacks` was kept in sync as records were applied —
   it's only stored on `recovery_switch_to_oxid`'s save path (line
   1730-1741), which runs **only when switching away** from the oxid,
   not after each modify.

3. **Inter-oxid races in parallel recovery.** When records are sharded
   to workers, the worker stores its *own* `cur_recovery_xid_state`
   (per-worker hash). The leader's hash entry for the same oxid is
   distinct and is updated only when the leader itself touches the
   record. The undo stack the leader sees might lag behind the
   worker's. `workers_synchronize` aligns workers' progress against an
   LSN, but does NOT export the workers' undo-stack tail to the leader.
   On end-of-WAL with no COMMIT, the leader's `recovery_finish`
   cleanup uses the leader's hash entry — which may have a partial
   undo stack — to call `apply_undo_stack`. The workers' hash entries
   are torn down separately in `recovery_finish` per-worker.

4. **`apply_undo_stack` with `toLocation=NULL`** (line 1659 of
   `recovery.c`) walks the whole stack tail. If, however, the stack is
   anchored at `cur_undo_locations` which itself was reset by an
   earlier `reset_cur_undo_locations` (line 2031 of undo.c, or any of
   the per-record `reset_cur_undo_locations` calls at 2147, 2386), the
   walk silently terminates immediately. The current branch's
   instrumentation (the `csn-trace` markers) is designed to expose
   exactly this scenario.

### 12.4 Relevant `csn-trace` / `wal-trace` markers

The active investigation has surrounded several call sites with
elog(LOG,…) markers (intentionally — these are timing scaffolding,
not just diagnostics). The most relevant for this topic:

- `src/btree/page_state.c` per-op `lock_page request/got/wait/release`
  — broadens the SK race window so the bug repro rate climbs from ~33%
  to ~75%. Removing these silently changes the timing.
- `src/transam/undo.c` `commit-assert-trace pre/post` around line 2400
  — captures the COMMIT path's pre-flip / post-flip state.
- `src/recovery/wal.c` `wal-trace modify` (currently `#if 0` gated, line
  140-148) — when enabled, drops bug rate to ~0%. Useful to confirm a
  candidate fix actually narrows the window vs. masks it.

### 12.5 What this topic does NOT cover

- The exact undo-log layout, which is in Topic 4's territory (WAL +
  undo sequence edge cases).
- The runtime PK->SK ordering on the primary; this is in Topic 2's
  PG <-> orioledb interface coverage (the `pendingSkUndoLoc` mechanism).
- The actual fix candidates; those follow from instrumenting the
  hypotheses above.
