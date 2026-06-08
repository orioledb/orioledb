# Q2 — How does an undo record look in OrioleDB?

## Summary

An OrioleDB undo record is a variable-length, MAXALIGN'd blob written into one of
**three per-type in-memory circular undo buffers** (row / page-image / system).
Every record begins with a common 16-byte-ish header, `UndoStackItem`
(`include/transam/undo.h:255`), carrying a `prev` back-pointer (the intra-transaction
undo chain), an `itemSize`, a 1-byte `type` tag (`UndoItemType`,
`include/transam/undo.h:241`), and a 1-byte `indexType`. The `type` tag selects one of
ten concrete "stack item" structs that embed `UndoStackItem` as their first member, and
also selects a callback + an `callOnCommit` flag via a static dispatch table
`undoItemTypeDescrs[]` (`src/transam/undo.c:123`). The most important kind for user data is
`BTreeModifyUndoStackItem` (`include/btree/undo.h:55`), which carries the affected B-tree's
OIDs, the page block number / change count, a copy of the *previous* leaf tuple header
(`BTreeLeafTuphdr`, for chaining the row's version history), and a trailing tuple/key image.
A separate family of records (`UndoPageImageHeader`, `include/btree/undo.h:37`) stores
whole 8KB page images for split/merge/compaction. Records are reserved in advance
(`reserve_undo_size_extended`, `src/transam/undo.c:1658`), then carved out and bumped into the
circular buffer (`get_undo_record`, `src/transam/undo.c:1840`), then linked into the
transaction's per-process undo stack (`add_new_undo_stack_item`, `src/transam/undo.c:1922`).

## The three undo logs (UndoLogType)

`include/orioledb.h:203-227`. Each is a distinct circular buffer with its own
`UndoMeta`, its own on-disk eviction file template, and its own reserved-size counter.

| Enum | Value | Purpose | File template |
|---|---|---|---|
| `UndoLogNone` | -1 | invalid sentinel | — |
| `UndoLogRegular` | 0 | row-level records for user-data modifications (INSERT/UPDATE/DELETE/lock) | `...%02X%08Xrow` |
| `UndoLogRegularPageLevel` | 1 | page-level images for user-data trees (split/merge/compact) | `...%02X%08Xpage` |
| `UndoLogSystem` | 2 | modifications of system trees + catalog/invalidate items | `...%02X%08Xsystem` |
| `UndoLogsCount` | 3 | array bound | — |

`GET_PAGE_LEVEL_UNDO_TYPE()` (`include/orioledb.h:226`) maps a regular row tree to its
page-level log; page images therefore land in log 1 while row records land in log 0.
File templates are in the `undoBuffersDesc.filenameTemplate` array
(`src/transam/undo.c:204`) and the macros at `include/transam/undo.h:351-353`.

## Common header — `UndoStackItem`

`include/transam/undo.h:255-261`

| Field | Type | Meaning |
|---|---|---|
| `prev` | `UndoLocation` (uint64) | back-pointer to the previous undo record in this transaction's chain (the undo stack). Set by `add_new_undo_stack_item`. |
| `itemSize` | `LocationIndex` | unpadded logical size of this record |
| `type` | `uint8` | the `UndoItemType` tag (see below) — selects struct + callback |
| `indexType` | `uint8` | for B-tree records, the `OIndexType` (primary/unique/regular/toast/bridge) of the affected tree |

`UndoLocation` is a `uint64` (`include/orioledb.h:144`); high bit `0x2000000000000000`
is the "invalid" flag (`UndoLocationIsValid`). Locations are *logical*, taken modulo the
circular buffer size by `GET_UNDO_REC` (`src/transam/undo.c:61`).

### `OnCommitUndoStackItem` — header variant for on-commit items

`include/transam/undo.h:263-267`. Embeds `UndoStackItem base` plus an extra
`onCommitLocation` back-pointer. Records whose `callOnCommit` flag is true are *also*
threaded onto a second, commit-only chain (`onCommitLocation`), so the commit path can walk
just the on-commit items while abort walks the full `prev` chain
(`walk_undo_range`, `src/transam/undo.c:1202-1218`).

## The record types (`UndoItemType`) and their struct bodies

Enum: `include/transam/undo.h:241-253`. Dispatch table (callback + callOnCommit):
`src/transam/undo.c:123-174`.

| `UndoItemType` | Struct (defining file:line) | Header base | callOnCommit | callback | Role |
|---|---|---|---|---|---|
| `ModifyUndoItemType` (1) | `BTreeModifyUndoStackItem` (`include/btree/undo.h:55`) | `UndoStackItem` | false | `modify_undo_callback` | INSERT/UPDATE/DELETE of a row in a B-tree |
| `RowLockUndoItemType` (2) | `BTreeModifyUndoStackItem` (reused; `action==BTreeOperationLock`) | `UndoStackItem` | false | `lock_undo_callback` | row-level lock (lock-only record) |
| `RelnodeUndoItemType` (3) | `RelnodeUndoStackItem` (`include/btree/undo.h:65`) | `OnCommitUndoStackItem` | true | `btree_relnode_undo_callback` | create/drop/truncate relfilenode (file cleanup on commit/abort) |
| `SysTreesLockUndoItemType` (4) | `SysTreesLockUndoStackItem` (`src/checkpoint/checkpoint.c:152`) | `UndoStackItem` | false | `systrees_lock_callback` | lock/unlock of system trees during checkpoint |
| `InvalidateUndoItemType` (5) | `InvalidateUndoStackItem` (`include/tableam/descr.h:294`) | `OnCommitUndoStackItem` | true | `o_invalidate_undo_item_callback` | relcache invalidation of an OrioleDB table |
| `BranchUndoItemType` (6) | `BranchUndoStackItem` (`include/transam/undo.h:296`) | `UndoStackItem` | false | `o_stub_item_callback` | remembers the "long" undo path after a partial rollback (savepoint) |
| `SubXactUndoItemType` (7) | `SubXactUndoStackItem` (`include/transam/undo.h:306`) | `UndoStackItem` | false | `o_stub_item_callback` | savepoint marker (undo location for future subxact rollback) |
| `RewindRelFileNodeUndoItemType` (8) | `RewindRelFileNodeUndoStackItem` (`src/transam/undo.c:115`) | `OnCommitUndoStackItem` | true | `o_rewind_relfilenode_item_callback` | relfilenode list for the point-in-time rewind feature |
| `SysCacheDeleteUndoItemType` (9) | `SysCacheDeleteUndoStackItem` (`src/catalog/o_sys_cache.c:959`) | `UndoStackItem` | false | `o_sys_cache_delete_callback` | undo a delete in an OrioleDB sys-cache tree |
| `InvalidateComparatorUndoItemType` (10) | `InvalidateComparatorUndoStackItem` (`src/tableam/descr.c:53`) | `OnCommitUndoStackItem` | true | `o_invalidate_comparator_callback` | invalidate cached comparator for an opfamily/type pair |

Note `RowLockUndoItemType` does **not** have its own struct; `make_undo_record` writes a
`BTreeModifyUndoStackItem` and only switches the `type` tag to `RowLockUndoItemType` when
`action == BTreeOperationLock` (`src/btree/undo.c:329-332`).

### Row-modify record — `BTreeModifyUndoStackItem`

`include/btree/undo.h:55-63`. This is the row-level INSERT/UPDATE/DELETE/lock record.

| Field | Type | Meaning |
|---|---|---|
| `header` | `UndoStackItem` | common header; `type` = Modify or RowLock, `indexType` = tree's `OIndexType` |
| `action` | `BTreeOperationType` | Insert / Update / Delete / Lock — what was done |
| `oids` | `ORelOids` | (datoid, reloid, relnode) identifying the affected B-tree |
| `blkno` | `OInMemoryBlkno` | in-memory page block the row lived on |
| `pageChangeCount` | `uint32` | page change-count guard (detects page reuse) |
| `tuphdr` | `BTreeLeafTuphdr` | a **copy of the previous leaf tuple header** — this is the chaining link to the row's prior version (its `undoLocation` points further back) |
| *(trailing)* | tuple or key bytes | for UPDATE: full prior tuple image; for INSERT/DELETE: just the key. Appended right after the struct: `sizeof(BTreeModifyUndoStackItem) + tuplelen` (`src/btree/undo.c:324`) |

`BTreeLeafTuphdr` (`include/btree/page_contents.h:278-285`) is itself the per-row MVCC
header carried both in-page and in undo: bitfields `xactInfo:61, deleted:2,
chainHasLocks:1` and `undoLocation:62, formatFlags:2`. Copying it into the undo record is
how the version chain is preserved (`src/btree/undo.c:359-365`). On abort,
`modify_undo_callback` restores this prior header into the page; on commit the row-level
records need no action (`callOnCommit = false`).

### Page-image records — `UndoPageImageHeader`

`include/btree/undo.h:22-42`. These live in the **page-level** undo log
(`UndoLogRegularPageLevel`), not as `UndoItemType` stack items. A `UndoPageImageType`
(`UndoPageImageCompact` / `UndoPageImageSplit` / `UndoPageImageMerge`) header is followed
by one or two full `ORIOLEDB_BLCKSZ` page images (plus an optional split key). Written by
`page_add_image_to_undo` (`src/btree/undo.c:50`) and `make_merge_undo_image`. Size macros
at `include/btree/undo.h:79-93` (`O_COMPACT_UNDO_IMAGE_SIZE`, `O_SPLIT_UNDO_IMAGE_SIZE`,
`O_MERGE_UNDO_IMAGE_SIZE`, `O_MAX_UNDO_RECORD_SIZE`). These let concurrent readers
reconstruct the pre-split/merge page via `get_page_from_undo`.

### Branch / Subxact bookkeeping records

- `BranchUndoStackItem` (`include/transam/undo.h:296-301`): `header` + `longPathLocation`
  + `prevBranchLocation` — after a rollback-to-savepoint applies part of the undo, the
  still-needed "long" path is memorized here.
- `SubXactUndoStackItem` (`include/transam/undo.h:306-311`): `header` + `prevSubLocation`
  + `parentSubid` — a savepoint marker recording where to roll back to.

## Allocation / write path

1. **Reserve** (well ahead of use): `reserve_undo_size_extended(undoType, size,
   waitForUndoLocation)` (`src/transam/undo.c:1658`) bumps the shared
   `meta->advanceReservedLocation` and the per-process `reserved_undo_sizes[undoType]`
   counter, triggering eviction to disk if the circular buffer would overflow. Wrapper
   `reserve_undo_size()` (`include/transam/undo.h:448`) always waits;
   `O_MODIFY_UNDO_RESERVE_SIZE` (`include/btree/undo.h:87`) is what a row modify reserves
   (room for two split images + two update records).
2. **Carve out the bytes**: `get_undo_record(undoType, &undoLocation, MAXALIGN(size))`
   (`src/transam/undo.c:1840`) first pins the process's reserved location
   (`set_my_reserved_location`, `src/transam/undo.c:929`), then atomically
   `pg_atomic_fetch_add_u64(&meta->lastUsedLocation, size)`, decrements the reserved
   counter, and returns `GET_UNDO_REC(undoType, location)` — a raw pointer into the
   circular buffer. It retries if the allocation would straddle the buffer wrap boundary
   (it reserved 2× for exactly this). `get_undo_record_unreserved`
   (`src/transam/undo.c:1879`) is the reserve-then-get convenience used by the catalog/system
   records.
3. **Fill the struct** directly through the returned pointer: e.g. `make_undo_record`
   (`src/btree/undo.c:302`) sets `itemSize`, `type`, `indexType`, `action`, `oids`, `blkno`,
   `pageChangeCount`, copies the prior `tuphdr`, and `memcpy`s the tuple/key after the
   struct.
4. **Link into the transaction's undo stack**: `add_new_undo_stack_item(undoType,
   location)` (`src/transam/undo.c:1922`) sets `item->prev` to the current head and updates
   the per-process `UndoStackSharedLocations->location`; if the type's `callOnCommit` is
   true it *also* threads `onCommitLocation`. Then it releases the reserved location.
   `add_new_undo_stack_item_to_process` (`src/transam/undo.c:1962`) does the same on behalf of
   a waiter (group-insert optimization).

The per-process undo head/branch/subxact/onCommit locations live in
`UndoStackSharedLocations` (`include/orioledb.h:235-241`) inside `oProcData[...]`;
`UndoStackLocations` (`include/transam/undo.h:269-275`) is the non-atomic snapshot form.

## Abort vs commit walk

`walk_undo_range` (`src/transam/undo.c:1161`) iterates a transaction's records and calls
`descr->callback(...)` with an `OUndoCallbackStage` (`include/transam/undo.h:340-345`:
`Abort` / `PreCommit` / `Commit`). On **abort** it follows the full `prev` chain (every
record, undoing row modifications via `modify_undo_callback`). On **commit/precommit** it
follows only the `onCommitLocation` chain (just the `callOnCommit` records — file cleanup,
cache invalidation, rewind). Row-modify and lock records have `callOnCommit = false`, so
they do nothing at commit; their in-memory header is simply retained until the undo
location is no longer needed and can be cleaned.

## Code references

- `include/transam/undo.h:241` — `UndoItemType` enum (10 kinds)
- `include/transam/undo.h:255` — `UndoStackItem` common header (`prev`, `itemSize`, `type`, `indexType`)
- `include/transam/undo.h:263` — `OnCommitUndoStackItem` (adds `onCommitLocation`)
- `include/transam/undo.h:269` — `UndoStackLocations` snapshot
- `include/transam/undo.h:296` / `:306` — `BranchUndoStackItem` / `SubXactUndoStackItem`
- `include/transam/undo.h:313` — `UndoStackKind` enum; `:340` — `OUndoCallbackStage`
- `include/transam/undo.h:17` — `UndoMeta` (per-log circular-buffer metadata)
- `include/transam/undo.h:351-354` — undo file name templates / `UNDO_FILE_SIZE`
- `include/orioledb.h:144` — `UndoLocation` typedef + valid/invalid masks
- `include/orioledb.h:203` — `UndoLogType` (Regular / RegularPageLevel / System)
- `include/orioledb.h:226` — `GET_PAGE_LEVEL_UNDO_TYPE`
- `include/orioledb.h:235` — `UndoStackSharedLocations` (per-proc atomic heads)
- `include/btree/undo.h:22` — `UndoPageImageType`; `:37` — `UndoPageImageHeader`
- `include/btree/undo.h:55` — `BTreeModifyUndoStackItem` (row INSERT/UPDATE/DELETE/lock)
- `include/btree/undo.h:65` — `RelnodeUndoStackItem`
- `include/btree/undo.h:79-93` — page-image size macros, `O_MAX_UNDO_RECORD_SIZE`
- `include/btree/page_contents.h:278` — `BTreeLeafTuphdr` (MVCC per-row header carried in undo)
- `include/tableam/descr.h:294` — `InvalidateUndoStackItem`
- `src/transam/undo.c:61` — `GET_UNDO_REC` circular-buffer addressing macro
- `src/transam/undo.c:108` — `UndoItemTypeDescr`; `:123` — `undoItemTypeDescrs[]` dispatch table
- `src/transam/undo.c:115` — `RewindRelFileNodeUndoStackItem`
- `src/transam/undo.c:386` — `item_type_get_descr`
- `src/transam/undo.c:204` — `undoBuffersDesc.filenameTemplate` (per-log files)
- `src/transam/undo.c:1161` — `walk_undo_range` (abort vs commit dispatch)
- `src/transam/undo.c:1658` — `reserve_undo_size_extended`
- `src/transam/undo.c:1840` — `get_undo_record`; `:1879` — `get_undo_record_unreserved`
- `src/transam/undo.c:1922` — `add_new_undo_stack_item`; `:1962` — `..._to_process`
- `src/btree/undo.c:50` — `page_add_image_to_undo` (page-image record write)
- `src/btree/undo.c:302` — `make_undo_record` (row record write + layout)
- `src/btree/undo.c:394` — `make_waiter_undo_record`
- `src/checkpoint/checkpoint.c:152` — `SysTreesLockUndoStackItem`
- `src/catalog/o_sys_cache.c:959` — `SysCacheDeleteUndoStackItem`
- `src/tableam/descr.c:53` — `InvalidateComparatorUndoStackItem`
