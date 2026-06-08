# Q3 — What data structure stores undo records for a transaction?

## Short summary

OrioleDB's undo records are **not** a per-transaction allocated list. They live in a
**global shared-memory circular (ring) buffer** — one ring per *undo log type*. A
transaction's undo records are an **intrusive singly-linked stack ("chain") threaded
through that ring buffer**: each undo record begins with an `UndoStackItem` header whose
`prev` field is the `UndoLocation` (a `uint64` byte offset into the ring) of the previous
record this transaction wrote. The current head of that chain is *not* stored in the
transaction itself but in **shared per-process state** (`ODBProcData.undoStackLocations[...]`),
indexed by proc number and autonomous-nesting level. Pushing a record (`add_new_undo_stack_item`)
reads the old head into the new record's `prev` and writes the new record's location as the
new head — a classic LIFO push.

There are actually **two interleaved chains per transaction**, threaded through the same
records:

1. The **full abort stack** — every record, linked by `UndoStackItem.prev`. Walked on
   ABORT (and subxact rollback) to undo *everything*.
2. The **on-commit stack** — only records whose item-type descriptor has `callOnCommit`,
   linked by `OnCommitUndoStackItem.onCommitLocation` (a separate, sparser chain). Walked
   on PRE-COMMIT / COMMIT to run deferred commit-time actions (e.g. dropping obsolete files).

And there are **three independent undo logs** (`UndoLogType`), each with its own ring
buffer, its own `UndoMeta`, and its own head pointers per process:
`UndoLogRegular` (row-level user data), `UndoLogRegularPageLevel` (page-level user data),
`UndoLogSystem` (system trees). See `include/orioledb.h:203-224`.

---

## 1. The record header — the per-record link (`UndoStackItem`)

Every undo record starts with this header (`include/transam/undo.h:255-261`):

```c
struct UndoStackItem
{
    UndoLocation prev;        /* offset of the previous record in MY chain */
    LocationIndex itemSize;
    uint8        type;        /* UndoItemType — selects the descriptor/callback */
    uint8        indexType;
};
```

`UndoLocation` is just a `uint64` byte offset into the ring buffer
(`include/orioledb.h:144`, with `InvalidUndoLocation`/value-mask sentinels at 145-149).
`prev` is what makes the per-transaction undo a **linked-list / stack**.

Records that need a commit-time action embed the header plus a *second* link
(`include/transam/undo.h:263-267`):

```c
typedef struct
{
    UndoStackItem base;
    UndoLocation  onCommitLocation;   /* offset of previous callOnCommit record */
} OnCommitUndoStackItem;
```

Two more specialized records reuse the header for sub-chains:
`BranchUndoStackItem` (`undo.h:296-301`, remembers an already-aborted "long path" so a
checkpointed item can still find it) and `SubXactUndoStackItem`
(`undo.h:306-311`, a savepoint marker carrying `prevSubLocation`).

---

## 2. Where the per-transaction head/tail lives — `undoStackLocations` (shared per-proc)

The chain *heads* are NOT in a transaction object; they live in shared memory, in the
owning backend's `ODBProcData` (`include/orioledb.h:250-270`):

```c
typedef struct
{
    pg_atomic_uint64 location;        /* head of the FULL (abort) stack       */
    pg_atomic_uint64 branchLocation;  /* head of the branch chain             */
    pg_atomic_uint64 subxactLocation; /* head of the subxact savepoint chain  */
    pg_atomic_uint64 onCommitLocation;/* head of the on-commit stack          */
} UndoStackSharedLocations;            /* include/orioledb.h:235-241 */

/* inside ODBProcData: */
UndoStackSharedLocations undoStackLocations[PROC_XID_ARRAY_SIZE][(int) UndoLogsCount];
```

So the head pointers are indexed **[autonomousNestingLevel][undoType]**.
`GET_CUR_UNDO_STACK_LOCATIONS()` resolves the "current" slot
(`include/transam/undo.h:360-362`):

```c
&oProcData[MYPROCNUMBER].undoStackLocations
        [oProcData[MYPROCNUMBER].autonomousNestingLevel][(int) undoType]
```

The non-atomic snapshot of those four heads is `UndoStackLocations`
(`include/transam/undo.h:269-275`) — used as a value type when saving/restoring or
passing a "stop at this point" target (`get/set_cur_undo_locations`,
`read/write_shared_undo_locations` at `src/transam/undo.c:1999-2033`).

`cur_undo_locations` from CLAUDE.md is the conceptual name for this current per-process
slot; in the code the accessors are `get_cur_undo_locations()` /
`set_cur_undo_locations()` (`src/transam/undo.c:2019-2033`) and the macro
`GET_CUR_UNDO_STACK_LOCATIONS`.

---

## 3. Pushing a record — the LIFO stack push (`add_new_undo_stack_item`)

`src/transam/undo.c:1922-1941`:

```c
void
add_new_undo_stack_item(UndoLogType undoType, UndoLocation location)
{
    UndoStackItem *item = (UndoStackItem *) GET_UNDO_REC(undoType, location);
    UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);
    UndoItemTypeDescr *descr = item_type_get_descr(item->type);

    item->prev = pg_atomic_read_u64(&sharedLocations->location);   /* old head -> prev */
    pg_atomic_write_u64(&sharedLocations->location, location);     /* new head         */

    if (descr->callOnCommit)                                       /* second chain */
    {
        OnCommitUndoStackItem *fItem = (OnCommitUndoStackItem *) item;
        fItem->onCommitLocation = pg_atomic_read_u64(&sharedLocations->onCommitLocation);
        pg_atomic_write_u64(&sharedLocations->onCommitLocation, location);
    }
    release_reserved_undo_location(undoType);
}
```

`add_new_undo_stack_item_to_process()` (`undo.c:1962-1982`) does the same push but into
*another* backend's slot (group-insert optimization); it asserts `!callOnCommit`.

`GET_UNDO_REC` (`src/transam/undo.c:61-62`) is what turns a `UndoLocation` into a real
pointer — and the modulo is the proof the buffer is circular:

```c
#define GET_UNDO_REC(undoType, loc) (o_undo_buffers[(int) (undoType)] + \
    (loc) % o_undo_circular_sizes[(int) (undoType)])
```

---

## 4. The global ring buffer + its meta (`UndoMeta`)

The backing storage is per-type contiguous shared memory laid out in `undo_shmem_init`
(`src/transam/undo.c:329-349`): first an array of `UndoMeta` (one per undo log type),
then `PendingTruncatesMeta`, then the three ring buffers
`o_undo_buffers[UndoLogRegular | UndoLogRegularPageLevel | UndoLogSystem]`. Sizes are in
`o_undo_circular_sizes[]` (`undo.c:184-187`, computed in `undo.c:305-316`).

`UndoMeta` (`include/transam/undo.h:17-230`) tracks the monotonically-increasing
locations that bound the ring; logical offsets keep growing forever and wrap via modulo:

- `lastUsedLocation` (76) — boundary between reserved and free area; advanced by
  `get_undo_record()`. Header comment (45-75) describes the reserve algorithm.
- `advanceReservedLocation` (95) — pre-reservation high-water mark
  (`reserve_undo_size_extended`).
- `writeInProgressLocation` / `writtenLocation` (113-114) — eviction-to-disk interval.
- `minProcRetainLocation`, `minProcTransactionRetainLocation`, `minRewindRetainLocation`
  (143-145) — oldest offsets that snapshots / in-flight rollbacks / rewind still need.
- `minProcReservedLocation` (156), `checkpointRetainStart/EndLocation` (163-164),
  `cleanedLocation` + cleaned checkpoint range (180-182).
- Concurrency control: `minUndoLocationsMutex` (191), change counters (200, 210, 218),
  `undoWriteLock` (227).

When the ring fills, records older than the retain locations are **evicted to disk files**
(`evict_undo_to_disk`, the `circularBuffer`/`circularBufferSize` wrap logic at
`undo.c:1556-1650`); file templates are `...row`/`...page`/`...system`
(`include/transam/undo.h:351-354`).

The selector `get_undo_meta_by_type()` / `o_undo_buffers[]` give the per-type meta + ring.

---

## 5. Per-transaction retain head and how it's set at txn start

The transaction's *tail* (oldest record, the point rollback must reach) is tracked as
`transactionUndoRetainLocation`, in shared per-proc, per-type state
(`UndoRetainSharedLocations`, `include/orioledb.h:243-248`).

It is established lazily on the **first** undo record of the transaction in
`set_my_reserved_location()` (`src/transam/undo.c:929-960`): if
`transactionUndoRetainLocation` is currently invalid, it is overwritten with the current
`lastUsedLocation`, and an assertion checks the undo chain head
(`undoStackLocations[...].location`) is empty at that moment (`undo.c:958-959`). That is
effectively "transaction-start" initialization of the undo tail.

---

## 6. Walking the chains — abort vs commit (the separation)

The dual-chain design shows up directly in the walker `walk_undo_range`
(`src/transam/undo.c:1161-1230`). The same loop follows a *different* link depending on
stage:

```c
if (stage != OUndoCallbackStageAbort)
    location = fItem->onCommitLocation;   /* commit: sparse on-commit chain */
else
    location = item->prev;                /* abort: full prev chain         */
```

Entry points (`src/transam/undo.c:1413-1435`):

- `apply_undo_stack()` → `walk_undo_stack(..., abortTrx=true)` — ABORT / subxact rollback.
  Starts at `sharedLocations->location` (full-stack head), follows `prev`, stops at the
  optional `toLocation->location` target (`undo.c:1362-1366`), then can leave a
  `BranchUndoStackItem` so checkpointed items still reach already-aborted records
  (`undo.c:1371-1372, 1384-1385`).
- `precommit_undo_stack()` — PRE-COMMIT; starts at `onCommitLocation`, stage
  `OUndoCallbackStagePreCommit` (`undo.c:1419-1429`).
- `on_commit_undo_stack()` → `walk_undo_stack(..., abortTrx=false)` — COMMIT; asserts no
  `toLocation`, starts at `onCommitLocation` (`undo.c:1327-1334, 1431-1435`).

After walking, the heads in the shared slot are rewritten under
`undoStackLocationsFlushLock` (`undo.c:1379-1405`): `location` and `onCommitLocation` are
updated, and `branchLocation` is set when a branch item was created.

---

## 7. Reset between transactions

`reset_cur_undo_locations()` (`src/transam/undo.c:2035-2043`) clears all four head
pointers (`location`, `branchLocation`, `subxactLocation`, `onCommitLocation`) to
`InvalidUndoLocation` for **all** undo log types, by writing a zeroed `UndoStackLocations`
via `set_cur_undo_locations()`. It is invoked from the xact-callback teardown path
(`src/transam/undo.c:2152`). The retain tail is separately cleared via
`reset_cur_retained_location` / `transactionUndoRetainLocation := InvalidUndoLocation`
(`undo.c:1470-1487`), so the next `set_my_reserved_location()` re-initializes it.

---

## Chaining diagram

```
ODBProcData[myproc].undoStackLocations[nestLevel][UndoLogRegular]
   .location  ──────────────► head of FULL (abort) chain
   .onCommitLocation ───────► head of ON-COMMIT chain (subset)

 Global ring buffer  o_undo_buffers[UndoLogRegular]  (modulo o_undo_circular_sizes[])
 logical offset grows ───────────────────────────────────────────────────────►
   [ rec A ]        [ rec B (callOnCommit) ]   [ rec C ]      [ rec D (callOnCommit) ]
   prev=Invalid     prev=A                      prev=B         prev=C        ◄── .location head
                    onCommit=Invalid                           onCommit=B    ◄── .onCommitLocation head

 ABORT  walk: D ─prev→ C ─prev→ B ─prev→ A ─prev→ Invalid    (every record)
 COMMIT walk: D ─onCommit→ B ─onCommit→ Invalid              (only callOnCommit records)
```

Three such ring+head sets exist in parallel — one per `UndoLogType`
(`UndoLogRegular`, `UndoLogRegularPageLevel`, `UndoLogSystem`).

---

## Code references

- `include/orioledb.h:144-149` — `UndoLocation` = `uint64` ring offset + sentinels.
- `include/orioledb.h:203-227` — `UndoLogType` enum (3 logs) + `GET_PAGE_LEVEL_UNDO_TYPE`.
- `include/orioledb.h:235-241` — `UndoStackSharedLocations` (the 4 per-proc head pointers).
- `include/orioledb.h:243-248` — `UndoRetainSharedLocations` (incl. `transactionUndoRetainLocation`).
- `include/orioledb.h:250-270` — `ODBProcData` holds `undoStackLocations[PROC_XID_ARRAY_SIZE][UndoLogsCount]`.
- `include/transam/undo.h:17-230` — `UndoMeta` ring-buffer bookkeeping (lastUsed/written/retain/checkpoint).
- `include/transam/undo.h:239-261` — `UndoItemType` enum + `UndoStackItem` header (`prev` link).
- `include/transam/undo.h:263-267` — `OnCommitUndoStackItem` (`onCommitLocation` second link).
- `include/transam/undo.h:269-275` — `UndoStackLocations` (value snapshot of the 4 heads).
- `include/transam/undo.h:296-311` — `BranchUndoStackItem`, `SubXactUndoStackItem`.
- `include/transam/undo.h:313-318` — `UndoStackKind` (Full / Head / Tail).
- `include/transam/undo.h:360-362` — `GET_CUR_UNDO_STACK_LOCATIONS` macro (indexes by nesting+type).
- `src/transam/undo.c:61-62` — `GET_UNDO_REC` = buffer base + `loc % circular_size` (the ring).
- `src/transam/undo.c:180-187, 305-316` — `o_undo_buffers[]` / `o_undo_circular_sizes[]`.
- `src/transam/undo.c:329-349` — `undo_shmem_init` lays out metas + 3 ring buffers in shmem.
- `src/transam/undo.c:929-960` — `set_my_reserved_location` sets `transactionUndoRetainLocation` (txn-start tail init).
- `src/transam/undo.c:1161-1230` — `walk_undo_range`: abort follows `prev`, commit follows `onCommitLocation`.
- `src/transam/undo.c:1298-1409` — `walk_undo_stack`: abort vs commit start head, branch item, head rewrite under flush lock.
- `src/transam/undo.c:1413-1435` — `apply_undo_stack` / `precommit_undo_stack` / `on_commit_undo_stack`.
- `src/transam/undo.c:1470-1487` — clearing `transactionUndoRetainLocation`.
- `src/transam/undo.c:1556-1650` — `evict_undo_to_disk` circular-buffer wrap handling.
- `src/transam/undo.c:1922-1941` — `add_new_undo_stack_item` (LIFO push onto both chains).
- `src/transam/undo.c:1962-1982` — `add_new_undo_stack_item_to_process` (push into another backend's slot).
- `src/transam/undo.c:1999-2033` — `read/write_shared_undo_locations`, `get/set_cur_undo_locations`.
- `src/transam/undo.c:2035-2043` — `reset_cur_undo_locations` (clears all 4 heads, all types).
- `src/transam/undo.c:2152` — reset called from xact teardown.
