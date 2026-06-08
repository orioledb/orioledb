# Recovery & undo — Q&A digest

A flat list of clarifying questions asked during the SK-leak / Bug #2
investigation and the answers reached, with code references. Each Q&A
points at the corresponding section in [`recovery_investigation.md`](./recovery_investigation.md)
when one exists, so this file works as both a glossary and an index.

Companion docs:

- [`recovery_investigation.md`](./recovery_investigation.md) — full
  multi-topic recovery deep-dive
- [`tx_flow.md`](./tx_flow.md) — primary-side commit/abort flow
- [`streaming_replica_issue.md`](./streaming_replica_issue.md) — Bug #1
  + Bug #2 issue tracker

---

## Q1. The undo back-pointer is not in WAL — how is `BTreeLeafTuphdr.undoLocation` filled during redo?

**See also:** `recovery_investigation.md` §3.2 (modify-record contents) and §7 (redo undo state).

**Short answer.** `BTreeLeafTuphdr.undoLocation` is **re-derived locally** on
the redo side by re-running `o_btree_modify()` — the same function the
runtime path uses for an INSERT/UPDATE/DELETE.

**Why WAL omits it.** A `WAL_REC_INSERT/UPDATE/DELETE/REINSERT` record
carries the tuple bytes and the target tree (via the most-recent
`WAL_REC_RELATION`). It does **not** carry the runtime `undoLocation`
value, because that number is an offset into a particular process's
undo ring buffer — a fresh recovery process has its own undo log with
different physical offsets, so the runtime number is meaningless to
copy.

**What redo does at each modify record.** `o_btree_modify()`:

1. Allocates a **new** undo entry in the recovery process's undo log.
2. Stamps that new location into the replayed tuple's
   `BTreeLeafTuphdr.undoLocation`.
3. Advances `cur_undo_locations.{branch, subxact, onCommit}` for the
   current oxid.

So `undoLocation` is re-derived locally, and as a side effect the
in-memory undo log for the recovery oxid is rebuilt by the act of
replaying the modify records (`src/recovery/recovery.c:2528-2534`).

**Why this matters — the load-bearing consequence.** When
`WAL_REC_ROLLBACK` arrives later in the stream,
`recovery_finish_current_oxid` takes the abort branch and calls
`apply_undo_stack(undoType, oxid, NULL, true)`. That walks the undo
chain — the same one redo built as a byproduct — and inverse-applies
every record. Net behavior of an aborted transaction in the WAL:

> Replay the rows (rebuilding undo as a side effect), then undo them
> (consuming that same undo). The on-disk state ends up as if the
> transaction had never happened.

If WAL had carried explicit undo locations instead, recovery would
need a parallel "recovery-undo-emission" code path. Instead, redo
reuses the runtime modify path verbatim, and undo "comes for free."

---

## Q2. Inside different calls of `apply_modify_record()`, can the PK be traversed and applied multiple times — once via `apply_btree_modify_record` while `toast_consistent == false`, and once via `apply_tbl_modify_record` once it flips?

**See also:** `recovery_investigation.md` §6 (PK/SK page-LSN checks).

**Short answer.** Each WAL record is dispatched to `apply_modify_record`
**exactly once** per redo pass. The branch differs by phase, not by
record:

```c
if (descr && toast_consistent)
    apply_tbl_modify_record(descr, ...);       // PK + every SK
else
    apply_btree_modify_record(&id->desc, ...); // tree named in WAL_REC_RELATION (= PK if rec is for PK)
```

**Two readings of "multiple times":**

- *Across the whole stream* — yes, the PK is mutated by records in
  **both** regions: pre-boundary records hit it via
  `apply_btree_modify_record`, post-boundary records hit it via the
  first iteration of `apply_tbl_modify_record`'s `descr->indices`
  loop. But each individual record hits the PK only once.
- *For the same record* — no. A given WAL record never causes two
  PK applies in the same redo pass.

**What does cause the SAME record to hit the PK twice** is a redo
*restart* — if the recovery process crashes and is restarted from the
last checkpoint, all records since that checkpoint are replayed again.
Three layers make this safe (`recovery_investigation.md` §6.2):

- **Layer 1** — pre-`controlReplayStartPtr` containers are skipped
  entirely.
- **Layer 3** — `recovery_insert_primary_callback` /
  `recovery_delete_primary_callback` compare the existing tuple's
  `OTupleXactInfo`: inserting a row whose `(oxid, tuple)` already
  matches is a no-op; deleting a tuple that's already gone is a no-op.
- **Layer 4** — `write_page_to_disk` `checkpointNum` gating
  (write-path concern, not redo, but the field recovery reads later).

**One subtle correctness point.** Records that fall into the Layer-2
PK-only short-circuit register a `PendingSkFixup` queue entry (per
worker, per PK key). When `toast_consistent` flips at the boundary,
`apply_pending_sk_fixups()` runs in one shot and synthesises the SK
rows that Layer-2 didn't produce. Post-boundary records keep SKs
current via `apply_tbl_modify_record` directly. If this pending-fixup
machinery loses a PK record (or applies it to the wrong worker), the
SK desync described in the active investigation is the result.

---

## Q3. Does orioledb use per-tuple or per-tree undo?

**See also:** `recovery_investigation.md` §7 (oxid + CSN + undo log) and
the new "Undo storage model" subsection there.

**Short answer.** Neither — orioledb has **three layers of undo
organization**, and "per-tuple" / "per-tree" don't quite name any of
them:

### Storage: per-cluster (global), one of three by **type**

`include/orioledb.h:210-223`:

```c
typedef enum {
    UndoLogRegular = 0,            // user data, row-level
    UndoLogRegularPageLevel = 1,   // user data, page-level (merge/split)
    UndoLogSystem = 2,             // system trees (catalog)
    UndoLogsCount = 3
} UndoLogType;
```

`src/transam/undo.c:176,330`:

```c
static UndoMeta *undo_metas = NULL;
…
undo_metas = (UndoMeta *) ptr;        // shared-memory array of 3
```

Three global ring buffers per cluster, each addressed by `UndoLocation`
(62-bit byte offset). Not one per tree, not one per tuple.

### Tree-to-log mapping: every user tree uses the same log

`src/tableam/tree.c:128` is the *only* assignment for user-data trees:

```c
desc->undoType = UndoLogRegular;
```

So **PK + every SK + every TOAST tree** of every orioledb table share
`UndoLogRegular`. Sys-trees (the orioledb-internal catalog) use
`UndoLogSystem`. Page-level structural undo (merges/splits) goes to
`UndoLogRegularPageLevel`.

### Back-pointer: per-tuple

`include/btree/page_contents.h:283`:

```c
typedef struct {
    OTupleXactInfo xactInfo:61, deleted:2, chainHasLocks:1;
    UndoLocation   undoLocation:62, formatFlags:2;
} BTreeLeafTuphdr;
```

Every leaf tuple in every btree carries its own back-pointer into one
of the three ring buffers. So the linkage from data to undo is
**per-tuple**, even though the storage is global.

### Chain within each log: per-transaction

`include/transam/undo.h:269-275`:

```c
typedef struct {
    UndoLocation location;
    UndoLocation branchLocation;
    UndoLocation subxactLocation;
    UndoLocation onCommitLocation;
} UndoStackLocations;
```

Each oxid maintains four head pointers into each of the 3 logs
(`UndoStackLocations[UndoLogsCount]` per oxid). Items within a log are
chained backward through `UndoStackItem.prev`
(`include/transam/undo.h:255-261`). The chain belongs to a transaction,
not to a tree.

### Why the difference matters

**PK and SK of the same row do NOT share an undo entry, but they DO
share an undo log.** When you UPDATE a column that doesn't change the
SK key:

- The PK leaf tuple gets a new `undoLocation` pointing at a freshly
  appended undo record in `UndoLogRegular`.
- The matching SK leaf tuple is untouched — its `undoLocation` still
  points at whatever earlier undo entry recorded the row's last
  SK-affecting state.

If the SK key *does* change (or the row is deleted), the SK leaf
tuple's `undoLocation` gets updated to a new entry in the *same*
`UndoLogRegular` ring. So both trees' undo references mix into one
global ring, distinguished only by who owns each entry's PK key.

For Bug #2's debugging this matters because `apply_undo_stack` walks
**one log at a time** (`for each undoType: apply_undo_stack(undoType,
oxid, ...)`) — so PK and SK abort-undo happen together (both live in
`UndoLogRegular`), but the system-catalog undo is a separately walked
stream. Visibility checks, in contrast, follow the back-pointer from a
specific tuple, hitting the per-tuple chain in whichever log that
tree's `undoType` selected.

---

## Q4. To get the previous version of a specific tuple, does orioledb have to walk through unrelated entries (other tuples/trees) in the global ring?

**Short answer.** **No.** Two independent linked lists are threaded
through the same ring buffer, and you walk only the one you care about.

### Two chains, one ring

Every `BTreeModifyUndoStackItem` (`include/btree/undo.h:55-63`) contains
both:

- an outer `UndoStackItem header` whose `prev` points to the previous
  item in the **same transaction**; AND
- an embedded `BTreeLeafTuphdr tuphdr` whose `undoLocation` points to
  the previous version of **the same row**.

```c
typedef struct {
    UndoStackItem      header;     // .prev = prior item in OXID's stack
    BTreeOperationType action;
    ORelOids           oids;
    OInMemoryBlkno     blkno;
    uint32             pageChangeCount;
    BTreeLeafTuphdr    tuphdr;     // .undoLocation = prior version of THIS row
} BTreeModifyUndoStackItem;
```

And `src/btree/undo.c:362` shows the linking happen at write time:

```c
if (curTupHdr) {
    item->tuphdr.xactInfo      = curTupHdr->xactInfo;
    item->tuphdr.undoLocation  = curTupHdr->undoLocation;  // ← prev version for THIS row
    item->tuphdr.deleted        = curTupHdr->deleted;
    item->tuphdr.chainHasLocks  = curTupHdr->chainHasLocks;
}
add_new_undo_stack_item(desc->undoType, undoLocation);     // ← links into OXID's stack
```

A single insert creates one undo record that participates in **two**
linked lists at once.

### What a version walk looks like

To get the previous version of a specific tuple, you do **not** scan
through unrelated entries. You jump straight from one back-pointer to
the next:

```
PAGE: leaf tuple { xactInfo, undoLocation = U_A }
                              │
                              ▼
RING @ U_A: BTreeModifyUndoStackItem {
                header { prev = X (some other oxid's item) }  ← ignored for version walk
                tuphdr { xactInfo, undoLocation = U_B }         ← FOLLOW THIS
                + frozen tuple bytes of "version A-1"
            }
                              │
                              ▼
RING @ U_B: BTreeModifyUndoStackItem {
                header { prev = Y }
                tuphdr { xactInfo, undoLocation = U_C }         ← FOLLOW THIS
                ...
            }
```

The snapshot-visibility loops in `src/btree/undo.c:703-752` and
`1611-1730` do exactly this: read the record at `undoLocation`, copy
out the bytes if needed, jump to `tuphdr.undoLocation` (NOT
`header.prev`) to continue. The walk is O(versions) regardless of how
busy the ring is.

### When you DO walk the per-transaction chain

The `header.prev` chain (the one that *does* mix tuples of the same oxid
together) is walked for transactional purposes only:

- `apply_undo_stack` (ABORT/ROLLBACK) — walks every entry the transaction
  created, in any tree, to inverse-apply them.
- `on_commit_undo_stack` / `precommit_undo_stack` — commit-time
  bookkeeping.
- `rollback_to_savepoint` — walks back to the matching
  `SubXactUndoStackItem` frame.

For those operations you *want* every entry the transaction touched, so
the cross-tuple interleaving is the right shape.

### Summary

- **Per-tuple version walk (visibility)**: each record's embedded
  `tuphdr.undoLocation` is a direct link to the prior version. No
  scanning.
- **Per-transaction stack walk (abort/commit)**: each record's
  `header.prev` walks the whole transaction. By design, this *does*
  mix entries across tuples and trees.

Same ring, two chains, choose which field you follow.

---

## Q5. Are the pointers inside undo records physical (offsets into the ring) or logical (chain semantics)?

**Short answer.** **Both, in different senses.**

- **Physical placement**: every undo record sits in exactly one of the
  three global ring buffers, at some contiguous offset. If you walked
  the ring linearly start-to-end, you'd see an interleaved mix of
  records from many transactions, many tuples, many trees.

- **The pointer values themselves are physical offsets** —
  `UndoLocation` is a 62-bit byte offset into the relevant ring
  buffer. Following a pointer is one dereference (O(1)), not a scan.

- **But the targets are chosen by logical chain semantics**:
  `header.prev` is "previous record in this transaction's undo stack",
  `tuphdr.undoLocation` is "previous version of this row". The
  previous-in-chain record could be the next ring slot, 1000 records
  earlier, or 1M records earlier — whatever was the predecessor when
  the link was written.

So the precise statement is:

> Pointers are *physical addresses* into the ring (cheap O(1) jump),
> but they're laid out to encode *logical chains* (per-transaction via
> `header.prev`, per-tuple-version via `tuphdr.undoLocation`).

### Concrete example

Three writers committing to two rows R1 and R2, all in `UndoLogRegular`.
The ring fills like this (each cell = one undo record):

```
ring offset:    100      150      200      250      300      350      400
content:       [T1/R1]  [T2/R2]  [T1/R2]  [T3/R1]  [T2/R1]  [T3/R2]  [T1/R1]
```

R1's current leaf tuple's `undoLocation` = 400. To walk R1's version
history:

- @400 (T1/R1) → `tuphdr.undoLocation` = 250
- @250 (T3/R1) → `tuphdr.undoLocation` = 300
- @300 (T2/R1) → `tuphdr.undoLocation` = 100
- @100 (T1/R1) → end

We skipped @150, @200, @350 entirely — those belong to R2 and are
physically adjacent but logically unrelated. Each step was a single
offset dereference.

T2's abort, by contrast, would walk `header.prev` instead, hitting @300
and @150 (its two records, in either tree). Same ring, different chain,
same O(1) per hop.
