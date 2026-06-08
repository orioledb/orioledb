# OrioleDB Undo Mechanism — Investigation

> Scope: how OrioleDB's in-memory undo log works, end to end — what undo is, how a
> record is laid out, how records are stored per transaction, how they are applied on
> commit vs abort, their relationship (or lack thereof) to WAL, and how they drive MVCC
> visibility during tuple lookup. Every claim below is anchored to real code with
> `src/...:line` / `include/...:line` references.
>
> This file is self-contained: **Part 1** is a brief overview of each of the six
> questions; **Part 2** ("Detailed Findings") holds the full per-question write-ups.
> Each overview links to its detail section.

Primary sources: `src/transam/undo.c` (the heart of MVCC), `include/transam/undo.h`,
`src/btree/undo.c` + `include/btree/undo.h` (row-level undo records), `src/transam/oxid.c`
(the CSN state machine), `src/recovery/wal.c` + `include/recovery/wal_record.h` (WAL),
`src/recovery/recovery.c` (recovery-side application), `src/btree/iterator.c` &
`src/btree/modify.c` (read/conflict paths).

---

## Part 1 — Brief Overviews

### Q1. What is undo in general? → [details](#q1-detail)

Undo is the per-transaction record of how to **reverse** a change, plus the retained
**old version** of data for MVCC. In stock PostgreSQL there is no separate undo log —
old row versions simply remain as extra physical tuples in the heap, collected later by
vacuum. OrioleDB has **no heap**: the primary-key B-tree *is* the table, so an
update/delete rewrites the row's PK slot in place and the prior image is pushed into a
single **in-memory undo log** (`src/transam/undo.c`, module header at `src/transam/undo.c:3`).
That one log serves both jobs at once: (a) on **abort** the chain is *applied* to revert
each row to its saved image; (b) on **commit** the chain is *not* applied — old versions
are *retained* until no snapshot can still see them, then reclaimed by watermark. Whether a
given version is visible is gated by the **CSN (commit sequence number)** state machine in
`xidBuffer` (`src/transam/oxid.c`): `INPROGRESS → CSN_COMMITTING → final CSN`.

### Q2. What does an undo record look like? → [details](#q2-detail)

A record is a variable-length, `MAXALIGN`'d blob carved from one of three in-memory ring
buffers — `UndoLogRegular` (row records), `UndoLogRegularPageLevel` (whole-page
split/merge/compact images), `UndoLogSystem` (catalog/system-tree), enumerated by
`UndoLogType` (`include/orioledb.h:203`). Every record begins with the common
`UndoStackItem` header (`include/transam/undo.h:255`): a `prev` back-pointer (the
per-transaction chain link), `itemSize`, a 1-byte `type` tag, and `indexType`. A variant
`OnCommitUndoStackItem` adds a second `onCommitLocation` chain so commit only walks the
items that need commit-time work. The `type` tag is one of ten `UndoItemType` kinds
(`include/transam/undo.h:241`) dispatched through the static `undoItemTypeDescrs[]` table
(`src/transam/undo.c:123`). The load-bearing kind is `BTreeModifyUndoStackItem`
(`include/btree/undo.h:55`) for row INSERT/UPDATE/DELETE/lock: it carries the tree OIDs, the
page blkno/change-count, a copy of the prior `BTreeLeafTuphdr` (the MVCC version-chain
link), and a trailing tuple/key image.

### Q3. What data structure stores undo records for a transaction? → [details](#q3-detail)

There is no separately-allocated per-transaction list. Records live in a **global
shared-memory circular (ring) buffer**, one per `UndoLogType`, allocated in
`undo_shmem_init` (`src/transam/undo.c:329`) and addressed by the modulo macro `GET_UNDO_REC`
(`src/transam/undo.c:61`). Each record's `UndoStackItem.prev` links it to the previous record
the *same* transaction wrote, forming an **intrusive LIFO stack/chain**; `add_new_undo_stack_item`
(`src/transam/undo.c:1922`) pushes by copying the old head into `prev` and writing the new
location as head. The chain heads are stored in shared per-process state —
`ODBProcData.undoStackLocations[level][undoType]` of type `UndoStackSharedLocations`
(`include/orioledb.h:235`), reached via `GET_CUR_UNDO_STACK_LOCATIONS` (this is CLAUDE.md's
`cur_undo_locations`). There are **two interleaved chains** per transaction: the full `prev`
chain walked on ABORT, and a sparser `onCommitLocation` chain walked at PRE-COMMIT/COMMIT.
Heads are zeroed between transactions by `reset_cur_undo_locations` (`src/transam/undo.c:2035`).

### Q4. Undo application on COMMIT vs ABORT → [details](#q4-detail)

One `XactCallback`, `undo_xact_callback` (`src/transam/undo.c:2110`, registered at
`src/orioledb.c:1255`), handles `XACT_EVENT_PRE_COMMIT`, `_COMMIT`, and `_ABORT`. The key
asymmetry: **row-reversal undo is applied only on ABORT, never on COMMIT** —
`modify_undo_callback` even asserts the abort stage (`src/btree/undo.c:474`).
**COMMIT** only flips the CSN — `current_oxid_precommit` sets the `CSN_COMMITTING` bit
(`src/transam/oxid.c:1424`), `current_oxid_commit` writes the final CSN after a
`nextCommitSeqNo` fetch-add — then runs `on_commit_undo_stack` over **only** the
`callOnCommit` subset; ordinary row versions are left in place to be reclaimed later by
`update_min_undo_locations` (`src/transam/undo.c:430`). This branch runs under
`HOLD_INTERRUPTS`, **not** a full crit section. **ABORT** first calls
`current_oxid_clear_committing` to release CSN waiters before taking page locks
(deadlock-avoidance, `src/transam/undo.c:2438`), then `apply_undo_stack` walks the **full**
`prev` chain restoring each prior row header (`page_item_rollback`, `src/btree/undo.c:583`),
and `current_oxid_abort` writes `COMMITSEQNO_ABORTED`. **Recovery** reuses the same machinery
(`recovery_finish_current_oxid`, `src/recovery/recovery.c:1886`), and at recovery end
`recovery_finish` (`src/recovery/recovery.c:1620`) aborts any still-INPROGRESS oxid via
`apply_undo_stack` — the site of the `recovery-finish-abort-trace` instrumentation
(`src/recovery/recovery.c:1663`).

### Q5. Does undo (the record, or its application) emit WAL? → [details](#q5-detail)

**No — the undo log is WAL-silent at both ends.** Creating a record (`get_undo_record`
`src/transam/undo.c:1840`; `add_new_undo_stack_item` `:1922`) is pure shared-memory
allocation. Applying undo on abort (`apply_undo_stack` → `walk_undo_stack`) does only
in-memory page work (plus `csn-trace` elogs) — no WAL. The WAL stream is produced
**separately**: the row-level modify records (`o_wal_insert/update/delete` from
`src/btree/` / `src/tableam/`) and the transaction-boundary records, all assembled in
`src/recovery/wal.c` (18 types in `include/recovery/wal_record.h:42`). A **normal backend
abort** *does* emit `WAL_REC_ROLLBACK` — but only via `wal_rollback()` (`src/recovery/wal.c:410`),
which is called only when `!RecoveryInProgress()` (`src/transam/undo.c:2425`) and asserts
`!is_recovery_process()` (`src/recovery/wal.c:422`). **Confirmed bug precondition:** the
recovery-finish in-memory abort block (`src/recovery/recovery.c:1650`) calls only
`apply_undo_stack` + `walk_checkpoint_stacks` and emits **no WAL at all** — verified both by
the absence of any `wal_*` call in that block and by an in-source comment at
`src/recovery/recovery.c:1652`. A streaming standby never reaches `recovery_finish()`, so it
**never learns those in-flight transactions aborted**.

### Q6. How is undo used during versioned tuple lookup? → [details](#q6-detail)

A reader walks the B-tree, finds the latest version inline in the leaf header
(`BTreeLeafTuphdr`), and follows that header's `undoLocation` backward through the undo log
to older versions. The read-side MVCC walker is `o_find_tuple_version()`
(`src/btree/iterator.c:303`): it skips lock-only headers via `find_non_lock_only_undo_record()`
(`src/btree/undo.c:1693`), maps each modifier's oxid to a CSN with `oxid_match_snapshot()`
(`src/transam/oxid.c:1747`), and stops at the first version committed before the snapshot
(or `tupptr <= xlogptr` on a replica); otherwise it pulls the previous version and loops.
The CSN lookup `oxid_get_csn()` (`src/transam/oxid.c:1648`) busy-spins while the transient
`CSN_COMMITTING` bit is set, returns the `INPROGRESS` sentinel (the "csn=0" case) for
not-yet-resolved transactions, and otherwise returns the final CSN. **Bug tie-in:** the
conflict path `o_btree_modify_handle_conflicts()` (`src/btree/modify.c:435`, `oxid_get_csn`
call at `:514`) is where a recovery worker collides with a dangling in-flight oxid. *Observed
log signature:* `oxid_get_csn` *returns* `result_csn=0` (INPROGRESS) every iteration and the
**outer** `handle_conflicts` → `lock_page` → `refind_page` retry loop spins forever — because
the in-memory abort that would have resolved that oxid (`current_oxid_clear_committing` /
`current_oxid_abort`, reached only from `XACT_EVENT_ABORT` at `src/transam/undo.c:2459`) ran
on the primary but was **never propagated to the standby** (Q5). This is the same dangling
oxid the primary logs in `recovery-finish-abort-trace`.

---

### Q7. How is outdated (no-longer-needed) undo reclaimed? → [details](#q7-detail)

OrioleDB's undo logs are fixed-size **circular ring buffers** (`undo_circular_buffer_size`, split per `UndoLogType`), so every record's slot must eventually be reused — undo nobody needs anymore is *reclaimed* by advancing a global **retain watermark** past it, never by freeing individual records. A record is "outdated" once it sits below `minRetainLocation` = the minimum, across every backend, of three per-proc watermarks in `oProcData[i].undoRetainLocations[type]`: `reservedUndoLocation` (a backend mid-write), `transactionUndoRetainLocation` (an in-progress txn that may still roll back), and `snapshotRetainUndoLocation` (the oldest snapshot still needing historical versions for MVCC) — further bounded by the checkpoint-retain range, the rewind retain, and (system undo only) the logical-replication catalog retain. `update_min_undo_locations()` (`src/transam/undo.c:430`) recomputes that minimum, publishes it **monotonically** (`minProcRetainLocation`, "never goes backwards" at `:495`), advances the eviction watermark (`writtenLocation`), and — when called with `do_cleanup=true` — physically `unlink_unretained_o_buffers()` the on-disk undo files below it (`:585`/`:593`). The periodic reclaimer is the **bgwriter** (`BGWriterNum==0`, `src/workers/bgwriter.c:208`); the snapshot hook (`undo.c:3211`) also reclaims on snapshot lifecycle. The ring is flow-controlled: a backend reserving space that would wrap into still-retained undo **blocks** in `wait_for_reserved_location()` (`undo.c:1504`/`:1623`) until reclamation advances the watermark. **Bug tie-in:** an oxid stuck `INPROGRESS` forever (the recovery-livelock / `csn-incremented` case) keeps *its* `transactionUndoRetainLocation` pinned, so `minRetainLocation` can never advance past it — reclamation stalls, the ring fills, and writers/recovery workers can wedge in `wait_for_reserved_location`; the bgwriter comment at `bgwriter.c:200-203` explicitly notes that on a synced replica `minProcRetainLocation` set during recovery "may never be advanced" without this cleanup.

---

## Part 2 — Detailed Findings

Each subsection below is the full write-up for one question.


---

<a id="q1-detail"></a>

# Q1 — What is "undo" in general (as it applies in OrioleDB)?

## Summary

"Undo" is the mechanism a database uses to (a) **roll back** the changes of a
transaction that aborts, and (b) **retain old versions of rows** so concurrent
readers under MVCC can still see the pre-modification state of data that a newer
transaction has already changed in place.

PostgreSQL's native heap solves both problems by *never overwriting* a row:
`UPDATE`/`DELETE` leave the old tuple in the heap and append a new one, and
visibility is decided by comparing the snapshot against each tuple's `xmin`/`xmax`.
The old versions are physical heap tuples; `VACUUM` later reclaims them.

OrioleDB has **no heap** — the table *is* a B-tree whose primary key is the data
(an index-organized table). A row therefore has exactly one home: its slot in the
PK B-tree, keyed by the PK value. When that row is updated or deleted, it is
**rewritten in place**, so the prior image cannot stay in the page. Instead the
prior image is pushed into an **in-memory undo log** (`src/transam/undo.c`,
header doc: `undo.c:1-13`, "Implementation of OrioleDB undo log"). The live tuple
header keeps a pointer (`undoLocation`) back into that log, forming a per-row
version chain that lives outside the page. That single undo log is the substrate
for *both* rollback and MVCC old-version retention.

The visibility of every undo version is gated by a per-transaction **CSN (commit
sequence number)** that flips through a small state machine — `INPROGRESS`
→ `CSN_COMMITTING` → final CSN — stored in the shared `xidBuffer`
(`src/transam/oxid.c:140`).

---

## 1. The general concept of undo, and how PG vs OrioleDB differ

Two classic uses of "undo" in a transactional store:

1. **Rollback of uncommitted changes.** If a transaction aborts, every change it
   made must be reverted to the state before the transaction touched it.
2. **MVCC old-version retention.** While a transaction modifies a row, other
   transactions with older snapshots must still see the *old* value until the
   modifier commits and no snapshot old enough to need the prior version remains.

**PostgreSQL heap approach:** old versions are kept *in the heap itself*. An
update writes a brand-new tuple and leaves the old one behind with its `xmax`
set; readers walk the heap-page line pointers and test each tuple's xmin/xmax
against their snapshot. There is no separate "undo log"; the heap is the
old-version store, and `VACUUM` is the reclaimer.

**OrioleDB approach:** there is no heap (CLAUDE.md: "There is no PostgreSQL heap
behind an orioledb table. The primary key is the data"). The table is the PK
B-tree. Because a row's *only* physical location is its PK slot, an update/delete
overwrites that slot. To preserve the old image OrioleDB writes it into the undo
log and links the live tuple to it. Concretely:

- The undo log module is `src/transam/undo.c` — its file header literally calls
  itself "Implementation of OrioleDB undo log" (`src/transam/undo.c:3-4`).
- A row modification produces a `BTreeModifyUndoStackItem`
  (`include/btree/undo.h:55-63`): a generic `UndoStackItem header`, the
  `BTreeOperationType action` (insert/update/delete), the relation oids, the
  block number, a page change count, and a `BTreeLeafTuphdr tuphdr`. The **prior
  tuple body is appended immediately after the struct** —
  `src/btree/undo.c:341` copies the tuple to
  `(Pointer) item + sizeof(BTreeModifyUndoStackItem)`, and the abort callback
  reads it back from the same offset (`src/btree/undo.c:493`).
- The live leaf tuple header `BTreeLeafTuphdr`
  (`include/btree/page_contents.h:278-285`) contains a bit-packed
  `UndoLocation undoLocation:62` field (`page_contents.h:283`). That is the
  back-pointer from the in-page live row to its previous version in the undo log
  — i.e. the **version chain** is threaded through the undo log rather than
  through extra heap tuples.

So where PG stores N row versions as N physical heap tuples, OrioleDB stores 1
live tuple in the B-tree page plus a chain of older images in the undo log,
reachable via `tuphdr.undoLocation`.

---

## 2. Why OrioleDB *needs* undo (index-organized storage)

Because the PK B-tree *is* the table:

- A row's "location" is its PK key, not a `ctid`. Secondary indexes reference the
  PK value, not a physical tuple address (CLAUDE.md, "Index-organized storage").
- An update/delete cannot append a second physical tuple beside the old one the
  way the heap does; it rewrites the PK slot in place. The displaced image has
  nowhere to live *inside* the page, so it goes to the undo log.
- Even structural B-tree changes (page splits/compactions/merges) need undo:
  `include/btree/undo.h:22-42` defines page-image undo records
  (`UndoPageImageCompact/Split/Merge`) so an in-flight reader or an aborting
  writer can reconstruct the pre-split/merge page shape
  (`make_merge_undo_image`, `src/btree/undo.c:1256`).

Hence undo is not an optional add-on in OrioleDB; it is the *only* place
pre-modification state (both row-level and page-level) is kept.

---

## 3. Undo's two roles: (a) abort and (b) MVCC reads

### (a) Aborting a transaction — undo is *applied*

On abort the undo stack is walked newest-to-oldest and each record's callback
**reverts** the change:

- `undo_xact_callback` handles the `XACT_EVENT_ABORT` case
  (`src/transam/undo.c:2415`) and calls `apply_undo_stack(...)` for each undo log
  type (`src/transam/undo.c:2468`).
- `apply_undo_stack` (`src/transam/undo.c:1412-1417`) is a thin wrapper over
  `walk_undo_stack(..., abortTrx=true, ...)` (`src/transam/undo.c:1298`), which
  walks the chain from `sharedLocations->location`
  (`src/transam/undo.c:1353`) and invokes each item's callback with stage
  `OUndoCallbackStageAbort` (`src/transam/undo.c:1364`).
- For a row modification the callback is `modify_undo_callback`
  (`src/btree/undo.c:456`). It asserts the stage is abort
  (`src/btree/undo.c:474`), reconstructs the saved old tuple from the bytes
  trailing the undo record (`src/btree/undo.c:492-493`), re-finds the page, and
  restores the prior image. This is the literal "undo".

The `OUndoCallbackStage` enum documents the three callback stages
(`include/transam/undo.h:340-345`): `OUndoCallbackStageAbort` ("transaction is
being rolled back; the callback should undo whatever the transaction did"),
`OUndoCallbackStagePreCommit`, and `OUndoCallbackStageCommit`.

### (b) Serving MVCC snapshots to concurrent readers — undo is *traversed*

When a reader's snapshot is older than the live tuple's committing/committed
state, the reader does **not** apply undo — it *reads through* the version chain
to find the version visible to its snapshot:

- A snapshot is identified by a CSN; the in-progress snapshot constant is
  `o_in_progress_snapshot = {COMMITSEQNO_INPROGRESS, ...}`
  (`src/transam/oxid.c:146`).
- Page/row reads compare the stored CSN against the snapshot CSN and, when the
  page header's CSN is newer than the snapshot, follow `header->undoLocation`
  into the undo log to fetch the older image: see
  `src/btree/page_contents.c:225-265`
  (`if (read_undo && COMMITSEQNO_IS_NORMAL(csn) && headerCsn >= csn) ... pageUndoLoc = read_page_from_undo(desc, img, header->undoLocation, csn, ...)`).
  The same `undoLocation` back-pointer used for rollback is reused here to serve
  the correct historical version.
- Whether a given undo record is still physically present (not yet recycled) is
  tested by `UNDO_REC_EXISTS` (`include/transam/undo.h:356-358`), which compares
  the location against the retained-location watermarks. Snapshots hold a
  `snapshotRetainUndoLocation` (`get_snapshot_retained_undo_location`,
  `src/transam/undo.c:1459-1467`) so undo a live snapshot still needs is not
  discarded.

So abort = *apply* the undo (mutate the page back); MVCC read = *walk* the undo
(read an older image without mutating anything). Both use the identical undo
records and the same `undoLocation` links.

---

## 4. High-level lifecycle of per-transaction undo

Undo records accumulate per transaction and end one of two ways:

1. **Accumulate.** Each modification reserves and writes an undo record into the
   process's current undo stack. The per-process "current" stack locations are
   read/written via `get_cur_undo_locations` / `set_cur_undo_locations`
   (`src/transam/undo.c:2020`, `:2028`) and reset by `reset_cur_undo_locations`
   (`src/transam/undo.c:2036`). The shared meta tracks where each backend's
   records live (`lastUsedLocation`, `advanceReservedLocation`, etc., documented
   in `include/transam/undo.h:17-230`).

2. **On abort → apply, then reset.** `XACT_EVENT_ABORT` runs
   `apply_undo_stack` (reverts everything) and then
   `reset_cur_undo_locations` (`src/transam/undo.c:2468`, `:2480`).

3. **On commit → run on-commit actions, retain-until-invisible, then discard.**
   The commit path in `undo_xact_callback` is split:
   - `XACT_EVENT_PRE_COMMIT` calls `precommit_undo_stack`
     (`src/transam/undo.c:2236`) — `walk_undo_stack` is *not* used here; instead
     `walk_undo_range_with_buf` runs only the `callOnCommit` items with stage
     `OUndoCallbackStagePreCommit` (`src/transam/undo.c:1420-1429`), for
     durability work that must precede the commit WAL write
     (`include/transam/undo.h:327-334`).
   - `XACT_EVENT_COMMIT` calls `on_commit_undo_stack`
     (`src/transam/undo.c:2395`), which walks the **on-commit** sub-chain
     (`sharedLocations->onCommitLocation`, `src/transam/undo.c:1334`) with stage
     `OUndoCallbackStageCommit` for post-commit cleanup (e.g. dropping obsolete
     files), then resets the current locations (`src/transam/undo.c:2400`).
   - Crucially, on commit the row-level undo images are **not** applied — they
     stay in the log so older snapshots can still read them, and are only
     physically reclaimed once no snapshot can need them. That reclamation is
     driven by `update_min_undo_locations` advancing the retain watermarks
     (`include/transam/undo.h:369-371`, the `minProc*RetainLocation` fields), at
     which point the undo files behind `cleanedLocation` are cleaned
     (`include/transam/undo.h:33`).

Note the COMMIT case runs under `HOLD_INTERRUPTS` (commented at
`src/transam/undo.c:2334`), which is the timing-sensitive region the crash-test
harness on this branch instruments.

---

## 5. The CSN state machine and how it gates undo visibility

Each oxid has a slot in the shared `xidBuffer` (`src/transam/oxid.c:140`,
allocated at `:286`) holding a `csn` (and a `commitPtr`). The CSN passes through
three conceptual states:

1. **INPROGRESS** — the transaction has not committed; readers must treat its
   changes as invisible (and walk undo to the prior version). On reset/recovery,
   slots are stamped `COMMITSEQNO_INPROGRESS` (`src/transam/oxid.c:1261-1262`).
   The "special, in-progress" status bit is `COMMITSEQNO_STATUS_IN_PROGRESS`
   (`src/transam/oxid.c:42`).

2. **CSN_COMMITTING** — a transient "I am in the act of committing" marker.
   `current_oxid_precommit` (`src/transam/oxid.c:1416-1433`) stamps the slot with
   `COMMITSEQNO_MAKE_SPECIAL(..., COMMITSEQNO_STATUS_CSN_COMMITTING)`
   (`src/transam/oxid.c:1424-1426`) and sets the backend-local
   `csn_committing_set = true` (`:1427`). The committing-status bit is
   `COMMITSEQNO_STATUS_CSN_COMMITTING (0x1)` (`src/transam/oxid.c:43`).

3. **Final CSN** — `current_oxid_commit(CommitSeqNo csn)`
   (`src/transam/oxid.c:1507`, declared `include/transam/oxid.h:224`) writes the
   real, monotonically increasing commit sequence number, making the
   transaction's changes visible to snapshots taken at or after that CSN.

How readers consume this: `oxid_get_csn` (`src/transam/oxid.c:1647-1704`) maps an
oxid to its CSN. If it finds the `CSN_COMMITTING` status bit set
(`src/transam/oxid.c:1681-1683`) it **spins** (`perform_spin_delay`) until the
committer finishes — i.e. a reader will not make a visibility decision while a
commit is mid-flight, it waits for the final CSN. If the slot is still "special"
(in-progress) after that, it returns `COMMITSEQNO_INPROGRESS`
(`src/transam/oxid.c:1690-1696`), telling the caller the change is not yet
committed and the **older undo version** must be used instead.

The abort path must undo this stamping: `current_oxid_clear_committing`
(around `src/transam/oxid.c:1630`) reverts the COMMITTING marker back to
IN_PROGRESS before `apply_undo_stack` runs, using the backend-local
`csn_committing_set` / `xlog_ptr_committing_set` flags (documented at
`src/transam/oxid.c:92-99`) to know whether the bit was set.

Thus the CSN state machine is the visibility oracle that decides, for every undo
version, *which* transaction snapshot should see the live tuple versus an older
undo image:

```
                current_oxid_precommit()          current_oxid_commit(csn)
   INPROGRESS  ───────────────────────►  CSN_COMMITTING  ─────────────────────►  final CSN
       ▲   (readers walk undo to              (readers spin in                  (visible to
       │    the prior version)                 oxid_get_csn until                snapshots ≥ csn;
       │                                       this resolves)                    undo image kept
       └──── current_oxid_clear_committing() on abort ◄──────                    only while older
             (then apply_undo_stack reverts the changes)                         snapshots need it)
```

---

## Code references

- `src/transam/undo.c:1-13` — file header: "Implementation of OrioleDB undo log".
- `src/transam/undo.c:1298-1366` — `walk_undo_stack`: walks chain for abort vs commit, dispatches callbacks per stage.
- `src/transam/undo.c:1412-1417` — `apply_undo_stack` (abort: apply undo).
- `src/transam/undo.c:1420-1429` — `precommit_undo_stack` (pre-commit on-commit items).
- `src/transam/undo.c:1431-1435` — `on_commit_undo_stack` (post-commit cleanup).
- `src/transam/undo.c:1459-1467` — `get_snapshot_retained_undo_location` (snapshot pins undo).
- `src/transam/undo.c:2020-2042` — `get/set/reset_cur_undo_locations` (per-process current stack).
- `src/transam/undo.c:2110` — `undo_xact_callback` entry.
- `src/transam/undo.c:2222-2236` — `XACT_EVENT_PRE_COMMIT` → `precommit_undo_stack`.
- `src/transam/undo.c:2255-2400` — `XACT_EVENT_COMMIT` → `on_commit_undo_stack` + reset (under HOLD_INTERRUPTS, see :2334).
- `src/transam/undo.c:2415-2480` — `XACT_EVENT_ABORT` → `apply_undo_stack` + reset.
- `include/transam/undo.h:17-230` — `UndoMeta`: location watermarks (lastUsed/retain/cleaned/written) and their ordering invariants.
- `include/transam/undo.h:340-345` — `OUndoCallbackStage` enum (Abort / PreCommit / Commit) with doc comments.
- `include/transam/undo.h:356-358` — `UNDO_REC_EXISTS` (is an undo record still retained?).
- `include/transam/undo.h:369-371` — `update_min_undo_locations` (advance retain watermarks → reclaim).
- `include/btree/undo.h:22-42` — page-image undo record types (split/compact/merge).
- `include/btree/undo.h:55-63` — `BTreeModifyUndoStackItem` (row-modification undo record).
- `src/btree/undo.c:341` — old tuple body copied right after the undo struct.
- `src/btree/undo.c:456-493` — `modify_undo_callback`: abort-stage restore of the prior tuple image.
- `src/btree/undo.c:1256` — `make_merge_undo_image` (page-level undo).
- `include/btree/page_contents.h:278-285` — `BTreeLeafTuphdr` with `undoLocation:62` version-chain back-pointer.
- `src/btree/page_contents.c:225-265` — MVCC read: compare CSN, follow `undoLocation` via `read_page_from_undo`.
- `src/transam/oxid.c:42-53` — CSN status bits (`IN_PROGRESS`, `CSN_COMMITTING`) and `COMMITSEQNO_MAKE_SPECIAL`.
- `src/transam/oxid.c:140`, `:286` — `xidBuffer` shared CSN map.
- `src/transam/oxid.c:146` — `o_in_progress_snapshot` (INPROGRESS snapshot constant).
- `src/transam/oxid.c:1416-1433` — `current_oxid_precommit` (→ CSN_COMMITTING).
- `src/transam/oxid.c:1507` / `include/transam/oxid.h:224` — `current_oxid_commit` (→ final CSN).
- `src/transam/oxid.c:1630` — `current_oxid_clear_committing` (abort: COMMITTING → IN_PROGRESS).
- `src/transam/oxid.c:1647-1704` — `oxid_get_csn` (spins on CSN_COMMITTING; returns INPROGRESS → reader uses undo).


---

<a id="q2-detail"></a>

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


---

<a id="q3-detail"></a>

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


---

<a id="q4-detail"></a>

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


---

<a id="q5-detail"></a>

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


---

<a id="q6-detail"></a>

# Q6 — How undo is used during versioned tuple look-up (MVCC visibility resolution)

## Summary

In OrioleDB there is no heap: the **latest** version of a row lives inline in the
B-tree leaf page (PK tree, or a secondary-index tree), inside a
`BTreeLeafTuphdr`. Older versions are **not** on the page — they are reached by
following the tuple header's `undoLocation` pointer backward into the in-memory
undo log, one `BTreeLeafTuphdr` at a time.

Every leaf tuple header carries an `OTupleXactInfo` (`xactInfo`) bit-field that
encodes the **oxid** of the transaction that produced that version (plus a
lock-only flag and lock mode). Visibility is resolved by mapping that oxid to a
**CSN** via `oxid_get_csn()` / `oxid_match_snapshot()` and comparing it against
the reader's snapshot CSN (`OSnapshot.csn`). Three outcomes: the version is
**visible** (stop, return it), **not yet committed / too new** (follow the undo
pointer to the previous version), or the modifying txn is **in-progress**
(`csn == COMMITSEQNO_INPROGRESS`, numerically the "special" state) which the
reader treats specially depending on whether the snapshot itself is a dirty /
in-progress snapshot.

The core read-side traversal is **`o_find_tuple_version()`** in
`src/btree/iterator.c:303`. The core modify-side conflict resolver is
**`o_btree_modify_handle_conflicts()`** in `src/btree/modify.c:435`, and that is
exactly where the recovery worker busy-spins on `oxid_get_csn()` for an oxid that
was aborted in-memory on the primary but never resolved on the replica.

---

## Step-by-step visibility-resolution flow (read path)

### 1. Read the on-page (latest) tuple header
A reader positioned on a leaf item reads the inline header + tuple:

- `o_find_tuple_version()` does `BTREE_PAGE_READ_LEAF_ITEM(tupHdrPtr, curTuple, p, loc)`
  then copies `tupHdr = *tupHdrPtr` — `src/btree/iterator.c:319-321`.

### 2. Skip pure lock-only records at the head of the chain
A row can have **lock-only** undo records (row-level locks) stacked on top of the
real data version. Before doing visibility, the reader advances past any
lock-only header to the first real data header:

- `(void) find_non_lock_only_undo_record(desc->undoType, &tupHdr);`
  — `src/btree/iterator.c:322`.
- `find_non_lock_only_undo_record()` itself loops while
  `XACT_INFO_IS_LOCK_ONLY(xactInfo)`, following `tuphdr->undoLocation` via
  `get_prev_leaf_header_from_undo_if_exists()` — `src/btree/undo.c:1693-1708`.

### 3. Map this version's oxid to a CSN against the snapshot
Inside the main `while (true)` loop, the reader extracts the version's oxid from
`xactInfo` and resolves it relative to the snapshot:

- `oxid_match_snapshot(XACT_INFO_GET_OXID(xactInfo), oSnapshot, &tupcsn, ...)`
  — `src/btree/iterator.c:332-333`.
- `XACT_INFO_GET_OXID()` is the bit-mask extractor — `include/btree/btree.h:394`.
- `oxid_match_snapshot()` lives at `src/transam/oxid.c:1747`. It returns
  `COMMITSEQNO_FROZEN` for boot / below-xmin oxids, otherwise calls
  `map_oxid()` and, crucially, **spins** (`perform_spin_delay`) while the CSN
  carries the `COMMITSEQNO_STATUS_CSN_COMMITTING` bit, converting any other
  "special" CSN to `COMMITSEQNO_INPROGRESS` — `src/transam/oxid.c:1786-1809`.
- `txIsFinished = COMMITSEQNO_IS_COMMITTED(tupcsn)` — `src/btree/iterator.c:336`.

### 4. Decide: visible / follow-undo / in-progress
Still in the loop (`src/btree/iterator.c:368-426`):

- **In-progress modifier (`!txIsFinished`)** with a *normal* snapshot: this
  version is invisible, so the reader must descend the undo chain — unless the
  modifier is the reader's **own** transaction (`XACT_INFO_OXID_IS_CURRENT`) and
  the change predates the current command (`tupleCid < oSnapshot->cid`), in which
  case it is visible and we `break` — `src/btree/iterator.c:374-403`.
- **Dirty / in-progress snapshot** (`COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn)`):
  the reader wants the latest physical version, so it `break`s immediately
  regardless of the modifier state — `src/btree/iterator.c:375`, `:407`, `:413`.
  This is the "see everything, even uncommitted" mode (also `NON_DELETED`).
- **Committed modifier**: compare CSNs. With a CSN snapshot,
  `if (tupcsn < oSnapshot->csn) break;` → this version committed before our
  snapshot, it is visible — `src/btree/iterator.c:410-417`. With an LSN-based
  snapshot (`oSnapshot->xlogptr` valid, used on replicas), the comparison is
  `if (tupptr <= oSnapshot->xlogptr) break;` — `src/btree/iterator.c:419-422`.
- Otherwise (committed too late, or aborted, or in-progress and we need an older
  version): **fall through to follow the undo pointer.**

### 5. Follow the undo location to the previous version
At the bottom of the loop the reader steps one link back:

- `undoLocation = tupHdr.undoLocation;` — if invalid, chain is exhausted, return
  NULL (no visible version) — `src/btree/iterator.c:430-439`.
- If the current header is a delete or a lock-only header, only the **header** is
  pulled back: `get_prev_leaf_header_from_undo(desc->undoType, &tupHdr, true)`
  — `src/btree/iterator.c:441-445`.
- Otherwise the previous header **and** its tuple body are pulled back:
  `get_prev_leaf_header_and_tuple_from_undo(desc->undoType, &tupHdr, &curTuple, 0)`
  — `src/btree/iterator.c:446-453`.
- Then loop: re-resolve the new (older) `xactInfo` against the snapshot.

These helpers physically `undo_read()` a `BTreeLeafTuphdr` (and for the update
case a `BTreeModifyUndoStackItem` + tuple payload) from the undo log:
`get_prev_leaf_header_from_undo` — `src/btree/undo.c:1711-1733`;
`get_prev_leaf_header_from_undo_if_exists` (tolerates concurrently-cleaned undo,
returns false = "treat as committed/end of chain") — `src/btree/undo.c:1740-1756`;
`get_prev_leaf_header_and_tuple_from_undo` — `src/btree/undo.c:1758-1815`
(PANICs `"undo record does not exist"` if the record was already reclaimed —
`src/btree/undo.c:1768-1789`).

### 6. Final delete check & return
After the loop settles on a visible header, a deleted final version yields NULL
(`tupHdr.deleted != BTreeLeafTupleNonDeleted`) unless the snapshot is
`NON_DELETED` — `src/btree/iterator.c:457-477`. The surviving `curTuple` is
returned in `mcxt`.

### Page-level undo (whole-page historical images)
Besides per-tuple undo, OrioleDB also keeps **page images** in undo for
concurrent structural changes. Scans/finds resolve a consistent page image at a
given CSN via `get_page_from_undo()` (`src/btree/undo.c:1131`) and
`read_page_from_undo()` (called from `src/btree/iterator.c:235`,
`src/btree/scan.c:1078`). `find.c` re-finds the correct leaf offset inside an
in-progress page image when `COMMITSEQNO_IS_INPROGRESS(context->csn)` —
`src/btree/find.c:1542-1560`.

---

## The CSN states (INPROGRESS = "special", numerically not a clean 0)

`xactInfo` only stores the **oxid**; the CSN itself lives in `xidBuffer` and is
read through `oxid_get_csn()` (`src/transam/oxid.c:1648`). Its three lifecycle
states:

1. `INPROGRESS` — the slot holds a `COMMITSEQNO_IS_SPECIAL` value with
   `COMMITSEQNO_STATUS_IN_PROGRESS`. `oxid_get_csn()` returns the sentinel
   `COMMITSEQNO_INPROGRESS` for it — `src/transam/oxid.c:1686-1693`. This is the
   logical "csn = 0 / not yet committed" case the question refers to.
2. `CSN_COMMITTING` — a transient flagged state set during precommit. Readers
   must **not** observe a half-written CSN, so `oxid_get_csn()` **busy-spins**
   (`perform_spin_delay`) while `COMMITSEQNO_GET_STATUS(csn) &
   COMMITSEQNO_STATUS_CSN_COMMITTING` — `src/transam/oxid.c:1672-1684`.
3. Final CSN — a normal monotonic commit sequence number, returned directly —
   `src/transam/oxid.c:1695-1700`.

`oxid_get_csn()` also short-circuits boot (`COMMITSEQNO_FROZEN`,
`src/transam/oxid.c:1657-1664`) and below-`globalXmin` oxids (frozen,
`src/transam/oxid.c:1668-1676`). The macro `XACT_INFO_MAP_CSN(xactInfo)` is the
thin wrapper used in lock-conflict code — `include/btree/btree.h:392`.

---

## Interaction with row locking and lock-only undo records

Row-level locks are recorded as **lock-only** undo headers
(`XACT_INFO_IS_LOCK_ONLY`, `XACT_INFO_LOCK_ONLY_BIT` —
`include/btree/btree.h:381,390`). They sit on the chain but carry no data
version. The read path skips them (step 2 above). The **lock-conflict** path
walks them explicitly in `row_lock_conflicts()` — `src/btree/undo.c:1352` — which:

- For a not-conflicting mode, computes `csn = XACT_INFO_MAP_CSN(xactInfo)` and
  sets `xactIsFinished = !COMMITSEQNO_IS_INPROGRESS(csn)`,
  `xactIsFinal = (csn < my_csn)` — `src/btree/undo.c:1379-1382`.
- For each **lock-only** record owned by *another* oxid it calls
  `oxid_get_csn(oxid, false)` and, if that txn is committed/aborted/frozen,
  **garbage-collects** the stale lock-only record from the chain
  (`delete_record = true`); only genuinely in-progress lockers count as
  conflicts — `src/btree/undo.c:1410-1450`.

So lock-only records are pruned lazily during lookup whenever the locker is found
to be resolved.

---

## Recovery / concurrent-modify conflict path — the busy-spin tie-in

### Where it is
The modify path resolves conflicts in **`o_btree_modify_handle_conflicts()`**
(`src/btree/modify.c:435`), called from **`o_btree_modify_internal()`**
(`src/btree/modify.c:200`). Public entry is `o_btree_modify()`
(`src/btree/modify.c:1542`) → `o_btree_normal_modify` → `o_btree_modify_internal`.

When the conflicting version belongs to **another** oxid, the resolver — while
**still holding the leaf page-content lock** — does:

```c
csn = oxid_get_csn(oxid, false);     // src/btree/modify.c:514
```

(preceded by the `STOPEVENT(STOPEVENT_BEFORE_MODIFY_OXID_GET_CSN, ...)` test hook
at `src/btree/modify.c:511`, and `csn-trace handle_conflicts` LOG markers at
`:502` and `:517`).

It then branches on the result — `src/btree/modify.c:526-616`:
- `COMMITSEQNO_IS_ABORTED(csn)` → roll the conflicting item back in place
  (`page_item_rollback`, `src/btree/modify.c:541-549`).
- `COMMITSEQNO_IS_NORMAL/FROZEN(csn)` → serialization-conflict check
  (`src/btree/modify.c:551-564`).
- else `COMMITSEQNO_IS_INPROGRESS(csn)` → **wait** for the conflicting txn:
  `wait_for_tuple()` or the caller's `waitCallback`, then retry
  (`src/btree/modify.c:566-616`).

### Why the replica recovery worker busy-spins forever
The recovery replay calls `o_btree_modify(..., waitCallback = NULL, ...)` —
`src/recovery/worker.c:919,975,1008,1054,1098,1127` and
`src/recovery/recovery.c:552,570,2220`. With no wait callback, an in-progress
conflict means `wait_for_tuple()` / repeated `oxid_get_csn()` spinning.

The deadly case: a transaction reached **precommit** (so its CSN slot is stamped
`COMMITSEQNO_STATUS_CSN_COMMITTING`) but then **aborted in memory on the
primary** without `current_oxid_commit()` ever writing a final CSN. On the
**replica**, that abort is never replayed/resolved, so the slot stays special
forever. Any worker that finds a tuple stamped with that oxid calls
`oxid_get_csn()`, which loops at `src/transam/oxid.c:1681-1683` waiting for the
`CSN_COMMITTING` bit to clear — it never clears, so the spin exhausts
`NUM_DELAYS` and the cluster PANICs (`stuck spinlock`) at `oxid_get_csn` /
`oxid_match_snapshot`.

The intended primary-side fix is `current_oxid_clear_committing()`
(`src/transam/oxid.c:1601`), invoked from the abort branch of
`undo_xact_callback`'s `XACT_EVENT_ABORT` (`src/transam/undo.c:2459`), which
reverts the slot from `CSN_COMMITTING` back to `IN_PROGRESS` so spinners are
released *before* `apply_undo_stack()` goes after their page locks. The long
comment at `src/transam/undo.c:2437-2462` documents exactly this page-lock vs.
`apply_undo_stack()` cycle and the `oxid_get_csn` / `oxid_match_snapshot`
busy-spin. The bug is that this resolution does not happen on the **standby**:
the in-flight txn is never aborted there, so `oxid_get_csn` returns the special
COMMITTING/INPROGRESS state indefinitely.

---

## Code references

Read-path visibility traversal (per-tuple undo chain):
- `src/btree/iterator.c:303` — `o_find_tuple_version()` (the MVCC version walker)
- `src/btree/iterator.c:319-322` — read on-page header; skip lock-only head
- `src/btree/iterator.c:332-336` — `oxid_match_snapshot()` call; `txIsFinished`
- `src/btree/iterator.c:368-426` — visible / in-progress / follow-undo decision
- `src/btree/iterator.c:430-453` — follow `undoLocation` to previous version
- `src/btree/iterator.c:457-477` — final deleted-tuple handling / return
- `src/btree/iterator.c:235`, `src/btree/scan.c:1078` — `read_page_from_undo` (page-level)

Undo-chain helpers:
- `src/btree/undo.c:1693` — `find_non_lock_only_undo_record()`
- `src/btree/undo.c:1711` — `get_prev_leaf_header_from_undo()`
- `src/btree/undo.c:1740` — `get_prev_leaf_header_from_undo_if_exists()`
- `src/btree/undo.c:1758` — `get_prev_leaf_header_and_tuple_from_undo()`
- `src/btree/undo.c:1131` — `get_page_from_undo()` (page image from undo)

CSN resolution + INPROGRESS/COMMITTING handling:
- `src/transam/oxid.c:1648` — `oxid_get_csn()` (spins on `CSN_COMMITTING`, returns `INPROGRESS`)
- `src/transam/oxid.c:1672-1700` — spin loop + special→INPROGRESS / final-CSN return
- `src/transam/oxid.c:1747` — `oxid_match_snapshot()` (read-path snapshot mapping + spin)
- `src/transam/oxid.c:1601` — `current_oxid_clear_committing()` (primary-side fix)
- `include/btree/btree.h:392` — `XACT_INFO_MAP_CSN`
- `include/btree/btree.h:394,400` — `XACT_INFO_GET_OXID`, `XACT_INFO_IS_FINISHED`
- `include/btree/btree.h:381,390` — `XACT_INFO_LOCK_ONLY_BIT`, `XACT_INFO_IS_LOCK_ONLY`

Row-lock / lock-only handling during lookup:
- `src/btree/undo.c:1352` — `row_lock_conflicts()`
- `src/btree/undo.c:1379-1450` — CSN map, lock-only pruning, conflict detection

Modify / conflict-resolution path (the busy-spin site):
- `src/btree/modify.c:435` — `o_btree_modify_handle_conflicts()`
- `src/btree/modify.c:200` — call from `o_btree_modify_internal()`
- `src/btree/modify.c:511` — `STOPEVENT(STOPEVENT_BEFORE_MODIFY_OXID_GET_CSN)`
- `src/btree/modify.c:514` — `oxid_get_csn(oxid, false)` under page lock
- `src/btree/modify.c:526-616` — aborted / normal / in-progress branches (wait+retry)
- `src/btree/modify.c:1542` — `o_btree_modify()` public entry

Recovery callers (waitCallback = NULL → spin on in-progress conflict):
- `src/recovery/worker.c:919,975,1008,1054,1098,1127`
- `src/recovery/recovery.c:552,570,2220,2228,2235`

Busy-spin / abort-resolution documentation:
- `src/transam/undo.c:2437-2462` — page-lock vs. `apply_undo_stack` cycle comment
- `src/transam/undo.c:2459` — `current_oxid_clear_committing()` call in `XACT_EVENT_ABORT`
- `src/transam/oxid.c:1581-1599` — comment on spinners in oxid_get_csn/match_snapshot

---

<a id="q7-detail"></a>

# Q7 — How outdated undo is reclaimed (retain watermarks, the GC routine, and back-pressure)

## Summary

OrioleDB never frees undo records one at a time. Each `UndoLogType` is a **fixed-size circular ring buffer** (`undo_circular_buffer_size`, partitioned into regular-row / regular-page / system fractions), so a record's slot is recycled simply by letting the writer wrap around and overwrite it. The only safety question is *"has everything below this point become outdated?"* — and that is answered by a single global **retain watermark**, `minRetainLocation`, computed as the minimum over all backends of the locations any of them still needs. Undo below the watermark is dead: no in-flight transaction can roll back into it, no live snapshot can read through it, and it is outside the checkpoint/rewind/replication retain ranges. `update_min_undo_locations()` recomputes that watermark, publishes it monotonically, advances the on-disk eviction frontier, and (on cleanup) unlinks the now-dead undo files. The bgwriter drives this periodically; the snapshot hook drives it on snapshot churn. Because the ring is bounded, reclamation is also *flow control*: a writer that would lap a still-retained slot blocks until the watermark moves. This is the exact subsystem that a permanently-`INPROGRESS` oxid jams — the recurring failure mode in the recovery-livelock and `csn-incremented` investigations.

## 1. Undo lives in fixed-size circular ring buffers — reclamation is mandatory, not optional

Each undo log is a ring of `undo_circular_buffer_size` bytes (`undo.c:308-317` splits the total across `UndoLogRegular`, `UndoLogRegularPageLevel`, `UndoLogSystem` by GUC fractions). Locations are monotonically-increasing 64-bit offsets; the physical slot is `location % circularBufferSize`. There is no per-record free list — space is recovered only by advancing the global lower bound (`minRetainLocation`) so that the region `[old_min, new_min)` may be overwritten by future writes and its backing files unlinked. If the watermark cannot advance, the ring eventually fills and all writers stall (see §6).

## 2. The three per-proc retain watermarks — what keeps undo "live"

Each backend publishes, per undo type, three atomics in `oProcData[i].undoRetainLocations[type]`:

- **`reservedUndoLocation`** — held only while a backend is mid-write of an undo record into the RAM ring; released immediately after. Protects an in-flight *write*.
- **`transactionUndoRetainLocation`** — set at the transaction's *first* undo write (`undo.c:944-959`, guarded by `overwriteTransactionRetainUndoLoc`), held for the whole transaction, and cleared to `InvalidUndoLocation` at commit/abort (`undo.c:1487`, and the abort sweep at `undo.c:2515`). This is what guarantees an in-progress transaction can still `apply_undo_stack` (roll back) — its undo must not be reclaimed while it lives.
- **`snapshotRetainUndoLocation`** — set when a snapshot is registered (`undo.c:995-999`, `:2074-2075`) to the oldest undo location that snapshot might have to traverse for MVCC; reset to `InvalidUndoLocation` when the snapshot is released (`undo.c:2065`, `:1022`). This is what guarantees a long-running reader can still walk back to the versions visible in its snapshot.

The first two protect *rollback correctness*; the third protects *MVCC read correctness*. Outdated = below all of them.

## 3. Additional global retainers (bound the watermark further down)

Beyond live procs, `update_min_undo_locations()` also clamps `minRetainLocation` against:

- **Checkpoint retain range** `[checkpointRetainStartLocation, checkpointRetainEndLocation)` (`undo.h`, read at `undo.c:537-538`) — undo of transactions in-progress at checkpoint time, kept so a post-crash recovery can roll them back. Persisted files in this range are never cleaned (`undo.c:568-579`).
- **Rewind retain** `minRewindRetainLocation` (`undo.c:509`) — when `enable_rewind`, undo is held much longer to support point-in-time rewind.
- **Replication-catalog retain** (system undo only) `replicationCatalogUndoRetainLocation` (`undo.c:449`, `:488-489`) — keeps system-tree undo logical decoding may still need.

## 4. The reclamation routine — `update_min_undo_locations()` (`undo.c:430`)

The single GC routine, under `minUndoLocationsMutex` + a crit section:

1. Reads `lastUsedLocation` as the upper bound, then scans all `max_procs` backends, taking the running `Min` of `reservedUndoLocation`, `transactionUndoRetainLocation`, and `snapshotRetainUndoLocation` (`undo.c:472-484`).
2. Forces **monotonicity** — the new values may not go backwards relative to the published `minProcRetainLocation` / `minProcTransactionRetainLocation` (`undo.c:495-498`) — then publishes them (`:500-503`).
3. Advances the eviction frontier: if nothing is mid-eviction and `writtenLocation < minRetainLocation`, it bumps `writeInProgressLocation`/`writtenLocation` up to `minRetainLocation` (`undo.c:521-528`) — undo below the retain point no longer needs to be on disk.
4. **On `do_cleanup=true`** only: advances `cleanedLocation` and calls `unlink_unretained_o_buffers()` twice (`undo.c:585`, `:593`) to physically unlink the undo buffer files in the old active-retain and old checkpoint-retain ranges that are no longer covered — this is the actual disk reclamation.

## 5. Who triggers reclamation, and when

- **bgwriter (periodic, primary driver)** — `src/workers/bgwriter.c:208`, gated to `BGWriterNum == 0`, calls `update_min_undo_locations(type, false, /*do_cleanup=*/true)` every cycle even when no eviction is needed. The in-code comment (`bgwriter.c:198-203`) is explicit that this is what lets `minProcRetainLocation` advance **on a synced replica**, where it would otherwise stay pinned at the value loaded during recovery.
- **Snapshot hook** — `orioledb_snapshot_hook()` (`undo.c:3211`) calls it with `do_cleanup=true` as snapshots come and go (snapshot release lowers a `snapshotRetainUndoLocation`, which can unblock reclamation).
- **`reserve_undo_size` path** — writers call `update_min_undo_locations(..., do_cleanup=waitForUndoLocation)` (`undo.c:1510`, `:1719`) when they need space, so reclamation is also pulled on demand.
- **Eviction** — `update_min_undo_locations(type, /*undoEviction=*/true, false)` (`undo.c:1578`) is the disk-eviction variant (no cleanup).
- **Diagnostics** — the `orioledb_get_undo_meta()` SQL function (`undo.c:3442`) forces a cleanup pass.

## 6. Back-pressure — the bounded ring makes reclamation a hard dependency

Reservation is `reserve_undo_size_extended()` (`undo.c:1658`). When the requested range would wrap past `minProcReservedLocation + circularBufferSize`, the backend calls `wait_for_reserved_location(undoType, targetUndoLocation + circularBufferSize)` (`undo.c:1504`, `:1623`) and **spins/sleeps until other procs release their reserved locations and the watermark advances**. So undo reclamation is not a lazy background nicety — if it stops, *writes* stop. Any process that holds a retain location open indefinitely throttles every writer sharing that undo log.

## 7. Why this matters for the recovery-livelock / `csn-incremented` bug

The retain machinery is the second-order victim of a stranded oxid:

- A transaction whose oxid never resolves (left `INPROGRESS`/`COMMITTING` forever — the standby in the recovery-livelock, or the post-crash limbo oxid in `csn-incremented`) **never clears its `transactionUndoRetainLocation`** (the clear at `undo.c:1487`/`:2515` only runs from a real commit/abort that never comes).
- `update_min_undo_locations()` therefore pins `minRetainLocation` at that oxid's location forever — reclamation cannot pass it.
- On the standby this compounds the visible `oxid_get_csn` spin (Q6) with a *silent* resource leak: the undo ring stops reclaiming, files below the stuck point are never unlinked, and any writer/recovery worker needing fresh undo space can additionally wedge in `wait_for_reserved_location`.
- The bgwriter comment at `bgwriter.c:200-203` is the canonical hint that this path is replica-relevant: it exists precisely because a replica's `minProcRetainLocation` set during recovery "may never be advanced" otherwise — i.e. the design already anticipates retain-watermark stalls on standbys.

## Code references

Ring buffer + sizing:
- `src/transam/undo.c:201-209` — `undoBuffersDesc` (the undo file/buffer descriptor, `UNDO_FILE_SIZE`)
- `src/transam/undo.c:308-317` — `undo_circular_buffer_size` split across the three `UndoLogType`s

Per-proc retain watermarks (set / clear):
- `src/transam/undo.c:476-483` — read of `reserved` / `transaction` / `snapshot` retain in the GC scan
- `src/transam/undo.c:944-959` — set `transactionUndoRetainLocation` at first undo write
- `src/transam/undo.c:1487`, `:2515` — clear `transactionUndoRetainLocation` at txn end / abort sweep
- `src/transam/undo.c:995-999`, `:2074-2075` — set `snapshotRetainUndoLocation` (snapshot register)
- `src/transam/undo.c:1022`, `:2065` — clear `snapshotRetainUndoLocation` (snapshot release)
- `src/transam/undo.c:1442`, `:1466` — `have_retained_undo_location()` / current snapshot-retain getter

The GC routine:
- `src/transam/undo.c:430` — `update_min_undo_locations()`
- `src/transam/undo.c:472-484` — compute `minRetainLocation` = Min over all procs
- `src/transam/undo.c:495-503` — monotonic clamp + publish (`minProcRetainLocation`, …)
- `src/transam/undo.c:509` — rewind-retain clamp (`minRewindRetainLocation`)
- `src/transam/undo.c:521-528` — advance `writtenLocation` (eviction frontier)
- `src/transam/undo.c:532-552` — `do_cleanup` bookkeeping (`cleanedLocation`, checkpoint range)
- `src/transam/undo.c:585`, `:593` — `unlink_unretained_o_buffers()` (physical file reclamation)

Global retainers:
- `src/transam/undo.c:449`, `:488-489` — replication-catalog retain (system undo)
- `src/transam/undo.c:537-538`, `:544-545` — checkpoint retain range
- `include/transam/undo.h` — `UndoMeta` fields: `minProcRetainLocation`, `minProcTransactionRetainLocation`, `minRewindRetainLocation`, `checkpointRetainStart/EndLocation`, `cleanedLocation`, `writtenLocation`

Triggers + back-pressure:
- `src/workers/bgwriter.c:198-208` — periodic `update_min_undo_locations(..., do_cleanup=true)` (the replica-relevant comment)
- `src/transam/undo.c:3211` — `orioledb_snapshot_hook()` cleanup
- `src/transam/undo.c:1658` — `reserve_undo_size_extended()`
- `src/transam/undo.c:1027` — `wait_for_reserved_location()` (def); called at `:1504`, `:1623`
- `src/transam/undo.c:3442` — `orioledb_get_undo_meta()` (SQL diagnostic cleanup)
