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
