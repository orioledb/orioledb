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
