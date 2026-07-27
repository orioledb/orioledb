---
slug: disk-leaf-header-read-before-validation
attention_focus: Protocol Contracts
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# disk-leaf-header-read-before-validation

## Focus

Protocol Contracts, binary-format angle. This directly answers the task
prompt's specific question ("does a corrupted page ever get silently used
after only a WARNING?") raised from `sut-analysis.md` §4's note on
`check_orioledb_page_checksum` (`src/btree/io.c:1307-1330`) only issuing a
`WARNING` on checksum mismatch. Investigating that note by tracing every
caller of `read_page_from_disk()` surfaced a concrete ordering bug in one
specific caller, distinct from (and sharper than) the general "warn vs error"
framing in the SUT analysis.

## What was examined

- `src/btree/io.c:1307-1330` (`check_orioledb_page_checksum`): on mismatch,
  `ereport(WARNING, ERRCODE_DATA_CORRUPTED, ...)` and returns `false`. The
  `TODO: maybe ereport(ERROR) here once caller cleanup is safe` comment at
  line 1326 originally read as "corrupted pages might get silently used."
- Traced every caller of `read_page_from_disk()` (which propagates the
  checksum result as `OReadPageResultChecksumFailed`) to see whether *any*
  caller uses the freshly-read buffer before checking that result:
  - `src/checkpoint/checkpoint.c:5624-5643` — checks `read_result !=
    OReadPageResultOk` immediately after the read, before touching `buf`.
    Correct.
  - `src/btree/io.c:1708-1728` (a downlink-fetch path) — checks
    `read_result != OReadPageResultOk` immediately after the read, before
    doing anything else with the buffer (restores the downlink, unlocks IO,
    then `ereport(ERROR)`). Correct.
  - `src/btree/scan.c:1537-1591` (`load_next_disk_leaf_page`, a sequential
    B-tree scan reading a leaf page straight from disk) —
    ```c
    read_result = read_page_from_disk(scan->desc, scan->leafImg,
                                       downlink.downlink, &extent);
    header = (BTreePageHeader *) scan->leafImg;
    if (header->csn >= downlink.csn)
        read_page_from_undo(scan->desc, scan->leafImg, header->undoLocation,
                             downlink.csn, NULL, BTreeKeyNone, NULL);

    STOPEVENT(STOPEVENT_SCAN_DISK_PAGE, ...);

    if (read_result != OReadPageResultOk)
    {
        if (read_result == OReadPageResultChecksumFailed)
            ereport(ERROR, ERRCODE_DATA_CORRUPTED, "invalid leaf page ...");
        else
            elog(ERROR, "can not read leaf page from disk");
    }
    ```
    Here `header = (BTreePageHeader *) scan->leafImg` dereferences the buffer,
    and — if `header->csn >= downlink.csn` — `read_page_from_undo()` is called
    using `header->undoLocation` taken from that same buffer, **before**
    `read_result` is checked at all. This happens whether `read_result` is
    `OReadPageResultOk`, `OReadPageResultChecksumFailed`, or
    `OReadPageResultIOError`.

## Why this matters (what goes wrong)

- On a **checksum failure**: `scan->leafImg` genuinely contains the bytes read
  from disk (they failed the checksum, i.e. are corrupted, but the buffer was
  fully populated). The code reads `header->csn` and `header->undoLocation`
  from that corrupted buffer and, if the (corrupted, essentially random)
  `csn` field happens to compare `>=` downlink.csn, calls
  `read_page_from_undo()` with a garbage `undoLocation` — before the
  subsequent `ereport(ERROR)` ever fires. Depending on what `read_page_from_disk`
  does downstream, this is at minimum wasted work on garbage data, and at
  worst a wild/out-of-range undo-location dereference reached with corrupted
  input, ahead of the validation that was supposed to gate it.
- On an **I/O error** (`btree_smgr_read(...) != read_size`,
  `io.c:1390-1392`/`1417-1419`): `read_page_from_disk` returns
  `OReadPageResultIOError` without necessarily having filled `scan->leafImg`
  with the intended page's bytes at all (a short read leaves the buffer
  containing whatever was there before — plausibly a *different*, stale
  page's contents from a prior scan iteration, since `scan->leafImg` is a
  reused private buffer, not zeroed between reads per the comment at
  `scan.c:1593-1598` about partial-read state). The header-read-then-maybe-
  undo-overlay logic runs against that stale/foreign page's fields, again
  before the error check.
- Either way, this inverts the intended contract implied by the very existence
  of `OReadPageResult` as a tri-state return (Ok / IOError / ChecksumFailed):
  the result is supposed to gate whether the buffer's contents are trusted,
  but one call site consults the buffer's contents to decide on a further
  action (fetching undo data) *before* consulting that gate.

## Antithesis angle

This is the sharpest, most directly fault-injectable finding in this pass: a
disk-level bit-flip fault targeted at an on-disk B-tree leaf page during an
active sequential scan (`orioledb_seqscan`/table scan path) that goes through
`load_next_disk_leaf_page` is exactly the scenario that reaches this code.
Assert: whenever a leaf-page checksum failure or I/O error occurs during a
disk-backed sequential scan, no undo-stack read (`read_page_from_undo`) is
attempted using that page's header fields — i.e., the validation gate must run
*before* any use of `header->csn`/`header->undoLocation`, not after. This can
be checked either as a `Reachable`-anchored assertion at the top of
`read_page_from_undo` correlated with a preceding checksum failure on the same
scan, or (better, if a stopevent/instrumentation point is added) as a direct
`Always` assertion inside `load_next_disk_leaf_page` guarding the ordering.

## Open Questions

- What does `read_page_from_undo` actually do with an `undoLocation` value
  that is garbage (from a corrupted page) or belongs to a different page
  (stale buffer after a short read)? Does it validate the location range
  before dereferencing, or could this manifest as an out-of-bounds undo-log
  read/crash rather than a clean error? Not traced into
  `read_page_from_undo`'s body in this pass. `(needs follow-up: read
  read_page_from_undo in src/btree/undo.c or wherever it's defined)`
- Is `scan->leafImg` guaranteed to be zeroed or otherwise safely initialized
  before the *first* call to `load_next_disk_leaf_page` in a scan (so the
  "stale foreign page" scenario is only possible on the second and later
  reads within one scan, not the first)? Not confirmed.
- Whether this ordering bug is reachable outside of `orioledb_checksums_enabled
  = on` (does the code path differ when checksums are disabled, e.g. does
  `read_result` only ever get to `OReadPageResultChecksumFailed` when the GUC
  is on, making this a checksums-enabled-only finding)? Confirmed from
  `io.c:1397-1398`/`1424-1425`: yes, the checksum check itself is gated by
  `orioledb_checksums_enabled`. Checked the default and harness config:
  `orioledb_checksums_enabled` defaults to `true` (`src/orioledb.c:138`) and
  is never overridden anywhere in `test/antithesis/` (no `checksum` hits in
  that tree), so the existing harness already runs with checksums on and this
  path is reachable without any new config — resolved, not open.
