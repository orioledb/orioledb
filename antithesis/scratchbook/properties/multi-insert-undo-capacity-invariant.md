# multi-insert-undo-capacity-invariant

## Focus

Concurrency (attention focus 2).

## What led to this

`src/tableam/operations.c:508-563`, function `o_tbl_multi_insert()` (the batched multi-row insert path used for `COPY`/multi-VALUES `INSERT`/`INSERT...SELECT`, invoked from `src/tableam/handler.c:1886-1891`). The function's own header comment is unusually detailed about a tricky correctness/capacity coupling — this is exactly the kind of self-documented tricky invariant worth turning into a property:

> "Phase 3: stream sorted keys through primary leaves, holding each leaf's lwlock for as many adjacent keys as fit... Each iteration tops up row-undo for the upcoming batch, capped at `2 * O_MAX_UNDO_RECORD_SIZE` so `max_procs` concurrent multi_inserts can't outrun the row buffer; larger inputs split across iterations."

Two distinct concurrency-relevant claims are bundled here:

1. **Monotonicity/correctness claim (Phase 2):** the batch-insert helper's leaf probe "detects 'key past this leaf's hikey' but not 'key before this leaf's lokey' — lokey lives in the parent, not the leaf, so a key < lokey would silently corrupt the downlink invariant" (comment at `operations.c:630-637`). The code optimistically assumes `keys[]` are ascending, does an O(n) verify scan (`operations.c:645-652`), and only falls back to a full `qsort_arg` + permutation-array rebuild (`operations.c:654-682`) if the fast-path check finds an out-of-order pair. This is a correctness-under-concurrency-adjacent invariant: it's not itself a race between threads, but it's exactly the kind of "assumed-sorted input, unenforced by the type system" pattern that a single missed comparison (an off-by-one in the fast-path scan, or a `qsort_arg` comparator that ties inconsistently) would silently corrupt a shared B-tree structure that concurrent readers/other backends rely on.

2. **Capacity/concurrency-boundary claim (Phase 3):** verified directly against the undo sizing code, not just cited:
   - `src/tableam/operations.c:719-741` — the per-iteration batching loop bounds cumulative undo need (`need`) against `2 * O_MAX_UNDO_RECORD_SIZE`, breaking the batch (`batch = k`) before exceeding it, then calls `reserve_undo_size(pdesc->undoType, need)`.
   - `src/transam/undo.c:405-411` — the undo circular buffers are sized as `Max(..., max_procs * 2 * O_MAX_UNDO_RECORD_SIZE)` for `UndoLogRegular`, `UndoLogRegularPageLevel`, and `UndoLogSystem` — i.e., the shared buffer's capacity assumption is exactly "at most `max_procs` backends, each holding at most `2 * O_MAX_UNDO_RECORD_SIZE` reserved at once."
   - `src/transam/undo.c:1879-1880` — `Assert(size <= 2 * O_MAX_UNDO_RECORD_SIZE); Assert(reserved_undo_sizes[(int) undoType] <= 2 * O_MAX_UNDO_RECORD_SIZE);` — the invariant is only checked via `Assert()`, a no-op in non-assert builds (same caveat class as the `globalXmin` monotonicity assert noted in `sut-analysis.md` §4).

So the concurrency claim is: **as long as at most `max_procs` backends are ever concurrently reserving undo space, and each caps its own reservation at `2 * O_MAX_UNDO_RECORD_SIZE`, the shared circular undo buffer never overflows/wraps into space another backend still needs.** This is a resource-boundary invariant that is fundamentally about concurrent access to a shared bounded buffer — squarely a concurrency property, not just a sizing one.

## Edge case checked and ruled out (documented for the record)

Considered whether a single oversized tuple could make `need` exceed the cap on the very first item of a batch (`k == 0`), since the loop's break condition is guarded by `k > 0` (`operations.c:733`: `if (k > 0 && need + one + Max(maxrow, one) > 2 * O_MAX_UNDO_RECORD_SIZE) break;`) — meaning the first tuple in a batch is always included regardless of size. `O_MAX_UNDO_RECORD_SIZE` is `O_MERGE_UNDO_IMAGE_SIZE` = `MAXALIGN(sizeof(UndoPageImageHeader)) + ORIOLEDB_BLCKSZ * 2` (`include/btree/undo.h:165,173`) — i.e., roughly two full pages (~16KB+ at default `BLCKSZ`), while a single row's undo need is `MAXALIGN(sizeof(BTreeModifyUndoStackItem) + tuplen)` and `tuplen` is bounded well under one page by `o_btree_check_size_of_tuple()` (TOAST kicks in for larger values). So a single tuple's undo need is far below `2 * O_MAX_UNDO_RECORD_SIZE`, and this specific edge case does not appear reachable in practice. Not pursued as its own property, but recorded here so a future reader doesn't have to re-derive this.

## The property

**Type:** Safety.

**Property:** Under concurrent batched multi-row inserts from up to `max_procs` backends, (a) the per-backend undo reservation never exceeds `2 * O_MAX_UNDO_RECORD_SIZE` (the `Assert()`-checked invariant in `undo.c:1879-1880` never trips), and (b) the Phase 2 monotonicity check + qsort fallback never lets an out-of-order key reach the leaf-probe helper (which would silently corrupt the downlink invariant per the code's own comment) — checkable indirectly via `orioledb_tbl_check()` / amcheck-style structural verification finding no corruption after concurrent multi-row inserts complete.

**Invariant:** `Always` for both sub-claims, since a debug/assert-enabled Antithesis build turns `Assert()` failures into crashes Antithesis can catch (assuming assert-enabled builds are actually what Antithesis runs — see Open Questions in `sut-analysis.md`'s file-level notes, not re-litigated here). For the monotonicity claim specifically: `always(orioledb_tbl_check_passes_after_concurrent_multi_insert)`, run periodically during/after a workload that does concurrent `COPY`/multi-VALUES `INSERT` with intentionally non-monotone explicit-PK ordering (to force the qsort fallback path) from many concurrent backends.

**Antithesis Angle:** Drive `max_procs`-adjacent concurrency (many simultaneous backends, each doing large `COPY`/multi-row `INSERT` batches with non-sequential PK values to force the sort fallback) while Antithesis injects scheduling delays around the per-iteration `reserve_undo_size()`/leaf-probe boundary. The interesting adversarial angle is squeezing more concurrent reservations into the shared buffer than the `max_procs` sizing assumption expects — e.g., if background workers (autovacuum-equivalent, bgwriter, s3workers) that aren't counted in `max_procs` can also hold undo reservations of the same undo types concurrently with backend multi-inserts, the buffer could be undersized relative to actual concurrent demand. This pass did not confirm or rule out whether `max_procs` (checked against `MaxConnections`/`max_worker_processes`-derived sizing, not fully traced here) actually accounts for every process type that can reserve `UndoLogRegular`/`UndoLogSystem` space.

**Why It Matters:** This is exactly the kind of shared-bounded-buffer-vs-concurrent-demand mismatch that produces silent corruption rather than a clean error — if the buffer sizing assumption is ever violated, one backend's undo data could be overwritten before another backend expected it to be, which (given undo is the backbone of OrioleDB's entire MVCC/rollback model) is a severe, hard-to-diagnose data-integrity failure.

**Open Questions:**

- Does `max_procs` (the sizing input to `undo.c:405-411`) include every process type capable of holding an undo-type reservation, or only regular backends? If workers are undercounted, the "at most `max_procs` concurrent reservations" premise the whole capacity argument rests on would be false. `(needs human input)` — resolving this requires tracing `max_procs`'s definition/assignment site, which this pass did not do (out of scope given the five assigned leads; flagging as a natural follow-up for this property specifically).
- Whether the Phase 2 O(n) monotonicity-verify scan and the `qsort_arg` fallback have ever been fuzzed against adversarial (not just accidentally-unsorted) key sequences designed to probe comparator edge cases (ties, `o_btree_cmp` on borderline key encodings). Not investigated in this pass.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** No assertion anywhere touches `o_tbl_multi_insert`, undo reservation capacity, or the monotonicity/qsort fallback path. This is an especially good candidate for a SUT-side `reachable()` or counter at the qsort-fallback branch (`operations.c:654`, "if (!sorted)") — confirming the slow path is actually exercised under a concurrent workload is not otherwise observable from outside, and per `references/property-catalog.md`'s guidance on `Sometimes`/`Reachable` semantics, "the qsort fallback path was taken" is a meaningful state worth confirming Antithesis's workload actually reaches.

## Scope note (added by evaluation pass, R15)

This property, like the other undo-retention properties in this catalog
(`sk-fixup-undo-recycling-drop`, `replica-undo-reclaimed-too-early`,
`undo-wraparound-retry-cap`, `multi-insert-undo-capacity-invariant`),
implicitly exercises only the `enable_rewind=false` branch of shared
undo-retention logic — `orioledb.enable_rewind` is never set to `true`
anywhere in `test/antithesis/`. Flagged here explicitly since it was never
stated before; assessed as low-risk because `enable_rewind` is
`PGC_POSTMASTER` (fixed at server start, not a runtime-mutable/session-level
GUC that could flip mid-test), and rewind is out of this catalog's scope
regardless per the top-of-file scope restriction.

### Investigation Log

#### Does `max_procs` (the sizing input to the undo buffer's capacity) include every process type capable of holding an undo-type reservation, or only regular backends?

- Examined: `src/transam/undo.c:405-411` (undo circular buffer sizing, `Max(..., max_procs * 2 * O_MAX_UNDO_RECORD_SIZE)`).
- Found: the undo buffer's capacity assumption rests entirely on `max_procs`; the capacity math itself was verified directly against the code.
- Not found: `max_procs`'s own definition/assignment site was not traced — whether it's derived from `MaxConnections`/`max_worker_processes` in a way that covers every process type (bgwriter, s3workers, other background workers) that can reserve `UndoLogRegular`/`UndoLogSystem` space, or only regular backends.
- Conclusion: tagged `(needs human input)` — resolving requires tracing `max_procs`'s definition, out of scope for this pass given the five assigned leads.
