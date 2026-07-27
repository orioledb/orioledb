# page-pool-exhaustion-fails-safe

## Focus

Resource Boundaries / Infrastructure faults — filling the gap flagged by
evaluation: "no property covers ... shared-memory/backend OOM." OrioleDB's
in-memory B-tree page pool (`orioledb.main_buffers` and siblings —
`OPagePool`, distinct from Postgres's own `shared_buffers`) is a fixed-size
shared-memory arena that every backend must successfully reserve space from
before modifying any page. This is the concrete mechanism behind "what
happens when shared memory allocation for undo/B-tree structures fails under
memory pressure" — not process-level OOM-killer activity (which this pass
found has no OrioleDB-specific twist beyond ordinary crash-restart, already
covered by the catalog's many crash/recovery properties), but the *fixed-size
shared arena filling up under legitimate concurrent load*, which is a
resource-exhaustion mechanism distinct from both existing undo-buffer
properties (`multi-insert-undo-capacity-invariant`,
`undo-wraparound-retry-cap`) — those are about the *undo log's* circular
buffer; this is about the *page pool* (in-memory B-tree page cache), a
different shared structure with its own reservation/eviction machinery.

## What led to this

`o_ppool_reserve_pages()` (`src/utils/page_pool.c:176-217`) is the entry
point every page-modifying operation calls *before* taking any page lock
(the function's own comment explains why: eviction itself takes page locks,
so reserving first avoids a caller holding a lock while triggering more
locking). It attempts an atomic reservation
(`pg_atomic_sub_fetch_u64(o_pool->availablePagesCount, count)`), and if the
pool doesn't have enough free pages, falls into a retry loop that invokes
`ppool_run_maintenance()` (a clock-sweep eviction algorithm) to try to free
some pages:

```c
while (pg_atomic_sub_fetch_u64(o_pool->availablePagesCount, count) & (UINT64CONST(1) << 63))
{
    pg_atomic_add_fetch_u64(o_pool->availablePagesCount, count);
    if (!ppool_run_maintenance(pool, true, NULL))
    {
        o_stop_saving_inval_messages(was_saving);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("orioledb page pool is exhausted"),
                 errhint("Increase \"orioledb.main_buffers\" or reduce "
                         "the number of tables accessed in a single "
                         "transaction.")));
    }
}
```

`ppool_run_maintenance()` → `o_ppool_run_maintenance()`
(`src/utils/page_pool.c:379-483`) implements a bounded clock-sweep: it walks
pages via `ucm_next_blkno()`, calling `CHECK_FOR_INTERRUPTS()` every
iteration (interruptible, not a hard spin), and tracks
`skippedLocalEvictions` against a computed limit
(`skippedLocalEvictionsLimit = pool->size * UCM_USAGE_LEVELS`,
`page_pool.c:431`). If a full local sweep finds nothing evictable *and* no
concurrent process advanced the shared `pageEvictCount` in the meantime, it
sets `exhausted = true` and returns `false` — a genuine, bounded "give up"
signal, not an infinite loop. The caller then raises a clean, catchable
`ereport(ERROR, ERRCODE_OUT_OF_MEMORY, ...)` with an actionable hint, rather
than crashing or hanging.

This is, on direct reading, a well-engineered resource-exhaustion path —
similar in spirit to `disk-write-enospc-fails-safe`'s finding for disk writes:
the codebase gets the *common* case right. The two things that make this
still worth an Antithesis property rather than a pure "nothing to see here"
note:

1. **A documented, intentionally-supported nested-recursion case.** The
   function's own comments (`page_pool.c:194-199`, `410-428`) describe a
   real nested-call scenario: reserving pages for one pool
   (e.g. the main table pool) can, inside its own eviction sweep
   (`walk_page()` → `walk_page_prelock_check()` →
   `index_oids_get_btree_descr()`), need to load a TOAST table descriptor
   from a *different* pool (`OPagePoolFreeTree` or `OPagePoolCatalog`),
   which recursively calls `ppool_reserve_pages()`/`ppool_run_clock()` again
   for that second pool. The code explicitly guards this with
   `Assert(ppool_run_clock_depth <= 1)` and asserts the inner pool is one of
   exactly two expected types and differs from the outer pool
   (`page_pool.c:411-421`) — i.e., the code assumes recursion never goes
   deeper than one level, and never re-enters the *same* pool type. Whether
   an adversarial workload that concurrently exhausts *multiple* pool types
   at once (main data pages + TOAST/free-tree + catalog pages all under
   memory pressure simultaneously) can violate either assumption was not
   traced to a conclusion this pass — the `Assert()`-only depth check means
   a violation would be silent in a non-assert (release) build, per the same
   build-flag caveat the catalog's file-level Open Questions already raise
   for other `Assert()`-only invariants (`replica-xmin-monotonicity`,
   `multi-insert-undo-capacity-invariant`).
2. **Zero existing test/Antithesis coverage.** No config in
   `test/antithesis/` sets `orioledb.main_buffers` (or the other pool-sizing
   GUCs) to a small value to deliberately force exhaustion, and no test in
   `test/sql`/`test/t`/`test/specs` was found (not exhaustively grepped this
   pass) exercising the `"orioledb page pool is exhausted"` error message
   under real concurrent adversarial load — the clean-ERROR path exists and
   looks correct, but has never actually been exercised by a workload
   designed to hit it, let alone one combined with Antithesis's scheduling
   fault injection widening the exact window between reservation-failure and
   retry.

## Property

| | |
|---|---|
| **Type** | Safety (verified-mostly-correct contract; the multi-pool nested-recursion edge case is a genuine, unresolved lead rather than a confirmed bug) |
| **Property** | When OrioleDB's in-memory page pool is genuinely exhausted (every page pinned/dirty/unevictable under concurrent load), every backend attempting to reserve pages for a new operation eventually receives a clean, catchable `ERROR` (`ERRCODE_OUT_OF_MEMORY`, "orioledb page pool is exhausted") — never a hang, a crash, or (worse) proceeding without having actually reserved the space it needed — and this holds even when the exhaustion is discovered through a nested reservation call for a different pool type (main pool eviction needing a TOAST/free-tree/catalog page from a second pool) rather than a single flat reservation. |
| **Invariant** | `Always(page_pool_exhaustion_yields_clean_ERROR_never_hang_or_crash)`, confirmed by a workload that reserves enough concurrent pinned/dirty pages across enough distinct backends to guarantee `o_ppool_run_maintenance()`'s bounded sweep finds nothing evictable. Paired with `Sometimes(nested_pool_reservation_path_taken_during_exhaustion)` — a `Sometimes` targeting the specific `ppool_run_clock_depth > 0` branch — to confirm the harder, TOAST/free-tree/catalog nested case (point 1 above) is actually exercised at all under a fault-injection-driven run, since today nothing forces it. |
| **Antithesis Angle** | A table with a TOASTed column and at least one secondary index (to guarantee catalog/free-tree/TOAST page-pool traffic interleaves with main-pool traffic) plus a workload configured with a small `orioledb.main_buffers`/`orioledb.undo_buffers`-adjacent GUC set and many concurrent long-running transactions each touching many distinct tables/rows (to pin/dirty pages across the pool without releasing them) — this deliberately manufactures the exhaustion condition rather than waiting for chance. Antithesis's scheduling-fault injection adds value by widening the window between one backend's reservation-failure retry and another concurrent backend's eviction, maximizing the chance the nested multi-pool-type recursion path (point 1) is actually reached rather than only the simpler single-pool exhaustion. |
| **Why It Matters** | This is the concrete, checkable answer to "what happens when shared memory allocation for undo/B-tree structures fails under memory pressure" that the evaluation gap asked for — and unlike disk-write failure handling, this path has a real, documented, not-fully-traced edge case (the nested-recursion depth assumption) sitting directly on top of an otherwise well-designed bounded-retry mechanism. If that assumption is ever violated under real concurrent adversarial exhaustion across multiple pool types, the failure mode in a release (non-assert) build is unknown — potentially a silently wrong `skip_ucm`/`outer_pool` bookkeeping state rather than a clean error, which would be a materially worse outcome than the intended, already-implemented clean-ERROR path. |

**Open Questions:**

- Can a real workload drive OrioleDB into the nested (`ppool_run_clock_depth
  > 0`) recursion path concurrently with the *same* pool also being
  independently exhausted from a different backend, in a way that stresses
  the `Assert(pool != outer_pool)`/`Assert(ppool_run_clock_depth <= 1)`
  assumptions rather than merely reaching the nested branch harmlessly?
  `(needs further investigation — the recursive call chain's necessary
  preconditions (TOAST descriptor lookup mid-eviction) were read, but not
  exercised against a deliberately adversarial concurrent multi-pool
  workload)`
- In a non-assert (release) build, if the recursion-depth or pool-identity
  assumption is ever violated, what actually happens — does `skip_ucm`
  bookkeeping (`page_pool.c:427-428,479-482`) get left in an inconsistent
  state for a *different* concurrent caller, given it's process-local (not
  per-call) state guarding a global flag? Not traced to a conclusion; this is
  the crux of whether the Assert-only guard is load-bearing or merely
  documentation. `(needs further investigation)`
- Are Antithesis's target build images compiled with `Assert()` enabled? This
  is the same catalog-wide open question already raised for
  `replica-xmin-monotonicity`/`multi-insert-undo-capacity-invariant` — it
  applies identically here, since the recursion-depth/pool-identity guards
  are `Assert()`-only. `(needs human input — catalog-wide, not specific to this property)`
- Does any existing SQL-level GUC combination make it *impossible* in
  practice to reach genuine exhaustion (e.g. a floor on `main_buffers`
  relative to `max_connections` analogous to the undo buffer's `max_procs`
  floor, `undo.c:404-411`) — or is exhaustion realistically reachable under
  ordinary heavy concurrent load with default settings, not just deliberately
  tiny `main_buffers`? Not checked this pass — the reservation code was read,
  but the pool-sizing GUCs' own floor/minimum logic (analogous to
  `min_pool_size = Max(PPOOL_MIN_SIZE_BLCKS, max_procs * 4)` for the undo
  buffer, `orioledb.c:494`) was not cross-checked against realistic
  concurrent-transaction-count scenarios.

## SUT-side instrumentation

`existing-assertions.md`: 0 hits in `src/utils/page_pool.c` or `ucm.c`
(**missing**). Suggested (both **missing**):
- `Reachable("page pool reservation entered nested clock sweep", {pool_type, outer_pool_type, depth})` at the `ppool_run_clock_depth > 0` branch (`page_pool.c:413-418`) — gives direct, positive confirmation the harder nested-multi-pool-type path was ever exercised, since today it's invisible from outside and the assert-only guard gives no signal in a release build even when it's *not* violated.
- `Reachable("orioledb page pool exhausted", {pool_type, size, reserved_kind})` co-located with the `ereport(ERROR, ..., "orioledb page pool is exhausted", ...)` call site (`page_pool.c:205-210`) — confirms a fault-injection run actually forced genuine exhaustion rather than the invariant holding vacuously because exhaustion was never reached.

### Investigation Log

#### Can a real workload drive the nested (`ppool_run_clock_depth > 0`) recursion path concurrently with the same pool also being independently exhausted, stressing the `Assert(pool != outer_pool)`/`Assert(ppool_run_clock_depth <= 1)` assumptions rather than merely reaching the branch harmlessly?

- Examined: `page_pool.c:194-199` and `410-428` (comments describing the nested-call scenario — main-pool eviction needing a TOAST/free-tree/catalog page from a second pool), and the guards at `page_pool.c:411-421`.
- Found: the code explicitly documents and guards a one-level-deep nested reservation case with `Assert(ppool_run_clock_depth <= 1)` and an assert that the inner pool is one of two expected types and differs from the outer pool.
- Not found: whether an adversarial workload that concurrently exhausts multiple pool types at once can actually violate either assumption — not exercised against a deliberately adversarial concurrent multi-pool workload.
- Conclusion: tagged `(needs further investigation — the recursive call chain's necessary preconditions (TOAST descriptor lookup mid-eviction) were read, but not exercised against a deliberately adversarial concurrent multi-pool workload)`.

#### In a non-assert (release) build, if the recursion-depth or pool-identity assumption is ever violated, what actually happens — does `skip_ucm` bookkeeping get left in an inconsistent state for a different concurrent caller?

- Examined: `skip_ucm` bookkeeping at `page_pool.c:427-428,479-482`.
- Found: `skip_ucm` is process-local (not per-call) state guarding a global flag, which is the mechanism that would be at risk if the recursion-depth/pool-identity assumption were ever violated.
- Not found: what actually happens if the assumption is violated in a non-assert build — not traced to a conclusion.
- Conclusion: tagged `(needs further investigation)` — this is the crux of whether the Assert-only guard is load-bearing or merely documentation.

#### Are Antithesis's target build images compiled with `Assert()` enabled?

- Examined: this is the same catalog-wide open question already raised for `replica-xmin-monotonicity`/`multi-insert-undo-capacity-invariant`; no independent investigation was performed for this property specifically.
- Found: the recursion-depth/pool-identity guards in this file are `Assert()`-only, so the question applies identically here.
- Not found: whether Antithesis's actual target build has asserts enabled — unresolved catalog-wide, not just for this file.
- Conclusion: tagged `(needs human input — catalog-wide, not specific to this property)`.
