# undo-wraparound-retry-cap

## Focus

Resource Boundaries. Directly targets `src/transam/undo.c`, named explicitly
in this pass's assignment (the "circular buffer wraparound retry with a
'shouldn't happen twice' comment near a 2x reservation cap", `sut-analysis.md`
§11).

## What led to this

`get_undo_record()` (`src/transam/undo.c:2049-2091`):

```c
while (true)
{
    if (reserved_undo_sizes[(int) undoType] < size)
        elog(PANIC, "get_undo_record(): not enough reserved undo (...)");

    location = pg_atomic_fetch_add_u64(&meta->lastUsedLocation, size);
    reserved_undo_sizes[(int) undoType] -= size;

    /*
     * We might hit the boundary of circular buffer.  If so then just
     * retry. Thankfully we've reserved twice more space than required.
     *
     * This situation shouldn't happen twice, since we've reserved undo
     * location.
     */
    if ((location + size) % circularBufferSize > location % circularBufferSize)
    {
        mark_undo_range_dirty(undoType, location, size);
        *undoLocation = location;
        return GET_UNDO_REC(undoType, location);
    }
}
```

Mechanics, worked through:

- `lastUsedLocation` is a shared atomic counter; `pg_atomic_fetch_add_u64`
  gives this call a `[location, location+size)` slice, but **the actual
  value of `location` depends on how much every other concurrently-calling
  backend has already added**, not just this backend's own prior attempts.
- If the returned slice straddles the circular buffer's wrap boundary
  (`location % circularBufferSize + size > circularBufferSize`), the record
  can't be written as one contiguous piece at this offset, so the code
  discards this slice (it is never freed for reuse — `lastUsedLocation` only
  increases) and retries via a fresh `fetch_add`.
- Every "normal" caller reserves *exactly* `2 * size` via
  `reserve_undo_size(undoType, 2 * size)` (see e.g.
  `get_undo_record_unreserved()`, `undo.c:2103-2109`) before calling
  `get_undo_record(undoType, ..., size)`. The comment's assumption is: one
  wrap-retry costs one extra `size` unit from the 2x reservation, so at most
  one retry is affordable before `reserved_undo_sizes[type] < size` trips the
  `elog(PANIC, ...)` guard at the top of the next loop iteration.
- **The guard is genuinely self-defending, not silently wrong**: if the
  "shouldn't happen twice" assumption is violated, the code does not silently
  corrupt the undo log — it PANICs (crashes the backend/instance) via the
  explicit reservation check. So the actual risk this property targets is
  "does this ever PANIC in practice," not "does it silently corrupt state."

## Why "twice" might not be watertight (the part not addressed in `sut-analysis.md`)

Two independent reasons the single-retry assumption could be violated, both
plausible under Antithesis-style adversarial scheduling rather than normal
operation:

1. **Concurrent interleaving.** Because `lastUsedLocation` is a shared
   counter across *all* backends writing to the same `undoType`, a wrap can
   in principle be induced more than once for the same logical call if enough
   concurrent `fetch_add`s from *other* backends land in the wrap zone
   between this backend's own retries — the "we've reserved 2x" argument is a
   per-backend accounting argument, but the actual wrap outcome is a
   property of the *global* interleaving of all concurrent allocations, not
   just this backend's two attempts. Whether the buffer-size floor
   (`Max(fraction * undo_circular_buffer_size, max_procs * 2 *
   O_MAX_UNDO_RECORD_SIZE)`, `undo.c:404-411`) is large enough relative to
   `max_procs` to make a double-wrap for one backend combinatorially
   impossible, or merely *very unlikely*, was not resolved this pass — it
   would need either a formal argument about the floor's sizing relative to
   worst-case concurrent allocation patterns, or an empirical stress test.
2. **Shared reservations across multiple `get_undo_record()` calls.**
   Not every reservation is a clean `2 * size` matched to a single
   `get_undo_record()` call. `reserve_undo_for_modification()`
   (`src/btree/modify.c:1008-1022`) reserves
   `O_MODIFY_UNDO_RESERVE_SIZE = 2 * (O_MAX_SPLIT_UNDO_IMAGE_SIZE +
   O_UPDATE_MAX_UNDO_SIZE)` (`include/btree/undo.h:163`) as one combined pool
   intended to cover *up to two different record kinds* (a split-undo image
   and an update-undo record) that may each need their own
   `get_undo_record()` call within the same logical modify operation. If the
   first `get_undo_record()` call in that sequence already consumes part of
   the "spare" cushion via its own wrap-retry, the second call draws from a
   pool that may no longer have a full 2x cushion relative to *its own*
   `size` — the "shouldn't happen twice" argument is stated per-call in the
   comment, but the reservation accounting (`reserved_undo_sizes[type] -=
   size` on every `fetch_add`, shared across all calls against that
   `undoType` until `release_undo_size()`) is per-transaction-scope, not
   strictly per-call. Whether `O_MODIFY_UNDO_RESERVE_SIZE`'s specific
   constant already accounts for this (i.e., is sized so that even a
   worst-case single wrap on the first call still leaves a valid 2x cushion
   for the second) was not traced through the arithmetic this pass — flagged
   as an open question below rather than asserted either way.

## Property

| | |
|---|---|
| **Type** | Reachability / Safety |
| **Property** | `get_undo_record()`'s circular-buffer wraparound retry never needs a second retry in the same call under concurrent, adversarial scheduling — i.e., the `elog(PANIC, "not enough reserved undo...")` guard is never actually tripped by the wraparound path (as opposed to a genuine caller bug). |
| **Invariant** | `Reachable("undo wraparound retry taken")` as an exploration hint (confirms Antithesis's fault/scheduling injection actually lands allocations on the buffer-wrap boundary at all — currently this branch has no visibility from outside), paired with `Unreachable("get_undo_record PANIC: not enough reserved undo")` as the actual safety claim — the PANIC message is specific and greppable in Postgres logs, so this can likely be implemented as a log-pattern check even without new SUT-side instrumentation, though a direct `Unreachable` assertion at the `elog(PANIC, ...)` call site would be more precise and immediate. |
| **Antithesis Angle** | This needs (a) concurrent writers against a table with a small/near-floor `undo_circular_buffer_size` and low `max_procs` to make wrap-boundary crossings frequent rather than rare, and (b) Antithesis's scheduling-fault injection to maximize the chance of the multi-backend interleaving scenario in point 1 above landing two wraps in the same call's short retry window — a pure single-process test cannot exercise the "other backends' concurrent fetch_adds" mechanism at all. |
| **Why It Matters** | If reachable, the failure mode is a backend/instance PANIC (crash), not silent corruption — still a real availability hit, and notably it would be a crash whose trigger (adversarial concurrent scheduling around a buffer-size boundary) is exactly what makes it hard to find via the deterministic test suites (`sut-analysis.md` §9) and easy to find via Antithesis's fault-guided search. |

**Open Questions:**

- Is the undo circular buffer sized (relative to `max_procs` and
  `O_MAX_UNDO_RECORD_SIZE`) such that a double-wrap for a single backend's
  single `get_undo_record()` call is combinatorially impossible, or merely
  empirically rare? No formal argument found this pass; the floor
  `max_procs * 2 * O_MAX_UNDO_RECORD_SIZE` (`undo.c:405,408,411`) is the only
  sizing guarantee identified. `(partial: floor formula located, no proof of sufficiency examined)`
- Does `O_MODIFY_UNDO_RESERVE_SIZE`'s constant already account for a
  worst-case single wrap consumed by the first of two `get_undo_record()`
  calls sharing one reservation pool, leaving the second call's 2x cushion
  intact? Not traced through the arithmetic of `O_MAX_SPLIT_UNDO_IMAGE_SIZE`/
  `O_UPDATE_MAX_UNDO_SIZE` this pass.
- Is this reachable at all in an assert-disabled (production-style) build —
  i.e., is `elog(PANIC, ...)` here unconditional (yes, `elog(PANIC,...)` is
  not `Assert()`-gated, confirmed by reading the code directly — this is a
  real, always-compiled-in crash path, not a debug-only check), which makes
  it a legitimate Antithesis target regardless of build flags (contrast with
  the `Assert()`-only `globalXmin` monotonicity invariant flagged as a build-flag-dependent open question in `sut-analysis.md`'s top-level Assumptions section).

## SUT-side instrumentation

No existing assertions near `src/transam/undo.c` (`existing-assertions.md`:
0 hits in `src/`). Suggested (both **missing**):
- `Reachable("get_undo_record hit wraparound retry", {undoType, location, size})` in the wrap branch (the `else` implicit path — i.e., where the loop doesn't return and iterates again) — gives Antithesis positive confirmation the interesting boundary condition is being explored at all, since today it's invisible.
- `Unreachable("get_undo_record: reserved-undo-size exhausted by repeated wraparound")` co-located with (or replacing the need to log-grep for) the existing `elog(PANIC, "get_undo_record(): not enough reserved undo...")` call.

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

#### Is the undo circular buffer sized such that a double-wrap for a single backend's single call is combinatorially impossible, or merely empirically rare?

- Examined: `get_undo_record()` (`src/transam/undo.c:2049-2091`); the buffer-size floor formula `Max(fraction * undo_circular_buffer_size, max_procs * 2 * O_MAX_UNDO_RECORD_SIZE)` (`undo.c:404-411`).
- Found: the floor formula ties minimum buffer size to `max_procs`, suggesting a deliberate intent to bound concurrent wrap collisions.
- Not found: no formal argument or empirical stress test confirming this floor actually makes a second wrap in the same call's retry window combinatorially impossible, versus merely unlikely.
- Conclusion: tagged `(partial: floor formula located, no proof of sufficiency examined)`.
