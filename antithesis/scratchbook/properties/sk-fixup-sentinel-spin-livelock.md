# sk-fixup-sentinel-spin-livelock

## Merge note

Merges two independently-written files describing the same finding:
`sk-fixup-waitingloc-checkpoint-spin.md` and this file
(`sk-fixup-sentinel-spin-livelock.md`, kept as the canonical slug — has the
fuller catalog-format property table).

## Focus

Failure Recovery / Idempotency and Replay. Targets `sut-analysis.md` §6/§11's note: "A 'new' livelock possibly introduced by the #855 fix itself" — the self-created-table fast path in `checkpoint_write_pending_sk_fixups()` spins unboundedly with no `CHECK_FOR_INTERRUPTS()`.

## What led to this

`src/checkpoint/checkpoint.c:1017-1041`, inside `checkpoint_write_pending_sk_fixups()` (the same function underlying `sk-fixup-undo-recycling-drop.md`), read directly:

```c
for (i = 0; i < max_procs; i++)
{
    UndoLocation pendingLoc;
    OXid oxid;
    int level;

    /*
     * If the backend is in the PK-applied/SK-pending window on a
     * self-created table (signalled by WaitingSkUndoLoc), there is no
     * undo record to point a fix-up entry at -- but the table is private
     * to that in-progress txn, so the SK btree_modify cannot be blocked
     * by anyone.  Spin until the sentinel clears.
     *
     * Spin OUTSIDE the proc's flushLock: the backend's own commit/abort
     * path also acquires that lock (walk_undo_stack); holding it across
     * the sleep would deadlock against any backend that would clear the
     * sentinel at commit time.
     */
    for (;;)
    {
        pendingLoc = pg_atomic_read_u64(&oProcData[i].pendingSkUndoLoc);
        if (pendingLoc != WaitingSkUndoLoc)
            break;
        pg_usleep(100L);
    }
    ...
```

`WaitingSkUndoLoc` (`include/orioledb.h:152-162`) is a sentinel set by PK `btree_modify` for a **self-created table** (no undo record exists to point a fix-up at, because the table is private to the still-in-progress transaction).

This loop is the checkpoint process itself (`o_perform_checkpoint()` calling `checkpoint_write_pending_sk_fixups()`, called from `checkpoint_tables_callback()` per the function's own header comment) — i.e., **the checkpoint blocks on every single `max_procs` slot in sequence**, spinning at 100μs granularity with no `CHECK_FOR_INTERRUPTS()` anywhere in this specific loop, until whichever backend set `WaitingSkUndoLoc` for a self-created table clears the sentinel (at its own commit/abort time).

The comment's own justification is explicitly a **deadlock**-avoidance argument only ("Spin OUTSIDE the proc's flushLock... holding it across the sleep would deadlock") — it says nothing about a liveness bound on how long the spin itself can run, and the loop has no escape valve (no timeout, no interrupt check, no iteration cap) if the backend that set the sentinel never reaches commit/abort promptly (e.g., it is itself blocked, descheduled, or killed in a way that leaves the sentinel set without clearing it — see Open Questions).

## Why this specifically deserves a property distinct from `sk-fixup-undo-recycling-drop.md`

That property is about a **safety** violation (silent data divergence from a dropped fixup). This one is about a **liveness** violation in the *same function*, on a different fast path (self-created tables, where there's no undo record at all, so the "recycled" concern doesn't apply — instead the concern is the checkpoint process itself getting stuck). Different assertion type, different failure mode, same code region — worth keeping as two properties per `references/property-catalog.md`'s guidance to split rather than conflate distinct claims under one assertion message.

## The property

| | |
|---|---|
| **Type** | Liveness (progress), with an implicit availability consequence since checkpoints gate WAL retention/disk growth and clean shutdown. |
| **Property** | The checkpoint process's wait for a `WaitingSkUndoLoc` sentinel to clear (self-created-table fast path in `checkpoint_write_pending_sk_fixups()`) always resolves within a bounded time proportional to normal commit/abort latency — it does not stall the entire checkpoint indefinitely if the backend that set the sentinel is itself delayed, descheduled, or fails to reach commit/abort promptly. |
| **Invariant** | `Sometimes(checkpoint_entered_sentinel_spin_wait)` to confirm the interesting path (a self-created table caught in the PK-applied/SK-pending window at exactly checkpoint time) is actually reached — this is a narrow window (the table must be created *and* modified within the same still-in-progress transaction that is also caught by the checkpoint's per-proc walk). Paired with `Always(checkpoint_completes_within_bounded_time_after_sentinel_clears)` — under fault injection that delays or descheduling-starves the specific backend holding the sentinel, assert the checkpoint's total duration for this step stays within a generous multiple of ordinary commit latency, rather than growing unboundedly with the injected delay. |
| **Antithesis Angle** | Construct a workload that creates a table and immediately performs DML on it within the same transaction (so the `WaitingSkUndoLoc` sentinel path is taken, not the ordinary undo-location path — e.g. `CREATE TABLE ... AS SELECT` or a `CREATE TABLE` + DML in one transaction, with a secondary index), then force a `CHECKPOINT` to land exactly while that backend is mid-PK-applied/SK-pending on its own new table — mirroring the existing `sk-recovery-race` construction but for the create-and-modify-same-transaction case specifically. Combine with Antithesis scheduling-delay fault injection targeting that specific backend process (CPU starvation, process pause) between setting the sentinel and reaching commit, to see whether the checkpoint's spin genuinely just waits patiently (acceptable, since it has no CHECK_FOR_INTERRUPTS but the backend eventually resumes) or whether a fault that never lets the backend resume (kill without cleanup, or a secondary crash of that specific backend that leaves the sentinel set) can wedge the checkpoint process itself indefinitely. |
| **Why It Matters** | Since this spin has no `CHECK_FOR_INTERRUPTS()`, the checkpoint process cannot even be cleanly cancelled/signaled out of this specific wait — worse than a merely slow checkpoint, this is potentially an *uncancellable* one. Combined with the S3-mode findings elsewhere in this focus (uninterruptible checkpoint-tail waits when S3 is down), this would be a second, independent way for a checkpoint to become stuck with no operator recourse short of killing the whole instance — and per `sut-analysis.md` §8, this fast path is explicitly flagged as possibly "relocating the original race rather than closing it," i.e. worth checking it isn't a regression introduced by the very fix meant to close orioledb#855. |

**Open Questions:**

- Can a backend that has set `pendingSkUndoLoc = WaitingSkUndoLoc` ever be killed, crash, or otherwise fail to clear the sentinel without another mechanism (e.g., the checkpoint's own error handling, or a subsequent recovery pass) resetting `oProcData[i].pendingSkUndoLoc` back to a non-waiting value? If the sentinel can be left set by a backend that dies uncleanly, the checkpoint's spin would wait forever with no possible resolution — `oProcData` slot cleanup on backend crash/`proc_exit` was not traced. `(needs further investigation)` — this is the single most important unresolved question for this property, since it determines whether the failure mode is "slow but bounded" or "genuinely permanent."
- Is there any per-iteration bound (max retry count) implicit elsewhere that this reading missed — e.g., does the calling `checkpoint_tables_callback()` or `o_perform_checkpoint()` itself have an overall checkpoint timeout that would eventually abort the whole checkpoint? Not investigated.
- What is the realistic worst-case duration of the window between a backend setting `WaitingSkUndoLoc` and clearing it, absent any fault injection? If it's provably microseconds under all code paths that reach it, the liveness risk is much lower than the "no timeout at all" framing suggests; not measured.
- Would a checkpoint stuck in this loop show up to the existing Antithesis health-checker as anything other than a generic timeout (the health-checker only checks `pg_isready`, per `sut-analysis.md` §5) — i.e. is there any existing signal that would distinguish this specific hang from a generic slow checkpoint? Not checked.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** No existing assertion touches this spin, `WaitingSkUndoLoc`, or the self-created-table fast path. This is a strong SUT-side instrumentation candidate: the spin loop has no visibility hook at all today (not even a `DEBUG` log), unlike the sibling `apply_one_pending_sk_fixup()` recycling-drop case (which at least logs at `DEBUG2`). No stopevent currently exists inside this spin loop either. Suggested additions:
- A `reachable()`/`sometimes()` call recording iteration count each time this loop is entered — and especially one that fires if the iteration count crosses a high threshold — would give Antithesis's search a concrete, otherwise-invisible signal that this exact path is being stressed, rather than relying on an eventual checkpoint-duration timeout to notice indirectly.
- Adding a stopevent immediately after `WaitingSkUndoLoc` is observed (before the `pg_usleep`) would let a deterministic test pin a checkpoint here and pair it with a `sometimes()`/bounded-time liveness assertion ("the checkpoint's pending-SK-fixup scan completes within N seconds even when a backend is parked mid-self-created-table-insert"), giving this a concrete, checkable liveness property instead of a purely static "no timeout" observation.

## Cross-cutting pattern (added by evaluation pass, R14)

The Wildcard evaluation lens identified this property as one of four sharing
the same "unbounded busy-wait, no `CHECK_FOR_INTERRUPTS()`" shape (the
others: `recovery-worker-idxbuild-stall`, `recovery-worker-stall-blocks-
leader`, `checkpointer-startup-lock-drain-progress` — see
`property-relationships.md` Cluster 9/Cluster 11 for the full cross-
reference). All four originally proposed only container/process-level fault
injection (`SIGSTOP`, CPU throttling, scheduling delay) to test whether the
wait is interruptible — but interruptibility in Postgres is a first-class
SQL-level concept. A cheaper, more direct alternative test for *this*
property specifically: since this loop has no `CHECK_FOR_INTERRUPTS()` at
all, issuing `pg_cancel_backend()`/`SIGINT` against the checkpointer process
itself while it's parked in this spin (or setting a `statement_timeout`-
style bound and observing it has no effect on this specific wait) directly
demonstrates the uncancellable-hang claim without needing to construct a
process-freeze scenario first — a simpler, cheaper first probe before
investing in the full adversarial fault-injection scenario above.
