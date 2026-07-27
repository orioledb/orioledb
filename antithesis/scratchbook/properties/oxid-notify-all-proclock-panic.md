# oxid-notify-all-proclock-panic

## Status

**Open lead, not yet in the catalog before this pass.** New property filling
Task A, item 3: `src/transam/oxid.c:1262`'s
`elog(PANIC, "failed to re-find shared proclock object")`, named in
`sut-analysis.md` §11 as one of several "lower-priority" trip-wires, alongside
siblings (`undo.c:2076`'s wraparound-retry guard, `page_state.c:851`'s
`CHECK_PAGE_STRUCT` PANIC) that *did* become catalog properties
(`undo-wraparound-retry-cap`, folded structurally into the disk/corruption
properties) — this one didn't. This file closes that gap: it is a legitimate,
distinct property, not already covered by any existing catalog entry.

## The mechanism

`oxid_notify_all()` (`src/transam/oxid.c:1207-1293`) is called by a backend to
wake up any other backend blocked in `wait_for_oxid()` (`oxid.c:1082-1122`)
waiting specifically on the *calling* backend's own current transaction. It
is called from three sites, all outside normal COMMIT/ABORT machinery:

- `src/transam/undo.c:2896` — subxact-abort (`SUBXACT_EVENT_ABORT_SUB`)
  rollback-to-savepoint path, with the comment "It might happen that we've
  released some row-level locks. Some waiters must be woken up. We currently
  can't distinguish them and just wake up everybody."
- `src/tableam/operations.c:1478` and `:1571` — `INSERT ... ON CONFLICT`
  handling, after `apply_undo_stack()` rolls back a failed insert attempt
  (conflict-detection paths).

`wait_for_oxid()`'s waiting side works by calling core Postgres's
`VirtualXactLock(vxid, true)` (`oxid.c:1118`) against the target's *own*
virtual transaction id (`vxid.localTransactionId = MyProc->LXID`,
`vxid.BACKENDID = MyBackendId` — this is the same VXID core Postgres
registers via `VirtualXactLockTableInsert()` at the start of every
transaction, not an OrioleDB-invented concept). Normally, a waiter blocked on
someone else's VXID lock is released only when that VXID's owner ends its
transaction (`VirtualXactLockTableCleanup()` at commit/abort). OrioleDB needs
to wake such a waiter *without* ending the owning transaction (e.g., a
savepoint rollback releases row locks but the outer transaction is still
running) — hence the "hack" comment at `oxid.c:1161-1165`: it manually walks
the shared lock-manager hash tables (`LockMethodLockHash`,
`LockMethodProcLockHash`) to find and forcibly release *just* the waiting
proc from the wait queue, bypassing the normal `LockRelease()`/
`VirtualXactLockTableCleanup()` API entirely.

`oxid_notify_all()`'s own control flow (`oxid.c:1207-1293`):

1. Build the `LOCKTAG_VIRTUALTRANSACTION` tag for **its own** vxid (`MyProc`'s
   `LXID`/`BackendId`).
2. `LWLockAcquire(partitionLock, LW_EXCLUSIVE)`.
3. Look up the `LOCK` object for that tag via `hash_search_with_hash_value(...,
   HASH_FIND, ...)`. If not found: comment says `/* Must be granted with fast
   path */` and returns early (`oxid.c:1236-1242`).
4. If found, look up **its own `PROCLOCK`** on that lock
   (`proclocktag.myLock = lock; proclocktag.myProc = MyProc;`) via
   `hash_search_with_hash_value(..., HASH_FIND, ...)`. **If not found:**
   `elog(PANIC, "failed to re-find shared proclock object")` (`oxid.c:1262`).
5. Otherwise, scan the lock's wait queue for procs with
   `waitingForOxid == true` matching this tag, force-remove them from the
   wait queue, and wake them via `SetLatch()`.

## Why step 3's early-return comment is likely wrong, and what that implies for step 4's PANIC

The `/* Must be granted with fast path */` comment at the `!lock` early
return (`oxid.c:1236-1242`) appears to be reasoning ported from analogous
*relation*-lock code. **`LOCKTAG_VIRTUALTRANSACTION` locks are not eligible
for Postgres's fast-path locking mechanism at all** — fast-path is scoped to
`LOCKTAG_RELATION` at weak lock levels (`EligibleForRelationFastPath()` in
core Postgres's lock manager checks `locktag_type == LOCKTAG_RELATION`
specifically). `VirtualXactLockTableInsert()` always registers directly in
the main shared lock hash, unconditionally, once per transaction. That means,
for the entire duration `MyProc` is inside a transaction, its own VXID
`LOCK` object should **already always exist** — the `!lock` branch is
probably close to unreachable in practice, and the far more load-bearing
assumption is step 4's: *given* the lock exists (which per the above should
be the normal case, not a rare one), the calling backend's own `PROCLOCK` on
it must also be findable, since a proc always registers a `PROCLOCK` for any
lock it acquires. The PANIC guards exactly this "should always hold together"
pairing — a lock/proclock consistency assumption that this code re-derives
manually outside the normal `LockAcquire()`/`LockRelease()` API path, rather
than one enforced by calling into that API directly.

This is precisely the "trip-wire that guards an assumption believed to
always hold, for a process whose own lock-table entry is presumed
consistent" shape the task named — structurally analogous to
`undo-wraparound-retry-cap`'s "this situation shouldn't happen twice" guard
and the `page_state.c:851` structural-corruption PANIC, but in the lock
manager rather than the undo/page subsystem, and reachable via a
completely different trigger (subxact abort / `INSERT ON CONFLICT`
conflict-resolution, not undo-buffer wraparound or page corruption).

## Why this is not already covered by an existing catalog property

- Not `checkpointer-heavyweight-lock-deadlock` / `checkpointer-startup-lock-drain-progress`:
  both concern `LOCKTAG_RELATION` locks taken by the checkpointer process
  specifically, and Postgres's real deadlock detector — a completely
  different lock type and a completely different process (checkpointer vs.
  any ordinary backend running a savepoint rollback or `ON CONFLICT` insert).
- Not `recovery-worker-stall-blocks-leader` / `recovery-worker-idxbuild-stall`:
  those concern `shm_mq`/condition-variable-based recovery-worker
  coordination, entirely unrelated to the core Postgres lock manager.
- Not `undo-wraparound-retry-cap`: that PANIC guards circular undo-buffer
  arithmetic; this one guards lock-manager hash-table consistency. Different
  subsystem, different trigger, different failure mode (this one is not
  tied to buffer sizing at all).
- No git history hit for "proclock" or "notify_all" in commit messages
  (`git log --oneline --all --grep=proclock -i` / `--grep=notify_all -i`,
  both zero results) — this specific PANIC has, as far as commit-message
  search can tell, never fired in a reported/fixed bug, unlike the checked
  siblings (`0cf76e17` for the visibility-ordering property above, or the
  documented reverts around undo/checkpoint bookkeeping). This is a genuinely
  fresh lead, not a rediscovery of known history.

## Property

| | |
|---|---|
| **Type** | Reachability / Safety |
| **Property** | Whenever `oxid_notify_all()` finds a `LOCK` object registered for the calling backend's own virtual-transaction id, it always also finds that backend's own `PROCLOCK` on that lock — the `elog(PANIC, "failed to re-find shared proclock object")` guard at `oxid.c:1262` is never actually tripped by any reachable subxact-abort or `INSERT ... ON CONFLICT` conflict-resolution sequence. |
| **Invariant** | `Reachable("oxid_notify_all found a registered LOCK for own vxid")` as an exploration hint (confirms the interesting, less-common branch — where the lock actually exists and has waiters — is exercised at all, as opposed to always taking the `!lock` early return), paired with `Unreachable("oxid_notify_all PANIC: failed to re-find shared proclock object")` as the actual safety claim. Modeled on the same `Reachable`+`Unreachable` pairing already used for `undo-wraparound-retry-cap`. |
| **Antithesis Angle** | Concurrent backends doing (a) `INSERT ... ON CONFLICT DO UPDATE` against a shared, contended PK/secondary-key range — deliberately maximizing `wait_for_oxid()`/`VirtualXactLock` contention on the same rows from many sessions — combined with (b) nested-subtransaction (`SAVEPOINT`/`ROLLBACK TO SAVEPOINT`) workloads that exercise `SUBXACT_EVENT_ABORT_SUB`, under scheduling-delay fault injection widening the window between a waiter registering in the lock's wait queue and the owner calling `oxid_notify_all()`. This is a pure single-node concurrency property — no replication topology needed, unlike most of this pass's other findings. |
| **Why It Matters** | If this trip-wire were ever tripped, the failure mode is an immediate cluster-wide PANIC (crash-restart) — a hard availability hit triggered by ordinary DML concurrency (savepoints, upserts), not by any fault injection or rare recovery/replication scenario, making it a legitimate concern independent of the rest of this catalog's replication-topology-gated findings. The trip-wire also documents a place where OrioleDB reimplements part of the core lock manager's internal bookkeeping (walking `LockMethodLockHash`/`LockMethodProcLockHash` directly) rather than using the public `LockAcquire`/`LockRelease` API — exactly the kind of narrow, hand-rolled concurrency surface likely to have an edge case the original author didn't anticipate. |

**Open Questions:**

- Is the `/* Must be granted with fast path */` comment at the `!lock` early
  return actually wrong (i.e., does `LOCKTAG_VIRTUALTRANSACTION` really never
  go through Postgres's fast-path mechanism), or is there some
  OrioleDB-specific registration path for this lock type that does use a
  fast-path-like shortcut this analysis didn't find? `(needs further
  investigation — reasoned from general Postgres lock-manager design,
  specifically that `EligibleForRelationFastPath()`-style gating checks
  `locktag_type == LOCKTAG_RELATION`, but not independently re-verified
  against this patched tree's exact `VirtualXactLockTableInsert()`
  call site, since `/Users/artur/supabase/orioledb_postgres` is out of
  scope per this task's scope restriction)`
- Concretely, what sequence of events could cause the calling backend's own
  `PROCLOCK` to go missing while its `LOCK` still exists? Candidates not
  ruled out: a race between this hand-rolled wait-queue surgery and Postgres's
  own deadlock detector or `RemoveFromWaitQueue()` running concurrently on
  the same lock from a different code path; a hashcode/partition-lock
  mismatch between `LockTagHashCode()` and `proclock_hash()`'s XOR
  construction under an unusual `MyProc` pointer value. Neither was traced to
  a concrete reachable sequence in this pass — this is the central unresolved
  question for whether the property is realistically triggerable or purely
  defensive. `(needs further investigation)`
- Since `oxid.c:1180`'s sibling function `oxid_notify()` ("No existing
  callers" per its own comment) has structurally similar logic but skips the
  proclock re-find entirely (it works directly off the `PGPROC` returned by
  `GetPGProcByNumber()`, not a fresh hash lookup) — does that asymmetry
  suggest the `oxid_notify_all()` re-find is defensive/paranoid rather than
  load-bearing, or does it reveal that `oxid_notify()`'s design was
  considered safer and `oxid_notify_all()`'s manual re-find is the actual
  weak point? `(needs further investigation — noted the asymmetry, didn't
  resolve which function's approach the PANIC's author considered "more
  correct")`

## SUT-side instrumentation

`existing-assertions.md` confirms zero assertions in `src/transam/`. This
PANIC is currently only observable as a hard crash after the fact — there is
no signal distinguishing "this code path is never exercised in a given
Antithesis run" from "it's exercised constantly and just never trips." A
`Reachable()` marker at the point the `lock` lookup succeeds (before the
proclock lookup) would let Antithesis's coverage-guided search confirm the
interesting branch is actually being reached, which is a prerequisite for the
`Unreachable()` claim on the PANIC itself to mean anything beyond "we never
tried."

## Investigation Log

#### Is the `!lock` early-return's "fast path" framing correct, and what does that imply about the PANIC's reachability?

- Examined: `oxid.c:1207-1293` (`oxid_notify_all`), `oxid.c:1129-1181`
  (`oxid_notify`, the "no existing callers" sibling), general knowledge of
  Postgres's fast-path lock manager design (fast path is scoped to
  `LOCKTAG_RELATION` at weak modes only).
- Found: `SET_LOCKTAG_VIRTUALTRANSACTION` builds a `LOCKTAG_VIRTUALTRANSACTION`
  tag, a type not eligible for fast-path locking in stock Postgres design;
  `VirtualXactLockTableInsert()` (core Postgres, called once per transaction
  start) registers directly in the shared lock hash.
- Not found: independent confirmation from the patched-Postgres source
  itself (out of scope per this task's explicit scope restriction — must not
  consult `/Users/artur/supabase/orioledb_postgres`), so this conclusion
  rests on general Postgres lock-manager design knowledge rather than a
  direct read of this specific patched tree's `VirtualXactLockTableInsert()`.
- Conclusion: flagged as `(needs further investigation)` above rather than
  stated as settled fact, per the honest-summaries convention — the
  practical implication (the `!lock` branch is likely rare, making the
  proclock-PANIC branch the operationally relevant one whenever a waiter is
  actually present) is recorded as the working hypothesis driving this
  property's framing, not as a verified conclusion.
