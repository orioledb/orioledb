# checkpointer-heavyweight-lock-deadlock

## Focus

Concurrency (attention focus 2).

## What led to this

`sut-analysis.md` §3 flags that OrioleDB's patched checkpointer (`/Users/artur/supabase/orioledb_postgres/src/backend/postmaster/checkpointer.c:218-227`) bootstraps subsystems stock Postgres's checkpointer never initializes:

```c
InitializeTimeouts(); /* establishes SIGALRM handler */
InitDeadLockChecking();
RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);
RelationCacheInitialize();
InitCatalogCache();
SharedInvalBackendInit(false);
```

The comment directly above (checkpointer.c, same patch) states the reason: "To use OrioleDB checkpoint, we must initialize the data for the primary lock mechanism (lock.h) to work correctly... locks of this type are needed by the OrioleDB module for debug events and relation locks, but they are not used by the postgres checkpointer."

This means the checkpointer process is no longer just a page-flushing background worker — it is now a first-class participant in Postgres's heavyweight lock manager and its deadlock detector. That is a structural assumption change: stock Postgres's design (and every piece of code written assuming "the checkpointer doesn't block on locks") no longer holds for this fork.

## Code confirming the checkpointer actually takes heavyweight relation locks

Traced the "relation locks" claim to source, since the SUT analysis's checkpointer-side comment (checkpoint.c:553-554) calls it "the OrioleDB checkpointer table lock (the special LOCKTAG_USERLOCK variant of AccessShareLock used by the checkpointer)" — but that specific comment is actually describing an LWLock (`checkpoint_state->oTablesMetaLock`), not a heavyweight lock. The actual heavyweight-lock call site is in `src/catalog/o_tables.c`:

- `o_tables_rel_lock_extended()` / `o_tables_rel_try_lock_extended()` (`o_tables.c:1576-1613`) build a `LOCKTAG` via `o_tables_rel_fill_locktag(&locktag, oids, lockmode, checkpoint)` — the `checkpoint` bool parameter is threaded all the way down — and call `LockAcquire(&locktag, lockmode, false, ...)`, the same core Postgres API any backend uses for relation-level locking.
- When `lockmode == AccessExclusiveLock` (`checkpoint` true), the locktag's lock method is switched to `NO_LOG_LOCKMETHOD`, presumably to avoid poisoning the regular lock namespace / WAL-logged lock behavior, but it is still routed through `LockAcquire()`, i.e. the same deadlock-detectable lock table as ordinary backend locks.
- Both callers call `AcceptInvalidationMessages()` right after acquiring — this is why `InitCatalogCache()`/`SharedInvalBackendInit(false)` had to be bootstrapped too: without them, `AcceptInvalidationMessages()` would operate on an uninitialized catcache/sinval state in the checkpointer.

So the full chain is real: checkpointer takes a heavyweight `LockAcquire()`-mediated lock on a relation (via the same path DDL/table-locking code uses), which means it can appear as a party in Postgres's deadlock graph, which is exactly why `InitDeadLockChecking()` + `RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert)` were added.

## The property

**Type:** Safety (no deadlock) / Reachability (deadlock detector actually engages the checkpointer as a party).

**Property:** When the checkpointer's heavyweight relation lock (acquired via `o_tables_rel_lock_extended(..., checkpoint=true)`) conflicts with a concurrent backend's lock on the same relation (e.g., a DDL statement, or a lock-mode escalation during DML), the conflict is resolved — either the checkpointer proceeds after the other lock is released, or Postgres's real deadlock detector (now correctly initialized in the checkpointer, per the bootstrap above) breaks a genuine cycle by erroring out one side. The checkpointer must never be permanently stuck holding partial checkpoint state while waiting on a lock nobody will release.

**Invariant:** `Always` — an assertion checked after every checkpoint attempt: checkpoint either completes within a bounded number of attempts/time, or if it errors out (deadlock detected, victim chosen), the error is a clean `ERROR`/retry, not a hang. Practically: `sometimes(checkpoint_completed_after_lock_conflict)` to prove the interesting case is reached at all, plus `always(no_process_wedged_forever)` as a liveness backstop (see Antithesis Angle for how to make this checkable).

**Antithesis Angle:** Construct a workload that runs concurrent DDL (`ALTER TABLE`, `DROP INDEX CONCURRENTLY` alternatives OrioleDB supports, or `TRUNCATE`) against a table while a `CHECKPOINT` is in flight, so the checkpointer's `o_tables_rel_lock_extended(checkpoint=true)` call has a real chance to conflict with the DDL backend's own relation lock acquired in the opposite order (DDL backend holds table lock, wants the checkpointer's LWLocks via a code path the checkpointer itself might be waiting on — or vice versa). Antithesis's scheduling-fault injection (delaying one side just before it would release its lock) is exactly the kind of adversarial interleaving needed to provoke the deadlock-detector's `DEADLOCK_TIMEOUT` path rather than the common case where locks simply queue and drain. Existing stopevents (`checkpoint_step`, `checkpoint_table_start`, `checkpoint_index_start` — see `stopevents.txt`) can pin the checkpointer mid-lock-acquisition to deterministically construct the interleaving, the same way `sk_modify_pending` pins the PK/SK race in the existing harness.

**Why It Matters:** If the deadlock-checker bootstrap is subtly incomplete (e.g., missing a piece of process-local state the real deadlock detector expects, since the checkpointer was never meant to run this code path), a genuine deadlock involving the checkpointer could hang forever instead of being detected — and because checkpoints gate clean shutdown and (per `sut-analysis.md` §6) the S3 path already has an unbounded checkpoint-tail wait, a wedged checkpointer is a severe availability failure: no more checkpoints, growing WAL, and (in S3 mode) an un-shutdownable instance.

**Open Questions:**

- Has this specific interleaving (checkpointer's heavyweight lock vs. a concurrent DDL statement's opposite-order lock) ever been deliberately tested, deterministically or otherwise? No isolation test or Python test with a matching name was found in this pass; this needs a dedicated search of `test/specs/*.spec` before assuming it's uncovered. `(needs human input)` on whether this is truly novel or already covered by an existing isolation spec not surfaced by this evidence-gathering pass.
- Whether `NO_LOG_LOCKMETHOD` (the alternate lock method for `AccessExclusiveLock` + `checkpoint=true`) changes deadlock-detection semantics (e.g., whether cross-lock-method deadlock cycles, checkpointer using `NO_LOG_LOCKMETHOD` while a backend uses `DEFAULT_LOCKMETHOD` on the same relation OID, are still detected as one cycle) is unresolved from this pass — would need to read `lock.c`'s multi-lockmethod deadlock graph handling, which is out of scope for the orioledb-side evidence gathered here.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** No Antithesis SDK assertion exists anywhere touching checkpointer lock acquisition, deadlock detection, or `o_tables_rel_lock_extended`. The only existing assertions (`sk-recovery-race[-chaos]/driver.py`) target the PK/SK undo-recycling race, not checkpointer lock contention. A SUT-side `reachable()` right after `o_tables_rel_lock_extended(..., checkpoint=true)` succeeds under contention (payload: wait duration, whether `LockAcquire` returned after retry vs. immediately) would be valuable since this state isn't otherwise observable from a SQL client.

### Investigation Log

#### Has this specific interleaving (checkpointer's heavyweight lock vs. a concurrent DDL statement's opposite-order lock) ever been deliberately tested?

- Examined: search for isolation specs (`test/specs/*.spec`) and Python tests with names matching this interleaving.
- Found: no isolation test or Python test with a matching name found in this pass.
- Not found: a dedicated grep of `test/specs/*.spec` was not performed, so full absence isn't confirmed.
- Conclusion: tagged `(needs human input)` — whether this is genuinely novel or already covered needs a dedicated search this pass didn't complete.
