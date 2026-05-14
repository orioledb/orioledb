# Recovery-side PK↔SK desynchronization after PANIC before the COMMIT WAL marker

## Summary

When an OrioleDB transaction PANICs **after** modify records have been
appended to the local WAL buffer (and possibly already flushed to disk
by `flush_local_wal_if_needed`) but **before** the trailing
`WAL_REC_COMMIT` finish record is appended, the post-restart cluster's
**secondary `UNIQUE (token)` index drifts out of sync with the PK
tree**. The workload preserves the multiset of tokens by construction
(every transaction is an atomic swap), so the distinct-token count
must equal the row count both before and after a crash. After recovery
it sometimes doesn't.

The on-disk WAL stream for the crashed oxid contains modify records
but **no `WAL_REC_COMMIT` and no `WAL_REC_ROLLBACK`** — recovery has
to infer "no finish marker -> tx was in-flight -> discard" purely from
the absence of a completion record. The PK side appears to be
discarded correctly; the SK side does not always follow.

Failure symptom (bank-account stress test, observed both ways):

```
AssertionError: unique_tokens 101 != 100   (one extra SK entry)
AssertionError: unique_tokens  99 != 100   (one missing SK entry)
```

The direction (extra vs missing) depends on which side of the
consistency window leaked across recovery; the underlying bug — SK
state diverging from PK after crash + replay — is the same.

A PK-authoritative seq-scan diagnostic confirms the corruption is
SK-only and the PK tree is internally clean: PK row count 100, PK
distinct ids 100, PK distinct tokens 100, balance sum matches. The
test's default `count(DISTINCT token)` is answered by an `Index Only
Scan` on `o_bank_account_token_uniq` (`Heap Fetches: 0`) and returns
the wrong number — i.e. the SK index and the PK tree disagree about
which tokens are present.

`orioledb_tbl_check('o_bank_account')` has been observed returning
`true` while this divergence is present, meaning OrioleDB's own
consistency check does not catch the case.

## Reproducer

* Inject at [`src/recovery/wal.c`#L336](../../src/recovery/wal.c#L336)
  (`orioledb-before-pre-commit-wal-finish`, wrapped in
  `START_CRIT_SECTION` so an attached `error` action escalates to
  PANIC). This site sits inside `wal_commit`, after the
  `flush_local_wal_if_needed(recLength)` call at line 311 and after
  the (possibly second) `add_xid_wal_record` call at line 315, but
  **before** `add_finish_wal_record(WAL_REC_COMMIT, ...)` at line 340.
* Bank-account stress harness `test/t/crash/rr_stress_test.py`:
  20 writers transferring money + tokens in deferred-unique pairs,
  3 firings spread over the trial duration.

```bash
source ../venv/bin/activate
RR_ASSERT_POINTS='orioledb-before-pre-commit-wal-finish' \
  RR_INJECTION_POINTS='__none__' \
  RR_ASSERT_FIRINGS=3 RR_PANIC_FATAL=0 \
  RR_DURATION=15 RR_WRITERS=20 \
  RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
  python3 -m unittest test.t.crash.rr_stress_test.RrStressTest
```

Cluster runs with `restart_after_crash=on`; the watchdog tolerates the
PANIC (`RR_PANIC_FATAL=0`), waits for recovery, then runs invariant
queries. Catch rate ~67 % per trial observed at this config — modify
records are already buffered (often already flushed) by the time the
PANIC fires, so per-arm catch probability is higher than for sites
deeper in the commit pipeline.

## Why this is production-reachable, not synthetic

The injection's `error`-mode escalates to PANIC because it sits in
`START_CRIT_SECTION`; in production the equivalent crash sources at
this exact code path are:

* `Assert` failure inside `wal_commit`
* `palloc` OOM at the same site
* internal `ereport(PANIC, ...)`
* OOM killer **SIGKILL** (uncatchable, bypasses `HOLD_INTERRUPTS`)
* SIGSEGV / SIGBUS from kernel fault

All five are reachable in production. `CommitTransaction` runs
`CallXactCallbacks(XACT_EVENT_COMMIT)` under
[`HOLD_INTERRUPTS()`/`RESUME_INTERRUPTS()`](https://github.com/postgres/postgres/blob/master/src/backend/access/transam/xact.c#L2319),
so async query-cancel/SIGTERM is the **only** failure mode this site is
shielded from — every code-internal or SIGKILL-class crash still lands
here.

## WAL state at recovery start

For a typical bug-reproducing run the crashed backend's WAL trace
looks like:

```
wal-trace modify    pid=PID oxid=OXID rec=UPDATE ...
wal-trace xid       pid=PID oxid=OXID rec=WAL_REC_XID
wal-trace modify    pid=PID oxid=OXID rec=UPDATE ...
wal-trace flush     pid=PID oxid=OXID bytes=N isCommit=0
wal-trace flush-done pid=PID bytes=N isCommit=0 lsn=0/...
wal-trace xid       pid=PID oxid=OXID rec=WAL_REC_XID  (re-added because
                                                         the flush emptied
                                                         the local buffer)
TRAP: failed Assert("CritSectionCount == 0 || ...")
```

i.e. XID + two UPDATE modify records have been **flushed to disk**
under an `isCommit=0` flush; the COMMIT record was never appended. So
the on-disk WAL stream for the crashed oxid is:

```
[WAL_REC_XID] [WAL_REC_UPDATE id=A token=...] [WAL_REC_UPDATE id=B token=...]
```

with no finish record. Recovery must treat this oxid as in-flight and
discard both modifies. Yet post-recovery the bank table has one more
distinct SK-index entry than expected — recovery is not fully
discarding the modify path's SK insert.

## Open hypotheses

1. **Dirty SK page** from a pre-crash checkpoint carries a stale
   entry corresponding to one of the discarded modify records --
   explains the "extra entry" direction.
2. **Undo-log replay** fails to undo (or over-undoes) the SK side
   when the oxid has no commit-or-rollback marker -- can produce
   either direction depending on which write is affected.
3. SK pages aren't WAL'd directly — the rebuild from PK + undo has a
   window where the SK ends up with a state that doesn't match what
   the PK replay produced for the same oxid.

## Severity / impact

Any unhandled `Assert`/`palloc` OOM/SIGKILL inside `wal_commit` after
the local buffer has been flushed but before `WAL_REC_COMMIT` is
appended produces a permanent **PK↔SK divergence** after recovery.
The PK tree stays internally correct, so queries planned over the PK
return correct results; queries that the planner answers from the
unique index return wrong counts and may either return rows that no
longer exist or omit rows that do. `orioledb_tbl_check()` has been
observed returning `true` while this is the case, so the divergence is
silent at the level of OrioleDB's own consistency check.
Production-reachable for the failure modes listed above.

## Artefacts in this repo for follow-up

* `test/t/crash/rr_stress_test.py` — harness, `assert_chaos_loop` worker.
* `test/t/crash/RECOVERY_TOKEN_LEAK.md` — investigation notes.
* `test/t/crash/tx_flow.md` — annotated COMMIT/ABORT/UPDATE flow with
  this injection marked at the `wal_commit` step.
* `test/t/crash/results/*.log` — saved PANIC logs (>200 MB each) with
  `wal-trace` brackets.
