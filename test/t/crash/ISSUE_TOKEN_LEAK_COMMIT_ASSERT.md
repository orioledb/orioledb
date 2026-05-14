# Recovery-side SK index desynchronization on crash-restart with concurrent commit traffic

## Summary

When the postmaster has to crash-restart any time there is concurrent
commit traffic against an OrioleDB table with a secondary `UNIQUE`
index, the post-restart cluster's **SK index sometimes drifts out of
sync with the PK tree**. The workload preserves the multiset of
tokens by construction (every transaction is an atomic swap), so the
distinct-token count must equal the row count both before and after a
crash. After recovery it sometimes doesn't.

**The bug does NOT depend on the crashed backend's own WAL state.**
Empirically verified (see "Hypotheses tested so far" below): bug
reproduction rate is essentially the same across three injection
placements in `undo_xact_callback`:

| Injection placement | Crashed tx's WAL state at TRAP | Per-trial bug rate |
|---|---|---|
| Original (after `wal_commit()` flush of WAL_REC_COMMIT, before CSN flip) | full commit durable | ~25-30 % |
| Late (very end of `XACT_EVENT_COMMIT` case, after every commit-side cleanup) | full commit durable | ~30 % |
| Entry-of-`undo_xact_callback` (before `wal_commit()` is even called) | NO `WAL_REC_COMMIT`, only XID + modify records | ~14 % |
| PG-side `finish_xact_command()` (before `CommitTransactionCommand()`) | NOTHING orioledb-specific for this tx | ~8 % |

At the entry-of-callback placement the crashed tx has **no
`WAL_REC_COMMIT`** on disk; by the standard rule recovery should treat
it as in-flight and discard. At the PG-side `finish_xact_command()`
placement the crashed tx hasn't even started its commit pipeline --
recovery has no evidence the tx ever existed. Yet the SK leak still
appears in both cases.

**The bug is in recovery's reconstruction of the SK index from
*other* (cleanly-committed) backends' WAL.** The crashed backend's
identity, oxid, WAL state, and even its existence are incidental.
What matters is that the cluster goes through crash-recovery while
some non-crashed transactions had recently flushed their
`WAL_REC_COMMIT` to disk -- and recovery's SK rebuild for *those*
committed transactions miscomputes. The crashed backend is just the
trigger that forces postmaster to crash-restart; the bug lives in
the replay path for everyone else.

Failure symptom (bank-account stress test, observed variants):

```
AssertionError: unique_tokens 101 != 100   (one extra SK entry)
AssertionError: unique_tokens  99 != 100   (one missing SK entry)
AssertionError: unique_tokens 102 != 100   (two extra SK entries)
AssertionError: unique_tokens 104 != 100   (four extra SK entries)
```

The exact deviation (±1, ±2, ±4) depends on how many of the crashed
transaction's modify records leaked; the direction (extra vs missing)
depends on which side of the consistency window slipped. The underlying
bug — SK state diverging from PK after crash + replay — is the same in
every variant.

A PK-authoritative seq-scan diagnostic confirms the corruption is
SK-only and the PK tree is internally clean: PK row count is 100, PK
distinct ids is 100, PK distinct tokens is 100, balance sum matches.
The test's default `count(DISTINCT token)` is answered by an `Index
Only Scan` on the token unique index (`Heap Fetches: 0`) and returns
the wrong number — i.e. the SK index and the PK tree disagree about
which tokens are present.

`orioledb_tbl_check('o_bank_account')` has been observed returning
`true` while this divergence is present, meaning OrioleDB's own
consistency check does not catch the case.

## Reproducer

* Inject `orioledb-commit-assert` anywhere in `undo_xact_callback`'s
  `XACT_EVENT_COMMIT` branch *after* `wal_commit()` has appended and
  flushed `WAL_REC_COMMIT`. The injection is wrapped in
  `START_CRIT_SECTION` so an attached `error` action escalates to
  PANIC. Tested placements that all reproduce the bug:
  - Original site: right after `wal_commit()`, before
    `current_oxid_precommit()` (the "narrow window" hypothesis the
    issue first targeted).
  - Late site: at the very end of the case body, after every
    post-commit bookkeeping call (`current_oxid_precommit`,
    `current_oxid_commit`, `walk_undo_stack` on-commit, `wal_after_commit`,
    `reset_cur_undo_locations`, etc.) -- right before `break;`.
  Both placements reproduce the SK leak at ~25–30% per trial under
  the same workload.
* Bank-account stress harness `test/t/crash/rr_stress_test.py`:
  20 writers transferring money + tokens in deferred-unique pairs,
  3 firings spread over the trial duration.

```bash
source ../venv/bin/activate
RR_ASSERT_POINTS='orioledb-commit-assert' \
  RR_INJECTION_POINTS='__none__' \
  RR_ASSERT_FIRINGS=3 RR_PANIC_FATAL=0 \
  RR_DURATION=15 RR_WRITERS=20 \
  RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
  python3 -m unittest test.t.crash.rr_stress_test.RrStressTest
```

Cluster runs with `restart_after_crash=on`; the watchdog tolerates the
PANIC (`RR_PANIC_FATAL=0`), waits for recovery, then runs invariant
queries. The `Heap Fetches: 0` Index-Only Scan on
`o_bank_account_token_uniq` is the load-bearing fact for the failure
signature.

### Per-trial bug rate is timing-sensitive

The bug reproduction rate depends sharply on per-commit-path latency:

| `src/btree/page_state.c` csn-trace state | Per-trial bug rate |
|---|---|
| All 8 `lock_page` csn-traces ENABLED | **~75 % per trial** |
| All csn-traces DISABLED (release-build-like) | **~4 % per trial** |

Every UPDATE goes through several page-lock cycles; each csn-trace
elog adds a few microseconds of per-op latency, widening the SK race
window the assert-injection has to land in. **The bug exists at both
ends of this spectrum**, but reliable reproduction needs the
high-rate state. The empirical rate at the release-build-like timing
(~4 %) is the lower bound on how often the underlying race fires in
production traffic that already has equivalent per-page-op delay
(e.g. logging, lock contention, ASLR-driven cache effects).

## Why this is production-reachable, not synthetic

The injection's `error`-mode escalates to PANIC because it sits in
`START_CRIT_SECTION`; in production the equivalent crash sources at
this exact code path are:

* `Assert` failure inside `undo_xact_callback`'s commit branch
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

## Bug localization: PK is correct, only SK is wrong

The constraint chain forced by what we observe rules out everything
except the recovery-side SK rebuild:

1. **PK tree state is correct after recovery.** The diagnostic
   `pk_seq` query (forced Seq Scan, which walks the PK tree directly
   in OrioleDB's index-organized storage) returns
   `(rows=100, distinct_id=100, distinct_token=100, sum_balance=100000)`
   on every bug-reproducing trial. The PK row count, PK distinct ids,
   PK distinct tokens, and PK balance sum are all exactly the expected
   values.
2. **WAL records are deterministic** — every `WAL_REC_UPDATE` replays
   the same PK row change on every recovery. Recovery applies them
   authoritatively to the PK tree.
3. **PK is what recovery emits authoritatively.** WAL records target
   the PK tree directly.
4. **SK is derivative.** OrioleDB's SK pages aren't WAL'd; they're
   rebuilt from PK + undo during recovery.

Since the PK ends up correct but the SK is off, **the bug must live in
the recovery-side SK-rebuild path**. The PK+WAL inputs to that path
are right; only its output (the rebuilt SK index) is wrong.

### Most likely sub-mechanism

A bank-account UPDATE that changes the `token` column requires the SK
to do both:

- `DELETE [old_token -> row_id]` from the unique index
- `INSERT [new_token -> row_id]` into the unique index

The observed variants match an asymmetric leak in this pair:

| Variant | Likely SK-replay miscompute |
|---|---|
| `unique_tokens = 101` | 1 replayed UPDATE emitted the INSERT but missed the DELETE -> 1 stale leftover entry |
| `unique_tokens = 102` | both UPDATEs in a single swap-tx missed their DELETE -> 2 stale entries |
| `unique_tokens = 104` | two swap pairs leaked -> 4 stale entries |
| `unique_tokens =  99` | mirror: 1 UPDATE missed its INSERT, leaving a PK row without an SK entry |

### Likely code path

The bug is in how recovery dispatches `WAL_REC_UPDATE` to the SK side
of `o_btree_modify_internal` (or the equivalent recovery-only
modify-handler). Candidates to inspect:

- `src/recovery/recovery.c` — WAL replay dispatcher
- `src/btree/modify.c` — `o_btree_modify_internal` and its per-index
  loop; the old-key vs new-key handling for column-changing UPDATE
- The conditional that decides whether an UPDATE needs both
  delete-old-key + insert-new-key on a non-PK index, vs just
  insert-new-key

If recovery treats every `WAL_REC_UPDATE` as a pure insert into the SK
index (without first deleting the old SK entry for the changed
column), every column-changing UPDATE leaks one stale entry per
affected SK row.

## WAL state at recovery start

For one bug-reproducing run the crashed backend's full WAL is visible
in the saved log via the `wal-trace` LOG bracket:

```
wal-trace xid       pid=PID oxid=OXID rec=WAL_REC_XID
wal-trace modify    pid=PID oxid=OXID rec=UPDATE ...
wal-trace modify    pid=PID oxid=OXID rec=UPDATE ...
wal-trace finish    pid=PID oxid=OXID rec=COMMIT xmin=...
wal-trace flush     pid=PID oxid=OXID isCommit=1
wal-trace flush-done pid=PID bytes=... isCommit=1 lsn=0/...
TRAP: failed Assert("CritSectionCount == 0 || ...")
```

i.e. the `WAL_REC_COMMIT` record is on disk before the PANIC. Recovery
must replay a fully-committed transaction. The leak therefore comes
from WAL replay rebuilding the in-memory state, not from a partial
commit on the writer side. OrioleDB SK pages aren't WAL'd directly —
they're reconstructed from PK + undo during recovery — so the SK
rebuild path is the likely site of the stale entry.

## Hypotheses tested so far

### Ruled out: mid-abort interruption causes the leak

Hypothesis: a backend interrupted by `quickdie()` mid-rollback (between
`apply_undo_stack` and `current_oxid_abort`) leaves the SK index
half-reverted, and recovery starts from that inconsistent state.

Test: 11 saved logs from the same build (3 buggy + 8 normal-with-PANIC,
all `cav16` batch). The full XACT_EVENT_ABORT case body in
`undo_xact_callback` was bracketed with `abort-trace enter`/`exit`
markers covering `apply_undo_stack`, `wal_rollback`, `current_oxid_abort`
and all post-abort cleanup. A backend with `enter` but no matching
`exit` before postmaster's `all server processes terminated` was
considered mid-abort.

Result:

| | buggy | normal-with-PANIC |
|---|---|---|
| logs with mid-abort > 0 | 1 / 3 | 6 / 8 |
| mean mid-abort count per log | 0.67 | 0.75 |

**2 of 3 buggy logs had ZERO mid-abort backends**, and the bug still
reproduced. **6 of 8 normal logs had ≥1 mid-abort**, and the bug did
NOT reproduce. The two phenomena are uncorrelated; mid-abort is
neither necessary nor sufficient for the leak.

### Ruled out: the bug requires a specific in-commit-window PANIC

Hypothesis 1 (narrow): the bug fires only when the PANIC lands
between the `WAL_REC_COMMIT` flush and `current_oxid_precommit()`,
leaving the crashed backend's in-memory commit state half-written.

Test 1: moved the injection to the very end of the
`XACT_EVENT_COMMIT` case body in `undo_xact_callback` -- after every
commit-side cleanup call has returned. Reran 20 trials at the same
workload config.

Result 1: **6 / 20 trials reproduced the SK leak** at the late
injection site (variants: 101 ×3, 102, 103, 104) -- statistically
indistinguishable from the original site's rate.

### Ruled out: the crashed tx's own WAL_REC_COMMIT being durable is the trigger

Hypothesis 2 (broader): the bug requires the *crashed* backend's
`WAL_REC_COMMIT` to be durable on disk so recovery has to replay it.

Test 2: moved the injection to the very entrance of
`undo_xact_callback` -- before any per-event dispatch, before
`wal_commit()` is even called for this tx. At this site the crashed
tx's WAL stream has only XID + modify records, NO COMMIT marker; by
the standard "no finish marker -> tx was in-flight -> discard" rule,
recovery should drop it. Reran 14 trials at the same config.

Result 2: **2 / 14 trials still reproduced the SK leak** (variants:
101 and 104), despite the crashed tx having no `WAL_REC_COMMIT` on
disk. Per-trial PANIC count was 47-98 (vs 1-10 at later sites)
because function-entry placement fires on every XactCallback event
(`PRE_COMMIT`, `COMMIT`, `ABORT`, etc.), catching almost every tx
that runs. Even with that dramatically wider catch surface, bug rate
went *down* slightly -- so catching more backends doesn't make the
bug more likely.

### Ruled out: the crashed backend's existence is required at all

Hypothesis 3 (strongest): even if the crashed tx hasn't started its
commit pipeline, *some* orioledb-side state from that backend is
required to trigger the bug.

Test 3: moved the injection to PG-side `finish_xact_command()` at
`src/backend/tcop/postgres.c`, **before** `CommitTransactionCommand()`
is even called. At this site the crashed backend has done nothing
orioledb-specific for its current tx -- no `wal_commit()`, no
XACT_EVENT_COMMIT callback, no `undo_xact_callback` entry. Recovery
has zero evidence the crashed tx ever existed. Ran 12 trials.

Result 3: **1 / 12 trials still reproduced the SK leak** (variant:
101). Bug rate is lower (~8%) than at later placements, which is
consistent with most catches at this site landing on non-commit code
paths (query startup, ABORT events, etc.) that would never have
produced a commit anyway -- but it is not zero. The "crashed backend
must exist as an orioledb participant" condition is also not
necessary.

### Combined conclusion across all four placements

**The bug does not depend on the crashed backend at all.** It depends
only on the cluster going through crash-recovery while some
*other* (non-crashed, cleanly-committing) transactions had recently
flushed their `WAL_REC_COMMIT` records to disk. Recovery's SK rebuild
for those committed peers is the broken path -- the crashed backend
is just the mechanism that triggers the postmaster crash-restart
cycle. The identity, WAL state, oxid, and even existence of the
crashed backend in OrioleDB's perspective are all irrelevant.

### Ruled out: high-level per-backend state at TRAP discriminates

Hypothesis: buggy and normal logs differ in the distribution of
backend states at TRAP time (mid-commit vs in-flight vs mid-abort).

Test: same 11 cav16 logs, per-backend categorization based on last
`wal-trace flush-done` and `abort-trace enter`/`exit`.

Result: means across categories are within sampling noise:

| | buggy | normal |
|---|---|---|
| committed (last `isCommit=1`) | 16.3 | 15.25 |
| in-flight (last `isCommit=0`) | 4.0 | 5.0 |
| mid-abort | 0.67 | 0.75 |

High-level state at TRAP is not the discriminator.

## Still-open hypotheses

1. **Checkpointed dirty SK page** survived from before the crash,
   carrying a soon-to-be-committed insert that recovery then re-applies
   on top of itself (explains the "extra entry" direction). To test:
   correlate checkpoint LSN with crashed-tx flush LSN.
2. **Undo-log replay** is double-applying or under-applying the SK
   side of the swap -- can produce either direction depending on which
   side leaks. To test: extend tracing to the SK-rebuild path in
   recovery and check whether the same WAL_REC_UPDATE triggers two SK
   inserts or zero.
3. **Page-state convergence at TRAP time** — the crashed tx and one or
   more in-flight peers were modifying the same SK page; the SK rebuild
   sees a composite page state that doesn't match what either tx
   intended. To test: cross-reference the crashed tx's `idx_oids` /
   blkno with concurrent backends' last `lock_page` activity.

## Severity / impact

**Any unhandled fault that crashes the cluster while a `WAL_REC_COMMIT`
sits durable on disk** produces a permanent **PK↔SK divergence** after
recovery. The triggering fault can land anywhere in the commit
callback's post-flush region (we verified two distinct placements);
the bug is therefore much broader than a narrow inter-step window
race.

Production failure modes that hit this:
- `Assert` / `palloc` OOM / internal `ereport(PANIC)` inside
  `undo_xact_callback`'s commit branch -- any line after `wal_commit()`
  returns.
- OOM-killer **SIGKILL** (uncatchable, bypasses `HOLD_INTERRUPTS`) on a
  backend that has just flushed its commit but hasn't yet returned
  control to `CommitTransaction()`.
- SIGSEGV / SIGBUS from kernel fault in the same window.
- Any crash of a *different* postgres process (postmaster fan-out
  SIGQUITs everyone) while at least one backend has a durable
  `WAL_REC_COMMIT` from a tx that was about to finish.

Resulting damage:
- The PK tree stays internally correct.
- Queries planned over the PK return correct results.
- Queries that the planner answers from the unique index return
  wrong counts and may either return rows that no longer exist or
  omit rows that do.
- `orioledb_tbl_check('o_bank_account')` has been observed returning
  `true` while this divergence is present, so the inconsistency is
  silent at the level of OrioleDB's own consistency check.
- Recovery does not repair it on subsequent restart cycles -- the
  inconsistent SK page is now the source of truth that future
  recoveries treat as authoritative.

## Artefacts in this repo for follow-up

* `test/t/crash/rr_stress_test.py` — harness, `assert_chaos_loop` worker.
* `test/t/crash/RECOVERY_TOKEN_LEAK.md` — investigation notes.
* `test/t/crash/tx_flow.md` — annotated COMMIT/ABORT/UPDATE flow with
  this injection marked.
* `test/t/crash/results/*.log` — saved PANIC logs (>200 MB each) with
  `wal-trace` + `commit-assert-trace` brackets.
