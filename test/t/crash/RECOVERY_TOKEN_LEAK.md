# Recovery-side token leak — investigation notes

Working notes on a data-corruption bug surfaced by the bank-account
stress test when the cluster recovers from a `orioledb-commit-assert`
PANIC. **Distinct from** the spinlock-deadlock bug filed in
`ISSUE_DRAFT.md` — this one lives in the recovery / WAL-replay path,
not the live abort path. The CSN-precommit fix that closed the
deadlock did NOT close this one.

## Symptom

After the cluster restarts from a crash triggered by the
`orioledb-commit-assert` injection at [undo.c#L2378](../../src/transam/undo.c#L2378),
the bank table sometimes ends up with one extra distinct `token`
value. The failing assertion:

```
AssertionError: 101 != 100 : expected 100 unique tokens, got 101
```

The total balance invariant (`sum(balance) == n_accounts *
initial_balance`) typically holds — only the unique-token count is
off-by-one. Suggests one writer's first UPDATE (which writes
`my_token` onto `v_from`) survived recovery, while its paired second
UPDATE (which writes `v_from`'s old token onto `v_to`) did not.
Result: an extra writer-injected token persisted in the table without
a matching swap-out.

## Minimum reproducer

```bash
RR_INJECTION_POINTS='__none__'  # no wal-chaos, only assert worker
RR_ASSERT_FIRINGS=3             # 3 PANIC + recovery cycles per test
RR_PANIC_FATAL=0                # let the cluster recover; only fail
                                # on data-invariant violation
RR_DURATION=15
RR_WRITERS=20
RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0
python3 -m unittest test.t.crash.rr_stress_test.RrStressTest
```

- **Catch rate**: ~20-25 % per test at this config. 4-8 trials usually
  enough to reproduce.
- Writer count between 10-20 doesn't materially change the rate (we
  bisected; rate is ~constant in that range).
- Duration 30 s gave ~60 %, 15 s gave 20-80 % (high variance per
  batch); 10 s gave 20 %. **Settled on 15 s** as the shortest
  duration that's still fast-enough-to-iterate.

## What WAL tracing reveals

After adding `wal-trace` LOG lines at every WAL emission site
(`add_xid_wal_record`, `add_modify_wal_record_extended`,
`add_finish_wal_record`, `flush_local_wal`), one bug-reproducing run
(`results/20260512_141042_solo___none__.log`) shows:

- Two PANIC-affected PIDs (83304 and 83307) — both hit the
  `START_CRIT_SECTION` assertion via the brief-attach window of the
  `assert_chaos_loop`.
- **Both crashed backends fully committed their last tx in WAL before
  the TRAP**: complete sequence XID → UPDATE-1 → UPDATE-2 →
  intermediate buffer-overflow flush (`isCommit=0`) → second XID →
  COMMIT → final flush (`isCommit=1`), all flushed to PG XLog. So
  recovery will see those txs as fully committed and replay both
  UPDATEs in order.
- **No "orphan" writer oxids**: scanning every oxid in the log for
  the pattern *"flushed some modify records but never emitted a
  finish record"*, the only hit was the bootstrap/system oxid=4 from
  PID 83286, not a bank-account writer.

So the stray token does NOT come from a simple
"writer's partial-WAL-then-PANIC" race. The bug is more subtle.

## Hypotheses (ordered by plausibility)

### H1: Dirty page survives `quickdie` via orioledb's own checkpoint
1. Writer A starts tx, runs UPDATE 1 in shared buffers (page P1
   dirtied with `token = my_token`).
2. `orioledb-commit-assert` PANICs *another* backend B.
3. Postmaster sends SIGQUIT to all other backends including A. A
   exits via `_exit(2)` (quickdie) without running its own
   AbortTransaction / undo path — A's in-memory page mutation is
   never reverted, and A's local WAL buffer is discarded.
4. Before the crash, the orioledb checkpointer (or buffer eviction)
   wrote page P1 to disk with the partial mutation included.
5. Recovery rebuilds from the on-disk page (UPDATE 1 visible) +
   replays WAL after the checkpoint LSN. WAL doesn't have any
   records from A (its local buffer never flushed), so A is treated
   as "tx never existed / aborted" at the CSN layer. But the
   PARTIAL mutation on the page survives because there's no
   `WAL_REC_ROLLBACK` or undo record to drive its revert.

This is the strongest fit for the empirical evidence: the
crashed-PID WAL is consistent (so its own tx isn't the source), but
some other backend's partial in-buffer mutation persists into the
on-disk image.

### H2: SK index inconsistency under partial PK replay
- PK writes ARE WAL'd via `add_modify_wal_record`.
- SK writes are NOT WAL'd separately — recovery re-derives SK page
  changes from the PK's `WAL_REC_UPDATE` by running
  `o_tbl_indices_overwrite` on the redo side
  ([tx_flow.md UPDATE TX FLOW](tx_flow.md), Phase 2 notes).
- If recovery REPLAYS a PK update but the SK page on disk already
  has the partial SK mutation (from a checkpointed dirty SK page
  belonging to a different writer's still-running tx), the SK index
  may end up with both the old and the new token entry → distinct
  tokens count goes up by 1.

This is essentially H1 specialised to the SK index, which is where
the `unique_tokens` invariant fails. The PK page mutation might be
correctly reverted (via undo replay or absence of WAL), but the SK
page's stale entry survives because nothing knows to revert it.

### H3: Undo-log loss for SIGQUIT'd writers
- `ModifyUndoItemType` records are pushed into the per-backend undo
  log on each UPDATE. These records are what `apply_undo_stack`
  walks to revert in-memory mutations during a normal (in-process)
  abort.
- On `_exit(2)` via quickdie, the per-backend undo log is in shared
  memory but not flushed to disk. After recovery, the undo log is
  reinitialised — any in-flight tx's undo records are lost.
- If those undo records would have been needed to revert the on-disk
  state of partially-modified pages, recovery doesn't have the
  information to do so.

H3 is a generalised version of H1 (same mechanism, expressed at the
undo-log layer).

## What's NOT yet verified

1. **Which row holds the stray token after recovery.** We haven't
   inspected the post-recovery table state yet. Knowing which `id`
   has the unexpected token, and what that token's value is (whether
   it matches `n_accounts + writer_id` for some writer), would
   localise the bug to one writer's specific commit attempt.
2. **Whether the PK or SK has the orphan entry.** Cross-check
   `SELECT id, token FROM o_bank_account ORDER BY id`
   against `SELECT id, token FROM o_bank_account ORDER BY token`
   — disagreement indicates SK has an entry the PK doesn't (H2).
3. **Whether the orioledb checkpointer ran between the writer's
   in-buffer UPDATE and the SIGQUIT.** The current trace doesn't
   include checkpoint timestamps; we'd need to grep for orioledb's
   checkpoint LOG lines and correlate.
4. **Whether a single PANIC + recovery cycle is enough.** Currently
   running with `RR_ASSERT_FIRINGS=3` so we get 3 crashes per test.
   Reducing to 1 would tell us whether the bug needs cumulative
   damage across multiple recovery cycles, or one is enough.
5. **Whether reducing writer count below the bisect floor (W=10)
   eliminates the bug entirely.** We bisected down to W=10 and rate
   stayed at ~20 %, but didn't try W=2 or W=5. A bug that needs
   "many writers actively mutating at the moment of crash" hints at
   a different root cause than "any writer hitting the wrong
   window".

## Concrete next-step recipes

### A. Dump the failing table state (highest leverage)

Modify the post-test invariant check to print the row(s) with the
unexpected token before failing the assertion. Something like:

```python
final_total, unique_tokens, final_rows = ... # existing
if unique_tokens != n_accounts:
    pk = node.execute("SELECT id, token FROM o_bank_account ORDER BY id")
    sk = node.execute("SELECT id, token FROM o_bank_account ORDER BY token")
    print('[pk-scan]', pk, flush=True)
    print('[sk-scan]', sk, flush=True)
    # diff to find rows where SK has an entry PK doesn't reference
    pk_set = set((r[0], r[1]) for r in pk)
    sk_set = set((r[0], r[1]) for r in sk)
    print('[pk-only]', pk_set - sk_set, flush=True)
    print('[sk-only]', sk_set - pk_set, flush=True)
self.assertEqual(unique_tokens, n_accounts, ...)
```

### B. Reduce firings to 1 + confirm single-cycle bug

```bash
RR_ASSERT_FIRINGS=1 ...   # one crash per test
```

If the bug still reproduces, the partial-page-survives-recovery race
is sufficient in a single cycle. If it only reproduces with multiple
firings, recovery-time state accumulates and amplifies.

### C. Correlate with orioledb checkpoint timestamps

Grep the saved log for orioledb checkpoint LOG lines around the
crash, and see if a checkpoint completed between the surviving
writers' UPDATE 1 and the SIGQUIT fan-out.

### D. Compare WAL flush LSNs with checkpoint LSN

The `wal-trace flush-done ... lsn=X/Y` lines give us each commit's
LSN. orioledb's checkpoint records its LSN somewhere — if we can
find the last checkpoint LSN before the crash, we know what WAL gets
replayed during recovery. Any writer whose modify-record LSN is
*before* the checkpoint but whose COMMIT-record LSN is *after* (or
absent) is a candidate for partial-page survival.

## Why this is a real production hazard

The injection models a **single-backend uncatchable crash**, which
is a common production event.

When one backend dies uncatchably, the postmaster observes the
abnormal exit and fans out SIGQUIT to every other backend; each one
runs `quickdie()` → `_exit(2)` (no `XactCallbacks`, no
`AbortTransaction`, no undo replay). The critical detail is the
**~10-100 ms window** between the first crash and SIGQUIT reaching
the others: during that window other writers and the checkpointer
keep running and may flush partial state to disk. That window is
what exposes the recovery-time inconsistency this bug surfaces.

**Production events that reproduce the same race:**

- Any `Assert` failure or `ereport(PANIC)` in any backend (commit-,
  recovery-, or any other code path that ends in `abort()`).
- SIGSEGV / SIGBUS from a backend bug, hardware ECC error, mmap /
  storage fault.
- OOM-killer choosing a backend.
- `pg_ctl stop -m immediate` (admin-issued SIGQUIT fan-out).
- Postmaster TERMing a backend that ends up escalating to PANIC
  inside a critical section.

So the bug is reachable in any normal Linux deployment under normal
memory pressure / hardware / admin practices. The injection is just
the easiest reliable reproducer; the race itself is naturally
occurring.

The deadlock fix (CSN precommit ordering) addressed visibility races
at the live tx layer. This bug is at the recovery layer — orthogonal
fix needed.

## Files / artefacts

- Test: [`rr_stress_test.py`](rr_stress_test.py) (`assert_chaos_loop`
  worker, `RR_ASSERT_FIRINGS` env var).
- Injection site: [`src/transam/undo.c`](../../src/transam/undo.c) —
  search for `orioledb-commit-assert` inside the
  `START_CRIT_SECTION` bracket. Also `commit-assert-trace pre/post`
  log lines around the bracket.
- WAL emission traces: [`src/recovery/wal.c`](../../src/recovery/wal.c)
  — `wal-trace` LOG lines in `add_xid_wal_record`,
  `add_modify_wal_record_extended`, `add_finish_wal_record`,
  `flush_local_wal`.
- Saved logs of bug-reproducing runs:
  `test/t/crash/results/<timestamp>_solo___none__.log` (look for
  `AssertionError: 101 != 100` in the test stdout; the
  corresponding log is the one whose timestamp matches the failing
  run).
- Related: [`ISSUE_DRAFT.md`](ISSUE_DRAFT.md) covers the
  precommit-spin deadlock (different bug, fixed in main);
  [`tx_flow.md`](tx_flow.md) and [`tx_flow_formatted.md`](tx_flow_formatted.md)
  document the full COMMIT / ABORT / UPDATE WAL flows.
