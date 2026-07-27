# recovery-sk-rebuild-desync

## Summary

The single highest-priority finding from this pass: a distinct, thoroughly-
investigated bug family in which a secondary (unique) index diverges from the
primary key after crash recovery — **caused by recovery's replay/rebuild of
*other, cleanly-committed* transactions' SK entries, not by anything about the
crashing transaction itself.** Two companion root-cause docs
(`ISSUE_TOKEN_LEAK_COMMIT_ASSERT.md`, `ISSUE_TOKEN_LEAK_PRE_COMMIT_WAL_FINISH.md`)
each read in full (395 and 155 lines) document this from two different
injection placements, both concluding the same thing. I found **no
corresponding fix commit** for this specific bug anywhere in the repository's
history (searched by title/keyword; see below), making this the strongest
candidate in this focus for a currently-open, previously-undiscovered-by-this-
harness data integrity bug.

## What led here

Both docs are unusually rigorous — each is structured as a sequence of
explicitly ruled-out hypotheses with quantitative evidence, not a single
anecdote:

- `ISSUE_TOKEN_LEAK_COMMIT_ASSERT.md`: systematically moved the fault
  injection to four different points along the commit pipeline (original
  post-`wal_commit`-flush site; the very end of `XACT_EVENT_COMMIT`; the
  entrance of `undo_xact_callback` before any WAL is written; PG-side
  `finish_xact_command()` before `CommitTransactionCommand()` is even called)
  and found the SK-leak bug reproduces at every single site (30%, 30%, 14%,
  8% respectively) — including the last one, where "the crashed backend has
  done nothing orioledb-specific for its current tx." The doc's own
  conclusion, quoted directly: "The bug does not depend on the crashed
  backend at all. It depends only on the cluster going through crash-recovery
  while some *other* (non-crashed, cleanly-committing) transactions had
  recently flushed their WAL_REC_COMMIT records to disk. Recovery's SK
  rebuild for those committed peers is the broken path."
- It also separately ruled out "mid-abort interruption causes the leak" via a
  controlled log-correlation study (11 saved logs, mid-abort presence
  uncorrelated with bug reproduction: 2/3 buggy logs had zero mid-abort
  backends; 6/8 normal logs had at least one).
- `ISSUE_TOKEN_LEAK_PRE_COMMIT_WAL_FINISH.md` reproduces the same symptom
  (SK entry count off by ±1/±2/±4 from expected) from a narrower, single
  injection site (`src/recovery/wal.c:336`, inside `wal_commit`, after modify
  records are flushed but before `WAL_REC_COMMIT` is appended) at ~67% catch
  rate — i.e. a crashed transaction with **no finish record at all** on disk
  (which recovery should, by the standard rule, discard entirely) still
  sometimes leaves a stale SK entry.
- Both docs report `orioledb_tbl_check('o_bank_account')` returning `true`
  while the divergence is present — i.e. **the existing structural
  consistency checker (the same oracle `sk-recovery-race` relies on) does not
  catch this class of bug.** This is a materially important finding for how
  much to trust `orioledb_tbl_check()` as a general-purpose Antithesis oracle
  beyond the specific #855 shape it was built to catch — corroborating
  `sut-analysis.md` §8's independent observation that the checker itself has
  had correctness problems.
- Both docs' diagnostic methodology is itself sound and reusable: a
  PK-forced Seq Scan (index-organized, so it reads the PK tree directly)
  confirms PK correctness (row count, distinct ids, distinct tokens, balance
  sum all exactly right), while the token-uniqueness check is forced through
  an Index-Only Scan on the SK (`Heap Fetches: 0`), so the two numbers are
  guaranteed to be answered from different physical structures — ruling out
  "the query planner masked the divergence" as an explanation.

## Fix status — not found

I searched `git log --all --oneline -i --grep` for "token leak", "SK leak",
and related phrasing, and found only harness-side commits (`rr_stress:
localize SK leak by diffing SK token set vs PK token set`,
`e0233226 rr_stress: run_hunt verifies sk-forced uses token_uniq SK...`) —
i.e. commits that *build the detection harness*, not commits that fix the
underlying recovery-side SK rebuild. I did not find a commit whose message
plausibly describes fixing "recovery doesn't delete the old SK entry on a
column-changing UPDATE" or similar. The docs' own "Most likely sub-mechanism"
section hypothesizes recovery treats a WAL_REC_UPDATE that changes an indexed
column as insert-only (missing the corresponding delete-old-key step) on the
SK side — but this is explicitly labeled a hypothesis with candidate code
locations (`src/recovery/recovery.c`'s WAL replay dispatcher,
`src/btree/modify.c`'s `o_btree_modify_internal`), not a confirmed root
cause. I did not trace this further myself this pass (time-boxed) — the
docs' own "Still-open hypotheses" section (three numbered items: dirty
checkpointed SK page, double/under-applied undo-log replay of the SK side, or
composite page-state convergence) is the most current state of the
investigation.

## What goes wrong

Any unhandled fault (Assert/OOM/PANIC/SIGKILL) that crashes the cluster while
at least one *other*, unrelated transaction's `WAL_REC_COMMIT` sits durable
on disk produces a **permanent** PK↔SK divergence after recovery — not
repaired by subsequent restarts, since the diverged SK page becomes the new
source of truth. Per the docs: "Queries that the planner answers from the
unique index return wrong counts and may either return rows that no longer
exist or omit rows that do" — and the divergence is silent at the level of
`orioledb_tbl_check()`.

## Antithesis angle

This is directly testable with the *existing* `sk-recovery-race[-chaos]`
harness's table shape and PK/SK-count assertion pattern, but needs a
different trigger: rather than pinning a 3-way DML race at a specific
stopevent, it needs (a) sustained concurrent DML committing normally, and
(b) an unrelated fault (crash/PANIC/SIGKILL) landing at effectively any point
in the commit pipeline of *some* backend — which Antithesis's own fault
injection is naturally suited to provide without needing a deliberately
placed stopevent, unlike the #855 race. The existing `always()` assertion
(PK-count == distinct-SK-count, plus `orioledb_tbl_check()`) is exactly the
right check — the gap is in workload/fault shape, not assertion design. Given
`orioledb_tbl_check()` is confirmed blind to this class, this property's
value is specifically in the count-comparison half of the existing
assertion, not the structural-check half.

## Existing assertion cross-reference

The invariant is **already checked** by the existing `always()` assertions in
both `sk-recovery-race/driver.py` and `sk-recovery-race-chaos/driver.py`
(PK-row-count == distinct-SK-token-count). No new assertion is required —
what's missing is a workload/fault-injection shape that doesn't rely on the
`sk_modify_pending` stopevent at all, letting Antithesis's generic fault
injection (not a deliberately-placed race window) trigger the divergence,
since the docs show the bug reproduces via faults that have nothing to do
with that specific stopevent's window.

## Open Questions

- What is the actual root cause in `src/recovery/recovery.c` /
  `src/btree/modify.c`? The docs leave this as an open hypothesis (SK-side
  delete-old-key step missing on WAL_REC_UPDATE replay for column-changing
  updates). `(needs further code-reading investigation — not done this pass,
  time-boxed)`.
- Is this bug present at the analyzed commit `a975c702`, or was it fixed by
  a commit whose message doesn't mention "token"/"SK leak" (making it
  invisible to my keyword search)? `(partial: no plausibly-matching fix
  commit found by keyword search; not confirmed absent by code tracing)`.
  Given the severity and the lack of any harness-independent fix evidence,
  this should be treated as likely still open unless independently
  disproven.

### Investigation Log

#### Is this bug present at the analyzed commit `a975c702`, or was it fixed by a commit whose message doesn't mention "token"/"SK leak"?

- Examined: `git log --all --oneline -i --grep` for "token leak", "SK leak", and related phrasing.
- Found: only harness-side commits that build the detection harness (e.g. `rr_stress: localize SK leak by diffing SK token set vs PK token set`, `e0233226 rr_stress: run_hunt verifies sk-forced uses token_uniq SK...`) — no commit plausibly fixing the underlying recovery-side SK rebuild.
- Not found: no confirmation by direct code tracing of `src/recovery/recovery.c`/`src/btree/modify.c` that the bug is actually absent at this commit.
- Conclusion: tagged `(partial: no plausibly-matching fix commit found by keyword search; not confirmed absent by code tracing)` — treat as likely still open unless independently disproven.
