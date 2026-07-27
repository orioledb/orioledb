# recovery-finish-abort-livelock

## Merge note

Merges two independently-written files that both independently validated and
corrected the same `sut-analysis.md` claim about orioledb#876:
`recovery-finish-rollback-wal-emission.md` and this file
(`recovery-finish-abort-livelock.md`, kept as the canonical slug).

## Category

Distributed coordination / streaming-standby recovery liveness.

## Correction to sut-analysis.md — validated independently by two passes, conclusion changed

`sut-analysis.md` §2/§8 describes this as "a third, currently-unresolved bug found in unmerged branch history (not on `main`): streaming-standby livelock," citing a root-cause doc on `origin/add_stress_test_pr` and stating the fix (`fb1a8acc`/`714c99ca`/`9bc39d3b`) "was attempted **and reverted** (`3ea73f3d`, 2026-06-15) — **not present on `main`**."

Per `references/validating-claims.md` (treat leads as leads, discriminate rather than cite), this was re-checked directly against git history rather than the doc, independently, in both source passes:

```
$ git merge-base --is-ancestor 3ea73f3d HEAD && echo YES || echo NO
NO
$ git merge-base --is-ancestor 9bc39d3b HEAD && echo YES || echo NO
YES
$ git branch -a --contains 9bc39d3b   # includes: main, artur/antithesis-workload, origin/main, ...
$ git branch -a --contains 3ea73f3d   # only: remotes/origin/add_stress_test_pr
```

- `9bc39d3b` ("Emit WAL_REC_ROLLBACK for in-flight oxids aborted by recovery_finish") **is** an ancestor of current `HEAD` (`a975c702`) and of `main`.
- `3ea73f3d` (the revert cited by the doc) is **not** an ancestor of `HEAD`/`main` at all — it lives only on the unmerged branch(es) where the doc was found.
- Reading `3ea73f3d`'s diff shows it reverts `fb1a8acce6756db37773eb14c09dadb295b0d767` specifically — an *earlier* attempt at the same fix (`fb1a8acc`), not the later one (`9bc39d3b`) that actually shipped. The true sequence: an early attempt (`fb1a8acc`) was made and reverted (`3ea73f3d`) on a side branch, then the fix was reattempted (`714c99ca`, `9bc39d3b`) and **that** version is what actually merged to `main` — the SUT analysis's doc evidently predates the successful re-landing, or conflated the reverted early attempt with the final one.
- The current code (`src/recovery/recovery.c:1844-1885`, function `o_emit_recovery_finish_rollbacks()`) directly implements the described fix: it walks a list of oxids `recovery_finish()` aborted in-memory (`recovery_finish_aborted_oxids`, populated at `recovery.c:1745-1768`) and calls `wal_emit_recovery_finish_rollback()` for each, from the `after_checkpoint_cleanup_hook` once `LocalSetXLogInsertAllowed()` has run. The function's own comment cites "issue #876" and explains the exact livelock this fixes: "streaming standbys that eagerly applied the in-flight txn's modify records hold the oxid INPROGRESS forever, and any later replayed modify targeting the same row spins in `o_btree_modify_handle_conflicts` (issue #876)."
- A regression test for exactly this scenario exists and is present on `main`: `test/t/replication_test.py::test_recovery_finish_aborts_propagate_to_replica` (docstring cites "Issue #876" verbatim, same mechanism).

**Conclusion: the sut-analysis.md claim that this livelock is unfixed on `main` does not hold up.** The doc it was sourced from appears to describe an earlier, mid-investigation snapshot (the unmerged branch's own revert/re-fix experimentation, dated between 2026-06-03 and 2026-06-15) that predates the version of the fix that actually landed on `main`. This property is therefore reframed as a **regression target for an already-fixed, non-trivially-iterated bug** (property-catalog.md explicitly recommends this: "a recently-fixed bug is a great Antithesis test because the fix may not cover all edge cases"), not as an open unfixed defect.

## Mechanism

1. A long-running transaction on the primary performs enough writes to overflow the 8 KB `local_wal` buffer, so its row-modify records stream to a connected standby **before** the transaction commits or aborts (eager per-backend WAL streaming, matching `sut-analysis.md`'s general description of this pattern in §2).
2. The primary crashes (or is force-killed) before the transaction reaches `COMMIT`/`ROLLBACK`. On restart, `recovery_finish()` (`src/recovery/recovery.c:1698-1826`) aborts this in-flight oxid **purely in memory** (`walk_checkpoint_stacks(..., COMMITSEQNO_ABORTED, ...)`) as part of crash recovery.
3. Without emitting any WAL for that abort, a streaming standby that already applied the txn's modify records has no way to learn the oxid resolved — it stays `INPROGRESS` on the standby forever.
4. Any later replayed modify against a row touched by that phantom-in-progress oxid spins forever in `o_btree_modify_handle_conflicts` (`src/btree/modify.c:430`, via `wait_for_oxid()` at `oxid.c:1083`) waiting for a resolution that will never come from the primary — a genuine unbounded livelock on the standby's recovery worker.

## The fix (present on `main`) — confirmed via direct code reading, not just commit ancestry

- `src/recovery/recovery.c:294-308` — `recovery_finish_aborted_oxids` array, explicitly commented: "In-flight oxids that recovery_finish() aborted in memory. These were left COMMITSEQNO_INPROGRESS at end-of-redo with no COMMIT/ROLLBACK on the wire, so a streaming standby cannot resolve them on its own. Captured here for the after-checkpoint hook to flush as WAL_REC_ROLLBACK once LocalSetXLogInsertAllowed() has run (issue #876)."
- `src/recovery/recovery.c:1698-1790`, inside `recovery_finish()`: when `worker_id < 0` (main recovery process only) and an in-flight oxid is discovered at `COMMITSEQNO_INPROGRESS`, it's appended to `recovery_finish_aborted_oxids` (capacity-doubling array, `TopMemoryContext`) instead of being silently dropped.
- `src/recovery/recovery.c:1810-1822` (comment right after the loop): explains `runXmin` is deliberately **not** advanced inside `recovery_finish()` — doing so before the rollback WAL is flushed would let a post-recovery checkpoint persist an advanced horizon before the standby has seen the justifying ROLLBACK records, which the comment says would "drag [globalXmin] back across slots already stamped FROZEN, breaking `oxid_get_csn()`'s fast-path (orioledb/orioledb#889)" — i.e., the fix's authors identified and closed a second-order bug (#889) introduced by naively fixing the first one (#876). See the sibling property `replica-xmin-monotonicity` for that half of the same investigation.
- `src/checkpoint/checkpoint.c:1896-1906`, inside `o_after_checkpoint_cleanup_hook()`: `if (flags == 0) o_emit_recovery_finish_rollbacks();` with the comment "Right after end-of-recovery, XLog inserts have just been enabled. Flush WAL_REC_ROLLBACK markers for in-flight oxids that recovery_finish() aborted in memory, so streaming standbys can resolve them too (issue #876)."
- `src/recovery/wal.c:325-362`, `wal_emit_recovery_finish_rollback()`: emits a stand-alone `WAL_REC_ROLLBACK` for each captured oxid, with an extensive comment restating the exact livelock mechanism ("Without an explicit ROLLBACK marker on the wire, the standby holds the oxid INPROGRESS forever and livelocks on the next conflicting modify (orioledb/orioledb#876)").

## Why It Matters

This is precisely the "stale leader / replica never learns the outcome" pattern this focus is meant to catch: the primary silently resolves state in memory during its own crash recovery and, without this fix, never communicates the resolution downstream — the standby is left waiting on a decision that already happened. Given the multi-iteration fix history (at least 4 distinct attempts across different unmerged branches — `recovery-finish-fastpath-abort`, `add_stress_bank_account_test`, `replica-runxmin-from-recovery-xmin[-2]`, `add_stress_test_pr` — before the version on `main` landed), this is exactly the kind of area where the *fix itself* may have edge cases Antithesis's fault injection can still find (e.g., a second primary crash occurring between `recovery_finish()` populating the list and `o_emit_recovery_finish_rollbacks()` flushing it, or the emitted rollback itself racing a concurrent checkpoint).

## Reframing as a property (per property-catalog.md's "Cross-Reference Closed Issues")

**Type:** Safety (regression guard on a fixed bug) — **not** a currently-open defect; do not represent it as one. Liveness-shaped in its consequence (a livelock), but the assertion is best framed as a bounded-progress safety check.

**Property:** After a primary crashes with in-flight (uncommitted, unaborted) transactions and a streaming standby has eagerly replayed their modifications as `COMMITSEQNO_INPROGRESS`, once the primary restarts and completes recovery, every such oxid is eventually resolved on the standby (via the `WAL_REC_ROLLBACK` emitted by `o_emit_recovery_finish_rollbacks()`) — no oxid is left permanently `INPROGRESS`, and no later conflicting modify against rows touched by that oxid livelocks in `oxid_get_csn()`.

**Invariant:** `Always`-style bounded-progress check (the standard Antithesis idiom for livelock: assert that no observed "waiting since" duration exceeds a generous bound, rather than `Sometimes` — the guarantee is that this always resolves, not that it resolves at least once):
- `always(no_oxid_remains_inprogress_beyond_bounded_time_after_primary_recovery)`: after a primary crash that leaves an in-flight oxid whose modify records already reached a streaming standby, a subsequent conflicting modify against the same row resolves (the standby's replay LSN advances past a probe write) within a bounded wall-clock window — mirroring `test_recovery_finish_aborts_propagate_to_replica`'s existing assertion shape (`self.assertFalse(replica_crashed, ...)` after a bounded `poll_query_until`).
- Complemented by `sometimes(recovery_finish_aborted_oxids_nonempty)` — confirms the interesting path (recovery actually aborting in-flight oxids in memory, not just the common case of clean shutdown with no in-flight transactions) is reached at all; without this, an `always()`-only check could pass vacuously in a run that never exercises the aborting path.
- Optionally, `reachable()` on the `o_emit_recovery_finish_rollbacks()` emission path itself firing with a non-empty `recovery_finish_aborted_oxids` list.

**Antithesis Angle:** Run a workload that deliberately creates long-running, large (buffer-overflowing) transactions concurrently with primary crashes (`SIGKILL`/`-m immediate`) while a streaming standby is attached, then continues issuing modifies against the same key ranges. Needs a primary + streaming-standby topology (same gap noted generally in `sut-analysis.md` §9). Antithesis's fault injection (random process kills at arbitrary points, network delay between primary and standby) explores crash timing the deterministic test's fixed recipe (INSERT 2000 rows, then force-crash) cannot reach — e.g., crashing during the deferred-rollback emission window itself, or interleaving with a second recovery. This is exactly the most promising angle for finding a residual gap in the fix (see Open Questions).

**Why It Matters:** Confirms a real, previously-reproduced (per the doc, "~1-in-6-to-11 repro rate") livelock class stays fixed as the codebase evolves, and specifically stresses the fix's own new invariant (deferred `runXmin` advance) which is exactly the kind of subtle sequencing a future refactor could break without any test catching it — nothing in the Antithesis harness or, as far as this pass could tell, the deterministic Python suite covers this path with a repeatable/forced construction (only `replication_test.py::test_recovery_finish_rollback_does_not_regress_replica_xmin`, added then reverted per `git log`, suggests the team itself struggled to keep a stable deterministic test here).

## SUT-side instrumentation candidates

- No stopevent currently pins this window (unlike the sk_modify_pending/#855 race or the abort-snapshot race in `checkpoint-abort-snapshot-standby-panic.md`). A stopevent inside `recovery_finish()` right after populating `recovery_finish_aborted_oxids` (or inside `o_emit_recovery_finish_rollbacks()` before the emit loop) would let a future deterministic test — and Antithesis reachability checks — target this precisely.
- `elog(LOG, "orioledb: emitting WAL_REC_ROLLBACK for in-flight oxid %lu aborted by recovery_finish", ...)` (`recovery.c:1860`) is already a distinctive log line; an `antithesis-query-logs`-style correlation (does this log line ever precede a standby stall/timeout?) is a cheap way to validate this from existing instrumentation without adding new SDK calls.
- `wal_emit_recovery_finish_rollback()` itself already logs `elog(DEBUG1, "recovery-finish ROLLBACK oxid...")` (`wal.c:355-356`) — a SUT-side `reachable()`/`sometimes()` call at that call site would confirm the interesting path is reached during a run, avoiding a vacuous `always()`-only check.
- One related-but-distinct existing stopevent: `STOPEVENT_BEFORE_MODIFY_OXID_GET_CSN` (`src/btree/modify.c:498`, fires immediately before `oxid_get_csn(oxid, false)` inside the modify-conflict-resolution path) sits in the same function family (`o_btree_modify_handle_conflicts`/`oxid_get_csn`) as where a stuck standby would livelock on a later conflicting modify. Its own comment describes a *different* scenario (a page-lock-held deadlock with a concurrent COMMITTING-bit aborter, not a permanently-`INPROGRESS` oxid with no WAL resolution) — plausible pinning point for constructing the "later conflicting modify" half of the scenario, but does not already assert anything about the recovery-finish/streaming-standby case.

## Open Questions

- Whether the fix fully closes the window if a *second* primary crash happens between `recovery_finish()` (memory-only abort, list populated) and the deferred `o_emit_recovery_finish_rollbacks()` flush (i.e., does the list survive across that second crash, or is it lost, silently re-creating the original livelock for that oxid on the next restart)? `recovery_finish_aborted_oxids` is a plain `palloc`'d array in `TopMemoryContext`, not obviously WAL-logged itself, so its survival across a second immediate crash is unclear. `(partial: mechanism for a single recovery pass fully confirmed and appears correct; double-crash-during-cleanup-window recursion not traced.)`
- Whether `recovery-finish-fastpath-abort`/`replica-runxmin-from-recovery-xmin[-2]` (the other unmerged branches in this fix's history) contain independent, still-unlanded refinements worth comparing against `main`'s final version — not read, out of scope for a single-focus discovery pass.
- The test `test/t/replication_test.py::test_recovery_finish_rollback_does_not_regress_replica_xmin` was added (`57e587fe`) then reverted (`e8fc5d46`) in the git history seen from `9bc39d3b`'s branch context. Whether that specific deterministic test currently exists on `main` in some form, was permanently dropped, or was replaced by something else, was not checked directly against the current `test/t/replication_test.py` file content. `(needs human input or a direct file check)`

### Investigation Log

#### Is the streaming-standby livelock (fb1a8acc/714c99ca/9bc39d3b, reverted by 3ea73f3d per sut-analysis.md) actually absent from `main`?

- Examined: `git merge-base --is-ancestor <commit> HEAD` for `9bc39d3b`, `3ea73f3d`, and `fb1a8acc`; `git branch -a --contains` for both; `git show 3ea73f3d` (confirmed it reverts `fb1a8acc` specifically, not `9bc39d3b`); `git show main:src/recovery/recovery.c` grepped for `o_emit_recovery_finish_rollbacks`/`recovery_finish_aborted`; current `src/recovery/recovery.c:1698-1885`; `test/t/replication_test.py::test_recovery_finish_aborts_propagate_to_replica` (lines 1566-1761).
- Found: `9bc39d3b` (the fix) is an ancestor of `main`/`HEAD`; `3ea73f3d` (the revert cited by the doc) is not, and reverts an earlier attempt (`fb1a8acc`), not the version that shipped. The fix function and its regression test are both present and reference issue #876 by name, matching the doc's own description of the bug mechanism.
- Not found: no evidence the fix was subsequently re-reverted or disabled on `main`.
- Conclusion: `sut-analysis.md`'s "not fixed on main" claim is incorrect for the current codebase state; reclassified this property from "open unfixed bug" to "regression target for a fixed, heavily-iterated bug." (Independently confirmed by two separate discovery passes, merged into this file.)
