# replica-xmin-monotonicity

## Merge note

Merges two independently-written files that both corrected the same
`sut-analysis.md` claim about orioledb#889 and reached the same conclusion:
`standby-globalxmin-regression-guard.md` and this file
(`replica-xmin-monotonicity.md`, kept as the canonical slug — renamed from
`standby-globalxmin-regression-guard` for naming consistency with sibling
properties `replica-globalxmin-catchup-lag.md` and
`replica-undo-reclaimed-too-early.md`, all from the same investigation).

## Category

Distributed coordination / streaming-standby recovery liveness (sibling to
`recovery-finish-abort-livelock.md`; same investigation, distinct bug and
distinct fix — #876 vs #889).

## What led to identifying this property

While validating the "streaming-standby livelock" lead from `sut-analysis.md` §2/§8, git history showed the livelock investigation actually covers **two** distinct issues, both fixed on `main`:

- **#876** — a standby never learns that the primary aborted an in-flight oxid during its own crash recovery (covered in the sibling property `recovery-finish-abort-livelock.md`).
- **#889** — a standby's `runXmin`/`globalXmin` horizon can **regress**, which independently causes the same class of symptom: `oxid_get_csn()`'s fast path mis-reads a legitimately `FROZEN` xid-map slot as `IN_PROGRESS` once the horizon has slid backward across it, and `o_btree_modify_handle_conflicts` livelocks waiting for a resolution of an oxid that is not actually in flight.

`sut-analysis.md` §4 separately flags "**globalXmin monotonicity** is an explicit invariant (`src/recovery/recovery.c:2683-2691`), enforced only via `Assert()` — a no-op in non-assert builds." That exact invariant is what #889's fix turned into an explicit, named guarantee.

## Root-cause doc (read in full via `git show`, unmerged branch)

`origin/add_stress_bank_account_test:test/t/crash/ISSUE_recovery_committed_oxid_reverts_to_inprogress.md`
(321 lines) gives a rigorous, instrumented root-cause chain: a deferred
`recovery_finish()` rollback is stamped with `runXmin`-at-emission time
(`wal.c:485-504`), which for a late/deferred rollback of a low oxid can exceed
the oxid itself; replaying it inserts the low oxid into the standby's
`xmin_queue` below the already-advanced `writtenXmin`, and `update_run_xmin()`
writes `globalXmin` backward into an already-FROZEN band. The doc's own §9
lists five fix directions, ordered from "fix the stamp at the source" to
"defense in depth." The doc's own §5 calls the mechanism "CONFIRMED" only
after an instrumented catch, and this repo's history around this exact bug is
unusually deep — dozens of near-duplicate-titled commits suggest a long
iterate-hunt-fix-regress cycle, consistent with the bug's own description as
genuinely hard.

Note: the specific hashes named in `sut-analysis.md` (`fb1a8acc`, `714c99ca`,
`3ea73f3d`) do not exist anywhere in this repository — likely hashes from a
different lineage of the same investigation (this bug has extensive
branch/rebase history).

## Mechanism and fix (validated directly against current code)

- `test/t/replication_test.py::test_recovery_finish_rollback_does_not_regress_replica_xmin` (present on `main`, docstring cites "Issue #889" verbatim) describes the pre-fix bug: a long-running transaction whose modifications never overflow the local WAL buffer stays invisible to the standby pre-crash; the primary's checkpoint records it as in-flight in its xids file (a file local to the primary); after a `SIGKILL` + restart, the standby's `runXmin`/`globalXmin` could end up regressed relative to what the horizon should be, breaking the FROZEN fast path.
- The actual fix chain that **is** an ancestor of `a975c702` (verified with `git merge-base --is-ancestor <hash> a975c702`, all "YES"): `9bc39d3b` (emit `WAL_REC_ROLLBACK` for in-flight oxids aborted by `recovery_finish`, resolves #876), `ef8e93b9` ("recovery: defer `free_run_xmin` + seed horizons from checkpoint retain range", explicitly "Fixes orioledb/orioledb#889"), `a0d628c1` ("recovery: drop fast-path-aborted oxids off `xmin_queue` in `update_run_xmin`" — the final iteration of the fix, precisely fix-direction §9(2)/§11 from the root-cause doc), followed by a long visible chain of iteration commits (`746e1ef2` "tear down drained fast-path-aborted state fully", `8c2588d6` "allocate xmin_queue only for the recovery leader", `9ec6d26a` "Add tests for runXmin >= globalXmin invariant on secondary node") — all confirmed ancestors of `a975c702`. `a0d628c1`'s own commit message states: "All three #876 / #889 / floor-seeding replication tests still pass" — direct evidence the author treated this as the closing fix for both issues together.
- Concretely, `update_run_xmin()` (`src/recovery/recovery.c`, current code) now (a) drains any oxid off `xmin_queue` that was named in the checkpoint's xids file but never produced a `WAL_REC_XID` on the wire and whose oxid is already below `recovery_xmin` — recognizing the primary's `wal_rollback()` no-WAL fast-path abort — and (b) replaces the old `if (xmin < globalXmin) globalXmin = xmin;` (a write that could regress the published horizon) with `Assert(xmin >= pg_atomic_read_u64(&xid_meta->globalXmin));` in **both** `update_run_xmin()` and `free_run_xmin()`. The same pattern change was made in both functions — read `a0d628c1`'s full diff to confirm this directly: it turns the silent-regression write into a hard (debug-build) invariant violation if the fix chain above it is ever incomplete, and drains fast-path-aborted oxids off `xmin_queue` before they can pin the horizon backward.

## Why this is still worth an Antithesis property despite being "fixed"

The monotonicity guarantee that used to be enforced by a mutating write is now enforced **only by `Assert()`** — exactly the no-op-in-release-builds concern `sut-analysis.md` §4/§11 raises generally for this codebase's `Assert()`-backed invariants. If Antithesis's images are built without assertions enabled (an open question — see below), a regression that reintroduces a backward globalXmin move would silently continue rather than crash, and the *symptom* (a livelocked recovery worker on the standby) is the only externally observable signal left. That makes this a high-value place to either (a) confirm assert-enabled Antithesis images, or (b) add an explicit SDK assertion at the same call sites so the invariant is checked regardless of build type.

Given the fix chain is present, this property is a **regression guard**: across the analyzed commit, `globalXmin` on any recovering/standby process must never move backward, and specifically must never drop below `writtenXmin` (the frozen-slot high-water mark). The new test added by `9ec6d26a` ("runXmin >= globalXmin invariant on secondary node") is exactly this claim, already checked in — but only as a `testgres` unit test, not as an Antithesis property that a fuzzed fault schedule can try to violate.

## Why It Matters

`globalXmin`/`runXmin` regressing is a horizon-consistency bug with the same shape as clock-skew or stale-leader bugs in distributed consensus systems: a participant (the standby) computes visibility/liveness decisions off a value that must only move forward, and any backward move corrupts every subsequent decision built on it (here: whether an xid-map slot means "definitely resolved" or "still must wait"). The bug is silent until a livelock or (potentially) an MVCC-visibility bug surfaces downstream.

## Antithesis angle

A workload mixing: (a) long-running low-volume transactions on the primary that stay below the WAL-buffer-overflow threshold (so they're invisible to the standby pre-crash, matching the #889 repro recipe), (b) bursts of short committed transactions to advance `nextXid` far past those long transactions, (c) a `CHECKPOINT`, (d) an unclean primary crash (`SIGKILL` of the whole process tree), all under Antithesis's own fault injection for timing rather than the deterministic test's fixed pgbench recipe — mirroring exactly the no-injection reproducer in the root-cause doc (`RR_KILL_POSTMASTER=1`, plain `SIGKILL`, no injection points needed — the doc explicitly notes this reproduces "under plain SIGKILL of the primary postmaster, no injection point attached," with a ~12-30% repro rate per trial in various configurations). The property to watch is the same one the existing test checks explicitly via `orioledb_get_xid_meta()` (`runxmin`, `globalxmin` columns) on both nodes. Needs a streaming-standby topology (same gap noted throughout this focus).

## Invariant / Assertion type

**Safety**, `Always`:
- SUT-side: replace/augment the two `Assert(xmin >= pg_atomic_read_u64(&xid_meta->globalXmin))` sites (`update_run_xmin()`, `free_run_xmin()` in `src/recovery/recovery.c`) with an `always()` call carrying the observed `xmin` and current `globalXmin` as payload, so the check fires unconditionally regardless of build type.
- Workload-side complement: periodically sample `orioledb_get_xid_meta()` on both primary and any streaming standby and assert `always()` that (i) each node's own `runxmin`/`globalxmin` never decreases between successive samples, and (ii) the standby's lag behind the primary stays within a generous bound (mirroring the existing test's `assertLess(master_runxmin - replica_runxmin, 1000, ...)` — a loose bound to tolerate legitimate bookkeeping bloat while catching a catastrophic/unbounded pin).

## SUT-side instrumentation candidates

- The two `Assert()` sites above are the direct, minimal-effort candidates — they already encode exactly the right condition; they just need to fire in non-assert builds too.
- `update_run_xmin()`'s new fast-path-abort drain loop (the `while (!pairingheap_is_empty(xmin_queue)) { ... }` block added by `a0d628c1`) is itself a good `reachable()` target — confirming the drain path (which only fires for the specific `checkpoint_xid && !wal_xid && oxid < recovery_xmin` combination) is actually exercised under a chaos workload, not just the deterministic test's exact recipe.

## Open Questions

- Are Antithesis's target build images compiled with assertions enabled? This determines whether the current `Assert()`-only invariant already gives a signal today, or whether adding an explicit SDK assertion is required before this property can be checked at all. Flagged in `sut-analysis.md`'s own catalog-wide Open Questions as unconfirmed; carries directly into this property. `(needs human input)`
- Whether `free_run_xmin()`'s comment ("globalXmin is the actual horizon, including any live read-only sessions that survive a promote... leave globalXmin alone") is itself fully correct is not independently re-derived here — taken from the commit's own reasoning, not re-verified against a live promote scenario.
- Is the fix chain actually complete, or could a slightly different fault ordering (not covered by `9ec6d26a`'s specific test) still regress `globalXmin`? Not verified beyond code reading — this property's value is precisely to let Antithesis's fuzzed fault schedules probe for exactly that residual case, since the bug's own history shows multiple earlier "fixes" needed follow-up iteration.
- `ef8e93b9`'s own commit message documents a **currently-admitted separate open issue**: a restored test, `test_recovery_finish_rollback_does_not_regress_replica_xmin`, "currently fails on a separate 'stuck low globalXmin' symptom (replica's globalXmin does not catch up to master's after recovery completes)... kept in place so the diagnostic surface remains visible while maintainer iterates on that orthogonal issue." This is a distinct, apparently-still-open liveness-flavored issue — see `replica-globalxmin-catchup-lag.md` for a dedicated property. `(needs human input: is this test currently expected to fail in CI, i.e. is it skipped/xfail, or does it actually fail the suite today?)` — no `expectedFailure`/`skip` decorator was found on it in `test/t/replication_test.py`; the suite was not run to confirm.

### Investigation Log

#### Does the #889 fix (globalXmin regression) actually land on `main`, and is the monotonicity invariant Assert-only as sut-analysis.md's general claim suggests?

- Examined: `git log --all --oneline --grep="889"`; `git merge-base --is-ancestor a0d628c1 HEAD` and for the chain of related commits (`ef8e93b9`, `bda0c10c`, `eb3b765f`, `86422b7a`, `81e8edcc`, `1c2ac4ba`, `4bdde236`, `03db2496` — all found NOT ancestors, confirming they are unmerged-branch experiments); `git show a0d628c1` full diff; current `src/recovery/recovery.c` `update_run_xmin()`/`free_run_xmin()`; root-cause doc `ISSUE_recovery_committed_oxid_reverts_to_inprogress.md` (321 lines, read in full).
- Found: `a0d628c1` and `ef8e93b9` are both ancestors of `main`/`HEAD`; the diff replaces the old regressing write with `Assert(xmin >= globalXmin)` in both functions, exactly matching `sut-analysis.md` §4's general observation about this invariant's enforcement mechanism, now tied to a specific, confirmed-fixed bug (#889) rather than a generic worry.
- Not found: whether the Antithesis build config enables assertions (out of scope for source-code-only investigation).
- Conclusion: property built on a confirmed-present fix; the Assert-only enforcement question is carried forward as an explicit open question rather than assumed either way. (Independently confirmed by two separate discovery passes, merged into this file.)
