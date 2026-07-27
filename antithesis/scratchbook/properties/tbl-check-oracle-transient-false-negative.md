# tbl-check-oracle-transient-false-negative

## Focus

Wildcard (attention focus 12) — directly answers the task prompt's own question: "Does the `verify_orioledb`/`orioledb_tbl_check` oracle's own historical instability suggest a meta-property about trusting the checker itself?" `sut-analysis.md` §8 already flags the checker's revert-heavy history at a high level; this property pins down a *specific, currently-reproducible* false-negative mechanism and ties it directly to the exact call pattern the existing Antithesis harness uses.

## What led to this

Git history search (`git log --oneline --all | grep -i "tbl_check\|BROKEN_SPLIT\|phase-1 split\|phantom-leak"`) turned up a dense, repeatedly-reverted cluster:

```
ea0efa7f Traverse every index before the final reporting in orioledb_tbl_check
0ea982e0 Suppress BROKEN_SPLIT NOTICE in verify_orioledb
978cfe85 Add test for verify_orioledb false positive on BROKEN_SPLIT
f7799a06 Suppress BROKEN_SPLIT NOTICE in verify_orioledb
7f3bacde Add test for verify_orioledb false positive on BROKEN_SPLIT
b15c227e test: update checkpoint_split3 phantom-leak expectations
524a0d4f Revert "Don't misreport phase-1 splits as leaked extents in orioledb_tbl_check"
da78ee75 Revert "Don't misreport phase-1 splits as leaked extents in orioledb_tbl_check"
0bb61a3c Deprecate orioledb_tbl_check() and switch all callers to verify_orioledb()
d2a2723e Don't misreport phase-1 splits as leaked extents in orioledb_tbl_check
```

Two `Revert` commits of the *same* attempted fix ("Don't misreport phase-1 splits as leaked extents") is a strong signal this false-positive/false-negative mechanism is genuinely hard to fix, not a one-off.

**Primary-source confirmation on the current tree (not just an unmerged branch):** `test/t/checkpoint_split_base_test.py` (in the worktree at commit `a975c702`, i.e. on `main`) contains, right after a checkpoint with in-flight splits:

```python
node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)"
             )  # no errors, can be true or false

if with_second_checkpoint:
    node.safe_psql('postgres', "CHECKPOINT;")
    self.assertTrue(
        node.execute(
            "SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0]
        [0])
```

The test itself documents, in a comment, that `orioledb_tbl_check()` **immediately after a checkpoint is known to return an unreliable result** ("can be true or false") — it is only asserted `true` after a **second, later, explicit `CHECKPOINT`**. This is not an inference — the team's own test suite encodes this as expected behavior on the branch actually used by Antithesis.

**Root cause, per an unmerged branch's commit message** (`origin/deprecate-orioledb-tbl-check`, commit `0bb61a3c`, read via `git show`, not on `main`/this branch):

> "Autonomous page writes during a checkpoint leave per-page state that only clears on the next explicit CHECKPOINT, so the checkpoint_split_root* tests now run that follow-up CHECKPOINT before asserting a clean verify_orioledb result."

That commit deprecates `orioledb_tbl_check()` entirely (`src/tableam/func.c`, replaced with an `ereport(ERROR, ... "no longer supported")` stub) in favor of `verify_orioledb()`. But diffing `verify_orioledb`'s implementation against the old `orioledb_tbl_check`, the underlying check (`check_btree()`) is unchanged — only the locking wrapper around it changed (`o_tables_rel_lock_extended` around the whole check vs. just an `AccessExclusiveLock` on the relation). The deprecation commit's actual *fix* for the false-negative window is not a code fix to `check_btree()`; it's a test-suite change that adds a second `CHECKPOINT` before asserting. That means the underlying "autonomous page writes leave transient per-page state" condition is very plausibly still present in `verify_orioledb()` too, not just in the deprecated `orioledb_tbl_check()`. This has not been independently confirmed by reading `check_btree()` itself in this pass — flagged below.

**Direct relevance to the existing Antithesis harness:** both `test/antithesis/sk-recovery-race/driver.py` and `sk-recovery-race-chaos/driver.py` call `orioledb_tbl_check()` (not `verify_orioledb()`) as part of their `always()` assertion, and — critically — the chaos driver's "post-burst" check (`assert_consistent(ctl_conn, "post-burst")`, `driver.py:198`) runs **immediately after the DML burst ends, with no intervening explicit `CHECKPOINT`**. Automatic checkpoints fire throughout the burst (`checkpoint_timeout=30s`, confirmed by the harness's own `sometimes()` assertion at `driver.py:182-188`). This is exactly the "checkpoint just happened, no follow-up CHECKPOINT yet" shape that `checkpoint_split_base_test.py` documents as producing an unreliable result.

## The property

**Type:** Safety / meta — a property about the *oracle*, not about OrioleDB's data itself.

**Property:** `orioledb_tbl_check()` (and, pending the open question below, possibly `verify_orioledb()`) returns a result that reflects genuine structural consistency, not a transient artifact of "a checkpoint recently observed autonomous page writes (e.g., in-flight splits) and hasn't had a follow-up explicit `CHECKPOINT` to clear that per-page state." Put differently: the existing harness's `always()` assertions (orioledb#855's PK/SK check) should never fail *because of the checker's own known transient-state limitation* — a failure should mean real corruption, not oracle noise.

**Invariant:** This is best expressed as two linked assertions, since the goal is to separate "the checker is unreliable right now" from "the data is actually inconsistent":
- `Always(tbl_check_result_stable)`: call `orioledb_tbl_check()` (or `verify_orioledb()`) twice in immediate succession with no intervening writes/checkpoints, and assert the two results agree. Disagreement between back-to-back calls on a quiescent table is direct proof of oracle nondeterminism, independent of whether real corruption exists.
- `AlwaysOrUnreachable(tbl_check_false_only_before_followup_checkpoint)`: if the first call disagrees with a call taken after an explicit follow-up `CHECKPOINT`, and the *only* structural difference between the two calls is "a checkpoint intervened," treat the pre-follow-up-checkpoint `false`/inconsistent result as expected-and-benign (per the test suite's own documented behavior) rather than corruption — but this requires the harness to *know* it's inside that window, which it currently has no way to detect (see Open Questions).

**Antithesis Angle:** This doesn't need Antithesis's fault injection to reach the interesting state — it's reachable by ordinary DML + automatic checkpoints, which the existing `sk-recovery-race-chaos` workload already does continuously. What Antithesis's fault injection *adds* is exactly the kind of timing pressure (delayed backends, scheduling jitter) that increases the chance of the checkpoint landing mid-split and the post-burst check firing before a follow-up checkpoint clears the transient state — i.e., Antithesis is more likely than a clean CI run to trip this specific false-negative path, at the exact moment triage would otherwise assume it found a real #855 regression.

**Why It Matters:** The entire existing Antithesis harness's `always()` correctness signal (both `sk-recovery-race` and `sk-recovery-race-chaos`) rests on `orioledb_tbl_check()` returning a trustworthy `true`/`false`. If a run reports an `always()` violation and the root cause is actually "the checker's known transient post-checkpoint state, not real PK/SK divergence," that's a false alarm that consumes triage time and — worse — could train a team to distrust or dismiss future *real* violations of the same assertion ("oh, that's just the checker being flaky again"). This is precisely the failure mode `validating-claims.md`'s framing warns about applied reflexively to the test's own oracle: an unreliable oracle undermines every property built on top of it, silently.

**Open Questions:**

- Does `verify_orioledb()` (the non-deprecated replacement, already present on `main` since PG-version `1.7--1.8`) share the exact same transient-state false-negative window as `orioledb_tbl_check()`, or did the underlying `check_btree()` behavior actually change between them? The unmerged deprecation commit's fix was to the *test structure* (add a follow-up CHECKPOINT), not proven to be a fix to `check_btree()` itself. This determines whether migrating the harness to `verify_orioledb()` (which is a good idea regardless, since `orioledb_tbl_check()` is deprecated upstream and may eventually error on `main` once that branch merges) would actually close this gap or just rename it. `(needs reading check_btree() and the exact commits that touch its "autonomous page write" handling, e.g. d2a2723e / b97d2545 / their reverts)`
- Has the harness's `sk-recovery-race-chaos` driver ever actually hit this false-negative window in a real run (i.e., is this a live risk or a theoretical one bounded by how rarely automatic checkpoints land exactly mid-split at the moment `assert_consistent("post-burst")` runs)? Not measured in this pass — would need either historical run logs or a deliberate repro (loop the driver's post-burst check many times against a workload engineered to maximize split-during-checkpoint frequency).
- Is `orioledb_tbl_check()` itself still callable in the image the Antithesis harness builds (per `test/antithesis/orioledb/Dockerfile`, built from `ORIOLEDB_REF: main`)? Confirmed yes as of commit `a975c702` (deprecation commit `0bb61a3c` is unmerged), but this is a one-way trip: if/when that branch merges, `sk-recovery-race[-chaos]/driver.py`'s `orioledb_tbl_check()` calls would start raising `ERRCODE_FEATURE_NOT_SUPPORTED` instead of returning a boolean, silently breaking both existing Antithesis assertions (the `always()` call would presumably start raising a Python exception instead of failing cleanly). Worth flagging to whoever maintains the harness independent of this property's core finding.

## SUT-side instrumentation cross-reference (existing-assertions.md)

The two existing `always()` calls (`sk-recovery-race/driver.py:89-95`, `sk-recovery-race-chaos/driver.py:87-93`) are exactly the call sites at risk. No SUT-side instrumentation exists inside `check_btree()`/`orioledb_tbl_check()`/`verify_orioledb()` itself to distinguish "structurally broken" from "transiently mid-checkpoint-write, expected to clear." A `reachable()` or counter inside `check_btree()` recording whether a check observed the specific "autonomous page write, no follow-up checkpoint yet" per-page state (whatever flag/condition `d2a2723e`'s reverted fix checked) would let the harness's own assertions self-filter this known-benign condition instead of reporting it as a #855-shaped failure.
