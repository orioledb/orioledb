# replica-globalxmin-catchup-lag

## Summary

A companion, apparently-still-open issue to `replica-xmin-monotonicity.md`:
per commit `ef8e93b9`'s own message, a restored test documents that a
replica's `globalXmin` may fail to catch up to the primary's after recovery
completes ("stuck low globalXmin"), the mirror-image problem to the
regression this same commit fixes. This is a liveness-leaning property, but
it matters for data integrity because a permanently-lagging `globalXmin` on a
replica pins undo retention and MVCC visibility horizons indefinitely, with
downstream effects on undo space growth and potentially stale-but-legitimate
visibility decisions.

## What led here

Read directly from `ef8e93b9`'s commit message (`git show ef8e93b9`, quoted
in full in `replica-xmin-monotonicity.md`):

> `test_recovery_finish_rollback_does_not_regress_replica_xmin` — restored
> from a previous iteration. Currently fails on a separate "stuck low
> globalXmin" symptom (replica's globalXmin does not catch up to master's
> after recovery completes); kept in place so the diagnostic surface remains
> visible while maintainer iterates on that orthogonal issue.

I located the test at `test/t/replication_test.py:1762` and read its full
docstring and body. The docstring explicitly frames the check as loose by
design: "A bounded lag between primary and replica xmins is legitimate
bookkeeping bloat... not the bug. The bounds catch a catastrophic pin (lag
growing without bound...) while tolerating ordinary post-recovery bloat." So
the test's author already anticipated this exact ambiguity and tried to write
a bound loose enough to avoid false positives — but the commit message that
introduced/restored it says it currently fails anyway, i.e. even the loose
bound is being violated by the "stuck low globalXmin" symptom.

I did not find an `expectedFailure` or `skip` marker anywhere near this test
in `test/t/replication_test.py` (checked via grep for both markers file-wide).
That means, at face value, this is a checked-in test that its own introducing
commit says currently fails — an inconsistency worth flagging rather than
silently trusting either the commit message or the test's presence.

## What goes wrong

If `globalXmin` never catches up to the primary's post-recovery value on a
replica, undo space the primary has already reclaimed cannot be reclaimed on
the replica, growing unboundedly under sustained write load (an
availability/resource-exhaustion failure, not a correctness one on its own —
but see the "Open Questions" note below on whether a stuck-low horizon could
also mask a real visibility bug, since it's the same code path that the
regression-guard property depends on for correctness).

## Antithesis angle

`Sometimes()`-shaped: confirm that after a primary crash/restart with an
attached streaming standby under load, the replica's `globalXmin` *does*
converge (within the same "bounded lag" semantics the existing test tries to
define) to the primary's within a reasonable window, rather than staying
pinned low indefinitely. This is a genuine liveness property (progress),
distinct from the safety property in `replica-xmin-monotonicity.md`
(never moves backward). Best implemented as a bounded-wait check similar to
the existing test's own polling loop, run repeatedly under Antithesis fault
injection rather than the single deterministic scenario the existing test
constructs.

## Existing assertion cross-reference

Not covered by any existing Antithesis assertion (no replica topology in the
current harness at all, per `sut-analysis.md` §9's largest-gap finding).

## Open Questions

- Is this test actually failing today, or was it fixed by a later commit not
  yet identified? `(needs human input / needs test run)` — I did not execute
  the test suite (out of scope per task instructions to not build/run tests
  without explicit request), and did not find a definitive later fix commit
  via `git log` message search for "stuck low globalXmin" or similar phrasing
  (searched, no hits).
- Does a stuck-low `globalXmin` ever cause an actual *visibility* bug (e.g. a
  transaction that should be invisible staying visible, or vice versa), or is
  its only effect undo-retention bloat? The doc frames it as "orthogonal" to
  the correctness regression, but I have not independently verified there is
  no correctness edge — this determines whether this property belongs in a
  liveness or safety category. `(partial: framed as liveness by the source
  commit message; not independently re-derived)`.

### Investigation Log

#### Is this test actually failing today, or was it fixed by a later commit not yet identified?

- Examined: `git show ef8e93b9` commit message; `test/t/replication_test.py:1762` (`test_recovery_finish_rollback_does_not_regress_replica_xmin`) docstring and body; file-wide grep for `expectedFailure`/`skip` markers; `git log` message search for "stuck low globalXmin" phrasing.
- Found: the introducing commit message states the test currently fails on the "stuck low globalXmin" symptom; no `expectedFailure`/`skip` marker found near the test; no later fix commit found by message search.
- Not found: the test suite was not executed (out of scope per task instructions), so current pass/fail status is not directly confirmed.
- Conclusion: tagged `(needs human input / needs test run)`.

#### Does a stuck-low `globalXmin` ever cause an actual visibility bug, or is its only effect undo-retention bloat?

- Examined: `ef8e93b9` commit message and the restored test's own docstring framing.
- Found: both the commit message and test docstring frame the symptom as "orthogonal" bookkeeping bloat, not a correctness regression.
- Not found: no independent re-derivation of whether a stuck-low horizon could ever cause a real MVCC visibility bug — the framing is taken from the source material as-is.
- Conclusion: tagged `(partial: framed as liveness by the source commit message; not independently re-derived)`.
