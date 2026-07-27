# replica-undo-reclaimed-too-early

## Summary

A replica-side `Assert(UNDO_REC_EXISTS(...))` crash observed during the same
chaos-hunt campaign that found the `globalXmin` livelock (see
`replica-xmin-monotonicity.md`), hypothesized by the investigating
doc to be the "undo sibling" of that bug: a B-tree page dereferences an undo
record location that has already been reclaimed. This is a genuine data
corruption vector if the assert is compiled out (release build): reading a
freed/reused undo slot as if it were still the original record.

## What led here

Read in full from `ISSUE_recovery_committed_oxid_reverts_to_inprogress.md`
§10 ("Newly observed *second* replica failure mode"), captured under the
**same no-injection chaos** (streaming standby + `SIGKILL` of the primary
postmaster every 6s) that produced the CSN livelock documented in the rest of
that file:

```
TRAP: failed Assert("UNDO_REC_EXISTS(undoType, undo_loc)"), File: "src/btree/page_contents.c", Line: 64, PID: 4116821
server process (PID 4116821) was terminated by signal 6: Aborted
the database system is in recovery mode
orioledb recovery after fatal error started.  Unable to make multiprocess recovery.
```

I independently confirmed the assert site still exists in the current
codebase: `src/btree/page_contents.c:66` and `:81` both contain
`Assert(UNDO_REC_EXISTS(undoType, undo_loc));` inside what is evidently a
page-image-from-undo reconstruction loop (read the surrounding ~30 lines).
The doc's own framing is explicit about confidence level: "**Why it likely
matters (hypothesis, not yet proven).**" — the shared-root-cause claim (same
recovery xmin/retain-bookkeeping fault as the CSN livelock) is stated as a
working hypothesis, not a confirmed mechanism. The doc also explicitly notes
the harness's "divergence" bucket conflated this with two unrelated failure
modes (the known extent leak and the actual livelock) until a later commit
in the doc's own history split them apart (§10's closing note, and
`8703c229`'s commit message "Correct note: tbl_check=false divergence is the
KNOWN extent leak, not this issue" — which I also read via `git show`).

## What goes wrong

If `undo` is reclaimed (trimmed by retention bookkeeping) while a B-tree page
still holds a reference into it, and the assert is not compiled in
(production/release builds typically disable `Assert`), the read would
proceed against memory/disk content that has been reused for something else
— a genuine silent-corruption path, not just a crash. In a debug/assert build
(which is what caught this), it instead crashes the replica backend and
forces single-process crash recovery, which is a strong, attributable signal
— exactly the outcome Antithesis testing wants, provided the image under
test has asserts enabled (see `sut-analysis.md`'s open question on whether
Antithesis images are assert-enabled, repeated here since it's directly
relevant).

## Antithesis angle

Same topology and fault shape as `replica-xmin-monotonicity.md`
(streaming standby + primary `SIGKILL` under concurrent DML) — the doc
observed this as an alternative outcome of the *same* hunt campaign, not a
separately-triggered scenario. An `Unreachable()` assertion on this
particular TRAP firing (or, more directly, the underlying condition it
guards) is the natural fit: this should never happen, and if it does, it's a
serious, standalone finding distinct from the livelock.

## Existing assertion cross-reference

Not covered by any existing Antithesis assertion. No client-visible symptom
exists for this failure mode other than the replica connection dying — the
existing `sk-recovery-race[-chaos]` drivers only check PK/SK counts on a
single (non-replicated) instance and would not observe this at all. This is a
strong candidate for a **SUT-side** assertion (an `Always()`/`Unreachable()`
wrapping the same condition the `Assert` checks, so it fires and is captured
by Antithesis's search even in a build where the C `Assert` might be
disabled) — per `property-catalog.md`'s guidance on states that are "dangerous,
timing-sensitive, hard to observe externally."

## Open Questions

- Is the shared-root-cause hypothesis (same recovery-xmin/retain-bookkeeping
  fault as the CSN livelock) correct, or is this an independent bug in undo
  retention scheduling? The source doc itself states this is unproven
  (`(needs human input / needs dedicated investigation)` — the doc's own
  words: "hypothesis, not yet proven"). If the `globalXmin` fix chain
  (`ef8e93b9`/`a0d628c1`, confirmed present at `a975c702`) shares the root
  cause, it may have incidentally fixed this too — I did not find a commit
  message specifically claiming to fix an `UNDO_REC_EXISTS` replica crash, so
  I am not treating it as fixed.
- Does this reproduce at the analyzed commit (`a975c702`) at all, given the
  `globalXmin` fix chain post-dates the doc's observation? Not verified —
  would require a dedicated hunt against the current code, which is out of
  scope for this static-analysis pass.

## Scope note (added by evaluation pass, R15)

This property, like the other undo-retention properties in this catalog
(`sk-fixup-undo-recycling-drop`, `replica-undo-reclaimed-too-early`,
`undo-wraparound-retry-cap`, `multi-insert-undo-capacity-invariant`),
implicitly exercises only the `enable_rewind=false` branch of shared
undo-retention logic — `orioledb.enable_rewind` is never set to `true`
anywhere in `test/antithesis/`. Flagged here explicitly since it was never
stated before; assessed as low-risk because `enable_rewind` is
`PGC_POSTMASTER` (fixed at server start, not a runtime-mutable/session-level
GUC that could flip mid-test), and rewind is out of this catalog's scope
regardless per the top-of-file scope restriction.

### Investigation Log

#### Is the shared-root-cause hypothesis (same recovery-xmin/retain-bookkeeping fault as the CSN livelock) correct, or is this an independent bug in undo retention scheduling?

- Examined: `ISSUE_recovery_committed_oxid_reverts_to_inprogress.md` §10 (read in full); `src/btree/page_contents.c:66,81` (confirmed `Assert(UNDO_REC_EXISTS(...))` still present in current code); `8703c229` commit message via `git show`.
- Found: the source doc itself labels the shared-root-cause claim "hypothesis, not yet proven"; no commit message found claiming to fix an `UNDO_REC_EXISTS` replica crash specifically, including in the `globalXmin` fix chain (`ef8e93b9`/`a0d628c1`).
- Not found: no independent confirmation either way of whether the `globalXmin` fix incidentally resolved this bug too.
- Conclusion: tagged `(needs human input / needs dedicated investigation)` — the doc's own hypothesis is unproven and no fix commit was found to confirm or refute it.
