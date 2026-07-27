# pk-update-chain-race-consistency

## Summary

`doc/architecture/row-level-concurrency.mdx` explicitly documents two
row-level concurrency divergences from stock Postgres as accepted behavior,
not bugs: (1) a concurrent primary-key update causes a concurrent
updater/deleter to error out with "tuple to be locked has its primary key
changed due to concurrent update"; (2) a delete-then-reinsert of the same PK
value can cause a concurrent updater to silently update the *newly inserted*
row instead of erroring or updating the original. `sut-analysis.md` §4 lists
this as a "claimed guarantee... explicitly documented as known/accepted."
Per `references/validating-claims.md`, a claimed guarantee should be turned
into a property that verifies the system actually behaves as claimed — the
discipline being not to assert the guarantee holds, but that the system
claims it and the property checks it.

## What led here

Read `doc/architecture/row-level-concurrency.mdx` in full (86 lines). Both
scenarios are given as concrete, reproducible two-session SQL examples in the
doc itself (session 1 does `UPDATE ... SET id = 2 WHERE id = 1` or
`DELETE ...; INSERT ...` with the same PK; session 2 concurrently does
`UPDATE ... SET value = value + 1 WHERE id = 1`), which is unusually
directly-executable as an Antithesis property compared to most doc claims —
the doc essentially already specifies the test.

## What matters here for Data Integrity specifically

This focus's brief calls out "write ordering assumptions" and "constraint
enforcement" explicitly. The second scenario (delete+reinsert) is the more
interesting one from a data-integrity angle: it's not merely "session 2
gets an error" (safe, if unfriendly) — it's "session 2's UPDATE silently
succeeds against a *different* row than the one it read," which is exactly
the shape of a lost-update/wrong-row anomaly if it goes even slightly further
than documented. The property is not "does this race exist" (the docs
already say yes) but "does the race stay exactly as narrowly scoped as
documented, or can a slightly different interleaving (e.g. the reinsert
racing against a *third* concurrent session, or the reinsert's own undo
record overlapping the original delete's undo chain) produce a result the
docs don't describe" — e.g. corrupting the row's contents rather than simply
"following the chain to a legitimate, fully-formed new row."

## Antithesis angle

This is directly implementable without needing new topology: a workload with
N concurrent sessions doing PK-preserving delete+reinsert cycles on a shared
key set, plus concurrent updaters on the same keys, checking that every
successful UPDATE (from the updater's perspective) landed on a row whose
current content is internally consistent (i.e. a real row that was actually
committed by some real transaction — not a torn/partial write straddling the
delete and the reinsert). This is a good `AlwaysOrUnreachable`-shaped
property: *if* the race is hit (updater lands on the reinserted row), the
row content must be exactly what the reinsert wrote, never a mix. Antithesis
fault injection (scheduling delays widening the window between the delete
and the reinsert, or between the updater's read and its lock-wait) increases
the chance of hitting rarer sub-interleavings than the docs' own two-line
example constructs.

## Existing assertion cross-reference

Not covered by any existing assertion — the existing harness's DML shapes
(`sk-recovery-race`'s 3-way race, the jepsen workload's generic
read/write/CAS operations) don't specifically construct a delete+reinsert
chain on a shared PK. This would be a new, small, targeted workload.

## Open Questions

- Is "session 2 updates the newly inserted row" actually the *only* possible
  outcome of this race, or are there timing windows (e.g. reinsert's WAL/undo
  overlapping recovery replay, or a third concurrent session) where the
  result is something the docs don't cover — e.g. an error that should have
  fired but didn't, or a row content that's neither the original nor the
  reinserted value? Not investigated beyond reading the doc's two-session
  example; this needs actual multi-session interleaving exploration, which
  is exactly what Antithesis fault injection is suited to attempt.
- Does this interact with the PK/SK checkpoint-fixup mechanism (#855) at all
  if the reinsert happens to land in the PK-applied/SK-pending window covered
  by `sk-fixup-undo-recycling-drop.md`? Not traced — plausible but unconfirmed
  interaction between two separately-documented races on the same
  index-organized-table architecture.
