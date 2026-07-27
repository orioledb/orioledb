# checkpoint-abort-snapshot-standby-panic

## Merge note

This evidence file merges three independently-written files that converged on
the same finding: `checkpoint-abort-snapshot-resurrection.md`,
`standby-panic-abort-snapshot-race.md`, and this file
(`checkpoint-abort-snapshot-standby-panic.md`, kept as the canonical slug).
All three focus passes (Failure Recovery, Distributed Coordination) trace the
same bug family from slightly different depths; content below is the union,
using `standby-panic-abort-snapshot-race.md`'s deeper mechanism trace as the
backbone.

## Focus

Failure Recovery / Distributed coordination (primary-replica consistency at
a checkpoint boundary). Targets `sut-analysis.md` §2's second distinct
checkpoint-boundary bug: the checkpoint abort-snapshot race that PANICs a
standby. Per the task's explicit framing, this is fixed on `main` but — per
the fix's own root-cause doc — only one of two failure legs was closed.

## What led to this

Root-cause doc (read via `git show`, present on unmerged branch
`origin/add_stress_bank_account_test:test/t/crash/ISSUE_deferred_rollback_replica_two_crash_modes.md`,
not in this worktree) describes: `finish_write_xids()` snapshots in-flight
oxids purely from `oProcData[i].vxids[j].oxid` validity, with no awareness of
whether that oxid's rollback WAL has already been durably flushed below the
checkpoint's replay-start boundary. Crash recovery can then re-discover the
oxid as in-flight and emit a spurious deferred `WAL_REC_ROLLBACK` — which, if
a standby has already drained the resurrected oxid's undo-stack entry to 0,
PANICs the standby in `walk_undo_stack`.

**Two load-bearing facts independently confirmed against the actual worktree**
(not just the unmerged-branch doc):

1. **The regression test exists in this tree at the analyzed commit.**
   `test/t/replication_test.py::test_checkpoint_snapshot_resurrects_aborted_oxid`
   is present (added by commit `8a00a986`, confirmed ancestor of `a975c702`
   via `git merge-base --is-ancestor`).
2. **A narrow consumer-side fix (`93db964d`) is present at the analyzed
   commit** — "Init the undo_stacks with undo_stack_locations_set_invalid by
   default," a 4-line change in `src/recovery/recovery.c`. Consistent with
   the characterization that only one of two failure legs was closed.

Two purpose-built stopevents exist only for this repro: `before_abort_vxids_clear`
(in `current_oxid_abort()`, `src/transam/oxid.c:1501-1524`) and `replay_on_record`
(in the standby's recovery leader dispatch loop, `src/recovery/recovery.c:3988-4003`).

## Mechanism (validated by direct code reading, not just the test docstring)

1. **Primary side (root cause, still present today):** `finish_write_xids()` (`src/checkpoint/checkpoint.c:902-948`) snapshots each backend's in-flight oxid purely via `OXidIsValid(oProcData[i].vxids[j].oxid)` (`checkpoint.c:927`). Read directly — there is no check anywhere in this loop for whether that oxid's resolving `WAL_REC_ROLLBACK` has already been durably flushed below the checkpoint's `replayStartPtr`. A backend that has already flushed its abort WAL but has not yet cleared its `vxids[]` slot (the exact window `current_oxid_abort()` parks in at the `before_abort_vxids_clear` stopevent) gets recorded as "in-flight" in the checkpoint's durable xids dump.
2. **Recovery re-discovery:** After a crash, recovery replay skips the sub-`replayStartPtr` rollback (already accounted for) but sees the checkpoint's xids dump claiming the oxid was in-flight, so it re-aborts the "phantom" and emits a **spurious deferred `WAL_REC_ROLLBACK`** that streams to any standby.
3. **Standby side (the bug that actually PANICs):** The standby's recovery leader dispatches modifies to workers by row-key hash and never materializes undo itself; for a resurrected oxid the leader's `undo_stacks` entry starts at 0 (a `memset` zero-fill). If the real rollback replays first and RETAINs the entry on `finished_list` (deferred finalization keeps the entry present with location 0), and the *drain* (`update_proc_retain_undo_location`, gated by `get_workers_commit_ptr()`) has not yet removed it by the time the spurious deferred rollback re-finds it, `set_cur_undo_locations` propagates the raw `0` and `walk_undo_stack` (`src/transam/undo.c:1395`) calls `undo_item_buf_read_item()` on a bogus location zero → **PANIC**, taking the whole standby postmaster down (confirmed by the test using `restart_after_crash = off` specifically so the PANIC is observable as a clean down, not a recovery-loop that re-masks it).

## What the landed fix actually closes (and what it does not)

Commit `93db964d` changes `recovery_switch_to_oxid()` (`src/recovery/recovery.c:1943-1949`, standby leader code) to also call `undo_stack_locations_set_invalid()` after the zero-memset, so a resurrected-but-never-materialized undo stack now reads as *invalid* rather than as *location 0* — this is presumably what stops `walk_undo_stack` from misinterpreting the sentinel as a real, dereferenceable location.

This is a **consumer-side (standby-leader) fix**, not a fix to the primary's checkpoint snapshot. `finish_write_xids()` at `checkpoint.c:902-948` is unchanged and still gates purely on `OXidIsValid()` — re-read the current function body directly and confirmed no replay-boundary check was added. The root inconsistency (checkpoint's in-flight snapshot is not synchronized against `replayStartPtr` for already-flushed aborts) still exists; only this one specific downstream misinterpretation (`location=0` treated as valid) was closed. This "only one of two failure legs was closed" framing is consistent with what direct code reading found; the "second failure leg" characterization itself is attributed to the unmerged-branch doc's own account (not independently re-derived from a design discussion beyond confirming the primary-side gate is unchanged).

## Why a variant could still slip through

The fix closes the specific bug where the leader's placeholder happens to be bit-pattern `0`. Any other consumer code path that treats a resurrected-oxid's default/placeholder undo state as valid (rather than explicitly checking an "invalid" sentinel) would reproduce an analogous crash through a different downstream symptom. Since the primary-side race (checkpoint snapshotting an oxid whose abort is already durable) is unchanged, *any* future consumer of the checkpoint's xids dump inherits the same "resurrected phantom oxid" input; only today's one known consumer bug was patched.

## What goes wrong if the property is violated

A standby that should be simply replaying WAL PANICs and goes down — an availability outage for a read replica (or a delayed promotion target) triggered purely by primary-side checkpoint/commit timing, not by any external fault.

## The property

| | |
|---|---|
| **Type** | Safety |
| **Property** | A checkpoint's snapshot of in-flight oxids (`finish_write_xids()`) never causes crash recovery to resurrect an oxid whose rollback has already been durably applied below the checkpoint's own replay-start boundary — no such resurrection ever reaches a point where a streaming standby's undo-stack walk sees a zeroed/invalid location and PANICs. |
| **Invariant** | `Always`: a standby involved in a checkpoint/crash-recovery cycle must never PANIC. Checkable as "the standby process does not crash with a PANIC in the log" combined with a deliberately adversarial checkpoint-timing workload; complemented by `Sometimes(recovery_leader_undo_drain_lagged_behind_deferred_rollback)` to confirm the interesting timing window (leader far ahead of workers, per the test's own mechanism) is actually reached under fault injection, not just under the test's hand-crafted stopevent pinning — since the consumer-side fix (`93db964d`) specifically targeted the finished-list-drain-lag scenario, an `Always`-only check that never observes this window would be a vacuous pass. |
| **Antithesis Angle** | Requires the same primary+streaming-standby topology gap noted throughout this focus (`sut-analysis.md` §9's "no replication topology" gap). The test's own construction is the recipe: a transaction that aborts with material changes under `synchronous_commit=on`/`fsync=on` so its rollback is durably flushed, timed so `finish_write_xids()` still sees the (about-to-clear) vxids slot, while the standby's recovery leader is lagging behind its own workers' commit-pointer drain (naturally induced by scheduling delay / CPU throttling fault injection on the standby's worker processes, rather than the test's explicit `replay_on_record` stopevent pin) — Antithesis's process-level scheduling faults are a more organic way to reach the same window the deterministic test manufactures. |
| **Why It Matters** | A standby PANIC is a hard availability failure (the whole standby process crashes and must recover from scratch), triggered purely by checkpoint/abort timing on the *primary* with no data-integrity mistake by the user — and the root-cause doc's own account states the producer-side condition that causes it is still present in the code. Per `references/property-catalog.md`'s "Cross-Reference Closed Issues" guidance, a fix that closes only one of two identified failure legs is exactly the kind of thing worth a dedicated regression/edge-case property. |

**Open Questions:**

- Is there a fault sequence — not relying on the `replay_on_record` stopevent the deterministic test uses — under which the *consumer-side* fix (`93db964d`) itself fails to prevent the PANIC, i.e., a variant where the finished-list drain lags for a different reason than the one the test constructs? `(needs further investigation)`
- Does any other current consumer of the checkpoint's xids dump (besides the standby recovery leader's `undo_stacks` initialization) read a resurrected-phantom-oxid's undo location without checking a validity sentinel? Not traced beyond `recovery_switch_to_oxid()`/`walk_undo_stack`.
- Is the "second failure leg" (primary-side snapshot/`replayStartPtr` synchronization) actually tracked anywhere (issue number, TODO comment) or only described in the unmerged-branch doc? Not found — the doc itself was not re-read this pass beyond the original citation; only the current `checkpoint.c` code and the landed fix commit were inspected directly. `(partial: confirmed the one found fix is narrow/consumer-side via direct code reading; a broader search for an independently-titled primary-side fix commit was not performed)`
- Does `test_checkpoint_snapshot_resurrects_aborted_oxid` currently pass at the analyzed commit (i.e., does the narrow fix already suffice for the test's specific scenario, even if the general primary-side race remains)? Not run — out of scope for a static/code-reading discovery pass.

### Investigation Log

#### Is the checkpoint's primary-side snapshot inconsistency (finish_write_xids gating only on OXidIsValid) actually still present in current `checkpoint.c`?

- Examined: `src/checkpoint/checkpoint.c:902-948` (`finish_write_xids`) at commit `a975c702` (current HEAD).
- Found: the loop still gates purely on `OXidIsValid(xidRec.oxid)`; no reference to `replayStartPtr`, no synchronization against a backend's abort-WAL-flush state anywhere in the function.
- Conclusion: the primary-side gap is confirmed still open by direct reading of the current function body, not merely inherited from the doc's claim.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** No existing assertion anywhere touches `finish_write_xids`, checkpoint-time oxid snapshotting, or standby undo-stack PANICs — `existing-assertions.md` confirms the only assertions target the PK/SK race (a related but structurally distinct checkpoint-boundary bug family).

- A new stopevent or `reachable()`/`always()` at `finish_write_xids()` (`checkpoint.c:927`) that records, for each snapshotted in-flight oxid, whether its most recent WAL activity is already below the checkpoint's in-progress `replayStartPtr` would let Antithesis flag the *root* race directly, rather than only observing its consequence (a standby PANIC) after the fact.
- An `unreachable()` wrapping the standby's `elog(PANIC, ...)` inside `undo_item_buf_read_item()` (reached via `walk_undo_stack`, `src/transam/undo.c:1395`) would give a precise, attributable signal distinct from a generic container crash, and would also catch any *other* code path that reaches this PANIC for unrelated reasons.
