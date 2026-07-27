# malformed-wal-container-double-finish

## Summary

orioledb#865: an `ereport(ERROR)` that fires between a commit's
`add_finish_wal_record(WAL_REC_COMMIT, ...)` and the following
`flush_local_wal()` causes the subsequent abort path to append a
`WAL_REC_ROLLBACK` into the *same* still-unflushed local WAL buffer, producing
a single on-disk WAL container with two finish records (`COMMIT` and
`ROLLBACK`) for the same oxid. A streaming replica applying that container
hits `Assert("rec->oxid != InvalidOXid")` at `src/recovery/recovery.c:3712`
and dies. A fix is present at the analyzed commit, but the fix's own
originating investigation documents a residual, rare, different-shaped
failure mode under one specific injection site that may or may not still
apply.

## What led here

Read `wal_container_double_finish_issue.md` in full (146 lines, via `git show
origin/add_stress_bank_account_test:test/t/crash/wal_container_double_finish_issue.md`).
Concrete evidence quoted directly from the doc: a real replica log fragment,

```
WAL redo at 0/3018210 for OrioleDB resource manager/OrioleDB WAL container:
  XID      (562 192 0);
  COMMIT   (562 192 0 - xmin 552 csn 420);
  ROLLBACK (562 192 0 - xmin 552 csn 420);
```

with a 100%-reproducing narrowed injection (`orioledb-after-finish-wal-rec`,
2/2 trials) and an attributable log chain (`walk_undo_stack exit` for the
COMMIT half, then the TRAP on the ROLLBACK half of the *same* malformed
record). The doc explicitly separates this ("Bug #1", same-container,
replica TRAP) from a related-but-distinct "Bug #2" (COMMIT and ROLLBACK in
*separate*, individually-valid containers, causing silent primary/replica
*divergence* rather than a crash) — the watershed being whether the ERROR
fires before or after `flush_local_wal` has already submitted the COMMIT
container.

I confirmed the fix is present at the analyzed commit:

- `7d04814b` ("Wrap `log_logical_wal_container` and wal buffer reset into
  critical section", `git show` read in full) is `Fixes #865` per its own
  commit message and is an ancestor of `a975c702`. I read the current
  `src/recovery/wal.c` (`flush_local_wal()`, lines ~689-724) and confirmed the
  `START_CRIT_SECTION()`/`END_CRIT_SECTION()` wrapping around
  `log_logical_wal_container()` is present in the code as checked out, with a
  comment: "it's too late to append another ROLLBACK in case of error...
  escalate any failure to PANIC."
- A second, broader companion investigation (`streaming_replica_issue.md`,
  575 lines, read the first ~250 lines in full) documents **four** distinct
  failure modes (#1 this bug; #2 silent divergence; #3 replica undo-panic;
  #4 primary-side PK/SK desync) from `ereport(ERROR)` firing at different
  points across the same commit window, and records a broader fix
  (`START_CRIT_SECTION` spanning `wal_commit` through
  `current_oxid_commit`, using a cross-function flag
  `commit_wal_record_added`). I confirmed the **currently-shipped** version
  of this broader fix is `4f4c365a` ("Wrap wal_commit->current_oxid_commit
  code into CRIT_SECTION") — an ancestor of `a975c702` — which is a
  *different* implementation than the flag-balanced approach the doc
  describes as "Fix committed" under commit `200073b5` (that specific hash is
  **not** an ancestor of `a975c702`; instead an earlier attempt at the same
  idea, `b0488bd5`, was reverted (`20812559`, "Put START_CRIT_SECTION()/
  END_CRIT_SECTION() into different functions considered as error prone
  pattern") before `4f4c365a` landed a restructured version that wraps the
  crit section inside `undo_xact_callback` in `src/transam/undo.c` directly,
  rather than spanning `wal.c`/`oxid.c` via a flag.

## What goes wrong

Pre-fix: any `ereport(ERROR)` in the narrow post-COMMIT-flush window — which
the doc argues is production-reachable via `Assert` failure, `palloc` OOM,
internal `ereport(PANIC)`, or an uncatchable OOM-killer `SIGKILL` (all of
which the doc cross-references against Postgres's `HOLD_INTERRUPTS()` window
in `CommitTransaction`) — produces a malformed WAL container that kills any
attached streaming replica outright, and per the doc's own §"Implications for
the primary's crash recovery," could in principle also make the *primary's
own* future crash recovery fail at the same assert if the primary crashes
again before its next checkpoint advances past the malformed record (this
specific "primary cannot start" outcome is explicitly flagged by the doc as
not yet empirically observed, only inferred).

## Antithesis angle

Needs the same streaming-standby topology gap noted throughout this focus's
other findings. The most direct test replays the doc's own reproducer shape:
inject/force an error (via a stopevent, an assertion, or relying on
Antithesis's own fault injection) inside the commit-flow window on a primary
with an attached standby, and assert the standby never TRAPs on
`rec->oxid != InvalidOXid` and never silently diverges from the primary's
final row/aggregate state.

## Existing assertion cross-reference

Not covered by any existing Antithesis assertion — no replica topology
exists in the current harness. This is a strong Unreachable/Always candidate:
`Unreachable("WAL container with two finish records for the same oxid")` on
the replica/recovery decode side would directly instrument the exact
`rec->oxid != InvalidOXid` condition rather than waiting for the assert to
crash the process, giving Antithesis a signal even in non-assert builds.

## Open Questions

- Does `4f4c365a`'s restructured crit section (wrapping the whole
  `undo_xact_callback` commit branch, rather than the flag-balanced
  cross-function approach the doc's `200073b5` used) actually close all four
  bug classes (#1-#4) the broader doc enumerates, or only #1 (the specific
  one `7d04814b`'s narrower, earlier fix targeted)? Not verified — I read
  `4f4c365a`'s diff far enough to see it wraps the commit branch in
  `undo_xact_callback` starting from `START_CRIT_SECTION()` right after
  entering the `XACT_EVENT_COMMIT` case (per the diff excerpt captured), but
  did not trace where its matching `END_CRIT_SECTION()` falls relative to
  the doc's documented "closing edge" (`curOxid = InvalidOXid`), nor
  reconcile it against the doc's warning that `palloc` inside a critical
  section trips its own assert — an audit gap the doc explicitly calls out as
  essential when widening this section. `(partial: fix commit for the exact
  #865 shape confirmed present and its intent matches; broader four-bug-class
  closure not independently re-verified against the current diff)`.
- The doc's own residual finding — `orioledb-csn-incremented` (Bug #2 class)
  changing from "silent divergence" to "rare (~5%) replica recovery-livelock"
  post-fix, rather than being eliminated — was investigated under the
  `200073b5` implementation, which is *not* what shipped at `a975c702`
  (`4f4c365a` is a different, later implementation). Whether this specific
  residual applies to the currently-shipped fix is unknown. `(needs
  re-investigation against 4f4c365a specifically, or a dedicated hunt)`.

### Investigation Log

#### Does `4f4c365a`'s restructured crit section actually close all four bug classes (#1-#4), or only #1?

- Examined: `7d04814b` commit (`git show`, read in full, `Fixes #865`), current `src/recovery/wal.c` `flush_local_wal()` (lines ~689-724), `streaming_replica_issue.md` (575 lines, first ~250 read in full), and `4f4c365a`'s diff (read far enough to see the crit-section placement).
- Found: `7d04814b` is present and confirmed as the narrow #865 fix (COMMIT/ROLLBACK in the same container). `4f4c365a` ("Wrap wal_commit->current_oxid_commit code into CRIT_SECTION") is the currently-shipped broader fix — a different implementation than the doc's `200073b5` flag-balanced approach (not an ancestor of `a975c702`); an earlier attempt at the same idea (`b0488bd5`) was reverted (`20812559`) before `4f4c365a` landed, wrapping the commit branch inside `undo_xact_callback` starting at `START_CRIT_SECTION()` right after entering the `XACT_EVENT_COMMIT` case.
- Not found: where `4f4c365a`'s matching `END_CRIT_SECTION()` falls relative to the doc's documented "closing edge" (`curOxid = InvalidOXid`), and whether it reconciles with the doc's warning that `palloc` inside a critical section trips its own assert.
- Conclusion: tagged `(partial: fix commit for the exact #865 shape confirmed present and its intent matches; broader four-bug-class closure not independently re-verified against the current diff)`.

#### Does the residual Bug #2 finding (`orioledb-csn-incremented`, ~5% replica recovery-livelock post-fix) still apply to the currently-shipped `4f4c365a` fix?

- Examined: `streaming_replica_issue.md`'s documented residual finding (investigated against the `200073b5` implementation) and commit ancestry (`200073b5` is not an ancestor of `a975c702`; `4f4c365a` is a different, later implementation that is).
- Found: the doc's residual finding — Bug #2 changing from silent divergence to a rare (~5%) replica recovery-livelock post-fix — was measured against `200073b5`, not what's actually shipped.
- Not found: whether this residual applies to `4f4c365a` specifically, since it's a differently-structured fix.
- Conclusion: tagged `(needs re-investigation against 4f4c365a specifically, or a dedicated hunt)`.
