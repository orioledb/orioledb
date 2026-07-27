# backup-window-crash-untested

## Focus

Backup/restore under fault injection. Directly targets the evaluation gap:
"Backup/restore under fault injection has zero properties... `test/integration/`
in this repo already has pgbackrest_test.py and walg_test.py (real tools, no
fault injection, not wired into CI per sut-analysis.md §9)." This property
identifies the specific missing fault-injection angle: a crash/interruption
landing inside the backup-consistency window itself, structurally analogous
to the PK/SK checkpoint-boundary race category that already anchors the
existing Antithesis harness.

## What led to this

`test/integration/pgbackrest_test.py::test_integration` already contains one
deliberately-constructed race (lines ~490-544, "Backup concurrent with an
in-flight OrioleDB checkpoint"): it sets the `checkpoint_writeback` stopevent,
starts a `pgbackrest backup --type=incr --start-fast` in a background thread,
waits for the checkpointer to actually park mid-writeback
(`wait_checkpointer_stopevent`), *then* mutates data and computes a
fingerprint, releases the stopevent, and finally restores the backup to a
scratch node and compares fingerprints. This is a real, deterministic,
already-existing test of the checkpoint/backup timing boundary — it is not a
gap. What it does *not* do, in either this test or anywhere else in either
622-line (`pgbackrest_test.py`) or 581-line (`walg_test.py`) file, confirmed
by grep across both:

- No `SIGKILL`, no `node.stop()` mid-backup, no simulated crash of the
  **primary** while a backup is in progress (only clean `node.stop()` calls
  between distinct restore scenarios, never during one).
- No interruption/kill of the **backup tool process itself** (`pgbackrest`/
  `wal-g`) mid-copy — every `_pgbackrest(...)`/`_walg(...)` helper call runs
  to completion or raises on nonzero exit; nothing kills the subprocess
  partway and then asserts the *next* backup attempt (or a `--delta`
  redo) still produces a correct, restorable backup.
- No test of a **standby crashing while restoring/replaying** a backup
  (`_create_standby`/`_repromote_standby` in pgbackrest, the standby
  restore+promotion flow in wal-g) — restores either fully succeed or the
  test fails outright; there's no "kill the standby's startup process midway
  through applying the backup's WAL and confirm it can resume/recover
  cleanly" scenario.

This is the exact gap the task description names: "a crash/interruption
mid-backup, or a base-backup taken while a checkpoint boundary race is in
flight." The *existing* test already covers the deterministic-timing half of
that (a stopevent-pinned checkpoint-vs-backup race); what's missing is
combining that same timing window with an actual process-level fault
(kill/restart), which is precisely what Antithesis's fault injection is
suited to add on top of a scenario the team has already hand-built the
scaffolding for.

## Why this is structurally similar to the PK/SK checkpoint-race category

Per the task's framing and per `property-catalog.md`'s "Checkpoint / Recovery
Boundary Consistency" category: a physical backup's consistency point *is* a
checkpoint. `pg_backup_start()` (invoked internally by `pgbackrest backup`
and `wal-g backup-push`) forces (or, without `--start-fast`, waits for) a
checkpoint; that checkpoint runs through OrioleDB's own `CheckPoint_hook`
exactly like any other checkpoint (`sut-analysis.md` §2's "Checkpoint is two
cooperating subsystems" finding applies unchanged here). Every property in
the existing "Checkpoint / Recovery Boundary Consistency" category
(`sk-fixup-undo-recycling-drop`, `checkpoint-recovery-lsn-sync-gap`,
`checkpoint-abort-snapshot-standby-panic`) concerns exactly this kind of
window — a checkpoint's captured LSN/undo/oxid boundary vs. what crash
recovery later replays. A backup's consistency-point checkpoint is not a
distinct mechanism from an ordinary checkpoint; it's the *same* mechanism,
gated by `pg_backup_start()`/`pg_backup_stop()` instead of the checkpointer's
own timer. A primary crash landing between the backup's consistency
checkpoint completing and `pg_backup_stop()` finalizing `backup_label` is
therefore a variant of the same "checkpoint boundary + crash" shape the team
already treats as high-value — just gated by a different trigger
(`pg_backup_start()`/pgbackrest/wal-g) than the existing `sk-recovery-race`
harness's automatic-checkpoint-timer trigger.

## What goes wrong if the property is violated

Two distinct failure shapes are worth distinguishing:

1. **The backup itself is silently non-restorable or restores to a
   structurally/logically wrong state** — the worse failure, since it would
   only be discovered at actual disaster-recovery time (per
   `sut-analysis.md` §10's severity framing: "medium-high impact,
   concentrated at incident-response time").
2. **The backup tool's own retry/resume logic (e.g. pgbackrest's
   `--delta` restore, already exercised for idempotency at
   `pgbackrest_test.py:387-403`, or a repeated `backup-push` after an
   interrupted prior attempt) does not correctly recover from a
   partially-completed prior attempt** — an availability/operability failure
   rather than a correctness one, but still untested under real interruption
   (today's `restore_count=2` test re-runs a *successful* restore twice, not
   a restore following a genuinely interrupted one).

## The property

| | |
|---|---|
| **Type** | Safety |
| **Property** | A primary crash (or a killed backup-tool process) landing at any point between a physical backup's consistency-point checkpoint (`pg_backup_start`) and its finalization (`pg_backup_stop`) never produces a backup that, once restored, has row content or `verify_orioledb()` structural state different from what a backup taken with no such interruption would have produced for the same logical point in time — and a subsequent backup/restore attempt against the same repository still succeeds. |
| **Invariant** | `Always(restored_content_and_structure_match_expected_state)`: extend the existing `test_integration`-style scenario (stopevent-pinned checkpoint concurrent with a backup) by adding an actual process fault — kill the primary's postmaster (`SIGKILL`/`SIGABRT`) or the backup-tool subprocess itself while the stopevent holds the checkpoint mid-writeback — then restart/retry and assert the restored fingerprint plus `verify_orioledb()` (see `backup-restore-lacks-structural-oracle`) both match. Complement with `Sometimes(crash_landed_inside_backup_consistency_window)` so the assertion isn't vacuously satisfied by runs where the fault happened to land outside the window. |
| **Antithesis Angle** | This is the one property in this discovery pass that benefits directly from Antithesis's process-level fault injection rather than needing new topology: reuse the exact scaffolding `pgbackrest_test.py`'s `test_integration` already built (the `checkpoint_writeback` stopevent + background backup thread + `wait_checkpointer_stopevent`), but instead of only mutating data while the checkpoint is parked, have Antithesis's fault injection (or a driver-level `SIGKILL`) hit the primary or the backup-tool process during that exact parked window — a much more organic way to explore the space of "crash exactly during the backup's consistency checkpoint" than hand-scripting every possible kill point. |
| **Why It Matters** | Backup/restore is explicitly named (`sut-analysis.md` §9/§10) as the one workflow category with zero fault-injection coverage despite two substantial real-tool integration suites existing; per §10, its severity is "concentrated at incident-response time" — exactly when a corrupted or non-restorable backup is most costly to discover. The mechanism (backup consistency checkpoint = ordinary `CheckPoint_hook` invocation) is the same one every other property in the "Checkpoint / Recovery Boundary Consistency" category already treats as high-value; this property is the backup-triggered instance of that same category, previously unaddressed because it requires an external backup tool rather than a bare `CHECKPOINT` command. |

**Open Questions:**

- Does `pg_backup_start()`/`pg_backup_stop()` (core Postgres, not orioledb-patched
  per this repo's own code — no hits for `backup_label`/`BackupInProgress`/
  `pg_backup_start` anywhere in `src/`/`include/`) request or wait for a
  distinct OrioleDB-specific checkpoint step beyond the standard
  `CheckPoint_hook` call already covered by `checkpoint-recovery-lsn-sync-gap`
  and friends, or is it exactly the same code path with no special-casing?
  `(partial: confirmed no orioledb-specific backup_label/BackupInProgress
  code exists in this repo, meaning OrioleDB participates in Postgres's
  standard backup-checkpoint machinery unmodified via CheckPoint_hook, not a
  bespoke path — but the patched-Postgres side of pg_backup_start itself was
  not re-examined, per the scope restriction on consulting
  orioledb_postgres)`
- Is a killed-mid-copy backup-tool subprocess (as opposed to a killed
  primary) actually reachable/meaningful for pgbackrest/wal-g, which run as
  independent processes outside the postgres container's own fault-injection
  surface in the current harness topology — would this require adding fault
  injection targeting a *client-side* process, a different mechanism than
  the SUT-process kills the rest of this catalog assumes? `(needs human
  input / harness design decision)`
- What does `--delta`/idempotent-retry actually do if pointed at a
  repository containing a partial, never-finalized backup (no prior test
  constructs this state)? Not traced through pgbackrest/wal-g's own source
  (external tools, out of this repo's scope) — flagged as needing either a
  black-box experiment or vendor documentation.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** `existing-assertions.md` confirms the only existing SDK
assertions target the PK/SK checkpoint race in `sk-recovery-race[-chaos]`;
nothing in `test/integration/` (pgbackrest/wal-g) uses the Antithesis SDK at
all — consistent with `sut-analysis.md` §9's note that these tests are "not
wired into CI/`installcheck`; no fault injection during backup/restore." A
`reachable()` marker at the point where `wait_checkpointer_stopevent` confirms
the checkpoint is parked (mirroring `sk-recovery-race/driver.py:164-168`'s
existing pattern) would let Antithesis's search confirm the fault actually
landed inside the intended window, the same discipline
`checkpoint-abort-snapshot-standby-panic`'s `Sometimes` companion assertion
already applies to a structurally similar timing window.

### Investigation Log

#### Does pg_backup_start()/pg_backup_stop() request or wait for a distinct OrioleDB-specific checkpoint step, or is it the same CheckPoint_hook path with no special-casing?

- Examined: grep across `src/` and `include/` for `backup_label`, `BackupInProgress`, `pg_backup_start`.
- Found: no orioledb-specific hits — OrioleDB participates in Postgres's standard backup-checkpoint machinery unmodified, via the same `CheckPoint_hook` already covered by `checkpoint-recovery-lsn-sync-gap` and friends, not a bespoke path.
- Not found: the patched-Postgres side of `pg_backup_start()` itself was not re-examined, per the scope restriction on consulting `orioledb_postgres`.
- Conclusion: tagged `(partial: ...)` — resolved for the orioledb-repo side; the patched-core side remains unchecked.

#### Is a killed-mid-copy backup-tool subprocess actually reachable/meaningful for pgbackrest/wal-g, given they run outside the postgres container's fault-injection surface?

- Examined: current harness topology and fault-injection scope.
- Found: pgbackrest and wal-g run as independent client-side processes outside the postgres container, not on the SUT-process fault-injection surface the rest of the catalog assumes.
- Not found: whether the harness has, or could add, a mechanism to fault-inject a client-side process.
- Conclusion: tagged `(needs human input / harness design decision)` — resolving this requires a harness/topology decision, not more code reading.
