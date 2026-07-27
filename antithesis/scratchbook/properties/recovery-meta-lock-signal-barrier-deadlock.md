# recovery-meta-lock-signal-barrier-deadlock

## Status

**Confirmed open/unfixed at the analyzed commit `a975c702156cd449e9c0a8db6f8d9bf5bca4537d`.**
This is Task B, item 1, of a follow-up gap-filling pass: a bug found by a
fresh `git branch -r --sort=-committerdate` sweep during evaluation, with a
fix (`1df605da`) and the team's own deterministic repro test
(`meta_lock_deadlock_test.py`) already existing on branch
`origin/recovery-meta-buffering`, but **not yet merged**. Ancestry confirmed
directly (not assumed from the branch's own framing):
`git merge-base --is-ancestor 1df605da a975c702156cd449e9c0a8db6f8d9bf5bca4537d`
returns exit code 1 (false) — the fix is absent from the analyzed commit. The
earlier stopevent-only commit on that branch, `ccf9697b`, is also not an
ancestor. `git merge-base` of `1df605da`/`ccf9697b` against `a975c702` shows
they diverge at `3a957b0e`, well upstream of both.

This bug is a genuine, independently-verified standby-freezing self-deadlock
— not a stale claim (unlike orioledb#876/#889, which two prior passes found
already fixed on `main` despite `sut-analysis.md` calling them open). The
mechanism was independently re-derived against the current worktree, not
just accepted from the branch's own commit message, per
`validating-claims.md`'s discipline.

## The mechanism, verified against current `a975c702` code

**The bug shape:** a standby's recovery leader (startup process replaying
WAL) acquires `checkpoint_state->oTablesMetaLock` (an `LWLock`, declared in
`include/checkpoint/checkpoint.h`) on `WAL_REC_O_TABLES_META_LOCK` and holds
it across replay of *subsequent, unrelated* WAL records until the matching
`WAL_REC_O_TABLES_META_UNLOCK` arrives — potentially many records later. If
one of those intervening unrelated records is a `dbase_redo` record for
`CREATE DATABASE`/`DROP DATABASE`/`ALTER DATABASE ... SET TABLESPACE`, that
record's handler calls `WaitForProcSignalBarrier(EmitProcSignalBarrier(
PROCSIGNAL_BARRIER_SMGRRELEASE))` — which requires *every* backend/process,
including the replaying leader's own process, to acknowledge the new
ProcSignal barrier generation before the wait can complete. Acknowledging a
ProcSignal barrier happens inside `ProcessInterrupts()`/
`ProcessProcSignalBarrier()`, which is gated by `InterruptHoldoffCount == 0`
— and `LWLockAcquire()` brackets its held-lock section with
`HOLD_INTERRUPTS()`/`RESUME_INTERRUPTS()`. Because the leader is still
holding `oTablesMetaLock` (acquired via `LWLockAcquire`) when it reaches the
`dbase_redo` record, `InterruptHoldoffCount != 0`, so the leader's own
barrier-processing is a permanent no-op — it can never absorb the very
barrier it itself needs to acknowledge in order to proceed to the
`WAL_REC_O_TABLES_META_UNLOCK` record that would release the lock and
resume interrupt processing. This is a genuine circular self-deadlock: the
leader can't release the lock without processing the barrier, and can't
process the barrier without releasing the lock.

**Confirmed present in the current worktree** (independently verified
line-by-line, not merely trusted from the branch's commit message):

- `src/recovery/recovery.c` — the `WAL_REC_O_TABLES_META_LOCK` case (around
  line 4270-4275) calls `o_tables_meta_lock_no_wal()` directly, which
  (`src/catalog/o_tables.c`, around line 2412-2422) does
  `LWLockAcquire(&checkpoint_state->oTablesMetaLock, LW_SHARED)` and returns
  — no buffering, no early release.
- The `WAL_REC_O_TABLES_META_UNLOCK` case (`recovery.c`, around line
  4281-4293) calls `handle_o_tables_meta_unlock` (`recovery.c:3645`), which is
  the *only* place that releases the lock — reached only after every
  intervening WAL record between LOCK and UNLOCK has been replayed in full.
- `include/checkpoint/checkpoint.h` (around line 191) declares
  `oTablesMetaLock`; `src/checkpoint/checkpoint.c` never touches it during
  replay, confirming nothing else shortens the hold.
- Core Postgres's own interrupt/barrier machinery (patched tree, read only
  to confirm the general mechanism, per this task's explicit permission to
  read the fix commit and test — not to consult the patched source further
  for anything else): `LWLockAcquire`/`LWLockRelease` bracket the critical
  section with `HOLD_INTERRUPTS()`/`RESUME_INTERRUPTS()`;
  `ProcessInterrupts()` returns early whenever `InterruptHoldoffCount != 0`;
  `dbase_redo`'s `XLOG_DBASE_DROP` (and related) branches call
  `WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE))`.

**The fix, on `origin/recovery-meta-buffering` (commit `1df605da`, not
present at `a975c702`):** the leader no longer holds the lock across
unrelated records at all. On `WAL_REC_O_TABLES_META_LOCK`, it opens a
lock-free "buffered window," collecting `SYS_TREES_O_TABLES`/`O_INDICES`
modifies into a new `RecoveryXidState.meta_buf` list (every other sys-tree
modify in that window still applies in place via an extracted helper,
`recovery_apply_systree_modify()`). On `WAL_REC_O_TABLES_META_UNLOCK`, the
leader takes the lock only briefly, flushes the buffer atomically, and
releases. A new `checkpoint_state.oldestOpenMetaWindow` watermark clamps
restartpoint `replayStartPtr` so a restartpoint can't skip over a
buffered-but-not-yet-flushed window.

## The team's own deterministic repro (`meta_lock_deadlock_test.py`, on the same branch, added by `ccf9697b`, extended by `1df605da`)

A new stopevent, `before_o_tables_meta_unlock` (fired in
`o_tables.c`'s `o_tables_meta_unlock`, before the primary writes
`WAL_REC_O_TABLES_META_UNLOCK`), is used to deterministically construct the
race:

1. Park a `CREATE TABLE ... USING orioledb` backend on the primary at that
   stopevent (filtered by `application_name='ddlparker'`) — its `META_LOCK`
   WAL and the O_TABLES modify are already flushed, but `META_UNLOCK` isn't
   written yet.
2. Concurrently run `DROP DATABASE victim` on the primary — its `dbase_redo`/
   `PROCSIGNAL_BARRIER_SMGRRELEASE` WAL record lands, by construction,
   between the flushed `META_LOCK` and the still-pending `META_UNLOCK`.
3. Release the stopevent so `META_UNLOCK` is finally written.
4. Assert the standby (`self.catchup_orioledb(replica)`) replays the full
   interleaved stream (`META_LOCK` → `dbase_redo` → `META_UNLOCK`) without
   hanging, and ends up with the new table present and the dropped database
   correctly reflected.

The test authors' own comments note the deadlock reproduces reliably on
Linux CI; on macOS/PG18 locally it behaves as a non-regression check
(doesn't hang there, for platform-specific reasons not further investigated
in this pass).

## Property

| | |
|---|---|
| **Type** | Liveness (manifests as a permanent standby freeze — a hard availability failure, not merely slow progress) |
| **Property** | A streaming standby's recovery leader never permanently stalls replaying WAL because `oTablesMetaLock` was held across a `dbase_redo` record's `WaitForProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE)` call — i.e., a DDL statement that emits `WAL_REC_O_TABLES_META_LOCK`/`WAL_REC_O_TABLES_META_UNLOCK` around its systree changes never interleaves, on the WAL stream, with a concurrent `CREATE DATABASE`/`DROP DATABASE`/`ALTER DATABASE ... SET TABLESPACE` in a way that freezes standby replay indefinitely. |
| **Invariant** | `Always(standby_replay_progresses_within_bound)` — sample the standby's replay LSN/`pg_stat_replication` position (or `orioledb_get_xid_meta()`, already used elsewhere in this catalog for standby liveness sampling) at a steady cadence under a workload that mixes OrioleDB DDL with database-lifecycle DDL, and assert it never stalls past a generous bound. `Sometimes(meta_lock_open_during_dbase_redo)` as a reachability companion — confirms the interesting interleaving (a `dbase_redo` record landing while `oTablesMetaLock` is held open) was actually exercised in a given run, since without it the `Always` claim could pass vacuously. |
| **Antithesis Angle** | Requires a primary+streaming-standby topology (the harness's largest documented gap). Workload: concurrent `CREATE TABLE`/`CREATE INDEX ... USING orioledb` DDL (which brackets systree changes with `META_LOCK`/`META_UNLOCK`) racing against `CREATE DATABASE`/`DROP DATABASE`/tablespace-move DDL on the primary, with the standby actively replaying — no stopevent needed if Antithesis's own fault-driven scheduling can land the interleaving organically at a useful rate; the existing `before_o_tables_meta_unlock` stopevent (once merged, or added independently) would let a workload pin the race deterministically the way `sk-recovery-race` already does for the PK/SK checkpoint race. |
| **Why It Matters** | This is a **permanent, unrecoverable standby freeze** triggered by ordinary DDL concurrency — no crash, no corruption, no fault injection needed to reach it in principle, just an unlucky WAL interleaving of two DDL statement types that individually are completely ordinary. A frozen standby is a severe availability failure, and — unlike most of this catalog's replication-topology-gated findings, which concern subtle MVCC/undo bookkeeping — this one is a plain deadlock reachable by combinatorially common DDL, making it plausibly higher-probability in a busy multi-tenant workload (frequent table/index creation alongside occasional database drops) than many of the narrower checkpoint-timing races elsewhere in this catalog. |

**Open Questions:**

- Why does the deadlock reproduce reliably on Linux CI but only act as a
  non-regression (non-hanging) check on macOS/PG18 locally, per the test
  authors' own comment? Is this a genuine platform-dependent timing
  difference (e.g., signal-delivery latency, `ProcSignal` implementation
  differences), or a difference in how the two platforms' CI matrices
  configure `recovery_pool_size`/worker counts? `(needs human input — not
  investigated further, since the fix branch's own test comments were the
  source of this observation, not independently re-derived)`
- Does this same deadlock shape recur for *any* other `WaitForProcSignalBarrier`
  call reachable from a "regular" redo function's replay path landing inside
  an open `META_LOCK`/`META_UNLOCK` window — i.e., is `PROCSIGNAL_BARRIER_SMGRRELEASE`
  (via `dbase_redo`) the only trigger, or could other barrier types (e.g. a
  future 2PC-related or config-reload barrier) reach the same freeze via a
  different redo function? The investigating agent noted "any barrier type
  routed through the general `CHECK_FOR_INTERRUPTS`/`ProcessInterrupts` path
  while the lock is held would suffice" and that this is "not tied to 2PC or
  config-reload barriers specifically" — but did not enumerate every current
  or future `WaitForProcSignalBarrier` call site to confirm `dbase_redo` is
  the only one reachable during ordinary replay today. `(partial: mechanism
  generalized correctly; exhaustive call-site enumeration not performed)`
- Is the fix on `origin/recovery-meta-buffering` (`1df605da`) actively being
  reviewed/targeted for merge, or is it stalled/abandoned? Affects whether
  this property should be framed as "regression guard for a soon-to-land
  fix" vs. "open defect with no active remediation in progress." `(needs
  human input)`

## SUT-side instrumentation

`existing-assertions.md` confirms zero assertions in `src/recovery/`. A
`Reachable()` marker at the point `oTablesMetaLock` is acquired
(`o_tables_meta_lock_no_wal()`) paired with one at its release
(`handle_o_tables_meta_unlock()`), tagged with the elapsed WAL distance/record
count between them, would let Antithesis's search distinguish "the lock is
held briefly, as intended" from "the lock is held across an unusually long
replay window" — directly useful for biasing exploration toward the
dangerous interleaving without needing to merge the fix branch's stopevent
first.

## Investigation Log

#### Is the buggy pattern actually present and reachable in the current worktree, or only in the branch's own (possibly stale) framing?

- Examined: current worktree files (checked out at `a975c702`) —
  `src/recovery/recovery.c` (the `WAL_REC_O_TABLES_META_LOCK`/
  `WAL_REC_O_TABLES_META_UNLOCK` case handlers), `src/catalog/o_tables.c`
  (`o_tables_meta_lock_no_wal`, `handle_o_tables_meta_unlock`),
  `include/checkpoint/checkpoint.h` (the `oTablesMetaLock` declaration), plus
  the general Postgres interrupt/barrier mechanism (`HOLD_INTERRUPTS`,
  `ProcessInterrupts`, `WaitForProcSignalBarrier`, `dbase_redo`).
- Found: every link in the causal chain the branch's fix commit describes is
  present and traceable independently in the current worktree — the lock
  acquire/release call sites, the absence of any buffering between them, and
  the general Postgres mechanism by which a held `LWLock` masks
  `ProcessProcSignalBarrier` via `HOLD_INTERRUPTS`.
- Not found: an actual runtime reproduction — this investigation did not
  build or run the code (read-only investigation, no test execution), so the
  freeze was not directly observed, only traced statically end-to-end.
- Conclusion: treated as **confirmed reachable by static trace**, not merely
  by trusting the branch's own commit message — but explicitly not
  "empirically reproduced in this pass." A future pass building the
  Antithesis workload for this property should still expect to need to
  actually trigger and observe the freeze once a topology exists, per
  `validating-claims.md`'s general discipline of preferring evidence that
  exhibits the behavior over evidence that merely asserts it.
