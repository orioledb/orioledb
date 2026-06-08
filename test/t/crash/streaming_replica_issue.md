# Issues — OrioleDB commit-window injection-point bugs (streaming replica)

> **STATUS (2026-06-03):** Fixed by the flag-balanced crit section in commit **`200073b5`**. Per-injection verification: **6 of 7** in-window injections are now harmless (primary TRAPs, replica consistent). The one exception, **`orioledb-csn-incremented`**, has its failure mode *changed* (silent divergence → replica recovery-livelock), not eliminated — see [Fix committed + comprehensive per-injection verification](#fix-committed--comprehensive-per-injection-verification-2026-06-03-commit-200073b5) and the open-regression note below it. The rest of this document describes the **pre-fix** failure analysis.

**Four distinct failure modes** surface when `ereport(ERROR)` fires anywhere inside the o-o-branch commit pipeline of `undo_xact_callback`'s `XACT_EVENT_COMMIT` case. All four are manifestations of **a single root defect**: `ereport(ERROR)` is allowed to propagate after `WAL_REC_COMMIT` has been emitted into the local WAL buffer. The specific manifestation depends on which step of the pipeline the ERROR interrupts.

| # | Position in window | Confirmed solo trigger(s) | Failure mode | Solo rate |
|---|---|---|---|---|
| 1 | inside `wal_commit`, between `add_finish_wal_record` and `flush_local_wal` | `orioledb-wal-flush-guarded`, `orioledb-after-finish-wal-rec` | malformed same-container `COMMIT+ROLLBACK` → replica TRAP at `recovery.c:3712`, standby dies | 8/10 (15s) for wal-flush-guarded; 2/2 for after-finish-wal-rec |
| 2 | from `local_wal_has_material_changes = false` through to `current_oxid_commit`'s `set_oxid_csn` CAS | `orioledb-csn-incremented`, `orioledb-set-csn-guarded`, `orioledb-after-wal-commit`, `orioledb-after-assign-commit-lsn` | **silent** replica divergence — primary aborts in memory, replica only sees the COMMIT (wal_rollback short-circuits); SUM(balance) and/or PK dump differ on replica only | 5–25% depending on site; 60–100% paired with amplifiers |
| 3 | inside `flush_local_wal` after `log_logical_wal_container`, before flag reset, AND directly after `flush_local_wal` returns | `orioledb-after-flush-local-wal` | replica receives COMMIT then ROLLBACK in separate containers → replica `apply_undo_stack` panics: `undo_item_buf_read_item(): read of unexisting undo record` → standby dies | 2/2 (100%) |
| 4 | between `current_oxid_commit`'s CAS and `curOxid = InvalidOXid` in `oxid.c` | `oriole-before-curOxid-clear` | **primary-side** PK/SK desync — `apply_undo_stack` runs but the visibility callbacks see `xidBuffer.csn = real_csn` and skip some entries → PK heap and SK indexes disagree on the primary itself | 2/4 (~50%) |

## The complete bug-window map

The window opens at `add_finish_wal_record(WAL_REC_COMMIT, ...)` inside `wal_commit` (where the COMMIT bytes first enter any buffer) and closes at `curOxid = InvalidOXid` inside `current_oxid_commit` (where the abort callback's early-return becomes effective).

```
[entry: undo_xact_callback XACT_EVENT_COMMIT case body opens]
                                                ┌── SAFE before this point ──
                                                │
wal_commit() (src/recovery/wal.c):              │
    add_finish_wal_record(WAL_REC_COMMIT, ...)  ▼ ◄── WINDOW OPENS
                                                ▲
                                                │ ◄── Malformed WAL sequence
                                                │
    flush_local_wal()                           │
        log_logical_wal_container() submits to PG shared XLog
                                                │ ◄── Replica undo panic
                                                │
    [flush_local_wal returns]                   │
                                                │ ◄── Replica undo panic
                                                │
    local_wal_has_material_changes = false      │
                                                │ ◄── Primary-Replica desync from here onward
                                                │
[wal_commit returns]                            │
                                                │
set_oxid_xlog_ptr(oxid, xidless_commit_lsn)     │ Primary-Replica desync
[assign_xidless_commit_lsn returns]             │
XLogFlush(flushPos)                             │ Primary-Replica desync
SyncRepWaitForLSN(flushPos, true)               │ Primary-Replica desync
                                                │
current_oxid_precommit()                        │ Primary-Replica desync
    → set_oxid_csn(oxid, COMMITTING_SENTINEL)   │
                                                │
csn = pg_atomic_fetch_add(&nextCommitSeqNo, 1)  │ Primary-Replica desync
                                                │
current_oxid_commit(csn) (src/transam/oxid.c):  │
    set_oxid_csn(curOxid, real_csn | ...)       │ Primary-Replica desync
    pg_write_barrier()                          │ Primary-Replica desync → PK-SK desync transition
    csn_committing_set = false                  │
    xlog_ptr_committing_set = false             │
    my_proc_info->vxids[nl].oxid = InvalidOXid  │
    advance_run_xmin(curOxid)                   │ ◄── PK-SK desync (primary-internal)
    curOxid = InvalidOXid                       ▲ ◄── WINDOW CLOSES
                                                │  After this line, get_current_oxid_if_any() returns
                                                │  InvalidOXid → undo_xact_callback's early-return at
                                                │  the case entry fires → skips wal_rollback,
                                                │  current_oxid_abort, AND apply_undo_stack entirely.
    release_assigned_logical_xids()             │
[current_oxid_commit returns]                   │
                                                ├── SAFE from this point ──
[if (enable_rewind) {...}]                      │
for (i) on_commit_undo_stack(...)               │
wal_after_commit()                              │
reset_cur_undo_locations()
reset_command_undo_locations()
oxid_needs_wal_flush = false
xidless_commit_lsn = InvalidXLogRecPtr
[case ends, break]
```

## Why each manifestation has its specific fingerprint

| Boundary crossing | What changes | Effect when ERROR fires after |
|---|---|---|
| `add_finish_wal_record` writes COMMIT bytes | bytes in process-local buffer | abort's `wal_rollback` appends ROLLBACK into the **same** buffer → both finish records in one container → Bug #1 |
| `flush_local_wal` submits container to shared XLog | walsender can ship COMMIT | replica receives the COMMIT |
| `flush_local_wal` resets buffer state (offset=0, contains_xid=false) | local buffer drained | abort's `wal_rollback` (if still active) starts a fresh container → Bug #3 |
| `local_wal_has_material_changes = false` | flag flips | abort's `wal_rollback` **short-circuits** → no ROLLBACK to replica → Bug #2 |
| `set_oxid_csn(real_csn)` CAS | `xidBuffer.csn = real_csn` | abort still runs `apply_undo_stack`; visibility callbacks see "tx committed" → some entries skip → Bug #4 |
| `curOxid = InvalidOXid` | per-process oxid cleared | `get_current_oxid_if_any() = InvalidOXid` → abort callback's early-return → no abort processing at all → SAFE |

The window therefore has a **clear opening edge** (the moment COMMIT bytes exist anywhere) and a **clear closing edge** (the moment `curOxid` is cleared, signalling "no orioledb tx left to abort"). Every position in between produces *some* form of corruption, with the specific shape determined by which intermediate states have already been published.

## Root cause of primary/replica rollback-flow asymmetry — `local_wal_has_material_changes == false`

The single line that turns a normal commit-error into Bug #2 silent divergence is:

```c
// src/recovery/wal.c — inside wal_commit, right after flush_local_wal
local_wal_has_material_changes = false;
```

This flag tracks "this process owes the WAL stream a finish record (COMMIT or ROLLBACK)." Once `wal_commit` flushes the COMMIT container, the flag is cleared because the WAL is internally consistent.

The abort handler `wal_rollback` reads this flag as its first action (`src/recovery/wal.c:401-411`):

```c
wal_rollback(OXid oxid, TransactionId logicalXid, bool isAutonomous)
{
    XLogRecPtr wait_pos;
    if (!local_wal_has_material_changes)
    {
        local_wal_buffer_offset = 0;
        local_type = oIndexInvalid;
        ORelOidsSetInvalid(local_oids);
        return;                              // ← SHORT-CIRCUIT: no WAL_REC_ROLLBACK emitted
    }
    ...
}
```

After the COMMIT has been written, this early-return is what makes the primary's rollback flow diverge from the replica's:

| Side | What happens when ERROR fires AFTER `local_wal_has_material_changes = false` |
|---|---|
| **Primary** | xact machinery runs `AbortTransaction` → `wal_rollback` short-circuits (no WAL_REC_ROLLBACK emitted), then `apply_undo_stack` walks per-row undo entries and inverse-applies them. Primary's in-memory rows revert to pre-tx values. |
| **Replica** | Walreceiver receives only the WAL_REC_COMMIT that the primary already shipped (no ROLLBACK ever arrives). Replica replays the COMMIT, treats the tx as durably committed, keeps post-tx row values. |

Net: primary state ≠ replica state on the rows the doomed tx touched. The `wal_rollback` short-circuit is the **root mechanism** behind the asymmetry — it's a correctness optimization that assumes "if there's nothing pending in the WAL buffer, the WAL stream is already consistent for this transaction." That assumption breaks only in the post-`add_finish_wal_record` window, where the COMMIT was flushed-and-shipped but the primary then decides (via the propagated ERROR) to roll back anyway. The primary's in-memory state reverts; the replica's doesn't; nothing in WAL signals the rollback.

This is why "force `local_wal_has_material_changes = true` after `assign_xidless_commit_lsn` returns" was a partial fix: it forced `wal_rollback` to take the non-short-circuit path and emit a ROLLBACK, restoring symmetry of the rollback flow — but at the cost of introducing Bug #3 (replica gets COMMIT+ROLLBACK in separate containers and PANICs trying to walk undo it already committed). The principled fix has to escalate the ERROR to PANIC before reaching the abort path at all.

## The principled fix

The defect is allowing `ereport(ERROR)` to propagate from any code path between these two edges. The fix is to **`START_CRIT_SECTION()` across the entire window** — but the window spans across three function boundaries (`wal_commit`, `assign_xidless_commit_lsn`, `current_oxid_commit`) and a `XACT_EVENT_COMMIT` case body. The most surgical equivalents:

1. **`START_CRIT_SECTION()` at the open edge, `END_CRIT_SECTION()` at the close edge.** Wraps everything in between in a critical section. Any ERROR raised inside is escalated to PANIC. The primary crashes, recovery brings it back, the malformed WAL never reaches the replica (because the ERROR happens before flush, OR because crash recovery on the primary sees the COMMIT-and-no-ROLLBACK and discards via WAL replay). The hazard: critical sections forbid `palloc`, so any palloc-ing call inside the window (logging, error reporting, etc.) becomes unsafe — needs auditing.

2. **Move `curOxid = InvalidOXid` to immediately after `wal_commit` returns.** This pulls the closing edge as far left as possible. Combined with `START_CRIT_SECTION` around just `wal_commit`'s body (Bug #1 and Bug #3 paths), this would leave only a tiny remaining window between `wal_commit` and the new early `curOxid` clear. ERROR in that tiny window would skip the entire abort path (Bug #2 silent divergence becomes "no abort runs, primary stays at committed state matching replica — no divergence").

3. **Both layered.** `START_CRIT_SECTION` around the wal_commit body (closes Bug #1, Bug #3), plus early `curOxid` clear (closes Bug #2, Bug #4). Most defensive but requires the most code restructuring.

### Implemented fix — flag-balanced crit section (option 1)

Option 1 is implemented. The window spans two functions (`wal_commit` opens it, `current_oxid_commit` closes it), so the `START_CRIT_SECTION()` and `END_CRIT_SECTION()` live in different files and cannot be lexically paired. A module-global flag carries the "we opened a crit section" state across the gap.

**Crit-section BEGIN — `src/recovery/wal.c`, inside `wal_commit()`:**

```c
// wal.c:53  — module-global flag (definition)
bool commit_wal_record_added = false;

// wal.c:351 — open edge, immediately before the COMMIT bytes enter the buffer
START_CRIT_SECTION();                 // ◄── CRIT BEGINS (replica-bug-fix)
commit_wal_record_added = true;       // wal.c:352 — record that WE opened it
add_finish_wal_record(WAL_REC_COMMIT, ...);   // wal.c:354 — WINDOW OPENS
walPos = flush_local_wal(true, !isAutonomous); // wal.c:357
```

**Crit-section END — `src/transam/oxid.c`, inside `current_oxid_commit()`:**

```c
// oxid.c:105 — flag visible here (extern declaration)
extern bool commit_wal_record_added;

// oxid.c:1538 — close edge, the moment curOxid is cleared
curOxid = InvalidOXid;                // WINDOW CLOSES
if (commit_wal_record_added)          // oxid.c:1543 — only if WE opened it
{
    commit_wal_record_added = false;
    END_CRIT_SECTION();               // ◄── CRIT ENDS (replica-bug-fix)
}
release_assigned_logical_xids();      // oxid.c:1550 — palloc/pfree, correctly OUTSIDE crit
```

**Why the flag is mandatory (not cosmetic).** `wal_commit`'s `START_CRIT_SECTION` is *conditional*: it is skipped by the early-return at wal.c:312 (`if (!local_wal.has_material_changes) return;`), and `wal_commit` is only called on the o-o commit branch (`assign_xidless_commit_lsn`, undo.c). But `current_oxid_commit` runs on *every* commit, including h-h transactions and DDL (e.g. `CREATE TABLE`) that never entered `wal_commit`. Without the flag, those paths hit `END_CRIT_SECTION()` with no matching `START` → `TRAP: failed Assert("CritSectionCount > 0")` at the END site, killing the cluster during ordinary setup. The flag makes END fire **iff** this backend actually opened the section.

**Essential steps when widening/auditing this crit section:**

1. **No `palloc` inside the window.** Critical sections assert `CritSectionCount == 0 || allowInCritSection` on every allocation. Every palloc-ing call between the two edges must be removed or disabled:
   - All `elog()`/`ereport()` traces in the window are commented (they `palloc` to format args). Search for the marker `elog disabled: inside replica-bug-fix CRIT_SECTION` in `wal.c` and `oxid.c`.
   - The `STOPEVENT(STOPEVENT_AFTER_CSN_PRECOMMIT, ...)` block in `undo.c` (between `current_oxid_precommit` and `current_oxid_commit`) is commented out — `make_empty_params()` (`stopevent.c`) palloc's even with `NULL` params. NOTE: this disables the stop event `transaction_test.py` relies on; re-enabling it requires moving it out of the window.
2. **Keep `release_assigned_logical_xids()` OUTSIDE** — it palloc/pfrees; it must stay after `END_CRIT_SECTION()` (it already does, oxid.c:1550).
3. **Injection points inside the window cannot fire cleanly in a dev/assert build.** `InjectionPointRun` itself palloc's on first load (`load_external_function` → `pstrdup`), so an attached `error` action TRAPs on the allocation (`mcxt.c` assert) *before* the intended `ERROR`→PANIC escalation. This patched PG17 lacks PG18's `INJECTION_POINT_CACHED()`/`INJECTION_POINT_LOAD()` macros (designed precisely for crit sections), so injection-point reproducers for Bugs #1/#3/#4 now produce a primary assert-TRAP instead of a clean PANIC. The TRAP is still *protective* — it fires at the injection site (e.g. wal.c:830, before `log_logical_wal_container` writes COMMIT to XLog) and `abort(2)` skips `wal_rollback`, so no malformed WAL reaches the replica. In production (no asserts, or a real non-injected error) it is a clean PANIC.

**Validation.** Raw bank-account workload, streaming replica, no injection, no kill (`RR_INJECTION_POINTS=NONE`, 10×15s, 20 writers): **10/10 clean**, ~8.4k real commits, plan-check 10/10. The flag-balanced bracket is correctly balanced across the o-o and h-h commit paths and does not TRAP under normal load. The injection path (Bug #1) shows the expected primary crash with the **replica staying consistent** — `consistent recovery state` → reconnect after primary recovery, no replica TRAP / undo-panic / divergence.

### Fix committed + comprehensive per-injection verification (2026-06-03, commit `200073b5`)

The flag-balanced crit section is committed as **`200073b5` "Reapply: first attempt to add crit section into commit flow done"** (on top of the recovery-livelock fix `fb1a8acc`). Built with `IS_DEV=1`.

Verified two ways against a streaming standby (`RR_PANIC_FATAL=0`, `RR_ASSERT_FIRINGS=0`, 20 writers):

1. **All-injections soak** (`RR_INJECTION_POINTS=ALL`, 20×15s): **20/20 clean, 0 divergence, 0 replica TRAP**; primary TRAPped in the 14 trials where an in-window guarded point fired, replica consistent every trial.
2. **Per-injection sweep** — one mini-hunt per in-window point, checking (a) 0 BUG, (b) primary `TRAP`/`PANIC`, (c) **no `AbortTransaction while in COMMIT state`** (the pre-patch broken-rollback fingerprint). Result map:

   | In-window injection | Bug class | Post-fix verdict |
   |---|---|---|
   | `orioledb-wal-flush-guarded` | #1 | ✅ HARMLESS — primary TRAP, replica consistent |
   | `orioledb-set-csn-guarded` | #2 | ✅ HARMLESS (3/3, primary TRAP) |
   | `orioledb-after-flush-local-wal` | #3 | ✅ HARMLESS (3/3, primary TRAP) |
   | `oriole-before-curOxid-clear` | #4 | ✅ HARMLESS (3/3, primary TRAP) |
   | `orioledb-set-xlog-ptr-guarded` | amp | ✅ HARMLESS (3/3; TRAP not observed — didn't fire) |
   | `orioledb-add-finish-wal-guarded` | amp | ✅ HARMLESS (3/3, primary TRAP) |
   | **`orioledb-csn-incremented`** | #2 | ❌ **FAILED — rare (≲5%) replica recovery-livelock** (see below) |

   **6 of 7 in-window injections are now harmless.** Across every harmless point, `AbortTransaction while in COMMIT state` count is **0** — the crit section escalates the in-window `ereport(ERROR)` to a primary TRAP (`abort(2)`, which skips `wal_rollback`), so the broken rollback never runs and no malformed WAL reaches the replica.

### Open regression — `orioledb-csn-incremented` now livelocks the replica (mode change, not eliminated)

`csn-incremented` sits at `undo.c:2360`, between the **global** `nextCommitSeqNo` fetch-add and the **per-oxid** `xidBuffer` CSN flip in `current_oxid_commit()`. The crit-section fix protects the *primary* correctly here (TRAP `1`, `AbortTransaction while in COMMIT state` = `0`), but it **changed the failure mode rather than removing it**:

- **Pre-fix** (May logs, `*_invariant.log`): silent replica **divergence** (Bug #2 — primary OK, replica total differs).
- **Post-fix** (`200073b5`): the primary TRAPs mid-commit **after** the COMMIT was flushed+streamed but **before** the per-oxid CSN was finalized, leaving the standby holding in-flight oxids it can never resolve → **replica recovery-livelock**: the standby recovery workers spin in `o_btree_modify_handle_conflicts` → `oxid_get_csn`, observed **~1.4M `oxid_get_csn-call` spins on 3 oxids (e.g. 326/350/352)** over a 150s timeout, never re-reaching a stable consistent state. Replica log balloons to ~2GB; `replicaTRAP=0` (livelock, not crash).

This is the **same `oxid_get_csn`-spin class** as the recovery-livelock investigation, but `fb1a8acc` does **not** cover it: those oxids are stranded in COMMITTING-limbo (global CSN advanced, per-oxid never set), not the `recovery_finish`-abort case `fb1a8acc` handles. Note the pre-flush/post-flush position is **not** the discriminator — `after-flush-local-wal` (post-flush, Bug #3) is harmless; only the global-CSN-advance site (`csn-incremented`) misbehaves.

**Rate — it is RARE / intermittent, not deterministic.** The tight 1.4M-spin livelock has been observed **once** (per-injection sweep, 1 trial). A dedicated confirmation hunt (**20×30s, only `csn-incremented`, streaming, timeout 100s**) did **not** reproduce it: **19/20 clean, 0 tight-livelock, 1 timeout that was *slow catchup* not a livelock** — that one replica was *progressing* (advancing redo LSNs `0/30A49F0→0/30A4AD0`, only 623 `oxid_get_csn-call`, reached consistent state), likely aggravated by csn-trace logging slowing replica redo. So the failure spectrum for `csn-incremented` post-crash is: usually the replica resolves (clean), sometimes catches up slowly, and **rarely** wedges in the hard `oxid_get_csn` spin. Estimated tight-livelock rate ≲ 5% at this config — needs a wider hunt (≳ 40–60 trials) to catch another instance for root-cause logs. (The original 2GB sweep-1 livelock log was deleted before this hunt; the kept `results/KEEP_csnbug_*` pair is the *slow-catchup* sample, not the tight spin.)

---

## Bug #1 — `orioledb-wal-flush-guarded` produces malformed WAL → replica recovery TRAP

### Summary

When the `orioledb-wal-flush-guarded` injection point fires `ereport(ERROR)` in a
backend that is mid-COMMIT (after the COMMIT WAL record has been flushed but
before the in-memory commit bookkeeping completes), the primary emits a WAL
record that contains **both `COMMIT` and `ROLLBACK` sub-records for the same
`oxid`** inside one OrioleDB WAL container. Any standby applying that record
fails an assert at `src/recovery/recovery.c:3712`:

```
TRAP: failed Assert("rec->oxid != InvalidOXid"),
      File: "src/recovery/recovery.c", Line: 3712
```

The replica's startup process aborts (signal 6), the postmaster shuts down the
replica entirely. The primary keeps running unaffected.

### Observed cluster behavior

Reproduced in `RR_INJECTION_POINTS=orioledb-wal-flush-guarded` mode against an
orioledb table with a streaming-replication standby:

- **Primary**: continues normally. The error-injected backend logs the standard
  PostgreSQL warning:
  ```
  WARNING:  AbortTransaction while in COMMIT state
  ```
  No `PANIC:` and no `TRAP:` on the primary. The COMMIT WAL record had already
  been flushed to disk; the abort path then emits a ROLLBACK WAL record for the
  same `oxid`, producing the malformed combined record.
- **Replica**: walreceiver pulls the malformed record from the primary's
  walsender, applies it via `orioledb_redo` → `rec->oxid` is `InvalidOXid` for
  the combined record → assert fires → startup process aborts.
- **Result on the test harness**: replica is dead for the remainder of the
  trial. `replica.catchup()` returns without detecting the death (testgres polls
  primary-side `pg_stat_replication`, a disconnected standby looks like zero
  lag). Subsequent diagnostic queries fail with `connection refused`.

### Minimal WAL record shape (from a real replica log)

```
WAL redo at 0/30060B8 for OrioleDB resource manager/OrioleDB WAL container:
  XID (123 224 0);
  COMMIT   (123 224 0 - xmin 107 csn 146);
  ROLLBACK (123 224 0 - xmin 107 csn 146);
```

Both COMMIT and ROLLBACK appear in a single `OrioleDB WAL container` for the
same `oxid = 123/224/0`. Reachable on the primary with no chaos other than this
one injection point + an active write workload.

### Frequency

Hunt config: 10 trials × 15 s × 20 writers, streaming replica.

| `RR_INJECTION_POINTS` | BUG rate | Failure mode |
|---|---|---|
| `orioledb-wal-flush-guarded` (only) | **8 / 10 (80 %)** | replica recovery TRAP, then `catchup() failed: CatchUpException` + `connection refused` on diagnostic queries |
| all 8 other injection points (no wal-flush) | 5 / 10 (50 %) | different bug — replica stays alive with divergent state |

So `orioledb-wal-flush-guarded` is, in isolation, an ~80 %-reliable reproducer
of this specific failure mode.

### Why this fingerprint and not the other one

Compared to the other commit-window injections, `orioledb-wal-flush-guarded` is
the only one that fires **after** `XLogFlush()` of the COMMIT record:

| injection point | fires where | effect on WAL on disk |
|---|---|---|
| `orioledb-pk-mutated-pre-wal` | after PK mutation, before any WAL emit | clean rollback; no COMMIT on disk |
| `orioledb-set-csn-guarded` | inside `current_oxid_commit` after CSN set, pre-flush | clean rollback; no COMMIT on disk |
| `orioledb-set-xlog-ptr-guarded` | inside XLog pointer setting in commit, pre-flush | clean rollback; no COMMIT on disk |
| `orioledb-add-finish-wal-guarded` | when adding the finish WAL record, pre-flush | clean rollback; no COMMIT on disk |
| **`orioledb-wal-flush-guarded`** | **after** `XLogFlush()` of the COMMIT record | **COMMIT already on disk → abort adds ROLLBACK to same WAL container** |

Only the last fires *post-flush*, so only it produces an on-disk record that
contains both COMMIT and ROLLBACK for the same `oxid`.

### Root cause (hypothesised)

orioledb's commit path lets `ereport(ERROR)` propagate after `XLogFlush()` of
the COMMIT record. Vanilla PostgreSQL escalates `ERROR` to `PANIC` inside the
critical section that wraps a flushed-but-not-finalised commit, on the grounds
that you can't safely roll back something that's already been made durable. In
orioledb the abort path runs anyway and emits its own ROLLBACK record into
the same WAL flush window.

The replica then sees a record it can't apply atomically. Even if the
recovery side were hardened to handle the COMMIT+ROLLBACK pair (e.g. treat
the COMMIT as authoritative because it landed first), the primary's commit
machinery is already in an inconsistent state — the in-memory CSN/oxid
bookkeeping has been partially set up but then unwound, which is exactly the
kind of split-brain that subsequent transactions inherit.

### Fix candidates (not yet implemented)

1. **`START_CRIT_SECTION()` around the post-flush portion of `current_oxid_commit`** — escalate any error there to `PANIC`, matching vanilla PG semantics. The injection point would still fire; it would just crash the primary instead of corrupting the WAL stream.
2. **Suppress the ROLLBACK emission when the COMMIT record has already been flushed** — the abort path could detect that the commit is durable and skip emitting a contradictory ROLLBACK. Primary's in-memory state would still be wrong, but the replica would survive.
3. **Replica-side hardening** at `src/recovery/recovery.c:3712` — treat COMMIT+ROLLBACK pair as "commit wins" instead of asserting. Defence-in-depth, doesn't fix the primary's split state.

### Reproducer

```bash
RR_STORAGE_ENGINE=orioledb RR_REPLICA_MODE=streaming \
RR_WRITERS=20 RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
RR_DURATION=15 RR_INJECTION_POINTS=orioledb-wal-flush-guarded \
RR_ASSERT_FIRINGS=0 RR_KILL_POSTMASTER=0 RR_PANIC_FATAL=0 \
TIMEOUT=180 ./test/t/crash/run_hunt.sh 10 only_wal_flush
```

Expected: ~8/10 trials report `BUG plan-OK` with the `catchup() failed:
CatchUpException` fingerprint. Saved logs in
`test/t/crash/results/*_only_wal_flush_*_invariant.log` contain the primary's
trace; the replica's log (in `tmp_check_t/*_tgsb/logs/postgresql.log` if it
survives teardown) carries the TRAP.

---

## Bug #2 — silent replica divergence in the CSN-flip race window

### Summary

A race window inside `current_oxid_commit()`'s call chain — between the
moment `nextCommitSeqNo` is fetch-added (giving this transaction its CSN
number) and the moment `set_oxid_csn` publishes that number into `xidBuffer`
— is unsafe against `ereport(ERROR)`. Any error raised inside this window
unwinds the transaction via the regular abort path; no PANIC, no TRAP, no
malformed WAL. The primary's in-memory state ends up internally consistent
(the bank-account invariant passes on the primary), but the streaming
standby's post-replay state **diverges** from the primary's: when the same
invariant query is run on the replica at end-of-test it returns a
*different* total.

The replica never crashes, replication never stalls, `replica.catchup()`
succeeds. The bug is invisible from the primary alone — only the cross-engine
/ cross-instance check at the end of the trial flags it.

**Two distinct injection points sit inside this window** and reproduce the
same bug with the same fingerprint at similar low-but-nonzero rates:

| Injection point | Site | Solo BUG rate (20×30s) |
|---|---|---|
| `orioledb-csn-incremented` | `src/transam/undo.c:2360` — between `nextCommitSeqNo` fetch-add and the call to `current_oxid_commit(csn)` | **3 / 20 (15 %)** |
| `orioledb-set-csn-guarded` | `src/transam/oxid.c:592` — inside `set_oxid_csn()`, *before* the `pg_atomic_compare_exchange_u64` that flips `xidBuffer[oxid].csn` | **2 / 20 (10 %)** |

`orioledb-set-csn-guarded` fires *downstream* of `orioledb-csn-incremented`
in the same call chain (`undo.c:2368` → `current_oxid_commit` → `set_oxid_csn`
→ `oxid.c:592`). Two independent injection sites inside the same ~3-line
window producing the same silent-divergence fingerprint is the central
evidence that the bug is the window, not either point individually.

### Observed cluster behavior

Reproduced in `RR_INJECTION_POINTS=orioledb-csn-incremented` *or*
`RR_INJECTION_POINTS=orioledb-set-csn-guarded` mode against an orioledb
table with a streaming-replication standby — same fingerprint either way:

- **Primary**: continues normally. The error-injected backend logs the standard
  PostgreSQL warning (same as Bug #1):
  ```
  WARNING:  AbortTransaction while in COMMIT state
  ```
  No `PANIC:` and no `TRAP:`. Bank-account invariant on the primary: **OK**.
- **Replica**: walreceiver pulls every WAL record, `orioledb_redo` applies them
  all without complaint. `pg_stat_replication` shows zero lag. Bank-account
  invariant on the replica: **VIOLATED** — total sum across all accounts
  differs from the primary's total.
- **Result on the test harness**: the `replica invariant` block at the end of
  the trial is the only thing that catches the bug; the trial logs
  `BUG plan-OK` with no crash signature in any postgres log.

### The race window — two injection points, one window

The window spans the call chain from `undo_xact_callback`'s
`XACT_EVENT_COMMIT` branch (`src/transam/undo.c`) down into `set_oxid_csn`
(`src/transam/oxid.c`). Annotated with both injection sites:

```c
// src/transam/undo.c, undo_xact_callback() XACT_EVENT_COMMIT branch
current_oxid_precommit();                            // ← line 2321: undo on-commit
                                                     //   actions are precommitted
...
csn = pg_atomic_fetch_add_u64(&nextCommitSeqNo, 1);  // ← line 2342-2344:
                                                     //   GLOBAL CSN counter advanced
INJECTION_POINT("orioledb-csn-incremented");         // ← line 2360: SITE 1
current_oxid_commit(csn);                            // ← line 2368: descends into
                                                     //   set_oxid_csn(oxid, csn)
//        │
//        ▼
// src/transam/oxid.c, set_oxid_csn_internal()
    INJECTION_POINT("orioledb-set-csn");             // ← line 590
    if (csn != COMMITSEQNO_ABORTED)
        INJECTION_POINT("orioledb-set-csn-guarded"); // ← line 592: SITE 2
    ...
    pg_atomic_compare_exchange_u64(                  // ← line 599: PER-OXID CSN
        &xidBuffer[oxid % xid_circular_buffer_size].csn,  //   FINALLY flipped
        &oldCsn, csn);
```

The window between the global counter advance (`undo.c:2344`) and the
xidBuffer flip (`oxid.c:599`) is asymmetric:

- The **global** `nextCommitSeqNo` has been advanced.
- The **per-oxid** state in `xidBuffer` is still `COMMITSEQNO_COMMITTING`
  (set by `current_oxid_precommit()` at undo.c:2321).
- `current_oxid_precommit()` has already executed undo on-commit actions, so
  some commit side-effects are durable in the in-memory state.

An `ereport(ERROR)` raised anywhere inside this window — whether at
**site 1** (`orioledb-csn-incremented`) or **site 2**
(`orioledb-set-csn-guarded`) — triggers the abort path. The precommit
side-effects above are not reversed, the xidBuffer flip never happens, and
the streaming standby's replay path produces a state different from the
primary's.

### Frequency

Hunt config: 20 writers, streaming replica. Two sampling regimes:
**20 × 30 s** for solo-trigger characterization, **10 × 15 s** for paired
runs (already saturating).

| `RR_INJECTION_POINTS` | BUG rate | Notes |
|---|---|---|
| `orioledb-csn-incremented` (only) | **3 / 20 (15 %)** at 30 s | 0/10 at 15 s — solo rate sits below 10×15s sampling threshold |
| `orioledb-set-csn-guarded` (only) | **2 / 20 (10 %)** at 30 s | downstream site in the same race window; 1/10 at 15 s in prior re-verify |
| `orioledb-csn-incremented` + `before-tx-commit` | **10 / 10 (100 %)** | partner is 0/20 alone at 30 s |
| `orioledb-csn-incremented` + `orioledb-set-xlog-ptr-guarded` | **9 / 10 (90 %)** | partner is 0/20 alone at 30 s |
| `orioledb-csn-incremented` + `orioledb-add-finish-wal-guarded` | **6 / 10 (60 %)** | partner is 0/20 alone at 30 s |
| any of those three partners alone (no solo trigger) | 0 / 20 at 30 s | confirmed non-triggers without a window-resident solo trigger |
| `orioledb-pk-mutated-pre-wal`, `orioledb-update-pk-done-pre-sk`, `orioledb-sk-mid-update` (each alone) | 0 / 20 at 30 s | outside the CSN-flip window; non-triggers |

So the bug is the **race window**, not a single injection:

- Both `orioledb-csn-incremented` and `orioledb-set-csn-guarded` produce the
  bug on their own at similar low rates (10–15 % at 30 s, sampling-floored
  to 0 at 15 s); they live ~5 source-lines apart in the same call chain.
- Three otherwise-innocent commit-pipeline injections
  (`before-tx-commit`, `orioledb-set-xlog-ptr-guarded`,
  `orioledb-add-finish-wal-guarded`) act as **amplifiers**: each holds the
  backend longer somewhere in the commit pipeline, widening the
  CSN-flip window so the next commit attempt almost always lands inside it.

> Pair-bisect with `orioledb-set-csn-guarded` as the seed has not been
> run yet. If the window hypothesis is right, it should light up the
> same three amplifiers at comparable rates.

### Why this fingerprint and not Bug #1

Both site 1 and site 2 fire **before** any COMMIT WAL record reaches
`XLogFlush()`:

| injection point | window | on-disk WAL effect |
|---|---|---|
| `orioledb-csn-incremented` / `orioledb-set-csn-guarded` (both sites) | between global CSN fetch-add and per-oxid CSN flip; both still before WAL flush | only ROLLBACK on disk — no malformed pair |
| `orioledb-wal-flush-guarded` (Bug #1) | after `XLogFlush()` of the COMMIT record | COMMIT already on disk → abort adds ROLLBACK → replica TRAP |

So Bug #2 produces no malformed WAL — just an **asymmetric primary state**:
the primary's in-memory bookkeeping records partial commit side-effects
(precommit on-commit actions executed) that the WAL stream alone cannot
reproduce on the replica.

### Root cause (hypothesised)

Working hypothesis — to be confirmed by direct trace of the precommit /
abort sequence:

1. `current_oxid_precommit()` at `undo.c:2321` commits some in-memory state
   for the oxid (undo on-commit actions, possibly undo-stack `onCommit`
   linkage).
2. An injected ERROR anywhere in the CSN-flip window — site 1 at
   `undo.c:2360` or site 2 at `oxid.c:592` — triggers `XACT_EVENT_ABORT`,
   which calls `apply_undo_stack(...)` to inverse-apply the modifications.
3. **The inverse-apply does not unwind step 1's precommit side-effects** —
   it only unwinds the per-row modify records. The primary therefore ends
   in a state that mixes "aborted" (undo inverted) with "partially committed"
   (precommit side-effects still in place).
4. The WAL stream carries only modify-records + a ROLLBACK record. The
   replica replays both, runs its own `apply_undo_stack` against its locally
   re-derived undo log, and ends up in a *clean* aborted state.
5. Net: primary and replica diverge on exactly the rows whose precommit
   side-effects survived the abort. The bank-account invariant sums one
   account differently between the two — invariant on primary still passes
   because the asymmetry preserves total balance on the primary's
   bookkeeping, but on the replica the row in question carries the pure
   pre-transaction value.

That the same fingerprint reproduces from *two* injection sites separated
by the `current_oxid_commit` → `set_oxid_csn` function boundary is the
clearest evidence that the bug is the window, not either site. The
amplifier observation (60–100 % when paired with any of three unrelated
commit-pipeline injections) is consistent with a tight timing window: any
slowdown in the commit pipeline that holds the backend longer between
`nextCommitSeqNo` advance and the xidBuffer flip widens the race that
either solo trigger exploits.

### Fix candidates (not yet implemented)

1. **Move `nextCommitSeqNo` fetch-add inside the same indivisible step as
   `current_oxid_commit(csn)`** — or atomically pair the global counter
   advance with the per-oxid xidBuffer flip, so there is no observable
   window where `nextCommitSeqNo > xidBuffer[oxid].csn` and an ERROR can
   strand the oxid in COMMITTING.
2. **Make `XACT_EVENT_ABORT` unwind precommit side-effects symmetrically with
   precommit** — every action taken by `current_oxid_precommit()` should
   have a matching undo entry that the abort path consumes before
   `apply_undo_stack`. Today only the per-row modify undo is unwound.
3. **`START_CRIT_SECTION()` around the precommit → commit sequence** —
   escalate `ERROR` to `PANIC` between `current_oxid_precommit()` and
   `current_oxid_commit(csn)`, on the grounds that nothing reachable in
   that window has a sound rollback semantics. Matches the
   "post-XLogFlush" fix proposal from Bug #1 but earlier in the pipeline.

### Reproducer

**Solo (low rate, ~10–15 %)** — either of the two window-resident
injection points alone:

```bash
# Site 1 — orioledb-csn-incremented (~15 % at 30 s)
RR_STORAGE_ENGINE=orioledb RR_REPLICA_MODE=streaming \
RR_WRITERS=20 RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
RR_DURATION=30 RR_INJECTION_POINTS=orioledb-csn-incremented \
RR_ASSERT_FIRINGS=0 RR_KILL_POSTMASTER=0 RR_PANIC_FATAL=0 \
TIMEOUT=240 ./test/t/crash/run_hunt.sh 20 only_csn_incremented

# Site 2 — orioledb-set-csn-guarded (~10 % at 30 s)
RR_STORAGE_ENGINE=orioledb RR_REPLICA_MODE=streaming \
RR_WRITERS=20 RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
RR_DURATION=30 RR_INJECTION_POINTS=orioledb-set-csn-guarded \
RR_ASSERT_FIRINGS=0 RR_KILL_POSTMASTER=0 RR_PANIC_FATAL=0 \
TIMEOUT=240 ./test/t/crash/run_hunt.sh 20 only_set_csn_guarded
```

**Paired (near-deterministic, 60–100 %)** — combine site 1 with any of
the three amplifiers:

```bash
RR_INJECTION_POINTS=orioledb-csn-incremented,before-tx-commit             # 10/10 at 15 s
RR_INJECTION_POINTS=orioledb-csn-incremented,orioledb-set-xlog-ptr-guarded #  9/10 at 15 s
RR_INJECTION_POINTS=orioledb-csn-incremented,orioledb-add-finish-wal-guarded #  6/10 at 15 s
```

Expected fingerprint in either solo or paired form: trial reports
`BUG plan-OK` with the **replica-side invariant violation** (primary OK,
replica total differs). No PANIC, no TRAP, no `connection refused`.

---

## Companion docs

- [`recovery_overview.md`](./recovery_overview.md) — high-level recovery flow,
  including the `orioledb_redo` dispatch table where the assert sits
- [`recovery_investigation.md`](./recovery_investigation.md) §3.7
  ("Recovery transaction state — oxid, CSN, undo") — context for what the
  assert at `recovery.c:3712` is guarding
- [`tx_flow.md`](./tx_flow.md) — commit / abort control flow on the primary
  side; useful to identify the `orioledb-wal-flush-guarded` call site relative
  to `XLogFlush`
