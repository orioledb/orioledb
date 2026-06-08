# Checkpoint flow and the SK-leak — investigation report

This report consolidates a six-phase read-only investigation of the
PostgreSQL ↔ orioledb checkpoint flow, with the goal of identifying
where a recovery-side secondary-index (SK) desynchronization can be
introduced. The user-reproducible symptom, hunt data, and bug
fingerprints are documented in the companion file
[`sk_leak_issue.md`](./sk_leak_issue.md).

All line numbers refer to repositories on the host machine:
- PG: `/home/user/work/postgres/` (orioledb's `patches17_6` PG17 fork)
- Orioledb: `/home/user/work/orioledb/`

> **Maintenance note (2026-05-19):** line numbers were refreshed after a
> rebase. One substantive code change since the original investigation:
> `src/checkpoint/checkpoint.c:5105-5114` now reads
> `if (td->type >= oIndexUnique && XLogRecPtrIsInvalid(...))` — the
> early-bind branch widened from `oIndexRegular`-only to all
> `oIndexUnique` and above. This appears to be the implementation of
> the §6 patch hypothesis and narrows the bug window for the bank-account
> table; the analysis below describes the pre-fix state.

---

## 1. PostgreSQL checkpoint mechanism (background)

### Redo LSN — what recovery starts from

`CreateCheckPoint()` at `src/backend/access/transam/xlog.c:6875` computes
the "redo LSN" in two ways:

- **Shutdown checkpoint** (`xlog.c:6993-7028`): redo = current WAL insert
  position, rounded forward to the next page header.
- **Online checkpoint** (`xlog.c:7036-7058`): an explicit
  `XLOG_CHECKPOINT_REDO` record is inserted; its LSN becomes the redo
  point. This allows WAL writes to continue while the checkpoint runs.

The redo LSN is the earliest WAL position that recovery must replay to
reach a consistent state at the moment the checkpoint completes.

### Write order inside `CheckPointGuts()`

```
src/backend/access/transam/xlog.c:7530-7560

  CheckPointCLOG()           - xlog.c:7541
  CheckPointCommitTs()       - xlog.c:7542
  CheckPointSUBTRANS()       - xlog.c:7543
  CheckPointMultiXact()      - xlog.c:7544
  CheckPointPredicate()      - xlog.c:7545
  CheckPointBuffers(flags)   - xlog.c:7546   <- PG dirty buffers
  CheckPoint_hook(...)       - xlog.c:7549   <- ORIOLEDB INJECTED HERE
  ProcessSyncRequests()      - xlog.c:7554   <- global fsync barrier
  CheckPointTwoPhase(...)    - xlog.c:7559
```

### The consistency boundary

The checkpoint becomes durable only after `pg_control` is rewritten and
synced at `xlog.c:7259-7280`. Any crash before that point is recovered
from the *previous* checkpoint. After it, recovery starts at the new
redo LSN.

### Triggers and GUCs

Triggers (in `src/backend/postmaster/checkpointer.c`):

| Trigger | Site | Default |
|---|---|---|
| Time-based | `checkpointer.c:399-407` | `checkpoint_timeout = 300 s` |
| WAL volume | `xlog.c:2513` (`RequestCheckpoint(CHECKPOINT_CAUSE_XLOG)`) | `max_wal_size = 1 GB` |
| Explicit `CHECKPOINT` SQL | sets flags in shared memory, signals checkpointer | — |
| End-of-recovery | `CHECKPOINT_END_OF_RECOVERY` | once per startup |
| Shutdown | `CHECKPOINT_IS_SHUTDOWN` | once per stop |

Relevant GUCs (defaults in `src/backend/utils/misc/guc_tables.c`):

| GUC | Default | Notes |
|---|---|---|
| `checkpoint_timeout` | 300 s | upper bound between auto-checkpoints |
| `checkpoint_completion_target` | 0.9 | spread of buffer writes |
| `max_wal_size` | 1 GB | volume-based trigger threshold |
| `min_wal_size` | 80 MB | floor before WAL recycling |
| `checkpoint_warning` | 30 s | "checkpoints too close" warning |

---

## 2. PG → orioledb checkpoint callback bridge

Vanilla PG has no checkpoint hook. Orioledb's patchset adds **two** of
them (commit `c5c52adcc1` "Recovery and checkpointer hooks").

### Hook definitions (`src/include/access/xlog.h:314-320`)

```c
typedef void (*CheckPoint_hook_type)(XLogRecPtr checkPointRedo, int flags);
extern PGDLLIMPORT CheckPoint_hook_type CheckPoint_hook;

typedef void (*after_checkpoint_cleanup_hook_type)(XLogRecPtr checkPointRedo, int flags);
extern PGDLLIMPORT after_checkpoint_cleanup_hook_type after_checkpoint_cleanup_hook;
```

### Invocation sites in PG

- **`CheckPoint_hook`** at `xlog.c:7548-7549` — fires inside
  `CheckPointGuts()`, **after `CheckPointBuffers()` and before
  `ProcessSyncRequests()`**. Orioledb's writes therefore go through
  the same global fsync barrier as PG's.
- **`after_checkpoint_cleanup_hook`** at `xlog.c:7371` (normal
  checkpoint) and `xlog.c:6140` (StartupXLOG end-of-recovery). The
  end-of-recovery firing is particularly relevant for our bug: it gives
  orioledb a chance to do post-recovery cleanup, and a missing or buggy
  step there would directly leave SK in an inconsistent state.

### Registration site in orioledb

`src/orioledb.c:1249, 1259-1260`:

```c
next_CheckPoint_hook = CheckPoint_hook;
...
CheckPoint_hook                = o_perform_checkpoint;
after_checkpoint_cleanup_hook  = o_after_checkpoint_cleanup_hook;
```

The previous hook is saved so a chain is possible — but orioledb is
typically the only AM registering, so the chain has length 1.

### Semantics

- **Synchronous, blocking** — PG waits for the hook to return.
- **Single slot** — chained manually if more than one extension
  registers.
- **Called once per checkpoint** (no start/end pair).
- **Errors propagate** — if the hook `ereport(ERROR)`s, the entire
  PG checkpoint aborts; if it `PANIC`s (e.g. via `START_CRIT_SECTION`),
  the postmaster crashes.

### Sequence diagram

```
PG                                 orioledb
─────────────────────────────────  ──────────────────────────────
CreateCheckPoint()
  CheckPointGuts()
    CheckPointBuffers()  ──┐
    CheckPoint_hook ──────┼──→ o_perform_checkpoint(redo_pos, flags)
                          │      │
                          │      ├─ replayStartPtr = get_xlog_ptr()  [1392]
                          │      ├─ checkpoint_sys_trees(...)         [1399]
                          │      ├─ sysTreesStartPtr = get_xlog_ptr() [1407]
                          │      ├─ o_indices_foreach_oids(...)       [1414]
                          │      │    iterates PK→Unique→Toast→Regular order
                          │      ├─ toastConsistentPtr = ...           [1428]
                          │      │    (late-bound fallback; see below)
                          │      ├─ write_checkpoint_control(...)      [1517]
                          │      └─ release undo retention             [1546-1554]
                          │      ↓
    ProcessSyncRequests()─┘  (global fsync of orioledb extents + PG buffers)
    CheckPointTwoPhase()
  ↓ pg_control update [7259-7280] ← consistency boundary
  after_checkpoint_cleanup_hook ─→ o_after_checkpoint_cleanup_hook(...)
```

---

## 3. Orioledb checkpoint internals

### Entry and high-level flow — `src/checkpoint/checkpoint.c`

`o_perform_checkpoint(XLogRecPtr redo_pos, int flags)` at line 1306 runs:

```
line 1366  toastConsistentPtr = InvalidXLogRecPtr        (reset)
line 1392  replayStartPtr = get_checkpoint_xlog_ptr()    (LSN_A)
line 1393  wait_finish_active_commits(replayStartPtr)
line 1399  checkpoint_sys_trees(...)                     (system trees)
line 1407  sysTreesStartPtr = get_checkpoint_xlog_ptr()  (LSN_B)
line 1414  o_indices_foreach_oids(checkpoint_tables_callback, ...)
              ├─ for each table:
              │    for each index in (PK, Unique, Toast, Regular) order:
              │       checkpoint_btree_loop(...)   ─ depth-first post-order
              │          ├─ walk down to leaves
              │          ├─ write each dirty leaf via CoW (src/btree/io.c)
              │          └─ walk up, write internal nodes
              └─ if td->type >= oIndexUnique AND toastConsistentPtr invalid:
                   toastConsistentPtr = get_xlog_ptr()    (line 5114)
                   checkpoint_write_pending_sk_fixups()    (PendingSkFixup snapshot)
line 1426  if toastConsistentPtr still invalid:
              toastConsistentPtr = get_checkpoint_xlog_ptr()   (LSN_C, line 1428)
              checkpoint_write_pending_sk_fixups()
line 1517  write_checkpoint_control(&control)
              control.replayStartPtr = LSN_A
              control.toastConsistentPtr = LSN_C
line 1551  pg_atomic_write_u64(&undo_meta->checkpointRetainStartLocation, ...)
              release undo retention from prior checkpoint
```

### Copy-on-write page mechanism

Per-page CoW in `src/btree/io.c`:

- New disk extent allocated via `get_free_disk_extent()` (around
  `io.c:1060`).
- Page header `o_header.checkpointNum` is bumped to the current
  checkpoint number BEFORE writing the new copy (`io.c:1789-1792`).
- Page is written to the new disk extent (`io.c:1957`).
- There is **no WAL record for the CoW swap itself**. The
  `checkpointNum` page-header field is the durable marker.

### Tree-walk order (per tree)

`checkpoint_btree_loop()` at `src/checkpoint/checkpoint.c:3579-3934`
does depth-first **post-order**: leaves are written first, then their
parents up to the root. This ensures referential integrity within one
tree — internal nodes always point to already-written leaves.

### Inter-tree order — PK before SK

`o_indices_foreach_oids` (`src/catalog/o_indices.c:1648-1708`) iterates indices
in ascending sort order over the `SYS_TREES_O_INDICES` system tree. The
keying is by `(datoid, reloid, relnode, type, ...)`, and the type enum
in `include/orioledb.h:176-182` is:

```c
oIndexInvalid = 0, oIndexToast = 1, oIndexBridge = 2,
oIndexPrimary = 3, oIndexUnique = 4, oIndexRegular = 5,
oIndexExclusion = 6,
```

So for the bank-account test (PK + one `UNIQUE(token)` SK), the order
within one table is **PK (type=3) → Unique SK (type=4)**.

### Checkpoint LSN binding — two LSNs, two roles

- `replayStartPtr` (captured at line 1392) is where **recovery starts
  replaying WAL** from. It is set *before* any tree is flushed, so any
  modification recorded in WAL after this point may not be reflected in
  the about-to-be-flushed pages.
- `toastConsistentPtr` is what gates the recovery's **whole-table vs
  per-tree apply mode**. Set in one of two places:
  - `line 5114`: when the **first `oIndexUnique`-or-higher** index is
    reached in the per-table loop (post-rebase condition; was
    `oIndexRegular`-only previously).
  - `line 1428` (late fallback): **after the loop returns**, if no
    qualifying SK was seen.

**Important for our test (pre-fix interpretation):** the bank-account
table has no TOASTed fields and no `oIndexRegular`. Under the old
`td->type == oIndexRegular` condition, line 5114 never fired, and
`toastConsistentPtr` was captured by the late fallback at line 1428 —
**after the SK leaf-flush was complete**. Under the new
`td->type >= oIndexUnique` condition the early-bind site fires at the
first SK, capturing `toastConsistentPtr` **before** the SK leaf-flush
— closing the race window analysed in §6.

### Undo retention release

After the control file is durable (line 1517), the prior checkpoint's
undo retention window is released (line 1546-1554). The current
checkpoint's window stays retained until the *next* checkpoint
completes. This is consistent with the role of undo in recovering aborts
across crash boundaries.

The retention window is enforced **per `UndoLogType`** — orioledb has three
global ring buffers (`UndoLogRegular`, `UndoLogRegularPageLevel`,
`UndoLogSystem`) and each has its own `checkpointRetainStartLocation` /
`checkpointRetainEndLocation` pair in `UndoMeta`. Page-level structural undo
(`UndoLogRegularPageLevel`) is what protects mid-merge / mid-split pages
from being prematurely garbage-collected before redo can replay them.
See [`recovery_investigation.md`](./recovery_investigation.md) §7.0 for the
ring-buffer / per-tuple back-pointer / per-transaction stack model, and
[`recovery_undo_qa.md`](./recovery_undo_qa.md) Q3 for the same in Q&A form.

---

## 4. Periodic checkpoint scheduling

Orioledb does **not** spawn a dedicated checkpoint worker. PG's
`checkpointer` process drives the entire flow — it decides cadence,
calls `CreateCheckPoint`, and that in turn calls
`CheckPoint_hook → o_perform_checkpoint`.

### Orioledb's workers

| Worker | File | Role |
|---|---|---|
| Background writer | `src/workers/bgwriter.c:65` | page eviction, dirty-buffer flushing, undo file eviction |
| Recovery worker | `src/recovery/worker.c:200` | dynamic, started during startup |
| Rewind worker | `src/rewind/rewind.c:1098` | point-in-time rewind |
| S3 worker | `src/s3/worker.c:905` | S3 archival |

None of these run checkpoints. The bgwriter polls at `BgWriterDelay`
(default 200 ms) to evict pages under memory pressure, but does not
issue checkpoints.

### Orioledb GUCs affecting checkpoints

- `orioledb.remove_old_checkpoint_files` (bool, default true) —
  `src/orioledb.c:651`.
- `orioledb.debug_checkpoint_timeout` (int, default = `CheckPointTimeout`)
  — `src/orioledb.c:740`. Test-only override that allows sub-30-second
  intervals.
- `orioledb.checkpoint_completion_ratio` (real, default 0.5) —
  `src/orioledb.c:757`. Throttles orioledb's work to a fraction of PG's
  checkpoint budget.

No GUC controls checkpoint *frequency* directly; all cadence is
inherited from PG.

---

## 5. The test's `checkpointer_loop` thread

In `test/t/crash/rr_stress_test.py`:

```python
# line 87
checkpoint_interval = 0.5

# lines 397-417
def checkpointer_loop():
    con = node.connect()
    local_cp = 0
    try:
        while not stop.is_set():
            try:
                con.execute("CHECKPOINT")
                con.commit()
                local_cp += 1
            except Exception:
                try: con.rollback()
                except Exception: pass
            if stop.wait(checkpoint_interval):
                break
    finally:
        try: con.close()
        except Exception: pass
```

A dedicated thread issues `CHECKPOINT` SQL every 500 ms over a long-lived
connection, alongside writers, readers, and chaos threads.

### What this means quantitatively

- In a 15-second trial → ~30 checkpoints per trial (vs the PG default of
  one every 300 s, i.e. zero auto-checkpoints in a trial).
- With `RR_ASSERT_FIRINGS=3` over 15 s → an injection fires every ~3.75
  s. So roughly **7-8 checkpoints land between two injection firings**.
- Workload: writers commit at 70-250 commits/s in the observed hunts.
  So between two checkpoints, ~35-125 transactions commit.

### Recommended frequency adjustments — a tuning experiment

The bug has been observed at the current 0.5 s cadence. The fact that
each checkpoint covers only ~35-125 commits means **most of those
commits land *between* checkpoints, not inside one** — which is fine for
WAL but means few commits are concurrent with a running checkpoint.

The race window we identified in Phase 6 lives between
`replayStartPtr` and `toastConsistentPtr` — i.e. it scales with the
*duration of one checkpoint*. Wider checkpoints → more concurrent
commits → larger replay window for them on recovery.

**Suggested experiments (independent, do them as a small grid):**

| Variant | Rationale | Hypothesis |
|---|---|---|
| `checkpoint_interval = 0.1` | More frequent, smaller per-checkpoint windows | Bug rate should *decrease* if the race scales with checkpoint duration |
| `checkpoint_interval = 2.0` | Fewer, longer checkpoints, more dirty pages per CP | Bug rate should *increase* — more commits between PK-flush and `toastConsistentPtr` capture |
| Remove the loop entirely | Lean on PG's natural cadence (which is essentially never within a 15 s trial) | Bug rate should drop towards zero — fewer races |
| `checkpoint_interval = 0.05` | Stress checkpoint-overlap with writes | May saturate the system; useful only briefly |

Run each variant for 30 trials and compare rates. The shape of the
result will indicate whether the race window is per-checkpoint
(0.1 → drops, 2.0 → rises) or commit-rate driven (no change with
interval).

---

## 6. PK/SK consistency vs WAL/undo — the bug-explanation candidate

This is where the investigation pays off.

### Fact 1 — User-table writes WAL only for the PK

In `src/tableam/operations.c`:

- INSERT: `o_wal_insert(&primary->desc, tup, ...)` at lines 468, 1058
  (plus a bridge-index variant at line 334).
- UPDATE: `o_wal_update(&primary->desc, final_tup, oldSlot, ...)` at
  line 1429.
- DELETE: `o_wal_delete(&primary->desc, primary_tuple, ...)` at line
  1533.

All three pass `&primary->desc` — i.e. only the PK tree's identity
goes into WAL. **There is no separate WAL record for each SK**. The SK
state is reconstructed at recovery time by *deriving* the SK tuples
from the PK tuple stored in the WAL record.

### Fact 2 — Recovery has two apply modes, gated by `toast_consistent`

In `src/recovery/worker.c:671-692`, `apply_modify_record` branches:

```c
if (descr && toast_consistent)
    apply_tbl_modify_record(descr, type, p, oxid, COMMITSEQNO_INPROGRESS);
else
    apply_btree_modify_record(&id->desc, type, p, oxid, COMMITSEQNO_INPROGRESS);
```

Comment at lines 678-682 spells out the intent:

> Don't apply changes to secondary indices before TOAST is consistent.
> Otherwise, values of secondary indices on TOASTed fields can be
> invalid.

- **`apply_tbl_modify_record`** (`worker.c:796+`) — iterates **all
  indices** of the table (PK + every SK) and applies the modification
  to each. This is the "whole-table" mode.
- **`apply_btree_modify_record`** (`recovery.c:2159+`) — touches **only
  the one btree** that the WAL record identifies. Used for PK-only
  records during the pre-TOAST-consistent phase.

### Fact 3 — `toast_consistent` flips at `controlToastConsistentPtr`

In `src/recovery/recovery.c:1171-1190`:

```c
if (record->ReadRecPtr >= checkpoint_state->controlToastConsistentPtr
    && !toast_consistent)
{
    /*
     * Before running the PK->SK fix-up pass we need every pre-toast
     * WAL record to have been applied to PK by the workers ...
     */
    if (!recovery_single)
        workers_synchronize(record->ReadRecPtr, true);
    apply_pending_sk_fixups();   /* post-fix addition */
    toast_consistent = true;
    if (!recovery_single)
        workers_notify_toast_consistent();
}
```

**Post-fix note (2026-05-19):** the `workers_synchronize` +
`apply_pending_sk_fixups()` pair is new since the original analysis.
On the gate-open transition, recovery now: (1) drains all workers up
to `ReadRecPtr` to make the PK state authoritative, (2) replays the
`PendingSkFixup` snapshot taken at checkpoint time (see
`checkpoint_write_pending_sk_fixups()` calls at checkpoint.c:5114 and
1428), and only then (3) flips `toast_consistent`. This pass closes
the §6 bug window directly.

Until the LSN of the record being replayed crosses
`controlToastConsistentPtr`, `toast_consistent` is false → per-btree
apply mode → **SK never gets updated by replay**.

### Fact 4 — For our test, the gate window covers the full checkpoint

For the bank-account table (PK + one `UNIQUE` SK, no
`oIndexRegular`/`oIndexToast`):

- `replayStartPtr` is captured at line 1392 (start of `o_perform_checkpoint`)
- `toastConsistentPtr` is captured at line 1428 (end of the
  per-table loop) **in the pre-fix code path**.
- The window `[replayStartPtr, toastConsistentPtr]` therefore spans the
  **entire checkpoint duration**.

**Post-fix note (2026-05-19):** with the `td->type >= oIndexUnique`
condition now at `checkpoint.c:5105`, the early-bind site at line 5114
fires when the first `oIndexUnique` SK is reached. For the bank-account
test that's the `o_bank_account_token_uniq` index — captured **before**
its leaf-flush. The race window now spans only the PK-flush phase, not
the full checkpoint.

### Assembling the race

The CoW capture of each B-tree happens at one specific moment during
the checkpoint loop:

- PK page visited / CoW'd at `T_PK`
- SK page visited / CoW'd at `T_SK`, with `T_PK < T_SK`

Modifications committed at LSN `T`:

| `T` relative to `T_PK`, `T_SK` | PK CoW image | SK CoW image | Replay window | Replay action | Resulting state |
|---|---|---|---|---|---|
| `T < T_PK < T_SK` | reflects `T` | reflects `T` | yes (per-btree) | applies to PK only (PK already has it; idempotent), SK already has it | **consistent** |
| `T_PK < T < T_SK` | does NOT reflect `T` | reflects `T` | yes (per-btree) | applies to PK only → PK gets `T`, SK already has `T` | **consistent** |
| `T_PK < T_SK < T` (still in gate window) | does NOT reflect | does NOT reflect | yes (per-btree) | applies to PK only → PK gets `T`, **SK does NOT** | **SK missing** |
| `T > toastConsistentPtr` | does NOT reflect | does NOT reflect | yes (whole-table) | PK and SK both updated | **consistent** |

The third row is the bug window. Any user-table commit whose LSN falls
between `T_SK` and `toastConsistentPtr` will:

1. Have its modification flushed into neither the PK CoW image nor
   the SK CoW image (both were captured before this commit).
2. Be replayed at recovery via `apply_btree_modify_record(PK, ...)`.
3. Update the PK only; the SK is left in its CoW state.

This produces exactly the **"SK missing"** fingerprint we see (e.g.
trial 13 of the recent hunt: `pk_set\sk_set = [15]`).

### Why we also see "SK extra"

The "SK extra" fingerprint (trial 18: `sk_set\pk_set = [79, 95]`) is
symmetric:

- A row's SK entry for token `X` is present in the SK CoW image because
  the SK page was flushed *before* a tx removed `X` from that row.
- The tx that removed `X` (and added `Y` in its place) commits at LSN
  `T` with `T_SK < T < toastConsistentPtr`.
- At recovery, PK is updated from token `X` to token `Y`; the SK is
  not touched → SK still has the stale `X` entry. PK no longer has any
  row with token `X` → **SK extra** for `X`.

### Where the consistency story rhymes with the data

- **PK clean in every failure** — explained: PK is always replayed
  (whether `toast_consistent` is true or false), and the bank-account
  workload writes WAL for the PK on every modification.
- **SK off by small numbers (1-4)** — explained: each leaked token
  corresponds to one tx whose LSN landed in the gate window's hot zone.
  At our 0.5 s cadence with ~70-250 commits/s and a per-checkpoint
  window measured in tens of milliseconds, 1-4 hits per failed trial
  matches order-of-magnitude.
- **Symmetric leak directions** — explained: depending on whether the
  SK page was visited before or after the offending commit's
  delete-old/insert-new pair, you get "missing new" or "extra old".

### What this is NOT (yet) a proof of

This is a strong hypothesis grounded in the code paths and the data, but
it has not been verified empirically. The cleanest empirical test is
the Phase 5 frequency experiment (rate should rise with longer
checkpoint intervals). The cleanest source-level test is to
**move the `toastConsistentPtr = get_checkpoint_xlog_ptr()` from line
1428 forward to right after line 1392** (i.e. capture it at the start
of `o_perform_checkpoint`, not the end) and rerun. If the bug rate
drops to ~zero, the hypothesis is confirmed.

**Post-fix note (2026-05-19):** the applied fix is structurally close
to the above patch hypothesis but not identical. Instead of moving the
late-fallback assignment outright, the early-bind condition at
`checkpoint.c:5105` was widened from `oIndexRegular`-only to
`>= oIndexUnique`. This captures `toastConsistentPtr` at the first SK
visit for the bank-account table — well before the SK leaf-flush
— and pairs with a new `apply_pending_sk_fixups()` recovery pass that
replays the in-flight rows that the boundary cut across. See `Top
suspect code paths` below for the post-fix references.

---

## Top suspect code paths

1. **`src/checkpoint/checkpoint.c:1428`** — late fallback for
   `toastConsistentPtr`. Captures the LSN *after* the entire per-table
   index loop returns. In the **pre-fix** code this fired for tables
   with only `oIndexUnique` SKs (early-bind condition was
   `oIndexRegular`-only at the old equivalent of line 5114), widening
   the recovery gate window to the full checkpoint duration. **Post-fix
   note (2026-05-19):** the condition at `checkpoint.c:5105` is now
   `td->type >= oIndexUnique`, so the early-bind site at line 5114
   fires for `oIndexUnique` SKs too — late fallback now only fires for
   tables with PK-only schemas.

2. **`src/recovery/worker.c:683-692`** — the global `toast_consistent`
   flag that suppresses SK updates during PK-only replay. The check is
   per-process (one flag for all tables) rather than per-table, so a
   table with no TOAST inherits the conservatism of a table with TOAST.

3. **`src/recovery/recovery.c:1171-1190`** — the LSN comparison that
   flips `toast_consistent` to true. The comparison is global and uses
   `controlToastConsistentPtr`, which was the wide-window value from
   suspect #1 for tables like ours. **Post-fix note:** the same block
   now calls `workers_synchronize` and `apply_pending_sk_fixups()`
   before the flip, replaying any SK changes that were in flight at the
   checkpoint cut.

The original patch hypothesis (set `toastConsistentPtr` to
`replayStartPtr` immediately at `o_perform_checkpoint:1392`,
unconditionally) was the intuition behind the fix that was actually
applied — namely, widening the early-bind condition at
`checkpoint.c:5105` from `oIndexRegular` to `>= oIndexUnique` plus the
new `apply_pending_sk_fixups` recovery pass.

---

## Tracing-log placement recommendations

Ranked list of points where adding `elog(LOG, ...)` markers would best
illuminate the SK-leak race during bank-account trials. Each entry says
what the log should print and what it would reveal.

### Tier 1 — directly bracket the race window

1. **`src/checkpoint/checkpoint.c:1392`** —
   `elog(LOG, "[chkp] start cp=%u replayStartPtr=%X/%X", cur_chkp_num,
   LSN_FORMAT_ARGS(checkpoint_state->replayStartPtr));`
   Reveals **LSN_A** for each checkpoint.

2. **`src/checkpoint/checkpoint.c:1428`** (right after the
   assignment) —
   `elog(LOG, "[chkp] toastConsistentPtr=%X/%X (late-bound)",
   LSN_FORMAT_ARGS(checkpoint_state->toastConsistentPtr));`
   Reveals **LSN_C**. The width of `[LSN_A, LSN_C]` is the size of the
   race window per checkpoint. **Post-fix:** this only fires for
   PK-only schemas; for the bank-account test the early-bind at line
   5114 fires first.

3. **`src/checkpoint/checkpoint.c:5114`** —
   `elog(LOG, "[chkp] toastConsistentPtr=%X/%X (early, oIndexUnique+)",
   ...);`
   **Post-fix:** under `td->type >= oIndexUnique` this DOES fire for
   the bank-account `UNIQUE(token)` SK. The LSN captured here is the
   one written to `controlToastConsistentPtr`.

4. **`src/recovery/recovery.c:1171-1190`** (after `toast_consistent =
   true;`) —
   `elog(LOG, "[recv] toast_consistent flipped at ReadRecPtr=%X/%X
   (gate=%X/%X)", LSN_FORMAT_ARGS(record->ReadRecPtr),
   LSN_FORMAT_ARGS(checkpoint_state->controlToastConsistentPtr));`
   Confirms when the gate opens during replay.

### Tier 2 — bracket per-tree CoW captures

5. **`src/checkpoint/checkpoint.c` inside `checkpoint_tables_callback`**
   (around the per-tree iteration) — before entering each tree's flush:
   `elog(LOG, "[chkp] tree begin type=%d datoid=%u reloid=%u relnode=%u
   xlog_ptr=%X/%X", td->type, td->oids.datoid, td->oids.reloid,
   td->oids.relnode, LSN_FORMAT_ARGS(get_checkpoint_xlog_ptr()));`
   Yields `T_PK` and `T_SK` for our table. Cross-correlated with Tier 1
   logs and with the per-tx WAL LSNs, the bug window becomes exactly
   countable.

6. **`src/btree/io.c:1957`** (just before `write_page_to_disk()`) —
   `elog(LOG, "[chkp] flush type=%d blk=%u extent=... cp=%u",
   tree->type, blkno, ..., checkpoint_number);`
   Page-level resolution of the CoW captures. Cheap if compiled out
   when not needed.

### Tier 3 — bracket the apply-decision in recovery

7. **`src/recovery/worker.c:683`** (the if-else branch) —
   `elog(LOG, "[recv] apply tree=%d toast_cons=%d", id->desc.type,
   toast_consistent);`
   Confirms which path each per-record apply took during the trial.
   If we see `tree=4 (oIndexUnique) toast_cons=0` for our bank-account
   table replay, that's the pre-fix smoking-gun emission. **Post-fix:**
   under the new early-bind, expect `tree=4 toast_cons=0` for records
   strictly before `controlToastConsistentPtr` and `toast_cons=1`
   thereafter — with PK and SK both updated on the latter.

8. **`src/recovery/recovery.c:1191`** (around `replay_container`) — log
   when a container is skipped (LSN < `controlReplayStartPtr`) vs
   replayed. Useful for ruling out incorrect record skipping.

### Tier 4 — commit-emission side

9. **`src/tableam/operations.c:468, 1429, 1533`** — log the LSN at
   which the PK WAL record is emitted, along with the affected token
   value. Lets us correlate a buggy trial's leak set (e.g. token 15)
   to the specific commit's LSN, then back to the checkpoint window it
   landed in.

10. **`src/transam/undo.c` around `current_oxid_precommit` and
    `current_oxid_commit`** — log when each transaction's CSN moves
    through `INPROGRESS → CSN_COMMITTING → final`. Complements
    point 9 by surfacing the commit-time window relative to checkpoints.

### Use sparingly

Per the CLAUDE.md note ("Why instrumentation drops bug repro rate"),
even two extra `elog`s on a hot path can move the bug rate by an order
of magnitude. Recommended approach:

- Enable **Tier 1** for *every* trial — those are off the hot path
  (one per checkpoint) and give the race window directly.
- Enable **Tier 2 and 3** selectively (e.g. only when chasing a
  specific token leak); compile-out via a `#define` toggle.
- Treat **Tier 4** as one-shot: enable, run a few trials, capture a
  buggy trial's log, disable. The hot path emissions will move the
  rate.

---

## Appendix: open questions for future work

- The Phase 3 sub-agent originally claimed `toastConsistentPtr` is set
  when the first `oIndexRegular` *or* SK index is reached. Under the
  pre-fix code that was inaccurate — the equivalent of line 5114
  explicitly checked `td->type == oIndexRegular` (not `oIndexUnique`),
  so for a `UNIQUE`-only table the early-bind never fired and the
  late fallback at the equivalent of line 1428 took over.
  **Post-fix note (2026-05-19):** `src/checkpoint/checkpoint.c:5105`
  now reads `td->type >= oIndexUnique`, which retroactively validates
  the Phase 3 claim — for the bank-account `UNIQUE(token)` table, the
  early-bind at line 5114 fires. The original "report corrects" line
  is therefore obsolete; left here for historical context.
- Whether a `UNIQUE NOT DEFERRABLE` constraint changes the index type
  (still `oIndexUnique`, just non-deferrable) and therefore the
  flush-loop position is worth confirming, but probably doesn't affect
  the recovery gate.
- The bridge-index type (`oIndexBridge = 2`) is part of the
  `apply_btree_modify_record` PK-side branch. If your test ever ends up
  with a bridge index, audit that branch too.
