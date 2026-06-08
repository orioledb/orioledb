# Recovery livelock: the standby regresses `globalXmin` below `writtenXmin`, so a committed-and-FROZEN oxid is mis-read as `IN_PROGRESS`, stranding parallel-recovery workers

**Status: ROOT CAUSE CONFIRMED** (trial caught 2026-06-05, `CAUGHT_noinj_replica.log`, 6.4 GB). Direct replica-side instrumentation captured the cause, the effect, the 1 ms causal ordering, and the raw slot value. See §5.
**Branch:** `add_stress_bank_account_test`
**Mode:** streaming standby, parallel recovery. **Injection-independent** (reproduces under plain `SIGKILL` of the primary postmaster, no injection point attached).
**Symptom:** the replica's parallel-recovery workers spin forever in `o_btree_modify_handle_conflicts()`; the replica never finishes replay and never shuts down.

> **Note on naming.** The original title/§2 framing ("a committed oxid's CSN slot *reverts* to INPROGRESS") turned out to be **wrong**: the slot is **never** rewritten. It holds `COMMITSEQNO_FROZEN` the whole time. What changes is the **horizon under it** — the standby's `globalXmin` regresses below `writtenXmin`, which disables the `oxid < globalXmin ⇒ FROZEN` fast-path and forces a *slot read* that maps the FROZEN special value to `IN_PROGRESS`. The "revert/clobber" hypothesis is **refuted** (§4, §5). Earlier sections below are kept for the investigation trail but are superseded by §5.

---

## 1. Symptom

After the primary is `SIGKILL`ed and the streaming standby resumes replay, one or more parallel-recovery workers enter an unbounded busy-spin inside:

```
o_btree_modify_handle_conflicts()   (src/btree/modify.c:435)
```

In the caught trial the spin counter reached **4,972,218** iterations and the replica's log grew to **6.5 GB** while making zero forward progress; the replica would not shut down. The two spinning workers were:

```
pid=3997315  blkno=2050  opOxid=754  conflictOxid=610
pid=3997316  blkno=2050  opOxid=750  conflictOxid=616
```

Each worker is trying to apply a modify whose key conflicts with an in-row `conflictOxid` (610, 616). It reads that conflicting oxid's commit state, sees `INPROGRESS`, and — because the recovery conflict handler waits with `waitCallback == NULL` — re-reads in a tight loop forever instead of blocking.

## 2. What the conflicting oxids actually are

The decisive question was the lifecycle of `conflictOxid` 610 / 616. Instrumentation (`finished-push`, leader `finished-drain`, and a per-result `oxid_get_csn` exit trace) shows:

```
oxid 610: finished-push path=commit-defer worker=-1 ptr=0/30189D0
          leader drain=1   (FINALIZED by the leader drain)
          oxid_get_csn →  result_csn=0   x 4,988,531   (INPROGRESS)
                          result_csn=461  x 3          (its real, committed CSN)
                          result=FROZEN   x 4

oxid 616: finished-push path=commit-defer worker=-1 ptr=0/3019040
          leader drain=1   (FINALIZED)
          oxid_get_csn →  result_csn=0   x 4,956,720   (INPROGRESS)
                          result=FROZEN   x 4
```

So both conflicting oxids:

1. **committed** (they entered the deferred-commit path: `finished-push path=commit-defer`),
2. were **finalized** by the leader drain (`drain=1`; for 610 its assigned CSN **461** was observed read 3 times),
3. were then read as **`INPROGRESS` (csn=0)** ~5 million times — which is the spin, and
4. were *also* read as **`FROZEN`** a handful of times.

This rules out the "never finalized" hypothesis: the oxid **was** finalized. The `result_csn=0` reads were originally (mis)interpreted as a "finalize-then-revert". **§5 corrects this:** the slot is never rewritten — it holds `COMMITSEQNO_FROZEN (0x3)`, and the `result_csn=0` (`IN_PROGRESS`) reads are the FROZEN slot being mis-mapped after `globalXmin` regressed below the oxid. The `result=FROZEN` reads are the *fast-path* (`oxid < globalXmin`) firing in the brief window before the regression; the `result_csn=0` reads are the slot-read path firing after it.

## 3. The deadlock topology (why a single reverted slot freezes the whole replica)

```
worker A (pid 3997315) ──spins on──▶ oxid 610  (committed; FROZEN slot mis-read as IN_PROGRESS)
worker B (pid 3997316) ──spins on──▶ oxid 616  (committed; FROZEN slot mis-read as IN_PROGRESS)
        │                                  ▲
        │ spinning workers never advance    │ finalize happens only in the
        │ their commitPtr                    │ leader drain, bounded by listPtr
        ▼                                    │
  get_workers_commit_ptr() = MIN(commitPtr) │
        = listPtr  ── FROZEN ───────────────┘
        ▲
        │ leader drain is itself stopped on the aborted oxid 676
        │   (csn=2 = ABORTED, state_ptr=0/301F066 > listPtr=0/301F000)
```

- The leader finalizes deferred oxids in `update_proc_retain_undo_location()`'s drain loop **only up to `listPtr = get_workers_commit_ptr()`**, the MIN of all workers' `commitPtr`.
- A worker that is busy-spinning on a modify **never advances its `commitPtr`**, so `listPtr` is pinned.
- The leader's drain is independently parked on an **aborted** oxid (676, `csn=2`) whose `state_ptr` sits *beyond* `listPtr`.
- Net effect: a circular wait. The workers wait for a CSN that only the leader can publish; the leader's publish horizon is pinned by the very workers that are stuck. No process makes progress; the replica livelocks.

## 4. What we have *ruled out*

| Hypothesis | Verdict | Evidence |
|---|---|---|
| oxid **reuse** (same number, different txn) | **ruled out** | generation counter `gen=0` throughout; max backward oxid jump = 258 ≪ a reset (~3600). No incarnation boundary crossed. |
| CSN-slot **collision** (two oxids share a slot) | **ruled out** | `xid_circular_buffer_size = 524288`; max oxid in trial ~3618 → `oxid % size` cannot collide. |
| **commit-then-rollback** of the same txn | **ruled out** | the stuck oxids are on `path=commit-defer` and were finalized to a *normal* CSN (610→461); they are committed, not aborted. |
| Injection-point artifact | **ruled out** | reproduced with `RR_INJECTION_POINTS=NONE`, plain `SIGKILL`. |
| **`NORMAL → INPROGRESS`** slot overwrite (`set_oxid_csn` / `advance_oxids`) | **ruled out** | CLOBBER trace fired 0 times; and the decisive spin-site trace shows `rawSlotCsn=3` (`FROZEN`) on every spinning worker — the slot is **never** rewritten to `INPROGRESS` (§5). |
| **any slot rewrite / "revert"** (the original hypothesis) | **ruled out** | at the spin site the raw `xidBuffer` slot reads `COMMITSEQNO_FROZEN (0x3)`, not `INPROGRESS (0x0)`. The slot is correct; the *horizon* is wrong (§5). |

## 5. CONFIRMED mechanism — `globalXmin` regresses below `writtenXmin`; a FROZEN slot is read as `IN_PROGRESS`

A spin-site trace was added at the exact point a recovery worker reads `conflictOxid` as in-progress (`o_btree_modify_handle_conflicts`, `src/btree/modify.c`): it logs the **raw `xidBuffer` slot value** plus `globalXmin`/`writtenXmin` via the `oxid_debug_raw_slot()` accessor (`src/transam/oxid.c`). A trial caught on 2026-06-05 (`CAUGHT_noinj_replica.log`, 6.4 GB, 5,109,500 spins) captured the whole chain **on the replica**.

**The cause — the replica's own `update_run_xmin()` writes `globalXmin` backward, below `writtenXmin`:**
```
17:35:48.992 [8513] runxmin-backward func=update_run_xmin
             old_globalXmin=352 → new_globalXmin=248  writtenXmin=352  nextXid=468  recovery_xmin=468  queueEmpty=0  belowWritten=1
```
`queueEmpty=0` ⇒ the backward value came from the `xmin_queue`'s oldest entry (a committed/frozen oxid re-entered the in-flight set and dragged the horizon back), **not** the `nextXid` fallback.

**The effect — 1 ms later, every spinning worker reads a FROZEN slot and gets `IN_PROGRESS`:**
```
17:35:48.993 [8516] conflict-inprogress opOxid=378 conflictOxid=320 rawSlotCsn=3 globalXmin=248 writtenXmin=352 oxidGEglobalXmin=1 belowWritten=1
17:35:48.994 [8517] conflict-inprogress opOxid=427 conflictOxid=337 rawSlotCsn=3 globalXmin=248 writtenXmin=352 oxidGEglobalXmin=1 belowWritten=1
17:35:48.998 [8518] conflict-inprogress opOxid=469 conflictOxid=351 rawSlotCsn=3 globalXmin=248 writtenXmin=352 oxidGEglobalXmin=1 belowWritten=1
```

Every field corroborates the mechanism:

- **`rawSlotCsn=3` = `COMMITSEQNO_FROZEN`** on *all* spinners. The slot is **never** rewritten to `INPROGRESS (0x0)` — it holds the legitimate `FROZEN` value `advance_global_xmin` stamped when the oxid committed. **No clobber, no revert.**
- **`globalXmin=248`** equals the value the regression just wrote (352→248), with **1 ms causal ordering** (`.992` regress → `.993` first spin).
- conflictOxids **320 / 337 / 351 all lie in the band `[248, 352)`** = the re-exposed frozen region:
  - `≥ globalXmin (248)` ⇒ `oxidGEglobalXmin=1` ⇒ the `oxid < globalXmin ⇒ FROZEN` fast-path in `oxid_get_csn` is **skipped**;
  - `< writtenXmin (352)` ⇒ `belowWritten=1` ⇒ the slot is already frozen.

So `oxid_get_csn(320)` skips its fast-path, reads the slot (`FROZEN`, a *special* value), maps the special value to **`IN_PROGRESS`**, and the worker spins forever. It is not one oxid but the **entire `[248,352)` band** of committed-and-frozen oxids mis-resolved at once (here, 3 workers on 3 oxids).

**Why the standby can regress at all (the two opposed guards):**
- steady-state `advance_global_xmin()` is monotonic — `if (globalXmin > prevGlobalXmin)` (`src/transam/oxid.c:1166`), never backward;
- recovery `update_run_xmin()` / `free_run_xmin()` use the **reversed** guard `if (xmin < globalXmin) write(xmin)` (`src/recovery/recovery.c:2533`, `:2545`) with **no `writtenXmin` floor**, so they can shove `globalXmin` back into the already-frozen region.

**Correlation check:** in the same hunt, clean trials and the known-extent-leak trials all had replica `runxmin-backward = 0`; only the livelock trial had replica `runxmin-backward = 1` (352→248). **Replica `globalXmin`-below-`writtenXmin` regression ⟺ livelock.**

> The earlier "the slot is re-initialized FROZEN→INPROGRESS by `advance_oxids`" guess (and the whole "revert/clobber" line) is **refuted**: the slot stays FROZEN; `advance_oxids` never touches it (it would need `nextXid` to regress, which never happens — see the parent investigation). The single moving part is `globalXmin`.

## 6. The watermark-increment logic and the upstream cause (Layer B)

§5 proves the *downstream* fault (`globalXmin < writtenXmin` ⇒ FROZEN slot read as `IN_PROGRESS`). This section explains **how the bad horizon write is produced** in the first place.

### How the `*Xmin` watermarks normally increment

The scheme is **min-based and follows-up-only**:

- **`globalXmin` = the minimum still-needed oxid.** `advance_global_xmin` computes `globalXmin = min(runXmin, every backend proc xmin)` and writes it **only if it grew** — `if (globalXmin > prevGlobalXmin)` (`oxid.c:1166`). `runXmin = min(xmin_queue top, recovery_xmin)`.
- **`writtenXmin` chases `globalXmin` upward.** When `globalXmin` advances, the page-out block (`oxid.c:1175-1199`) fills `[writtenXmin, globalXmin)` with `COMMITSEQNO_FROZEN`, pages those slots to the `.xidmap`, and sets **`writtenXmin = globalXmin`**.

So by construction `writtenXmin ≤ globalXmin` *always* (`writtenXmin` is *assigned* `globalXmin`), which is why `advance_global_xmin` can `Assert(globalXmin >= writtenXmin)` (`oxid.c:1181`).

**Load-bearing assumption:** *once the horizon passes oxid X, no oxid `< X` ever rejoins the unfinished set.* True in normal operation (oxids are allocated monotonically; a finished oxid stays finished), so the min — and thus the watermark — only moves forward.

### What violates it: a *deferred* crash-rollback whose stamped xmin races ahead of its oxid

The assumption is broken by OrioleDB's **eager WAL + deferred crash-rollback** path:

1. A transaction's row changes are WAL-logged *before* commit. When the primary is `SIGKILL`ed, its in-flight transactions have UPDATE records in the WAL but **no terminator** (commit/rollback record).
2. The restarted primary's `recovery_finish()` **aborts each such in-flight oxid in memory after end-of-redo**, then emits a stand-alone `WAL_REC_ROLLBACK` for it via **`wal_emit_recovery_finish_rollback` (`wal.c:485-504`)**. (The normal abort path `wal_rollback` no-ops here because the startup process never wrote the txn's material changes into *its* `local_wal` — see that function's comment; this is the fix for **orioledb#876 / `fb1a8acc`**.) These rollbacks are *new forward WAL*, streamed to the standby.
3. The standby replays them as a **mass `abort-defer` burst** — in the caught trial, **22 in-flight oxids aborted in 8 ms, 0 commits**, in WAL/commit order (not oxid order): 452, 362, 426, 444, 446, 360, 454, 378, 404, 439, 427, 352, 374, **248**, 401, 392, 399, …
4. Replaying `ROLLBACK(248)` runs `recovery_switch_to_oxid(248)` → fresh `HASH_ENTER` → **`pairingheap_add(xmin_queue, …)` at `recovery.c:1919`** — inserting the low oxid 248 into the horizon queue **below the already-advanced `writtenXmin = 352`**.
5. `update_run_xmin` reads 248 as the new queue-min and writes `globalXmin = 248 < writtenXmin = 352` — the §5 fault.

### The true root: the rollback is stamped with `runXmin`-at-emit, not the oxid's real horizon

Both finish paths stamp the record with **`runXmin` *at emission time*** — `add_finish_wal_record(WAL_REC_ROLLBACK, pg_atomic_read_u64(&xid_meta->runXmin))` (`wal.c:452-453` normal, `wal.c:495-496` recovery-finish). For a *deferred* rollback this is wrong: by the time `recovery_finish` emits it, `runXmin` has raced far past the oxid.

The caught trial proves it. Each burst record's carried `xmin` vs its oxid:

```
oxid=248  carried_xmin=468   ← 248 < 468  (primary had already frozen it)
oxid=332  carried_xmin=468   ← 332 < 468
oxid=345  carried_xmin=468   ← 345 < 468
…all other burst members:    oxid >= carried_xmin   (genuinely live when rolled back)
```

The carried xmins climb monotonically across the burst (`66, 121, 147, 179, 223, 326, 346, 468`) — that is `runXmin` advancing as `recovery_finish` sweeps the in-flight set, ending at `runXmin = nextXid = 468` (`free_run_xmin`). So the **low oxids swept last carry the highest xmin**; `carried_xmin = 468 ≫ oxid = 248` is the fingerprint of the deferred emission. A *normal* `wal_rollback` of 248 would have stamped `runXmin ≈ 248`.

This corrects two earlier mis-statements:

- **248 was *not* "unknown-open" / pre-stream-start.** It is in the *same* crash-rollback burst as 352; both were in-flight on the original primary. 248 is special only because its deferred rollback was emitted *last*, with a stale-high stamped xmin.
- **`writtenXmin = 352` is not "the oldest known-open oxid".** It is simply the incremental freeze frontier reached by the *earlier* burst members (whose oxids/xmins climb toward 352) **before** the late, low oxid 248 is processed. The freeze happens *during* the burst, so arrival order matters: process 248 first and `writtenXmin` never passes it.
- Consequently **sorting the burst by oxid *would* help** (contrary to an earlier claim that "the horizon is a min so order is irrelevant") — because freezing is incremental, oxid-ascending order pins `globalXmin` at 248 first. But the principled discriminator is better than sorting: **`oxid < record.carried_xmin`** uniquely flags the deferred-late rollbacks ({248, 332, 345}) using the primary's own stamped horizon, with no buffering/reordering. (`oxid < writtenXmin` is only the *symptom* site; a live rollback such as `oxid=452, carried_xmin=352` legitimately has `452 ≥ writtenXmin` and *should* move the horizon.)

The original primary never froze 248 — it was genuinely fresh/in-flight there. The "frozen" status is a **recovery-side artifact**: the *restarted* primary's `runXmin` ran past 248 (its start record predates the redo/checkpoint start, so 248 isn't in the restarted primary's recovery `xmin_queue`) before `recovery_finish` emitted its rollback. The same shape that bites the standby, one level up.

### End-to-end chain

```
SIGKILL primary mid-transaction (oxid 248 in-flight, no terminator in WAL)
  → restarted primary recovery_finish() aborts 248 in memory, but runXmin has raced to 468
  → wal_emit_recovery_finish_rollback(248) emits ROLLBACK stamped xmin=runXmin=468 (≫ 248)  [wal.c:495]
  → standby replays ROLLBACK(248) → recovery_switch_to_oxid → pairingheap_add(xmin_queue)    [recovery.c:1919]
       (inserts oxid 248 below the standby's writtenXmin=352)
  → update_run_xmin reads queue-min=248 → writes globalXmin=248 < writtenXmin=352            [recovery.c:2556]   (Layer A)
  → oxid_get_csn skips the oxid<globalXmin fast-path, reads FROZEN slot, maps it to IN_PROGRESS
  → recovery workers spin on committed-frozen oxids in [248,352) → listPtr pinned → livelock
```

This is a **second-order effect of `fb1a8acc` (#876)**: that fix added the deferred ROLLBACK marker to cure "standby stuck INPROGRESS forever (no marker)", but the marker it emits carries a `runXmin`-at-recovery-finish xmin inconsistent with its low oxid — and replaying *that* marker regresses the standby horizon. The missing-marker livelock was replaced by a marker-that-regresses-the-horizon livelock.

## 7. Reproduction (self-contained, no local scripts)

Streaming-standby crash loop, **no injection**:

```bash
# Build dev extension + patched PG with injection_points (the latter only for other trials; not needed here)
make USE_PGXS=1 IS_DEV=1 install

# Drive the bank-account stress harness against a streaming standby and SIGKILL the
# primary postmaster on a fixed interval; watch the replica fail to shut down.
RR_REPLICA_MODE=streaming \
RR_INJECTION_POINTS=NONE \
RR_ASSERT_FIRINGS=0 \
RR_KILL_POSTMASTER=1 \
RR_KILL_POSTMASTER_INTERVAL=6 \
RR_PANIC_FATAL=0 \
RR_SAVE_ALL_LOGS=1 \
RR_DURATION=45 \
RR_WRITERS=8 \
python3 -m unittest test.t.crash.rr_stress_test.RrStressTest.test_bank_account_invariant
```

A caught trial is identified by: replica `postgresql.log` growing without bound, `handle_conflicts` spin count climbing into the millions, and `pg_ctl stop` on the replica timing out ("server does not shut down"). Saved evidence from the caught trial: `test/t/crash/results/CAUGHT_noinj_{primary,replica}.log` (replica 6.5 GB).

## 8. Relationship to prior fixes on this branch

- `fb1a8acc` fixed the *original* recovery livelock (eager-WAL / in-memory abort not propagated to the standby) — **verified**.
- `200073b5` wrapped the buggy commit-flow window in a crit section so the primary **PANICs instead of silently rolling back** — verified: 6/7 error injections are now harmless, the primary TRAPs, and replica/primary stay consistent.
- **This issue is distinct from both.** It is a *liveness* defect in parallel recovery's horizon bookkeeping that survives those fixes and needs no injection: the standby's recovery `update_run_xmin()` regresses `globalXmin` below `writtenXmin`, so committed-and-FROZEN oxids in the re-exposed band are mis-read as `IN_PROGRESS`, and the recovery conflict handler's no-wait spin turns that into an unrecoverable livelock.

## 9. Fix directions

Ordered from most-principled (deepest, at the *emission* site on the primary) to most-local (tolerate it on the replica). §6 shows the root is a **deferred recovery-finish rollback stamped with `runXmin`-at-emit**, so the cleanest fixes act where that stamp is produced or consumed.

1. **(Deepest — fix the stamp at the source) Emit the recovery-finish rollback with an `xmin` consistent with its oxid.** In `wal_emit_recovery_finish_rollback` (`wal.c:485-504`), the record is stamped with `runXmin`-at-emit (`wal.c:496`), which for a deferred rollback races far past the oxid (oxid 248 stamped xmin 468). Stamp instead a value that cannot exceed the oxid being rolled back (e.g. `min(runXmin, oxid)`), so the standby never sees `oxid < carried_xmin` and never inserts a sub-horizon entry. Removes the defect for *all* downstream consumers (standby and any future reader), not just this one.
2. **(Consume it safely on the standby — principled discriminator) Don't add a *deferred-late* rollback oxid to the horizon queue.** On replay, if **`oxid < the record's carried xmin`**, the primary had already retired this oxid from its own horizon: apply its undo but **skip** the `pairingheap_add(xmin_queue, …)` at `recovery.c:1919` so `update_run_xmin` never sees a regressing min. Prefer `oxid < carried_xmin` over `oxid < writtenXmin` — the former uses the primary's authoritative horizon and correctly leaves *live* rollbacks (`oxid ≥ carried_xmin`, e.g. 452) in the queue; the latter is only the symptom site. Caveat to verify: the abort's undo must still be applied; only `xmin` participation is suppressed. Add an `elog`/assert if a *commit* (not rollback) ever arrives with `oxid < carried_xmin` — that would be a strictly worse bug.
3. **(One-line symptom floor) Clamp the recovery `globalXmin` write at `writtenXmin`.** In `update_run_xmin()` / `free_run_xmin()` (`recovery.c:2556`, `:2582`), write `max(xmin, writtenXmin)` to `globalXmin`. Safe (everything `< writtenXmin` is already frozen = globally visible, so raising the visibility horizon back to `writtenXmin` hides nothing) and provably stops the livelock, but papers over the fact that the horizon was advanced past a still-open oxid.
4. **(Defence in depth) Make the recovery conflict handler yield.** A worker spinning on a conflicting oxid with `waitCallback == NULL` should advance/yield instead of busy-waiting, so it cannot pin `listPtr` against the leader's drain — breaking the circular wait even if a stale state is briefly observed.
5. **(Defence in depth) Treat a `FROZEN` slot read as committed-visible, not `IN_PROGRESS`.** If `oxid_get_csn` reads `COMMITSEQNO_FROZEN` from the slot itself (not via the fast-path), it should resolve to *visible/frozen*, never `IN_PROGRESS` — a frozen slot can never legitimately mean in-progress.

(1) fixes the producer; (2) fixes the consumer; (3) is the minimal floor; (4)/(5) are independent backstops that each break the livelock regardless of the exact write. Recommended: ship (1) (root) — or (2) if the wire format can't change — plus (5) as a cheap invariant guard, and keep (3) as belt-and-suspenders. Note: sorting the burst by oxid would also work (freezing is incremental, §6) but needs WAL buffering, so it's strictly worse than (1)/(2).

## 10. Newly observed *second* replica failure mode — `UNDO_REC_EXISTS` assertion crash

Under the **same no-injection chaos** (streaming standby + `SIGKILL` of the primary postmaster every 6 s), a trial produced a *different* terminal failure than the livelock. The harness reported it as a "divergence," but that label is misleading — it was a **replica-side assertion crash**, not a data divergence.

**What it is NOT:**
- Not a PK/SK set divergence (`sk_extra`/`sk_missing` empty), not a `sum(balance)` / row-count / distinct-token mismatch.
- Not `orioledb_tbl_check = false` (that structural check runs only in *logical* mode; it is skipped for a streaming standby, `rr_stress_test.py:1516`).

**What it actually is:** the harness's only violation was
```
replica: scalar check failed: OperationalError('server closed the connection')
```
raised when `SELECT count(*), sum(balance), count(DISTINCT token) FROM o_bank_account` was run against the replica (`rr_stress_test.py:1506-1508`). The connection closed because a **replica backend aborted on an assertion**:

```
TRAP: failed Assert("UNDO_REC_EXISTS(undoType, undo_loc)"), File: "src/btree/page_contents.c", Line: 64, PID: 4116821
server process (PID 4116821) was terminated by signal 6: Aborted
the database system is in recovery mode
orioledb recovery after fatal error started.  Unable to make multiprocess recovery.
```

So a replica backend hit `Assert(UNDO_REC_EXISTS(undoType, undo_loc))` at `src/btree/page_contents.c:64`, aborted (signal 6), and the replica dropped into single-process crash recovery — which is what closed the harness connection.

**Why it likely matters (hypothesis, not yet proven).** `UNDO_REC_EXISTS(undoType, undo_loc)` failing means a B-tree page still references an **undo record location that has already been reclaimed** — undo was trimmed while a page still needed it. Undo retention is driven by the *same* recovery watermark machinery as the CSN livelock: `update_run_xmin()` / `free_run_xmin()` set `runXmin`/`globalXmin`, and `update_proc_retain_undo_location()` reclaims undo against those watermarks. The working hypothesis is that **both symptoms share one root cause — a recovery xmin/retain-bookkeeping fault**:
- the **CSN side** strands a worker reading a committed oxid as `IN_PROGRESS` (the §1–§5 livelock),
- the **undo side** reclaims an undo record too early, so a page dereferences a freed undo location (this assertion).

**Evidence saved:** `test/t/crash/results/CAUGHT_divergence_1_{primary,replica}.log` (replica ~169 MB, primary ~247 MB). The replica log's tail carries the `TRAP` line above; the 77 `orioledb_tbl_check` hits in the *primary* log are just the primary executing that check statement, not failures.

**Detection note:** this failure currently lands in the harness's "divergence" bucket via the scalar-check `except` handler. It should be split into its own bucket (grep the replica log for `TRAP: failed Assert("UNDO_REC_EXISTS`) so undo-retention crashes are not conflated with real PK/SK divergences or with the does-not-shut-down livelock.

### Important: the "divergence" bucket conflates *three* unrelated outcomes

The harness's single "divergence" label has now masked three different things. Two are **not** this issue and one is **already known**:

1. **`orioledb structural check returned false` → the KNOWN extent leak.** Fingerprint `NOTICE: Extent X 1 is neither free or busy` + `Corrupted index name = o_bank_account_token_uniq` (SK only; PK always clean), emitted from `check_extents()` at `src/btree/check.c:404`. This is documented in `extent_leak_issue.md` with a *different* root cause (a phase-1 split right-page invisible to the top-down downlink walk in `check_walk_btree`). It is **not** the recovery livelock and is **not** shown to be caused by the `globalXmin` regression — although `belowWritten=1` regressions have been observed co-occurring on the primary in the same trial, which is at most a lead for the *extent-leak* investigation, not evidence here.
2. **`UNDO_REC_EXISTS` assertion crash** (above) — a replica-side undo-retention TRAP; possibly the undo sibling of this issue.
3. **The does-not-shut-down livelock** — this issue proper.

A correct harness should bucket these separately (`grep` for `Extent .* is neither free or busy`, `TRAP: failed Assert("UNDO_REC_EXISTS`, and `does not shut down` respectively) rather than collapsing all three into "divergence".
