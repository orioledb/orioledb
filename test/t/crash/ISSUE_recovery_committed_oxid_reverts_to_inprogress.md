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

### What violates it: a crash-aborted in-flight oxid surfacing late

The assumption is broken by OrioleDB's **eager WAL + crash-rollback** path:

1. A transaction's row changes are WAL-logged *before* commit. When the primary is `SIGKILL`ed, its in-flight transactions have UPDATE records in the WAL but no terminator.
2. The primary restarts, crash-recovers, and at recovery-finish **emits `ROLLBACK` WAL records** for every transaction that was in-flight at crash time (the `o_emit_recovery_finish_rollbacks` path). These are *new forward WAL*, streamed to the standby.
3. The standby (here, started streaming at `0/3000000` from a base backup) replays them as a **mass `abort-defer` burst** — in the caught trial, **22 in-flight oxids aborted in 8 ms, 0 commits**: oxids 452, 362, 426, 444, 446, 360, 454, 378, 404, 439, 427, 352, 374, **248**, 401, 392, 399, …
4. Replaying the `ROLLBACK` for oxid **248** runs `recovery_switch_to_oxid(248)` → fresh `HASH_ENTER` → **`pairingheap_add(xmin_queue, …)` at `recovery.c:1919`** — inserting the low oxid 248 into the horizon queue **below the already-advanced `writtenXmin=352`**.
5. `update_run_xmin` then reads 248 as the new queue-min and writes `globalXmin = 248 < writtenXmin = 352` — the §5 fault.

Evidence: `ROLLBACK (248 0 0 - xmin 468 csn 438)` is present in **`CAUGHT_noinj_primary.log`** (the primary emitted it; `0 0` = no heap xid, an orioledb-only txn); 248 has **zero** `commit-defer` pushes in the whole log; the burst is preceded by `started streaming WAL from primary at 0/3000000` (post-crash reconnect).

### Why `writtenXmin` = 352 = an aborted oxid is the whole story

Both 248 and 352 are crash-aborted, and `writtenXmin` sits exactly at 352. That is the signature, not a coincidence:

- **352 was the oldest in-flight oxid the standby *knew about*** — so the horizon correctly stalled at 352 and froze everything below it.
- **248 was an *older* in-flight oxid the standby *did not* know about** — it opened on the primary *before* the standby's streaming start (`0/3000000`), so it was never in this session's `xmin_queue`, and the horizon sailed past it. Its existence is revealed only by the late crash-`ROLLBACK`.

So the watermark logic was *locally correct* (352 genuinely was the oldest *known* open oxid); it was defeated by an **old, unknown-open oxid surfacing late**. Note: **sorting the abort burst does not help** — the horizon is a *minimum*, so `min(248, …) = 248` regardless of processing order. The lever is **membership** (whether 248 enters the horizon set at all), not order.

### End-to-end chain

```
SIGKILL primary
  → primary crash-recovery finish emits ROLLBACK for in-flight oxids (incl. 248, < writtenXmin)
  → standby replays ROLLBACK(248) → recovery_switch_to_oxid → pairingheap_add(xmin_queue) [recovery.c:1919]
       (inserts oxid 248 below writtenXmin=352, which the standby had frozen past while 248 was unknown-open)
  → update_run_xmin reads queue-min=248 → writes globalXmin=248 < writtenXmin=352 [recovery.c:2556]   (Layer A)
  → oxid_get_csn skips the oxid<globalXmin fast-path, reads FROZEN slot, maps it to IN_PROGRESS
  → recovery workers spin on committed-frozen oxids [248,352) → listPtr pinned → livelock
```

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

Ordered from most-principled (deepest) to most-local. The upstream cause (§6) is now understood, so the options split between *preventing the bad `xmin_queue` insert* and *tolerating it*.

1. **(Deepest — true root) Don't advance the horizon past an unknown-open oxid.** The standby froze past oxid 248 because the base backup / checkpoint that seeded it did not account for 248 still being open on the primary. If the seed `runXmin`/`globalXmin` reflected the primary's true oldest in-flight oxid at backup time, `writtenXmin` would never have frozen `[248, …)`. Correct but the largest change (touches checkpoint/backup xmin recording).
2. **(Targets the bad insert — matches the "handle these oxids specially" intuition) Don't add an already-frozen crash-aborted oxid to the horizon queue.** When a `ROLLBACK` is replayed for an oxid `< writtenXmin`, that oxid is already frozen/retired: it needs its **undo applied** but it must **not** participate in the MVCC horizon. Skip the `pairingheap_add(xmin_queue, …)` at `recovery.c:1919` for such oxids so `update_run_xmin` never sees a sub-`writtenXmin` min. Caveat to verify: the abort's undo must still be applied and its abort state recorded; only its `xmin` participation is suppressed.
3. **(One-line symptom floor) Clamp the recovery `globalXmin` write at `writtenXmin`.** In `update_run_xmin()` / `free_run_xmin()` (`recovery.c:2556`, `:2582`), write `max(xmin, writtenXmin)` to `globalXmin`. Safe (everything `< writtenXmin` is already frozen = globally visible, so raising the visibility horizon back to `writtenXmin` hides nothing) and provably stops the livelock, but papers over the fact that the horizon was advanced past a still-open oxid.
4. **(Defence in depth) Make the recovery conflict handler yield.** A worker spinning on a conflicting oxid with `waitCallback == NULL` should advance/yield instead of busy-waiting, so it cannot pin `listPtr` against the leader's drain — breaking the circular wait even if a stale state is briefly observed.
5. **(Defence in depth) Treat a `FROZEN` slot read as committed-visible, not `IN_PROGRESS`.** If `oxid_get_csn` reads `COMMITSEQNO_FROZEN` from the slot itself (not via the fast-path), it should resolve to *visible/frozen*, never `IN_PROGRESS` — a frozen slot can never legitimately mean in-progress.

(1) or (2) remove the cause; (3) is the minimal floor; (4)/(5) are independent backstops that each break the livelock regardless of the exact upstream write. Recommended: ship (2) + (5), keep (3) as a cheap belt-and-suspenders, and track (1) as the long-term correctness item.

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
