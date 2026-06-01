# Replica parallel-recovery deadlock: unresolvable CSN conflict on a hot leaf (blkno=2050)

**Status:** root-caused via live gdb on a wedged replica (2026-06-01), not fixed.
Independent of the streaming-replica commit-window fix (that one touches the *primary*
commit path only; this is *replica*, recovery-only).

> **Root cause was revised on 2026-06-01.** The original write-up (preserved below under
> "Superseded hypothesis") blamed *barging page locks*. Live backtraces of the wedge prove
> that is only the **surface symptom**: the workers busy-spin on page 2050 because they are
> stuck in **conflict resolution** — `o_btree_modify_handle_conflicts` →
> `oxid_get_csn(oxid=480)` returns `INPROGRESS` forever, because oxid 480's commit/abort
> record is **absent from the WAL the workers were given**. The page-2050 lock churn is the
> retry loop of that unresolvable conflict, not a lock-fairness defect.

**Discovered:** 2026-06-01, during the assert-injection trial that validated the
crit-section fix (`RR_ASSERT_POINTS` enabled, 15s × 1 trial). Evidence saved in
`/tmp/assert1_evidence.txt`.

---

## Symptom

After the primary takes an assert-induced crash and recovers, the streaming replica's
parallel recovery wedges. Concretely:

- Walreceiver flushed all WAL (`0/303BD48`) but the startup process is stuck at replay
  `0/3010470` inside a `RecoveryWalStream` wait.
- Three OrioleDB parallel-recovery processes (PIDs 3126853 / 3126854 / 3126855) spin
  forever on the **same btree page `blkno=2050`**.
- The csn-trace log shows the tell-tale pattern, repeating indefinitely:

  ```
  PID 3126855 ... lock_page got  ... blkno=2050 waited=0      <- barger keeps winning
  PID 3126854 ... lock_page wait ... blkno=2050 tail_procno=107
  PID 3126853 ... lock_page got  ... blkno=2050 waited=0
  PID 3126854 ... lock_page wait ... blkno=2050 tail_procno=107   <- woken, re-queued, never acquires
  ```

The replica never catches up; it is a hard livelock, not a slow replay.

---

## Root cause: parallel recovery deadlocks on a CSN conflict it can never resolve

Live gdb on a still-wedged trial (50-trial run, trial 50; PIDs below) gives the decisive
picture. Two recovery workers are pinned at 68% CPU (state `R`), the other four workers,
checkpointer, bgwriters and walreceiver are all idle (`S`), and the startup/leader process
is parked waiting for WAL that will never come.

### The two spinning workers (gdb backtraces)

```
Worker 1 (pid 3168140, applying oxid 563's UPDATE):
  oxid_get_csn (oxid=480) at src/transam/oxid.c:1664
  o_btree_modify_handle_conflicts at src/btree/modify.c:523
  o_btree_modify_internal (opOxid=563) at src/btree/modify.c:203
  apply_tbl_update (oxid=563) at src/recovery/worker.c:1054

Worker 0 (pid 3168141, applying oxid 576's UPDATE):
  lock_page (blkno=2050) at src/btree/page_state.c:452
  o_btree_modify_handle_conflicts at src/btree/modify.c:620
  o_btree_modify_internal (opOxid=576) at src/btree/modify.c:203
  apply_tbl_update (oxid=576) at src/recovery/worker.c:1054
```

A gdb breakpoint on `oxid_get_csn` confirmed the argument is **always `oxid=480`** for
worker 1 — it is calling `oxid_get_csn(480, false)` from `handle_conflicts` (modify.c:523)
over and over. `oxid_get_csn(480)` returns `COMMITSEQNO_INPROGRESS` (oxid.c:1714-1724:
the shared xid buffer has no final CSN for 480), so the conflict path takes the
"conflicting transaction is in-progress" branch (modify.c:578-628): it
`unlock_page(2050)` (603), `refind_page(2050)` (620), and returns `ConflictResolutionRetry`
— the outer modify loop retries from the top, forever. The page-2050 lock churn the
original write-up saw is exactly this **conflict-retry loop's** lock/unlock, not a
fairness defect.

### Why oxid 480 never finalizes — it isn't in the WAL the workers got

This is the real bug. There is a clean logical proof from how recovery dispatches records:

- **Same row ⇒ same worker.** Recovery partitions by row-key hash:
  `hash = o_btree_hash(desc, rec, BTreeKeyLeafTuple)` (recovery.c:4694),
  `GET_WORKER_ID = hash % recovery_pool_size_guc` (internal.h:41). oxid 480, 563 and 576 all
  conflict on the **same row** (that's what `handle_conflicts` is resolving), so all three
  transactions' modifies for that row are dispatched to the **same worker queue**.
- **Commit-finish is delivered to exactly the workers that touched the oxid.**
  `workers_send_oxid_finish` (recovery.c:4518-4560) sends `RecoveryMsgTypeCommit/Rollback`
  only to workers with `cur_recovery_xid_state->used_by[i]` set — the workers that applied a
  modify for that oxid. A worker handles that message by calling
  `recovery_finish_current_oxid(...)` (worker.c:588), which writes oxid 480's final CSN into
  the **shared** xid buffer that `oxid_get_csn` reads.
- **Per-worker queues are FIFO.** So if oxid 480's COMMIT had been read by the leader, it
  would sit in worker 1's queue **ahead of** oxid 563's UPDATE (480 commits before 563 can
  touch the same row on the primary, hence earlier in WAL, hence dispatched earlier), and
  FIFO guarantees worker 1 processes the COMMIT first — no spin.

The spin is therefore **proof that oxid 480's commit/abort record was never read by the
leader**: it is not in the WAL stream the replica received. The leader (startup pid 3168136)
confirms it — it is idle in `WaitForWALToBecomeAvailable` (xlogrecovery.c:4041) at
replayLSN `0/3020D70`, having consumed and dispatched everything available; the walreceiver
has no more to give because the primary crashed. All non-spinning workers are drained
(`S`/idle), so 480's COMMIT is in **no** worker's queue either.

But truncation alone cannot explain it: the replica **has** oxid 563's UPDATE (a worker is
actively applying it), and on a correct primary 563's UPDATE has a *later* LSN than 480's
COMMIT (563 can only touch the row after 480 releases it). WAL truncation removes the
**tail** — if a later record survived, the earlier prerequisite record must survive too. So
the WAL the replica holds is **internally inconsistent**: it contains a row-modify
(563/576) whose prerequisite commit (480) is missing. That points at a WAL
ordering/atomicity defect on the **primary's** side — a transaction's dependent row-modify
can become durable/streamable ahead of (or without a barrier against) the earlier
conflicting transaction's commit record — even though the visible symptom is replica-only.

> **Open sub-question (next diagnostic):** is 480's COMMIT (a) genuinely never written to
> WAL, or (b) written but mis-ordered after 563's UPDATE, or (c) written but its delivery to
> the worker is dropped by a `used_by[]` tracking gap? (a)/(b) are primary WAL-ordering bugs;
> (c) is a recovery-dispatch bug. Distinguish by `pg_waldump` of the primary's WAL around
> oxid 480's LSN vs. inspecting worker 1's local `recovery_xid_state_hash` for a 480 entry.

### Why parallel recovery, and not the primary

On the primary the conflicting transaction is *live*: `oxid_get_csn` of an in-progress
neighbour eventually resolves because that neighbour really does commit/abort in shared
memory, and the waiter is a heavyweight `wait_for_tuple` (modify.c:608) that wakes on
release. In replica recovery the "transaction" is just a WAL record; its resolution depends
entirely on a **later WAL record** arriving and being applied. If that record is missing,
there is nothing to wake the waiter — the conflict is unresolvable and the retry loop spins
forever. The single hot leaf (below) just guarantees that *every* replayed row collides, so
the missing-commit deadlock is hit reliably rather than rarely.

### Substrate: blkno=2050 is the one hot leaf

`blkno=2050` is a **leaf (level 0)** holding essentially the whole 100-account table (the PK
B-tree is ~2 leaves; saved `refind_page` traces show `blkno=2050 level=0` for 18130/19402 =
94% of leaf re-finds, `blkno=2058` for the rest, **no** internal pages). Per-row-hash worker
partitioning spreads rows across workers, but since nearly every row lives on leaf 2050,
every worker's `o_btree_modify` lands on that one leaf — which is why the deadlock and its
lock churn are concentrated there.

---

## Amplifier: the csn-trace `elog(LOG)` instrumentation

Per CLAUDE.md the three `elog(LOG)` calls in `lock_page` (page_state.c:425 request,
438 wait, 456 got — all gated by `#ifdef USE_INJECTION_POINTS`) are "load-bearing timing
scaffolding". They almost certainly convert transient unfairness into a *hard* livelock
here: each `elog` widens the interval the woken waiter spends **outside** the lock
(returning from the semaphore, formatting+emitting the log line, re-entering
`lock_page_or_queue`), handing bargers many extra acquire windows per wake. The same
property that broadens the SK race on the primary starves the waiter on the replica.

**Resolved (2026-06-01):** the livelock is **NOT** reproducible with the csn-trace elogs
removed. Experiment: all 65 `elog(LOG, "csn-trace ...")` statements across
`modify.c / find.c / btree-undo.c / transam-undo.c / oxid.c / page_state.c` were disabled
(wrapped in `#if 0`, replaced by a `(void) 0;` no-op so lone-`if` bodies stay valid),
rebuilt, and the exact assert config that had wedged at 180s was re-run:

| build | config | result |
|-------|--------|--------|
| csn-trace **ON**  | assert_firings=2, streaming replica, 1 trial | **TIMEOUT 180s** (livelock) |
| csn-trace **OFF** | same | 33s clean |
| csn-trace **OFF** | same, 8 more trials | 8/8 clean (~33s each) |
| csn-trace **OFF** | same, 50 trials | **49 clean, 1 TIMEOUT 180s** (trial 50) |

**Conclusion (revised after the 50-trial run):** the livelock is a **genuine, rare
production recovery hazard — NOT merely an instrumentation artifact.** With the csn-trace
elogs *off* it still reproduced once in 50 assert-crash trials (~2%). With them *on* it hit
on the very first trial (≈100%). So the elogs are a **~50× amplifier**, not the root cause.
The first 9/9-clean result was simply too small a sample to catch a 2%-rate bug — a
cautionary note for sign-off thresholds on rare-race fixes.

The real defect is the **unresolvable CSN conflict** (above): a missing commit record for
oxid 480 means the conflict-retry loop in `handle_conflicts` can never terminate. The
csn-trace elogs are an amplifier of the *rate at which that interleaving is produced*, not
the cause. They sit on the hot conflict/lock path, so they (a) slow each retry iteration and
(b) skew the timing of which transactions are mid-flight when the primary takes its assert
crash — both of which make the "primary crashed with a dependent modify durable but its
prerequisite commit not yet streamed" window far more likely. With the elogs off that window
is rarely hit (~2%); with them on it is hit almost every trial. Any hot-path instrumentation,
or a slow enough body on this path, widens it toward 100%.

Evidence for trial 50 (saved logs):
`test/t/crash/results/20260601_132838_assert_nocsn50_NONE_panic.log` (primary) and
`..._panic_replica.log`. Primary recovered cleanly twice (redo done `0/3015CC0`, then
`0/301A6E8`, both "ready"); the **replica** reconnected at `0/3000000` (13:28:34.651) and
then emitted nothing for the remaining ~165s until the 180s kill — the wedge is replica
recovery, not the primary.

> **Caveat / open confirmation:** with csn-trace off there are no `lock_page` traces in the
> log, so the trial-50 wedge is identified by *signature* (healthy primary + silent replica
> after "started streaming" + 180s hang under the exact livelock-prone config), not by a
> direct blkno=2050 trace. To pin it definitively, the next repro should run with a
> **gstack/gdb-backtrace-on-timeout hook** on the replica's `orioledb recovery worker`
> PIDs — a wedge stack in `lock_page` → `PGSemaphoreLock` confirms it without re-adding the
> amplifying elogs.

The toggle script lives at `/tmp/csn_toggle.py` (`disable` / `enable`).

---

## Diagnostic trace points for a future fix (step 4)

Now that the mechanism is known, the highest-value instrumentation is **off the hot path**
(any elog on the conflict/lock path perturbs the race — it is the amplifier above) and aimed
at proving *which* of the three sub-causes (a/b/c in the open sub-question) holds.

1. **Bounded-spin detector → diagnostic dump (the actionable one).** In
   `o_btree_modify_handle_conflicts`, count consecutive `ConflictResolutionRetry` for the
   same `(blkno, conflicting oxid)`. Past a threshold (e.g. 10k), emit a **one-shot**
   `elog(WARNING)` (not per-iteration) with `oxid=<op>`, `conflict_oxid=<480>`, `blkno`,
   and `oxid_get_csn` result. This converts the silent infinite spin into a single
   self-identifying log line — exactly the signal the no-elog harness lacks today. It is the
   natural place to later **break the deadlock** (see candidate fix #1).

2. **WAL provenance of the conflicting oxid.** When the bounded-spin detector fires, also
   log whether the conflicting oxid exists in the worker's local `recovery_xid_state_hash`
   (was a modify for it ever dispatched here?) and the leader's last `replayLSN`. This
   directly separates sub-cause (c) (`used_by[]`/dispatch gap — entry present, never
   finalized) from (a)/(b) (entry absent — WAL ordering/truncation).

3. **`pg_waldump` cross-check (no code).** Dump the primary's WAL around oxid 480's first
   appearance and confirm whether a `WAL_REC_COMMIT`/`WAL_REC_ROLLBACK` for oxid 480 exists
   at all, and its LSN relative to oxid 563's `WAL_REC_UPDATE`. This settles (a) vs (b)
   outright.

(The barge/requeue counters from the superseded hypothesis are demoted — they would measure
a real-but-secondary lock-fairness property, not the deadlock.)

---

## Candidate fixes (not implemented — for a dedicated pass)

1. **Recovery-side safety net: bound the conflict spin.** A recovery worker must never spin
   forever waiting for a CSN that the WAL it has cannot provide. After a bounded number of
   `ConflictResolutionRetry` on the same conflicting oxid, the worker should treat the
   missing-finish as a hard recovery error (PANIC with the offending oxid/LSN) rather than
   livelock — turning a silent hang into a diagnosable, restartable failure. This is a
   containment fix, not a correctness fix.
2. **Primary-side correctness fix (the real one, pending a/b/c).** If the WAL truly lacks (or
   mis-orders) the prerequisite commit, the fix belongs on the **primary's** WAL emission:
   ensure a transaction's commit record is durable/streamed before any later transaction's
   dependent row-modify can be. This ties into the active commit-window crit-section work —
   confirm whether that fix already closes this window once `pg_waldump` settles the
   sub-cause.

---

## Reproduction

`RR_ASSERT_POINTS` enabled, streaming replica mode, 15s × 1 trial is enough to wedge the
replica once the primary takes an assert crash+recover (≈100% with csn-trace on; ~2% with
it off). See `streaming_replica_issue.md` for the harness/env-knob reference.
Evidence: `/tmp/assert1_evidence.txt`.

### Live-wedge gdb evidence (2026-06-01, trial 50, csn-trace OFF)

Captured with `kernel.yama.ptrace_scope=0` (user lowered it for the session; **restore to
1 afterward**). The wedge was still alive ~hours after the 180s harness kill — proof it is a
true deadlock, not slow progress:

| pid | role | state | stuck at |
|-----|------|-------|----------|
| 3168136 | startup/leader | S | `WaitForWALToBecomeAvailable` xlogrecovery.c:4041, replayLSN `0/3020D70` |
| 3168140 | recovery worker 1 (oxid 563) | R, 68% CPU | `oxid_get_csn(480)` ← `handle_conflicts` modify.c:523 |
| 3168141 | recovery worker 0 (oxid 576) | R, 68% CPU | `lock_page(2050)` ← `handle_conflicts` modify.c:620 |
| 3168138/9, 3168142/3 | recovery workers 2-5 | S | idle, queues drained |

gdb breakpoint on `oxid_get_csn` in pid 3168140 fired repeatedly with `oxid=480`,
`getRawCsn=false`, called from `o_btree_modify_handle_conflicts` (modify.c:523) — the
direct proof that the spin is conflict resolution waiting on oxid 480's never-arriving CSN.

---

## Superseded hypothesis: barging (non-handoff) page locks

> Kept for the record. The lock-fairness analysis below is *accurate about the page-lock
> implementation* but is **not** the cause of this wedge — see the gdb root cause above.
> The page-2050 lock churn it describes is the retry loop of the unresolvable CSN conflict,
> not a starvation livelock.

OrioleDB's page lock (`src/btree/page_state.c`, custom non-LWLock) is **barging**: the
acquire fast-path `lock_page_or_queue` (page_state.c:142-144) CAS-grabs an unlocked page
even with waiters queued; `unlock_page_internal` (1067-1085) clears the LOCKED flag and only
then wakes one waiter (1104-1133), who re-races from scratch in `lock_page`'s `while(true)`
loop (429-453). This *is* how the lock works, and it does let bargers jump the queue — but
in this wedge both workers are cycling the lock deliberately as part of `ConflictResolutionRetry`,
so fairness is a red herring. The earlier conclusion that the elogs were a "~50× amplifier
of a barging livelock" was half right: they are a ~50× amplifier, but of the
**missing-commit conflict window**, not of lock unfairness.
