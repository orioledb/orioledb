# Recovery livelock: a committed (finalized) oxid's CSN slot reverts to `INPROGRESS`, stranding parallel-recovery workers

**Status:** root cause strongly evidenced, one instrumentation gap remains before it is *proven* mechanically.
**Branch:** `add_stress_bank_account_test`
**Mode:** streaming standby, parallel recovery. **Injection-independent** (reproduces under plain `SIGKILL` of the primary postmaster, no injection point attached).
**Symptom:** the replica's parallel-recovery workers spin forever in `o_btree_modify_handle_conflicts()`; the replica never finishes replay and never shuts down.

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

This rules out the earlier "never finalized" hypothesis: the oxid **was** finalized and **then reverted** to `INPROGRESS`. This is the same finalize-then-revert pattern previously observed for oxid 483 (finalized to CSN 369, then read INPROGRESS for 5.6 s).

## 3. The deadlock topology (why a single reverted slot freezes the whole replica)

```
worker A (pid 3997315) ──spins on──▶ oxid 610  (committed, slot now reads INPROGRESS)
worker B (pid 3997316) ──spins on──▶ oxid 616  (committed, slot now reads INPROGRESS)
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
| **`NORMAL → INPROGRESS`** slot overwrite (`set_oxid_csn` / `advance_oxids`) | **not observed** — but see §5 | dedicated CLOBBER trace gated on `is_recovery_process() && IS_NORMAL(old) && !IS_NORMAL(new)` fired **0 times**. |

## 5. The remaining gap — and why `CLOBBER=0` is *not* exoneration

The CLOBBER trace was gated on `COMMITSEQNO_IS_NORMAL(old)`. The four **`FROZEN` reads** of 610/616 are the tell it missed: `result=FROZEN` is returned by `oxid_get_csn`'s top guard `oxid < globalXmin`, so `globalXmin` is **non-monotonic** around these oxids — it advances past them (stamping the slot `FROZEN` via `advance_global_xmin`, csn `0x3`) and later recedes (after the primary crash resets the horizon).

That means the slot's value just before it reads `INPROGRESS` is most plausibly **`FROZEN (0x3)`, not `NORMAL`**. The transition that strands the worker is therefore:

```
FROZEN (0x3)  ──▶  INPROGRESS (0x0)        ← re-initialization of an already-committed, frozen slot
```

most likely via **`advance_oxids`** re-initializing the slot when the reset horizon brings the oxid back into the `[nextXid, xmax)` init range. Because `FROZEN` is **not** `NORMAL`, the CLOBBER filter's `IS_NORMAL(old)` predicate **silently skipped exactly this write**. So `CLOBBER=0` proves only "no *committed→INPROGRESS* write," not "no revert." This is the third too-narrow predicate this investigation has tripped over (after the arbitrary `1000` reset threshold and a stray `)` in a drain grep); the lesson is to **log all writes and filter in analysis**, never filter at the source.

### The one experiment that would close it

Broaden the recovery slot-write trace to log **every** write to a tracked oxid's slot — `func`, `seq`, `old`, `new`, with **no old-value filter** — then re-catch. The expected confirming line is literally:

```
csn-trace slot-write seq=… func=advance_oxids oxid=610 old=0x3(FROZEN) new=0x0(INPROGRESS)
```

If that line appears with `globalXmin` having receded below 610 just before it, the mechanism is mechanically proven: **a primary crash makes the replica's `globalXmin`/`nextXid` horizon recede, and `advance_oxids` re-initializes the slot of an already-committed (frozen) oxid back to `INPROGRESS`, after which the recovery conflict handler spins on it forever.**

## 6. Reproduction (self-contained, no local scripts)

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

## 7. Relationship to prior fixes on this branch

- `fb1a8acc` fixed the *original* recovery livelock (eager-WAL / in-memory abort not propagated to the standby) — **verified**.
- `200073b5` wrapped the buggy commit-flow window in a crit section so the primary **PANICs instead of silently rolling back** — verified: 6/7 error injections are now harmless, the primary TRAPs, and replica/primary stay consistent.
- **This issue is distinct from both.** It is a *liveness* defect in parallel recovery's deferred-commit finalization that survives those fixes and needs no injection: a committed oxid's CSN slot is reverted to `INPROGRESS` by horizon re-initialization after a primary crash, and the recovery conflict handler's no-wait spin turns that into an unrecoverable livelock.

## 8. Suggested fix directions (for discussion, not yet implemented)

1. **Don't re-initialize a committed slot.** In `advance_oxids`, refuse to overwrite a slot whose current value is `FROZEN`/`NORMAL` back to `INPROGRESS` during recovery — a committed/frozen oxid must never regress.
2. **Make the recovery conflict handler yield.** A worker spinning on a conflicting oxid with `waitCallback == NULL` should at minimum advance/yield so it cannot pin `listPtr` against the leader's drain — breaking the circular wait even if a stale `INPROGRESS` is briefly observed.
3. **Keep `globalXmin` monotonic across the crash boundary** on the standby, so the horizon cannot recede below already-finalized oxids.

Of these, (1) targets the proven-most-likely write; (2) is the robust liveness backstop regardless of the exact write path.

## 9. Newly observed *second* replica failure mode — `UNDO_REC_EXISTS` assertion crash

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
