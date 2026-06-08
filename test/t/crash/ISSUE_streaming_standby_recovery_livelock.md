# Issue: streaming standby livelocks on an in-flight oxid the primary aborted in-memory at recovery-finish

**Status:** root-caused, reproduced 3× this session
**Severity:** high — a streaming hot-standby permanently wedges (100% CPU spin, no progress) after the primary crash-recovers
**Component:** `src/recovery/` (redo + recovery-finish), `src/transam/` (CSN/undo), `src/btree/` (conflict resolution)
**Branch:** `add_stress_bank_account_test`
**Repro harness:** `test/t/crash/rr_stress_test.py` with `RR_REPLICA_MODE=streaming`

---

## 1. One-paragraph summary

In OrioleDB streaming replication, a backend's row modifications are written to WAL **eagerly** (on local-WAL-buffer overflow), so an in-flight transaction's changes are streamed to and applied on the standby **before** its commit/abort verdict exists. When the primary is killed and crash-recovers, it aborts any still-in-flight transactions **in memory at the end of recovery (`recovery_finish`), emitting no WAL**. A streaming standby (a) has already applied those changes marking the oxid `INPROGRESS`, (b) never receives a rollback record for them, and (c) never runs `recovery_finish` itself (it is perpetually in recovery and only reconnects across the primary restart, never restarting). The oxid is therefore stuck `INPROGRESS` **forever** on the standby. The standby is fine until a later replayed modify of the *same row* must resolve a conflict against that oxid; `o_btree_modify_handle_conflicts` calls `oxid_get_csn(staleOxid)`, gets `INPROGRESS` every time, and the conflict-resolution retry loop spins without end → the cluster wedges (and eventually PANICs / times out).

---

## 2. Reproductions (this session)

Each wedge shows the identical signature: a standby recovery worker spinning on `oxid_get_csn(X)` for an oxid `X` that appears in the primary's `recovery-finish-abort-trace` list, all on the same hot leaf `blkno=2050`.

| Run | Aborted-list position of spin oxid | Spin oxid | Colliding `opOxid` | Page |
|-----|-----------------------------------|-----------|--------------------|------|
| wedge_repro2 | 1 / 12 (first) | 587 | 781 | 2050 |
| wedge_loop iter 6 | 12 / 12 (last) | 931 | 959 | 2050 |
| wedge_multi #1 | 1 / 17 (first) | 885 | — | 2050 |

**Position-independence:** the victim is whichever dangling oxid holds the top-of-undo-chain uncommitted version of the contended hot row — *not* a function of its order in the abort list. We observed both the first-listed (587, 885) and last-listed (931) oxid as the victim, which disproves any "only the newest/last transaction is affected" hypothesis. (`opOxid` of the colliding writer is always *higher* than the victim — i.e. a later writer of the same row.)

**Rate:** ~1 wedge per 6–11 trials with this config (`writers=20 accounts=100 duration=15`, csn-trace instrumentation ON as timing scaffolding, `RR_ASSERT_FIRINGS=2`).

---

## 3. Mechanism, step by step (with code references)

> All code links below are permalinks to **`orioledb/orioledb`** pinned at commit
> [`4458e16c`](https://github.com/orioledb/orioledb/tree/4458e16c2e1aae35b0c6609c59aa2562daf1244f)
> (branch `add_stress_bank_account_test`). Lines marked **[repro-branch trace]** are
> `IS_DEV`-only instrumentation added on this branch, not in upstream `main`.

### 3.1 Changes are streamed before the verdict (eager WAL)

Each backend accumulates row-level WAL in a per-process buffer that is flushed into the shared/streamed WAL on overflow — not deferred to commit:

- `flush_local_wal_if_needed` flushes on buffer overflow via `log_logical_wal_container` — [`src/recovery/wal.c:871`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/wal.c#L871), flush at [`:895`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/wal.c#L895).
- Called at the head of every record append ([`:193`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/wal.c#L193), [`:235`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/wal.c#L235), [`:319`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/wal.c#L319), …).

⇒ An in-flight (uncommitted) transaction's INSERT/UPDATE/DELETE records reach the standby while the transaction is still `INPROGRESS`.

### 3.2 The standby applies them eagerly, marked INPROGRESS

Redo is single-pass, forward-only, no look-ahead, no per-tx buffering. Modify records are applied immediately with `COMMITSEQNO_INPROGRESS`:

- `apply_modify_record` → `apply_tbl_modify_record(... COMMITSEQNO_INPROGRESS)` / `apply_btree_modify_record(... COMMITSEQNO_INPROGRESS)` — [`src/recovery/worker.c:686`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/worker.c#L686), [`:690`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/worker.c#L690).
- Main loop dispatch for `WAL_REC_INSERT/UPDATE/DELETE/REINSERT` — [`src/recovery/recovery.c:3939–3971`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/recovery.c#L3939-L3971).
- The verdict is a *separate later* record: `WAL_REC_COMMIT` / `WAL_REC_ROLLBACK` → `recovery_finish_current_oxid` — [`src/recovery/recovery.c:3711–3753`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/recovery.c#L3711-L3753), [`:1886`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/recovery.c#L1886).

### 3.3 The primary aborts in-flight oxids in memory with NO WAL

When the primary reaches end-of-WAL in crash recovery, `recovery_finish` aborts every still-`INPROGRESS` oxid by applying its undo stack — **emitting no WAL**:

- `recovery_finish` INPROGRESS sweep: `apply_undo_stack` + `walk_checkpoint_stacks`, **no `wal_*` call in the block** — [`src/recovery/recovery.c:1650–1677`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/recovery.c#L1650-L1677).
- In-source comment documenting exactly this bug — [`src/recovery/recovery.c:1654–1661`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/recovery.c#L1654-L1661). **[repro-branch trace]**
- The `recovery-finish-abort-trace` elog used to capture the aborted oxids — [`src/recovery/recovery.c:1663`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/recovery.c#L1663). **[repro-branch trace]**

By contrast a **normal backend abort** *does* emit `WAL_REC_ROLLBACK` — but only when `!RecoveryInProgress()` ([`src/transam/undo.c:2425`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/transam/undo.c#L2425)) and it asserts `!is_recovery_process()` ([`src/recovery/wal.c:422`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/wal.c#L422)). The recovery-leader in-memory abort takes neither path.

### 3.4 The standby never resolves the oxid

- A streaming standby is **perpetually in recovery**; it **never calls `recovery_finish()`** — so it never performs the in-memory abort sweep that would resolve the oxid. (Comment: [`src/recovery/recovery.c:1657`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/recovery/recovery.c#L1657).)
- The standby is **not restarted** when the primary crashes. The harness starts the replica once ([`rr_stress_test.py:241`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/test/t/crash/rr_stress_test.py#L241)) and never restarts it; `wal_keep_size` lets it **reconnect and resume from existing in-memory state** after the primary restarts ([`rr_stress_test.py:178–180`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/test/t/crash/rr_stress_test.py#L178-L180)). So the poison oxid persists in the standby's shared memory across the primary's restart.

⇒ The oxid is stuck `INPROGRESS` on the standby forever (see §4 for why INPROGRESS specifically, not COMMITTING).

### 3.5 The livelock fires on the next conflicting modify

A later replayed modify of the same row must resolve the conflict against the dangling version:

- `o_btree_modify_handle_conflicts` calls `oxid_get_csn` under the page-content lock — [`src/btree/modify.c:435`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/btree/modify.c#L435), `oxid_get_csn` call at [`:514`](https://github.com/orioledb/orioledb/blob/4458e16c2e1aae35b0c6609c59aa2562daf1244f/src/btree/modify.c#L514).
- Recovery workers drive `o_btree_modify` with `waitCallback = NULL` — there is no wait/sleep mechanism in redo, so instead of registering a wait the worker **spin-retries** the whole operation.
- Observed loop: `handle_conflicts enter → oxid_get_csn-call(staleOxid) → oxid_get_csn returns INPROGRESS → lock_page release → refind_page → handle_conflicts enter → …` indefinitely, at 100% CPU, on `blkno=2050`.

---

## 4. The dangling oxid is INPROGRESS, not COMMITTING

This is settled from existing evidence + code; no extra instrumentation needed.

- `oxid_get_csn` (`src/transam/oxid.c:1648`) has two regimes:
  - **COMMITTING** → it busy-spins *internally* (`perform_spin_delay`, `:1683`) and **never returns** — you would see an `enter` trace with no matching `exit`.
  - **INPROGRESS** → it `break`s and **returns `COMMITSEQNO_INPROGRESS` immediately** (`:1696`), every call.
- The captured csn-trace shows matched, repeating `oxid_get_csn enter` / `exit` / `handle_conflicts oxid_get_csn-returned csn=0` triples — i.e. it **returns every iteration**. By the code above that is only possible in the INPROGRESS regime; the unbounded loop is the **outer** `handle_conflicts` retry, not an inner committing-spin.
- Why INPROGRESS specifically on the standby: the replica sets `COMMITTING` only *transiently* inside commit-record processing (`recovery_finish_current_oxid`, `src/recovery/recovery.c:1900→1910`). A transaction that never committed produces no commit record, so the standby leaves the oxid at the `COMMITSEQNO_INPROGRESS` set by its modify records (`src/recovery/recovery.c:610, 915`) and never advances it.

(Supersedes an earlier hypothesis that the slot was stuck in `CSN_COMMITTING`; the observed return-every-iteration behavior rules that out.)

---

## 5. Why it is timing-sensitive / probabilistic

Only transactions large or old enough to have **overflowed their local WAL buffer** get their changes streamed to the standby before the crash; a tiny transaction whose changes never left the local buffer vanishes with the crash and causes no standby-side residue. Combined with the requirement that a *later* writer collide with the dangling version on a hot page, this yields the observed ~1-in-6-to-11 rate. The csn-trace `elog`s in `src/btree/page_state.c` deliberately widen the race window (load-bearing timing scaffolding — see CLAUDE.md).

---

## 6. Root cause (one line)

The end-of-crash-recovery in-memory abort of in-flight transactions (`recovery_finish`) is **not propagated into the WAL stream**, so a streaming standby — which applied those transactions' changes eagerly and never runs `recovery_finish` itself — can never resolve their oxids, and livelocks when a later modify conflicts with one.

---

## 7. Fix direction (not yet implemented)

Propagate the recovery-finish abort to consumers of the WAL stream: emit a rollback/abort record (e.g. `WAL_REC_ROLLBACK`) — or a checkpoint/recovery-boundary abort step — for **every** oxid that `recovery_finish` aborts in memory (`src/recovery/recovery.c:1650–1677`), so the standby resolves those oxids to `ABORTED` exactly as a normal backend abort would. Care required: this WAL must be emittable from the recovery/startup process (the normal `wal_rollback` path asserts `!is_recovery_process()` — `src/recovery/wal.c:422`), and it must be idempotent under standby re-streaming.

---

## 8. Reproduce it

### 8.1 Prerequisites

- A **`IS_DEV=1`** build of the extension (enables `USE_INJECTION_POINTS`; the repro is driven entirely by injection points):
  ```bash
  make USE_PGXS=1 IS_DEV=1 install
  ```
  `pg_config` must resolve to the patched PostgreSQL build (PG16 `patches16_34` / PG17 `patches17_6`).
- The patched PostgreSQL's **`injection_points`** contrib module installed (the test runs `CREATE EXTENSION injection_points`):
  ```bash
  make -C src/test/modules/injection_points install   # in the patched PG source tree
  ```
- Python **testgres** + the test deps: `pip install -r requirements.txt` (from the repo root).
- OrioleDB tables require an **ICU / C / POSIX** collation — if your default `initdb` locale yields `UTF-8`, run with `--locale=C`.

### 8.2 One self-contained trial

Run a single bank-account stress trial against a **streaming hot-standby**, with the primary periodically PANIC'd mid-commit (the injection `orioledb-commit-assert` is armed with the `error` action; inside its `START_CRIT_SECTION` that escalates to PANIC → crash recovery). `timeout` bounds the run: a clean trial finishes in ~35–40 s, a wedged trial hangs until killed (exit code 124).

```bash
cd /path/to/orioledb          # repo root; pg_config -> patched PG; testgres importable

timeout 180 env \
  RR_REPLICA_MODE=streaming \   # second node = pg_basebackup'd hot-standby (this is essential)
  RR_INSTANCE=wedge_repro \     # names the saved logs under test/t/crash/results/
  RR_WRITERS=20 \               # 20 concurrent writers onto...
  RR_ACCOUNTS=100 \             # ...100 accounts -> hot, contended key space
  RR_DURATION=15 \              # workload seconds
  RR_ASSERT_POINTS=orioledb-commit-assert \  # which injection to weaponize
  RR_ASSERT_FIRINGS=2 \         # arm it ~2x over the run -> 2 PANIC/crash-recover cycles
  RR_INJECTION_POINTS=NONE \    # no other (non-fatal) injection chaos
  RR_PANIC_FATAL=0 \            # tolerate PANIC + allow cluster restart (must be 0: we need the post-crash recovery)
  RR_SAVE_ALL_LOGS=0 \          # keep server logs only when a bug/panic is detected
  python3 -m unittest -v \
    test.t.crash.rr_stress_test.RrStressTest.test_bank_account_invariant
```

Outcomes:
- **exit 0** — clean trial, no wedge (most runs). Retry.
- **exit 124** — `timeout` killed a hung run = **the standby wedge** (this bug).
- non-zero, non-124 with an `invariant violations: [...]` line — a different (data-invariant) failure.

The bug is timing-sensitive (~1 wedge per 6–11 trials with this config), so loop the command until one hangs — plain shell, no extra scripts:

```bash
for i in $(seq 1 40); do
  echo "=== trial $i ==="
  timeout 180 env RR_REPLICA_MODE=streaming RR_INSTANCE=wedge_repro \
    RR_WRITERS=20 RR_ACCOUNTS=100 RR_DURATION=15 \
    RR_ASSERT_POINTS=orioledb-commit-assert RR_ASSERT_FIRINGS=2 \
    RR_INJECTION_POINTS=NONE RR_PANIC_FATAL=0 RR_SAVE_ALL_LOGS=0 \
    python3 -m unittest test.t.crash.rr_stress_test.RrStressTest.test_bank_account_invariant
  rc=$?
  if [ "$rc" = 124 ]; then echo "WEDGE on trial $i (exit 124)"; break; fi
  # clean trial: drop its (large) saved logs before the next iteration
  rm -f test/t/crash/results/*wedge_repro*_panic*.log
done
```

### 8.3 Confirm the signature from the saved logs

On a wedge, the server logs are saved to `test/t/crash/results/*wedge_repro*_panic*.log`:

```bash
P=$(ls -t test/t/crash/results/*wedge_repro*_panic.log         | head -1)   # primary
R=$(ls -t test/t/crash/results/*wedge_repro*_panic_replica.log | head -1)   # standby

# Primary: oxids the crash recovery aborted in-memory with NO WAL:
grep -a 'recovery-finish-abort-trace' "$P" | grep -oE 'oxid=[0-9]+'

# Standby: the oxid a recovery worker is spinning on (most frequent in the tail):
tail -4000 "$R" | grep -aoE 'oxid_get_csn-call .* oxid=[0-9]+' \
  | grep -oE 'oxid=[0-9]+' | sort | uniq -c | sort -rn | head
```

**Expected:** the standby's spin `oxid=X` is **one of** the primary's `recovery-finish-abort-trace` oxids, and the `handle_conflicts` block repeats endlessly on a single hot `blkno`.

> The `recovery-finish-abort-trace` and `csn-trace` markers are `IS_DEV`-only (`#ifdef USE_INJECTION_POINTS`) `elog`s already in the tree — see `src/recovery/recovery.c:1663`, `src/transam/oxid.c:1648`, `src/btree/modify.c:502`. They are also load-bearing timing scaffolding that widens the race window; a prod build will neither emit them nor reproduce at this rate.

**Note:** wedge logs are large (primary ~1.4 GB, standby ~0.4 GB). Extract the two oxid facts above, then delete them.

---

## 9. Related files

- Background/architecture: `test/t/crash/recovery_livelock_issue.md`, `test/t/crash/streaming_replica_issue.md`, `test/t/crash/tx_flow.md`
- Undo subsystem deep-dive (this session): `test/t/crash/undo_investigation/UNDO_INVESTIGATION.md`
