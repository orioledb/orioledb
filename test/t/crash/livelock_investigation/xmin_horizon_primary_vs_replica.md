# xid-horizon watermarks: primary vs. streaming replica

Scope: **streaming physical replication only.** How OrioleDB's in-memory
transaction-horizon watermarks (`runXmin`, `globalXmin`, `writtenXmin`, …) rise and
fall on the **primary** and on a **streaming standby** when an in-flight
transaction is aborted because the cluster was shut down (SIGKILL) while it was
still running — the orioledb#876 / #889 deferred recovery-finish rollback scenario.

**All numbers in this doc are real**, captured from one instrumented run of
`test.t.replication_test.ReplicationTest.test_recovery_finish_rollback_does_not_regress_replica_xmin`
(traces tagged `XMINTRACE` in `advance_global_xmin`, `update_run_xmin`,
`free_run_xmin`, and the `recovery_finish` abort). The placeholder oxids from the
first draft (52/250) are replaced with the actual values of that run.

### The run, in one table (oxids; timestamps `12:59:NN.mmm`)

| t | node | event | runXmin | globalXmin | writtenXmin | nextXid |
|---|---|---|---|---|---|---|
| 10.949 | primary | `recovery_finish` aborts in-flight **oxid 52** | 52 | 52 | 52 | 253 |
| 10.954 | primary | **`free_run_xmin`**: runXmin → nextXid | **52→253** | 52 | 52 | 253 |
| 15.980 | primary | `advance_global_xmin` (restartpoint `write_xidsmap`) | 255 | **52→255** | →255 | 255 |
| 10.046 | replica | `update_run_xmin` while T alive (T pins recovery_xmin=52) | 4→52 | 52 | 52 | 55 |
| 15.860 | replica | `update_run_xmin` after post-restart commit folds recovery_xmin=254 | **52→254** | 52 | 52 | 255 |
| **16.001** | replica | **TEST QUERIES** `runxmin, globalxmin` | **254** | **52** | 52 | 255 |
| 16.008 | replica | checkpointer starts restartpoint | 254 | 52 | 52 | 255 |
| 16.013 | replica | `advance_global_xmin` (restartpoint `write_xidsmap`) | 254 | **52→254** | →254 | 255 |

The test asserts `master_globalxmin − replica_globalxmin ≤ 1` and reads
`255 − 52 = 203` at **16.001** — *12 ms before* the replica's globalXmin raise at
**16.013** would have made it `255 − 254 = 1` (a pass). **The replica does
converge; the test races the raiser.**

---

## 1. The watermarks (`XidMeta`, `include/transam/oxid.h:25-53`)

| field | meaning |
|---|---|
| `nextXid` | next oxid to hand out. High watermark, monotonically increasing. |
| `runXmin` | oldest oxid still *running* (in-progress). The in-flight floor. |
| `globalXmin` | global visibility horizon — oldest oxid any live snapshot might need. `oxid < globalXmin` is **FROZEN** (always-visible) on the `oxid_get_csn` fast path. |
| `writeInProgressXmin` | transient target while a circular-buffer write is in progress. |
| `writtenXmin` | **frozen watermark**: oxids `< writtenXmin` are written out of the `xidBuffer` circular buffer and their slots recycled; reading `xidBuffer[oxid]` below it is invalid. |
| `cleanedXmin`, `cleanedCheckpoint*`, `checkpointRetain*` | cleanup / checkpoint-retention bookkeeping. |
| `lastXidWhenUpdatedGlobalXmin` | last oxid at which the *periodic* `advance_global_xmin` trigger fired. |

### Ordering invariant
```
cleanedXmin ≤ writtenXmin ≤ writeInProgressXmin ≤ globalXmin ≤ runXmin ≤ nextXid
```
The **safety-critical** link is `writtenXmin ≤ globalXmin`: the fast path
`oxid < globalXmin → FROZEN` is sound only if every oxid that misses it still has a
live `xidBuffer` slot, i.e. `globalXmin ≥ writtenXmin`. A regression of `globalXmin`
below `writtenXmin` makes oxids in `[globalXmin, writtenXmin)` take the slow path
against a recycled slot → mis-read as IN_PROGRESS → the #876 livelock.
`advance_global_xmin` asserts `globalXmin ≥ writtenXmin` (oxid.c:1169-1170).

---

## 2. Who moves each watermark, and in which direction

| function | node / phase | `nextXid` | `runXmin` | `globalXmin` | `writtenXmin` |
|---|---|---|---|---|---|
| `get_current_oxid` (oxid.c) | **primary**, assigning oxids | **↑** | — | (via periodic `advance_global_xmin`) | (via `advance_global_xmin`) |
| `advance_global_xmin(newXid)` (oxid.c:1109) | both (see triggers) | — | — | **↑ only** to `Min(runXmin, procXmins, rewind)` (1155) | **↑** to `globalXmin` (FROZEN-stamps gap under `xidMapWriteLock`, 1176-1186) |
| `write_xidsmap` (oxid.c:792) → `advance_global_xmin(Invalid)` | both, on **checkpoint/restartpoint** or buffer flush | — | — | (raise) | ↑ |
| `advance_oxids(new_xid)` (oxid.c:1233) | recovery leader (both) | **↑** | — | (via `advance_global_xmin`, only while `new_xid ≥ nextXid`) | (idem) |
| `update_run_xmin()` (recovery.c) | recovery leader (both) | — | **= `Min(queue_min∨nextXid, recovery_xmin)`** (↑/↓) | **↓ only** | — |
| `free_run_xmin()` (recovery.c) | **primary crash recovery only** (`recovery_finish`, worker<0) | — | **↑ = `nextXid`** | ↓ only (no-op) | — |
| `read_xids()` (recovery.c) | recovery start (both) | — | seeds via `update_run_xmin` | seeds ↓ to oldest checkpoint in-flight | — |
| `recovery_xmin` fold (COMMIT/ROLLBACK/JOINT_COMMIT) | **replica/recovery** | — | (feeds `update_run_xmin`) | — | — |

Two facts drive everything below:
1. **`globalXmin` and `writtenXmin` can only be RAISED by `advance_global_xmin`.**
   `update_run_xmin` lowers globalXmin; `free_run_xmin` doesn't raise it.
2. **`advance_global_xmin`'s ceiling is `runXmin`** (oxid.c:1128). Its triggers are
   oxid-assignment (`get_current_oxid`/`advance_oxids` — fire only when a *new*
   oxid extends `nextXid`) and `write_xidsmap` (**checkpoints / restartpoints**).
   None of these is fired *by the act of `runXmin` rising.*

---

## 3. Primary: the 52 → 255 bump (real values)

**Phase A — T(52) alive.** T holds oxid 52; `runXmin` pinned to 52; `nextXid`
climbs to 253. Periodic `advance_global_xmin` keeps `globalXmin = Min(52, procs) =
52`. Every COMMIT stamps `finish.xmin = runXmin = 52` into WAL (the value the
replica folds).

**Phase B — SIGKILL.** T never aborts cleanly.

**Phase C — restart → crash recovery.**
- `read_xids` reloads T=52 into `xmin_queue`; seeds `runXmin=globalXmin=52`.
- T has no COMMIT/ROLLBACK → stays INPROGRESS.
- End of redo → **`recovery_finish()`** (recovery.c:1642):
  - `XMINTRACE recovery_finish ABORT in-flight oxid=52 (nextXid=253 runXmin=52 globalXmin=52)` — T aborted in memory; remembered for the deferred `WAL_REC_ROLLBACK`.
  - **`free_run_xmin()`** (recovery.c, called at :1759): `XMINTRACE free_run_xmin runXmin 52->253 (=nextXid) globalXmin=52`. **runXmin lifts to 253; globalXmin is NOT raised here** (`free_run_xmin` only lowers).

**Phase D — primary serving / checkpointing.**
- `XMINTRACE advance_global_xmin RAISE globalXmin 52->255 (runXmin=255 ...)` — once
  `runXmin` is 255 and a raiser fires (here a `write_xidsmap` during a checkpoint),
  globalXmin is pulled up to runXmin; `writtenXmin` follows.

So the bump is **two steps**: `free_run_xmin` lifts `runXmin` (Phase C), then
`advance_global_xmin` lifts `globalXmin` up to it (Phase D). The primary, being live
(assigning oxids + checkpointing frequently), fires the raiser quickly.

---

## 4. Replica: convergence, but lazy (real values)

Counters confirm the structural asymmetry: on the replica
`recovery_finish ABORT = 0` and `free_run_xmin = 0` — **it never runs them**
(continuous recovery; comment at recovery.c:1678-1680).

**Phase A — T(52) alive (streaming).** Replica folds each COMMIT's `finish.xmin =
52` → `recovery_xmin = 52`. `XMINTRACE update_run_xmin runXmin 4->52 ...
recovery_xmin=52`; `advance_global_xmin` then raises `globalXmin` to 52
(on a streamed oxid). State **52 / 52 / 52**.

**Phase B — primary SIGKILL.** Replica frozen at 52/52.

**Phase C — primary ships the deferred rollback; replica applies it + the
post-restart commit.**
- `WAL_REC_XID(52)` then `WAL_REC_ROLLBACK(52, finish.xmin=Invalid)`: the row fix's
  `deferred_rollback` branch — `recovery_xmin` not folded (sentinel), 52 dropped
  from `xmin_queue`, T aborted on the replica. **This is the only moment the replica
  learns T aborted.**
- The post-restart `UPDATE` + `COMMIT(finish.xmin=254)`: `recovery_xmin = 254`;
  finalization → `XMINTRACE update_run_xmin runXmin 52->254 ... recovery_xmin=254
  queue_empty=1`. **runXmin lifts to 254; globalXmin stays 52** (`update_run_xmin`
  is lower-only).

**Phase D — the raiser fires at the next restartpoint.**
- **16.001** the test queries → `runXmin=254, globalXmin=52` (it's in the gap).
- **16.008** the checkpointer (pid was the checkpointer in the run) begins a
  restartpoint — triggered by the primary's CHECKPOINT WAL.
- **16.013** that restartpoint runs `write_xidsmap` →
  `XMINTRACE advance_global_xmin RAISE globalXmin 52->254 (runXmin=254 ...)`.
  **globalXmin finally converges to 254** (= primary 255 − 1). This is the last
  globalXmin event; 254 is the replica's final value.

### The crux of the asymmetry
Both nodes lift `runXmin` to ~254/253 (primary via `free_run_xmin`; replica via
`update_run_xmin`/`recovery_xmin`). Both rely on `advance_global_xmin` — the only
globalXmin raiser — to pull `globalXmin` up. The difference is *when the raiser
fires relative to the `runXmin` lift*:

- **Primary:** live node, fires the raiser constantly (oxid assignment +
  checkpoints) → catches up in tens of ms.
- **Replica:** no oxid assignment; the raiser fires only on (a) a *newly streamed*
  oxid — but `advance_oxids` reads `runXmin` on the `WAL_REC_XID`, *before* the
  matching COMMIT lifts it — or (b) a **restartpoint** (`write_xidsmap`). After
  `runXmin` rises at 15.860, the next trigger is the restartpoint at 16.013. The
  test queries at 16.001, in the **151 ms gap**, and reports the transient lag.

So the #889 failure the test reports (`203`) is a **transient** state of a
**lazy-but-real** convergence — *not* a permanent stuck horizon. On a busy replica,
the next streamed oxid (after `runXmin` rose) or the next restartpoint closes the
gap; an idle replica closes it at the next restartpoint.

---

## 5. Invariants, per node

### Universal (safety; must always hold)
- `writtenXmin ≤ globalXmin` — else FROZEN slots are mis-read (#876 livelock).
- `globalXmin ≤ runXmin ≤ nextXid`; `cleanedXmin ≤ writtenXmin ≤ writeInProgressXmin ≤ globalXmin`.
- `globalXmin`/`writtenXmin` are monotonically non-decreasing except where a
  newly-discovered older in-flight oxid legitimately lowers them (`read_xids` seed;
  `update_run_xmin` lowering for a fresh in-flight oxid).

### Primary (maintained)
- `globalXmin` tracks `runXmin` *promptly* (raiser fires continuously).
- After `recovery_finish` with no surviving in-flight txn: `runXmin = nextXid`, and
  `globalXmin` reaches `nextXid` at the next raiser firing.
- `globalXmin = Min(runXmin, live proc xmins, rewind floor)` — reflects the true
  oldest snapshot.

### Replica (weaker — convergence holds but is LAZY)
- `globalXmin ≤ runXmin` always holds, but `globalXmin` **lags `runXmin` until the
  next raiser firing** (new streamed oxid *after* the lift, or a restartpoint). The
  lag is bounded by that interval, not unbounded — but it is *non-zero and not tied
  to the lift event*.
- `runXmin` is driven by `recovery_xmin = max(finish.xmin stamped by the primary)`,
  floored below by the replica-local `xmin_queue`.
- The replica never runs `free_run_xmin`; the "all in-flight aborted ⇒ lift to
  `nextXid`" that the primary gets for free does not exist — the replica reaches the
  same `runXmin` only via the post-restart commit's `recovery_xmin` fold.

### The #889 defect, restated
The missing property is **"`globalXmin` is raised toward `runXmin` *at the moment
`runXmin` rises*"** — currently the raise is deferred to the next oxid-assignment /
restartpoint, so a query in between (the test) observes a large transient lag. A fix
must trigger the raise right after `runXmin` rises, **bounded by the worker-applied
horizon** so it never advances past an oxid whose undo a recovery worker has not yet
applied (the SK-safety bound — see the worker-bounded-raise plan). Note an *eager*
unbounded raise is exactly what reintroduced the SK desync on the primary's parallel
crash recovery.
