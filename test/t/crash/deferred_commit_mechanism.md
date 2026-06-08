# Deferred commit finalization in OrioleDB parallel recovery

How a transaction's **commit sequence number (CSN)** is assigned during parallel
WAL recovery / streaming-standby replay, why it is *deferred*, and how the
**recovery leader** and **parallel workers** cooperate to do it. This is the
machinery behind the `finished_list`, and the substrate of the
`csn-incremented` standby recovery-livelock (see
[`streaming_replica_issue.md`](./streaming_replica_issue.md) and the
`CAUGHT_livelock_*` evidence).

> Line numbers are from `src/recovery/recovery.c` in this branch's working tree
> (with the investigation's `csn-trace` instrumentation, so they sit a few lines
> below the committed `200073b5`). Function names are the stable references.

---

## Summary

During parallel recovery, OrioleDB does **not** assign a committed transaction's
final CSN at the moment it replays the commit record. Instead it **defers**:
the transaction is parked on a `finished_list` and left `INPROGRESS`, and a
later **leader-only** pass (`update_proc_retain_undo_location`) stamps the real
CSN — but only for transactions whose commit LSN is at or below the
**synchronized replay point** (the slowest worker's position). This guarantees
CSNs are assigned in commit order across out-of-order parallel apply. The cost:
a committed transaction is briefly visible as `INPROGRESS`, and its finalization
depends on *every* worker advancing past its commit LSN.

---

## 1. The two roles — leader vs. workers (`worker_id`)

Parallel recovery splits into:

| Role | `worker_id` | Job |
|---|---|---|
| **Leader** (startup / main recovery process) | **`< 0`** (i.e. `-1`) | reads the WAL stream, dispatches each row-modify record to a worker (by key hash), processes `WAL_REC_XID` / `WAL_REC_COMMIT` / `WAL_REC_ROLLBACK` / synchronization records, **and is the only process that assigns final CSNs** |
| **Workers** | `0, 1, 2, …` | each owns a shard of the key space; applies the modify records dispatched to it; advances its own `commitPtr` |

`worker_id < 0` is the canonical "am I the leader?" test, used throughout
`recovery.c` (e.g. `recovery.c:2596`, `:2021`, `recovery_finish` at `:1637`).
Single-process recovery runs entirely as the leader (`worker_id < 0`).

Because workers apply different keys concurrently, **modify records are applied
out of WAL order across workers** — but commits must still be ordered. That
tension is what makes deferral necessary.

---

## 2. The CSN lifecycle of one oxid during recovery

```
  modify records applied        WAL_REC_COMMIT replayed         leader drain
  (worker, INPROGRESS)   ─────►  (pushed to finished_list,  ───►  (CSN stamped,
                                  still INPROGRESS)                 visible committed)
        xidBuffer[oxid] = INPROGRESS  ........................  set_oxid_csn(real CSN)
```

Crucially, replaying the **commit record does not change `xidBuffer[oxid]`** — it
only enqueues the oxid on `finished_list`. Until the leader drains it,
`oxid_get_csn(oxid)` returns `INPROGRESS`. Any concurrent modify that conflicts
with that oxid therefore sees it as in-progress (this is the hook the livelock
hangs on).

---

## 3. The deferral — `recovery_finish_current_oxid()` (`recovery.c:1967`)

Called when the leader replays a `WAL_REC_COMMIT` / `WAL_REC_ROLLBACK`. It
branches on `sync`:

- **`sync == true`** (single-process or a synchronous point) — finalize **now**
  (`recovery.c:1978`): run precommit/on-commit undo, then
  `set_oxid_csn(oxid, fetch_add(nextCommitSeqNo))`. No deferral.

- **`sync == false`** (the parallel path) — **defer**:
  - **commit** (`!COMMITSEQNO_IS_ABORTED(csn)`, `recovery.c:1994`): run
    precommit/on-commit undo + `walk_checkpoint_stacks`, then
    `in_finished_list = true` and `dlist_push_tail(&finished_list, …)`
    (`:2004`/`:2010`). **No CSN assigned yet.** (This is the `commit-defer` push
    in the `finished-push` trace.)
  - **abort** (`recovery.c:2008`): apply the undo stack; then, *if* leader and
    not `sync`, also defer the abort onto `finished_list` (`:2035`/`:2041`,
    the `abort-defer` push) — postponed "until it will be aborted by all the
    workers" so a worker can't transiently read it as committed via `runXmin`.

So both commits **and** aborts can ride the same `finished_list`; the entry's
`csn` field records which it is (`MAX_NORMAL-1` placeholder for commit,
`COMMITSEQNO_ABORTED` for abort).

---

## 4. The `finished_list` data structure

- `static dlist_head finished_list;` (`recovery.c:240`) — a **per-process**
  list. The leader has one; **each worker has its own**. They are independent
  in-memory lists, not shared.
- **Ordered by commit LSN** — entries are `dlist_push_tail`'d in the order the
  leader processes commit records, i.e. WAL order.
- Each node is a `RecoveryXidState` (`finished_list_node`, `in_finished_list`,
  `csn`, `ptr` = the commit LSN).
- The shared atomic `recovery_finished_list_ptr` (`recovery.c:661`) records how
  far finalization has progressed and is read by
  `recovery_get_effective_replay_ptr()`.

Why per-process: each process must track *its own* retain-undo locations for the
transactions it has seen (so undo isn't reclaimed underneath it), but **only the
leader's drain assigns the globally-visible CSN**.

---

## 5. The finalization — `update_proc_retain_undo_location()` (`recovery.c:2548`)

Called by leader and workers at synchronization / retain-update points. The core
is the `finished_list` drain (`recovery.c:2583`):

```c
if (worker_id < 0)                                  // recovery.c:2578
    listPtr = recoveryPtr = recovery_get_current_ptr();  // leader: synchronized point
else
    listPtr = pg_atomic_read_u64(recovery_finished_list_ptr);  // worker

dlist_foreach_modify(miter, &finished_list) {
    state = dlist_container(...);
    if (state->ptr > listPtr)                       // recovery.c (the STOP)
        break;                                      //   stop at first commit beyond listPtr

    if (worker_id < 0) {                            // recovery.c:2596 — LEADER ONLY
        if (!COMMITSEQNO_IS_ABORTED(state->csn)) {
            set_oxid_csn(state->oxid, COMMITSEQNO_COMMITTING);
            state->csn = pg_atomic_fetch_add_u64(&...nextCommitSeqNo, 1);
            set_oxid_csn(state->oxid, state->csn);  //   ← assign the real CSN
            set_oxid_xlog_ptr(state->oxid, state->ptr);
        } else {
            set_oxid_csn(state->oxid, COMMITSEQNO_ABORTED);
        }
    }
    dlist_delete(miter.cur);                        // remove from this list
    state->in_finished_list = false;
}
```

Two invariants make this correct **and** make it the bug's choke point:

1. **Ordered, gated drain.** The loop walks entries in commit-LSN order and
   `break`s at the first one whose `ptr > listPtr`. So finalization only ever
   advances up to `listPtr`; nothing past it is finalized, even if later entries
   would otherwise be eligible.
2. **Leader-only CSN assignment.** Only `worker_id < 0` runs `set_oxid_csn`.
   Workers iterate their own list (for retain bookkeeping and to drop nodes) but
   **cannot finalize a CSN**. A committed-deferred oxid stays `INPROGRESS` until
   the *leader* drains it.

---

## 6. Leader ↔ worker interaction — the synchronized replay point

The `listPtr` the leader drains up to is **not** an arbitrary cursor — it is the
**slowest worker's replay position**:

- `recovery_get_current_ptr()` (`recovery.c:1321`) → `get_workers_commit_ptr()`
  (`recovery.c:1277`) returns **`MIN` over all workers of `worker_ptrs[i].commitPtr`**
  (lock-free, cached on a change-counter).
- Each worker advances its `commitPtr` as it finishes applying records up to a
  given LSN; the leader drives barriers via `workers_synchronize()`
  (`recovery.c:694`).

Putting it together, the cooperation is:

```
   leader: reads WAL ──► dispatches modifies to workers (by key hash)
                    └──► replays a COMMIT  ──► defers it onto finished_list
   workers: apply their shard of modifies ──► advance commitPtr

   synchronized replay point  =  MIN(worker commitPtr)
   leader drain finalizes every finished_list commit with ptr ≤ that point,
       assigning CSNs in commit order, and stops at the first commit beyond it.
```

So a deferred commit's CSN is stamped **only once every worker has replayed past
that commit's LSN**. That is the ordering guarantee — and the dependency that
can deadlock: if any single worker stalls (e.g. busy-spinning on a conflict
against an `INPROGRESS` oxid), the `MIN` (`listPtr`) freezes, the leader's drain
stalls at the first un-finalizable entry, and the very oxid the worker is
waiting on never gets its CSN. (See §8.)

---

## 7. End of recovery — `recovery_finish()` (`recovery.c:1637`)

When recovery *ends*, `recovery_finish()` makes a final sweep of the
`recovery_xid_state_hash`:

- in-flight (`COMMITSEQNO_IS_INPROGRESS`) oxids are **aborted in memory**
  (and — post-`fb1a8acc` — emit `WAL_REC_ROLLBACK` so a standby can resolve them);
- any oxid still `in_finished_list` is finalized here too (`worker_id < 0`
  branch, `recovery.c:1751`-area): `set_oxid_csn(COMMITTING)` → fetch-add →
  `set_oxid_csn(real csn)`.

**A continuous streaming standby never reaches `recovery_finish()`** (its
recovery never ends). So on a standby, the *only* path that finalizes a
deferred commit is the leader's runtime drain (§5) — there is no end-of-recovery
backstop.

---

## 8. Failure mode (why this matters)

The deferred-commit design is correct under normal replay (workers keep
advancing, barriers drain `finished_list` promptly). It deadlocks when a worker
**cannot** advance:

1. A worker replays a modify that conflicts with a **committed-deferred** oxid;
   `oxid_get_csn` returns `INPROGRESS` (not yet finalized).
2. Recovery workers run `o_btree_modify` with `waitCallback = NULL`, so they
   **busy-retry** in `o_btree_modify_handle_conflicts()` (`src/btree/modify.c:435`)
   instead of yielding.
3. The stuck worker's `commitPtr` freezes → the synchronized replay point
   (`MIN` of worker `commitPtr`s) freezes.
4. The leader's drain (§5) can therefore never advance `listPtr`, so it never
   finalizes the conflicting oxid's CSN — it sits forever `STOP(beyond-listPtr)`
   on the first un-finalizable entry.
5. → circular wait: the worker needs the CSN that only the (frozen) leader can
   assign; the leader is frozen because the worker won't yield.

This is the `csn-incremented` standby livelock, captured directly in the traces
(`conflict-inprogress` with a frozen `replayPtr`, `finished-drain` stuck at one
oxid, `finished-push path=commit-defer` for the spun-on oxids). It is a property
of parallel recovery on **any** primary crash, not of the injection itself.

---

## Code references

Roles & the leader test:
- `src/recovery/recovery.c:1637` — `recovery_finish(int worker_id)`
- `src/recovery/recovery.c:2596`, `:2021`, `:1751` — `if (worker_id < 0)` (leader-only)

Deferral (push onto `finished_list`):
- `src/recovery/recovery.c:1967` — `recovery_finish_current_oxid(csn, ptr, worker_id, sync)`
- `:1978` — `sync` path: immediate CSN assignment
- `:1994`–`:2010` — `!sync` **commit** defer (`in_finished_list = true`, `dlist_push_tail`)
- `:2008`–`:2041` — `!sync` **abort** defer

Structure:
- `src/recovery/recovery.c:240` — `static dlist_head finished_list` (per-process)
- `:661` — `recovery_finished_list_ptr` (shared finalization watermark)

Finalization (drain) & synchronization:
- `src/recovery/recovery.c:2548` — `update_proc_retain_undo_location(int worker_id)`
- `:2583` — `dlist_foreach_modify(miter, &finished_list)` (the drain loop)
- `:2596` — leader-only `set_oxid_csn(...)` (the actual CSN stamp)
- `:1277` — `get_workers_commit_ptr()` (MIN of worker `commitPtr`s)
- `:1321` — `recovery_get_current_ptr()` (the leader's `listPtr` source)
- `:694` — `workers_synchronize()` (the barrier)

Conflict / spin tie-in:
- `src/btree/modify.c:435` — `o_btree_modify_handle_conflicts()` (busy-retry on `INPROGRESS`)
- `src/transam/oxid.c:1648` — `oxid_get_csn()` (returns `INPROGRESS` for un-finalized commits)
