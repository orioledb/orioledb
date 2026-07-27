# recovery-idxbuild-registration-fallback-bug

## Focus

Resource Boundaries / Infrastructure faults — filling the gap flagged by
evaluation: "no property covers ... process-count exhaustion." This targets
`max_worker_processes` (background-worker slot) exhaustion specifically during
crash recovery's parallel WAL-apply worker pool startup, a mechanism entirely
distinct from the two existing resource-boundary properties in the catalog
(`multi-insert-undo-capacity-invariant`, `undo-wraparound-retry-cap`), which
are both about the undo buffer, not process/worker-slot exhaustion.

## What led to this

`src/recovery/recovery.c` registers two logically separate pools of dynamic
background workers at the start of multi-process recovery: the main
"recovery worker" pool (`recovery_first_worker`..`recovery_last_worker`) and,
if `orioledb.recovery_pool_size` (recovery_idx_pool_size_guc) is nonzero, an
"index build" pool nested under a dedicated `index_build_leader` worker
(`index_build_first_worker`..`index_build_last_worker`). Both pools are
registered via `RegisterDynamicBackgroundWorker()`, which returns `NULL` in
`handle` when Postgres has no free background-worker slot left (i.e.
`max_worker_processes` is exhausted) — see `recovery_worker_register()`
(`src/recovery/worker.c:164-194`).

The index range macros are defined in `include/catalog/indices.h:24-29`:

```c
#define recovery_first_worker      (0)
#define recovery_last_worker       (recovery_pool_size_guc - 1)
#define index_build_leader         (recovery_pool_size_guc)
#define index_build_first_worker   (recovery_pool_size_guc + 1)
#define index_build_last_worker    (recovery_pool_size_guc + recovery_idx_pool_size_guc - 1)
```

Critically, `index_build_last_worker` is a **macro**, re-evaluated against
the *current* value of `recovery_idx_pool_size_guc` every time it's used —
not a value captured once. This matters because the index-build pool's own
registration loop mutates `recovery_idx_pool_size_guc` mid-loop (see below).

### The main recovery-worker pool's fallback (correct, the reference pattern)

`orioledb_recovery_worker_pool_start()`-adjacent code, `src/recovery/recovery.c:1097-1139`:

```c
if (!recovery_single)
{
    int  finish = recovery_idx_pool_size_guc ? index_build_leader : recovery_last_worker;
    workers_pool = palloc0(sizeof(RecoveryWorkerState) * (finish + 1));
    for (i = recovery_first_worker; i <= finish; i++)
    {
        state = &workers_pool[i];
        ...
        workers_pool[i].handle = recovery_worker_register(i);
        if (workers_pool[i].handle == NULL)
        {
            for (i--; i >= 0; i--)
                TerminateBackgroundWorker(workers_pool[i].handle);
            recovery_single = *recovery_single_process = true;
            finish = -1;
            ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                     errmsg("unable to start recovery workers"),
                     errdetail("You must increase max_worker_processes value "
                               "or decrease orioledb.recovery_pool_size value. "
                               "Fallback to recovery in single-process mode.")));
            break;                      /* <-- KEY: explicit break */
        }
        state->queue = shm_mq_attach(GET_WORKER_QUEUE(i), NULL, workers_pool[i].handle);
        state->queue_buf_len = 0;
    }
    for (i = recovery_first_worker; i <= finish; i++)
    {
        if (shm_mq_wait_for_attach(workers_pool[i].queue) != SHM_MQ_SUCCESS)
            elog(ERROR, "unable to attach recovery workers to shm queue");
        recovery_send_init(i);
    }
}
```

On registration failure this: (a) terminates every already-registered worker
in the pool, (b) sets `recovery_single = true` (a documented, deliberate
"run recovery entirely in the startup process" degradation mode — the same
mode used after a fatal error, per `recovery_single = *recovery_single_process
= IsFatalError()` a few lines above), (c) sets `finish = -1` so the
wait-for-attach loop that follows does nothing, and (d) **`break`s out of the
registration loop immediately**, so no further code in the loop body runs
using the now-inconsistent `i`. This is a clean, self-consistent fallback.

### The index-build pool's fallback (missing the `break`, and off-by-one on top)

`orioledb_recovery_worker_main()`-adjacent code (worker_id == index_build_leader
branch), `src/recovery/recovery.c:1607-1646`:

```c
if (worker_id == index_build_leader)
{
    workers_pool = palloc0(sizeof(RecoveryWorkerState) * (recovery_idx_pool_size_guc + recovery_pool_size_guc));
    for (i = index_build_first_worker; i <= index_build_last_worker; i++)
    {
        state = &workers_pool[i];
        ...
        workers_pool[i].handle = recovery_worker_register(i);
        if (workers_pool[i].handle == NULL)
        {
            for (i--; i >= index_build_first_worker; i--)
                TerminateBackgroundWorker(workers_pool[i].handle);
            recovery_idx_pool_size_guc = 1;
            ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                     errmsg("unable to start recovery workers"),
                     errdetail("You must increase max_worker_processes value "
                               "or decrease orioledb.recovery_idx_pool_size value. "
                               "Fallback to index build in single-process mode.")));
            /* NO break here, unlike the main pool's loop above */
        }
        state->queue = shm_mq_attach(GET_WORKER_QUEUE(i), NULL, workers_pool[i].handle);
        state->queue_buf_len = 0;
    }
    for (i = index_build_first_worker; i <= index_build_last_worker; i++)
    {
        if (shm_mq_wait_for_attach(workers_pool[i].queue) != SHM_MQ_SUCCESS)
            elog(ERROR, "unable to attach recovery workers to shm queue");
        recovery_send_init(i);
    }
}
```

Worked through step by step, assuming registration fails at some
`i = idx_fail` (with one or more workers already successfully registered
before it):

1. The inner cleanup loop `for (i--; i >= index_build_first_worker; i--)`
   reuses the *same* loop variable `i` as the outer loop (there is no
   shadowing — both are the same `int i` declared once at the top of the
   enclosing function). After this inner loop runs to completion, `i` has
   been driven down to `index_build_first_worker - 1`, which is exactly
   `index_build_leader`'s own index (`index_build_first_worker` is defined as
   `recovery_pool_size_guc + 1`, and `index_build_leader` is
   `recovery_pool_size_guc`).
2. `recovery_idx_pool_size_guc = 1` is set. But because
   `index_build_last_worker` is a macro
   (`recovery_pool_size_guc + recovery_idx_pool_size_guc - 1`), this
   immediately changes the outer loop's own bound: it becomes
   `recovery_pool_size_guc + 1 - 1 = recovery_pool_size_guc =
   index_build_first_worker - 1`. Setting the "pool size" to `1` does **not**
   yield one valid worker index in this macro's arithmetic — it yields an
   *empty* range (`index_build_last_worker < index_build_first_worker`),
   because the macro already subtracts 1. To actually get exactly one
   surviving index-build worker slot, the fallback would need to set
   `recovery_idx_pool_size_guc = 2`, not `1` — this looks like a plain
   off-by-one in the fallback value itself, independent of the missing
   `break`.
3. There is **no `break`**, so execution falls through past the `if` block to
   `state->queue = shm_mq_attach(GET_WORKER_QUEUE(i), NULL, workers_pool[i].handle);`.
   But `state` still points at `&workers_pool[idx_fail]` (set at the *top* of
   this same outer-loop iteration, before `i` was mutated by the inner
   cleanup loop), while `i` itself is now `index_build_leader`'s index. So
   this line actually executes, in effect:
   `workers_pool[idx_fail].queue = shm_mq_attach(GET_WORKER_QUEUE(index_build_leader), NULL, workers_pool[index_build_leader].handle);`
   — it attaches the **leader's own dedicated queue** (using
   `workers_pool[index_build_leader].handle`, which was never populated by
   this loop and is `NULL` from the `palloc0`), and stores the resulting
   `shm_mq_handle` into the **wrong pool slot** (`idx_fail`'s slot, not the
   leader's).
4. The outer `for` loop then increments `i` back up to
   `index_build_first_worker`, and re-checks
   `i <= index_build_last_worker`, which (per point 2) is now
   `index_build_first_worker <= index_build_first_worker - 1` — false. The
   loop exits. So control flow does eventually terminate, but only after the
   one stray `shm_mq_attach` call in point 3 already ran.
5. The second loop (`for (i = index_build_first_worker; i <=
   index_build_last_worker; i++) { shm_mq_wait_for_attach(...); ... }`) also
   sees the now-empty range and **does not execute at all** — meaning
   `recovery_send_init()` is never called for any index-build sub-worker,
   even though `recovery_idx_pool_size_guc` is nonzero (`1`) and downstream
   code (`Assert(recovery_idx_pool_size_guc)`-guarded branches at
   `recovery.c:3255-3257,4867-4869`, and the leader-dispatch logic that
   assumes an index-build pool exists whenever `recovery_idx_pool_size_guc >
   0`) may still believe a pool of size 1 exists and try to dispatch
   index-build work to it.

## Property

| | |
|---|---|
| **Type** | Safety / Liveness (concrete control-flow bug, directly confirmed by reading; end-to-end runtime consequence not yet empirically reproduced) |
| **Property** | When Postgres's background-worker slots (`max_worker_processes`) are exhausted partway through registering the parallel index-build worker sub-pool during crash recovery, the leader falls back to a self-consistent single-process (or reduced-pool) index-build mode — analogous to, and exactly as safe as, the main recovery-worker pool's already-correct fallback (`recovery_single = true` + `break`) — rather than leaving `workers_pool` state inconsistent, corrupting an unrelated pool slot's `queue` field, or later attempting to dispatch index-build work to a worker pool whose computed valid-index range is empty. |
| **Invariant** | `Always(index_build_pool_state_consistent_after_registration_failure)`: after any WARNING-logged index-build worker registration failure, assert (a) `recovery_idx_pool_size_guc` and the macro-derived `index_build_last_worker` describe a non-negative-size range consistent with what was actually registered, and (b) no `workers_pool[i].queue` for `i != index_build_leader` was ever assigned a `shm_mq_handle` obtained via `GET_WORKER_QUEUE(index_build_leader)`. Complemented by `Reachable("index-build worker registration fallback taken")` since this path has zero visibility today and no existing config forces it. |
| **Antithesis Angle** | Configure a low `max_worker_processes` relative to `orioledb.recovery_pool_size + orioledb.recovery_idx_pool_size` (both GUCs are directly settable) so that recovery worker registration is guaranteed to exhaust available slots partway through the index-build sub-pool specifically (letting the main recovery pool register fully first makes this deterministic: set `recovery_pool_size` to fit, `recovery_idx_pool_size` to overflow). Trigger a crash with a concurrent `CREATE INDEX`/parallel-index-build-eligible DDL in flight so recovery actually attempts to spin up the index-build pool. This requires no fault injection beyond config + a crash — a pure resource-limit workload, exactly the "process-count exhaustion" gap the evaluation flagged. |
| **Why It Matters** | The main recovery-worker pool already has a correct, deliberate degradation path for exactly this resource limit (worker-slot exhaustion), showing the team designed for it — but the structurally near-identical index-build pool's copy of that logic is missing the loop-terminating `break` and uses an off-by-one fallback value, so the same failure mode that's handled safely in one pool is handled inconsistently in its sibling. The plausible failure surface ranges from relatively benign (a stray, harmless write to an unused struct field) to a stuck/hung recovery leader (if later code dispatches index-build work assuming a pool that was never actually attached) — either way, a resource-exhaustion condition this codebase clearly intended to handle gracefully is not handled identically on both paths, and nothing in the existing test suite or Antithesis harness exercises `orioledb.recovery_idx_pool_size` against a constrained `max_worker_processes` at all. |

**Open Questions:**

- Does any later code path (index-build work dispatch to the leader's sub-workers, e.g. around `recovery.c:3255-3262` or `4867-4874`) actually get reached with `recovery_idx_pool_size_guc == 1` but zero attached workers, and if so does it hang (waiting on a queue nobody drains), error out, or silently no-op? `(needs further investigation — the dispatch-side code that assumes the index-build pool exists was not traced to a conclusion this pass)`
- Is the stray `shm_mq_attach(GET_WORKER_QUEUE(index_build_leader), NULL, ...)` call (point 3 above) ever consequential, or is `workers_pool[index_build_leader]`'s own `.queue`/handle state re-established correctly elsewhere later, making this a harmless clobber of an already-unused slot? `(needs further investigation)`
- Does `RegisterDynamicBackgroundWorker()`'s failure mode ever partially succeed (return a non-NULL handle for a worker that nonetheless never actually starts), which would invalidate the "at most one registration failure per loop" framing this analysis assumes? Not traced — assumed to behave as documented (all-or-nothing per call).

## SUT-side instrumentation

`existing-assertions.md`: 0 hits in `src/recovery/` — this path has no
Antithesis SDK instrumentation today (**missing**). Suggested (both
**missing**):
- `Reachable("recovery index-build worker registration failed, falling back", {failed_index, recovery_idx_pool_size_guc_before, recovery_idx_pool_size_guc_after})` at the `ereport(WARNING, ...)` call site (`recovery.c:1631-1634`) — gives positive confirmation the interesting resource-exhaustion path was ever exercised at all, since today it's invisible from outside.
- `Always(workers_pool_queue_index_matches_handle_owner)` — would need a lightweight assertion co-located with the `state->queue = shm_mq_attach(...)` line checking that the `state` pointer and the `i`-derived `GET_WORKER_QUEUE(i)`/`workers_pool[i].handle` arguments refer to the same slot, catching the exact mismatch described above directly at the point it occurs rather than inferring it from a later hang.

### Investigation Log

#### Is this reachable in a real deployment, or does `orioledb.recovery_idx_pool_size` default to 0, disabling the path?

- Examined: `src/orioledb.c:724-748` (`DefineCustomIntVariable` for both
  `orioledb.recovery_pool_size` and `orioledb.recovery_idx_pool_size`); grepped
  `test/antithesis/` for either GUC name or `max_worker_processes`.
- Found: both GUCs default to **3** (min 1, max 128, `PGC_POSTMASTER`) — the
  index-build pool is active by default, not an opt-in feature that needs
  deliberate enabling. No file under `test/antithesis/` sets either GUC or
  `max_worker_processes` — every existing config runs with defaults (3 + 3 = 6
  recovery-related dynamic workers needed on top of whatever `max_worker_processes`
  the base Postgres config leaves available).
- Conclusion: the buggy fallback branch is reachable under default
  configuration purely by constraining `max_worker_processes` low enough
  (or by raising `recovery_pool_size`/`recovery_idx_pool_size`) — no unusual
  or opt-in GUC combination is required. This raises this property's priority:
  it's not a dormant, hard-to-reach edge case.
