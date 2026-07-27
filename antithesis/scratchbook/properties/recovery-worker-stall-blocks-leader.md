# recovery-worker-stall-blocks-leader

## Focus

Resource Boundaries. Targets `src/recovery/` (shm_mq-based recovery queues),
named explicitly in this pass's assignment, and sharpens the backpressure
angle already gestured at in `sut-analysis.md` §1 ("Ordering/partitioning
across queues and backpressure... is an internal RPC-like boundary that can
break under injected scheduling delays or partial process kills mid-replay")
into a specific, checkable mechanism.

## What led to this

The recovery leader (startup process or a designated worker acting as
leader) distributes decoded WAL records to `recovery_pool_size_guc` worker
processes over fixed-size `shm_mq` queues (`orioledb.recovery_queue_size`,
default 1024 KB / min 512 KB per worker, `src/orioledb.c:706-716`). Two
distinct leader-side wait mechanisms depend on individual workers making
progress, and neither has a bound tied to *worker liveness* (only to worker
*process existence*):

1. **`worker_queue_flush()`** (`src/recovery/recovery.c:5083-5099`) calls
   `shm_mq_send(state->queue, state->queue_buf_len, state->queue_buf, false, true)`
   — `nowait = false` means this **blocks** until the receiving worker drains
   enough queue space (standard, correct `shm_mq` backpressure — the queue
   itself can't grow past `recovery_queue_size_guc`). But it blocks the
   *leader*, not just that one worker's lane — while blocked sending to
   worker `i`, the leader isn't distributing records to any other worker
   either, since distribution is sequential per the call sites (e.g.
   `recovery.c:4914,4950,4999,5031,5077` all loop `for (i = 0; i <
   recovery_pool_size_guc; i++) { worker_send_msg(...); }`, implying
   `worker_queue_flush` for worker `i` happens in-line with distributing to
   worker `i+1`).
2. **`workers_synchronize()`** (`src/recovery/recovery.c:5016-5060`), used
   e.g. before deleting a relnode, waits per-worker:
   ```c
   while (pg_atomic_read_u64(&worker_ptrs[i].commitPtr) < ptr && workers_pool[i].queue)
   {
       pg_usleep(10);
       if (j % 100 == 0)
       {
           status = GetBackgroundWorkerPid(workers_pool[i].handle, &pid);
           if (status != BGWH_STARTED && status != BGWH_NOT_YET_STARTED)
           { unexpected_worker_detach = true; break; }
       }
       j++;
   }
   ```
   This is a **tight busy-poll** (10 *microsecond* sleep, not millisecond —
   note this differs in granularity from the `QUEUE_READ_USLEEP_*` exponential
   backoff on the worker's own read side, `worker.c:48-50`, which starts at
   10us but backs off up to ~10ms; here there's no backoff at all across the
   whole wait). The only escape hatch is detecting the worker process has
   fully exited (`GetBackgroundWorkerPid` status check, every 100 iterations
   = roughly every 1ms) — **there is no detection of, or bound on, a worker
   that is alive but stuck** (blocked on a lock, blocked on an S3 fetch if
   the table involved is S3-backed, or simply CPU-starved by a scheduling
   fault). No `CHECK_FOR_INTERRUPTS()` is visible in this loop either, so the
   calling process (startup process or a worker acting as leader) cannot be
   cancelled out of this wait.

## Why this matters as a resource-boundary (not just a concurrency) finding

The queue itself is correctly bounded (fixed `shm_mq` size, standard
Postgres backpressure) — there's no unbounded queue growth here. The
resource-boundary angle is specifically: **a single stalled recovery worker
converts a bounded, per-worker queue-full condition into an unbounded,
leader-wide stall**, because the leader's distribution loop and its
synchronization barrier both wait on workers one at a time with no fan-out
timeout and no interrupt path. This means the "capacity" that actually
matters in practice isn't the queue's byte size, it's each worker's
*liveness*, and there's no circuit breaker for a live-but-wedged worker —
only for a dead one.

This composes directly with other findings already surfaced in
`sut-analysis.md`:
- §3: "Recovery worker parallel index build: workers wait on
  `recovery_index_cv` for an index-build leader; no timeout observed in that
  wait path" — a symmetric case (workers waiting on the leader) to this one
  (leader waiting on a worker); together they suggest the recovery
  leader/worker protocol generally lacks a wedged-peer detection mechanism in
  either direction, beyond simple process-exit detection.
- §1: the general observation that recovery's queue-based fan-out is an
  "internal RPC-like boundary" — this property gives it a concrete failure
  mechanism (leader-side blocking send/wait, no liveness timeout) rather
  than a general statement.

## Property

| | |
|---|---|
| **Type** | Liveness |
| **Property** | If one recovery worker among `recovery_pool_size_guc` becomes slow or wedged (without exiting) — e.g., due to lock contention, S3 I/O stall on an S3-backed table, or CPU starvation — the recovery leader's overall replay progress stalls in proportion to that one worker's stall, with no timeout-based detection or corrective action distinct from full process-exit detection. |
| **Invariant** | `Sometimes(single_worker_stall_detected_and_bounded)` would be the desirable liveness claim, but as implemented today there is no such detection — so the more honest, currently-checkable framing is `Reachable("leader blocked in worker_queue_flush/workers_synchronize past N seconds while the target worker's PID is still alive")`, i.e., first confirm the stall condition is reachable and observable at all (it currently isn't, from outside — see instrumentation below), before a stronger `Always`/`Sometimes` claim about bounded recovery can be asserted meaningfully. |
| **Antithesis Angle** | Inject a stall on exactly one recovery worker (CPU throttling / `SIGSTOP`-then-resume / scheduling delay targeting one specific worker PID, or — if the workload includes an S3-backed table under recovery — a slow/faulted S3 response specifically timed to land while that worker is mid-replay) while the leader is actively distributing records to all workers, then observe whether overall recovery throughput (not just that one worker's lane) degrades and for how long. |
| **Why It Matters** | Recovery/replication is already identified as the single largest Antithesis coverage gap (`sut-analysis.md` §9, point 1) and the hottest, most bug-dense area of the codebase by commit count (§8: `recovery.c` is the single hottest file at 159 commits). A mechanism by which one slow worker silently degrades whole-cluster recovery throughput — with no distinguishing signal from "recovery is just naturally slow" — would be hard to diagnose in production and is a legitimate resource/capacity-planning concern for parallel-recovery deployments generally, independent of any single correctness bug. |

**Open Questions:**

- Is `workers_synchronize()`'s tight busy-poll (10us sleep, no backoff) called frequently enough in normal operation (the comment says "Actually used only before delete a relnode... we assume that it does not happen too often") that its lack of backoff/interrupt-check is low-impact in practice, or can DDL-heavy workloads (frequent relnode deletion, e.g. via `DROP`/`TRUNCATE` under concurrent replay) trigger it often enough to matter? Not measured this pass.
- Does any *other* leader-side wait path in `recovery.c` (beyond the two identified here) have a similar shape — blocking on one worker's progress with only process-exit detection, no liveness/timeout check? Only these two call sites were examined in this pass; `recovery.c` is 5227 lines and a full audit of every worker-wait site was out of scope for the time available.

## SUT-side instrumentation

`existing-assertions.md` confirms 0 assertions in `src/recovery/`. This is
one of the properties where SUT-side instrumentation is closest to
essential rather than merely nice-to-have: the "leader blocked on one
worker" state is not distinguishable from "recovery is just slow" via any
externally-observable signal today (no per-worker progress metric, no
stall-duration marker). Suggested (missing):
- A `Reachable`/timed marker in `worker_queue_flush()` when the underlying
  `shm_mq_send` blocks (i.e., wraps the call with a before/after timestamp
  and fires past a threshold), and similarly in `workers_synchronize()`'s
  wait loop, tagged with the worker id — this directly targets the "which
  worker, how long" visibility gap that makes this property currently
  unobservable from outside the process.

## Cross-cutting pattern (added by evaluation pass, R14)

One of four properties sharing the "unbounded busy-wait, no
`CHECK_FOR_INTERRUPTS()`"-shaped gap identified by the Wildcard evaluation
lens (the others: `sk-fixup-sentinel-spin-livelock`, `recovery-worker-
idxbuild-stall`, `checkpointer-startup-lock-drain-progress` — see
`property-relationships.md` Cluster 11). This property's own evidence
(above) already independently confirms `workers_synchronize()`'s busy-poll
has no `CHECK_FOR_INTERRUPTS()` at all — and this exact bug was found again,
independently, during the evaluation pass's own branch sweep
(`origin/nickb/fix_worker_wait_for_sync`, fix `eaeb556f`, dated the same day
as the analyzed commit, not an ancestor), corroborating that this is a real,
well-targeted finding likely to be fixed imminently upstream. A cheaper
first test than the process-freeze/CPU-throttling scenario already
proposed: `pg_cancel_backend()`/`pg_terminate_backend()`/`statement_timeout`
targeted at the specific wedged worker's PID directly probes whether
`workers_synchronize()` ever consults `CHECK_FOR_INTERRUPTS()`, without
needing to construct an external freeze/resume scenario first.
