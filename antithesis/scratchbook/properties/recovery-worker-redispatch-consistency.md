---
slug: recovery-worker-redispatch-consistency
attention_focus: Idempotency and Replay
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# Recovery-worker key routing is deterministic; the redelivery risk narrows to a config-change edge case

## What led to this

The assigned focus explicitly asked: "could a crash mid-replay cause a WAL
record to be re-delivered to a different worker than originally, and does
that worker's overwrite-callback handle that safely?" This property records
the investigation and its (mostly reassuring, with one residual edge case)
answer, rather than asserting the worry is unfounded without checking.

## What was examined

- `spread_idx_modify()` (`src/recovery/recovery.c:5128-5161`) dispatches every
  index modify record by `GET_WORKER_ID(hash)` where `hash =
  o_btree_hash(desc, rec, ...)` — a hash of the **key**, not the oxid or
  arrival order. `GET_WORKER_ID(hash)` (`include/recovery/internal.h:41`) is
  `(hash) % recovery_pool_size_guc`. The doc (`doc/architecture/recovery.mdx`,
  "Splitting work between multiple processes") confirms this is deliberate:
  "each worker is responsible for his own set of keys... the main process
  distributes these operations to the queues based on the hash of the `id`
  column."
- Recovery workers are registered with `bgw_restart_time = BGW_NEVER_RESTART`
  and **without** `BGWORKER_CLASS_SYSTEM` (`src/recovery/worker.c:174-185`),
  unlike S3 workers. An abnormal exit of a recovery worker is therefore
  expected to be treated as an ordinary shmem-attached backend crash by the
  postmaster, which (standard Postgres `HandleChildCrash` behavior) triggers
  a full cluster crash-restart: every process killed, shared memory
  reinitialized, WAL replay restarted from scratch at the last checkpoint's
  `replayStartPtr`. This was not independently re-verified against the
  patched postmaster source in this pass (mirrors the caveat already logged
  in `sut-analysis.md` §11 for the analogous S3 crash-escalation claim), but
  is the standard-Postgres behavior for a non-`BGWORKER_CLASS_SYSTEM` shmem
  worker crash.
- Given that, "mid-replay crash and resume" for OrioleDB's own recovery
  workers is not really "resume where a previous partial attempt left off
  with reshuffled worker assignments" — it's "start over completely," and
  `GET_WORKER_ID(hash)` is a pure function of the key and the *current*
  `recovery_pool_size_guc`. As long as that GUC is unchanged across the
  crash, every key hashes to the same worker on the new attempt as it would
  have on any previous attempt, and each worker's queue is still processed
  strictly in WAL order (records are sent to a worker's `shm_mq` in the
  order the leader reads them and `worker_send_modify` always appends to
  that worker's own queue) — so per-key ordering across a fresh replay
  attempt is preserved regardless of how many times replay has been
  restarted.

## The residual edge case

If an operator (or, in principle, a fault-injection scenario that mutates
config between a crash and restart) changes `orioledb.recovery_pool_size`
between the crash and the subsequent restart, `GET_WORKER_ID(hash)` for a
given key would map to a *different* worker index on the new attempt than
it would have used previously. This is not unsafe on its own — since the
whole replay restarts from the checkpoint boundary and a single worker's
queue is still strictly ordered, a key simply gets a different (but still
internally consistent) worker for this attempt. The concern would only
become real if some *other* piece of state assumes worker N always handles
the same key set across restarts (e.g. any left-over per-worker on-disk
state from a previous partial recovery attempt) — nothing found in this
pass suggests such state exists, but it was not exhaustively ruled out.

## Why this is a weaker/lower-confidence property than the others in this
   batch

Unlike the S3 queue findings or the SK-fixup findings, this pass did not
find a concrete mechanism-level bug here — the dispatch design appears to
correctly avoid the redelivery hazard the focus asked about, given the
full-restart-on-crash model. This property is recorded primarily so the
question is documented as investigated (not skipped), and to flag the one
remaining variable (mid-incident GUC change) as worth a deliberate
Antithesis config-fuzzing pass rather than being assumed safe by analogy.

## Antithesis angle

Lower priority than the other properties in this batch. If pursued: restart
the instance with a different `orioledb.recovery_pool_size` between
deliberate crashes during a sustained-DML + automatic-checkpoint workload
(extending `sk-recovery-race-chaos`'s existing pattern), and assert
`orioledb_tbl_check()` / PK-vs-SK consistency still holds — same oracle the
existing harness already uses, just with the added config-changing step.

## Open Questions

- Does the postmaster's crash-restart path actually re-read
  `orioledb.recovery_pool_size` from a possibly-edited `postgresql.conf`
  before respawning the startup process, or is the value pinned from the
  original process start in a way that makes this edge case unreachable in
  practice? Not checked — this determines whether the residual edge case
  above is reachable at all.
- Is there genuinely no per-worker persisted state that survives a crash
  and assumes stable key-to-worker assignment across restarts? This pass
  only checked the in-memory dispatch function and the worker registration
  flags; it did not audit every on-disk artifact recovery workers touch.
  `(needs further code reading, out of scope for this pass)`
