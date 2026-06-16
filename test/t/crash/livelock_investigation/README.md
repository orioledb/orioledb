# Streaming-standby recovery livelock — investigation index

Status: **root cause proven; fix verified (0 livelocks / 100×60s-6s chaos trials).**

## TL;DR (read this first)

A streaming standby (or a node's own crash recovery) wedges a recovery worker forever
in `o_btree_modify_handle_conflicts`, spinning on `oxid_get_csn` for an oxid that reads
`INPROGRESS` but is actually long-committed. Root cause, in one line:

> An aborted transaction advances the **in-memory** horizon (`runXmin`) — which is
> streamed to the standby via `finish.xmin` — but its abort leaves **no durable
> rollback record** (the `wal_rollback` `!has_material_changes` no-op). Crash recovery,
> which can only see durable artifacts, rediscovers that oxid as in-flight (it sits in
> the checkpoint xids dump with no replayed finish record), aborts it, and emits a
> **deferred `WAL_REC_ROLLBACK`**. On the standby that rollback re-admits the oxid into
> `xmin_queue` *below* `writtenXmin`, dragging `globalXmin` under the frozen watermark
> and re-exposing FROZEN slots → committed oxids in the band read as `INPROGRESS` → the
> conflict-resolver (no `waitCallback` in recovery) retries forever.

The deep "why": a watermark is a **per-instance, in-memory recyclability horizon**
("nobody here still needs this oxid's CSN"), *not* a durability claim. Serializing it
across the instance boundary (`finish.xmin` → standby, or persist → crash recovery)
silently upgrades it to "everything below is durably finalized" — a promise it can't
keep for in-memory-only aborts.

## Verified fix (the one that works, zero added WAL)

In `o_emit_recovery_finish_rollbacks` (`src/recovery/recovery.c`), skip the deferred
rollback for any aborted oxid below the recovery horizon:

```c
if (recovery_finish_aborted_oxids[i].oxid < recovery_xmin)
    continue;   /* horizon already passed it -> in-memory-resolved -> consumer froze it */
```

Correctness: `oxid < recovery_xmin` ⟺ the oxid released its `runXmin` pin before the
crash ⟺ it resolved in memory (and since it's in the aborted set, via the no-op path).
The standby already froze it; re-announcing it is the bug. Genuinely in-flight oxids
that #876 needs **pin `runXmin` while live**, so they are `≥ recovery_xmin` and are
still emitted. Verified: **0 livelock timeouts in 100×60s/6s streaming-chaos trials**,
replica data clean (the only divergences were the unrelated known extent leak).

## What did NOT work (and why) — see `postfix_81e8edcc_livelock_persists.md`

| attempt | lever | why it failed |
|---|---|---|
| `81e8edcc` seed horizons + defer free_run_xmin | where horizon values come from | the lowering enters via `queue_min` from the deferred rollback, not the checkpoint |
| `618396d2` pin `recovery_xmin = checkpointRetainXmin` | constrains `recovery_xmin` | the lowering value comes from `queue_min`, the *other* input to `Min()`; the pin is bypassed |

Two robust options exist; the **producer-side** alternative (remove the `wal_rollback`
no-op so every abort emits a durable marker) was also verified 0/100 but adds WAL on the
hot abort path. The consumer-side `recovery_xmin` filter above is preferred (no added WAL).

## Documents in this directory

- **`recovery_oxid_classification_investigation.md`** — the comprehensive one: how
  checkpoint + WAL replay classify oxids INPROGRESS/COMMITTED/ABORTED, and the precise
  reason `finish.xmin=618` coexists with "oxid 557 in-progress". Start here for the model.
- **`recovery_livelock_deferred_rollback_root_cause.md`** — the original proven root-cause
  writeup (watermark invariant, the full primary+replica sequence with log evidence).
- **`long_running_commit_vs_deferred_rollback.md`** — why a normal long-running commit is
  safe but a deferred recovery-finish rollback is not (the streaming-gap distinction).
- **`xmin_horizon_primary_vs_replica.md`** — primary-vs-replica horizon comparison.
- **`postfix_81e8edcc_livelock_persists.md`** — analysis of the failed fix attempts.
- **`gxmin_trace_catch_trial8.md`** — a distilled `GXMIN-TRACE` catch (watermark history).

## Key code sites

- `src/recovery/wal.c` — `wal_rollback` `!has_material_changes` no-op (the missing marker);
  `wal_emit_recovery_finish_rollback` (#876 deferred rollback).
- `src/recovery/recovery.c` — `o_emit_recovery_finish_rollbacks` (the fix site),
  `recovery_finish` (force-aborts INPROGRESS survivors), `update_run_xmin`
  (`globalXmin = Min(queue_min, recovery_xmin)` — the lowering site), `recovery_map_oxid_csn`.
- `src/transam/oxid.c` — `advance_run_xmin` (called by both commit *and* abort),
  `oxid_get_csn`/`map_oxid` (the `oxid < globalXmin → FROZEN` fast path).
- `src/checkpoint/checkpoint.c` — `finish_write_xids` (the xids dump: presence, no status).

## Diagnostic instrumentation (load-bearing, `#ifdef USE_INJECTION_POINTS`)

- `GXMIN-TRACE` markers in `recovery.c`/`oxid.c`/`checkpoint.c` — every globalXmin
  raise/lower/seed; the lower carries `*** BELOW-writtenXmin INVARIANT-VIOLATION ***`.
- `csn-trace … oxid_get_csn-call` in `btree/modify.c` — the spin signature; the hunt
  detector counts these (`> 50000` ⇒ tight livelock).
