# Fix `2b463f27` does NOT close the bug — Assert turns it into a crash-loop (trial 1 proof)

Build under test: `2b463f27` ("recovery: drop fast-path-aborted oxids off xmin_queue in
update_run_xmin") on top of `404d4b37` ("recovery: pin recovery_xmin floor to
checkpointRetainXmin before read_xids", re-applied). Hunt: 100×60s/6s streaming
(`RR_REPLICA_MODE=streaming RR_DURATION=60 RR_KILL_POSTMASTER=1
RR_KILL_POSTMASTER_INTERVAL=6`, instance `fix2b463f`). Result: **trial 1 ERROR —
the commit's new monotonicity Assert TRAPs on BOTH nodes; the primary enters a
permanent crash-loop. Hunt aborted after trial 1** (each TRAP trial saves ~550 MB of
panic logs; 100 trials would exhaust the disk and the verdict was already determined).

Saved evidence: `results/20260611_110555_fix2b463f_NONE_panic.log` (primary, 535 MB)
and `..._panic_replica.log` (19 MB).

## The TRAP

```
TRAP: failed Assert("xmin >= pg_atomic_read_u64(&xid_meta->globalXmin)"),
      File: "src/recovery/recovery.c", Line: 2582
```

- replica: 1 TRAP (PID 1606076, the recovery/apply worker)
- primary: 6 TRAPs across successive restart attempts → crash-loop, cluster never
  came back up (`first_crash_at=19.22s`, trial ends with connection-refused)

## Replica timeline (the proof)

```
11:04:59.663  GXMIN-TRACE advance_global_xmin RAISE globalXmin 509 -> 558
              (writtenXmin 509 -> 558, FROZEN-stamped [509,558))
11:05:04.818  WAL redo: XID (554 0 0); ROLLBACK (554 0 0 - xmin 750 csn 634)   ← deferred rollback (#876)
11:05:04.829  TRAP Assert("xmin >= globalXmin")    ← queue_min=554 < globalXmin=558
```

The streamed finish records around the TRAP all carry healthy horizons (`xmin 750`,
`xmin 808`) — `recovery_xmin` never regresses. The regression enters **exactly where
predicted**: the bare `WAL_REC_XID(554)` of a deferred rollback re-admits oxid 554
into `xmin_queue` below `writtenXmin=558`; `update_run_xmin()` computes
`xmin = Min(queue_min=554, recovery_xmin) = 554 < globalXmin=558` → TRAP.

## Why the commit's drain loop never fires for this path

The drain predicate is `state->checkpoint_xid && !state->wal_xid && oxid < recovery_xmin`.
But the re-admission vehicle is the deferred rollback itself, which **always streams a
`WAL_REC_XID` first** — the re-admitted entry has `wal_xid = true` (and typically
`checkpoint_xid = false`: it was created by the XID record, not by `read_xids()`).
The loop `break`s on the very entry it needed to drain. The commit fixes a different,
narrower population (checkpoint-named oxids that never get any WAL record) — real, but
not the livelock's re-admission path.

## Why the primary crash-loops (worse than the livelock)

The deferred-rollback records written by the *first* crash recovery (`csn 634`,
`xmin 750` batch) are durable in the primary's WAL. The second crash's recovery seeds
`globalXmin` from `checkpointRetainXmin` (the `404d4b37` floor), then replays
`XID (624 0 0); ROLLBACK (624 ...)` — oxid 624 below the seeded floor — and TRAPs:

```
11:05:11.977  WAL redo: XID (624 0 0); ROLLBACK (624 0 0 - xmin 750 csn 634)
              TRAP Assert("xmin >= globalXmin")  (×6, one per restart attempt)
```

Because the offending record sits at a fixed LSN, **every restart replays it and dies
at the same spot** — on an assert build the cluster is bricked until WAL is discarded.
The pre-fix behavior (silent lowering → livelock on the standby) at least left the
primary running.

## Prod-build (non-assert) semantics of this fix — untested

With the Assert compiled out, nothing lowers `globalXmin` any more (the downward write
is gone), so the frozen-band re-exposure that drives the livelock *may* be closed.
But `runXmin` is still written downward (`pg_atomic_write_u64(&runXmin, xmin)` happens
before the Assert), and the invariant the commit claims ("every legitimate xmin here
is >= globalXmin") is empirically false — trial 1, first kill. Verifying the no-assert
behavior would require a separate hunt with the Assert demoted to a trace.

## Conclusion

Third source-of-the-value attempt, same outcome family: `81e8edcc` and `618396d2`
constrained the seed, `404d4b37+2b463f27` constrains the queue — but the lowering
still enters via `queue_min` from the deferred rollback's `WAL_REC_XID`. The only
verified fix remains the **emission-site** one: in `o_emit_recovery_finish_rollbacks`,
skip the deferred rollback for `oxid < recovery_xmin` (0 livelocks / 100×60s-6s
trials, zero added WAL) — see `recovery_livelock_deferred_rollback_root_cause.md`.
