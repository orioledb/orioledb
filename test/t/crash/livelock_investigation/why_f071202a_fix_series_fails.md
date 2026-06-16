# Why the `f071202a` fix series fails (and why the emission-site fix works)

The fix series `53a88240 → 03db2496 → c64f55a3 → ca00562d` (+ `f071202a` conflict
resolution) does not close the streaming-standby deferred-rollback bug. It converts
the **livelock** into a **standby PANIC** (verified: `catchrepl3` trial 45 and
`trigcatch` trial 4, both `Assert("xmin >= globalXmin")` in `update_run_xmin`).

## The idea (4-part campaign, all at the *consumer* of the bad horizon)

1. `53a88240` — seed `recovery_xmin` from `checkpointRetainXmin` before `read_xids()`
   so `update_run_xmin`'s `Min(xmin, recovery_xmin)` pins `runXmin` at the floor.
2. `03db2496` — drain "fast-path-aborted" oxids off `xmin_queue` with predicate
   **`checkpoint_xid && !wal_xid && oxid < recovery_xmin`**; replace the downward
   `if (xmin < globalXmin) globalXmin = xmin` with `Assert(xmin >= globalXmin)`.
3. `c64f55a3` — fully tear down the drained entry so `recovery_finish` won't
   re-emit a "spurious `WAL_REC_ROLLBACK`."
4. `ca00562d` — save/restore `recovery_oxid`/`curUndoLocations`/`oxid_needs_wal_flush`
   around the drainer's `walk_checkpoint_stacks`.

Stated premise (`03db2496`): "every legitimate value of xmin in update_run_xmin()
is >= globalXmin." That premise is false.

## Why it fails — traced on oxid 316 (trigcatch trial 4)

**(a) The drain's `!wal_xid` clause is evaded by the very record that re-admits the
oxid.** The standby re-admits 316 by replaying the deferred rollback
`XID (316 0 0); ROLLBACK (316 0 0 ...)`. The leading `XID(316)` *is* a `WAL_REC_XID`,
so on the standby 316 has **`wal_xid = true`** → the drainer (`!wal_xid`) skips it.
The drain catches quiet checkpoint-only oxids, not the one arriving with a wire XID.

**(b) On the emitting side the oxid sits *at* the floor, not below it.** The master's
crash recovery emits 316's rollback while its own `recovery_xmin ≈ 316`, so
`oxid < recovery_xmin` (`316 < 316`) is **false** → `c64f55a3`'s teardown never
engages → `recovery_finish` ships the rollback. The pinning oxid is by definition
equal to the floor; strict `<` excludes it. The two gaps are complementary: emitter
skips because the oxid is *at* the floor, consumer skips because it now carries a
wire XID.

**(c) The `Assert` is a detector, not a fix.** It doesn't prevent the regression, it
makes it fatal — livelock (silent lowering → frozen band re-exposed → infinite
retry) became standby PANIC. Strictly worse.

## Root reason

The series makes every *reader* of the horizon robust against a record that should
never exist. Once the master emits a deferred `WAL_REC_XID + WAL_REC_ROLLBACK`, that
oxid is indistinguishable from a legitimately in-flight transaction's rollback;
ignoring it generically would be a correctness bug. The heuristic
`checkpoint_xid && !wal_xid && oxid < recovery_xmin` tries to thread that needle and
has holes on both sides.

## Why the emission-site fix works

Skip the deferred rollback in `o_emit_recovery_finish_rollbacks` when
`oxid < recovery_xmin`. Same predicate as the failed drainer — but evaluated at the
**end of recovery, after full WAL replay**, where `recovery_xmin` is final (≈387, not
≈316). There `316 < 387` is unambiguously true ⇒ the toxic record is never written ⇒
no consumer ever has to recognize it. 0 added WAL, 0 livelocks / 100 trials.

One line: *the series tries to make every reader robust to a record that should not
exist; the working fix does not emit that record — and tests `oxid < recovery_xmin`
at the one site where the horizon is final, not in `update_run_xmin` mid-replay where
`recovery_xmin` can still equal the offending oxid.*

## Supporting facts (corrected mechanism)

Oxid 316 was **aborted in memory on the master before the SIGKILL** (not in-flight);
that in-memory abort advanced `runXmin` to 329 and streamed legitimately. Its
`wal_rollback` was a no-op (uncommitted modifies are never row-WAL'd), so the abort
left no durable record. Crash recovery then resurrected 316 as in-flight and emitted
the deferred rollback at a *higher LSN* than where the standby's (in-memory,
reconnect-surviving) watermark had already passed 316. Full trace:
`trigger_oxid_captured_trial4.md`. Root model: `recovery_livelock_deferred_rollback_root_cause.md`.
