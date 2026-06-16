# Trigger oxid captured — `trigcatch` trial 4 (instrumented build `9ab55419`)

Goal: name the oxid that triggers the standby `xmin >= globalXmin` assert.
Achieved on trial 4 of 4 with the new `GXMIN-TRACE … TRIGGER` instrumentation.

Build: `9ab55419` (= `f071202a` + trigger-logging at the assert sites).
Config: 20 writers, 6/6/6 readers, streaming standby, **kill every 3 s** (the
working repro lever; `RR_ROLLBACKERS` is a no-op — not read by the test).
Repro rate jumped to **1/4** vs 2/102 at kill=6s — the shorter kill interval is
the effective rate-raiser.

## The captured trigger (one line before the TRAP)

```
GXMIN-TRACE update_run_xmin TRIGGER xmin=316 < globalXmin=329
   (queue_min_oxid=316 recovery_xmin=387 nextXid=400 writtenXmin=329 pid=2038604)
   *** ASSERT-WILL-FIRE ***
TRAP: failed Assert("xmin >= globalXmin"), recovery.c:2640, PID 2038604
```

(Assert at line **2640** in the instrumented build; it's `update_run_xmin` —
the `2623` of the prior build was the same function, mislabeled `free_run_xmin`
in an earlier note. Corrected.)

## Full causal chain — end to end, oxid named (CORRECTED 2026-06-15)

NOTE: an earlier draft of this file said "316 was in-flight at the SIGKILL".
That was WRONG — re-investigation of the primary log proved 316 **aborted in
memory before the kill**. That distinction is the whole point (it is why the
primary could legitimately stream runXmin=329 past 316). Corrected chain:

1. **Primary, gen N — 316 does work, then ABORTS IN MEMORY** (11:20:32.58→.62):
   ```
   undo_xact_callback enter event=2 oxid=316              (event 2 = ABORT)
   abort enter / wal_rollback begin oxid=316
   walk_undo_stack abort-branch oxid=316 location=111072  (unwinds its 3 modifies)
   current_oxid_abort set_oxid_csn(ABORTED) oxid=316      ← resolved ABORTED in memory
   ```
2. **The in-memory abort advances runXmin** (abort calls advance_run_xmin, same
   as commit). 316 gone ⇒ runXmin climbs `316 -> 329` at 11:20:32.663
   (`advance_global_xmin RAISE ... srcXid=366`). The primary streams runXmin=329
   **legitimately** — 316 is NOT in-flight. (No invariant violated on the primary.)
3. **The abort writes NO durable record** — backend's `wal_rollback` is a no-op
   (316's modifies were uncommitted, never row-WAL'd; orioledb logs rows at
   commit). The ONLY durable WAL_REC_ROLLBACK for 316 is later emitted by
   *recovery* (pid 2038775), not the backend (2038652).
4. **Standby follows legitimately** — first streaming pass advances
   `writtenXmin/globalXmin` to 329 at 32.663, band `[316,329)` frozen-stamped,
   before the stream breaks (`invalid record length at 0/300D788`, 32.704).
5. **SIGKILL loses the in-memory abort entirely.**
6. **Gen N+1 crash recovery rebuilds from durable state only** ⇒ 316 looks
   in-flight ⇒ re-aborts ⇒ emits the deferred, empty rollback at LSN 0/300E4A8:
   `XID (316 0 0); ROLLBACK (316 0 0 - xmin 316 csn 299)` (11:20:35.032).
7. **Standby re-streams from 0/3000000** (walreceiver reconnect, 11:20:37.710) but
   `writtenXmin=329` **persists in shared memory across the reconnect**. Replay
   reaches 0/300E4A8 and re-admits 316 into `xmin_queue` **below** 329.
8. **update_run_xmin drags runXmin under the horizon → PANIC**:
   `xmin = Min(queue_min_oxid=316, recovery_xmin=387) = 316 < globalXmin=329`
   (`nextXid=400`). Pre-series this silently lowered globalXmin to 316
   (→ frozen band re-exposed → **livelock**); the series' monotonicity Assert
   turns it into a **standby crash**.

The defect is a durability asymmetry: the abort that *advanced* the horizon left
no durable record, so recovery resurrects 316 and emits a deferred rollback at a
HIGHER LSN than where the (in-memory, reconnect-surviving) watermark already
passed 316.

## Significance

This is the **same root cause** as the original streaming-standby livelock
(`recovery_livelock_deferred_rollback_root_cause.md`): an empty/no-material-change
deferred rollback for an oxid **below `recovery_xmin`** re-enters `xmin_queue`
below the frozen watermark. The `f071202a` series doesn't fix it — it converts the
livelock into a hard assert crash. The emission-site fix (skip deferred rollback
for `oxid < recovery_xmin` in `o_emit_recovery_finish_rollbacks`) would suppress
exactly this: oxid 316 < recovery_xmin 387, so it would never be emitted.

The new `TRIGGER` trace (queue_min_oxid) + `srcXid` on the RAISE trace make the
chain readable directly from one standby log instead of inferred.

Evidence: `results/20260615_112133_trigcatch_NONE_invariant_replica.log`.
