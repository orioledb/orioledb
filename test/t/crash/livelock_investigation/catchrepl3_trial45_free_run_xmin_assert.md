# catchrepl3 trial 45: `free_run_xmin` monotonicity Assert TRAPs the standby

Build: `f071202a` (rebuilt 2026-06-15 07:46). With-readers streaming hunt
(`catchrepl3`, default 6/6/6 readers, kill every 6s). Stopped at 52 trials:
**50 clean / 2 BUG** — trial 41 SK extent leak, **trial 45 = the replica failure
we were hunting.**

Evidence: `results/20260615_084937_catchrepl3_NONE_invariant_replica.log` (14M)
+ `..._invariant.log` (747M primary).

## The crash — replica-only, fix series' own Assert

```
TRAP: failed Assert("xmin >= pg_atomic_read_u64(&xid_meta->globalXmin)"),
      File: "src/recovery/recovery.c", Line: 2623, PID: 1987107   ← free_run_xmin()
08:48:47  startup process (PID 1987107) terminated by signal 6: Aborted
08:48:47  shutting down due to startup process failure
```

Primary TRAP count = 0, replica = 1 → **standby-only**. The replica's startup
process dies → `catchup() failed` + `connection refused` → BUG.

## Why it fires

`free_run_xmin()` sets `runXmin = nextXid` then asserts `nextXid >= globalXmin`.
On this standby that premise is **false**:

```
GXMIN-TRACE advance_global_xmin RAISE globalXmin 498 -> 534 (writtenXmin=498)
            advance_global_xmin writtenXmin 498 -> 534 FROZEN-stamped [498,534)
...
WAL redo: XID (609 608 0); COMMIT (609 608 0 - xmin 589 csn 524)
TRAP Assert(xmin >= globalXmin)   ← free_run_xmin: nextXid < globalXmin=534
```

`advance_global_xmin` had pushed `globalXmin`/`writtenXmin` to **534** off the
**streamed `finish.xmin`** values. But `free_run_xmin` derives its xmin from the
standby's local `nextXid`, which lags 534. So `nextXid < globalXmin` → TRAP.

## Root cause: the fix series turned TWO downward-globalXmin clamps into Asserts,
## and BOTH premises are false on a streaming standby

The original code had, in *both* `update_run_xmin` and `free_run_xmin`:

```c
if (xmin < globalXmin) { globalXmin = xmin; }   /* clamp down */
```

The series replaced both with `Assert(xmin >= globalXmin)` on the theory that the
checkpoint seed (`checkpointRetainXmin`) makes every later value monotone. That
theory holds on the **primary** (where nextXid is the true ceiling) but not on a
**standby**, where `globalXmin` is advanced from *streamed* `finish.xmin` that can
outrun local `nextXid`. The clamp was load-bearing on the standby; converting it
to an assertion converts a silent horizon adjustment into a PANIC.

This is the **same class** as the earlier `2b463f27` crash, which TRAPped at
`update_run_xmin` (recovery.c:2582). This build TRAPs at the *sibling* assert in
`free_run_xmin` (recovery.c:2623). Two assert sites, one false premise.

## Scoreboard — `f071202a` replica failures now number TWO distinct modes

| trial / build | replica failure |
|---|---|
| `fixca00562d` trial 16 | undo-read PANIC (`walk_undo_stack location=0`, empty deferred rollback) |
| `catchrepl3` trial 45 | `free_run_xmin` Assert `xmin >= globalXmin` (streamed globalXmin > nextXid) |

Both are standby-only, both rooted in the streamed-horizon / deferred-rollback
handling the series rewrote. The emission-site fix
(`o_emit_recovery_finish_rollbacks`, skip `oxid < recovery_xmin`) remains the only
variant that survived 100 trials without a replica failure.

## Process note (my miss)

The live monitor filtered on `status=BUG` (the wrapper's *log-file* format) but the
wrapper's *stdout* uses bare `BUG`; trials 41 and 45 slipped past the monitor and
were only caught on the final tally grep at stop time. Fixed understanding for next
run: grep stdout for `\bBUG\b`/`\bTIMEOUT\b`, not `status=`.
