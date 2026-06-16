# GXMIN-TRACE catch — globalXmin lowered below writtenXmin (build 1c2ac4ba)

Direct instrumentation catch of the #889 streaming-standby livelock, build `1c2ac4ba`
(= fix `81e8edcc` + the `GXMIN-TRACE` markers). Hunt: 100×60s/6s streaming, **caught
trial 8** (clean=6, 1 prior structural-divergence on trial 1 = known extent leak).
Detector keyed off the new `BELOW-writtenXmin INVARIANT-VIOLATION` marker and stopped
immediately. Raw logs (7.0G replica / 303M primary) distilled here then removed.

## The watermark history, fully logged (replica pid=1186454)

```
[1] SEED   checkpoint_shmem_init  globalXmin=66 writtenXmin=66  (clean ckpt: retainXmin==Xmax==66)
[2] CLIMB  advance_global_xmin    ... RAISE/writtenXmin step up, FROZEN-stamping bands:
             writtenXmin 547 -> 720  FROZEN-stamped [547,720)
             RAISE globalXmin 720 -> 727 ;  writtenXmin 720 -> 727  FROZEN-stamped [720,727)
             RAISE globalXmin 727 -> 764 ;  writtenXmin 727 -> 764  FROZEN-stamped [727,764)
[3] LOWER  update_run_xmin  globalXmin 764 -> 596
             (recovery_xmin=935 writtenXmin=764 nextXid=962 queue_empty=0)
             *** BELOW-writtenXmin INVARIANT-VIOLATION ***
```

Exactly **one** LOWER event — a clean signal. `queue_empty=0` proves the lowering value
596 came from the **xmin_queue** (queue_min=596), not `recovery_xmin` (935):
`Min(596, 935) = 596`.

## What put 596 in the queue: the deferred rollback

```
WAL redo:  XID (596 0 0); ROLLBACK (596 0 0 - xmin 596 csn 838)
```
- `596` is the **lowest** of the 30 recovery_finish-aborted oxids:
  `596 635 646 655 695 706 713 726 731 748 775 784 788 789 792 800 803 804 817 822
   827 850 858 865 866 885 892 894 905 906`.
- The bare `WAL_REC_XID(596)` re-admits it into `xmin_queue` → `update_run_xmin` writes
  `globalXmin = 596`, **below `writtenXmin = 764`**.
- Note `xmin 596` (not a lifted high value): fix `81e8edcc` part-2 *did* lower the
  rollback's `finish.xmin` to the floor — but the bare XID re-admission is what drags
  globalXmin down, and that is untouched.

## The livelock

Frozen band `[596, 764)` is re-exposed. The spun oxids are **committed-frozen** oxids
in that band — NOT in the aborted set (they sit in its gaps: 646<652<655, 713<718<726,
748<762<775):
```
  1,766,466  oxid=652      (committed; csn slot frozen; now read INPROGRESS)
  1,763,134  oxid=762
  1,748,542  oxid=718
```
`oxid_get_csn` returns INPROGRESS for them → `o_btree_modify_handle_conflicts`
(no waitCallback in recovery) retries forever. **4,804,585** spins / 130s timeout.

## Verdict

Same mechanism as the trial-9 catch (oxids 196→403/431/442) — confirmed across two
independent runs. Fix `81e8edcc` does **not** close the livelock; the consumer-side
defect (a deferred-rollback `WAL_REC_XID` re-admitting an oxid `< writtenXmin` into
`xmin_queue` and lowering `globalXmin` below the frozen watermark) remains. The
airtight fix is still the `update_run_xmin` floor:
`globalXmin = Max(Min(queue_min, recovery_xmin), writtenXmin)`.
