# Fix `81e8edcc` does NOT close the recovery livelock — trial 9 proof

Build under test: `81e8edcc` ("recovery: defer free_run_xmin + seed horizons from
checkpoint retain range"). Hunt: 100×60s/6s streaming (`RR_REPLICA_MODE=streaming
RR_DURATION=60 RR_KILL_POSTMASTER_INTERVAL=6`). Result: **livelock on trials 9, 11,
12** of the first 12 (clean=9). Same tight-spin fingerprint as the original bug.

Raw logs (trials 9/11/12) removed after analysis to reclaim disk; all evidence
quoted inline below was extracted from `TIMEOUT_9_{primary,replica}.log` before removal.

## The catch (trial 9)

- replica: **4,663,679** `oxid_get_csn-call`, **13,994,789** `handle_conflicts`,
  spinning on oxids **403 / 431 / 442** (~1.55M each), all reading **`result_csn=0`
  (INPROGRESS)**. 130s timeout, 6.2G replica log (~48MB/s = the spin).
- The wrapper's auto-detector missed it: it greps the `*_panic_replica.log`
  snapshot, which doesn't exist on a no-injection timeout (no PANIC) → saw
  `spins=0` → misfiled as "slow-catchup". The saved `TIMEOUT_9_replica.log` holds
  the real signature. **(Detector bug — should grep the live tmp log / use the
  130s-timeout-itself as the signal.)**

## Why it still livelocks (the spun oxids are committed, not aborted)

The 26 recovery_finish-aborted oxids are `292 319 322 345 356 399 414 427 428 435
453 462 474 482 490 491 497 502 505 524 527 537 556 560 563 564`. The spun oxids
**403 / 431 / 442 are NOT in that set** — they sit in its gaps (399<403<414,
428<431<435, 435<442<453). They are **committed-and-frozen** oxids: oxid 403 reads
correctly as `csn=311` at 09:19:42.278, then flips to `csn=0` at 09:19:49.048.

What flips it (replica timeline):
```
09:19:42.278  oxid_get_csn(403) = 311           ← committed, correct
09:19:44.952  (primary) recovery_finish aborts 26 in-flight oxids, min=292
09:19:49.042  WAL redo: XID (292 0 0); ROLLBACK (292 0 0 - xmin 292 csn 590)
09:19:49.048  oxid_get_csn(403) = 0 (INPROGRESS) ← spin begins → 130s timeout
```
The bare `WAL_REC_XID(292)` re-admits the **lowest** aborted oxid into the standby's
`xmin_queue`; `update_run_xmin` writes `globalXmin = Min(292, recovery_xmin) = 292`,
**below `writtenXmin` (≈590, seeded high)**. The frozen band `[292, 590)` is
re-exposed; committed oxids 403/431/442 in it are misread as INPROGRESS;
`o_btree_modify_handle_conflicts` (no `waitCallback` in recovery) retries forever.

## What the fix changed — and what it left open

- **Part 2 (defer `free_run_xmin`) partially worked.** The deferred rollback now
  stamps `xmin 292` (the floor) instead of the old lifted `xmin 530`. So
  `recovery_xmin` no longer over-climbs to `nextXid` via the rollbacks. Real, but
  **not the thing that drives the livelock.**
- **The consumer-side defect is untouched.** The bare `WAL_REC_XID(292)` still
  re-admits an oxid `< writtenXmin` into `xmin_queue`, and `update_run_xmin` still
  lowers `globalXmin` to it. `globalXmin < writtenXmin` recurs exactly as before.
- **Part 1's premise is false on a standby.** The comment says `writtenXmin` is kept
  high "so the on-disk xidmap range ... is still served via `o_buffers_read`." But on
  the replica (`is_recovery_process()`), `map_oxid` sources the csn from
  **`recovery_map_oxid_csn` first** (oxid.c:717), not `o_buffers_read`. For a
  long-committed-and-cleaned oxid like 403 that hash has no entry →
  `recovery_map_oxid_csn` returns `0` with `found=false` (recovery.c:1478-1485) → the
  code falls to the **in-memory circular buffer** (the recycled, FROZEN-stamped slot)
  → INPROGRESS. The on-disk path the fix relies on is never consulted for the csn.
  Seeding `globalXmin` low / `writtenXmin` high therefore **builds the unsafe
  `globalXmin < writtenXmin` band by construction.**

## Conclusion

`81e8edcc` addresses the checkpoint-seeding/ordering (producer + checkpoint) side but
leaves the **consumer-side floor open**. The only airtight fix remains
[root-cause doc §6.1]: in `update_run_xmin`, never let `globalXmin` drop below
`writtenXmin` —
```c
xmin = Max(Min(queue_min, recovery_xmin), pg_atomic_read_u64(&xid_meta->writtenXmin));
```
or refuse to admit an oxid `< writtenXmin` into `xmin_queue` (a deferred-rollback
tombstone for an already-frozen oxid carries no live state the horizon must protect).
Keep the normal in-flight path (oxid `≥ writtenXmin`) untouched.

Empirical gate: re-run the 100×60s/6s streaming hunt → **0 livelock timeouts**.
