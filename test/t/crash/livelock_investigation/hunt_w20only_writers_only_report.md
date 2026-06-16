# Hunt report — writers-only config (`w20only`), build `f071202a`

Stopped at user request after 61/100 trials to switch back to the with-readers
config. Goal of this run was to chase the replica deferred-rollback failure with a
minimal workload (20 writers, no readers, no rollbackers).

## Config

`RR_REPLICA_MODE=streaming RR_DURATION=60 RR_KILL_POSTMASTER=1
RR_KILL_POSTMASTER_INTERVAL=6 RR_WRITERS=20 RR_ACCOUNTS=100 RR_ROLLBACKERS=0
RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 RR_INJECTION_POINTS=NONE
RR_ASSERT_FIRINGS=0 RR_PANIC_FATAL=0`, assert build.

## Result (61 trials)

| outcome | count |
|---|---|
| clean | 58 |
| BUG (SK extent leak) | 3 — trials 2, 3, 11 |
| TIMEOUT (livelock) | 0 |
| ERROR | 0 |
| **replica deferred-rollback failure** | **0** |

writes/trial: min 2586 / median 3110 / max 3687 / avg 3079 — full pre-patch
throughput (no readers ⇒ no chain-walk drag).

## BUG classification — all 3 are the pre-existing SK extent leak

Each BUG was solely `orioledb_tbl_check` failing with
`Extent {3,4} 1 is neither free or busy` + `Corrupted index name =
o_bank_account_token_uniq`; every logical invariant (sum, counts, SK/PK set
diffs, ghost/missing/dup) clean; replica clean (no PANIC/TRAP); PK btree clean.
Documented pre-existing issue (`../extent_leak_issue.md`), orthogonal to the
`f071202a` standby-recovery fix series. Observed rate this run: 3/61 ≈ 4.9%,
consistent with the ~6% from earlier no-readers runs.

## Conclusion — confirms the regime split

This run is the control that proves the earlier hypothesis: **without readers the
replica failure does not fire** (0/61 here; 0/100 in `noreaders` run #1; 0/2 in
`noreaders2`), while the SK extent leak does (~5%). The replica deferred-rollback
PANIC needs reader load (RR snapshots widening the standby replay window) — it has
only ever appeared in a with-readers run (trial 16 of `fixca00562d`, 1/50).
Therefore the discriminating config to catch the replica failure is **with
readers**, which also suppresses the SK leak to 0. Next run reverts to that config.

## Cross-run scoreboard for `f071202a`

| config | trials | clean | SK-leak | replica failure |
|---|---|---|---|---|
| with readers (`fixca00562d`+`-b`+logsample) | 50 | 49 | 0 | **1 (trial 16 undo-read PANIC)** |
| no readers (`noreaders`) | 100 | 94 | 6 | 0 |
| no readers (`noreaders2`) | 2 | 1 | 1 | 0 |
| writers-only (`w20only`, this run) | 61 | 58 | 3 | 0 |
