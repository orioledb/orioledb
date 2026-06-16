# Hunt results — fix series `ca00562d` (+ conflict resolution `f071202a`)

Config (all hunts): 100×60s streaming-standby trials, postmaster SIGKILL every 6s,
no injection/assert chaos, `RR_PANIC_FATAL=0`, assert build.

## With readers (default 6 PK + 6 SK + 6 mixed) — instance `fixca00562d`/`-b` + `logsample`

Stopped at user request after 50 trials: **49 clean / 1 BUG**.
- Trial 16 BUG: **replica startup PANIC** `undo_item_buf_read_item(): read of
  unexisting undo record ... location=0` while replaying deferred rollback
  `XID (460 0 0); ROLLBACK (460 0 0 ...)` — the series' own bug; see
  `trial16_undo_read_panic_ca00562d.md`.
- writes/trial ≈ 900 (the ~3× regression; see
  `perf_regression_writes_drop_ca00562d.md`).

## Without readers (`RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0`) — instance `noreaders`

Completed 100/100: **clean=94 BUG=6 TIMEOUT=0 ERROR=0**.
- All 6 BUGs (trials 20, 36, 39, 40, 51, 89) individually verified as the
  **pre-existing SK extent leak** (`Extent {2,3} 1 is neither free or busy` +
  `Corrupted index name = o_bank_account_token_uniq`, data invariants clean) —
  documented in `../extent_leak_issue.md`, NOT attributable to the fix series.
  Repro rate ~6% without readers vs 0/50 with readers.
- plan-check: **0/100 passed** — the `sk-forced count(*)` o_scan-over-PK fallback
  (also documented in `extent_leak_issue.md`) is deterministic without readers.
  Other sk-forced diagnostics kept using the SK index; detection validity intact.
- **Zero deferred-rollback-family failures** (no livelock TIMEOUT, no Assert TRAP,
  no undo-read PANIC) in 100 trials.
- writes/trial: min 2389 / median 3081 / max 3846 / avg 3088 — **full restoration
  of pre-patch throughput**, confirming the with-readers writes drop is the
  readers' chain-walk cost (46× longer version chains) dragging writers, not a
  writer-side regression.
- Two trials (30, 36) showed inflated wall-clock (1380s/1602s): VM suspends
  (host uptime counter advanced ~7 min while wall clock advanced ~2 h), not hangs.

## Bottom line for the series

1. The deferred-rollback path can still kill the replica (trial 16, with-readers,
   1/50). The bug family is not closed; reader load (RR snapshots widening the
   replay window) appears to be a precondition for hitting it.
2. The ~3× throughput regression is reader-mediated but real: version chains are
   ~46× longer under the series, and any read-heavy workload pays it.
3. The extent leak and the sk-forced plan fallback are pre-existing issues,
   exposed more often in no-readers mode.
