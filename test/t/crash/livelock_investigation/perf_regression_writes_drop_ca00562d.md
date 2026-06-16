# Writes-per-trial regression under fix series `ca00562d` — it is NOT recovery time

Question investigated: writes/trial dropped ~3000 → ~900 under the new fix series.
Hypothesis offered: crash-recovery time dramatically increased. **Disproven** — the
regression is per-statement MVCC chain-walk cost, uniform across the trial.

Data: clean-trial logs `results/20260612_050159_logsample_NONE_clean.log` (post-patch,
92s, writes=906) vs `rootcause_evidence/TIMEOUT_6_primary.log` (pre-patch `eb3b765f`-era,
same 60s/6s streaming-kill config).

## 1. Crash recovery got FASTER, not slower

| metric (per kill→ready cycle) | pre-patch | post-patch (`ca00562d`) |
|---|---|---|
| `pg_ctl start` → ready | 1.1–1.8 s | **0.75–0.9 s** |
| redo elapsed | 30–60 ms | 50–70 ms |
| kill → restart begins | ~0.5 s | ~0.5–1.5 s |

## 2. Kill cadence stretched (~8s → ~13s) — but that means MORE uptime, not less

Pre-patch: 7 kills/trial, start-to-start ≈ 8s. Post-patch: 5 kills/trial, ≈ 13s.
(Kill moment pinned by the simultaneous backend `FATAL: terminating connection due to
unexpected postmaster exit` burst — the postmaster death-pipe wakeup.) Downtime per
cycle is ~1.5s in both eras. Total downtime cannot explain a 3× writes drop; uptime
per trial actually *increased*.

## 3. The real regression: undo-chain walking per statement

`oxid_match_snapshot` fires once per undo-chain hop in `o_find_tuple_version`
(`src/btree/iterator.c:332`):

| | pre-patch | post-patch |
|---|---|---|
| `oxid_match_snapshot enter`, whole trial | 67,650 | **1,124,240** |
| issued `statement: UPDATE` | 6,505 | 2,445 |
| chain hops per issued UPDATE | ~10 | **~459** |
| UPDATE issuance per minute | 1985 / 4520 | 565 / 1880 (uniformly ~3× lower) |

- A **storm window** in the first uptime period (05:00:51–58, before the first kill):
  120k–245k hops/sec, driven by the full-scan readers
  (`SELECT sum(balance)…`, `ORDER BY token`) under repeatable read. ~1.09M of the
  1.12M total hops land in those ~7 seconds. Steady state afterwards: 300–500/s.
- At each kill, 16–17 of 20 writers are caught **blocked inside a statement**
  (death-pipe FATAL during execution); pre-patch almost all were idle between
  statements (4 such FATALs across 7 kills total). Writers spend their time waiting,
  consistent with reader chain-walk storms holding page locks (every hop crosses the
  csn-trace speed bumps in `page_state.c`).
- The walked oxids include **ancient ones (1, 4 — the setup INSERTs)** still being
  re-checked at t+50s: resolved-frozen versions are not getting stamped/truncated
  away, so every scan re-pays the full walk.
- `globalXmin` is NOT stuck — GXMIN-TRACE shows healthy raises throughout. But the
  new checkpoint seed publishes `globalXmin = checkpointRetainXmin` far below
  `writtenXmin` on every restart (e.g. `SEED globalXmin=133 writtenXmin=343`).

## Open question (next dig)

What exactly in the series lengthens the chains: the retain-floor pinning keeping
undo (and thus physical version chains) alive, vs. lost in-page frozen-stamping of
resolved versions. Candidates to read: what gates version-chain truncation /
in-page xactInfo fixup — `globalXmin`, `writtenXmin`, or
`transactionUndoRetainLocation` (now floor-pinned at recovery).

## Side observation

Trial 10 of the resumed hunt (`fixca00562d-b`): first `plan-check FAIL` —
`[explain sk-forced count(*)] did NOT use o_bank_account_token_uniq`. Single
occurrence so far; watch whether it recurs (planner fallback would invalidate
PK-vs-SK diagnostics for affected trials).
