---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-27
external_references:
  - path: /Users/artur/supabase/orioledb_postgres
    why: checked for Antithesis SDK usage; none found (as expected — it's a plain Postgres source fork, not workload code)
  - path: doc/
    why: checked for any documentation of Antithesis assertion conventions; none found (workload assertions are only in test/antithesis/)
---

# Existing Antithesis SDK Assertions

Scanned: `src/`, `include/` (the orioledb C extension — no hits), `/Users/artur/supabase/orioledb_postgres` (patched Postgres core — no hits), and `test/antithesis/` (the Antithesis harness — all hits found here). Search patterns: `assert_always`, `assert_sometimes`, `assert_reachable`, `assert_unreachable`, `antithesis` (case-sensitive grep across `*.py`, `*.c`, `*.h`, `*.go`).

**No Antithesis SDK assertions exist anywhere in the C extension itself (`src/`, `include/`) or in the patched PostgreSQL core.** All existing instrumentation lives in the Python workload drivers under `test/antithesis/`. This means every property currently checked is verified from the workload/client side only — there is no SUT-side (in-process) instrumentation yet. See `sut-analysis.md` §11-12 for candidate internal states (e.g. the checkpoint-during-recovery LSN synchronization gap, the S3 lock-file deletion path, the rewind/tini interaction) where a surgical SUT-side assertion would give materially better search guidance than a workload-only check, since they concern internal states not directly observable from a SQL client.

## Found assertions

### `test/antithesis/sk-recovery-race/driver.py`

Python SDK: `from antithesis.assertions import always, reachable` (line 18).

| Line | Type | Message | Notes |
|---|---|---|---|
| 89-95 | `always` | "o_sk_pending PK rows match distinct SK tokens after the sk_modify_pending race (orioledb#855)" | Checked at both "startup" and "post-race" labels (called at lines 193, 195 in `main()`); payload includes `label`, `pk_rows`, `sk_distinct`, `tbl_check`. Verifies PK-row-count == distinct-SK-token-count and `orioledb_tbl_check()` returns true. |
| 164-168 | `reachable` | "all three DML backends parked at sk_modify_pending (PK applied, SK pending)" | Confirms the deterministically-constructed race window (via `pg_stopevent_set('sk_modify_pending', ...)`) is actually reached before a `CHECKPOINT` is issued into it. Payload: `pids`. |

### `test/antithesis/sk-recovery-race-chaos/driver.py`

Python SDK: `from antithesis.assertions import always, sometimes` (line 17).

| Line | Type | Message | Notes |
|---|---|---|---|
| 87-93 | `always` | "o_sk_pending PK rows match distinct SK tokens after concurrent DML + automatic checkpoints (orioledb#855)" | Same invariant as the deterministic driver's `always`, checked at "startup" and "post-burst" (called at lines 196, 198). No stopevent pinning — relies on chance overlap + Antithesis's own fault injection. |
| 182-188 | `sometimes` | "at least one automatic checkpoint fired while concurrent INSERT/UPDATE/DELETE were in flight against o_sk_pending" | Liveness-flavored: confirms the intended race window is exercised at all (checkpoint firing during load), backed by `checkpoint_timeout=30s`. Payload: `checkpoints_before`, `checkpoints_after`. |

### `test/antithesis/jepsen/`

No direct Antithesis SDK assertion calls found in this repo's jepsen wrapper scripts (`entrypoint`, `test/v1/jepsen-postgres/singleton_driver_jepsen-postgres`, `finally_jepsen-postgres`). The jepsen tool itself (external Clojure image, not vendored in this repo) performs its own serializability/anomaly analysis and writes `results.edn`/`history.edn`; `finally_jepsen-postgres` only copies those files to `$ANTITHESIS_OUTPUT_DIR` for external inspection. **This means the jepsen workload's correctness verdict is not currently wired into an Antithesis SDK assertion** (`always`/`sometimes`/`reachable`) — it produces an artifact for post-hoc review rather than a signal Antithesis's search can act on directly. This is a gap worth flagging to the `antithesis-workload` skill: converting the jepsen anomaly-detection result into an explicit `always(no_anomalies_found, ...)` assertion (parsed from `results.edn`) would let Antithesis's fault-guided search prioritize toward violations directly, rather than relying on an external results file nobody scores during the run.

### `test/antithesis/health-checker/main.go`

Contains the string `antithesis_setup` (line 9) as a static JSON status marker (`{"antithesis_setup":{"status":"complete", ...}}`), which is the Antithesis "setup complete" signal convention, not an SDK assertion call. No `assert_always`/`assert_sometimes`/`assert_reachable`/`assert_unreachable` usage.

## Summary

- **2 files** contain real Antithesis SDK assertions (`sk-recovery-race/driver.py`, `sk-recovery-race-chaos/driver.py`): 2 `always`, 1 `reachable`, 1 `sometimes` — all four target the same underlying claim family (orioledb#855, PK/secondary-key consistency across a checkpoint boundary).
- **0 assertions** exist in the C extension (`src/`, `include/`) or the patched Postgres core.
- **0 assertions** exist in the jepsen workload — its verdict is currently exported as a results file, not scored as an SDK assertion (gap, see above).
- The health-checker uses the standard setup-complete signal convention correctly but is not an assertion.

## Assumptions / Open Questions

- Did not inspect `test/antithesis/target/` (build output, likely snouty-generated docker-compose artifacts) for assertion usage — assumed it's derived from the source files scanned above, not hand-authored.
- Whether the jepsen Docker image itself (external, not vendored) contains any Antithesis SDK calls internally was not checked — only this repo's wrapper scripts were scanned, per the SUT path scope.
