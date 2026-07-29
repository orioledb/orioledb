---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-29
external_references:
  - path: doc/
    why: checked for any documentation of Antithesis assertion conventions; none found (workload assertions are only in test/antithesis/)
---

# Existing Antithesis SDK Assertions

Scanned: `src/`, `include/` (the orioledb C extension — no hits) and `test/antithesis/` (the Antithesis harness — all hits found here). Search patterns: `assert_always`, `assert_sometimes`, `assert_reachable`, `assert_unreachable`, `antithesis` (case-sensitive grep across `*.py`, `*.c`, `*.h`, `*.go`). Per the scope restriction recorded in `property-catalog.md`, `/Users/artur/supabase/orioledb_postgres` is no longer scanned as of this update (it was checked in the original pass, with no hits, before the scope was narrowed).

**No Antithesis SDK assertions exist anywhere in the C extension itself (`src/`, `include/`).** All existing instrumentation lives in the Python workload drivers under `test/antithesis/`. This means every property currently checked is verified from the workload/client side only — there is no SUT-side (in-process) instrumentation yet. See `sut-analysis.md` §11 for candidate internal states (e.g. the checkpoint-during-recovery LSN synchronization gap) where a surgical SUT-side assertion would give materially better search guidance than a workload-only check, since they concern internal states not directly observable from a SQL client.

**Update (2026-07-29):** `test/antithesis/sk-recovery-race-chaos/` was retired and folded into `test/antithesis/sk-rebuild-desync/` (checkpoint-timing swarming + its `sometimes()` check moved there; see below and `property-catalog.md`'s `recovery-sk-rebuild-desync` entry). Its assertions no longer exist as a separate file.

## Found assertions

### `test/antithesis/sk-recovery-race/driver.py`

Python SDK: `from antithesis.assertions import always, reachable` (line 18).

| Line | Type | Message | Notes |
|---|---|---|---|
| 89-95 | `always` | "o_sk_pending PK rows match distinct SK tokens after the sk_modify_pending race (orioledb#855)" | Checked at both "startup" and "post-race" labels (called at lines 193, 195 in `main()`); payload includes `label`, `pk_rows`, `sk_distinct`, `tbl_check`. Verifies PK-row-count == distinct-SK-token-count and `orioledb_tbl_check()` returns true. |
| 164-168 | `reachable` | "all three DML backends parked at sk_modify_pending (PK applied, SK pending)" | Confirms the deterministically-constructed race window (via `pg_stopevent_set('sk_modify_pending', ...)`) is actually reached before a `CHECKPOINT` is issued into it. Payload: `pids`. |

### `test/antithesis/sk-rebuild-desync/`

Python SDK: `from antithesis.assertions import always` (in `helper_common.py`) and `from antithesis.assertions import reachable, sometimes` (in `parallel_driver_sk-rebuild-desync-dml`).

| File:Line | Type | Message | Notes |
|---|---|---|---|
| `helper_common.py:196` | `always` | "o_sk_desync PK rows match distinct SK tokens after ordinary commits and crash recovery of unrelated transactions (recovery-sk-rebuild-desync)" | Shared check called from `first_sk-rebuild-desync-setup`, `anytime_sk-rebuild-desync-check`, and `finally_sk-rebuild-desync-check`. Verifies PK-row-count == distinct-SK-token-count and `orioledb_tbl_check()` returns true, on a table with no `pg_stopevent_set` pinning — targets `recovery-sk-rebuild-desync`, not orioledb#855 directly. |
| `parallel_driver_sk-rebuild-desync-dml:74` | `reachable` | "sk-rebuild-desync DML unit observed a lost connection, consistent with fault injection landing mid-transaction" | Confirms Antithesis's fault injection is actually landing mid-transaction during the DML driver, not just configured to. |
| `parallel_driver_sk-rebuild-desync-dml:102` | `sometimes` | "at least one automatic checkpoint fired while concurrent INSERT/UPDATE/DELETE were in flight against o_sk_desync" | Folded in from the retired `sk-recovery-race-chaos` workload: confirms whichever checkpoint-timing preset this timeline swarmed in `first_` (`CHECKPOINT_CONFIG_PRESETS` in `helper_common.py`, applied via `ALTER SYSTEM SET` + `pg_reload_conf()`) is actually producing checkpoints that overlap DML, not just sitting configured-but-unexercised. |

### `test/antithesis/jepsen/`

No direct Antithesis SDK assertion calls found in this repo's jepsen wrapper scripts (`entrypoint`, `test/v1/jepsen-postgres/singleton_driver_jepsen-postgres`, `finally_jepsen-postgres`). The jepsen tool itself (external Clojure image, not vendored in this repo) performs its own serializability/anomaly analysis and writes `results.edn`/`history.edn`; `finally_jepsen-postgres` only copies those files to `$ANTITHESIS_OUTPUT_DIR` for external inspection. **This means the jepsen workload's correctness verdict is not currently wired into an Antithesis SDK assertion** (`always`/`sometimes`/`reachable`) — it produces an artifact for post-hoc review rather than a signal Antithesis's search can act on directly. This is a gap worth flagging to the `antithesis-workload` skill: converting the jepsen anomaly-detection result into an explicit `always(no_anomalies_found, ...)` assertion (parsed from `results.edn`) would let Antithesis's fault-guided search prioritize toward violations directly, rather than relying on an external results file nobody scores during the run.

### `test/antithesis/health-checker/main.go`

Contains the string `antithesis_setup` (line 9) as a static JSON status marker (`{"antithesis_setup":{"status":"complete", ...}}`), which is the Antithesis "setup complete" signal convention, not an SDK assertion call. No `assert_always`/`assert_sometimes`/`assert_reachable`/`assert_unreachable` usage.

## Summary

- **`sk-recovery-race/driver.py`** (unchanged): 1 `always`, 1 `reachable` — targets orioledb#855's PK/SK consistency across the deterministically-pinned checkpoint boundary.
- **`sk-rebuild-desync/`** (new, replaces the retired `sk-recovery-race-chaos/`): 1 `always` (shared, called from three test commands), 1 `reachable`, 1 `sometimes` — targets `recovery-sk-rebuild-desync` (a distinct property from #855) plus the checkpoint-overlap liveness check folded in from the retired chaos workload.
- **0 assertions** exist in the C extension (`src/`, `include/`).
- **0 assertions** exist in the jepsen workload — its verdict is currently exported as a results file, not scored as an SDK assertion (gap, see above).
- The health-checker uses the standard setup-complete signal convention correctly but is not an assertion.

## Assumptions / Open Questions

- Did not inspect `test/antithesis/target/` (build output, likely snouty-generated docker-compose artifacts) for assertion usage — assumed it's derived from the source files scanned above, not hand-authored.
- Whether the jepsen Docker image itself (external, not vendored) contains any Antithesis SDK calls internally was not checked — only this repo's wrapper scripts were scanned, per the SUT path scope.
