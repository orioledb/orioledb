---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-29
external_references:
  - path: doc/
    why: checked for any documentation of Antithesis assertion conventions; none found (workload assertions are only in test/antithesis/)
---

# Existing Antithesis SDK Assertions

Scanned: `src/`, `include/` (the orioledb C extension) and `test/antithesis/` (the Antithesis harness). Search patterns: `assert_always`, `assert_sometimes`, `assert_reachable`, `assert_unreachable`, `antithesis` (case-sensitive grep across `*.py`, `*.c`, `*.h`, `*.go`), plus `ALWAYS(`, `ALWAYS_OR_UNREACHABLE(`, `SOMETIMES(`, `REACHABLE(`, `UNREACHABLE(` for the vendored C macro names (see below). Per the scope restriction recorded in `property-catalog.md`, `/Users/artur/supabase/orioledb_postgres` is no longer scanned as of this update (it was checked in the original pass, with no hits, before the scope was narrowed).

**Update (2026-07-29): a vendored C SDK now exists and is already in use — this section is no longer accurate as originally written.** `include/utils/antithesis_sdk.h` (commit `f0b429c8`, "antithesis: vendor Antithesis SDK") implements `ALWAYS`/`ALWAYS_OR_UNREACHABLE`/`SOMETIMES`/`REACHABLE`/`UNREACHABLE` as C macros (mirroring `antithesis-sdk-cpp` 0.4.8's wire protocol), backed by `src/utils/antithesis_sdk.c`, compiled in only when `USE_ANTITHESIS_SDK` is defined — and it is: `test/antithesis/orioledb/Dockerfile` passes `-DUSE_ANTITHESIS_SDK` via `COPT` in the instrumented build. Two follow-up commits added real call sites (see "Found assertions" below): `72fb9c47` ("antithesis: add lock group instrumentation") and `d98de50c` ("antithesis: downlink instrumentation"). SUT-side (in-process) instrumentation is no longer absent — 5 call sites exist today, all guarding one specific concurrency gate (see below). See `sut-analysis.md` §11 for other candidate internal states (e.g. the checkpoint-during-recovery LSN synchronization gap) that remain uninstrumented.

**Also (2026-07-29):** `test/antithesis/sk-recovery-race-chaos/` was retired and folded into `test/antithesis/sk-rebuild-desync/` (checkpoint-timing swarming + its `sometimes()` check moved there; see below and `property-catalog.md`'s `recovery-sk-rebuild-desync` entry). Its assertions no longer exist as a separate file.

## Found assertions

### `src/btree/scan.c` and `src/btree/iterator.c` (C extension, SUT-side)

C SDK: `#include "utils/antithesis_sdk.h"`, macros used directly (no import statement — these are preprocessor macros, compiled in only under `USE_ANTITHESIS_SDK`).

| File:Line | Type | Message | Notes |
|---|---|---|---|
| `src/btree/scan.c:708` | `ALWAYS_OR_UNREACHABLE` | "parallel scan: on-disk downlink published before the disk-phase sort" | Parallel B-tree scan: a shared-memory downlink slot must never be published to the DSM array before `O_PARALLEL_DOWNLINKS_SORTED` is set on the same poscan, or a later-published slot could land on an index a finished consumer already read past, silently skipping that leaf page. Sampled under `downlinksPublish` held in shared mode. |
| `src/btree/scan.c:1369` | `ALWAYS_OR_UNREACHABLE` | (same invariant, second call site) | Second instance of the same publish-ordering gate; not yet read in detail — same mechanism as line 708 by inspection of the surrounding function. |
| `src/btree/iterator.c:219` | `ALWAYS` | "combined-read gate covers lock-group undo: point lookup" | `o_btree_find_tuple_by_key_cb()`: the combined-result gate (whether a read must merge the page image with undo) must account for the *whole parallel lock group's* undo, not just the calling backend's — a parallel worker's transaction undo hangs off the lock-group leader. Guards **orioledb#982**, a read-own-writes anomaly (a non-combined read in a parallel worker reverts pages below the transaction's own writes, silently losing them). |
| `src/btree/iterator.c:326` | `ALWAYS` | "combined-read gate covers lock-group undo: find-tuples start" | Same gate, at `o_btree_find_tuples_start()`; comment cross-references the point-lookup site above. |
| `src/btree/iterator.c:951` | `ALWAYS` | "combined-read gate covers lock-group undo: range iterator" | Same gate again, at the range-iterator variant. |

All five call sites check the identical condition shape: `combinedResult || !COMMITSEQNO_IS_NORMAL(csn) || !have_lock_group_undo(desc->undoType)` (or the scan.c equivalent for downlink publish ordering), backed by a new `have_lock_group_undo()` function (`src/transam/undo.c:2944`, declared `include/transam/undo.h:408`) that widens `have_current_undo()` to the whole lock group when running as a parallel worker. **orioledb#982 is not currently in `property-catalog.md`** — this instrumentation exists but no property entry, evidence file, or workload targets it yet.

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

- **C extension (`src/`, `include/`)**: 5 SUT-side call sites, all guarding one gate (parallel lock-group undo / combined-read correctness, `orioledb#982`) — 3 `ALWAYS` (`src/btree/iterator.c`), 2 `ALWAYS_OR_UNREACHABLE` (`src/btree/scan.c`, parallel downlink publish ordering, likely the same underlying parallel-worker concurrency surface). This was **0** as of the original research pass; the vendored SDK (`include/utils/antithesis_sdk.h`, `src/utils/antithesis_sdk.c`) and these call sites were added afterward. No corresponding property/evidence file/workload exists yet for `orioledb#982`.
- **`sk-recovery-race/driver.py`** (unchanged): 1 `always`, 1 `reachable` — targets orioledb#855's PK/SK consistency across the deterministically-pinned checkpoint boundary.
- **`sk-rebuild-desync/`** (replaces the retired `sk-recovery-race-chaos/`): 1 `always` (shared, called from three test commands), 1 `reachable`, 1 `sometimes` — targets `recovery-sk-rebuild-desync` (a distinct property from #855) plus the checkpoint-overlap liveness check folded in from the retired chaos workload.
- **0 assertions** exist in the jepsen workload — its verdict is currently exported as a results file, not scored as an SDK assertion (gap, see above).
- The health-checker uses the standard setup-complete signal convention correctly but is not an assertion.

## Assumptions / Open Questions

- Did not inspect `test/antithesis/target/` (build output, likely snouty-generated docker-compose artifacts) for assertion usage — assumed it's derived from the source files scanned above, not hand-authored.
- Whether the jepsen Docker image itself (external, not vendored) contains any Antithesis SDK calls internally was not checked — only this repo's wrapper scripts were scanned, per the SUT path scope.
- `src/btree/scan.c:1369` was located by grep but not read in as much depth as line 708 — assumed (not independently confirmed line-by-line) to guard the identical publish-ordering invariant based on the surrounding function's shape.
- Whether `orioledb#982` should become a cataloged property, and whether any workload currently exercises parallel scans/lock groups enough to reach these 5 call sites at all, was not investigated here (out of scope for an assertion inventory) — noted for whoever next updates `property-catalog.md`.
