# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

OrioleDB is a **PostgreSQL extension** that replaces the heap with a new index-organized table access method. It is loaded via `shared_preload_libraries = 'orioledb.so'` and tables are created with `USING orioledb`. It is **not** standalone — every build links against a forked PostgreSQL that carries extensibility patches:

- PostgreSQL 16: branch [`patches16_34`](https://github.com/orioledb/postgres/tree/patches16_34)
- PostgreSQL 17: branch [`patches17_6`](https://github.com/orioledb/postgres/tree/patches17_6)

`pg_config` must resolve to the patched PostgreSQL build before any `make` invocation. On this dev machine that build lives at `/Users/voffk4/pg17/`.

## Build & test commands

```bash
# Standard build (production):
make USE_PGXS=1
make USE_PGXS=1 install

# Dev build — enables injection points, dev-only SQL upgrade scripts,
# and the IS_DEV preprocessor define. Required for the testgres tests.
make USE_PGXS=1 IS_DEV=1 install

# Full preferred rebuild on this machine (writes compile_commands.json
# for clangd/IDE integration):
compiledb make USE_PGXS=1 IS_DEV=1 \
  CFLAGS_SL="$(pg_config --cflags_sl)" \
  CFLAGS="$(pg_config --cflags) -DIS_DEV" \
  -j10 install

# Run all tests against the installed extension:
make USE_PGXS=1 installcheck

# Individual test groups (each accepts VALGRIND=1):
make USE_PGXS=1 regresscheck       # SQL tests in test/sql, compared to test/expected
make USE_PGXS=1 isolationcheck     # concurrency tests in test/specs
make USE_PGXS=1 testgrescheck      # python tests in test/t (requires `pip install -r requirements.txt`)
make USE_PGXS=1 testgrescheck_part_1   # first half (CI split)
make USE_PGXS=1 testgrescheck_part_2   # second half

# Code formatting (in-place):
make USE_PGXS=1 pgindent           # requires pgindent + pg_bsd_indent + (g)objdump in PATH
make USE_PGXS=1 yapf               # python formatter
```

### Running a single test

```bash
# Single SQL regression test:
psql -h /tmp -p <port> -f test/sql/<name>.sql
# (then diff against test/expected/<name>.out)

# Single isolation test:
pg_isolation_regress --inputdir=test --outputdir=test --temp-instance=tmp_check \
  --schedule=- <spec_name>   # e.g. `tx_speculative`

# Single testgres test (the most common in this branch):
source ../venv/bin/activate    # testgres lives in ../venv
python3 -m unittest test.t.crash.rr_stress_test.RrStressTest.test_bank_account_invariant
```

Testgres tests are dispatched by Python module path. The test file **must** end in `_test.py` to be discovered.

## Architecture — the parts that take multiple files to grok

### Index-organized storage, no heap

There is no PostgreSQL heap behind an orioledb table. The **primary key is the data**. Secondary indexes (SK) reference the PK value rather than a `ctid`. Consequences scattered throughout the codebase:

- `src/tableam/` is the PG table-AM glue but most of the real work happens in `src/btree/`.
- A row's "location" is its PK key; deletes/updates rewrite the tuple in place via undo records, not via separate heap and index tuples.
- Whole-table seqscans walk the PK B-tree. `count(DISTINCT col)` over a unique secondary index is answered by an **Index Only Scan on the SK** with `Heap Fetches: 0` — i.e. SK alone, without ever touching the PK. This split matters for diagnostics: a query result mismatch tells you which tree is out of sync.

### MVCC via in-memory undo log

`src/transam/undo.c` is the heart of MVCC. Per-transaction undo records accumulate in `cur_undo_locations`. The **CSN (commit sequence number)** lives in `xidBuffer` and flips through three states: `INPROGRESS` → `CSN_COMMITTING` (set by `current_oxid_precommit`) → final CSN (set by `current_oxid_commit`). Other transactions reading a row check that state to decide visibility.

The `undo_xact_callback` (registered via `RegisterXactCallback`) handles the three lifecycle events: `XACT_EVENT_PRE_COMMIT`, `XACT_EVENT_COMMIT`, `XACT_EVENT_ABORT`. The commit path inside `XACT_EVENT_COMMIT` is timing-sensitive — both bug investigations on this branch (deadlock + SK-leak) involve windows in this code path.

### Copy-on-write checkpoints + row-level WAL

`src/checkpoint/` implements CoW checkpoints — instead of overwriting pages, the next version is written elsewhere and the page pointer is atomically swapped. `src/recovery/wal.c` produces **row-level** WAL records (not page images), with record types `WAL_REC_XID`, `WAL_REC_UPDATE/INSERT/DELETE`, `WAL_REC_COMMIT`, `WAL_REC_ROLLBACK`. The replay side lives in `src/recovery/recovery.c` + `src/recovery/worker.c`.

### Page locking discipline

`src/btree/page_state.c` owns the per-page lock state machine (`lock_page`, `lock_page_with_tuple`, `release-after-split`). Most concurrent operations grab a page lock before walking — and the locks are non-PG (custom, not LWLock) because OrioleDB avoids the PG buffer mapper entirely. Anything page-related in a bug report should land you here first.

### Source ↔ header mirroring

`src/<topic>/*.c` is mirrored by `include/<topic>/*.h`. Topics: `btree`, `catalog` (orioledb's own catalog tables/caches), `checkpoint`, `indexam` (PG indexam glue), `recovery`, `rewind` (point-in-time rewind), `s3` (cloud-archive checkpoints), `tableam`, `transam`, `tuple`, `utils`, `workers` (orioledb's own bgwriter etc.).

## Generated files — don't hand-edit

- `include/utils/stopevents_defs.h` + `include/utils/stopevents_data.h` are generated from `stopevents.txt` by `stopevents_gen.py`. Re-run after adding a stop event.
- `orioledb.typedefs` is generated by `typedefs_gen.py` from the built `.so`; pgindent consumes it. Re-run after adding new C structs.
- `sql/orioledb--X.Y--A.B.sql` is generated by the build from `_prod.sql` + (when `IS_DEV=1`) `_dev.sql` siblings. Hand-edit only the `_prod`/`_dev` source pairs.

## Stop events vs injection points (both exist, different purposes)

**Stop events** (`STOPEVENT(id, jsonb_params)` macros in C, `pg_stopevent_set()` SQL) are the *original* concurrency-test mechanism: they're permanent code annotations that pause execution when a jsonpath condition matches. Defined in `stopevents.txt`, listed via `pg_stopevents()`. Used heavily by isolation tests in `test/specs/`. Enabled per-process via `SET orioledb.enable_stopevents = on` and have nontrivial overhead.

**Injection points** (`INJECTION_POINT(name)` macros from PostgreSQL's testing framework, requires `USE_INJECTION_POINTS`, gated by `IS_DEV=1`) are the *newer* mechanism for forcing crashes/errors at specific code sites. Used heavily by the crash-test harness in `test/t/crash/`. Attached at runtime with `SELECT injection_points_attach('name', 'error')`. When the point is inside a `START_CRIT_SECTION` an attached `error` escalates to PANIC.

If you're chasing a concurrency *deadlock* or *visibility* bug, stop events are the right tool. If you're chasing a *crash/recovery* bug, injection points are.

## Active investigation: crash-test harness under `test/t/crash/`

This branch (`add_stress_bank_account_test`) carries an in-development crash-recovery test harness. The user is hunting a recovery-side SK-index desynchronization bug. The harness, conventions, and current state are documented in detail in the auto-memory under `~/.claude/projects/-Users-voffk4-work-old-repos-orioledb/memory/` — see specifically `project_crash_test_system.md`, `project_minimal_repro_state.md`, and `reference_env_vars.md`. Key entry points:

- `test/t/crash/rr_stress_test.py` — primary harness (bank-account invariant under chaos)
- `test/t/crash/tx_flow.md` — annotated narrative of the COMMIT/ABORT/UPDATE WAL flow
- `test/t/crash/results/` — saved postgres logs from bug-reproducing trials (>200 MB each; logs only survive when PANIC or invariant violation detected)
- `src/transam/undo.c` around line 2400 — `orioledb-commit-assert` and `commit-assert-trace pre/post` instrumentation lives here
- `src/recovery/wal.c:336` — `orioledb-before-pre-commit-wal-finish` injection
- Many `csn-trace` and `wal-trace` `elog(LOG, ...)` markers throughout `src/btree/` and `src/transam/` — these are **load-bearing timing scaffolding** for the bug repro, NOT to be reflowed without expecting bug rate to change

### Streaming-standby recovery livelock — ROOT CAUSE PROVEN + FIX VERIFIED

The headline bug on this branch is solved. Full writeup + the failed-attempt history is in
**`test/t/crash/livelock_investigation/`** (read its `README.md` first; the deep model is
`recovery_oxid_classification_investigation.md`). One-paragraph summary:

> A transaction that aborts **in memory** advances the horizon `runXmin` (via
> `advance_run_xmin`, called by *both* commit and abort) — and that horizon is streamed to
> the standby as `finish.xmin`. But an empty/no-material-change abort takes the
> `wal_rollback` `!has_material_changes` **no-op**, leaving **no durable rollback record**.
> Crash recovery, which reconstructs state from durable artifacts only (the checkpoint xids
> dump + replayed WAL finish records — it never sees the primary's in-memory CSN),
> rediscovers that oxid as in-flight and emits a **deferred `WAL_REC_ROLLBACK`** (#876). On
> the standby that rollback re-admits the oxid into `xmin_queue` *below* `writtenXmin`,
> dragging `globalXmin` under the frozen watermark → committed oxids in the re-exposed band
> read `INPROGRESS` → `o_btree_modify_handle_conflicts` (no `waitCallback` in recovery)
> retries forever. **A watermark is a per-instance in-memory recyclability horizon, not a
> durability claim — serializing it across the instance boundary over-promises.**

Verified fix (zero added WAL): in `o_emit_recovery_finish_rollbacks` (`src/recovery/recovery.c`)
skip the deferred rollback for `oxid < recovery_xmin` — the horizon already passed those, so
they resolved in memory and the consumer already froze them; genuinely-in-flight oxids that
#876 needs are `≥ recovery_xmin` and still emitted. **0 livelocks / 100×60s-6s chaos trials.**
Two maintainer attempts (`81e8edcc`, `618396d2`) failed because they constrained *where the
horizon value comes from*, but the lowering enters via `queue_min` from the deferred rollback.

## Conventions that bite if you don't know them

- **Collation:** orioledb tables only support ICU, C, and POSIX collations. Default-locale `initdb` on macOS gives `UTF-8` collation which **breaks** orioledb text columns. Use `initdb --locale=C`.
- **`IS_DEV=1` must match the build that runs the tests.** A prod-built `orioledb.so` will pass injection-point attach calls (they go to an empty hash table) but never fire them, silently invalidating any test that depends on them.
- **postmaster's `SIGQUIT` (quickdie) bypasses `HOLD_INTERRUPTS`.** Backends mid-abort can die without finishing their undo cleanup, which is the suspected root cause of the active SK-leak investigation. `quickdie()` is its own signal handler that doesn't consult `InterruptHoldoffCount`; it runs immediately and calls `_exit(2)` — no `wal_rollback()`, no `apply_undo_stack` completion, nothing.

## Crit-section & PANIC tricks

These idioms are used throughout the crash-test harness and the recovery code. Understanding why each one works is essential for reading or extending bug-repro instrumentation.

### Forcing an immediate PANIC

- **`ereport(ERROR, ...)` inside `START_CRIT_SECTION`** escalates to `PANIC` automatically. PostgreSQL's elog machinery promotes `ERROR` → `PANIC` whenever `CritSectionCount > 0` — the reasoning is that if you can't safely longjmp out of a critical section, the whole cluster has to come down. **This is how `INJECTION_POINT(name)` with attached `'error'` action produces a PANIC** when its call site is wrapped by `START_CRIT_SECTION()` / `END_CRIT_SECTION()`. The idiom in `src/transam/undo.c` and `src/recovery/wal.c`:

  ```c
  START_CRIT_SECTION();
  INJECTION_POINT("orioledb-commit-assert");
  END_CRIT_SECTION();
  ```

- **`elog(LOG, "literal")` inside `START_CRIT_SECTION`** can ALSO produce an immediate crash, but via a different mechanism: `ereport`'s formatting helpers call `palloc`, and `palloc` checks `CritSectionCount` against `allowInCritSection` on the target context. The mismatch fires `Assert("CritSectionCount == 0 || (context)->allowInCritSection")` at `mcxt.c:1185`, which produces a `TRAP:` line in the server log and aborts the backend via `ExceptionalCondition` → `abort(2)`. **Use this as a back-door PANIC** when you can't reach `ereport(ERROR)` (e.g. when you specifically want the `mcxt.c` assertion fingerprint in the log for downstream filtering). Side effect: it's how a *misplaced* trace line silently brings down the cluster — search the log for `TRAP: failed Assert("CritSectionCount == 0` if a crash appears from nowhere after adding instrumentation.

- The two paths produce different log signatures:
  - `ereport(ERROR)` inside crit → `PANIC:` line + the original error message
  - `palloc` / `Assert` violation inside crit → `TRAP: failed Assert(...)` line, no `PANIC:`

  Any PANIC-detecting watchdog must match **both** patterns. The crash-test harness uses `grep -E "PANIC|TRAP:"` for exactly this reason.

### Forcing PANIC outside a crit section

- **`elog(PANIC, ...)`** is the obvious direct route — does not require a crit section, escalates immediately. Used in load-bearing invariant checks (e.g. `stuck spinlock detected at oxid_get_csn` in the original deadlock bug).
- **`Assert(false)`** in a debug/assert build produces `TRAP:` regardless of crit-section state (assert builds always `abort()` on failure).

### Crash propagation and detection

- **`restart_after_crash = on`** (default): postmaster restarts crashed backends and lets the cluster recover. Useful when you want to test recovery itself — the cluster comes back up and you can then validate invariants.
- **`restart_after_crash = off`**: PANIC kills the postmaster permanently. Useful when you want **unambiguous crash detection** — any "connection refused" to a previously-up cluster is proof of a PANIC, no race with the recovery window. The harness toggles this based on `RR_PANIC_FATAL`.
- **`quickdie()`'s "you should be able to reconnect" message goes ONLY to the client**, never to the server log. SIGQUIT'd backends die silently from the server-log perspective; their last log line is whatever workload trace they happened to emit just before SIGQUIT arrived. Only the postmaster's `server process (PID N) was terminated by signal 6` and `terminating any other active server processes` show up in the server log.

### `START_CRIT_SECTION` vs `HOLD_INTERRUPTS`

These two are often confused. They are not interchangeable.

| | `HOLD_INTERRUPTS()` | `START_CRIT_SECTION()` |
|---|---|---|
| Defers async signals (`SIGINT`/`SIGTERM`/timer) | yes | yes |
| Defers `SIGQUIT` (postmaster crash cascade) | **no** | **no** |
| `palloc` allowed | yes | **no** |
| `ereport(ERROR)` allowed | yes (raises ERROR normally) | **escalates to PANIC** |

The COMMIT path in `undo_xact_callback`'s `XACT_EVENT_COMMIT` case is wrapped in `HOLD_INTERRUPTS()` but is NOT inside `START_CRIT_SECTION` for most of its length — that's why a `palloc`-ing `elog(LOG)` inside it works fine, while the same call inside the small `INJECTION_POINT` crit-section bracket would TRAP.

### Why instrumentation drops bug repro rate

`elog(LOG, ...)` is *not* free even in production: it serialises arguments, acquires `ErrorContext`'s lock, writes through the logging pipeline. Adding two `elog(LOG, ...)` calls inside `wal_commit` (the per-commit-injection pre/post bracket) drops the `before-pre-commit-wal-finish` bug rate from ~33% per trial to ~0%. The timing-sensitive race window shifts by microseconds when ~12k extra LOG lines/sec are emitted. **Rule:** when chasing a timing-sensitive bug, identify crashed PIDs from the `TRAP: ... PID: N` line directly rather than adding hot-path pre/post brackets around the injection.

Conversely, this is a *useful* knob: the 8 `csn-trace` `elog`s in `src/btree/page_state.c` (`lock_page request/got/wait/release` etc.) act as deliberate per-op speed bumps that broaden the SK race window enough to push the bug rate to ~75%. The current branch keeps those enabled as load-bearing timing scaffolding — do not reflow them without expecting the bug rate to change.
