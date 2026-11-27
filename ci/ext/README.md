# Extension compatibility harness

Runs the regression suites that PostgreSQL contrib modules and third-party
extensions ship themselves, unmodified, against a server whose default table
access method is `orioledb`, and reports per test whether the upstream expected
output still matches.  The goal is a coverage picture, not a pass/fail gate:
every raw pg_regress verdict is kept, and the existing
`ci/filter_regression_diff.py` is applied only as a second, clearly labelled
column so the result reads like every other OrioleDB CI job.

Nothing in any suite is edited.  The only differences from a stock
`make installcheck` are `default_table_access_method = orioledb`,
`--load-extension=orioledb` on the pg_regress command line (the flag
`ci/check.sh` already uses for the core suites), and whatever a module itself
requests through the `--temp-config` file in its `REGRESS_OPTS` (which
temp-instance mode would otherwise have applied).  Start-time settings from
those files (`shared_preload_libraries`, `wal_level`, ...) are unioned into the
server configuration; everything else is applied with `ALTER SYSTEM` for that
suite only and reset afterwards, so one module's configuration cannot leak into
another module's output.

## Files

| File | Role |
|---|---|
| `extensions.json` | Manifest.  `contrib` entries come from the patched PostgreSQL tree at the `.pgtags` tag; `external` entries name an upstream repo and a `tag`: `latest` (the default) resolves to the newest release at fetch time, optionally filtered by `tag_match` (`${PG_MAJOR}` expands; pgaudit releases per major, wal2json names tags `wal2json_2_6`), a tag name or a 40-hex commit pins (pgjwt has no releases).  The resolved version is recorded in the results.  Per entry: `skip`, `notes`, `subdir` (module lives below the repo root), `build` / `build_args`, `depends_on` (built and installed first, like the extension's own CI does), `preload` and `server_config` (start-time settings its CI uses; `${EXT_DIR}` expands to this directory for fixture scripts), `pre_dirs` and `pre_sql` (what its CI runs before `make installcheck` for a `--use-existing` suite), `build` is `pgxs`, `pgrx` (cargo-pgrx at the version the crate pins, `build_args` = extra cargo features) or `postgis` (autogen/configure/make install).  `runner` selects how the suite is driven, see below; `regress` states the REGRESS list (`glob:test/sql/*.sql` or a list) and options for suites that have no PGXS Makefile. |
| `getkey.sh` | Zero-key script for pgsodium / vault, identical to what vault's own CI generates. |
| `smoke/` | `sql/` and `expected/` for the three extensions that ship no SQL suite at all (pg_jsonschema and wrappers test with pgrx `#[pg_test]` functions in their own server; pg_net with pytest behind nix-provisioned nginx/pathod).  These are the harness's own checks and are labelled `smoke` in the report. |
| `fetch.sh` | Downloads `contrib/<module>/` from the codeload tarball of `orioledb/postgres` at the pinned tag (same technique `ci/post_build_prerequisites.sh` uses) and shallow-clones externals. |
| `run.py` | Evaluates each module's Makefile with GNU make (`REGRESS`, `REGRESS_OPTS`, `ENCODING`, `NO_LOCALE`, `NO_INSTALLCHECK`) and invokes `pg_regress` exactly as `installcheck` would, one database per suite.  `--config-only` prints the server settings the selected suites need. |
| `report.py` | Parses `regression.out` / `regression.diffs`, saves one diff per failed test, runs the drift filter, labels diffs with its divergence classes (`PATTERNS`), writes `results.json`, `report.md`. |
| `Dockerfile.ext` | Published OrioleDB image plus python3, make, gcc, git, unidiff. |
| `docker-run.sh` | Local end-to-end run using the image. |

## Runners

| `runner` | Used for | What runs |
|---|---|---|
| `pg_regress` (default) | contrib, pgvector, wal2json, hypopg, index_advisor, http, plpgsql_check, pg_cron, pgaudit, pgmq, supabase_vault, pg_graphql | pg_regress with the arguments the module's `make installcheck` (or, for pg_graphql, its `bin/installcheck`) would pass |
| `installcheck` | pgtap | the module's own `make installcheck` target, which builds its parallel schedule; `EXTRA_REGRESS_OPTS` (a pgxs hook) carries `--load-extension=orioledb` |
| `pgtap` | pg_partman, pgsodium, pgjwt | each TAP file through psql the way `pg_prove` does, `tap_tests` globs from the extension's test README; the plan / `ok` / `not ok` lines are the verdict |
| `smoke` | pg_jsonschema, wrappers, pg_net | pg_regress over `smoke/` |
| `postgis` | postgis (core, loader, dumper, raster, topology, sfcgal) | PostGIS's own `make installcheck-base` (`regress/run_test.pl --extension`, then the upgrade pass that target runs after it, reported as `upgrade__*`) |

Non-pg_regress runners write their verdicts in pg_regress's `regression.out` / `regression.diffs` shape, so `report.py` treats every suite the same way.  Their diffs are not pg_regress diffs, so the drift filter cannot vouch for them: a pgTAP failure is the `not ok` lines and diagnostics, a PostGIS failure is run_test.pl's `N.OBT:` / `N.EXP:` listing (where the index-bridging NOTICE accounts for most lines); `report.py` still labels them.  PostGIS's `installcheck-base` runs its upgrade pass only after a clean first pass, as upstream.

## Run locally

```bash
ci/ext/docker-run.sh                    # all suites
ci/ext/docker-run.sh --kind contrib     # contrib only
ci/ext/docker-run.sh --only hstore,pgvector
ci/ext/docker-run.sh --rebuild           # rebuild the externals (after a `latest` tag moved)
open ci/ext/work/results/report.md
```

## In CI

`CHECK_TYPE=ext_tests` in `ci/check.sh` runs exactly the three steps below on
the patched server the job already built, appends `report.md` to the step
summary and uploads `ci/ext/work/results` as an artifact.  The check type is
listed in `check.yml` but only scheduled through `workflow_dispatch`, so it adds
nothing to pull-request runs unless asked for.  Its job status is informational.

## Run against a server you started yourself (CI, or a local patched build)

```bash
ci/ext/fetch.sh --pg-major 17
python3 ci/ext/run.py --config-only          # settings to put in postgresql.conf
python3 ci/ext/run.py                         # verifies settings, runs suites
python3 ci/ext/report.py --step-summary       # results.json, report.md
```

`run.py` refuses to start if the server does not have the settings the selected
suites need, so a misconfigured run cannot masquerade as an incompatibility.

## Reading the report

- **Raw pass**: pg_regress said `ok`.  This is the upstream compatibility number.
- **After drift filter**: raw passes plus failures whose entire diff the
  repository's existing `filter_regression_diff.py` already treats as known
  OrioleDB-vs-heap drift (NOTICE lines, plan node renames, row order).
- **🚫 unsupported**: the diff names an OrioleDB feature that is not supported or
  a heap-only introspection function.  Expected, and worth listing.
- **🔧 environment**: the failure comes from the test image, not OrioleDB (for
  example the Alpine image ships `plperl.so` without `libperl.so`).
- **❌ fail**: everything else.  These are the interesting rows.  When the drift
  filter itself crashed on a diff, the per-test table says so.

Statuses are derived from pg_regress's verdict plus the diff text; the harness
never rewrites expected output.

Adding an extension is one manifest entry.  Adding a divergence class is one
line to `PATTERNS` in `report.py`.
