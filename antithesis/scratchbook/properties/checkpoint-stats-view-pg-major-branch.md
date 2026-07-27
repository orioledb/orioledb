# checkpoint-stats-view-pg-major-branch

## Focus

Version Compatibility (attention focus pass). Task prompt explicitly names
this as the example PG-major-version-conditional code path to check: "the
sk-recovery-race-chaos driver's own PG17-specific pg_stat_checkpointer-vs-
pg_stat_bgwriter branching." Unlike the other properties in this pass, the
version-conditional code under test lives in the **workload driver**
(`test/antithesis/`), not the OrioleDB C extension itself — but a bug here
would silently blind an existing liveness assertion on specific PG major
versions without any visible failure, which is squarely in scope for a
test-quality-focused "Version Compatibility" pass.

## What was examined

- `test/antithesis/sk-recovery-race-chaos/driver.py:96-114`
  (`checkpoint_count`), called from `dml_burst` (`driver.py:160-188`) which
  backs the existing `sometimes(overlapped, "at least one automatic
  checkpoint fired while concurrent INSERT/UPDATE/DELETE were in flight
  against o_sk_pending", ...)` liveness assertion
  (`existing-assertions.md`'s already-cataloged `sometimes` finding at
  `driver.py:182-188`).
- **This exact function had a real, recently-fixed cross-PG-version bug.**
  `git show f0c818c1` ("sk-recovery-race-chaos/driver.py: Fix bug in
  checkpoint_count()", the 5th-most-recent commit on this branch per the
  session's git log):
  ```diff
  -    (timed, req) = execute(
  -        conn,
  -        "SELECT checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter"
  -    )[0]
  +    (has_checkpointer_view,) = execute(
  +        conn, "SELECT to_regclass('pg_stat_checkpointer') IS NOT NULL")[0]
  +    if has_checkpointer_view:
  +        (timed, req) = execute(
  +            conn,
  +            "SELECT num_timed, num_requested FROM pg_stat_checkpointer"
  +        )[0]
  +    else:
  +        (timed, req) = execute(
  +            conn,
  +            "SELECT checkpoints_timed, checkpoints_req "
  +            "FROM pg_stat_bgwriter"
  +        )[0]
  ```
  Commit message: "PostgreSQL 17 removed those columns and moved them
  (renamed to num_timed/num_requested) into a new pg_stat_checkpointer
  view." Before the fix, this function unconditionally queried
  `pg_stat_bgwriter.checkpoints_timed`/`checkpoints_req`, columns that do
  not exist on PG17+ (removed upstream, not just renamed within
  `pg_stat_bgwriter` — they moved to a whole new view). Since
  `test/antithesis/README`'s `PG_MAJOR` parameter and the CI matrix
  (`sut-analysis.md` §9) both span PG16/17/18, this bug would have fired on
  any PG17 or PG18 run: the `execute()` call would raise (undefined column),
  which — depending on how the surrounding driver handles a query exception
  in `dml_burst` — most likely crashes the driver/workload process outright
  (a loud, visible failure) rather than silently under-reporting, since
  `checkpoint_count()` has no exception handling of its own. Confirmed the
  fix is present in the current tree (`driver.py:96-114`, read above) and
  correctly detects the view via `to_regclass()` **at runtime**, which is
  the robust pattern (matches the file's own comment: "detect which one
  exists at runtime instead of hardcoding a version").
- Checked whether the fixed version has any remaining version-conditional
  gap: `to_regclass('pg_stat_checkpointer')` returns non-null exactly when
  the view exists (PG17+), so the branch is keyed off actual server
  capability, not off a `PG_MAJOR` environment variable or client-side
  constant — this means it would also transparently handle a hypothetical
  future PG19 change without further edits, as long as the new schema is
  introduced as a rename/split rather than removed again. No further gap
  identified in this function specifically.

## Conclusion

The bug was real, matches this focus's exact concern (PG-major-version
behavioral divergence causing a workload/driver defect), and has already
been fixed with the right pattern (runtime capability detection instead of
a hardcoded version branch). This is exactly the shape of "closed issue as
regression target" the property catalog methodology asks for: the fix is
recent enough (this session's own git history) that a regression — e.g., a
future refactor that reintroduces a hardcoded column list, or extends
`checkpoint_count()` to read another stats column that also moved across PG
majors without applying the same runtime-detection pattern — is a live risk
worth guarding against explicitly, not just historically interesting.

## What goes wrong if this is violated

If a future edit to `checkpoint_count()` (or a similar stats-reading helper
added elsewhere in the harness) reintroduces a hardcoded PG16-only or
PG17-only column reference:
- On the "wrong" PG major, the query raises an error. Given
  `checkpoint_count()` has no `try/except` of its own, this propagates up
  through `dml_burst()` (also unguarded around the `checkpoint_count` calls
  specifically, only the per-worker DML loop has `psycopg2.Error` handling)
  and crashes the whole chaos driver process — turning a workload-code bug
  into what would present as an environment/infra failure, an already-flagged
  triage-confusion pattern in `sut-analysis.md` §12 (the rewind/tini finding
  makes the same point about a different failure class): a defect in test
  code masquerading as a SUT or infra problem during triage.
- Less severely, if a future variant guards the query in a `try/except` that
  swallows the error, `checkpoint_count()` could return a stale/zero count
  that makes `overlapped` spuriously `False`, silently weakening the
  `sometimes()` liveness assertion on just one PG major version without any
  visible signal — the harder-to-detect failure mode this focus looks for.

## Antithesis angle

This is a driver-side regression guard rather than something SUT-side fault
injection interacts with directly. The useful "assertion" here is really a
CI/test-hygiene one: run the `sk-recovery-race-chaos` workload against all
three supported PG majors (16, 17, 18) as part of validating any future
change to `checkpoint_count()` or its callers, and confirm the `sometimes`
assertion fires (checkpoints observed) on all three — not just the one PG
major a developer happens to test locally. Cross-reference
`existing-assertions.md`: the `sometimes(overlapped, ...)` assertion already
exists and already exercises this function every run; no new SUT-side
instrumentation is needed here — the gap is process (multi-PG-major CI for
the harness itself), not missing instrumentation.

## Open Questions

- Does the Antithesis harness's own CI/validation process actually run the
  `sk-recovery-race-chaos` config against all three PG majors before
  merging driver changes, or only whichever `PG_MAJOR` a developer happens
  to set locally? If only one, this exact bug class (works on PG16, breaks
  on PG17/18, or vice versa) could recur and ship undetected until an
  Antithesis run happens to use the other major version.
  `(needs human input — depends on the team's CI setup for test/antithesis/,
  not found in this repo's `.github/workflows/` which appear scoped to the
  core extension's own `check.yml`, not the antithesis harness)`
- ~~Whether any other stats-reading helper in `test/antithesis/` has a similar
  unguarded, hardcoded version-specific column/view reference.~~ Resolved
  below.

### Investigation Log

#### Does any other stats-reading helper in test/antithesis/ have a similar hardcoded version-specific reference?

- Examined: `grep -rn "pg_stat_\|pg_catalog\.\|to_regclass\|PG_VERSION_NUM\|server_version" test/antithesis/**/*.py`.
- Found: all five matches are inside `sk-recovery-race-chaos/driver.py:96-114`
  (the `checkpoint_count` function itself, comment + code). No other
  Python file in `test/antithesis/` (health-checker is Go, not Python, and
  was not covered by this grep — see below) references any `pg_stat_*`
  view, `pg_catalog`, `to_regclass`, `PG_VERSION_NUM`, or `server_version`.
- Not found / not checked: `test/antithesis/health-checker/main.go` is Go,
  not Python, and wasn't covered by this grep pattern; per
  `existing-assertions.md` it only checks `pg_isready`, which has no
  version-specific SQL surface, so it's very unlikely to have this class of
  bug, but wasn't independently re-verified here.
- Conclusion: no other instance of this bug class found in the harness
  today. Question resolved (not `needs human input` after all) — dropping
  the tag.
