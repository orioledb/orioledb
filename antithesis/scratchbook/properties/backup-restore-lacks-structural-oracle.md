# backup-restore-lacks-structural-oracle

## Focus

Backup/restore under fault injection (gap identified in evaluation: `sut-analysis.md`
§9/§10 name `pg_rewind`, pgbackrest, wal-g, `orioledb.replay_until_lsn` as an
untested workflow class). This property is about the *existing*
`test/integration/pgbackrest_test.py` and `test/integration/walg_test.py`
suites specifically — a meta/oracle-integrity finding in the same family as
the catalog's existing "Test Oracle and Harness Integrity" category
(`tbl-check-oracle-transient-false-negative`, `jepsen-verdict-not-sdk-visible`),
applied to the backup/restore surface, which that category didn't cover.

## What led to this

Per the task, I read `test/integration/pgbackrest_test.py` (622 lines) and
`test/integration/walg_test.py` (581 lines) end to end, plus their shared
scaffolding `test/t/base_backup_test.py`. Both files are real, substantial
integration tests: full/incremental backup, standby restore+promotion,
PITR by time/name/immediate target, delta/block-incremental diffing,
restore applied twice (idempotency), and (in pgbackrest's case) an explicit
"backup concurrent with an in-flight OrioleDB checkpoint" test that pins the
checkpoint with the `checkpoint_writeback` stopevent (`pgbackrest_test.py:499-533`).

Every correctness check in both files, without exception, is one of:

- `self.assertEqual(scratch.execute(...), <expected value>)` — a single
  scalar/column read (e.g. `SELECT message FROM status`), or
- `self.assertEqual(md5(string_agg(val, '' ORDER BY id)), <fingerprint>)` — a
  full-table content fingerprint.

I grepped both files (and `base_backup_test.py`) for `verify_orioledb`,
`amcheck`, and `orioledb_tbl_check`: **zero matches in either file.** No
restored node, in either integration test, is ever checked for OrioleDB
B-tree structural integrity — only for row-content equality.

This matters because `test/t/amcheck_test.py` (read for comparison, see
`doc/usage/getting-started.mdx:217-234`) confirms `verify_orioledb()` is the
project's own purpose-built structural checker (successor to
`orioledb_tbl_check()`, wired into `pg_amcheck` itself), and the catalog
already has direct evidence (`recovery-sk-rebuild-desync`'s evidence file)
that a structurally-broken table can still read back byte-identical *content*
via a plain query — the two root-cause docs it cites explicitly report
`orioledb_tbl_check()` returning `true` while a genuine PK/SK divergence was
present. A content fingerprint is exactly the class of check that class of
bug survives.

## Why this matters for backup/restore specifically

A physical backup/restore round-trip is a much more aggressive perturbation
of on-disk layout than ordinary crash recovery: files are copied out-of-band
(by pgbackrest/wal-g, not by Postgres's own WAL machinery) in whatever order
and timing the tool chooses, block-incrementally reconstructed from a chain
of prior backups (pgbackrest's `--repo1-block` path, tested at
`pgbackrest_test.py:325-419`), or diffed page-by-page against a previous full
backup (wal-g's delta path). Any of these reconstruction paths has more
opportunity than plain crash recovery to reproduce exactly the shape of bug
`recovery-sk-rebuild-desync` and `sk-extent-leak-after-crash` describe
(orphaned/leaked extents, PK/SK divergence) — silently, since the existing
tests' only oracle (content fingerprint) is not sensitive to it.

Put differently: today, if pgbackrest's block-incremental reconstruction (or
wal-g's delta reconstruction) ever reassembled an OrioleDB file with a
structurally broken B-tree that nonetheless still returned the right rows for
the specific queries these tests run, **both integration suites would report
green**.

## The property

| | |
|---|---|
| **Type** | Safety / Meta — a property about the backup/restore tests' own oracle, not directly about OrioleDB's data |
| **Property** | Every OrioleDB relation restored from a pgbackrest or wal-g backup (full, incremental/block-incremental, delta, or any PITR target) passes `verify_orioledb()` (equivalently, `pg_amcheck`) with zero rows returned, in addition to matching the expected row content — a restore is not considered verified by content-equality alone. |
| **Invariant** | `Always(verify_orioledb_returns_no_rows_after_restore)`: call `SELECT * FROM verify_orioledb(<relation>::regclass, true)` (the thorough/`force_file_check` variant, per `test/t/amcheck_test.py:38-41`) on every restored/scratch node these tests already stand up, immediately alongside the existing content-fingerprint assertions — not a new topology, just a new check bolted onto scenarios the tests already construct. |
| **Antithesis Angle** | No new fault-injection angle by itself — this is about strengthening the oracle used by scenarios the existing tests (and `backup-window-crash-untested`'s proposed new fault-injection scenarios) already construct or will construct. Once wired in, it turns every existing and future backup/restore scenario in these two files into a candidate for catching structural corruption, not just content divergence. |
| **Why It Matters** | Per `sut-analysis.md` §10, "wrong query results... is the worst-case failure for a database engine" — but a structurally-broken-yet-content-correct-today table is often a ticking time bomb (the next split, the next VACUUM-equivalent, the next checkpoint can turn it into a wrong-content bug later). The project's own oracle for this exists, is cheap to call (`AccessShareLock` only, per `sut-analysis.md` §10), and is simply never invoked anywhere in the backup/restore test surface — a pure coverage gap, not a design tradeoff. |

**Open Questions:**

- Does `verify_orioledb()`'s thorough (`force_file_check=true`) mode have a
  runtime cost that would meaningfully slow the existing integration tests
  (which already restore several scratch nodes per run) if added to all of
  them? `(needs a quick timing measurement, not investigated here)`
- `tbl-check-oracle-transient-false-negative`'s evidence file documents a
  transient false-negative window for `orioledb_tbl_check()` right after a
  checkpoint before a follow-up `CHECKPOINT`; does `verify_orioledb()` share
  that window, and if so, does calling it immediately after a restore (before
  the restored node has had a chance to run its own settling checkpoint) risk
  a false alarm specifically in this new context? `(needs investigation —
  flagged as open in the source property too, not re-resolved here)`
- Should this be `Always` per-relation or aggregated per-database (e.g. via
  `pg_amcheck -d mydb`, checking every checkable relation in one call)? A
  design choice for whoever implements the assertion.

### Investigation Log

#### Do either integration test file already call verify_orioledb/orioledb_tbl_check/pg_amcheck anywhere?

- Examined: `test/integration/pgbackrest_test.py`, `test/integration/walg_test.py`, `test/t/base_backup_test.py` (full text of all three).
- Found: zero occurrences of `verify_orioledb`, `orioledb_tbl_check`, or `amcheck` in any of the three files (confirmed via direct grep, not just skimming). Every correctness assertion in both integration tests is a scalar-column or content-fingerprint (`assertEqual`) check.
- Conclusion: the gap is real and total, not partial — no structural check exists anywhere in the current backup/restore test surface.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing**, same conclusion as `existing-assertions.md`'s overall finding:
zero Antithesis SDK assertions exist outside `test/antithesis/sk-recovery-race*`.
This property doesn't need new SUT-side (in-process C) instrumentation — it
needs a workload-side call to an already-exported SQL function
(`verify_orioledb()`), which is a client-visible check, not an internal state
requiring new C-level instrumentation.
