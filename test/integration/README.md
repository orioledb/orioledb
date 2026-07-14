# Integration tests

This directory holds testgres-based tests that exercise OrioleDB against
external tools, rather than PostgreSQL itself. Unlike `test/t`, these tests
are not wired into the Makefile's `testgrescheck`/`installcheck`/`check`
targets. Run them manually, with the corresponding tool installed and on
`PATH`.

## Tests

- `pgbackrest_test.py` - exercises the pgBackRest integration flow
  (stanza creation, full/incremental backups, standby restore/promotion,
  async archiving, and point-in-time recovery) against an OrioleDB table.
  Requires `pgbackrest` on `PATH`.

- `walg_test.py` - exercises the wal-g v2.0.1 integration flow
  (full/incremental backups, standby restore/promotion, and point-in-time
  recovery by time and named restore point) against an OrioleDB table.
  Requires `wal-g` on `PATH`.

## Running a test

Build and install OrioleDB first (see the top-level README), then run an
individual test module from the repository root:

```bash
python3 -m unittest -v test.integration.pgbackrest_test
```

Or a single test case:

```bash
python3 -m unittest -v test.integration.pgbackrest_test.PgBackRestTest.test_integration
```
