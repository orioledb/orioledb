# Integration tests

This directory holds testgres-based tests that exercise OrioleDB against
external tools, rather than PostgreSQL itself. Unlike `test/t`, these tests
are not wired into the Makefile's `testgrescheck`/`installcheck`/`check`
targets (or its `yapf` target), since they depend on external binaries not
guaranteed to be present in every build environment. Run them manually,
with the corresponding tool installed and on `PATH`.

## Tests

- `pgbackrest_test.py` - exercises the pgBackRest integration flow
  (stanza creation, full/incremental backups, standby restore/promotion,
  async archiving, and point-in-time recovery) against an OrioleDB table.
  Requires `pgbackrest` on `PATH`.

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
