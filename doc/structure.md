OrioleDB Source Code Structure
==============================

File structure
--------------

```
.github
  workflows  -- workflow definition
    build.yml -- build & test each commit
    docker.yml -- build docker images for each release
    rpm.yml -- build CentOS 7 packages on demand
  FUNDING.yml
ci -- scripts used within build.yml workflow
doc -- documentation
expected -- expected output for regression and isolation tests
include -- C-headers of extension
specs -- isolation tests
sql -- regression tests
src -- C-sources of extension
t -- python tests
Dockerfile -- Dockerfile used within docker.yml workflow
LICENSE -- defines PostgreSQL-licence for the project
Makefile -- defines make targets
README.md -- main documentation entrypoint
docker-entrypoint.sh -- entrypoint file for Docker images
orioledb--1.0.sql -- script for definition of extension SQL-level objects
orioledb.control -- extension control file
orioledb_isolation.conf -- configuration file used during isolation tests
orioledb_regression.conf -- configuration file used during regression tests
stopevents.txt -- list of "stop event"
stopevents_gen.py -- generates include/utils/stopevents_(defs|data).h
                     from stopevents.txt
typedefs_gen.py -- generates list of C-symbols in orioledb.so
                   to orioledb.typedefs
valgrind.supp -- suppression rules for valgrind checks
```

Makefile targets
----------------

All test-related targets accept `VALGRIND=1` argument, which makes the tests to
be run under [valgrind](https://valgrind.org/).  Valgrind makes tests about ~100
times slower but catches uninitialized memory access.  Another positive
side-effect of Valgrind's super slow test runs is completely different timings,
which can spot various types of errors.

 * `regresscheck` -- run SQL-tests (see below).
 * `isolationcheck` -- run isolation tests (see below).
 * `testgrescheck` -- run testgres tests (see below).
 * `testgrescheck_part_1` -- first half of testgres checks.  Testgres tests are
                             splitted in a two nearly equal halfs to evade
                             too long individual CI runs.
 * `testgrescheck_part_2` -- second half of testgres checks.
 * `installcheck` -- run all types of tests when installed using PostgreSQL
                     extension system (`USE_PGXS=1`).
 * `check` -- run all types of tests when installed from `contrib` folder of
              PostgreSQL source code.
 * `pgindent` -- automatically indents OrioleDB sources.
   [pgindent](https://github.com/postgres/postgres/blob/master/src/tools/pgindent/pgindent) 
   tool should be available in `$PATH`.  Note, that you need to install
   [pg_bsd_indent](https://git.postgresql.org/gitweb/?p=pg_bsd_indent.git;a=summary)
   first.  Also, you need GNU Objdump available in `$PATH` as `objdump` or
   `gobjdump`, or be specified in `OBJDUMP` environment variable.


Tests
-----

OrioleDB has 3 groups of tests described below.

 * SQL tests are located in the `sql` folder.  This is the most simple type of
   test. The sql-file gets passed to `psql` and the result is compared to
   the reference output in the `expected` folder.  Note that there might be
   multiple reference outputs for one input file.  For instance, `collate.sql`
   contains collation-aware tests.  The result may match to `collate.out`,
   `collate_1.out`, or `collate_2.out` depending on database encoding and the
   presence of libicu.

 * Isolation tests are located in the `specs` folder.  These tests simulate
   multiple connections running simultaneously.  See
   [README](https://github.com/postgres/postgres/blob/master/src/test/isolation/README)
   in the PostgreSQL source tree.  These tests are especially powerful in
   conjunction with stop events.

 * Python [testgres](https://pypi.org/project/testgres/) tests located in `t`.
   These are the most powerful and complex tests.  Additionally to the ability
   to simulate multiple simultaneous connections, they can perform actions with
   the whole PostgreSQL instance such as start, stop, backup, replication, etc.
   See the [testgres docs](https://postgrespro.github.io/testgres/) for details.

CI
--

OrioleDB uses GitHub CI.  The CI workflows are described below.

### `build.yml`

This workflow runs the following tests for each of the two compilers (gcc and
clang) and each of the supported PostgreSQL major versions (13, 14, and 15).

 * `normal` -- run tests without asserts and without debug symbols.
 * `debug` -- run test with asserts and with debug symbols.
 * `alignment` -- run test with asserts, with debug symbols, and with alignment
                  sanitizers.  This replaces running tests on strict alignment
                  architectures providing even somewhat stricter checks
                  (for instance, it traps you on accessing a properly-aligned
                  member of an unproperly aligned structure, which real hardware
                  wouldn't do).
 * `check_page` -- runs tests with asserts, with debug symbols, and with
                   `CHECK_PAGE_STRUCT` macro enabled.  This macro provides
                   the page structure check on every page unlock.
 * `valgrind_1` -- runs `regresscheck`, `isolationcheck` and
                   `testgrescheck_part_1` under valgrind with asserts and
                   with debug symbols.
 * `valgrind_2` -- runs `testgrescheck_part_2` under Valgrind with asserts and
                   with debug symbols.
 * `static` -- runs `clang-analyzer` or `cppcheck` over sources.

### `docker.yml`

This workflow builds docker images for amd64 and arm64v8 architectures under
Alpine Linux.  This Dockerfile is slightly adjusted
[PostgreSQL Dockerfile](https://github.com/docker-library/postgres).
See [our dockerhub](https://hub.docker.com/r/orioledb/orioledb) for details.

### `rpm.yml`

This workflow build RPM packages for CentOS 7 using specification from the
[orioledb/pgrpms](https://github.com/orioledb/pgrpms) repository, a fork
of [pgrpms](https://git.postgresql.org/gitweb/?p=pgrpms.git;a=summary)
repository.

Stop events
-----------

Stop events are special places in the code, where the execution could be stopped
on some condition.  Stop events are used for the reliable reproduction of
concurrency issues.  OrioleDB isolation and testgres tests use stop events.

Stop event exposes a set of parameters, encapsulated into jsonb value.  The
condition over stop event parameters is defined using jsonpath
language.

The SQL-level functions and variables for stop events manipulation are listed
below.

 * `orioledb.enable_stopevents` -- enables stop events checking for the process.
    Stop events checking is expensive and significantly affects the performance.
    This is why stop events are disabled by default.

 * `orioledb.trace_stopevents` -- enables logging of all the stop events.
    Disabled by default.

 * `pg_stopevent_set(eventname text, condition jsonpath) RETURNS void` --
    set the condition for the stop event.  Once the function is executed, all
    the processes, which run a given stop event with parameters satisfying
    the given jsonpath condition, will be stopped.

 * `pg_stopevent_reset(eventname text) RETURNS bool` --
    reset the stop event.  All the processes previously stopped on the given
    stop event will continue the execution.

 * `pg_stopevents(OUT stopevent text, OUT condition jsonpath, OUT waiter_pids int[]) RETURNS SETOF record` --
    returns all the stop events currently set with their conditions and
    waiter process pids.

At C-level, the following macros are there for managing stop events.

 * `STOPEVENTS_ENABLED()` -- checks if stop events are enabled.
 * `STOPEVENT(event_id, params)` -- raises given stop event with given jsonb
                                    parameters.
 * `STOPEVENT_CONDITION(event_id, params)` -- check stop event condition without
                                              stopping the execution.  Used for
                                              error simulation.

The list of stop events is defined in `stopevents.txt` file.  The
`stopevent_gen.py` script generates `include/utils/stopevents_defs.h` (macros)
and `include/utils/stopevents_data.h` (name strings) files with C-definitions
of the stop events list.
