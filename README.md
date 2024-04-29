# OrioleDB – building a modern cloud-native storage engine
(... and solving some PostgreSQL wicked problems)

[![build status](https://github.com/orioledb/orioledb/actions/workflows/build.yml/badge.svg)](https://github.com/orioledb/orioledb/actions)
[![codecov](https://codecov.io/gh/orioledb/orioledb/branch/main/graph/badge.svg?token=shh4jn0DUK)](https://codecov.io/gh/orioledb/orioledb) [![dockerhub](https://github.com/orioledb/orioledb/actions/workflows/docker.yml/badge.svg)](https://hub.docker.com/r/orioledb/orioledb/tags)


OrioleDB is a new storage engine for PostgreSQL, bringing a modern approach to
database capacity, capabilities and performance to the world's most-loved
database platform.

OrioleDB consists of an extension, building on the innovative table access
method framework and other standard Postgres extension interfaces. By extending
and enhancing the current table access methods, OrioleDB opens the door to
a future of more powerful storage models that are optimized for cloud and
modern hardware architectures.

OrioleDB is currently distributed under the standard PostgreSQL license.

1. Designed for modern hardware.  OrioleDB design avoids legacy CPU bottlenecks
   on modern servers containing dozens and hundreds CPU cores, providing
   optimized usage of modern storage technologies such as SSD and NVRAM.

2. Reduced maintenance needs.  OrioleDB implements the concepts of undo log
   and page-mergins, eliminating the need for dedicated garbage collection
   processes.  Additionally, OrioleDB implements default 64-bit transaction
   identifiers, thus eliminating the well-known and painful wraparound problem.

3. Designed to be distributed.  OrioleDB implements a row-level write-ahead
   log with support for parallel apply.  This log architecture is optimized
   for raft consensus-based replication allowing the implementation of
   active-active multimaster.

The key technical differentiations of OrioleDB are as follows:

1. No buffer mapping and lock-less page reading.  In-memory pages in OrioleDB
   are connected with direct links to the storage pages.  This eliminates the
   need for in-buffer mapping along with its related bottlenecks. Additionally,
   in OrioleDB in-memory page reading doesn't involve atomic operations.
   Together, these design decisions bring vertical scalability for Postgres
   to the whole new level.

2. MVCC is based on the UNDO log concept.  In OrioleDB, old versions of tuples
   do not cause bloat in the main storage system, but eviction into the undo
   log comprising undo chains.  Page-level undo records allow the system
   to easily reclaim space occupied by deleted tuples as soon as possible.
   Together with page-mergins, these mechanisms eliminate bloat in the majority
   of cases.  Dedicated VACUUMing of tables is not needed as well, removing
   a significant and common cause of system performance deterioration and
   database outages.

3. Copy-on-write checkpoints and row-level WAL.  OrioleDB utilizes
   copy-on-write checkpoints, which provides a structurally consistent snapshot
   of data every moment of time.  This is friendly for modern SSDs and allows
   row-level WAL logging.  In turn, row-level WAL logging is easy to
   parallelize (done), compact and suitable for active-active
   multimaster (planned).

See [introduction](doc/intro.mdx), [getting started](doc/usage/getting-started.mdx), and [architecture](doc/architecture/overview.mdx)
 documentation as well as
[PostgresBuild 2021 slides](https://www.slideshare.net/AlexanderKorotkov/solving-postgresql-wicked-problems).  To start the development see [OrioleDB development quickstart](doc/contributing/local-builds.mdx), and [project structure](doc/contributing/structure.mdx).

## Status

OrioleDB now has public beta status.  It is recommended for experiments,
testing, benchmarking, etc., but is not recommended for production usage.
If you are interested in OrioleDB's benefits in production, please
[contact us](mailto:sales@orioledb.com).

## Installation

### Use docker container

We provide docker images for `amd64` and `arm64v8` architectures under Alpine Linux.

```
docker pull orioledb/orioledb:latest-pg16
```
For example it can be started same as postgres server:
```bash
# !Don't forget to set default locale to C, POSIX or use icu-locale
docker run --name some-postgres -e POSTGRES_PASSWORD=... -e POSTGRES_INITDB_ARGS="--locale=C" -d -p5432:5432 orioledb/orioledb:latest-pg16
```

See [our dockerhub](https://hub.docker.com/r/orioledb/orioledb) for details on our docker container usage.  See [the docker build guide](doc/contributing/docker-builds.mdx) for information on how to build the docker images locally.

### Build from source

Before building and installing OrioleDB, one should ensure to have the following:

 * [PostgreSQL with extensibility patches](https://github.com/orioledb/postgres): [16 (tag: patches16_33)](https://github.com/orioledb/postgres/tree/patches16_33) or [17 (tag: patches17_5)](https://github.com/orioledb/postgres/tree/patches17_5);
 * Development package of libzstd;
 * python 3.5+ with testgres package.

Typical installation procedure may look like this:

```bash
 $ git clone https://github.com/orioledb/orioledb
 $ cd orioledb
 # Make sure that postgres bin directory is in PATH before running
 $ make USE_PGXS=1
 $ make USE_PGXS=1 install
 $ make USE_PGXS=1 installcheck
```

Before starting working with OrioleDB, adding the following line to
`postgresql.conf` is required.  This change requires a restart of
the PostgreSQL database server.

```
shared_preload_libraries = 'orioledb.so'
```

## Collations
OrioleDB tables support only ICU, C, and POSIX collations.

So that you don't have to write COLLATE for every "text" field of tables you have options:
### Create whole cluster with one of these collations:
```bash
initdb --locale=C -D..
# OR
initdb --locale=POSIX -D..
# OR
initdb --locale-provider=icu --icu-locale=en -D...
```

### Create new database with default collation from template0
```bash
createdb --locale=C --template template0 ...
# OR
createdb --locale=POSIX --template template0 ...
# OR
createdb --locale-provider=icu --icu-locale=en --template template0 ...
```
Or using `CREATE DATABASE` with `LOCALE` or `ICU_LOCALE` parameters.

## Setup

Run the following SQL query on the database to enable the OrioleDB engine.


```sql
CREATE EXTENSION orioledb;
```

Once the above steps are complete, you can start using OrioleDB's tables.
See [getting started](doc/usage/getting-started.mdx) documentation for details.

```sql
CREATE TABLE table_name (...) USING orioledb;
```
