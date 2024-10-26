# Docker Documentation for OrioleDB

This document provides instructions on how to build Docker images for OrioleDB, and how to test them.

#### Prerequisites

Before you begin, make sure you have Docker installed on your local machine. If not, you can download and install it from the Docker official website. https://docs.docker.com/get-docker/

* `docker -v`

## Quickstart

Open a terminal and navigate to the OrioleDB project directory, if you are not already in it:
* `cd path/to/orioledb`

Build (Alpine) PostgreSQL 17 + OrioleDB extension:

```bash
docker build -t orioletest:17 --pull --network=host --progress=plain --build-arg PG_MAJOR="17" .
```

Create a simple password for testing:
```bash
# echo -n OrioleDB | sha1sum
8f50c87b77ab9621c9ac8bb396d0630f306adcb0  -
```

Start server:

```bash
docker run --name orioletest17 \
   -v orioletest17data:/var/lib/postgresql/data \
   -e POSTGRES_PASSWORD=8f50c87b77ab9621c9ac8bb396d0630f306adcb0 \
   -d orioletest:17
```

Connect to the server via psql:

```bash
docker exec -ti orioletest17 psql -U postgres
```

You should expect a similar psql message:
```bash
psql (17.0 OrioleDB public beta 5 PGTAG=patches17_3 alpine:3.20+clang build:2024-10-25T19:54:25+00:00 17.0)
Type "help" for help.
postgres=#
```

Enable orioledb extension:

```sql
create extension if not exists orioledb;
```

Test some commands:
```
postgres=# select orioledb_version();
    orioledb_version
------------------------
 OrioleDB public beta 5
(1 row)

postgres=# CREATE TABLE oriole_test (a int) USING orioledb;
CREATE TABLE
postgres=# INSERT INTO  oriole_test VALUES (1), (2);
INSERT 0 2
postgres=# SELECT * FROM oriole_test;
 a
---
 1
 2
(2 rows)

postgres=# VACUUM ANALYZE oriole_test;
VACUUM

postgres=# \d+ oriole_test
                                       Table "public.oriole_test"
 Column |  Type   | Collation | Nullable | Default | Storage | Compression | Stats target | Description
--------+---------+-----------+----------+---------+---------+-------------+--------------+-------------
 a      | integer |           |          |         | plain   |             |              |
Access method: orioledb


postgres=# \d+
                                             List of relations
 Schema |         Name         | Type  |  Owner   | Persistence | Access method |    Size    | Description
--------+----------------------+-------+----------+-------------+---------------+------------+-------------
 public | oriole_test          | table | postgres | permanent   | orioledb      | 8192 bytes |
 public | orioledb_index       | view  | postgres | permanent   |               | 0 bytes    |
 public | orioledb_index_descr | view  | postgres | permanent   |               | 0 bytes    |
 public | orioledb_table       | view  | postgres | permanent   |               | 0 bytes    |
 public | orioledb_table_descr | view  | postgres | permanent   |               | 0 bytes    |
(5 rows)

postgres=# \dx
                              List of installed extensions
   Name   | Version |   Schema   |                     Description
----------+---------+------------+------------------------------------------------------
 orioledb | 1.2     | public     | OrioleDB -- the next generation transactional engine
 plpgsql  | 1.0     | pg_catalog | PL/pgSQL procedural language
(2 rows)

postgres=# \dx+ orioledb
                     Objects in extension "orioledb"
                           Object description
-------------------------------------------------------------------------
 access method orioledb
 function orioledb_commit_hash()
 function orioledb_compression_max_level()
 function orioledb_evict_pages(oid,integer)
 function orioledb_get_evicted_trees()
 function orioledb_get_index_descrs()
 function orioledb_get_table_descrs()
 function orioledb_has_retained_undo()
 function orioledb_idx_structure(oid,text,character varying,integer)
 function orioledb_index_description(oid,oid,oid,text)
 function orioledb_index_oids()
 function orioledb_index_rows(oid)
 function orioledb_page_stats()
 function orioledb_parallel_debug_start()
 function orioledb_parallel_debug_stop()
 function orioledb_recovery_synchronized()
 function orioledb_relation_size(oid)
 function orioledb_sys_tree_check(integer,boolean)
 function orioledb_sys_tree_rows(integer)
 function orioledb_sys_tree_structure(integer,character varying,integer)
 function orioledb_table_description(oid)
 function orioledb_table_description(oid,oid,oid)
 function orioledb_table_oids()
 function orioledb_table_pages(oid)
 function orioledb_tableam_handler(internal)
 function orioledb_tbl_are_indices_equal(regclass,regclass)
 function orioledb_tbl_bin_structure(oid,boolean,integer)
 function orioledb_tbl_check(oid,boolean)
 function orioledb_tbl_compression_check(bigint,oid,integer[])
 function orioledb_tbl_indices(oid)
 function orioledb_tbl_structure(oid,character varying,integer)
 function orioledb_ucm_check()
 function orioledb_version()
 function orioledb_write_pages(oid)
 function pg_stopevent_reset(text)
 function pg_stopevent_set(text,jsonpath)
 function pg_stopevents()
 function s3_get(text)
 function s3_put(text,text)
 type orioledb_index
 type orioledb_index[]
 type orioledb_index_descr
 type orioledb_index_descr[]
 type orioledb_table
 type orioledb_table[]
 type orioledb_table_descr
 type orioledb_table_descr[]
 view orioledb_index
 view orioledb_index_descr
 view orioledb_table
 view orioledb_table_descr
(51 rows)
```

Quit from the database:  `\q`


Stop the server:

* `docker stop orioletest17`

Remove container:

* `docker container rm orioletest17`

Remove docker image:

* `docker rmi orioletest:17`

Remove the data volume:

* `docker volume rm orioletest17data`

## Supported environment variables:

This project aims to maintain compatibility with the Docker Official PostgreSQL image, and therefore, it also supports the environmental variables found there:

* `POSTGRES_PASSWORD`
* `POSTGRES_USER`
* `POSTGRES_DB`
* `POSTGRES_INITDB_ARGS`
* `POSTGRES_INITDB_WALDIR`
* `POSTGRES_HOST_AUTH_METHOD`
* `PGDATA`

Read more:  https://github.com/docker-library/docs/blob/master/postgres/README.md

## Available Docker build args

Please check the Dockerfiles for the full list of build args!
- Alpine Linux: `./Dockerfile`
  - supported [ `edge 3.20 3.19 3.18 3.17 3.16 3.15 3.14` ]
  - example: `--build-arg ALPINE_VERSION="3.20" -f Dockerfile `
- Ubuntu Linux: `./Dockerfile.ubuntu`
  - supported [ `devel 24.10 24.04 22.04 20.04 oracular noble jammy focal` ]
  - example: `--build-arg UBUNTU_VERSION="24.04" -f Dockerfile.ubuntu `

Other important build args:
* `--build-arg PG_MAJOR="17"`
  * Choose the main version of PostgreSQL. Default is `17`.
  * You can choose from `16`, `17`.
* `--build-arg BUILD_CC_COMPILER="gcc"`
  * Choose the C compiler. Default is `clang`.
  * You can choose either `clang` or `gcc`.

For example, to build an image using Alpine version `3.20`, the `gcc` compiler and PostgreSQL version `16`, use the following command:

```bash
docker build --pull --network=host --progress=plain \
    --build-arg ALPINE_VERSION="3.20" \
    --build-arg BUILD_CC_COMPILER="gcc" \
    --build-arg PG_MAJOR="16" \
    -f Dockerfile \
    -t orioletest:16-gcc-alpine3.20 .
```

To build an image using Ubuntu version `devel`, the `clang` compiler and PostgreSQL version `17`, use the following command:

```bash
docker build --pull --network=host --progress=plain \
    --build-arg UBUNTU_VERSION="devel" \
    --build-arg BUILD_CC_COMPILER="clang" \
    --build-arg PG_MAJOR="17" \
    -f Dockerfile.ubuntu \
    -t orioletest:17-clang-ubuntu-devel .
```
The "devel" version is the latest development Ubuntu version, so it might not be stable.

## Experimental OrioleDB + PostGIS Extension build:

Known limitations:
- OrioleDB `gist`, `sp-gist`, and other related indexes are not yet supported.

#### Step 1: create image: `orioletest:17-gcc-alpine3.20`

```bash
docker build --pull --network=host --progress=plain \
    --build-arg ALPINE_VERSION="3.20" \
    --build-arg BUILD_CC_COMPILER="gcc" \
    --build-arg PG_MAJOR="17" \
    -t orioletest:17-gcc-alpine3.20 .
```

#### Step2: Build the `oriolegis:17-3.5-alpine` image.

in a new directory, run this commands:

```bash
git clone --depth=1 https://github.com/postgis/docker-postgis.git
cd ./docker-postgis/17-3.5/alpine
docker build --network=host --progress=plain \
     --build-arg BASE_IMAGE=orioletest:17-gcc-alpine3.20 \
     -t oriolegis:17-3.5-alpine .
```

## Developer notes:

To build all Docker image variations on a local machine, run the following command:
* `./ci/local_docker_matrix.sh`
* or (experimental)  `./ci/docker_matrix.sh --help`

##### Ubuntu
- Supported base images (with security updates):
  - https://hub.docker.com/_/ubuntu
- Supported base image architectures:
  - [ amd64, arm32v7, arm64v8, ppc64le, riscv64, s390x]

##### Alpine
- Supported base images (with security updates):
  - https://hub.docker.com/_/alpine
- Supported base image architectures:
  - [ amd64, arm32v6, arm32v7, arm64v8, i386, ppc64le, riscv64, s390x ]

#### macOS
On macOS you might need to install `bash` and `gnu-getopt` from Homebrew. To install them run the command:

```bash
brew install bash gnu-getopt
```

Update your `/etc/shells`:

```bash
echo /opt/homebrew/bin/bash >> /etc/shells
```

You may need to update your `PATH` variable in the `.bashrc` or `.zshrc` file:

```
PATH=/opt/homebrew/bin:/opt/homebrew/opt/gnu-getopt/bin:$PATH
```

##### Other:

* Testing: If you can test on architectures other than `amd64`, please let us know!

* Some QEMU versions can't emulate PostgreSQL JIT. In this case, use `jit=off`.

* Security: Note that ports which are not bound to the host (i.e., `-p 5432:5432` instead of `-p 127.0.0.1:5432:5432`) will be accessible from the outside. This also applies if you configured UFW to block this specific port, as Docker manages its own iptables rules. ( [Read More](https://docs.docker.com/network/iptables/) ). With a simple password and open ports, you can be infected by [crypto miners]( https://github.com/docker-library/postgres/issues/770#issuecomment-704460980 ) !

* Windows: If you encounter any problems, please use Windows Subsystem for Linux (WSL2).

* Extending the current OrioleDB Docker images is not easy; you can't use Ubuntu PostgreSQL packages (like: `postgresql-16-mobilitydb`) - you need to build from source.
