# Docker Documentation for OrioleDB

This document provides instructions on how to build Docker images for OrioleDB, and how to test them.

#### Prerequisites

Before you begin, make sure you have Docker installed on your local machine. If not, you can download and install it from the Docker official website. https://docs.docker.com/get-docker/

* `docker -v`

## Quickstart

Open a terminal and navigate to the OrioleDB project directory, if you are not already in it:
* `cd path/to/orioledb`

Build PostgreSQL 15 + OrioleDB extension:

* `docker build -t orioletest:15 --pull --network=host --progress=plain --build-arg PG_MAJOR="15" .`

Start server:

* `docker run --name oriolest15 -v orioletest15data:/var/lib/postgresql/data -e POSTGRES_PASSWORD=oriole123 -d orioletest:15`

Connect to the server via psql:

* `docker exec -ti oriolest15 psql -U postgres`

You should expect a similar psql message:
```
psql (15.4 OrioleDB public beta 2 PGTAG=patches15_18 alpine:3.17+clang build:2023-11-01T21:29:09+00:00)
Type "help" for help.

postgres=#
```

Enable orioledb extension:

* `create extension if not exists orioledb;`

Test some commands:
```
postgres=# select orioledb_version();
    orioledb_version
------------------------
 OrioleDB public beta 2
(1 row)

postgres=# \d+
                                           List of relations
 Schema |         Name         | Type |  Owner   | Persistence | Access method |  Size   | Description
--------+----------------------+------+----------+-------------+---------------+---------+-------------
 public | orioledb_index       | view | postgres | permanent   |               | 0 bytes |
 public | orioledb_index_descr | view | postgres | permanent   |               | 0 bytes |
 public | orioledb_table       | view | postgres | permanent   |               | 0 bytes |
 public | orioledb_table_descr | view | postgres | permanent   |               | 0 bytes |
(4 rows)

postgres=# \dx
                              List of installed extensions
   Name   | Version |   Schema   |                     Description
----------+---------+------------+------------------------------------------------------
 orioledb | 1.0     | public     | OrioleDB -- the next generation transactional engine
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
 view orioledb_index
 view orioledb_index_descr
 view orioledb_table
 view orioledb_table_descr
(39 rows)
```

Quit from the database:  `\q`


Stop the server:

* `docker stop oriolest15`

Remove container:

* `docker container rm oriolest15`

Remove docker image:

* `docker rmi orioletest:15`

Remove the data volume:

* `docker volume rm orioletest15data`



## Building Docker Images

To build a Docker image, use one of the following commands:

#### To build PostgreSQL 16 + OrieleDB extension
```
docker build -t orioletest:16 --pull --network=host --progress=plain --build-arg PG_MAJOR="16" .
```

#### To build PostgreSQL 15 + OrieleDB extension
```
docker build -t orioletest:15 --pull --network=host --progress=plain --build-arg PG_MAJOR="15" .
```

#### To build PostgreSQL 14 + OrieleDB extension
```
docker build -t orioletest:14 --pull --network=host --progress=plain --build-arg PG_MAJOR="14" .
```

#### To build PostgreSQL 13 + OrieleDB extension
```
docker build -t orioletest:13 --pull --network=host --progress=plain --build-arg PG_MAJOR="13" .
```

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

* `--build-arg ALPINE_VERSION="3.18"`
  *  Choose which version of Alpine Linux to use. Default is `3.17`.
  *  You can choose from `edge`, `3.18`, `3.17`, `3.16`, `3.15`, `3.14`, `3.13`.
* `--build-arg BUILD_CC_COMPILER="gcc"`
  * Choose the C compiler. Default is `clang`.
  * You can choose either `clang` or `gcc`.

* `--build-arg PG_MAJOR="16"`
  * Choose the main version of PostgreSQL. Default is `14`.
  * You can choose from `16`, `15`, `14`, `13`.
* `--build-arg DOCKER_PG_LLVM_DEPS='lvm15-dev clang15'`
  * Choose the LLVM build environment. Default is `llvm-dev clang`.
  * If you're using Alpine version `3.18` or higher, use `llvm15` because `llvm16` is not supported yet.

For example, to build an image using Alpine version `3.17`, the `gcc` compiler and PostgreSQL version `14`, use the following command:

```
docker build --network=host --progress=plain \
    --build-arg ALPINE_VERSION="3.17" \
    --build-arg BUILD_CC_COMPILER="gcc" \
    --build-arg PG_MAJOR="14" \
    -t orioletest:14-gcc-alpine3.17 .
```
This command will build the Docker image and tag it as `orioletest:14-gcc-alpine3.17.`

## Experimental OrioleDB + PostGIS Extension build:

Known limitations:
- It only works with Alpine `3.18`. This is due to a Docker `postgis/docker-postgis` limitation. The build script expects the `sfcgal` package, which is only available in Alpine 3.18 or later versions.
- OrioleDB `gist`, `sp-gist`, and other related indexes are not yet supported.


#### Step 1: create image: `orioletest:16-gcc-alpine3.18`

```
docker build --pull --network=host --progress=plain \
    --build-arg ALPINE_VERSION="3.18" \
    --build-arg BUILD_CC_COMPILER="gcc" \
    --build-arg PG_MAJOR="16" \
    --build-arg DOCKER_PG_LLVM_DEPS="llvm15-dev clang15" \
    -t orioletest:16-gcc-alpine3.18 .
```

#### Step2: Build the `oriolegis:16-3.4` image.

in a new directory, run this commands:

```
git clone https://github.com/postgis/docker-postgis.git
cd ./docker-postgis/16-3.4/alpine
docker build --network=host --progress=plain \
     --build-arg BASE_IMAGE=orioletest:16-gcc-alpine3.18 \
    -t oriolegis:16-3.4 .
```

## Developer notes:

To build all Docker image variations on a local machine, run the following command:
* `./ci/local_docker_matrix.sh`
