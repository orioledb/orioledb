CREATE DATABASE reindex_concurrently_test_db;
\c reindex_concurrently_test_db

CREATE SCHEMA orioledb;
CREATE EXTENSION orioledb SCHEMA orioledb;
create table t_reindex
(
    id int,
    someint int,
    balance int
) using orioledb;

CREATE TABLE test_step (
    test_step int
);

CREATE TABLE done_items (
    done_items int
);

INSERT INTO t_reindex SELECT t, 11 - t, 0 FROM generate_series(1,1000) as t;

SELECT oid, relname, relfilenode FROM pg_class where relname like 't_reindex%';

CREATE INDEX CONCURRENTLY t_reindex_idx ON t_reindex (id);
SELECT oid, relname, relfilenode FROM pg_class where relname like 't_reindex%';
SELECT pg_relation_size( 't_reindex_idx'::regclass);

UPDATE t_reindex SET id = id + 1000000;

SELECT pg_relation_size( 't_reindex_idx'::regclass);

\d+ t_reindex;
SELECT * FROM orioledb.orioledb_tbl_indices( 't_reindex'::regclass, true, true);

REINDEX INDEX CONCURRENTLY t_reindex_idx;

SELECT pg_relation_size( 't_reindex_idx'::regclass);
\d+ t_reindex;
SELECT * FROM orioledb.orioledb_tbl_indices( 't_reindex'::regclass, true, true);

set enable_seqscan = off;
SELECT COUNT(*) FROM t_reindex WHERE id > 1000000;
EXPLAIN SELECT COUNT(*) FROM t_reindex WHERE id > 1000000;
RESET enable_seqscan;

\c regression
DROP DATABASE reindex_concurrently_test_db;


