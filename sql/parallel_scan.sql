CREATE SCHEMA parallel_scan;
SET SESSION search_path = 'parallel_scan';
CREATE EXTENSION orioledb;

SET min_parallel_table_scan_size = 1;
SET min_parallel_index_scan_size = 1;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET enable_seqscan = ON;
SET enable_bitmapscan = OFF;
SET enable_indexscan = OFF;

CREATE TABLE seq_scan_test
(
	id serial primary key,
	i int4
) USING orioledb;

CREATE FUNCTION pseudo_random(seed bigint, i bigint) RETURNS float8 AS
$$
	SELECT substr(sha256(($1::text || ' ' || $2::text)::bytea)::text,2,16)::bit(52)::bigint::float8 / pow(2.0, 52.0);
$$ LANGUAGE sql;

ALTER SEQUENCE seq_scan_test_id_seq RESTART WITH 100000;

INSERT INTO seq_scan_test (i)
	SELECT pseudo_random(1, v) * 1200000 FROM generate_series(1,300000) v;
ANALYZE seq_scan_test;

CREATE INDEX seq_scan_test_ix1 ON seq_scan_test (i);

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF) SELECT count(*) FROM seq_scan_test WHERE i < 100;
SELECT count(*) FROM seq_scan_test WHERE i < 100;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF) SELECT count(*) FROM seq_scan_test WHERE i < 100;
SELECT count(*) FROM seq_scan_test WHERE i < 100;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF)
	SELECT * FROM seq_scan_test WHERE i < 100 ORDER BY i,id LIMIT 20;
SELECT * FROM seq_scan_test WHERE i < 100 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF)
	SELECT * FROM seq_scan_test WHERE i < 100 ORDER BY i,id LIMIT 20;
SELECT * FROM seq_scan_test WHERE i < 100 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF) SELECT count(*) FROM seq_scan_test WHERE i < 1000;
SELECT count(*) FROM seq_scan_test WHERE i < 1000;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF) SELECT count(*) FROM seq_scan_test WHERE i < 1000;
SELECT count(*) FROM seq_scan_test WHERE i < 1000;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF)
	SELECT * FROM seq_scan_test WHERE i < 1000 ORDER BY i,id LIMIT 20;
SELECT * FROM seq_scan_test WHERE i < 1000 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF)
	SELECT * FROM seq_scan_test WHERE i < 1000 ORDER BY i,id LIMIT 20;
SELECT * FROM seq_scan_test WHERE i < 1000 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM seq_scan_test WHERE i < 1000 OR i > 13000;
SELECT count(*) FROM seq_scan_test WHERE i < 1000 OR i > 13000;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM seq_scan_test WHERE i < 1000 OR i > 13000;
SELECT count(*) FROM seq_scan_test WHERE i < 1000 OR i > 13000;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF)
	SELECT * FROM seq_scan_test WHERE i < 1000 OR i > 13000 ORDER BY i,id LIMIT 20;
SELECT * FROM seq_scan_test WHERE i < 1000 OR i > 13000 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF)
	SELECT * FROM seq_scan_test WHERE i < 1000 OR i > 13000 ORDER BY i,id LIMIT 20;
SELECT * FROM seq_scan_test WHERE i < 1000 OR i > 13000 ORDER BY i,id LIMIT 20;

RESET max_parallel_workers_per_gather;
RESET min_parallel_table_scan_size;
RESET min_parallel_index_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexscan;

CREATE TABLE o_test_o_scan_register (
	val_1 TEXT PRIMARY KEY,
	val_2 DECIMAL NOT NULL
) USING orioledb;

INSERT INTO o_test_o_scan_register (val_1, val_2) VALUES ('A', 0), ('B', 0);

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT orioledb_parallel_debug_start();

EXPLAIN (COSTS OFF) SELECT val_1, val_2 FROM o_test_o_scan_register
    WHERE val_1 IN ('A', 'B') ORDER BY val_1;

SELECT val_1, val_2 FROM o_test_o_scan_register
    WHERE val_1 IN ('A', 'B') ORDER BY val_1;

SELECT orioledb_parallel_debug_stop();
COMMIT;

CREATE FUNCTION func_1(int, int) RETURNS int LANGUAGE SQL
AS
'select pg_advisory_xact_lock_shared($1); select 1;'
PARALLEL SAFE;

CREATE TABLE o_test_1 USING orioledb
AS SELECT val_1 FROM generate_series(1, 1000) val_1;

SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;
SET parallel_leader_participation = off;
SELECT sum(func_1(1,val_1)) FROM o_test_1;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;
RESET parallel_leader_participation;

BEGIN;
CREATE TABLE o_test_parallel_bitmap_scan (
	val_1 int
) USING orioledb;
CREATE INDEX ind_1 ON o_test_parallel_bitmap_scan (val_1);
INSERT INTO o_test_parallel_bitmap_scan SELECT generate_series(1, 10);
SELECT orioledb_parallel_debug_start();
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM o_test_parallel_bitmap_scan WHERE val_1 < 5;
SELECT count(*) FROM o_test_parallel_bitmap_scan WHERE val_1 < 5;
SELECT orioledb_parallel_debug_stop();
COMMIT;

BEGIN;

SELECT orioledb_parallel_debug_start();
SET LOCAL enable_seqscan = OFF;

CREATE TABLE o_test_parallel_index_scan (
	val_1 text,
	PRIMARY KEY(val_1)
) USING orioledb;

SELECT * FROM o_test_parallel_index_scan;

SELECT orioledb_parallel_debug_stop();
COMMIT;

-- Wrapper function, which converts result of SQL query to the text
CREATE OR REPLACE FUNCTION query_to_text(sql TEXT, out result text)
	RETURNS SETOF TEXT AS $$
	BEGIN
		FOR result IN EXECUTE sql LOOP
			RETURN NEXT;
		END LOOP;
	END $$
LANGUAGE plpgsql;

BEGIN;

SELECT orioledb_parallel_debug_start();

CREATE TABLE o_test_parallel_bitmap_scan_explain_buffers (
	val_1 integer NOT NULL PRIMARY KEY
)USING orioledb;


SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text($$
	EXPLAIN (ANALYZE, BUFFERS)
		SELECT * FROM o_test_parallel_bitmap_scan_explain_buffers
			ORDER BY val_1;
$$) as t;

SELECT orioledb_parallel_debug_stop();
COMMIT;

CREATE TABLE o_test_parallel_unique_index_scan (
	val_1 int,
	val_2 int,
	UNIQUE (val_1)
) USING orioledb;

INSERT INTO o_test_parallel_unique_index_scan
	SELECT v, v FROM generate_series(0, 2) v;

BEGIN;
SELECT orioledb_parallel_debug_start();

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_parallel_unique_index_scan ORDER BY val_1;
SELECT * FROM o_test_parallel_unique_index_scan ORDER BY val_1;

SELECT orioledb_parallel_debug_stop();
COMMIT;

BEGIN;
SELECT orioledb_parallel_debug_start();

CREATE TABLE o_test_parallel (
	val_1 int
) USING orioledb;

INSERT INTO o_test_parallel (val_1) VALUES (1);

SELECT * FROM o_test_parallel;

SELECT orioledb_parallel_debug_stop();
COMMIT;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA parallel_scan CASCADE;
RESET search_path;
