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
SET LOCAL force_parallel_mode = on;

EXPLAIN (COSTS OFF) SELECT val_1, val_2 FROM o_test_o_scan_register
    WHERE val_1 IN ('A', 'B') ORDER BY val_1;

SELECT val_1, val_2 FROM o_test_o_scan_register
    WHERE val_1 IN ('A', 'B') ORDER BY val_1;
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

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA parallel_scan CASCADE;
RESET search_path;
