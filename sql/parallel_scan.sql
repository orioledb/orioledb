CREATE EXTENSION orioledb;

SET min_parallel_table_scan_size = 1;
SET min_parallel_index_scan_size = 1;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET enable_seqscan = ON;
SET enable_bitmapscan = OFF;
SET enable_indexscan = OFF;

CREATE TABLE bitmap_test
(
	id serial primary key,
	i int4
) USING orioledb;

CREATE FUNCTION pseudo_random(seed bigint, i bigint) RETURNS float8 AS
$$
	SELECT substr(sha256(($1::text || ' ' || $2::text)::bytea)::text,2,16)::bit(52)::bigint::float8 / pow(2.0, 52.0);
$$ LANGUAGE sql;

ALTER SEQUENCE bitmap_test_id_seq RESTART WITH 100000;

INSERT INTO bitmap_test (i)
	SELECT pseudo_random(1, v) * 1200000 FROM generate_series(1,300000) v;
ANALYZE bitmap_test;

CREATE INDEX bitmap_test_ix1 ON bitmap_test (i);

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test WHERE i < 100;
SELECT count(*) FROM bitmap_test WHERE i < 100;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test WHERE i < 100;
SELECT count(*) FROM bitmap_test WHERE i < 100;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 100 ORDER BY i,id LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 100 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 100 ORDER BY i,id LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 100 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test WHERE i < 1000;
SELECT count(*) FROM bitmap_test WHERE i < 1000;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test WHERE i < 1000;
SELECT count(*) FROM bitmap_test WHERE i < 1000;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 1000 ORDER BY i,id LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 1000 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 1000 ORDER BY i,id LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 1000 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM bitmap_test WHERE i < 1000 OR i > 13000;
SELECT count(*) FROM bitmap_test WHERE i < 1000 OR i > 13000;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM bitmap_test WHERE i < 1000 OR i > 13000;
SELECT count(*) FROM bitmap_test WHERE i < 1000 OR i > 13000;

SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 1000 OR i > 13000 ORDER BY i,id LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 1000 OR i > 13000 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 5;
EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 1000 OR i > 13000 ORDER BY i,id LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 1000 OR i > 13000 ORDER BY i,id LIMIT 20;

SET max_parallel_workers_per_gather = 0;
RESET min_parallel_table_scan_size;
RESET min_parallel_index_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexscan;
DROP FUNCTION pseudo_random;
DROP EXTENSION orioledb CASCADE;
