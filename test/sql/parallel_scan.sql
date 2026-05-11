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

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
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
SET LOCAL enable_indexonlyscan = off;
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

BEGIN;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL min_parallel_table_scan_size = 1;
SET LOCAL min_parallel_index_scan_size = 1;
SET LOCAL max_parallel_workers_per_gather = 1;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_sort = off;
SET LOCAL enable_bitmapscan = off;

CREATE TABLE o_test_parallel_seq_rescan1 (
	val_1 int,
	val_2 int
) USING orioledb;

CREATE TABLE o_test_parallel_seq_rescan2 (
	val_1 int,
	val_2 int
) USING orioledb;

INSERT INTO o_test_parallel_seq_rescan1 VALUES (1, 10001);
INSERT INTO o_test_parallel_seq_rescan2 VALUES (1, 20001);

ANALYZE o_test_parallel_seq_rescan1;

EXPLAIN (COSTS OFF)
	SELECT ot1.val_1, ot1.val_2 ot1v, ot2.val_2 ot2v
		FROM o_test_parallel_seq_rescan1 ot1
		INNER JOIN o_test_parallel_seq_rescan2 ot2
			ON ot1.val_1 = ot2.val_1
				WHERE ot1.val_1 = 1;
SELECT ot1.val_1, ot1.val_2 ot1v, ot2.val_2 ot2v
	FROM o_test_parallel_seq_rescan1 ot1
	INNER JOIN o_test_parallel_seq_rescan2 ot2
	ON ot1.val_1 = ot2.val_1
		WHERE ot1.val_1 = 1;
COMMIT;

BEGIN;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL min_parallel_table_scan_size = 1;
SET LOCAL min_parallel_index_scan_size = 1;
SET LOCAL max_parallel_workers_per_gather = 1;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_sort = off;
SET LOCAL enable_bitmapscan = off;

CREATE TABLE o_test_parallel_seq_rescan_multiple_joins1 (
	val_1 int,
	val_2 int
) USING orioledb;

CREATE TABLE o_test_parallel_seq_rescan_multiple_joins2 (
	val_1 int,
	val_2 int
) USING orioledb;

CREATE TABLE o_test_parallel_seq_rescan_multiple_joins3 (
	val_1 int,
	val_2 int
) USING orioledb;

INSERT INTO o_test_parallel_seq_rescan_multiple_joins1
	SELECT v, v + 10000 FROM generate_series(1, 1000) v;
INSERT INTO o_test_parallel_seq_rescan_multiple_joins2
	SELECT v, v + 20000 FROM generate_series(1, 1000) v;
INSERT INTO o_test_parallel_seq_rescan_multiple_joins3
	VALUES (1, 30001), (1, 30002), (1, 30003);

ANALYZE o_test_parallel_seq_rescan_multiple_joins1,
		o_test_parallel_seq_rescan_multiple_joins2;

EXPLAIN (COSTS OFF)
	SELECT ot1.val_1, ot1.val_2 ot1v, ot2.val_2 ot2v, ot3.val_2 ot3v
		FROM o_test_parallel_seq_rescan_multiple_joins1 ot1
		INNER JOIN o_test_parallel_seq_rescan_multiple_joins2 ot2
			ON ot1.val_1 = ot2.val_1
		INNER JOIN o_test_parallel_seq_rescan_multiple_joins3 ot3
			ON ot1.val_1 = ot3.val_1
				WHERE ot1.val_1 = 1 ORDER BY ot1.val_1, ot1v, ot2v, ot3v;
SELECT ot1.val_1, ot1.val_2 ot1v, ot2.val_2 ot2v, ot3.val_2 ot3v
	FROM o_test_parallel_seq_rescan_multiple_joins1 ot1
	INNER JOIN o_test_parallel_seq_rescan_multiple_joins2 ot2
		ON ot1.val_1 = ot2.val_1
	INNER JOIN o_test_parallel_seq_rescan_multiple_joins3 ot3
		ON ot1.val_1 = ot3.val_1
			WHERE ot1.val_1 = 1 ORDER BY ot1.val_1, ot1v, ot2v, ot3v;
COMMIT;

BEGIN;

CREATE TABLE o_test_parallelscan_reinitialize1 (
	val_1 int,
	val_2 int,
	PRIMARY KEY(val_1, val_2)
) USING orioledb;

CREATE TABLE o_test_parallelscan_reinitialize2 (
	val_1 int,
	val_2 int,
	PRIMARY KEY(val_1,val_2)
) USING orioledb;

CREATE INDEX o_test_parallelscan_reinitialize1_ix
	ON o_test_parallelscan_reinitialize1 (val_1);
CREATE INDEX o_test_parallelscan_reinitialize2_ix
	ON o_test_parallelscan_reinitialize2 (val_1);

INSERT INTO o_test_parallelscan_reinitialize1
	SELECT 1, v FROM generate_series(1, 5) v;
INSERT INTO o_test_parallelscan_reinitialize2 VALUES (1,1), (1,2);

ANALYZE o_test_parallelscan_reinitialize1;
SET LOCAL parallel_leader_participation = off;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL parallel_tuple_cost = 0;
SET LOCAL min_parallel_table_scan_size = 0;
SET LOCAL max_parallel_workers_per_gather = 3;
SET LOCAL enable_indexscan = OFF;
SET LOCAL enable_hashjoin = OFF;
SET LOCAL enable_sort = OFF;
SET LOCAL enable_bitmapscan = OFF;

EXPLAIN (COSTS OFF)
	SELECT ot1.val_1, ot1.val_2 ot1v2, ot2.val_2 ot2v2
		FROM o_test_parallelscan_reinitialize1 ot1
		INNER JOIN o_test_parallelscan_reinitialize2 ot2
			ON ot1.val_1 = ot2.val_1
				WHERE ot1.val_1 % 1000 = 1 AND ot2.val_1 = any(array[1]);
SELECT ot1.val_1, ot1.val_2 ot1v2, ot2.val_2 ot2v2
	FROM o_test_parallelscan_reinitialize1 ot1
	INNER JOIN o_test_parallelscan_reinitialize2 ot2
		ON ot1.val_1 = ot2.val_1
			WHERE ot1.val_1 % 1000 = 1 AND ot2.val_1 = any(array[1]);

COMMIT;

BEGIN;
CREATE TABLE o_test_parallel_join (
	val_1 float8 NOT NULL,
	val_2 text NOT NULL,
	PRIMARY KEY(val_1, val_2)
) USING orioledb;

INSERT INTO o_test_parallel_join
	SELECT a, repeat('x', a) FROM generate_series(1, 3) as a;

CREATE TABLE o_test_parallel_join2 (
	val_1 float8 NOT NULL,
	val_2 text NOT NULL,
	PRIMARY KEY (val_1, val_2)
) USING orioledb;

INSERT INTO o_test_parallel_join2
	SELECT a, repeat('x', a) FROM generate_series(1, 3) as a;

SET LOCAL max_parallel_workers_per_gather = 2;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL parallel_tuple_cost = 0;
SET LOCAL min_parallel_table_scan_size = 1;
SET LOCAL enable_indexscan = OFF;
SET LOCAL enable_hashjoin = OFF;
SET LOCAL enable_mergejoin = OFF;
SET LOCAL enable_bitmapscan = OFF;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_parallel_join
		JOIN o_test_parallel_join2 USING (val_1, val_2);
SELECT * FROM o_test_parallel_join
	JOIN o_test_parallel_join2 USING (val_1, val_2);
COMMIT;

BEGIN;

SET LOCAL parallel_setup_cost = 0;
SET LOCAL min_parallel_table_scan_size = 1;

CREATE TABLE o_test_no_parallel_bitmap_scan (
    val_1 int4
) USING orioledb;

INSERT INTO o_test_no_parallel_bitmap_scan SELECT val_1 FROM generate_series(1,100) val_1;
CREATE INDEX o_test_no_parallel_bitmap_scan_ix1 ON o_test_no_parallel_bitmap_scan (val_1);

SET LOCAL enable_seqscan = off;

EXPLAIN (COSTS OFF)
    SELECT count(*) FROM o_test_no_parallel_bitmap_scan WHERE val_1 >= 10 AND val_1 < 50;
SELECT count(*) FROM o_test_no_parallel_bitmap_scan WHERE val_1 >= 10 AND val_1 < 50;
-- Parallel Bitmap Heap Scan is not implemented yet, so it doesn't used here
EXPLAIN (COSTS OFF)
    SELECT count(*) FROM o_test_no_parallel_bitmap_scan WHERE val_1 <@ int4range(10,50);
SELECT count(*) FROM o_test_no_parallel_bitmap_scan WHERE val_1 <@ int4range(10,50);
-- Parallel Bitmap Heap Scan is not implemented yet, so it doesn't used here
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_no_parallel_bitmap_scan WHERE val_1 <@ int4range(10,50);
SELECT * FROM o_test_no_parallel_bitmap_scan WHERE val_1 <@ int4range(10,50);

COMMIT;

BEGIN;

-- Parallel index range scan on primary index
CREATE TABLE o_test_parallel_index_range (
	id SERIAL PRIMARY KEY,
	val INT,
	name TEXT
) USING orioledb;

INSERT INTO o_test_parallel_index_range (val, name)
	SELECT g, 'name_' || g FROM generate_series(1, 100000) g;

CREATE INDEX o_test_parallel_index_range_val_idx
	ON o_test_parallel_index_range (val);

ANALYZE o_test_parallel_index_range;

SET LOCAL max_parallel_workers_per_gather = 2;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL parallel_tuple_cost = 0;
SET LOCAL min_parallel_table_scan_size = 1;
SET LOCAL min_parallel_index_scan_size = 1;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;

-- Range scan on primary key
EXPLAIN (COSTS OFF)
	SELECT id FROM o_test_parallel_index_range
		WHERE id BETWEEN 1000 AND 5000;
SELECT count(*) FROM o_test_parallel_index_range
	WHERE id BETWEEN 1000 AND 5000;

-- Range scan: serial vs parallel equivalence
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT count(*) AS serial_count
	FROM o_test_parallel_index_range WHERE id BETWEEN 1000 AND 5000;
SET LOCAL max_parallel_workers_per_gather = 2;
SELECT count(*) AS parallel_count
	FROM o_test_parallel_index_range WHERE id BETWEEN 1000 AND 5000;

-- Parallel index scan with aggregation on secondary index
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM o_test_parallel_index_range WHERE val > 50000;
SELECT count(*) FROM o_test_parallel_index_range WHERE val > 50000;

-- Serial vs parallel equivalence on primary index range
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT array_agg(id ORDER BY id) AS serial_ids
	FROM o_test_parallel_index_range WHERE id BETWEEN 990 AND 1010;
SET LOCAL max_parallel_workers_per_gather = 2;
SELECT array_agg(id ORDER BY id) AS parallel_ids
	FROM o_test_parallel_index_range WHERE id BETWEEN 990 AND 1010;

-- Plan shape: primary index range scan
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM o_test_parallel_index_range WHERE id > 50000;

-- Plan shape: index-only scan on secondary index
EXPLAIN (COSTS OFF)
	SELECT val FROM o_test_parallel_index_range
		WHERE val BETWEEN 100 AND 200;

-- Plan shape: full index scan
EXPLAIN (COSTS OFF)
	SELECT id FROM o_test_parallel_index_range;

-- Edge case: empty result set
SELECT count(*) FROM o_test_parallel_index_range
	WHERE val BETWEEN 999900 AND 999999;

-- Edge case: single row result
SELECT id, val FROM o_test_parallel_index_range WHERE id = 42;

-- Edge case: full table scan via index
SELECT count(*) FROM o_test_parallel_index_range WHERE val > 0;

-- Filter on non-index column: serial vs parallel equivalence
-- Tests that generic Filter conditions (not convertible to scan keys)
-- are correctly applied in parallel index scans
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT count(*) AS serial_filter_count
	FROM o_test_parallel_index_range WHERE val > 0 AND id > 50000;
SET LOCAL max_parallel_workers_per_gather = 2;
SELECT count(*) AS parallel_filter_count
	FROM o_test_parallel_index_range WHERE val > 0 AND id > 50000;

-- Parallel index scan with ORDER BY
SELECT count(*) FROM (
	SELECT id FROM o_test_parallel_index_range
		WHERE val BETWEEN 1000 AND 2000 ORDER BY id
) sub;

-- Secondary index parallel scan: index-only
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT count(*) AS serial_val_count
	FROM o_test_parallel_index_range WHERE val BETWEEN 1000 AND 2000;
SET LOCAL max_parallel_workers_per_gather = 2;
SELECT count(*) AS parallel_val_count
	FROM o_test_parallel_index_range WHERE val BETWEEN 1000 AND 2000;

-- Secondary index parallel scan: full row lookup
SET LOCAL enable_indexonlyscan = off;

SET LOCAL max_parallel_workers_per_gather = 0;
SELECT count(*) AS serial_full_count
	FROM o_test_parallel_index_range WHERE val BETWEEN 1000 AND 2000;
SET LOCAL max_parallel_workers_per_gather = 2;
SELECT count(*) AS parallel_full_count
	FROM o_test_parallel_index_range WHERE val BETWEEN 1000 AND 2000;

SET LOCAL max_parallel_workers_per_gather = 0;
SELECT array_agg(id ORDER BY id) AS serial_ids
	FROM o_test_parallel_index_range WHERE val BETWEEN 990 AND 1010;
SET LOCAL max_parallel_workers_per_gather = 2;
SELECT array_agg(id ORDER BY id) AS parallel_ids
	FROM o_test_parallel_index_range WHERE val BETWEEN 990 AND 1010;

DROP TABLE o_test_parallel_index_range;

-- Parallel index scan with SAOP (ScalarArrayOpExpr) on secondary index
-- Uses repeating val values (low cardinality) so planner picks parallel
CREATE TABLE o_test_parallel_saop (
	id SERIAL PRIMARY KEY,
	val INT,
	name TEXT
) USING orioledb;
INSERT INTO o_test_parallel_saop (val, name)
	SELECT (g % 100) + 1, 'name_' || g FROM generate_series(1, 100000) g;
CREATE INDEX o_test_parallel_saop_val_idx
	ON o_test_parallel_saop (val);
ANALYZE o_test_parallel_saop;
SET LOCAL max_parallel_workers_per_gather = 2;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL parallel_tuple_cost = 0;
SET LOCAL min_parallel_table_scan_size = 1;
SET LOCAL min_parallel_index_scan_size = 1;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;

-- SAOP literal IN-list: plan shape
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM o_test_parallel_saop
		WHERE val IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
-- SAOP literal IN-list: parallel result
SELECT count(*) FROM o_test_parallel_saop
	WHERE val IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
-- SAOP literal IN-list: serial result
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT count(*) AS serial_saop_count FROM o_test_parallel_saop
	WHERE val IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);

-- SAOP with full row lookup through secondary index
SET LOCAL enable_indexonlyscan = off;
SET LOCAL max_parallel_workers_per_gather = 2;
SELECT count(*) AS parallel_saop_full FROM o_test_parallel_saop
	WHERE val IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT count(*) AS serial_saop_full FROM o_test_parallel_saop
	WHERE val IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);

DROP TABLE o_test_parallel_saop;

COMMIT;

BEGIN;
CREATE TABLE o_test1(i int, t text) USING orioledb;
CREATE TABLE o_test2(i int, t text) USING orioledb;
INSERT INTO o_test1 SELECT x, repeat('a', 500) FROM generate_series(1,10) as x;
INSERT INTO o_test2 SELECT x, repeat('b', 500) FROM generate_series(1,20) as x;
ALTER TABLE o_test1 SET (parallel_workers = 0);
ALTER TABLE o_test2 SET (parallel_workers = 1);

SET LOCAL enable_material = off;
SET LOCAL parallel_setup_cost = 1.0;
SET LOCAL min_parallel_table_scan_size =0;
EXPLAIN (COSTS OFF)
	SELECT o_test1.i, substring(o_test1.t, 1,2) FROM o_test1
		LEFT JOIN (SELECT o_test2.i FROM o_test2 ORDER BY 1 LIMIT 5) ss
		ON o_test1.i < ss.i WHERE o_test1.i < 3;

SELECT COUNT(*) FROM orioledb_table_pages('o_test2'::regclass::oid);
SELECT orioledb_evict_pages('o_test2'::regclass::oid, 1);
SELECT COUNT(*) FROM orioledb_table_pages('o_test2'::regclass::oid);

SELECT o_test1.i, substring(o_test1.t, 1,2) FROM o_test1
	LEFT JOIN (SELECT o_test2.i FROM o_test2 ORDER BY 1 LIMIT 5) ss
	ON o_test1.i < ss.i WHERE o_test1.i < 3;

DROP TABLE o_test1;
DROP TABLE o_test2;
COMMIT;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA parallel_scan CASCADE;
RESET search_path;
