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

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA parallel_scan CASCADE;
RESET search_path;
