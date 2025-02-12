CREATE SCHEMA bitmap_scan;
SET SESSION search_path = 'bitmap_scan';
CREATE EXTENSION orioledb;

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

-- TODO: Fix these queries
-- INSERT INTO bitmap_test SELECT generate_series(1,100000);
-- ANALYZE bitmap_test; -- dumps core

INSERT INTO bitmap_test (i)
	SELECT pseudo_random(1, v) * 20000 FROM generate_series(1,5000) v;
ANALYZE bitmap_test;

CREATE INDEX bitmap_test_ix1 ON bitmap_test (i);

SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test WHERE i < 100;
SELECT count(*) FROM bitmap_test WHERE i < 100;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 100 ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 100 ORDER BY i LIMIT 20;

EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test WHERE i < 1000;
SELECT count(*) FROM bitmap_test WHERE i < 1000;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 1000 ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 1000 ORDER BY i LIMIT 20;

EXPLAIN (COSTS OFF)
	SELECT count(*) FROM bitmap_test WHERE i < 1000 OR i > 13000;
SELECT count(*) FROM bitmap_test WHERE i < 1000 OR i > 13000;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 1000 OR i > 13000 ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 1000 OR i > 13000 ORDER BY i LIMIT 20;

ALTER TABLE bitmap_test ADD COLUMN j int4;
ALTER TABLE bitmap_test ADD COLUMN h int4;
UPDATE bitmap_test SET j = pseudo_random(2, id) * 20000, h = pseudo_random(10, id) * 20000;
CREATE INDEX bitmap_test_ix2 ON bitmap_test (j);
CREATE INDEX bitmap_test_ix3 ON bitmap_test (h);
ANALYZE bitmap_test;
EXPLAIN (COSTS OFF)
	SELECT count(*) FROM bitmap_test
		WHERE i < 1000 AND j < 1000;
SELECT count(*) FROM bitmap_test
		WHERE i < 1000 AND j < 1000;

-- Wrapper function, which converts result of SQL query to the text
CREATE OR REPLACE FUNCTION query_to_text(sql TEXT, out result text)
	RETURNS SETOF TEXT AS $$
	BEGIN
		FOR result IN EXECUTE sql LOOP
			RETURN NEXT;
		END LOOP;
	END $$
LANGUAGE plpgsql;

-- Tests for bitmap EXPLAIN ANALYZE, BUFFERS
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN
						SELECT count(*) FROM bitmap_test WHERE i < 100') as t;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE, COSTS OFF)
						SELECT count(*) FROM bitmap_test WHERE i < 100') as t;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (COSTS OFF, BUFFERS)
						SELECT count(*) FROM bitmap_test WHERE i < 100') as t;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE, COSTS OFF, BUFFERS)
						SELECT count(*) FROM bitmap_test WHERE i < 100') as t;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE, COSTS OFF, BUFFERS)
						SELECT count(*) FROM bitmap_test
							WHERE i < 1000 OR i > 13000') as t;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE, COSTS OFF, BUFFERS)
						SELECT count(*) FROM bitmap_test
							WHERE i < 1000 AND j < 1000') as t;

-- Tests for bitmap EXPLAIN FORMAT JSON
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (FORMAT JSON, ANALYZE, COSTS OFF, BUFFERS)
						SELECT count(*) FROM bitmap_test WHERE i < 100;') as t;

-- Tests for intersection/union of all possible bitmap entries
CREATE OR REPLACE FUNCTION bitmap_test_high(int4) RETURNS int4
	AS $$ SELECT $1 & x'FFFFFC00'::int4
		$$
	LANGUAGE SQL;
CREATE OR REPLACE FUNCTION bitmap_test_low(int4) RETURNS int4
	AS $$ SELECT ($1::bit(32) & x'000003FF'::bit(32))::int4
		$$
	LANGUAGE SQL;
SET enable_bitmapscan = off;
SELECT *, bitmap_test_high(id) high, bitmap_test_low(id) low
	INTO TEMP bitmap_test_seq FROM bitmap_test;
SET enable_bitmapscan = on;
-- Select all possible bitmap entries and accept all possible operators
-- 1 - single
-- 2 - value list
-- 3 - bitmap
-- - - not present

CREATE OR REPLACE FUNCTION test_const_high_int() RETURNS int4
  IMMUTABLE PARALLEL SAFE AS $$
  SELECT 103424 $$ LANGUAGE sql;

SELECT i INTO bti FROM bitmap_test_seq
	WHERE high = test_const_high_int() ORDER BY low;
SELECT i, j INTO btj FROM bitmap_test_seq
	WHERE high = test_const_high_int() ORDER BY low;

-- i:1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  bitmap_test_high(id) = test_const_high_int();

-- i:2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 50)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 50)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  bitmap_test_high(id) = test_const_high_int();

-- i:3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 300)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 300)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  bitmap_test_high(id) = test_const_high_int();

-- i:1 AND j:- -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:1 OR j:- -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:1 AND j:1 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM btj OFFSET 1 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM btj OFFSET 1 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:1 AND j:1 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:1 OR j:1 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM btj OFFSET 1 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM btj OFFSET 1 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 AND j:- -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 OR j:- -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 AND j:1 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 8 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 8 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 AND j:1 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 3 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 3 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 OR j:1 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 6 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 6 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 OR j:1 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 69),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 200 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i ORDER BY i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 69),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 200 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
WHERE (i = ANY((SELECT ARRAY_AGG(i ORDER BY i) FROM s1)::int4[]) OR
		j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
		bitmap_test_high(id) = test_const_high_int();

-- i:2 AND j:2 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 6 LIMIT 5)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 6 LIMIT 5)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 AND j:2 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 4 LIMIT 5)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 4 LIMIT 5)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 AND j:2 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti OFFSET 200 LIMIT 10),
		 s2 AS (SELECT j FROM btj OFFSET 205 LIMIT 10)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti OFFSET 200 LIMIT 10),
	 s2 AS (SELECT j FROM btj OFFSET 205 LIMIT 10)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 OR j:2 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 10),
		 s2 AS (SELECT j FROM btj OFFSET 10 LIMIT 10)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti LIMIT 10),
	 s2 AS (SELECT j FROM btj OFFSET 10 LIMIT 10)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 OR j:2 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 69),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 200 LIMIT 50)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 69),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 200 LIMIT 50)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 AND j:- -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 OR j:- -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM bitmap_test_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 AND j:1 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 73 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 73 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 AND j:1 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 71 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 71 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 AND j:2 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 80 LIMIT 10)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 80 LIMIT 10)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 AND j:2 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 50 LIMIT 10)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 50 LIMIT 10)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 AND j:3 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 72)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 72)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 AND j:3 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 50 LIMIT 72)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 50 LIMIT 72)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 OR j:1 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 OR j:2 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 20)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 20)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:3 OR j:3 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 80)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 80)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:2 AND j:3 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i OFFSET 79 LIMIT 10),
		 s2 AS (SELECT j FROM btj ORDER BY i LIMIT 80)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i OFFSET 79 LIMIT 10),
	 s2 AS (SELECT j FROM btj ORDER BY i LIMIT 80)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

-- i:1 OR j:2 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i OFFSET 79 LIMIT 1),
		 s2 AS (SELECT j FROM btj ORDER BY i LIMIT 68)
	SELECT COUNT(*) FROM bitmap_test
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_high(id) = test_const_high_int();
WITH s1 AS (SELECT i FROM bti ORDER BY i OFFSET 79 LIMIT 1),
	 s2 AS (SELECT j FROM btj ORDER BY i LIMIT 68)
SELECT COUNT(*) FROM bitmap_test
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_high(id) = test_const_high_int();

DROP TABLE bitmap_test_seq;
DROP TABLE bti;
DROP TABLE btj;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 1000 AND j < 1000 ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 1000 AND j < 1000 ORDER BY i LIMIT 20;

EXPLAIN (COSTS OFF)
	SELECT count(*) FROM bitmap_test
		WHERE i < 1000 AND j > 1000 AND h > 19000;

SELECT count(*) FROM bitmap_test
		WHERE i < 1000 AND j > 1000 AND h > 19000;

SELECT * FROM bitmap_test
		WHERE i < 1000 AND j > 1000 AND h > 19000;

EXPLAIN (COSTS OFF)
	SELECT count(*) FROM bitmap_test
		WHERE i < 1000 OR j < 1000 OR h > 19000;

SELECT count(*) FROM bitmap_test
		WHERE i < 1000 OR j < 1000 OR h > 19000;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test WHERE i < 1000 OR j < 1000 OR h > 19000
	ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test WHERE i < 1000 OR j < 1000 OR h > 19000
	ORDER BY i LIMIT 20;

EXPLAIN (COSTS OFF)
	SELECT count(*) FROM bitmap_test
		WHERE i < 1000 OR j < 1000 OR h > 19000 AND ABS(h) > 10;
SELECT count(*) FROM bitmap_test
		WHERE i < 1000 OR j < 1000 OR h > 19000 AND ABS(h) > 10;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test
		WHERE i < 1000 OR j < 1000 OR h > 19000 AND ABS(h) > 10
		ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test
	WHERE i < 1000 OR j < 1000 OR h > 19000 AND ABS(h) > 10
	ORDER BY i LIMIT 20;

-- Test int8 indices
CREATE TABLE bitmap_test_int8
(
	id bigserial primary key,
	i int
) USING orioledb;

ALTER SEQUENCE bitmap_test_int8_id_seq RESTART WITH 100000;

INSERT INTO bitmap_test_int8 (i)
	SELECT pseudo_random(3, v) * 20000 FROM generate_series(1,5000) v;
ANALYZE bitmap_test_int8;

CREATE INDEX bitmap_test_int8_ix1 ON bitmap_test_int8 (i);

EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test_int8 WHERE i < 100;
SELECT count(*) FROM bitmap_test_int8 WHERE i < 100;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test_int8 WHERE i < 100 ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test_int8 WHERE i < 100 ORDER BY i LIMIT 20;


-- Test bitmap with another order of fields
CREATE TABLE bitmap_second_field_pk
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	PRIMARY KEY(value)
) USING orioledb;
CREATE INDEX bitmap_second_field_pk_ix1 ON bitmap_second_field_pk (key);
SELECT orioledb_tbl_indices('bitmap_second_field_pk'::regclass);
INSERT INTO bitmap_second_field_pk (key, value)
	SELECT pseudo_random(4, v) * 20000, v FROM generate_series(1,500) v;

EXPLAIN (COSTS OFF) SELECT * FROM bitmap_second_field_pk WHERE key < 1000;
SELECT COUNT(*) FROM bitmap_second_field_pk WHERE key < 1000;

-- Test not building bitmap for pkey
CREATE TABLE pkey_bitmap_test
(
	i int4 PRIMARY KEY
) USING orioledb;
INSERT INTO pkey_bitmap_test (i)
	SELECT pseudo_random(5, v) * 20000 FROM generate_series(1,5000) v
		ON CONFLICT DO NOTHING;
EXPLAIN (COSTS OFF) SELECT * FROM pkey_bitmap_test WHERE i < 100;

-- Test ctid bitmap
CREATE TABLE bitmap_test_ctid
(
	i int,
	j int
) USING orioledb;

INSERT INTO bitmap_test_ctid (i, j)
	SELECT pseudo_random(6, v) * 20000, pseudo_random(11, v) * 20000 FROM generate_series(1,5000) v;
ANALYZE bitmap_test_ctid;

CREATE INDEX bitmap_test_ctid_ix1 ON bitmap_test_ctid (i);
CREATE INDEX bitmap_test_ctid_ix2 ON bitmap_test_ctid (j);

CREATE OR REPLACE FUNCTION bitmap_test_ctid_high(tid) RETURNS int4
    AS $$ SELECT ($1::text::point)[0]::int4
		$$
	LANGUAGE SQL;
CREATE OR REPLACE FUNCTION bitmap_test_ctid_low(tid) RETURNS int4
    AS $$ SELECT ($1::text::point)[1]::int4
		$$
	LANGUAGE SQL;
SET enable_bitmapscan = off;
SELECT ctid ctid1, *, bitmap_test_ctid_high(ctid) high,
	bitmap_test_ctid_low(ctid) low
	INTO TEMP bitmap_test_ctid_seq FROM bitmap_test_ctid;
SET enable_bitmapscan = on;
-- Select all possible bitmap entries and accept all possible operators
-- 1 - single
-- 2 - value list
-- 3 - bitmap
-- - - not present

CREATE OR REPLACE FUNCTION test_const_high_ctid() RETURNS int4
  IMMUTABLE PARALLEL SAFE AS $$
  SELECT 0 $$ LANGUAGE sql;

SELECT i INTO bti FROM bitmap_test_ctid_seq
	WHERE high = test_const_high_ctid() ORDER BY low;
SELECT i, j INTO btj FROM bitmap_test_ctid_seq
	WHERE high = test_const_high_ctid() ORDER BY low;

-- i:1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 50)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 50)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 300)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 300)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:1 AND j:- -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:1 OR j:- -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:1 AND j:1 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM btj OFFSET 1 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM btj OFFSET 1 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:1 AND j:1 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:1 OR j:1 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 1),
		 s2 AS (SELECT j FROM btj OFFSET 1 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 1),
	 s2 AS (SELECT j FROM btj OFFSET 1 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 AND j:- -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 OR j:- -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 AND j:1 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 8 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 8 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 AND j:1 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 3 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 3 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 OR j:1 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 6 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 6 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 OR j:1 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 69),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 200 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i ORDER BY i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 69),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 200 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i ORDER BY i) FROM s1)::int4[]) OR
			j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 AND j:2 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 6 LIMIT 5)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 6 LIMIT 5)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 AND j:2 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 5),
		 s2 AS (SELECT j FROM btj OFFSET 4 LIMIT 5)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 5),
	 s2 AS (SELECT j FROM btj OFFSET 4 LIMIT 5)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 AND j:2 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti OFFSET 200 LIMIT 10),
		 s2 AS (SELECT j FROM btj OFFSET 205 LIMIT 10)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti OFFSET 200 LIMIT 10),
	 s2 AS (SELECT j FROM btj OFFSET 205 LIMIT 10)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 OR j:2 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti LIMIT 10),
		 s2 AS (SELECT j FROM btj OFFSET 10 LIMIT 10)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti LIMIT 10),
	 s2 AS (SELECT j FROM btj OFFSET 10 LIMIT 10)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 OR j:2 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i DESC LIMIT 69),
		 s2 AS (SELECT j FROM btj ORDER BY i DESC OFFSET 200 LIMIT 50)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i DESC LIMIT 69),
	 s2 AS (SELECT j FROM btj ORDER BY i DESC OFFSET 200 LIMIT 50)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 AND j:- -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 OR j:- -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM bitmap_test_ctid_seq EXCEPT SELECT j FROM btj LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		  j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[]) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 AND j:1 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 73 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 73 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 AND j:1 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 71 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 71 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 AND j:2 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 80 LIMIT 10)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 80 LIMIT 10)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 AND j:2 -> 2
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 50 LIMIT 10)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 50 LIMIT 10)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 AND j:3 -> -
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 72)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 72)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 AND j:3 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 50 LIMIT 72)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 50 LIMIT 72)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 OR j:1 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 1)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 1)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 OR j:2 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 20)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 20)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:3 OR j:3 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
		 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 80)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i LIMIT 72),
	 s2 AS (SELECT j FROM btj ORDER BY i OFFSET 100 LIMIT 80)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:2 AND j:3 -> 1
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i OFFSET 79 LIMIT 10),
		 s2 AS (SELECT j FROM btj ORDER BY i LIMIT 80)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i OFFSET 79 LIMIT 10),
	 s2 AS (SELECT j FROM btj ORDER BY i LIMIT 80)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) AND
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

-- i:1 OR j:2 -> 3
EXPLAIN (COSTS OFF)
	WITH s1 AS (SELECT i FROM bti ORDER BY i OFFSET 79 LIMIT 1),
		 s2 AS (SELECT j FROM btj ORDER BY i LIMIT 68)
	SELECT COUNT(*) FROM bitmap_test_ctid
		WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
			   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			  bitmap_test_ctid_high(ctid) = test_const_high_ctid();
WITH s1 AS (SELECT i FROM bti ORDER BY i OFFSET 79 LIMIT 1),
	 s2 AS (SELECT j FROM btj ORDER BY i LIMIT 68)
SELECT COUNT(*) FROM bitmap_test_ctid
	WHERE (i = ANY((SELECT ARRAY_AGG(i) FROM s1)::int4[]) OR
		   j = ANY((SELECT ARRAY_AGG(j) FROM s2)::int4[])) AND
			bitmap_test_ctid_high(ctid) = test_const_high_ctid();

DROP TABLE bitmap_test_ctid_seq;
DROP TABLE bti;
DROP TABLE btj;

EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test_ctid WHERE i < 100;
SELECT count(*) FROM bitmap_test_ctid WHERE i < 100;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test_ctid WHERE i < 100 ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test_ctid WHERE i < 100 ORDER BY i LIMIT 20;

-- Test multi column all valid bitmap
CREATE TABLE bitmap_test_multi
(
	id bigserial,
	id2 bigserial,
	i int,
	PRIMARY KEY (id, id2)
) USING orioledb;

ALTER SEQUENCE bitmap_test_multi_id_seq RESTART WITH 100000;
ALTER SEQUENCE bitmap_test_multi_id2_seq RESTART WITH 100000;

INSERT INTO bitmap_test_multi (i)
	SELECT pseudo_random(7, v) * 20000 FROM generate_series(1,5000) v;
ANALYZE bitmap_test_multi;

CREATE INDEX bitmap_test_multi_ix1 ON bitmap_test_multi (i);

EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test_multi WHERE i < 100;
SELECT count(*) FROM bitmap_test_multi WHERE i < 100;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test_multi WHERE i < 100 ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test_multi WHERE i < 100 ORDER BY i LIMIT 20;

CREATE SEQUENCE bitmap_test_multi_inval_id2_seq AS integer;

-- Test multi column some not valid bitmap
CREATE TABLE bitmap_test_multi_inval
(
	id bigserial,
	id2 real NOT NULL DEFAULT nextval('bitmap_test_multi_inval_id2_seq')::real,
	i int,
	PRIMARY KEY (id, id2)
) USING orioledb;

ALTER SEQUENCE bitmap_test_multi_inval_id_seq RESTART WITH 100000;

INSERT INTO bitmap_test_multi_inval (i)
	SELECT pseudo_random(8, v) * 20000 FROM generate_series(1,5000) v;
ANALYZE bitmap_test_multi_inval;

CREATE INDEX bitmap_test_multi_inval_ix1 ON bitmap_test_multi_inval (i);

EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test_multi_inval WHERE i < 100;
SELECT count(*) FROM bitmap_test_multi_inval WHERE i < 100;

EXPLAIN (COSTS OFF)
	SELECT * FROM bitmap_test_multi_inval WHERE i < 100 ORDER BY i LIMIT 20;
SELECT * FROM bitmap_test_multi_inval WHERE i < 100 ORDER BY i LIMIT 20;
SET enable_indexscan = ON;
SET enable_seqscan = ON;

-- Test complex query
CREATE TABLE bitmap_test_complex
(
	id integer NOT NULL,
	id2 integer DEFAULT 5,
	id3 integer DEFAULT 2,
	val text,
	PRIMARY KEY(id)
) USING orioledb;

CREATE INDEX bitmap_test_complex_ix1 ON bitmap_test_complex(val);
CREATE INDEX bitmap_test_complex_ix2 ON bitmap_test_complex(id2, id);
CREATE INDEX bitmap_test_complex_ix3 ON bitmap_test_complex(id, id2, val);
CREATE INDEX bitmap_test_complex_ix4 ON bitmap_test_complex(id, val, id2);
CREATE INDEX bitmap_test_complex_ix5 ON bitmap_test_complex(val, id, id2);

INSERT INTO bitmap_test_complex (id, val) SELECT i, i||'!' FROM generate_series(1,30,2) AS i;
UPDATE bitmap_test_complex SET id2 = id WHERE id < 10;

CREATE OR REPLACE FUNCTION smart_explain(sql TEXT) RETURNS SETOF TEXT AS $$
	DECLARE
		row RECORD;
		line text;
		indent integer;
		skip_indent integer;
		skip_start integer;
	BEGIN
		skip_indent := 0;
		skip_start := 0;
		FOR row IN EXECUTE sql LOOP
			line := row."QUERY PLAN";
			indent := length((regexp_match(line, '^ *'))[1]);
			IF line ~ '^ *->  Result' OR line ~ '^Result' THEN
				skip_indent := 6;
				skip_start := indent;
			ELSE
				IF indent >= skip_start THEN
					line := substr(line, skip_indent + 1);
				ELSE
					skip_indent := 0;
					skip_start := 0;
				END IF;
				RETURN NEXT line;
			END IF;
		END LOOP;
	END $$
LANGUAGE plpgsql;

SELECT smart_explain(
'EXPLAIN (COSTS OFF) SELECT * FROM bitmap_test_complex WHERE id IN
	(ABS((SELECT id FROM bitmap_test_complex WHERE id2 = 1)),
	 (SELECT id * 500 FROM bitmap_test_complex WHERE id = 1),
	 GREATEST(1,2), LEAST(11,12), COALESCE (NULL, NULL, 12),
	 15) ORDER BY id;');

SELECT * FROM bitmap_test_complex WHERE id IN
	(ABS((SELECT id FROM bitmap_test_complex WHERE id2 = 1)),
	 (SELECT id * 500 FROM bitmap_test_complex WHERE id = 1),
	 GREATEST(1,2), LEAST(11,12), COALESCE (NULL, NULL, 12),
	 15) ORDER BY id;

EXPLAIN (COSTS OFF) SELECT * FROM bitmap_test_complex WHERE val IN ('13!', 'b');
SELECT * FROM bitmap_test_complex WHERE val IN ('13!', 'b');
EXPLAIN (COSTS OFF) SELECT * FROM bitmap_test_complex WHERE val < '13!';
SELECT * FROM bitmap_test_complex WHERE val < '13!';

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA bitmap_scan CASCADE;
RESET search_path;