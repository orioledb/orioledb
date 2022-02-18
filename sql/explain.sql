CREATE EXTENSION orioledb;

----
-- EXPLAIN (ANALYZE TRUE, BUFFERS TRUE) test
----

-- Generate pseudo-random string in deterministic way
CREATE FUNCTION generate_string(seed integer, length integer) RETURNS text
	AS $$
		SELECT substr(string_agg(
						substr(encode(sha256(seed::text::bytea || '_' || i::text::bytea), 'hex'), 1, 21),
				''), 1, length)
		FROM generate_series(1, (length + 20) / 21) i; $$
LANGUAGE SQL;

-- Wrapper function, which converts result of SQL query to the text
CREATE OR REPLACE FUNCTION query_to_text(sql TEXT) RETURNS SETOF TEXT AS $$
	BEGIN
		RETURN QUERY EXECUTE sql;
	END $$
LANGUAGE plpgsql;

-- table: primary index + TOAST
CREATE TABLE IF NOT EXISTS o_explain (
	key integer NOT NULL,
	val text,
	PRIMARY KEY(key)
) USING orioledb;

SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					INSERT INTO o_explain (
						SELECT id, generate_string(1, 3000)
						FROM generate_series(4501, 4700, 1) id
					);') as t;

-- just explain analyze without buffers
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE)
					SELECT count(*)
					FROM o_explain;') as t;

SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN ANALYZE
					SELECT count(*)
					FROM o_explain;') as t;

-- just explain buffers without analyze, fails
EXPLAIN (BUFFERS TRUE) SELECT count(*) FROM o_explain;

-- test lowecase letters
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('Explain (analyze TRUE, buffers TRUE)
					SELECT count(*)
					FROM o_explain;') as t;

-- does not use TOAST tree (does not fetch TOASTed values)
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT count(*)
					FROM o_explain;') as t;

-- uses TOAST to fetch values
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT *
					FROM o_explain ORDER BY val;') as t;

-- table: primary index + secondary index without TOAST
DROP TABLE o_explain;
CREATE TABLE o_explain (
	key integer NOT NULL,
	val integer NOT NULL,
	PRIMARY KEY(key)
) USING orioledb;
CREATE INDEX o_explain_sec_non_val ON o_explain (val);

SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					INSERT INTO o_explain (
						SELECT id, id + 1
						FROM generate_series(1, 5000, 1) id
					);') as t;

-- use secondary index for scan
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT *
					FROM o_explain ORDER BY val;') as t;

-- do not use secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT *
					FROM o_explain;') as t;

DROP TABLE o_explain;
CREATE TABLE o_explain (
	key integer NOT NULL,
	val1 integer NOT NULL,
	val2 integer NOT NULL
) USING orioledb;
CREATE INDEX o_explain_sec_non_val2 ON o_explain (val2);

INSERT INTO o_explain (SELECT id, id + 1, id + 2 FROM generate_series(1, 1000, 1) id);
ANALYZE o_explain;
SELECT SUM(key)  FROM o_explain WHERE val2 > 0;
SELECT SUM(val1) FROM o_explain WHERE val2 > 0;
SELECT SUM(val2) FROM o_explain WHERE val2 > 0; -- check sum

-- uses only secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT SUM(val2) FROM o_explain WHERE val2 > 0
					AND val2 < 1000;') as t;

-- uses only secondary index - primary index is ctid index stored in secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT ctid FROM o_explain WHERE val2 > 0
					AND val2 < 1000;') as t;
-- uses primary and secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT key FROM o_explain WHERE val2 > 0
					AND val2 < 1000;') as t;
-- uses primary and secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT key, val1 FROM o_explain WHERE val2 > 0
					AND val2 < 1000;') as t;
-- uses primary and secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT key, val2 FROM o_explain WHERE val2 > 0
					AND val2 < 1000;') as t;
-- uses primary and secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT val1 FROM o_explain WHERE val2 > 0;') as t;
-- uses primary and secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT val1, val2 FROM o_explain WHERE val2 > 0
					AND val2 < 1000;') as t;
-- uses only secondary index for fetching values from secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT val2 FROM o_explain WHERE val2 > 0
					AND val2 < 1000;') as t;
SELECT * FROM o_explain WHERE val2 BETWEEN 1 AND 10;

DROP TABLE o_explain;
CREATE TABLE o_explain (
	key integer NOT NULL,
	val1 integer NOT NULL,
	val2 integer NOT NULL,
	PRIMARY KEY(key)
) USING orioledb;
CREATE INDEX o_explain_sec_non_val2 ON o_explain (val2);

INSERT INTO o_explain (SELECT id, id + 1, id + 2 FROM generate_series(1, 1000, 1) id);
ANALYZE o_explain;
SELECT SUM(key)  FROM o_explain WHERE val2 > 0;
SELECT SUM(val1) FROM o_explain WHERE val2 > 0;
SELECT SUM(val2) FROM o_explain WHERE val2 > 0; -- check sum

-- uses only secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT SUM(s.val2) FROM (
						SELECT val2 FROM o_explain 
							WHERE val2 > 0 AND val2 < 1000 
							ORDER BY val2
					) s;') as t;
-- uses secondary index, primary key is stored in secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT key FROM o_explain WHERE val2 > 0
					AND val2 < 1000 ORDER BY val2;') as t;
-- uses primary and secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT key, val1 FROM o_explain WHERE val2 > 0
					AND val2 < 1000 ORDER BY val2;') as t;
-- uses only secondary index for fetching secondary index value and primary key
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT key, val2 FROM o_explain WHERE val2 > 0
					AND val2 < 1000 ORDER BY val2;') as t;
-- uses primary and secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT val1 FROM o_explain WHERE val2 > 0
					AND val2 < 1000 ORDER BY val2;') as t;
-- uses primary and secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT val1, val2 FROM o_explain WHERE val2 > 0
					AND val2 < 1000 ORDER BY val2;') as t;
-- uses only secondary index for fetching values from secondary index
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT val2 FROM o_explain WHERE val2 > 0
					AND val2 < 1000 ORDER BY val2;') as t;
SELECT * FROM o_explain WHERE val2 BETWEEN 1 AND 10;

DROP TABLE o_explain;

---
-- Test for TOAST values update
---
CREATE TABLE o_explain (
	key integer NOT NULL,
	t text NOT NULL,
	val integer NOT NULL,
	PRIMARY KEY(key)
) USING orioledb;
INSERT INTO o_explain (SELECT id, generate_string(1, 3000), id FROM generate_series(1, 200, 1) id);

-- do not use TOAST index for this queries (UPDATE TOAST with same values)
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					UPDATE o_explain SET val = val + 1;') as t;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					INSERT INTO o_explain
					(SELECT id, generate_string(1, 3000), id
					 FROM generate_series(1, 100, 1) id)
					ON CONFLICT (key) DO UPDATE
					SET val = o_explain.val + 1;') as t;

-- UPDATE TOAST with equal values (only TOAST reads for compare values)
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					INSERT INTO o_explain
					(SELECT id, generate_string(1, 3000), id
					 FROM generate_series(1, 100, 1) id)
					ON CONFLICT (key) DO UPDATE
					SET val = o_explain.val + 1, t = EXCLUDED.t;') as t;

-- clean-up after EXPLAIN ANALYZE test
DROP FUNCTION query_to_text;
DROP TABLE o_explain;
DROP EXTENSION orioledb CASCADE;
