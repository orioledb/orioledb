CREATE SCHEMA indices_build;
SET SESSION search_path = 'indices_build';
CREATE EXTENSION orioledb;

-- Index build with primary key
CREATE TABLE o_indices0
(
	key bigint NOT NULL,
	val int,
	val2 int,
	PRIMARY KEY (key)
) USING orioledb;

CREATE INDEX o_indices0_idx1 ON o_indices0 (val);

SELECT orioledb_tbl_indices('o_indices0'::regclass);

INSERT INTO o_indices0 SELECT 1000 + i, 3000 + i, 3000 + i FROM generate_series(1, 500) AS i;

SET enable_seqscan = off;
SET enable_bitmapscan = off;

EXPLAIN (COSTS off) SELECT val FROM o_indices0 WHERE val > 0 ORDER BY val;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices0 WHERE val2 > 0;

CREATE INDEX o_indices0_idx2 ON o_indices0 (val2);
SELECT orioledb_tbl_indices('o_indices0'::regclass);

SELECT orioledb_tbl_structure('o_indices0'::regclass, 'nue');

EXPLAIN (COSTS off) SELECT val FROM o_indices0 WHERE val > 0 ORDER BY val;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices0 WHERE val2 > 0 ORDER BY val2;

SELECT val2 FROM o_indices0 WHERE val2 > 0 ORDER BY val2;

SELECT orioledb_tbl_structure('o_indices0'::regclass, 'nue');

SELECT orioledb_tbl_are_indices_equal('o_indices0_idx1'::regclass, 'o_indices0_pkey'::regclass);
SELECT orioledb_tbl_are_indices_equal('o_indices0_idx1'::regclass, 'o_indices0_idx2'::regclass);

DROP INDEX o_indices0_idx2;

EXPLAIN (COSTS off) SELECT val FROM o_indices0 WHERE val > 0 ORDER BY val;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices0 WHERE val2 > 0;

SELECT orioledb_tbl_indices('o_indices0'::regclass);
SELECT orioledb_tbl_structure('o_indices0'::regclass, 'nue');

SELECT COUNT(val2) FROM o_indices0 WHERE val2 > 0;

-- Index build with ctid
CREATE TABLE o_indices1
(
	key int8 NOT NULL,
	val int8,
	val2 int8
) USING orioledb;

CREATE INDEX o_indices1_idx1 on o_indices1 (val);

INSERT INTO o_indices1 (SELECT id, id, id FROM generate_series(1, 10) as id);

CREATE INDEX o_indices1_idx2 on o_indices1 (val2);
CREATE INDEX o_indices1_idx3 on o_indices1 (key);

EXPLAIN (COSTS off) SELECT * FROM o_indices1;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices1 WHERE val2 > 5 ORDER BY val2;
EXPLAIN (COSTS off) SELECT key FROM o_indices1 WHERE key > 5 ORDER BY key;

SELECT * FROM o_indices1;
SELECT val2 FROM o_indices1 WHERE val2 > 5;
SELECT key FROM o_indices1 WHERE key > 5;

SELECT orioledb_tbl_structure('o_indices1'::regclass, 'nue');

SELECT orioledb_tbl_are_indices_equal('o_indices1_idx1'::regclass, 'o_indices1_idx2'::regclass);

SELECT orioledb_tbl_indices('o_indices1'::regclass);

DROP INDEX o_indices1_idx2;
SELECT orioledb_tbl_indices('o_indices1'::regclass);
DROP INDEX o_indices1_idx3;
SELECT orioledb_tbl_indices('o_indices1'::regclass);

-- Index build empty
CREATE TABLE o_indices2
(
	key bigint NOT NULL,
	val bigint,
	val2 bigint,
	PRIMARY KEY (key)
) USING orioledb;

CREATE INDEX o_indices2_idx1 on o_indices2 (val);

SELECT orioledb_tbl_indices('o_indices2'::regclass);

SELECT * FROM o_indices2;
SELECT val FROM o_indices2 WHERE val > 0 ORDER BY val;

-- index has data on disk after select, so table has
-- no values in primary index, but index should build
CREATE INDEX o_indices2_idx2 ON o_indices2 (val2);

SELECT orioledb_tbl_indices('o_indices2'::regclass);

SELECT orioledb_tbl_structure('o_indices2'::regclass, 'nue');

SELECT orioledb_tbl_are_indices_equal('o_indices2_idx1'::regclass, 'o_indices2_idx2'::regclass);

EXPLAIN (COSTS off) SELECT val FROM o_indices2 WHERE val > 0 ORDER BY val;

INSERT INTO o_indices2 SELECT 1000 + i, 3000 + i, 3000 + i FROM generate_series(1, 500) AS i;

SELECT COUNT(val) FROM o_indices2 WHERE val > 0;

SELECT orioledb_tbl_are_indices_equal('o_indices2_idx1'::regclass, 'o_indices2_idx2'::regclass);

-- Index build with root level > 1
CREATE TABLE o_indices3
(
	key text NOT NULL,
	val int8,
	val2 int8,
	PRIMARY KEY (key)
) USING orioledb;

CREATE INDEX o_indices3_idx1 ON o_indices3 (val);

INSERT INTO o_indices3 (SELECT id || 'key' || repeat('x', 500), id, id FROM generate_series(201, 700, 1) id);
INSERT INTO o_indices3 (SELECT id || 'key' || repeat('x', 500), id, id FROM generate_series(701, 1200, 1) id);
INSERT INTO o_indices3 (SELECT id || 'key' || repeat('x', 500), id, id FROM generate_series(1201, 1700, 1) id);
CREATE INDEX o_indices3_idx2 ON o_indices3 (val2);

SELECT orioledb_tbl_indices('o_indices3'::regclass);

SELECT regexp_replace
(
	orioledb_tbl_structure('o_indices3'::regclass, 'nue'),
	'x{2,}', 'XXXX', 'g'
);

SELECT val2 FROM o_indices3 WHERE val2 = 281;
SELECT val2 FROM o_indices3 WHERE val2 = 815;

SELECT regexp_replace
(
	orioledb_tbl_structure('o_indices3'::regclass, 'nue'),
	'x{2,}', 'XXXX', 'g'
);

EXPLAIN (COSTS off) SELECT val2 FROM o_indices3 WHERE val2 > 0 ORDER BY val2;

SELECT COUNT(val) FROM o_indices3 WHERE val > 0;
SELECT COUNT(val2) FROM o_indices3 WHERE val2 > 0;

SELECT orioledb_tbl_are_indices_equal('o_indices3_idx1'::regclass, 'o_indices3_idx2'::regclass);

-- Index build with reverse ordering
CREATE TABLE o_indices4
(
	key int8 NOT NULL,
	val int,
	val2 int,
	PRIMARY KEY (key)
) USING orioledb;

CREATE INDEX o_indices4_idx1 ON o_indices4 (val DESC);

INSERT INTO o_indices4 SELECT 1000 + i, 3000 + i, 3000 + i FROM generate_series(500, 1, -1) AS i;

CREATE INDEX o_indices4_idx2 ON o_indices4 (val2 DESC);

EXPLAIN (COSTS off) SELECT val FROM o_indices4 WHERE val > 0 ORDER BY val ASC;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices4 WHERE val2 > 0 ORDER BY val2 ASC;

EXPLAIN (COSTS off) SELECT val FROM o_indices4 WHERE val2 = 3001 AND
													 val2 > 0
													 ORDER BY val2;
SELECT val FROM o_indices4 WHERE val2 = 3001 AND val2 > 0 ORDER BY val2;

SELECT val FROM o_indices4 WHERE val > 0 ORDER BY val ASC;
SELECT val2 FROM o_indices4 WHERE val2 > 0 ORDER BY val2 ASC;

SELECT orioledb_tbl_structure('o_indices4'::regclass, 'nue');
SELECT orioledb_tbl_are_indices_equal('o_indices4_idx1'::regclass, 'o_indices4_idx2'::regclass);

-- Index build with multiple columns and reverse ordering
CREATE TABLE o_indices5
(
	key int8 NOT NULL,
	val int,
	val2 int,
	val3 int,
	val4 int,
	PRIMARY KEY (key)
) USING orioledb;

CREATE INDEX o_indices5_idx1 ON o_indices5 (val ASC, val2 DESC);

INSERT INTO o_indices5 SELECT 1000 + i, 3000 + i, 4000 + i,
										 3000 + i, 4000 + i
						FROM generate_series(500, 1, -1) AS i;

CREATE INDEX o_indices5_idx2 ON o_indices5 (val3 ASC, val4 DESC);

EXPLAIN (COSTS off) SELECT val2, val FROM o_indices5 WHERE val BETWEEN 3300 AND 3320 AND val2 > 0 ORDER BY val2 DESC;
EXPLAIN (COSTS off) SELECT val4, val3 FROM o_indices5 WHERE val3 BETWEEN 3300 AND 3320 AND val4 > 0 ORDER BY val4 DESC;

SELECT val4, val3 FROM o_indices5 WHERE val3 BETWEEN 3300 AND 3320 ORDER BY val4 DESC;

EXPLAIN (COSTS off) SELECT val4, val3 FROM o_indices5 ORDER BY val3 ASC;
SELECT val4, val3 FROM o_indices5 ORDER BY val3 ASC;

SELECT orioledb_tbl_structure('o_indices5'::regclass, 'nue');
SELECT orioledb_tbl_are_indices_equal('o_indices5_idx1'::regclass, 'o_indices5_idx2'::regclass);

-- Unique index build
CREATE TABLE o_indices6
(
	key int8 NOT NULL,
	val int,
	val2 int,
	PRIMARY KEY (key)
) USING orioledb;

INSERT INTO o_indices6 SELECT 1000 + i, 3000 + i % 100, 4000 + i FROM generate_series(1, 500) AS i;

CREATE UNIQUE INDEX o_indices6_idx1 ON o_indices6 (val); -- fail
CREATE UNIQUE INDEX o_indices6_idx2 ON o_indices6 (val2); -- success

INSERT INTO o_indices6 VALUES (1501, 3000, 4001); -- fail

SELECT orioledb_tbl_indices('o_indices6'::regclass);

EXPLAIN (COSTS off) SELECT val FROM o_indices6 WHERE val > 0;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices6 WHERE val2 > 0 ORDER BY val2;

SELECT COUNT(val2) FROM o_indices6 WHERE val2 > 0;
SELECT val2 FROM o_indices6 WHERE val2 > 0 ORDER BY val2;

SELECT orioledb_tbl_structure('o_indices6'::regclass, 'nue');

-- Unique index build on table without primary index
CREATE TABLE o_indices6_ctid
(
	key int8 NOT NULL,
	val int,
	val2 int
) USING orioledb;

INSERT INTO o_indices6_ctid SELECT 1000 + i, 3000 + i % 100, 4000 + i FROM generate_series(1, 500) AS i;

CREATE UNIQUE INDEX o_indices6_ctid_idx1 ON o_indices6_ctid (val); -- fail
CREATE UNIQUE INDEX o_indices6_ctid_idx2 ON o_indices6_ctid (val2); -- success

SELECT orioledb_tbl_indices('o_indices6_ctid'::regclass);

EXPLAIN (COSTS off) SELECT val FROM o_indices6_ctid WHERE val > 0;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices6_ctid WHERE val2 > 0;

SELECT COUNT(val2) FROM o_indices6_ctid WHERE val2 > 0;

SELECT orioledb_tbl_structure('o_indices6_ctid'::regclass, 'nue');

-- index drop test
CREATE TABLE o_indices7
(
	key int NOT NULL,
	val int,
	val2 int,
	PRIMARY KEY (key)
) USING orioledb;

INSERT INTO o_indices7 SELECT 1000 + i, 3000 + i, 4000 + i
FROM generate_series(1, 500) AS i;

SELECT orioledb_tbl_indices('o_indices7'::regclass);

BEGIN;
CREATE INDEX o_indices7_idx1 ON o_indices7 (val);
ROLLBACK;

SELECT orioledb_tbl_indices('o_indices7'::regclass);

EXPLAIN (COSTS off) SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0 ORDER BY val;
SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0 ORDER BY val;

BEGIN;
CREATE INDEX o_indices7_idx1 ON o_indices7 (val);
COMMIT;

SELECT orioledb_tbl_indices('o_indices7'::regclass);

EXPLAIN (COSTS off) SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0 ORDER BY val;
SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0 ORDER BY val;

BEGIN;
DROP INDEX o_indices7_idx1;
ROLLBACK;

SELECT orioledb_tbl_indices('o_indices7'::regclass);

EXPLAIN (COSTS off) SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0 ORDER BY val;
SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0 ORDER BY val;

BEGIN;
DROP INDEX o_indices7_idx1;
COMMIT;

SELECT orioledb_tbl_indices('o_indices7'::regclass);

EXPLAIN (COSTS off) SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0 ORDER BY val;
SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0 ORDER BY val;

SELECT orioledb_tbl_indices('o_indices7'::regclass);
CREATE INDEX o_indices7_idx1 ON o_indices7 (val2);
CREATE INDEX o_indices7_idx2 ON o_indices7 (val2);
CREATE INDEX o_indices7_idx3 ON o_indices7 (val2);
CREATE INDEX o_indices7_idx4 ON o_indices7 (val2);
SELECT orioledb_tbl_indices('o_indices7'::regclass);
DROP INDEX o_indices7_idx2;
DROP INDEX o_indices7_idx3;
DROP INDEX o_indices7_idx4;
SELECT orioledb_tbl_indices('o_indices7'::regclass);
CREATE INDEX o_indices7_idx5 ON o_indices7 (val);
SELECT orioledb_tbl_indices('o_indices7'::regclass);

EXPLAIN (COSTS off) SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0
	ORDER BY val;
SELECT val FROM o_indices7 WHERE val = 3400 AND val > 0
	ORDER BY val;
ANALYZE o_indices7;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices7 WHERE val2 = 4400 AND val2 > 0
	ORDER BY val2;
SELECT val2 FROM o_indices7 WHERE val2 = 4400 AND val2 > 0
	ORDER BY val2;

DROP INDEX o_indices7_idx5;
SELECT orioledb_tbl_indices('o_indices7'::regclass);

ALTER TABLE o_indices7 DROP CONSTRAINT o_indices7_pkey;
SELECT orioledb_tbl_indices('o_indices7'::regclass);

EXPLAIN (COSTS off) SELECT * FROM o_indices7 WHERE val = 3400 AND val > 0
	ORDER BY val;
SELECT * FROM o_indices7 WHERE val = 3400 AND val > 0
	ORDER BY val;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices7 WHERE val2 = 4400 AND val2 > 0
	ORDER BY val2;
SELECT val2 FROM o_indices7 WHERE val2 = 4400 AND val2 > 0
	ORDER BY val2;

ALTER TABLE o_indices7
  ADD CONSTRAINT o_indices7_pkey
    PRIMARY KEY (key);

EXPLAIN (COSTS off) SELECT * FROM o_indices7 WHERE val = 3400 AND val > 0
	ORDER BY val;
SELECT * FROM o_indices7 WHERE val = 3400 AND val > 0
	ORDER BY val;
EXPLAIN (COSTS off) SELECT val2 FROM o_indices7 WHERE val2 = 4400 AND val2 > 0
	ORDER BY val2;
SELECT val2 FROM o_indices7 WHERE val2 = 4400 AND val2 > 0
	ORDER BY val2;

-----
--- Test TOAST
-----

-- generate pseudo-random string function in deterministic way
CREATE FUNCTION generate_string(seed integer, length integer) RETURNS text
	AS $$
		SELECT substr(string_agg(
						substr(encode(sha256(seed::text::bytea || '_' || i::text::bytea), 'hex'), 1, 21),
				''), 1, length)
		FROM generate_series(1, (length + 20) / 21) i; $$
LANGUAGE SQL;

CREATE TABLE o_indices8
(
	key integer NOT NULL,
	val text COLLATE "C" NOT NULL,
	val2 text COLLATE "C" NOT NULL,
	PRIMARY KEY (key)
) USING orioledb;
INSERT INTO o_indices8
	VALUES (1, generate_string(5, 10), generate_string(1, 20));
INSERT INTO o_indices8
	VALUES (2, generate_string(3, 10), generate_string(2, 4000));
INSERT INTO o_indices8
	SELECT i, generate_string(i, 10), generate_string(i, 3000)
	FROM generate_series(3, 5) i;
CREATE INDEX o_indices8_idx1 ON o_indices8 (val);
CREATE INDEX o_indices8_idx2 ON o_indices8 (val2);
SELECT * FROM o_indices8;
SELECT val FROM o_indices8 WHERE val > 'a' ORDER BY val;

SELECT orioledb_tbl_structure('o_indices8'::regclass, 'nue');

EXPLAIN SELECT val, key FROM o_indices8 WHERE val > 'a' ORDER BY val;
SELECT val, key FROM o_indices8 WHERE val > 'a' ORDER BY val;
ALTER TABLE o_indices8 DROP CONSTRAINT o_indices8_pkey;
EXPLAIN SELECT val, key FROM o_indices8 WHERE val > 'a' ORDER BY val;
SELECT val, key FROM o_indices8 WHERE val > 'a' ORDER BY val;

SELECT key, val FROM o_indices8;
SELECT val FROM o_indices8 WHERE val > 'a' ORDER BY val;
SELECT orioledb_tbl_structure('o_indices8'::regclass, 'nue');
SELECT * FROM o_indices8;
SELECT val FROM o_indices8 WHERE val > 'a' ORDER BY val;
SELECT orioledb_tbl_structure('o_indices8'::regclass, 'nue');

ALTER TABLE o_indices8
  ADD CONSTRAINT o_indices8_pkey
    PRIMARY KEY (key);
EXPLAIN SELECT val, key FROM o_indices8 WHERE val > 'a' ORDER BY val;
SELECT val, key FROM o_indices8 WHERE val > 'a' ORDER BY val;
SELECT key, val FROM o_indices8;
SELECT * FROM o_indices8;
SELECT val FROM o_indices8 WHERE val > 'a' ORDER BY val;
SELECT orioledb_tbl_structure('o_indices8'::regclass, 'nue');

ALTER TABLE o_indices8 DROP CONSTRAINT o_indices8_pkey;
EXPLAIN SELECT val, key FROM o_indices8 WHERE val > 'a' ORDER BY val;
SELECT val, key FROM o_indices8 WHERE val > 'a' ORDER BY val;

CREATE TABLE o_indices8_ctid
(
	key integer NOT NULL,
	val text COLLATE "C" NOT NULL,
	val2 text COLLATE "C" NOT NULL
) USING orioledb;
INSERT INTO o_indices8_ctid
	VALUES (1, generate_string(5, 10), generate_string(1, 20));
INSERT INTO o_indices8_ctid
	VALUES (2, generate_string(3, 10), generate_string(2, 4000));
INSERT INTO o_indices8_ctid
	SELECT i, generate_string(i, 10), generate_string(i, 3000)
	FROM generate_series(3, 5) i;
CREATE INDEX o_indices8_ctid_idx1 ON o_indices8_ctid (val);
CREATE INDEX o_indices8_ctid_idx2 ON o_indices8_ctid (val2);
SELECT * FROM o_indices8_ctid;
SELECT val FROM o_indices8_ctid WHERE val > 'a' ORDER BY val;

SELECT orioledb_tbl_structure('o_indices8_ctid'::regclass, 'nue');

ALTER TABLE o_indices8_ctid
  ADD CONSTRAINT o_indices8_ctid_pkey
    PRIMARY KEY (key);
SELECT key, val FROM o_indices8_ctid;
SELECT * FROM o_indices8_ctid;
SELECT val FROM o_indices8_ctid WHERE val > 'a' ORDER BY val;
SELECT orioledb_tbl_structure('o_indices8_ctid'::regclass, 'nue');

ALTER TABLE o_indices8_ctid DROP CONSTRAINT o_indices8_ctid_pkey;

SELECT key, val FROM o_indices8_ctid;
SELECT val FROM o_indices8_ctid WHERE val > 'a' ORDER BY val;
SELECT orioledb_tbl_structure('o_indices8_ctid'::regclass, 'nue');
SELECT * FROM o_indices8_ctid;
SELECT val FROM o_indices8_ctid WHERE val > 'a' ORDER BY val;
SELECT orioledb_tbl_structure('o_indices8_ctid'::regclass, 'nue');

-- Check external sorting
CREATE TABLE o_indices9
(
	key integer PRIMARY KEY,
	val text COLLATE "C" NOT NULL
) USING orioledb;

INSERT INTO o_indices9
SELECT i, generate_string(i, 1000)
FROM generate_series(1, 200) i;
SET work_mem = '64kB';
CREATE INDEX o_indices9_val_idx ON o_indices9(val);
WITH x(val) AS (SELECT val FROM o_indices9 ORDER BY val)
SELECT sum(length(val)) FROM x;

-- Test drop first index without primary
CREATE TABLE o_indices10
(
	key int NOT NULL,
	val int,
	val2 int
) USING orioledb;
INSERT INTO o_indices10
	SELECT 1000 + i, 3000 + i,
			3000 + i FROM generate_series(1, 500) AS i;
CREATE UNIQUE INDEX o_indices10_idx1 ON o_indices10 (val2);
SELECT orioledb_tbl_indices('o_indices10'::regclass);
DROP INDEX o_indices10_idx1;
SELECT orioledb_tbl_indices('o_indices10'::regclass);

RESET enable_seqscan;
RESET enable_bitmapscan;

-- Test table rewrite after adding new column with default
-- with volatile function
CREATE TABLE o_test_default_volatile (
	a int,
	d int
) USING orioledb;

INSERT INTO o_test_default_volatile (a, d) VALUES (1, 100), (2, 100), (3, 100);
SELECT * FROM o_test_default_volatile;

CREATE SEQUENCE o_test_default_volatile_seq;
ALTER TABLE o_test_default_volatile
	ADD COLUMN b int DEFAULT nextval('o_test_default_volatile_seq');
SELECT * FROM o_test_default_volatile;

\d+ o_test_default_volatile
SELECT orioledb_table_description('o_test_default_volatile'::regclass);

SELECT attname, atthasdef, atthasmissing FROM pg_attribute
	WHERE attrelid = 'o_test_default_volatile'::regclass;

INSERT INTO o_test_default_volatile (a, d)
	VALUES (10, 200), (20, 200), (30, 200);
SELECT * FROM o_test_default_volatile ORDER BY a DESC;
SELECT * FROM o_test_default_volatile;
SELECT * FROM o_test_default_volatile ORDER BY a DESC;

-- Same as test above but checking that serial does the same
CREATE TABLE o_test_add_serial (
	a int
) USING orioledb;

INSERT INTO o_test_add_serial VALUES (1), (2), (3);

ALTER TABLE o_test_add_serial ADD COLUMN b serial;

\d+ o_test_add_serial

INSERT INTO o_test_add_serial VALUES (10), (20), (30);
SELECT * FROM o_test_add_serial ORDER BY a DESC;
SELECT * FROM o_test_add_serial;
SELECT * FROM o_test_add_serial ORDER BY a DESC;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA indices_build CASCADE;
RESET search_path;
