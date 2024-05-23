-----
-- do not init TOAST table if table does not consists non-index keys varlens
-----
CREATE SCHEMA toast;
SET SESSION search_path = 'toast';
CREATE EXTENSION orioledb;
SELECT orioledb_parallel_debug_start();
CREATE TABLE o_test1 (
       id integer NOT NULL,
       t text NOT NULL,
       PRIMARY KEY(id)
) USING orioledb;
CREATE INDEX o_test1_reg on o_test1 (t);
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

DROP TABLE o_test1;
CREATE TABLE o_test1(
       id integer NOT NULL
) USING orioledb;
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

----
-- TOAST btree init check with manually indices
----
DROP TABLE o_test1;
CREATE TABLE o_test1
(
	id integer NOT NULL,
        t text NOT NULL,
	PRIMARY KEY(id)
) USING orioledb;
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

DROP TABLE o_test1;
CREATE TABLE IF NOT EXISTS o_test1
(
	id integer NOT NULL,
	t1 text NOT NULL,
	t2 text NOT NULL,
	t3 text NOT NULL,
	t4 text NOT NULL,
	t5 text NOT NULL,
	t6 text NOT NULL,
	t7 text NOT NULL,
	t8 text NOT NULL,
	t9 text NOT NULL,
	PRIMARY KEY (id)
) USING orioledb;
CREATE UNIQUE INDEX o_test1_uniq on o_test1(t1);
CREATE INDEX o_test1_reg1 on o_test1 (t2);
CREATE INDEX o_test1_reg2 on o_test1 (t3);
CREATE INDEX o_test1_reg3 on o_test1 (t5, t6);
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');
-- ok
INSERT INTO o_test1 VALUES
(
	1,
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 3000),
	repeat('x', 3000)
);
-- fails (t1 is index field)
INSERT INTO o_test1 VALUES
(
	1,
	repeat('x', 3000),
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 3000),
	repeat('x', 3000)
);
-- fails t2 is index field
INSERT INTO o_test1 VALUES
(
	1,
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 3000),
	repeat('x', 3000)
);
-- fails t3 is index field
INSERT INTO o_test1 VALUES
(
	1,
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 3000),
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 3000),
	repeat('x', 3000)
);
-- fails t5 is index field
INSERT INTO o_test1 VALUES
(
	1,
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 3000),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 3000),
	repeat('x', 3000)
);
-- fails t6 is index field
INSERT INTO o_test1 VALUES
(
	1,
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 10),
	repeat('x', 3000),
	repeat('x', 3000),
	repeat('x', 3000),
	repeat('x', 3000)
);
-----
--- Test compression inside primary index tuples. Do not TOASTed.
-----
DROP TABLE o_test1;
CREATE TABLE o_test1 (
       id integer NOT NULL,
       t text NOT NULL
) USING orioledb;

INSERT INTO o_test1 VALUES (1, repeat('x', 2665));
INSERT INTO o_test1 VALUES (2, repeat('x', 2672));
INSERT INTO o_test1 SELECT id, repeat('x', 2664) FROM generate_series(3, 1000) id;
SELECT t, count(*) FROM o_test1 GROUP BY t;

DROP TABLE o_test1;
CREATE TABLE o_test1 (
       id integer NOT NULL,
       t text NOT NULL,
       PRIMARY KEY(id)
) USING orioledb;
CREATE INDEX o_test1_reg ON o_test1(t);
INSERT INTO o_test1 VALUES (1, repeat('x', 2600));
INSERT INTO o_test1 VALUES (2, repeat('x', 3000));
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

DROP TABLE o_test1;
CREATE TABLE o_test1 (
       id integer NOT NULL,
       t text NOT NULL
) USING orioledb;
INSERT INTO o_test1 VALUES (1, repeat('x', 3000));
-- DELETE with TOAST values
INSERT INTO o_test1 VALUES (2, repeat('x', 3000));
DELETE FROM o_test1 WHERE id = 2;
INSERT INTO o_test1 VALUES (2, repeat('y', 3000));
SELECT * FROM o_test1;

-- test UPDATE without change primary index values
-- update not TOAST value with new TOAST
INSERT INTO o_test1 VALUES (3, repeat('x', 10));
SELECT * FROM o_test1 WHERE id = 3;
UPDATE o_test1 SET t = repeat('y', 3100) WHERE id = 3;
SELECT * FROM o_test1 WHERE id = 3;

-- update TOAST value with new non TOAST
INSERT INTO o_test1 VALUES (4, repeat('x', 3100));
SELECT * FROM o_test1 WHERE id = 4;
UPDATE o_test1 SET t = repeat('y', 10) WHERE id = 4;
SELECT * FROM o_test1 WHERE id = 4;
UPDATE o_test1 SET t = repeat('z', 4000) WHERE id = 4;
SELECT * FROM o_test1 WHERE id = 4;

-- update TOAST value to a new one
INSERT INTO o_test1 VALUES (5, repeat('x', 5000));
SELECT * FROM o_test1 WHERE id = 5;
UPDATE o_test1 SET t = repeat('y', 5000) WHERE id = 5;
SELECT * FROM o_test1 WHERE id = 5;
UPDATE o_test1 SET t = repeat('z', 6000) WHERE id = 5;
SELECT * FROM o_test1 WHERE id = 5;

-- update pk, delete all old toasted values and insert new
INSERT INTO o_test1 VALUES (6, repeat('x', 4000));
UPDATE o_test1 SET id = 7 WHERE id = 6;
SELECT * FROM o_test1 WHERE id = 7;
UPDATE o_test1 SET id = 6, t = repeat('y', 5000) WHERE id = 7;
SELECT * FROM o_test1 WHERE id > 5;
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

-- value after TOASTed value
DROP TABLE o_test1;
CREATE TABLE o_test1 (
       id integer NOT NULL,
       t text NOT NULL,
       val integer NOT NULL
) USING orioledb;
INSERT INTO o_test1 SELECT id, repeat('x', 3500), 101 FROM generate_series(3, 1000) id;
INSERT INTO o_test1 SELECT id, repeat('y', 3500), 302 FROM generate_series(1001, 2000) id;
INSERT INTO o_test1 VALUES (2001, 'asd', 444);
SELECT val, count(*) FROM o_test1 GROUP BY val;
SELECT * FROM o_test1 WHERE id = 4;
SELECT * FROM o_test1 WHERE id = 1002;
SELECT * FROM o_test1 WHERE id = 2001;

-- compare
DROP TABLE o_test1;
CREATE TABLE o_test1 (
       id integer NOT NULL,
       t text NOT NULL
) USING orioledb;
INSERT INTO o_test1 (SELECT id, id || repeat('x', 5000) FROM generate_series(1, 500, 1) id);
INSERT INTO o_test1 (SELECT 500 + id, id || repeat('y', 6000) FROM generate_series(1, 500, 1) id);
SELECT * FROM o_test1 WHERE t > 398 || repeat('x', 5000) and t < 400 || repeat('x', 5000);
SELECT * FROM o_test1 WHERE t = 500 || repeat('y', 6000);

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

-- TOAST tree should be freed after DROP TABLE.
CREATE TABLE o_toast_free (
       key integer NOT NULL,
       val text,
       PRIMARY KEY(key)
) USING orioledb;
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(1, 100, 1) id);
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(101, 200, 1) id);
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(201, 300, 1) id);
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(301, 400, 1) id);
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(401, 500, 1) id);
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(501, 600, 1) id);
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(601, 700, 1) id);
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(701, 800, 1) id);
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(801, 900, 1) id);
INSERT INTO o_toast_free (SELECT id, generate_string(1, 3000) FROM generate_series(901, 1000, 1) id);
DROP TABLE o_toast_free;

-- unique violetes messages test
DROP TABLE o_test1;
CREATE TABLE IF NOT EXISTS o_test1
(
	id integer NOT NULL,
	t1 text NOT NULL,
	t2 integer NOT NULL,
	t3 text NOT NULL,
	t4 text NOT NULL,
	t5 text NOT NULL,
	PRIMARY KEY(id)
) USING orioledb;
CREATE INDEX o_test1_reg on o_test1(t3);
CREATE UNIQUE INDEX o_test1_unique on o_test1(t4);

INSERT INTO o_test1 VALUES
(
	1,
	generate_string(1, 3000),
	32,
	repeat('x', 10),
	repeat('x', 10),
	generate_string(1, 3000)
);
INSERT INTO o_test1 VALUES
(
	1,
	generate_string(1, 3000),
	33,
	repeat('x', 10),
	repeat('x', 10),
	generate_string(1, 3000)
);
INSERT INTO o_test1 VALUES
(
	2,
	generate_string(1, 3000),
	34,
	repeat('x', 10),
	repeat('x', 10),
	generate_string(1, 3000)
);
INSERT INTO o_test1 VALUES
(
	2,
	generate_string(2, 3000),
	32,
	repeat('y', 10),
	repeat('y', 10),
	generate_string(2, 3000)
);
UPDATE o_test1 SET t4 = repeat('y', 10) WHERE id = 1;
UPDATE o_test1 SET id = 2 WHERE id = 1;

SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

DROP TABLE o_test1;
CREATE TABLE o_test1 (
       id integer NOT NULL,
       val text NOT NULL
)USING orioledb;

INSERT INTO o_test1 VALUES (1, (SELECT generate_string(1, 2000)));
INSERT INTO o_test1 VALUES (2, (SELECT generate_string(2, 10000)));
INSERT INTO o_test1 VALUES (3, (SELECT generate_string(3, 15000)));
SELECT val, count(*) FROM o_test1 GROUP BY val;
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

TRUNCATE o_test1;
-- INSERT
INSERT INTO o_test1 VALUES (1, generate_string(4, 10000));
SELECT id, length(val) FROM o_test1;

-- DELETE with TOAST values
INSERT INTO o_test1 VALUES (2, generate_string(5, 10000));
DELETE FROM o_test1 WHERE id = 2;
INSERT INTO o_test1 VALUES (2, generate_string(6, 15000));
SELECT id, length(val) FROM o_test1;

-- test UPDATE without change primary index values
-- update not TOAST value with new TOAST
INSERT INTO o_test1 VALUES (3, repeat('x', 10));
SELECT id, length(val) FROM o_test1 WHERE id = 3;
UPDATE o_test1 SET val = generate_string(7, 13000) WHERE id = 3;
SELECT id, length(val) FROM o_test1 WHERE id = 3;

-- update TOAST value with new non TOAST
INSERT INTO o_test1 VALUES (4, generate_string(8, 3100));
SELECT id, length(val) FROM o_test1 WHERE id = 4;
UPDATE o_test1 SET val = repeat('y', 10) WHERE id = 4;
SELECT id, length(val) FROM o_test1 WHERE id = 4;
UPDATE o_test1 SET val = generate_string(9, 4000) WHERE id = 4;
SELECT id, length(val) FROM o_test1 WHERE id = 4;

-- update TOAST value to a new one
INSERT INTO o_test1 VALUES (5, generate_string(10, 3000));
SELECT id, length(val) FROM o_test1 WHERE id = 5;
UPDATE o_test1 SET val = generate_string(11, 4000) WHERE id = 5;
SELECT id, length(val) FROM o_test1 WHERE id = 5;
UPDATE o_test1 SET val = generate_string(12, 6000) WHERE id = 5;
SELECT id, length(val) FROM o_test1 WHERE id = 5;

-- update pk, delete all old toasted values and insert new
INSERT INTO o_test1 VALUES (6, generate_string(13, 4000));
UPDATE o_test1 SET id = 7 WHERE id = 6;
SELECT id, length(val) FROM o_test1 WHERE id = 7;
UPDATE o_test1 SET id = 6, val = generate_string(14, 5000) WHERE id = 7;
SELECT id, length(val) FROM o_test1 WHERE id > 5;

-- and see results
SELECT id, length(val) FROM o_test1;
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

-- value after TOASTed value
DROP TABLE o_test1;
CREATE TABLE o_test1 (
       id integer NOT NULL,
       t text NOT NULL,
       val integer NOT NULL,
       PRIMARY KEY(id)
) USING orioledb;
INSERT INTO o_test1 SELECT id, generate_string(id, 3000), id + 100 FROM generate_series(3, 100) id;
INSERT INTO o_test1 SELECT id, generate_string(id, 4000), id + 100 FROM generate_series(101, 200) id;
INSERT INTO o_test1 VALUES (201, 'asd', 444);
SELECT length(t), count(*) FROM o_test1 GROUP BY length(t);
SELECT id, length(t), val FROM o_test1 WHERE id = 4;
SELECT id, length(t), val FROM o_test1 WHERE id = 102;
SELECT * FROM o_test1 WHERE id = 201;

-- compare
DROP TABLE o_test1;
CREATE TABLE o_test1 (
       id integer NOT NULL,
       t text NOT NULL,
       PRIMARY KEY(id)
) USING orioledb;
INSERT INTO o_test1 (SELECT id, id || generate_string(id, 3000) FROM generate_series(1, 200, 1) id);
SELECT id, length(t) FROM o_test1 WHERE t = 1 || generate_string(15, 3000);

-- page splits do not fails
TRUNCATE o_test1;
INSERT INTO o_test1 (SELECT id, id || generate_string(16, 3000) FROM generate_series(1, 200, 1) id);
DELETE FROM o_test1 WHERE id > 10 and id < 100;
INSERT INTO o_test1 (SELECT id, id || generate_string(17, 6000) FROM generate_series(11, 99, 1) id);
DELETE FROM o_test1 WHERE id > 120 and id < 200;
INSERT INTO o_test1 (SELECT id, id || generate_string(18, 4000) FROM generate_series(121, 199, 1) id);
DELETE FROM o_test1 WHERE id > 220 and id < 300;
INSERT INTO o_test1 (SELECT id, id || generate_string(19, 5000) FROM generate_series(221, 299, 1) id);
SELECT length(t), count(*) FROM o_test1 GROUP BY length(t);

DROP TABLE o_test1;
CREATE TABLE o_test1
(
	id integer NOT NULL,
	t1 text NOT NULL,
	t2 text NOT NULL,
	t3 text NOT NULL,
	t4 text NOT NULL,
	t5 text NOT NULL,
	PRIMARY KEY (id)
) USING orioledb;

----
-- it should not TOAST values except t5
----
INSERT INTO o_test1 VALUES(1, generate_string(1, 10),
							generate_string(1, 10), generate_string(1, 10),
							generate_string(1, 10), generate_string(1, 30000));
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');
DROP TABLE o_test1;

----
-- fetch TOAST value when secondary index used test
----
CREATE TABLE o_test1
(
	id integer NOT NULL,
	t1 text NOT NULL,
	t2 integer NOT NULL,
	t3 text NOT NULL,
	t4 text NOT NULL,
	t5 text NOT NULL,
	PRIMARY KEY(id)
) USING orioledb;
CREATE INDEX o_test1_reg ON o_test1(t2);

INSERT INTO o_test1 VALUES(1, generate_string(1, 10),
							2, generate_string(1, 10),
							generate_string(1, 10), generate_string(1, 30000));
EXPLAIN (COSTS off) SELECT * FROM o_test1 WHERE t2 = 2;
SELECT * FROM o_test1 WHERE t2 = 2;
DROP TABLE o_test1;

-- TEST substring
CREATE TABLE o_test_substr (
	key int,
	val text
) USING orioledb;

INSERT INTO o_test_substr VALUES (1, repeat('a', 4000) || repeat('b', 4000));
INSERT INTO o_test_substr VALUES (2, repeat('a', 150000) || repeat('b', 150000));
INSERT INTO o_test_substr VALUES (3, (SELECT array_agg(md5(g::text))::text
									FROM generate_series(1, 256) g));
SELECT * FROM o_test_substr;

SELECT key, length(val), pg_column_size(val), substring(val for 30),
	   substr(val, 10, 10) from o_test_substr WHERE key = 1;
SELECT key, length(val), pg_column_size(val), substring(val for 30),
	   substr(val, 20, 10) from o_test_substr WHERE key = 2;
SELECT key, length(val), pg_column_size(val), substring(val for 30),
	   substr(val, 30, 10) from o_test_substr WHERE key = 3;

BEGIN;
CREATE TABLE o_test_toast_rewrite (
	id integer NOT NULL,
	t text NOT NULL
) USING orioledb;
INSERT INTO o_test_toast_rewrite VALUES
	(1, generate_string(1, 3000)),
	(2, generate_string(2, 3000));
SELECT id, substr(t, 1, 20) FROM o_test_toast_rewrite;
SELECT orioledb_tbl_structure('o_test_toast_rewrite'::regclass, 'nue');
ALTER TABLE o_test_toast_rewrite ADD PRIMARY KEY (id);
SELECT id, substr(t, 1, 20) FROM o_test_toast_rewrite;
SELECT orioledb_tbl_structure('o_test_toast_rewrite'::regclass, 'nue');
ALTER TABLE o_test_toast_rewrite ALTER id TYPE bigint USING id * 10;
SELECT id, substr(t, 1, 20) FROM o_test_toast_rewrite;
SELECT orioledb_tbl_structure('o_test_toast_rewrite'::regclass, 'nue');
COMMIT;

----
-- TOAST logical decoding
----
DROP TABLE if exists o_logical;
SELECT pg_drop_replication_slot('regression_slot');

-- Wrapper function, which converts result of SQL query to the text
CREATE OR REPLACE FUNCTION query_to_text_filtered(sql TEXT, out result text)
        RETURNS SETOF TEXT AS $$
        BEGIN
                FOR result IN EXECUTE sql LOOP
			IF result NOT LIKE '%COMMIT%' AND result NOT LIKE '%BEGIN%' THEN
				RETURN NEXT;
			END IF;
                END LOOP;
        END $$
LANGUAGE plpgsql;

-- Uncompressed

CREATE TABLE o_logical(id integer PRIMARY KEY, v1 text, v2 text) using orioledb WITH (compress = -1, toast_compress = -1, primary_compress = -1);
SELECT slot_name FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);
---- Insert
INSERT INTO o_logical VALUES ('1', generate_string(10 + 1, 4000), generate_string(10 + 2, 5000));
INSERT INTO o_logical VALUES ('2', generate_string(20 + 1, 4000), generate_string(20 + 2, 5000));
--SELECT * FROM o_logical;
---- Update TOAST->TOAST
UPDATE o_logical SET (v1, v2) = (generate_string(50 + 1, 4000), generate_string(50 + 2, 5000)) WHERE id = 1;
UPDATE o_logical SET v2 = generate_string(60 + 2, 5000) WHERE id = 2;
--SELECT * FROM o_logical;
--- Update TOAST->Inline
UPDATE o_logical SET (v1, v2) = (generate_string(70 + 1, 4000), generate_string(70 + 2, 20)) WHERE id = 1;
--SELECT * FROM o_logical WHERE id = 1;
SELECT query_to_text_filtered($$ SELECT data from pg_logical_slot_get_changes('regression_slot', NULL, NULL); $$);


SELECT pg_drop_replication_slot('regression_slot');
DROP TABLE o_logical;

-- Compressed
CREATE TABLE o_logical(id integer PRIMARY KEY, v1 text, v2 text) using orioledb WITH (compress = -1, toast_compress = -1, primary_compress = -1);
SELECT slot_name FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);
---- Insert
INSERT INTO o_logical VALUES ('1', repeat('1', 4000) || generate_string(10 + 1, 4000), repeat('1', 4000) || generate_string(10 + 2, 5000));
INSERT INTO o_logical VALUES ('2', repeat('2', 4000) || generate_string(20 + 1, 4000), repeat('2', 4000) || generate_string(20 + 2, 5000));
--SELECT * FROM o_logical;
---- Update TOAST->TOAST
UPDATE o_logical SET (v1, v2) = (repeat('5', 4000) || generate_string(50 + 1, 4000), repeat('5', 4000) || generate_string(50 + 2, 5000)) WHERE id = 1;
UPDATE o_logical SET v2 = repeat('6', 4000) || generate_string(60 + 1, 4000) WHERE id = 2;
--SELECT * FROM o_logical;
--- Update TOAST->Inline
UPDATE o_logical SET (v1, v2) = (repeat('7', 4000) || generate_string(70 + 1, 4000), repeat('7', 20) || generate_string(70 + 2, 20)) WHERE id = 1;
--SELECT * FROM o_logical WHERE id = 1;
SELECT query_to_text_filtered($$ SELECT data from pg_logical_slot_get_changes('regression_slot', NULL, NULL); $$);

SELECT * FROM pg_drop_replication_slot('regression_slot');
DROP TABLE o_logical;

SELECT orioledb_parallel_debug_stop();
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA toast CASCADE;
RESET search_path;
