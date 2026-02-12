-----
-- do not init TOAST table if table does not consists non-index keys varlens
-----
CREATE SCHEMA toast;
SET SESSION search_path = 'toast', 'public';
CREATE EXTENSION IF NOT EXISTS orioledb;
CREATE EXTENSION IF NOT EXISTS hstore;
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

-- generate pseudo-random string function in deterministic way
CREATE FUNCTION generate_string(seed integer, length integer) RETURNS text
	AS $$
		SELECT substr(string_agg(
						substr(encode(sha256(seed::text::bytea || '_' || i::text::bytea), 'hex'), 1, 21),
				''), 1, length)
		FROM generate_series(1, (length + 20) / 21) i; $$
LANGUAGE SQL;

CREATE TABLE o_test1 (
       id integer NOT NULL,
       t text NOT NULL,
       PRIMARY KEY(id)
) USING orioledb;
CREATE INDEX o_test1_reg ON o_test1(t);
INSERT INTO o_test1 VALUES (1, repeat('x', 2600));
INSERT INTO o_test1 VALUES (2, generate_string(1, 3000));
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
SELECT orioledb_rewind_sync();
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
SELECT orioledb_rewind_sync();
SELECT id, length(val) FROM o_test1 WHERE id = 7;
UPDATE o_test1 SET id = 6, val = generate_string(14, 5000) WHERE id = 7;
SELECT orioledb_rewind_sync();
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
SELECT length(t), count(*) FROM o_test1 GROUP BY length(t) ORDER BY 1, 2;
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
SELECT length(t), count(*) FROM o_test1 GROUP BY length(t) ORDER BY 1, 2;

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
-- Test copying of TOAST values
----

-- Copy from orioledb table
CREATE TABLE o_test1
(
	id integer PRIMARY KEY,
	val text
) USING orioledb;
INSERT INTO o_test1 VALUES (1, generate_string(1, 3000));

CREATE TABLE o_test2
(
	id integer PRIMARY KEY,
	val text
) USING orioledb;
INSERT INTO o_test2 (SELECT * FROM o_test1);

SELECT id, length(val), substr(val, 1, 20) FROM o_test2;
SELECT orioledb_tbl_structure('o_test2'::regclass, 'nue');

DROP TABLE o_test1;
DROP TABLE o_test2;

-- Copy from heap table
CREATE TABLE h_test1
(
	id integer PRIMARY KEY,
	val text
) USING heap;
INSERT INTO h_test1 VALUES (1, generate_string(1, 4000));

CREATE TABLE o_test2
(
	id integer PRIMARY KEY,
	val text
) USING orioledb;
INSERT INTO o_test2 (SELECT * FROM h_test1);

SELECT id, length(val), substr(val, 1, 20) FROM o_test2;
SELECT orioledb_tbl_structure('o_test2'::regclass, 'nue');

DROP TABLE h_test1;
DROP TABLE o_test2;

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

CREATE TABLE IF NOT EXISTS o_test_toast_update_delete (
	id integer PRIMARY KEY,
	v1 text,
	v2 text,
	v3 text
) USING orioledb;
CREATE INDEX o_test_toast_update_delete_idx1 ON o_test_toast_update_delete(v1);

INSERT INTO o_test_toast_update_delete VALUES (10, generate_string(1, 2500), generate_string(2, 2500), generate_string(3, 2500));
SELECT orioledb_tbl_structure('o_test_toast_update_delete'::regclass, 'ne');
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_toast_update_delete;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_toast_update_delete ORDER BY v1;
SELECT * FROM o_test_toast_update_delete ORDER BY v1;
COMMIT;

UPDATE o_test_toast_update_delete SET v1 = generate_string(4, 2500), v2 = generate_string(5, 2500), v3 = generate_string(6, 2500) WHERE id = 10;
SELECT orioledb_tbl_structure('o_test_toast_update_delete'::regclass, 'ne');
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_toast_update_delete;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_toast_update_delete ORDER BY v1;
SELECT * FROM o_test_toast_update_delete ORDER BY v1;
COMMIT;

DELETE FROM o_test_toast_update_delete WHERE id = 10;
SELECT orioledb_tbl_structure('o_test_toast_update_delete'::regclass, 'ne');
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_toast_update_delete;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_toast_update_delete ORDER BY v1;
SELECT * FROM o_test_toast_update_delete ORDER BY v1;
COMMIT;

CREATE TABLE o_test_ctid_toast_truncate (
       id integer NOT NULL,
       val text NOT NULL
) USING orioledb;

CREATE TEMPORARY VIEW o_test_ctid_toast_truncate_toast_oids AS
  SELECT index_reloid, index_relnode, index_type FROM orioledb_index
    WHERE table_reloid = 'o_test_ctid_toast_truncate'::regclass AND index_type = 'toast';
CREATE TEMP TABLE o_test_ctid_toast_truncate_old_toast_oids AS
	SELECT * FROM o_test_ctid_toast_truncate_toast_oids;

-- not existed means it uses new toast relnode, created in transaction
CREATE TEMPORARY VIEW o_test_ctid_toast_truncate_check_toast_oids AS
  SELECT nto.index_type, oto.index_relnode IS NOT NULL existed
    FROM o_test_ctid_toast_truncate_old_toast_oids oto
    RIGHT JOIN o_test_ctid_toast_truncate_toast_oids nto
		ON oto.index_relnode = nto.index_relnode;

INSERT INTO o_test_ctid_toast_truncate VALUES (1, (SELECT generate_string(1, 10000)));
begin;
SELECT * FROM o_test_ctid_toast_truncate_check_toast_oids;
truncate o_test_ctid_toast_truncate;
SELECT * FROM o_test_ctid_toast_truncate_check_toast_oids;
INSERT INTO o_test_ctid_toast_truncate VALUES (1, (SELECT generate_string(1, 10000)));
ROLLBACK;
SELECT * FROM o_test_ctid_toast_truncate_check_toast_oids;

begin;
SELECT * FROM o_test_ctid_toast_truncate_check_toast_oids;
truncate o_test_ctid_toast_truncate;
SELECT * FROM o_test_ctid_toast_truncate_check_toast_oids;
INSERT INTO o_test_ctid_toast_truncate VALUES (1, (SELECT generate_string(1, 10000)));
COMMIT;
SELECT * FROM o_test_ctid_toast_truncate_check_toast_oids;

-----
-- STORAGE MAIN regression tests
-- oversized MAIN handling, index bridging, hstore
-----

-- use existing schema and helper function from the first part
-- keep this part single-process to avoid flaky parallel-worker crashes
SET max_parallel_workers_per_gather = 0;

-----
-- basic STORAGE MAIN check
-----
CREATE TABLE o_sm_text (
	id integer PRIMARY KEY,
	v  text STORAGE MAIN COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_text VALUES (1, generate_string(1, 120));
INSERT INTO o_sm_text VALUES (2, generate_string(2, 3200));
INSERT INTO o_sm_text VALUES (3, generate_string(3, 7000));

SELECT id, length(v) FROM o_sm_text ORDER BY id;

UPDATE o_sm_text SET v = generate_string(10, 6000) WHERE id = 1;
UPDATE o_sm_text SET v = generate_string(20, 100) WHERE id = 2;
SELECT id, length(v) FROM o_sm_text ORDER BY id;

SELECT orioledb_tbl_structure('o_sm_text'::regclass, 'nuet');

-- delete and reinsert with STORAGE MAIN value
DELETE FROM o_sm_text WHERE id = 3;
INSERT INTO o_sm_text VALUES (3, generate_string(30, 7000));
SELECT id, length(v) FROM o_sm_text WHERE id = 3;

-----
-- mixed STORAGE MAIN, EXTENDED, PLAIN check
-----
CREATE TABLE o_sm_mix (
	id      integer PRIMARY KEY,
	m_main  text STORAGE MAIN COMPRESSION pglz,
	e_ext   text STORAGE EXTENDED COMPRESSION pglz,
	p_plain text STORAGE PLAIN
) USING orioledb;

INSERT INTO o_sm_mix
VALUES (1, generate_string(91, 3000), generate_string(92, 3000), generate_string(93, 500));

SELECT id, length(m_main), length(e_ext), length(p_plain) FROM o_sm_mix;
SELECT orioledb_tbl_structure('o_sm_mix'::regclass, 'nuet');

-----
-- bytea STORAGE MAIN check
-----
CREATE TABLE o_sm_bytea (
	id integer PRIMARY KEY,
	b  bytea STORAGE MAIN COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_bytea VALUES (1, decode(generate_string(95, 6000), 'hex'));
INSERT INTO o_sm_bytea VALUES (2, decode(generate_string(96, 8000), 'hex'));
SELECT id, length(b) FROM o_sm_bytea ORDER BY id;
SELECT orioledb_tbl_structure('o_sm_bytea'::regclass, 'nuet');

-----
-- MAIN + EXTENDED last-resort check
-----
CREATE TABLE o_sm_main_ext (
	id bigint PRIMARY KEY,
	m  text STORAGE MAIN COMPRESSION pglz,
	e  text STORAGE EXTENDED COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_main_ext VALUES (1, generate_string(101, 2000), generate_string(102, 9000));
INSERT INTO o_sm_main_ext VALUES (2, generate_string(103, 9000), generate_string(104, 9000));

SELECT id, length(m), length(e) FROM o_sm_main_ext ORDER BY id;
SELECT orioledb_tbl_structure('o_sm_main_ext'::regclass, 'nuet');

DO $$
DECLARE
	s text;
BEGIN
	s := orioledb_tbl_structure('o_sm_main_ext'::regclass, 'nuet');

	IF position('PK: (''1''), attnum 2' IN s) > 0 THEN
		RAISE EXCEPTION 'unexpected MAIN toast for o_sm_main_ext id=1';
	END IF;
	IF position('PK: (''1''), attnum 3' IN s) = 0 THEN
		RAISE EXCEPTION 'expected EXTENDED toast for o_sm_main_ext id=1';
	END IF;
	IF position('PK: (''2''), attnum 2' IN s) = 0 THEN
		RAISE EXCEPTION 'expected MAIN toast for o_sm_main_ext id=2';
	END IF;
	IF position('PK: (''2''), attnum 3' IN s) = 0 THEN
		RAISE EXCEPTION 'expected EXTENDED toast for o_sm_main_ext id=2';
	END IF;
END $$;

-----
-- two MAIN columns, force out-of-line for larger first
-----
CREATE TABLE o_sm_two_main (
	id      bigint PRIMARY KEY,
	m_big   text STORAGE MAIN COMPRESSION pglz,
	m_small text STORAGE MAIN COMPRESSION pglz,
	e       text STORAGE EXTENDED COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_two_main
VALUES (1, generate_string(201, 9000), generate_string(202, 900), generate_string(203, 9000));

SELECT id, length(m_big), length(m_small), length(e) FROM o_sm_two_main;
SELECT orioledb_tbl_structure('o_sm_two_main'::regclass, 'nuet');

DO $$
DECLARE
	s text;
BEGIN
	s := orioledb_tbl_structure('o_sm_two_main'::regclass, 'nuet');

	IF position('PK: (''1''), attnum 2' IN s) = 0 THEN
		RAISE EXCEPTION 'expected m_big to be toasted for o_sm_two_main id=1';
	END IF;
	IF position('PK: (''1''), attnum 3' IN s) > 0 THEN
		RAISE EXCEPTION 'unexpected m_small toast for o_sm_two_main id=1';
	END IF;
	IF position('PK: (''1''), attnum 4' IN s) = 0 THEN
		RAISE EXCEPTION 'expected EXTENDED column e to be toasted for o_sm_two_main id=1';
	END IF;
END $$;

-----
-- update path with toasted MAIN values
-----
CREATE TABLE o_sm_update (
	id bigint PRIMARY KEY,
	m  text STORAGE MAIN COMPRESSION pglz,
	e  text STORAGE EXTENDED COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_update VALUES (1, generate_string(11, 800), repeat('b', 2000));
UPDATE o_sm_update
	SET m = generate_string(12, 2500),
		e = generate_string(13, 5000)
	WHERE id = 1;
UPDATE o_sm_update
	SET m = generate_string(14, 600),
		e = repeat('c', 1200)
	WHERE id = 1;
UPDATE o_sm_update SET m = m WHERE id = 1;
SELECT id, length(m), length(e) FROM o_sm_update;

-----
-- rebuild path check (toast_sort_add)
-----
CREATE TABLE o_sm_rebuild (
	id bigint PRIMARY KEY,
	h  text STORAGE MAIN COMPRESSION pglz,
	e  text STORAGE EXTENDED COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_rebuild VALUES (1, 'h_small', repeat('e', 1200));
INSERT INTO o_sm_rebuild VALUES (2, generate_string(301, 7000), generate_string(302, 9000));
INSERT INTO o_sm_rebuild VALUES (3, generate_string(303, 4500), generate_string(304, 7000));

-- hash index enables index bridging and rebuilds existing rows
CREATE INDEX o_sm_rebuild_hash_idx ON o_sm_rebuild USING hash (h);

SELECT id FROM o_sm_rebuild WHERE h = 'h_small';
SELECT id FROM o_sm_rebuild WHERE h = (SELECT h FROM o_sm_rebuild WHERE id = 2);
REINDEX INDEX o_sm_rebuild_hash_idx;

-----
-- non-btree secondary index checks with STORAGE MAIN
-- include a non-btree secondary index case
-----
CREATE TABLE o_sm_bridge (
	id   bigint PRIMARY KEY,
	h    text STORAGE MAIN COMPRESSION pglz,
	bt   text STORAGE MAIN COMPRESSION pglz,
	tags text[] STORAGE MAIN COMPRESSION pglz,
	vec  tsvector STORAGE MAIN COMPRESSION pglz,
	br   text STORAGE MAIN COMPRESSION pglz
) USING orioledb;

CREATE INDEX o_sm_bridge_hash_idx ON o_sm_bridge USING hash (h);
CREATE INDEX o_sm_bridge_btree_idx ON o_sm_bridge (bt);
CREATE INDEX o_sm_bridge_gin_idx ON o_sm_bridge USING gin (tags);
CREATE INDEX o_sm_bridge_gist_idx ON o_sm_bridge USING gist (vec);
CREATE INDEX o_sm_bridge_brin_idx ON o_sm_bridge USING brin (br);

INSERT INTO o_sm_bridge VALUES (
	1,
	'h_small',
	'bt_small',
	ARRAY['a','b'],
	to_tsvector('simple', 'alpha beta'),
	'b_small'
);

INSERT INTO o_sm_bridge VALUES (
	2,
	repeat('h', 7000),
	'bt_mid',
	array_fill('x'::text, ARRAY[300]),
	to_tsvector('simple', repeat('tok ', 700)),
	repeat('b', 7000)
);

INSERT INTO o_sm_bridge VALUES (
	3,
	generate_string(23, 3500),
	'bt_rand',
	ARRAY(SELECT substr(md5(i::text), 1, 8) FROM generate_series(1, 250) AS i),
	to_tsvector('simple', (SELECT string_agg('term' || i, ' ') FROM generate_series(1, 300) AS i)),
	generate_string(24, 4000)
);

SELECT id FROM o_sm_bridge WHERE h = (SELECT h FROM o_sm_bridge WHERE id = 2);
SELECT id FROM o_sm_bridge WHERE bt = 'bt_mid';
SELECT id FROM o_sm_bridge WHERE tags @> ARRAY['x'];
SELECT id FROM o_sm_bridge WHERE vec @@ to_tsquery('simple', 'tok');
SELECT id FROM o_sm_bridge WHERE br LIKE 'b%';

UPDATE o_sm_bridge
	SET h = repeat('H', 6500),
		tags = array_fill('y'::text, ARRAY[200]),
		vec = to_tsvector('simple', repeat('upd ', 500)),
		br = repeat('c', 6500)
	WHERE id = 2;

SELECT id FROM o_sm_bridge WHERE h = repeat('H', 6500);
SELECT id FROM o_sm_bridge WHERE tags @> ARRAY['y'];
SELECT id FROM o_sm_bridge WHERE vec @@ to_tsquery('simple', 'upd');

REINDEX INDEX o_sm_bridge_hash_idx;
REINDEX INDEX o_sm_bridge_gin_idx;

-----
-- hstore STORAGE MAIN check
-----
CREATE TABLE o_sm_hstore (
	id   integer PRIMARY KEY,
	hs   hstore STORAGE MAIN COMPRESSION pglz,
	note text COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_hstore VALUES (1, 'k1=>v1,k2=>v2'::hstore, 'small');
INSERT INTO o_sm_hstore
SELECT
	2,
	hstore(array_agg('k' || i), array_agg(md5((1000 + i)::text))),
	'large'
FROM generate_series(1, 500) AS i;

SELECT id, length(hs::text), hs ? 'k1' AS has_k1 FROM o_sm_hstore ORDER BY id;

UPDATE o_sm_hstore
SET hs = (
	SELECT hstore(array_agg('u' || i), array_agg(md5((2000 + i)::text)))
	FROM generate_series(1, 450) AS i
)
WHERE id = 2;

SELECT id, length(hs::text), hs ? 'u1' AS has_u1 FROM o_sm_hstore WHERE id = 2;

CREATE TABLE o_sm_hstore_src (
	id integer PRIMARY KEY,
	hs hstore STORAGE EXTENDED COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_hstore_src
SELECT
	3,
	hstore(array_agg('s' || i), array_agg(md5((3000 + i)::text)))
FROM generate_series(1, 400) AS i;

INSERT INTO o_sm_hstore
SELECT id, hs, 'from_src'
FROM o_sm_hstore_src
ON CONFLICT (id) DO UPDATE SET hs = EXCLUDED.hs, note = EXCLUDED.note;

SELECT id, length(hs::text), note FROM o_sm_hstore ORDER BY id;

-----
-- EXTENDED to MAIN cross-table transfer check
-----
CREATE TABLE o_sm_src (
	id bigint PRIMARY KEY,
	v  text STORAGE EXTENDED COMPRESSION pglz
) USING orioledb;

CREATE TABLE o_sm_dst (
	id bigint PRIMARY KEY,
	v  text STORAGE MAIN COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_src VALUES (1, generate_string(401, 8000));
INSERT INTO o_sm_dst SELECT * FROM o_sm_src;

SELECT id, length(v) FROM o_sm_dst;
SELECT orioledb_tbl_structure('o_sm_dst'::regclass, 'nuet');

-----
-- MAIN + EXTERNAL mix check
-----
CREATE TABLE o_sm_external (
	id bigint PRIMARY KEY,
	m  text STORAGE MAIN COMPRESSION pglz,
	x  text STORAGE EXTERNAL
) USING orioledb;

INSERT INTO o_sm_external
VALUES (1, generate_string(411, 2400), generate_string(412, 6000));

SELECT id, length(m), length(x) FROM o_sm_external;
SELECT orioledb_tbl_structure('o_sm_external'::regclass, 'nuet');

-----
-- NULL and empty string handling check
-----
CREATE TABLE o_sm_nulls (
	id bigint PRIMARY KEY,
	m  text STORAGE MAIN COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_nulls VALUES (1, NULL);
INSERT INTO o_sm_nulls VALUES (2, '');
INSERT INTO o_sm_nulls VALUES (3, generate_string(421, 2500));

UPDATE o_sm_nulls SET m = generate_string(422, 2600) WHERE id = 1;
UPDATE o_sm_nulls SET m = NULL WHERE id = 3;

SELECT id, length(m), m = '' AS is_empty FROM o_sm_nulls ORDER BY id;

-----
-- upsert check with MAIN + EXTENDED text columns
-----
CREATE TABLE o_sm_upsert (
	id bigint PRIMARY KEY,
	m  text STORAGE MAIN COMPRESSION pglz,
	e  text STORAGE EXTENDED COMPRESSION pglz
) USING orioledb;

INSERT INTO o_sm_upsert VALUES (1, generate_string(431, 800), repeat('d', 2000));
INSERT INTO o_sm_upsert VALUES (1, generate_string(432, 2500), generate_string(433, 5000))
ON CONFLICT (id) DO UPDATE SET m = EXCLUDED.m, e = EXCLUDED.e;

SELECT id, length(m), length(e) FROM o_sm_upsert;
SELECT orioledb_tbl_structure('o_sm_upsert'::regclass, 'nuet');

-----
-- Cleanup
-----
DROP TABLE o_sm_text;
DROP TABLE o_sm_mix;
DROP TABLE o_sm_bytea;
DROP TABLE o_sm_main_ext;
DROP TABLE o_sm_two_main;
DROP TABLE o_sm_update;
DROP TABLE o_sm_rebuild;
DROP TABLE o_sm_bridge;
DROP TABLE o_sm_hstore;
DROP TABLE o_sm_hstore_src;
DROP TABLE o_sm_src;
DROP TABLE o_sm_dst;
DROP TABLE o_sm_external;
DROP TABLE o_sm_nulls;
DROP TABLE o_sm_upsert;
DROP FUNCTION generate_string(integer, integer);
RESET max_parallel_workers_per_gather;

SELECT orioledb_parallel_debug_stop();
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA toast CASCADE;
RESET search_path;
