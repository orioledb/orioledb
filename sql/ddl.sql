CREATE EXTENSION orioledb;

CREATE TABLE o_ddl_check
(
	f1 text,
	f2 varchar,
	f3 integer,
	PRIMARY KEY(f1)
) USING orioledb;

SELECT * FROM o_ddl_check;
INSERT INTO o_ddl_check VALUES ('1', NULL, NULL);
-- Fails because of NULL values
ALTER TABLE o_ddl_check ALTER f2 SET NOT NULL;
TRUNCATE o_ddl_check;
INSERT INTO o_ddl_check VALUES ('1', '2', NULL);
-- OK
ALTER TABLE o_ddl_check ALTER f2 SET NOT NULL;

DROP TABLE o_ddl_check;
CREATE TABLE o_ddl_check
(
	f1 text NOT NULL COLLATE "C",
	f2 varchar NOT NULL,
	f3 integer,
	PRIMARY KEY (f1)
) USING orioledb;
INSERT INTO o_ddl_check VALUES ('1', '2', NULL);
-- Fails, because of NOT NULL constraint
INSERT INTO o_ddl_check VALUES ('2', NULL, '3');
-- Fails, because of unique constraint
INSERT INTO o_ddl_check VALUES ('1', '2', '3');

SELECT * FROM o_ddl_check;
SELECT orioledb_table_description('o_ddl_check'::regclass);

-- Fails because can't drop NOT NULL contraint on PK
ALTER TABLE o_ddl_check ALTER f1 DROP NOT NULL;
SELECT orioledb_table_description('o_ddl_check'::regclass);

SELECT * FROM o_ddl_check;
-- Fails on unknown option
ALTER TABLE o_ddl_check OPTIONS (SET hello 'world');
ALTER TABLE o_ddl_check ALTER f2 TYPE text;
SELECT orioledb_table_description('o_ddl_check'::regclass);

-- OK, because 'f2' isn't indexed
ALTER TABLE o_ddl_check ALTER f2 TYPE varchar COLLATE "POSIX";
SELECT orioledb_table_description('o_ddl_check'::regclass);

-- Fails, because 'f2' is indexed with different collation
ALTER TABLE o_ddl_check ALTER f1 TYPE text;
ALTER TABLE o_ddl_check ALTER f1 TYPE varchar COLLATE "POSIX";
-- OK, because binary compatible and collations match
ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "C";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- Fails, because types aren't binary compatible
ALTER TABLE o_ddl_check ALTER f2 TYPE timestamp;

ALTER TABLE o_ddl_check ALTER f2 DROP NOT NULL;
ALTER TABLE o_ddl_check ALTER f2 SET NOT NULL;
SELECT orioledb_table_description('o_ddl_check'::regclass);

ALTER TABLE o_ddl_check DROP f2;
ALTER TABLE o_ddl_check DROP f1;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

DROP TABLE o_ddl_check;
CREATE TABLE o_ddl_check
(
	f1 varchar COLLATE "C",
	f2 text NOT NULL,
	PRIMARY KEY(f1)
) USING orioledb;

INSERT INTO o_ddl_check VALUES ('a', NULL);
INSERT INTO o_ddl_check VALUES (NULL, 'b');
INSERT INTO o_ddl_check VALUES ('a', 'b');
UPDATE o_ddl_check SET f1 = NULL WHERE f1 = 'a';
SELECT * FROM o_ddl_check;
ALTER TABLE o_ddl_check ADD CHECK (f2 < 'f');
INSERT INTO o_ddl_check VALUES ('b', 'ddd');
INSERT INTO o_ddl_check VALUES ('c', 'ffff');

CREATE UNIQUE INDEX o_ddl_check_f2_idx ON o_ddl_check(f2);
ALTER TABLE o_ddl_check ALTER f2 DROP NOT NULL;
ALTER TABLE o_ddl_check ALTER f2 SET NOT NULL;

-- Check partition consraint.
CREATE TABLE o_ddl_parted
(
	f1 varchar COLLATE "C",
	f2 text NOT NULL
) PARTITION BY RANGE (f1);
ALTER TABLE o_ddl_parted ATTACH PARTITION o_ddl_check FOR VALUES FROM ('a') TO ('d');
SELECT * FROM o_ddl_parted;

INSERT INTO o_ddl_parted VALUES ('abc', 'def');
-- OK
UPDATE o_ddl_parted SET f1 = 'bcd' WHERE f1 = 'abc';
-- Partition constraint failure
UPDATE o_ddl_parted SET f1 = 'efg' WHERE f1 = 'bcd';
SELECT * FROM o_ddl_parted;

CREATE TABLE o_ddl_check_2
(
	f1 varchar COLLATE "C",
	f2 text NOT NULL,
	PRIMARY KEY(f1)
) USING orioledb;

ALTER TABLE o_ddl_parted ATTACH PARTITION o_ddl_check_2 FOR VALUES FROM ('e') TO ('h');
-- Move row between partitions
UPDATE o_ddl_parted SET f1 = 'efg' WHERE f1 = 'bcd';
SELECT * FROM o_ddl_parted;
ALTER TABLE o_ddl_parted DETACH PARTITION o_ddl_check;
ALTER TABLE o_ddl_parted DETACH PARTITION o_ddl_check_2;
DROP TABLE o_ddl_parted;

DROP TABLE o_ddl_check;
DROP TABLE o_ddl_check_2;

CREATE TABLE o_ddl_check
(
	f1 int NOT NULL,
	f2 int,
	f3 int,
	f4 int,
	PRIMARY KEY(f1)
) USING orioledb;
CREATE UNIQUE INDEX o_ddl_check_unique ON o_test24 (f2, f3, f4);
CREATE INDEX o_ddl_check_regular ON o_test24 (f2, f3, f4);

INSERT INTO o_ddl_check VALUES (1, 2, NULL, 5);
INSERT INTO o_ddl_check VALUES (2, 2, NULL, 3);
INSERT INTO o_ddl_check VALUES (3, 2, NULL, 2);
INSERT INTO o_ddl_check VALUES (4, 1, NULL, 4);
INSERT INTO o_ddl_check VALUES (5, 2, NULL, 3);
INSERT INTO o_ddl_check VALUES (6, 2, NULL, NULL);
INSERT INTO o_ddl_check VALUES (7, 2, NULL, NULL);

SELECT * FROM o_ddl_check;
SELECT orioledb_tbl_structure('o_ddl_check'::regclass, 'nuebc');

DROP TABLE o_ddl_check;

CREATE TABLE o_ddl_missing (
	i int4 NOT NULL
) USING orioledb;
INSERT INTO o_ddl_missing SELECT * FROM generate_series(1, 10);
ALTER TABLE o_ddl_missing ADD COLUMN l int4;
SELECT * FROM o_ddl_missing;
ALTER TABLE o_ddl_missing ADD COLUMN m int4 DEFAULT 2;
SELECT * FROM o_ddl_missing;
ALTER TABLE o_ddl_missing ADD COLUMN n int4, ADD COLUMN o int4[];
SELECT * FROM o_ddl_missing;
UPDATE o_ddl_missing SET l = 5, n = 6, o = '{1, 5, 2}' WHERE i BETWEEN 3 AND 7;
SELECT * FROM o_ddl_missing;
ALTER TABLE o_ddl_missing 
	DROP COLUMN m, 
	ADD COLUMN p int4[] DEFAULT '{2, 4, 8}', 
	ADD COLUMN r int4[];
SELECT * FROM o_ddl_missing;

CREATE TABLE o_test_add_column
(
	id serial primary key,
	i int4
) USING orioledb;
\d o_test_add_column
SELECT orioledb_tbl_indices('o_test_add_column'::regclass);
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');

INSERT INTO o_test_add_column (i)
	SELECT pseudo_random(1, v) * 20000 FROM generate_series(1,10) v;

-- test new null column
ALTER TABLE o_test_add_column ADD COLUMN y int4;
\d o_test_add_column
SELECT orioledb_tbl_indices('o_test_add_column'::regclass);
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');

-- test new column with volatile default
ALTER TABLE o_test_add_column ADD COLUMN z int4 default 5;
\d o_test_add_column
SELECT orioledb_tbl_indices('o_test_add_column'::regclass);
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');

CREATE SEQUENCE o_test_j_seq;

-- test new column with non-volatile default
ALTER TABLE o_test_add_column
	ADD COLUMN j int4 not null default pseudo_random(2, nextval('o_test_j_seq')) * 20000;
\d o_test_add_column
SELECT orioledb_tbl_indices('o_test_add_column'::regclass);
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');

INSERT INTO o_test_add_column (i)
	SELECT pseudo_random(3, v) * 20000 FROM generate_series(1,5) v;
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');
EXPLAIN (COSTS OFF) SELECT * FROM o_test_add_column;
SELECT * FROM o_test_add_column;
-- Test that default fields not recalculated
SELECT * FROM o_test_add_column;

CREATE TABLE o_test_multiple_analyzes (
    aid integer NOT NULL PRIMARY KEY
) USING orioledb;


-- Wrapper function, which converts result of SQL query to the text
CREATE OR REPLACE FUNCTION query_to_text(sql TEXT) RETURNS SETOF TEXT AS $$
	BEGIN
		RETURN QUERY EXECUTE sql;
	END $$
LANGUAGE plpgsql;

INSERT INTO o_test_multiple_analyzes
	SELECT aid FROM generate_series(1, 10) aid;
BEGIN;
select count(1) from o_test_multiple_analyzes;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('explain (analyze, buffers) 
	select * from o_test_multiple_analyzes ORDER BY aid DESC LIMIT 10;') as t;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('explain (analyze, buffers)
	select count(1) from o_test_multiple_analyzes;') as t;
ROLLBACK;

CREATE FOREIGN DATA WRAPPER dummy;
CREATE SERVER s0 FOREIGN DATA WRAPPER dummy;
CREATE FOREIGN TABLE ft1 (
	c1 integer OPTIONS ("param 1" 'val1') NOT NULL,
	c2 text OPTIONS (param2 'val2', param3 'val3') CHECK (c2 <> ''),
	c3 date,
	CHECK (c3 BETWEEN '1994-01-01'::date AND '1994-01-31'::date)
) SERVER s0 OPTIONS (delimiter ',', quote '"', "be quoted" 'value');

DROP FOREIGN DATA WRAPPER dummy CASCADE;

CREATE TABLE o_unexisting_column
(
	key int4,
	PRIMARY KEY(key)
) USING orioledb;

ALTER TABLE o_unexisting_column ALTER COLUMN key_2 SET DEFAULT 5;
ALTER TABLE o_unexisting_column ALTER COLUMN key_2 DROP DEFAULT;
ALTER TABLE o_unexisting_column RENAME COLUMN key_2 TO key_3;
ALTER TABLE o_unexisting_column DROP COLUMN key_2;
ALTER TABLE o_unexisting_column ALTER COLUMN key_2 SET NOT NULL;
ALTER TABLE o_unexisting_column ALTER COLUMN key_2 DROP NOT NULL;
ALTER TABLE o_unexisting_column ALTER key_2 TYPE int;
ALTER TABLE o_unexisting_column ALTER key_2 TYPE int USING key_2::integer;
ALTER TABLE o_unexisting_column ALTER COLUMN key_2
	ADD GENERATED ALWAYS AS IDENTITY;
ALTER TABLE o_unexisting_column ALTER COLUMN key
	ADD GENERATED ALWAYS AS IDENTITY;

UPDATE o_unexisting_column SET key_2 = 4 WHERE key = 2;

DROP EXTENSION orioledb CASCADE;
