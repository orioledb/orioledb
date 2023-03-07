CREATE SCHEMA ddl;
SET SESSION search_path = 'ddl';
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
INSERT INTO o_ddl_check VALUES ('ABC1', 'ABC2', NULL);
-- Fails, because of NOT NULL constraint
INSERT INTO o_ddl_check VALUES ('2', NULL, '3');
-- Fails, because of unique constraint
INSERT INTO o_ddl_check VALUES ('ABC1', '2', '3');

INSERT INTO o_ddl_check VALUES ('ABC2', 'ABC4', NULL);
INSERT INTO o_ddl_check VALUES ('ABC3', 'ABC6', NULL);

SELECT * FROM o_ddl_check;
SELECT orioledb_table_description('o_ddl_check'::regclass);

-- Fails because can't drop NOT NULL contraint on PK
ALTER TABLE o_ddl_check ALTER f1 DROP NOT NULL;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- Fails on unknown option
ALTER TABLE o_ddl_check OPTIONS (SET hello 'world');
ALTER TABLE o_ddl_check ALTER f2 TYPE text;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- OK, because 'f2' isn't indexed
ALTER TABLE o_ddl_check ALTER f2 TYPE varchar COLLATE "POSIX";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- OK, because binary compatible and collations match
ALTER TABLE o_ddl_check ALTER f1 TYPE text;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
-- OK, because binary compatible and collations match
ALTER TABLE o_ddl_check ALTER f1 TYPE varchar COLLATE "POSIX";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
-- OK, because binary compatible and collations match
ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "C";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
-- OK, because same type and collation
ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "C";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
-- OK, because binary compatible and collations match
ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "POSIX";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- Fails, because no default conversion
ALTER TABLE o_ddl_check ALTER f2 TYPE timestamp;
-- Fails, because wrong date format
ALTER TABLE o_ddl_check ALTER f2 TYPE timestamp
  USING f2::timestamp without time zone;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- OK, because expression is valid conversion to char
ALTER TABLE o_ddl_check ALTER f2 TYPE char
	USING substr(f2, substr(f2,4,1)::int / 2, 1);
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

BEGIN;
ALTER TABLE o_ddl_check ALTER f2 TYPE int
	USING ('x' || lpad(f2, 8, '0'))::bit(32)::int;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
ROLLBACK;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

ALTER TABLE o_ddl_check ALTER f2 DROP NOT NULL;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
ALTER TABLE o_ddl_check ALTER f2 SET NOT NULL;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

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
SELECT orioledb_tbl_structure('o_ddl_check'::regclass, 'nue');

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

CREATE FUNCTION pseudo_random(seed bigint, i bigint) RETURNS float8 AS
$$
	SELECT substr(sha256(($1::text || ' ' || $2::text)::bytea)::text,2,16)::bit(52)::bigint::float8 / pow(2.0, 52.0);
$$ LANGUAGE sql;

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

CREATE TABLE o_test_unique_on_conflict (
	key int
) USING orioledb;

CREATE UNIQUE INDEX ON o_test_unique_on_conflict(key);

INSERT INTO o_test_unique_on_conflict(key)
	(SELECT key FROM generate_series (1, 1) key);
INSERT INTO o_test_unique_on_conflict (key)
	SELECT * FROM generate_series(1, 1)
	ON CONFLICT (key) DO UPDATE
		SET key = o_test_unique_on_conflict.key + 100;
SELECT * FROM o_test_unique_on_conflict;

CREATE TABLE o_test_update_set_renamed_column(
	val_1 int PRIMARY KEY,
	val_2 int
) USING orioledb;

INSERT INTO o_test_update_set_renamed_column(val_1, val_2)
	(SELECT val_1, val_1 FROM generate_series (1, 1) val_1);
SELECT * FROM o_test_update_set_renamed_column;

ALTER TABLE o_test_update_set_renamed_column RENAME COLUMN val_2 to val_3;

UPDATE o_test_update_set_renamed_column SET val_3 = 5;

SELECT * FROM o_test_update_set_renamed_column;

CREATE TABLE o_test_inherits_1 (
  val_1 int PRIMARY KEY
) USING orioledb;

CREATE TABLE o_test_inherits_2 (
	val_2 int
) INHERITS (o_test_inherits_1) USING orioledb;

BEGIN;
CREATE TABLE o_test(
	id integer NOT NULL,
	val text NOT NULL,
	PRIMARY KEY(id),
	UNIQUE(id, val)
) USING orioledb;
CREATE TABLE o_test_child(
	id integer NOT NULL,
	o_test_ID integer NOT NULL REFERENCES o_test (id),
	PRIMARY KEY(id)
) USING orioledb;
INSERT INTO o_test(id, val) VALUES (1, 'hello');
INSERT INTO o_test(id, val) VALUES (2, 'hey');
DELETE FROM o_test where id = 1;
COMMIT;

CREATE FUNCTION func_1(int) RETURNS int AS $$
DECLARE TOTAL int;
BEGIN
	CREATE TEMP TABLE o_test_1(val_1 int)USING orioledb;
	INSERT INTO o_test_1 VALUES($1);
	INSERT INTO o_test_1 VALUES(11);
	INSERT INTO o_test_1 VALUES(12);
	INSERT INTO o_test_1 VALUES(13);
	SELECT sum(val_1) INTO total FROM o_test_1;
	DROP TABLE o_test_1;
	RETURN total;
end
$$ language plpgsql;

SELECT func_1(1);
SELECT func_1(2);
SELECT func_1(3);

CREATE TABLE o_test_opcoptions_reset (
	val_1 int NOT NULL,
	val_3 text DEFAULT 'abc'
) USING orioledb;

INSERT INTO o_test_opcoptions_reset (val_1) VALUES (1);

BEGIN;
CREATE INDEX o_test_opcoptions_reset_idx1 ON o_test_opcoptions_reset (val_3);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_opcoptions_reset;
SELECT * FROM o_test_opcoptions_reset;
ALTER TABLE o_test_opcoptions_reset ADD PRIMARY KEY (val_1);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_opcoptions_reset;
SELECT * FROM o_test_opcoptions_reset;
COMMIT;

CREATE TABLE o_test_null_hasdef (
	val_1	int DEFAULT 1,
	val_2	text,
	val_3	text DEFAULT 'a'
) USING orioledb;

INSERT INTO o_test_null_hasdef VALUES (3);
INSERT INTO o_test_null_hasdef VALUES (4, NULL);
INSERT INTO o_test_null_hasdef VALUES (5, 'b', NULL);
INSERT INTO o_test_null_hasdef VALUES (6, NULL, NULL);
SELECT orioledb_tbl_structure('o_test_null_hasdef'::regclass, 'nue');
SELECT * FROM o_test_null_hasdef;

CREATE VIEW test_view_1 AS SELECT * FROM o_test_null_hasdef;

CREATE rule test_view_1 AS
	ON INSERT TO test_view_1
	  DO INSTEAD INSERT INTO o_test_null_hasdef SELECT new.*;

INSERT INTO test_view_1 VALUES (7);

SELECT orioledb_tbl_structure('o_test_null_hasdef'::regclass, 'nue');
SELECT * FROM test_view_1;
SELECT * FROM o_test_null_hasdef;

CREATE TABLE o_test_float_default (
  val_1 int DEFAULT 1,
  val_2 text DEFAULT 'a',
  val_3 float8 DEFAULT 1.1
)USING orioledb;
INSERT INTO o_test_float_default VALUES (2, null, 2.0);
SELECT * FROM o_test_float_default;

CREATE TABLE o_test_alter_change_byval (
    val_1 int
) USING orioledb;

INSERT INTO o_test_alter_change_byval VALUES (1);
ALTER TABLE o_test_alter_change_byval ADD COLUMN val_2 float8 DEFAULT 0.1;
SELECT * FROM o_test_alter_change_byval;
SELECT orioledb_tbl_structure('o_test_alter_change_byval'::regclass, 'nue');
ALTER TABLE o_test_alter_change_byval ALTER COLUMN val_2 SET DEFAULT 0.2;
INSERT INTO o_test_alter_change_byval VALUES (2);
SELECT * FROM o_test_alter_change_byval;
SELECT orioledb_tbl_structure('o_test_alter_change_byval'::regclass, 'nue');
ALTER TABLE o_test_alter_change_byval ALTER val_2 TYPE text USING val_2::text;
SELECT * FROM o_test_alter_change_byval;
SELECT orioledb_tbl_structure('o_test_alter_change_byval'::regclass, 'nue');

CREATE TABLE o_test_pkey_alter_type (
	val_1 int,
	val_2 int
) USING orioledb;

INSERT INTO o_test_pkey_alter_type
	SELECT v, v * 10 FROM generate_series(1, 5) v;

ALTER TABLE o_test_pkey_alter_type ADD PRIMARY KEY (val_1);
CREATE INDEX o_test_pkey_alter_type_ix1 ON o_test_pkey_alter_type (val_2);

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT orioledb_tbl_structure('o_test_pkey_alter_type'::regclass, 'nue');

ALTER TABLE o_test_pkey_alter_type ALTER val_1 TYPE int4;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT orioledb_tbl_structure('o_test_pkey_alter_type'::regclass, 'nue');

ALTER TABLE o_test_pkey_alter_type ALTER val_2 TYPE text USING val_2 || 'ROR';
SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT orioledb_tbl_structure('o_test_pkey_alter_type'::regclass, 'nue');

ALTER TABLE o_test_pkey_alter_type ALTER val_1 TYPE text USING val_1 || 'BOB';
SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT orioledb_tbl_structure('o_test_pkey_alter_type'::regclass, 'nue');
RESET enable_seqscan;

CREATE TABLE o_test_duplicate_key_fields (
	val_1 int
) USING orioledb;

CREATE INDEX o_test_duplicate_key_fields_ix1
	ON o_test_duplicate_key_fields (val_1, val_1) INCLUDE (val_1);

INSERT INTO o_test_duplicate_key_fields SELECT generate_series(1, 5);

SELECT orioledb_tbl_indices('o_test_duplicate_key_fields'::regclass);
SELECT orioledb_tbl_structure('o_test_duplicate_key_fields'::regclass, 'nue');

SET enable_seqscan = off;
SELECT * FROM o_test_duplicate_key_fields ORDER BY val_1;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_duplicate_key_fields ORDER BY val_1;
RESET enable_seqscan;

CREATE TABLE o_test_pkey_fields_same_as_index (
	val_1 int,
	val_2 int,
	val_3 int,
	UNIQUE (val_1, val_3)
) USING orioledb;
SELECT orioledb_tbl_indices('o_test_pkey_fields_same_as_index'::regclass);

INSERT INTO o_test_pkey_fields_same_as_index
	SELECT 1 * 10 ^ v, 2 * 10 ^ v, 3 * 10 ^ v FROM generate_series(0, 2) v;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;
SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;;

ALTER TABLE o_test_pkey_fields_same_as_index ADD PRIMARY KEY (val_1, val_3);
SELECT orioledb_tbl_indices('o_test_pkey_fields_same_as_index'::regclass);
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;
SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;

ALTER TABLE o_test_pkey_fields_same_as_index
	DROP CONSTRAINT o_test_pkey_fields_same_as_index_pkey;
SELECT orioledb_tbl_indices('o_test_pkey_fields_same_as_index'::regclass);
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;
SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;

CREATE TABLE o_test_null_pkey_field (
	val_1 text,
	val_2 text,
	val_3 text
) USING orioledb;

ALTER TABLE o_test_null_pkey_field ADD COLUMN val_10 text;

INSERT INTO o_test_null_pkey_field
	SELECT 1 * 10 ^ v, 2 * 10 ^ v, 3 * 10 ^ v
		FROM generate_series(0, 2) v;

ALTER TABLE o_test_null_pkey_field ADD PRIMARY KEY (val_1, val_3, val_10);
SELECT orioledb_tbl_indices('o_test_null_pkey_field'::regclass);
SELECT orioledb_tbl_structure('o_test_null_pkey_field'::regclass, 'nue');
SELECT * FROM o_test_null_pkey_field;

CREATE TABLE o_test_included_ix_name (
	a int,
	b int,
	c int,
	d int
) USING orioledb;
ALTER TABLE o_test_included_ix_name ADD PRIMARY KEY (d);
\d o_test_included_ix_name
CREATE INDEX ON o_test_included_ix_name (a, b) INCLUDE (a, c);
\d o_test_included_ix_name

CREATE TABLE o_test_add_pkey_empty_index (
	a int,
	b int,
	c int,
	d int
) USING orioledb;
CREATE INDEX ON o_test_add_pkey_empty_index (a, b);
\d o_test_add_pkey_empty_index
SELECT orioledb_tbl_indices('o_test_add_pkey_empty_index'::regclass);
ALTER TABLE o_test_add_pkey_empty_index ADD PRIMARY KEY (d);
\d o_test_add_pkey_empty_index
SELECT orioledb_tbl_indices('o_test_add_pkey_empty_index'::regclass);
INSERT INTO o_test_add_pkey_empty_index
	SELECT v, v*10, v*100, v*1000 FROM generate_series(1, 5) v;
EXPLAIN (COSTS OFF) SELECT a, b, d FROM o_test_add_pkey_empty_index ORDER BY a;
SELECT a, b, d FROM o_test_add_pkey_empty_index ORDER BY a;
SELECT orioledb_tbl_structure('o_test_add_pkey_empty_index'::regclass, 'nue');
\d o_test_add_pkey_empty_index
SELECT orioledb_tbl_indices('o_test_add_pkey_empty_index'::regclass);

CREATE TABLE o_test_empty() USING orioledb;
\d o_test_empty
SELECT orioledb_table_description('o_test_empty'::regclass);
SELECT * FROM o_test_empty;
SELECT orioledb_tbl_structure('o_test_empty'::regclass, 'nue');
TRUNCATE o_test_empty;
SELECT * FROM o_test_empty;

BEGIN;
CREATE TABLE o_test_multiple_set_type_same_trx (
	val_1 int,
	val_3 int
) USING orioledb;

SELECT * FROM o_test_multiple_set_type_same_trx;

CREATE UNIQUE INDEX o_test_multiple_set_type_same_trx_ix1
	ON o_test_multiple_set_type_same_trx (val_1);

SELECT * FROM o_test_multiple_set_type_same_trx ORDER BY val_1;

INSERT INTO o_test_multiple_set_type_same_trx
	SELECT x, 3*x FROM generate_series(1,10) AS x;

ALTER TABLE o_test_multiple_set_type_same_trx ALTER val_1 TYPE bigint;

ALTER TABLE o_test_multiple_set_type_same_trx ALTER val_3 TYPE bigint;

COMMIT;

\d o_test_multiple_set_type_same_trx

CREATE FUNCTION o_test_plpgsql_default_func(a int)
RETURNS TEXT
AS $$
    BEGIN
		RETURN 'WOW' || a;
    END;
$$ LANGUAGE plpgsql;
CREATE TABLE o_test_plpgsql_default (
    val_1 int DEFAULT LENGTH(o_test_plpgsql_default_func(6))
) USING orioledb;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA ddl CASCADE;
RESET search_path;
