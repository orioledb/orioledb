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
SELECT orioledb_parallel_debug_start();
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
SELECT orioledb_parallel_debug_stop();

SELECT * FROM o_ddl_check;
SELECT orioledb_table_description('o_ddl_check'::regclass);

-- Fails because can't drop NOT NULL contraint on PK
ALTER TABLE o_ddl_check ALTER f1 DROP NOT NULL;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- Fails on unknown option
ALTER TABLE o_ddl_check OPTIONS (SET hello 'world');

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

CREATE SEQUENCE o_test_add_column_id_seq2;
CREATE TABLE o_test_add_column
(
	id serial primary key,
	i int4,
	v int4 default nextval('o_test_add_column_id_seq2'::regclass)
) USING orioledb;
\d o_test_add_column
SELECT orioledb_tbl_indices('o_test_add_column'::regclass);
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');

INSERT INTO o_test_add_column VALUES (0, 15, NULL);
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

-- Test primary key usage after rewrite
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_add_column ORDER BY id;
SELECT * FROM o_test_add_column ORDER BY id;
COMMIT;

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

CREATE VIEW o_test_view_1 AS SELECT * FROM o_test_null_hasdef;

CREATE rule o_test_view_1 AS
	ON INSERT TO o_test_view_1
	  DO INSTEAD INSERT INTO o_test_null_hasdef SELECT new.*;

INSERT INTO o_test_view_1 VALUES (7);

SELECT orioledb_tbl_structure('o_test_null_hasdef'::regclass, 'nue');
SELECT * FROM o_test_view_1;
SELECT * FROM o_test_null_hasdef;

CREATE TABLE o_test_float_default (
  val_1 int DEFAULT 1,
  val_2 text DEFAULT 'a',
  val_3 float8 DEFAULT 1.1
)USING orioledb;
INSERT INTO o_test_float_default VALUES (2, null, 2.0);
SELECT * FROM o_test_float_default;

CREATE TABLE o_test_duplicate_key_fields (
	val_2 int,
	val_1 int
) USING orioledb;

CREATE INDEX o_test_duplicate_key_fields_ix1
	ON o_test_duplicate_key_fields (val_1, val_2, val_1) INCLUDE (val_1);

INSERT INTO o_test_duplicate_key_fields SELECT v, v * 10 FROM generate_series(1, 5) v;

SELECT orioledb_tbl_indices('o_test_duplicate_key_fields'::regclass);
SELECT orioledb_tbl_structure('o_test_duplicate_key_fields'::regclass, 'nue');

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT val_1 FROM o_test_duplicate_key_fields ORDER BY val_1;
SELECT val_1 FROM o_test_duplicate_key_fields ORDER BY val_1;
RESET enable_seqscan;

CREATE TABLE o_test_pkey_fields_same_as_index (
	val_1 int,
	val_2 int,
	val_3 int,
	UNIQUE (val_1, val_3)
) USING orioledb;
SELECT orioledb_tbl_indices('o_test_pkey_fields_same_as_index'::regclass);

SET enable_seqscan = off;

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

RESET enable_seqscan;

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
	d int8
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

CREATE TABLE o_test_35_columns (
  gid serial,
  col1 varchar(1),
  col2 varchar(1),
  col3 varchar(1),
  col4 varchar(1),
  col5 varchar(1),
  col6 varchar(1),
  col7 varchar(1),
  col8 varchar(1),
  col9 varchar(1),
  col10 varchar(1),
  col11 varchar(1),
  col12 varchar(1),
  col13 varchar(1),
  col14 varchar(1),
  col15 varchar(1),
  col16 varchar(1),
  col17 varchar(1),
  col18 varchar(1),
  col19 varchar(1),
  col20 varchar(1),
  col21 varchar(1),
  col22 varchar(1),
  col23 varchar(1),
  col24 varchar(1),
  col25 varchar(1),
  col26 varchar(1),
  col27 varchar(1),
  col28 varchar(1),
  col29 varchar(1),
  col30 varchar(1),
  col31 varchar(1),
  col32 varchar(1),
  col33 varchar(1),
  col34 varchar(1)
) using orioledb;

INSERT INTO o_test_35_columns (col27, col10) VALUES ('A', 'J');
SELECT gid, col10, col15, col27, col33, col34 FROM o_test_35_columns;

CREATE TABLE o_test_replica_identity_set (i int PRIMARY KEY, t text) USING orioledb;
INSERT INTO o_test_replica_identity_set VALUES(1, 'foofoo');
INSERT INTO o_test_replica_identity_set VALUES(2, 'barbar');
ALTER TABLE o_test_replica_identity_set REPLICA IDENTITY FULL;
INSERT INTO o_test_replica_identity_set VALUES(3, 'aaaaaa');
SELECT * FROM o_test_replica_identity_set;
\d+ o_test_replica_identity_set

CREATE TABLE o_test_replica_identity_fail (i int PRIMARY KEY, t text) USING orioledb;
INSERT INTO o_test_replica_identity_fail VALUES(1, 'foofoo');
INSERT INTO o_test_replica_identity_fail VALUES(2, 'barbar');
ALTER TABLE o_test_replica_identity_fail REPLICA IDENTITY NOTHING;
INSERT INTO o_test_replica_identity_fail VALUES(3, 'aaaaaa');
SELECT * FROM o_test_replica_identity_fail;
\d+ o_test_replica_identity_fail

CREATE TABLE o_test_set_access_method_fail (i int PRIMARY KEY, t text) USING orioledb;
ALTER TABLE o_test_set_access_method_fail SET ACCESS METHOD heap;

-- Test AT_SetStatistics
CREATE TABLE o_test_set_statistics (
	i int PRIMARY KEY,
	t text,
	v varchar
) USING orioledb;

INSERT INTO o_test_set_statistics VALUES (1, 'test', 'data');

-- Set statistics target for columns
ALTER TABLE o_test_set_statistics ALTER COLUMN t SET STATISTICS 100;
ALTER TABLE o_test_set_statistics ALTER COLUMN v SET STATISTICS 1000;

-- Verify the changes
SELECT
	attname,
	CASE
		WHEN attstattarget is NULL THEN -1 -- as PG17 changes behaviour for default stats target
		ELSE attstattarget
	END AS attstattarget
FROM pg_attribute
WHERE attrelid = 'o_test_set_statistics'::regclass
  AND attnum > 0
ORDER BY attnum;

-- Reset statistics to default
ALTER TABLE o_test_set_statistics ALTER COLUMN t SET STATISTICS -1;

SELECT 
	attname,
	CASE
		WHEN attstattarget is NULL THEN -1 -- as PG17 changes behaviour for default stats target
		ELSE attstattarget
	END AS attstattarget
FROM pg_attribute
WHERE attrelid = 'o_test_set_statistics'::regclass
  AND attname = 't';

-- Test AT_SetLogged / AT_SetUnLogged
CREATE UNLOGGED TABLE o_test_logged_changes (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Check initial unlogged state
SELECT relname, relpersistence
FROM pg_class
WHERE relname = 'o_test_logged_changes';

-- Change to logged
ALTER TABLE o_test_logged_changes SET LOGGED;

SELECT relname, relpersistence
FROM pg_class
WHERE relname = 'o_test_logged_changes';

-- Change back to unlogged
ALTER TABLE o_test_logged_changes SET UNLOGGED;

SELECT relname, relpersistence
FROM pg_class
WHERE relname = 'o_test_logged_changes';

-- Test with data
INSERT INTO o_test_logged_changes VALUES (1, 'test data');
ALTER TABLE o_test_logged_changes SET LOGGED;

SELECT * FROM o_test_logged_changes;

-- Test AT_SetOptions / AT_ResetOptions (column-level options)
CREATE TABLE o_test_column_options (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Set column-level storage options
ALTER TABLE o_test_column_options ALTER COLUMN t SET (n_distinct = 100, n_distinct_inherited = 50);

-- Verify options are set
SELECT attname, attoptions
FROM pg_attribute
WHERE attrelid = 'o_test_column_options'::regclass
  AND attnum > 0
  AND attoptions IS NOT NULL;

-- Reset specific option
ALTER TABLE o_test_column_options ALTER COLUMN t RESET (n_distinct);

-- Verify reset
SELECT attname, attoptions
FROM pg_attribute
WHERE attrelid = 'o_test_column_options'::regclass
  AND attname = 't';

-- Reset all options
ALTER TABLE o_test_column_options ALTER COLUMN t RESET (n_distinct_inherited);

SELECT attname, attoptions
FROM pg_attribute
WHERE attrelid = 'o_test_column_options'::regclass
  AND attname = 't';

-- Test AT_ResetRelOptions / AT_SetRelOptions (table-level options)
CREATE TABLE o_test_table_options (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Set table-level options
ALTER TABLE o_test_table_options SET (fillfactor = 70, autovacuum_enabled = false);

-- Verify table options
SELECT relname, reloptions
FROM pg_class
WHERE relname = 'o_test_table_options';

-- Reset specific option
ALTER TABLE o_test_table_options RESET (autovacuum_enabled);

SELECT relname, reloptions
FROM pg_class
WHERE relname = 'o_test_table_options';

-- Reset all options
ALTER TABLE o_test_table_options RESET (fillfactor);

SELECT relname, reloptions
FROM pg_class
WHERE relname = 'o_test_table_options';

-- Test AT_ClusterOn / AT_DropCluster
CREATE TABLE o_test_cluster (
	i int,
	t text,
	v varchar,
	PRIMARY KEY (i)
) USING orioledb;

CREATE INDEX o_test_cluster_idx ON o_test_cluster(t);

-- Set cluster index
ALTER TABLE o_test_cluster CLUSTER ON o_test_cluster_idx;

-- Verify cluster setting
SELECT indexrelid::regclass AS index_name, indisclustered
FROM pg_index
WHERE indrelid = 'o_test_cluster'::regclass
ORDER BY indexrelid::regclass::text;

-- Drop cluster setting
ALTER TABLE o_test_cluster SET WITHOUT CLUSTER;

-- Verify cluster removed
SELECT indexrelid::regclass AS index_name, indisclustered
FROM pg_index
WHERE indrelid = 'o_test_cluster'::regclass
ORDER BY indexrelid::regclass::text;

-- Test AT_EnableRule / AT_DisableRule (on tables)
-- ENABLE/DISABLE RULE commands only work on tables, not views
CREATE TABLE o_test_rule_table (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Create a rule on the table that filters certain inserts
CREATE RULE o_test_insert_rule AS
	ON INSERT TO o_test_rule_table
	WHERE t = 'skip'
	DO INSTEAD NOTHING;

-- Verify rule is enabled (ev_enabled = 'O' means origin)
SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'o_test_insert_rule';

-- Test that rule works: insert with 'skip' should be ignored
INSERT INTO o_test_rule_table VALUES (1, 'skip');
INSERT INTO o_test_rule_table VALUES (2, 'normal');
SELECT * FROM o_test_rule_table ORDER BY i;

-- Disable the rule
ALTER TABLE o_test_rule_table DISABLE RULE o_test_insert_rule;

-- Verify rule is disabled (ev_enabled = 'D')
SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'o_test_insert_rule';

-- Now the 'skip' insert should work since rule is disabled
INSERT INTO o_test_rule_table VALUES (1, 'skip');
SELECT * FROM o_test_rule_table ORDER BY i;

-- Enable the rule back (origin mode)
ALTER TABLE o_test_rule_table ENABLE RULE o_test_insert_rule;

SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'o_test_insert_rule';

-- Enable rule for replica (ev_enabled = 'R')
ALTER TABLE o_test_rule_table ENABLE REPLICA RULE o_test_insert_rule;

SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'o_test_insert_rule';

-- Enable rule always (ev_enabled = 'A')
ALTER TABLE o_test_rule_table ENABLE ALWAYS RULE o_test_insert_rule;

SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'o_test_insert_rule';

-- Cleanup
DROP TABLE o_test_rule_table CASCADE;

-- Test AT_CheckNotNull (internally generated for partitioned tables)
-- AT_CheckNotNull is generated when you use ALTER TABLE ONLY ... SET NOT NULL
-- on a partitioned table. It verifies that all child partitions can satisfy NOT NULL.

-- Test AT_CheckNotNull failure case: partition without NOT NULL
CREATE TABLE o_test_check_not_null_fail (
	i int,
	val text  -- Note: no NOT NULL constraint
) PARTITION BY RANGE (i);

CREATE TABLE o_test_check_not_null_fail_p1 PARTITION OF o_test_check_not_null_fail
	FOR VALUES FROM (1) TO (100) USING orioledb;

-- Verify partition does not have NOT NULL
SELECT c.relname, a.attname, a.attnotnull
FROM pg_class c
JOIN pg_attribute a ON a.attrelid = c.oid
WHERE c.relname LIKE 'o_test_check_not_null_fail%'
  AND a.attname = 'val'
  AND c.relnamespace = 'ddl'::regnamespace
ORDER BY c.relname;

-- Try to set NOT NULL on parent ONLY (should fail because partition has NULL values)
ALTER TABLE ONLY o_test_check_not_null_fail ALTER COLUMN val SET NOT NULL;

-- Test AT_ValidateConstraint (validate a NOT VALID constraint)
CREATE TABLE o_test_validate_constraint (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Insert some data
INSERT INTO o_test_validate_constraint VALUES (1, 'test'), (2, 'data');

-- Add a check constraint without validation
ALTER TABLE o_test_validate_constraint ADD CONSTRAINT check_t_length CHECK (length(t) > 2) NOT VALID;

-- Verify constraint exists but not validated
SELECT conname, convalidated
FROM pg_constraint
WHERE conrelid = 'o_test_validate_constraint'::regclass
  AND conname = 'check_t_length';

-- Now validate the constraint
ALTER TABLE o_test_validate_constraint VALIDATE CONSTRAINT check_t_length;

-- Verify constraint is now validated
SELECT conname, convalidated
FROM pg_constraint
WHERE conrelid = 'o_test_validate_constraint'::regclass
  AND conname = 'check_t_length';

-- Test AT_ReplaceRelOptions (used by CREATE OR REPLACE VIEW with options)
-- AT_ReplaceRelOptions is triggered internally when CREATE OR REPLACE VIEW
-- changes the view's options (security_barrier, security_invoker, check_option)
CREATE TABLE o_test_view_base (
	i int PRIMARY KEY,
	t text,
	val int
) USING orioledb;

INSERT INTO o_test_view_base VALUES (1, 'alice', 100), (2, 'bob', 200), (3, 'charlie', 300);

-- Create view without options
CREATE VIEW o_test_replace_view AS SELECT * FROM o_test_view_base WHERE val > 0;

-- Check initial view options (should be NULL or empty)
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'o_test_replace_view';

-- Use CREATE OR REPLACE VIEW to add security_barrier option
-- This triggers AT_ReplaceRelOptions internally
CREATE OR REPLACE VIEW o_test_replace_view WITH (security_barrier=true)
AS SELECT * FROM o_test_view_base WHERE val > 100;

-- Verify security_barrier option is set
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'o_test_replace_view';

-- Test the view still works
SELECT * FROM o_test_replace_view ORDER BY i;

-- Replace view again with different options (security_invoker)
-- This replaces the entire options list with new one
CREATE OR REPLACE VIEW o_test_replace_view WITH (security_invoker=true)
AS SELECT * FROM o_test_view_base WHERE val > 50;

-- Verify options replaced (should now have security_invoker, not security_barrier)
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'o_test_replace_view';

SELECT * FROM o_test_replace_view ORDER BY i;

-- Replace view with multiple options
CREATE OR REPLACE VIEW o_test_replace_view
WITH (security_barrier=true, security_invoker=true, check_option=local)
AS SELECT * FROM o_test_view_base WHERE val > 0;

-- Verify multiple options set
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'o_test_replace_view';

-- Replace view with no options (clears all options)
CREATE OR REPLACE VIEW o_test_replace_view
AS SELECT * FROM o_test_view_base WHERE val >= 100;

-- Verify options cleared
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'o_test_replace_view';

SELECT * FROM o_test_replace_view ORDER BY i;

DROP VIEW o_test_replace_view;
DROP TABLE o_test_view_base CASCADE;

-- Test AT_AddOf and AT_DropOf (typed tables)
-- Typed tables are tables that are bound to a composite type
-- AT_AddOf converts a regular table to a typed table
-- AT_DropOf converts a typed table back to a regular table

-- Create a composite type for employee data
CREATE TYPE employee_type AS (
	emp_id int,
	emp_name text,
	emp_salary numeric(10,2)
);

-- Create a regular table (not typed)
CREATE TABLE o_test_regular_table (
	emp_id int PRIMARY KEY,
	emp_name text,
	emp_salary numeric(10,2)
) USING orioledb;

-- Check initial state (reloftype should be 0 for regular table)
SELECT
	relname,
	CASE 
		WHEN reloftype = 0 THEN 'regular'
		ELSE 'typed'
	END AS reloftype,
	relkind
FROM pg_class
WHERE relname = 'o_test_regular_table';

-- Insert test data
INSERT INTO o_test_regular_table VALUES (1, 'Alice', 70000);
INSERT INTO o_test_regular_table VALUES (2, 'Bob', 65000);

SELECT * FROM o_test_regular_table ORDER BY emp_id;

-- Test AT_AddOf - convert regular table to typed table
ALTER TABLE o_test_regular_table OF employee_type;

-- Verify table is now typed (reloftype should be OID of employee_type)
SELECT
	relname,
	CASE 
		WHEN reloftype = 0 THEN 'regular'
		ELSE 'typed'
	END AS reloftype,
	relkind
FROM pg_class
WHERE relname = 'o_test_regular_table';

-- Verify data is preserved
SELECT * FROM o_test_regular_table ORDER BY emp_id;

-- Typed tables still work normally for DML
INSERT INTO o_test_regular_table VALUES (3, 'Charlie', 80000);
UPDATE o_test_regular_table SET emp_salary = 72000 WHERE emp_id = 1;
DELETE FROM o_test_regular_table WHERE emp_id = 2;

SELECT * FROM o_test_regular_table ORDER BY emp_id;

-- Test that ADD COLUMN fails on typed table (must modify type instead)
ALTER TABLE o_test_regular_table ADD COLUMN emp_department text;  -- Should fail

-- Test that DROP COLUMN fails on typed table
ALTER TABLE o_test_regular_table DROP COLUMN emp_salary;  -- Should fail

-- Test AT_DropOf - convert typed table back to regular table
ALTER TABLE o_test_regular_table NOT OF;

-- Verify table is no longer typed (reloftype should be 0)
SELECT
	relname,
	CASE 
		WHEN reloftype = 0 THEN 'regular'
		ELSE 'typed'
	END AS reloftype,
	relkind
FROM pg_class
WHERE relname = 'o_test_regular_table';

-- Verify data is still preserved
SELECT * FROM o_test_regular_table ORDER BY emp_id;

-- Regular table operations still work, including ADD COLUMN
INSERT INTO o_test_regular_table VALUES (4, 'Diana', 85000);

-- Now that it's a regular table, ADD COLUMN should succeed
ALTER TABLE o_test_regular_table ADD COLUMN emp_department text;

-- Verify new column exists
SELECT * FROM o_test_regular_table ORDER BY emp_id;

-- Create a typed table directly using OF syntax
CREATE TABLE o_test_typed_table OF employee_type (
	PRIMARY KEY (emp_id)
) USING orioledb;

-- Verify it's typed from creation
SELECT
	relname,
	CASE 
		WHEN reloftype = 0 THEN 'regular'
		ELSE 'typed'
	END AS reloftype,
	relkind
FROM pg_class
WHERE relname = 'o_test_typed_table';

-- Insert data into typed table
INSERT INTO o_test_typed_table VALUES (10, 'Eve', 60000);
INSERT INTO o_test_typed_table VALUES (11, 'Frank', 62000);

SELECT * FROM o_test_typed_table ORDER BY emp_id;

-- Convert it to regular table using AT_DropOf
ALTER TABLE o_test_typed_table NOT OF;

-- Verify it's no longer typed
SELECT
	relname,
	CASE 
		WHEN reloftype = 0 THEN 'regular'
		ELSE 'typed'
	END AS reloftype,
	relkind
FROM pg_class
WHERE relname = 'o_test_typed_table';

-- Data still accessible
SELECT * FROM o_test_typed_table ORDER BY emp_id;

ALTER TABLE o_test_typed_table ADD COLUMN emp_department text;
SELECT * FROM o_test_typed_table ORDER BY emp_id;

-- Cleanup
DROP TABLE o_test_typed_table CASCADE;
DROP TABLE o_test_regular_table CASCADE;
DROP TYPE employee_type;

-- Test AT_AddColumnToView
-- AT_AddColumnToView is triggered internally by CREATE OR REPLACE VIEW
-- when the replacement view has additional columns compared to the original
CREATE TABLE o_test_view_source (
	i int PRIMARY KEY,
	name text,
	value int,
	score int
) USING orioledb;

INSERT INTO o_test_view_source VALUES
	(1, 'alice', 100, 85),
	(2, 'bob', 200, 92),
	(3, 'charlie', 300, 78);

-- Create an initial view with 2 columns
CREATE VIEW o_test_add_col_view AS SELECT i, name FROM o_test_view_source;

-- Check initial view structure (2 columns)
\d o_test_add_col_view
SELECT * FROM o_test_add_col_view ORDER BY i;

-- Test AT_AddColumnToView: Replace view with an additional column
-- This internally triggers AT_AddColumnToView for the 'value' column
CREATE OR REPLACE VIEW o_test_add_col_view AS
	SELECT i, name, value FROM o_test_view_source;

-- Check updated view structure (3 columns now)
\d o_test_add_col_view
SELECT * FROM o_test_add_col_view ORDER BY i;

ALTER TABLE o_test_view_source ADD COLUMN extra_info text DEFAULT 'N/A';

CREATE OR REPLACE VIEW o_test_add_col_view AS
	SELECT i, name, value, extra_info FROM o_test_view_source;

-- Check updated view structure (4 columns now)
\d o_test_add_col_view
SELECT * FROM o_test_add_col_view ORDER BY i;

-- Cleanup views
DROP VIEW o_test_add_col_view CASCADE;
DROP TABLE o_test_view_source CASCADE;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA ddl CASCADE;
RESET search_path;
