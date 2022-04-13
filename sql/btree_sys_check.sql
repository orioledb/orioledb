CREATE EXTENSION orioledb;

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
INSERT INTO o_test_child(id, o_test_ID) VALUES (1, 1);
INSERT INTO o_test_child(id, o_test_ID) VALUES (2, 2);
UPDATE o_test SET val = 'new_dog' where id = 7;
UPDATE o_test SET id = 10 where id = 3;
DELETE FROM o_test_child where o_test_ID = 1;
DELETE FROM o_test where id = 1;

SELECT regexp_replace(c.relname, '\d+', 'NNN') relname, d.refcnt
FROM orioledb_table_descr d JOIN
     pg_class c ON c.oid = d.reloid
ORDER BY c.relname;
SELECT regexp_replace(c.relname, '\d+', 'NNN') relname, d.refcnt
FROM orioledb_index_descr d JOIN
     pg_class c ON c.oid = d.reloid
ORDER BY c.relname;

DROP TABLE IF EXISTS o_test_child;
DROP TABLE IF EXISTS o_test;

CREATE TYPE o_enum AS ENUM ('a', 'b', 'c');
ALTER TYPE o_enum ADD VALUE 'd';
ALTER TYPE o_enum RENAME VALUE 'd' TO 'e';

CREATE TYPE custom_range as range (subtype=int8);

CREATE TYPE custom_type AS (x timestamp, y float);

CREATE TABLE o_test_typecaches (
	key o_enum,
	key2 custom_range,
	key3 custom_type,
	key4 int[],
	PRIMARY KEY(key, key2, key3, key4)
) USING orioledb;

DROP TYPE custom_range CASCADE;
DROP TABLE o_test_typecaches;
DROP TYPE o_enum;
DROP TYPE custom_type;

CHECKPOINT;

-- SYS_TREES_SHARED_ROOT_INFO
SELECT regexp_replace(
		orioledb_sys_tree_structure(1, 'ne'),
		'\(\d+, \d+\), \d+, \d+\)',
		'(NNN, NNN), NNN, NNN)',
		'g');

-- SYS_TREES_O_TABLES
SELECT regexp_replace(
		orioledb_sys_tree_structure(2, 'ne'),
		'\(\d+, \d+, \d+\)',
		'(NNN, NNN, NNN)',
		'g');

-- SYS_TREES_O_INDICES
SELECT regexp_replace(
		orioledb_sys_tree_structure(3, 'ne'),
		'\(\d+, \d+, \d+\)',
		'(NNN, NNN, NNN)',
		'g');

-- SYS_TREES_OPCLASSES
SELECT regexp_replace(
		orioledb_sys_tree_structure(4, 'ne'),
		'\(\d+, \d+\)',
		'(NNN, NNN)',
		'g');

-- SYS_TREES_ENUM_CACHE
SELECT regexp_replace(
		orioledb_sys_tree_structure(5, 'ne'),
		'\d+, \d+, [A-F0-9]+/[A-F0-9]+, ',
		'NNN, NNN, X/X, ',
		'g');

-- SYS_TREES_ENUMOID_CACHE
SELECT regexp_replace(
		orioledb_sys_tree_structure(6, 'ne'),
		'\d+, \d+, [A-F0-9]+/[A-F0-9]+, ([YN])\), \d+',
		'NNN, NNN, X/X, \1), NNN',
		'g');

-- SYS_TREES_RANGE_CACHE
SELECT regexp_replace(
		orioledb_sys_tree_structure(7, 'ne'),
		'\d+, \d+, [A-F0-9]+/[A-F0-9]+, ',
		'NNN, NNN, X/X, ',
		'g');

-- SYS_TREES_RECORD_CACHE
SELECT regexp_replace(
		orioledb_sys_tree_structure(8, 'ne'),
		'\d+, \d+, [A-F0-9]+/[A-F0-9]+, ',
		'NNN, NNN, X/X, ',
		'g');

-- SYS_TREES_TYPE_ELEMENT_CACHE
SELECT regexp_replace(
		orioledb_sys_tree_structure(9, 'ne'),
		'\d+, \d+, [A-F0-9]+/[A-F0-9]+, ',
		'NNN, NNN, X/X, ',
		'g');

-- SYS_TREES_EXTENTS_OFF_LEN
SELECT orioledb_sys_tree_structure(10, 'ne');

-- SYS_TREES_EXTENTS_LEN_OFF
SELECT orioledb_sys_tree_structure(11, 'ne');

-- fail
SELECT orioledb_sys_tree_structure(9999);
SELECT orioledb_sys_tree_check(-1111);

DROP EXTENSION orioledb CASCADE;