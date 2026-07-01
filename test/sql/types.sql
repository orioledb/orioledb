CREATE SCHEMA types;
SET SESSION search_path = 'types';
CREATE EXTENSION orioledb;

CREATE TYPE coordinates AS (
	x int,
	y int
);

-- Check support of custom types indices
CREATE TABLE o_test_record_type
(
	location coordinates NOT NULL,
	val int NOT NULL,
	PRIMARY KEY(location)
) USING orioledb;

CREATE INDEX o_test_record_type_ix1 ON o_test_record_type(val);

INSERT INTO o_test_record_type SELECT (id, id * 2)::coordinates,
									  id * 10 val FROM
										generate_series(1, 10) id;

SELECT * FROM o_test_record_type;

-- Check support of range indices
CREATE TABLE o_test_range
(
	area int8range NOT NULL,
	val int NOT NULL,
	PRIMARY KEY(area)
) USING orioledb;

INSERT INTO o_test_range SELECT int8range(id * 5, id * 5 + 5),
								id * 10 val FROM generate_series(1, 10) id;

SELECT * FROM o_test_range;

CREATE TYPE custom_range as range (subtype=int8);

-- Custom range also
CREATE TABLE o_test_custom_range
(
	area custom_range NOT NULL,
	val int NOT NULL,
	PRIMARY KEY(area)
) USING orioledb;

INSERT INTO o_test_custom_range SELECT custom_range(id * 5, id * 5 + 5),
								id * 10 val FROM generate_series(1, 10) id;

SELECT * FROM o_test_custom_range;

-- Check support of array indices
CREATE TABLE o_test_array
(
	locations coordinates[] NOT NULL,
	id int NOT NULL,
	PRIMARY KEY(locations)
) USING orioledb;

INSERT INTO o_test_array SELECT ARRAY[(id, id * 2)::coordinates,
									  (id, id * 10)::coordinates],
								id FROM generate_series(1, 10) id;

SELECT * FROM o_test_array;

CREATE TABLE o_test_int_array
(
	arr int2[] NOT NULL,
	PRIMARY KEY(arr)
) USING orioledb;
BEGIN;
INSERT INTO o_test_int_array VALUES ('{1, 2}');
COMMIT;
INSERT INTO o_test_int_array VALUES ('{2, 3, 4}');
SELECT * FROM o_test_int_array;

---
-- PostgreSQL has no full support for arrays comparison with CID and XIP elements.
-- We forbit create indices with such arrays.
-- Behavior differs from postgres tables, which illustrated by tests below
---
CREATE TABLE pg_test_cid_array
(
	arr cid[] NOT NULL,
	PRIMARY KEY (arr)
);
BEGIN;
INSERT INTO pg_test_cid_array VALUES (ARRAY['1'::cid, '2'::cid]);
COMMIT;
INSERT INTO pg_test_cid_array VALUES (ARRAY['1'::cid, '3'::cid]);
SELECT * FROM pg_test_cid_array;
DROP TABLE pg_test_cid_array;

CREATE TABLE o_test_cid_array
(
	arr cid[] NOT NULL,
	PRIMARY KEY(arr)
) USING orioledb;
BEGIN;
INSERT INTO o_test_cid_array VALUES (ARRAY['1'::cid, '2'::cid]);
COMMIT;

CREATE TYPE pg_rec AS (br cid);
CREATE TABLE pg_test_cid_record
(
	a pg_rec,
	PRIMARY KEY(a)
);
BEGIN;
INSERT INTO pg_test_cid_record VALUES (ROW('1'::cid));
INSERT INTO pg_test_cid_record VALUES (ROW('2'::cid));
COMMIT;
DROP TABLE pg_test_cid_record;

CREATE TYPE o_rec AS (br cid);
CREATE TABLE o_test_cid_record
(
	a o_rec,
	PRIMARY KEY(a)
) USING orioledb;
BEGIN;
INSERT INTO o_test_cid_record VALUES (ROW('1'::cid));
INSERT INTO o_test_cid_record VALUES (ROW('2'::cid));
COMMIT;

CREATE TABLE pg_test_xid_array
(
	arr xid[] NOT NULL,
	PRIMARY KEY (arr)
);
BEGIN;
INSERT INTO pg_test_xid_array VALUES (ARRAY['1'::xid, '2'::xid]);
COMMIT;
INSERT INTO pg_test_xid_array VALUES (ARRAY['1'::xid, '3'::xid]);
SELECT * FROM pg_test_xid_array;
DROP TABLE pg_test_xid_array;

CREATE TABLE o_test_xid_array
(
	arr xid[] NOT NULL,
	PRIMARY KEY (arr)
) USING orioledb;
BEGIN;
INSERT INTO o_test_xid_array VALUES (ARRAY['1'::xid, '2'::xid]);
COMMIT;

-- Check support of enum indices
CREATE TYPE o_happiness AS ENUM ('happy', 'very happy', 'ecstatic');

-- autovacuum off: DROP TYPE o_happiness CASCADE later in the test drops the
-- index on this table; a concurrent ANALYZE deadlocks with that cascade.
CREATE TABLE o_test_enum_index (
	num_weeks integer NOT NULL,
	happiness o_happiness NOT NULL,
	PRIMARY KEY(happiness)
) USING orioledb WITH (autovacuum_enabled = false);

CREATE INDEX o_test_enum_index_ix1 ON o_test_enum_index(happiness);
CREATE UNIQUE INDEX o_test_enum_index_ix2 ON o_test_enum_index(happiness);

INSERT INTO o_test_enum_index(num_weeks, happiness) VALUES (2, 'happy');
INSERT INTO o_test_enum_index(num_weeks, happiness) VALUES (6, 'very happy');
INSERT INTO o_test_enum_index(num_weeks, happiness) VALUES (8, 'ecstatic');
ALTER TYPE o_happiness ADD VALUE 'sad' BEFORE 'very happy';
INSERT INTO o_test_enum_index(num_weeks, happiness) VALUES (4, 'sad');

SELECT * FROM o_test_enum_index;
SELECT * FROM o_test_enum_index WHERE happiness = 'very happy';
SELECT * FROM o_test_enum_index WHERE happiness > 'very happy';
SELECT * FROM o_test_enum_index WHERE happiness < 'very happy';

-- check that column and index drops
SELECT orioledb_table_description('o_test_enum_index'::regclass);
SELECT orioledb_tbl_indices('o_test_enum_index'::regclass);

DROP TYPE o_happiness;
DROP INDEX o_test_enum_index_ix1;
BEGIN;
ALTER TABLE o_test_enum_index DROP CONSTRAINT o_test_enum_index_pkey;
ROLLBACK;
DROP TYPE o_happiness CASCADE;

SELECT orioledb_table_description('o_test_enum_index'::regclass);
SELECT orioledb_tbl_indices('o_test_enum_index'::regclass);

SELECT * FROM o_test_enum_index;

-- Check that domains also works
CREATE DOMAIN myint AS integer CHECK (VALUE > 5);
CREATE DOMAIN myint2 AS myint;
CREATE DOMAIN mytime AS time CHECK (VALUE > '00:00:00');
CREATE DOMAIN myarray AS myint[];
CREATE DOMAIN mybool AS boolean;

-- autovacuum off: DROP DOMAIN ... CASCADE later in the test drops columns
-- (and PK) on this table; concurrent ANALYZE deadlocks with that cascade.
CREATE TABLE o_test_domain_index
(
	key myint NOT NULL,
	key2 myint2,
	t   mytime,
	a   myarray,
	b   mybool,
	val int NOT NULL,
	net inet,
	PRIMARY KEY(key, key2, val, t, a, b, net)
) USING orioledb WITH (autovacuum_enabled = false);

INSERT INTO o_test_domain_index VALUES
	(10, 10, '00:00:10', '{10}', false, 10, '10.10.10.10'),
	(20, 20, '00:00:20', '{20}', true, 20, '20.20.20.20');

SELECT * FROM o_test_domain_index;

-- autovacuum off: same reason as o_test_domain_index above.
CREATE TABLE o_test_domain_array_index
(
	key myint[] NOT NULL,
	PRIMARY KEY(key)
) USING orioledb WITH (autovacuum_enabled = false);

INSERT INTO o_test_domain_array_index VALUES (ARRAY[10::myint, 20::myint]);

SELECT * FROM o_test_domain_array_index;

SELECT orioledb_table_description('o_test_domain_index'::regclass);
SELECT orioledb_tbl_indices('o_test_domain_index'::regclass);

DROP DOMAIN myint CASCADE;
DROP DOMAIN mytime CASCADE;
DROP DOMAIN mybool CASCADE;

SELECT orioledb_table_description('o_test_domain_index'::regclass);
SELECT orioledb_tbl_indices('o_test_domain_index'::regclass);

SELECT * FROM o_test_domain_index;

-- Check support of custom types indices and alters on that indices
CREATE TYPE record_type_non_altered AS (
	a int,
	b int
);

CREATE TYPE record_type_altered AS (
	a int,
	b int
);

CREATE TABLE o_test_record_type_alter
(
	key record_type_non_altered NOT NULL,
	val int NOT NULL DEFAULT 5
) USING orioledb;

INSERT INTO o_test_record_type_alter
	SELECT (id, id * 2)::record_type_non_altered FROM generate_series(1, 10) id;
SELECT * FROM o_test_record_type_alter;

ALTER TABLE o_test_record_type_alter ADD COLUMN val3 int DEFAULT 18;
ALTER TABLE o_test_record_type_alter ADD COLUMN val4 text DEFAULT 'abc';
SELECT * FROM o_test_record_type_alter;
ALTER TABLE o_test_record_type_alter ALTER COLUMN val3 DROP DEFAULT;
ALTER TABLE o_test_record_type_alter ALTER COLUMN val4 SET DEFAULT 'b';
SELECT * FROM o_test_record_type_alter;
UPDATE o_test_record_type_alter tb SET val3 = 33 WHERE (key).a BETWEEN 5 AND 8;
UPDATE o_test_record_type_alter tb SET val4 = 'c' WHERE (key).a BETWEEN 7 AND 9;
INSERT INTO o_test_record_type_alter (key, val3)
	SELECT (id, id * 2)::record_type_non_altered, id
		FROM generate_series(11, 13) id;
INSERT INTO o_test_record_type_alter (key)
	SELECT (id, id * 2)::record_type_non_altered
		FROM generate_series(14, 15) id;
SELECT * FROM o_test_record_type_alter;

CREATE INDEX o_test_record_type_alter_idx1 ON o_test_record_type_alter (val4);
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_record_type_alter WHERE val4 = 'abc' ORDER BY val4;
SELECT * FROM o_test_record_type_alter WHERE val4 = 'abc' ORDER BY val4;
SELECT orioledb_tbl_structure('o_test_record_type_alter'::regclass, 'ne');
RESET enable_seqscan;

ALTER TABLE o_test_record_type_alter ADD PRIMARY KEY (key);
SELECT * FROM o_test_record_type_alter;

BEGIN;
ALTER TABLE o_test_record_type_alter ADD COLUMN val5 int NOT NULL DEFAULT 12;
ROLLBACK;

SELECT * FROM o_test_record_type_alter;

ALTER TABLE o_test_record_type_alter
	ADD COLUMN val5 record_type_altered NOT NULL
		DEFAULT (1, 5)::record_type_altered;

ALTER TYPE record_type_non_altered DROP ATTRIBUTE b;
ALTER TYPE record_type_non_altered ADD ATTRIBUTE c int;
ALTER TYPE record_type_non_altered ALTER ATTRIBUTE b TYPE text;
ALTER TYPE record_type_non_altered RENAME TO record_type_renamed;

ALTER TYPE record_type_altered RENAME TO record_type_altered_new;
ALTER TYPE record_type_altered_new ADD ATTRIBUTE c int;
ALTER TYPE record_type_altered_new DROP ATTRIBUTE b;
ALTER TYPE record_type_altered_new ALTER ATTRIBUTE c TYPE text;

SELECT * FROM o_test_record_type_alter;
UPDATE o_test_record_type_alter SET val5.b = 4;
UPDATE o_test_record_type_alter t SET val5.c = (t.key).a * 2;
SELECT * FROM o_test_record_type_alter;
SELECT * FROM o_test_record_type_alter WHERE (val5).c % 6 = 0;
SELECT * FROM o_test_record_type_alter WHERE val4 = 'abc';
SELECT orioledb_tbl_structure('o_test_record_type_alter'::regclass, 'ne');

-- ALTER TYPE ADD ATTRIBUTE with a composite type inside an array column
-- covered by a UNIQUE index.
CREATE TYPE o_test_filter AS (col text, op text, val text);
CREATE TABLE o_test_filter_tbl
(
	id bigint PRIMARY KEY,
	sub_id uuid NOT NULL,
	filters o_test_filter[] NOT NULL DEFAULT '{}'
) USING orioledb;
CREATE UNIQUE INDEX o_test_filter_tbl_uq ON o_test_filter_tbl (sub_id, filters);
INSERT INTO o_test_filter_tbl (id, sub_id, filters) VALUES
	(1, '11111111-1111-1111-1111-111111111111',
	 ARRAY[('c','eq','v')::o_test_filter]);
ALTER TYPE o_test_filter ADD ATTRIBUTE negate boolean CASCADE;
SELECT id, filters FROM o_test_filter_tbl ORDER BY id;
INSERT INTO o_test_filter_tbl (id, sub_id, filters) VALUES
	(2, '22222222-2222-2222-2222-222222222222',
	 ARRAY[('c2','eq','v2',true)::o_test_filter]);
SELECT id, filters FROM o_test_filter_tbl ORDER BY id;
INSERT INTO o_test_filter_tbl (id, sub_id, filters) VALUES
	(3, '11111111-1111-1111-1111-111111111111',
	 ARRAY[('c','eq','v',NULL)::o_test_filter]);

-- ALTER TYPE ADD ATTRIBUTE with a composite as the primary key column,
-- mixing pre-ALTER and post-ALTER rows in the same primary tree.
CREATE TYPE o_test_pk AS (a int, b int);
CREATE TABLE o_test_pk_tbl (key o_test_pk PRIMARY KEY, v text) USING orioledb;
INSERT INTO o_test_pk_tbl VALUES
	((1,10)::o_test_pk, 'r1'),
	((2,20)::o_test_pk, 'r2'),
	((3,30)::o_test_pk, 'r3');
ALTER TYPE o_test_pk ADD ATTRIBUTE c int CASCADE;
INSERT INTO o_test_pk_tbl VALUES
	((4,40,400)::o_test_pk, 'r4'),
	((5,50,NULL)::o_test_pk, 'r5');
SELECT key, v FROM o_test_pk_tbl ORDER BY key;
SELECT v FROM o_test_pk_tbl WHERE key = (1,10,NULL)::o_test_pk;
SELECT v FROM o_test_pk_tbl WHERE key = (4,40,400)::o_test_pk;
UPDATE o_test_pk_tbl SET v = 'r1u' WHERE key = (1,10,NULL)::o_test_pk;
SELECT key, v FROM o_test_pk_tbl ORDER BY key;
-- New-shape probe must collide with an existing pre-ALTER row.
INSERT INTO o_test_pk_tbl VALUES ((2,20,NULL)::o_test_pk, 'dup');
SELECT key, v FROM o_test_pk_tbl WHERE key > (2,20,NULL)::o_test_pk ORDER BY key;

-- ALTER TYPE ADD ATTRIBUTE with a bridged (non-orioledb) index over a column
-- referencing the composite type.
CREATE TYPE o_test_br AS (a int, b int);
CREATE TABLE o_test_br_tbl (id int PRIMARY KEY, keys o_test_br[]) USING orioledb;
INSERT INTO o_test_br_tbl SELECT g,
	ARRAY[(g, g*10)::o_test_br, (g+1, g*20)::o_test_br]
	FROM generate_series(1, 3) g;
CREATE INDEX o_test_br_tbl_gin ON o_test_br_tbl USING gin (keys);
ALTER TYPE o_test_br ADD ATTRIBUTE c int CASCADE;
SELECT id, keys FROM o_test_br_tbl ORDER BY id;
SET enable_seqscan = off;
SELECT id FROM o_test_br_tbl
	WHERE keys @> ARRAY[(2, 20, NULL)::o_test_br] ORDER BY id;
INSERT INTO o_test_br_tbl VALUES (100, ARRAY[(2, 20, 999)::o_test_br]);
SELECT id FROM o_test_br_tbl
	WHERE keys @> ARRAY[(2, 20, 999)::o_test_br];
SELECT id FROM o_test_br_tbl
	WHERE keys @> ARRAY[(2, 20, NULL)::o_test_br] ORDER BY id;
RESET enable_seqscan;

-- ALTER TYPE RENAME TO / RENAME ATTRIBUTE with a composite inside an array
-- column covered by a UNIQUE index.
CREATE TYPE o_test_rfilter AS (col text, op text, val text);
CREATE TABLE o_test_rfilter_tbl
(
	id bigint PRIMARY KEY,
	sub_id uuid NOT NULL,
	filters o_test_rfilter[] NOT NULL DEFAULT '{}'
) USING orioledb;
CREATE UNIQUE INDEX o_test_rfilter_tbl_uq
	ON o_test_rfilter_tbl (sub_id, filters);
INSERT INTO o_test_rfilter_tbl (id, sub_id, filters) VALUES
	(1, '11111111-1111-1111-1111-111111111111',
	 ARRAY[('c','eq','v')::o_test_rfilter]);
ALTER TYPE o_test_rfilter RENAME ATTRIBUTE val TO value;
ALTER TYPE o_test_rfilter RENAME TO o_test_rfilter_new;
SELECT id, filters FROM o_test_rfilter_tbl ORDER BY id;
INSERT INTO o_test_rfilter_tbl (id, sub_id, filters) VALUES
	(2, '22222222-2222-2222-2222-222222222222',
	 ARRAY[('c2','eq','v2')::o_test_rfilter_new]);
INSERT INTO o_test_rfilter_tbl (id, sub_id, filters) VALUES
	(3, '11111111-1111-1111-1111-111111111111',
	 ARRAY[('c','eq','v')::o_test_rfilter_new]);
SELECT (f).col, (f).op, (f).value
	FROM o_test_rfilter_tbl, unnest(filters) f ORDER BY id, (f).col;

-- ALTER TYPE RENAME TO / RENAME ATTRIBUTE with a composite as the primary
-- key column.
CREATE TYPE o_test_rpk AS (a int, b int);
CREATE TABLE o_test_rpk_tbl (key o_test_rpk PRIMARY KEY, v text) USING orioledb;
INSERT INTO o_test_rpk_tbl VALUES
	((1,10)::o_test_rpk, 'r1'),
	((2,20)::o_test_rpk, 'r2'),
	((3,30)::o_test_rpk, 'r3');
ALTER TYPE o_test_rpk RENAME ATTRIBUTE b TO b_new;
ALTER TYPE o_test_rpk RENAME TO o_test_rpk_new;
SELECT key, v FROM o_test_rpk_tbl ORDER BY key;
SELECT v FROM o_test_rpk_tbl WHERE key = (2,20)::o_test_rpk_new;
INSERT INTO o_test_rpk_tbl VALUES ((4,40)::o_test_rpk_new, 'r4');
INSERT INTO o_test_rpk_tbl VALUES ((1,10)::o_test_rpk_new, 'dup');
SELECT (key).a, (key).b_new, v FROM o_test_rpk_tbl ORDER BY (key).a;

-- ALTER TYPE RENAME TO / RENAME ATTRIBUTE with a bridged (non-orioledb)
-- index over a column referencing the composite type.
CREATE TYPE o_test_rbr AS (a int, b int);
CREATE TABLE o_test_rbr_tbl (id int PRIMARY KEY, keys o_test_rbr[]) USING orioledb;
INSERT INTO o_test_rbr_tbl SELECT g,
	ARRAY[(g, g*10)::o_test_rbr, (g+1, g*20)::o_test_rbr]
	FROM generate_series(1, 3) g;
CREATE INDEX o_test_rbr_tbl_gin ON o_test_rbr_tbl USING gin (keys);
ALTER TYPE o_test_rbr RENAME ATTRIBUTE b TO b_new;
ALTER TYPE o_test_rbr RENAME TO o_test_rbr_new;
SELECT id, keys FROM o_test_rbr_tbl ORDER BY id;
SET enable_seqscan = off;
SELECT id FROM o_test_rbr_tbl
	WHERE keys @> ARRAY[(2, 20)::o_test_rbr_new] ORDER BY id;
INSERT INTO o_test_rbr_tbl VALUES
	(100, ARRAY[(4, 40)::o_test_rbr_new]);
SELECT id FROM o_test_rbr_tbl
	WHERE keys @> ARRAY[(4, 40)::o_test_rbr_new];
RESET enable_seqscan;

-- Test missing with fixed format
CREATE TABLE o_test_record_type_alter2
(
	key int NOT NULL,
	val int NOT NULL DEFAULT 5
) USING orioledb;

INSERT INTO o_test_record_type_alter2
	SELECT id FROM generate_series(1, 10) id;
SELECT * FROM o_test_record_type_alter2;

SELECT orioledb_tbl_structure('o_test_record_type_alter2'::regclass, 'ne');
ALTER TABLE o_test_record_type_alter2 ADD COLUMN val3 int NOT NULL DEFAULT 18;
ALTER TABLE o_test_record_type_alter2 ADD COLUMN val4 text DEFAULT 'abc';
SELECT * FROM o_test_record_type_alter2;
SELECT orioledb_tbl_structure('o_test_record_type_alter2'::regclass, 'ne');
ALTER TABLE o_test_record_type_alter2 ALTER COLUMN val3 DROP DEFAULT;
ALTER TABLE o_test_record_type_alter2 ALTER COLUMN val4 SET DEFAULT 'b';
SELECT * FROM o_test_record_type_alter2;
SELECT orioledb_tbl_structure('o_test_record_type_alter2'::regclass, 'ne');
UPDATE o_test_record_type_alter2 tb SET val3 = 33 WHERE key BETWEEN 6 AND 8;
UPDATE o_test_record_type_alter2 tb SET val4 = 'c' WHERE key BETWEEN 7 AND 9;
SELECT orioledb_tbl_structure('o_test_record_type_alter2'::regclass, 'ne');
SELECT * FROM o_test_record_type_alter2;
INSERT INTO o_test_record_type_alter2 (key, val3)
	SELECT id, id FROM generate_series(11, 15) id;
SELECT * FROM o_test_record_type_alter2;

CREATE DOMAIN o_test_domain_1 AS TEXT NOT NULL;
CREATE table o_test_domain_check (
    val_1 int,
    val_2 int
)USING orioledb;
INSERT INTO o_test_domain_check VALUES (1, 2);
ALTER TABLE o_test_domain_check ADD COLUMN val_3 o_test_domain_1;
CREATE DOMAIN o_test_domain_2 AS text CHECK (VALUE <> 'foo') DEFAULT 'foo';
ALTER TABLE o_test_domain_check ADD COLUMN val_4 o_test_domain_2;

BEGIN;

CREATE DOMAIN myint_domain_rollback AS integer CHECK (VALUE > 5);
CREATE DOMAIN myarray_domain_rollback AS myint_domain_rollback[];

CREATE TABLE o_test_domain_rollback (
	val_1 myarray_domain_rollback,
	val_2 int NOT NULL,
	val_3 inet,
	PRIMARY KEY(val_1, val_2, val_3)
)USING orioledb;

INSERT INTO o_test_domain_rollback VALUES ('{10}', 10, '10.10.10.10');
SELECT * FROM o_test_domain_rollback;
ROLLBACK;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA types CASCADE;
RESET search_path;
