CREATE SCHEMA ddl;
SET SESSION search_path = 'ddl';
CREATE EXTENSION orioledb;


CREATE TABLE o_test_is_null_assert (
	key int not null,
	val int,
	val2 int,
	filler char(84)
) USING orioledb;
ALTER TABLE o_test_is_null_assert ADD PRIMARY KEY (key);
INSERT INTO o_test_is_null_assert (key,val,val2) VALUES (2, 0, 0);
BEGIN;
SELECT * FROM o_test_is_null_assert WHERE key = 2 ORDER BY key;
SELECT orioledb_tbl_structure('o_test_is_null_assert'::regclass, 'nue');
UPDATE o_test_is_null_assert SET val2 = val2 WHERE key = 2;
UPDATE o_test_is_null_assert SET val2 = val2 WHERE key = 2;
SELECT orioledb_tbl_structure('o_test_is_null_assert'::regclass, 'nue');
COMMIT;

CREATE TABLE o_test_ioc1
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL PRIMARY KEY,
	UNIQUE (id1) WITH (orioledb_index=false)
) USING orioledb;

INSERT INTO o_test_ioc1 VALUES (7, 20);
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_ioc1 ORDER BY id1;
SELECT * FROM o_test_ioc1 ORDER BY id1;
COMMIT;

DELETE FROM o_test_ioc1 WHERE id2 = 20;
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (6, 19);
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (5, 18);
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (4, 17);
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (3, 16);
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (3, 15);
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (3, 14);
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (3, 13) ON CONFLICT (id1) DO NOTHING;
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

BEGIN;
SELECT * FROM o_test_ioc1 WHERE id1 = 3 FOR UPDATE;
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
COMMIT;

INSERT INTO o_test_ioc1 VALUES (3, 12) ON CONFLICT (id1) DO UPDATE SET id2 = EXCLUDED.id2 * 10;
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (2, 11);
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

CREATE TABLE ranges0 (
  t1 char(1),
  c1 int4,
  c2 text PRIMARY KEY
) USING orioledb;

CREATE UNIQUE INDEX ranges0_ix1 ON ranges0(c1) WITH (orioledb_index=false);

-- these should succeed because they don't match the index predicate
INSERT INTO ranges0 VALUES('A', -1, '-3');
INSERT INTO ranges0 VALUES('B', -6, '12');

-- succeed
INSERT INTO ranges0 VALUES('C', -5, '55');
SELECT * FROM ranges0;
SELECT orioledb_tbl_structure('ranges0'::regclass);

-- fail, overlaps
INSERT INTO ranges0 VALUES('D', -5, '62');
SELECT * FROM ranges0;
SELECT orioledb_tbl_structure('ranges0'::regclass);

CREATE OR REPLACE FUNCTION int4range_overlaps(a int4range, b int4range)
  RETURNS boolean
AS
$$
    select a && b;
$$ LANGUAGE sql IMMUTABLE;

CREATE OPERATOR <-> (
	LEFTARG = int4range,
	RIGHTARG = int4range,
	PROCEDURE = int4range_overlaps,
	COMMUTATOR = '<->'
);

ALTER OPERATOR FAMILY range_ops USING btree ADD OPERATOR 3 <->(int4range, int4range);

CREATE TABLE ranges (
  c1 int4range,
  c2 TEXT -- ,
  -- i1 serial,
  -- i2 serial,
  -- PRIMARY KEY (i1, i2)
) USING orioledb;

ALTER TABLE ranges
	ADD EXCLUDE USING btree (c1 WITH <->, (orioledb_int4range_immutable(c2)) WITH <->)
  WITH (orioledb_index=false)
  WHERE (NOT c1 @> 0::int4);
-- CREATE UNIQUE INDEX ON ranges (c1, orioledb_int4range_immutable(c2))
--   WITH (orioledb_index=false)
--   WHERE (NOT c1 @> 0::int4);
\d+ ranges

-- these should succeed because they don't match the index predicate
INSERT INTO ranges VALUES(int4range(-1, 3), '[-2, 2]');
INSERT INTO ranges VALUES(int4range(-6, 2), '[-21, 14]');

-- succeed
INSERT INTO ranges VALUES(int4range(-5, -2), '[2, 5)');
SELECT * FROM ranges;
-- fail, duplicate
INSERT INTO ranges VALUES(int4range(-5, -2), '[2, 5)');
SELECT * FROM ranges;
-- fail, overlaps
INSERT INTO ranges VALUES(int4range(-6, -3), '[2, 5)');
SELECT * FROM ranges;

-- succeed, because violation is ignored
INSERT INTO ranges VALUES(int4range(-6, -3), '[2, 5)')
  ON CONFLICT ON CONSTRAINT ranges_c1_orioledb_int4range_immutable_excl DO NOTHING RETURNING *;

SELECT * FROM ranges;

SELECT * FROM ranges;

-- fail, because DO UPDATE variant requires unique index
INSERT INTO ranges VALUES(int4range(-6, -3), '[2, 5)')
  ON CONFLICT ON CONSTRAINT ranges_c1_orioledb_int4range_immutable_excl DO UPDATE SET c2 = EXCLUDED.c2;
SELECT * FROM ranges;

-- succeed because c1 doesn't overlap
INSERT INTO ranges VALUES(int4range(-16, -13), '[2, 5)');
SELECT * FROM ranges;
-- succeed because c2 doesn't overlap
INSERT INTO ranges VALUES(int4range(-4, -3), '[12, 15)');
SELECT * FROM ranges;

CREATE TABLE unique_tbl (
	i int UNIQUE DEFERRABLE, 
	t text
) USING orioledb;

INSERT INTO unique_tbl VALUES (0, 'one');
INSERT INTO unique_tbl VALUES (1, 'two');
INSERT INTO unique_tbl VALUES (2, 'tree');
INSERT INTO unique_tbl VALUES (3, 'four');
INSERT INTO unique_tbl VALUES (4, 'five');

SELECT orioledb_tbl_structure('unique_tbl'::regclass);
BEGIN;

-- check is done at end of transaction, so this should succeed
UPDATE unique_tbl SET i = 1 WHERE i = 0;

ROLLBACK;

SELECT orioledb_tbl_structure('unique_tbl'::regclass);

-- check is done at end of statement, so this should succeed
-- UPDATE unique_tbl SET i = i+1;
UPDATE unique_tbl SET i = i+2+(3-i/2*3);
SELECT orioledb_tbl_structure('unique_tbl'::regclass);

SELECT * FROM unique_tbl;
\q

-- explicitly defer the constraint
BEGIN;

SET CONSTRAINTS unique_tbl_i_key DEFERRED;

SELECT orioledb_tbl_structure('unique_tbl'::regclass);
INSERT INTO unique_tbl VALUES (3, 'three');
SELECT orioledb_tbl_structure('unique_tbl'::regclass);
DELETE FROM unique_tbl WHERE t = 'tree'; -- makes constraint valid again
SELECT orioledb_tbl_structure('unique_tbl'::regclass);

COMMIT; -- should succeed

SELECT * FROM unique_tbl;
\q

-- try adding an initially deferred constraint
ALTER TABLE unique_tbl DROP CONSTRAINT unique_tbl_i_key;
ALTER TABLE unique_tbl ADD CONSTRAINT unique_tbl_i_key
	UNIQUE (i) DEFERRABLE INITIALLY DEFERRED;

BEGIN;

INSERT INTO unique_tbl VALUES (1, 'five');
INSERT INTO unique_tbl VALUES (5, 'one');
UPDATE unique_tbl SET i = 4 WHERE i = 2;
UPDATE unique_tbl SET i = 2 WHERE i = 4 AND t = 'four';
DELETE FROM unique_tbl WHERE i = 1 AND t = 'one';
DELETE FROM unique_tbl WHERE i = 5 AND t = 'five';

COMMIT;

SELECT * FROM unique_tbl;

-- should fail at commit-time
BEGIN;
INSERT INTO unique_tbl VALUES (3, 'Three'); -- should succeed for now
-- SET log_error_verbosity = 'terse';
COMMIT; -- should fail
\q

-- make constraint check immediate
BEGIN;

SET CONSTRAINTS ALL IMMEDIATE;

INSERT INTO unique_tbl VALUES (3, 'Three'); -- should fail

COMMIT;

-- forced check when SET CONSTRAINTS is called
BEGIN;

SET CONSTRAINTS ALL DEFERRED;

INSERT INTO unique_tbl VALUES (3, 'Three'); -- should succeed for now

SET CONSTRAINTS ALL IMMEDIATE; -- should fail

COMMIT;


DROP EXTENSION orioledb CASCADE;
DROP SCHEMA ddl CASCADE;
RESET search_path;
