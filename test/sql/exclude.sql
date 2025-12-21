CREATE SCHEMA exclude;
SET SESSION search_path = 'exclude';
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
UPDATE o_test_is_null_assert SET val2 = val2 WHERE key = 2;
UPDATE o_test_is_null_assert SET val2 = val2 WHERE key = 2;
COMMIT;

CREATE TABLE o_test_ioc1
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL PRIMARY KEY,
	UNIQUE (id1) WITH (orioledb_index=true)
) USING orioledb;

INSERT INTO o_test_ioc1 VALUES (7, 20);
SELECT * FROM o_test_ioc1 ORDER BY id1;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_ioc1 ORDER BY id1;
SELECT * FROM o_test_ioc1 ORDER BY id1;
COMMIT;

DELETE FROM o_test_ioc1 WHERE id2 = 20;
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (6, 19);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (5, 18);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (4, 17);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (3, 16);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (3, 15);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (3, 14);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (3, 13) ON CONFLICT (id1) DO NOTHING;
SELECT * FROM o_test_ioc1 ORDER BY id1;

BEGIN;
SELECT * FROM o_test_ioc1 WHERE id1 = 3 FOR UPDATE;
COMMIT;

INSERT INTO o_test_ioc1 VALUES (3, 12) ON CONFLICT (id1) DO UPDATE SET id2 = EXCLUDED.id2 * 10;
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (2, 11);
SELECT * FROM o_test_ioc1 ORDER BY id1;

CREATE TABLE ranges0 (
  t1 char(1),
  c1 int4,
  c2 text PRIMARY KEY
) USING orioledb;

CREATE UNIQUE INDEX ranges0_ix1 ON ranges0(c1) WITH (orioledb_index=true);

-- these should succeed because they don't match the index predicate
INSERT INTO ranges0 VALUES('A', -1, '-3');
INSERT INTO ranges0 VALUES('B', -6, '12');

-- succeed
INSERT INTO ranges0 VALUES('C', -5, '55');
SELECT * FROM ranges0;

-- fail, overlaps
INSERT INTO ranges0 VALUES('D', -5, '62');
SELECT * FROM ranges0;

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
  WITH (orioledb_index=true)
  WHERE (NOT c1 @> 0::int4);
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
	i int,
	t text,
	UNIQUE (i) WITH (orioledb_index=true) DEFERRABLE
) USING orioledb;

INSERT INTO unique_tbl VALUES (0, 'one');
INSERT INTO unique_tbl VALUES (1, 'two');
INSERT INTO unique_tbl VALUES (2, 'tree');
INSERT INTO unique_tbl VALUES (3, 'four');
INSERT INTO unique_tbl VALUES (4, 'five');

BEGIN;

-- default is immediate so this should fail right away
UPDATE unique_tbl SET i = 1 WHERE i = 0;

ROLLBACK;

-- check is done at end of statement, so this should succeed
UPDATE unique_tbl SET i = i+1;

SELECT * FROM unique_tbl;

-- explicitly defer the constraint
BEGIN;

SET CONSTRAINTS unique_tbl_i_key DEFERRED;

INSERT INTO unique_tbl VALUES (3, 'three');
SELECT * FROM unique_tbl;
DELETE FROM unique_tbl WHERE t = 'tree'; -- makes constraint valid again
SELECT * FROM unique_tbl;

COMMIT; -- should succeed

SELECT * FROM unique_tbl;

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
COMMIT; -- should fail

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

CREATE TABLE deferred_excl (
  f1 int,
  f2 int,
  CONSTRAINT deferred_excl_con EXCLUDE (f1 WITH =) WITH (orioledb_index=true) INITIALLY DEFERRED
) USING orioledb;
INSERT INTO deferred_excl VALUES(1);
INSERT INTO deferred_excl VALUES(2);
INSERT INTO deferred_excl VALUES(1); -- fail
INSERT INTO deferred_excl VALUES(1) ON CONFLICT ON CONSTRAINT deferred_excl_con DO NOTHING; -- fail
BEGIN;
INSERT INTO deferred_excl VALUES(2); -- no fail here
COMMIT; -- should fail here
BEGIN;
INSERT INTO deferred_excl VALUES(3);
INSERT INTO deferred_excl VALUES(3); -- no fail here
COMMIT; -- should fail here
-- bug #13148: deferred constraint versus HOT update
BEGIN;
INSERT INTO deferred_excl VALUES(2, 1); -- no fail here
DELETE FROM deferred_excl WHERE f1 = 2 AND f2 IS NULL; -- remove old row
UPDATE deferred_excl SET f2 = 2 WHERE f1 = 2;
COMMIT; -- should not fail
SELECT * FROM deferred_excl;
 
CREATE TABLE o_test_ib_ioc1
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL PRIMARY KEY,
	UNIQUE (id1) WITH (orioledb_index=false)
) USING orioledb;

INSERT INTO o_test_ib_ioc1 VALUES (7, 20);
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_ib_ioc1 ORDER BY id1;
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;
COMMIT;

DELETE FROM o_test_ib_ioc1 WHERE id2 = 20;
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

INSERT INTO o_test_ib_ioc1 VALUES (6, 19);
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

INSERT INTO o_test_ib_ioc1 VALUES (5, 18);
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

INSERT INTO o_test_ib_ioc1 VALUES (4, 17);
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

INSERT INTO o_test_ib_ioc1 VALUES (3, 16);
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

INSERT INTO o_test_ib_ioc1 VALUES (3, 15);
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

INSERT INTO o_test_ib_ioc1 VALUES (3, 14);
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

INSERT INTO o_test_ib_ioc1 VALUES (3, 13) ON CONFLICT (id1) DO NOTHING;
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

BEGIN;
SELECT * FROM o_test_ib_ioc1 WHERE id1 = 3 FOR UPDATE;
COMMIT;

INSERT INTO o_test_ib_ioc1 VALUES (3, 12) ON CONFLICT (id1) DO UPDATE SET id2 = EXCLUDED.id2 * 10;
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

INSERT INTO o_test_ib_ioc1 VALUES (2, 11);
SELECT * FROM o_test_ib_ioc1 ORDER BY id1;

CREATE TABLE ib_ranges0 (
  t1 char(1),
  c1 int4,
  c2 text PRIMARY KEY
) USING orioledb;

CREATE UNIQUE INDEX ib_ranges0_ix1 ON ib_ranges0(c1) WITH (orioledb_index=false);

-- these should succeed because they don't match the index predicate
INSERT INTO ib_ranges0 VALUES('A', -1, '-3');
INSERT INTO ib_ranges0 VALUES('B', -6, '12');

-- succeed
INSERT INTO ib_ranges0 VALUES('C', -5, '55');
SELECT * FROM ib_ranges0;

-- fail, overlaps
INSERT INTO ib_ranges0 VALUES('D', -5, '62');
SELECT * FROM ib_ranges0;

CREATE TABLE ib_ranges (
  c1 int4range,
  c2 TEXT -- ,
  -- i1 serial,
  -- i2 serial,
  -- PRIMARY KEY (i1, i2)
) USING orioledb;

ALTER TABLE ib_ranges
	ADD EXCLUDE USING btree (c1 WITH <->, (orioledb_int4range_immutable(c2)) WITH <->)
  WITH (orioledb_index=false)
  WHERE (NOT c1 @> 0::int4);
\d+ ib_ranges

-- these should succeed because they don't match the index predicate
INSERT INTO ib_ranges VALUES(int4range(-1, 3), '[-2, 2]');
INSERT INTO ib_ranges VALUES(int4range(-6, 2), '[-21, 14]');

-- succeed
INSERT INTO ib_ranges VALUES(int4range(-5, -2), '[2, 5)');
SELECT * FROM ib_ranges;
-- fail, duplicate
INSERT INTO ib_ranges VALUES(int4range(-5, -2), '[2, 5)');
SELECT * FROM ib_ranges;
-- fail, overlaps
INSERT INTO ib_ranges VALUES(int4range(-6, -3), '[2, 5)');
SELECT * FROM ib_ranges;

-- succeed, because violation is ignored
INSERT INTO ib_ranges VALUES(int4range(-6, -3), '[2, 5)')
  ON CONFLICT ON CONSTRAINT ib_ranges_c1_orioledb_int4range_immutable_excl DO NOTHING RETURNING *;

SELECT * FROM ib_ranges;

SELECT * FROM ib_ranges;

-- fail, because DO UPDATE variant requires unique index
INSERT INTO ib_ranges VALUES(int4range(-6, -3), '[2, 5)')
  ON CONFLICT ON CONSTRAINT ib_ranges_c1_orioledb_int4range_immutable_excl DO UPDATE SET c2 = EXCLUDED.c2;
SELECT * FROM ib_ranges;

-- succeed because c1 doesn't overlap
INSERT INTO ib_ranges VALUES(int4range(-16, -13), '[2, 5)');
SELECT * FROM ib_ranges;
-- succeed because c2 doesn't overlap
INSERT INTO ib_ranges VALUES(int4range(-4, -3), '[12, 15)');
SELECT * FROM ib_ranges;

CREATE TABLE ib_unique_tbl (
	i int,
	t text,
	UNIQUE (i) WITH (orioledb_index=false) DEFERRABLE
) USING orioledb;

INSERT INTO ib_unique_tbl VALUES (0, 'one');
INSERT INTO ib_unique_tbl VALUES (1, 'two');
INSERT INTO ib_unique_tbl VALUES (2, 'tree');
INSERT INTO ib_unique_tbl VALUES (3, 'four');
INSERT INTO ib_unique_tbl VALUES (4, 'five');

BEGIN;

-- default is immediate so this should fail right away
UPDATE ib_unique_tbl SET i = 1 WHERE i = 0;

ROLLBACK;

-- check is done at end of statement, so this should succeed
UPDATE ib_unique_tbl SET i = i+1;

SELECT * FROM ib_unique_tbl;

-- explicitly defer the constraint
BEGIN;

SET CONSTRAINTS ib_unique_tbl_i_key DEFERRED;

INSERT INTO ib_unique_tbl VALUES (3, 'three');
SELECT * FROM ib_unique_tbl;
DELETE FROM ib_unique_tbl WHERE t = 'tree'; -- makes constraint valid again
SELECT * FROM ib_unique_tbl;

COMMIT; -- should succeed

SELECT * FROM ib_unique_tbl;

-- try adding an initially deferred constraint
ALTER TABLE ib_unique_tbl DROP CONSTRAINT ib_unique_tbl_i_key;
ALTER TABLE ib_unique_tbl ADD CONSTRAINT ib_unique_tbl_i_key
	UNIQUE (i) DEFERRABLE INITIALLY DEFERRED;

BEGIN;

INSERT INTO ib_unique_tbl VALUES (1, 'five');
INSERT INTO ib_unique_tbl VALUES (5, 'one');
UPDATE ib_unique_tbl SET i = 4 WHERE i = 2;
UPDATE ib_unique_tbl SET i = 2 WHERE i = 4 AND t = 'four';
DELETE FROM ib_unique_tbl WHERE i = 1 AND t = 'one';
DELETE FROM ib_unique_tbl WHERE i = 5 AND t = 'five';

COMMIT;

SELECT * FROM ib_unique_tbl;

-- should fail at commit-time
BEGIN;
INSERT INTO ib_unique_tbl VALUES (3, 'Three'); -- should succeed for now
COMMIT; -- should fail

-- make constraint check immediate
BEGIN;

SET CONSTRAINTS ALL IMMEDIATE;

INSERT INTO ib_unique_tbl VALUES (3, 'Three'); -- should fail

COMMIT;

-- forced check when SET CONSTRAINTS is called
BEGIN;

SET CONSTRAINTS ALL DEFERRED;

INSERT INTO ib_unique_tbl VALUES (3, 'Three'); -- should succeed for now

SET CONSTRAINTS ALL IMMEDIATE; -- should fail

COMMIT;

CREATE TABLE ib_deferred_excl (
  f1 int,
  f2 int,
  CONSTRAINT ib_deferred_excl_con EXCLUDE (f1 WITH =) WITH (orioledb_index=false) INITIALLY DEFERRED
) USING orioledb;
INSERT INTO ib_deferred_excl VALUES(1);
INSERT INTO ib_deferred_excl VALUES(2);
INSERT INTO ib_deferred_excl VALUES(1); -- fail
INSERT INTO ib_deferred_excl VALUES(1) ON CONFLICT ON CONSTRAINT ib_deferred_excl_con DO NOTHING; -- fail
BEGIN;
INSERT INTO ib_deferred_excl VALUES(2); -- no fail here
COMMIT; -- should fail here
BEGIN;
INSERT INTO ib_deferred_excl VALUES(3);
INSERT INTO ib_deferred_excl VALUES(3); -- no fail here
COMMIT; -- should fail here
-- bug #13148: deferred constraint versus HOT update
BEGIN;
SET LOCAL log_error_verbosity = 'terse';
INSERT INTO ib_deferred_excl VALUES(2, 1); -- no fail here
DELETE FROM ib_deferred_excl WHERE f1 = 2 AND f2 IS NULL; -- remove old row
UPDATE ib_deferred_excl SET f2 = 2 WHERE f1 = 2;
COMMIT; -- should not fail

SELECT * FROM ib_deferred_excl;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA exclude CASCADE;
RESET search_path;
