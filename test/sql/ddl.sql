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
  c2 TEXT
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
SET log_error_verbosity = 'terse';
INSERT INTO ranges VALUES(int4range(-6, -3), '[2, 5)');
SELECT * FROM ranges;
\q

-- succeed, because violation is ignored
SET log_error_verbosity = 'terse';
INSERT INTO ranges VALUES(int4range(-6, -3), '[2, 5)')
  ON CONFLICT ON CONSTRAINT ranges_c1_orioledb_int4range_immutable_excl DO NOTHING RETURNING *;
\q

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

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA ddl CASCADE;
RESET search_path;
