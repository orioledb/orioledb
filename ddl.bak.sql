CREATE SCHEMA ddl;
SET SESSION search_path = 'ddl';
CREATE EXTENSION orioledb;


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
SET log_error_verbosity = 'terse';
SELECT * FROM o_test_ioc1 WHERE id1 = 3 FOR UPDATE;
RESET log_error_verbosity;
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
COMMIT;

SET log_error_verbosity = 'terse';
INSERT INTO o_test_ioc1 VALUES (3, 12) ON CONFLICT (id1) DO UPDATE SET id2 = EXCLUDED.id2 * 10;
RESET log_error_verbosity;
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

INSERT INTO o_test_ioc1 VALUES (2, 11);
SELECT orioledb_tbl_structure('o_test_ioc1'::regclass);
SELECT * FROM o_test_ioc1 ORDER BY id1;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA ddl CASCADE;
RESET search_path;

\q

CREATE OR REPLACE FUNCTION int4range_immutable(input_str text)
  RETURNS int4range
AS
$$
    select int4range(input_str);
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION int4range_overlaps(a int4range, b int4range)
  RETURNS boolean
AS
$$
    select lower(a) >= lower(b) OR upper(a) <= upper(b);
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
	ADD EXCLUDE USING btree (c1 WITH <->, (int4range_immutable(c2)) WITH <->)
  WITH (orioledb_index=false)
  WHERE (NOT c1 @> 0::int4);

-- these should succeed because they don't match the index predicate
INSERT INTO ranges VALUES(int4range(-1, 3), '[-2, 2]');
INSERT INTO ranges VALUES(int4range(-6, 2), '[-21, 14]');

-- succeed
INSERT INTO ranges VALUES(int4range(-5, -2), '[2, 5)');
SELECT * FROM ranges;
-- fail, overlaps
INSERT INTO ranges VALUES(int4range(-6, -3), '[2, 5)');
SELECT * FROM ranges;
SELECT orioledb_tbl_structure('ranges'::regclass);

SET log_error_verbosity = 'terse';
-- succeed, because violation is ignored
INSERT INTO ranges VALUES(int4range(-6, -3), '[2, 5)')
  ON CONFLICT ON CONSTRAINT ranges_c1_int4range_immutable_excl DO NOTHING RETURNING *;
SELECT * FROM ranges;
SELECT orioledb_tbl_structure('ranges'::regclass);
\q
SELECT * FROM ranges;
SELECT orioledb_tbl_structure('ranges'::regclass);

-- fail, because DO UPDATE variant requires unique index
INSERT INTO ranges VALUES('<(20,20), 10>', '<(0,0), 4>')
  ON CONFLICT ON CONSTRAINT ranges_c1_int4range_immutable_excl DO UPDATE SET c2 = EXCLUDED.c2;
SELECT * FROM ranges;

SELECT orioledb_tbl_structure('ranges'::regclass);
\q

-- -- succeed because c1 doesn't overlap
-- INSERT INTO ranges VALUES('<(20,20), 1>', '<(0,0), 5>');
-- -- succeed because c2 doesn't overlap
-- INSERT INTO ranges VALUES('<(20,20), 10>', '<(10,10), 5>');

-- SELECT orioledb_tbl_structure('ranges'::regclass);
-- SELECT * FROM ranges;

-- -- CREATE INDEX ranges_ix2 ON ranges USING gist (c1, (c2::circle));
-- ALTER TABLE ranges ADD EXCLUDE USING gist (c1 WITH &&, (c2::circle) WITH &&);

-- -- try reindexing an existing constraint
-- REINDEX INDEX ranges_c1_c2_excl;
-- REINDEX INDEX ranges_ix1;

-- DROP TABLE ranges;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA ddl CASCADE;
RESET search_path;
