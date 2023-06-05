CREATE SCHEMA nulls_schema;
SET SESSION search_path = 'nulls_schema';
CREATE EXTENSION orioledb;

-- Test null clauses in index scan
create table o_test_null_clauses (
	id bigint PRIMARY KEY,
	val int
) USING orioledb;
CREATE INDEX ON o_test_null_clauses(val);
INSERT INTO o_test_null_clauses
	SELECT id, id + 2 val FROM generate_series(1, 10) id;
INSERT INTO o_test_null_clauses VALUES (11, NULL), (12, NULL);
SET enable_bitmapscan = off;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses ORDER BY val NULLS FIRST;
SELECT * FROM o_test_null_clauses ORDER BY val NULLS FIRST;
-- Test primary index scan with null clause
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses
	WHERE val IS NULL ORDER BY val;
SELECT * FROM o_test_null_clauses WHERE val IS NULL ORDER BY val;
-- Test primary index scan with not null clause
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL ORDER BY val;
SELECT * FROM o_test_null_clauses WHERE val IS NOT NULL ORDER BY val;
-- Test primary index scan with not null and another clause
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL AND val > 5 ORDER BY val;
SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL AND val > 5 ORDER BY val;
-- Test primary index scan with not null and another clause and nullsfirst
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL AND val > 5 ORDER BY val NULLS FIRST;
SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL AND val > 5 ORDER BY val NULLS FIRST;
RESET enable_seqscan;
RESET enable_bitmapscan;
-- Test primary index scan with null array clause
EXPLAIN (COSTS off) select id from o_test_null_clauses
	WHERE id = ANY(NULL::int4[]);
select id from o_test_null_clauses WHERE id = ANY(NULL::int4[]);
-- Test nested primary index scan with not null clause
EXPLAIN (COSTS off) select max(id) from o_test_null_clauses;
select max(id) from o_test_null_clauses;

CREATE TABLE o_test_null_comparison (
	a int
) USING orioledb;

INSERT INTO o_test_null_comparison VALUES (1);
INSERT INTO o_test_null_comparison VALUES (NULL);

CREATE INDEX o_test_null_comparison_ix1 ON o_test_null_comparison (a);

BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_null_comparison ORDER BY a;
SELECT * FROM o_test_null_comparison ORDER BY a;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_null_comparison WHERE a > 1 ORDER BY a;
SELECT * FROM o_test_null_comparison WHERE a > 1 ORDER BY a;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_null_comparison WHERE a < 1 ORDER BY a;
SELECT * FROM o_test_null_comparison WHERE a < 1 ORDER BY a;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_null_comparison WHERE a IS NULL ORDER BY a;
SELECT * FROM o_test_null_comparison WHERE a IS NULL ORDER BY a;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_null_comparison WHERE a IS NOT NULL ORDER BY a;
SELECT * FROM o_test_null_comparison WHERE a IS NOT NULL ORDER BY a;
COMMIT;

CREATE TABLE o_test_null_row_comparison (
   a int,
   b int
) USING orioledb;

INSERT INTO o_test_null_row_comparison VALUES (1, 2);
INSERT INTO o_test_null_row_comparison VALUES (1, NULL);

CREATE INDEX ON o_test_null_row_comparison (a,b);

BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT a,b FROM o_test_null_row_comparison WHERE (a,b) > (1,1) ORDER BY a,b;
COMMIT;

CREATE TABLE o_test_row_comparison_nulls_first (
   a int,
   b int
) USING orioledb;

INSERT INTO o_test_row_comparison_nulls_first VALUES (1, 2);
INSERT INTO o_test_row_comparison_nulls_first VALUES (1, NULL);

CREATE INDEX ON o_test_row_comparison_nulls_first (a, b NULLS FIRST);

BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT a,b FROM o_test_row_comparison_nulls_first
	WHERE (a,b) < (1,5) ORDER BY a,b;
COMMIT;

CREATE TABLE o_test_nulls
(
  id bigint NOT NULL,
  value bigint,
  PRIMARY KEY (id)
) USING orioledb;

INSERT INTO o_test_nulls (SELECT i, i FROM generate_series(1,10000) i);
INSERT INTO o_test_nulls (SELECT i, NULL FROM generate_series(10001,11000) i);

CREATE INDEX o_test_nulls_value_idx ON o_test_nulls(value NULLS FIRST);
SELECT count(*) FROM o_test_nulls WHERE value IS NULL;
SELECT count(*) FROM o_test_nulls WHERE value IS NOT NULL;
DROP INDEX o_test_nulls_value_idx;

CREATE INDEX o_test_nulls_value_idx ON o_test_nulls(value NULLS LAST);
SELECT count(*) FROM o_test_nulls WHERE value IS NULL;
SELECT count(*) FROM o_test_nulls WHERE value IS NOT NULL;
DROP INDEX o_test_nulls_value_idx;

CREATE INDEX o_test_nulls_value_idx ON o_test_nulls(value ASC NULLS FIRST);
SELECT count(*) FROM o_test_nulls WHERE value IS NULL;
SELECT count(*) FROM o_test_nulls WHERE value IS NOT NULL;
DROP INDEX o_test_nulls_value_idx;

CREATE INDEX o_test_nulls_value_idx ON o_test_nulls(value ASC NULLS LAST);
SELECT count(*) FROM o_test_nulls WHERE value IS NULL;
SELECT count(*) FROM o_test_nulls WHERE value IS NOT NULL;
DROP INDEX o_test_nulls_value_idx;

CREATE INDEX o_test_nulls_value_idx ON o_test_nulls(value DESC NULLS FIRST);
SELECT count(*) FROM o_test_nulls WHERE value IS NULL;
SELECT count(*) FROM o_test_nulls WHERE value IS NOT NULL;
DROP INDEX o_test_nulls_value_idx;

CREATE INDEX o_test_nulls_value_idx ON o_test_nulls(value DESC NULLS LAST);
SELECT count(*) FROM o_test_nulls WHERE value IS NULL;
SELECT count(*) FROM o_test_nulls WHERE value IS NOT NULL;
DROP INDEX o_test_nulls_value_idx;

CREATE TABLE o_test_unique_nulls_not_distinct (
	val_1 int UNIQUE NULLS NOT DISTINCT,
	val_2 text
) USING orioledb;

TABLE o_test_unique_nulls_not_distinct;
INSERT INTO o_test_unique_nulls_not_distinct(val_2) VALUES ('six');
INSERT INTO o_test_unique_nulls_not_distinct(val_2) VALUES ('seven');
TABLE o_test_unique_nulls_not_distinct;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA nulls_schema CASCADE;
RESET search_path;
