CREATE SCHEMA temp_schema;
SET SESSION search_path = 'temp_schema';
CREATE EXTENSION orioledb;

CREATE TEMP TABLE o_test_temp_indices (
	val_1 int,
	val_2 int,
	val_3 int
) USING orioledb;

CREATE INDEX o_test_temp_indices_ix1 ON o_test_temp_indices(val_2);

INSERT INTO o_test_temp_indices
	SELECT val, val * 10, val * 100 FROM generate_series (1, 5) val;

SELECT orioledb_tbl_structure('o_test_temp_indices'::regclass, 'nue');

CREATE INDEX o_test_temp_indices_ix2 ON o_test_temp_indices(val_3);

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_temp_indices ORDER BY val_2;
SELECT * FROM o_test_temp_indices ORDER BY val_2;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_temp_indices ORDER BY val_3;
SELECT * FROM o_test_temp_indices ORDER BY val_3;
COMMIT;

SELECT orioledb_tbl_structure('o_test_temp_indices'::regclass, 'nue');

UPDATE o_test_temp_indices SET val_2 = val_2 * 2;
UPDATE o_test_temp_indices SET val_3 = val_3 * 3;

SELECT orioledb_tbl_structure('o_test_temp_indices'::regclass, 'nue');

ALTER TABLE o_test_temp_indices ADD CONSTRAINT o_test_temp_indices_pkey PRIMARY KEY (val_1);

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_temp_indices ORDER BY val_2;
SELECT * FROM o_test_temp_indices ORDER BY val_2;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_temp_indices ORDER BY val_3;
SELECT * FROM o_test_temp_indices ORDER BY val_3;
COMMIT;

SELECT orioledb_tbl_structure('o_test_temp_indices'::regclass, 'nue');

UPDATE o_test_temp_indices SET val_1 = val_1 * 6;

SELECT orioledb_tbl_structure('o_test_temp_indices'::regclass, 'nue');

ALTER TABLE o_test_temp_indices DROP CONSTRAINT o_test_temp_indices_pkey;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_temp_indices ORDER BY val_2;
SELECT * FROM o_test_temp_indices ORDER BY val_2;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_temp_indices ORDER BY val_3;
SELECT * FROM o_test_temp_indices ORDER BY val_3;
COMMIT;

SELECT orioledb_tbl_structure('o_test_temp_indices'::regclass, 'nue');

UPDATE o_test_temp_indices SET val_1 = val_1 * 6;

SELECT orioledb_tbl_structure('o_test_temp_indices'::regclass, 'nue');

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

-- test expressions on tmp table
CREATE TEMP TABLE o_test_expression_tmp
(
	key int8 NOT NULL,
	value text
) USING orioledb;

CREATE INDEX o_test_expression_tmp_ix1 ON o_test_expression_tmp ((key * 100));
CREATE INDEX o_test_expression_tmp_ix2 ON o_test_expression_tmp ((value::int));
CREATE INDEX o_test_expression_tmp_ix3 ON o_test_expression_tmp ((value || 'WOW'));
DROP TABLE o_test_expression_tmp;

BEGIN;
CREATE TEMPORARY TABLE o_test_2 (val_1, val_2) USING orioledb
    ON COMMIT DROP
    AS (SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);
SELECT * FROM o_test_2;
COMMIT;

CREATE TEMP TABLE o_tmp_1 () USING orioledb
    ON COMMIT DELETE ROWS;

CREATE TABLE o_test_1 USING orioledb
    AS SELECT * FROM generate_series(1, 1000, 1);

SELECT pg_my_temp_schema()::regnamespace as temp_schema_name \gset

REINDEX SCHEMA :temp_schema_name;

CREATE TEMP TABLE o_test_temp_inherit (
	val_1 text,
	val_2 text
) USING orioledb;

CREATE INDEX ind_1 ON o_test_temp_inherit ((val_1 || val_2));

CREATE TEMP TABLE o_test_temp_inherit2 (
	val_3 text PRIMARY KEY
) USING orioledb;

INSERT INTO o_test_temp_inherit VALUES ('a', 'b'), ('x', 'y');
INSERT INTO o_test_temp_inherit2 VALUES ('ab'), ('xy');

CREATE TEMP TABLE o_test_temp_inherit_child (
	val_1 text,
	val_2 text
) USING orioledb;

ALTER TABLE o_test_temp_inherit_child INHERIT o_test_temp_inherit;

CREATE TEMP TABLE o_test_temp_inherit_child2 (
	PRIMARY KEY (val_3)
) INHERITS (o_test_temp_inherit2) USING orioledb;

INSERT INTO o_test_temp_inherit_child VALUES ('q', 'w'), ('e', 'r'),
											 ('t', 'y'), ('u', 'i');
INSERT INTO o_test_temp_inherit_child2 VALUES ('qw'), ('er'), ('ty'), ('ui');

CREATE INDEX ind_2 on o_test_temp_inherit_child ((val_1 || val_2));

SELECT * FROM (SELECT val_1 || val_2 AS val_3 FROM o_test_temp_inherit
			   UNION ALL
			   SELECT val_3 FROM o_test_temp_inherit2) t
	ORDER BY 1 LIMIT 8;

CREATE TEMP TABLE o_test_reinsert_to_primary (
	val_1 int
) USING orioledb;

INSERT INTO o_test_reinsert_to_primary(val_1)
	(SELECT val_1 - 5 FROM generate_series (1, 5) val_1);
SELECT * FROM o_test_reinsert_to_primary;

CREATE INDEX ON o_test_reinsert_to_primary(val_1);

UPDATE o_test_reinsert_to_primary SET val_1 = val_1 * 6;

SELECT * FROM o_test_reinsert_to_primary;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA temp_schema CASCADE;
RESET search_path;
