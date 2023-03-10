CREATE SCHEMA tableam;
SET SESSION search_path = 'tableam';
CREATE EXTENSION orioledb;

SELECT orioledb_version();

SELECT translate(orioledb_commit_hash(), '0123456789abcdef', '################');

CREATE VIEW db_o_tables AS (SELECT c.relname, ot.description
FROM orioledb_table ot JOIN
     pg_database db ON db.oid = ot.datoid JOIN
     pg_class c ON c.oid = ot.reloid
WHERE db.datname = current_database());
CREATE VIEW db_o_indices AS (SELECT regexp_replace(c.relname, 'pg_toast_\d+', 'pg_toast_NNN') relname, oi.name, oi.description
FROM orioledb_index oi JOIN
     pg_database db ON db.oid = oi.datoid JOIN
     pg_class c ON c.oid = oi.index_reloid
WHERE db.datname = current_database());

-- index on empty table test
CREATE TABLE o_tableam1
(
	key int8 NOT NULL,
	value text
) USING orioledb;

-- not supported
CREATE INDEX CONCURRENTLY o_tableam1_ix_concurrently ON o_tableam1 (key);
CREATE INDEX o_tableam1_ix_options ON o_tableam1 (value) WITH (compression = on);
ALTER TABLE o_tableam1 ADD EXCLUDE USING btree (value WITH =);

SELECT orioledb_tbl_indices('o_tableam1'::regclass);

-- supported
ALTER TABLE o_tableam1 OWNER TO current_user;
CREATE UNIQUE INDEX o_tableam1_ix1 ON o_tableam1 (key);
CREATE INDEX o_tableam1_ix2 on o_tableam1 (value);

SELECT orioledb_tbl_indices('o_tableam1'::regclass);

INSERT INTO o_tableam1 (SELECT id, id || 'text' FROM generate_series(1, 20) as id);
ANALYZE o_tableam1;

EXPLAIN (COSTS off) SELECT * FROM o_tableam1;
SELECT * FROM o_tableam1;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT * FROM o_tableam1 WHERE key = 5;
SELECT * FROM o_tableam1 WHERE key = 5;
EXPLAIN (COSTS off) SELECT * FROM o_tableam1 ORDER BY key DESC;
SELECT * FROM o_tableam1 ORDER BY key DESC;
EXPLAIN (COSTS off) SELECT * FROM o_tableam1 WHERE value = '5text';
SELECT * FROM o_tableam1 WHERE value = '5text';
EXPLAIN (COSTS off) SELECT * FROM o_tableam1 WHERE value = '5text' AND key = 5;
SELECT * FROM o_tableam1 WHERE value = '5text' AND key = 5;
RESET enable_seqscan;

TRUNCATE o_tableam1;
INSERT INTO o_tableam1 (SELECT id, id || 'text' FROM generate_series(11, 20) as id);
SELECT * FROM o_tableam1;

CREATE TABLE IF NOT EXISTS o_tableam1_like
(LIKE o_tableam1 INCLUDING INDEXES INCLUDING CONSTRAINTS) USING orioledb;

INSERT INTO o_tableam1_like (SELECT id, id || 'text' FROM generate_series(1, 20) as id);
ANALYZE o_tableam1_like;
SELECT * FROM o_tableam1_like;
DROP TABLE o_tableam1_like;

DROP TABLE o_tableam1;

-- partial index test
CREATE TABLE o_test_partial
(
	key int8 NOT NULL PRIMARY KEY,
	value text
) USING orioledb;

CREATE FUNCTION plpgsql_func_test(in bigint) RETURNS int AS $$
BEGIN
	RETURN ABS($1) * 100;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION boolfunc_test(in bigint) RETURNS bool AS $$
BEGIN
	RETURN ABS($1) * 100 = 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE INDEX o_test_partial_ix1 ON o_test_partial (key) WHERE key > 10;
CREATE INDEX o_test_partial_ix2 ON o_test_partial (key) WHERE ABS(key) > 10;
CREATE INDEX o_test_partial_ix3 ON o_test_partial (key)
	WHERE not ABS(key) = 0;
CREATE INDEX o_test_partial_ix4 ON o_test_partial (key)
	WHERE ((not ABS(key) = 0) OR (not ABS(key) = 1));
CREATE INDEX o_test_partial_ix5 ON o_test_partial (key)
	WHERE ABS(-ABS(key)) > 10;
CREATE INDEX o_test_partial_ix6 ON o_test_partial (key)
	WHERE boolfunc_test(key);
CREATE INDEX o_test_partial_ix6 ON o_test_partial (key)
	WHERE plpgsql_func_test(key) = 0;
CREATE INDEX o_test_partial_ix7 ON o_test_partial (key)
	WHERE plpgsql_func_test(-key) * 2 = 0;
CREATE INDEX o_test_partial_ix7 ON o_test_partial (key)
	WHERE plpgsql_func_test(-key) * 2 = 0;
CREATE INDEX o_test_partial_ix7 ON o_test_partial (key)
	WHERE COALESCE(NULL, NULL, (plpgsql_func_test(-key) > 0)) != NULL;
CREATE INDEX o_test_partial_ix7 ON o_test_partial (key)
	WHERE GREATEST(NULL, NULL, (ABS(plpgsql_func_test(-key)) > 0)) != NULL;
CREATE INDEX o_test_partial_ix8 ON o_test_partial (value)
	WHERE value > '5';
CREATE INDEX o_test_partial_ix9 ON o_test_partial (value)
	WHERE (value || 'WOW') > '5';

SELECT orioledb_tbl_indices('o_test_partial'::regclass);

INSERT INTO o_test_partial (SELECT id, id || 'text'
								FROM generate_series(0, 20) as id);
ANALYZE o_test_partial;

EXPLAIN (COSTS off) SELECT * FROM o_test_partial WHERE key BETWEEN 15 AND 25;
SELECT * FROM o_test_partial WHERE key BETWEEN 15 AND 25;
EXPLAIN (COSTS off) SELECT key FROM o_test_partial WHERE key BETWEEN 15 AND 25;
SELECT key FROM o_test_partial WHERE key BETWEEN 15 AND 25;
SET enable_seqscan = OFF;
EXPLAIN (COSTS off) SELECT value FROM o_test_partial
	WHERE value BETWEEN '6' AND '9';
SELECT value FROM o_test_partial WHERE value BETWEEN '6' AND '9';
EXPLAIN (COSTS off) SELECT value FROM o_test_partial
	WHERE (value || 'WOW') BETWEEN '6' AND '9' ORDER BY value;
RESET enable_seqscan;
SELECT value FROM o_test_partial
	WHERE (value || 'WOW') BETWEEN '6' AND '9';
SELECT orioledb_tbl_structure('o_test_partial'::regclass, 'ne');

DELETE FROM o_test_partial WHERE key = 1;
UPDATE o_test_partial SET key = key + 100 WHERE key BETWEEN 5 AND 15;

SELECT orioledb_tbl_structure('o_test_partial'::regclass, 'ne');

ALTER TABLE o_test_partial DROP CONSTRAINT o_test_partial_pkey;
EXPLAIN (COSTS off) SELECT * FROM o_test_partial WHERE key > 15;
SELECT * FROM o_test_partial WHERE key > 15;

SELECT orioledb_tbl_structure('o_test_partial'::regclass, 'ne');

DROP TABLE o_test_partial;

-- expression index test
CREATE TABLE o_test_expression
(
	key int8 NOT NULL PRIMARY KEY,
	value text
) USING orioledb;

CREATE INDEX o_test_expression_ix1 ON o_test_expression ((key * 100));
CREATE INDEX o_test_expression_ix2 ON o_test_expression (key, (key * 100));
CREATE INDEX o_test_expression_ix3 ON o_test_expression ((value || 'WOW'));
CREATE INDEX o_test_expression_ix4
	ON o_test_expression (value, (value || 'WOW'));
CREATE INDEX o_test_expression_ix5
	ON o_test_expression ((key * 1000), value, key, (value || 'WOW2'), key);
CREATE INDEX o_test_expression_ix6 ON o_test_expression (UPPER(value), key,
														 LOWER(value));
CREATE INDEX o_test_expression_ix7
	ON o_test_expression ((TRUE), key,
						  ((not ABS(key) = 0) OR (not ABS(key) = 1)));
CREATE INDEX o_test_expression_ix8
	ON o_test_expression (boolfunc_test(key));

SELECT orioledb_tbl_indices('o_test_expression'::regclass);

INSERT INTO o_test_expression (SELECT id, id || 'text'
								FROM generate_series(0, 20) as id);

ANALYZE o_test_expression;

CREATE OR REPLACE FUNCTION smart_explain(sql TEXT) RETURNS SETOF TEXT AS $$
	DECLARE
		row RECORD;
		line text;
		indent integer;
		skip_indent integer;
		skip_start integer;
	BEGIN
		skip_indent := 0;
		skip_start := 0;
		FOR row IN EXECUTE sql LOOP
			line := row."QUERY PLAN";
			indent := length((regexp_match(line, '^ *'))[1]);
			IF line ~ '^ *->  Result' OR line ~ '^Result' THEN
				skip_indent := 6;
				skip_start := indent;
			ELSE
				IF indent >= skip_start THEN
					line := substr(line, skip_indent + 1);
				ELSE
					skip_indent := 0;
					skip_start := 0;
				END IF;
				RETURN NEXT line;
			END IF;
		END LOOP;
	END $$
LANGUAGE plpgsql;

SET enable_seqscan = off;
SELECT smart_explain($$
	EXPLAIN (COSTS off) SELECT (key * 100) FROM o_test_expression
		WHERE (key * 100) BETWEEN 100 AND 1000;
$$);
SELECT (key * 100) FROM o_test_expression
	WHERE (key * 100) BETWEEN 100 AND 1000;
SELECT smart_explain($$
	EXPLAIN (COSTS off) SELECT (value || 'WOW') FROM o_test_expression
		WHERE (value || 'WOW') BETWEEN '6' AND '9';
$$);
SELECT (value || 'WOW') FROM o_test_expression
	WHERE (value || 'WOW') BETWEEN '6' AND '9';
SELECT smart_explain($$
	EXPLAIN (COSTS off) SELECT (value || 'WOW') FROM o_test_expression
		WHERE UPPER(value) BETWEEN '6' AND '9';
$$);
SELECT (value || 'WOW') FROM o_test_expression
	WHERE UPPER(value) BETWEEN '6' AND '9';
EXPLAIN (COSTS off) SELECT * FROM o_test_expression
	WHERE UPPER(value) BETWEEN '1' AND '9' AND
		  LOWER(value) BETWEEN '4' AND '7';
SELECT * FROM o_test_expression
	WHERE UPPER(value) BETWEEN '1' AND '9' AND
		  LOWER(value) BETWEEN '4' AND '7';
RESET enable_seqscan;

DELETE FROM o_test_expression WHERE key = 1;
UPDATE o_test_expression SET key = key + 100 WHERE key BETWEEN 5 AND 15;

SELECT orioledb_tbl_structure('o_test_expression'::regclass, 'ne');

ALTER TABLE o_test_expression DROP CONSTRAINT o_test_expression_pkey;
ANALYZE o_test_expression;
SET enable_bitmapscan = off;
SET enable_seqscan = off;
SELECT smart_explain($$
	EXPLAIN (COSTS off) SELECT (key * 100) FROM o_test_expression
		WHERE (key * 100) BETWEEN 100 AND 1000;
$$);
SELECT (key * 100) FROM o_test_expression
	WHERE (key * 100) BETWEEN 100 AND 1000;
RESET enable_bitmapscan;
RESET enable_seqscan;

SELECT orioledb_tbl_structure('o_test_expression'::regclass, 'ne');

DROP TABLE o_test_expression;

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

-- create table with primary key
CREATE TABLE o_tableam3
(
  id integer NOT NULL,
  PRIMARY KEY (id)
) USING orioledb;

SELECT orioledb_tbl_indices('o_tableam3'::regclass);
INSERT INTO o_tableam3 (SELECT id FROM generate_series(1, 10) as id);
SELECT * FROM o_tableam3;
DROP TABLE o_tableam3;

CREATE TABLE o_tableam4
(
	key int8 NOT NULL,
	value int8,
	value2 int8
) USING orioledb;

CREATE UNIQUE INDEX o_tableam4_ix1 ON o_tableam4 (key);
CREATE INDEX o_tableam4_ix2 on o_tableam4 (value);

SELECT * FROM db_o_tables;
SELECT * FROM db_o_indices;

SELECT orioledb_tbl_indices('o_tableam4'::regclass);

INSERT INTO o_tableam4 (SELECT id, id + 1, id + 4 FROM generate_series(1, 10) as id);
ANALYZE o_tableam4;

SELECT * FROM o_tableam4;
EXPLAIN (COSTS off) SELECT * FROM o_tableam4;
SET enable_seqscan = off;
SELECT * FROM o_tableam4 WHERE key = 5;
EXPLAIN (COSTS off) SELECT * FROM o_tableam4 WHERE key = 5;
SELECT * FROM o_tableam4 WHERE value = 5;
EXPLAIN (COSTS off) SELECT * FROM o_tableam4 WHERE value = 5;
RESET enable_seqscan;
SELECT * FROM o_tableam4 WHERE value2 = 5;
EXPLAIN (COSTS off) SELECT * FROM o_tableam4 WHERE value2 = 5;

DROP TABLE o_tableam4;

CREATE TABLE o_tableam5
(
	id int8 NOT NULL,
	val text,
	val2 text
) USING orioledb;

CREATE UNIQUE INDEX o_tableam5_primary ON o_tableam5 (id DESC);
CREATE INDEX o_tableam5_ix1 ON o_tableam5 (val ASC);

SELECT orioledb_tbl_indices('o_tableam5'::regclass);

INSERT INTO o_tableam5 (SELECT id, id || 'text', id || 'text2' FROM generate_series(1, 10) as id);
ANALYZE o_tableam5;
SELECT id FROM o_tableam5;
EXPLAIN (COSTS off) SELECT id FROM o_tableam5;
SELECT id FROM o_tableam5 ORDER BY id DESC;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT id FROM o_tableam5
	WHERE id > 0 ORDER BY id DESC;
SELECT id FROM o_tableam5 ORDER BY val;
SELECT val2 FROM o_tableam5 ORDER BY val;

-- converts result of SQL query to text becase we can not
-- get result of EXPLAIN ANALYZE as TEXT in SQL statements
CREATE OR REPLACE FUNCTION query_to_text(sql TEXT) RETURNS SETOF TEXT AS $$
	BEGIN
		RETURN QUERY EXECUTE sql;
	END $$
LANGUAGE plpgsql;

SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
					SELECT val2 FROM o_tableam5
					WHERE val > ''0'' AND val < ''a''
					ORDER BY val;') as t;
DROP TABLE o_tableam5;

CREATE TABLE o_tableam_delete1
(
	id int8 NOT NULL,
	val text,
	val2 text,
	PRIMARY KEY(id)
) USING orioledb;

CREATE INDEX o_tableam_delete1_ix ON o_tableam_delete1 (val);
INSERT INTO o_tableam_delete1 (SELECT id, id || 'text', id || 'text2' FROM generate_series(1, 10) as id);
ANALYZE o_tableam_delete1;

SELECT * FROM db_o_tables;
SELECT * FROM db_o_indices;

EXPLAIN (COSTS off) DELETE FROM o_tableam_delete1 WHERE id > 5;
EXPLAIN (COSTS off) DELETE FROM o_tableam_delete1 WHERE val = '1text' RETURNING *;
EXPLAIN (COSTS off) DELETE FROM o_tableam_delete1 WHERE val = '2text' RETURNING val;

DELETE FROM o_tableam_delete1 WHERE id > 5;
DELETE FROM o_tableam_delete1 WHERE val = '1text' RETURNING *;
DELETE FROM o_tableam_delete1 WHERE val = '2text' RETURNING val;

SELECT * FROM o_tableam_delete1;

DROP TABLE o_tableam_delete1;

CREATE TABLE o_tableam_update1
(
	id int8 NOT NULL,
	val text,
	val2 text,
	PRIMARY KEY(id)
) USING orioledb;

CREATE INDEX o_tableam_update1_ix ON o_tableam_update1 (val);
INSERT INTO o_tableam_update1 (SELECT id, id || 'text', id || 'text2' FROM generate_series(1, 10) as id);
ANALYZE o_tableam_update1;

SELECT smart_explain($$
	EXPLAIN (COSTS off)
		UPDATE o_tableam_update1 SET id = id + 100 WHERE id > 5;
$$);
SELECT smart_explain($$
	EXPLAIN (COSTS off)
		UPDATE o_tableam_update1 SET id = id + 100 WHERE val = '1text'
			RETURNING *;
$$);
SELECT smart_explain($$
	EXPLAIN (COSTS off)
		UPDATE o_tableam_update1 SET id = id + 100 WHERE val = '2text'
			RETURNING val;
$$);

UPDATE o_tableam_update1 SET id = id + 100 WHERE id > 5;
UPDATE o_tableam_update1 SET id = id + 100 WHERE val = '1text' RETURNING *;
UPDATE o_tableam_update1 SET id = id + 100 WHERE val = '2text' RETURNING val;

SELECT * FROM o_tableam_update1;

DROP TABLE o_tableam_update1;
RESET enable_seqscan;

---
-- Transaction check
---
CREATE TABLE o_tableam6
(
	id int8 NOT NULL,
	val text
) USING orioledb;
INSERT INTO o_tableam6 (SELECT id, id || 'text' FROM generate_series(1, 10) as id);
SELECT * FROM db_o_tables;
SELECT * FROM db_o_indices;
SELECT index_type FROM orioledb_index_oids();
SELECT * FROM o_tableam6;

BEGIN;
DROP TABLE o_tableam6;
ROLLBACK;

SELECT * FROM db_o_tables;
SELECT * FROM db_o_indices;
SELECT index_type FROM orioledb_index_oids();
SELECT * FROM o_tableam6;

BEGIN;
TRUNCATE o_tableam6;
ROLLBACK;

SELECT * FROM o_tableam6;

BEGIN;
CREATE TABLE o_tableam6a
(
	id int8 NOT NULL,
	val text
) USING orioledb;
ROLLBACK;
SELECT * FROM db_o_tables;
SELECT * FROM db_o_indices;
SELECT index_type FROM orioledb_index_oids();

BEGIN;
CREATE TABLE o_tableam6a
(
	id int8 NOT NULL,
	val text
) USING orioledb;
INSERT INTO o_tableam6a (SELECT id, id || 'text' FROM generate_series(1, 10) as id);
SELECT * FROM o_tableam6a;
TRUNCATE TABLE o_tableam6a;
SELECT * FROM o_tableam6a;
ROLLBACK;
SELECT * FROM db_o_tables;
SELECT * FROM db_o_indices;
SELECT index_type FROM orioledb_index_oids();

BEGIN;
CREATE TABLE o_tableam6a
(
	id int8 NOT NULL,
	val text
) USING orioledb;
INSERT INTO o_tableam6a (SELECT id, id || 'text' FROM generate_series(1, 10) as id);
SELECT * FROM o_tableam6a;
TRUNCATE TABLE o_tableam6a;
COMMIT;
SELECT * FROM db_o_tables;
SELECT * FROM db_o_indices;
SELECT index_type FROM orioledb_index_oids();
INSERT INTO o_tableam6a (SELECT id, id || 'text' FROM generate_series(11, 20) as id);
SELECT * FROM o_tableam6a;
DROP TABLE o_tableam6a;

SET default_table_access_method = 'orioledb';
CREATE TABLE o_tableam7
(
	id int8 not null,
	value int8 not null,
	PRIMARY KEY(id)
);
RESET default_table_access_method;

INSERT INTO o_tableam7 SELECT i, 100 - i FROM generate_series(1, 100, 1) AS i;
UPDATE o_tableam7 SET id = id - 5 WHERE value < 99;

SELECT * FROM o_tableam7 ORDER BY id;
SELECT * FROM o_tableam7 ORDER BY id DESC;

UPDATE o_tableam7 SET id = 150 WHERE id = 50;
SELECT key, value FROM o_tableam7 WHERE id IN (50, 150);
SELECT value FROM o_tableam7 WHERE id = 10;
BEGIN;
UPDATE o_tableam7 SET value = value + 10 WHERE id = 10;
ROLLBACK;
SELECT value FROM o_tableam7 WHERE id = 10;
BEGIN;
UPDATE o_tableam7 SET value = value + 10 WHERE id = 10;
UPDATE o_tableam7 SET value = value + 10 WHERE id = 10;
ROLLBACK;
SELECT value FROM o_tableam7 WHERE id = 10;

-- Check for parallel scan
SET min_parallel_table_scan_size = 0;
TRUNCATE o_tableam7;
RESET min_parallel_table_scan_size;
DROP TABLE o_tableam7;

CREATE TABLE o_tableam8
(
	id int4 not null,
	t text not null,
	PRIMARY KEY(id)
) USING orioledb;

-- Test page split
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(1, 1000, 10) AS id;
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(2, 1000, 10) AS id;
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(999, 1, -10) AS id;
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(998, 1, -10) AS id;
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(7, 1000, 10) AS id;
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(6, 1000, 10) AS id;
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(5, 1000, 10) AS id;
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(993, 1, -10) AS id;
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(994, 1, -10) AS id;
INSERT INTO o_tableam8 SELECT id, repeat('x', id) FROM generate_series(1000, 1, -10) AS id;

SELECT (SELECT array_agg(id) FROM o_tableam8) =
       (SELECT array_agg(id)::int4[] FROM generate_series(1, 1000) as id);
SELECT (SELECT array_agg(t) FROM o_tableam8) =
       (SELECT array_agg(repeat('x', id)) FROM generate_series(1, 1000) as id);
DROP TABLE o_tableam8;

CREATE TABLE o_tableam_join1
(
	id integer NOT NULL,
	val text,
	PRIMARY KEY (id)
) USING orioledb;

INSERT INTO o_tableam_join1 (id, val) SELECT i, i||'!' FROM generate_series(1,30,2) AS i;

CREATE TABLE heap_table (id integer PRIMARY KEY, ids integer[]);
INSERT INTO heap_table (id, ids)
SELECT i, ARRAY[ (i*3+1)%30, (i*7+3)%30, (i*11+7)%30 ]
FROM generate_series(1,100) as i;

SELECT * FROM o_tableam_join1;

ANALYZE o_tableam_join1;
ANALYZE heap_table;

EXPLAIN (COSTS off) SELECT id, val FROM heap_table JOIN o_tableam_join1 USING (id);
SELECT id, val FROM heap_table JOIN o_tableam_join1 USING (id);
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS off) SELECT id, val FROM heap_table JOIN o_tableam_join1 USING (id);
SELECT id, val FROM heap_table JOIN o_tableam_join1 USING (id);
SET enable_nestloop = off;
RESET enable_mergejoin;
EXPLAIN (COSTS off) SELECT id, val FROM heap_table JOIN o_tableam_join1 USING (id);
SELECT id, val FROM heap_table JOIN o_tableam_join1 USING (id);

RESET enable_hashjoin;
RESET enable_nestloop;

-- Check full join
EXPLAIN (COSTS off) SELECT id, val FROM heap_table FULL JOIN o_tableam_join1 USING (id);
SELECT id, val FROM heap_table FULL JOIN o_tableam_join1 USING (id);

EXPLAIN (COSTS off) SELECT * FROM heap_table
			     JOIN o_tableam_join1 ON o_tableam_join1.id = ANY (heap_table.ids);
SELECT * FROM heap_table JOIN o_tableam_join1 ON o_tableam_join1.id = ANY (heap_table.ids);
EXPLAIN (COSTS off) SELECT * FROM heap_table
                             JOIN o_tableam_join1 ON o_tableam_join1.id = ANY (heap_table.ids)
			     WHERE o_tableam_join1.id < 20;
SELECT * FROM heap_table
	 JOIN o_tableam_join1 ON o_tableam_join1.id = ANY (heap_table.ids)
	 WHERE o_tableam_join1.id < 23 AND o_tableam_join1.id > 5;

CREATE TABLE o_tableam_cte_base
(
  id integer NOT NULL,
  val integer,
  val2 integer
) USING orioledb;

CREATE TABLE o_tableam_cte_primary_idx
(
  id integer NOT NULL,
  val integer,
  val2 integer,
  PRIMARY KEY (id)
) USING orioledb;

INSERT INTO o_tableam_cte_base (SELECT id, id + 1, id + 4 FROM generate_series(1, 10) as id);

INSERT INTO o_tableam_cte_primary_idx (SELECT id + 10, id * 2 + 1, id * 2 + 4 FROM generate_series(1, 10) as id);

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_cte_primary_idx;

WITH src AS (
	UPDATE o_tableam_cte_base SET val = id
	WHERE id in (1, 5, 2) RETURNING *
) INSERT INTO o_tableam_cte_primary_idx (SELECT * FROM src) RETURNING *;

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_cte_primary_idx;

WITH src AS (
	UPDATE o_tableam_cte_base SET val = id * 100
	WHERE id in (1, 5, 2) RETURNING *
) DELETE FROM o_tableam_cte_primary_idx dst
  WHERE dst.id in (SELECT id FROM src) RETURNING *;

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_cte_primary_idx;

WITH src AS (
	UPDATE o_tableam_cte_base SET val = id * 100
	WHERE id in (1, 5, 2) RETURNING *
) UPDATE o_tableam_cte_primary_idx dst
  SET val = src.val
  FROM src WHERE dst.id = src.id + 10 RETURNING *;

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_cte_primary_idx;

CREATE TABLE o_tableam_ctid
(
  id integer NOT NULL,
  val integer
) USING orioledb;

INSERT INTO o_tableam_ctid (SELECT id + 5, id * 2 + 1 FROM generate_series(1, 10) as id);

UPDATE o_tableam_cte_base SET val = id + 1, val2 = id + 4;
SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_ctid;

WITH src AS (
	UPDATE o_tableam_cte_base SET val = id * 100
	WHERE id > 5 RETURNING *
) DELETE FROM o_tableam_ctid dst
  WHERE dst.id in (SELECT id FROM src) RETURNING *;

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_ctid;

WITH src AS (
	UPDATE o_tableam_cte_base SET val = id
	WHERE id > 5 RETURNING *
) UPDATE o_tableam_ctid dst
  SET val = src.val
  FROM src WHERE dst.id = src.id + 5 RETURNING *;

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_ctid;

CREATE TABLE o_tableam_multicolumn_idx
(
  id integer NOT NULL,
  val integer,
  val2 integer,
  val3 integer
) USING orioledb;

CREATE UNIQUE INDEX o_tableam_multicolumn_idx_ix1 on o_tableam_multicolumn_idx (id, val, val2, val3);
CREATE UNIQUE INDEX o_tableam_multicolumn_idx_ix2 on o_tableam_multicolumn_idx (val, val3);

UPDATE o_tableam_cte_base SET val = id + 1, val2 = id + 4;
INSERT INTO o_tableam_multicolumn_idx (SELECT id + 5, id + 1, id + 2, id + 3 FROM generate_series(1, 10) as id);

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_multicolumn_idx;

WITH src AS (
	UPDATE o_tableam_cte_base SET val = id * 100 RETURNING *
) DELETE FROM o_tableam_multicolumn_idx dst
  WHERE dst.id in (SELECT id FROM src) RETURNING *;

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_multicolumn_idx;

WITH src AS (
	UPDATE o_tableam_cte_base SET val = id
	WHERE id < 5 RETURNING *
) UPDATE o_tableam_multicolumn_idx dst
  SET val = src.val
  FROM src
  WHERE dst.id = src.id + 10 RETURNING dst.*;

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_multicolumn_idx;

CREATE TABLE o_tableam_multicolumn_same_idx
(
  id integer NOT NULL,
  val integer,
  val2 integer,
  val3 integer
) USING orioledb;

CREATE UNIQUE INDEX o_tableam_multicolumn_same_idx_ix1 on o_tableam_multicolumn_same_idx (val2, val, val2, val);

UPDATE o_tableam_cte_base SET val = id + 1, val2 = id + 4;
INSERT INTO o_tableam_multicolumn_same_idx (SELECT id + 5, id + 1, id + 2, id + 3 FROM generate_series(1, 10) as id);

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_multicolumn_same_idx;

WITH src AS (
	UPDATE o_tableam_cte_base SET val = id * 100 RETURNING *
) DELETE FROM o_tableam_multicolumn_same_idx dst
  WHERE dst.id in (SELECT id FROM src) RETURNING *;

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_multicolumn_same_idx;

WITH src AS (
	UPDATE o_tableam_cte_base SET
		val = id,
		val2 = id + 1
	WHERE id < 5 RETURNING *
) UPDATE o_tableam_multicolumn_same_idx dst
  SET val = src.val,
	  val2 = src.val2,
	  val3 = src.val2
  FROM src
  WHERE dst.id = src.id + 10 RETURNING *;

SELECT * FROM o_tableam_cte_base;
SELECT * FROM o_tableam_multicolumn_same_idx;

CREATE TABLE o_tableam_ioc
(
	id integer NOT NULL,
	val integer,
	PRIMARY KEY(id)
) USING orioledb;

INSERT INTO o_tableam_ioc (SELECT id, id + 1 FROM generate_series(1, 10) as id);
SELECT * FROM o_tableam_ioc;
INSERT INTO o_tableam_ioc VALUES (1, 10) ON CONFLICT (id) DO NOTHING;
INSERT INTO o_tableam_ioc VALUES (1, 11) ON CONFLICT (id) DO NOTHING RETURNING *;
INSERT INTO o_tableam_ioc VALUES (11, 12) ON CONFLICT (id) DO NOTHING;
INSERT INTO o_tableam_ioc VALUES (12, 13) ON CONFLICT (id) DO NOTHING RETURNING *;

SELECT * FROM o_tableam_ioc;

INSERT INTO o_tableam_ioc VALUES (1, 14) ON CONFLICT (id) DO UPDATE SET id = 13;
INSERT INTO o_tableam_ioc VALUES (1, 15) ON CONFLICT (id) DO UPDATE SET id = 14 RETURNING *;
INSERT INTO o_tableam_ioc VALUES (1, 16) ON CONFLICT (id) DO UPDATE SET id = 14 RETURNING *;

SELECT * FROM o_tableam_ioc;

CREATE TABLE o_tableam_explain_1
(
  id integer NOT NULL,
  val integer,
  val2 integer
) USING orioledb;

CREATE TABLE o_tableam_explain_2
(
  id integer NOT NULL,
  val integer,
  val2 integer,
  val3 integer,
  PRIMARY KEY(id)
) USING orioledb;

INSERT INTO o_tableam_explain_1 (SELECT id, id + 1, id + 4 FROM generate_series(1, 10) as id);
INSERT INTO o_tableam_explain_2 (SELECT id + 5, id + 1, id + 2, id + 3 FROM generate_series(1, 10) as id);

SELECT * FROM o_tableam_explain_1;
SELECT * FROM o_tableam_explain_2;

SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)
	INSERT INTO o_tableam_explain_2 VALUES (6, 12, 13, 14)
	ON CONFLICT (id) DO UPDATE
	SET val = 14, val2 = 13, val3 = 12;') as t;

SELECT * FROM o_tableam_explain_2;

SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('EXPLAIN (ANALYZE TRUE, BUFFERS TRUE) WITH src AS (
	UPDATE o_tableam_explain_1 SET
		val = id,
		val2 = id + 1
	WHERE id < 5 RETURNING *
) UPDATE o_tableam_explain_2 dst
  SET val = src.val,
	  val2 = src.val2,
	  val3 = src.val2
  FROM src
  WHERE dst.id = src.id + 10 RETURNING *;') as t;

SELECT * FROM o_tableam_explain_1;
SELECT * FROM o_tableam_explain_2;

-- Check snapshot deregistering
CREATE TABLE o_tableam_snapshot_check (
	t text PRIMARY KEY,
	cnt int)
USING orioledb;
WITH inserted AS (
	INSERT INTO o_tableam_snapshot_check (
		SELECT t, COUNT(*)
		FROM (
			SELECT ID, 'abcdef' || (ID % 100) t
			FROM generate_series(1,100) ID) tmp
			GROUP BY t) returning *)
SELECT substr(t, 6) FROM inserted;
WITH inserted AS (
	INSERT INTO o_tableam_snapshot_check (
		SELECT t, COUNT(*)
		FROM (
			SELECT ID, 'abcdef' || (ID % 100) t
			FROM generate_series(1,100) ID) tmp
			GROUP BY t) returning *)
SELECT substr(t, 6) FROM inserted;

DROP TABLE heap_table;

CREATE TABLE o_test_add_column
(
  id serial primary key,
  i int4
) USING orioledb;

CREATE FUNCTION pseudo_random(seed bigint, i bigint) RETURNS float8 AS
$$
  SELECT i::text::bigint::float8;
$$ LANGUAGE sql;
INSERT INTO o_test_add_column (i)
  SELECT pseudo_random(1, v) * 20000 FROM generate_series(1,10) v;
CREATE SEQUENCE o_test_j_seq;
BEGIN;
ALTER TABLE o_test_add_column
  ADD COLUMN j int4 not null default pseudo_random(2, nextval('o_test_j_seq')) * 20000;
ROLLBACK;

BEGIN;
DECLARE a CURSOR FOR SELECT ctid, * FROM o_test_add_column;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
   UPDATE o_test_add_column SET i = -i
      WHERE CURRENT OF a RETURNING *;
COMMIT;

DROP TABLE o_test_add_column;

CREATE TABLE o_heap_test
(
	key int8 NOT NULL PRIMARY KEY,
	value text
);

SELECT orioledb_tbl_structure('o_heap_test'::regclass);
SELECT orioledb_idx_structure('o_heap_test'::regclass, 'o_heap_test_pkey');
SELECT orioledb_tbl_check('o_heap_test'::regclass);
SELECT orioledb_tbl_indices('o_heap_test'::regclass);
SELECT * FROM orioledb_index_rows('o_heap_test_pkey'::regclass) r;
SELECT * FROM orioledb_table_pages('o_heap_test'::regclass) p;
SELECT orioledb_table_description('o_heap_test'::regclass);
SELECT orioledb_relation_size('o_heap_test'::regclass);
SELECT orioledb_evict_pages('o_heap_test'::regclass, 1);
SELECT orioledb_write_pages('o_heap_test'::regclass);

DROP TABLE o_heap_test;

CREATE TABLE o_ctid_test (val_1 int) USING orioledb;
INSERT INTO o_ctid_test VALUES (1);
INSERT INTO o_ctid_test SELECT * FROM o_ctid_test;
INSERT INTO o_ctid_test VALUES (0);
SELECT count(*) FROM o_ctid_test;
DELETE FROM o_ctid_test WHERE val_1 != 0;
SELECT * FROM o_ctid_test;

BEGIN;
SAVEPOINT x;
CREATE TABLE o_test_1 (val_1 int) USING orioledb;
INSERT INTO o_test_1 VALUES (1);
DECLARE c1 CURSOR FOR SELECT * FROM o_test_1;
FETCH FROM c1;
ROLLBACK TO x;
COMMIT;

BEGIN;
CREATE TEMPORARY TABLE o_test_2 (val_1, val_2) USING orioledb
    ON COMMIT DROP
    AS (SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);
SELECT * FROM o_test_2;
COMMIT;

CREATE TABLE o_test_1 (
	val_1 int4,
	val_2 text
) USING orioledb;
INSERT INTO o_test_1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
CREATE UNIQUE INDEX o_test_1_val_2_idx ON o_test_1 (val_2);
INSERT INTO o_test_1 VALUES (6, 'e');
DROP TABLE o_test_1;

CREATE TEMP TABLE o_tmp_1 () USING orioledb
    ON COMMIT DELETE ROWS;

CREATE TABLE o_test_1 USING orioledb
    AS SELECT * FROM generate_series(1, 1000, 1);

SELECT pg_my_temp_schema()::regnamespace as temp_schema_name \gset

REINDEX SCHEMA :temp_schema_name;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA tableam CASCADE;
RESET search_path;
