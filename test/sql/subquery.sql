CREATE SCHEMA subquery;
SET SESSION search_path = 'subquery';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_subquery
(
  key bigint NOT NULL,
  val int,
  val2 int NOT NULL,
  PRIMARY KEY (key)
) USING orioledb;

CREATE INDEX o_test_subquery_idx1 ON o_test_subquery (val);
INSERT INTO o_test_subquery SELECT 1000 + i, 2000 + i, 3000 + i FROM generate_series(1, 500) AS i;
CREATE INDEX o_test_subquery_idx2 ON o_test_subquery (val2);

analyze o_test_subquery;

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

-- index subscan; index only qual
SELECT smart_explain(
'EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT COUNT(*) FROM o_test_subquery_all;');
-- returns 500
WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT COUNT(*) FROM o_test_subquery_all;

-- index subscan; index only qual; query rows
SELECT smart_explain(
'EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT * FROM o_test_subquery_all LIMIT 10;');
WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT * FROM o_test_subquery_all LIMIT 10;

-- index subscan
SELECT smart_explain(
'EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND val > 0
    ORDER BY val2
) SELECT COUNT(*) FROM o_test_subquery_all;');
-- returns 500
WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND val > 0
    ORDER BY val2
) SELECT COUNT(*) FROM o_test_subquery_all;

-- index subscan; query rows
EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND val > 0
    ORDER BY val2
) SELECT * FROM o_test_subquery_all LIMIT 10;
WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND val > 0
    ORDER BY val2
) SELECT * FROM o_test_subquery_all LIMIT 10;

-- index only subscan; index only qual
EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT val2 FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT COUNT(o_test_subquery_all.val2) FROM o_test_subquery_all;
-- returns 500
WITH o_test_subquery_all AS (
    SELECT val2 FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT COUNT(o_test_subquery_all.val2) FROM o_test_subquery_all;

-- index only subscan; index only qual; query rows
EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT val2 FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT o_test_subquery_all.val2 FROM o_test_subquery_all LIMIT 10;
WITH o_test_subquery_all AS (
    SELECT val2 FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT o_test_subquery_all.val2 FROM o_test_subquery_all LIMIT 10;

-- index only subscan
EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT val2 FROM o_test_subquery
    WHERE val2 > 0 AND val > 4
    ORDER BY val2
) SELECT COUNT(o_test_subquery_all.val2) FROM o_test_subquery_all;

-- returns 500
WITH o_test_subquery_all AS (
    SELECT val2 FROM o_test_subquery
    WHERE val2 > 0 AND val > 4
    ORDER BY val2
) SELECT COUNT(o_test_subquery_all.val2) FROM o_test_subquery_all;

-- index only subscan; query rows
EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT val2 FROM o_test_subquery
    WHERE val2 > 0 AND val > 4
    ORDER BY val2
) SELECT o_test_subquery_all.val2 FROM o_test_subquery_all LIMIT 10;

WITH o_test_subquery_all AS (
    SELECT val2 FROM o_test_subquery
    WHERE val2 > 0 AND val > 4
    ORDER BY val2
) SELECT o_test_subquery_all.val2 FROM o_test_subquery_all LIMIT 10;

CREATE TABLE t (
    i1 SMALLINT NOT NULL,
    i2 SMALLINT NOT NULL,
    i3 SMALLINT NOT NULL,
    PRIMARY KEY (i1, i2, i3)
)
USING orioledb;

INSERT INTO t (i1, i2, i3) VALUES
  (1, 1, 1001), (1, 1, 1002), (1, 2, 1001), (1, 2, 1002), (2, 1, 1001), (2, 2, 1001);

WITH t_delete AS (
    DELETE
    FROM t as del_t
    USING UNNEST(ARRAY[1,2]) AS i2_values
    WHERE i2 = i2_values
    AND i1 = 1
    AND del_t.i3 = (
        select min(select_t.i3)
        from t as select_t
        where i2 = i2_values
        and i1 = 1
    )
    RETURNING del_t.i3, del_t.i2
)
SELECT array_agg(i3), array_agg(i2)
    FROM t_delete order by 1,2;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA subquery CASCADE;
RESET search_path;
