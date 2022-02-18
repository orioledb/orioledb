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

-- index subscan; index only qual
EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT COUNT(*) FROM o_test_subquery_all;
-- returns 500
WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT COUNT(*) FROM o_test_subquery_all;

-- index subscan; index only qual; query rows
EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT * FROM o_test_subquery_all LIMIT 10;
WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND ABS(val2) IS NOT NULL
    ORDER BY val2
) SELECT * FROM o_test_subquery_all LIMIT 10;

-- index subscan
EXPLAIN (COSTS off) WITH o_test_subquery_all AS (
    SELECT * FROM o_test_subquery
    WHERE val2 > 0 AND val > 0
    ORDER BY val2
) SELECT COUNT(*) FROM o_test_subquery_all;
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

DROP EXTENSION orioledb CASCADE;
