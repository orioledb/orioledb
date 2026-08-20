-- An ordered scan of a secondary index used to be the one shape OrioleDB left
-- to the PostgreSQL index AM adapter, which materialises a rowid and a whole
-- heap or index tuple for every row it returns.  It runs inside o_scan now, so
-- the plans below name the index and its direction as an o_scan property.
-- What must not change is a single answer, which is what the queries check.
CREATE SCHEMA native_si_scan;
SET SESSION search_path = 'native_si_scan';
CREATE EXTENSION orioledb;

CREATE TABLE o_si (
	id int NOT NULL PRIMARY KEY,
	k int NOT NULL,
	v text NOT NULL
) USING orioledb;
INSERT INTO o_si SELECT i, i * 2, 'v' || i FROM generate_series(1, 2000) i;
CREATE INDEX o_si_k_ix ON o_si (k);
CREATE UNIQUE INDEX o_si_v_ix ON o_si (v);
ANALYZE o_si;

SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Covered, not covered, backwards, and by a unique index.
EXPLAIN (COSTS OFF) SELECT k FROM o_si WHERE k BETWEEN 200 AND 210;
SELECT k FROM o_si WHERE k BETWEEN 200 AND 210;
EXPLAIN (COSTS OFF) SELECT k, v FROM o_si WHERE k BETWEEN 200 AND 210;
SELECT k, v FROM o_si WHERE k BETWEEN 200 AND 210;
EXPLAIN (COSTS OFF) SELECT k, v FROM o_si WHERE k BETWEEN 200 AND 210 ORDER BY k DESC;
SELECT k, v FROM o_si WHERE k BETWEEN 200 AND 210 ORDER BY k DESC;
EXPLAIN (COSTS OFF) SELECT id, k FROM o_si WHERE v = 'v100';
SELECT id, k FROM o_si WHERE v = 'v100';

-- A range wider than one leaf page, checked by aggregate rather than by eye.
SELECT count(*), sum(k), min(v), max(v) FROM o_si WHERE k BETWEEN 100 AND 3000;

-- The primary key is unaffected, and so is a scan that returns nothing.
EXPLAIN (COSTS OFF) SELECT id FROM o_si WHERE id BETWEEN 100 AND 110;
SELECT count(*) FROM o_si WHERE k BETWEEN 100000 AND 100010;

RESET enable_seqscan;
RESET enable_bitmapscan;
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA native_si_scan CASCADE;
RESET search_path;
