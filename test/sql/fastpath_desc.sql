-- Exercises the intra-page "fast path" downlink search (fastpath.c) on
-- DESC-ordered key columns, which used to fall back to the slow binary search.
-- A multi-level tree plus forced index scans drive the fastpath during descent;
-- every result is cross-checked against an identical heap table whose ordering
-- comes from a Sort node, so a wrong downlink (wrong subtree) shows up as a
-- non-zero symmetric difference.
CREATE SCHEMA fastpath_desc;
SET SESSION search_path = 'fastpath_desc';
CREATE EXTENSION orioledb;

CREATE FUNCTION pseudo_random(seed bigint, i bigint) RETURNS bigint AS
$$
	SELECT (substr(sha256(($1::text || ' ' || $2::text)::bytea)::text, 2, 16)::bit(52)::bigint % 20000);
$$ LANGUAGE sql;

-- orioledb table under test and an identical heap oracle.
CREATE TABLE o (id int8 PRIMARY KEY, v int4, a int4, b int8) USING orioledb;
CREATE TABLE h (id int8 PRIMARY KEY, v int4, a int4, b int8);

INSERT INTO o
	SELECT g,
		   CASE WHEN g % 50 = 0 THEN NULL ELSE pseudo_random(1, g)::int4 END,
		   (pseudo_random(2, g) % 100)::int4,
		   pseudo_random(3, g)
	FROM generate_series(1, 30000) g;
INSERT INTO h SELECT * FROM o;

-- DESC index (NULLS FIRST by default), a distinct DESC NULLS LAST index, and a
-- mixed (ASC, DESC) composite key.  Keeping the two single-column indexes
-- distinct lets each ORDER BY below map unambiguously to one of them.
CREATE INDEX o_v_desc ON o (v DESC);
CREATE INDEX o_v_desc_nl ON o (v DESC NULLS LAST);
CREATE INDEX o_ab ON o (a, b DESC);
CREATE INDEX h_v_desc ON h (v DESC);
CREATE INDEX h_v_desc_nl ON h (v DESC NULLS LAST);
CREATE INDEX h_ab ON h (a, b DESC);
ANALYZE o;
ANALYZE h;

-- Force index access on the orioledb table so the multi-level DESC indexes are
-- descended through the fast path.
SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Symmetric-difference helper: returns the number of rows that the orioledb
-- index scan and the heap oracle disagree on for a predicate.  Expected 0.
-- Equality, including a value that is absent.
SELECT count(*) AS eq_mismatch FROM (
	(SELECT v, id FROM o WHERE v = 12345 EXCEPT ALL SELECT v, id FROM h WHERE v = 12345)
	UNION ALL
	(SELECT v, id FROM h WHERE v = 12345 EXCEPT ALL SELECT v, id FROM o WHERE v = 12345)
) d;

-- Range scans (each end inclusive/exclusive) over the DESC key.
SELECT count(*) AS lt_mismatch FROM (
	(SELECT v, id FROM o WHERE v < 5000 EXCEPT ALL SELECT v, id FROM h WHERE v < 5000)
	UNION ALL
	(SELECT v, id FROM h WHERE v < 5000 EXCEPT ALL SELECT v, id FROM o WHERE v < 5000)
) d;

SELECT count(*) AS ge_mismatch FROM (
	(SELECT v, id FROM o WHERE v >= 15000 EXCEPT ALL SELECT v, id FROM h WHERE v >= 15000)
	UNION ALL
	(SELECT v, id FROM h WHERE v >= 15000 EXCEPT ALL SELECT v, id FROM o WHERE v >= 15000)
) d;

SELECT count(*) AS between_mismatch FROM (
	(SELECT v, id FROM o WHERE v BETWEEN 8000 AND 12000 EXCEPT ALL SELECT v, id FROM h WHERE v BETWEEN 8000 AND 12000)
	UNION ALL
	(SELECT v, id FROM h WHERE v BETWEEN 8000 AND 12000 EXCEPT ALL SELECT v, id FROM o WHERE v BETWEEN 8000 AND 12000)
) d;

-- Full content (incl. NULLs) must match.
SELECT count(*) AS all_mismatch FROM (
	(SELECT v, id FROM o EXCEPT ALL SELECT v, id FROM h)
	UNION ALL
	(SELECT v, id FROM h EXCEPT ALL SELECT v, id FROM o)
) d;

-- Composite key: equality on the leading ASC column, range on the DESC column.
SELECT count(*) AS comp_mismatch FROM (
	(SELECT a, b, id FROM o WHERE a = 7 EXCEPT ALL SELECT a, b, id FROM h WHERE a = 7)
	UNION ALL
	(SELECT a, b, id FROM h WHERE a = 7 EXCEPT ALL SELECT a, b, id FROM o WHERE a = 7)
) d;

SELECT count(*) AS comp_range_mismatch FROM (
	(SELECT a, b, id FROM o WHERE a = 42 AND b >= 10000 EXCEPT ALL SELECT a, b, id FROM h WHERE a = 42 AND b >= 10000)
	UNION ALL
	(SELECT a, b, id FROM h WHERE a = 42 AND b >= 10000 EXCEPT ALL SELECT a, b, id FROM o WHERE a = 42 AND b >= 10000)
) d;

-- Ordered samples.  The EXPLAIN before each query shows the scan is served by
-- the expected DESC index (forward) or its backward scan, not a seqscan/sort
-- of the heap -- i.e. the fastpath descent is actually exercised.  Each
-- ORDER BY's (direction, NULLS placement) maps unambiguously to one index:
--   v DESC NULLS FIRST -> o_v_desc            (forward)
--   v ASC  NULLS LAST  -> o_v_desc            (backward)
--   v DESC NULLS LAST  -> o_v_desc_nl         (forward)
--   v ASC  NULLS FIRST -> o_v_desc_nl         (backward)
--   a = c, b DESC      -> o_ab                (forward)
EXPLAIN (COSTS OFF)
	SELECT v, id FROM o ORDER BY v DESC NULLS FIRST, id LIMIT 5;
SELECT v, id FROM o ORDER BY v DESC NULLS FIRST, id LIMIT 5;

EXPLAIN (COSTS OFF)
	SELECT v, id FROM o ORDER BY v ASC NULLS LAST, id LIMIT 5;
SELECT v, id FROM o ORDER BY v ASC NULLS LAST, id LIMIT 5;

EXPLAIN (COSTS OFF)
	SELECT v, id FROM o ORDER BY v DESC NULLS LAST, id LIMIT 5;
SELECT v, id FROM o ORDER BY v DESC NULLS LAST, id LIMIT 5;

EXPLAIN (COSTS OFF)
	SELECT v, id FROM o ORDER BY v ASC NULLS FIRST, id LIMIT 5;
SELECT v, id FROM o ORDER BY v ASC NULLS FIRST, id LIMIT 5;

EXPLAIN (COSTS OFF)
	SELECT b, id FROM o WHERE a = 7 ORDER BY b DESC, id LIMIT 10;
SELECT b, id FROM o WHERE a = 7 ORDER BY b DESC, id LIMIT 10;

RESET enable_seqscan;
RESET enable_bitmapscan;
DROP SCHEMA fastpath_desc CASCADE;
