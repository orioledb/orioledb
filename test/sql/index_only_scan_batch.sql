CREATE SCHEMA index_only_scan_batch;
SET SESSION search_path = 'index_only_scan_batch';
CREATE EXTENSION IF NOT EXISTS orioledb;

-- Index-only scans of a secondary index take the rows of a leaf chunk that
-- need no per-row check in one batch, straight from the page image into the
-- slot.  Compare them with the same queries over a heap table.
CREATE TABLE o (id int PRIMARY KEY, c int, b bigint, n int) USING orioledb;
CREATE TABLE h (id int PRIMARY KEY, c int, b bigint, n int);
INSERT INTO o SELECT g, (g * 7919) % 5000, (g::bigint * 1000003) % 77777,
	CASE WHEN g % 13 = 0 THEN NULL ELSE g % 37 END
	FROM generate_series(1, 30000) g;
INSERT INTO h SELECT * FROM o;
CREATE INDEX o_c ON o (c);
CREATE INDEX h_c ON h (c);
CREATE INDEX o_nb ON o (n, b);
CREATE INDEX h_nb ON h (n, b);
VACUUM ANALYZE h;
ANALYZE o;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

EXPLAIN (COSTS OFF) SELECT count(*), sum(c), sum(id) FROM o WHERE c >= 0;

SELECT (SELECT row(count(*), sum(c), sum(id)) FROM o WHERE c >= 0) =
	   (SELECT row(count(*), sum(c), sum(id)) FROM h WHERE c >= 0) AS full_range;
SELECT (SELECT row(count(c), sum(c)) FROM o) =
	   (SELECT row(count(c), sum(c)) FROM h) AS no_condition;
-- Bounds that end inside a leaf
SELECT (SELECT row(count(*), sum(c), sum(id)) FROM o WHERE c BETWEEN 1000 AND 1234) =
	   (SELECT row(count(*), sum(c), sum(id)) FROM h WHERE c BETWEEN 1000 AND 1234) AS bounded;
SELECT (SELECT row(count(*), sum(c), sum(id)) FROM o WHERE c > 1000 AND c < 1500) =
	   (SELECT row(count(*), sum(c), sum(id)) FROM h WHERE c > 1000 AND c < 1500) AS exclusive;
-- A qual on a batched row
SELECT (SELECT row(count(*), sum(c)) FROM o WHERE c >= 10 AND id % 3 = 0) =
	   (SELECT row(count(*), sum(c)) FROM h WHERE c >= 10 AND id % 3 = 0) AS qual;
-- NULLs, and a condition on the second key column
SELECT (SELECT array_agg(row(n, b, id)::text) FROM
			(SELECT n, b, id FROM o ORDER BY n, b, id) s) =
	   (SELECT array_agg(row(n, b, id)::text) FROM
			(SELECT n, b, id FROM h ORDER BY n, b, id) s) AS composite;
SELECT (SELECT row(count(*), sum(b), sum(id)) FROM o WHERE n = 5 AND b > 100) =
	   (SELECT row(count(*), sum(b), sum(id)) FROM h WHERE n = 5 AND b > 100) AS second_key;
-- Many short lookups
SELECT (SELECT count(*) FROM generate_series(0, 4999, 7) k JOIN o ON o.c = k) =
	   (SELECT count(*) FROM generate_series(0, 4999, 7) k JOIN h ON h.c = k) AS joined;

-- A cursor stopping inside a batch
BEGIN;
DECLARE cur CURSOR FOR SELECT c, id FROM o WHERE c >= 0;
FETCH 3 FROM cur;
MOVE 60 IN cur;
FETCH 3 FROM cur;
COMMIT;

-- Rows changed by a transaction still open are checked one by one
BEGIN;
DELETE FROM o WHERE id % 10 = 0;
DELETE FROM h WHERE id % 10 = 0;
UPDATE o SET c = c + 1 WHERE id % 7 = 0;
UPDATE h SET c = c + 1 WHERE id % 7 = 0;
INSERT INTO o SELECT g, g % 5000, g, g % 37 FROM generate_series(40001, 41000) g;
INSERT INTO h SELECT g, g % 5000, g, g % 37 FROM generate_series(40001, 41000) g;
SELECT (SELECT row(count(*), sum(c), sum(id)) FROM o WHERE c >= 0) =
	   (SELECT row(count(*), sum(c), sum(id)) FROM h WHERE c >= 0) AS in_transaction;
ROLLBACK;
SELECT (SELECT row(count(*), sum(c), sum(id)) FROM o WHERE c >= 0) =
	   (SELECT row(count(*), sum(c), sum(id)) FROM h WHERE c >= 0) AS rolled_back;

-- And once committed, batched again
DELETE FROM o WHERE id % 10 = 0;
DELETE FROM h WHERE id % 10 = 0;
UPDATE o SET c = c + 1 WHERE id % 7 = 0;
UPDATE h SET c = c + 1 WHERE id % 7 = 0;
SELECT (SELECT row(count(*), sum(c), sum(id)) FROM o WHERE c >= 0) =
	   (SELECT row(count(*), sum(c), sum(id)) FROM h WHERE c >= 0) AS committed;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET max_parallel_workers_per_gather;
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA index_only_scan_batch CASCADE;
RESET search_path;
