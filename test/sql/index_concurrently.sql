CREATE DATABASE reindex_concurrently_test_db;
\c reindex_concurrently_test_db

CREATE SCHEMA orioledb;
CREATE EXTENSION orioledb SCHEMA orioledb;


CREATE TABLE t_concurrent_build (
	id int PRIMARY KEY,
	val int
) USING orioledb;

-- Insert enough data to make index build take time
INSERT INTO t_concurrent_build SELECT i, i * 100 FROM generate_series(2, 5000000) i;

-- Create a simple done table to signal when background job completes
CREATE TABLE concurrent_build_done (step int);

-- Start concurrent index build in background
\! psql -d reindex_concurrently_test_db -c "CREATE INDEX CONCURRENTLY t_concurrent_build_idx ON t_concurrent_build (val);"  && psql -d reindex_concurrently_test_db -c "INSERT INTO concurrent_build_done VALUES (1);" &

-- Wait for build phase to start (1 seconds)
SELECT pg_sleep(1);

-- Case 1: INSERT new tuple (key=99999) - will be deleted later
-- Case 2: DELETE existing tuple (key=100) - will be reinserted later
INSERT INTO t_concurrent_build VALUES (1, 100);
DELETE FROM t_concurrent_build WHERE id = 5;
INSERT INTO t_concurrent_build VALUES (5000001, 500000100);
DELETE FROM t_concurrent_build WHERE id = 50000;
DELETE FROM t_concurrent_build WHERE id = 50001;

SELECT pg_sleep(3);
BEGIN;
INSERT INTO t_concurrent_build VALUES (50000, 5000000);
SAVEPOINT sp1;
INSERT INTO t_concurrent_build VALUES (50001, 5000100);
SELECT pg_sleep(30);  -- Ensure this transaction overlaps with index build
ROLLBACK TO sp1;  -- Rollback second insert, but keep first one
COMMIT;
-- Wait for background job completion signal
DO $$
DECLARE
	done_count int := 0;
BEGIN
	WHILE done_count = 0 LOOP
		SELECT COUNT(*) INTO done_count FROM concurrent_build_done WHERE step = 1;
		IF done_count = 0 THEN
			PERFORM pg_sleep(1);
		END IF;
        COMMIT;
	END LOOP;
END $$;

-- Verify the index was created
\d+ t_concurrent_build


SELECT * FROM orioledb.orioledb_tbl_indices( 't_concurrent_build'::regclass, true, true);

-- Verify index works correctly
SET enable_seqscan = off;
SET enable_bitmapscan = off;

EXPLAIN (COSTS OFF) SELECT id FROM t_concurrent_build WHERE val = 100;
SELECT id FROM t_concurrent_build WHERE val = 100;

EXPLAIN (COSTS OFF) SELECT id FROM t_concurrent_build WHERE val = 500;
SELECT id FROM t_concurrent_build WHERE val = 500;

EXPLAIN (COSTS OFF) SELECT id FROM t_concurrent_build WHERE val = 5000000;
SELECT id FROM t_concurrent_build WHERE val = 5000000;

EXPLAIN (COSTS OFF) SELECT id FROM t_concurrent_build WHERE val = 5000100;
SELECT id FROM t_concurrent_build WHERE val = 5000100;

EXPLAIN (COSTS OFF) SELECT id FROM t_concurrent_build WHERE val = 500000100;
SELECT id FROM t_concurrent_build WHERE val = 500000100;

RESET enable_seqscan;
RESET enable_bitmapscan;

\c regression
DROP DATABASE reindex_concurrently_test_db;