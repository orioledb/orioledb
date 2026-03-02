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


INSERT INTO t_concurrent_build VALUES (1, 200); 
CREATE UNIQUE INDEX CONCURRENTLY t_concurrent_build_idx ON t_concurrent_build (val);
\d+ t_concurrent_build
DROP INDEX t_concurrent_build_idx;
SELECT * FROM orioledb.orioledb_tbl_indices( 't_concurrent_build'::regclass, true, true);
DELETE FROM t_concurrent_build WHERE id = 1;

-- Start concurrent index build in background
\! psql -d reindex_concurrently_test_db -c "CREATE UNIQUE INDEX CONCURRENTLY t_concurrent_build_idx ON t_concurrent_build (val);"  && psql -d reindex_concurrently_test_db -c "INSERT INTO concurrent_build_done VALUES (1);" &

-- Wait for build phase to start (1 seconds)
SELECT pg_sleep(1);

DELETE FROM t_concurrent_build WHERE id = 4000000;
INSERT INTO t_concurrent_build VALUES (1, 400000000);

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

DELETE FROM t_concurrent_build WHERE id = 1;
INSERT INTO t_concurrent_build VALUES (1, 200);

DROP INDEX t_concurrent_build_idx;

INSERT INTO t_concurrent_build VALUES (1, 200);

\c regression
DROP DATABASE reindex_concurrently_test_db;