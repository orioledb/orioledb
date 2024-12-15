CREATE SCHEMA index_bridging;
SET SESSION search_path = 'index_bridging';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_ix_ams (
	i int NOT NULL,
	j int4[],
	p point,
	pk1 int,
	pk2 int
-- ) USING orioledb;
) USING orioledb WITH (index_bridging);

SELECT orioledb_table_description('o_test_ix_ams'::regclass);
\d+ o_test_ix_ams
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

INSERT INTO o_test_ix_ams VALUES (1, ARRAY[2,3], point(4, 5), 6, 7);
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams;
SELECT * FROM o_test_ix_ams;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

CREATE INDEX o_test_ix_ams_ix1 on o_test_ix_ams using btree (j) WITH (index_bridging);

\d+ o_test_ix_ams
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

INSERT INTO o_test_ix_ams VALUES (10, ARRAY[20,30], point(40, 50), 60, 70);

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
EXPLAIN (COSTS OFF)
	SELECT j FROM o_test_ix_ams ORDER BY j;
SELECT j FROM o_test_ix_ams ORDER BY j;
COMMIT;

SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
ALTER TABLE o_test_ix_ams ADD PRIMARY KEY (pk2, pk1);
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams;
SELECT * FROM o_test_ix_ams;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

INSERT INTO o_test_ix_ams VALUES (100, ARRAY[200,300], point(400, 500), 600, 700);
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams;
SELECT * FROM o_test_ix_ams;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
ALTER TABLE o_test_ix_ams DROP CONSTRAINT o_test_ix_ams_pkey;
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams;
SELECT * FROM o_test_ix_ams;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

SELECT orioledb_table_description('o_test_ix_ams'::regclass);
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
ALTER TABLE o_test_ix_ams ALTER j TYPE int USING 200-j[1];
SELECT orioledb_table_description('o_test_ix_ams'::regclass);
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams;
SELECT * FROM o_test_ix_ams;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

ALTER TABLE o_test_ix_ams ADD COLUMN k int;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

UPDATE o_test_ix_ams SET k = j/2 + 1000;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

CREATE INDEX o_test_ix_ams_ix2 on o_test_ix_ams using btree (k) WITH (index_bridging);

SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY k;
SELECT * FROM o_test_ix_ams ORDER BY k;
COMMIT;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

INSERT INTO o_test_ix_ams VALUES (1000, 2000, point(4000, 5000), 6000, 7000, 8000);

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY k;
SELECT * FROM o_test_ix_ams ORDER BY k;
COMMIT;

DROP INDEX o_test_ix_ams_ix2;

CREATE INDEX o_test_ix_ams_hash_ix ON o_test_ix_ams USING hash (k);
\d+ o_test_ix_ams
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams WHERE k = 8000;
SELECT * FROM o_test_ix_ams WHERE k = 8000;
COMMIT;

-- CREATE INDEX o_test_ix_ams_ix3 ON o_test_ix_ams USING gin (j);
-- CREATE INDEX o_test_ix_ams_ix4 ON o_test_ix_ams USING gist (p);
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA index_bridging CASCADE;
RESET search_path;
