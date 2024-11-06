CREATE SCHEMA index_bridging;
SET SESSION search_path = 'index_bridging';
CREATE EXTENSION orioledb;
CREATE EXTENSION pageinspect;

CREATE TABLE o_test_ix_ams (
	i int NOT NULL,
	j int4[],
	p point,
	pk1 int,
	pk2 int
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

CREATE INDEX o_test_ix_ams_ix1 on o_test_ix_ams using btree (j) WITH (index_bridging, deduplicate_items = off);
SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;

\d+ o_test_ix_ams
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

INSERT INTO o_test_ix_ams VALUES (10, ARRAY[20,30], point(40, 50), 60, 70);

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
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

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
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

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
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

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');
SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
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

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

ALTER TABLE o_test_ix_ams ADD COLUMN k int;
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

\d+ o_test_ix_ams
-- Don't update bridge_index when updated column not indexed by any bridged index
UPDATE o_test_ix_ams SET p = point(i * 40 + 1, i * 5);

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

-- Now it should update bridging_ctid-s
UPDATE o_test_ix_ams SET j = j/2 + 1000;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

-- Rows with new bridging_ctid now not stored in o_test_ix_ams_ix1
SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

-- After reindex new rows should be visible
REINDEX INDEX o_test_ix_ams_ix1;

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix1'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix1', p)
	ORDER BY ctid;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

CREATE INDEX o_test_ix_ams_ix2 on o_test_ix_ams using btree (k) WITH (index_bridging, deduplicate_items = off);

SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix2'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix2', p)
	ORDER BY ctid;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY k;
SELECT * FROM o_test_ix_ams ORDER BY k;
COMMIT;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

INSERT INTO o_test_ix_ams VALUES (1000, 2000, point(4000, 5000), 6000, 7000, 8000);

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

SELECT ctid, htid, tids FROM
		 generate_series(1,
						 (SELECT relpages - 1 FROM pg_class
							 WHERE oid = 'o_test_ix_ams_ix2'::regclass)) p,
		 LATERAL bt_page_items('o_test_ix_ams_ix2', p)
	ORDER BY ctid;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY k;
SELECT * FROM o_test_ix_ams ORDER BY k;
COMMIT;

DROP INDEX o_test_ix_ams_ix2;

UPDATE o_test_ix_ams SET k = j WHERE i < 1000;
UPDATE o_test_ix_ams SET k = 8000 WHERE i = 1000;
CREATE INDEX o_test_ix_ams_hash_ix ON o_test_ix_ams USING hash (k);
\d+ o_test_ix_ams
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

SELECT * FROM hash_page_items(get_raw_page('o_test_ix_ams_hash_ix', 1));
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams WHERE k = 8000;
SELECT * FROM o_test_ix_ams WHERE k = 8000;
COMMIT;

-- Test bitmap scans
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_indexscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams WHERE j < 2100;
SELECT * FROM o_test_ix_ams WHERE j < 2100;
COMMIT;

BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_indexscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams WHERE k = 8000;
SELECT * FROM o_test_ix_ams WHERE k = 8000;
COMMIT;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');
ALTER TABLE o_test_ix_ams ADD COLUMN r int4[];
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
SELECT attname, atthasmissing, atthasdef FROM pg_attribute WHERE attrelid = 'o_test_ix_ams'::regclass;
SELECT orioledb_table_description('o_test_ix_ams'::regclass);
\d+ o_test_ix_ams
SELECT *, r IS NULL FROM o_test_ix_ams;
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');
UPDATE o_test_ix_ams SET r = ARRAY[(i*8+j)%100, 11];
SELECT orioledb_table_description('o_test_ix_ams'::regclass);
\d+ o_test_ix_ams
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');
SELECT * FROM o_test_ix_ams;

CREATE INDEX o_test_ix_ams_ix3 ON o_test_ix_ams USING gin (r);

BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams WHERE r @> array[11, 11];
SELECT * FROM o_test_ix_ams WHERE r @> array[11, 11];
COMMIT;

CREATE INDEX o_test_ix_ams_ix4 ON o_test_ix_ams USING gist (p);

SELECT * FROM gist_page_items(get_raw_page('o_test_ix_ams_ix4', 0), 'o_test_ix_ams_ix4');
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT p FROM o_test_ix_ams WHERE p <@ box(point(0,0), point(4000, 5000));
SELECT p FROM o_test_ix_ams WHERE p <@ box(point(0,0), point(4000, 5000));
COMMIT;

CREATE TABLE o_briging_vacuum_test (id serial primary key, val float, p point) USING orioledb WITH (index_bridging);
INSERT INTO o_briging_vacuum_test (p) (SELECT point(0.01 * i, 0.02 * i) FROM generate_series(1,5) i);
SELECT orioledb_tbl_structure('o_briging_vacuum_test'::regclass, 'ne');
CREATE INDEX o_briging_vacuum_test_p_idx on o_briging_vacuum_test using gist(p);
DELETE FROM o_briging_vacuum_test;
VACUUM o_briging_vacuum_test;
SELECT * FROM o_briging_vacuum_test WHERE p <@ box(point(0,0), point(1,1));
SELECT orioledb_tbl_structure('o_briging_vacuum_test'::regclass, 'ne');
SELECT * FROM gist_page_items(get_raw_page('o_briging_vacuum_test_p_idx', 0), 'o_briging_vacuum_test_p_idx');
DROP TABLE o_briging_vacuum_test;


CREATE TABLE o_test_bridging_with_regular_no_pkey (
	i int NOT NULL,
	j int
) USING orioledb WITH (index_bridging);

CREATE INDEX o_test_bridging_with_regular_no_pkey_ix1 on
	o_test_bridging_with_regular_no_pkey using btree (j) WITH (index_bridging);
CREATE INDEX o_test_bridging_with_regular_no_pkey_ix2 on
	o_test_bridging_with_regular_no_pkey using btree (j);

INSERT INTO o_test_bridging_with_regular_no_pkey
	SELECT v, v FROM generate_series(1, 10) v;
ANALYZE o_test_bridging_with_regular_no_pkey;

SELECT orioledb_tbl_indices('o_test_bridging_with_regular_no_pkey'::regclass);
SELECT orioledb_tbl_structure('o_test_bridging_with_regular_no_pkey'::regclass, 'ne');

UPDATE o_test_bridging_with_regular_no_pkey SET j = j * 10 WHERE mod(i, 4) = 0;

DELETE FROM o_test_bridging_with_regular_no_pkey WHERE mod(i, 4) = 0;

SELECT orioledb_tbl_structure('o_test_bridging_with_regular_no_pkey'::regclass, 'ne');

CREATE TABLE o_test_bridging_with_regular_pkey (
	i int NOT NULL PRIMARY KEY,
	j int,
	k int
) USING orioledb WITH (index_bridging);

CREATE INDEX o_test_bridging_with_regular_pkey_ix1 on
	o_test_bridging_with_regular_pkey using btree (j) WITH (index_bridging);
CREATE INDEX o_test_bridging_with_regular_pkey_ix2 on
	o_test_bridging_with_regular_pkey using btree (k);
INSERT INTO o_test_bridging_with_regular_pkey
	SELECT v * 1000 + 673, 1000 - v * 100 + 15, v FROM generate_series(1, 10) v;
ANALYZE o_test_bridging_with_regular_pkey;

SELECT orioledb_tbl_structure('o_test_bridging_with_regular_pkey'::regclass, 'ne');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_bridging_with_regular_pkey ORDER BY k;
SELECT * FROM o_test_bridging_with_regular_pkey ORDER BY k;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_bridging_with_regular_pkey ORDER BY j;
SELECT * FROM o_test_bridging_with_regular_pkey ORDER BY j;
COMMIT;

DROP EXTENSION pageinspect;
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA index_bridging CASCADE;
RESET search_path;
