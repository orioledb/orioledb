CREATE SCHEMA index_bridging;
SET SESSION search_path = 'index_bridging';
CREATE EXTENSION orioledb;
\set VERBOSITY terse
CREATE EXTENSION pageinspect;
\set VERBOSITY default

CREATE TABLE o_test_ix_ams (
	i int NOT NULL,
	j int4[],
	p point,
	pk1 int,
	pk2 int
) USING orioledb;

SELECT orioledb_table_description('o_test_ix_ams'::regclass);
\d+ o_test_ix_ams
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

INSERT INTO o_test_ix_ams VALUES (1, ARRAY[2,3], point(4, 5), 6, 7);
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams;
SELECT * FROM o_test_ix_ams;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

CREATE INDEX o_test_ix_ams_ix1 on o_test_ix_ams using btree (j) WITH (orioledb_index = off, deduplicate_items = off);
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

CREATE INDEX o_test_ix_ams_ix2 on o_test_ix_ams using btree (k) WITH (orioledb_index = off, deduplicate_items = off);

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

CREATE TABLE o_briging_vacuum_test (id serial primary key, val float, p point) USING orioledb;
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
	o_test_bridging_with_regular_no_pkey using btree (j) WITH (orioledb_index = off);
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
	o_test_bridging_with_regular_pkey using btree (j) WITH (orioledb_index = off);
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

CREATE TABLE o_test_bitmap_scans (
	i int NOT NULL,
	j int4[],
	j2 int4[],
	p point
) USING orioledb WITH (index_bridging);

INSERT INTO o_test_bitmap_scans
	SELECT v, ARRAY[v+17,v+33], ARRAY[v+66,v+95], point(v + 5, v + 5) FROM generate_series(1, 10) v;

CREATE INDEX o_test_bitmap_scans_ix1 on o_test_bitmap_scans using hash (j);
CREATE INDEX o_test_bitmap_scans_ix2 on o_test_bitmap_scans using btree (j);
CREATE INDEX o_test_bitmap_scans_ix3 on o_test_bitmap_scans using gin (j);
CREATE INDEX o_test_bitmap_scans_ix4 on o_test_bitmap_scans using gist (p);
CREATE INDEX o_test_bitmap_scans_ix5 on o_test_bitmap_scans using btree (j2) WITH (orioledb_index = off);

BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_indexscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[22,38] OR j = ARRAY[24, 40] OR j > ARRAY[25, 25];
SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[22,38] OR j = ARRAY[24, 40] OR j > ARRAY[25, 25];
COMMIT;

BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_indexscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[19,35] OR j <@ ARRAY[20, 20] OR j > ARRAY[26, 42];
SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[19,35] OR j <@ ARRAY[20, 20] OR j > ARRAY[26, 42];

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_bitmap_scans
		WHERE  (j > ARRAY[25, 41] OR j2 < ARRAY[68,97]) OR p <@ box(point(8,8), point(10, 10));
SELECT * FROM o_test_bitmap_scans
	WHERE (j > ARRAY[25, 41] OR j2 < ARRAY[68,97]) OR p <@ box(point(8,8), point(10, 10));

COMMIT;

CREATE TABLE o_test_index_bridging_options (
	i int NOT NULL,
	j int4[],
	p point
) USING orioledb WITH (index_bridging);

CREATE INDEX o_test_index_bridging_options_ix1 ON o_test_index_bridging_options USING btree (i);
\d+ o_test_index_bridging_options
CREATE INDEX o_test_index_bridging_options_ix2 ON o_test_index_bridging_options USING btree (i) WITH (orioledb_index = off);
\d+ o_test_index_bridging_options
CREATE INDEX o_test_index_bridging_options_ix3 ON o_test_index_bridging_options USING hash (i);
\d+ o_test_index_bridging_options
CREATE INDEX o_test_index_bridging_options_ix4 ON o_test_index_bridging_options USING hash (i) WITH (fillfactor=65);
\d+ o_test_index_bridging_options
CREATE INDEX o_test_index_bridging_options_ix5 ON o_test_index_bridging_options USING gist (p);
\d+ o_test_index_bridging_options
CREATE INDEX o_test_index_bridging_options_ix6 ON o_test_index_bridging_options USING gin (j);
\d+ o_test_index_bridging_options
CREATE INDEX o_test_index_bridging_options_ix7 ON o_test_index_bridging_options USING spgist (p);
\d+ o_test_index_bridging_options
CREATE INDEX o_test_index_bridging_options_ix8 ON o_test_index_bridging_options USING brin (i);
\d+ o_test_index_bridging_options

ALTER TABLE o_test_index_bridging_options RESET (index_bridging);
\d+ o_test_index_bridging_options
ALTER TABLE o_test_index_bridging_options SET (index_bridging);
\d+ o_test_index_bridging_options
ALTER INDEX o_test_index_bridging_options_ix2 RESET (orioledb_index);
\d+ o_test_index_bridging_options
ALTER INDEX o_test_index_bridging_options_ix1 SET (orioledb_index = off);
\d+ o_test_index_bridging_options

CREATE TABLE o_test_non_index_bridging_options (
	i int NOT NULL,
	j int4[],
	p point
) USING orioledb;
\d+ o_test_non_index_bridging_options
SELECT orioledb_tbl_indices('o_test_non_index_bridging_options'::regclass, true);

INSERT INTO o_test_non_index_bridging_options
	SELECT v, ARRAY[v+17,v+33], point(v + 5, v + 5) FROM generate_series(1, 10) v;
SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');
SELECT * FROM o_test_non_index_bridging_options;

ALTER TABLE o_test_non_index_bridging_options RESET (index_bridging);
\d+ o_test_non_index_bridging_options
SELECT orioledb_tbl_indices('o_test_non_index_bridging_options'::regclass, true);
SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');
SELECT * FROM o_test_non_index_bridging_options;

ALTER TABLE o_test_non_index_bridging_options SET (index_bridging);
\d+ o_test_non_index_bridging_options
SELECT orioledb_tbl_indices('o_test_non_index_bridging_options'::regclass, true);
SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');
SELECT * FROM o_test_non_index_bridging_options;

CREATE INDEX o_test_non_index_bridging_options_ix2 ON o_test_non_index_bridging_options USING btree (i) WITH (orioledb_index = off);
\d+ o_test_non_index_bridging_options
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_non_index_bridging_options ORDER BY i;
SELECT * FROM o_test_non_index_bridging_options ORDER BY i;
COMMIT;

ALTER TABLE o_test_non_index_bridging_options RESET (index_bridging);
\d+ o_test_non_index_bridging_options
DROP INDEX o_test_non_index_bridging_options_ix2;

ALTER TABLE o_test_non_index_bridging_options RESET (index_bridging);
\d+ o_test_non_index_bridging_options
SELECT orioledb_tbl_indices('o_test_non_index_bridging_options'::regclass, true);
SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');
SELECT * FROM o_test_non_index_bridging_options;

INSERT INTO o_test_non_index_bridging_options
	SELECT v, ARRAY[v+17,v+33], point(v + 5, v + 5) FROM generate_series(11, 15) v;
SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');
SELECT * FROM o_test_non_index_bridging_options;

CREATE INDEX o_test_non_index_bridging_options_ix1 ON o_test_non_index_bridging_options USING btree (i);
\d+ o_test_non_index_bridging_options
CREATE INDEX o_test_non_index_bridging_options_ix2 ON o_test_non_index_bridging_options USING btree (i) WITH (orioledb_index = off);
\d+ o_test_non_index_bridging_options

ALTER INDEX o_test_non_index_bridging_options_ix2 RESET (orioledb_index);
\d+ o_test_non_index_bridging_options
ALTER INDEX o_test_non_index_bridging_options_ix1 SET (orioledb_index = off);
\d+ o_test_non_index_bridging_options

ALTER TABLE o_test_non_index_bridging_options SET (index_bridging);
\d+ o_test_non_index_bridging_options
SELECT orioledb_tbl_indices('o_test_non_index_bridging_options'::regclass, true);
ALTER TABLE o_test_non_index_bridging_options RESET (index_bridging);
\d+ o_test_non_index_bridging_options
SELECT orioledb_tbl_indices('o_test_non_index_bridging_options'::regclass, true);

SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');
SELECT * FROM o_test_non_index_bridging_options;

ALTER TABLE o_test_non_index_bridging_options ADD PRIMARY KEY (i);
\d+ o_test_non_index_bridging_options
SELECT orioledb_tbl_indices('o_test_non_index_bridging_options'::regclass, true);
SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');
SELECT * FROM o_test_non_index_bridging_options;

INSERT INTO o_test_non_index_bridging_options
	SELECT v, ARRAY[v+17,v+33], point(v + 5, v + 5) FROM generate_series(16, 20) v;
SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');

DROP INDEX o_test_non_index_bridging_options_ix1;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_non_index_bridging_options ORDER BY i;
SELECT * FROM o_test_non_index_bridging_options ORDER BY i;
COMMIT;

DROP INDEX o_test_non_index_bridging_options_ix2;

ALTER TABLE o_test_non_index_bridging_options RESET (index_bridging);
\d+ o_test_non_index_bridging_options
ALTER TABLE o_test_non_index_bridging_options DROP CONSTRAINT o_test_non_index_bridging_options_pkey;
\d+ o_test_non_index_bridging_options
SELECT orioledb_tbl_indices('o_test_non_index_bridging_options'::regclass, true);
SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');
SELECT * FROM o_test_non_index_bridging_options;

INSERT INTO o_test_non_index_bridging_options
	SELECT v, ARRAY[v+17,v+33], point(v + 5, v + 5) FROM generate_series(21, 25) v;
SELECT orioledb_tbl_structure('o_test_non_index_bridging_options'::regclass, 'ne');

CREATE INDEX o_test_non_index_bridging_options_ix2 ON o_test_non_index_bridging_options USING btree (i) WITH (orioledb_index = off);
\d+ o_test_non_index_bridging_options
SELECT orioledb_tbl_indices('o_test_non_index_bridging_options'::regclass, true);
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_non_index_bridging_options ORDER BY i;
SELECT * FROM o_test_non_index_bridging_options ORDER BY i;
COMMIT;

create table o_test_toastable_offset(i int, b int, a text, m int) using orioledb;
alter table o_test_toastable_offset set (index_bridging=on);
insert into o_test_toastable_offset values (4,6,'X',11);
insert into o_test_toastable_offset values (1,3,'Y',12);
delete from o_test_toastable_offset;

DROP EXTENSION pageinspect;
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA index_bridging CASCADE;
RESET search_path;
