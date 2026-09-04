CREATE SCHEMA index_bridging;
SET SESSION search_path = 'index_bridging';
CREATE EXTENSION orioledb;
\set VERBOSITY terse
CREATE EXTENSION pageinspect;
\set VERBOSITY default

CREATE FUNCTION btree_index_content(index name)
	RETURNS TABLE (ctid tid, htid tid, tids tid[]) AS $$
	SELECT ctid, htid, tids FROM
			generate_series(2,
							(pg_relation_size(index::regclass) /
							current_setting('block_size')::BIGINT)) p,
			LATERAL bt_page_items(index, p - 1)
		ORDER BY ctid
$$ LANGUAGE SQL;

CREATE FUNCTION hash_index_content(index name) RETURNS TABLE (ctid tid) AS $$
	SELECT ctid FROM
		generate_series(2,
						(pg_relation_size(index::regclass) /
						 current_setting('block_size')::BIGINT)) p,
		LATERAL hash_page_type(get_raw_page(index, p - 1)) pt,
		LATERAL hash_page_items(get_raw_page(index, p - 1))
		WHERE pt = 'bucket'
		ORDER BY ctid
$$ LANGUAGE SQL;

CREATE FUNCTION gist_index_content(index name)
	RETURNS TABLE (ctid tid, keys text) AS $$
	SELECT ctid, keys FROM
			generate_series(1,
							(pg_relation_size(index::regclass) /
							current_setting('block_size')::BIGINT)) p,
			LATERAL gist_page_items(get_raw_page(index, p - 1), index::regclass)
		ORDER BY ctid
$$ LANGUAGE SQL;

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
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

\d+ o_test_ix_ams
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

INSERT INTO o_test_ix_ams VALUES (10, ARRAY[20,30], point(40, 50), 60, 70);

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

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
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

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
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

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
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

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
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;
SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

ALTER TABLE o_test_ix_ams ADD COLUMN k int;
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

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
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

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
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

-- After reindex new rows should be visible
REINDEX INDEX o_test_ix_ams_ix1;
SELECT * FROM btree_index_content('o_test_ix_ams_ix1');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY j;
SELECT * FROM o_test_ix_ams ORDER BY j;
COMMIT;

CREATE INDEX o_test_ix_ams_ix2 on o_test_ix_ams using btree (k) WITH (orioledb_index = off, deduplicate_items = off);

SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
SELECT * FROM btree_index_content('o_test_ix_ams_ix2');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_ix_ams ORDER BY k;
SELECT * FROM o_test_ix_ams ORDER BY k;
COMMIT;

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');

INSERT INTO o_test_ix_ams VALUES (1000, 2000, point(4000, 5000), 6000, 7000, 8000);

SELECT orioledb_tbl_structure('o_test_ix_ams'::regclass, 'ne');
SELECT * FROM btree_index_content('o_test_ix_ams_ix2');

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

SELECT * FROM hash_index_content('o_test_ix_ams_hash_ix');
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

SELECT * FROM gist_index_content('o_test_ix_ams_ix4');
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT p FROM o_test_ix_ams WHERE p <@ box(point(0,0), point(4000, 5000));
SELECT p FROM o_test_ix_ams WHERE p <@ box(point(0,0), point(4000, 5000));
COMMIT;

CREATE TABLE o_bridging_vacuum_test (id serial primary key, val float, p point) USING orioledb;
INSERT INTO o_bridging_vacuum_test (p) (SELECT point(0.01 * i, 0.02 * i) FROM generate_series(1,5) i);
SELECT orioledb_tbl_structure('o_bridging_vacuum_test'::regclass, 'ne');
CREATE INDEX o_bridging_vacuum_test_p_idx on o_bridging_vacuum_test using gist(p);
SELECT orioledb_tbl_structure('o_bridging_vacuum_test'::regclass, 'ne');
SELECT * FROM gist_index_content('o_bridging_vacuum_test_p_idx');
DELETE FROM o_bridging_vacuum_test;
SELECT orioledb_tbl_structure('o_bridging_vacuum_test'::regclass, 'ne');
SELECT * FROM gist_index_content('o_bridging_vacuum_test_p_idx');
VACUUM o_bridging_vacuum_test;
SELECT * FROM o_bridging_vacuum_test WHERE p <@ box(point(0,0), point(1,1));
SELECT orioledb_tbl_structure('o_bridging_vacuum_test'::regclass, 'ne');
SELECT * FROM gist_index_content('o_bridging_vacuum_test_p_idx');
DROP TABLE o_bridging_vacuum_test;


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
	SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[19,35];
SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[19,35];
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_bitmap_scans WHERE j <@ ARRAY[20, 36, 15];
SELECT * FROM o_test_bitmap_scans WHERE j <@ ARRAY[20, 36, 15];
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_bitmap_scans WHERE j > ARRAY[26, 42];
SELECT * FROM o_test_bitmap_scans WHERE j > ARRAY[26, 42];
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[19,35] OR j <@ ARRAY[20, 36, 15];
SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[19,35] OR j <@ ARRAY[20, 36, 15];
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[19,35] OR j <@ ARRAY[20, 36, 15] OR j > ARRAY[26, 42];
SELECT * FROM o_test_bitmap_scans WHERE j = ARRAY[19,35] OR j <@ ARRAY[20, 36, 15] OR j > ARRAY[26, 42];

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

CREATE TABLE o_test_bridged_hash_btree_bitmap_scans (
	id integer NOT NULL,
	t text NOT NULL,
	v int4[],
	PRIMARY KEY(id)
) USING orioledb;
SELECT orioledb_tbl_indices('o_test_bridged_hash_btree_bitmap_scans'::regclass, true);
CREATE INDEX o_test_bridged_hash_btree_bitmap_scans_ix1
	ON o_test_bridged_hash_btree_bitmap_scans USING hash (t);
CREATE INDEX o_test_bridged_hash_btree_bitmap_scans_ix2
	ON o_test_bridged_hash_btree_bitmap_scans USING hash (v);
CREATE INDEX o_test_bridged_hash_btree_bitmap_scans_ix3
	ON o_test_bridged_hash_btree_bitmap_scans USING btree (v) WITH (orioledb_index = off);
SELECT orioledb_tbl_indices('o_test_bridged_hash_btree_bitmap_scans'::regclass, true);

INSERT INTO o_test_bridged_hash_btree_bitmap_scans VALUES (1, 'D', ARRAY[47,16]);
INSERT INTO o_test_bridged_hash_btree_bitmap_scans VALUES (2, 'A', ARRAY[48,17]);
INSERT INTO o_test_bridged_hash_btree_bitmap_scans VALUES (3, 'C', ARRAY[49,18]);
INSERT INTO o_test_bridged_hash_btree_bitmap_scans VALUES (4, 'F', ARRAY[50,19]);
INSERT INTO o_test_bridged_hash_btree_bitmap_scans VALUES (5, 'B', ARRAY[51,20]);

SELECT orioledb_tbl_structure('o_test_bridged_hash_btree_bitmap_scans'::regclass, 'nue');
SELECT * FROM hash_index_content('o_test_bridged_hash_btree_bitmap_scans_ix1');
SELECT * FROM hash_index_content('o_test_bridged_hash_btree_bitmap_scans_ix2');
SELECT * FROM btree_index_content('o_test_bridged_hash_btree_bitmap_scans_ix3');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_bridged_hash_btree_bitmap_scans
		WHERE v = ARRAY[48, 17] OR v = ARRAY[49, 18];
SELECT * FROM o_test_bridged_hash_btree_bitmap_scans
	WHERE v = ARRAY[48, 17] OR v = ARRAY[49, 18];
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_bridged_hash_btree_bitmap_scans WHERE t = 'C';
SELECT * FROM o_test_bridged_hash_btree_bitmap_scans WHERE t = 'C';
COMMIT;

CREATE TABLE o_test_bridged_index_only_scan (
       id integer NOT NULL,
	   id2 integer,
       t text NOT NULL,
       PRIMARY KEY(id)
) USING orioledb;
SELECT orioledb_tbl_indices('o_test_bridged_index_only_scan'::regclass, true);
CREATE INDEX o_test_bridged_index_only_scan_ix1 ON o_test_bridged_index_only_scan USING btree (t) WITH (orioledb_index = off);
CREATE INDEX o_test_bridged_index_only_scan_ix2 ON o_test_bridged_index_only_scan USING btree (id2) WITH (orioledb_index = off);
SELECT orioledb_tbl_indices('o_test_bridged_index_only_scan'::regclass, true);

INSERT INTO o_test_bridged_index_only_scan VALUES (1, 56, 'x');
INSERT INTO o_test_bridged_index_only_scan VALUES (2, 27, repeat('x', 2600));

CREATE INDEX o_test_bridged_index_only_scan_ix1 ON o_test_bridged_index_only_scan USING btree (t) WITH (orioledb_index = off);
SELECT orioledb_tbl_structure('o_test_bridged_index_only_scan'::regclass, 'nue');
SELECT * FROM btree_index_content('o_test_bridged_index_only_scan_ix1');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_bridged_index_only_scan ORDER BY t;
SELECT * FROM o_test_bridged_index_only_scan ORDER BY t;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
	SELECT t FROM o_test_bridged_index_only_scan WHERE t = 'x';
SELECT t FROM o_test_bridged_index_only_scan WHERE t = 'x';
COMMIT;

CREATE FUNCTION generate_string(seed integer, length integer) RETURNS text
	AS $$
		SELECT substr(string_agg(
						substr(encode(sha256(seed::text::bytea || '_' || i::text::bytea), 'hex'), 1, 21),
				''), 1, length)
		FROM generate_series(1, (length + 20) / 21) i; $$
LANGUAGE SQL;

CREATE TABLE o_test_toast_with_bridged (
	id integer NOT NULL,
	t text NOT NULL,
	id2 integer
) USING orioledb;
SELECT orioledb_tbl_indices('o_test_toast_with_bridged'::regclass, true);
CREATE INDEX o_test_toast_with_bridged_ix1 ON o_test_toast_with_bridged USING btree (id) WITH (orioledb_index = off);
SELECT orioledb_tbl_indices('o_test_toast_with_bridged'::regclass, true);

INSERT INTO o_test_toast_with_bridged VALUES (1, 'x', 10);
INSERT INTO o_test_toast_with_bridged VALUES (2, repeat('x', 2600), 78);

INSERT INTO o_test_toast_with_bridged VALUES (3, generate_string(1, 2690), 66);
INSERT INTO o_test_toast_with_bridged VALUES (4, generate_string(2, 2690), 56);
INSERT INTO o_test_toast_with_bridged VALUES (5, generate_string(3, 2690), 12);

SELECT orioledb_tbl_structure('o_test_toast_with_bridged'::regclass, 'nue');
SELECT * FROM btree_index_content('o_test_toast_with_bridged_ix1');

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_toast_with_bridged ORDER BY id;
SELECT * FROM o_test_toast_with_bridged ORDER BY id;
COMMIT;

UPDATE o_test_toast_with_bridged SET t = generate_string(5, 2690) WHERE id = 4;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_toast_with_bridged ORDER BY id;
SELECT * FROM o_test_toast_with_bridged ORDER BY id;
COMMIT;

DELETE FROM o_test_toast_with_bridged WHERE id BETWEEN 2 AND 4;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_toast_with_bridged ORDER BY id;
SELECT * FROM o_test_toast_with_bridged ORDER BY id;
COMMIT;

-- Test bug https://github.com/orioledb/orioledb/issues/524
-- add a column to existing table
CREATE TABLE IF NOT EXISTS refresh_tokens();
ALTER TABLE refresh_tokens ADD COLUMN IF NOT EXISTS parent text;
-- create a separate table with default value
CREATE TABLE IF NOT EXISTS one_time_tokens (
  token_hash text,
  created_at timestamp without time zone NOT NULL DEFAULT now()
);
-- now create a hash index with bridge index
CREATE INDEX IF NOT EXISTS one_time_tokens_token_hash_hash_idx
	ON one_time_tokens USING hash (token_hash);
create table size_test(i1 int, i2 int, t text) using orioledb;
INSERT INTO size_test (SELECT id, id, generate_string(id, 3000) FROM generate_series(1, 1000) id);
create index size_test_i2_brin_idx on size_test using brin (i2);
select pg_size_pretty(pg_relation_size('size_test'::regclass));
select pg_size_pretty(pg_table_size('size_test'::regclass));
select pg_size_pretty(pg_indexes_size('size_test'::regclass));
select pg_size_pretty(pg_total_relation_size('size_test'::regclass));
select pg_size_pretty(pg_table_size('size_test_i2_brin_idx'::regclass));

CREATE TABLE tbl_with_pkey_bridged_toast (
	i1 int,
	i2 int,
	i3 int,
	i4 int,
	t text,
	PRIMARY KEY(i1)
) USING orioledb;

INSERT INTO tbl_with_pkey_bridged_toast
	(SELECT id, id, id * 12, id * 17 % 31 + id*2, generate_string(id, 3000) FROM generate_series(11, 650) id);

INSERT INTO tbl_with_pkey_bridged_toast
	(SELECT id, id, id * 12, id * 17 % 20, generate_string(id, 3000) FROM generate_series(1, 10) id);
CREATE INDEX tbl_with_pkey_bridged_toast_idx1 ON tbl_with_pkey_bridged_toast USING brin (i2) WITH (pages_per_range=1);
CREATE INDEX tbl_with_pkey_bridged_toast_idx2 ON tbl_with_pkey_bridged_toast USING hash (i3);
CREATE INDEX tbl_with_pkey_bridged_toast_idx3 ON tbl_with_pkey_bridged_toast USING btree (i4) WITH (orioledb_index=true);

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM tbl_with_pkey_bridged_toast
	WHERE
		i4 BETWEEN 8 AND 13
		-- ;
		OR
		i2 BETWEEN 299 AND 305
		-- ;
		OR
		i3 IN (7356, 6996, 7692)
		-- ;
		OR
		i4 BETWEEN 13 AND 17
		;
	ORDER BY i2;
SELECT * FROM tbl_with_pkey_bridged_toast
	WHERE
		i4 BETWEEN 8 AND 13
		-- ;
		OR
		i2 BETWEEN 299 AND 305
		-- ;
		OR
		i3 IN (7356, 6996, 7692)
		-- ;
		OR
		i4 BETWEEN 13 AND 17
		;
	ORDER BY i2;
COMMIT;

CREATE TABLE bitmap_test
(
	id serial primary key,
	i int4,
	j int4,
	k int4,
	h int4
) USING orioledb;

INSERT INTO bitmap_test (i, j, k, h)
	SELECT
		(v * 1032 + 1854) % 2000,
		(v * 5011 + 2159) % 2000,
		(v * 5011 + 2102) % 2000,
		(v * 4102 + 5857) % 2000
		FROM generate_series(1,5000) v;

CREATE INDEX bitmap_test_ix1 ON bitmap_test (i);
CREATE INDEX bitmap_test_ix2 ON bitmap_test (j);
CREATE INDEX bitmap_test_ix3 ON bitmap_test (k) WITH (orioledb_index=false);
CREATE INDEX bitmap_test_ix4 ON bitmap_test (h);
ANALYZE bitmap_test;

SET enable_seqscan = off;
SET cpu_tuple_cost = 0.05;

CREATE VIEW bitmap_test_mv AS (SELECT * FROM bitmap_test WHERE i < 100 AND h < 100 OR j < 100 LIMIT 20);
EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test_mv;
SELECT count(*) FROM bitmap_test_mv;
SELECT * FROM bitmap_test_mv;
DROP VIEW bitmap_test_mv;

CREATE VIEW bitmap_test_mv AS (SELECT * FROM bitmap_test WHERE i < 100 AND j < 100 OR h < 100 LIMIT 20);
EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test_mv;
SELECT count(*) FROM bitmap_test_mv;
SELECT * FROM bitmap_test_mv;
DROP VIEW bitmap_test_mv;

CREATE VIEW bitmap_test_mv AS (SELECT * FROM bitmap_test WHERE i < 100 AND j < 100 AND k < 200 OR h < 100 LIMIT 20);
EXPLAIN (COSTS OFF) SELECT count(*) FROM bitmap_test_mv;
SELECT count(*) FROM bitmap_test_mv;
SELECT * FROM bitmap_test_mv;
DROP VIEW bitmap_test_mv;

RESET enable_seqscan;
RESET cpu_tuple_cost;

CREATE TABLE test_no_bmscan_on_text_pkey (data1 text PRIMARY KEY, data2 text, data3 text, i int) USING orioledb;
CREATE INDEX ON test_no_bmscan_on_text_pkey USING spgist (data2);
INSERT INTO test_no_bmscan_on_text_pkey VALUES('foofoo','barbar', 'aaaaaa', 1);
INSERT INTO test_no_bmscan_on_text_pkey VALUES('mmm','nnn', 'ooo', 2);
SELECT data3 from test_no_bmscan_on_text_pkey where data3 = 'aaaaaa';

SELECT orioledb_tbl_indices('test_no_bmscan_on_text_pkey'::regclass, true);
EXPLAIN (COSTS OFF) SELECT data2 from test_no_bmscan_on_text_pkey where data2 = 'barbar';
SELECT data2 from test_no_bmscan_on_text_pkey where data2 = 'barbar';

-- Test bridge index behavior when table is truncated.
CREATE TABLE brin_test_multi (a INT, b BIGINT) using orioledb WITH (fillfactor=10) ;
INSERT INTO brin_test_multi
SELECT i/5 + mod(911 * i + 483, 25),
       i/10 + mod(751 * i + 221, 41)
  FROM generate_series(1,1000) s(i);

SELECT COUNT(*) FROM brin_test_multi WHERE a < 37;

CREATE INDEX brin_test_multi_idx ON brin_test_multi USING brin (a int4_minmax_multi_ops) WITH (pages_per_range=5);

SELECT orioledb_tbl_indices('brin_test_multi'::regclass, true);
\d+ brin_test_multi

TRUNCATE brin_test_multi;

SELECT orioledb_tbl_indices('brin_test_multi'::regclass, true);
\d+ brin_test_multi

INSERT INTO brin_test_multi
SELECT i/5 + mod(911 * i + 483, 25),
       i/10 + mod(751 * i + 221, 41)
  FROM generate_series(1,1000) s(i);

set enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM brin_test_multi WHERE a < 37;
SELECT COUNT(*) FROM brin_test_multi WHERE a < 37;

CREATE TEMP TABLE hash_i4_heap (
    seqno   int4,
    random  int4
) USING orioledb;

INSERT INTO hash_i4_heap VALUES (20000, 1492795354);

CREATE INDEX hash_i4_index ON hash_i4_heap USING hash (random int4_ops);

SELECT * FROM hash_index_content('hash_i4_index');
UPDATE hash_i4_heap
   SET seqno = 20000
 WHERE random = 1492795354;
SELECT * FROM hash_index_content('hash_i4_index');

BEGIN;
SET LOCAL enable_seqscan = OFF;
SET LOCAL enable_bitmapscan = OFF;
EXPLAIN (COSTS OFF) SELECT h.seqno AS i20000
  FROM hash_i4_heap h
 WHERE h.random = 1492795354;
SELECT h.seqno AS i20000
  FROM hash_i4_heap h
 WHERE h.random = 1492795354;
COMMIT;

SELECT * FROM hash_i4_heap ORDER BY seqno;

CREATE TEMP TABLE gin_test_table (
    id   serial PRIMARY KEY,
    tags int4[]
) USING orioledb;

INSERT INTO gin_test_table (id, tags) VALUES
(411, '{802, 654}'::int[]),
(412, '{814, 738}'::int[]);  -- This row will be updated

CREATE INDEX idx_gin_tags ON gin_test_table USING gin (tags);

UPDATE gin_test_table
SET id = 20000
WHERE tags @> '{814}';

BEGIN;
SET LOCAL enable_seqscan = OFF;
-- SET LOCAL enable_bitmapscan = OFF;
EXPLAIN (COSTS OFF) SELECT id, tags
FROM gin_test_table
WHERE tags @> '{814}';
SELECT id, tags
FROM gin_test_table
WHERE tags @> '{814}';
COMMIT;

-- Test partitioned with bridge index (brin). Test checks that bridge index can
-- be safely added before full building the concrete partition
CREATE TABLE tab1 (
	c text,
	a int PRIMARY KEY,
	b text
) PARTITION BY LIST (a);
CREATE INDEX tab1_c_brin_idx ON tab1 USING brin (c);
CREATE TABLE tab1_2_2 PARTITION OF tab1
	FOR VALUES IN (4, 6) USING orioledb;

INSERT INTO tab1 (c, a, b) VALUES
	('c1', 4, 'b1'),
	('c2', 6, 'b2');

SELECT * FROM tab1 ORDER BY a, b;

DROP TABLE tab1;

-- Same-txn CREATE TABLE / CREATE bridged-INDEX / INSERT / DELETE / ROLLBACK
-- exercises o_btree_modify_internal's "self-created" shortcut for the bridge
-- DELETE (needsUndoForSelfCreated=false), which used to leave the entry as
-- (deleted, InvalidUndoLocation|cmd).  The subsequent ROLLBACK then walked
-- the bridge INSERT undo record and re-entered page_item_rollback, tripping
-- Assert(UndoLocationIsValid(tuphdr->undoLocation)).
BEGIN;
CREATE TABLE bridge_undo_rollback (
	id int PRIMARY KEY,
	val text
) USING orioledb;
CREATE INDEX bridge_undo_rollback_idx ON bridge_undo_rollback(val)
	WITH (orioledb_index=false);
INSERT INTO bridge_undo_rollback VALUES (1, 'a');
DELETE FROM bridge_undo_rollback WHERE id = 1;
ROLLBACK;
SELECT 1 AS still_alive;

CREATE TABLE o_table_filled(pk text PRIMARY KEY, data text) USING orioledb;
INSERT INTO o_table_filled VALUES ('key1', 'some_data');
CREATE INDEX idx_filled_data ON o_table_filled USING brin(data);
DROP TABLE o_table_filled;

---
--- Bridge DML: basic DELETE and UPDATE
---

CREATE TABLE o_bridge_dml_gist (
	id int NOT NULL PRIMARY KEY, pos point, val int
) USING orioledb;
INSERT INTO o_bridge_dml_gist
	SELECT i, point(i, i), i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_dml_gist USING gist(pos);

SELECT count(*) FROM o_bridge_dml_gist;
DELETE FROM o_bridge_dml_gist WHERE id BETWEEN 200 AND 250;
SELECT count(*) FROM o_bridge_dml_gist;
UPDATE o_bridge_dml_gist SET val = val + 10000 WHERE id BETWEEN 100 AND 150;
SELECT count(*) FROM o_bridge_dml_gist WHERE val > 10000;
SELECT count(*) FROM o_bridge_dml_gist;

CREATE TABLE o_bridge_dml_spgist (
	id int NOT NULL PRIMARY KEY, pos point, val int
) USING orioledb;
INSERT INTO o_bridge_dml_spgist
	SELECT i, point(i, i), i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_dml_spgist USING spgist(pos);

SELECT count(*) FROM o_bridge_dml_spgist;
DELETE FROM o_bridge_dml_spgist WHERE id BETWEEN 200 AND 250;
SELECT count(*) FROM o_bridge_dml_spgist;
UPDATE o_bridge_dml_spgist SET val = val + 10000 WHERE id BETWEEN 100 AND 150;
SELECT count(*) FROM o_bridge_dml_spgist WHERE val > 10000;
SELECT count(*) FROM o_bridge_dml_spgist;

CREATE TABLE o_bridge_dml_gin (
	id int NOT NULL PRIMARY KEY, tags int[], val int
) USING orioledb;
INSERT INTO o_bridge_dml_gin
	SELECT i, ARRAY[i, i+1], i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_dml_gin USING gin(tags);

SELECT count(*) FROM o_bridge_dml_gin;
DELETE FROM o_bridge_dml_gin WHERE id BETWEEN 200 AND 250;
SELECT count(*) FROM o_bridge_dml_gin;
UPDATE o_bridge_dml_gin SET val = val + 10000 WHERE id BETWEEN 100 AND 150;
SELECT count(*) FROM o_bridge_dml_gin WHERE val > 10000;
SELECT count(*) FROM o_bridge_dml_gin;

CREATE TABLE o_bridge_dml_btree (
	id int NOT NULL PRIMARY KEY, ival int, val int
) USING orioledb;
INSERT INTO o_bridge_dml_btree
	SELECT i, i, i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_dml_btree USING btree(ival) WITH (orioledb_index=off);

SELECT count(*) FROM o_bridge_dml_btree;
DELETE FROM o_bridge_dml_btree WHERE id BETWEEN 200 AND 250;
SELECT count(*) FROM o_bridge_dml_btree;
UPDATE o_bridge_dml_btree SET val = val + 10000 WHERE id BETWEEN 100 AND 150;
SELECT count(*) FROM o_bridge_dml_btree WHERE val > 10000;
SELECT count(*) FROM o_bridge_dml_btree;

---
--- Bridge DML: multipage (wide rows force page splits)
---

CREATE TABLE o_bridge_big_gist (
	id int NOT NULL PRIMARY KEY, pad text, pos point
) USING orioledb;
INSERT INTO o_bridge_big_gist
	SELECT i, repeat('x', 200), point(i, i) FROM generate_series(1, 2000) i;
CREATE INDEX ON o_bridge_big_gist USING gist(pos);

SELECT count(*) FROM o_bridge_big_gist;
DELETE FROM o_bridge_big_gist WHERE id BETWEEN 500 AND 1500;
SELECT count(*) FROM o_bridge_big_gist;
UPDATE o_bridge_big_gist SET pad = 'updated' WHERE id BETWEEN 1 AND 200;
SELECT count(*) FROM o_bridge_big_gist WHERE pad = 'updated';

CREATE TABLE o_bridge_big_spgist (
	id int NOT NULL PRIMARY KEY, pad text, pos point
) USING orioledb;
INSERT INTO o_bridge_big_spgist
	SELECT i, repeat('x', 200), point(i, i) FROM generate_series(1, 2000) i;
CREATE INDEX ON o_bridge_big_spgist USING spgist(pos);

SELECT count(*) FROM o_bridge_big_spgist;
DELETE FROM o_bridge_big_spgist WHERE id BETWEEN 500 AND 1500;
SELECT count(*) FROM o_bridge_big_spgist;
UPDATE o_bridge_big_spgist SET pad = 'updated' WHERE id BETWEEN 1 AND 200;
SELECT count(*) FROM o_bridge_big_spgist WHERE pad = 'updated';

CREATE TABLE o_bridge_big_gin (
	id int NOT NULL PRIMARY KEY, pad text, tags int[]
) USING orioledb;
INSERT INTO o_bridge_big_gin
	SELECT i, repeat('x', 200), ARRAY[i, i+1] FROM generate_series(1, 2000) i;
CREATE INDEX ON o_bridge_big_gin USING gin(tags);

SELECT count(*) FROM o_bridge_big_gin;
DELETE FROM o_bridge_big_gin WHERE id BETWEEN 500 AND 1500;
SELECT count(*) FROM o_bridge_big_gin;
UPDATE o_bridge_big_gin SET pad = 'updated' WHERE id BETWEEN 1 AND 200;
SELECT count(*) FROM o_bridge_big_gin WHERE pad = 'updated';

CREATE TABLE o_bridge_big_btree (
	id int NOT NULL PRIMARY KEY, pad text, ival int
) USING orioledb;
INSERT INTO o_bridge_big_btree
	SELECT i, repeat('x', 200), i FROM generate_series(1, 2000) i;
CREATE INDEX ON o_bridge_big_btree USING btree(ival) WITH (orioledb_index=off);

SELECT count(*) FROM o_bridge_big_btree;
DELETE FROM o_bridge_big_btree WHERE id BETWEEN 500 AND 1500;
SELECT count(*) FROM o_bridge_big_btree;
UPDATE o_bridge_big_btree SET pad = 'updated' WHERE id BETWEEN 1 AND 200;
SELECT count(*) FROM o_bridge_big_btree WHERE pad = 'updated';

---
--- Bridge DML: composite primary key
---

CREATE TABLE o_bridge_cpk_gist (
	a int NOT NULL, b int NOT NULL, pos point, val int,
	PRIMARY KEY (a, b)
) USING orioledb;
INSERT INTO o_bridge_cpk_gist
	SELECT i / 10, i % 10, point(i, i), i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_cpk_gist USING gist(pos);

SELECT count(*) FROM o_bridge_cpk_gist;
DELETE FROM o_bridge_cpk_gist WHERE a BETWEEN 20 AND 30;
SELECT count(*) FROM o_bridge_cpk_gist;
UPDATE o_bridge_cpk_gist SET val = val + 10000 WHERE a BETWEEN 10 AND 15;
SELECT count(*) FROM o_bridge_cpk_gist WHERE val > 10000;
SELECT count(*) FROM o_bridge_cpk_gist;

CREATE TABLE o_bridge_cpk_spgist (
	a int NOT NULL, b int NOT NULL, pos point, val int,
	PRIMARY KEY (a, b)
) USING orioledb;
INSERT INTO o_bridge_cpk_spgist
	SELECT i / 10, i % 10, point(i, i), i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_cpk_spgist USING spgist(pos);

SELECT count(*) FROM o_bridge_cpk_spgist;
DELETE FROM o_bridge_cpk_spgist WHERE a BETWEEN 20 AND 30;
SELECT count(*) FROM o_bridge_cpk_spgist;
UPDATE o_bridge_cpk_spgist SET val = val + 10000 WHERE a BETWEEN 10 AND 15;
SELECT count(*) FROM o_bridge_cpk_spgist WHERE val > 10000;
SELECT count(*) FROM o_bridge_cpk_spgist;

CREATE TABLE o_bridge_cpk_gin (
	a int NOT NULL, b int NOT NULL, tags int[], val int,
	PRIMARY KEY (a, b)
) USING orioledb;
INSERT INTO o_bridge_cpk_gin
	SELECT i / 10, i % 10, ARRAY[i, i+1], i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_cpk_gin USING gin(tags);

SELECT count(*) FROM o_bridge_cpk_gin;
DELETE FROM o_bridge_cpk_gin WHERE a BETWEEN 20 AND 30;
SELECT count(*) FROM o_bridge_cpk_gin;
UPDATE o_bridge_cpk_gin SET val = val + 10000 WHERE a BETWEEN 10 AND 15;
SELECT count(*) FROM o_bridge_cpk_gin WHERE val > 10000;
SELECT count(*) FROM o_bridge_cpk_gin;

CREATE TABLE o_bridge_cpk_btree (
	a int NOT NULL, b int NOT NULL, ival int, val int,
	PRIMARY KEY (a, b)
) USING orioledb;
INSERT INTO o_bridge_cpk_btree
	SELECT i / 10, i % 10, i, i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_cpk_btree USING btree(ival) WITH (orioledb_index=off);

SELECT count(*) FROM o_bridge_cpk_btree;
DELETE FROM o_bridge_cpk_btree WHERE a BETWEEN 20 AND 30;
SELECT count(*) FROM o_bridge_cpk_btree;
UPDATE o_bridge_cpk_btree SET val = val + 10000 WHERE a BETWEEN 10 AND 15;
SELECT count(*) FROM o_bridge_cpk_btree WHERE val > 10000;
SELECT count(*) FROM o_bridge_cpk_btree;

---
--- Bridge DML: UPDATE of the indexed column
---

CREATE TABLE o_bridge_updidx_gist (
	id int NOT NULL PRIMARY KEY, pos point, val int
) USING orioledb;
INSERT INTO o_bridge_updidx_gist
	SELECT i, point(i, i), i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_updidx_gist USING gist(pos);

UPDATE o_bridge_updidx_gist SET pos = point(id + 1000, id + 1000)
	WHERE id BETWEEN 100 AND 200;
SELECT count(*) FROM o_bridge_updidx_gist WHERE id BETWEEN 100 AND 200;

CREATE TABLE o_bridge_updidx_spgist (
	id int NOT NULL PRIMARY KEY, pos point, val int
) USING orioledb;
INSERT INTO o_bridge_updidx_spgist
	SELECT i, point(i, i), i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_updidx_spgist USING spgist(pos);

UPDATE o_bridge_updidx_spgist SET pos = point(id + 1000, id + 1000)
	WHERE id BETWEEN 100 AND 200;
SELECT count(*) FROM o_bridge_updidx_spgist WHERE id BETWEEN 100 AND 200;

CREATE TABLE o_bridge_updidx_gin (
	id int NOT NULL PRIMARY KEY, tags int[], val int
) USING orioledb;
INSERT INTO o_bridge_updidx_gin
	SELECT i, ARRAY[i, i+1], i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_updidx_gin USING gin(tags);

UPDATE o_bridge_updidx_gin SET tags = ARRAY[id + 1000]
	WHERE id BETWEEN 100 AND 200;
SELECT count(*) FROM o_bridge_updidx_gin WHERE id BETWEEN 100 AND 200;

CREATE TABLE o_bridge_updidx_btree (
	id int NOT NULL PRIMARY KEY, ival int, val int
) USING orioledb;
INSERT INTO o_bridge_updidx_btree
	SELECT i, i, i FROM generate_series(1, 500) i;
CREATE INDEX ON o_bridge_updidx_btree USING btree(ival) WITH (orioledb_index=off);

UPDATE o_bridge_updidx_btree SET ival = ival + 1000
	WHERE id BETWEEN 100 AND 200;
SELECT count(*) FROM o_bridge_updidx_btree WHERE id BETWEEN 100 AND 200;

-- Secondary bridged index query tests: verify that queries through the
-- bridged secondary index return correct results after DML on the table.

CREATE TABLE o_bridge_sec_gist (
	id int NOT NULL PRIMARY KEY,
	pos point,
	val int
) USING orioledb;
INSERT INTO o_bridge_sec_gist
	SELECT i, point(i, i), i FROM generate_series(1, 300) i;
CREATE INDEX ON o_bridge_sec_gist USING gist(pos);

SET enable_seqscan = off;
SELECT count(*) FROM o_bridge_sec_gist WHERE pos <@ box '(0,0),(300,300)';
DELETE FROM o_bridge_sec_gist WHERE id BETWEEN 200 AND 250;
SELECT count(*) FROM o_bridge_sec_gist WHERE pos <@ box '(0,0),(300,300)';
UPDATE o_bridge_sec_gist SET pos = point(id + 1000, id + 1000)
	WHERE id BETWEEN 100 AND 150;
SELECT count(*) FROM o_bridge_sec_gist WHERE pos <@ box '(0,0),(300,300)';
RESET enable_seqscan;

CREATE TABLE o_bridge_sec_spgist (
	id int NOT NULL PRIMARY KEY,
	pos point,
	val int
) USING orioledb;
INSERT INTO o_bridge_sec_spgist
	SELECT i, point(i, i), i FROM generate_series(1, 300) i;
CREATE INDEX ON o_bridge_sec_spgist USING spgist(pos);

SET enable_seqscan = off;
SELECT count(*) FROM o_bridge_sec_spgist WHERE pos <@ box '(0,0),(300,300)';
DELETE FROM o_bridge_sec_spgist WHERE id BETWEEN 200 AND 250;
SELECT count(*) FROM o_bridge_sec_spgist WHERE pos <@ box '(0,0),(300,300)';
UPDATE o_bridge_sec_spgist SET pos = point(id + 1000, id + 1000)
	WHERE id BETWEEN 100 AND 150;
SELECT count(*) FROM o_bridge_sec_spgist WHERE pos <@ box '(0,0),(300,300)';
RESET enable_seqscan;

CREATE TABLE o_bridge_sec_gin (
	id int NOT NULL PRIMARY KEY,
	tags int[],
	val int
) USING orioledb;
INSERT INTO o_bridge_sec_gin
	SELECT i, ARRAY[i, i+1], i FROM generate_series(1, 300) i;
CREATE INDEX ON o_bridge_sec_gin USING gin(tags);

SET enable_seqscan = off;
SELECT count(*) FROM o_bridge_sec_gin WHERE tags @> ARRAY[150];
DELETE FROM o_bridge_sec_gin WHERE id BETWEEN 200 AND 250;
SELECT count(*) FROM o_bridge_sec_gin WHERE tags @> ARRAY[150];
UPDATE o_bridge_sec_gin SET tags = ARRAY[id + 1000]
	WHERE id BETWEEN 100 AND 150;
SELECT count(*) FROM o_bridge_sec_gin WHERE tags @> ARRAY[150];
RESET enable_seqscan;

CREATE TABLE o_bridge_sec_btree (
	id int NOT NULL PRIMARY KEY,
	ival int,
	val int
) USING orioledb;
INSERT INTO o_bridge_sec_btree
	SELECT i, i, i FROM generate_series(1, 300) i;
CREATE INDEX ON o_bridge_sec_btree USING btree(ival) WITH (orioledb_index=off);

SET enable_seqscan = off;
SELECT count(*) FROM o_bridge_sec_btree WHERE ival BETWEEN 100 AND 200;
DELETE FROM o_bridge_sec_btree WHERE id BETWEEN 150 AND 180;
SELECT count(*) FROM o_bridge_sec_btree WHERE ival BETWEEN 100 AND 200;
UPDATE o_bridge_sec_btree SET ival = ival + 1000
	WHERE id BETWEEN 100 AND 120;
SELECT count(*) FROM o_bridge_sec_btree WHERE ival BETWEEN 100 AND 200;
RESET enable_seqscan;

-- ON CONFLICT on a table with a bridged index.  OrioleDB resolves conflicts on
-- its own indexes by inserting into them, so by the time the bridged arbiters
-- are checked the row is already in those trees: checking all the arbiters
-- there used to find our own row and call it a conflict.  DO NOTHING then left
-- the row in the table but out of every bridged index, and DO UPDATE failed
-- with "unexpected self-updated tuple".
CREATE TABLE o_bridge_ioc (
	id int NOT NULL PRIMARY KEY,
	tags int[]
) USING orioledb;
CREATE INDEX ON o_bridge_ioc USING gin(tags);

INSERT INTO o_bridge_ioc VALUES (1, ARRAY[10]) ON CONFLICT (id) DO NOTHING;
INSERT INTO o_bridge_ioc VALUES (2, ARRAY[10])
	ON CONFLICT (id) DO UPDATE SET tags = EXCLUDED.tags;
INSERT INTO o_bridge_ioc VALUES (3, ARRAY[10]);

-- and again, now that the keys really do conflict
INSERT INTO o_bridge_ioc VALUES (1, ARRAY[99]) ON CONFLICT (id) DO NOTHING;
INSERT INTO o_bridge_ioc VALUES (2, ARRAY[20])
	ON CONFLICT (id) DO UPDATE SET tags = EXCLUDED.tags;

-- without a conflict target, so the arbiter list arrives empty
INSERT INTO o_bridge_ioc VALUES (4, ARRAY[10]) ON CONFLICT DO NOTHING;
INSERT INTO o_bridge_ioc VALUES (4, ARRAY[77]) ON CONFLICT DO NOTHING;

SELECT id, tags FROM o_bridge_ioc ORDER BY id;
SET enable_seqscan = off;
SELECT id FROM o_bridge_ioc WHERE tags @> ARRAY[10] ORDER BY id;
SELECT id FROM o_bridge_ioc WHERE tags @> ARRAY[20] ORDER BY id;
RESET enable_seqscan;

-- One statement, some rows changing a bridged index column and some not.  The
-- slot is reused for every row, and the flag saying "this row got a new bridge
-- ctid" used to survive into the next one -- so a row that kept its ctid was
-- inserted into the bridged index a second time under it.  GIN's build
-- accumulator asserts on a repeated tid, so on an assert build the UPDATE
-- below took the server down at the next pending-list cleanup.
CREATE TABLE o_bridge_mixed (
	id int NOT NULL PRIMARY KEY,
	tags int[],
	other int
) USING orioledb;
CREATE INDEX ON o_bridge_mixed USING gin(tags);
INSERT INTO o_bridge_mixed
	SELECT i, ARRAY[i], i FROM generate_series(1, 4) i;

UPDATE o_bridge_mixed
   SET tags = CASE WHEN id % 2 = 1 THEN tags || 99 ELSE tags END
 WHERE other > 0;
VACUUM o_bridge_mixed;

SET enable_seqscan = off;
SELECT id FROM o_bridge_mixed WHERE tags @> ARRAY[99] ORDER BY id;
SELECT id FROM o_bridge_mixed WHERE tags @> ARRAY[2] ORDER BY id;
RESET enable_seqscan;

-- A bitmap qual can pair a bridged index with the primary one.  The bridged
-- arm puts the scan on the TIDBitmap path, and the primary arm then has to
-- contribute its own bridge pointers there -- it used to assert instead.
CREATE TABLE o_bridge_pk (
	id int PRIMARY KEY,
	tags int[]
) USING orioledb;
INSERT INTO o_bridge_pk
	SELECT g, ARRAY[g % 7, g % 11] FROM generate_series(1, 2000) g;
CREATE INDEX o_bridge_pk_gin ON o_bridge_pk USING gin (tags);
ANALYZE o_bridge_pk;

SET enable_seqscan = off;
SET enable_indexscan = off;
SELECT count(*), sum(id) FROM o_bridge_pk
	WHERE tags @> ARRAY[3] OR id IN (5, 50, 500, 1500, 1999);
RESET enable_indexscan;
RESET enable_seqscan;
-- same answer without the bitmap path
SELECT count(*), sum(id) FROM o_bridge_pk
	WHERE tags @> ARRAY[3] OR id IN (5, 50, 500, 1500, 1999);
DROP TABLE o_bridge_pk;

-- the same, with a composite primary key: the densified uint64 bitmap key only
-- carries one column, so this has to go through the fixed-width encoding --
-- in the bridge path as well, which builds its own bitmap
CREATE TABLE o_bridge_pk2 (
	a int,
	b int,
	tags int[],
	PRIMARY KEY (a, b)
) USING orioledb;
INSERT INTO o_bridge_pk2
	SELECT g / 50, g % 50, ARRAY[g % 7, g % 11] FROM generate_series(1, 2000) g;
CREATE INDEX o_bridge_pk2_gin ON o_bridge_pk2 USING gin (tags);
ANALYZE o_bridge_pk2;

SET enable_seqscan = off;
SET enable_indexscan = off;
SELECT count(*), sum(a * 1000 + b) FROM o_bridge_pk2
	WHERE tags @> ARRAY[3] OR (a, b) IN ((0, 5), (7, 10), (20, 40), (39, 49));
RESET enable_indexscan;
RESET enable_seqscan;
SELECT count(*), sum(a * 1000 + b) FROM o_bridge_pk2
	WHERE tags @> ARRAY[3] OR (a, b) IN ((0, 5), (7, 10), (20, 40), (39, 49));
DROP TABLE o_bridge_pk2;

DROP EXTENSION pageinspect;
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA index_bridging CASCADE;
RESET search_path;
