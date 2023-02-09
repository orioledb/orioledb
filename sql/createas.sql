CREATE SCHEMA createas;
SET SESSION search_path = 'createas';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_create_as (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00));
SELECT orioledb_tbl_indices('o_test_create_as'::regclass);
-- Should fail - exists
CREATE TABLE o_test_create_as (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00));
SELECT * FROM o_test_create_as;

BEGIN;
CREATE TABLE o_test_create_as_abort (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00));
SELECT * FROM o_test_create_as_abort;
ROLLBACK;

CREATE TABLE o_test_create_as_less_atts (order_id, item_id)
	USING orioledb AS (VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00));
SELECT * FROM o_test_create_as_less_atts;

CREATE TABLE o_test_create_as_no_atts
	USING orioledb AS (VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00));
SELECT * FROM o_test_create_as_no_atts;

CREATE TABLE o_test_create_as_with_compress (order_id, item_id, quantity, price)
	USING orioledb WITH (compress = 1) AS
		(VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00));
SELECT orioledb_tbl_indices('o_test_create_as_with_compress'::regclass);
SELECT * FROM o_test_create_as_with_compress;

CREATE TABLE o_test_create_as_with_data (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00))
	WITH DATA;
SELECT * FROM o_test_create_as_with_data;
SELECT relname FROM orioledb_table_oids()
	JOIN pg_class ON reloid = oid WHERE relname = 'o_test_create_as_with_data';
DROP TABLE o_test_create_as_with_data;
SELECT * FROM o_test_create_as_with_data;
SELECT relname FROM orioledb_table_oids()
	JOIN pg_class ON reloid = oid WHERE relname = 'o_test_create_as_with_data';

CREATE TABLE o_test_create_as_no_data (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00))
	WITH NO DATA;
SELECT * FROM o_test_create_as_no_data;
INSERT INTO o_test_create_as_no_data
	VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00);
SELECT * FROM o_test_create_as_no_data;

-- EXPLAIN ANALYZE tests

-- Wrapper function, which converts result of SQL query to the text
CREATE OR REPLACE FUNCTION query_to_text(sql TEXT, out result text)
	RETURNS SETOF TEXT AS $$
	BEGIN
		FOR result IN EXECUTE sql LOOP
			RETURN NEXT;
		END LOOP;
	END $$
LANGUAGE plpgsql;

BEGIN;
SET LOCAL default_table_access_method = 'orioledb';
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
	FROM query_to_text($$
		EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
			SELECT * INTO tbl_into FROM generate_series(1,3) a; $$) as t;
COMMIT;

BEGIN;
SET LOCAL default_table_access_method = 'orioledb';
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
	FROM query_to_text($$
		EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
			CREATE TABLE tbl_as_nodata (a)
				AS SELECT generate_series(1,3) WITH NO DATA; $$) as t;
SELECT * FROM tbl_as_nodata;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
	FROM query_to_text($$
		EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
			CREATE TABLE tbl_as_nodata (a)
				AS SELECT generate_series(1,3) WITH NO DATA; $$) as t;
ROLLBACK;

BEGIN;
SET LOCAL default_table_access_method = 'orioledb';
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
	FROM query_to_text($$
		EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
			CREATE TABLE tbl_as_nodata (a)
				AS SELECT generate_series(1,3) WITH NO DATA; $$) as t;
COMMIT;

BEGIN;
SET LOCAL default_table_access_method = 'orioledb';
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
	FROM query_to_text($$
		EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
			CREATE TABLE tbl_as_data (a)
				AS SELECT generate_series(1,3) WITH DATA; $$) as t;
SELECT * FROM tbl_as_data;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
	FROM query_to_text($$
		EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
			CREATE TABLE tbl_as_data (a)
				AS SELECT generate_series(1,3) WITH DATA; $$) as t;
ROLLBACK;

BEGIN;
SET LOCAL default_table_access_method = 'orioledb';
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
	FROM query_to_text($$
		EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
			CREATE TABLE tbl_as_data (a)
				AS SELECT generate_series(1,3) WITH DATA; $$) as t;
COMMIT;

SELECT * FROM tbl_into;
SELECT * FROM tbl_as_nodata;
SELECT * FROM tbl_as_data;

CREATE SEQUENCE o_matview_seq;
CREATE MATERIALIZED VIEW o_test_matview (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 'a',
							   nextval('o_matview_seq'::regclass)),
							  (100, 3, 'b',
							   nextval('o_matview_seq'::regclass)));
SELECT orioledb_tbl_indices('o_test_matview'::regclass);
SELECT * FROM o_test_matview;
SELECT * FROM o_test_matview;
REFRESH MATERIALIZED VIEW o_test_matview;
SELECT orioledb_tbl_indices('o_test_matview'::regclass);
SELECT * FROM o_test_matview;
SELECT * FROM o_test_matview;
REFRESH MATERIALIZED VIEW o_test_matview;
SELECT * FROM o_test_matview;
SELECT * FROM o_test_matview;

CREATE INDEX o_test_matview_ix1 ON o_test_matview (item_id DESC);
SELECT orioledb_tbl_indices('o_test_matview'::regclass);
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_matview ORDER BY item_id DESC;
SELECT * FROM o_test_matview ORDER BY item_id DESC;
REINDEX INDEX o_test_matview_ix1;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_matview ORDER BY item_id DESC;
SELECT * FROM o_test_matview ORDER BY item_id DESC;

CREATE INDEX o_test_matview_ix2 ON o_test_matview (quantity);
SELECT orioledb_tbl_indices('o_test_matview'::regclass);
ALTER MATERIALIZED VIEW o_test_matview RENAME quantity TO quantity2;
SELECT orioledb_tbl_indices('o_test_matview'::regclass);
SELECT orioledb_table_description('o_test_matview'::regclass);
\d+ o_test_matview
EXPLAIN (COSTS OFF) SELECT * FROM o_test_matview ORDER BY quantity2;
SELECT * FROM o_test_matview ORDER BY quantity2;

CREATE MATERIALIZED VIEW o_test_matview_no_data
	(order_id, item_id, quantity, price) USING orioledb
	AS (VALUES (100, 1, 4, nextval('o_matview_seq'::regclass)),
			   (100, 3, 1, nextval('o_matview_seq'::regclass)))
	WITH NO DATA;

SELECT relname FROM orioledb_table_oids() JOIN pg_class ON reloid = oid
	WHERE relname = 'o_test_matview_no_data';

SELECT * FROM o_test_matview_no_data;
REFRESH MATERIALIZED VIEW o_test_matview_no_data;
SELECT * FROM o_test_matview_no_data;
DROP MATERIALIZED VIEW o_test_matview_no_data;

SELECT relname FROM orioledb_table_oids() JOIN pg_class ON reloid = oid
	WHERE relname = 'o_test_matview_no_data';

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA createas CASCADE;
RESET search_path;
