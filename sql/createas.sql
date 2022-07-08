CREATE EXTENSION orioledb;

CREATE TABLE o_test_create_as (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00));
-- Should fail - exists
CREATE TABLE o_test_create_as (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 4, 100.00), (100, 3, 1, 200.00));
SELECT * FROM o_test_create_as;

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

CREATE SEQUENCE o_matview_seq;
-- TODO: Implement refresh of materialized view and add tests
-- TODO: Implement indices on materialized views and add tests
-- TODO: Implement alters of materialized view and add tests
CREATE MATERIALIZED VIEW o_test_matview (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 4, nextval('o_matview_seq'::regclass)),
							  (100, 3, 1, nextval('o_matview_seq'::regclass)));
SELECT * FROM o_test_matview;
REFRESH MATERIALIZED VIEW o_test_matview;
SELECT * FROM o_test_matview;

DROP EXTENSION orioledb CASCADE;
