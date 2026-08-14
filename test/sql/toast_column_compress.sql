CREATE SCHEMA toast_column_compress;
SET SESSION search_path = 'toast_column_compress';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_2 (
	key int,
	val text COMPRESSION pglz
) USING orioledb;

INSERT INTO o_test_2 VALUES (1, repeat('a', 4000) || repeat('b', 4000));
INSERT INTO o_test_2 VALUES (2, repeat('a', 150000) || repeat('b', 150000));
INSERT INTO o_test_2 VALUES (3, (SELECT array_agg(md5(g::text))::text
									FROM generate_series(1, 256) g));
SELECT * FROM o_test_2;

SELECT octet_length(val), pg_column_size(val),
	   pg_column_compression(val) FROM o_test_2 WHERE key = 1;
SELECT octet_length(val), pg_column_size(val),
	   pg_column_compression(val) FROM o_test_2 WHERE key = 2;
SELECT octet_length(val), pg_column_size(val),
	   pg_column_compression(val) FROM o_test_2 WHERE key = 3;

SELECT val FROM o_test_2 WHERE key = 1;
SELECT val FROM o_test_2 WHERE key = 2;
SELECT val FROM o_test_2 WHERE key = 3;

SELECT key, length(val), substring(val for 30),
	   substr(val, 10, 10) from o_test_2 WHERE key = 1;
SELECT key, length(val), substring(val for 30),
	   substr(val, 20, 10) from o_test_2 WHERE key = 2;
SELECT key, length(val), substring(val for 30),
	   substr(val, 30, 10) from o_test_2 WHERE key = 3;

CREATE SEQUENCE o_matview_seq;
CREATE MATERIALIZED VIEW o_test_matview (order_id, item_id, quantity, price)
	USING orioledb AS (VALUES (100, 1, 'a',
							   nextval('o_matview_seq'::regclass)),
							  (100, 3, 'b',
							   nextval('o_matview_seq'::regclass)));

\set HIDE_TOAST_COMPRESSION false
SET orioledb.table_description_compress = true;
SELECT orioledb_tbl_indices('o_test_matview'::regclass);
SELECT orioledb_table_description('o_test_matview'::regclass);
\d+ o_test_matview
ALTER MATERIALIZED VIEW o_test_matview
	ALTER COLUMN quantity SET COMPRESSION pglz;
SELECT orioledb_tbl_indices('o_test_matview'::regclass);
SELECT orioledb_table_description('o_test_matview'::regclass);
\d+ o_test_matview
SET orioledb.table_description_compress = false;
\set HIDE_TOAST_COMPRESSION true

-- Test AT_SetCompression (column compression method)
-- PostgreSQL supports compression methods: pglz (default), lz4
CREATE TABLE o_test_set_compression (
	i int PRIMARY KEY,
	data1 text
) USING orioledb;

-- Check initial compression settings (should be DEFAULT or empty)
SELECT attname, attcompression
FROM pg_attribute
WHERE attrelid = 'o_test_set_compression'::regclass
  AND attname = 'data1'
ORDER BY attname;

-- Set compression method for data1 column
-- FIXME: Currently unsupported in orioledb, so this command will fail. Orioledb always uses pglz.
ALTER TABLE o_test_set_compression ALTER COLUMN data1 SET COMPRESSION pglz;

SELECT attname, attcompression
FROM pg_attribute
WHERE attrelid = 'o_test_set_compression'::regclass
  AND attname = 'data1'
ORDER BY attname;

DROP TABLE o_test_set_compression CASCADE;

-- Test compression behavior with different storage options
-- and index creation order.

-- case 1: extended storage, insert before index creation
CREATE TABLE cmdata(a int, f1 text STORAGE EXTENDED) using orioledb;
INSERT INTO cmdata VALUES(1, repeat('1234567890', 1000));
CREATE INDEX idx ON cmdata(f1);
SELECT length(f1) FROM cmdata WHERE a = 1;
DROP TABLE cmdata;

-- case 1.5: extended storage, insert after index creation
CREATE TABLE cmdata(a int, f1 text STORAGE EXTENDED) using orioledb;
CREATE INDEX idx ON cmdata(f1);
INSERT INTO cmdata VALUES(1, repeat('1234567890', 1000));
SELECT length(f1) FROM cmdata WHERE a = 1;
DROP TABLE cmdata;

-- case 2: main storage, insert before index creation
CREATE TABLE cmdata(a int, f1 text STORAGE MAIN) using orioledb;
INSERT INTO cmdata VALUES(1, repeat('1234567890', 1000));
CREATE INDEX idx ON cmdata(f1);
SELECT length(f1) FROM cmdata WHERE a = 1;
DROP TABLE cmdata;

-- case 2.5: main storage, insert after index creation
CREATE TABLE cmdata(a int, f1 text STORAGE MAIN) using orioledb;
CREATE INDEX idx ON cmdata(f1);
INSERT INTO cmdata VALUES(1, repeat('1234567890', 1000));
SELECT length(f1) FROM cmdata WHERE a = 1;
DROP TABLE cmdata;

-- case 3: external storage, insert before index creation
CREATE TABLE cmdata(a int, f1 text STORAGE EXTERNAL) using orioledb;
INSERT INTO cmdata VALUES(1, repeat('1234567890', 1000));
-- should error because key can only be compressed for index, but not toasted (as for heap).
-- Disable parallel maintenance workers so the error originates from the
-- leader; otherwise psql output gets a flaky "CONTEXT: parallel worker"
-- frame depending on which process hits the oversized key first.
SET max_parallel_maintenance_workers = 0;
CREATE INDEX idx ON cmdata(f1);
RESET max_parallel_maintenance_workers;
SELECT length(f1) FROM cmdata WHERE a = 1;
DROP TABLE cmdata;

-- case 3.5: external storage, insert after index creation
CREATE TABLE cmdata(a int, f1 text STORAGE EXTERNAL) using orioledb;
CREATE INDEX idx ON cmdata(f1);
-- should error because key can only be compressed for index, but not toasted (as for heap).
INSERT INTO cmdata VALUES(1, repeat('1234567890', 1000));
SELECT length(f1) FROM cmdata WHERE a = 1;
DROP TABLE cmdata;

-- case 4: plain storage, insert before index creation
CREATE TABLE cmdata(a int, f1 text STORAGE PLAIN) using orioledb;
-- should error because key is too long to be stored in plain storage.
INSERT INTO cmdata VALUES(1, repeat('1234567890', 1000));
-- should error because key is too long to be stored in plain orioledb storage.
-- note that the max tuple size for plain storage for heap is about 3 times
-- higher than that for orioledb because of the different page layout,
-- so we can insert a longer value for heap than for orioledb.
INSERT INTO cmdata VALUES(1, repeat('1234567890', 267));
-- should not error
INSERT INTO cmdata VALUES(1, repeat('1234567890', 266));
CREATE INDEX idx ON cmdata(f1);
SELECT length(f1) FROM cmdata WHERE a = 1;
DROP TABLE cmdata;

-- case 4.5: plain storage, insert after index creation
CREATE TABLE cmdata(a int, f1 text STORAGE PLAIN) using orioledb;
CREATE INDEX idx ON cmdata(f1);
-- should error because key is too long to be stored in plain storage.
INSERT INTO cmdata VALUES(1, repeat('1234567890', 1000));
-- should error because key is too long to be stored in plain orioledb storage.
INSERT INTO cmdata VALUES(1, repeat('1234567890', 267));
-- should not error
INSERT INTO cmdata VALUES(1, repeat('1234567890', 266));
SELECT length(f1) FROM cmdata WHERE a = 1;
DROP TABLE cmdata;

-- case 5: a value moved into the primary key.  The key has to be a plain datum
-- -- it is compared against keys already in the tree, and its length is what
-- the O_BTREE_MAX_TUPLE_SIZE check measures.  A value arriving still
-- compressed measured as its compressed length, so an over-long key sailed
-- past that check and only expanded later: with asserts it tripped
-- Assert(!VARATT_IS_COMPRESSED(...)), and an external value reached
-- "PANIC: not enough reserved undo".  An UPDATE must now be rejected exactly
-- like the equivalent INSERT.
CREATE TABLE cmpk(k text PRIMARY KEY, body text) USING orioledb;
INSERT INTO cmpk VALUES ('a', repeat('compressible ', 500));
-- should error: the key does not fit, same as the INSERT below
UPDATE cmpk SET k = body;
-- should error for the same reason
INSERT INTO cmpk VALUES (repeat('compressible ', 500), 'x');
-- an incompressible value is rejected too, rather than panicking on undo
CREATE TABLE cmpk2(k text PRIMARY KEY, body text) USING orioledb;
INSERT INTO cmpk2 VALUES ('a', (SELECT string_agg(md5(g::text), '')
                                  FROM generate_series(1, 400) g));
UPDATE cmpk2 SET k = body;
-- a key that does fit still works
CREATE TABLE cmpk3(k text PRIMARY KEY, body text) USING orioledb;
INSERT INTO cmpk3 VALUES ('a', repeat('x', 2000));
UPDATE cmpk3 SET k = body;
SELECT length(k), k = body AS key_matches_body FROM cmpk3;
SELECT orioledb_tbl_check('cmpk3'::regclass, true);
DROP TABLE cmpk, cmpk2, cmpk3;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA toast_column_compress CASCADE;
RESET search_path;
