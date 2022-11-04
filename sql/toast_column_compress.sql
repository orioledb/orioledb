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

DROP EXTENSION orioledb CASCADE;
