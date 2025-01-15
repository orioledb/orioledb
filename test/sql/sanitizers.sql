-- Random bugs that was catched during sanitizer checks
CREATE SCHEMA sanitizers;
SET SESSION search_path = 'sanitizers';
CREATE EXTENSION orioledb;

-- https://github.com/orioledb/orioledb/issues/406
CREATE TABLE o_test_1
(
	id int2
) USING orioledb;

INSERT INTO o_test_1 (id) values (generate_series(1,306));
ALTER TABLE o_test_1 ADD COLUMN h int2;
UPDATE o_test_1 SET h = 0;

CREATE TABLE o_test_2 (
	val text
) USING orioledb;

INSERT INTO o_test_2 (val) values (
	(SELECT string_agg(chr(32 + floor(random() * 95)::int), '')
		FROM generate_series(1, 2669)) --2668 is OK
);

SELECT * FROM o_test_2 WHERE val = 'A';

CREATE TABLE o_test_3
(
	key int8 PRIMARY KEY,
	spacer text
) USING orioledb;

INSERT INTO o_test_3 (SELECT i, i FROM generate_series(1, 100) i);
UPDATE o_test_3 SET spacer = repeat('a', 96) WHERE key > 1;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA sanitizers CASCADE;
RESET search_path;
