-----
-- Test TOAST with STORAGE MAIN
-- This tests the fix for handling large values with STORAGE MAIN
-- which should keep data inline but compress when needed
-----
CREATE SCHEMA toast_main;
SET SESSION search_path = 'toast_main';
CREATE EXTENSION orioledb;

-- generate pseudo-random string function in deterministic way
CREATE FUNCTION generate_string(seed integer, length integer) RETURNS text
	AS $$
		SELECT substr(string_agg(
						substr(encode(sha256(seed::text::bytea || '_' || i::text::bytea), 'hex'), 1, 21),
				''), 1, length)
		FROM generate_series(1, (length + 20) / 21) i; $$
LANGUAGE SQL;

-----
-- Basic STORAGE MAIN test with text column
-----
CREATE TABLE o_test_main (
	id integer PRIMARY KEY,
	content text STORAGE MAIN
) USING orioledb;

-- Insert data that would normally be TOASTed but should stay inline with MAIN
INSERT INTO o_test_main SELECT 1, generate_string(1, 3200);
INSERT INTO o_test_main SELECT 2, generate_string(2, 5000);
INSERT INTO o_test_main SELECT 3, generate_string(3, 8000);

SELECT id, length(content), substr(content, 1, 20) FROM o_test_main ORDER BY id;

-- Verify structure
SELECT orioledb_tbl_structure('o_test_main'::regclass, 'nue');

-----
-- Update test with STORAGE MAIN
-----
UPDATE o_test_main SET content = generate_string(10, 6000) WHERE id = 1;
SELECT id, length(content), substr(content, 1, 20) FROM o_test_main WHERE id = 1;

UPDATE o_test_main SET content = generate_string(20, 100) WHERE id = 2;
SELECT id, length(content), substr(content, 1, 20) FROM o_test_main WHERE id = 2;

-----
-- Delete and re-insert with STORAGE MAIN
-----
DELETE FROM o_test_main WHERE id = 3;
INSERT INTO o_test_main SELECT 3, generate_string(30, 7000);
SELECT id, length(content), substr(content, 1, 20) FROM o_test_main WHERE id = 3;

-----
-- Multiple columns with different STORAGE options
-----
DROP TABLE o_test_main;
CREATE TABLE o_test_main (
	id integer PRIMARY KEY,
	col_main text STORAGE MAIN,
	col_extended text STORAGE EXTENDED,
	col_plain text STORAGE PLAIN
) USING orioledb;

INSERT INTO o_test_main VALUES (
	1,
	generate_string(100, 3000),
	generate_string(101, 3000),
	generate_string(102, 500)
);

SELECT id,
	length(col_main), substr(col_main, 1, 20),
	length(col_extended), substr(col_extended, 1, 20),
	length(col_plain), substr(col_plain, 1, 20)
FROM o_test_main;

SELECT orioledb_tbl_structure('o_test_main'::regclass, 'nue');

-----
-- bytea with STORAGE MAIN
-----
DROP TABLE o_test_main;
CREATE TABLE o_test_main (
	id integer PRIMARY KEY,
	data bytea STORAGE MAIN
) USING orioledb;

INSERT INTO o_test_main SELECT 1, decode(generate_string(200, 6000), 'hex');
INSERT INTO o_test_main SELECT 2, decode(generate_string(201, 8000), 'hex');

SELECT id, length(data) FROM o_test_main ORDER BY id;
SELECT orioledb_tbl_structure('o_test_main'::regclass, 'nue');

-----
-- Index on column with STORAGE MAIN (should work but index values stay inline)
-----
DROP TABLE o_test_main;
CREATE TABLE o_test_main (
	id integer PRIMARY KEY,
	val text STORAGE MAIN
) USING orioledb;
CREATE INDEX o_test_main_idx ON o_test_main(val);

-- Small values that fit in index
INSERT INTO o_test_main SELECT id, generate_string(id, 100) FROM generate_series(1, 10) id;
SELECT id, length(val) FROM o_test_main ORDER BY val LIMIT 5;

-----
-- Cleanup
-----
DROP TABLE o_test_main;
DROP FUNCTION generate_string;
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA toast_main CASCADE;
RESET search_path;
