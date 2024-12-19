CREATE SCHEMA btree_compression;
SET SESSION search_path = 'btree_compression';
CREATE EXTENSION orioledb;

CREATE TABLE o_test1
(
	id integer NOT NULL,
	val text NOT NULL,
	PRIMARY KEY(id)
) USING orioledb;
CREATE INDEX o_test1_reg on o_test1 (val);

INSERT INTO o_test1 (SELECT id, id || 'val' FROM generate_series(-1000, 0, 1) id);
INSERT INTO o_test1 (SELECT id, id || 'val' FROM generate_series(1001, 2000, 1) id);
INSERT INTO o_test1 (SELECT id, id || 'val' FROM generate_series(11, 1000, 1) id);
SELECT * FROM o_test1;

-- without the ranges array
SELECT regexp_replace
(
       orioledb_tbl_compression_check(0, 'o_test1'::regclass),
       '= [0-9]+(\.[0-9]+)?', '= xxx', 'g'
);

SELECT regexp_replace
(
	orioledb_tbl_compression_check(5, 'o_test1'::regclass),
	'= [0-9]+(\.[0-9]+)?', '= xxx', 'g'
);

SELECT regexp_replace
(
	orioledb_tbl_compression_check(10, 'o_test1'::regclass),
	'= [0-9]+(\.[0-9]+)?', '= xxx', 'g'
);

-- with wrong the ranges array
SELECT orioledb_tbl_compression_check(10, 'o_test1'::regclass, array[0]);
SELECT orioledb_tbl_compression_check(10, 'o_test1'::regclass, array[200, 100]);
SELECT orioledb_tbl_compression_check(10, 'o_test1'::regclass, array[8192]);

-- with valid the ranges array
SELECT regexp_replace
(
       orioledb_tbl_compression_check(10, 'o_test1'::regclass, array[]::integer[]),
       '= [0-9]+(\.[0-9]+)?', '= xxx', 'g'
);

SELECT regexp_replace
(
	orioledb_tbl_compression_check(10, 'o_test1'::regclass, array[4096]),
	'= [0-9]+(\.[0-9]+)?', '= xxx', 'g'
);

SELECT regexp_replace
(
	orioledb_tbl_compression_check(10, 'o_test1'::regclass, array[2048, 4096, 6144]),
	'= [0-9]+(\.[0-9]+)?', '= xxx', 'g'
);

SELECT * FROM o_test1;

DROP TABLE IF EXISTS o_test1;

CREATE TABLE o_test1
(
	id integer NOT NULL,
	val text NOT NULL,
	PRIMARY KEY(id)
) USING orioledb WITH (compress = 11, toast_compress = 13);

INSERT INTO o_test1 (SELECT id, id || 'val' || repeat('x', 500) FROM generate_series(100, 200, 1) id);

SELECT regexp_replace
(
	orioledb_tbl_compression_check(10, 'o_test1'::regclass),
	'= [0-9]+(\.[0-9]+)?', '= xxx', 'g'
);

SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA btree_compression CASCADE;
RESET search_path;
