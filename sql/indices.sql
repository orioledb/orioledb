--
-- Test orioledb indexes:
-- 1) orioledb_tbl_indices correct only for non-unique secondary indices
-- 2) INSERT works only for non-unique secondary indices

-- create tables with base variations of indexes

-- ctid is primary index
CREATE SCHEMA indices;
SET SESSION search_path = 'indices';
CREATE EXTENSION orioledb;

CREATE TABLE o_test50
(
	key int8 NOT NULL,
	value int8
) USING orioledb;
SELECT orioledb_tbl_indices('o_test50'::regclass);
INSERT INTO o_test50 SELECT 100 + i, 200 + i FROM generate_series(1, 100) AS i;
EXPLAIN (COSTS off) SELECT * FROM o_test50 WHERE key BETWEEN 100 and 110;
SELECT * FROM o_test50 WHERE key BETWEEN 100 and 110;
EXPLAIN (COSTS off) SELECT * FROM o_test50 WHERE value BETWEEN 200 and 210;
SELECT * FROM o_test50 WHERE value BETWEEN 200 and 210;
EXPLAIN (COSTS off) SELECT * FROM o_test50;
SELECT count(*) FROM o_test50;
TRUNCATE o_test50;

-- value is primary index
CREATE TABLE o_test51
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	PRIMARY KEY(value)
) USING orioledb;
SELECT orioledb_tbl_indices('o_test51'::regclass);
INSERT INTO o_test51 SELECT 100 + i, 200 + i FROM generate_series(1, 100) AS i;
EXPLAIN (COSTS off) SELECT * FROM o_test51 WHERE key BETWEEN 100 and 110;
SELECT * FROM o_test51 WHERE key BETWEEN 100 and 110;
EXPLAIN (COSTS off) SELECT * FROM o_test51 WHERE value BETWEEN 200 and 210;
SELECT * FROM o_test51 WHERE value BETWEEN 200 and 210;
EXPLAIN (COSTS off) SELECT * FROM o_test51;
SELECT count(*) FROM o_test51;
TRUNCATE o_test51;

-- 2 fields as primary index
CREATE TABLE o_test52
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	PRIMARY KEY(key, value)
) USING orioledb;
SELECT orioledb_tbl_indices('o_test52'::regclass);
INSERT INTO o_test52 SELECT 100 + i, 200 + i FROM generate_series(1, 100) AS i;
EXPLAIN (COSTS off) SELECT * FROM o_test52 WHERE key BETWEEN 100 and 110;
SELECT * FROM o_test52 WHERE key BETWEEN 100 and 110;
EXPLAIN (COSTS off) SELECT * FROM o_test52 WHERE value BETWEEN 200 and 210;
SELECT * FROM o_test52 WHERE value BETWEEN 200 and 210;
EXPLAIN (COSTS off) SELECT * FROM o_test52;
SELECT count(*) FROM o_test52;
TRUNCATE o_test52;

-- key is primary index
CREATE TABLE o_test53
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	PRIMARY KEY(key)
) USING orioledb;
SELECT orioledb_tbl_indices('o_test53'::regclass);
INSERT INTO o_test53 SELECT 100 + i, 200 + i FROM generate_series(1, 100) AS i;
EXPLAIN (COSTS off) SELECT * FROM o_test53 WHERE key BETWEEN 100 and 110;
SELECT * FROM o_test53 WHERE key BETWEEN 100 and 110;
EXPLAIN (COSTS off) SELECT * FROM o_test53 WHERE value BETWEEN 200 and 210;
SELECT * FROM o_test53 WHERE value BETWEEN 200 and 210;
EXPLAIN (COSTS off) SELECT * FROM o_test53;
SELECT count(*) FROM o_test53;
TRUNCATE o_test53;

-- key is primary index, value is secondary
CREATE TABLE o_test54
(
	key int8 NOT NULL,
	value int8,
	PRIMARY KEY(key)
) USING orioledb;
CREATE INDEX o_test54_sec ON o_test54 (value);
SELECT orioledb_tbl_indices('o_test54'::regclass);
INSERT INTO o_test54 SELECT 100 + i, 200 + i FROM generate_series(1, 100) AS i;
ANALYZE o_test54;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT * FROM o_test54 WHERE key BETWEEN 100 and 110;
SELECT * FROM o_test54 WHERE key BETWEEN 100 and 110;
EXPLAIN (COSTS off) SELECT * FROM o_test54 WHERE value BETWEEN 200 and 210;
SELECT * FROM o_test54 WHERE value BETWEEN 200 and 210;
EXPLAIN (COSTS off) SELECT value, key FROM o_test54 WHERE value BETWEEN 200 and 210;
SELECT value, key FROM o_test54 WHERE value BETWEEN 200 and 210;
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT * FROM o_test54;
SELECT count(*) FROM o_test54;
TRUNCATE o_test54;

-- value is primary index, key is secondary
CREATE TABLE o_test55
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	PRIMARY KEY(value)
) USING orioledb;
CREATE UNIQUE INDEX o_test55_uniq ON o_test55(key);
SELECT orioledb_tbl_indices('o_test55'::regclass);
INSERT INTO o_test55 VALUES (1, 2);
EXPLAIN (COSTS OFF) SELECT * FROM o_test55 WHERE key = 1;
SELECT * FROM o_test55 WHERE key = 1;
-- fails, secondary unique index contains key = 1
INSERT INTO o_test55 VALUES (1, 3);
-- fails, primary index contains value = 2;
INSERT INTO o_test55 VALUES (3, 2);
-- success
INSERT INTO o_test55 VALUES (3, 4);
ANALYZE o_test55;
SELECT * FROM o_test55 WHERE key >= 1;

-- key is primary index (first unique)
CREATE TABLE o_test56
(
	key int8 NOT NULL,
	value int8,
	PRIMARY KEY (key)
) USING orioledb;
CREATE UNIQUE INDEX o_test56_uniq ON o_test56(value);
SELECT orioledb_tbl_indices('o_test56'::regclass);
INSERT INTO o_test56 VALUES (1, 1);
INSERT INTO o_test56 VALUES (3, 3);
EXPLAIN (COSTS OFF) SELECT * FROM o_test56 WHERE key = 1;
SELECT * FROM o_test56 WHERE key = 1;
EXPLAIN (COSTS OFF) SELECT * FROM o_test56 WHERE value = 3;
SELECT * FROM o_test56 WHERE value = 3;
-- fails, primary index contains key = 3;
UPDATE o_test56 SET key = 3 WHERE value = 1;
-- fails, secondary unique index contains value = 3;
UPDATE o_test56 SET value = 3 WHERE value = 1;
-- success
UPDATE o_test56 SET value = 2 WHERE value = 3;
UPDATE o_test56 SET key = 2 WHERE key = 1;
SELECT * FROM o_test56;
SELECT * FROM o_test56 WHERE value > 0;

-- three indices, value is primary
CREATE TABLE o_test57
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	foo int8,
	PRIMARY KEY (value)
) USING orioledb;
CREATE UNIQUE INDEX o_test57_uniq ON o_test57(key);
CREATE INDEX o_test57_reg ON o_test57(foo);
SELECT orioledb_tbl_indices('o_test57'::regclass);

CREATE OR REPLACE FUNCTION smart_explain(sql TEXT) RETURNS SETOF TEXT AS $$
	DECLARE
		row RECORD;
		line text;
		indent integer;
		skip_indent integer;
		skip_start integer;
	BEGIN
		skip_indent := 0;
		skip_start := 0;
		FOR row IN EXECUTE sql LOOP
			line := row."QUERY PLAN";
			indent := length((regexp_match(line, '^ *'))[1]);
			IF line ~ '^ *->  Result' OR line ~ '^Result' THEN
				skip_indent := 6;
				skip_start := indent;
			ELSE
				IF indent >= skip_start THEN
					line := substr(line, skip_indent + 1);
				ELSE
					skip_indent := 0;
					skip_start := 0;
				END IF;
				RETURN NEXT line;
			END IF;
		END LOOP;
	END $$
LANGUAGE plpgsql;

-- three indices, (key, value) is primary
CREATE TABLE o_test58
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	foo int8,
	bar int8,
	PRIMARY KEY(key, value)
) USING orioledb;
CREATE INDEX o_test58_reg1 ON o_test58(key);
CREATE INDEX o_test58_reg2 ON o_test58(foo);
SELECT orioledb_tbl_indices('o_test58'::regclass);
INSERT INTO o_test58 SELECT 100 + i, 200 + i, 300 + i, 400 + i FROM generate_series(1, 10) AS i;
SELECT orioledb_tbl_check('o_test58'::regclass);

EXPLAIN (COSTS off) SELECT * FROM o_test58;
EXPLAIN (COSTS off) SELECT * FROM o_test58 WHERE foo = 301;
EXPLAIN (COSTS off) SELECT * FROM o_test58 WHERE foo > 300 and foo < 400 and
												 bar = 300;
EXPLAIN (COSTS off) SELECT * FROM o_test58 WHERE bar = 200;
SELECT smart_explain(
'EXPLAIN (COSTS off) UPDATE o_test58 SET foo = 100 WHERE foo > 102 AND
														 foo < 400;');

UPDATE o_test58 SET foo = 100 WHERE foo > 102;
--SELECT orioledb_tbl_structure('o_test58'::regclass);

UPDATE o_test58 SET key = 100 WHERE foo > 100;
--SELECT orioledb_tbl_structure('o_test58'::regclass);

-- three indices, (pri1, pri2) is primary, sec1 and sec2 - secondary
CREATE TABLE o_test59
(
	sec1 int8 NOT NULL,
	pri1 int8 NOT NULL,
	pri2 int8 NOT NULL,
	sec2 int8,
	foo int8,
	PRIMARY KEY(pri1, pri2)
) USING orioledb;
CREATE INDEX o_test59_reg1 ON o_test59(sec1);
CREATE INDEX o_test59_reg2 ON o_test59(sec2);
EXPLAIN (COSTS off) SELECT * FROM o_test59 WHERE sec1 = 100;
EXPLAIN (COSTS off) SELECT * FROM o_test59 WHERE sec1 = 100 and pri1 > 200;
EXPLAIN (COSTS off) SELECT * FROM o_test59 WHERE sec1 = 100 and pri1 = 200;
EXPLAIN (COSTS off) SELECT * FROM o_test59 WHERE pri1 = 100;
EXPLAIN (COSTS off) SELECT * FROM o_test59 WHERE sec2 = 200;
EXPLAIN (COSTS off) SELECT * FROM o_test59 WHERE sec2 = 200 and pri1 = 100;

INSERT INTO o_test59 SELECT 100 + i, 200 + i, 300 + i, 400 + i, 500 + i FROM generate_series(1, 10) AS i;
--SELECT orioledb_tbl_structure('o_test59'::regclass);

-- update not index field
SELECT smart_explain(
'EXPLAIN (COSTS off) UPDATE o_test59 SET foo = 0 WHERE sec1 > 105 AND
													   sec1 < 200;');
UPDATE o_test59 SET foo = 0 WHERE sec1 > 105;
--SELECT orioledb_tbl_structure('o_test59'::regclass);

-- update only sec1 index field
SELECT smart_explain(
'EXPLAIN (COSTS off) UPDATE o_test59 SET sec1 = 100 WHERE sec2 > 405 and sec2 < 408;');
UPDATE o_test59 SET sec1 = 100 WHERE sec2 > 405 and sec2 < 408;
--SELECT orioledb_tbl_structure('o_test59'::regclass);

-- update primary index field
SELECT smart_explain(
'EXPLAIN (COSTS off) UPDATE o_test59 SET pri1 = 50 WHERE sec1 = 100;');
UPDATE o_test59 SET pri1 = 50 WHERE sec1 = 100;
--SELECT orioledb_tbl_structure('o_test59'::regclass);

-- 1 ctid index + 2 non-unique indices
CREATE TABLE o_test61
(
	key int8 NOT NULL,
	value int8 NOT NULL
) USING orioledb;
CREATE INDEX o_test61_reg1 ON o_test61(key);
CREATE INDEX o_test61_reg2 ON o_test61(value);
SELECT orioledb_tbl_indices('o_test61'::regclass);
INSERT INTO o_test61 SELECT 100 + i, 200 + i FROM generate_series(1, 100) AS i;
ANALYZE o_test61;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT * FROM o_test61 WHERE key BETWEEN 100 and 110;
SELECT * FROM o_test61 WHERE key BETWEEN 100 and 110;
EXPLAIN (COSTS off) SELECT * FROM o_test61 WHERE value BETWEEN 250 and 260;
SELECT * FROM o_test61 WHERE value BETWEEN 250 and 260;
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT count(*) FROM o_test61;
SELECT count(*) FROM o_test61;

-- primary key - first field
-- secondary key - second field DESC
CREATE TABLE o_test65
(
	val text NOT NULL,
	id int8 NOT NULL,
	PRIMARY KEY(val)
) USING orioledb;
CREATE INDEX o_test65_reg1 ON o_test65(id DESC);

INSERT INTO o_test65 SELECT i+1, i FROM generate_series(1, 60) AS i;
SELECT count(*) FROM o_test65;
SELECT id, val FROM o_test65;
EXPLAIN (COSTS off) SELECT val, id FROM o_test65 ORDER BY id;
SELECT val, id FROM o_test65 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, val FROM o_test65 ORDER BY id;
SELECT id, val FROM o_test65 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, val FROM o_test65 ORDER BY id DESC;
SELECT id, val FROM o_test65 ORDER BY id DESC;
EXPLAIN (COSTS off) SELECT id, val FROM o_test65 WHERE id = 30;
SELECT id, val FROM o_test65 WHERE id = 30;
EXPLAIN (COSTS off) SELECT id, val FROM o_test65 WHERE id <= 30 AND id >= 1;
SELECT id, val FROM o_test65 WHERE id <= 30 AND id >= 1;
EXPLAIN (COSTS off) SELECT id, val FROM o_test65 WHERE id >= 30 AND id < 100;
SELECT id, val FROM o_test65 WHERE id >= 30 AND id < 100;
EXPLAIN (COSTS off) SELECT id, val FROM o_test65 WHERE id <= 30 AND id > 15;
SELECT id, val FROM o_test65 WHERE id <= 30 AND id > 15;
EXPLAIN (COSTS off) SELECT id, val FROM o_test65 WHERE id >= 30 AND id < 45;
SELECT id, val FROM o_test65 WHERE id >= 30 AND id < 45;
EXPLAIN (COSTS off) SELECT id, val FROM o_test65 WHERE id <= 30 OR id > 45;
SELECT id, val FROM o_test65 WHERE id <= 30 OR id > 45;
EXPLAIN (COSTS off) SELECT id, val FROM o_test65 WHERE id >= 30 OR id < 15;
SELECT id, val FROM o_test65 WHERE id >= 30 OR id < 15;

-- 2 fields secondary index
CREATE TABLE test66
(
	idi int4 NOT NULL,
	idv varchar NOT NULL,
	PRIMARY KEY (idi, idv)
);

INSERT INTO test66
	SELECT i, j::text||k
	FROM generate_series(1,3) as i,
	     generate_series(1,3) as j,
	     generate_series(1,3) as k
	WHERE (j+k)%4 <> 0;

CREATE TABLE o_test66
(
	idi int4 NOT NULL,
	idp int4 NOT NULL,
	idv varchar NOT NULL,
	PRIMARY KEY (idp)
) USING orioledb;
CREATE INDEX o_test66_reg1 ON o_test66(idi, idv);

INSERT INTO o_test66
	SELECT i, i * 100 + j * 10 + k, j::text||k
	FROM generate_series(1,3) as i,
	     generate_series(1,3) as j,
	     generate_series(1,3) as k;
ANALYZE o_test66;
SELECT count(*) FROM o_test66;
SELECT idi, idv FROM o_test66;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 ORDER BY idi;
SELECT idi, idv FROM o_test66 ORDER BY idi;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 ORDER BY idi DESC;
SELECT idi, idv FROM o_test66 ORDER BY idi DESC;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 ORDER BY idi, idv;
SELECT idi, idv FROM o_test66 ORDER BY idi, idv;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 ORDER BY idi DESC, idv;
SELECT idi, idv FROM o_test66 ORDER BY idi DESC, idv;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 ORDER BY idi, idv DESC;
SELECT idi, idv FROM o_test66 ORDER BY idi, idv DESC;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 ORDER BY idi DESC, idv DESC;
SELECT idi, idv FROM o_test66 ORDER BY idi DESC, idv DESC;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2;
SELECT idi, idv FROM o_test66 WHERE idi = 2;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idv = '22';
SELECT idi, idv FROM o_test66 WHERE idv = '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv = '22';
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv = '22';

-- Test Result node processing
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idi = 1;
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idi = 1;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv > '22';
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv < '22';
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv >= '22';
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv >= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv <= '22';
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv >= '12' AND idv <= '22';
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv >= '12' AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv > '12' AND idv <= '22';
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv > '12' AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv >= '12' AND idv < '22';
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv >= '12' AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv > '12' AND idv < '22';
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv > '12' AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv <= '12' OR idv >= '22');
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv <= '12' OR idv >= '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv < '12' OR idv >= '22');
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv < '12' OR idv >= '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv <= '12' OR idv > '22');
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv <= '12' OR idv > '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv < '12' OR idv > '22');
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv < '12' OR idv > '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv < '12' AND idv > '22');
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND (idv < '12' AND idv > '22');

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv IN ('12', '22');
SELECT idi, idv FROM o_test66 WHERE idi = 2 AND idv IN ('12', '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi IN (2, 4) AND idv IN ('12', '22');
SELECT idi, idv FROM o_test66 WHERE idi IN (2, 4) AND idv IN ('12', '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE (idi = 2 AND idv = '12') OR (idi = 4 AND idv = '2_3');
SELECT idi, idv FROM o_test66 WHERE (idi = 2 AND idv = '12') OR (idi = 4 AND idv = '2_3');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi > 1 AND
														idi < 100 AND
														idv < '22';
SELECT idi, idv FROM o_test66 WHERE idi > 1 AND
									idi < 100 AND
									idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi > 1 AND
									idi < 100 AND
									idv > '22';
SELECT idi, idv FROM o_test66 WHERE idi > 1 AND
									idi < 100 AND
									idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi > 1 AND idv = '22';
SELECT idi, idv FROM o_test66 WHERE idi > 1 AND idv = '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi < 3 AND
									idi > 0 AND
									idv < '22';
SELECT idi, idv FROM o_test66 WHERE idi < 3 AND
									idi > 0 AND
									idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi < 3 AND
									idi > 0 AND
									idv > '22';
SELECT idi, idv FROM o_test66 WHERE idi < 3  AND
									idi > 0 AND
									idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi < 3 AND idv = '22';
SELECT idi, idv FROM o_test66 WHERE idi < 3 AND idv = '22';

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test66 WHERE idi > 1 AND idv IN ('12', '22');
SELECT idi, idv FROM o_test66 WHERE idi > 1 AND idv IN ('12', '22');
RESET enable_seqscan;

EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test66 USING(idi, idv);
SELECT * FROM test66 JOIN o_test66 USING(idi, idv);
set enable_nestloop=off;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test66 USING(idi, idv);
SELECT * FROM test66 JOIN o_test66 USING(idi, idv);
set enable_hashjoin=off;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test66 USING(idi, idv);
SELECT * FROM test66 JOIN o_test66 USING(idi, idv);
set enable_nestloop=on;
set enable_hashjoin=on;

-- 2 field secondary index with second field desc
CREATE TABLE o_test67
(
	idi int4 NOT NULL,
	idp int4 NOT NULL,
	idv varchar NOT NULL,
	PRIMARY KEY (idp)
) USING orioledb;
CREATE INDEX o_test67_reg1 ON o_test67(idi, idv DESC);

INSERT INTO o_test67
	SELECT i, i * 100 + j * 10 + k, j::text||k
	FROM generate_series(1,3) as i,
	     generate_series(1,3) as j,
	     generate_series(1,3) as k;
ANALYZE o_test67;
SELECT count(*) FROM o_test67;
SELECT idi, idv FROM o_test67;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 ORDER BY idi;
SELECT idi, idv FROM o_test67 ORDER BY idi;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 ORDER BY idi DESC;
SELECT idi, idv FROM o_test67 ORDER BY idi DESC;
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 ORDER BY idi, idv;
SELECT idi, idv FROM o_test67 ORDER BY idi, idv;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 ORDER BY idi DESC, idv;
SELECT idi, idv FROM o_test67 ORDER BY idi DESC, idv;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 ORDER BY idi, idv DESC;
SELECT idi, idv FROM o_test67 ORDER BY idi, idv DESC;
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 ORDER BY idi DESC, idv DESC;
SELECT idi, idv FROM o_test67 ORDER BY idi DESC, idv DESC;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2;
SELECT idi, idv FROM o_test67 WHERE idi = 2;
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idv = '22';
SELECT idi, idv FROM o_test67 WHERE idv = '22';
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv = '22';
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv = '22';

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv > '22';
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv < '22';
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv >= '22';
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv >= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv <= '22';
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv >= '12' AND idv <= '22';
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv >= '12' AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv > '12' AND idv <= '22';
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv > '12' AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv >= '12' AND idv < '22';
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv >= '12' AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv > '12' AND idv < '22';
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv > '12' AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv <= '12' OR idv >= '22');
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv <= '12' OR idv >= '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv < '12' OR idv >= '22');
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv < '12' OR idv >= '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv <= '12' OR idv > '22');
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv <= '12' OR idv > '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv < '12' OR idv > '22');
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv < '12' OR idv > '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv < '12' AND idv > '22');
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND (idv < '12' AND idv > '22');

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv IN ('12', '22');
SELECT idi, idv FROM o_test67 WHERE idi = 2 AND idv IN ('12', '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi IN (2, 4) AND idv IN ('12', '22');
SELECT idi, idv FROM o_test67 WHERE idi IN (2, 4) AND idv IN ('12', '22');
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE (idi = 2 AND idv = '12') OR (idi = 4 AND idv = '2_3');
SELECT idi, idv FROM o_test67 WHERE (idi = 2 AND idv = '12') OR (idi = 4 AND idv = '2_3');
SET enable_seqscan = off;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi > 1 AND
														idi < 100 AND
														idv < '22';
SELECT idi, idv FROM o_test67 WHERE idi > 1 AND idi < 100 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi > 1 AND
														idi < 100 AND
														idv > '22';
SELECT idi, idv FROM o_test67 WHERE idi > 1 AND idi < 100 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi > 1 AND idv = '22';
SELECT idi, idv FROM o_test67 WHERE idi > 1 AND idv = '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi < 3 AND
														idi > 0 AND
														idv < '22';
SELECT idi, idv FROM o_test67 WHERE idi < 3 AND idi > 0 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi < 3 AND
														idi > 0 AND
														idv > '22';
SELECT idi, idv FROM o_test67 WHERE idi < 3 AND idi > 0 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi < 3 AND idv = '22';
SELECT idi, idv FROM o_test67 WHERE idi < 3 AND idv = '22';

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test67 WHERE idi > 1 AND idv IN ('12', '22');
SELECT idi, idv FROM o_test67 WHERE idi > 1 AND idv IN ('12', '22');

RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test67 USING(idi, idv);
SELECT * FROM test66 JOIN o_test67 USING(idi, idv);
set enable_nestloop=off;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test67 USING(idi, idv);
SELECT * FROM test66 JOIN o_test67 USING(idi, idv);
set enable_hashjoin=off;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test67 USING(idi, idv);
SELECT * FROM test66 JOIN o_test67 USING(idi, idv);
set enable_nestloop=on;
set enable_hashjoin=on;

-- 2 field secondary index with different field order and first field desc
CREATE TABLE o_test68
(
	idv varchar NOT NULL,
	idp int4 NOT NULL,
	idi int4 NOT NULL,
	PRIMARY KEY(idp)
) USING orioledb;
CREATE INDEX o_test68_reg1 ON o_test68(idi DESC, idv);

INSERT INTO o_test68
	SELECT j::text||k, i * 100 + j * 10 + k, i
	FROM generate_series(1,3) as i,
	     generate_series(1,3) as j,
	     generate_series(1,3) as k;
ANALYZE o_test68;
SELECT count(*) FROM o_test68;
SELECT idi, idv FROM o_test68;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 ORDER BY idi;
SELECT idi, idv FROM o_test68 ORDER BY idi;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 ORDER BY idi DESC;
SELECT idi, idv FROM o_test68 ORDER BY idi DESC;
RESET enable_seqscan;

SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 ORDER BY idi, idv;
SELECT idi, idv FROM o_test68 ORDER BY idi, idv;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 ORDER BY idi DESC, idv;
SELECT idi, idv FROM o_test68 ORDER BY idi DESC, idv;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 ORDER BY idi, idv DESC;
SELECT idi, idv FROM o_test68 ORDER BY idi, idv DESC;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 ORDER BY idi DESC, idv DESC;
SELECT idi, idv FROM o_test68 ORDER BY idi DESC, idv DESC;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2;
SELECT idi, idv FROM o_test68 WHERE idi = 2;
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idv = '22';
SELECT idi, idv FROM o_test68 WHERE idv = '22';
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv = '22';
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv = '22';

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv > '22';
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv < '22';
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv >= '22';
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv >= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv <= '22';
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv >= '12' AND idv <= '22';
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv >= '12' AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv > '12' AND idv <= '22';
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv > '12' AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv >= '12' AND idv < '22';
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv >= '12' AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv > '12' AND idv < '22';
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv > '12' AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv <= '12' OR idv >= '22');
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv <= '12' OR idv >= '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv < '12' OR idv >= '22');
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv < '12' OR idv >= '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv <= '12' OR idv > '22');
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv <= '12' OR idv > '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv < '12' OR idv > '22');
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv < '12' OR idv > '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv < '12' AND idv > '22');
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND (idv < '12' AND idv > '22');

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv IN ('12', '22');
SELECT idi, idv FROM o_test68 WHERE idi = 2 AND idv IN ('12', '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi IN (2, 4) AND idv IN ('12', '22');
SELECT idi, idv FROM o_test68 WHERE idi IN (2, 4) AND idv IN ('12', '22');
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE (idi = 2 AND idv = '12') OR (idi = 4 AND idv = '2_3');
SELECT idi, idv FROM o_test68 WHERE (idi = 2 AND idv = '12') OR (idi = 4 AND idv = '2_3');
SET enable_seqscan = off;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi > 1 AND
														idi < 100 AND
														idv < '22';
SELECT idi, idv FROM o_test68 WHERE idi > 1 AND idi < 100 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi > 1 AND
														idi < 100 AND
														idv > '22';
SELECT idi, idv FROM o_test68 WHERE idi > 1 AND idi < 100 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi > 1 AND idv = '22';
SELECT idi, idv FROM o_test68 WHERE idi > 1 AND idv = '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi < 3 AND
														idi > 0 AND
														idv < '22';
SELECT idi, idv FROM o_test68 WHERE idi < 3 AND idi > 0 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi < 3 AND
														idi > 0 AND
														idv > '22';
SELECT idi, idv FROM o_test68 WHERE idi < 3 AND idi > 0 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi < 3 AND idv = '22';
SELECT idi, idv FROM o_test68 WHERE idi < 3 AND idv = '22';

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test68 WHERE idi > 1 AND idv IN ('12', '22');
SELECT idi, idv FROM o_test68 WHERE idi > 1 AND idv IN ('12', '22');

RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test68 USING(idi, idv);
SELECT * FROM test66 JOIN o_test68 USING(idi, idv);
set enable_nestloop=off;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test68 USING(idi, idv);
SELECT * FROM test66 JOIN o_test68 USING(idi, idv);
set enable_hashjoin=off;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test68 USING(idi, idv);
SELECT * FROM test66 JOIN o_test68 USING(idi, idv);
set enable_nestloop=on;
set enable_hashjoin=on;

-- 2 field sk with text field first
CREATE TABLE o_test69
(
	idv varchar NOT NULL,
	idi int4 NOT NULL,
	idp int4 NOT NULL,
	PRIMARY KEY(idp)
) USING orioledb;
CREATE INDEX o_test69_reg1 ON o_test69(idv, idi);

INSERT INTO o_test69
	SELECT j::text||k, i, 100 * i + 10 * j + k
	FROM generate_series(1,3) as i,
	     generate_series(1,3) as j,
	     generate_series(1,3) as k;
ANALYZE o_test69;
SELECT count(*) FROM o_test69;
SELECT idi, idv FROM o_test69;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 ORDER BY idv;
SELECT idi, idv FROM o_test69 ORDER BY idv;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 ORDER BY idv DESC;
SELECT idi, idv FROM o_test69 ORDER BY idv DESC;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 ORDER BY idv, idi;
SELECT idi, idv FROM o_test69 ORDER BY idv, idi;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 ORDER BY idv DESC, idi;
SELECT idi, idv FROM o_test69 ORDER BY idv DESC, idi;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 ORDER BY idv, idi DESC;
SELECT idi, idv FROM o_test69 ORDER BY idv, idi DESC;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 ORDER BY idv DESC, idi DESC;
SELECT idi, idv FROM o_test69 ORDER BY idv DESC, idi DESC;
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2;
SELECT idi, idv FROM o_test69 WHERE idi = 2;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idv = '22';
SELECT idi, idv FROM o_test69 WHERE idv = '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv = '22';
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv = '22';

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv > '22';
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv < '22';
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv >= '22';
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv >= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv <= '22';
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv >= '12' AND idv <= '22';
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv >= '12' AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv > '12' AND idv <= '22';
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv > '12' AND idv <= '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv >= '12' AND idv < '22';
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv >= '12' AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv > '12' AND idv < '22';
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv > '12' AND idv < '22';

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv <= '12' OR idv >= '22') ORDER BY idv;
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv <= '12' OR idv >= '22') ORDER BY idv;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv < '12' OR idv >= '22') ORDER BY idv;
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv < '12' OR idv >= '22') ORDER BY idv;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv <= '12' OR idv > '22') ORDER BY idv;
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv <= '12' OR idv > '22') ORDER BY idv;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv < '12' OR idv > '22') ORDER BY idv;
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv < '12' OR idv > '22') ORDER BY idv;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv < '12' AND idv > '22') ORDER BY idv;
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND (idv < '12' AND idv > '22') ORDER BY idv;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv IN ('12', '22');
SELECT idi, idv FROM o_test69 WHERE idi = 2 AND idv IN ('12', '22');
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi IN (2, 4) AND idv IN ('12', '22');
SELECT idi, idv FROM o_test69 WHERE idi IN (2, 4) AND idv IN ('12', '22');
RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE (idi = 2 AND idv = '12') OR (idi = 4 AND idv = '2_3');
SELECT idi, idv FROM o_test69 WHERE (idi = 2 AND idv = '12') OR (idi = 4 AND idv = '2_3');
SET enable_seqscan = off;

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi > 1 AND
														idi < 100 AND
														idv < '22';
SELECT idi, idv FROM o_test69 WHERE idi > 1 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi > 1 AND
														idi < 100 AND
														idv > '22';
SELECT idi, idv FROM o_test69 WHERE idi > 1 AND idi < 100 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi > 1 AND idv = '22';
SELECT idi, idv FROM o_test69 WHERE idi > 1 AND idi < 100 AND idv = '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi < 3 AND
														idi > 0 AND
														idv < '22';
SELECT idi, idv FROM o_test69 WHERE idi < 3 AND idi > 0 AND idv < '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi < 3 AND
														idi > 0 AND
														idv > '22';
SELECT idi, idv FROM o_test69 WHERE idi < 3 AND idi > 0 AND idv > '22';
EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi < 3 AND idv = '22';
SELECT idi, idv FROM o_test69 WHERE idi < 3 AND idv = '22';

EXPLAIN (COSTS off) SELECT idi, idv FROM o_test69 WHERE idi > 1 AND idv IN ('12', '22');
SELECT idi, idv FROM o_test69 WHERE idi > 1 AND idv IN ('12', '22');

RESET enable_seqscan;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test69 USING(idi, idv);
SELECT * FROM test66 JOIN o_test69 USING(idi, idv);
set enable_nestloop=off;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test69 USING(idi, idv);
SELECT * FROM test66 JOIN o_test69 USING(idi, idv);
set enable_hashjoin=off;
EXPLAIN (COSTS off) SELECT * FROM test66 JOIN o_test69 USING(idi, idv);
SELECT * FROM test66 JOIN o_test69 USING(idi, idv);
set enable_nestloop=on;
set enable_hashjoin=on;

CREATE TABLE o_test70
(
	key int8 not null,
	value int8,
	PRIMARY KEY(key)
) USING orioledb;
CREATE INDEX o_test70_reg ON o_test70(value);

-- Test page split
INSERT INTO o_test70 SELECT i, i + 1 FROM generate_series(1, 1000, 4) AS i;
INSERT INTO o_test70 SELECT i, i + 1 FROM generate_series(1000, 1, -4) AS i;
INSERT INTO o_test70 SELECT i, i + 1 FROM generate_series(999, 1, -4) AS i;
INSERT INTO o_test70 SELECT i, i + 1 FROM generate_series(998, 1, -4) AS i;
SELECT (SELECT array_agg(key) FROM o_test70) =
       (SELECT array_agg(i)::int8[] FROM generate_series(1, 1000) as i);
SELECT (SELECT array_agg(value) FROM o_test70) =
       (SELECT array_agg(i+1)::int8[] FROM generate_series(1, 1000) as i);

SELECT count(*)
	FROM o_test70,
	     (SELECT array_agg(i) as keys FROM generate_series(1, 1000) as i) t
	WHERE key = ANY (keys);
TRUNCATE o_test70;

-- Test rollback for upsert
INSERT INTO o_test70 SELECT i, i + 1 FROM generate_series(1, 20, 1) AS i;
SELECT value FROM o_test70 WHERE key = 1;
--SELECT orioledb_tbl_structure('o_test70'::regclass);
BEGIN;
UPDATE o_test70 SET value = value + 2 WHERE key = 1;
--SELECT orioledb_tbl_structure('o_test70'::regclass);
ROLLBACK;
--SELECT orioledb_tbl_structure('o_test70'::regclass);
SELECT value FROM o_test70 WHERE key = 1;
BEGIN;
UPDATE o_test70 SET value = value + 2 WHERE key = 1;
UPDATE o_test70 SET value = value + 2 WHERE key = 1;
--SELECT orioledb_tbl_structure('o_test70'::regclass);
ROLLBACK;
--SELECT orioledb_tbl_structure('o_test70'::regclass);
SELECT value FROM o_test70 WHERE key = 1;

-- Test for key update
UPDATE o_test70 SET key = 1001 WHERE key = 1;
SELECT key, value FROM o_test70 WHERE key = 1 OR key = 1001;
--SELECT orioledb_tbl_structure('o_test70'::regclass);

-- Test split bug
TRUNCATE o_test70;
INSERT INTO o_test70 SELECT i, i + 1 FROM generate_series(1, 100, 4) AS i;
DELETE FROM o_test70 WHERE key % 4 = 1;
INSERT INTO o_test70 SELECT i, i + 1 FROM generate_series(2, 100, 4) AS i;
SELECT sum(key), sum(value), count(*) FROM o_test70;
--SELECT orioledb_tbl_structure('o_test70'::regclass);
TRUNCATE o_test70;

CREATE TABLE o_test71
(
	idf varchar NOT NULL,
	ids int4 NOT NULL,
	idt int4 NOT NULL,
	PRIMARY KEY (ids, idt)
) USING orioledb;
CREATE UNIQUE INDEX o_test71_uniq ON o_test71 (idf, idt);
SELECT orioledb_tbl_indices('o_test71'::regclass);
INSERT INTO o_test71 VALUES (1, 2, 3);
SELECT * FROM o_test71 WHERE idf = '1';
UPDATE o_test71 SET ids = 4 WHERE idf = '1';
SELECT * FROM o_test71;
SELECT * FROM o_test71 WHERE idf >= '1';

-- compressed index
CREATE TABLE o_test72
(
	key int8 NOT NULL,
	value int8 NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_test72_uniq ON o_test72(value) WITH (compress);
SELECT orioledb_tbl_indices('o_test72'::regclass);
TRUNCATE o_test72;

-- compressed index with invalid compression lvl
CREATE TABLE o_test73
(
	key int8 NOT NULL,
	value int8 NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_test73_uniq ON o_test73(value) WITH (compress = -50);
CREATE UNIQUE INDEX o_test73_uniq ON o_test73(value) WITH (compress = 500);
-- compressed index with valid compression lvl
CREATE UNIQUE INDEX o_test73_uniq ON o_test73(value) WITH (compress = 11);

SELECT orioledb_tbl_indices('o_test73'::regclass);
TRUNCATE o_test73;

-- invalid toast compression
CREATE TABLE o_test74
(
	key int8 NOT NULL,
	value text NOT NULL,
	PRIMARY KEY(key)
) USING orioledb WITH (toast_compress = -10);

CREATE TABLE o_test74
(
	key int8 NOT NULL,
	value text NOT NULL,
	PRIMARY KEY(key)
) USING orioledb WITH (toast_compress = 1000);

-- valid default toast compression
CREATE TABLE o_test74
(
	key int8 NOT NULL,
	value text NOT NULL,
	PRIMARY KEY(key)
) USING orioledb WITH (toast_compress);
SELECT orioledb_tbl_indices('o_test74'::regclass);

-- a valid toast compression
CREATE TABLE o_test75
(
	key int8 NOT NULL,
	value text NOT NULL,
	PRIMARY KEY(key)
) USING orioledb WITH (toast_compress = 11);
SELECT orioledb_tbl_indices('o_test75'::regclass);

-- a valid default compression
CREATE TABLE o_test76
(
	key int8 NOT NULL,
	value text NOT NULL,
	PRIMARY KEY(key)
) USING orioledb WITH (compress = 9);
CREATE UNIQUE INDEX o_test76_uniq ON o_test76 (key) WITH (compress);
SELECT orioledb_tbl_indices('o_test76'::regclass);

-- a valid default compression
CREATE TABLE o_test77
(
	key int8 NOT NULL,
	value text NOT NULL
) USING orioledb WITH (compress = 9);
CREATE INDEX o_test77_reg1 ON o_test77 (key);
CREATE INDEX o_test77_reg2 ON o_test77 (key);
SELECT orioledb_tbl_indices('o_test77'::regclass);

-- valid ctid compression
CREATE TABLE o_test78
(
	key int8 NOT NULL,
	value text NOT NULL
) USING orioledb WITH (primary_compress = 12);
CREATE INDEX o_test78_reg ON o_test78 (key);
SELECT orioledb_tbl_indices('o_test78'::regclass);

-- valid ctid compression
CREATE TABLE o_test79
(
	key int8 NOT NULL,
	value text NOT NULL
) USING orioledb WITH (primary_compress = 7);
SELECT orioledb_tbl_indices('o_test79'::regclass);

--- array index
CREATE TABLE o_test80
(
	arr integer[] NOT NULL,
	PRIMARY KEY (arr)
) USING orioledb;

BEGIN;
INSERT INTO o_test80 VALUES ('{1, 2}');
COMMIT;
INSERT INTO o_test80 VALUES ('{2, 3, 4}');
SELECT * FROM o_test80;
DROP TABLE o_test80;

-- disable compression value
CREATE TABLE o_test81
(
	key int8 NOT NULL,
	value text NOT NULL
) USING orioledb WITH (primary_compress = -1);
SELECT orioledb_tbl_indices('o_test81'::regclass);
DROP TABLE o_test81;

CREATE TABLE o_test81
(
	key int8 NOT NULL,
	value text NOT NULL
) USING orioledb WITH (compress = -1);
SELECT orioledb_tbl_indices('o_test81'::regclass);
DROP TABLE o_test81;

CREATE TABLE o_test81
(
	key int8 NOT NULL,
	value text NOT NULL,
	PRIMARY KEY (key)
) USING orioledb WITH (toast_compress = -1);
SELECT orioledb_tbl_indices('o_test81'::regclass);
DROP TABLE o_test81;

CREATE TABLE o_test81
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	PRIMARY KEY (value)
) USING orioledb WITH (compress = -1);
SELECT orioledb_tbl_indices('o_test81'::regclass);
DROP TABLE o_test81;

CREATE TABLE o_test81
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	PRIMARY KEY(value)
) USING orioledb WITH (compress = -1);
SELECT orioledb_tbl_indices('o_test81'::regclass);
DROP TABLE o_test81;

SET orioledb.default_compress = 5;
SET orioledb.default_primary_compress = 6;
SET orioledb.default_toast_compress = 7;

CREATE TABLE o_test81
(
	key int8 NOT NULL,
	value int8 NOT NULL,
	PRIMARY KEY (value)
) USING orioledb WITH (compress = -1);
SELECT orioledb_table_description('o_test81'::regclass);
SELECT orioledb_tbl_indices('o_test81'::regclass);
DROP TABLE o_test81;

RESET orioledb.default_compress;
RESET orioledb.default_primary_compress;
RESET orioledb.default_toast_compress;


-- Index rename
CREATE TABLE o_test82
(
	key bigint NOT NULL,
	val int,
	val2 int NOT NULL,
	PRIMARY KEY (key)
) USING orioledb;

CREATE INDEX o_test82_idx1 ON o_test82 (val);
INSERT INTO o_test82 SELECT 1000 + i, 3000 + i, 3000 + i FROM generate_series(1, 500) AS i;
CREATE INDEX o_test82_idx2 ON o_test82 (val2);

SELECT orioledb_tbl_indices('o_test82'::regclass);

SET enable_seqscan = off;
SET enable_bitmapscan = off;

SELECT smart_explain(
'EXPLAIN (COSTS off) WITH o_test82_all AS (
    SELECT * FROM o_test82
		WHERE val2 > 0 AND val > 0
		ORDER BY val2
) SELECT COUNT(*) FROM o_test82_all;');

WITH o_test82_all AS (
    SELECT * FROM o_test82
		WHERE val2 > 0 AND val > 0
		ORDER BY val2
) SELECT COUNT(*) FROM o_test82_all;

BEGIN;
ALTER INDEX o_test82_idx2 RENAME TO o_test82_idx2_renamed;
ROLLBACK;

SELECT orioledb_tbl_indices('o_test82'::regclass);

SELECT smart_explain(
'EXPLAIN (COSTS off) WITH o_test82_all AS (
    SELECT * FROM o_test82
		WHERE val2 > 0 AND val > 0
		ORDER BY val2
) SELECT COUNT(*) FROM o_test82_all;');

WITH o_test82_all AS (
    SELECT * FROM o_test82
		WHERE val2 > 0 AND val > 0
		ORDER BY val2
) SELECT COUNT(*) FROM o_test82_all;

ALTER INDEX o_test82_idx2 RENAME TO o_test82_idx2_renamed;

SELECT orioledb_tbl_indices('o_test82'::regclass);

SELECT smart_explain(
'EXPLAIN (COSTS off) WITH o_test82_all AS (
    SELECT * FROM o_test82
		WHERE val2 > 0 AND val > 0
		ORDER BY val2
) SELECT COUNT(*) FROM o_test82_all;');

WITH o_test82_all AS (
    SELECT * FROM o_test82
		WHERE val2 > 0 AND val > 0
		ORDER BY val2
) SELECT COUNT(*) FROM o_test82_all;

RESET enable_seqscan;
RESET enable_bitmapscan;

DROP INDEX o_test82_idx2; -- fail
DROP INDEX o_test82_idx2_renamed; -- success

SELECT orioledb_tbl_indices('o_test82'::regclass);

-- Column rename
CREATE TABLE o_test83
(
	key bigint NOT NULL,
	val int,
	val2 int NOT NULL,
	PRIMARY KEY (key)
) USING orioledb;

CREATE INDEX o_test83_idx1 ON o_test83 (val, val2);

BEGIN;
ALTER TABLE o_test83
RENAME COLUMN val TO vala;
ROLLBACK;

SELECT orioledb_table_description('o_test83'::regclass);

SELECT orioledb_tbl_indices('o_test83'::regclass);

ALTER TABLE o_test83
RENAME COLUMN val TO vala;

SELECT orioledb_table_description('o_test83'::regclass);

SELECT orioledb_tbl_indices('o_test83'::regclass);

-- Check that partial indices also updated
CREATE TABLE o_test_partial_idx_update
(
  id bigint NOT NULL,
  user_id bigint,
  am bigint,
  CONSTRAINT o_test_partial_idx_update_pkey PRIMARY KEY (id),
  CONSTRAINT o_test_partial_idx_update_amount_check CHECK (am >= 0)
) USING orioledb;
CREATE INDEX o_test_partial_idx_update_idx1
	ON o_test_partial_idx_update (user_id, am);
CREATE INDEX o_test_partial_idx_update_idx2
	ON o_test_partial_idx_update (user_id) WHERE am > 0;
INSERT INTO o_test_partial_idx_update VALUES (1, 100500, 100);
SELECT * FROM o_test_partial_idx_update;
SELECT * FROM o_test_partial_idx_update WHERE user_id=100500 and am > 0;
UPDATE o_test_partial_idx_update SET am=0 WHERE user_id=100500;
SELECT * FROM o_test_partial_idx_update;
EXPLAIN SELECT * FROM o_test_partial_idx_update
	WHERE user_id=100500 and am > 0;
SELECT * FROM o_test_partial_idx_update WHERE user_id=100500 and am > 0;
SET enable_bitmapscan = off;
EXPLAIN SELECT * FROM o_test_partial_idx_update
	WHERE user_id=100500 and am > 0;
SELECT * FROM o_test_partial_idx_update WHERE user_id=100500 and am > 0;
SET enable_bitmapscan = on;

-- Check that build of index with same fields as pkey succeeds
SET enable_seqscan = off;
CREATE TABLE IF NOT EXISTS o_test_unique_as_pkey (
	key integer NOT NULL,
	val integer NOT NULL,
	val2 integer NOT NULL,
	PRIMARY KEY(key, val)
) USING orioledb;
CREATE UNIQUE INDEX o_test_unique_as_pkey_ix1
	ON o_test_unique_as_pkey (val, key);
INSERT INTO o_test_unique_as_pkey (key, val, val2)
	(SELECT val, val * 100, val * 1000  FROM generate_series(1, 5) val);
\d+ o_test_unique_as_pkey
SELECT orioledb_tbl_indices('o_test_unique_as_pkey'::regclass);
EXPLAIN SELECT * FROM o_test_unique_as_pkey ORDER BY val;
SELECT * FROM o_test_unique_as_pkey ORDER BY val;
CREATE UNIQUE INDEX o_test_unique_as_pkey_ix2
	ON o_test_unique_as_pkey (val, key);
\d+ o_test_unique_as_pkey
SELECT orioledb_tbl_indices('o_test_unique_as_pkey'::regclass);
EXPLAIN SELECT * FROM o_test_unique_as_pkey ORDER BY val;
SELECT * FROM o_test_unique_as_pkey ORDER BY val;
RESET enable_seqscan;

CREATE TABLE o_test_renames
(
	key bigint NOT NULL,
	val int,
	PRIMARY KEY (key)
) USING orioledb;

CREATE INDEX o_test_renames_idx ON o_test_renames (val);

\d o_test_renames
SELECT orioledb_table_description('o_test_renames'::regclass);
SELECT description FROM orioledb_table WHERE reloid = 'o_test_renames'::regclass;
\d o_test_renames_idx
SELECT orioledb_tbl_indices('o_test_renames'::regclass);
SELECT description FROM orioledb_index WHERE index_reloid = 'o_test_renames_idx'::regclass;
ALTER TABLE o_test_renames_idx RENAME TO o_test_renames_idx_as_tbl;
\d o_test_renames
\d o_test_renames_idx
ALTER INDEX o_test_renames RENAME TO o_test_renames_as_ix;
\d o_test_renames
\d o_test_renames_as_ix
ALTER TABLE o_test_renames_as_ix RENAME COLUMN o_test_renames_idx_as_tbl TO val2;
\d o_test_renames_as_ix

CREATE TABLE o_test_subexpr_collate (
    val_1 int PRIMARY KEY,
    val_2 text COLLATE "C" NOT NULL
)USING orioledb;

INSERT INTO o_test_subexpr_collate
	SELECT v, 'XXX' || v FROM generate_series(1, 5) v;

SET enable_seqscan = off;

CREATE INDEX o_test_subexpr_collate_ix1 ON o_test_subexpr_collate (val_2);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_subexpr_collate ORDER BY val_2;
SELECT * FROM o_test_subexpr_collate ORDER BY val_2;

CREATE INDEX o_test_subexpr_collate_ix2
	ON o_test_subexpr_collate (val_2 COLLATE "C");
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_subexpr_collate ORDER BY val_2 COLLATE "C";
SELECT * FROM o_test_subexpr_collate ORDER BY val_2 COLLATE "C";

CREATE INDEX o_test_subexpr_collate_ix3
	ON o_test_subexpr_collate ((val_2 COLLATE "C"));
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_subexpr_collate ORDER BY (val_2 COLLATE "C");
SELECT * FROM o_test_subexpr_collate ORDER BY (val_2 COLLATE "C");

ALTER TABLE o_test_subexpr_collate DROP CONSTRAINT o_test_subexpr_collate_pkey;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_subexpr_collate ORDER BY (val_2 COLLATE "C");
SELECT * FROM o_test_subexpr_collate ORDER BY (val_2 COLLATE "C");

CREATE TABLE o_test_reuse_multiple_indices (
	val_1 int PRIMARY KEY,
	val_2 text COLLATE "C" NOT NULL,
	val_3 int,
	val_4 int,
	val_5 int
) USING orioledb;

CREATE INDEX o_test_reuse_multiple_indices_ix1
	ON o_test_reuse_multiple_indices(val_2);
CREATE INDEX o_test_reuse_multiple_indices_ix2
	ON o_test_reuse_multiple_indices(val_3 DESC, val_2 DESC);
CREATE INDEX o_test_reuse_multiple_indices_ix3
	ON o_test_reuse_multiple_indices(val_4) INCLUDE (val_2);
CREATE INDEX o_test_reuse_multiple_indices_ix4
	ON o_test_reuse_multiple_indices(val_5) INCLUDE (val_1, val_2);

SELECT orioledb_table_description('o_test_reuse_multiple_indices'::regclass);
SELECT orioledb_tbl_indices('o_test_reuse_multiple_indices'::regclass);

\d+ o_test_reuse_multiple_indices
\d+ o_test_reuse_multiple_indices_ix1
\d+ o_test_reuse_multiple_indices_ix2
\d+ o_test_reuse_multiple_indices_ix3
\d+ o_test_reuse_multiple_indices_ix4

INSERT INTO o_test_reuse_multiple_indices
	SELECT v, 'XXX' || v, v * 10, v * 100, v * 1000
		FROM generate_series(1, 5) v;
SELECT orioledb_tbl_structure('o_test_reuse_multiple_indices'::regclass,
							  'nue');

EXPLAIN (COSTS OFF)
	SELECT val_2 FROM o_test_reuse_multiple_indices ORDER BY val_2;
SELECT val_2 FROM o_test_reuse_multiple_indices ORDER BY val_2;
EXPLAIN (COSTS OFF)
	SELECT val_1, val_2, val_3 FROM o_test_reuse_multiple_indices
		ORDER BY val_3 DESC;
SELECT val_1, val_2, val_3 FROM o_test_reuse_multiple_indices
	ORDER BY val_3 DESC;
EXPLAIN (COSTS OFF)
	SELECT val_1, val_2, val_4 FROM o_test_reuse_multiple_indices
		ORDER BY val_4;
SELECT val_1, val_2, val_4 FROM o_test_reuse_multiple_indices
	ORDER BY val_4;
EXPLAIN (COSTS OFF)
	SELECT val_1, val_2, val_5 FROM o_test_reuse_multiple_indices
		ORDER BY val_5;
SELECT val_1, val_2, val_5 FROM o_test_reuse_multiple_indices ORDER BY val_5;

ALTER TABLE o_test_reuse_multiple_indices
	ALTER val_2 TYPE text COLLATE "C",
	ALTER val_2 TYPE text COLLATE "POSIX";

SELECT orioledb_table_description('o_test_reuse_multiple_indices'::regclass);
SELECT orioledb_tbl_indices('o_test_reuse_multiple_indices'::regclass);
SELECT orioledb_tbl_structure('o_test_reuse_multiple_indices'::regclass,
							  'nue');
\d+ o_test_reuse_multiple_indices

EXPLAIN (COSTS OFF)
	SELECT chr(ASCII('a') + val_5 / 1000) FROM o_test_reuse_multiple_indices;
SELECT chr(ASCII('a') + val_5 / 1000) FROM o_test_reuse_multiple_indices;
EXPLAIN SELECT val_1, val_5 FROM o_test_reuse_multiple_indices ORDER BY val_5;
SELECT val_1, val_5 FROM o_test_reuse_multiple_indices ORDER BY val_5;
ALTER TABLE o_test_reuse_multiple_indices ALTER val_5 TYPE char
	USING chr(ASCII('a') + val_5 / 1000);
SELECT orioledb_table_description('o_test_reuse_multiple_indices'::regclass);
SELECT orioledb_tbl_indices('o_test_reuse_multiple_indices'::regclass);
EXPLAIN SELECT val_1, val_5 FROM o_test_reuse_multiple_indices ORDER BY val_5;
SELECT val_1, val_5 FROM o_test_reuse_multiple_indices ORDER BY val_5;

\d+ o_test_reuse_multiple_indices
\d+ o_test_reuse_multiple_indices_ix1
\d+ o_test_reuse_multiple_indices_ix2
\d+ o_test_reuse_multiple_indices_ix3
\d+ o_test_reuse_multiple_indices_ix4

SET enable_seqscan = off;

EXPLAIN (COSTS OFF)
	SELECT val_2 FROM o_test_reuse_multiple_indices ORDER BY val_2;
SELECT val_2 FROM o_test_reuse_multiple_indices ORDER BY val_2;
EXPLAIN (COSTS OFF)
	SELECT val_1, val_2, val_3 FROM o_test_reuse_multiple_indices
		ORDER BY val_3 DESC;
SELECT val_1, val_2, val_3 FROM o_test_reuse_multiple_indices
	ORDER BY val_3 DESC;
EXPLAIN (COSTS OFF)
	SELECT val_1, val_2, val_4 FROM o_test_reuse_multiple_indices
		ORDER BY val_4;
SELECT val_1, val_2, val_4 FROM o_test_reuse_multiple_indices ORDER BY val_4;
EXPLAIN (COSTS OFF)
	SELECT val_1, val_2, val_5 FROM o_test_reuse_multiple_indices
		ORDER BY val_5;
SELECT val_1, val_2, val_5 FROM o_test_reuse_multiple_indices ORDER BY val_5;

SELECT orioledb_tbl_structure('o_test_reuse_multiple_indices'::regclass, 'nue');

ALTER TABLE o_test_reuse_multiple_indices
	DROP CONSTRAINT o_test_reuse_multiple_indices_pkey;

SELECT orioledb_table_description('o_test_reuse_multiple_indices'::regclass);
SELECT orioledb_tbl_indices('o_test_reuse_multiple_indices'::regclass);

EXPLAIN (COSTS OFF)
	SELECT val_2 FROM o_test_reuse_multiple_indices ORDER BY val_2;
SELECT val_2 FROM o_test_reuse_multiple_indices ORDER BY val_2;
EXPLAIN (COSTS OFF)
	SELECT val_1, val_2, val_3 FROM o_test_reuse_multiple_indices
		ORDER BY val_3 DESC;
SELECT val_1, val_2, val_3 FROM o_test_reuse_multiple_indices
	ORDER BY val_3 DESC;
EXPLAIN (COSTS OFF)
	SELECT val_1, val_2, val_4 FROM o_test_reuse_multiple_indices
		ORDER BY val_4;
SELECT val_1, val_2, val_4 FROM o_test_reuse_multiple_indices ORDER BY val_4;
EXPLAIN (COSTS OFF)
	SELECT val_1, val_2, val_5 FROM o_test_reuse_multiple_indices
		ORDER BY val_5;
SELECT val_1, val_2, val_5 FROM o_test_reuse_multiple_indices ORDER BY val_5;

SELECT orioledb_tbl_structure('o_test_reuse_multiple_indices'::regclass, 'nue');

\d+ o_test_reuse_multiple_indices
\d+ o_test_reuse_multiple_indices_ix1
\d+ o_test_reuse_multiple_indices_ix2
\d+ o_test_reuse_multiple_indices_ix3
\d+ o_test_reuse_multiple_indices_ix4

RESET enable_seqscan;

-- Test that fields added when we create table with include indices
CREATE TABLE o_test_include_like
(
	key int8 NOT NULL PRIMARY KEY,
	value text,
	val2 int
) USING orioledb;

CREATE INDEX o_test_include_like_ix1 ON o_test_include_like (key);
CREATE INDEX o_test_include_like_ix2 on o_test_include_like (value);
CREATE UNIQUE INDEX o_test_include_like_ix_include
	ON o_test_include_like (val2) INCLUDE (value);

SELECT orioledb_tbl_indices('o_test_include_like'::regclass);
\d+ o_test_include_like

CREATE TABLE IF NOT EXISTS o_test_include_like_like
	(LIKE o_test_include_like INCLUDING INDEXES INCLUDING CONSTRAINTS)
		USING orioledb;
SELECT orioledb_tbl_indices('o_test_include_like_like'::regclass);
\d+ o_test_include_like_like
DROP TABLE o_test_include_like_like;

ALTER TABLE o_test_include_like DROP CONSTRAINT o_test_include_like_pkey;
SELECT orioledb_tbl_indices('o_test_include_like'::regclass);
\d+ o_test_include_like

CREATE TABLE IF NOT EXISTS o_test_include_like_like
	(LIKE o_test_include_like INCLUDING INDEXES INCLUDING CONSTRAINTS)
		USING orioledb;
SELECT orioledb_tbl_indices('o_test_include_like_like'::regclass);
\d+ o_test_include_like_like
DROP TABLE o_test_include_like_like;

-- Test that fields also added when we create table like postgres table
CREATE TABLE pg_test_include_like
(
	key int8 NOT NULL PRIMARY KEY,
	value text,
	val2 int
) USING heap;

CREATE INDEX pg_test_include_like_ix1 ON pg_test_include_like (key);
CREATE INDEX pg_test_include_like_ix2 on pg_test_include_like (value);
CREATE UNIQUE INDEX pg_test_include_like_ix_include
	ON pg_test_include_like (val2) INCLUDE (value);
\d+ pg_test_include_like

CREATE TABLE IF NOT EXISTS o_test_include_like_like
	(LIKE pg_test_include_like INCLUDING INDEXES INCLUDING CONSTRAINTS)
		USING orioledb;
SELECT orioledb_tbl_indices('o_test_include_like_like'::regclass);
\d+ o_test_include_like_like
DROP TABLE o_test_include_like_like;

CREATE TABLE o_test_partial_unique(
    val_1 int4,
    val_2 text
)USING orioledb;
INSERT INTO o_test_partial_unique VALUES (25, 'ac');
INSERT INTO o_test_partial_unique VALUES (26, 'bc');
INSERT INTO o_test_partial_unique VALUES (27, 'C');
INSERT INTO o_test_partial_unique VALUES (25, 'dc');
SELECT orioledb_tbl_structure('o_test_partial_unique'::regclass, 'nue');
CREATE UNIQUE INDEX o_test_partial_unique_ix ON o_test_partial_unique(val_1)
	WHERE val_2 LIKE '%c';
DELETE FROM o_test_partial_unique WHERE val_2 = 'dc';
CREATE UNIQUE INDEX o_test_partial_unique_ix ON o_test_partial_unique(val_1)
	WHERE val_2 LIKE '%c';
BEGIN;
	SET LOCAL enable_seqscan = off;
	EXPLAIN SELECT val_2 FROM o_test_partial_unique WHERE val_2 LIKE '%c';
	SELECT val_2 FROM o_test_partial_unique WHERE val_2 LIKE '%c';
COMMIT;
SELECT orioledb_tbl_structure('o_test_partial_unique'::regclass, 'nue');
SELECT orioledb_tbl_indices('o_test_partial_unique'::regclass);

CREATE TABLE o_test_unique_expr (
    val_1 int4,
    val_2 text,
    val_3 int,
    val_4 int
) USING orioledb;

CREATE UNIQUE INDEX o_test_unique_expr_ix1 ON o_test_unique_expr(val_1);
CREATE UNIQUE INDEX o_test_unique_expr_ix2 ON o_test_unique_expr(lower(val_2));
CREATE UNIQUE INDEX o_test_unique_expr_ix3 ON o_test_unique_expr(abs(val_3),
																 val_4);

SELECT orioledb_tbl_indices('o_test_unique_expr'::regclass);

INSERT INTO o_test_unique_expr VALUES (1, 'a', 1, 1);
SELECT * FROM o_test_unique_expr;

INSERT INTO o_test_unique_expr VALUES (2, 'a', 1, 1);
SELECT * FROM o_test_unique_expr;

INSERT INTO o_test_unique_expr VALUES (2, 'b', 2, 1);
SELECT * FROM o_test_unique_expr;

INSERT INTO o_test_unique_expr VALUES (3, 'c', 2, 1);
SELECT * FROM o_test_unique_expr;

CREATE TABLE o_test_reindex_empty (
  val_1 int PRIMARY KEY
) USING orioledb;

REINDEX INDEX o_test_reindex_empty_pkey;

CREATE TABLE o_test_pkey_mixed
(
	f1 int,
	f2 int,
	f3 int,
	i1 int,
	i2 int,
	pk1 int,
	pk2 int,
	pk3 int,
	pk4 int,
	PRIMARY KEY (pk1, pk2, pk3, pk4)
) USING orioledb;

CREATE INDEX o_test_pkey_mixed_ix1
	ON o_test_pkey_mixed (f1, f2, pk1, f3) INCLUDE (i1, pk4, i2);
\d+ o_test_pkey_mixed
-- f1 f2 pk1 f3 i1 pk4 i2 pk2 pk3
SELECT orioledb_tbl_indices('o_test_pkey_mixed'::regclass);

CREATE UNIQUE INDEX o_test_pkey_mixed_uniq1
	ON o_test_pkey_mixed (f2, f1, pk1, f3) INCLUDE (i1, pk4, i2);
\d+ o_test_pkey_mixed
-- f1 f2 pk1 f3 i1 pk4 i2 pk2 pk3
-- f2 f1 pk1 f3 i1 pk4 i2 pk2 pk3
SELECT orioledb_tbl_indices('o_test_pkey_mixed'::regclass);

CREATE INDEX o_test_pkey_mixed_ix3
	ON o_test_pkey_mixed (f3, f2, pk4, f1, pk4) INCLUDE (pk3);
\d+ o_test_pkey_mixed
-- f1 f2 pk1 f3 i1 pk4 i2 pk2 pk3
-- f2 f1 pk1 f3 i1 pk4 i2 pk2 pk3
-- f3 f2 pk4 f1 pk3 pk1 pk2
SELECT orioledb_tbl_indices('o_test_pkey_mixed'::regclass);

CREATE INDEX o_test_pkey_mixed_ix4
	ON o_test_pkey_mixed (i1, f1, pk4, f2, pk4) INCLUDE (pk3, i2);
\d+ o_test_pkey_mixed
-- f1 f2 pk1 f3 i1 pk4 i2 pk2 pk3
-- f2 f1 pk1 f3 i1 pk4 i2 pk2 pk3
-- f3 f2 pk4 f1 pk3 pk1 pk2
-- i1 f1 pk4 f2 pk3 i2 pk1 pk2
SELECT orioledb_tbl_indices('o_test_pkey_mixed'::regclass);

INSERT INTO o_test_pkey_mixed
	SELECT 1 * 10 ^ v, 2 * 10 ^ v, 3 * 10 ^ v, 4 * 10 ^ v, 5 * 10 ^ v,
		   6 * 10 ^ v, 7 * 10 ^ v, 8 * 10 ^ v, 9 * 10 ^ v FROM
		   generate_series(0, 2) v;
SELECT orioledb_tbl_structure('o_test_pkey_mixed'::regclass, 'nue');
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_mixed;
SELECT * FROM o_test_pkey_mixed;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_mixed ORDER BY f1;
SELECT * FROM o_test_pkey_mixed ORDER BY f1;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_mixed ORDER BY f2;
SELECT * FROM o_test_pkey_mixed ORDER BY f2;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_mixed ORDER BY f3;
SELECT * FROM o_test_pkey_mixed ORDER BY f3;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_mixed ORDER BY i1;
SELECT * FROM o_test_pkey_mixed ORDER BY i1;
RESET enable_seqscan;

CREATE TABLE o_test_pkey_include_box
(
	pk1 int,
	pk2 box,
	f2 int,
	PRIMARY KEY (pk1) INCLUDE (pk2)
) USING orioledb;
\d+ o_test_pkey_include_box
SELECT orioledb_tbl_indices('o_test_pkey_include_box'::regclass);

INSERT INTO o_test_pkey_include_box
	SELECT 1 * 10 ^ v, box(point(2 * 10 ^ v, 3 * 10 ^ v),
						   point(4 * 10 ^ v, 5 * 10 ^ v)),
		   6 * 10 ^ v FROM generate_series(0, 2) v;
SELECT orioledb_tbl_structure('o_test_pkey_include_box'::regclass, 'nue');

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_include_box ORDER BY pk1;
SELECT * FROM o_test_pkey_include_box ORDER BY pk1;
RESET enable_seqscan;

CREATE TABLE o_test_include_box (
	val_1 int,
	val_4 box
) USING orioledb;

CREATE UNIQUE INDEX o_test_include_box_ix1
	ON o_test_include_box (val_1) INCLUDE (val_4);
\d+ o_test_include_box
SELECT orioledb_tbl_indices('o_test_include_box'::regclass);

INSERT INTO o_test_include_box
	SELECT 1 * 10 ^ v, box(point(2 * 10 ^ v, 3 * 10 ^ v),
						   point(4 * 10 ^ v, 5 * 10 ^ v))
		FROM generate_series(0, 2) v;
SELECT orioledb_tbl_structure('o_test_include_box'::regclass, 'nue');

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_include_box ORDER BY val_1;
SELECT * FROM o_test_include_box ORDER BY val_1;
RESET enable_seqscan;

CREATE TABLE o_test_include_box_with_pkey (
	val_1 int PRIMARY KEY,
	val_2 int,
	val_3 int,
	val_4 box
) USING orioledb;

CREATE UNIQUE INDEX o_test_include_box_with_pkey_ix1
	ON o_test_include_box_with_pkey (val_2) INCLUDE (val_4);
CREATE UNIQUE INDEX o_test_include_box_with_pkey_ix2
	ON o_test_include_box_with_pkey (val_3);
\d+ o_test_include_box_with_pkey
SELECT orioledb_tbl_indices('o_test_include_box_with_pkey'::regclass);

INSERT INTO o_test_include_box_with_pkey
	SELECT 1 * 10 ^ v, 2 * 10 ^ v, 3 * 10 ^ v,
		   box(point(4 * 10 ^ v, 5 * 10 ^ v), point(6 * 10 ^ v, 7 * 10 ^ v))
		FROM generate_series(0, 2) v;
SELECT orioledb_tbl_structure('o_test_include_box_with_pkey'::regclass, 'nue');

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_include_box_with_pkey ORDER BY val_1;
SELECT * FROM o_test_include_box_with_pkey ORDER BY val_1;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_include_box_with_pkey ORDER BY val_2;
SELECT * FROM o_test_include_box_with_pkey ORDER BY val_2;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_include_box_with_pkey ORDER BY val_3;
SELECT * FROM o_test_include_box_with_pkey ORDER BY val_3;
RESET enable_seqscan;

CREATE TABLE o_test_drop_included_pkey_field (
    val_1 int,
    val_2 int,
    val_3 int,
    val_4 int NULL,
    PRIMARY KEY (val_4, val_2)
) USING orioledb;
CREATE INDEX o_test_drop_included_pkey_field_ix1
	ON o_test_drop_included_pkey_field (val_1);
\d o_test_drop_included_pkey_field
ALTER TABLE o_test_drop_included_pkey_field DROP COLUMN val_4;
\d o_test_drop_included_pkey_field

CREATE TABLE o_test_row_searchkey (
	val_1 int,
	val_2 int,
	PRIMARY KEY (val_1, val_2)
) USING orioledb;

INSERT INTO o_test_row_searchkey
	SELECT v % 5, (20 - v) % 6 FROM generate_series(1, 20) v;
SELECT * FROM o_test_row_searchkey ORDER BY val_1, val_2;

SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_row_searchkey
		WHERE (val_1, val_2) < (2, 3) AND val_1 > 0 AND val_2 > 0
			ORDER BY val_1, val_2;
SELECT * FROM o_test_row_searchkey
	WHERE (val_1, val_2) < (2, 3) AND val_1 > 0 AND val_2 > 0
		ORDER BY val_1, val_2;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_row_searchkey
		WHERE (val_1 < 2 OR (val_1 = 2 AND (val_2 < 3))) AND
			  val_1 > 0 AND val_2 > 0
			ORDER BY val_1, val_2;
SELECT * FROM o_test_row_searchkey
	WHERE (val_1 < 2 OR (val_1 = 2 AND (val_2 < 3))) AND
		  val_1 > 0 AND val_2 > 0
		ORDER BY val_1, val_2;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_row_searchkey
		WHERE (val_1, val_2) <= (2, 3) AND val_1 > 0 AND val_2 > 0
			ORDER BY val_1, val_2;
SELECT * FROM o_test_row_searchkey
	WHERE (val_1, val_2) <= (2, 3) AND val_1 > 0 AND val_2 > 0
		ORDER BY val_1, val_2;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_row_searchkey
		WHERE (val_1 < 2 OR (val_1 = 2 AND (val_2 < 3 OR val_2 = 3))) AND
			  val_1 > 0 AND val_2 > 0
			ORDER BY val_1, val_2;
SELECT * FROM o_test_row_searchkey
	WHERE (val_1 < 2 OR (val_1 = 2 AND (val_2 < 3 OR val_2 = 3))) AND
		  val_1 > 0 AND val_2 > 0
		ORDER BY val_1, val_2;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_row_searchkey
		WHERE (val_1, val_2) < (2, 3) AND (val_1, val_2) > (1, 2)
			ORDER BY val_1, val_2;
SELECT * FROM o_test_row_searchkey
	WHERE (val_1, val_2) < (2, 3) AND (val_1, val_2) > (1, 2)
		ORDER BY val_1, val_2;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_row_searchkey
		WHERE (val_1 < 2 OR (val_1 = 2 AND (val_2 < 3))) AND
			  (val_1 > 1 OR (val_1 = 1 AND val_2 > 2))
			ORDER BY val_1, val_2;
SELECT * FROM o_test_row_searchkey
	WHERE (val_1 < 2 OR (val_1 = 2 AND (val_2 < 3))) AND
		  (val_1 > 1 OR (val_1 = 1 AND val_2 > 2))
		ORDER BY val_1, val_2;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_row_searchkey
		WHERE (val_1, val_2) <= (2, 3) AND (val_1, val_2) >= (1, 2)
			ORDER BY val_1, val_2;
SELECT * FROM o_test_row_searchkey
	WHERE (val_1, val_2) <= (2, 3) AND (val_1, val_2) >= (1, 2)
		ORDER BY val_1, val_2;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_row_searchkey
		WHERE (val_1 < 2 OR (val_1 = 2 AND (val_2 <= 3))) AND
			  (val_1 > 1 OR (val_1 = 1 AND val_2 >= 2))
			ORDER BY val_1, val_2;
SELECT * FROM o_test_row_searchkey
	WHERE (val_1 < 2 OR (val_1 = 2 AND (val_2 <= 3))) AND
		  (val_1 > 1 OR (val_1 = 1 AND val_2 >= 2))
		ORDER BY val_1, val_2;
RESET enable_seqscan;

CREATE TABLE o_test_row_searchkey_pkey_include (
	val_1 int,
	val_2 int,
	PRIMARY KEY(val_1) INCLUDE(val_2)
) USING orioledb;

INSERT INTO o_test_row_searchkey_pkey_include
	SELECT v, NULL FROM generate_series(1,5) v;

SELECT orioledb_tbl_structure('o_test_row_searchkey_pkey_include'::regclass,
							  'nue');

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_row_searchkey_pkey_include
		WHERE (val_1) < (2);
SELECT * FROM o_test_row_searchkey_pkey_include
	WHERE (val_1) < (2);

CREATE TABLE o_test_duplicate_error (
	val_1 int,
	val_2 int,
	val_3 int,
	val_4 int,
	PRIMARY KEY (val_1,val_2) INCLUDE (val_3,val_4)
) USING orioledb;
INSERT INTO o_test_duplicate_error VALUES (1, 2, 3, 4);
INSERT INTO o_test_duplicate_error VALUES (1, 2, 5, 6);

CREATE TABLE o_test_drop_add_primary (
	val_1 int,
	val_2 int,
	val_3 int,
	val_4 int,
	CONSTRAINT const1 PRIMARY KEY (val_4)
) USING orioledb;
ALTER TABLE o_test_drop_add_primary DROP CONSTRAINT const1;
CREATE UNIQUE INDEX o_test_drop_add_primary_ix1
	ON o_test_drop_add_primary (val_1, val_2) INCLUDE (val_3, val_1);
ALTER TABLE o_test_drop_add_primary ADD PRIMARY KEY (val_4);
\d o_test_drop_add_primary
TRUNCATE TABLE o_test_drop_add_primary;

CREATE TYPE type_1 AS (
	a int,
	b int
);
CREATE TABLE o_test_add_drop_pkey_same_trx (
	val_1 type_1 NOT NULL,
	val_2 int NOT NULL DEFAULT 5,
	val_3 text DEFAULT 'abc'
) USING orioledb;
CREATE INDEX o_test_add_drop_pkey_same_trx_idx1
	ON o_test_add_drop_pkey_same_trx (val_2, val_3);
INSERT INTO o_test_add_drop_pkey_same_trx VALUES ((1, 2)), ((2, 3));
INSERT INTO o_test_add_drop_pkey_same_trx VALUES ((3, 4), 6, 'def');
SELECT * FROM o_test_add_drop_pkey_same_trx;
SELECT * FROM o_test_add_drop_pkey_same_trx WHERE val_3 = 'abc';
BEGIN;
ALTER TABLE o_test_add_drop_pkey_same_trx
    ADD CONSTRAINT o_test_add_drop_pkey_same_trx_pkey PRIMARY KEY (val_1);
ALTER TABLE o_test_add_drop_pkey_same_trx
	DROP CONSTRAINT o_test_add_drop_pkey_same_trx_pkey;
SELECT * FROM o_test_add_drop_pkey_same_trx;
SELECT * FROM o_test_add_drop_pkey_same_trx WHERE val_3 = 'abc';
COMMIT;

CREATE TABLE o_test_unique_ix_duplicate (
	c1 int,
	c2 int,
	c3 int,
	c4 int
) USING orioledb;
INSERT INTO o_test_unique_ix_duplicate VALUES (1, 2, 3, 4);
INSERT INTO o_test_unique_ix_duplicate VALUES (1, 2, 5, 6);
CREATE UNIQUE INDEX o_test_unique_ix_duplicate_uniq1
	ON o_test_unique_ix_duplicate USING btree (c1, c2) INCLUDE (c3, c4);

CREATE TABLE o_test_expr_index_on_conflict(
	val_1 text NOT NULL,
	val_2 text
)USING orioledb;

CREATE UNIQUE INDEX
	ON o_test_expr_index_on_conflict(lower(val_1));

INSERT INTO o_test_expr_index_on_conflict(val_1, val_2)
	VALUES('AaBb', 'insert1') ON CONFLICT (lower(val_1))
		DO UPDATE set val_1 = EXCLUDED.val_1;

SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_expr_index_on_conflict ORDER BY lower(val_1);
SELECT * FROM o_test_expr_index_on_conflict ORDER BY lower(val_1);

INSERT INTO o_test_expr_index_on_conflict(val_1, val_2)
	VALUES('AABB', 'insert2') ON CONFLICT (lower(val_1))
		DO UPDATE set val_1 = EXCLUDED.val_1;

SELECT * FROM o_test_expr_index_on_conflict ORDER BY lower(val_1);

CREATE TABLE o_test_expr_include_index(
	val_1 text NOT NULL,
	val_2 text
)USING orioledb;

CREATE UNIQUE INDEX ON o_test_expr_include_index(lower(val_1)) INCLUDE (val_2);

INSERT INTO o_test_expr_include_index(val_1, val_2)
	VALUES('AaBb', 'insert1') ON CONFLICT (lower(val_1))
		DO UPDATE set val_1 = EXCLUDED.val_1;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_expr_include_index ORDER BY lower(val_1);
SELECT * FROM o_test_expr_include_index ORDER BY lower(val_1);

INSERT INTO o_test_expr_include_index(val_1, val_2)
	VALUES('AABB', 'insert2') ON CONFLICT (lower(val_1))
		DO UPDATE set val_1 = EXCLUDED.val_1;

SELECT * FROM o_test_expr_include_index ORDER BY lower(val_1);
RESET enable_seqscan;

CREATE TABLE o_test_equal_but_binary_not_equal(
  val_1 float4,
  val_2 int,
  PRIMARY KEY (val_1)
)USING orioledb;

SELECT 'NaN'::float4 = '-NaN'::float4;
SELECT '0'::float4 = '-0'::float4;

INSERT INTO o_test_equal_but_binary_not_equal VALUES ('NaN', 2);
INSERT INTO o_test_equal_but_binary_not_equal VALUES ('0', 4);
SELECT orioledb_tbl_structure('o_test_equal_but_binary_not_equal'::regclass,
							  'nue');
UPDATE o_test_equal_but_binary_not_equal
	SET val_1 = '-NaN', val_2 = 3 WHERE val_1 = 'NaN';
UPDATE o_test_equal_but_binary_not_equal
	SET val_1 = '-0', val_2 = 3 WHERE val_1 = '0';
SELECT orioledb_tbl_structure('o_test_equal_but_binary_not_equal'::regclass,
							  'nue');

CREATE TABLE o_test_pkey_include_same_fields
(
	val_1 int,
	val_2 int,
	PRIMARY KEY (val_1) INCLUDE (val_2, val_2)
) USING orioledb;
CREATE INDEX o_test_pkey_include_same_fields_ix1
	ON o_test_pkey_include_same_fields (val_1) INCLUDE (val_2, val_2);
\d+ o_test_pkey_include_same_fields
SELECT orioledb_tbl_indices('o_test_pkey_include_same_fields'::regclass);

INSERT INTO o_test_pkey_include_same_fields VALUES (1, 1);
SELECT * FROM o_test_pkey_include_same_fields;
EXPLAIN (COSTS OFF)
	UPDATE o_test_pkey_include_same_fields SET val_2 = 2 WHERE val_1 = 1;
UPDATE o_test_pkey_include_same_fields SET val_2 = 2 WHERE val_1 = 1;
SELECT * FROM o_test_pkey_include_same_fields;

CREATE TABLE o_test_include_same_as_pkey
(
	val_1 int,
	val_2 int,
	PRIMARY KEY (val_1) INCLUDE (val_1)
) USING orioledb;
CREATE INDEX o_test_include_same_as_pkey_ix1
	ON o_test_include_same_as_pkey (val_1) INCLUDE (val_1);
INSERT INTO o_test_include_same_as_pkey VALUES (1, 1);
SELECT * FROM o_test_include_same_as_pkey;
EXPLAIN (COSTS OFF)
	UPDATE o_test_include_same_as_pkey SET val_2 = 2 WHERE val_1 = 1;
UPDATE o_test_include_same_as_pkey SET val_2 = 2 WHERE val_1 = 1;

CREATE TABLE o_test_index_already_exists_skip
(
	val_1 int
) USING orioledb;

CREATE INDEX o_test_index_already_exists_skip_ix1
	ON o_test_index_already_exists_skip(val_1);
CREATE INDEX IF NOT EXISTS o_test_index_already_exists_skip_ix1
	ON o_test_index_already_exists_skip(val_1);
\d o_test_index_already_exists_skip
SELECT orioledb_tbl_indices('o_test_index_already_exists_skip'::regclass);

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA indices CASCADE;
RESET search_path;

