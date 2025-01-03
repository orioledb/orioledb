CREATE SCHEMA fillfactor;
SET SESSION search_path = 'fillfactor';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_fillfactor
(
	f1 text,
	f2 varchar,
	f3 integer,
	f4 integer,
	f5 integer,
	f6 integer,
	PRIMARY KEY(f1)
) USING orioledb WITH (fillfactor = 60);

INSERT INTO o_test_fillfactor SELECT (10000-v+7)%3000, 10-v, (v+5)%7, (v+15)%7, (v+77)%7, (v-177)%7
    FROM generate_series(1, 1000) v;

CREATE INDEX o_test_fillfactor_ix1 ON o_test_fillfactor(f3) WITH (fillfactor = 10);
CREATE INDEX o_test_fillfactor_ix2 ON o_test_fillfactor(f4) WITH (fillfactor = 40);
CREATE INDEX o_test_fillfactor_ix3 ON o_test_fillfactor(f5) WITH (fillfactor = 100);
CREATE INDEX o_test_fillfactor_ix4 ON o_test_fillfactor(f6);
\d+ o_test_fillfactor

SELECT level, count, ROUND(avgoccupied * 100 / 8192)
FROM orioledb_tree_stat('o_test_fillfactor'::regclass);
SELECT level, count, ROUND(avgoccupied * 100 / 8192)
FROM orioledb_tree_stat('o_test_fillfactor_ix1'::regclass);
SELECT level, count, ROUND(avgoccupied * 100 / 8192)
FROM orioledb_tree_stat('o_test_fillfactor_ix2'::regclass);
SELECT level, count, ROUND(avgoccupied * 100 / 8192)
FROM orioledb_tree_stat('o_test_fillfactor_ix3'::regclass);
SELECT level, count, ROUND(avgoccupied * 100 / 8192)
FROM orioledb_tree_stat('o_test_fillfactor_ix4'::regclass);

DROP INDEX o_test_fillfactor_ix1;
DROP INDEX o_test_fillfactor_ix2;
DROP INDEX o_test_fillfactor_ix3;
DROP INDEX o_test_fillfactor_ix4;

TRUNCATE o_test_fillfactor;

INSERT INTO o_test_fillfactor SELECT to_char(v, '0000000'), 10-v, (v+5)%7, (v+15)%7, (v+77)%7, (v-177)%7
    FROM generate_series(1, 1000001, 1000) v;
INSERT INTO o_test_fillfactor SELECT to_char(v, '0000000'), 10-v, (v+5)%7, (v+15)%7, (v+77)%7, (v-177)%7
    FROM generate_series(500002, 501000, 1) v;

SELECT level, count, ROUND(avgoccupied * 100 / 8192)
    FROM orioledb_tree_stat('o_test_fillfactor'::regclass);

TRUNCATE o_test_fillfactor;

INSERT INTO o_test_fillfactor SELECT to_char(v, '0000000'), 10-v, (v+5)%7, (v+15)%7, (v+77)%7, (v-177)%7
    FROM generate_series(1000001, 1, -1000) v;
INSERT INTO o_test_fillfactor SELECT to_char(v, '0000000'), 10-v, (v+5)%7, (v+15)%7, (v+77)%7, (v-177)%7
    FROM generate_series(501000, 500002, -1) v;

SELECT level, count, ROUND(avgoccupied * 100 / 8192)
    FROM orioledb_tree_stat('o_test_fillfactor'::regclass);

ALTER TABLE o_test_fillfactor SET (fillfactor = 20);
\d+ o_test_fillfactor

-- fillfactor option change doesn't rewrite table
SELECT level, count, ROUND(avgoccupied * 100 / 8192)
    FROM orioledb_tree_stat('o_test_fillfactor'::regclass);

TRUNCATE o_test_fillfactor;
INSERT INTO o_test_fillfactor SELECT to_char(v, '0000000'), 10-v, (v+5)%7, (v+15)%7, (v+77)%7, (v-177)%7
    FROM generate_series(501000, 500002, -1) v;

-- Now it looks like it should
SELECT level, count, ROUND(avgoccupied * 100 / 8192)
    FROM orioledb_tree_stat('o_test_fillfactor'::regclass);

CREATE INDEX o_test_fillfactor_ix1 ON o_test_fillfactor(f1) WITH (fillfactor = 100);
\d+ o_test_fillfactor

SELECT level, count, ROUND(avgoccupied * 100 / 8192)
    FROM orioledb_tree_stat('o_test_fillfactor_ix1'::regclass);

ALTER INDEX o_test_fillfactor_ix1 SET (fillfactor = 50);
\d+ o_test_fillfactor

SELECT level, count, ROUND(avgoccupied * 100 / 8192)
    FROM orioledb_tree_stat('o_test_fillfactor_ix1'::regclass);

TRUNCATE o_test_fillfactor;
INSERT INTO o_test_fillfactor SELECT to_char(v, '0000000'), 10-v, (v+5)%7, (v+15)%7, (v+77)%7, (v-177)%7
    FROM generate_series(1000001, 1, -800) v;

SELECT level, count, ROUND(avgoccupied * 100 / 8192)
    FROM orioledb_tree_stat('o_test_fillfactor_ix1'::regclass);

ALTER INDEX o_test_fillfactor_ix1 SET (fillfactor = 20);
\d+ o_test_fillfactor

SELECT level, count, ROUND(avgoccupied * 100 / 8192)
    FROM orioledb_tree_stat('o_test_fillfactor_ix1'::regclass);

TRUNCATE o_test_fillfactor;
INSERT INTO o_test_fillfactor SELECT to_char(v, '0000000'), 10-v, (v+5)%7, (v+15)%7, (v+77)%7, (v-177)%7
    FROM generate_series(1000001, 1, -800) v;

SELECT level, count, ROUND(avgoccupied * 100 / 8192)
    FROM orioledb_tree_stat('o_test_fillfactor_ix1'::regclass);

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA fillfactor CASCADE;
RESET search_path;
