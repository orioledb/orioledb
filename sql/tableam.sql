CREATE EXTENSION orioledb;

-- index on empty table test
CREATE TABLE o_tableam1
(
	key int8 NOT NULL,
	value text
) USING orioledb;

CREATE UNIQUE INDEX o_tableam1_ix1 ON o_tableam1 USING orioledb_btree (key);
CREATE INDEX o_tableam1_ix2 on o_tableam1 USING orioledb_btree (value);

SELECT orioledb_tbl_indices('o_tableam1'::regclass);

INSERT INTO o_tableam1 (SELECT id, id || 'text' FROM generate_series(1, 20) as id);
ANALYZE o_tableam1;
SELECT orioledb_tbl_structure('o_tableam1'::regclass);

EXPLAIN (COSTS off) SELECT * FROM o_tableam1;
SELECT * FROM o_tableam1;
SET enable_seqscan = off;
SET log_error_verbosity = 'verbose';
EXPLAIN (COSTS off) SELECT * FROM o_tableam1 WHERE key = 5;
SELECT * FROM o_tableam1 WHERE key = 5;
EXPLAIN (COSTS off) SELECT * FROM o_tableam1 ORDER BY key DESC;
SELECT * FROM o_tableam1 ORDER BY key DESC;
EXPLAIN (COSTS off) SELECT * FROM o_tableam1 WHERE value = '5text';
SELECT * FROM o_tableam1 WHERE value = '5text';
EXPLAIN (COSTS off) SELECT * FROM o_tableam1 WHERE value = '5text' AND key = 5;
SELECT * FROM o_tableam1 WHERE value = '5text' AND key = 5;
RESET enable_seqscan;
RESET log_error_verbosity;

TRUNCATE o_tableam1;
INSERT INTO o_tableam1 (SELECT id, id || 'text' FROM generate_series(11, 20) as id);
SELECT * FROM o_tableam1;

DROP TABLE o_tableam1;
