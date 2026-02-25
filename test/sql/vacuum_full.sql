CREATE SCHEMA orioledb;
CREATE EXTENSION orioledb SCHEMA orioledb;
CREATE EXTENSION pageinspect;

-- Test vacuum full for ctid.
CREATE TABLE t_table (
  id int,
  val int,
  val2 int NOT NULL
) USING orioledb;

INSERT INTO t_table SELECT i, i, i FROM generate_series(0, 10) AS s(i);

SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 'ctid_primary', '', 1);

DELETE FROM t_table WHERE id < 5;

SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 'ctid_primary', '', 1);

VACUUM FULL t_table;

SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 'ctid_primary', '', 1);

DROP TABLE t_table;

-- Test vacuum full for secondary indexes, with bridge and pk.
CREATE TABLE t_table (
  id int PRIMARY KEY,
  val int,
  val2 int NOT NULL
) USING orioledb;

INSERT INTO t_table SELECT i, i, i FROM generate_series(0, 10) AS s(i);

CREATE INDEX idx_val ON t_table(val);
CREATE INDEX idx_val2 ON t_table using hash (val2);

SELECT * FROM orioledb.orioledb_tbl_indices('t_table'::regclass, true, false);

SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 't_table_pkey', '', 1);
SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 'idx_val', '', 1);
SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 'index_bridge', '', 1);

DELETE FROM t_table WHERE id < 5;

SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 't_table_pkey', '', 1);
SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 'idx_val', '', 1);
SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 'index_bridge', '', 1);

VACUUM FULL t_table;

-- just to load pages
set enable_seqscan = off;
SELECT * FROM t_table WHERE val >= 5;
SELECT * FROM t_table WHERE val2 = 6;
reset enable_seqscan;

SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 't_table_pkey', '', 1);
SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 'idx_val', '', 1);
SELECT * FROM orioledb.orioledb_idx_structure('t_table'::regclass, 'index_bridge', '', 1);

DROP TABLE t_table;

DROP EXTENSION orioledb;

