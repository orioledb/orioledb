CREATE SCHEMA ddl;
SET SESSION search_path = 'ddl';
CREATE EXTENSION orioledb;

\echo :HIDE_TABLEAM
\set HIDE_TABLEAM off
SET default_table_access_method = 'orioledb';

SET allow_in_place_tablespaces = true;
-- CREATE TABLESPACE regress_tblspace LOCATION '';
CREATE TABLESPACE regress_tblspace LOCATION '/Users/birhburh/projects/orioledb/tblspc';

CREATE DATABASE tblspace_test_db TABLESPACE regress_tblspace;

SHOW default_table_access_method;

SELECT orioledb_sys_tree_structure(23);
CREATE TABLE foo_def (i int) USING orioledb TABLESPACE pg_default;
\d+ foo_def
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'foo_def'::regclass;
INSERT INTO foo_def VALUES (54), (12), (7);
TABLE foo_def;
CREATE INDEX foo_def_ix1 ON foo_def(i) TABLESPACE regress_tblspace;
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'foo_def_ix1'::regclass;
BEGIN;
SET enable_seqscan = off;
EXPLAIN SELECT * FROM foo_def ORDER BY i;
SELECT * FROM foo_def ORDER BY i;
COMMIT;

CREATE TABLE foo_def_pg (i int) USING heap TABLESPACE pg_default;
\d+ foo_def_pg
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'foo_def_pg'::regclass;
INSERT INTO foo_def_pg VALUES (32), (73), (71);
TABLE foo_def_pg;

CREATE TABLE foo (i int) TABLESPACE regress_tblspace;
SELECT orioledb_sys_tree_structure(23);
\d+ foo
INSERT INTO foo VALUES(3);
TABLE foo;
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'foo'::regclass;

\c tblspace_test_db
CREATE SCHEMA tblspace_test_schema;
SET SESSION search_path = 'tblspace_test_schema';
CREATE EXTENSION orioledb;
SET default_table_access_method = 'orioledb';
SHOW default_table_access_method;

CREATE TABLE foo_def (i int) TABLESPACE pg_default;
SELECT orioledb_sys_tree_structure(23);
\d+ foo_def
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'foo_def'::regclass;
INSERT INTO foo_def VALUES (66), (17), (35);
TABLE foo_def;

CREATE TABLE foo (i int);
\d+ foo
INSERT INTO foo VALUES(3);
TABLE foo;
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'foo'::regclass;

\c regression
SET default_table_access_method = 'orioledb';
SHOW default_table_access_method;
SET SESSION search_path = 'ddl';

CREATE TABLE atable AS VALUES (1), (2);
CREATE UNIQUE INDEX anindex ON atable(column1 DESC);
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'anindex'::regclass;
SELECT orioledb_sys_tree_structure(1);
SELECT orioledb_sys_tree_structure(2);
SELECT orioledb_sys_tree_structure(3);
\d+ atable
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
INSERT INTO atable VALUES(3);
TABLE atable;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;

SELECT pt.oid, pt.spcname, pg_tablespace_location(pt.oid) ptl, ARRAY_AGG(pd.datname ORDER BY dbs.oid ASC)
	FROM pg_tablespace pt
	LEFT JOIN LATERAL pg_tablespace_databases(pt.oid) dbs(oid) ON true
	LEFT JOIN pg_database pd ON pd.oid = dbs.oid
	GROUP BY pt.oid, spcname, ptl;
SELECT oid, datname, dattablespace FROM pg_database;

ALTER TABLE atable SET TABLESPACE pg_default;
SELECT orioledb_sys_tree_structure(23);
\d+ atable
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
INSERT INTO atable VALUES(4);
TABLE atable;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;

BEGIN;
SELECT orioledb_tbl_indices('atable'::regclass, true, true);
ALTER TABLE atable SET TABLESPACE regress_tblspace;
\d+ atable
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
INSERT INTO atable VALUES(5);
TABLE atable;
SET LOCAL enable_seqscan = off;
EXPLAIN SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
ROLLBACK;
\d+ atable
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;

BEGIN;
ALTER TABLE atable SET TABLESPACE regress_tblspace;
SELECT orioledb_sys_tree_structure(23);
\d+ atable
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
INSERT INTO atable VALUES(6);
TABLE atable;
SET LOCAL enable_seqscan = off;
EXPLAIN SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;
\d+ atable
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
SELECT orioledb_sys_tree_structure(23);

SELECT orioledb_tbl_indices('atable'::regclass, true, true);
ALTER TABLE atable ADD CONSTRAINT atable_pk PRIMARY KEY (column1);
SELECT orioledb_tbl_indices('atable'::regclass, true, true);
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable_pk'::regclass;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;

SELECT orioledb_tbl_indices('atable'::regclass, true, true);
ALTER TABLE atable DROP CONSTRAINT atable_pk;
SELECT orioledb_tbl_indices('atable'::regclass, true, true);
BEGIN;
-- EXPLAIN SELECT * FROM atable;
-- SELECT * FROM atable;
SET LOCAL enable_seqscan = off;
EXPLAIN SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;
SELECT orioledb_tbl_structure('atable'::regclass);

SELECT orioledb_tbl_indices('atable'::regclass, true, true);
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'anindex'::regclass;
ALTER INDEX anindex SET TABLESPACE regress_tblspace;
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'anindex'::regclass;
SELECT orioledb_sys_tree_structure(1);
SELECT orioledb_sys_tree_structure(2);
SELECT orioledb_sys_tree_structure(3);
SELECT orioledb_tbl_indices('atable'::regclass, true, true);
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'anindex'::regclass;
BEGIN;
EXPLAIN SELECT * FROM atable;
SELECT * FROM atable;
SET LOCAL enable_seqscan = off;
EXPLAIN SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;
SELECT orioledb_tbl_structure('atable'::regclass);
\d+ atable
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
INSERT INTO atable VALUES(7);
TABLE atable;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;

SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'anindex'::regclass;
SELECT orioledb_sys_tree_structure(1);
SELECT orioledb_sys_tree_structure(2);
SELECT orioledb_sys_tree_structure(3);
ALTER INDEX anindex SET TABLESPACE pg_default;
SELECT orioledb_tbl_indices('atable'::regclass, true, true);
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable'::regclass;
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'anindex'::regclass;
SELECT orioledb_sys_tree_structure(23);
SELECT orioledb_sys_tree_structure(1);
SELECT orioledb_sys_tree_structure(2);
SELECT orioledb_sys_tree_structure(3);

CREATE INDEX atable_ix2 ON atable(column1) TABLESPACE regress_tblspace;
SELECT orioledb_tbl_indices('atable'::regclass, true, true);
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable_ix2'::regclass;
SELECT orioledb_sys_tree_structure(23);

ALTER INDEX atable_ix2 SET TABLESPACE pg_default;
SELECT orioledb_tbl_indices('atable'::regclass, true, true);
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable_ix2'::regclass;
SELECT orioledb_sys_tree_structure(23);

ALTER INDEX atable_ix2 SET TABLESPACE regress_tblspace;
SELECT orioledb_tbl_indices('atable'::regclass, true, true);
SELECT oid, relname, relfilenode, reltablespace FROM pg_class pc WHERE oid = 'atable_ix2'::regclass;
SELECT orioledb_sys_tree_structure(23);

DROP INDEX atable_ix2;
SELECT orioledb_tbl_indices('atable'::regclass, true, true);
SELECT orioledb_sys_tree_structure(23);

DROP TABLE atable;
SELECT orioledb_sys_tree_structure(23);

DROP DATABASE tblspace_test_db;
-- TODO: Delete from cache when table or index deleted; add test for this to types_test.py
-- TODO: Add tablespaces support to recovery_cleanup_old_files, iterate_files
\set HIDE_TABLEAM on