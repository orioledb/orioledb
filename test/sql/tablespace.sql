CREATE SCHEMA tablespace;
SET SESSION search_path = 'tablespace';
CREATE EXTENSION orioledb;

SET allow_in_place_tablespaces = true;
CREATE TABLESPACE regress_tblspace LOCATION '';

CREATE DATABASE tblspace_test_db TABLESPACE regress_tblspace;

CREATE TABLE foo_def (i int) USING orioledb TABLESPACE pg_default;
\d+ foo_def
INSERT INTO foo_def VALUES (54), (12), (7);
TABLE foo_def;
CREATE INDEX foo_def_ix1 ON foo_def(i) TABLESPACE regress_tblspace;
BEGIN;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM foo_def ORDER BY i;
SELECT * FROM foo_def ORDER BY i;
COMMIT;

CREATE TABLE foo_def_pg (i int) USING heap TABLESPACE pg_default;
\d+ foo_def_pg
INSERT INTO foo_def_pg VALUES (32), (73), (71);
TABLE foo_def_pg;

CREATE TABLE foo (i int) USING orioledb TABLESPACE regress_tblspace;
\d+ foo
INSERT INTO foo VALUES(3);
TABLE foo;

\c tblspace_test_db
CREATE SCHEMA tblspace_test_schema;
SET SESSION search_path = 'tblspace_test_schema';
CREATE EXTENSION orioledb;

CREATE TABLE foo_def (i int) USING orioledb TABLESPACE pg_default;
\d+ foo_def
INSERT INTO foo_def VALUES (66), (17), (35);
TABLE foo_def;

CREATE TABLE foo (i int) USING orioledb;
\d+ foo
INSERT INTO foo VALUES(3);
TABLE foo;

\c regression
SET SESSION search_path = 'tablespace';

CREATE TABLE atable USING orioledb AS VALUES (1), (2);
CREATE UNIQUE INDEX anindex ON atable(column1 DESC);
\d+ atable
INSERT INTO atable VALUES(3);
TABLE atable;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;

ALTER TABLE atable SET TABLESPACE pg_default;
SELECT 
    CASE 
        WHEN reltablespace = 0 THEN 'pg_default'
        ELSE 'custom tablespace'
    END AS tablespace_name
FROM pg_class
WHERE relname = 'atable' AND relnamespace = 'tablespace'::regnamespace;
\d+ atable
INSERT INTO atable VALUES(4);
TABLE atable;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;

BEGIN;
SELECT orioledb_tbl_indices('atable'::regclass);
ALTER TABLE atable SET TABLESPACE regress_tblspace;
SELECT 
    CASE 
        WHEN reltablespace = 0 THEN 'pg_default'
        ELSE 'custom tablespace'
    END AS tablespace_name
FROM pg_class
WHERE relname = 'atable' AND relnamespace = 'tablespace'::regnamespace;
\d+ atable
INSERT INTO atable VALUES(5);
TABLE atable;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
ROLLBACK;
SELECT 
    CASE 
        WHEN reltablespace = 0 THEN 'pg_default'
        ELSE 'custom tablespace'
    END AS tablespace_name
FROM pg_class
WHERE relname = 'atable' AND relnamespace = 'tablespace'::regnamespace;
\d+ atable

BEGIN;
ALTER TABLE atable SET TABLESPACE regress_tblspace;
SELECT 
    CASE 
        WHEN reltablespace = 0 THEN 'pg_default'
        ELSE 'custom tablespace'
    END AS tablespace_name
FROM pg_class
WHERE relname = 'atable' AND relnamespace = 'tablespace'::regnamespace;
\d+ atable
INSERT INTO atable VALUES(6);
TABLE atable;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;
SELECT 
    CASE 
        WHEN reltablespace = 0 THEN 'pg_default'
        ELSE 'custom tablespace'
    END AS tablespace_name
FROM pg_class
WHERE relname = 'atable' AND relnamespace = 'tablespace'::regnamespace;
\d+ atable

SELECT orioledb_tbl_indices('atable'::regclass);
ALTER TABLE atable ADD CONSTRAINT atable_pk PRIMARY KEY (column1);
SELECT orioledb_tbl_indices('atable'::regclass);
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;

SELECT orioledb_tbl_indices('atable'::regclass);
ALTER TABLE atable DROP CONSTRAINT atable_pk;
SELECT orioledb_tbl_indices('atable'::regclass);
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;
SELECT orioledb_tbl_structure('atable'::regclass, 'ne');

SELECT orioledb_tbl_indices('atable'::regclass);
ALTER INDEX anindex SET TABLESPACE regress_tblspace;
SELECT orioledb_tbl_indices('atable'::regclass);
BEGIN;
EXPLAIN (COSTS OFF) SELECT * FROM atable;
SELECT * FROM atable;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;
SELECT orioledb_tbl_structure('atable'::regclass, 'ne');
\d+ atable
INSERT INTO atable VALUES(7);
TABLE atable;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM atable ORDER BY column1;
SELECT * FROM atable ORDER BY column1;
COMMIT;

ALTER INDEX anindex SET TABLESPACE pg_default;
SELECT orioledb_tbl_indices('atable'::regclass);

CREATE INDEX atable_ix2 ON atable(column1) TABLESPACE regress_tblspace;
SELECT orioledb_tbl_indices('atable'::regclass);

ALTER INDEX atable_ix2 SET TABLESPACE pg_default;
SELECT orioledb_tbl_indices('atable'::regclass);

ALTER INDEX atable_ix2 SET TABLESPACE regress_tblspace;
SELECT orioledb_tbl_indices('atable'::regclass);

DROP INDEX atable_ix2;
SELECT orioledb_tbl_indices('atable'::regclass);

DROP TABLE atable;

DROP DATABASE tblspace_test_db;

ALTER TABLE ALL IN TABLESPACE regress_tblspace SET TABLESPACE pg_default;
ALTER INDEX ALL IN TABLESPACE regress_tblspace SET TABLESPACE pg_default;
ALTER MATERIALIZED VIEW ALL IN TABLESPACE regress_tblspace SET TABLESPACE pg_default;
-- Should show notice that nothing was done
ALTER TABLE ALL IN TABLESPACE regress_tblspace SET TABLESPACE pg_default;
ALTER TABLESPACE regress_tblspace RENAME TO regress_tblspace_renamed;
ALTER MATERIALIZED VIEW ALL IN TABLESPACE regress_tblspace_renamed SET TABLESPACE pg_default;

SELECT orioledb_rewind_sync();

DROP TABLESPACE regress_tblspace_renamed;

-- TODO: Add tablespaces support to iterate_files
-- TODO: Add test for symlinked tablespaces, probably
-- \set cwd `echo "$PWD/tblspc"`
-- CREATE TABLESPACE regress_tblspace LOCATION :'cwd';

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA tablespace CASCADE;
RESET search_path;
