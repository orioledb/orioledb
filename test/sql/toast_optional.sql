CREATE SCHEMA toast_optional;
SET search_path = toast_optional, public;
CREATE EXTENSION orioledb;

-- Fixed-width and bounded-width rows need no TOAST.  varchar(600) in UTF8
-- exceeds heap's threshold, but fits OrioleDB's larger primary tuple limit.
CREATE TABLE ints (id integer PRIMARY KEY, value bigint) USING orioledb
  WITH (fillfactor = 80, primary_compress = 1);
CREATE TABLE bounded (id integer, value varchar(600)) USING orioledb;
CREATE TABLE wide (value varchar(2800)) USING orioledb;
CREATE TABLE unlimited (value text) USING orioledb;
CREATE TABLE plain (value text STORAGE PLAIN) USING orioledb;

CREATE TABLE virtual (
  id integer, value text GENERATED ALWAYS AS (id::text) VIRTUAL
) USING orioledb;

SELECT relname, reltoastrelid <> 0 AS has_toast FROM pg_class
WHERE oid IN ('ints'::regclass, 'bounded'::regclass, 'wide'::regclass,
              'unlimited'::regclass, 'plain'::regclass, 'virtual'::regclass)
ORDER BY relname;

-- Dropped columns are ignored when a new bounded column is added.
ALTER TABLE plain DROP COLUMN value;
ALTER TABLE plain ADD COLUMN value varchar(20);
SELECT reltoastrelid <> 0 AS has_toast FROM pg_class WHERE oid = 'plain'::regclass;

-- Primary key INCLUDE fields, like primary key fields, cannot be toasted.

CREATE TABLE pk_only (value text STORAGE PLAIN PRIMARY KEY) USING orioledb;
ALTER TABLE pk_only ALTER COLUMN value SET STORAGE EXTERNAL;

CREATE TABLE pk_include (
  id integer, value text STORAGE PLAIN, PRIMARY KEY (id) INCLUDE (value)
) USING orioledb;
ALTER TABLE pk_include ALTER COLUMN value SET STORAGE EXTENDED;

SELECT relname, reltoastrelid = 0 AS no_toast FROM pg_class
WHERE oid IN ('pk_only'::regclass, 'pk_include'::regclass) ORDER BY relname;

INSERT INTO pk_include VALUES (1, 'short');
ALTER TABLE pk_include DROP CONSTRAINT pk_include_pkey;

SELECT reltoastrelid <> 0 AS has_toast FROM pg_class WHERE oid = 'pk_include'::regclass;
INSERT INTO pk_include VALUES (2, repeat('x', 10000));
SELECT id, length(value) FROM pk_include ORDER BY id;

-- This row fits the explicit primary tuple, but adding ctid makes it too
-- large.  TOAST must be available during DROP PRIMARY KEY's own rebuild.
INSERT INTO pk_only VALUES (repeat('x', 2670));
BEGIN;
ALTER TABLE pk_only DROP CONSTRAINT pk_only_pkey;
SELECT reltoastrelid <> 0 AS has_toast FROM pg_class WHERE oid = 'pk_only'::regclass;
SELECT length(value) FROM pk_only;
-- In single-byte encodings length() reads only the TOAST pointer's size.
-- Read the complete value to verify its contents and load the TOAST tree:
-- idx_structure() reports "not loaded" without loading an unloaded tree.
SELECT md5(value) = md5(repeat('x', 2670)) AS value_matches FROM pk_only;
SELECT position('Item' IN orioledb_idx_structure('pk_only'::regclass, 'toast')) > 0
  AS chunks_written;
ROLLBACK;
SELECT reltoastrelid = 0 AS no_toast FROM pg_class WHERE oid = 'pk_only'::regclass;
ALTER TABLE pk_only DROP CONSTRAINT pk_only_pkey;
SELECT reltoastrelid <> 0 AS has_toast FROM pg_class WHERE oid = 'pk_only'::regclass;
SELECT length(value) FROM pk_only;

-- An external value from heap or OrioleDB must be detoasted inline when
-- inserted into a destination whose declared maximum row length fits.
CREATE TABLE heap_source (value text, filler text) WITH (toast_tuple_target = 128);
ALTER TABLE heap_source ALTER COLUMN value SET STORAGE EXTERNAL;
ALTER TABLE heap_source ALTER COLUMN filler SET STORAGE EXTERNAL;
INSERT INTO heap_source VALUES (repeat('x', 600), repeat('y', 4000));
INSERT INTO bounded SELECT 1, value FROM heap_source;
CREATE TABLE oriole_source (value text, filler text) USING orioledb;
ALTER TABLE oriole_source ALTER COLUMN value SET STORAGE EXTERNAL;
ALTER TABLE oriole_source ALTER COLUMN filler SET STORAGE EXTERNAL;
INSERT INTO oriole_source VALUES (repeat('x', 600), repeat('y', 2100));
INSERT INTO bounded SELECT 2, value FROM oriole_source;
DROP TABLE heap_source, oriole_source;
SELECT id, length(value), octet_length(value) FROM bounded ORDER BY id;
UPDATE bounded SET value = value WHERE id = 2;
SELECT orioledb_tbl_check('bounded'::regclass);

-- Table options, diagnostics, index rebuilds and truncation work without
-- the TOAST object-access hook.  A rolled-back rebuild preserves the rows.
INSERT INTO ints SELECT i, i * 10 FROM generate_series(1, 10) i;
SELECT pg_table_size('ints') = pg_relation_size('ints') AS no_toast_size;
SELECT pg_total_relation_size('ints') = pg_relation_size('ints') AS only_primary;
SELECT orioledb_tbl_structure('ints'::regclass, 'ne');
ALTER TABLE ints SET (fillfactor = 70, index_bridging = true);
CREATE INDEX ints_value ON ints(value);
BEGIN;
ALTER TABLE ints ALTER COLUMN value TYPE numeric USING value::numeric;
SELECT sum(value) FROM ints;
ROLLBACK;
ALTER TABLE ints ALTER COLUMN value TYPE integer USING value::integer;
SELECT reltoastrelid = 0 AS no_toast, (SELECT sum(value) FROM ints) AS total
FROM pg_class WHERE oid = 'ints'::regclass;
ALTER TABLE ints DROP CONSTRAINT ints_pkey;
ALTER TABLE ints ADD PRIMARY KEY (id);
SELECT orioledb_tbl_check('ints'::regclass);
BEGIN;
TRUNCATE ints;
INSERT INTO ints VALUES (100, 100);
ROLLBACK;
SELECT sum(value) FROM ints;

-- ALTER ADD COLUMN creates TOAST when the schema first needs it, including
-- after a rolled-back attempt.  A rewrite can also remove the TOAST tree.
BEGIN;
ALTER TABLE ints ADD COLUMN payload text;
INSERT INTO ints VALUES (11, 110, repeat('z', 10000));
ROLLBACK;
SELECT reltoastrelid = 0 AS no_toast FROM pg_class WHERE oid = 'ints'::regclass;
ALTER TABLE ints ADD COLUMN payload text;
UPDATE ints SET payload = repeat('z', 10000);
SELECT reltoastrelid <> 0 AS has_toast, (SELECT min(length(payload)) FROM ints)
FROM pg_class WHERE oid = 'ints'::regclass;
ALTER TABLE ints DROP COLUMN payload;
ALTER TABLE ints ALTER COLUMN value TYPE bigint;
SELECT reltoastrelid = 0 AS no_toast, (SELECT sum(value) FROM ints)
FROM pg_class WHERE oid = 'ints'::regclass;

CREATE TABLE ctas USING orioledb AS SELECT * FROM bounded;
CREATE MATERIALIZED VIEW mv USING orioledb AS SELECT id FROM bounded;
REFRESH MATERIALIZED VIEW mv;
SELECT relname, reltoastrelid = 0 AS no_toast FROM pg_class
WHERE oid IN ('ctas'::regclass, 'mv'::regclass) ORDER BY relname;
SELECT count(*) FROM ctas;
SELECT * FROM mv ORDER BY id;
CHECKPOINT;
SELECT orioledb_tbl_check('ints'::regclass);

-- Parallel rebuilds share exactly the sort states for the trees present.
-- Exercise both layouts without TOAST: native indexes alone, and indexes
-- followed directly by a bridge tree (with no intervening TOAST sort).
SET min_parallel_table_scan_size = 0;
SET max_parallel_maintenance_workers = 2;
SET maintenance_work_mem = '512MB';
CREATE TABLE parallel_plain (id integer, value integer) USING orioledb;
CREATE TABLE parallel_bridge (id integer, value integer) USING orioledb
  WITH (index_bridging = true);
INSERT INTO parallel_plain SELECT i, i FROM generate_series(1, 1000) i;
INSERT INTO parallel_bridge SELECT * FROM parallel_plain;
ALTER TABLE parallel_plain ADD PRIMARY KEY (id);
ALTER TABLE parallel_bridge ADD PRIMARY KEY (id);
CREATE INDEX parallel_plain_value ON parallel_plain(value);
CREATE INDEX parallel_bridge_value ON parallel_bridge(value);
SELECT relname, reltoastrelid = 0 AS no_toast FROM pg_class
WHERE oid IN ('parallel_plain'::regclass, 'parallel_bridge'::regclass)
ORDER BY relname;
SELECT orioledb_tbl_check('parallel_plain'::regclass),
       orioledb_tbl_check('parallel_bridge'::regclass);
SELECT sum(value) FROM parallel_plain;
SELECT sum(value) FROM parallel_bridge;
RESET min_parallel_table_scan_size;
RESET max_parallel_maintenance_workers;
RESET maintenance_work_mem;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA toast_optional CASCADE;
RESET search_path;
