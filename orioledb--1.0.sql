/* contrib/orioledb/orioledb--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION orioledb" to load this file. \quit

-------------------------------------
-- Table AM interface functions
-------------------------------------
CREATE FUNCTION orioledb_tableam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE ACCESS METHOD orioledb TYPE TABLE
HANDLER orioledb_tableam_handler;

-------------------------------------
-- Diagnostic functions
-------------------------------------
CREATE FUNCTION orioledb_version()
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_commit_hash()
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_tbl_structure(relid oid,
                                       options varchar default '',
                                       depth int default 32)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_idx_structure(relid oid,
                                       tree_name text,
                                       options varchar default '',
                                       depth int default 32)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_tbl_check(relid oid, force_map_check bool default False)
RETURNS bool
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_compression_max_level()
RETURNS int8
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_tbl_compression_check
(
       level int8,
       relid oid,
       ranges integer[] default array[1024, 2048, 3072, 4096, 5120, 6144, 7168]
)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_tbl_indices(relid oid)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_sys_tree_structure(num int,
                                            options varchar default '',
                                            depth int default 32)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_tbl_are_indices_equal(idx_oid1 regclass,
                                               idx_oid2 regclass)
RETURNS bool
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_sys_tree_check(num integer, force_map_check bool default False)
RETURNS bool
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_sys_tree_rows(num integer)
RETURNS SETOF jsonb
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_index_rows(relid oid, OUT total int, OUT dead int)
RETURNS record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_table_oids(OUT datoid oid, OUT reloid oid, OUT relnode oid)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_index_oids(OUT datoid oid,
                                    OUT table_reloid oid, OUT table_relnode oid,
                                    OUT index_reloid oid, OUT index_relnode oid,
                                    OUT index_type text)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_table_pages(relid oid, OUT blkno int8, OUT level int4, OUT rightlink int8, OUT hikey jsonb)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_page_stats(OUT pool_name text, OUT busy_pages int8, OUT free_pages int8, OUT dirty_pages int8, OUT all_pages int8)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_table_description(relid oid)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_table_description(datoid oid, relid oid, relnode oid)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_index_description(IN datoid oid, IN relid oid, IN relnode oid, IN index_type text,
                                           OUT name text, OUT description text)
RETURNS record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_relation_size(relid oid)
RETURNS BIGINT
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

-------------------------------------
-- Debug support functions
-------------------------------------
CREATE FUNCTION orioledb_has_retained_undo()
RETURNS bool
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_evict_pages(relid oid, maxLevel int)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_write_pages(relid oid)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_ucm_check()
RETURNS bool
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION pg_stopevent_set(eventname text, condition jsonpath)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pg_stopevent_reset(eventname text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pg_stopevents(OUT stopevent text, OUT condition jsonpath, OUT waiter_pids int[])
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_recovery_synchronized()
RETURNS boolean
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_get_table_descrs(OUT datoid oid, OUT reloid oid, OUT relnode oid, OUT refcnt oid)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_get_index_descrs(OUT datoid oid, OUT reloid oid, OUT relnode oid, OUT refcnt oid)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE VIEW orioledb_table AS
  SELECT t.*,
         orioledb_table_description(t.datoid, t.reloid, t.relnode) AS description
  FROM orioledb_table_oids() t;

CREATE VIEW orioledb_index AS
  SELECT t.*,
         (orioledb_index_description(t.datoid, t.index_reloid, t.index_relnode, t.index_type)).*
  FROM orioledb_index_oids() t;

CREATE VIEW orioledb_table_descr AS
  SELECT * FROM orioledb_get_table_descrs();

CREATE VIEW orioledb_index_descr AS
  SELECT * FROM orioledb_get_index_descrs();

CREATE FUNCTION orioledb_parallel_debug_start()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_parallel_debug_stop()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION s3_get(objectname text)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION s3_put(objectname text, filename text)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
