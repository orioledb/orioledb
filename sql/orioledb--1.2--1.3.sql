/* contrib/orioledb/sql/orioledb--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.3'" to load this file. \quit

DROP FUNCTION orioledb_table_pages(relid oid, OUT blkno int8, OUT level int4, OUT rightlink int8, OUT hikey jsonb);
CREATE FUNCTION orioledb_table_pages(relid oid, OUT index name, OUT blkno int8, OUT level int4, OUT rightlink int8, OUT hikey jsonb, OUT real_fillfactor int4)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
