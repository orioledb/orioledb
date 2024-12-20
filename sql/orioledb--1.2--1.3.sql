/* contrib/orioledb/orioledb--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.3'" to load this file. \quit

DROP FUNCTION orioledb_tbl_indices(relid oid);
CREATE FUNCTION orioledb_tbl_indices(relid oid, internal bool default false, oids bool default false)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;