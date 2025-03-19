/* contrib/orioledb/sql/orioledb--1.3--1.4.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.4'" to load this file. \quit

CREATE FUNCTION orioledb_tbl_indices(relid oid, internal bool, oids bool default false)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
