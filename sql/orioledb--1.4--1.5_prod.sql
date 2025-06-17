/* contrib/orioledb/sql/orioledb--1.4--1.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.5'" to load this file. \quit

CREATE FUNCTION orioledb_rewind_sync()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_rewind_by_time(rewind_time int)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
