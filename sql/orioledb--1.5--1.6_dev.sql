/* contrib/orioledb/sql/orioledb--1.5--1.6_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.6'" to load this file. \quit

-- Get current logical xid to remember it
CREATE FUNCTION orioledb_get_current_logical_xid()
RETURNS int
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

-- Get current heap xid to remember it
CREATE FUNCTION orioledb_get_current_heap_xid()
RETURNS int
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
