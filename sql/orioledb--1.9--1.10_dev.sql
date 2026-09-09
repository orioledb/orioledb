/* contrib/orioledb/sql/orioledb--1.9--1.10_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.10'" to load this file. \quit

CREATE FUNCTION orioledb_test_corrupt_row_undo(relid oid)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
