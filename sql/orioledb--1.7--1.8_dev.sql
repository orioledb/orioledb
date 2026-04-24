/* contrib/orioledb/sql/orioledb--1.7--1.8_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.8'" to load this file. \quit

DROP FUNCTION orioledb_int4range_immutable(input_str text);
CREATE FUNCTION orioledb_int4range_immutable(input_str text)
RETURNS int4range
AS 'MODULE_PATHNAME'
IMMUTABLE PARALLEL SAFE LANGUAGE C;

CREATE FUNCTION orioledb_inject_wal_error(enable boolean)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
