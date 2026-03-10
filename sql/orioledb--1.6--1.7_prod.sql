/* contrib/orioledb/sql/orioledb--1.6--1.7_prod.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.7'" to load this file. \quit

CREATE FUNCTION orioledb_rewind_by_time(rewind_time int, attempt_restart bool)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_rewind_to_transaction(xid int, oxid bigint, attempt_restart bool)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_rewind_to_timestamp(rewind_timestamp TimestampTz, attempt_restart bool)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_undo_size(OUT undo_type text, OUT undo_size int8)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
