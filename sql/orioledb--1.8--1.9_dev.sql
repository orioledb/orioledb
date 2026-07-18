/* contrib/orioledb/sql/orioledb--1.8--1.9_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.9'" to load this file. \quit

CREATE FUNCTION orioledb_test_endkey_returned_skip(relid oid, start_at int4)
RETURNS int4[]
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_test_back_refind_skip_tail(relid oid, fake_curkey int4)
RETURNS int4[]
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
