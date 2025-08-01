/* contrib/orioledb/sql/orioledb--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.6'" to load this file. \quit

CREATE FUNCTION orioledb_total_relation_size(relid oid)
RETURNS BIGINT
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_table_size(relid oid)
RETURNS BIGINT
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
