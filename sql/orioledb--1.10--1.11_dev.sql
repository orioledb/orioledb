/* contrib/orioledb/sql/orioledb--1.10--1.11_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.11'" to load this file. \quit

CREATE FUNCTION orioledb_page_checksum(page bytea)
RETURNS int8
AS 'MODULE_PATHNAME'
VOLATILE STRICT LANGUAGE C;
