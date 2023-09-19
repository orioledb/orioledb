/* contrib/orioledb/orioledb--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION orioledb_tbl_bin_structure(relid oid,
                                           print_bytes bool default 'false',
                                           depth int default 32)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
