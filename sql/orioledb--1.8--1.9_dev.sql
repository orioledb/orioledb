/* contrib/orioledb/sql/orioledb--1.8--1.9_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.9'" to load this file. \quit

-- While most OrioleDB functions use the "orioledb_" prefix, it is
-- omitted here for brevity since this is an internal, non-user-facing helper.
CREATE FUNCTION inspect_sys_tree_structure(systree int, depth int)
RETURNS text
AS 'MODULE_PATHNAME', 'inspect_sys_tree_structure_sql'
LANGUAGE C STRICT;