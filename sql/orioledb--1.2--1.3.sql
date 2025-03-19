/* contrib/orioledb/sql/orioledb--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.3'" to load this file. \quit

CREATE FUNCTION orioledb_tree_stat(relid regclass,
								   OUT level int,
								   OUT count int8,
								   OUT avgoccupied float8,
								   OUT avgvacated float8)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
