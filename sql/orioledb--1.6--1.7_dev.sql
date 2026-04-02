/* contrib/orioledb/sql/orioledb--1.6--1.7_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.7'" to load this file. \quit

CREATE FUNCTION reset_read_page_checkpoint_stats()
RETURNS VOID
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION fetch_read_page_checkpoint_stats(OUT min_read_page_checkpoint int4, OUT max_read_page_checkpoint int4)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
