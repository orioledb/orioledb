/* contrib/orioledb/sql/orioledb--1.7--1.8_prod.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.8'" to load this file. \quit

-- verify all b-trees of the orioledb relation and emit
-- one row per failed index for pg_amcheck integration
CREATE FUNCTION verify_orioledb(relation regclass,
                                thorough_check bool default false,
                                OUT index_name text,
                                OUT msg text)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

REVOKE ALL ON FUNCTION verify_orioledb(regclass, boolean) FROM PUBLIC;
