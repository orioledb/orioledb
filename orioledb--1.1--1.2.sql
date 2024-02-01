/* contrib/orioledb/orioledb--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.2'" to load this file. \quit

CREATE FUNCTION orioledb_get_evicted_trees(OUT datoid oid,
                                           OUT relnode oid,
                                           OUT root_downlink int8,
                                           OUT file_length int8)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
