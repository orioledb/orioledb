/* contrib/orioledb/sql/orioledb--1.4--1.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.5'" to load this file. \quit

-- tuple_chunk implementation testing functions

CREATE FUNCTION test_btree_leaf_tuple_chunk(relation regclass)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION test_btree_leaf_tuple_chunk_builder(relation regclass)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION test_btree_hikey_chunk(relation regclass)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION test_btree_hikey_chunk_builder(relation regclass)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
