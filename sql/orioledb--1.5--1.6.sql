/* contrib/orioledb/sql/orioledb--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.6'" to load this file. \quit

CREATE FUNCTION pg_stopevent_set(eventname text, condition jsonpath, flags text)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
