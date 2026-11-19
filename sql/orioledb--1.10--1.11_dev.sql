/* contrib/orioledb/sql/orioledb--1.10--1.11_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.11'" to load this file. \quit

CREATE FUNCTION orioledb_test_s3_queue_wraparound()
RETURNS int4
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
