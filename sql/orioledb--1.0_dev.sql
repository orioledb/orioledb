/* contrib/orioledb/sql/orioledb--1.0.sql */

CREATE FUNCTION orioledb_parallel_debug_start()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_parallel_debug_stop()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION s3_get(objectname text)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION s3_put(objectname text, filename text)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
