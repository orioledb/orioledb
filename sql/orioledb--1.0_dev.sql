/* contrib/orioledb/sql/orioledb--1.0_dev.sql */

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


CREATE FUNCTION orioledb_engine_status()
RETURNS text
AS 'MODULE_PATHNAME', 'orioledb_engine_status'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE FUNCTION orioledb_custom_page_stats(text)
RETURNS text
AS 'MODULE_PATHNAME', 'orioledb_custom_page_stats'
LANGUAGE C STRICT PARALLEL SAFE;
