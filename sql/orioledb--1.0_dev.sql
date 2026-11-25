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

CREATE FUNCTION orioledb_flush_local_wal()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

-- Reads this node's WAL for the rewind keys its promotion logged at
-- 'switchpoint'; NULL when its timeline did not fork there.  Only the tests
-- read these records directly: pg_rewind looks for them in the target's WAL
-- as it scans it, and replay reads them out of the WAL stream.
CREATE FUNCTION orioledb_pg_rewind_divergence_keys(switchpoint pg_lsn)
RETURNS bytea
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
