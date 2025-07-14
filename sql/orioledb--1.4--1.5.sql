/* contrib/orioledb/sql/orioledb--1.4--1.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.5'" to load this file. \quit

-- Rewind by rewind_time (in seconds) back from the present
CREATE FUNCTION orioledb_rewind_by_time(rewind_time int)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

-- Rewind to just before xid/oxid pair (remembered previously using pg_current_xact_id() and orioledb_get_current_oxid())
CREATE FUNCTION orioledb_rewind_to_transaction(xid int, oxid bigint)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

-- Rewind to before a particular timestamp
CREATE FUNCTION orioledb_rewind_to_timestamp(rewind_timestamp TimestampTz)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

-- Get current oxid to remember it. This does the same for Oriole transactions that pg_current_xact_id() does for heap transaction
CREATE FUNCTION orioledb_get_current_oxid()
RETURNS bigint
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_get_rewind_queue_length()
RETURNS bigint
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_get_rewind_evicted_length()
RETURNS bigint
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

-- Get last non-complete xid and oxid still in the queue. Mainly intended for tests.
CREATE FUNCTION orioledb_get_complete_oxid()
RETURNS bigint
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_get_complete_xid()
RETURNS int
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;


--CREATE FUNCTION orioledb_rewind_queue_age()
--RETURNS bigint
--AS 'MODULE_PATHNAME'
--VOLATILE LANGUAGE C;
/* contrib/orioledb/sql/orioledb--1.4--1.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.5'" to load this file. \quit

CREATE FUNCTION orioledb_rewind_set_complete(xid int, oxid bigint)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_rewind_sync()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

