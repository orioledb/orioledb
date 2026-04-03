/* contrib/orioledb/sql/orioledb--1.6--1.7_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.7'" to load this file. \quit

CREATE FUNCTION orioledb_get_xid_meta(
    OUT nextXid int8,
    OUT runXmin int8,
    OUT globalXmin int8,
    OUT lastXidWhenUpdatedGlobalXmin int8,
    OUT writeInProgressXmin int8,
    OUT writtenXmin int8,
    OUT checkpointRetainXmin int8,
    OUT checkpointRetainXmax int8,
    OUT cleanedXmin int8)
RETURNS record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_get_undo_meta(
    OUT undo_type text,
    OUT lastUsedLocation int8,
    OUT advanceReservedLocation int8,
    OUT writeInProgressLocation int8,
    OUT writtenLocation int8,
    OUT minProcTransactionRetainLocation int8,
    OUT minProcRetainLocation int8,
    OUT minProcReservedLocation int8,
    OUT checkpointRetainStartLocation int8,
    OUT checkpointRetainEndLocation int8,
    OUT cleanedLocation int8,
    OUT cleanedCheckpointStartLocation int8,
    OUT cleanedCheckpointEndLocation int8,
    OUT minRewindRetainLocation int8)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_get_proc_retain_undo_locations(
    OUT pid int4,
    OUT procno int4,
    OUT undo_type text,
    OUT reservedUndoLocation int8,
    OUT transactionUndoRetainLocation int8,
    OUT snapshotRetainUndoLocation int8,
    OUT effectiveRetainLocation int8)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
