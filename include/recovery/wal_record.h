/*-------------------------------------------------------------------------
 *
 * wal_record.h
 *	  WAL records list. The exact representation of a WAL record is determined
 *	  by a X-Macro, which is not defined in this file; it can be defined by the
 *	  caller for special purposes.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/wal_record.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __WAL_RECORD_H__
#define __WAL_RECORD_H__

/*
 * WAL record types
 *
 * WAL_REC_REINSERT:
 *   UPDATE with changed pkey represented as DELETE + INSERT in OrioleDB but
 *   externally exported as an UPDATE in logical decoding
 *
 * WAL_REC_SWITCH_LOGICAL_XID:
 *   Record type for a case when both heap and Oriole apply changes within a
 *   single transaction, so one logical xid is assigned by heap, and the other
 *   is assigned by Oriole.
 *   This record defines a connection between Oriole's sub-transaction xid and
 *   a xid of the top heap transaction which is needed for logical decoder.
 *   Otherwise, without this connection, main transaction suddenly becomes
 *   splitted into two independent parts.
 *   From logical decoder's point of view this looks like two independent
 *   transactions but in fact internally related to each other. This situation
 *   outcomes in troubles for logical decoder with visibility of heap
 *   modifications in Oriole's sub-part due to incorrect state of the
 *   MVCC-historical snapshot.
 */

#define ORIOLE_WAL_RECORDS(X) \
	X(WAL_REC_XID,                   1, "XID",                wal_parse_rec_xid) \
	X(WAL_REC_COMMIT,                2, "COMMIT",             wal_parse_rec_finish) \
	X(WAL_REC_ROLLBACK,              3, "ROLLBACK",           wal_parse_rec_finish) \
	X(WAL_REC_RELATION,              4, "RELATION",           wal_parse_rec_relation) \
	X(WAL_REC_INSERT,                5, "INSERT",             wal_parse_rec_modify) \
	X(WAL_REC_UPDATE,                6, "UPDATE",             wal_parse_rec_modify) \
	X(WAL_REC_DELETE,                7, "DELETE",             wal_parse_rec_modify) \
	X(WAL_REC_O_TABLES_META_LOCK,    8, "META_LOCK",          wal_parse_empty) \
	X(WAL_REC_O_TABLES_META_UNLOCK,  9, "META_UNLOCK",        wal_parse_rec_o_tables_meta_unlock) \
	X(WAL_REC_SAVEPOINT,            10, "SAVEPOINT",          wal_parse_rec_savepoint) \
	X(WAL_REC_ROLLBACK_TO_SAVEPOINT,11, "RB_TO_SP",           wal_parse_rec_rollback_to_savepoint) \
	X(WAL_REC_JOINT_COMMIT,         12, "JOINT_COMMIT",       wal_parse_rec_joint_commit) \
	X(WAL_REC_TRUNCATE,             13, "TRUNCATE",           wal_parse_rec_truncate) \
	X(WAL_REC_BRIDGE_ERASE,         14, "BRIDGE_ERASE",       wal_parse_rec_bridge_erase) \
	X(WAL_REC_REINSERT,             15, "REINSERT",           wal_parse_rec_modify) \
	X(WAL_REC_REPLAY_FEEDBACK,      16, "REPLAY_FEEDBACK",    wal_parse_empty) \
	X(WAL_REC_SWITCH_LOGICAL_XID,   17, "SWITCH_LOGICAL_XID", wal_parse_rec_switch_logical_xid) \
	X(WAL_REC_RELREPLIDENT,         18, "RELREPLIDENT",       wal_parse_rec_relreplident)

typedef enum
{
#define X(sym, val, name, fn) sym = (val),
	ORIOLE_WAL_RECORDS(X)
#undef X
} WalRecordType;

#endif							/* __WAL_RECORD_H__ */
