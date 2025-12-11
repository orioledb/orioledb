/*-------------------------------------------------------------------------
 *
 * wal.c
 *		Routines dealing with WAL for orioledb.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/wal.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "access/xloginsert.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "transam/oxid.h"

#include "replication/message.h"
#include "storage/proc.h"

static char local_wal_buffer[LOCAL_WAL_BUFFER_SIZE];
static int	local_wal_buffer_offset = 0;
static bool local_wal_has_material_changes = false;
static bool local_wal_contains_xid = false;
static bool local_wal_contains_switch_xid = false;
static ORelOids local_oids = {InvalidOid, InvalidOid, InvalidOid};
static OIndexType local_type = oIndexInvalid;

static void add_finish_wal_record(uint8 rec_type, OXid xmin);
static void add_joint_commit_wal_record(TransactionId xid, OXid xmin);
static void add_xid_wal_record(OXid oxid, TransactionId logicalXid);
static void add_xid_wal_record_if_needed(void);
static void add_rel_wal_record(ORelOids oids, OIndexType type, uint32 version);
static void flush_local_wal_if_needed(int required_length);
static inline void add_local_modify(uint8 record_type, OTuple record, OffsetNumber length, OTuple record2, OffsetNumber length2);
static void add_modify_wal_record_extended(uint8 rec_type, BTreeDescr *desc,
										   OTuple tuple, OffsetNumber length, OTuple tuple2, OffsetNumber length2, char relreplident, uint32 version);
static void add_relreplident_wal_record(char relreplident);

#define XID_RESERVED_LENGTH ((local_wal_contains_xid) ? 0 : sizeof(WALRecXid))

const char *
wal_record_type_to_string(int wal_record)
{
	switch (wal_record)
	{
		 /* 0 */ case WAL_REC_NONE:
			return "NONE";
		 /* 1 */ case WAL_REC_XID:
			return "XID";
		 /* 2 */ case WAL_REC_COMMIT:
			return "COMMIT";
		 /* 3 */ case WAL_REC_ROLLBACK:
			return "ROLLBACK";
		 /* 4 */ case WAL_REC_RELATION:
			return "RELATION";
		 /* 5 */ case WAL_REC_INSERT:
			return "INSERT";
		 /* 6 */ case WAL_REC_UPDATE:
			return "UPDATE";
		 /* 7 */ case WAL_REC_DELETE:
			return "DELETE";
		 /* 8 */ case WAL_REC_O_TABLES_META_LOCK:
			return "O_TABLES_META_LOCK";
		 /* 9 */ case WAL_REC_O_TABLES_META_UNLOCK:
			return "O_TABLES_META_UNLOCK";
		 /* 10 */ case WAL_REC_SAVEPOINT:
			return "SAVEPOINT";
		 /* 11 */ case WAL_REC_ROLLBACK_TO_SAVEPOINT:
			return "ROLLBACK_TO_SAVEPOINT";
		 /* 12 */ case WAL_REC_JOINT_COMMIT:
			return "JOINT_COMMIT";
		 /* 13 */ case WAL_REC_TRUNCATE:
			return "TRUNCATE";
		 /* 14 */ case WAL_REC_BRIDGE_ERASE:
			return "BRIDGE_ERASE";
		 /* 15 */ case WAL_REC_REINSERT:
			return "REINSERT";
		 /* 16 */ case WAL_REC_REPLAY_FEEDBACK:
			return "REPLAY_FEEDBACK";
		 /* 17 */ case WAL_REC_SWITCH_LOGICAL_XID:
			return "SWITCH_LOGICAL_XID";
		 /* 18 */ case WAL_REC_RELREPLIDENT:
			return "RELREPLIDENT";
		default:
			return "UNKNOWN";
	}
	return "UNKNOWN";
}

#define PARSE(ptr, valptr) \
do \
{ \
	if (valptr) \
	{ \
		memcpy(valptr, ptr, sizeof(*(valptr))); \
	} \
	ptr += sizeof(*(valptr)); \
} while(0)

/* Parser for WAL_REC_XID */
Pointer
wal_parse_rec_xid(Pointer ptr, OXid *oxid, TransactionId *logicalXid, TransactionId *heapXid, uint16 wal_version)
{
	Assert(ptr);

	PARSE(ptr, oxid);
	PARSE(ptr, logicalXid);
	if (wal_version >= 17)
	{
		PARSE(ptr, heapXid);
	}

	return ptr;
}

/* Parser for WAL_REC_COMMIT and WAL_REC_ROLLBACK */
Pointer
wal_parse_rec_finish(Pointer ptr, OXid *xmin, CommitSeqNo *csn)
{
	Assert(ptr);

	PARSE(ptr, xmin);
	PARSE(ptr, csn);

	return ptr;
}

/* Parser for WAL_REC_JOINT_COMMIT */
Pointer
wal_parse_rec_joint_commit(Pointer ptr, TransactionId *xid, OXid *xmin, CommitSeqNo *csn)
{
	Assert(ptr);

	PARSE(ptr, xid);
	PARSE(ptr, xmin);
	PARSE(ptr, csn);

	return ptr;
}

/* Parser for WAL_REC_RELATION */
Pointer
wal_parse_rec_relation(Pointer ptr, uint8 *treeType, ORelOids *oids, OXid *xmin, CommitSeqNo *csn, CommandId *cid, uint32 *version)
{
	Assert(ptr);
	Assert(oids);

	PARSE(ptr, treeType);
	PARSE(ptr, &oids->datoid);
	PARSE(ptr, &oids->reloid);
	PARSE(ptr, &oids->relnode);

	PARSE(ptr, xmin);
	PARSE(ptr, csn);
	PARSE(ptr, cid);

	PARSE(ptr, version);

	return ptr;
}

/* Parser for WAL_REC_RELREPLIDENT */
Pointer
wal_parse_rec_relreplident(Pointer ptr, char *relreplident, Oid *relreplident_ix_oid)
{
	Assert(ptr);
	Assert(relreplident);
	/* Should be set only once and only from default */
	Assert(*relreplident == REPLICA_IDENTITY_DEFAULT);

	PARSE(ptr, relreplident);

	/*
	 * relreplident_ix_oid is reserved in the WAL_REC_RELREPLIDENT for the
	 * future implementation of REPLICA IDENTITY USING INDEX and not used now
	 */
	PARSE(ptr, relreplident_ix_oid);
	Assert(*relreplident_ix_oid == InvalidOid);

	return ptr;
}

/* Parser for WAL_REC_O_TABLES_META_UNLOCK */
Pointer
wal_parse_rec_o_tables_meta_unlock(Pointer ptr, ORelOids *oids, Oid *old_relnode)
{
	Assert(ptr);
	Assert(oids);

	PARSE(ptr, &oids->datoid);
	PARSE(ptr, &oids->reloid);
	PARSE(ptr, old_relnode);
	PARSE(ptr, &oids->relnode);

	return ptr;
}

/* Parser for WAL_REC_SAVEPOINT */
Pointer
wal_parse_rec_savepoint(Pointer ptr, SubTransactionId *parentSubid, TransactionId *logicalXid, TransactionId *parentLogicalXid)
{
	Assert(ptr);

	PARSE(ptr, parentSubid);
	PARSE(ptr, logicalXid);
	PARSE(ptr, parentLogicalXid);

	return ptr;
}

/* Parser for WAL_REC_ROLLBACK_TO_SAVEPOINT */
Pointer
wal_parse_rec_rollback_to_savepoint(Pointer ptr, SubTransactionId *parentSubid, OXid *xmin, CommitSeqNo *csn, uint16 wal_version)
{
	Assert(ptr);

	PARSE(ptr, parentSubid);
	if (wal_version >= 17)
	{
		PARSE(ptr, xmin);
		PARSE(ptr, csn);
	}

	return ptr;
}

/* Parser for WAL_REC_TRUNCATE */
Pointer
wal_parse_rec_truncate(Pointer ptr, ORelOids *oids)
{
	Assert(ptr);
	Assert(oids);

	PARSE(ptr, &oids->datoid);
	PARSE(ptr, &oids->reloid);
	PARSE(ptr, &oids->relnode);

	return ptr;
}

/* Parser for WAL_REC_BRIDGE_ERASE */
Pointer
wal_parse_rec_bridge_erase(Pointer ptr, ItemPointerData *iptr)
{
	Assert(ptr);

	PARSE(ptr, iptr);

	return ptr;
}

/* Parser for WAL_REC_SWITCH_LOGICAL_XID */
Pointer
wal_parse_rec_switch_logical_xid(Pointer ptr, TransactionId *topXid, TransactionId *subXid)
{
	Assert(ptr);

	PARSE(ptr, topXid);
	PARSE(ptr, subXid);

	return ptr;
}

Pointer
wal_container_read_header(Pointer ptr, uint16 *version, uint8 *flags)
{
	uint16		wal_version = 0;
	uint8		wal_flags = 0;

	if (*ptr >= FIRST_ORIOLEDB_WAL_VERSION)
	{
		/*
		 * Container starts with a valid WAL version. First WAL record is just
		 * after it.
		 */
		memcpy(&wal_version, ptr, sizeof(wal_version));
		ptr += sizeof(wal_version);
	}
	else
	{
		/*
		 * Container starts with rec_type of first WAL record (its maximum
		 * value was under FIRST_ORIOLEDB_WAL_VERSION at the time of
		 * introducing WAL versioning. Consider this as version 0 and don't
		 * increase pointer
		 */
		wal_version = 0;
	}

	if (wal_version > ORIOLEDB_WAL_VERSION)
	{
#ifdef IS_DEV
		/* Always fail tests on difference */
		elog(FATAL, "Can't apply WAL container version %u that is newer than supported %u. Intentionally fail tests", wal_version, ORIOLEDB_WAL_VERSION);
#else
		elog(WARNING, "Can't apply WAL container version %u that is newer than supported %u", wal_version, ORIOLEDB_WAL_VERSION);
		/* Further fail and output is caller-specific */
#endif
	}
	else if (wal_version < ORIOLEDB_WAL_VERSION)
	{
#ifdef IS_DEV
		/* Always fail tests on difference */
		elog(FATAL, "WAL container version %u is older than current %u. Intentionally fail tests", wal_version, ORIOLEDB_WAL_VERSION);
#else
		elog(LOG, "WAL container version %u is older than current %u. Applying with conversion.", wal_version, ORIOLEDB_WAL_VERSION);
#endif
	}

	if (wal_version >= ORIOLEDB_XACT_INFO_WAL_VERSION)
	{
		/*
		 * WAL container flags were added by ORIOLEDB_XACT_INFO_WAL_VERSION.
		 */
		memcpy(&wal_flags, ptr, sizeof(wal_flags));
		ptr += sizeof(wal_flags);
	}

	*version = wal_version;
	*flags = wal_flags;

	return ptr;
}

void
add_modify_wal_record(uint8 rec_type, BTreeDescr *desc,
					  OTuple tuple, OffsetNumber length, char relreplident, uint32 version)
{
	OTuple		nulltup;

	O_TUPLE_SET_NULL(nulltup);
	add_modify_wal_record_extended(rec_type, desc, tuple, length, nulltup, 0, relreplident, version);
}

/*
 * Extended version of add_modify_wal_record for WAL records that can accommodate two tuples.
 * This is used for UPDATE/DELETE with REPLICA IDENTITY FULL and for REINSERT
 */
static void
add_modify_wal_record_extended(uint8 rec_type, BTreeDescr *desc,
							   OTuple tuple, OffsetNumber length, OTuple tuple2, OffsetNumber length2, char relreplident, uint32 version)
{
	int			required_length;
	ORelOids	oids = desc->oids;
	OIndexType	type = desc->type;
	bool		write_two_tuples;

	/* Do not write WAL during recovery */
	if (OXidIsValid(recovery_oxid))
		return;

	if (!IS_SYS_TREE_OIDS(oids) && type == oIndexPrimary)
	{
		OIndexDescr *id = (OIndexDescr *) desc->arg;

		oids = id->tableOids;
		type = oIndexInvalid;
	}

	Assert(!is_recovery_process());
	Assert(rec_type == WAL_REC_INSERT || rec_type == WAL_REC_UPDATE || rec_type == WAL_REC_DELETE || rec_type == WAL_REC_REINSERT);
	Assert(!O_TUPLE_IS_NULL(tuple));

	write_two_tuples = (rec_type == WAL_REC_REINSERT || (rec_type == WAL_REC_UPDATE && relreplident == REPLICA_IDENTITY_FULL));

	if (!write_two_tuples)
	{
		Assert(length2 == 0);
		Assert(O_TUPLE_IS_NULL(tuple2));
		required_length = sizeof(WALRecModify1) + length;
	}
	else
	{
		Assert(length2 > 0);
		Assert(!O_TUPLE_IS_NULL(tuple2));
		required_length = sizeof(WALRecModify2) + length + length2;
	}


	elog(DEBUG4, "add_modify_wal_record_extended length1 %d length2 %d", length, length2);
	if (!ORelOidsIsEqual(local_oids, oids) || type != local_type)
		required_length += sizeof(WALRecRelation);

	if (relreplident != REPLICA_IDENTITY_DEFAULT)
		required_length += sizeof(WALRecRelReplident);

	flush_local_wal_if_needed(required_length);
	Assert(local_wal_buffer_offset + required_length + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	if (!ORelOidsIsEqual(local_oids, oids) || type != local_type)
	{
		add_rel_wal_record(oids, type, version);
		if (relreplident != REPLICA_IDENTITY_DEFAULT)
			add_relreplident_wal_record(relreplident);
	}

	add_local_modify(rec_type, tuple, length, tuple2, length2);
}

void
add_bridge_erase_wal_record(BTreeDescr *desc, ItemPointer iptr)
{
	int			required_length;
	ORelOids	oids = desc->oids;
	OIndexType	type = desc->type;
	WALRecBridgeErase *rec;

	/* Do not write WAL during recovery */
	if (OXidIsValid(recovery_oxid))
		return;

	if (!IS_SYS_TREE_OIDS(oids) && type == oIndexPrimary)
	{
		OIndexDescr *id = (OIndexDescr *) desc->arg;

		oids = id->tableOids;
		type = oIndexInvalid;
	}

	Assert(!is_recovery_process());

	required_length = sizeof(WALRecBridgeErase);

	if (!ORelOidsIsEqual(local_oids, oids) || type != local_type)
		required_length += sizeof(WALRecRelation);

	flush_local_wal_if_needed(required_length);
	Assert(local_wal_buffer_offset + required_length + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	if (OXidIsValid(get_current_oxid_if_any()))
		add_xid_wal_record_if_needed();

	if (!ORelOidsIsEqual(local_oids, oids) || type != local_type)
		add_rel_wal_record(oids, type, O_TABLE_INVALID_VERSION /* @TODO !!! version */ );

	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecBridgeErase *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = WAL_REC_BRIDGE_ERASE;
	memcpy(rec->iptr, iptr, sizeof(rec->iptr));
	local_wal_buffer_offset += sizeof(*rec);
}

/*
 * Adds the record to the local_wal_buffer.
 */
static inline void
add_local_modify(uint8 record_type, OTuple record1, OffsetNumber length1, OTuple record2, OffsetNumber length2)
{
	Assert(!O_TUPLE_IS_NULL(record1));
	Assert(length1);

	if (!O_TUPLE_IS_NULL(record2))
	{
		/* Two-tuple modify record */
		WALRecModify2 *wal_rec;

		Assert(length2);
		Assert(local_wal_buffer_offset + sizeof(*wal_rec) + length1 + length2 + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);
		wal_rec = (WALRecModify2 *) (&local_wal_buffer[local_wal_buffer_offset]);
		wal_rec->recType = record_type;
		wal_rec->tupleFormatFlags1 = record1.formatFlags;
		wal_rec->tupleFormatFlags2 = record2.formatFlags;
		memcpy(wal_rec->length1, &length1, sizeof(OffsetNumber));
		memcpy(wal_rec->length2, &length2, sizeof(OffsetNumber));
		local_wal_buffer_offset += sizeof(*wal_rec);

		memcpy(&local_wal_buffer[local_wal_buffer_offset], record1.data, length1);
		local_wal_buffer_offset += length1;
		memcpy(&local_wal_buffer[local_wal_buffer_offset], record2.data, length2);
		local_wal_buffer_offset += length2;
	}
	else
	{
		/* One-tuple modify record */
		WALRecModify1 *wal_rec;

		Assert(local_wal_buffer_offset + sizeof(*wal_rec) + length1 + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);
		Assert(length2 == 0);

		wal_rec = (WALRecModify1 *) (&local_wal_buffer[local_wal_buffer_offset]);
		wal_rec->recType = record_type;
		wal_rec->tupleFormatFlags = record1.formatFlags;
		memcpy(wal_rec->length, &length1, sizeof(OffsetNumber));
		local_wal_buffer_offset += sizeof(*wal_rec);

		memcpy(&local_wal_buffer[local_wal_buffer_offset], record1.data, length1);
		local_wal_buffer_offset += length1;
	}

	local_wal_has_material_changes = true;
}

XLogRecPtr
wal_commit(OXid oxid, TransactionId logicalXid, bool isAutonomous)
{
	XLogRecPtr	walPos;
	int			recLength;

	Assert(!is_recovery_process());

	if (!local_wal_has_material_changes)
	{
		local_wal_buffer_offset = 0;
		local_type = oIndexInvalid;
		local_oids = create_oids();
		return InvalidXLogRecPtr;
	}

	recLength = sizeof(WALRecFinish) + ((synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_APPLY) ? sizeof(WALRec) : 0);
	flush_local_wal_if_needed(recLength);
	Assert(local_wal_buffer_offset + recLength + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	if (!local_wal_contains_xid)
		add_xid_wal_record(oxid, logicalXid);

	add_finish_wal_record(WAL_REC_COMMIT, pg_atomic_read_u64(&xid_meta->runXmin));
	walPos = flush_local_wal(true, !isAutonomous);
	local_wal_has_material_changes = false;

	elog(DEBUG4, "COMMIT oxid %lu logicalXid %u", oxid, logicalXid);

	return walPos;
}

XLogRecPtr
wal_joint_commit(OXid oxid, TransactionId logicalXid, TransactionId xid)
{
	XLogRecPtr	walPos;

	Assert(!is_recovery_process());

	flush_local_wal_if_needed(sizeof(WALRecJointCommit));
	Assert(local_wal_buffer_offset + sizeof(WALRecJointCommit) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	if (!local_wal_contains_xid)
		add_xid_wal_record(oxid, logicalXid);

	add_joint_commit_wal_record(xid, pg_atomic_read_u64(&xid_meta->runXmin));
	walPos = flush_local_wal(true, false);
	local_wal_has_material_changes = false;

	/*
	 * Don't need to flush local WAL, because we only commit if builtin
	 * transaction commits.
	 */
	return walPos;
}

void
wal_after_commit()
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	pg_atomic_write_u64(&curProcData->commitInProgressXlogLocation, OWalInvalidCommitPos);
}

void
wal_rollback(OXid oxid, TransactionId logicalXid, bool isAutonomous)
{
	XLogRecPtr	wait_pos;

	if (!local_wal_has_material_changes)
	{
		local_wal_buffer_offset = 0;
		local_type = oIndexInvalid;
		local_oids = create_oids();
		return;
	}

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(WALRecFinish));
	Assert(local_wal_buffer_offset + sizeof(WALRecFinish) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	if (!local_wal_contains_xid)
		add_xid_wal_record(oxid, logicalXid);

	add_finish_wal_record(WAL_REC_ROLLBACK,
						  pg_atomic_read_u64(&xid_meta->runXmin));
	wait_pos = flush_local_wal(false, !isAutonomous);
	local_wal_has_material_changes = false;

	elog(DEBUG4, "ROLLBACK oxid %lu logicalXid %u", oxid, logicalXid);

	if (synchronous_commit > SYNCHRONOUS_COMMIT_OFF)
		XLogFlush(wait_pos);
}

static void
add_finish_wal_record(uint8 rec_type, OXid xmin)
{
	WALRecFinish *rec;
	int			recLength PG_USED_FOR_ASSERTS_ONLY;
	CommitSeqNo csn;

	Assert(!is_recovery_process());
	Assert(rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK);

	ereport(DEBUG4, errmsg("rec_type %d (%s)", rec_type, wal_record_type_to_string(rec_type)));

	recLength = sizeof(WALRecFinish);
	if (rec_type == WAL_REC_COMMIT &&
		synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_APPLY)
		recLength += sizeof(WALRec);

	Assert(local_wal_buffer_offset + recLength + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecFinish *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = rec_type;
	memcpy(rec->xmin, &xmin, sizeof(xmin));
	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));

	local_wal_buffer_offset += sizeof(*rec);

	local_wal_contains_switch_xid = false;
	if (rec_type == WAL_REC_COMMIT &&
		synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_APPLY)
	{
		WALRec	   *feedbackRec = (WALRec *) (&local_wal_buffer[local_wal_buffer_offset]);

		feedbackRec->recType = WAL_REC_REPLAY_FEEDBACK;
		local_wal_buffer_offset += sizeof(*feedbackRec);
	}
}

static void
add_joint_commit_wal_record(TransactionId xid, OXid xmin)
{
	WALRecJointCommit *rec;
	CommitSeqNo csn;

	Assert(!is_recovery_process());

	flush_local_wal_if_needed(sizeof(*rec));

	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecJointCommit *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = WAL_REC_JOINT_COMMIT;
	memcpy(rec->xid, &xid, sizeof(xid));
	memcpy(rec->xmin, &xmin, sizeof(xmin));
	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));
	local_wal_buffer_offset += sizeof(*rec);

	local_wal_contains_switch_xid = false;
}

/*
 * Returns size of a new record.
 */
static void
add_xid_wal_record(OXid oxid, TransactionId logicalXid)
{
	WALRecXid  *rec;
	TransactionId heapXid;

	Assert(!local_wal_contains_xid);
	local_wal_contains_xid = true;
	Assert(!is_recovery_process());
	Assert(OXidIsValid(oxid));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	heapXid = GetTopTransactionIdIfAny();

	elog(DEBUG4, "WAL_REC_XID oxid %lu logicalXid %u heapXid %u", oxid, logicalXid, heapXid);

	rec = (WALRecXid *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = WAL_REC_XID;
	memcpy(rec->oxid, &oxid, sizeof(OXid));
	memcpy(rec->logicalXid, &logicalXid, sizeof(TransactionId));
	memcpy(rec->heapXid, &heapXid, sizeof(TransactionId));

	local_wal_buffer_offset += sizeof(*rec);
}

static void
add_xid_wal_record_if_needed(void)
{
	if (!local_wal_contains_xid)
	{
		OXid		oxid = get_current_oxid_if_any();
		TransactionId logicalXid = get_current_logical_xid();

		Assert(oxid != InvalidOXid);
		add_xid_wal_record(oxid, logicalXid);
	}
}

static void
add_relreplident_wal_record(char relreplident)
{
	WALRecRelReplident *rec = (WALRecRelReplident *) (&local_wal_buffer[local_wal_buffer_offset]);
	Oid			ix_oid = InvalidOid;

	Assert(!is_recovery_process());
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec->recType = WAL_REC_RELREPLIDENT;
	rec->relreplident = relreplident;
	memcpy(rec->relreplident_ix_oid, &ix_oid, sizeof(Oid));

	local_wal_buffer_offset += sizeof(*rec);
}

static void
add_rel_wal_record(ORelOids oids, OIndexType type, uint32 version)
{
	OXid		runXmin;
	CommitSeqNo csn;
	CommandId	cid;
	WALRecRelation *rec = (WALRecRelation *) (&local_wal_buffer[local_wal_buffer_offset]);

	Assert(!is_recovery_process());
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec->recType = WAL_REC_RELATION;
	rec->treeType = type;
	memcpy(rec->datoid, &oids.datoid, sizeof(Oid));
	memcpy(rec->reloid, &oids.reloid, sizeof(Oid));
	memcpy(rec->relnode, &oids.relnode, sizeof(Oid));

	runXmin = pg_atomic_read_u64(&xid_meta->runXmin);
	memcpy(rec->xmin, &runXmin, sizeof(runXmin));

	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));

	cid = o_get_current_command();
	memcpy(rec->cid, &cid, sizeof(cid));

	memcpy(rec->version, &version, sizeof(version));

	elog(LOG, "[%s] WAL_REC_RELATION xmin/csn/cid %lu/%lu/%lu version %u", __func__, runXmin, csn, cid, version);

	local_wal_buffer_offset += sizeof(*rec);

	local_type = type;
	local_oids = oids;
}

void
add_o_tables_meta_lock_wal_record(void)
{
	WALRec	   *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRec *) (&local_wal_buffer[local_wal_buffer_offset]);

	rec->recType = WAL_REC_O_TABLES_META_LOCK;

	local_wal_buffer_offset += sizeof(*rec);
}

void
add_o_tables_meta_unlock_wal_record(ORelOids oids, Oid oldRelnode)
{
	WALRecOTablesUnlockMeta *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecOTablesUnlockMeta *) (&local_wal_buffer[local_wal_buffer_offset]);

	rec->recType = WAL_REC_O_TABLES_META_UNLOCK;
	memcpy(rec->datoid, &oids.datoid, sizeof(Oid));
	memcpy(rec->reloid, &oids.reloid, sizeof(Oid));
	memcpy(rec->old_relnode, &oldRelnode, sizeof(Oid));
	memcpy(rec->new_relnode, &oids.relnode, sizeof(Oid));

	local_wal_buffer_offset += sizeof(*rec);
}

void
add_switch_logical_xid_wal_record(TransactionId logicalXid_top, TransactionId logicalXid_sub)
{
	WALRecSwitchLogicalXid *rec;

	if (local_wal_contains_switch_xid)
		return;

	local_wal_contains_switch_xid = true;

	Assert(!is_recovery_process());
	Assert(TransactionIdIsValid(logicalXid_top));
	Assert(TransactionIdIsValid(logicalXid_sub));
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecSwitchLogicalXid *) (&local_wal_buffer[local_wal_buffer_offset]);

	rec->recType = WAL_REC_SWITCH_LOGICAL_XID;
	memcpy(rec->topXid, &logicalXid_top, sizeof(TransactionId));
	memcpy(rec->subXid, &logicalXid_sub, sizeof(TransactionId));

	local_wal_buffer_offset += sizeof(*rec);
}

void
add_savepoint_wal_record(SubTransactionId parentSubid,
						 TransactionId prentLogicalXid)
{
	WALRecSavepoint *rec;
	TransactionId logicalXid = get_current_logical_xid();

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecSavepoint *) (&local_wal_buffer[local_wal_buffer_offset]);

	rec->recType = WAL_REC_SAVEPOINT;
	memcpy(rec->parentSubid, &parentSubid, sizeof(SubTransactionId));
	memcpy(rec->parentLogicalXid, &prentLogicalXid, sizeof(TransactionId));
	memcpy(rec->logicalXid, &logicalXid, sizeof(TransactionId));

	local_wal_buffer_offset += sizeof(*rec);
}

void
add_rollback_to_savepoint_wal_record(SubTransactionId parentSubid)
{
	WALRecRollbackToSavepoint *rec;
	OXid		runXmin;
	CommitSeqNo csn;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	local_wal_contains_xid = false;
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecRollbackToSavepoint *) (&local_wal_buffer[local_wal_buffer_offset]);

	rec->recType = WAL_REC_ROLLBACK_TO_SAVEPOINT;
	memcpy(rec->parentSubid, &parentSubid, sizeof(SubTransactionId));

	runXmin = pg_atomic_read_u64(&xid_meta->runXmin);
	memcpy(rec->xmin, &runXmin, sizeof(runXmin));
	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));

	local_wal_buffer_offset += sizeof(*rec);

	/*
	 * Force adding xid record on future changes going after this rollback to
	 * sp, this is necessary for correct xids restoring in logical decoder
	 */
	local_wal_contains_xid = false;
}

bool
local_wal_is_empty(void)
{
	return (local_wal_buffer_offset == 0);
}

/*
 * Returns end position of a new WAL container.
 */
XLogRecPtr
flush_local_wal(bool isCommit, bool withXactTime)
{
	XLogRecPtr	location;
	int			length = local_wal_buffer_offset;

	Assert(!is_recovery_process());
	Assert(length > 0);

	if (isCommit)
		pg_atomic_write_u64(&GET_CUR_PROCDATA()->commitInProgressXlogLocation, OWalTmpCommitPos);
	location = log_logical_wal_container(local_wal_buffer, length, withXactTime);
	if (isCommit)
		pg_atomic_write_u64(&GET_CUR_PROCDATA()->commitInProgressXlogLocation, location);

	local_wal_buffer_offset = 0;
	local_wal_contains_xid = false;
	local_wal_contains_switch_xid = false;
	local_type = oIndexInvalid;
	local_oids = create_oids();
	local_wal_has_material_changes = true;

	return location;
}

static void
flush_local_wal_if_needed(int required_length)
{
	Assert(!is_recovery_process());
	if (local_wal_buffer_offset + required_length + XID_RESERVED_LENGTH > LOCAL_WAL_BUFFER_SIZE)
	{
		log_logical_wal_container(local_wal_buffer, local_wal_buffer_offset,
								  false);

		local_wal_buffer_offset = 0;
		local_wal_contains_xid = false;
		local_wal_contains_switch_xid = false;
		local_type = oIndexInvalid;
		local_oids = create_oids();
		local_wal_has_material_changes = true;
	}
}

XLogRecPtr
log_logical_wal_container(Pointer ptr, int length, bool withXactTime)
{
	uint16		wal_version = ORIOLEDB_WAL_VERSION;
	uint8		flags = 0;
	WALRecXactInfo rec;

	Assert(ORIOLEDB_WAL_VERSION >= FIRST_ORIOLEDB_WAL_VERSION);

	XLogBeginInsert();
	XLogRegisterData((char *) (&wal_version), sizeof(wal_version));

	if (withXactTime)
		flags |= WAL_CONTAINER_HAS_XACT_INFO;

	XLogRegisterData((char *) (&flags), sizeof(flags));

	if (withXactTime)
	{
		TimestampTz xactTime = GetCurrentTransactionStopTimestamp();
		TransactionId xid = GetTopTransactionIdIfAny();

		memcpy(rec.xactTime, &xactTime, sizeof(xactTime));
		memcpy(rec.xid, &xid, sizeof(xid));

		XLogRegisterData((char *) &rec, sizeof(rec));
	}

	XLogRegisterData(ptr, length);
	return XLogInsert(ORIOLEDB_RMGR_ID, ORIOLEDB_XLOG_CONTAINER);
}

/*
 * Makes WAL insert record.
 */
void
o_wal_insert(BTreeDescr *desc, OTuple tuple, char relreplident, uint32 version)
{
	OTuple		wal_record;
	bool		call_pfree;
	int			size;

	elog(LOG, "[%s] [ %u %u %u ] version %u", __func__,
		 desc->oids.datoid, desc->oids.reloid, desc->oids.relnode,
		 version);

	Assert(!O_TUPLE_IS_NULL(tuple));
	wal_record = recovery_rec_insert(desc, tuple, &call_pfree, &size);
	add_modify_wal_record(WAL_REC_INSERT, desc, wal_record, size, relreplident, version);
	if (call_pfree)
		pfree(wal_record.data);
}

/*
 * Makes WAL update record.
 */
void
o_wal_update(BTreeDescr *desc, OTuple tuple, OTuple oldtuple, char relreplident, uint32 version)
{
	OTuple		wal_record1;
	OTuple		wal_record2;
	bool		call_pfree1;
	bool		call_pfree2 = false;
	int			size1;
	int			size2;

	elog(LOG, "[%s] [ %u %u %u ] version %u", __func__,
		 desc->oids.datoid, desc->oids.reloid, desc->oids.relnode,
		 version);

	Assert(!O_TUPLE_IS_NULL(tuple));
	wal_record1 = recovery_rec_update(desc, tuple, &call_pfree1, &size1);

	/*
	 * For REPLICA_IDENTITY_FULL include new and old tuples into
	 * WAL_REC_UPDATE
	 */
	if (relreplident != REPLICA_IDENTITY_FULL)
	{
		add_modify_wal_record(WAL_REC_UPDATE, desc, wal_record1, size1, relreplident, version);
	}
	else
	{
		Assert(!O_TUPLE_IS_NULL(oldtuple));
		wal_record2 = recovery_rec_update(desc, oldtuple, &call_pfree2, &size2);
		add_modify_wal_record_extended(WAL_REC_UPDATE, desc, wal_record1, size1, wal_record2, size2, relreplident, version);
		if (call_pfree2)
			pfree(wal_record2.data);
	}

	if (call_pfree1)
		pfree(wal_record1.data);
}

/*
 * Makes WAL delete record.
 */
void
o_wal_delete(BTreeDescr *desc, OTuple tuple, char relreplident, uint32 version)
{
	OTuple		wal_record;
	bool		call_pfree;
	int			size;

	elog(LOG, "[%s] [ %u %u %u ] version %u", __func__,
		 desc->oids.datoid, desc->oids.reloid, desc->oids.relnode,
		 version);

	Assert(!O_TUPLE_IS_NULL(tuple));
	wal_record = recovery_rec_delete(desc, tuple, &call_pfree, &size, relreplident);
	add_modify_wal_record(WAL_REC_DELETE, desc, wal_record, size, relreplident, version);

	if (call_pfree)
		pfree(wal_record.data);
}

/*
 * Makes WAL delete+insert record.
 */
void
o_wal_reinsert(BTreeDescr *desc, OTuple oldtuple, OTuple newtuple, char relreplident, uint32 version)
{
	OTuple		oldrecord;
	OTuple		newrecord;
	bool		new_call_pfree;
	bool		old_call_pfree;
	int			newsize;
	int			oldsize;

	Assert(!O_TUPLE_IS_NULL(newtuple));
	Assert(!O_TUPLE_IS_NULL(oldtuple));

	oldrecord = recovery_rec_delete(desc, oldtuple, &old_call_pfree, &oldsize, relreplident);
	newrecord = recovery_rec_insert(desc, newtuple, &new_call_pfree, &newsize);
	add_modify_wal_record_extended(WAL_REC_REINSERT, desc, newrecord, newsize, oldrecord, oldsize, relreplident, version);
	if (old_call_pfree)
	{
		pfree(oldrecord.data);
	}
	if (new_call_pfree)
	{
		pfree(newrecord.data);
	}
}

/* Could be used only for system trees and bridge trees that are not replicated logically */
void
o_wal_delete_key(BTreeDescr *desc, OTuple key, bool is_bridge_index, uint32 version)
{
	OTuple		wal_record;
	bool		call_pfree;
	int			size;

	Assert(IS_SYS_TREE_OIDS(desc->oids) || is_bridge_index);
	Assert(!O_TUPLE_IS_NULL(key));
	wal_record = recovery_rec_delete_key(desc, key, &call_pfree, &size);
	add_modify_wal_record(WAL_REC_DELETE, desc, wal_record, size, REPLICA_IDENTITY_DEFAULT, version);

	if (call_pfree)
		pfree(wal_record.data);
}

void
add_truncate_wal_record(ORelOids oids)
{
	WALRecTruncate *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecTruncate *) (&local_wal_buffer[local_wal_buffer_offset]);

	rec->recType = WAL_REC_TRUNCATE;
	memcpy(rec->datoid, &oids.datoid, sizeof(Oid));
	memcpy(rec->reloid, &oids.reloid, sizeof(Oid));
	memcpy(rec->relnode, &oids.relnode, sizeof(Oid));

	local_wal_buffer_offset += sizeof(*rec);

	local_type = oIndexInvalid;
	local_oids.datoid = InvalidOid;
	local_oids.reloid = InvalidOid;
	local_oids.relnode = InvalidOid;
}

bool
get_local_wal_has_material_changes(void)
{
	return local_wal_has_material_changes;
}

void
set_local_wal_has_material_changes(bool value)
{
	local_wal_has_material_changes = value;
}

/*
 * Read one or two tuples from modify WAL record.
 * Two tuples in certain cases: (1) WAL_REC_REINSERT, (2) WAL_REC_UPDATE with REPLICA_IDENTITY_FULL
 */
Pointer
wal_parse_rec_modify(Pointer ptr, OFixedTuple *tuple1, OFixedTuple *tuple2, OffsetNumber *length1_out, bool read_two_tuples)
{
	OffsetNumber length1;
	OffsetNumber length2;

	if (!read_two_tuples)
	{
		PARSE(ptr, &tuple1->tuple.formatFlags);
		memcpy(&length1, ptr, sizeof(OffsetNumber));
		ptr += sizeof(OffsetNumber);
		Assert(length1 > 0);

		Assert(tuple1->fixedData);
		memcpy(tuple1->fixedData, ptr, length1);
		if (length1 != MAXALIGN(length1))
			memset(&tuple1->fixedData[length1], 0, MAXALIGN(length1) - length1);

		ptr += length1;
		tuple1->tuple.data = tuple1->fixedData;
		O_TUPLE_SET_NULL(tuple2->tuple);
	}
	else
	{
		PARSE(ptr, &tuple1->tuple.formatFlags);
		PARSE(ptr, &tuple2->tuple.formatFlags);
		memcpy(&length1, ptr, sizeof(OffsetNumber));
		ptr += sizeof(OffsetNumber);
		memcpy(&length2, ptr, sizeof(OffsetNumber));
		ptr += sizeof(OffsetNumber);

		Assert(length1 > 0 && length2 > 0);

		Assert(tuple1->fixedData);
		memcpy(tuple1->fixedData, ptr, length1);
		if (length1 != MAXALIGN(length1))
			memset(&tuple1->fixedData[length1], 0, MAXALIGN(length1) - length1);

		ptr += length1;
		tuple1->tuple.data = tuple1->fixedData;

		Assert(tuple2->fixedData);
		memcpy(tuple2->fixedData, ptr, length2);
		if (length2 != MAXALIGN(length2))
			memset(&tuple2->fixedData[length2], 0, MAXALIGN(length2) - length2);

		ptr += length2;
		tuple2->tuple.data = tuple2->fixedData;
	}
	*length1_out = length1;

	return ptr;
}
