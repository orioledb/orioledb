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
static ORelOids local_oids = {InvalidOid, InvalidOid, InvalidOid};
static OIndexType local_type = oIndexInvalid;

static void add_finish_wal_record(uint8 rec_type, OXid xmin);
static void add_joint_commit_wal_record(TransactionId xid, OXid xmin);
static void add_xid_wal_record(OXid oxid, TransactionId logicalXid);
static void add_xid_wal_record_if_needed(void);
static void add_wal_container_header_if_needed(void);
static void add_rel_wal_record(ORelOids oids, OIndexType type);
static void flush_local_wal_if_needed(int required_length);
static inline void add_local_modify(uint8 record_type, OTuple record, OffsetNumber length, OTuple record2, OffsetNumber length2);
static void add_modify_wal_record_extended(uint8 rec_type, BTreeDescr *desc,
										   OTuple tuple, OffsetNumber length, OTuple tuple2, OffsetNumber length2);

#define XID_RESERVED_LENGTH ((local_wal_contains_xid) ? 0 : sizeof(WALRecXid))

uint16
check_wal_container_version(Pointer *ptr)
{
	uint16		wal_version;

	if (**ptr >= FIRST_WAL_VERSION)
	{
		/*
		 * Container starts with a valid WAL version. First WAL record is just
		 * after it.
		 */
		memcpy(&wal_version, *ptr, sizeof(uint16));
		(*ptr) += sizeof(uint16);
	}
	else
	{
		/*
		 * Container starts with rec_type of first WAL record (its maximum
		 * value was under FIRST_WAL_VERSION at the time of introducing WAL
		 * versioning. Consider this as version 0 and don't increase pointer
		 */
		wal_version = 0;
	}

	if (wal_version > CURRENT_WAL_VERSION)
	{
#ifdef IS_DEV
		/* Always fail tests on difference */
		elog(FATAL, "Can't apply WAL container version %u that is newer than supported %u. Intentionally fail tests", wal_version, CURRENT_WAL_VERSION);
#else
		elog(WARNING, "Can't apply WAL container version %u that is newer than supported %u", wal_version, CURRENT_WAL_VERSION);
		/* Further fail and output is caller-specific */
#endif
	}
	else if (wal_version < CURRENT_WAL_VERSION)
	{
#ifdef IS_DEV
		/* Always fail tests on difference */
		elog(FATAL, "WAL container version %u is older than current %u. Intentionally fail tests", wal_version, CURRENT_WAL_VERSION);
#else
		elog(LOG, "WAL container version %u is older than current %u. Applying with conversion.", wal_version, CURRENT_WAL_VERSION);
#endif
	}

	return wal_version;
}

void
add_modify_wal_record(uint8 rec_type, BTreeDescr *desc,
					  OTuple tuple, OffsetNumber length, char relreplident)
{
	OTuple		nulltup;

	O_TUPLE_SET_NULL(nulltup);
	add_modify_wal_record_extended(rec_type, desc, tuple, length, nulltup, 0, relreplident);
}

/*
 * Extended version of add_modify_wal_record for WAL records that can accommodate two tuples. Now the only user of this is
 * REINSERT action. It will also be needed for WAL logging with REPLICA IDENTITY FULL.
 */
static void
add_modify_wal_record_extended(uint8 rec_type, BTreeDescr *desc,
							   OTuple tuple, OffsetNumber length, OTuple tuple2, OffsetNumber length2, char relreplident)
{
	int			required_length;
	ORelOids	oids = desc->oids;
	OIndexType	type = desc->type;

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

	if (length2 == 0)
	{
		Assert(O_TUPLE_IS_NULL(tuple2));
		required_length = sizeof(WALRecModify1) + length;
	}
	else
	{
		Assert(!O_TUPLE_IS_NULL(tuple2));
		required_length = sizeof(WALRecModify2) + length + length2;
	}

	if (!ORelOidsIsEqual(local_oids, oids) || type != local_type)
		required_length += sizeof(WALRecRelation);

	flush_local_wal_if_needed(required_length);
	Assert(local_wal_buffer_offset + required_length + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_wal_container_header_if_needed();
	add_xid_wal_record_if_needed();

	if (!ORelOidsIsEqual(local_oids, oids) || type != local_type)
		add_rel_wal_record(oids, type, relreplident);

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

	add_wal_container_header_if_needed();

	if (OXidIsValid(get_current_oxid_if_any()))
		add_xid_wal_record_if_needed();

	if (!ORelOidsIsEqual(local_oids, oids) || type != local_type)
		add_rel_wal_record(oids, type, REPLICA_IDENTITY_DEFAULT);

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

	if (record_type != WAL_REC_REINSERT)
	{
		/* One-tuple modify record */
		WALRecModify1 *wal_rec;

		Assert(local_wal_buffer_offset + sizeof(*wal_rec) + length1 + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);
		Assert(O_TUPLE_IS_NULL(record2));
		Assert(length2 == 0);

		wal_rec = (WALRecModify1 *) (&local_wal_buffer[local_wal_buffer_offset]);
		wal_rec->recType = record_type;
		wal_rec->tupleFormatFlags = record1.formatFlags;
		memcpy(wal_rec->length, &length1, sizeof(OffsetNumber));
		local_wal_buffer_offset += sizeof(*wal_rec);

		memcpy(&local_wal_buffer[local_wal_buffer_offset], record1.data, length1);
		local_wal_buffer_offset += length1;
	}
	else
	{
		/* Two-tuple modify record */
		WALRecModify2 *wal_rec;

		Assert(length2);
		Assert(!O_TUPLE_IS_NULL(record2));
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
	local_wal_has_material_changes = true;
}

XLogRecPtr
wal_commit(OXid oxid, TransactionId logicalXid)
{
	XLogRecPtr	walPos;

	Assert(!is_recovery_process());

	if (!local_wal_has_material_changes)
	{
		local_wal_buffer_offset = 0;
		local_type = oIndexInvalid;
		local_oids.datoid = InvalidOid;
		local_oids.reloid = InvalidOid;
		local_oids.relnode = InvalidOid;
		return InvalidXLogRecPtr;
	}

	flush_local_wal_if_needed(sizeof(WALRecFinish));
	Assert(local_wal_buffer_offset + sizeof(WALRecFinish) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_wal_container_header_if_needed();

	if (!local_wal_contains_xid)
		add_xid_wal_record(oxid, logicalXid);

	add_finish_wal_record(WAL_REC_COMMIT, pg_atomic_read_u64(&xid_meta->runXmin));
	walPos = flush_local_wal(true);
	local_wal_has_material_changes = false;

	return walPos;
}

XLogRecPtr
wal_joint_commit(OXid oxid, TransactionId logicalXid, TransactionId xid)
{
	XLogRecPtr	walPos;

	Assert(!is_recovery_process());

	flush_local_wal_if_needed(sizeof(WALRecJointCommit));
	Assert(local_wal_buffer_offset + sizeof(WALRecJointCommit) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_wal_container_header_if_needed();

	if (!local_wal_contains_xid)
		add_xid_wal_record(oxid, logicalXid);

	add_joint_commit_wal_record(xid, pg_atomic_read_u64(&xid_meta->runXmin));
	walPos = flush_local_wal(true);
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
wal_rollback(OXid oxid, TransactionId logicalXid)
{
	XLogRecPtr	wait_pos;

	if (!local_wal_has_material_changes)
	{
		local_wal_buffer_offset = 0;
		local_type = oIndexInvalid;
		local_oids.datoid = InvalidOid;
		local_oids.reloid = InvalidOid;
		local_oids.relnode = InvalidOid;
		return;
	}

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(WALRecFinish));
	Assert(local_wal_buffer_offset + sizeof(WALRecFinish) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_wal_container_header_if_needed();
	if (!local_wal_contains_xid)
		add_xid_wal_record(oxid, logicalXid);

	add_finish_wal_record(WAL_REC_ROLLBACK, pg_atomic_read_u64(&xid_meta->runXmin));
	wait_pos = flush_local_wal(false);
	local_wal_has_material_changes = false;

	if (synchronous_commit > SYNCHRONOUS_COMMIT_OFF)
		XLogFlush(wait_pos);
}

static void
add_finish_wal_record(uint8 rec_type, OXid xmin)
{
	WALRecFinish *rec;
	CommitSeqNo csn;

	Assert(!is_recovery_process());
	Assert(rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK);

	add_wal_container_header_if_needed();
	add_xid_wal_record_if_needed();
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecFinish *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = rec_type;
	memcpy(rec->xmin, &xmin, sizeof(xmin));
	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));

	local_wal_buffer_offset += sizeof(*rec);
}

static void
add_joint_commit_wal_record(TransactionId xid, OXid xmin)
{
	WALRecJointCommit *rec;
	CommitSeqNo csn;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));

	add_wal_container_header_if_needed();
	add_xid_wal_record_if_needed();
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecJointCommit *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = WAL_REC_JOINT_COMMIT;
	memcpy(rec->xid, &xid, sizeof(xid));
	memcpy(rec->xmin, &xmin, sizeof(xmin));
	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));
	local_wal_buffer_offset += sizeof(*rec);
}

static void
add_wal_container_header_if_needed(void)
{
	if (local_wal_buffer_offset == 0)
	{
		uint16	   *wal_version_header;

		wal_version_header = (uint16 *) (&local_wal_buffer[local_wal_buffer_offset]);
		Assert(CURRENT_WAL_VERSION >= FIRST_WAL_VERSION);
		*wal_version_header = CURRENT_WAL_VERSION;
		local_wal_buffer_offset += sizeof(uint16);

		local_wal_contains_xid = false;
	}
}

/*
 * Returns size of a new record.
 */
static void
add_xid_wal_record(OXid oxid, TransactionId logicalXid)
{
	WALRecXid  *rec;

	Assert(!local_wal_contains_xid);
	local_wal_contains_xid = true;
	Assert(!is_recovery_process());
	Assert(OXidIsValid(oxid));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecXid *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = WAL_REC_XID;
	memcpy(rec->oxid, &oxid, sizeof(OXid));
	memcpy(rec->logicalXid, &logicalXid, sizeof(TransactionId));

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
add_rel_wal_record(ORelOids oids, OIndexType type, char relreplident)
{
	WALRecRelation *rec = (WALRecRelation *) (&local_wal_buffer[local_wal_buffer_offset]);

	Assert(!is_recovery_process());
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec->recType = WAL_REC_RELATION;
	rec->treeType = type;
	memcpy(rec->datoid, &oids.datoid, sizeof(Oid));
	memcpy(rec->reloid, &oids.reloid, sizeof(Oid));
	memcpy(rec->relnode, &oids.relnode, sizeof(Oid));
	memcpy(rec->relreplident, &relreplident, sizeof(char));

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

	add_wal_container_header_if_needed();
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

	add_wal_container_header_if_needed();
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
add_savepoint_wal_record(SubTransactionId parentSubid,
						 TransactionId prentLogicalXid)
{
	WALRecSavepoint *rec;
	TransactionId logicalXid = get_current_logical_xid();

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_wal_container_header_if_needed();
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

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_wal_container_header_if_needed();
	add_xid_wal_record_if_needed();

	rec = (WALRecRollbackToSavepoint *) (&local_wal_buffer[local_wal_buffer_offset]);

	rec->recType = WAL_REC_ROLLBACK_TO_SAVEPOINT;
	memcpy(rec->parentSubid, &parentSubid, sizeof(SubTransactionId));

	local_wal_buffer_offset += sizeof(*rec);
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
flush_local_wal(bool commit)
{
	XLogRecPtr	location;
	int			length = local_wal_buffer_offset;

	Assert(!is_recovery_process());
	Assert(length > 0);

	if (commit)
		pg_atomic_write_u64(&GET_CUR_PROCDATA()->commitInProgressXlogLocation, OWalTmpCommitPos);
	location = log_logical_wal_container(local_wal_buffer, length);
	if (commit)
		pg_atomic_write_u64(&GET_CUR_PROCDATA()->commitInProgressXlogLocation, location);

	local_wal_buffer_offset = 0;
	local_type = oIndexInvalid;
	local_oids.datoid = InvalidOid;
	local_oids.reloid = InvalidOid;
	local_oids.relnode = InvalidOid;
	local_wal_has_material_changes = true;

	return location;
}

static void
flush_local_wal_if_needed(int required_length)
{
	Assert(!is_recovery_process());
	if (local_wal_buffer_offset + required_length + XID_RESERVED_LENGTH > LOCAL_WAL_BUFFER_SIZE)
	{
		log_logical_wal_container(local_wal_buffer, local_wal_buffer_offset);

		local_wal_buffer_offset = 0;
		local_type = oIndexInvalid;
		local_oids.datoid = InvalidOid;
		local_oids.reloid = InvalidOid;
		local_oids.relnode = InvalidOid;
		local_wal_has_material_changes = true;
	}
}

XLogRecPtr
log_logical_wal_container(Pointer ptr, int length)
{
	XLogBeginInsert();
	XLogRegisterData(ptr, length);
	return XLogInsert(ORIOLEDB_RMGR_ID, ORIOLEDB_XLOG_CONTAINER);
}

/*
 * Makes WAL insert record.
 */
void
o_wal_insert(BTreeDescr *desc, OTuple tuple)
{
	OTuple		wal_record;
	bool		call_pfree;
	int			size;

	wal_record = recovery_rec_insert(desc, tuple, &call_pfree, &size);
	add_modify_wal_record(WAL_REC_INSERT, desc, wal_record, size, REPLICA_IDENTITY_DEFAULT);
	if (call_pfree)
		pfree(wal_record.data);
}

/*
 * Makes WAL update record.
 */
void
o_wal_update(BTreeDescr *desc, OTuple tuple, OTuple oldtuple, char relreplident)
{
	OTuple		wal_record1;
	OTuple		wal_record2;
	bool		call_pfree;
	bool		call_pfree2;
	int			size1;
	int			size2;

	elog(DEBUG3, "o_wal_update");

	wal_record1 = recovery_rec_update(desc, tuple, &call_pfree1, &size1, relreplident);

	/*
	 * For REPLICA_IDENTITY_FULL include new and old tuples into
	 * WAL_REC_UPDATE
	 */
	if (relreplident != REPLICA_IDENTITY_FULL)
	{
		add_modify_wal_record(WAL_REC_UPDATE, desc, wal_record1, size1, relreplident);
	}
	else
	{
		wal_record2 = recovery_rec_update(desc, oldtuple, &call_pfree2, &size2, relreplident);
		add_modify_wal_record_extended(WAL_REC_UPDATE, desc, wal_record1, size1, wal_record2, size2, relreplident);
	}

	if (call_pfree1)
		pfree(wal_record1.data);
	if (call_pfree2)
		pfree(wal_record2.data);
}

/*
 * Makes WAL delete record.
 */
void
o_wal_delete(BTreeDescr *desc, OTuple tuple, char relreplident)
{
	OTuple		wal_record;
	bool		call_pfree;
	int			size;

	wal_record = recovery_rec_delete(desc, tuple, &call_pfree, &size, relreplident);
	add_modify_wal_record(WAL_REC_DELETE, desc, wal_record, size, relreplident);

	if (call_pfree)
		pfree(wal_record.data);
}

/*
 * Makes WAL delete+insert record.
 */
void
o_wal_reinsert(BTreeDescr *desc, OTuple oldtuple, OTuple newtuple, char relreplident)
{
	OTuple		oldrecord;
	OTuple		newrecord;
	bool		new_call_pfree;
	bool		old_call_pfree;
	int			newsize;
	int			oldsize;

	oldrecord = recovery_rec_delete(desc, oldtuple, &old_call_pfree, &oldsize, relreplident);
	newrecord = recovery_rec_insert(desc, newtuple, &new_call_pfree, &newsize);
	add_modify_wal_record_extended(WAL_REC_REINSERT, desc, newrecord, newsize, oldrecord, oldsize, relreplident);
	if (old_call_pfree)
	{
		pfree(oldrecord.data);
	}
	if (new_call_pfree)
	{
		pfree(newrecord.data);
	}
}

/* Could be used only for system trees that are not replicated logically */
void
o_wal_delete_key(BTreeDescr *desc, OTuple key)
{
	OTuple		wal_record;
	bool		call_pfree;
	int			size;

	Assert(IS_SYS_TREE_OIDS(desc->oids));
	wal_record = recovery_rec_delete_key(desc, key, &call_pfree, &size);
	add_modify_wal_record(WAL_REC_DELETE, desc, wal_record, size, REPLICA_IDENTITY_DEFAULT);

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

	add_wal_container_header_if_needed();
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
