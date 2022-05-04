/*-------------------------------------------------------------------------
 *
 * wal.c
 *		Routines dealing with WAL for orioledb.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/wal.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "catalog/sys_trees.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "transam/oxid.h"

#include "replication/message.h"
#include "storage/proc.h"

/*
 * We can use this value because if commit begins at 0
 * than WAL does not consist any modify record.
 */
#define INVALID_COMMIT_POS		(0)

static char local_wal_buffer[LOCAL_WAL_BUFFER_SIZE];
static int	local_wal_buffer_offset;
static ORelOids local_oids;
static OIndexType local_type;

static void add_wal_record(uint8 rec_type);
static void add_joint_commit_wal_record(TransactionId xid);
static void add_xid_wal_record(OXid oxid);
static void add_rel_wal_record(ORelOids oids, OIndexType type);
static void flush_local_wal_if_needed(int required_length);
static inline void add_local_modify(uint8 record_type, OTuple record, OffsetNumber length);

void
add_modify_wal_record(uint8 rec_type, BTreeDescr *desc,
					  OTuple tuple, OffsetNumber length)
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
	Assert(rec_type == WAL_REC_INSERT || rec_type == WAL_REC_UPDATE || rec_type == WAL_REC_DELETE);

	required_length = sizeof(WALRecModify) + length;

	if (!ORelOidsIsEqual(local_oids, oids) || type != local_type)
		required_length += sizeof(WALRecRelation);

	flush_local_wal_if_needed(required_length);
	Assert(local_wal_buffer_offset + required_length <= LOCAL_WAL_BUFFER_SIZE);

	if (local_wal_buffer_offset == 0)
	{
		OXid		oxid = get_current_oxid_if_any();

		Assert(oxid != InvalidOXid);
		add_xid_wal_record(oxid);
	}

	if (!ORelOidsIsEqual(local_oids, oids) || type != local_type)
		add_rel_wal_record(oids, type);

	add_local_modify(rec_type, tuple, length);
}

/*
 * Adds the record to the local_wal_buffer.
 */
static inline void
add_local_modify(uint8 record_type, OTuple record, OffsetNumber length)
{
	WALRecModify *wal_rec;

	Assert(local_wal_buffer_offset + sizeof(*wal_rec) + length <= LOCAL_WAL_BUFFER_SIZE);

	wal_rec = (WALRecModify *) (&local_wal_buffer[local_wal_buffer_offset]);
	wal_rec->recType = record_type;
	wal_rec->tupleFormatFlags = record.formatFlags;
	memcpy(wal_rec->length, &length, sizeof(OffsetNumber));
	local_wal_buffer_offset += sizeof(*wal_rec);
	memcpy(&local_wal_buffer[local_wal_buffer_offset], record.data, length);
	local_wal_buffer_offset += length;
}

void
wal_commit(OXid oxid)
{
	XLogRecPtr	wait_pos;

	Assert(!is_recovery_process());

	if (local_wal_buffer_offset == 0)
		return;

	flush_local_wal_if_needed(sizeof(WALRec));
	Assert(local_wal_buffer_offset + sizeof(WALRec) <= LOCAL_WAL_BUFFER_SIZE);

	if (local_wal_buffer_offset == 0)
		add_xid_wal_record(oxid);
	add_wal_record(WAL_REC_COMMIT);
	wait_pos = flush_local_wal(true);

	if (synchronous_commit > SYNCHRONOUS_COMMIT_OFF ||
		oxid_needs_wal_flush)
		XLogFlush(wait_pos);
}

void
wal_joint_commit(OXid oxid, TransactionId xid)
{
	Assert(!is_recovery_process());

	flush_local_wal_if_needed(sizeof(WALRec));
	Assert(local_wal_buffer_offset + sizeof(WALRec) <= LOCAL_WAL_BUFFER_SIZE);

	if (local_wal_buffer_offset == 0)
		add_xid_wal_record(oxid);
	add_joint_commit_wal_record(xid);
	(void) flush_local_wal(true);

	/*
	 * Don't need to flush local WAL, because we only commit if builtin
	 * transaction commits.
	 */
}

void
wal_after_commit()
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	pg_atomic_write_u64(&curProcData->commitInProgressXlogLocation, INVALID_COMMIT_POS);
}

void
wal_rollback(OXid oxid)
{
	XLogRecPtr	wait_pos;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(WALRec));
	Assert(local_wal_buffer_offset + sizeof(WALRec) <= LOCAL_WAL_BUFFER_SIZE);
	if (local_wal_buffer_offset == 0)
		add_xid_wal_record(oxid);
	add_wal_record(WAL_REC_ROLLBACK);
	wait_pos = flush_local_wal(false);
	if (synchronous_commit > SYNCHRONOUS_COMMIT_OFF)
		XLogFlush(wait_pos);
}

static void
add_wal_record(uint8 rec_type)
{
	WALRec	   *rec;

	Assert(!is_recovery_process());
	Assert(rec_type == WAL_REC_COMMIT || WAL_REC_ROLLBACK);

	if (local_wal_buffer_offset == 0)
	{
		OXid		oxid = get_current_oxid_if_any();

		Assert(oxid != InvalidOXid);
		add_xid_wal_record(oxid);
	}
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRec *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = rec_type;
	local_wal_buffer_offset += sizeof(*rec);
}

static void
add_joint_commit_wal_record(TransactionId xid)
{
	WALRecJointCommit *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));

	if (local_wal_buffer_offset == 0)
	{
		OXid		oxid = get_current_oxid_if_any();

		Assert(oxid != InvalidOXid);
		add_xid_wal_record(oxid);
	}
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecJointCommit *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = WAL_REC_JOINT_COMMIT;
	memcpy(rec->xid, &xid, sizeof(xid));
	local_wal_buffer_offset += sizeof(*rec);
}

/*
 * Returns size of a new record.
 */
static void
add_xid_wal_record(OXid oxid)
{
	WALRecXid  *rec;

	Assert(!is_recovery_process());
	Assert(OXidIsValid(oxid));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecXid *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = WAL_REC_XID;
	memcpy(rec->oxid, &oxid, sizeof(OXid));

	local_wal_buffer_offset += sizeof(*rec);
}

static void
add_rel_wal_record(ORelOids oids, OIndexType type)
{
	WALRecRelation *rec = (WALRecRelation *) (&local_wal_buffer[local_wal_buffer_offset]);

	Assert(!is_recovery_process());
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	rec->recType = WAL_REC_RELATION;
	rec->treeType = type;
	memcpy(rec->datoid, &oids.datoid, sizeof(Oid));
	memcpy(rec->reloid, &oids.reloid, sizeof(Oid));
	memcpy(rec->relnode, &oids.relnode, sizeof(Oid));

	local_wal_buffer_offset += sizeof(*rec);

	local_type = type;
	local_oids = oids;
}

void
add_invalidate_wal_record(ORelOids oids, Oid old_relnode)
{
	WALRecInvalidate *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	if (local_wal_buffer_offset == 0)
	{
		OXid		oxid = get_current_oxid_if_any();

		Assert(oxid != InvalidOXid);
		add_xid_wal_record(oxid);
	}

	rec = (WALRecInvalidate *) (&local_wal_buffer[local_wal_buffer_offset]);

	rec->recType = WAL_REC_INVALIDATE;
	memcpy(rec->datoid, &oids.datoid, sizeof(Oid));
	memcpy(rec->reloid, &oids.reloid, sizeof(Oid));
	memcpy(rec->old_relnode, &old_relnode, sizeof(Oid));
	memcpy(rec->new_relnode, &oids.relnode, sizeof(Oid));

	local_wal_buffer_offset += sizeof(*rec);
}

void
add_savepoint_wal_record(SubTransactionId parentSubid)
{
	WALRecSavepoint *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	if (local_wal_buffer_offset == 0)
	{
		OXid		oxid = get_current_oxid_if_any();

		Assert(oxid != InvalidOXid);
		add_xid_wal_record(oxid);
	}

	rec = (WALRecSavepoint *) (&local_wal_buffer[local_wal_buffer_offset]);

	rec->recType = WAL_REC_SAVEPOINT;
	memcpy(rec->parentSubid, &parentSubid, sizeof(SubTransactionId));

	local_wal_buffer_offset += sizeof(*rec);
}

void
add_rollback_to_savepoint_wal_record(SubTransactionId parentSubid)
{
	WALRecRollbackToSavepoint *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	if (local_wal_buffer_offset == 0)
	{
		OXid		oxid = get_current_oxid_if_any();

		Assert(oxid != InvalidOXid);
		add_xid_wal_record(oxid);
	}

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

	location = log_logical_wal_container(local_wal_buffer, length);
	if (commit)
	{
		pg_atomic_write_u64(&GET_CUR_PROCDATA()->commitInProgressXlogLocation, location);
	}

	local_wal_buffer_offset = 0;
	local_type = oIndexInvalid;
	local_oids.datoid = InvalidOid;
	local_oids.reloid = InvalidOid;
	local_oids.relnode = InvalidOid;

	return location;
}

static void
flush_local_wal_if_needed(int required_length)
{
	Assert(!is_recovery_process());
	if (local_wal_buffer_offset + required_length > LOCAL_WAL_BUFFER_SIZE)
	{
		log_logical_wal_container(local_wal_buffer, local_wal_buffer_offset);

		local_wal_buffer_offset = 0;
		local_type = oIndexInvalid;
		local_oids.datoid = InvalidOid;
		local_oids.reloid = InvalidOid;
		local_oids.relnode = InvalidOid;
	}
}

XLogRecPtr
log_logical_wal_container(Pointer ptr, int length)
{
	return LogLogicalMessage(ORIOLEDB_WAL_PREFIX, ptr, length, false);
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
	add_modify_wal_record(WAL_REC_INSERT, desc, wal_record, size);
	if (call_pfree)
		pfree(wal_record.data);
}

/*
 * Makes WAL update record.
 */
void
o_wal_update(BTreeDescr *desc, OTuple tuple)
{
	OTuple		wal_record;
	bool		call_pfree;
	int			size;

	wal_record = recovery_rec_update(desc, tuple, &call_pfree, &size);
	add_modify_wal_record(WAL_REC_UPDATE, desc, wal_record, size);
	if (call_pfree)
		pfree(wal_record.data);
}

/*
 * Makes WAL delete record.
 */
void
o_wal_delete(BTreeDescr *desc, OTuple tuple)
{
	OTuple		wal_record;
	bool		call_pfree;
	int			size;

	wal_record = recovery_rec_delete(desc, tuple, &call_pfree, &size);
	add_modify_wal_record(WAL_REC_DELETE, desc, wal_record, size);
	if (call_pfree)
		pfree(wal_record.data);
}

void
o_wal_delete_key(BTreeDescr *desc, OTuple key)
{
	OTuple		wal_record;
	bool		call_pfree;
	int			size;

	wal_record = recovery_rec_delete_key(desc, key, &call_pfree, &size);
	add_modify_wal_record(WAL_REC_DELETE, desc, wal_record, size);
	if (call_pfree)
		pfree(wal_record.data);
}
