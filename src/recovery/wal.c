/*-------------------------------------------------------------------------
 *
 * wal.c
 *		Routines dealing with WAL for orioledb.
 *
 * Copyright (c) 2021-2024, Oriole DB Inc.
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
static ORelOids local_oids = {InvalidOid, InvalidOid, InvalidOid};
static OIndexType local_type = oIndexInvalid;

static void add_finish_wal_record(uint8 rec_type, OXid xmin);
static void add_joint_commit_wal_record(TransactionId xid, OXid xmin);
static void add_xid_wal_record(OXid oxid, TransactionId logicalXid);
static void add_xid_wal_record_if_needed(void);
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

	add_xid_wal_record_if_needed();

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
	local_wal_has_material_changes = true;
}

void
wal_commit(OXid oxid, TransactionId logicalXid)
{
	XLogRecPtr	wait_pos;

	Assert(!is_recovery_process());

	if (!local_wal_has_material_changes)
	{
		local_wal_buffer_offset = 0;
		local_type = oIndexInvalid;
		local_oids.datoid = InvalidOid;
		local_oids.reloid = InvalidOid;
		local_oids.relnode = InvalidOid;
		return;
	}

	flush_local_wal_if_needed(sizeof(WALRecFinish));
	Assert(local_wal_buffer_offset + sizeof(WALRecFinish) <= LOCAL_WAL_BUFFER_SIZE);

	if (local_wal_buffer_offset == 0)
		add_xid_wal_record(oxid, logicalXid);
	add_finish_wal_record(WAL_REC_COMMIT, pg_atomic_read_u64(&xid_meta->runXmin));
	wait_pos = flush_local_wal(true);
	local_wal_has_material_changes = false;

	if (synchronous_commit > SYNCHRONOUS_COMMIT_OFF ||
		oxid_needs_wal_flush)
		XLogFlush(wait_pos);
}

void
wal_joint_commit(OXid oxid, TransactionId logicalXid, TransactionId xid)
{
	Assert(!is_recovery_process());

	flush_local_wal_if_needed(sizeof(WALRecJointCommit));
	Assert(local_wal_buffer_offset + sizeof(WALRecJointCommit) <= LOCAL_WAL_BUFFER_SIZE);

	if (local_wal_buffer_offset == 0)
		add_xid_wal_record(oxid, logicalXid);
	add_joint_commit_wal_record(xid, pg_atomic_read_u64(&xid_meta->runXmin));
	(void) flush_local_wal(true);
	local_wal_has_material_changes = false;

	/*
	 * Don't need to flush local WAL, because we only commit if builtin
	 * transaction commits.
	 */
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
	Assert(local_wal_buffer_offset + sizeof(WALRecFinish) <= LOCAL_WAL_BUFFER_SIZE);
	if (local_wal_buffer_offset == 0)
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

	Assert(!is_recovery_process());
	Assert(rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK);

	add_xid_wal_record_if_needed();
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecFinish *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = rec_type;
	memcpy(rec->xmin, &xmin, sizeof(xmin));

	local_wal_buffer_offset += sizeof(*rec);
}

static void
add_joint_commit_wal_record(TransactionId xid, OXid xmin)
{
	WALRecJointCommit *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));

	add_xid_wal_record_if_needed();
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecJointCommit *) (&local_wal_buffer[local_wal_buffer_offset]);
	rec->recType = WAL_REC_JOINT_COMMIT;
	memcpy(rec->xid, &xid, sizeof(xid));
	memcpy(rec->xmin, &xmin, sizeof(xmin));
	local_wal_buffer_offset += sizeof(*rec);
}

/*
 * Returns size of a new record.
 */
static void
add_xid_wal_record(OXid oxid, TransactionId logicalXid)
{
	WALRecXid  *rec;

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
	if (local_wal_buffer_offset == 0)
	{
		OXid		oxid = get_current_oxid_if_any();
		TransactionId logicalXid = get_current_logical_xid();

		Assert(oxid != InvalidOXid);
		add_xid_wal_record(oxid, logicalXid);
	}
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
add_o_tables_meta_lock_wal_record(void)
{
	WALRec	   *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

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
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

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
add_savepoint_wal_record(SubTransactionId parentSubid)
{
	WALRecSavepoint *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

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
	if (local_wal_buffer_offset + required_length > LOCAL_WAL_BUFFER_SIZE)
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

void
add_truncate_wal_record(ORelOids oids)
{
	WALRecTruncate *rec;

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal_buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

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
