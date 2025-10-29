/*-------------------------------------------------------------------------
 *
 * wal.h
 * 		WAL declarations for orioledb.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/wal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WAL_H__
#define __WAL_H__

/*
 * Data sturctures for transactions in-progress recording.
 */
#define WAL_REC_NONE		(0)
#define WAL_REC_XID			(1)
#define WAL_REC_COMMIT		(2)
#define WAL_REC_ROLLBACK	(3)
#define WAL_REC_RELATION	(4)
#define WAL_REC_INSERT		(5)
#define WAL_REC_UPDATE		(6)
#define WAL_REC_DELETE		(7)
#define WAL_REC_O_TABLES_META_LOCK (8)
#define WAL_REC_O_TABLES_META_UNLOCK (9)
#define WAL_REC_SAVEPOINT	(10)
#define WAL_REC_ROLLBACK_TO_SAVEPOINT (11)
#define WAL_REC_JOINT_COMMIT (12)
#define WAL_REC_TRUNCATE	(13)
#define WAL_REC_BRIDGE_ERASE (14)
#define WAL_REC_REINSERT (15)	/* UPDATE with changed pkey represented as
								 * DELETE + INSERT in OrioleDB but externally
								 * exported as an UPDATE in logical decoding */
#define WAL_REC_RELREPLIDENT (17)

#define FIRST_WAL_VERSION (16)

/* Bump it when WAL format changes */
#define CURRENT_WAL_VERSION (16)

/* Constants for commitInProgressXlogLocation */
#define OWalTmpCommitPos			(0)
#define OWalInvalidCommitPos		UINT64_MAX

typedef struct
{
	uint8		recType;
} WALRec;

typedef struct
{
	uint8		recType;
	uint8		oxid[sizeof(OXid)];
	uint8		logicalXid[sizeof(TransactionId)];
} WALRecXid;

typedef struct
{
	uint8		recType;
	uint8		treeType;
	uint8		datoid[sizeof(Oid)];
	uint8		reloid[sizeof(Oid)];
	uint8		relnode[sizeof(Oid)];
} WALRecRelation;

typedef struct
{
	uint8		recType;
	char		relreplident;
	uint8		relreplident_ix_oid[sizeof(Oid)];
} WALRecRelReplident;

typedef struct
{
	uint8		recType;
	uint8		datoid[sizeof(Oid)];
	uint8		reloid[sizeof(Oid)];
	uint8		old_relnode[sizeof(Oid)];
	uint8		new_relnode[sizeof(Oid)];
} WALRecOTablesUnlockMeta;

/* Modify record that contains one tuple */
typedef struct
{
	uint8		recType;
	uint8		tupleFormatFlags;
	uint8		length[sizeof(OffsetNumber)];
	/* tuple[length] */
} WALRecModify1;

/* Modify records that contains 2 tuples, old and new. Needed for REINSERT and for REPLICA IDENTITY FULL */
typedef struct
{
	uint8		recType;
	uint8		tupleFormatFlags1;
	uint8		tupleFormatFlags2;
	uint8		length1[sizeof(OffsetNumber)];
	uint8		length2[sizeof(OffsetNumber)];
	/* tuple1[length1] */
	/* tuple2[length2] */
} WALRecModify2;

typedef struct
{
	uint8		recType;
	uint8		parentSubid[sizeof(SubTransactionId)];
	uint8		logicalXid[sizeof(TransactionId)];
	uint8		parentLogicalXid[sizeof(TransactionId)];
} WALRecSavepoint;

typedef struct
{
	uint8		recType;
	uint8		parentSubid[sizeof(SubTransactionId)];
} WALRecRollbackToSavepoint;

typedef struct
{
	uint8		recType;
	uint8		xid[sizeof(TransactionId)];
	uint8		xmin[sizeof(OXid)];
	uint8		csn[sizeof(CommitSeqNo)];
} WALRecJointCommit;

typedef struct
{
	uint8		recType;
	uint8		xmin[sizeof(OXid)];
	uint8		csn[sizeof(CommitSeqNo)];
} WALRecFinish;

typedef struct
{
	uint8		recType;
	uint8		datoid[sizeof(Oid)];
	uint8		reloid[sizeof(Oid)];
	uint8		relnode[sizeof(Oid)];
} WALRecTruncate;

typedef struct
{
	uint8		recType;
	uint8		iptr[sizeof(ItemPointerData)];
} WALRecBridgeErase;

#define LOCAL_WAL_BUFFER_SIZE	(8192)
#define ORIOLEDB_WAL_PREFIX	"o_wal"
#define ORIOLEDB_WAL_PREFIX_SIZE (5)

extern void add_modify_wal_record(uint8 rec_type, BTreeDescr *desc,
								  OTuple tuple, OffsetNumber length, char relreplident);
extern void add_bridge_erase_wal_record(BTreeDescr *desc, ItemPointer iptr);
extern void add_o_tables_meta_lock_wal_record(void);
extern void add_o_tables_meta_unlock_wal_record(ORelOids oids, Oid oldRelnode);
extern void add_savepoint_wal_record(SubTransactionId parentSubid,
									 TransactionId prentLogicalXid);
extern void add_rollback_to_savepoint_wal_record(SubTransactionId parentSubid);
extern bool local_wal_is_empty(void);
extern XLogRecPtr flush_local_wal(bool commit);
extern XLogRecPtr wal_commit(OXid oxid, TransactionId logicalXid);
extern XLogRecPtr wal_joint_commit(OXid oxid, TransactionId logicalXid,
								   TransactionId xid);
extern void wal_after_commit(void);
extern void wal_rollback(OXid oxid, TransactionId logicalXid);
extern XLogRecPtr log_logical_wal_container(Pointer ptr, int length);
extern void o_wal_insert(BTreeDescr *desc, OTuple tuple);
extern void o_wal_update(BTreeDescr *desc, OTuple tuple, OTuple oldtuple, char relreplident);
extern void o_wal_delete(BTreeDescr *desc, OTuple tuple, char relreplident);
extern void o_wal_delete_key(BTreeDescr *desc, OTuple key, bool is_bridge_index);
extern void o_wal_reinsert(BTreeDescr *desc, OTuple oldtuple, OTuple newtuple, char relreplident);
extern void add_truncate_wal_record(ORelOids oids);
extern bool get_local_wal_has_material_changes(void);
extern void set_local_wal_has_material_changes(bool value);
extern uint16 check_wal_container_version(Pointer *ptr);

#endif							/* __WAL_H__ */
