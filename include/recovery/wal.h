/*-------------------------------------------------------------------------
 *
 * wal.h
 * 		WAL declarations for orioledb.
 *
 * Copyright (c) 2021-2024, Oriole DB Inc.
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
	uint8		datoid[sizeof(Oid)];
	uint8		reloid[sizeof(Oid)];
	uint8		old_relnode[sizeof(Oid)];
	uint8		new_relnode[sizeof(Oid)];
} WALRecOTablesUnlockMeta;

typedef struct
{
	uint8		recType;
	uint8		tupleFormatFlags;
	uint8		length[sizeof(OffsetNumber)];
} WALRecModify;

typedef struct
{
	uint8		recType;
	uint8		parentSubid[sizeof(SubTransactionId)];
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
} WALRecJointCommit;

typedef struct
{
	uint8		recType;
	uint8		xmin[sizeof(OXid)];
} WALRecFinish;

typedef struct
{
	uint8		recType;
	uint8		datoid[sizeof(Oid)];
	uint8		reloid[sizeof(Oid)];
	uint8		relnode[sizeof(Oid)];
} WALRecTruncate;

#define LOCAL_WAL_BUFFER_SIZE	(8192)
#define ORIOLEDB_WAL_PREFIX	"o_wal"
#define ORIOLEDB_WAL_PREFIX_SIZE (5)

extern void add_modify_wal_record(uint8 rec_type, BTreeDescr *desc,
								  OTuple tuple, OffsetNumber length);
extern void add_o_tables_meta_lock_wal_record(void);
extern void add_o_tables_meta_unlock_wal_record(ORelOids oids, Oid oldRelnode);
extern void add_savepoint_wal_record(SubTransactionId parentSubid);
extern void add_rollback_to_savepoint_wal_record(SubTransactionId parentSubid);
extern bool local_wal_is_empty(void);
extern XLogRecPtr flush_local_wal(bool commit);
extern void wal_commit(OXid oxid, TransactionId logicalXid);
extern void wal_joint_commit(OXid oxid, TransactionId logicalXid, TransactionId xid);
extern void wal_after_commit(void);
extern void wal_rollback(OXid oxid, TransactionId logicalXid);
extern XLogRecPtr log_logical_wal_container(Pointer ptr, int length);
extern void o_wal_insert(BTreeDescr *desc, OTuple tuple);
extern void o_wal_update(BTreeDescr *desc, OTuple tuple);
extern void o_wal_delete(BTreeDescr *desc, OTuple tuple);
extern void o_wal_delete_key(BTreeDescr *desc, OTuple key);
extern void add_truncate_wal_record(ORelOids oids);
extern bool get_local_wal_has_material_changes(void);
extern void set_local_wal_has_material_changes(bool value);

#endif							/* __WAL_H__ */
