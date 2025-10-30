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

/* WAL record types */
#define WAL_REC_NONE		(0)
#define WAL_REC_XID			(1)
#define WAL_REC_COMMIT		(2)
#define WAL_REC_ROLLBACK	(3)
#define WAL_REC_RELATION	(4)
#define WAL_REC_INSERT		(5)
#define WAL_REC_UPDATE		(6)
#define WAL_REC_DELETE		(7)
#define WAL_REC_O_TABLES_META_LOCK		(8)
#define WAL_REC_O_TABLES_META_UNLOCK	(9)
#define WAL_REC_SAVEPOINT				(10)
#define WAL_REC_ROLLBACK_TO_SAVEPOINT	(11)
#define WAL_REC_JOINT_COMMIT			(12)
#define WAL_REC_TRUNCATE				(13)
#define WAL_REC_BRIDGE_ERASE			(14)
#define WAL_REC_REINSERT				(15)	/* UPDATE with changed pkey
												 * represented as DELETE +
												 * INSERT in OrioleDB but
												 * externally exported as an
												 * UPDATE in logical decoding */
#define WAL_REC_SWITCH_LOGICAL_XID		(16)

/*
 * Value has been fixed at the moment of introducing WAL versioning.
 * Now, when we have WAL version in the beginning of each container
 * we should never change this value.
 */
#define FIRST_WAL_VERSION (16)

/* Bump it when WAL format changes compared to previous release. */
#define CURRENT_WAL_VERSION (16)

/* Constants for commitInProgressXlogLocation */
#define OWalTmpCommitPos			(0)
#define OWalInvalidCommitPos		UINT64_MAX


/*
 * Data structures for transactions in-progress recording.
 */
typedef struct
{
	uint8		recType;
} WALRec;

typedef struct
{
	uint8		recType;
	uint8		oxid[sizeof(OXid)];
	uint8		logicalXid[sizeof(TransactionId)];
	uint8		heapXid[sizeof(TransactionId)];
} WALRecXid;

typedef struct
{
	uint8		recType;
	uint8		topXid[sizeof(TransactionId)];
	uint8		subXid[sizeof(TransactionId)];
} WALRecSwitchLogicalXid;

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

extern const char *wal_record_type_to_string(int wal_record);

/* API for parsing WAL-records */

/* Parser for WAL_REC_XID */
extern Pointer wal_parse_rec_xid(Pointer ptr, OXid *oxid, TransactionId *logicalXid, TransactionId *heapXid);

/* Parser for WAL_REC_COMMIT and WAL_REC_ROLLBACK */
extern Pointer wal_parse_rec_finish(Pointer ptr, OXid *xmin, CommitSeqNo *csn);

/* Parser for WAL_REC_JOINT_COMMIT */
extern Pointer wal_parse_rec_joint_commit(Pointer ptr, TransactionId *xid, OXid *xmin, CommitSeqNo *csn);

/* Parser for WAL_REC_RELATION */
extern Pointer wal_parse_rec_relation(Pointer ptr, uint8 *treeType, Oid *datoid, Oid *reloid, Oid *relnode);

#define WAL_PARSE_REC_RELATION(ptr, treeType, oids) \
	wal_parse_rec_relation(ptr, &treeType, &oids.datoid, &oids.reloid, &oids.relnode)

/* Parser for WAL_REC_O_TABLES_META_UNLOCK */
extern Pointer wal_parse_rec_o_tables_meta_unlock(Pointer ptr, Oid *datoid, Oid *reloid, Oid *old_relnode, Oid *new_relnode);

#define WAL_PARSE_REC_O_TABLES_META_UNLOCK(ptr, oids, old_relnode) \
	wal_parse_rec_o_tables_meta_unlock(ptr, &oids.datoid, &oids.reloid, &old_relnode, &oids.relnode)

/* Parser for WAL_REC_SAVEPOINT */
extern Pointer wal_parse_rec_savepoint(Pointer ptr, SubTransactionId *parentSubid, TransactionId *logicalXid, TransactionId *parentLogicalXid);

/* Parser for WAL_REC_ROLLBACK_TO_SAVEPOINT */
extern Pointer wal_parse_rec_rollback_to_savepoint(Pointer ptr, SubTransactionId *parentSubid);

/* Parser for WAL_REC_TRUNCATE */
extern Pointer wal_parse_rec_truncate(Pointer ptr, Oid *datoid, Oid *reloid, Oid *relnode);

#define WAL_PARSE_REC_TRUNCATE(ptr, oids) \
	wal_parse_rec_truncate(ptr, &oids.datoid, &oids.reloid, &oids.relnode)

/* Parser for WAL_REC_BRIDGE_ERASE */
extern Pointer wal_parse_rec_bridge_erase(Pointer ptr, ItemPointerData *iptr);

/* Parser for WAL_REC_SWITCH_LOGICAL_XID */
extern Pointer wal_parse_rec_switch_logical_xid(Pointer ptr, TransactionId *topXid, TransactionId *subXid);

/* Parser for WAL_REC_INSERT */
/* Parser for WAL_REC_UPDATE */
/* Parser for WAL_REC_DELETE */
/* Parser for WAL_REC_REINSERT */

extern void add_modify_wal_record(uint8 rec_type, BTreeDescr *desc,
								  OTuple tuple, OffsetNumber length);
extern void add_bridge_erase_wal_record(BTreeDescr *desc, ItemPointer iptr);
extern void add_o_tables_meta_lock_wal_record(void);
extern void add_o_tables_meta_unlock_wal_record(ORelOids oids, Oid oldRelnode);
extern void add_switch_logical_xid_wal_record(TransactionId logicalXid_top, TransactionId logicalXid_sub);
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
extern void o_wal_update(BTreeDescr *desc, OTuple tuple);
extern void o_wal_delete(BTreeDescr *desc, OTuple tuple);
extern void o_wal_delete_key(BTreeDescr *desc, OTuple key);
extern void o_wal_reinsert(BTreeDescr *desc, OTuple oldtuple, OTuple newtuple);
extern void add_truncate_wal_record(ORelOids oids);
extern bool get_local_wal_has_material_changes(void);
extern void set_local_wal_has_material_changes(bool value);
extern uint16 check_wal_container_version(Pointer *ptr);

#endif							/* __WAL_H__ */
