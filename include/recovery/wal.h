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
#define WAL_REC_REPLAY_FEEDBACK	(16)

/*
 * Record type for a case when both heap and Oriole apply changes within a single transaction,
 * so one logical xid is assigned by heap, and the other is assigned by Oriole.
 *
 * This record defines a connection between Oriole's sub-transaction xid and a xid of the top heap transaction
 * which is needed for logical decoder.
 *
 * Otherwise, without this connection, main transaction suddenly becomes splitted into two independent parts.
 * From logical decoder's point of view this looks like two independent transactions but in fact internally related to each other.
 * This situation outcomes in troubles for logical decoder with visibility of heap modifications
 * in Oriole's sub-part due to incorrect state of the MVCC-historical snapshot.
 */
#define WAL_REC_SWITCH_LOGICAL_XID	(17)
#define WAL_REC_RELREPLIDENT		(18)

/*
 * Value has been fixed at the moment of introducing WAL versioning.
 * Now, when we have WAL version in the beginning of each container
 * we should never change this value.
 */
#define FIRST_ORIOLEDB_WAL_VERSION (16)

/*
 * Bump it when WAL format changes compared to previous release.
 * ORIOLEDB_WAL_VERSION makes sense and should be converted even between different ORIOLEDB_BINARY_VERSION's.
 * This is unlike ORIOLEDB_DATA_VERSION, ORIOLEDB_PAGE_VERSION and ORIOLEDB_COMPRESS_VERSION,
 * that make sense only inside one ORIOLEDB_BINARY_VERSION.
 */
#define ORIOLEDB_WAL_VERSION (17)

#define ORIOLEDB_XACT_INFO_WAL_VERSION (17)

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
	/* Since ORIOLEDB_WAL_VERSION = 17 */
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
	uint8		relreplident;
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
	uint8		xmin[sizeof(OXid)]; /* Since ORIOLEDB_WAL_VERSION = 17 */
	uint8		csn[sizeof(CommitSeqNo)];	/* Since ORIOLEDB_WAL_VERSION = 17 */
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

/*
 * Following WAL records don't have recType field and they are controlled by
 * WAL_CONTAINER_HAS_* flags.
 */

#define WAL_CONTAINER_HAS_XACT_INFO	(1U << 0)

typedef struct
{
	uint8		xactTime[sizeof(TimestampTz)];
	uint8		xid[sizeof(TransactionId)];
} WALRecXactInfo;

#define LOCAL_WAL_BUFFER_SIZE	(8192)
#define ORIOLEDB_WAL_PREFIX	"o_wal"
#define ORIOLEDB_WAL_PREFIX_SIZE (5)

extern const char *wal_record_type_to_string(int wal_record);

/* API for parsing WAL-records */

/* Parser for WAL_REC_XID */
extern Pointer wal_parse_rec_xid(Pointer ptr, OXid *oxid, TransactionId *logicalXid, TransactionId *heapXid, uint16 wal_version);

/* Parser for WAL_REC_COMMIT and WAL_REC_ROLLBACK */
extern Pointer wal_parse_rec_finish(Pointer ptr, OXid *xmin, CommitSeqNo *csn);

/* Parser for WAL_REC_JOINT_COMMIT */
extern Pointer wal_parse_rec_joint_commit(Pointer ptr, TransactionId *xid, OXid *xmin, CommitSeqNo *csn);

/* Parser for WAL_REC_RELATION */
extern Pointer wal_parse_rec_relation(Pointer ptr, uint8 *treeType, ORelOids *oids);

/* Parser for WAL_REC_RELREPLIDENT */
extern Pointer wal_parse_rec_relreplident(Pointer ptr, char *relreplident, Oid *relreplident_ix_oid);

/* Parser for WAL_REC_O_TABLES_META_UNLOCK */
extern Pointer wal_parse_rec_o_tables_meta_unlock(Pointer ptr, ORelOids *oids, Oid *old_relnode);

/* Parser for WAL_REC_SAVEPOINT */
extern Pointer wal_parse_rec_savepoint(Pointer ptr, SubTransactionId *parentSubid, TransactionId *logicalXid, TransactionId *parentLogicalXid);

/* Parser for WAL_REC_ROLLBACK_TO_SAVEPOINT */
extern Pointer wal_parse_rec_rollback_to_savepoint(Pointer ptr, SubTransactionId *parentSubid, OXid *xmin, CommitSeqNo *csn, uint16 wal_version);

/* Parser for WAL_REC_TRUNCATE */
extern Pointer wal_parse_rec_truncate(Pointer ptr, ORelOids *oids);

/* Parser for WAL_REC_BRIDGE_ERASE */
extern Pointer wal_parse_rec_bridge_erase(Pointer ptr, ItemPointerData *iptr);

/* Parser for WAL_REC_SWITCH_LOGICAL_XID */
extern Pointer wal_parse_rec_switch_logical_xid(Pointer ptr, TransactionId *topXid, TransactionId *subXid);

/* Parser for WAL_REC_INSERT, WAL_REC_UPDATE, WAL_REC_DELETE or WAL_REC_REINSERT */
extern Pointer wal_parse_rec_modify(Pointer ptr, OFixedTuple *tuple1, OFixedTuple *tuple2, OffsetNumber *length1, bool read_two_tuples);

extern void add_modify_wal_record(uint8 rec_type, BTreeDescr *desc,
								  OTuple tuple, OffsetNumber length, char relreplident);
extern void add_bridge_erase_wal_record(BTreeDescr *desc, ItemPointer iptr);
extern void add_o_tables_meta_lock_wal_record(void);
extern void add_o_tables_meta_unlock_wal_record(ORelOids oids, Oid oldRelnode);
extern void add_switch_logical_xid_wal_record(TransactionId logicalXid_top, TransactionId logicalXid_sub);
extern void add_savepoint_wal_record(SubTransactionId parentSubid,
									 TransactionId prentLogicalXid);
extern void add_rollback_to_savepoint_wal_record(SubTransactionId parentSubid);
extern bool local_wal_is_empty(void);
extern XLogRecPtr flush_local_wal(bool isCommit, bool withXactTime);
extern XLogRecPtr wal_commit(OXid oxid, TransactionId logicalXid,
							 bool isAutonomous);
extern XLogRecPtr wal_joint_commit(OXid oxid, TransactionId logicalXid,
								   TransactionId xid);
extern void wal_after_commit(void);
extern void wal_rollback(OXid oxid, TransactionId logicalXid,
						 bool isAutonomous);
extern XLogRecPtr log_logical_wal_container(Pointer ptr, int length,
											bool withXactTime);
extern void o_wal_insert(BTreeDescr *desc, OTuple tuple, char relreplident);
extern void o_wal_update(BTreeDescr *desc, OTuple tuple, OTuple oldtuple, char relreplident);
extern void o_wal_delete(BTreeDescr *desc, OTuple tuple, char relreplident);
extern void o_wal_delete_key(BTreeDescr *desc, OTuple key, bool is_bridge_index);
extern void o_wal_reinsert(BTreeDescr *desc, OTuple oldtuple, OTuple newtuple, char relreplident);
extern void add_truncate_wal_record(ORelOids oids);
extern bool get_local_wal_has_material_changes(void);
extern void set_local_wal_has_material_changes(bool value);

extern Pointer wal_container_read_header(Pointer ptr, uint16 *version,
										 uint8 *flags);

#endif							/* __WAL_H__ */
