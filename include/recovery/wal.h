/*-------------------------------------------------------------------------
 *
 * wal.h
 * 		WAL declarations for orioledb.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/wal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WAL_H__
#define __WAL_H__

#define WAL_REC_NONE           (0)

#define WAL_CONTAINER_HAS_XACT_INFO		(1U << 0)
#define WAL_CONTAINER_HAS_ORIGIN_INFO	(1U << 1)

/*
 * Current WAL version of OrioleDB.
 * Bump it when WAL format changes compared to previous release.
 * ORIOLEDB_WAL_VERSION makes sense and should be converted even between
 * different ORIOLEDB_BINARY_VERSION's. This is unlike
 * ORIOLEDB_SYS_TREE_VERSION, ORIOLEDB_PAGE_VERSION and
 * ORIOLEDB_COMPRESS_VERSION (see big comment on versioning
 * in include/orioledb.h)
 */
#define ORIOLEDB_WAL_VERSION (17)

/*
 * Value has been fixed at the moment of introducing WAL versioning.
 * WAL versions before FIRST_ORIOLEDB_WAL_VERSION are treated as zero
 * and still supported for on-the fly conversion to the current
 * ORIOLEDB_WAL_VERSION. (Exact value follows the fact that before
 * WAL version was introduced in the beginning of the WAL container,
 * WAL container started from rec_type byte with at most 4 lower bits
 * occupied. So it's a way to distinguish pre-WAL version container
 * from the container with WAL version.)
 *
 * We should never change this value.
 */
#define FIRST_ORIOLEDB_WAL_VERSION (16)

/*
 * Particular WAL version when per-container flags were added to WAL container.
 *
 * We should never change this value.
 */
#define ORIOLEDB_CONTAINER_FLAGS_WAL_VERSION (17)

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
	/* Since ORIOLEDB_WAL_VERSION = 17 */
	uint8		xmin[sizeof(OXid)];
	uint8		csn[sizeof(CommitSeqNo)];
	uint8		cid[sizeof(CommandId)];
	uint8		version[sizeof(uint32)];
	uint8		baseVersion[sizeof(uint32)];
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

typedef struct
{
	uint8		xactTime[sizeof(TimestampTz)];
	uint8		xid[sizeof(TransactionId)];
} WALRecXactInfo;

typedef struct
{
	uint8		origin_id[sizeof(RepOriginId)];
	uint8		origin_lsn[sizeof(XLogRecPtr)];
} WALRecOriginInfo;


#define LOCAL_WAL_BUFFER_SIZE	(8192)
#define ORIOLEDB_WAL_PREFIX	"o_wal"
#define ORIOLEDB_WAL_PREFIX_SIZE (5)

extern const char *wal_record_type_to_string(int wal_record);

extern void add_rel_wal_record(ORelOids oids, OIndexType type, uint32 version, uint32 base_version);

extern void add_modify_wal_record(uint8 rec_type, BTreeDescr *desc,
								  OTuple tuple, OffsetNumber length, char relreplident, uint32 version, uint32 base_version);
extern void add_bridge_erase_wal_record(BTreeDescr *desc, ItemPointer iptr, uint32 version, uint32 base_version);
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
								   TransactionId xid, bool subTransaction);
extern void wal_after_commit(void);
extern void wal_rollback(OXid oxid, TransactionId logicalXid,
						 bool isAutonomous);
extern XLogRecPtr log_logical_wal_container(Pointer ptr, int length,
											bool withXactTime);
extern void o_wal_insert(BTreeDescr *desc, OTuple tuple, char relreplident, uint32 version);
extern void o_wal_update(BTreeDescr *desc, OTuple tuple, OTuple oldtuple, char relreplident, uint32 version);
extern void o_wal_delete(BTreeDescr *desc, OTuple tuple, char relreplident, uint32 version);
extern void o_wal_delete_key(BTreeDescr *desc, OTuple key, bool is_bridge_index, uint32 version);
extern void o_wal_reinsert(BTreeDescr *desc, OTuple oldtuple, OTuple newtuple, char relreplident, uint32 version);
extern void add_truncate_wal_record(ORelOids oids);
extern bool get_local_wal_has_material_changes(void);
extern void set_local_wal_has_material_changes(bool value);

#endif							/* __WAL_H__ */
