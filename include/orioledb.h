/*-------------------------------------------------------------------------
 *
 * orioledb.h
 *		Common declarations for orioledb engine.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/orioledb.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __ORIOLEDB_H__
#define __ORIOLEDB_H__

#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "common/int.h"
#include "nodes/extensible.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/bufpage.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "storage/procarray.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/typcache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#if defined __has_include
#if __has_include ("sanitizer/asan_interface.h")
#include "sanitizer/asan_interface.h"
#endif
#endif

#ifndef ASAN_UNPOISON_MEMORY_REGION
#define ASAN_UNPOISON_MEMORY_REGION(addr, size) \
  ((void)(addr), (void)(size))
#endif

#define ORIOLEDB_VERSION "OrioleDB public beta 13"
/*
 * Clusters with different ORIOLEDB_BINARY_VERSION are completely incompatible.
 * Within the same ORIOLEDB_BINARY_VERSION clusters either fully compatible
 * or could be converted on the fly. See comment for ORIOLEDB_DATA_VERSION,
 * ORIOLEDB_PAGE_VERSION and ORIOLEDB_COMPRESS_VERSION below.
 *
 * ORIOLEDB_WAL_VERSION works even between different ORIOLEDB_BINARY_VERSION's
 * In this case convesrion of WAL records is still allowed and should be implemented
 * ( see check_wal_container_version() )
 */
#define ORIOLEDB_BINARY_VERSION 7
#define ORIOLEDB_DATA_DIR "orioledb_data"
#define ORIOLEDB_UNDO_DIR "orioledb_undo"
#define ORIOLEDB_RMGR_ID (129)
#define ORIOLEDB_XLOG_CONTAINER (0x00)
/*
 * Sub-versions in the same ORIOLEDB_BINARY_VERSION.
 *
 * Same ORIOLEDB_DATA_VERSION, ORIOLEDB_PAGE_VERSION and
 * ORIOLEDB_COMPRESS_VERSION clusters are compatible without conversion.
 * For different ORIOLEDB_DATA_VERSION conversion is done at the
 * reading/deserialization of system tables structures without using
 * any conversion tools.
 * For different ORIOLEDB_PAGE_VERSION and ORIOLEDB_COMPRESS_VERSION
 * conversion is done at first reading of disk page on the fly.
 */
#define ORIOLEDB_DATA_VERSION	2	/* Version of system catalog */
#define ORIOLEDB_PAGE_VERSION	1	/* Version of binary page format */
#define ORIOLEDB_COMPRESS_VERSION 1 /* Version of page compression (only for
									 * compressed pages) */

/*
 * perform_page_split() removes a key data from first right page downlink.
 * But the data can be useful for debug. The macro controls this behavior.
 *
 * See usage in perform_page_split().
 */
#define ORIOLEDB_CUT_FIRST_KEY 1
/* max a BTree depth */
#define ORIOLEDB_MAX_DEPTH		32
/* size of OrioleDB BTree page */
#define ORIOLEDB_BLCKSZ		8192
/* size of on disk compressed page chunk */
#define ORIOLEDB_COMP_BLCKSZ	512
/* size of data file segment */
#define ORIOLEDB_SEGMENT_SIZE	(1024 * 1024 * 1024)
/* size of S3 data file part */
#define ORIOLEDB_S3_PART_SIZE	(1024 * 1024)

#define GetMaxBackends() MaxBackends

/* Number of orioledb page */
typedef uint32 OInMemoryBlkno;
#define OInvalidInMemoryBlkno		((OInMemoryBlkno) 0xFFFFFFFF)
#define OInMemoryBlknoIsValid(blockNumber) \
	((bool) ((OInMemoryBlkno) (blockNumber) != OInvalidInMemoryBlkno))
#define ORootPageIsValid(desc) (OInMemoryBlknoIsValid((desc)->rootInfo.rootPageBlkno))
#define OMetaPageIsValid(desc) (OInMemoryBlknoIsValid((desc)->rootInfo.metaPageBlkno))

/* Undo log location */
typedef uint64 UndoLocation;
#define	InvalidUndoLocation		UINT64CONST(0x2000000000000000)
#define	MaxUndoLocation			UINT64CONST(0x1FFFFFFFFFFFFFFE)
#define	UndoLocationValueMask	UINT64CONST(0x1FFFFFFFFFFFFFFF)
#define UndoLocationIsValid(loc)	(((loc) & InvalidUndoLocation) == 0)
#define UndoLocationGetValue(loc)	((loc) & UndoLocationValueMask)

/* Identifier for orioledb transaction */
typedef uint64 OXid;
#define	InvalidOXid					UINT64CONST(0x7FFFFFFFFFFFFFFF)
#define OXidIsValid(oxid)			((oxid) != InvalidOXid)
#define LXID_NORMAL_FROM			(1)

/* Index number */
typedef uint16 OIndexNumber;

/* Index type */
typedef enum
{
	oIndexInvalid = 0,
	oIndexToast = 1,
	oIndexBridge = 2,
	oIndexPrimary = 3,
	oIndexUnique = 4,
	oIndexRegular = 5,
} OIndexType;

#define PROC_XID_ARRAY_SIZE	32

typedef enum
{
	/* Invalid value. */
	UndoLogNone = -1,

	/*
	 * Undo log for row-level record of modifications of user data.
	 */
	UndoLogRegular = 0,

	/*
	 * Undo log for page-level record of modifications of user data.
	 */
	UndoLogRegularPageLevel = 1,

	/*
	 * Undo log for modification of system trees.
	 */
	UndoLogSystem = 2,

	UndoLogsCount = 3
} UndoLogType;

#define GET_PAGE_LEVEL_UNDO_TYPE(undoType) \
	(((undoType) == UndoLogRegular) ? UndoLogRegularPageLevel : (undoType))

typedef struct
{
	OXid		oxid;
	VirtualTransactionId vxid;
} XidVXidMapElement;

typedef struct
{
	pg_atomic_uint64 location;
	pg_atomic_uint64 branchLocation;
	pg_atomic_uint64 subxactLocation;
	pg_atomic_uint64 onCommitLocation;
} UndoStackSharedLocations;

typedef struct
{
	pg_atomic_uint64 reservedUndoLocation;
	pg_atomic_uint64 transactionUndoRetainLocation;
	pg_atomic_uint64 snapshotRetainUndoLocation;
} UndoRetainSharedLocations;

typedef struct
{
	UndoRetainSharedLocations undoRetainLocations[(int) UndoLogsCount];
	pg_atomic_uint64 commitInProgressXlogLocation;
	int			autonomousNestingLevel;
	LWLock		undoStackLocationsFlushLock;
	bool		flushUndoLocations;
	bool		waitingForOxid;
	pg_atomic_uint64 xmin;
	UndoStackSharedLocations undoStackLocations[PROC_XID_ARRAY_SIZE][(int) UndoLogsCount];
	XidVXidMapElement vxids[PROC_XID_ARRAY_SIZE];
} ODBProcData;

typedef struct
{
	Oid			datoid;
	Oid			reloid;
	Oid			relnode;
} ORelOids;

typedef uint64 S3TaskLocation;

typedef RelFileLocator RelFileNode;
#define PG_FUNCNAME_MACRO   __func__
#define ORelOidsSetFromRel(oids, rel) \
	do { \
		(oids).datoid = MyDatabaseId; \
		(oids).reloid = (rel)->rd_id; \
		(oids).relnode = (rel)->rd_locator.relNumber; \
	} while (0)
#define RelIsInMyDatabase(rel) ((rel)->rd_locator.dbOid == MyDatabaseId)
#define RelGetNode(rel) ((rel)->rd_locator)
#define RelFileNodeGetNode(node) ((node)->relNumber)
#define IndexStmtGetOldNode(stmt) ((stmt)->oldNumber)
#define RelationSetNewRelfilenode(relation, persistence) \
		RelationSetNewRelfilenumber(relation, persistence)

#define ORelOidsIsValid(oids) (OidIsValid((oids).datoid) && OidIsValid((oids).reloid) && OidIsValid((oids).relnode))
#define ORelOidsIsEqual(l, r) ((l).datoid == (r).datoid && (l).reloid == (r).reloid && (l).relnode == (r).relnode)
#define ORelOidsSetInvalid(oids) \
	((oids).datoid = (oids).reloid = (oids).relnode = InvalidOid)

#if PG_VERSION_NUM >= 170000

#define LXID vxid.lxid
#define REORDER_BUFFER_TUPLE_TYPE HeapTuple
/* Renaming */
#define	TRANSAM_VARIABLES TransamVariables
#define WAIT_EVENT_MQ_PUT_MESSAGE WAIT_EVENT_MESSAGE_QUEUE_PUT_MESSAGE
#define vacuum_is_relation_owner vacuum_is_permitted_for_relation
#define ResourceOwnerEnlargeCatCacheRefs ResourceOwnerEnlarge
#define ResourceOwnerEnlargeCatCacheListRefs ResourceOwnerEnlarge
/* Join BackendId and ProcNumber */
#define BACKENDID procNumber
#define PROCBACKENDID vxid.procNumber
#define MYPROCNUMBER MyProcNumber
#define MyBackendId MyProcNumber
#define	PROCNUMBER(proc) GetNumberFromPGProc(proc)
/* Deprecated */
#define palloc0fast palloc0

#else

#define LXID lxid
#define REORDER_BUFFER_TUPLE_TYPE ReorderBufferTupleBuf *
/* Before renaming */
#define	TRANSAM_VARIABLES ShmemVariableCache
/* BackendId and ProcNumber were separate */
#define BACKENDID backendId
#define PROCBACKENDID backendId
#define MYPROCNUMBER MyProc->pgprocno
#define PROCNUMBER(proc) ((proc)->pgprocno)

#endif

typedef struct
{
	uint64		len:16,
				off:48;
} FileExtent;

/*
 * Should be used as a beginning of header in all orioledb shared in-memory pages:
 * BTree pages, Meta-pages, SeqBuf pages, etc.
 *
 * Isn't written to disk pages. See OrioleDBOndiskPageHeader. Both structs are
 * of equal size.
 */
typedef struct
{
	pg_atomic_uint64 state;
	uint32		pageChangeCount;
	uint32		checkpointNum;
} OrioleDBPageHeader;

/*
 * Should be used as a beginning of header in all orioledb disk pages. At reading
 * pages from disk all these contents are overwritten by OrioleDBPageHeader. Necessary
 * information related to compression is extracted before this overwrite.
 */
typedef struct
{
	/*
	 * We save number of chunks inside downlinks instead of size of compressed
	 * data because it helps us to avoid too often setup dirty flag for parent
	 * if page is changed.
	 *
	 * The header of compressed data contains compressed data length.
	 */
	uint32		checkpointNum;	/* Checkpoint number for both compressed and
								 * not compressed pages */
	uint16		compress_page_size; /* Reserved for compressed pages. Empty
									 * for non-compressed */
	uint8		compress_version;	/* Reserved for compressed pages. Empty
									 * for non-compressed */

	/*
	 * Version of binary page format for possible conversion. For compressed
	 * pages it should be used for conversion of uncompressed images
	 */
	uint8		page_version;
	uint32		reserved1;
	uint32		reserved2;
} OrioleDBOndiskPageHeader;

#define O_PAGE_HEADER_SIZE		sizeof(OrioleDBPageHeader)
#define O_PAGE_HEADER(page)	((OrioleDBPageHeader *)(page))

#define O_PAGE_CHANGE_COUNT_MAX		(0x7FFFFFFF)
#define InvalidOPageChangeCount		(O_PAGE_CHANGE_COUNT_MAX)
#define O_PAGE_CHANGE_COUNT_INC(page) \
	if (O_PAGE_HEADER(page)->pageChangeCount >= O_PAGE_CHANGE_COUNT_MAX) \
		O_PAGE_HEADER(page)->pageChangeCount = 0; \
	else \
		O_PAGE_HEADER(page)->pageChangeCount++;
#define O_PAGE_GET_CHANGE_COUNT(p) (O_PAGE_HEADER(p)->pageChangeCount)

#define S3_OFFSET_MASK		(0x00FFFFFFFF)
#define S3_CHKP_NUM_MASK	(0xFF00000000)
#define S3_CHKP_NUM_SHIFT	(32)
#define S3_GET_CHKP_NUM(offset) (((offset) & S3_CHKP_NUM_MASK) >> S3_CHKP_NUM_SHIFT)

#define InvalidFileExtentLen (0)
#define InvalidFileExtentOff (UINT64CONST(0xFFFFFFFFFFFF))
#define FileExtentLenIsValid(len) ((len) != InvalidFileExtentLen)
#define FileExtentOffIsValid(off) ((off) < InvalidFileExtentOff)
#define FileExtentIsValid(extent) (FileExtentLenIsValid((extent).len) && FileExtentOffIsValid((extent).off))
#define CompressedSize(page_size) ((page_size) == ORIOLEDB_BLCKSZ \
										? ORIOLEDB_BLCKSZ \
										: ((page_size) + sizeof(OrioleDBOndiskPageHeader) + ORIOLEDB_COMP_BLCKSZ - 1))
#define FileExtentLen(page_size) (CompressedSize(page_size) / ORIOLEDB_COMP_BLCKSZ)

typedef struct
{
	ORelOids	oids;
	int			ionum;
	FileExtent	fileExtent;
	uint32		flags:4,
				type:28;
	OInMemoryBlkno leftBlkno;
} OrioleDBPageDesc;

/* orioledb.c */
extern Size orioledb_buffers_size;
extern Size orioledb_buffers_count;
extern Size undo_circular_buffer_size;
extern uint32 undo_buffers_count;
extern Size xid_circular_buffer_size;
extern Size rewind_circular_buffer_size;
extern double regular_block_undo_circular_buffer_fraction;
extern double system_undo_circular_buffer_fraction;
extern uint32 xid_buffers_count;
extern uint32 rewind_buffers_count;
extern Pointer o_shared_buffers;
extern ODBProcData *oProcData;
extern int	max_procs;
extern OrioleDBPageDesc *page_descs;
extern bool remove_old_checkpoint_files;
extern bool skip_unmodified_trees;
extern bool debug_disable_bgwriter;
extern MemoryContext btree_insert_context;
extern MemoryContext btree_seqscan_context;
extern double o_checkpoint_completion_ratio;
extern int	max_io_concurrency;
extern bool use_mmap;
extern bool use_device;
extern bool orioledb_use_sparse_files;
extern int	device_fd;
extern char *device_filename;
extern Pointer mmap_data;
extern Size device_length;
extern int	default_compress;
extern int	default_primary_compress;
extern int	default_toast_compress;
extern bool orioledb_table_description_compress;
extern BlockNumber max_bridge_ctid_blkno;
extern bool orioledb_s3_mode;
extern int	s3_num_workers;
extern int	s3_desired_size;
extern int	s3_queue_size_guc;
extern char *s3_host;
extern bool s3_use_https;
extern char *s3_region;
extern char *s3_prefix;
extern char *s3_accesskey;
extern char *s3_secretkey;
extern char *s3_cainfo;
extern bool enable_rewind;
extern int	rewind_max_time;
extern int	rewind_max_transactions;
extern int	logical_xid_buffers_guc;
extern bool orioledb_strict_mode;

#define GET_CUR_PROCDATA() \
	(AssertMacro(MYPROCNUMBER >= 0 && \
				 MYPROCNUMBER < max_procs), \
	 &oProcData[MYPROCNUMBER])
#define O_GET_IN_MEMORY_PAGE(blkno) \
	(AssertMacro(OInMemoryBlknoIsValid(blkno)), \
	 (Page)(o_shared_buffers + (((uint64) (blkno)) * ((uint64) ORIOLEDB_BLCKSZ))))
#define O_GET_IN_MEMORY_PAGEDESC(blkno) \
	(AssertMacro(OInMemoryBlknoIsValid(blkno)), page_descs + (blkno))
#define O_GET_IN_MEMORY_PAGE_CHANGE_COUNT(blkno) \
	(O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(blkno)))

extern void orioledb_check_shmem(void);

typedef int OCompress;
#define O_COMPRESS_DEFAULT (10)
#define InvalidOCompress (-1)
#define OCompressIsValid(compress) ((compress) != InvalidOCompress)

typedef struct ORelOptions
{
	StdRdOptions std_options;
	int			compress_offset;
	int			primary_compress_offset;
	int			toast_compress_offset;
	bool		index_bridging;
} ORelOptions;

typedef struct OBTOptions
{
	BTOptions	bt_options;
	int			compress_offset;
	bool		orioledb_index;
} OBTOptions;

extern int16 o_parse_compress(const char *value);
extern void o_invalidate_oids(ORelOids oids);

#define EXPR_ATTNUM (FirstLowInvalidHeapAttributeNumber - 1)

/* orioledb.c */
typedef enum OPagePoolType
{
	OPagePoolMain = 0,
	OPagePoolFreeTree = 1,
	OPagePoolCatalog = 2
} OPagePoolType;
#define OPagePoolTypesCount 3

typedef struct OPagePool OPagePool;
struct BTreeDescr;

extern void o_verify_dir_exists_or_create(char *dirname, bool *created, bool *found);
extern uint64 orioledb_device_alloc(struct BTreeDescr *descr, uint32 size);
extern OPagePool *get_ppool(OPagePoolType type);
extern OPagePool *get_ppool_by_blkno(OInMemoryBlkno blkno);
extern OInMemoryBlkno get_dirty_pages_count_sum(void);
extern void jsonb_push_key(JsonbParseState **state, char *key);
extern void jsonb_push_null_key(JsonbParseState **state, char *key);
extern void jsonb_push_bool_key(JsonbParseState **state, char *key, bool value);
extern void jsonb_push_int8_key(JsonbParseState **state, char *key, int64 value);
extern void jsonb_push_string_key(JsonbParseState **state, const char *key, const char *value);
extern bool is_bump_memory_context(MemoryContext mxct);

extern CheckPoint_hook_type next_CheckPoint_hook;

/* tableam_handler.c */
extern bool is_orioledb_rel(Relation rel);

typedef struct OTableDescr OTableDescr;
typedef struct OIndexDescr OIndexDescr;

/* ddl.c */
extern List *reindex_list;
extern IndexBuildResult o_pkey_result;
extern bool o_in_add_column;

extern void orioledb_setup_ddl_hooks(void);
extern void o_ddl_cleanup(void);
extern void o_drop_table(ORelOids oids);

/* scan.c */
extern CustomScanMethods o_scan_methods;

#endif							/* __ORIOLEDB_H__ */
