/*-------------------------------------------------------------------------
 *
 * btree.h
 * 		General declarations for OrioleDB B-tree implementation
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/btree.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_H__
#define __BTREE_H__

#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/seq_buf.h"

#include "access/sdir.h"
#include "lib/stringinfo.h"
#include "storage/bufpage.h"
#include "storage/fd.h"
#include "storage/off.h"

#define BTREE_NUM_META_LWLOCKS	(128)

typedef struct BTreeDescr BTreeDescr;
typedef struct BTreeIterator BTreeIterator;
typedef struct CheckpointFileHeader CheckpointFileHeader;

typedef uint16 OIndexNumber;
typedef uint64 OTupleXactInfo;

#define PrimaryIndexNumber (0)
#define TOASTIndexNumber (0xFFFE)
#define InvalidIndexNumber (0xFFFF)

typedef enum BTreeKeyType
{
	BTreeKeyLeafTuple,
	BTreeKeyNonLeafKey,
	BTreeKeyBound,
	BTreeKeyUniqueLowerBound,
	BTreeKeyUniqueUpperBound,
	/* following values are never passed to comparison function */
	BTreeKeyNone,
	BTreeKeyPageHiKey,
	BTreeKeyRightmost
} BTreeKeyType;

#define IS_BOUND_KEY_TYPE(keyType) \
	((keyType) == BTreeKeyBound || \
	 (keyType) == BTreeKeyUniqueLowerBound || \
	 (keyType) == BTreeKeyUniqueUpperBound)

typedef int (*OBTreeKeyCmp) (BTreeDescr *descr,
							 void *p1, BTreeKeyType k1,
							 void *p2, BTreeKeyType k2);

typedef struct
{
	OInMemoryBlkno rootPageBlkno;
	uint32		rootPageChangeCount;
	OInMemoryBlkno metaPageBlkno;
} BTreeRootInfo;

typedef enum
{
	/* just in memory BTree, no eviction and no checkpoint support */
	BTreeStorageInMemory,
	/* no checkpoint support, but pages can be evicted into a disk */
	BTreeStorageTemporary,
	/* checkpoint and eviction for pages support */
	BTreeStoragePersistence
} BTreeStorageType;

typedef enum BTreeOperationType
{
	BTreeOperationInsert,
	BTreeOperationLock,
	BTreeOperationUpdate,
	BTreeOperationDelete
} BTreeOperationType;

typedef enum BTreeLeafTupleDeletedStatus
{
	BTreeLeafTupleNonDeleted = 0,
	BTreeLeafTupleDeleted = 1,
	BTreeLeafTupleMovedPartitions = 2,
	BTreeLeafTuplePKChanged = 3
} BTreeLeafTupleDeletedStatus;

typedef struct
{
	Pointer		data;
	uint8		formatFlags;
} OTuple;

#define O_TUPLE_IS_NULL(tup) ((tup).data == NULL)
#define O_TUPLE_SET_NULL(tup) \
	do { \
        (tup).data = NULL; \
        (tup).formatFlags = 0; \
    } while (false)

typedef union
{
	struct
	{
		File	   *files;
		int			filesAllocated;
	}			array;
	struct s3Files_hash *hash;
} OSmgr;

typedef enum
{
	OTupleLength,
	OKeyLength,
	OTupleKeyLength,
	OTupleKeyLengthNoVersion
} OLengthType;

typedef struct
{
	/*
	 * Get the length of a given `tuple` of a `type`.  Must be safe for
	 * critical sections.
	 */
	int			(*len) (BTreeDescr *desc, OTuple tuple, OLengthType type);

	/*
	 * Changes BTreeKeyLeafTuple to BTreeKeyNonLeafKey.  If `data` is given,
	 * then write data there.  Otherwise, it may allocate memory or use static
	 * memory for the result (the `*allocated` flag reflects this).  When
	 * `data` is given, this function must be safe for the critical section.
	 */
	OTuple		(*tuple_make_key) (BTreeDescr *desc, OTuple tuple, Pointer data,
								   bool keepVersion, bool *allocated);

	JsonbValue *(*key_to_jsonb) (BTreeDescr *desc, OTuple key,
								 JsonbParseState **state);
	bool		(*needs_undo) (BTreeDescr *desc, BTreeOperationType action,
							   OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
							   OTuple newTuple, OXid newOxid);
	uint32		(*hash) (BTreeDescr *desc, OTuple tuple, BTreeKeyType tupleType);
	uint32		(*unique_hash) (BTreeDescr *desc, OTuple tuple);
	OBTreeKeyCmp cmp;
} BTreeOps;

struct BTreeDescr
{
	BTreeRootInfo rootInfo;
	void	   *arg;
	OSmgr		smgr;
	ORelOids	oids;
	OIndexType	type;
	OPagePool  *ppool;
	OCompress	compress;
	UndoReserveType undoType;
	BTreeStorageType storageType;
	SeqBufDescPrivate freeBuf;
	SeqBufDescPrivate nextChkp[2];
	SeqBufDescPrivate tmpBuf[2];
	OXid		createOxid;
	BTreeOps   *ops;
};

static inline int
o_btree_len(BTreeDescr *desc, OTuple tuple, OLengthType type)
{
	return desc->ops->len(desc, tuple, type);
}

static inline OTuple
o_btree_tuple_make_key(BTreeDescr *desc, OTuple tuple, Pointer data,
					   bool keepVersion, bool *allocated)
{
	return desc->ops->tuple_make_key(desc, tuple, data, keepVersion, allocated);
}

static inline JsonbValue *
o_btree_key_to_jsonb(BTreeDescr *desc, OTuple key, JsonbParseState **state)
{
	return desc->ops->key_to_jsonb(desc, key, state);
}

static inline bool
o_btree_needs_undo(BTreeDescr *desc, BTreeOperationType action,
				   OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
				   OTuple newTuple, OXid newOxid)
{
	return (desc->ops->needs_undo != NULL) &&
		desc->ops->needs_undo(desc, action, oldTuple, oldXactInfo,
							  oldDeleted, newTuple, newOxid);
}

static inline uint32
o_btree_hash(BTreeDescr *desc, OTuple tuple, BTreeKeyType tupleType)
{
	return desc->ops->hash(desc, tuple, tupleType);
}

static inline uint32
o_btree_unique_hash(BTreeDescr *desc, OTuple tuple)
{
	return desc->ops->unique_hash(desc, tuple);
}

static inline int
o_btree_cmp(BTreeDescr *desc, void *p1, BTreeKeyType k1,
			void *p2, BTreeKeyType k2)
{
	return desc->ops->cmp(desc, p1, k1, p2, k2);
}


typedef struct BTreePageItemLocator BTreePageItemLocator;

typedef struct
{
	OInMemoryBlkno blkno;
	uint32		pageChangeCount;
} BTreeLocationHint;

typedef struct
{
	BTreeLocationHint hint;
	CommitSeqNo csn;
	uint32		version;
} ORowIdAddendumCtid;

typedef struct
{
	BTreeLocationHint hint;
	CommitSeqNo csn;
	uint8		flags;
} ORowIdAddendumNonCtid;

/*
 * Check if given tree has assigned datoid, reloid and relnode.
 */
#define TREE_HAS_OIDS(desc) (ORelOidsIsValid((desc)->oids))

/*
 * Get number of tree leaf pages.
 */
#define TREE_NUM_LEAF_PAGES(desc) \
	(pg_atomic_read_u32(&BTREE_GET_META(desc)->leafPagesNum))

/*
 * Check if given tree needs WAL and XIP records.  Currently, only primary index
 * tree and TOAST tree need it.  Argument is (BTreeDescr *).
 */
#define TREE_NEEDS_WAL(desc) \
	(TREE_HAS_OIDS(desc) && \
		((desc)->type == oIndexPrimary || (desc)->type == oIndexToast))

/* btree.c */
typedef enum OBTreeModifyCallbackAction
{
	OBTreeCallbackActionDoNothing = 1,
	OBTreeCallbackActionUpdate = 2,
	OBTreeCallbackActionDelete = 3,
	OBTreeCallbackActionLock = 4,
	OBTreeCallbackActionUndo = 5
} OBTreeModifyCallbackAction;

typedef enum OBTreeWaitCallbackAction
{
	OBTreeCallbackActionXidNoWait = 1,
	OBTreeCallbackActionXidWait = 2,
	OBTreeCallbackActionXidExit = 3
} OBTreeWaitCallbackAction;

typedef enum OBTreeModifyResult
{
	OBTreeModifyResultInserted = 1,
	OBTreeModifyResultUpdated = 2,
	OBTreeModifyResultDeleted = 3,
	OBTreeModifyResultLocked = 4,
	OBTreeModifyResultFound = 5,
	OBTreeModifyResultNotFound = 6
} OBTreeModifyResult;

typedef enum RowLockMode
{
	RowLockKeyShare = 0,
	RowLockShare = 1,
	RowLockNoKeyUpdate = 2,
	RowLockUpdate = 3
} RowLockMode;

#define ROW_LOCKS_CONFLICT(lock1, lock2) ((lock1) + (lock2) >= 3)

/*
 * OTupleXactInfo contains information about transaction, lock mode, lock only
 * flag.
 */
#define XACT_INFO_LOCK_ONLY_BIT \
	UINT64CONST(0x1000000000000000)
#define XACT_INFO_LOCK_MODE_MASK \
	UINT64CONST(0x0C00000000000000)
#define XACT_INFO_LOCK_OXID_MASK \
	UINT64CONST(0x03FFFFFFFFFFFFFF)
#define XACT_INFO_LOCK_MODE_SHIFT \
	(58)
#define	XACT_INFO_IS_LOCK_ONLY(xactInfo) \
	((xactInfo) & XACT_INFO_LOCK_ONLY_BIT)
#define XACT_INFO_MAP_CSN(xactInfo) \
	(oxid_get_csn(XACT_INFO_GET_OXID((xactInfo))))
#define XACT_INFO_GET_OXID(xactInfo) \
	((xactInfo) & XACT_INFO_LOCK_OXID_MASK)
#define XACT_INFO_OXID_EQ(xactInfo, oxid) \
	(XACT_INFO_GET_OXID((xactInfo)) == (oxid))
#define XACT_INFO_OXID_IS_CURRENT(xactInfo) \
	(XACT_INFO_GET_OXID((xactInfo)) == get_current_oxid_if_any())
#define	XACT_INFO_IS_FINISHED(xactInfo) \
	(xid_is_finished(XACT_INFO_GET_OXID(xactInfo)))
#define	XACT_INFO_FINISHED_FOR_EVERYBODY(xactInfo) \
	(xid_is_finished_for_everybody(XACT_INFO_GET_OXID(xactInfo)))
#define XACT_INFO_GET_LOCK_MODE(xactInfo) \
	(((xactInfo) & XACT_INFO_LOCK_MODE_MASK) >> XACT_INFO_LOCK_MODE_SHIFT)
#define OXID_GET_XACT_INFO(oxid, lockmode, lockonly) \
	(AssertMacro(((lockmode) & (XACT_INFO_LOCK_MODE_MASK >> XACT_INFO_LOCK_MODE_SHIFT)) == (lockmode)), \
	 (OTupleXactInfo)(oxid) | ((OTupleXactInfo) (lockmode) << XACT_INFO_LOCK_MODE_SHIFT) | \
	 ((lockonly) ? XACT_INFO_LOCK_ONLY_BIT : 0))

/* btree/btree.c */
extern LWLockPadded *unique_locks;
extern int	num_unique_locks;
typedef struct ItemPointerData ItemPointerData;

extern void o_btree_check_size_of_tuple(int len, char *relation_name, bool index);
extern void o_btree_init_unique_lwlocks(void);
extern void o_btree_init(BTreeDescr *descr);
extern void o_btree_cleanup_pages(OInMemoryBlkno root, OInMemoryBlkno metaPageBlkno,
								  uint32 rootPageChangeCount);
extern ItemPointerData btree_ctid_get_and_inc(BTreeDescr *desc);
extern void btree_ctid_update_if_needed(BTreeDescr *desc, ItemPointerData ctid);
extern void btree_desc_stopevent_params_internal(BTreeDescr *desc,
												 JsonbParseState **state);
extern void btree_page_stopevent_params_internal(BTreeDescr *desc, Page p,
												 JsonbParseState **state);
extern Jsonb *btree_page_stopevent_params(BTreeDescr *desc, Page p);
extern Jsonb *btree_downlink_stopevent_params(BTreeDescr *desc, Page p,
											  BTreePageItemLocator *loc);

#endif							/* __BTREE_H__ */
