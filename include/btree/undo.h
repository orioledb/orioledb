/*-------------------------------------------------------------------------
 *
 * undo.h
 *		Declarations of B-tree undo records and routines dealing with them.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/undo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_UNDO_H__
#define __BTREE_UNDO_H__

#include "btree/page_contents.h"

/*
 * B-Tree page images types which can be stored in undo log.
 */
typedef enum
{
	/* produced by pages split */
	UndoPageImageCompact,
	/* produced by pages split */
	UndoPageImageSplit,
	/* produced by pages merge */
	UndoPageImageMerge
} UndoPageImageType;

/*
 * B-Tree page images header in undo log.
 */
typedef struct
{
	UndoPageImageType type;
	uint8		splitKeyFlags;
	LocationIndex splitKeyLen;
} UndoPageImageHeader;

/*
 * Status of existing lock on the tuple made by the same transaction;
 */
typedef enum
{
	BTreeModifyNoLock = 1,
	BTreeModifyWeakerLock = 2,
	BTreeModifySameOrStrongerLock = 3
} BTreeModifyLockStatus;

/* size of image in undo log produced by page compaction  */
#define O_COMPACT_UNDO_IMAGE_SIZE (MAXALIGN(sizeof(UndoPageImageHeader)) + ORIOLEDB_BLCKSZ)
/* max size of image in undo log produced by page split */
#define O_MAX_SPLIT_UNDO_IMAGE_SIZE (MAXALIGN(sizeof(UndoPageImageHeader)) + ORIOLEDB_BLCKSZ + O_BTREE_MAX_KEY_SIZE)
/* size of image in undo log produced by page split */
#define O_SPLIT_UNDO_IMAGE_SIZE(splitKeySize) (MAXALIGN(sizeof(UndoPageImageHeader)) + ORIOLEDB_BLCKSZ + MAXALIGN(splitKeySize))
/* max size of update undo record */
#define O_UPDATE_MAX_UNDO_SIZE (BTreeLeafTuphdrSize + O_BTREE_MAX_TUPLE_SIZE)
/* on modification we should reserve size for split and update undo records */
#define O_MODIFY_UNDO_RESSERVE_SIZE (2 * (O_MAX_SPLIT_UNDO_IMAGE_SIZE + O_UPDATE_MAX_UNDO_SIZE))
/* size of image in undo log produced by pages merge */
#define O_MERGE_UNDO_IMAGE_SIZE (MAXALIGN(sizeof(UndoPageImageHeader)) + ORIOLEDB_BLCKSZ * 2)
/* undo location of a page image */
#define O_UNDO_GET_IMAGE_LOCATION(undo_loc, left) ((undo_loc) + MAXALIGN(sizeof(UndoPageImageHeader)) + ((left) ? 0 : ORIOLEDB_BLCKSZ))

extern bool page_item_rollback(BTreeDescr *desc, Page p, BTreePageItemLocator *locator,
							   bool loop, BTreeLeafTuphdr *non_lock_tuphdr_ptr,
							   UndoLocation nonLockUndoLocation);
extern BTreeLeafTuphdr *make_undo_record(BTreeDescr *desc, OTuple tuple,
										 bool is_tuple, BTreeOperationType action,
										 OInMemoryBlkno blkno, uint32 pageChangeCount,
										 UndoLocation *undoLocation);

extern void get_page_from_undo(BTreeDescr *desc, UndoLocation undo_loc, Pointer key,
							   BTreeKeyType kind, Pointer dest,
							   bool *is_left, bool *is_right, OFixedKey *lokey,
							   OFixedKey *page_lokey, OTuple *page_hikey);
extern UndoLocation page_add_item_to_undo(BTreeDescr *desc, Pointer p,
										  CommitSeqNo imageCsn,
										  OTuple *splitKey, LocationIndex splitKeyLen);
extern UndoLocation make_merge_undo_image(BTreeDescr *desc, Pointer left,
										  Pointer right, CommitSeqNo imageCsn);
extern bool row_lock_conflicts(BTreeLeafTuphdr *pageTuphdr,
							   BTreeLeafTuphdr *conflictTupHdr,
							   UndoLocation *conflictUndoLocation,
							   RowLockMode mode,
							   OXid my_oxid, CommitSeqNo my_csn,
							   OInMemoryBlkno blkno,
							   UndoLocation savepointUndoLocation,
							   bool *redundant_row_locks,
							   BTreeModifyLockStatus *lock_status);
extern void remove_redundant_row_locks(BTreeLeafTuphdr *tuphdr_ptr,
									   BTreeLeafTuphdr *conflictTuphdrPtr,
									   UndoLocation *conflictTupHdrUndoLocation,
									   RowLockMode mode, OXid my_oxid,
									   OInMemoryBlkno blkno,
									   UndoLocation savepointUndoLocation);
extern UndoLocation find_non_lock_only_undo_record(BTreeLeafTuphdr *tuphdr);
extern void modify_undo_callback(UndoLocation location,
								 UndoStackItem *baseItem,
								 OXid oxid,
								 bool abort,
								 bool changeCountsValid);
extern void lock_undo_callback(UndoLocation location, UndoStackItem *baseItem,
							   OXid oxid, bool abort,
							   bool changeCountsValid);
extern void btree_relnode_undo_callback(UndoLocation location,
										UndoStackItem *baseItem, OXid oxid,
										bool abort,
										bool changeCountsValid);
extern void get_prev_leaf_header_from_undo(BTreeLeafTuphdr *tuphdr,
										   bool inPage);
extern void get_prev_leaf_header_and_tuple_from_undo(BTreeLeafTuphdr *tuphdr,
													 OTuple *tuple,
													 LocationIndex sizeAvailable);
extern void update_leaf_header_in_undo(BTreeLeafTuphdr *tuphdr, UndoLocation location);
extern void add_undo_truncate_relnode(ORelOids oldOids, ORelOids *oldTreeOids,
									  int oldNumTreeOids,
									  ORelOids newOids, ORelOids *newTreeOids,
									  int newNumTreeOids);
extern void add_undo_drop_relnode(ORelOids oids, ORelOids *treeOids,
								  int numTreeOids);
extern void add_undo_create_relnode(ORelOids oids, ORelOids *treeOids,
									int numTreeOids);

#endif							/* __BTREE_UNDO_H__ */
