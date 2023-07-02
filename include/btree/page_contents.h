/*-------------------------------------------------------------------------
 *
 * page_contents.h
 *		Declarations of OrioleDB B-tree page structure.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/page_contents.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_PAGE_CONTENTS_H__
#define __BTREE_PAGE_CONTENTS_H__

#include "btree/page_state.h"
#include "s3/queue.h"

#define NUM_SEQ_SCANS_ARRAY_SIZE	32
#define MAX_NUM_DIRTY_PARTS			4

/* The structure of BTree meta page.  Referenced by metaPageBlkno. */
typedef struct
{
	OrioleDBPageHeader o_header;
	SeqBufDescShared freeBuf;
	SeqBufDescShared nextChkp[2];
	SeqBufDescShared tmpBuf[2];
	pg_atomic_uint64 numFreeBlocks;
	pg_atomic_uint64 datafileLength[2];
	LWLock		metaLock;
	LWLock		copyBlknoLock;

	/*
	 * A surrogate index key value which can be incremented on an INSERT
	 * operation.
	 *
	 * It can be used (and incremented) as an index key by primary index only
	 * if an index key isn't defined.
	 */
	pg_atomic_uint64 ctid;
	pg_atomic_uint32 leafPagesNum;

	/* Number of running sequential scans depending on the checkpoint number */
	pg_atomic_uint32 numSeqScans[NUM_SEQ_SCANS_ARRAY_SIZE];

	/*
	 * Pending data file parts to be synchronized with S3.
	 */
	struct
	{
		struct
		{
			int32		segNum;
			int32		partNum;
		}			dirtyParts[MAX_NUM_DIRTY_PARTS];
		S3TaskLocation writeMaxLocation;
	}			partsInfo[2];
} BTreeMetaPage;

StaticAssertDecl(sizeof(BTreeMetaPage) <= ORIOLEDB_BLCKSZ,
				 "BTreeMetaPage struct doesn't fit to the page size");

#define BTREE_GET_META(desc) \
	((BTreeMetaPage *) O_GET_IN_MEMORY_PAGE((desc)->rootInfo.metaPageBlkno))

typedef struct
{
	uint32		shortLocation:12,
				offset:10,
				hikeyShortLocation:8,
				hikeyFlags:2;
} BTreePageChunkDesc;

#define LOCATION_GET_SHORT(l) \
	(AssertMacro(((l) & 3) == 0), (l) / 4)
#define SHORT_GET_LOCATION(s) \
	((s) * 4)

typedef struct
{
	LocationIndex items[1];
} BTreePageChunk;

#define BTREE_PAGE_MAX_CHUNK_ITEMS \
	((ORIOLEDB_BLCKSZ - sizeof(BTreePageHeader)) / \
		(MAXIMUM_ALIGNOF + sizeof(LocationIndex)))

#define BTREE_PAGE_MAX_CHUNKS \
	((512 - offsetof(BTreePageHeader, chunkDesc)) / \
		(MAXIMUM_ALIGNOF + sizeof(BTreePageChunkDesc)))

struct BTreePageItemLocator
{
	OffsetNumber chunkOffset;
	OffsetNumber itemOffset;
	OffsetNumber chunkItemsCount;
	LocationIndex chunkSize;
	BTreePageChunk *chunk;
};

/* The header of the B-tree pages */
typedef struct
{
	OrioleDBPageHeader o_header;
	uint32		flags:6,

	/*
	 * For non-leafs, level of page in the tree.  Unused for leafs.
	 */
				field1:11,

	/*
	 * For leafs, number of bytes occupied by deleted tuples which could be
	 * potentially vacated during page compaction.  For non-leafs, number of
	 * on-disk downlinks.
	 */
				field2:15;

	/* Link to the page-level undo item and corresponding CSN */
	UndoLocation undoLocation;
	CommitSeqNo csn;

	uint64		rightLink;
	uint32		checkpointNum;
	LocationIndex maxKeyLen;
	OffsetNumber prevInsertOffset;
	OffsetNumber chunksCount;
	OffsetNumber itemsCount;
	OffsetNumber hikeysEnd;
	LocationIndex dataSize;
	BTreePageChunkDesc chunkDesc[1];
} BTreePageHeader;

/* Flags of B-tree pages */
#define O_BTREE_FLAG_LEFTMOST			(0x0001)
#define O_BTREE_FLAG_RIGHTMOST			(0x0002)
#define O_BTREE_FLAG_LEAF				(0x0004)
#define O_BTREE_FLAG_BROKEN_SPLIT		(0x0008)
#define O_BTREE_FLAG_PRE_CLEANUP		(0x0010)
#define O_BTREE_FLAGS_ROOT_INIT		(O_BTREE_FLAG_LEAF | O_BTREE_FLAG_RIGHTMOST | O_BTREE_FLAG_LEFTMOST)

/* Check given property of B-tree page */
#define O_PAGE_IS(page, property) ((((BTreePageHeader *)(page))->flags & O_BTREE_FLAG_##property) != 0)

#define BTREE_PAGE_HIKEYS_END(desc, p) (O_PAGE_IS(p, LEAF) ? 256 : 512)

typedef struct PartialPageState PartialPageState;

/* Macros for accessing B-tree page items */
#define ITEM_GET_OFFSET(item) ((item) & 0x3FFF)
#define ITEM_GET_FLAGS(item) ((item) >> 14)
#define ITEM_SET_FLAGS(item, flags) (flags ? (item) | ((LocationIndex) (1) << 14) : ((item) & ~((LocationIndex) (1) << 14)))

#define BTREE_PAGE_LOCATOR_FIRST(p, locptr) \
	page_item_fill_locator((p), 0, (locptr))
#define BTREE_PAGE_LOCATOR_LAST(p, locptr) \
	page_item_fill_locator_backwards((p), BTREE_PAGE_ITEMS_COUNT(p) - 1, (locptr))
#define BTREE_PAGE_LOCATOR_TAIL(p, locptr) \
	page_item_fill_locator_backwards((p), BTREE_PAGE_ITEMS_COUNT(p), (locptr))
#define BTREE_PAGE_LOCATOR_NEXT(p, locptr) \
	((++(locptr)->itemOffset < (locptr)->chunkItemsCount) ? true : page_locator_next_chunk((p), (locptr)))
#define BTREE_PAGE_LOCATOR_PREV(p, locptr) \
	(((locptr)->itemOffset > 0) ? (locptr)->itemOffset-- : page_locator_prev_chunk((p), (locptr)))
#define BTREE_PAGE_LOCATOR_IS_VALID(p, locptr) \
	((void) (p), (locptr)->chunk != NULL && (locptr)->itemOffset < (locptr)->chunkItemsCount)
#define BTREE_PAGE_FOREACH_ITEMS(p, locptr) \
	for (BTREE_PAGE_LOCATOR_FIRST((p), (locptr)); \
		 BTREE_PAGE_LOCATOR_IS_VALID((p), (locptr)); \
		 BTREE_PAGE_LOCATOR_NEXT((p), (locptr)))
#define BTREE_PAGE_LOCATOR_SET_INVALID(locptr) \
	((locptr)->chunk = 0)
#define BTREE_PAGE_LOCATOR_GET_ITEM(p, locptr) \
	((void) (p), (Pointer) (locptr)->chunk + \
		ITEM_GET_OFFSET((locptr)->chunk->items[(locptr)->itemOffset]))
#define BTREE_PAGE_OFFSET_GET_LOCATOR(p, offset, locptr) \
	(page_item_fill_locator((p), (offset), (locptr)))
#define BTREE_PAGE_LOCATOR_GET_OFFSET(p, locptr) \
	(((BTreePageHeader *) (p))->chunkDesc[(locptr)->chunkOffset].offset + \
		(locptr)->itemOffset)
#define BTREE_PAGE_GET_ITEM_SIZE(p, locptr) \
	(page_locator_get_item_size((p), (locptr)))
#define BTREE_PAGE_GET_ITEM_OFFSET(p, locptr) \
	((LocationIndex) ((Pointer) (locptr)->chunk - (Pointer) (p)) + \
		ITEM_GET_OFFSET((locptr)->chunk->items[(locptr)->itemOffset]))
#define BTREE_PAGE_GET_ITEM_FLAGS(p, locptr) \
	((void) (p), ITEM_GET_FLAGS((locptr)->chunk->items[(locptr)->itemOffset]))
#define BTREE_PAGE_SET_ITEM_FLAGS(p, locptr, flags) \
	((void) (p), (locptr)->chunk->items[(locptr)->itemOffset] = ITEM_SET_FLAGS((locptr)->chunk->items[(locptr)->itemOffset], (flags)))
#define BTREE_PAGE_READ_LEAF_ITEM(tuphdr, tup, p, locptr) \
	do { \
		Pointer		__item = BTREE_PAGE_LOCATOR_GET_ITEM(p, locptr); \
		Assert(O_PAGE_IS(p, LEAF)); \
		(tuphdr) = (BTreeLeafTuphdr *) __item; \
		(tup).data = __item + BTreeLeafTuphdrSize; \
		(tup).formatFlags = BTREE_PAGE_GET_ITEM_FLAGS(p, locptr); \
	} while (false)
#define BTREE_PAGE_READ_INTERNAL_ITEM(tuphdr, tup, p, locptr) \
	do { \
		Pointer		__item = BTREE_PAGE_LOCATOR_GET_ITEM(p, locptr); \
		Assert(!O_PAGE_IS(p, LEAF)); \
		(tuphdr) = (BTreeNonLeafTuphdr *) __item; \
		(tup).data = __item + BTreeNonLeafTuphdrSize; \
		(tup).formatFlags = BTREE_PAGE_GET_ITEM_FLAGS(p, locptr); \
	} while (false)
#define BTREE_PAGE_READ_LEAF_TUPLE(tup, p, locptr) \
	do { \
		Pointer		__item = BTREE_PAGE_LOCATOR_GET_ITEM(p, locptr); \
		Assert(O_PAGE_IS(p, LEAF)); \
		(tup).formatFlags = BTREE_PAGE_GET_ITEM_FLAGS(p, locptr); \
		(tup).data = __item + BTreeLeafTuphdrSize; \
	} while (false)
#define BTREE_PAGE_READ_INTERNAL_TUPLE(tup, p, locptr) \
	do { \
		Pointer		__item = BTREE_PAGE_LOCATOR_GET_ITEM(p, locptr); \
		Assert(!O_PAGE_IS(p, LEAF)); \
		(tup).formatFlags = BTREE_PAGE_GET_ITEM_FLAGS(p, locptr); \
		(tup).data = __item + BTreeNonLeafTuphdrSize; \
	} while (false)
#define BTREE_PAGE_READ_TUPLE(tup, p, locptr) \
	do { \
		Pointer		__item = BTREE_PAGE_LOCATOR_GET_ITEM(p, locptr); \
		(tup).formatFlags = BTREE_PAGE_GET_ITEM_FLAGS(p, locptr); \
		if (O_PAGE_IS(p, LEAF)) \
			(tup).data = __item + BTreeLeafTuphdrSize; \
		else \
			(tup).data = __item + BTreeNonLeafTuphdrSize; \
	} while (false)
#define BTREE_PAGE_ITEMS_COUNT(p)	\
	(((BTreePageHeader *)(p))->itemsCount)
#define BTREE_PAGE_READ_UNDO_ITEM(tuphdr, tup, rec) \
	do { \
		(tuphdr) = (BTreeLeafTuphdr *) (rec); \
		(tup).data = (Pointer) (rec) + BTreeLeafTuphdrSize; \
		(tup).formatFlags = (tuphdr)->formatFlags; \
	} while (false)
#define BTREE_PAGE_GET_HIKEY(hikey, p) \
	(hikey) = page_get_hikey((p))
#define BTREE_PAGE_GET_HIKEY_SIZE(p) \
	(page_get_hikey_size((p)))
#define BTREE_PAGE_SET_HIKEY_FLAGS(p, flags)	\
	(page_set_hikey_flags((p), (flags)))
#define BTREE_PAGE_FREE_SPACE(p) \
	(ORIOLEDB_BLCKSZ - ((BTreePageHeader *) (p))->dataSize)

#define BTREE_PAGE_GET_RIGHTLINK(p)	(((BTreePageHeader *)(p))->rightLink)

#define PAGE_GET_LEVEL(p) (O_PAGE_IS(p, LEAF) ? 0 : ((BTreePageHeader *)(p))->field1)
#define PAGE_SET_LEVEL(p, level) (AssertMacro(!O_PAGE_IS(p, LEAF)), ((BTreePageHeader *)(p))->field1 = (level))
#define PAGE_GET_N_ONDISK(p) (AssertMacro(!O_PAGE_IS(p, LEAF)), ((BTreePageHeader *)(p))->field2)
#define PAGE_SET_N_ONDISK(p, n) (AssertMacro(!O_PAGE_IS(p, LEAF)), ((BTreePageHeader *)(p))->field2 = (n))
#define PAGE_INC_N_ONDISK(p) (AssertMacro(!O_PAGE_IS(p, LEAF)), ((BTreePageHeader *)(p))->field2++)
#define PAGE_DEC_N_ONDISK(p) (AssertMacro(!O_PAGE_IS(p, LEAF)), ((BTreePageHeader *)(p))->field2--)
#define PAGE_GET_N_VACATED(p) (AssertMacro(O_PAGE_IS(p, LEAF)), ((BTreePageHeader *)(p))->field2)
#define PAGE_SET_N_VACATED(p, n) (AssertMacro(O_PAGE_IS(p, LEAF)), ((BTreePageHeader *)(p))->field2 = (n))
#define PAGE_ADD_N_VACATED(p, s) (AssertMacro(O_PAGE_IS(p, LEAF)), ((BTreePageHeader *)(p))->field2 += (s))
#define PAGE_SUB_N_VACATED(p, s) (AssertMacro(O_PAGE_IS(p, LEAF)), \
								  AssertMacro(((BTreePageHeader *)(p))->field2 >= (s)), \
								  ((BTreePageHeader *)(p))->field2 -= (s))

/* Header of non-leaf tuple */
typedef struct
{
	uint64		downlink;
} BTreeNonLeafTuphdr;

/* Header of leaf tuple  */
typedef struct
{
	OTupleXactInfo xactInfo:61,
				deleted:2,
				chainHasLocks:1;
	UndoLocation undoLocation:62,
				formatFlags:2;
} BTreeLeafTuphdr;

#define BTreeNonLeafTuphdrSize MAXALIGN(sizeof(BTreeNonLeafTuphdr))
#define BTreeLeafTuphdrSize MAXALIGN(sizeof(BTreeLeafTuphdr))

#define DOWNLINK_DISK_BIT				(UINT64CONST(1)<<63)
#define DOWNLINK_IO_BUF_MASK			(UINT64CONST(0xFFFFFFFF00000000))
#define DOWNLINK_GET_IN_MEMORY_BLKNO(downlink) ((uint32) (downlink))
#define DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(downlink) (((uint32) ((downlink) >> 32)) & 0x7FFFFFFF)
#define MAKE_IN_MEMORY_DOWNLINK(blkno, changeCount) ((uint64) (blkno) | ((uint64) (changeCount) << 32))
#define DOWNLINK_IS_IN_MEMORY(downlink) (((downlink) & DOWNLINK_DISK_BIT) == (uint64) 0)
#define DOWNLINK_IS_IN_IO(downlink) (((downlink) & DOWNLINK_IO_BUF_MASK) == DOWNLINK_IO_BUF_MASK)
#define DOWNLINK_IS_ON_DISK(downlink) (!DOWNLINK_IS_IN_MEMORY(downlink) && !DOWNLINK_IS_IN_IO(downlink))
#define MAKE_IO_DOWNLINK(locknum) ((uint64)(locknum) | DOWNLINK_IO_BUF_MASK)
#define DOWNLINK_GET_IO_LOCKNUM(downlink) ((uint32) ((downlink) & UINT64CONST(0xFFFFFFFF)))

#define MAKE_ON_DISK_DOWNLINK(extent) (((uint64)((extent).len) << 48) | (uint64)((extent).off) | DOWNLINK_DISK_BIT)
#define DOWNLINK_GET_DISK_OFF(downlink) ((uint64) ((downlink) & UINT64CONST(0xFFFFFFFFFFFF)))
#define DOWNLINK_GET_DISK_LEN(downlink) ((uint16) (((downlink) & UINT64CONST(0x7FFF000000000000)) >> 48))
#define InvalidDiskDownlink UINT64_MAX
#define DiskDownlinkIsValid(downlink) ((downlink) != InvalidDiskDownlink)

/* Macros for work with rightlink */
#define InvalidRightLink (0xFFFFFFFFFFFFFFFF)
#define RightLinkIsValid(rightLink) ((rightLink) != InvalidRightLink)
#define MAKE_IN_MEMORY_RIGHTLINK(blkno, changeCount) (MAKE_IN_MEMORY_DOWNLINK((blkno), (changeCount)))
#define RIGHTLINK_GET_BLKNO(rightLink) (DOWNLINK_GET_IN_MEMORY_BLKNO((rightLink)))
#define RIGHTLINK_GET_CHANGECOUNT(rightLink) (DOWNLINK_GET_IN_MEMORY_CHANGECOUNT((rightLink)))

/* Tuple and key max sizes */
#define O_BTREE_MAX_TUPLE_SIZE MAXALIGN_DOWN((ORIOLEDB_BLCKSZ - sizeof(BTreePageHeader)) / 3 - sizeof(LocationIndex) - BTreeLeafTuphdrSize)
#define O_BTREE_MAX_KEY_SIZE	O_BTREE_MAX_TUPLE_SIZE

typedef struct
{
	OTuple		tuple;
	char		fixedData[O_BTREE_MAX_TUPLE_SIZE];
} OFixedTuple;

typedef struct
{
	OTuple		tuple;
	char		fixedData[O_BTREE_MAX_KEY_SIZE];
} OFixedKey;

/*
 * Fixed structure for storage of B-tree key.  Separate key length field,
 * saves us from getting length of inconsistent key.
 */
typedef struct
{
	union
	{
		char		fixedData[O_BTREE_MAX_KEY_SIZE];
		void	   *p;			/* for alignment purposes */
	}			data;
	uint8		formatFlags;
	bool		notNull;
	int			len;
} OFixedShmemKey;

typedef enum ReadPageResult
{
	ReadPageResultOK,
	ReadPageResultWrongPageChangeCount,
	ReadPageResultFailed
} ReadPageResult;

extern bool o_btree_read_page(BTreeDescr *desc, OInMemoryBlkno blkno,
							  uint32 pageChangeCount, Page img, CommitSeqNo csn,
							  void *key, BTreeKeyType keyType,
							  OFixedKey *lokey, PartialPageState *partial,
							  UndoLocation *undoLocation, CommitSeqNo *readCsn);
extern ReadPageResult o_btree_try_read_page(BTreeDescr *desc,
											OInMemoryBlkno blkno,
											uint32 pageChangeCount, Page img,
											CommitSeqNo csn,
											Pointer key, BTreeKeyType keyType,
											PartialPageState *partial,
											CommitSeqNo *readCsn);
extern UndoLocation read_page_from_undo(BTreeDescr *desc, Page img,
										UndoLocation undo_loc,
										CommitSeqNo csn, void *key,
										BTreeKeyType keyType, OFixedKey *lokey);

extern void init_new_btree_page(BTreeDescr *desc, OInMemoryBlkno blkno,
								uint16 flags, uint16 level, bool noLock);
extern void init_meta_page(OInMemoryBlkno blkno, uint32 leafPagesNum);
extern LocationIndex page_get_vacated_space(BTreeDescr *desc, Page p,
											CommitSeqNo csn);
extern void null_unused_bytes(Page img);
extern void put_page_image(OInMemoryBlkno blkno, Page img);
extern void page_cut_first_key(Page node);

typedef struct ItemPointerData ItemPointerData;
extern ItemPointerData btree_ctid_get_and_inc(BTreeDescr *desc);
extern void btree_ctid_update_if_needed(BTreeDescr *desc, ItemPointerData ctid);

extern void copy_fixed_tuple(BTreeDescr *desc, OFixedTuple *dst, OTuple src);
extern void copy_fixed_key(BTreeDescr *desc, OFixedKey *dst, OTuple src);
extern void copy_fixed_page_key(BTreeDescr *desc, OFixedKey *dst,
								Page p, BTreePageItemLocator *loc);
extern void copy_fixed_hikey(BTreeDescr *desc, OFixedKey *dst, Page p);
extern void clear_fixed_tuple(OFixedTuple *dst);
extern void clear_fixed_key(OFixedKey *dst);

extern void copy_fixed_shmem_key(BTreeDescr *desc, OFixedShmemKey *dst,
								 OTuple src);
extern void copy_fixed_shmem_page_key(BTreeDescr *desc, OFixedShmemKey *dst,
									  Page p, BTreePageItemLocator *loc);
extern void copy_fixed_shmem_hikey(BTreeDescr *desc, OFixedShmemKey *dst,
								   Page p);
extern void clear_fixed_shmem_key(OFixedShmemKey *dst);
extern OTuple fixed_shmem_key_get_tuple(OFixedShmemKey *src);
extern void copy_from_fixed_shmem_key(OFixedKey *dst, OFixedShmemKey *src);

extern OTuple page_get_hikey(Page p);
extern int	page_get_hikey_size(Page p);
extern void page_set_hikey_flags(Page p, uint8 flags);
extern bool page_fits_hikey(Page p, LocationIndex newHikeySize);
extern void page_resize_hikey(Page p, LocationIndex newHikeySize);
extern void btree_page_update_max_key_len(BTreeDescr *desc, Page p);

#endif							/* __BTREE_PAGE_CONTENTS_H__ */
