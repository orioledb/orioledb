/*-------------------------------------------------------------------------
 *
 * find.h
 *		Declarations for finding page in orioledb B-tree.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/find.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_FIND_H__
#define __BTREE_FIND_H__

#include "btree.h"
#include "btree/page_contents.h"

typedef struct
{
	OInMemoryBlkno blkno;
	uint32		pageChangeCount;
	BTreePageItemLocator locator;
} OBtreePageFindItem;

struct PartialPageState
{
	Page		src;
	bool		isPartial;
	bool		chunkIsLoaded[BTREE_PAGE_MAX_CHUNKS];
};

typedef struct
{
	BTreeDescr *desc;
	char		img[ORIOLEDB_BLCKSZ];
	char		parentImg[ORIOLEDB_BLCKSZ];
	PartialPageState partial;
	CommitSeqNo csn;
	CommitSeqNo imgReadCsn;
	UndoLocation imgUndoLoc;
	int			index;
	OBtreePageFindItem items[ORIOLEDB_MAX_DEPTH];

	/*
	 * When BTREE_PAGE_FIND_LOKEY_SIBLING is not set, then lokey contains
	 * hikey of left sibling of parent.  Otherwise, contain hikey of left
	 * sibling.
	 */
	OFixedKey	lokey;

	/*
	 * Helps to avoid overwriting of a hikey by non-consistency read of the
	 * image from undo log.
	 *
	 * BTREE_PAGE_FIND_LOKEY_UNDO is set when present.
	 */
	OFixedKey	undoLokey;
	uint16		flags;
} OBTreeFindPageContext;

/* OBTreeFindPageContext flags */
#define BTREE_PAGE_FIND_KEEP_LOKEY		(0x0001)
#define BTREE_PAGE_FIND_LOKEY_EXISTS	(0x0002)
#define BTREE_PAGE_FIND_LOKEY_SIBLING	(0x0004)
#define BTREE_PAGE_FIND_LOKEY_UNDO		(0x0008)
#define BTREE_PAGE_FIND_TRY_LOCK		(0x0010)
#define BTREE_PAGE_FIND_FIX_LEAF_SPLIT	(0x0020)
#define BTREE_PAGE_FIND_NO_FIX_SPLIT	(0x0040)
#define BTREE_PAGE_FIND_MODIFY			(0x0080)
#define BTREE_PAGE_FIND_FETCH			(0x0100)
#define BTREE_PAGE_FIND_IMAGE			(0x0200)
#define BTREE_PAGE_FIND_DOWNLINK_LOCATION (0x0400)
#define BTREE_PAGE_FIND_READ_CSN		(0x0800)

#define BTREE_PAGE_FIND_SET(context, flag) ((context)->flags |= BTREE_PAGE_FIND_##flag)
#define BTREE_PAGE_FIND_UNSET(context, flag) ((context)->flags &= ~(BTREE_PAGE_FIND_##flag))
#define BTREE_PAGE_FIND_IS(context, flag) (((context)->flags & BTREE_PAGE_FIND_##flag)? true : false)

extern bool btree_page_search(BTreeDescr *desc, Page p, Pointer key,
							  BTreeKeyType keyType, PartialPageState *partial,
							  BTreePageItemLocator *locator);
extern void init_page_find_context(OBTreeFindPageContext *context,
								   BTreeDescr *desc,
								   CommitSeqNo csn, uint16 flags);

extern bool find_page(OBTreeFindPageContext *context, void *key,
					  BTreeKeyType keyType, uint16 targetLevel);
extern bool refind_page(OBTreeFindPageContext *context, void *key,
						BTreeKeyType keyType, uint16 level,
						OInMemoryBlkno blkno, uint32 pageChangeCount);

extern bool find_right_page(OBTreeFindPageContext *context, OFixedKey *hikey);
extern bool find_left_page(OBTreeFindPageContext *context, OFixedKey *hikey);
extern OTuple btree_find_context_lokey(OBTreeFindPageContext *context);
extern void btree_find_context_from_modify_to_read(OBTreeFindPageContext *context,
												   Pointer key,
												   BTreeKeyType keyType,
												   uint16 level);

#endif							/* __BTREE_FIND_H__ */
