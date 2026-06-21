/*-------------------------------------------------------------------------
 *
 * find.h
 *		Declarations for finding page in orioledb B-tree.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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
	bool		hikeysChunkIsLoaded;
	bool		chunkIsLoaded[BTREE_PAGE_MAX_CHUNKS];
};

typedef struct
{
	BTreeDescr *desc;
	Pointer		img;
	Pointer		parentImg;
	char		imgData[ORIOLEDB_BLCKSZ];
	char		parentImgData[ORIOLEDB_BLCKSZ];

	/*
	 * Partial read state of the target (leaf) page in context->img.  Used in
	 * FETCH mode (the leaf is read partially); in IMAGE mode the leaf is read
	 * in full and this is unused.
	 */
	PartialPageState partial;

	/*
	 * Partial read state of the immediate parent page in context->parentImg.
	 * Used in both IMAGE and FETCH modes so
	 * find_left_page()/find_right_page() can navigate to sibling pages
	 * through the parent's downlinks.
	 */
	PartialPageState parentPartial;
	CommitSeqNo csn;
	CommitSeqNo imgReadCsn;
	UndoLocation imgUndoLoc;
	int			index;
	OBtreePageFindItem items[ORIOLEDB_MAX_DEPTH];
	OTupleXactInfo insertXactInfo;
	OTuple		insertTuple;

	/*
	 * When BTREE_PAGE_FIND_LOKEY_SIBLING is not set, then lokey contains
	 * hikey of left sibling of parent.  Otherwise, contain hikey of left
	 * sibling.
	 */
	OFixedKey	lokey;

	/*
	 * Stable copy of the current page's own lokey (its downlink key in the
	 * immediate parent), captured during find_page() descent and refreshed by
	 * find_left_page() when stepping through parent downlinks.  Used instead
	 * of re-reading the parent image, which is unreliable in FETCH mode where
	 * parentImg is partial and may be reclaimed under page-pool pressure.
	 * Only meaningful when the current page's downlink is not the parent's
	 * first one (otherwise btree_find_context_lokey() falls back to lokey).
	 */
	OFixedKey	leafLokey;

	/*
	 * Helps to avoid overwriting of a hikey by non-consistency read of the
	 * image from undo log.
	 *
	 * BTREE_PAGE_FIND_LOKEY_UNDO is set when present.
	 */
	OFixedKey	undoLokey;

	/*
	 * A forward (non-KEEP_LOKEY) iterator descent that locates the immediate
	 * parent via the fastpath defers materializing it into parentImg until
	 * find_right_page() actually steps to a sibling.  While this is set the
	 * parent's locator still points into the shared page and
	 * parentImg/parentPartial are not yet valid; find_right_page() calls
	 * convert_fastpath_parent_to_img() on demand.  A backward (KEEP_LOKEY)
	 * descent reads the parent's lokey during the descent and so materializes
	 * the parent eagerly, leaving this false.
	 */
	bool		parentImgDeferred;
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
/*
 * The caller navigates to sibling pages with find_left_page()/find_right_page()
 * and therefore needs the parent materialized in context->parentImg.  This is
 * incompatible with the fastpath downlink search, which reads the downlink
 * straight from the shared page, so it disables that optimization.
 */
#define BTREE_PAGE_FIND_KEEP_PARENT		(0x1000)

#define BTREE_PAGE_FIND_SET(context, flag) ((context)->flags |= BTREE_PAGE_FIND_##flag)
#define BTREE_PAGE_FIND_UNSET(context, flag) ((context)->flags &= ~(BTREE_PAGE_FIND_##flag))
#define BTREE_PAGE_FIND_IS(context, flag) (((context)->flags & BTREE_PAGE_FIND_##flag)? true : false)

typedef enum
{
	OFindPageResultSuccess,
	OFindPageResultFailure,
	OFindPageResultInserted
} OFindPageResult;

extern bool btree_page_search(BTreeDescr *desc, Page p, Pointer key,
							  BTreeKeyType keyType, PartialPageState *partial,
							  BTreePageItemLocator *locator);
extern void init_page_find_context(OBTreeFindPageContext *context,
								   BTreeDescr *desc,
								   CommitSeqNo csn, uint16 flags);

extern OFindPageResult find_page(OBTreeFindPageContext *context, void *key,
								 BTreeKeyType keyType, uint16 targetLevel);
extern OFindPageResult refind_page(OBTreeFindPageContext *context, void *key,
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
