/*-------------------------------------------------------------------------
 *
 * page_chunks.h
 *		Declarations for routined dealing with OrioleDB page chunks.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/page_chunks.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_PAGE_CHUNKS_H__
#define __BTREE_PAGE_CHUNKS_H__

#include "btree/page_contents.h"

typedef enum BTreeItemPageFitType
{
	BTreeItemPageFitAsIs,
	BTreeItemPageFitCompactRequired,
	BTreeItemPageFitSplitRequired
} BTreeItemPageFitType;

typedef struct
{
	Pointer		data;
	LocationIndex size;
	uint8		flags;
	bool		newItem;
} BTreePageItem;

extern bool partial_load_chunk(PartialPageState *partial, Page img,
							   OffsetNumber chunkOffset);
extern BTreeItemPageFitType page_locator_fits_item(BTreeDescr *desc,
												   Page p,
												   BTreePageItemLocator *locator,
												   LocationIndex size,
												   bool replace,
												   CommitSeqNo csn);
extern void perform_page_compaction(BTreeDescr *desc, OInMemoryBlkno blkno,
									BTreePageItemLocator *loc,
									OTuple tuple, LocationIndex tuplesize, bool replace);
extern void o_btree_page_calculate_statistics(BTreeDescr *desc, Pointer p);
extern void init_page_first_chunk(BTreeDescr *desc, Page p,
								  LocationIndex hikeySize);
extern void page_chunk_fill_locator(Page p, OffsetNumber chunkOffset,
									BTreePageItemLocator *locator);
extern void page_item_fill_locator(Page p, OffsetNumber itemOffset,
								   BTreePageItemLocator *locator);
extern void page_item_fill_locator_backwards(Page p, OffsetNumber itemOffset,
											 BTreePageItemLocator *locator);
extern bool page_locator_next_chunk(Page p, BTreePageItemLocator *locator);
extern bool page_locator_prev_chunk(Page p, BTreePageItemLocator *locator);
extern void page_locator_insert_item(Page p, BTreePageItemLocator *locator,
									 LocationIndex itemsize);
extern bool page_locator_fits_new_item(Page p, BTreePageItemLocator *locator,
									   LocationIndex itemsize);
extern LocationIndex page_locator_get_item_size(Page p,
												BTreePageItemLocator *locator);
extern void page_locator_resize_item(Page p, BTreePageItemLocator *locator,
									 LocationIndex newsize);
extern void page_locator_delete_item(Page p, BTreePageItemLocator *locator);
extern void page_split_chunk_if_needed(BTreeDescr *desc, Page p,
									   BTreePageItemLocator *locator);
extern void btree_page_reorg(BTreeDescr *desc, Page p, BTreePageItem *items,
							 OffsetNumber count, LocationIndex hikeySize,
							 OTuple hikey, BTreePageItemLocator *newLoc);
extern void split_page_by_chunks(BTreeDescr *desc, Page p);
extern bool page_locator_find_real_item(Page p, PartialPageState *partial,
										BTreePageItemLocator *locator);
extern OffsetNumber page_locator_get_offset(Page p, BTreePageItemLocator *locator);

#endif							/* __BTREE_PAGE_CHUNKS_H__ */
