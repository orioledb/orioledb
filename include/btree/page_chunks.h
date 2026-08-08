/*-------------------------------------------------------------------------
 *
 * page_chunks.h
 *		Declarations for routines dealing with OrioleDB page chunks.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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
} BTreePageItem;

extern bool partial_load_hikeys_chunk(PartialPageState *partial, Page img);
extern bool partial_load_full_page(PartialPageState *partial, Page img);
extern bool partial_load_chunk_impl(PartialPageState *partial, Page img,
									OffsetNumber chunkOffset,
									BTreePageItemLocator *loc,
									const char *file, int line);

/*
 * The already-loaded early return does not position the caller's locator, so
 * the call site is what a violation has to name.
 */
#define partial_load_chunk(partial, img, chunkOffset, loc) \
	partial_load_chunk_impl((partial), (img), (chunkOffset), (loc), \
							__FILE__, __LINE__)
extern BTreeItemPageFitType page_locator_fits_item(BTreeDescr *desc,
												   Page p,
												   BTreePageItemLocator *locator,
												   LocationIndex size,
												   bool replace,
												   CommitSeqNo csn);
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
							 OTuple hikey);
extern void split_page_by_chunks(BTreeDescr *desc, Page p);
extern bool page_locator_find_real_item(Page p, PartialPageState *partial,
										BTreePageItemLocator *locator);
extern OffsetNumber page_locator_get_offset(Page p, BTreePageItemLocator *locator);

/*
 * Assert that the item a locator points to lives in a chunk that was actually
 * loaded into the partial (FETCH-mode) image.  BTREE_PAGE_LOCATOR_IS_VALID()
 * ignores the page entirely -- it only looks at the locator's own cached chunk
 * pointer and item count -- so a locator can happily address a region of the
 * image which no partial_load_chunk() ever filled, and the "tuple" read from
 * there is whatever the buffer happened to contain (typically another chunk's
 * item offset array).  Use this right before reading a tuple through a locator
 * in the places where the matching PartialPageState is at hand: it is silent in
 * IMAGE mode and on correctly loaded chunks, and fires exactly on the
 * "unloaded chunk got used" case.
 */
#ifdef USE_ASSERT_CHECKING
extern void assert_partial_chunk_loaded(PartialPageState *partial, Page img,
										BTreePageItemLocator *locator,
										const char *file, int line);
#define ASSERT_CHUNK_LOADED(partial, img, locator) \
	assert_partial_chunk_loaded((partial), (img), (locator), __FILE__, __LINE__)
#else
#define ASSERT_CHUNK_LOADED(partial, img, locator) ((void) 0)
#endif

#endif							/* __BTREE_PAGE_CHUNKS_H__ */
