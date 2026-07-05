/*-------------------------------------------------------------------------
 *
 * find.c
 *		Routines for finding appropriate page in B-tree.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/find.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/fastpath.h"
#include "btree/find.h"
#include "btree/insert.h"
#include "btree/io.h"
#include "btree/page_chunks.h"
#include "tableam/descr.h"
#include "utils/stopevent.h"

#include "access/transam.h"

typedef struct
{
	OBTreeFindPageContext *context;
	void	   *key;
	BTreeKeyType keyType;
	Page		pagePtr;
	int			targetLevel;
	OInMemoryBlkno blkno;
	uint32		pageChangeCount;
	PartialPageState *partial;
	bool		haveLock;
	bool		inserted;
	bool		tryLockFailed;
} OBTreeFindPageInternalContext;

static bool follow_rightlink(OBTreeFindPageInternalContext *intCxt);
static void step_upward_level(OBTreeFindPageInternalContext *intCxt);
static bool btree_find_read_page(OBTreeFindPageContext *context,
								 OInMemoryBlkno blkno, uint32 pageChangeCount,
								 bool parent, void *key, BTreeKeyType keyType,
								 PartialPageState *partial,
								 bool loadHikeysChunk);
static ReadPageResult btree_find_try_read_page(OBTreeFindPageContext *context,
											   OInMemoryBlkno blkno,
											   uint32 pageChangeCount, bool parent,
											   void *key, BTreeKeyType keyType,
											   PartialPageState *partial,
											   bool loadHikeysChunk);

static OffsetNumber btree_page_binary_search_chunks(BTreeDescr *desc, Page p,
													Pointer key,
													BTreeKeyType keyType);
static void btree_page_search_items(BTreeDescr *desc, Page p, Pointer key,
									BTreeKeyType keyType,
									BTreePageItemLocator *locator);
static void refresh_parent_img_chunk(OBTreeFindPageInternalContext *intCxt);
static bool convert_fastpath_parent_to_img(OBTreeFindPageContext *context,
										   BTreePageItemLocator *locator);

/*
 * A parent locator that find_left_page()/find_right_page() will navigate must
 * point into context->parentImg (or be NULL).  Assert that at every place we
 * record or advance such a locator, so a stray shared-page/img pointer is
 * caught at its producer rather than only when the sibling step consumes it.
 */
#define ASSERT_PARENT_LOCATOR_LOCAL(context, loc) \
	Assert((loc).chunk == NULL || \
		   ((Pointer) (loc).chunk >= (context)->parentImg && \
			(Pointer) (loc).chunk < (context)->parentImg + ORIOLEDB_BLCKSZ))

/*
 * Initialize B-tree page find context.
 */
void
init_page_find_context(OBTreeFindPageContext *context, BTreeDescr *desc,
					   CommitSeqNo csn, uint16 flags)
{
	ASAN_UNPOISON_MEMORY_REGION(context, sizeof(*context));
	context->partial.isPartial = false;
	context->desc = desc;
	context->csn = csn;
	context->index = 0;
	context->flags = flags;
	context->imgUndoLoc = InvalidUndoLocation;
	context->img = NULL;
	context->parentImg = NULL;
	O_TUPLE_SET_NULL(context->insertTuple);
	O_TUPLE_SET_NULL(context->lokey.tuple);
}



static OBTreeFastPathFindResult
page_find_downlink(OBTreeFindPageInternalContext *intCxt,
				   FastpathFindDownlinkMeta *meta,
				   int level,
				   bool fastPathDownlink,
				   BTreePageItemLocator *loc,
				   BTreeNonLeafTuphdr **tuphdr)
{
	OBTreeFindPageContext *context = intCxt->context;
	BTreeDescr *desc = context->desc;
	void	   *key = intCxt->key;
	BTreeKeyType keyType = intCxt->keyType;
	bool		itemFound = true;

	if (fastPathDownlink)
	{
		OBTreeFastPathFindResult result;

		result = fastpath_find_downlink(intCxt->pagePtr, intCxt->blkno,
										meta, loc, tuphdr);

		if (result != OBTreeFastPathFindSlowpath)
			return result;
	}

	if (intCxt->partial &&
		intCxt->partial->isPartial &&
		!intCxt->partial->hikeysChunkIsLoaded)
	{
		if (!partial_load_hikeys_chunk(intCxt->partial, intCxt->pagePtr))
			return OBTreeFastPathFindRetry;
	}

	/*
	 * BTreeKeyNone requests leftmost page.  Otherwise, consider following the
	 * rightlink.
	 */
	if (keyType != BTreeKeyNone)
	{
		if (follow_rightlink(intCxt))
		{
			if (intCxt->tryLockFailed)
				return OBTreeFastPathFindFailure;
			if (intCxt->inserted)
				return OBTreeFastPathFindFailure;
			Assert(context->index > 0);
			Assert(!intCxt->haveLock);
			step_upward_level(intCxt);
			return OBTreeFastPathFindRetry;
		}
	}

	/*
	 * Choose the appropriate downlink for further search.
	 */
	if (keyType == BTreeKeyRightmost)
		BTREE_PAGE_LOCATOR_LAST(intCxt->pagePtr, loc);
	else if (keyType == BTreeKeyNone)
		BTREE_PAGE_LOCATOR_FIRST(intCxt->pagePtr, loc);
	else
	{
		Assert(key);
		/* Have to do the binary search otherwise */
		itemFound = btree_page_search(desc, intCxt->pagePtr, key, keyType,
									  intCxt->partial, loc);
		if (itemFound)
		{
			BTREE_PAGE_LOCATOR_PREV(intCxt->pagePtr, loc);
			if (intCxt->partial)
				itemFound = partial_load_chunk(intCxt->partial,
											   intCxt->pagePtr,
											   loc->chunkOffset,
											   NULL);
		}
	}

	if (intCxt->partial)
	{
		if (!itemFound || !partial_load_chunk(intCxt->partial,
											  intCxt->pagePtr,
											  loc->chunkOffset,
											  NULL))
		{
			Assert(!intCxt->haveLock);
			if (BTREE_PAGE_FIND_IS(context, TRY_LOCK))
				return OBTreeFastPathFindFailure;
			return OBTreeFastPathFindRetry;
		}

		if (BTREE_PAGE_FIND_IS(context, IMAGE) &&
			level == intCxt->targetLevel + 1 &&
			BTREE_PAGE_FIND_IS(context, KEEP_LOKEY))
		{
			/*
			 * We may need to load another one tuple for a backward iteration.
			 */
			if (loc->itemOffset == 0 && loc->chunkOffset > 0 &&
				!partial_load_chunk(intCxt->partial, intCxt->pagePtr,
									loc->chunkOffset - 1, NULL))
			{
				Assert(!intCxt->haveLock);
				return OBTreeFastPathFindRetry;
			}
		}
	}

	*tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(intCxt->pagePtr, loc);

	return OBTreeFastPathFindOK;
}

static OBTreeFastPathFindResult
page_find_item(OBTreeFindPageInternalContext *intCxt,
			   FastpathFindDownlinkMeta *meta,
			   int level,
			   bool fastpath,
			   BTreePageItemLocator *loc,
			   BTreeNonLeafTuphdr **tuphdr)
{
	OBTreeFindPageContext *context = intCxt->context;
	BTreeDescr *desc = context->desc;
	void	   *key = intCxt->key;
	BTreeKeyType keyType = intCxt->keyType;
	bool		itemFound = true;

	if (fastpath && intCxt->partial->isPartial)
	{
		OBTreeFastPathFindResult result;
		int			chunkIndex;

		Assert(!BTREE_PAGE_FIND_IS(context, MODIFY));

		result = fastpath_find_chunk(intCxt->pagePtr,
									 intCxt->blkno,
									 meta,
									 &chunkIndex);

		if (result == OBTreeFastPathFindOK &&
			!partial_load_chunk(intCxt->partial,
								intCxt->pagePtr,
								chunkIndex,
								loc))
			result = OBTreeFastPathFindRetry;

		if (result == OBTreeFastPathFindOK)
		{
			if (keyType == BTreeKeyRightmost)
			{
				loc->itemOffset = loc->chunkItemsCount - 1;
			}
			else if (keyType == BTreeKeyNone)
			{
				loc->itemOffset = 0;
			}
			else
			{
				btree_page_search_items(desc, intCxt->pagePtr,
										key, keyType, loc);
			}

			if (page_locator_find_real_item(intCxt->pagePtr,
											intCxt->partial,
											loc))
			{
				if (level > 0)
					*tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(intCxt->pagePtr, loc);

				return OBTreeFastPathFindOK;
			}
			else
			{
				result = OBTreeFastPathFindRetry;
			}
		}

		if (result == OBTreeFastPathFindRetry)
		{
			/*
			 * Can not read partial page, it happens if the pages was
			 * concurrently changed. But it should not happen under the
			 * lock_page().
			 */
			Assert(!intCxt->haveLock);
			if (BTREE_PAGE_FIND_IS(context, TRY_LOCK))
				return OBTreeFastPathFindFailure;
			return OBTreeFastPathFindRetry;
		}
		else if (result == OBTreeFastPathFindFailure)
		{
			return OBTreeFastPathFindFailure;
		}
		Assert(result == OBTreeFastPathFindSlowpath);
	}

	if (intCxt->partial &&
		intCxt->partial->isPartial &&
		!intCxt->partial->hikeysChunkIsLoaded)
	{
		if (!partial_load_hikeys_chunk(intCxt->partial, intCxt->pagePtr))
			return OBTreeFastPathFindRetry;
	}

	/*
	 * BTreeKeyNone requests leftmost page.  Otherwise, consider following the
	 * rightlink.
	 */
	if (keyType != BTreeKeyNone)
	{
		if (follow_rightlink(intCxt))
		{
			if (intCxt->tryLockFailed)
				return OBTreeFastPathFindFailure;
			Assert(context->index > 0);
			Assert(!intCxt->haveLock);
			step_upward_level(intCxt);
			return OBTreeFastPathFindRetry;
		}
	}

	/*
	 * Choose the appropriate downlink for further search.
	 */
	if (keyType == BTreeKeyRightmost)
		BTREE_PAGE_LOCATOR_LAST(intCxt->pagePtr, loc);
	else if (keyType == BTreeKeyNone)
		BTREE_PAGE_LOCATOR_FIRST(intCxt->pagePtr, loc);
	else
	{
		Assert(key);
		/* Have to do the binary search otherwise */
		itemFound = btree_page_search(desc, intCxt->pagePtr,
									  key, keyType,
									  intCxt->partial, loc);
		if (itemFound && !BTREE_PAGE_FIND_IS(context, MODIFY))
			itemFound = page_locator_find_real_item(intCxt->pagePtr,
													intCxt->partial,
													loc);
	}

	if (intCxt->partial &&
		(!itemFound || !partial_load_chunk(intCxt->partial,
										   intCxt->pagePtr,
										   loc->chunkOffset,
										   NULL)))
	{
		/*
		 * Can not read partial page, it happens if the pages was concurrently
		 * changed. But it should not happen under the lock_page().
		 */
		Assert(!intCxt->haveLock);
		if (BTREE_PAGE_FIND_IS(context, TRY_LOCK))
			return OBTreeFastPathFindFailure;
		return OBTreeFastPathFindRetry;
	}

	if (level > 0)
		*tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(intCxt->pagePtr, loc);

	return OBTreeFastPathFindOK;
}

/*
 * Refresh context->parentImg from the locked shared-memory page held
 * by `intCxt` and rebind the current locator's chunk onto parentImg.
 * Used by find_page at level == targetLevel + 1 in IMAGE mode when
 * intCxt->pagePtr is the real shared-memory page (not parentImg):
 * without this rebind, the iterator's later find_right_page /
 * find_left_page would navigate through a chunk pointer into a page
 * the descent has already unlocked.
 *
 * Only the page header (with hikeys) and the chunk that the locator
 * currently references are copied; the partial state is set up so
 * other chunks can be loaded on demand by partial_load_chunk if
 * find_right_page / find_left_page later visit them.  The standard
 * consistency check in partial_load_chunk then falls through to a
 * find_page re-descent if the source has been concurrently mutated.
 */
static void
refresh_parent_img_chunk(OBTreeFindPageInternalContext *intCxt)
{
	OBTreeFindPageContext *context = intCxt->context;
	Pointer		src = intCxt->pagePtr;
	BTreePageItemLocator *locator = &context->items[context->index].locator;
	BTreePageHeader *hdr = (BTreePageHeader *) src;
	OffsetNumber chunkOffset = locator->chunkOffset;
	LocationIndex chunkBegin;
	LocationIndex chunkEnd;

	chunkBegin = SHORT_GET_LOCATION(hdr->chunkDesc[chunkOffset].shortLocation);
	if (chunkOffset + 1 < hdr->chunksCount)
		chunkEnd = SHORT_GET_LOCATION(hdr->chunkDesc[chunkOffset + 1].shortLocation);
	else
		chunkEnd = hdr->dataSize;

	/* Header including the hikeys chunk. */
	memcpy(context->parentImg, src, hdr->hikeysEnd);
	/* The single chunk that `locator` references. */
	memcpy(context->parentImg + chunkBegin,
		   src + chunkBegin,
		   chunkEnd - chunkBegin);

	context->parentPartial.src = src;
	context->parentPartial.isPartial = true;
	context->parentPartial.hikeysChunkIsLoaded = true;
	memset(context->parentPartial.chunkIsLoaded, 0,
		   sizeof(context->parentPartial.chunkIsLoaded));
	context->parentPartial.chunkIsLoaded[chunkOffset] = true;

	locator->chunk =
		(BTreePageChunk *) (context->parentImg + chunkBegin);
}

/*
 * The fastpath downlink search positions the locator straight onto the shared
 * page (it skips loading the hikeys chunk, so parentImg holds only the base
 * header that the descent's partial read already copied there).  Callers that
 * step to siblings (KEEP_PARENT) need the parent fully navigable in parentImg,
 * so finish that partial read on top of the *existing* snapshot: load the
 * hikeys chunk (the chunk-descriptor array) and the chunk holding the downlink
 * into parentImg through the already-set-up context->parentPartial, then rebind
 * the locator onto parentImg.
 *
 * We deliberately do NOT re-read the page from scratch (o_btree_read_page()):
 * the base header was snapshotted during the descent, and re-snapshotting would
 * capture a possibly newer page version, leaving parentImg inconsistent with
 * the downlink the fastpath already chose off that snapshot.  Both loads
 * validate against the snapshot's change count (state bits + pageChangeCount),
 * so a parent that changed or was evicted/reused under us makes them fail and
 * the caller re-finds.  partial_load_chunk() positions the locator at item 0,
 * so the caller's real itemOffset is restored afterwards.
 *
 * Returns false if the parent changed under us; the caller must re-find.
 */
static bool
convert_fastpath_parent_to_img(OBTreeFindPageContext *context,
							   BTreePageItemLocator *locator)
{
	OffsetNumber chunkOffset = locator->chunkOffset;
	OffsetNumber itemOffset = locator->itemOffset;

	if (!partial_load_hikeys_chunk(&context->parentPartial, context->parentImg))
		return false;

	if (!partial_load_chunk(&context->parentPartial, context->parentImg,
							chunkOffset, locator))
		return false;

	locator->itemOffset = itemOffset;
	return true;
}

/*--
 * Locate page and location within it for given key
 *
 * - context - context of parent pages
 * - key - key/tuple for search (NULL for the leftmost page)
 * - keyType - type of the key
 * - targetLevel - target page targetLevel to find
 *
 * For better efficiency on large pages we use partial approach for page read
 * from the shared memory. We have 3 alternative types of the call
 * depending on context->flags:
 *
 * 1. BTREE_PAGE_FIND_FETCH - fetches a single tuple. It uses partial read for
 * all pages.
 *
 * 2. BTREE_PAGE_FIND_MODIFY - find the page for modification. It uses partial read
 * for all parent pages, call lock_page() on a target page and search a tuple
 * on the target page in the shared memory.
 *
 * 3. BTREE_PAGE_FIND_IMAGE - copy a target leaf(!) to context->img. It useful
 * for iteration through the page. Reads parent pages partial and then
 * memcpy() a leaf page to the context.image. It holds lokey
 * if BTREE_PAGE_FIND_KEEP_LOKEY is set.
 */
OFindPageResult
find_page(OBTreeFindPageContext *context, void *key, BTreeKeyType keyType,
		  uint16 targetLevel)
{
	BTreeDescr *desc = context->desc;
	OBTreeFindPageInternalContext intCxt;
	BTreePageItemLocator loc;
	bool		needLock = false,
				fetchFlag = BTREE_PAGE_FIND_IS(context, FETCH),
				modifyFlag = BTREE_PAGE_FIND_IS(context, MODIFY),
				imageFlag = BTREE_PAGE_FIND_IS(context, IMAGE),
				tryFlag = BTREE_PAGE_FIND_IS(context, TRY_LOCK),
				fixLeafFlag = BTREE_PAGE_FIND_IS(context, FIX_LEAF_SPLIT),
				noFixFlag PG_USED_FOR_ASSERTS_ONLY = BTREE_PAGE_FIND_IS(context, NO_FIX_SPLIT),
				keepLokeyFlag = BTREE_PAGE_FIND_IS(context, KEEP_LOKEY),
				keepParentFlag = BTREE_PAGE_FIND_IS(context, KEEP_PARENT),
				downlinkLocationFlag = BTREE_PAGE_FIND_IS(context, DOWNLINK_LOCATION);
	bool		shmemIsReloaded = false;
	bool		loadHikeys;
	FastpathFindDownlinkMeta fastpathMeta;
	Jsonb	   *params = NULL;

	memset(&intCxt, 0, sizeof(intCxt));
	ASAN_UNPOISON_MEMORY_REGION(&intCxt, sizeof(intCxt));
	intCxt.context = context;
	intCxt.key = key;
	intCxt.keyType = keyType;
	intCxt.targetLevel = targetLevel;
	intCxt.inserted = false;
	context->parentImgDeferred = false;


	ASAN_UNPOISON_MEMORY_REGION(&fastpathMeta, sizeof(fastpathMeta));
	if (STOPEVENTS_ENABLED())
		fastpathMeta.enabled = false;
	else
		can_fastpath_find_downlink(context, key, keyType, &fastpathMeta);

	/*
	 * See description of the function.
	 */
	Assert((imageFlag && (targetLevel <= ORIOLEDB_MAX_DEPTH) && !fetchFlag && !modifyFlag)
		   || (imageFlag && targetLevel == 0 && !fetchFlag && modifyFlag)
		   || (!imageFlag && fetchFlag && !modifyFlag)
		   || (!imageFlag && !fetchFlag && modifyFlag && !keepLokeyFlag));
	Assert(!(COMMITSEQNO_IS_NORMAL(context->csn) && modifyFlag));

	/* resets the context before start */
	if (BTREE_PAGE_FIND_IS(context, KEEP_LOKEY))
	{
		BTREE_PAGE_FIND_UNSET(context, LOKEY_EXISTS);
		BTREE_PAGE_FIND_UNSET(context, LOKEY_SIBLING);
		BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
	}
	context->imgUndoLoc = InvalidUndoLocation;
	context->partial.isPartial = false;
	context->parentPartial.isPartial = false;
	context->index = 0;

	if (!tryFlag)
	{
		o_btree_load_shmem(desc);
	}
	else
	{
		if (!o_btree_try_use_shmem(desc))
			return OFindPageResultFailure;
	}
	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));

	/* starts from the rootPageBlkno */
	intCxt.blkno = desc->rootInfo.rootPageBlkno;
	intCxt.pageChangeCount = desc->rootInfo.rootPageChangeCount;
	while (true)
	{
		BTreeNonLeafTuphdr *nonLeafHdr = NULL;
		int			level;
		OInMemoryBlkno parentBlkno;
		bool		wrongChangeCount = false;
		Pointer		p;
		bool		fastpath;

		/*
		 * Local-pool slots are NULLed on eviction, unlike shared-pool slots
		 * whose shmem page stays readable (only pageChangeCount changes).  An
		 * IN_MEMORY downlink we just descended through may reference a slot
		 * the backend evicted earlier in the same call chain -- e.g. a
		 * reserve_page triggered during a seq scan, or a find_page invoked
		 * from an undo callback.  PAGE_GET_LEVEL below would segfault on the
		 * NULL slot, so step back to the parent and re-resolve the downlink
		 * (it now points to disk).  At the root there is no parent, so report
		 * failure.
		 */
		if (O_PAGE_IS_LOCAL(intCxt.blkno) &&
			local_ppool_pages[intCxt.blkno & O_BLKNO_MASK] == NULL)
		{
			if (context->index == 0)
				return OFindPageResultFailure;
			step_upward_level(&intCxt);
			continue;
		}

		p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		level = PAGE_GET_LEVEL(p);

		fastpath = fastpathMeta.enabled && !needLock;
		fastpath = fastpath && (keyType != BTreeKeyPageHiKey || level > 0);

		intCxt.partial = NULL;

		/*
		 * The leaf's partial state is re-initialized by each page read, so it
		 * is safe to reset here unconditionally.  The parent's partial state
		 * lives in context->parentPartial and is preserved across the leaf
		 * read so find_left_page()/find_right_page() can navigate siblings
		 * via the parent.
		 */
		context->partial.isPartial = false;

		if (needLock || (modifyFlag && level == targetLevel))
		{
			if (tryFlag)
			{
				if (!try_lock_page(intCxt.blkno))
					return OFindPageResultFailure;
				intCxt.pagePtr = p;
				intCxt.haveLock = true;
				needLock = false;
			}
			else if (!O_TUPLE_IS_NULL(context->insertTuple))
			{
				OLockPageWithTupleResult result;

				result = lock_page_with_tuple(desc,
											  &intCxt.blkno,
											  &intCxt.pageChangeCount,
											  context->insertXactInfo,
											  context->insertTuple);

				if (result == OLockPageWithTupleResultLocked)
				{
					p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
					intCxt.pagePtr = p;
					intCxt.haveLock = true;
					needLock = false;
				}
				else if (result == OLockPageWithTupleResultInserted)
				{
					return OFindPageResultInserted;
				}
				else
				{
					Assert(result == OLockPageWithTupleResultRefindNeeded);
					wrongChangeCount = true;
				}
			}
			else
			{
				lock_page(intCxt.blkno);
				intCxt.pagePtr = p;
				intCxt.haveLock = true;
				needLock = false;
			}
		}
		else
		{
			bool		useParentImg = false;

			if (imageFlag || fetchFlag)
			{
				/*
				 * In both BTREE_PAGE_FIND_IMAGE and BTREE_PAGE_FIND_FETCH we
				 * read upper non-leaf (parent) pages partially to
				 * context->parentImg using context->parentPartial, so
				 * find_left_page()/find_right_page() can navigate to sibling
				 * pages through the parent's downlinks.
				 *
				 * The target (leaf) page is read to context->img: in full for
				 * IMAGE (cheaper to iterate the whole page in memory) and
				 * partially for FETCH (cheaper when only part of the page is
				 * actually read).
				 *
				 * We consider it's OK to return page of lower targetLevel
				 * than required, if tree doesn't have enough height.  That's
				 * suitable for sequential scan (see btree_scan.c).
				 */
				if (level <= targetLevel)
				{
					useParentImg = false;
					if (fetchFlag)
					{
						intCxt.partial = &context->partial;
					}
					else
					{
						intCxt.partial = NULL;
						Assert(!fastpath);
					}
				}
				else
				{
					useParentImg = true;
					intCxt.partial = &context->parentPartial;
				}
			}
			else
			{
				/*
				 * BTREE_PAGE_FIND_MODIFY: parent pages are read partially to
				 * context->img; the target page is locked above.
				 */
				useParentImg = false;
				intCxt.partial = &context->partial;
			}

			intCxt.haveLock = false;

			/*
			 * The fastpath skips loading the hikeys chunk.  That is fine for
			 * a single-tuple search; a sibling-navigating caller
			 * (KEEP_PARENT, the iterator) loads the hikeys chunk on demand
			 * only when it needs it -- when stepping to a sibling, or when
			 * crossing a chunk boundary within a leaf (the chunk-descriptor
			 * array lives in the hikeys chunk).  So it does not need the
			 * hikeys chunk in the image here either.
			 */
			loadHikeys = !fastpath;

			if (tryFlag)
			{
				ReadPageResult result;

				result = btree_find_try_read_page(context, intCxt.blkno,
												  intCxt.pageChangeCount,
												  useParentImg,
												  key, keyType,
												  intCxt.partial,
												  loadHikeys);
				intCxt.pagePtr = useParentImg ? context->parentImg : context->img;
				if (result == ReadPageResultWrongPageChangeCount)
				{
					wrongChangeCount = true;
				}
				else if (result == ReadPageResultFailed)
				{
					return OFindPageResultFailure;
				}
			}
			else
			{
				bool		result;

				result = btree_find_read_page(context, intCxt.blkno,
											  intCxt.pageChangeCount,
											  useParentImg, key, keyType,
											  intCxt.partial,
											  loadHikeys);
				intCxt.pagePtr = useParentImg ? context->parentImg : context->img;
				if (!result)
				{
					if (context->index == 0)
					{
						wrongChangeCount = true;
					}
					else
					{
						step_upward_level(&intCxt);
						continue;
					}
				}
			}
		}

		/* Re-try the page level has been changed */
		if (!wrongChangeCount && level != PAGE_GET_LEVEL(intCxt.pagePtr))
		{
			if (intCxt.haveLock)
			{
				unlock_page(intCxt.blkno);
				intCxt.haveLock = false;
			}
			continue;
		}

		if (!wrongChangeCount && STOPEVENTS_ENABLED())
		{
			params = btree_page_stopevent_params(desc, intCxt.pagePtr);
			STOPEVENT(STOPEVENT_PAGE_READ, params);
		}

		/* Handle the incorrect root situation */
		if (context->index == 0 && (wrongChangeCount ||
									intCxt.pageChangeCount != O_PAGE_GET_CHANGE_COUNT(intCxt.pagePtr)))
		{
			/* Release lock if needed */
			if (intCxt.haveLock)
			{
				unlock_page(intCxt.blkno);
				intCxt.haveLock = false;
			}

			/*
			 * We don't need to re-read shared memory more that once with TRY
			 * flag.
			 */
			if (tryFlag && shmemIsReloaded)
				return OFindPageResultFailure;

			/* Reload root information from the shared memory */
			desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
			desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
			desc->rootInfo.rootPageChangeCount = 0;
			if (tryFlag)
			{
				if (!o_btree_try_use_shmem(desc))
					return OFindPageResultFailure;
			}
			else
			{
				o_btree_load_shmem(desc);
			}
			shmemIsReloaded = true;

			/* Initiate another attempt */
			intCxt.blkno = desc->rootInfo.rootPageBlkno;
			intCxt.pageChangeCount = desc->rootInfo.rootPageChangeCount;
			p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
			continue;
		}

		if (context->index > 0 && (wrongChangeCount ||
								   intCxt.pageChangeCount != O_PAGE_GET_CHANGE_COUNT(intCxt.pagePtr)))
		{
			/*
			 * It's not the expected page, try to refind it.
			 */
			step_upward_level(&intCxt);
			continue;
		}

		if (level > targetLevel || (downlinkLocationFlag && level > 0))
		{
			OBTreeFastPathFindResult result;

			result = page_find_downlink(&intCxt, &fastpathMeta, level,
										fastpath, &loc, &nonLeafHdr);

			Assert(result != OBTreeFastPathFindSlowpath);

			if (result == OBTreeFastPathFindFailure)
				return OFindPageResultFailure;
			else if (result == OBTreeFastPathFindRetry)
				continue;
			p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		}
		else
		{
			OBTreeFastPathFindResult result;

			result = page_find_item(&intCxt, &fastpathMeta, level,
									fastpath, &loc, &nonLeafHdr);

			if (result == OBTreeFastPathFindFailure)
				return OFindPageResultFailure;
			else if (result == OBTreeFastPathFindRetry)
			{
				if (intCxt.inserted)
					return OFindPageResultInserted;
				continue;
			}
			p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		}

		if (STOPEVENTS_ENABLED())
		{
			params = btree_page_stopevent_params(desc, intCxt.pagePtr);
			STOPEVENT(STOPEVENT_AFTER_FIND_DOWNLINK, params);
		}

		/* Place new item to the context */
		Assert(context->index < ORIOLEDB_MAX_DEPTH);

		context->items[context->index].locator = loc;
		context->items[context->index].blkno = intCxt.blkno;

		/*
		 * The immediate parent's downlink may have been located via the
		 * fastpath, leaving the locator pointing into the shared page (and
		 * parentImg not populated).  A sibling-navigating caller
		 * (KEEP_PARENT, e.g. the iterator) needs the parent in parentImg.
		 *
		 * A backward scan (KEEP_LOKEY) reads the parent's lokey from
		 * parentImg just below, so materialize it now; if the parent changed
		 * under us, re-read it from the top of the loop.  A forward scan
		 * touches the parent only when it steps right, so defer the copy to
		 * find_right_page() -- a scan that never crosses a parent boundary
		 * then pays nothing, and the on-demand copy is fresher than one
		 * carried across many iterator steps.  (A slowpath parent read
		 * already filled parentImg, so nothing to do there.)
		 */
		if (level == targetLevel + 1)
		{
			if (fastpath && keepParentFlag && !keepLokeyFlag)
				context->parentImgDeferred = true;
			else
			{
				context->parentImgDeferred = false;
				if (fastpath && keepParentFlag &&
					!convert_fastpath_parent_to_img(context,
													&context->items[context->index].locator))
					continue;
			}
		}

		context->items[context->index].pageChangeCount = O_PAGE_GET_CHANGE_COUNT(intCxt.pagePtr);

		/*
		 * Save the lokey if needed.
		 *
		 * For levels above the immediate parent the located downlink is the
		 * propagated lokey of the leftmost descent below; keep it in
		 * context->lokey (LOKEY_EXISTS), which btree_find_context_lokey()
		 * returns when the target page's own downlink is the parent's first
		 * one.
		 *
		 * For the immediate parent of a *leaf* target (level == 1, i.e. the
		 * leaf iterator's targetLevel == 0) the located downlink is the leaf
		 * page's own lokey; stash it in the dedicated, stable
		 * context->leafLokey so btree_find_context_lokey() can return it
		 * without re-reading the parent image -- which is unreliable in FETCH
		 * mode, where parentImg is partial and may be reclaimed under
		 * page-pool pressure during iteration.
		 *
		 * When targetLevel > 0 (e.g. the sequential scan descends to
		 * targetLevel == 1 with KEEP_LOKEY and reads context->lokey
		 * directly), the immediate parent's downlink is still the target
		 * page's own lokey, but its consumer expects it in context->lokey,
		 * exactly as the pre-FETCH-iterator code produced it.  So only divert
		 * to leafLokey for the leaf case (targetLevel == 0); otherwise fall
		 * through to context->lokey.
		 */
		if (keepLokeyFlag && level > targetLevel)
		{
			OTuple		lokey;

			/*
			 * A FETCH-mode descent locates the downlink via the fastpath,
			 * which does not materialize parentImg
			 * (can_fastpath_find_downlink() only enables the fastpath in
			 * FETCH mode).  The offset check and the lokey read below both
			 * touch parentImg -- the offset needs the hikeys chunk (chunk
			 * descriptors) and the read needs the chunk holding `loc` -- so
			 * materialize them.  This is a no-op when the parent was read
			 * whole (IMAGE/MODIFY disable the fastpath) or the chunk is
			 * already loaded; a lost race re-descends from the top.
			 */
			if (context->parentPartial.isPartial &&
				intCxt.pagePtr == context->parentImg &&
				!convert_fastpath_parent_to_img(context, &loc))
				continue;

			if (BTREE_PAGE_LOCATOR_GET_OFFSET(intCxt.pagePtr, &loc) > 0)
			{
				Assert(nonLeafHdr);

				BTREE_PAGE_READ_INTERNAL_TUPLE(lokey, intCxt.pagePtr, &loc);

				if (level == targetLevel + 1 && targetLevel == 0)
					copy_fixed_key(context->desc, &context->leafLokey, lokey);
				else
				{
					copy_fixed_key(context->desc, &context->lokey, lokey);
					BTREE_PAGE_FIND_SET(context, LOKEY_EXISTS);
					BTREE_PAGE_FIND_UNSET(context, LOKEY_SIBLING);
					BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
				}
			}
		}

		if (level != targetLevel && ((!imageFlag && !fetchFlag) || level > targetLevel) && !nonLeafHdr)
		{
			Assert(tryFlag);
			if (intCxt.haveLock)
			{
				unlock_page(intCxt.blkno);
				intCxt.haveLock = false;
			}
			return OFindPageResultFailure;
		}

		if (level == targetLevel || ((imageFlag || fetchFlag) && level <= targetLevel))
		{
			if (intCxt.haveLock)
			{
				/*
				 * The only way the target is reached under a page lock is the
				 * modify path -- needLock is set only on level > targetLevel
				 * and is cleared before we step down, and step_upward_level()
				 * clears haveLock when it unlocks. The IMAGE/FETCH callers
				 * expect context->img to be populated, which only happens in
				 * the lockless else branch above; if we ever reached here
				 * holding a lock without modifyFlag, that contract would be
				 * silently broken.
				 */
				Assert(modifyFlag);

				if (level == 0 && fixLeafFlag)
				{
					/* called from o_btree_normal_modify() */
					/* try to fix incomplete split for leafs here */
					bool		relocked = false;

					Assert(!noFixFlag);

					if (O_PAGE_IS(p, BROKEN_SPLIT))
					{
						o_btree_split_fix_for_right_page_and_unlock(desc, intCxt.blkno);
						intCxt.haveLock = false;
						step_upward_level(&intCxt);
						continue;
					}
					else if (relocked)
					{
						step_upward_level(&intCxt);
						continue;
					}
				}
			}

			O_TUPLE_SET_NULL(context->insertTuple);
			return OFindPageResultSuccess;
		}
		else if (!nonLeafHdr)
		{
			Assert(false);		/* make clang static analyzer happy */
		}
		else if (DOWNLINK_IS_ON_DISK(nonLeafHdr->downlink))
		{
			if (tryFlag)
			{
				/*
				 * Don't try to load page from write_page()
				 */
				if (intCxt.haveLock)
					unlock_page(intCxt.blkno);
				return OFindPageResultFailure;
			}

			if (intCxt.haveLock)
			{
				load_page(context);
				intCxt.blkno = context->items[context->index].blkno;
				loc = context->items[context->index].locator;
				intCxt.pagePtr = p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
				nonLeafHdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(intCxt.pagePtr, &loc);

				if (level != PAGE_GET_LEVEL(p))
				{
					unlock_page(intCxt.blkno);
					intCxt.haveLock = false;
					continue;
				}

				if ((imageFlag || keepParentFlag) && level == targetLevel + 1)
				{
					/*
					 * Just loaded the target's child into shared memory and
					 * refound the parent under MODIFY lock; the parent's
					 * downlinks differ from the pre-load partial read still
					 * sitting in parentImg.  Refresh parentImg and rebind the
					 * locator before stepping down.  Needed for any caller
					 * that later navigates siblings (IMAGE, or FETCH via
					 * KEEP_PARENT).
					 */
					refresh_parent_img_chunk(&intCxt);
				}
			}
			else
			{
				needLock = true;
				continue;
			}
		}
		else if (DOWNLINK_IS_IN_IO(nonLeafHdr->downlink))
		{
			int			ionum = DOWNLINK_GET_IO_LOCKNUM(nonLeafHdr->downlink);

			if (intCxt.haveLock)
			{
				unlock_page(intCxt.blkno);
				intCxt.haveLock = false;
			}
			wait_for_io_completion(ionum);
			continue;
		}
		else
		{
			/*
			 * IN_MEMORY downlink at the parent of the target in IMAGE mode.
			 * If we got here under the lock (needLock = true on an earlier
			 * iteration) intCxt.pagePtr is the real shared-memory page, not
			 * parentImg, and the locator that find_right_page/find_left_page
			 * will later consult still has its chunk pointer bound to shared
			 * memory.  Refresh parentImg from the locked page and rebind the
			 * locator onto parentImg so subsequent reads do not race against
			 * concurrent writers on the unlocked shared page.
			 */
			if ((imageFlag || keepParentFlag) && level == targetLevel + 1 &&
				intCxt.haveLock && intCxt.pagePtr != context->parentImg)
				refresh_parent_img_chunk(&intCxt);
		}

		parentBlkno = intCxt.blkno;
		context->index++;
		intCxt.blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(nonLeafHdr->downlink);
		intCxt.pageChangeCount = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(nonLeafHdr->downlink);

		if (STOPEVENTS_ENABLED())
		{
			params = btree_downlink_stopevent_params(desc, intCxt.pagePtr, &loc);
		}

		if (intCxt.haveLock)
		{
			unlock_page(parentBlkno);
			intCxt.haveLock = false;
		}

		p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		STOPEVENT(STOPEVENT_STEP_DOWN, params);
	}
}

static bool
follow_rightlink(OBTreeFindPageInternalContext *intCxt)
{
	OBTreeFindPageContext *context = intCxt->context;
	BTreeDescr *desc = context->desc;
	BTreeKeyType keykind = (intCxt->keyType == BTreeKeyPageHiKey ?
							BTreeKeyNonLeafKey :
							intCxt->keyType);
	int			followVal = (intCxt->keyType == BTreeKeyPageHiKey ? 1 : 0);
	OTuple		pageHiKey;

	if (!O_PAGE_IS(intCxt->pagePtr, RIGHTMOST))
		BTREE_PAGE_GET_HIKEY(pageHiKey, intCxt->pagePtr);
	while (!O_PAGE_IS(intCxt->pagePtr, RIGHTMOST) &&
		   (intCxt->keyType == BTreeKeyRightmost ||
			o_btree_cmp(desc, intCxt->key, keykind,
						&pageHiKey, BTreeKeyNonLeafKey) >= followVal))
	{
		uint64		rightlink = BTREE_PAGE_GET_RIGHTLINK(intCxt->pagePtr);

		if (!OInMemoryBlknoIsValid(RIGHTLINK_GET_BLKNO(rightlink)))
		{
			if (intCxt->haveLock)
			{
				unlock_page(intCxt->blkno);
				intCxt->haveLock = false;
			}
			return true;
		}

		if (BTREE_PAGE_FIND_IS(context, KEEP_LOKEY))
		{
			copy_fixed_hikey(desc, &context->lokey, intCxt->pagePtr);
			\
				Assert(!O_TUPLE_IS_NULL(context->lokey.tuple));
			BTREE_PAGE_FIND_SET(context, LOKEY_EXISTS);
			if (PAGE_GET_LEVEL(intCxt->pagePtr) == intCxt->targetLevel)
			{
				BTREE_PAGE_FIND_SET(context, LOKEY_SIBLING);
				BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
			}
			else
			{
				BTREE_PAGE_FIND_UNSET(context, LOKEY_SIBLING);
				BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
			}
		}

		if (intCxt->haveLock)
			unlock_page(intCxt->blkno);

		intCxt->blkno = RIGHTLINK_GET_BLKNO(rightlink);

		if (intCxt->haveLock)
		{
			if (BTREE_PAGE_FIND_IS(context, TRY_LOCK))
			{
				if (!try_lock_page(intCxt->blkno))
				{
					intCxt->haveLock = false;
					intCxt->tryLockFailed = true;
					return true;
				}
			}
			else if (!O_TUPLE_IS_NULL(context->insertTuple))
			{
				OLockPageWithTupleResult result;

				result = lock_page_with_tuple(desc,
											  &intCxt->blkno,
											  &intCxt->pageChangeCount,
											  context->insertXactInfo,
											  context->insertTuple);

				if (result == OLockPageWithTupleResultInserted)
				{
					intCxt->haveLock = false;
					intCxt->inserted = true;
					return true;
				}
				else if (result == OLockPageWithTupleResultRefindNeeded)
				{
					intCxt->haveLock = false;
					return true;
				}
				Assert(result == OLockPageWithTupleResultLocked);
			}
			else
			{
				lock_page(intCxt->blkno);
			}
			intCxt->pagePtr = O_GET_IN_MEMORY_PAGE(intCxt->blkno);
			intCxt->pageChangeCount = O_PAGE_GET_CHANGE_COUNT(intCxt->pagePtr);
			if (intCxt->pageChangeCount !=
				RIGHTLINK_GET_CHANGECOUNT(rightlink))
			{
				/*
				 * Split was finished and right page is already
				 * merged/evicted. Have to retry.
				 */
				unlock_page(intCxt->blkno);
				intCxt->haveLock = false;
				return true;
			}
		}
		else
		{
			bool		useParentImg = (intCxt->pagePtr == context->parentImg);

			if (!btree_find_read_page(context, intCxt->blkno,
									  RIGHTLINK_GET_CHANGECOUNT(rightlink),
									  useParentImg,
									  intCxt->key,
									  intCxt->keyType,
									  intCxt->partial,
									  true))
				return true;
			intCxt->pagePtr = useParentImg ? context->parentImg : context->img;
			intCxt->pageChangeCount = O_PAGE_GET_CHANGE_COUNT(intCxt->pagePtr);
			Assert(RIGHTLINK_GET_CHANGECOUNT(rightlink) ==
				   O_PAGE_GET_CHANGE_COUNT(intCxt->pagePtr));
		}
		if (!O_PAGE_IS(intCxt->pagePtr, RIGHTMOST))
			BTREE_PAGE_GET_HIKEY(pageHiKey, intCxt->pagePtr);
	}
	return false;
}

/*
 * Step to the upward level of the tree and retry the search.
 */
static void
step_upward_level(OBTreeFindPageInternalContext *intCxt)
{
	OBTreeFindPageContext *context = intCxt->context;

	if (intCxt->haveLock)
	{
		unlock_page(intCxt->blkno);
		intCxt->haveLock = false;
	}
	context->index--;
	intCxt->blkno = context->items[context->index].blkno;
	intCxt->pageChangeCount = context->items[context->index].pageChangeCount;
}

/*
 * Re-find the location of previously found key.  If search for modification,
 * assume lock was relesed (otherwise, no point to refind).
 */
OFindPageResult
refind_page(OBTreeFindPageContext *context, void *key, BTreeKeyType keyType,
			uint16 level, OInMemoryBlkno _blkno, uint32 _pageChangeCount)
{
	BTreeDescr *desc = context->desc;
	OBTreeFindPageInternalContext intCxt;
	BTreePageItemLocator loc;
	bool		item_found = true;

	ASAN_UNPOISON_MEMORY_REGION(&intCxt, sizeof(intCxt));
	intCxt.context = context;
	intCxt.key = key;
	intCxt.keyType = keyType;
	intCxt.blkno = _blkno;
	intCxt.targetLevel = level;
	intCxt.pageChangeCount = _pageChangeCount;
	intCxt.partial = NULL;
	intCxt.inserted = false;
	intCxt.tryLockFailed = false;

	if (!BTREE_PAGE_FIND_IS(context, TRY_LOCK))
	{
		o_btree_load_shmem(desc);
	}
	else
	{
		if (!o_btree_try_use_shmem(desc))
			return OFindPageResultFailure;
	}

retry:

	if (BTREE_PAGE_FIND_IS(context, MODIFY))
	{
		Pointer		p;

		if (intCxt.pageChangeCount == InvalidOPageChangeCount)
			return find_page(context, key, keyType, level);

		/*
		 * Local-pool slots are NULLed on eviction, unlike shared-pool slots
		 * where pageChangeCount alone signals replacement (the shmem page
		 * stays readable).  The slot at the caller's saved (blkno,
		 * pageChangeCount) may have been evicted since, so PAGE_GET_LEVEL
		 * below would segfault.  Fall back to find_page() to resolve the
		 * downlink from scratch.
		 */
		if (O_PAGE_IS_LOCAL(intCxt.blkno) &&
			local_ppool_pages[intCxt.blkno & O_BLKNO_MASK] == NULL)
			return find_page(context, key, keyType, level);

		if (!O_TUPLE_IS_NULL(context->insertTuple))
		{
			OLockPageWithTupleResult result;

			result = lock_page_with_tuple(desc,
										  &intCxt.blkno,
										  &intCxt.pageChangeCount,
										  context->insertXactInfo,
										  context->insertTuple);

			if (result == OLockPageWithTupleResultInserted)
				return OFindPageResultInserted;
			else if (result == OLockPageWithTupleResultRefindNeeded)
				return find_page(context, key, keyType, level);
			Assert(result == OLockPageWithTupleResultLocked);
		}
		else
		{
			lock_page(intCxt.blkno);
		}
		p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		intCxt.haveLock = true;
		intCxt.pagePtr = p;
		if (PAGE_GET_LEVEL(p) != level ||
			O_PAGE_GET_CHANGE_COUNT(p) != intCxt.pageChangeCount)
		{
			unlock_page(intCxt.blkno);
			return find_page(context, key, keyType, level);
		}

		if (level == 0 && BTREE_PAGE_FIND_IS(context, FIX_LEAF_SPLIT))
		{
			/* called from o_btree_normal_modify() */
			/* try to fix incomplete split for leafs here */

			Assert(!BTREE_PAGE_FIND_IS(context, NO_FIX_SPLIT));

			if (O_PAGE_IS(p, BROKEN_SPLIT))
			{
				o_btree_split_fix_for_right_page_and_unlock(desc, intCxt.blkno);
				intCxt.haveLock = false;
				o_btree_split_fix_and_unlock(desc, intCxt.blkno);
				goto retry;
			}
		}
	}
	else if (BTREE_PAGE_FIND_IS(context, FETCH))
	{
		Pointer		img;
		bool		success;

		if (intCxt.pageChangeCount == InvalidOPageChangeCount)
			return find_page(context, key, keyType, level);

		context->partial.isPartial = false;
		intCxt.partial = &context->partial;
		success = btree_find_read_page(context,
									   intCxt.blkno,
									   intCxt.pageChangeCount,
									   false,
									   key,
									   keyType,
									   intCxt.partial,
									   true);
		img = context->img;

		intCxt.haveLock = false;
		intCxt.pagePtr = img;
		if (!success ||
			PAGE_GET_LEVEL(img) != level)
		{
			return find_page(context, key, keyType, level);
		}
		Assert(O_PAGE_GET_CHANGE_COUNT(img) == intCxt.pageChangeCount);
	}
	else
	{
		Assert(false);
		/* quiet compiler warnings */
		intCxt.haveLock = false;
		intCxt.pagePtr = NULL;
	}

	/* Follow the page rightlink if needed */
	if (keyType != BTreeKeyNone)
	{
		if (follow_rightlink(&intCxt))
		{
			if (intCxt.tryLockFailed)
				return OFindPageResultFailure;
			if (intCxt.inserted)
				return OFindPageResultInserted;
			Assert(!intCxt.haveLock);
			return find_page(context, key, keyType, level);
		}
	}

	if (keyType == BTreeKeyRightmost)
	{
		/* We're looking for the rightmost page, so go the rightmost downlink */
		BTREE_PAGE_LOCATOR_LAST(intCxt.pagePtr, &loc);
	}
	else if (keyType == BTreeKeyNone)
	{
		/* We're looking for the leftmost page, so go the leftmost downlink */
		BTREE_PAGE_LOCATOR_FIRST(intCxt.pagePtr, &loc);
	}
	else
	{
		/* Locate the correct downlink within the non-leaf page */
		Assert(key);
		item_found = btree_page_search(desc, intCxt.pagePtr, key, keyType,
									   intCxt.partial, &loc);
		if (item_found)
		{
			if (BTREE_PAGE_FIND_IS(context, DOWNLINK_LOCATION))
			{
				Assert(!O_PAGE_IS(intCxt.pagePtr, LEAF));
				BTREE_PAGE_LOCATOR_PREV(intCxt.pagePtr, &loc);
				if (intCxt.partial)
					item_found = partial_load_chunk(intCxt.partial,
													intCxt.pagePtr,
													loc.chunkOffset,
													NULL);
			}
			else if (!BTREE_PAGE_FIND_IS(context, MODIFY))
				item_found = page_locator_find_real_item(intCxt.pagePtr,
														 intCxt.partial,
														 &loc);
		}
	}

	if (intCxt.partial)
	{
		if (!item_found)
			goto retry;

		if (!partial_load_chunk(intCxt.partial, intCxt.pagePtr,
								loc.chunkOffset, NULL))
			goto retry;
	}

	context->items[context->index].locator = loc;
	context->items[context->index].blkno = intCxt.blkno;
	context->items[context->index].pageChangeCount = intCxt.pageChangeCount;
	return OFindPageResultSuccess;
}

/*
 * Find the right sibling of the current page.
 *
 * Old page hikey will be saved to hikey_buf.  It helps to avoid redundant
 * buffering at BTree iterators code.
 *
 * Returns true on success, false for rightmost page.
 */
bool
find_right_page(OBTreeFindPageContext *context, OFixedKey *hikey)
{
	BTreeDescr *desc = context->desc;
	BTreePageItemLocator loc;
	OBtreePageFindItem *parentItem,
			   *item;
	int			level;
	Jsonb	   *params;
	OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;

	/* Nothing to do with rightmost page */
	if (O_PAGE_IS(context->img, RIGHTMOST))
		return false;

	/*
	 * Currenlty, the only user of this function is iterator, which is
	 * read-only.  So, no support for modification, but could we added later.
	 */
	Assert(!BTREE_PAGE_FIND_IS(context, MODIFY));

	if (STOPEVENTS_ENABLED())
	{
		params = btree_page_stopevent_params(desc, context->img);
		STOPEVENT(STOPEVENT_STEP_RIGHT, params);
	}

	level = PAGE_GET_LEVEL(context->img);

	/* In this case, we shouldn't be in the rootPageBlkno... */
	Assert(context->index > 0);

	parentItem = &context->items[context->index - 1];
	item = &context->items[context->index];

	/* copy hikey (also needed for the find_page() fallback below) */
	copy_fixed_hikey(desc, hikey, context->img);

	/*
	 * A forward descent that located the parent via the fastpath deferred
	 * copying it into parentImg (see find_page()).  Now that we actually need
	 * the parent's downlinks, materialize it; on failure (the parent changed
	 * or was evicted) fall back to a find_page() re-descent from the root.
	 */
	if (context->parentImgDeferred)
	{
		if (!convert_fastpath_parent_to_img(context, &parentItem->locator))
		{
			findResult = find_page(context, hikey, BTreeKeyNonLeafKey, level);
			Assert(findResult == OFindPageResultSuccess);
			return true;
		}
		context->parentImgDeferred = false;
	}

	/* Try to get next item from the parent page */
	loc = context->items[context->index - 1].locator;

	Assert(loc.chunk == NULL ||
		   ((Pointer) loc.chunk >= context->parentImg &&
			(Pointer) loc.chunk < context->parentImg + ORIOLEDB_BLCKSZ));

	if (BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
		BTREE_PAGE_LOCATOR_NEXT(context->parentImg, &loc);

	/* Try to load next page using next parent downlink */
	if (BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
	{
		OTuple		internalTuple;
		BTreeNonLeafTuphdr *tuphdr = NULL;
		bool		tup_loaded = true;

		tup_loaded = partial_load_chunk(&context->parentPartial, context->parentImg,
										loc.chunkOffset, NULL);
		if (tup_loaded)
		{
			BTREE_PAGE_READ_INTERNAL_ITEM(tuphdr, internalTuple, context->parentImg, &loc);
			Assert(tuphdr != NULL);
		}

		/* Check it's consistent with our hikey */
		if (tup_loaded && DOWNLINK_IS_IN_MEMORY(tuphdr->downlink) &&
			o_btree_cmp(desc,
						hikey, BTreeKeyNonLeafKey,
						&internalTuple, BTreeKeyNonLeafKey) == 0)
		{
			/* Try to traverse downlink */
			bool		success;

			item->blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
			item->pageChangeCount = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(tuphdr->downlink);

			success = btree_find_read_page(context, item->blkno, item->pageChangeCount,
										   false, &hikey->tuple, BTreeKeyNonLeafKey,
										   BTREE_PAGE_FIND_IS(context, FETCH) ?
										   &context->partial : NULL,
										   true);
			if (success &&
				PAGE_GET_LEVEL(context->img) == level)
			{
				Assert(O_PAGE_GET_CHANGE_COUNT(context->img) == item->pageChangeCount);
				BTREE_PAGE_LOCATOR_FIRST(context->img, &item->locator);
				parentItem->locator = loc;
				return true;
			}
		}
	}

	/*
	 * Give up with parent downlink.  Find the page from the root in a usual
	 * way.  Should happen rarely.
	 */
	findResult = find_page(context, hikey, BTreeKeyNonLeafKey, level);
	Assert(findResult == OFindPageResultSuccess);
	return true;
}

/*
 * Refresh the stable copy of the current page's own lokey after find_left_page()
 * steps to a sibling through the parent downlink at *loc.  When the downlink is
 * the parent's first one the sibling inherits the parent's propagated lokey
 * (kept in context->lokey), so there is nothing to capture here.
 */
static inline void
refresh_context_leaf_lokey(OBTreeFindPageContext *context,
						   BTreePageItemLocator *loc)
{
	if (BTREE_PAGE_LOCATOR_GET_OFFSET(context->parentImg, loc) > 0)
	{
		OTuple		lokey;

		BTREE_PAGE_READ_INTERNAL_TUPLE(lokey, context->parentImg, loc);
		copy_fixed_key(context->desc, &context->leafLokey, lokey);
	}
}

/*
 * Find the left sibling of the current page.
 *
 * Expected new page hikey (lokey for old page) will be saved to hikey_buf.
 * It helps to avoid redundant buffer at BTree iterators code.
 *
 * Returns true on success, false for leftmost page.
 */
bool
find_left_page(OBTreeFindPageContext *context, OFixedKey *hikey)
{
	BTreeNonLeafTuphdr *tuphdr;
	BTreeDescr *desc = context->desc;
	OBtreePageFindItem *parentItem,
			   *item;
	int			level;
	UndoLocation prevLoc;
	Jsonb	   *params;
	OTuple		imgHikey;
	OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;

	Assert(BTREE_PAGE_FIND_IS(context, KEEP_LOKEY));

	/*
	 * Currenlty, the only user of this function is iterator, which is
	 * read-only.  So, no support for modification, but could we added later.
	 */
	Assert(!BTREE_PAGE_FIND_IS(context, MODIFY));

	if (STOPEVENTS_ENABLED())
	{
		params = btree_page_stopevent_params(desc, context->img);
		STOPEVENT(STOPEVENT_STEP_LEFT, params);
	}

	level = PAGE_GET_LEVEL(context->img);
	/* In this case, we shouldn't be in the rootPageBlkno... */
	Assert(level == 0);
	Assert(context->index > 0);
	parentItem = &context->items[context->index - 1];
	item = &context->items[context->index];

	prevLoc = context->imgUndoLoc;
	while (true)
	{
		/* Nothing to do with leftmost page */
		if (O_PAGE_IS(context->img, LEFTMOST))
			return false;

		Assert(!O_TUPLE_IS_NULL(btree_find_context_lokey(context)));
		copy_fixed_key(desc, hikey, btree_find_context_lokey(context));

		/*
		 * if we have rightlink hikey on the same level (leaf in this case)
		 * just follow it.
		 */
		if (!BTREE_PAGE_FIND_IS(context, LOKEY_SIBLING) &&
			!BTREE_PAGE_FIND_IS(context, LOKEY_UNDO))
		{
			BTreePageItemLocator loc = parentItem->locator;
			bool		next_lokey_loaded = true;

			Assert(loc.chunk == NULL ||
				   ((Pointer) loc.chunk >= context->parentImg &&
					(Pointer) loc.chunk < context->parentImg + ORIOLEDB_BLCKSZ));

			/*
			 * Tries to read image from parent downlink without find_page().
			 */
			if (BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
			{
				BTREE_PAGE_LOCATOR_PREV(context->parentImg, &loc);
				next_lokey_loaded = partial_load_chunk(&context->parentPartial,
													   context->parentImg,
													   loc.chunkOffset,
													   NULL);
			}

			if (next_lokey_loaded && BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
			{
				tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(context->parentImg, &loc);

				/*
				 * else next lokey saved in context.lokey
				 */
				if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
				{
					bool		success;

					item->blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
					item->pageChangeCount = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(tuphdr->downlink);

					success = btree_find_read_page(context,
												   item->blkno,
												   item->pageChangeCount,
												   false,
												   NULL,
												   BTreeKeyRightmost,
												   BTREE_PAGE_FIND_IS(context, FETCH) ?
												   &context->partial : NULL,
												   true);

					if (success &&
						context->imgUndoLoc != InvalidUndoLocation &&
						prevLoc == context->imgUndoLoc)
					{
						parentItem->locator = loc;
						refresh_context_leaf_lokey(context, &loc);
						continue;
					}


					if (success &&
						PAGE_GET_LEVEL(context->img) == level &&
						!O_PAGE_IS(context->img, RIGHTMOST))
					{
						BTREE_PAGE_GET_HIKEY(imgHikey, context->img);

						if (o_btree_cmp(desc, &hikey->tuple, BTreeKeyNonLeafKey,
										&imgHikey, BTreeKeyNonLeafKey) == 0)
						{
							Assert(O_PAGE_GET_CHANGE_COUNT(context->img) == item->pageChangeCount);
							parentItem->locator = loc;
							refresh_context_leaf_lokey(context, &loc);
							BTREE_PAGE_LOCATOR_LAST(context->img, &item->locator);
							return true;
						}
					}
				}
			}
		}

		findResult = find_page(context, &hikey->tuple, BTreeKeyPageHiKey, level);
		Assert(findResult == OFindPageResultSuccess);

		/* context levels may be changed */
		parentItem = &context->items[context->index - 1];
		item = &context->items[context->index];

		if (prevLoc != InvalidUndoLocation && prevLoc == context->imgUndoLoc)
			continue;

		if (COMMITSEQNO_IS_INPROGRESS(context->csn) &&
			!O_PAGE_IS(context->img, RIGHTMOST))
			BTREE_PAGE_GET_HIKEY(imgHikey, context->img);

		if (COMMITSEQNO_IS_INPROGRESS(context->csn) &&
			(O_PAGE_IS(context->img, RIGHTMOST)
			 || o_btree_cmp(desc, &imgHikey, BTreeKeyNonLeafKey, hikey, BTreeKeyNonLeafKey) != 0))
		{
			/*
			 * The BTree may be changed in progress, but find_page() function
			 * setup leaf offset always as BTREE_PAGE_ITEMS_COUNT(page) - 1
			 * for the BTreeHiKey search case.
			 *
			 * We must refind the leaf offset in this case.
			 */
			btree_page_search(desc,
							  context->img,
							  (Pointer) &hikey->tuple, BTreeKeyNonLeafKey, NULL,
							  &item->locator);
			BTREE_PAGE_LOCATOR_PREV(context->img, &item->locator);
		}

		return true;
	}

	/* unreachable */
	Assert(false);
	return false;
}

/*
 * Return lokey of the context->img.
 *
 * It assumes that context->img have a lokey. All checks must be done by a caller code
 * (BTREE_PAGE_FIND_KEEP_LOKEY flag exist, !PAGE_IS_LEFTMOST(context->img)).
 */
OTuple
btree_find_context_lokey(OBTreeFindPageContext *context)
{
	BTreePageItemLocator ploc = context->items[context->index - 1].locator;

	Assert(BTREE_PAGE_FIND_IS(context, KEEP_LOKEY));

	if (BTREE_PAGE_FIND_IS(context, LOKEY_UNDO))
	{
		/*
		 * Hikey of a left sibling from undo log.
		 */
		return context->undoLokey.tuple;
	}
	else if (BTREE_PAGE_FIND_IS(context, LOKEY_SIBLING))
	{
		/*
		 * Hikey of the left sibling (had a rightlink to the current page).
		 */
		return context->lokey.tuple;
	}
	else if (BTREE_PAGE_LOCATOR_GET_OFFSET(context->parentImg, &ploc) > 0)
	{
		/*
		 * The current page's own lokey is its downlink key in the parent.
		 * find_page() descent and find_left_page() stepping keep it in the
		 * stable context->leafLokey, so return that instead of re-reading the
		 * parent image.  In FETCH mode parentImg is partial and may have been
		 * reclaimed under page-pool pressure since it was last read, which
		 * would make a re-read return garbage.
		 */
		return context->leafLokey.tuple;
	}
	else
	{
		/*
		 * The current page is the leftmost child of its immediate parent, so
		 * its lokey is the parent's lokey, carried down the descent in
		 * context->lokey (LOKEY_EXISTS).
		 *
		 * A frozen no-record split half (see o_btree_insert_split()) reached
		 * live during a backward FETCH scan can transiently arrive here with
		 * the lokey unestablished -- it is the leftmost child of its parent,
		 * has no carried lokey, and its frozen csn means no undo chain
		 * supplies one.  The backward iterator detects that via
		 * btree_find_context_has_lokey() and re-descends
		 * (iterator_refind_partial_leaf, which also switches to whole-page
		 * reads) before stepping left, so by the time we reach here
		 * LOKEY_EXISTS always holds.
		 */
		Assert(BTREE_PAGE_FIND_IS(context, LOKEY_EXISTS));
		return context->lokey.tuple;
	}
}

/*
 * Whether btree_find_context_lokey() can return the current page's real lokey,
 * i.e. the descent established one of its reliable sources.  Returns false only
 * in the transient state a frozen no-record split half leaves when reached live
 * in FETCH mode (leftmost child of its parent, no carried lokey and no undo
 * chain to recover it); the backward iterator recovers from that by
 * re-descending instead of stepping left off a bogus lokey.
 */
bool
btree_find_context_has_lokey(OBTreeFindPageContext *context)
{
	BTreePageItemLocator ploc = context->items[context->index - 1].locator;

	Assert(BTREE_PAGE_FIND_IS(context, KEEP_LOKEY));

	return BTREE_PAGE_FIND_IS(context, LOKEY_UNDO) ||
		BTREE_PAGE_FIND_IS(context, LOKEY_SIBLING) ||
		BTREE_PAGE_LOCATOR_GET_OFFSET(context->parentImg, &ploc) > 0 ||
		BTREE_PAGE_FIND_IS(context, LOKEY_EXISTS);
}

static Pointer
set_page_ptr(OBTreeFindPageContext *context, bool parent)
{
	Pointer		pagePtr;

	if (!parent)
		pagePtr = context->img = context->imgData;
	else
		pagePtr = context->parentImg = context->parentImgData;
	return pagePtr;
}

/*
 * Navigates and reads page image from undo log according to find context.
 * Saves lokey of the founded page to context->lokey if needed.
 */
static bool
btree_find_read_page(OBTreeFindPageContext *context, OInMemoryBlkno blkno,
					 uint32 pageChangeCount, bool parent, void *key,
					 BTreeKeyType keyType, PartialPageState *partial,
					 bool loadHikeysChunk)
{
	bool		keep_lokey = BTREE_PAGE_FIND_IS(context, KEEP_LOKEY);
	OFixedKey  *lokey = keep_lokey ? &context->undoLokey : NULL;
	CommitSeqNo *readCsn = BTREE_PAGE_FIND_IS(context, READ_CSN) ? &context->imgReadCsn : NULL;
	bool		success;
	Pointer		pagePtr;

	pagePtr = set_page_ptr(context, parent);

	BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
	if (lokey)
		clear_fixed_key(lokey);

	success = o_btree_read_page(context->desc, blkno, pageChangeCount, pagePtr,
								context->csn, key, keyType, lokey,
								partial, loadHikeysChunk, &context->imgUndoLoc,
								readCsn);

	if (!success)
		return false;

	if (lokey && !O_TUPLE_IS_NULL(lokey->tuple))
		BTREE_PAGE_FIND_SET(context, LOKEY_UNDO);
	return true;
}

/*
 * Navigates and reads page image from undo log according to find context.
 * Saves lokey of the founded page to context->lokey if needed.
 */
static ReadPageResult
btree_find_try_read_page(OBTreeFindPageContext *context, OInMemoryBlkno blkno,
						 uint32 pageChangeCount, bool parent, void *key,
						 BTreeKeyType keyType, PartialPageState *partial,
						 bool loadHikeysChunk)
{
	CommitSeqNo *readCsn = BTREE_PAGE_FIND_IS(context, READ_CSN) ? &context->imgReadCsn : NULL;
	ReadPageResult result;
	Pointer		pagePtr;

	pagePtr = set_page_ptr(context, parent);

	result = o_btree_try_read_page(context->desc, blkno, pageChangeCount,
								   pagePtr, context->csn,
								   key, keyType, partial, loadHikeysChunk,
								   readCsn);

	return result;
}

void
btree_find_context_from_modify_to_read(OBTreeFindPageContext *context,
									   Pointer key,
									   BTreeKeyType keyType,
									   uint16 level)
{
	BTreePageItemLocator loc;
	bool		success;

	Assert(!BTREE_PAGE_FIND_IS(context, DOWNLINK_LOCATION));
	Assert(BTREE_PAGE_FIND_IS(context, MODIFY));
	Assert(BTREE_PAGE_FIND_IS(context, IMAGE));
	BTREE_PAGE_FIND_UNSET(context, MODIFY);

	success = btree_find_read_page(context,
								   context->items[context->index].blkno,
								   context->items[context->index].pageChangeCount,
								   false,
								   key,
								   keyType,
								   NULL,
								   true);

	if (!success)
	{
		(void) find_page(context, key, keyType, level);
		return;
	}

	if (keyType == BTreeKeyRightmost)
	{
		/* We're looking for the rightmost page, so go the rightmost downlink */
		BTREE_PAGE_LOCATOR_LAST(context->img, &loc);
	}
	else if (keyType == BTreeKeyNone)
	{
		/* We're looking for the leftmost page, so go the leftmost downlink */
		BTREE_PAGE_LOCATOR_FIRST(context->img, &loc);
	}
	else
	{
		/* Locate the correct downlink within the non-leaf page */
		(void) btree_page_search(context->desc, context->img,
								 key, keyType,
								 NULL, &loc);
		(void) page_locator_find_real_item(context->img,
										   NULL,
										   &loc);
	}

	context->items[context->index].locator = loc;
}

/*
 * Search for a key within the page.  First, it does binary search of
 * appropriate chunk, then binary search within the chunk.
 *
 * This function is aware of partial page read.  Returns true if it managed
 * to read the required chunk and false otherwise.  When no partial page
 * state is give, always returns true.
 */
bool
btree_page_search(BTreeDescr *desc, Page p, Pointer key, BTreeKeyType keyType,
				  PartialPageState *partial, BTreePageItemLocator *locator)
{
	OffsetNumber chunkOffset;
	bool		isLeaf = O_PAGE_IS(p, LEAF);

	if (keyType == BTreeKeyPageHiKey && isLeaf)
	{
		BTREE_PAGE_LOCATOR_LAST(p, locator);
		if (partial && !partial_load_chunk(partial, p,
										   locator->chunkOffset, NULL))
			return false;
		return true;
	}

	chunkOffset = btree_page_binary_search_chunks(desc, p, key, keyType);

	if (partial && !partial_load_chunk(partial, p, chunkOffset, NULL))
		return false;

	page_chunk_fill_locator(p, chunkOffset, locator);

	btree_page_search_items(desc, p, key, keyType, locator);

	return true;
}

/*
 * Search for the chunk containing key.
 */
static OffsetNumber
btree_page_binary_search_chunks(BTreeDescr *desc, Page p,
								Pointer key, BTreeKeyType keyType)
{
	OffsetNumber mid,
				low,
				high;
	int			targetCmpVal,
				result;
	bool		nextkey;
	BTreePageHeader *header = (BTreePageHeader *) p;
	OBTreeKeyCmp cmpFunc = desc->ops->cmp;

	Assert(header->chunksCount > 0);

	low = 0;
	high = header->chunksCount - 1;
	nextkey = (keyType != BTreeKeyPageHiKey);

	if (high < low)
		return low;

	targetCmpVal = nextkey ? 0 : 1; /* a target value of cmpFunc() */

	/*
	 * Don't pass BTreeHiKey to comparison function, we've set nextkey flag
	 * instead.
	 */
	if (keyType == BTreeKeyPageHiKey)
		keyType = BTreeKeyNonLeafKey;

	while (high > low)
	{
		OTuple		midTup;

		mid = low + ((high - low) / 2);
		Assert(mid < header->chunksCount - 1);

		/* We have low <= mid < high, so mid points at a real slot */

		midTup.formatFlags = header->chunkDesc[mid].hikeyFlags;
		midTup.data = p + SHORT_GET_LOCATION(header->chunkDesc[mid].hikeyShortLocation);
		result = cmpFunc(desc, key, keyType, &midTup, BTreeKeyNonLeafKey);

		if (result >= targetCmpVal)
			low = mid + 1;
		else
			high = mid;
	}

	return low;
}

static void
btree_page_search_items(BTreeDescr *desc, Page p, Pointer key,
						BTreeKeyType keyType, BTreePageItemLocator *locator)
{
	OffsetNumber mid,
				low,
				high;
	bool		isLeaf = O_PAGE_IS(p, LEAF),
				nextkey;
	OBTreeKeyCmp cmpFunc = desc->ops->cmp;
	BTreeKeyType midkind;
	int			targetCmpVal,
				result;

	midkind = isLeaf ? BTreeKeyLeafTuple : BTreeKeyNonLeafKey;

	if (locator->chunkItemsCount == 0)
	{
		locator->itemOffset = 0;
		return;
	}

	low = 0;
	high = locator->chunkItemsCount - 1;
	nextkey = (!isLeaf && keyType != BTreeKeyPageHiKey);

	/* Shouldn't look for hikey on leafs, because we're already here */
	Assert(!(isLeaf && keyType == BTreeKeyPageHiKey));

	/*
	 * Binary search to find the first key on the page >= `key`, or first page
	 * key > `key` when nextkey is true.
	 *
	 * For nextkey=false (cmp=1), the loop invariant is: all slots before
	 * `low` are < `key`, all slots at or after `high` are >= `key`.
	 *
	 * For nextkey=true (cmp=0), the loop invariant is: all slots before `low`
	 * are <= `key`, all slots at or after `high` are > `key`.
	 *
	 * We can fall out when `high` == `low`.
	 */
	high++;						/* establish the loop invariant for high */

	targetCmpVal = nextkey ? 0 : 1; /* a target value of cmpFunc() */

	/*
	 * Don't pass BTreeHiKey to comparison function, we've set nextkey flag
	 * instead.
	 */
	if (keyType == BTreeKeyPageHiKey)
		keyType = BTreeKeyNonLeafKey;

	while (high > low)
	{
		mid = low + ((high - low) / 2);

		if (!isLeaf && mid == 0 && locator->chunkOffset == 0)
			result = 1;
		else
		{
			OTuple		midTup;

			locator->itemOffset = mid;
			BTREE_PAGE_READ_TUPLE(midTup, p, locator);
			result = cmpFunc(desc, key, keyType, &midTup, midkind);
		}

		if (result >= targetCmpVal)
			low = mid + 1;
		else
			high = mid;
	}

	locator->itemOffset = low;
}
