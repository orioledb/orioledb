/*-------------------------------------------------------------------------
 *
 * find.c
 *		Routines for finding appropriate page in B-tree.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
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
} OBTreeFindPageInternalContext;

static bool follow_rightlink(OBTreeFindPageInternalContext *intCxt);
static void step_upward_level(OBTreeFindPageInternalContext *intCxt);
static bool btree_find_read_page(OBTreeFindPageContext *context,
								 OInMemoryBlkno blkno, uint32 pageChangeCount,
								 bool parent, void *key, BTreeKeyType keyType,
								 PartialPageState *partial,
								 bool loadHikeysChunk);
static bool btree_find_try_read_page(OBTreeFindPageContext *context,
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
	context->imgEntry = NULL;
	context->parentImg = NULL;
	context->parentImgEntry = NULL;
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
bool
find_page(OBTreeFindPageContext *context, void *key, BTreeKeyType keyType,
		  uint16 targetLevel)
{
	BTreeDescr *desc = context->desc;
	OBTreeFindPageInternalContext intCxt;
	BTreePageItemLocator loc;
	bool		needLock = false,
				fetchFlag PG_USED_FOR_ASSERTS_ONLY = BTREE_PAGE_FIND_IS(context, FETCH),
				modifyFlag = BTREE_PAGE_FIND_IS(context, MODIFY),
				imageFlag = BTREE_PAGE_FIND_IS(context, IMAGE),
				tryFlag = BTREE_PAGE_FIND_IS(context, TRY_LOCK),
				fixLeafFlag = BTREE_PAGE_FIND_IS(context, FIX_LEAF_SPLIT),
				noFixFlag = BTREE_PAGE_FIND_IS(context, NO_FIX_SPLIT),
				keepLokeyFlag = BTREE_PAGE_FIND_IS(context, KEEP_LOKEY),
				downlinkLocationFlag = BTREE_PAGE_FIND_IS(context, DOWNLINK_LOCATION);
	bool		shmemIsReloaded = false;
	FastpathFindDownlinkMeta fastpathMeta;
	Jsonb	   *params = NULL;

	memset(&intCxt, 0, sizeof(intCxt));
	ASAN_UNPOISON_MEMORY_REGION(&intCxt, sizeof(intCxt));
	intCxt.context = context;
	intCxt.key = key;
	intCxt.keyType = keyType;
	intCxt.targetLevel = targetLevel;
	intCxt.inserted = false;


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
		   || (!imageFlag && fetchFlag && !modifyFlag && !keepLokeyFlag)
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
	context->index = 0;

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

		p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		level = PAGE_GET_LEVEL(p);

		fastpath = fastpathMeta.enabled && !needLock;
		fastpath = fastpath && (keyType != BTreeKeyPageHiKey || level > 0);

		intCxt.partial = NULL;
		if (!imageFlag || level > 0)
			context->partial.isPartial = false;

		/*
		 * else saves isPartial flag for the parent of the leaf in imageFlag
		 * case
		 */

		if (needLock || (modifyFlag && level == targetLevel))
		{
			if (tryFlag)
			{
				if (!try_lock_page(intCxt.blkno))
					return false;
				intCxt.pagePtr = p;
				intCxt.haveLock = true;
				needLock = false;
			}
			else if (!O_TUPLE_IS_NULL(context->insertTuple))
			{
				bool		upwards = false;

				if (!lock_page_with_tuple(desc,
										  &intCxt.blkno,
										  &intCxt.pageChangeCount,
										  context->insertXactInfo,
										  context->insertTuple,
										  &upwards))
				{
					if (upwards)
					{
						wrongChangeCount = true;
					}
					else
					{
						return false;
					}
				}
				else
				{
					p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
					intCxt.pagePtr = p;
					intCxt.haveLock = true;
					needLock = false;
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

			if (imageFlag)
			{
				/*
				 * In BTREE_PAGE_FIND_IMAGE case we read a target targetLevel
				 * to the context.img without partial and read upper non-leaf
				 * pages to the context.parentImg partially.
				 *
				 * We consider it's OK to return page of lower targetLevel
				 * than required, if tree doesn't have enough height.  That's
				 * suitable for sequential scan (see btree_scan.c).
				 */
				if (level <= targetLevel)
				{
					useParentImg = false;
					intCxt.partial = NULL;
					Assert(!fastpath);
				}
				else
				{
					useParentImg = true;
					intCxt.partial = &context->partial;
				}
			}
			else
			{
				/*
				 * In other cases we can use the img to hold a partial data.
				 */
				useParentImg = false;
				intCxt.partial = &context->partial;
			}

			intCxt.haveLock = false;
			if (tryFlag)
			{
				ReadPageResult result;

				result = btree_find_try_read_page(context, intCxt.blkno,
												  intCxt.pageChangeCount,
												  useParentImg,
												  key, keyType,
												  intCxt.partial,
												  !fastpath);
				intCxt.pagePtr = useParentImg ? context->parentImg : context->img;
				if (result == ReadPageResultWrongPageChangeCount)
				{
					wrongChangeCount = true;
				}
				else if (result == ReadPageResultFailed)
				{
					return false;
				}
			}
			else
			{
				bool		result;

				result = btree_find_read_page(context, intCxt.blkno,
											  intCxt.pageChangeCount,
											  useParentImg, key, keyType,
											  intCxt.partial,
											  !fastpath);
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
				return false;

			/* Reload root information from the shared memory */
			desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
			desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
			desc->rootInfo.rootPageChangeCount = 0;
			if (tryFlag)
			{
				if (!o_btree_try_use_shmem(desc))
					return false;
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

		/*
		 * Fix broken rootPageBlkno split if needed.
		 */
		if (context->index == 0 &&
			O_PAGE_IS(intCxt.pagePtr, BROKEN_SPLIT) &&
			!noFixFlag)
		{
			Page		rootPageBlkno;

			Assert(intCxt.blkno == desc->rootInfo.rootPageBlkno);

			if (!intCxt.haveLock)
			{
				lock_page(desc->rootInfo.rootPageBlkno);
				intCxt.haveLock = true;
			}

			rootPageBlkno = O_GET_IN_MEMORY_PAGE(desc->rootInfo.rootPageBlkno);
			if (O_PAGE_IS(rootPageBlkno, BROKEN_SPLIT))
			{
				o_btree_split_fix_and_unlock(desc, desc->rootInfo.rootPageBlkno);
				intCxt.haveLock = false;
				continue;
			}
		}

		if (level > targetLevel || downlinkLocationFlag)
		{
			OBTreeFastPathFindResult result;

			result = page_find_downlink(&intCxt, &fastpathMeta, level,
										fastpath, &loc, &nonLeafHdr);

			Assert(result != OBTreeFastPathFindSlowpath);

			if (result == OBTreeFastPathFindFailure)
				return false;
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
				return false;
			else if (result == OBTreeFastPathFindRetry)
				continue;
			p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		}

		/* Place new item to the context */
		Assert(context->index < ORIOLEDB_MAX_DEPTH);

		context->items[context->index].locator = loc;
		context->items[context->index].blkno = intCxt.blkno;
		context->items[context->index].pageChangeCount = O_PAGE_GET_CHANGE_COUNT(intCxt.pagePtr);

		/* Save the lokey if needed */
		if (keepLokeyFlag && level > 1 &&
			BTREE_PAGE_LOCATOR_GET_OFFSET(intCxt.pagePtr, &loc) > 0)
		{
			OTuple		lokey;

			Assert(nonLeafHdr);

			BTREE_PAGE_READ_INTERNAL_TUPLE(lokey, intCxt.pagePtr, &loc);
			copy_fixed_key(context->desc, &context->lokey, lokey);
			BTREE_PAGE_FIND_SET(context, LOKEY_EXISTS);
			BTREE_PAGE_FIND_UNSET(context, LOKEY_SIBLING);
			BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
		}

		if (level != targetLevel && (!imageFlag || level > targetLevel) && !nonLeafHdr)
		{
			Assert(tryFlag);
			if (intCxt.haveLock)
			{
				unlock_page(intCxt.blkno);
				intCxt.haveLock = false;
			}
			return false;
		}

		if (level == targetLevel || (imageFlag && level <= targetLevel))
		{
			if (intCxt.haveLock)
			{
				if (!modifyFlag)
				{
					unlock_page(intCxt.blkno);
				}
				else if (level == 0 && fixLeafFlag)
				{
					/* called from o_btree_normal_modify() */
					/* try to fix incomplete split for leafs here */
					bool		relocked = false;

					Assert(!noFixFlag);
					Assert(modifyFlag);

					if (o_btree_split_is_incomplete(intCxt.blkno, &relocked))
					{
						o_btree_split_fix_and_unlock(desc, intCxt.blkno);
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
			return true;
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
				return false;
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

				if (imageFlag && level == targetLevel + 1)
				{
					/*
					 * Especial case, we load a leaf for image search. Now we
					 * need to save tuples for the iterators code from the
					 * parent.
					 */
					memcpy(context->parentImg, intCxt.pagePtr, ORIOLEDB_BLCKSZ);
					context->partial.isPartial = false;
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
			if (!O_TUPLE_IS_NULL(context->insertTuple))
			{
				bool		upwards = false;

				if (!lock_page_with_tuple(desc,
										  &intCxt->blkno,
										  &intCxt->pageChangeCount,
										  context->insertXactInfo,
										  context->insertTuple,
										  &upwards))
				{
					intCxt->haveLock = false;
					if (upwards)
					{
						return true;
					}
					else
					{
						intCxt->inserted = true;
						return true;
					}
				}
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
		unlock_page(intCxt->blkno);
	context->index--;
	intCxt->blkno = context->items[context->index].blkno;
	intCxt->pageChangeCount = context->items[context->index].pageChangeCount;
}

/*
 * Re-find the location of previously found key.  If search for modification,
 * assume lock was relesed (otherwise, no point to refind).
 */
bool
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

retry:

	if (BTREE_PAGE_FIND_IS(context, MODIFY))
	{
		Pointer		p;

		if (intCxt.pageChangeCount == InvalidOPageChangeCount)
			return find_page(context, key, keyType, level);

		if (!O_TUPLE_IS_NULL(context->insertTuple))
		{
			bool	upwards = false;

			if (!lock_page_with_tuple(desc,
									  &intCxt.blkno,
									  &intCxt.pageChangeCount,
									  context->insertXactInfo,
									  context->insertTuple,
									  &upwards))
			{
				if (upwards)
					return find_page(context, key, keyType, level);
				else
					return false;
			}
		}
		else
		{
			lock_page(intCxt.blkno);
		}
		p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		intCxt.haveLock = true;
		intCxt.pagePtr = p;
		if (PAGE_GET_LEVEL(p) != level ||
			O_PAGE_GET_CHANGE_COUNT(p) != intCxt.pageChangeCount ||
			(O_PAGE_IS(p, BROKEN_SPLIT) && intCxt.blkno == desc->rootInfo.rootPageBlkno))
		{
			unlock_page(intCxt.blkno);
			return find_page(context, key, keyType, level);
		}

		if (level == 0 && BTREE_PAGE_FIND_IS(context, FIX_LEAF_SPLIT))
		{
			/* called from o_btree_normal_modify() */
			/* try to fix incomplete split for leafs here */
			bool		relocked = false;

			Assert(!BTREE_PAGE_FIND_IS(context, NO_FIX_SPLIT));

			if (o_btree_split_is_incomplete(intCxt.blkno, &relocked))
			{
				intCxt.haveLock = false;
				o_btree_split_fix_and_unlock(desc, intCxt.blkno);
				goto retry;
			}
			else if (relocked)
			{
				intCxt.haveLock = false;
				unlock_page(intCxt.blkno);
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
			PAGE_GET_LEVEL(img) != level ||
			(O_PAGE_IS(img, BROKEN_SPLIT) && intCxt.blkno == desc->rootInfo.rootPageBlkno))
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
			if (intCxt.inserted)
				return false;
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
	return true;
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

	/* In this case, we shoudn't be in the rootPageBlkno... */
	Assert(context->index > 0);

	parentItem = &context->items[context->index - 1];
	item = &context->items[context->index];

	/* Try to get next item from the parent page */
	loc = context->items[context->index - 1].locator;

	if (BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
		BTREE_PAGE_LOCATOR_NEXT(context->parentImg, &loc);

	/* copy hikey */
	copy_fixed_hikey(desc, hikey, context->img);

	/* Try to load next page using next parent downlink */
	if (BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
	{
		OTuple		internalTuple;
		BTreeNonLeafTuphdr *tuphdr = NULL;
		bool		tup_loaded = true;

		tup_loaded = partial_load_chunk(&context->partial, context->parentImg,
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
										   false, &hikey->tuple, BTreeKeyNonLeafKey, NULL,
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
	 * way.  Should happend rarely.
	 */
	(void) find_page(context, hikey, BTreeKeyNonLeafKey, level);
	return true;
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
	/* In this case, we shoudn't be in the rootPageBlkno... */
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

			/*
			 * Tries to read image from parent downlink without find_page().
			 */
			if (BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
			{
				BTREE_PAGE_LOCATOR_PREV(context->parentImg, &loc);
				next_lokey_loaded = partial_load_chunk(&context->partial,
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
												   NULL,
												   true);

					if (success &&
						context->imgUndoLoc != InvalidUndoLocation &&
						prevLoc == context->imgUndoLoc)
					{
						parentItem->locator = loc;
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
							BTREE_PAGE_LOCATOR_LAST(context->img, &item->locator);
							return true;
						}
					}
				}
			}
		}

		(void) find_page(context, &hikey->tuple, BTreeKeyPageHiKey, level);

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
		 * Fetches lokey for the left sibling from the parent image.
		 */
		OTuple		result;

		BTREE_PAGE_READ_INTERNAL_TUPLE(result, context->parentImg, &ploc);
		return result;
	}
	else
	{
		/*
		 * Hikey of the left sibling of the parent.
		 */
		Assert(context->flags & BTREE_PAGE_FIND_LOKEY_EXISTS);
		return context->lokey.tuple;
	}
}

static Pointer
set_page_ptr(OBTreeFindPageContext *context, bool parent)
{
	Pointer		pagePtr;

	if (!parent)
	{
		context->imgEntry = NULL;
		pagePtr = context->img = context->imgData;
	}
	else
	{
		context->parentImgEntry = NULL;
		pagePtr = context->parentImg = context->parentImgData;
	}
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
static bool
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
