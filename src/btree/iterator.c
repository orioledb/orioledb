/*-------------------------------------------------------------------------
 *
 * interator.c
 *		Implemetation of orioledb B-tree iterator.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/interator.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/iterator.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "catalog/sys_trees.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/page_pool.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "utils/memutils.h"

/* Iterates through undo images */
typedef struct
{
	BTreeIterator *it;
	/* a current page image from undo log */
	char		image[ORIOLEDB_BLCKSZ];
	/* a lokey of the image for backward scan */
	OFixedKey	lokey;
	/* a base undo location */
	UndoLocation baseLoc;
	/* undo location of the image */
	UndoLocation imageUndoLoc;
	/* is the image leftmost on the base location */
	bool		leftmost;
	/* is the image rightmost on the base location */
	bool		rightmost;
} UndoIterator;

struct BTreeIterator
{
	OBTreeFindPageContext context;
	CommitSeqNo csn;
	UndoIterator undoIt;
	/* scan direction of current iterator: forward or backward */
	ScanDirection scanDir;
	/* current tuple location in UndoIterator */
	BTreePageItemLocator undoLoc;
	/* do we have to combine results from both current and undo pages? */
	bool		combinedResult;
	/* do we need to combine results in the current page? */
	bool		combinedPage;
	/* memory context for returned tuples */
	MemoryContext tupleCxt;
	/* callback for fetching tuple version */
	TupleFetchCallback fetchCallback;
	void	   *fetchCallbackArg;
#ifdef USE_ASSERT_CHECKING
	/* additional check for iteration order */
	OFixedTuple prevTuple;
#endif
};

static void get_next_combined_location(BTreeIterator *it);
static void load_page_from_undo(BTreeIterator *it, void *key, BTreeKeyType kind);
static bool btree_iterator_check_load_next_page(BTreeIterator *it);
static OTuple o_btree_iterator_fetch_internal(BTreeIterator *it,
											  CommitSeqNo *tupleCsn);
static bool o_btree_interator_can_fetch_from_undo(BTreeDescr *desc, BTreeIterator *it);
static bool can_fetch_from_undo(BTreeIterator *it);
static void undo_it_create(UndoIterator *undoIt, BTreeIterator *it);
static void undo_it_init(UndoIterator *undoIt, UndoLocation location, void *key, BTreeKeyType kind);
static bool undo_it_next_page(BTreeDescr *desc, UndoIterator *undoIt);
static bool undo_it_switch(BTreeDescr *desc, UndoIterator *undoIt, UndoLocation location);
static void undo_it_find_internal(UndoIterator *undoIt, void *key, BTreeKeyType kind);

#define IT_IS_BACKWARD(it) ((it)->scanDir == BackwardScanDirection)
#define IT_IS_FORWARD(it) ((it)->scanDir == ForwardScanDirection)

#define IS_LAST_PAGE(page, it) ((IT_IS_FORWARD((it)) && O_PAGE_IS(page, RIGHTMOST)) \
								|| (IT_IS_BACKWARD((it)) && O_PAGE_IS(page, LEFTMOST)))

#define IT_NEXT_OFFSET(it, loc) \
	do { \
		if (IT_IS_FORWARD(it)) \
			BTREE_PAGE_LOCATOR_NEXT((it)->context.img, (loc)); \
		else if (IT_IS_BACKWARD(it)) \
			BTREE_PAGE_LOCATOR_PREV((it)->context.img, (loc)); \
	} while (0); \

#define UNDO_IT_NEXT_OFFSET(undoIt, loc) \
	do { \
		if (IT_IS_FORWARD(it)) \
			BTREE_PAGE_LOCATOR_NEXT((undoIt)->image, (loc)); \
		else if (IT_IS_BACKWARD(it)) \
			BTREE_PAGE_LOCATOR_PREV((undoIt)->image, (loc)); \
	} while (0); \

/*
 * Fetches tuple from the tree with given CSN snapshot.  Tuple is allocated
 * in the given context.  Leaf page is found using the given hint (if provided).
 * Given hint is adjusted with relevant leaf page.
 */
OTuple
o_btree_find_tuple_by_key_cb(BTreeDescr *desc, void *key,
							 BTreeKeyType kind, CommitSeqNo readCsn,
							 CommitSeqNo *outCsn, MemoryContext mcxt,
							 BTreeLocationHint *hint,
							 bool *deleted,
							 TupleFetchCallback cb,
							 void *arg)
{
	BTreePageItemLocator loc;
	OBTreeFindPageContext context;
	char	   *img = context.img;
	BTreePageHeader *header = (BTreePageHeader *) img;
	bool		combinedResult = false;
	OTuple		result;

	if (COMMITSEQNO_IS_NORMAL(readCsn))
		combinedResult = !have_current_undo();

	init_page_find_context(&context, desc,
						   combinedResult ? COMMITSEQNO_INPROGRESS : readCsn, BTREE_PAGE_FIND_FETCH);

	/* Use page location hint if provided */
	if (hint && OInMemoryBlknoIsValid(hint->blkno))
		refind_page(&context, key, kind, 0, hint->blkno, hint->pageChangeCount);
	else
		(void) find_page(&context, key, kind, 0);

	loc = context.items[context.index].locator;

	/* Adjust hint if given */
	if (hint)
	{
		hint->blkno = context.items[context.index].blkno;
		hint->pageChangeCount = context.items[context.index].pageChangeCount;
	}

	if (combinedResult && header->csn >= readCsn)
	{
		if (BTREE_PAGE_LOCATOR_IS_VALID(img, &loc))
		{
			BTreeLeafTuphdr *tupHdr;
			OTuple		curTuple;
			int			result_size;
			OTupleXactInfo xactInfo;

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, curTuple, img, &loc);
			tupHdr = (BTreeLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(img, &loc);
			xactInfo = tupHdr->xactInfo;

			if (XACT_INFO_OXID_IS_CURRENT(xactInfo))
			{
				if (outCsn)
					*outCsn = COMMITSEQNO_INPROGRESS;

				if (deleted)
					*deleted = (tupHdr->deleted != BTreeLeafTupleNonDeleted);

				if (tupHdr->deleted == BTreeLeafTupleNonDeleted)
				{
					result_size = o_btree_len(desc, curTuple, OTupleLength);
					result.data = (Pointer) MemoryContextAlloc(mcxt, result_size);
					memcpy(result.data, curTuple.data, result_size);
					result.formatFlags = curTuple.formatFlags;
					return result;
				}
				else
				{
					O_TUPLE_SET_NULL(result);
					return result;
				}
			}
		}
		read_page_from_undo(desc, img, header->undoLocation, readCsn,
							key, kind, NULL);
		btree_page_search(desc, img, key, kind, NULL, &loc);
		page_locator_find_real_item(img, NULL, &loc);
	}

	if (BTREE_PAGE_LOCATOR_IS_VALID(img, &loc))
	{
		BTreeLeafTuphdr *tupHdr;
		OTuple		curTuple;
		int			cmp;

		BTREE_PAGE_READ_LEAF_ITEM(tupHdr, curTuple, img, &loc);
		cmp = o_btree_cmp(desc, key, kind, &curTuple, BTreeKeyLeafTuple);

		if (deleted)
			*deleted = (tupHdr->deleted != BTreeLeafTupleNonDeleted);

		if (cmp == 0)
			return o_find_tuple_version(desc, img, &loc, readCsn, outCsn,
										mcxt, cb, arg);
	}

	/* Tuple isn't found */
	O_TUPLE_SET_NULL(result);
	return result;
}

OTuple
o_btree_find_tuple_by_key(BTreeDescr *desc, void *key, BTreeKeyType kind,
						  CommitSeqNo readCsn, CommitSeqNo *outCsn,
						  MemoryContext mcxt, BTreeLocationHint *hint)
{
	return o_btree_find_tuple_by_key_cb(desc, key, kind, readCsn, outCsn,
										mcxt, hint, NULL, NULL, NULL);
}


/*
 * Finds appropriate tuple version in the undo chain.
 */
OTuple
o_find_tuple_version(BTreeDescr *desc, Page p, BTreePageItemLocator *loc,
					 CommitSeqNo csn, CommitSeqNo *tupleCsn,
					 MemoryContext mcxt, TupleFetchCallback cb,
					 void *arg)
{
	BTreeLeafTuphdr tupHdr,
			   *tupHdrPtr;
	OTuple		curTuple;
	OTuple		result;
	int			result_size;
	UndoLocation undoLocation = InvalidUndoLocation;
	bool		curTupleAllocated = false;
	MemoryContext prevMctx;

	prevMctx = MemoryContextSwitchTo(mcxt);

	BTREE_PAGE_READ_LEAF_ITEM(tupHdrPtr, curTuple, p, loc);
	tupHdr = *tupHdrPtr;
	(void) find_non_lock_only_undo_record(&tupHdr);

	Assert(COMMITSEQNO_IS_NORMAL(csn) || COMMITSEQNO_IS_INPROGRESS(csn));

	while (true)
	{
		OTupleXactInfo xactInfo = tupHdr.xactInfo;
		bool		txIsFinished = XACT_INFO_IS_FINISHED(xactInfo);
		CommitSeqNo tupcsn;

		tupcsn = XACT_INFO_MAP_CSN(xactInfo);
		if (tupleCsn)
		{
			if (COMMITSEQNO_IS_NORMAL(tupcsn))
				*tupleCsn = COMMITSEQNO_IS_NORMAL(csn) ? Max(csn, tupcsn + 1) : COMMITSEQNO_MAX_NORMAL - 1;
			else if (COMMITSEQNO_IS_FROZEN(tupcsn))
				*tupleCsn = COMMITSEQNO_IS_NORMAL(csn) ? csn : COMMITSEQNO_MAX_NORMAL - 1;
			else
				*tupleCsn = COMMITSEQNO_INPROGRESS;
		}

		if (cb)
		{
			TupleFetchCallbackResult cbResult;
			bool		version_check = !txIsFinished;
			OXid		tupOxid = version_check ? XACT_INFO_GET_OXID(xactInfo) : InvalidOXid;
			TupleFetchCallbackCheckType check_type = version_check ?
				OTupleFetchCallbackVersionCheck :
				OTupleFetchCallbackKeyCheck;

			cbResult = cb(curTuple, tupOxid, csn, arg, check_type);

			if (cbResult == OTupleFetchMatch)
				break;
			if (cbResult == OTupleFetchNotMatch)
			{
				O_TUPLE_SET_NULL(result);
				MemoryContextSwitchTo(prevMctx);
				return result;
			}
		}

		if (!txIsFinished)
		{
			if (!cb)
			{
				if (COMMITSEQNO_IS_INPROGRESS(csn))
					break;

				/*
				 * We see the changes made by our transaction.  Exception are
				 * changes made by current command unless we're dealing with
				 * system tree.
				 */
				if (XACT_INFO_GET_OXID(xactInfo) == get_current_oxid_if_any() &&
					(UndoLocationGetValue(tupHdr.undoLocation) <= saved_undo_location ||
					 IS_SYS_TREE_OIDS(desc->oids)) &&
					csn != COMMITSEQNO_MAX_NORMAL)
					break;
			}
		}
		else
		{
			if (COMMITSEQNO_IS_INPROGRESS(csn))
				break;
		}

		if (!COMMITSEQNO_IS_INPROGRESS(tupcsn) &&
			!COMMITSEQNO_IS_ABORTED(tupcsn) &&
			(tupcsn < csn || COMMITSEQNO_IS_INPROGRESS(csn)))
			break;

		undoLocation = tupHdr.undoLocation;

		if (!UndoLocationIsValid(undoLocation))
		{
			O_TUPLE_SET_NULL(result);
			MemoryContextSwitchTo(prevMctx);
			return result;
		}

		if (tupHdr.deleted != BTreeLeafTupleNonDeleted ||
			XACT_INFO_IS_LOCK_ONLY(tupHdr.xactInfo))
		{
			get_prev_leaf_header_from_undo(&tupHdr, true);
		}
		else
		{
			if (curTupleAllocated)
				pfree(curTuple.data);
			get_prev_leaf_header_and_tuple_from_undo(&tupHdr, &curTuple, 0);
			curTupleAllocated = true;
		}

		Assert(UNDO_REC_EXISTS(undoLocation));
	}

	if (COMMITSEQNO_IS_NON_DELETED(csn))
	{
		if (tupHdr.deleted != BTreeLeafTupleNonDeleted &&
			XACT_INFO_IS_FINISHED(tupHdr.xactInfo))
		{
			O_TUPLE_SET_NULL(result);
			MemoryContextSwitchTo(prevMctx);
			return result;
		}
	}
	else if (tupHdr.deleted != BTreeLeafTupleNonDeleted && !cb)
	{
		O_TUPLE_SET_NULL(result);
		MemoryContextSwitchTo(prevMctx);
		return result;
	}

	if (!curTupleAllocated)
	{
		result_size = o_btree_len(desc, curTuple, OTupleLength);
		/* TODO: check result tuple size */
		result.data = (Pointer) MemoryContextAlloc(mcxt, result_size);
		memcpy(result.data, curTuple.data, result_size);
		result.formatFlags = curTuple.formatFlags;
	}
	else
	{
		result = curTuple;
	}

	Assert(!UndoLocationIsValid(undoLocation) || UNDO_REC_EXISTS(undoLocation));
	MemoryContextSwitchTo(prevMctx);
	return result;
}

BTreeIterator *
o_btree_iterator_create(BTreeDescr *desc, void *key, BTreeKeyType kind,
						CommitSeqNo csn, ScanDirection scanDir)
{
	BTreeIterator *it;
	uint16		findFlags = BTREE_PAGE_FIND_IMAGE;

	it = (BTreeIterator *) palloc(sizeof(BTreeIterator));
	it->combinedResult = !have_current_undo() && COMMITSEQNO_IS_NORMAL(csn);
	it->csn = csn;
	it->scanDir = scanDir;
	it->tupleCxt = CurrentMemoryContext;
	it->fetchCallback = NULL;
	it->fetchCallbackArg = NULL;
	BTREE_PAGE_LOCATOR_SET_INVALID(&it->undoLoc);
#ifdef USE_ASSERT_CHECKING
	O_TUPLE_SET_NULL(it->prevTuple.tuple);
#endif

	undo_it_create(&it->undoIt, it);

	if (IT_IS_BACKWARD(it))
		findFlags |= BTREE_PAGE_FIND_KEEP_LOKEY;

	init_page_find_context(&it->context, desc,
						   it->combinedResult ? COMMITSEQNO_INPROGRESS : csn, findFlags);

	if (key == NULL)
	{
		if (IT_IS_FORWARD(it))
			kind = BTreeKeyNone;
		else
			kind = BTreeKeyRightmost;
	}

	find_page(&it->context, key, kind, 0);

	if (key != NULL && IT_IS_BACKWARD(it))
	{
		BTreePageItemLocator *loc = &it->context.items[it->context.index].locator;
		bool		make_dec = false;

		/*
		 * From btree_page_binary_search(): "When nextkey is false (this
		 * case), we are looking for the first item >= scankey."
		 *
		 * If it's next item than decrement item offset. In case item ==
		 * search key no need to do this.
		 */
		Assert(BTREE_PAGE_LOCATOR_GET_OFFSET(it->context.img, loc) <= BTREE_PAGE_ITEMS_COUNT(it->context.img));

		if (BTREE_PAGE_LOCATOR_GET_OFFSET(it->context.img, loc) == BTREE_PAGE_ITEMS_COUNT(it->context.img))
			make_dec = true;
		else
		{
			OTuple		tup;

			BTREE_PAGE_READ_TUPLE(tup, it->context.img, loc);
			if (o_btree_cmp(desc, key, kind, &tup, BTreeKeyLeafTuple) < 0)
				make_dec = true;
		}

		if (make_dec)
			BTREE_PAGE_LOCATOR_PREV(it->context.img, loc);
	}

	load_page_from_undo(it, key,
						kind != BTreeKeyRightmost ? kind : BTreeKeyNone);

	return it;
}

void
o_btree_iterator_set_tuple_ctx(BTreeIterator *it, MemoryContext tupleCxt)
{
	it->tupleCxt = tupleCxt;
}

void
o_btree_iterator_set_callback(BTreeIterator *it,
							  TupleFetchCallback callback,
							  void *arg)
{
	it->fetchCallback = callback;
	it->fetchCallbackArg = arg;
}

OTuple
o_btree_iterator_fetch(BTreeIterator *it, CommitSeqNo *tupleCsn,
					   void *end, BTreeKeyType endType,
					   bool endIsIncluded, BTreeLocationHint *hint)
{
	BTreeDescr *desc = it->context.desc;
	OTuple		result;

	result = o_btree_iterator_fetch_internal(it, tupleCsn);

	if (!O_TUPLE_IS_NULL(result) && end != NULL)
	{
		int			cmp = o_btree_cmp(desc, &result, BTreeKeyLeafTuple, end, endType);

		if (IT_IS_BACKWARD(it))
			cmp *= -1;

		if (cmp >= (endIsIncluded ? 1 : 0))
		{
			pfree(result.data);
			O_TUPLE_SET_NULL(result);
			return result;
		}
	}

#ifdef USE_ASSERT_CHECKING
	if (!O_TUPLE_IS_NULL(result))
	{
		if (!O_TUPLE_IS_NULL(it->prevTuple.tuple))
		{
			int			cmp;

			cmp = o_btree_cmp(desc, &it->prevTuple.tuple, BTreeKeyLeafTuple,
							  &result, BTreeKeyLeafTuple);

			Assert((IT_IS_FORWARD(it) && cmp < 0) || cmp > 0);
		}
		copy_fixed_tuple(desc, &it->prevTuple, result);
	}
#endif

	if (hint)
	{
		hint->blkno = it->context.items[it->context.index].blkno;
		hint->pageChangeCount = it->context.items[it->context.index].pageChangeCount;
	}

	return result;
}

/*
 * Free resouces associated with iterator.
 */
void
btree_iterator_free(BTreeIterator *it)
{
	pfree(it);
}

/*
 * Load page from undo for combined result.
 */
static void
load_page_from_undo(BTreeIterator *it, void *key, BTreeKeyType kind)
{
	OBTreeFindPageContext *context = &it->context;
	BTreePageHeader *header = (BTreePageHeader *) context->img;
	BTreeDescr *desc = context->desc;

	if (it->combinedResult && header->csn >= it->csn)
	{
		undo_it_init(&it->undoIt, header->undoLocation, key, kind);

		if (key)
		{
			btree_page_search(desc,
							  it->undoIt.image,
							  key, kind, NULL,
							  &it->undoLoc);
			page_locator_find_real_item(it->undoIt.image, NULL,
										&it->undoLoc);

			if (IT_IS_BACKWARD(it))
			{
				OTuple		founded;
				OffsetNumber undoOffset;

				BTREE_PAGE_READ_TUPLE(founded, it->undoIt.image, &it->undoLoc);

				/*
				 * From btree_page_binary_search(): "When nextkey is false
				 * (this case), we are looking for the first item >= scankey."
				 *
				 * If it's next item than decrement item offset. In case item
				 * == key bound no need to do this.
				 */
				undoOffset = BTREE_PAGE_LOCATOR_GET_OFFSET(it->undoIt.image, &it->undoLoc);
				Assert(undoOffset <= BTREE_PAGE_ITEMS_COUNT(it->undoIt.image));
				if (undoOffset == BTREE_PAGE_ITEMS_COUNT(it->undoIt.image) ||
					o_btree_cmp(desc, key, kind, &founded, BTreeKeyLeafTuple))
					BTREE_PAGE_LOCATOR_PREV(it->undoIt.image, &it->undoLoc);
			}

		}
		else if (IT_IS_FORWARD(it))
		{
			BTREE_PAGE_LOCATOR_FIRST(it->undoIt.image, &it->undoLoc);
		}
		else
		{
			BTREE_PAGE_LOCATOR_LAST(it->undoIt.image, &it->undoLoc);
		}

		it->combinedPage = true;
		get_next_combined_location(it);
	}
	else
	{
		it->combinedPage = false;
	}
}

/*
 * Find the next tuple location for result combination.  It should have
 * current oxid.
 */
static void
get_next_combined_location(BTreeIterator *it)
{
	OBTreeFindPageContext *context = &it->context;
	OXid		oxid = get_current_oxid_if_any();
	BTreePageItemLocator *loc = &context->items[context->index].locator;
	Page		img = context->img;

	if (!BTREE_PAGE_LOCATOR_IS_VALID(img, loc))
		return;

	while (BTREE_PAGE_LOCATOR_IS_VALID(img, loc))
	{
		BTreeLeafTuphdr *tupHdr;

		tupHdr = (BTreeLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(img, loc);

		if (XACT_INFO_OXID_EQ(tupHdr->xactInfo, oxid))
			break;

		IT_NEXT_OFFSET(it, loc);
	}
}

/*
 * Fetch next tuple without checking for end condition.
 */
static OTuple
o_btree_iterator_fetch_internal(BTreeIterator *it, CommitSeqNo *tupleCsn)
{
	BTreeDescr *desc = it->context.desc;
	OBTreeFindPageContext *context = &it->context;
	OBtreePageFindItem *leaf_item;
	Page		img = context->img,
				hImg = it->undoIt.image;
	OTuple		result,
				itup,
				htup;
	int			cmp;

	while (true)
	{
		if (!btree_iterator_check_load_next_page(it))
		{
			O_TUPLE_SET_NULL(result);
			return result;
		}

		leaf_item = &context->items[context->index];

		if (it->combinedPage)
		{
			if (!BTREE_PAGE_LOCATOR_IS_VALID(hImg, &it->undoLoc))
				cmp = -1;
			else if (!BTREE_PAGE_LOCATOR_IS_VALID(img, &leaf_item->locator))
				cmp = 1;
			else
			{
				BTREE_PAGE_READ_LEAF_TUPLE(itup, img, &leaf_item->locator);
				BTREE_PAGE_READ_LEAF_TUPLE(htup, hImg, &it->undoLoc);
				cmp = o_btree_cmp(desc, &itup, BTreeKeyLeafTuple, &htup, BTreeKeyLeafTuple);
				if (IT_IS_BACKWARD(it))
					cmp *= -1;	/* mirror compare logic */
			}

			if (cmp <= 0)
			{
				result = o_find_tuple_version(desc, img,
											  &leaf_item->locator,
											  it->csn, tupleCsn, it->tupleCxt,
											  it->fetchCallback,
											  it->fetchCallbackArg);

				IT_NEXT_OFFSET(it, &leaf_item->locator);

				get_next_combined_location(it);

				if (cmp == 0)
					UNDO_IT_NEXT_OFFSET(&it->undoIt, &it->undoLoc);

				if (!O_TUPLE_IS_NULL(result))
					return result;
			}
			else
			{
				result = o_find_tuple_version(desc, hImg,
											  &it->undoLoc,
											  it->csn, tupleCsn, it->tupleCxt,
											  it->fetchCallback,
											  it->fetchCallbackArg);

				UNDO_IT_NEXT_OFFSET(&it->undoIt, &it->undoLoc);

				if (!O_TUPLE_IS_NULL(result))
					return result;
			}
		}
		else
		{
			result = o_find_tuple_version(desc, context->img,
										  &leaf_item->locator,
										  it->csn, tupleCsn,
										  it->tupleCxt,
										  it->fetchCallback,
										  it->fetchCallbackArg);

			IT_NEXT_OFFSET(it, &leaf_item->locator);

			if (!O_TUPLE_IS_NULL(result))
				return result;
		}
	}

	O_TUPLE_SET_NULL(result);
	return result;				/* unreachable */
}

/*
 * Check and load the next tree page if needed.  Works with both normal and undo
 * pages.  Return true on success.  False means there is nothing more to read.
 */
static bool
btree_iterator_check_load_next_page(BTreeIterator *it)
{
	OBTreeFindPageContext *context = &it->context;
	Page		img = context->img,
				hImg = it->undoIt.image;
	BTreePageHeader *header = (BTreePageHeader *) img;
	BTreeDescr *desc = context->desc;
	OFixedKey	key_buf;

	if (o_btree_interator_can_fetch_from_undo(context->desc, it))
		return true;

	while (!BTREE_PAGE_LOCATOR_IS_VALID(img, &context->items[context->index].locator))
	{
		bool		step_result;

		if (IS_LAST_PAGE(img, it))
			return false;

		if (IT_IS_FORWARD(it))
			step_result = find_right_page(context, &key_buf);
		else
			step_result = find_left_page(context, &key_buf);

		if (!step_result)
			return false;

		if (it->combinedResult && header->csn >= it->csn)
		{
			bool		reload = true;

			if (it->combinedPage)
				reload = !o_btree_interator_can_fetch_from_undo(context->desc, it);

			if (reload)
			{
				/*
				 * We can not to use current undo images iterator.
				 */

				/* finds a tuple to resume */
				undo_it_init(&it->undoIt,
							 header->undoLocation,
							 &key_buf,
							 (IT_IS_FORWARD(it) ? BTreeKeyNonLeafKey : BTreeKeyPageHiKey));

				btree_page_search(desc, hImg, (Pointer) &key_buf.tuple,
								  BTreeKeyNonLeafKey, NULL,
								  &it->undoLoc);
				page_locator_find_real_item(hImg, NULL,
											&it->undoLoc);

				if (IT_IS_BACKWARD(it))
					BTREE_PAGE_LOCATOR_PREV(hImg, &it->undoLoc);
			}
			get_next_combined_location(it);
			it->combinedPage = true;
		}
		else
		{
			it->combinedPage = false;
		}

		if (can_fetch_from_undo(it))
			break;
	}

	return true;
}

/*
 * Can we fetch more pages form undo page image?
 */
static bool
o_btree_interator_can_fetch_from_undo(BTreeDescr *desc, BTreeIterator *it)
{
	Page		hImg = it->undoIt.image,
				img = it->context.img;
	BTreePageHeader *header = (BTreePageHeader *) img;

	if (!BTREE_PAGE_LOCATOR_IS_VALID(hImg, &it->undoLoc))
	{
		bool		can_switch = it->combinedResult && header->csn >= it->csn;

		/* switch to next history page if we can */
		if (undo_it_next_page(it->context.desc, &it->undoIt) ||
			(can_switch && undo_it_switch(desc, &it->undoIt, header->undoLocation)))
		{
			if (IT_IS_FORWARD(it))
				BTREE_PAGE_LOCATOR_FIRST(hImg, &it->undoLoc);
			else
				BTREE_PAGE_LOCATOR_LAST(hImg, &it->undoLoc);

			Assert(it->combinedPage);
			Assert(BTREE_PAGE_LOCATOR_IS_VALID(hImg, &it->undoLoc));
		}
	}

	return can_fetch_from_undo(it);
}

/*
 * Check if `historicalImg` still contains tuples corresponding to the `img`
 * key range.
 */
static bool
can_fetch_from_undo(BTreeIterator *it)
{
	BTreeDescr *desc = it->context.desc;
	OBTreeFindPageContext *context = &it->context;
	OTuple		htup;
	int			cmp;

	/* False if no tuples to fetch */
	if (!BTREE_PAGE_LOCATOR_IS_VALID(it->undoIt.image, &it->undoLoc))
		return false;

	/* True if `img` key range is inifity in the required direction */
	if (IS_LAST_PAGE(context->img, it))
		return true;

	/* Compare the next tuple with corresponding key range bound */
	BTREE_PAGE_READ_LEAF_TUPLE(htup, it->undoIt.image, &it->undoLoc);
	if (IT_IS_FORWARD(it))
	{
		OTuple		hikey;

		BTREE_PAGE_GET_HIKEY(hikey, context->img);
		cmp = o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey, &htup, BTreeKeyLeafTuple);
		return cmp > 0;
	}
	else						/* backward iterator case */
	{
		OTuple		lokey = btree_find_context_lokey(context);

		cmp = o_btree_cmp(desc, &lokey, BTreeKeyNonLeafKey, &htup, BTreeKeyLeafTuple);
		return cmp <= 0;
	}
}

/*
 * Iterate over leaf page tuples without considering undo log.  Deleted tuples
 * are reported as NULLs.  So, the separate `*end` flag indicates finish of
 * iterations.
 */
OTuple
btree_iterate_raw(BTreeIterator *it, void *end, BTreeKeyType endKind,
				  bool endInclude, bool *scanEnd, BTreeLocationHint *hint)
{
	BTreeLeafTuphdr *tupHdr;
	OBTreeFindPageContext *context = &it->context;
	Page		img = context->img;
	OTuple		result;
	OFixedKey	key_buf;

	*scanEnd = false;

	while (true)
	{
		BTreePageItemLocator *loc = &context->items[context->index].locator;

		if (BTREE_PAGE_LOCATOR_IS_VALID(img, loc))
		{
			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, result, context->img, loc);
			IT_NEXT_OFFSET(it, loc);

			if (end != NULL && endKind != BTreeKeyNone)
			{
				BTreeDescr *desc = it->context.desc;
				int			cmp;

				cmp = o_btree_cmp(desc, &result, BTreeKeyLeafTuple, end, endKind);
				if (cmp > 0 || (cmp == 0 && !endInclude))
				{
					*scanEnd = true;
					O_TUPLE_SET_NULL(result);
					return result;
				}
			}

			if (tupHdr->deleted == BTreeLeafTupleNonDeleted)
			{
				if (hint)
				{
					hint->blkno = it->context.items[it->context.index].blkno;
					hint->pageChangeCount = it->context.items[it->context.index].pageChangeCount;
				}
				return result;
			}
			else
			{
				O_TUPLE_SET_NULL(result);
				return result;
			}
		}

		if (IS_LAST_PAGE(img, it))
		{
			*scanEnd = true;
			O_TUPLE_SET_NULL(result);
			return result;
		}

		if (IT_IS_FORWARD(it))
		{
			if (!find_right_page(context, &key_buf))
			{
				O_TUPLE_SET_NULL(result);
				return result;
			}
		}
		else
		{
			if (!find_left_page(context, &key_buf))
			{
				O_TUPLE_SET_NULL(result);
				return result;
			}
		}
	}
	O_TUPLE_SET_NULL(result);
	return result;				/* unreachable */
}

/*
 * Fills basic fields of undo iterator
 */
static void
undo_it_create(UndoIterator *undoIt, BTreeIterator *it)
{
	Assert(it->scanDir != NoMovementScanDirection);

	undoIt->it = it;
	undoIt->rightmost = true;
	undoIt->leftmost = true;
	undoIt->baseLoc = InvalidUndoLocation;
	undoIt->imageUndoLoc = InvalidUndoLocation;
}

/*
 * Initializes the undo iterator
 */
static void
undo_it_init(UndoIterator *undoIt, UndoLocation location, void *key, BTreeKeyType kind)
{
	undoIt->baseLoc = location;
	undo_it_find_internal(undoIt, key, kind);
}

/*
 * Tries to switch to next undo page from baseLoc
 */
static bool
undo_it_next_page(BTreeDescr *desc, UndoIterator *undoIt)
{
	BTreeKeyType kind;
	OFixedKey	key;
	UndoLocation prevLoc;

	if (!UndoLocationIsValid(undoIt->baseLoc))
		return false;

	/* Get bound key from the current undo page */
	if (IT_IS_FORWARD(undoIt->it))
	{
		if (undoIt->rightmost)
			return false;		/* no more pages */
		copy_fixed_hikey(desc, &key, undoIt->image);
		kind = BTreeKeyNonLeafKey;
	}
	else
	{
		if (undoIt->leftmost)
			return false;		/* no more pages */
		copy_fixed_key(desc, &key, undoIt->lokey.tuple);
		kind = BTreeKeyPageHiKey;
	}

	Assert(!IS_LAST_PAGE(undoIt->image, undoIt->it));

	prevLoc = undoIt->imageUndoLoc;

	/* Find undo page corresponding to the key from the undoIt->baseLoc */
	undo_it_find_internal(undoIt, &key.tuple, kind);

	/* Did we manage to find another page? */
	if (prevLoc != undoIt->imageUndoLoc)
	{
		return true;
	}
	else
	{
		Assert(undoIt->rightmost || undoIt->leftmost);
		return false;
	}
}

/*
 * Tries to switch to the next baseLoc
 */
static bool
undo_it_switch(BTreeDescr *desc, UndoIterator *undoIt, UndoLocation location)
{
	bool		is_forward = IT_IS_FORWARD(undoIt->it);

	if (!UndoLocationIsValid(location))
		return false;

	if (!UndoLocationIsValid(undoIt->baseLoc))
	{
		BTreeKeyType kind = is_forward ? BTreeKeyNone : BTreeKeyRightmost;

		/* load of full undo */
		undoIt->baseLoc = location;
		undo_it_find_internal(undoIt, NULL, kind);
		return true;
	}
	/* else we must find next page in undo */

	undoIt->baseLoc = location;
	if (IS_LAST_PAGE(undoIt->image, undoIt->it))
	{
		/* there is no more pages expected */
		return false;
	}
	else
	{
		/*
		 * We need to find the page, which hikey moves to the required
		 * direction in comparison with previous undo page.
		 */
		if (O_PAGE_IS(undoIt->image, RIGHTMOST))
		{
			Assert(!is_forward);

			undo_it_find_internal(undoIt, NULL, BTreeKeyRightmost);

			if (O_PAGE_IS(undoIt->image, RIGHTMOST))
			{
				/*
				 * we expect that a loaded rightmost page is equal to the
				 * previous
				 */
				return undo_it_next_page(desc, undoIt);
			}
			return true;
		}
		else
		{
			OFixedKey	prev_hikey;
			int			cmp;

			/* copy the previous page hikey */
			copy_fixed_hikey(desc, &prev_hikey, undoIt->image);
			undo_it_find_internal(undoIt, &prev_hikey.tuple, BTreeKeyPageHiKey);

			if (O_PAGE_IS(undoIt->image, RIGHTMOST))
			{
				cmp = 1;
			}
			else
			{
				OTuple		image_hikey;

				BTREE_PAGE_GET_HIKEY(image_hikey, undoIt->image);
				cmp = o_btree_cmp(desc, &image_hikey, BTreeKeyNonLeafKey,
								  &prev_hikey.tuple, BTreeKeyNonLeafKey);
			}

			if (cmp == 0)
				return undo_it_next_page(desc, undoIt);

			/* in forward case we must load next page */
			Assert(!is_forward || cmp > 0);
			/* in backward case we must load previous page */
			Assert(is_forward || cmp < 0);

			return true;
		}
	}
}

/*
 * Find in undo log page corresponding to the given key.
 */
static void
undo_it_find_internal(UndoIterator *undoIt, void *key, BTreeKeyType kind)
{
	BTreePageHeader *header;
	CommitSeqNo rec_csn;
	UndoLocation rec_undo_loc,
				undoLocation;
	bool		left,
				right;

	undoLocation = undoIt->baseLoc;
	undoIt->leftmost = true;
	undoIt->rightmost = true;

	while (true)
	{
		/* Load the next page item from page-level undo item */
		if (undoIt->it->scanDir == ForwardScanDirection)
			get_page_from_undo(undoIt->it->context.desc, undoLocation, key, kind,
							   undoIt->image, &left, &right, NULL, NULL, NULL);
		else
			get_page_from_undo(undoIt->it->context.desc, undoLocation, key, kind,
							   undoIt->image, &left, &right, &undoIt->lokey, NULL, NULL);

		undoIt->rightmost = (undoIt->rightmost && right) || O_PAGE_IS(undoIt->image, RIGHTMOST);
		undoIt->leftmost = (undoIt->leftmost && left) || O_PAGE_IS(undoIt->image, LEFTMOST);
		undoIt->imageUndoLoc = O_UNDO_GET_IMAGE_LOCATION(undoLocation, left);
		Assert(UNDO_REC_EXISTS(undoLocation));

		header = (BTreePageHeader *) undoIt->image;
		rec_csn = header->csn;
		rec_undo_loc = header->undoLocation;

		/* Check if we need to visit next page-level undo item */
		if (COMMITSEQNO_IS_NORMAL(rec_csn) && rec_csn >= undoIt->it->csn)
		{
			undoLocation = rec_undo_loc;
			continue;
		}
		else
		{
			break;
		}
	}

	/* if O_PAGE_IS(undoIt->image, RIGHTMOST) then undoIt->righmost == true */
	Assert(!O_PAGE_IS(undoIt->image, RIGHTMOST) || undoIt->rightmost);
	/* if O_PAGE_IS(undoIt->image, LEFTMOST) then undoIt->leftmost == true */
	Assert(!O_PAGE_IS(undoIt->image, LEFTMOST) || undoIt->leftmost);
}
