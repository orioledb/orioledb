/*-------------------------------------------------------------------------
 *
 * iterator.c
 *		Implementation of orioledb B-tree iterator.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/iterator.c
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
#include "tableam/descr.h"
#include "tableam/key_range.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/page_pool.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

/* Iterates through undo images */
typedef struct
{
	BTreeIterator *it;
	/* a current page image from undo log */
	char		image[ORIOLEDB_BLCKSZ];
	/* a lokey of the image for backward scan */
	OFixedKey	lokey;
	/* a base undo location, from the data page header */
	UndoLocation baseLoc;
	/* undo location of the `image` in this struct */
	UndoLocation imageUndoLoc;
	/* is the image leftmost on the base location */
	bool		leftmost;
	/* is the image rightmost on the base location */
	bool		rightmost;
} UndoIterator;

struct BTreeIterator
{
	OBTreeFindPageContext context;
	OIndexDescr *oidescr;
	OSnapshot	oSnapshot;
	UndoIterator undoIt;
	/* scan direction of current iterator: forward or backward */
	ScanDirection scanDir;
	/* current tuple location in UndoIterator */
	BTreePageItemLocator undoLoc;
	/* do we have to combine results from both current and undo pages? */
	bool		combinedResult;
	/* do we need to combine results in the current page? */
	bool		combinedPage;
	/* number of leaf pages visited; drives the FETCH->IMAGE switch */
	int			pageCount;
	/* memory context for returned tuples */
	MemoryContext tupleCxt;
	/* callback for fetching tuple version */
	TupleFetchCallback fetchCallback;
	void	   *fetchCallbackArg;

	/*
	 * Key of the last tuple position read from the current (FETCH-mode) leaf.
	 * If the partial leaf's backing page changes mid-scan and a chunk can no
	 * longer be loaded, we re-find this position from the root rather than
	 * read stale data.  curKey is seeded at creation with the first tuple to
	 * read, so it is valid even before the first fetch.  curKeyReturned tells
	 * whether curKey has already been handed out: if so the re-find resumes
	 * after it, otherwise it resumes at it.
	 */
	OFixedKey	curKey;
	bool		curKeySet;
	bool		curKeyReturned;

	/*
	 * The scan's original start key/kind, used to re-find from the beginning
	 * when a partial read fails before any tuple has been read (curKeySet is
	 * still false).  The caller keeps the key alive for the iterator's
	 * lifetime, so storing the pointer is enough.
	 */
	void	   *startKey;
	BTreeKeyType startKind;
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
							 BTreeKeyType kind, OSnapshot *read_o_snapshot,
							 CommitSeqNo *out_csn, MemoryContext mcxt,
							 BTreeLocationHint *hint,
							 bool *deleted,
							 TupleFetchCallback cb,
							 void *arg)
{
	BTreePageItemLocator loc;
	OBTreeFindPageContext context;
	char	   *img;
	BTreePageHeader *header;
	bool		combinedResult = false;
	OTuple		result;
	OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;
	bool		forceFind = false;

	/*
	 * If we need to get the result from given snapshot in the past, and in
	 * the same time we might have modifications in this tree, then we might
	 * need to combine results from the data page and the page image in undo
	 * log.
	 */
	if (COMMITSEQNO_IS_NORMAL(read_o_snapshot->csn))
		combinedResult = have_current_undo(desc->undoType);

	/*
	 * If we don't need to combine results, then ask find_page() to load the
	 * relevant page item from undo log for us by passing our snapshot csn.
	 */
	init_page_find_context(&context, desc,
						   combinedResult ? COMMITSEQNO_INPROGRESS : read_o_snapshot->csn,
						   BTREE_PAGE_FIND_FETCH);

retry:

	/* Use page location hint if provided */
	if (!forceFind && hint && OInMemoryBlknoIsValid(hint->blkno))
		findResult = refind_page(&context, key, kind, 0, hint->blkno, hint->pageChangeCount);
	else
		findResult = find_page(&context, key, kind, 0);

	Assert(findResult == OFindPageResultSuccess);

	loc = context.items[context.index].locator;
	img = context.img;
	header = (BTreePageHeader *) img;

	/* Adjust hint if given */
	if (hint)
	{
		hint->blkno = context.items[context.index].blkno;
		hint->pageChangeCount = context.items[context.index].pageChangeCount;
	}

	if (combinedResult && header->csn >= read_o_snapshot->csn)
	{
		/*
		 * Have to combine the results.  First look for a matching tuple on
		 * the data page modified by us.
		 */
		if (BTREE_PAGE_LOCATOR_IS_VALID(img, &loc))
		{
			BTreeLeafTuphdr *tupHdrPtr;
			OTuple		curTuple;
			int			result_size;

			BTREE_PAGE_READ_LEAF_ITEM(tupHdrPtr, curTuple, img, &loc);
			tupHdrPtr = (BTreeLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(img, &loc);

			if (o_btree_cmp(desc, key, kind, &curTuple, BTreeKeyLeafTuple) == 0)
			{
				BTreeLeafTuphdr tupHdr = *tupHdrPtr;

				/*
				 * We found the matching tuple.  Now check if it is modified
				 * by us.  Even if tuple is modified by us, there might be FOR
				 * KEY SHARE locks placed by concurrent transactions. Find the
				 * first non-lock-only undo record in the chain and check if
				 * it belongs to our transaction.
				 */
				(void) find_non_lock_only_undo_record(desc->undoType, &tupHdr);

				if (!XACT_INFO_IS_LOCK_ONLY(tupHdr.xactInfo) &&
					XACT_INFO_OXID_IS_CURRENT(tupHdr.xactInfo))
				{
					/*
					 * OK, we found the tuple modified by us.  It overrides
					 * whatever we could have from the undo log page image.
					 * Return it right away.
					 */
					if (out_csn)
						*out_csn = COMMITSEQNO_INPROGRESS;

					if (deleted)
						*deleted = (tupHdrPtr->deleted != BTreeLeafTupleNonDeleted);

					if (tupHdrPtr->deleted == BTreeLeafTupleNonDeleted)
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
						/* cppcheck-suppress uninitvar */
						return result;
					}
				}
			}
		}

		/*
		 * There is no matching tuple modified by us.  So, we have to fetch
		 * the page image from undo log as we didn't ask find_page() to do
		 * this for us.
		 *
		 * The page was read partially (BTREE_PAGE_FIND_FETCH).  Differential
		 * page-level undo images reconstruct the historical page in place
		 * from the live page in img, so they need every chunk present; fully
		 * materialize it first.  If the source page changed mid-load, the
		 * find result is stale -- redo it with a fresh top-down search.
		 */
		if (!partial_load_full_page(&context.partial, img))
		{
			forceFind = true;
			goto retry;
		}
		read_page_from_undo(desc, img, header->undoLocation, read_o_snapshot->csn,
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
		{
			/*
			 * The matching tuple is found.  Traverse the row-level undo chain
			 * for the relevant version and return it.
			 */
			return o_find_tuple_version(desc, img, &loc, read_o_snapshot,
										out_csn, mcxt, cb, arg);
		}
	}

	/* Tuple isn't found */
	O_TUPLE_SET_NULL(result);
	return result;
}

OTuple
o_btree_find_tuple_by_key(BTreeDescr *desc, void *key, BTreeKeyType kind,
						  OSnapshot *read_o_snapshot, CommitSeqNo *out_csn,
						  MemoryContext mcxt, BTreeLocationHint *hint)
{
	return o_btree_find_tuple_by_key_cb(desc, key, kind, read_o_snapshot,
										out_csn, mcxt, hint, NULL, NULL, NULL);
}


/*
 * Finds appropriate tuple version by traversing the undo chain.
 *
 * Starts with the on-page tuple, then walks historical versions from undo
 * records.  Visibility is determined by oSnapshot.
 *
 * When a TupleFetchCallback (cb) is provided, it is called for each version
 * to choose among multiple versions that share the same csn/xlogptr.  This is
 * useful when there are several uncommitted versions within a single
 * in-progress transaction.  The callback controls the iteration.
 *
 * The result's OTuple.data is allocated in mctx and is to be freed by the
 * caller.
 *
 * Note on COMMITSEQNO_NON_DELETED: this CSN is treated as in-progress
 * (COMMITSEQNO_IS_INPROGRESS returns true for it), so it returns data from
 * uncommitted transactions just like COMMITSEQNO_INPROGRESS.  The difference
 * is that NON_DELETED also returns tuples that are marked as deleted but are
 * still physically present on the page, which is needed when accessing trees
 * that may be deleted in uncommitted (sub-)transactions — on rollback those
 * trees become visible again.
 */
OTuple
o_find_tuple_version(BTreeDescr *desc, Page p, BTreePageItemLocator *loc,
					 OSnapshot *oSnapshot, CommitSeqNo *tupleCsn,
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
	bool		txIsFinished = false;

	prevMctx = MemoryContextSwitchTo(mcxt);

	BTREE_PAGE_READ_LEAF_ITEM(tupHdrPtr, curTuple, p, loc);
	tupHdr = *tupHdrPtr;
	(void) find_non_lock_only_undo_record(desc->undoType, &tupHdr);

	Assert(COMMITSEQNO_IS_NORMAL(oSnapshot->csn) ||
		   COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn));

	while (true)
	{
		OTupleXactInfo xactInfo = tupHdr.xactInfo;
		CommitSeqNo tupcsn;
		XLogRecPtr	tupptr = InvalidXLogRecPtr;

		oxid_match_snapshot(XACT_INFO_GET_OXID(xactInfo), oSnapshot, &tupcsn,
							XLogRecPtrIsInvalid(oSnapshot->xlogptr) ? NULL : &tupptr);

		txIsFinished = COMMITSEQNO_IS_COMMITTED(tupcsn);

		if (tupleCsn)
		{
			if (COMMITSEQNO_IS_NORMAL(tupcsn))
				*tupleCsn = COMMITSEQNO_IS_NORMAL(oSnapshot->csn) ? Max(oSnapshot->csn, tupcsn + 1) : COMMITSEQNO_MAX_NORMAL - 1;
			else if (COMMITSEQNO_IS_FROZEN(tupcsn))
				*tupleCsn = COMMITSEQNO_IS_NORMAL(oSnapshot->csn) ? oSnapshot->csn : COMMITSEQNO_MAX_NORMAL - 1;
			else
				*tupleCsn = COMMITSEQNO_INPROGRESS;
		}

		if (cb)
		{
			TupleFetchCallbackResult cbResult;

			/*
			 * Fetch from undo chain if txn is in progress OR historical
			 * version
			 */
			OXid		tupOxid = XACT_INFO_GET_OXID(xactInfo);

			cbResult = cb(curTuple, tupOxid, oSnapshot, arg, txIsFinished);

			if (cbResult == OTupleFetchMatch)
				break;
			if (cbResult == OTupleFetchNotMatch)
			{
				if (curTupleAllocated)
					pfree(curTuple.data);
				O_TUPLE_SET_NULL(result);
				MemoryContextSwitchTo(prevMctx);
				return result;
			}
		}

		if (!txIsFinished)
		{
			if (!cb)
			{
				if (COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn))
					break;

				/*
				 * We see the changes made by our transaction.  Exception are
				 * changes made by current command unless we're dealing with
				 * system tree.
				 */
				if (!XACT_INFO_IS_LOCK_ONLY(xactInfo) &&
					XACT_INFO_OXID_IS_CURRENT(xactInfo) &&
					oSnapshot->csn != COMMITSEQNO_MAX_NORMAL)
				{
					CommandId	tupleCid;

					if (IS_SYS_TREE_OIDS(desc->oids))
						break;

					/*
					 * Use cached UndoLocation if we have undo records in this
					 * command or below.  MaxUndoLocation means there are no
					 * undo records yet, so we need to recheck.
					 */
					tupleCid = undo_location_get_command(UndoLocationGetValue(tupHdr.undoLocation));

					if (tupleCid < oSnapshot->cid)
						break;
				}
			}
		}
		else
		{
			if (COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn))
				break;
		}

		if (!COMMITSEQNO_IS_INPROGRESS(tupcsn) &&
			!COMMITSEQNO_IS_ABORTED(tupcsn))
		{
			if (COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn))
			{
				Assert(XLogRecPtrIsInvalid(oSnapshot->xlogptr));
				break;
			}

			if (XLogRecPtrIsInvalid(oSnapshot->xlogptr))
			{
				if (tupcsn < oSnapshot->csn)
					break;
			}
			else
			{
				if (tupptr <= oSnapshot->xlogptr)
					break;
			}
		}

		undoLocation = tupHdr.undoLocation;

		if (!UndoLocationIsValid(undoLocation))
		{
			if (curTupleAllocated)
				pfree(curTuple.data);
			O_TUPLE_SET_NULL(result);
			MemoryContextSwitchTo(prevMctx);
			return result;
		}

		if (tupHdr.deleted != BTreeLeafTupleNonDeleted ||
			XACT_INFO_IS_LOCK_ONLY(tupHdr.xactInfo))
		{
			get_prev_leaf_header_from_undo(desc->undoType, &tupHdr, true);
		}
		else
		{
			if (curTupleAllocated)
				pfree(curTuple.data);
			get_prev_leaf_header_and_tuple_from_undo(desc->undoType, &tupHdr,
													 &curTuple, 0);
			curTupleAllocated = true;
		}

		Assert(UNDO_REC_EXISTS(desc->undoType, undoLocation));
	}

	if (COMMITSEQNO_IS_NON_DELETED(oSnapshot->csn))
	{
		if (tupHdr.deleted != BTreeLeafTupleNonDeleted &&
			txIsFinished)
		{
			if (curTupleAllocated)
				pfree(curTuple.data);
			O_TUPLE_SET_NULL(result);
			MemoryContextSwitchTo(prevMctx);
			return result;
		}
	}
	else if (tupHdr.deleted != BTreeLeafTupleNonDeleted && !cb)
	{
		if (curTupleAllocated)
			pfree(curTuple.data);
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

	Assert(!UndoLocationIsValid(undoLocation) || UNDO_REC_EXISTS(desc->undoType, undoLocation));
	MemoryContextSwitchTo(prevMctx);
	return result;
}

BTreeIterator *
o_btree_iterator_create(BTreeDescr *desc, void *key, BTreeKeyType kind,
						OSnapshot *o_snapshot, ScanDirection scanDir)
{
	BTreeIterator *it;
	uint16		findFlags;
	OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;

	it = (BTreeIterator *) palloc(sizeof(BTreeIterator));

	if (!IS_SYS_TREE_OIDS(desc->oids))
	{
		it->oidescr = (OIndexDescr *) desc->arg;
		ResourceOwnerRememberOIndexDescr(CurrentResourceOwner, it->oidescr);
	}
	else
		it->oidescr = NULL;

	it->combinedResult = have_current_undo(desc->undoType) && COMMITSEQNO_IS_NORMAL(o_snapshot->csn);
	it->oSnapshot = *o_snapshot;
	it->scanDir = scanDir;
	it->tupleCxt = CurrentMemoryContext;
	it->fetchCallback = NULL;
	it->fetchCallbackArg = NULL;
	it->curKeySet = false;
	it->curKeyReturned = false;
	it->startKey = NULL;
	it->startKind = BTreeKeyNone;
	BTREE_PAGE_LOCATOR_SET_INVALID(&it->undoLoc);
#ifdef USE_ASSERT_CHECKING
	O_TUPLE_SET_NULL(it->prevTuple.tuple);
#endif

	undo_it_create(&it->undoIt, it);

	/*
	 * Read leaf pages partially (FETCH) by default; undo-merging scans use
	 * IMAGE.  The iterator steps to siblings, so keep the parent image.
	 */
	findFlags = it->combinedResult ? BTREE_PAGE_FIND_IMAGE : BTREE_PAGE_FIND_FETCH;
	findFlags |= BTREE_PAGE_FIND_KEEP_PARENT;
	it->pageCount = 1;

	if (IT_IS_BACKWARD(it))
		findFlags |= BTREE_PAGE_FIND_KEEP_LOKEY;

	init_page_find_context(&it->context, desc,
						   it->combinedResult ? COMMITSEQNO_INPROGRESS : o_snapshot->csn, findFlags);

	if (key == NULL)
	{
		if (IT_IS_FORWARD(it))
			kind = BTreeKeyNone;
		else
			kind = BTreeKeyRightmost;
	}

	it->startKey = key;
	it->startKind = kind;

	findResult = find_page(&it->context, key, kind, 0);
	Assert(findResult == OFindPageResultSuccess);

	if (key != NULL && IT_IS_BACKWARD(it))
	{
		BTreePageItemLocator *loc = &it->context.items[it->context.index].locator;
		bool		make_dec = false;

		/*
		 * Positioning the backward start reads the leaf's chunk descriptor
		 * array (BTREE_PAGE_LOCATOR_GET_OFFSET / _PREV), which a partial
		 * (FETCH) image read through the fastpath does not contain
		 * (loadHikeys = !fastpath).  Load the hikeys chunk so the descriptors
		 * are present; if the backing page changed, fall back to a whole-page
		 * image read and re-position there.
		 */
		if (BTREE_PAGE_FIND_IS(&it->context, FETCH) &&
			!partial_load_hikeys_chunk(&it->context.partial, it->context.img))
		{
			BTREE_PAGE_FIND_UNSET(&it->context, FETCH);
			BTREE_PAGE_FIND_SET(&it->context, IMAGE);
			(void) find_page(&it->context, key, kind, 0);
			loc = &it->context.items[it->context.index].locator;
		}

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

	/*
	 * Seed the re-find key with the first tuple to read.  This way a partial
	 * read failure on the very first fetch (the backing page can change
	 * between positioning here and the first fetch, e.g. on a hot standby
	 * applying WAL) still has a valid key to re-find from, marked as not yet
	 * returned so the re-find resumes at it rather than past it.
	 */
	if (!it->combinedResult && BTREE_PAGE_FIND_IS(&it->context, FETCH))
	{
		BTreePageItemLocator *loc = &it->context.items[it->context.index].locator;

		if (BTREE_PAGE_LOCATOR_IS_VALID(it->context.img, loc) &&
			partial_load_chunk(&it->context.partial, it->context.img,
							   loc->chunkOffset, NULL))
		{
			OTuple		tup;

			BTREE_PAGE_READ_LEAF_TUPLE(tup, it->context.img, loc);
			copy_fixed_key(desc, &it->curKey, tup);
			it->curKeySet = true;
			it->curKeyReturned = false;
		}
	}

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

/*
 * Fetches tha next tuple from the iterator.  Returns null tuple when there
 * are no more tuples before the end boundary (defined by `end`, `endType`,
 * and `endIsIncluded`).
 *
 * The result's OTuple.data is allocated in it->tupleCxt memory context.  It's
 * the caller's responsibility to free this memory.
 */
OTuple
o_btree_iterator_fetch(BTreeIterator *it, CommitSeqNo *tupleCsn,
					   void *end, BTreeKeyType endType,
					   bool endIsIncluded, BTreeLocationHint *hint)
{
	BTreeDescr *desc = it->context.desc;
	OTuple		result;

	ASAN_UNPOISON_MEMORY_REGION(&result, sizeof(result));

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
	if (it->oidescr)
		ResourceOwnerForgetOIndexDescr(CurrentResourceOwner, it->oidescr);
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

	if (it->combinedResult && header->csn >= it->oSnapshot.csn)
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
 * Recover the scan position after a partial (FETCH) leaf read failed because
 * its backing page changed.  We must never read from a partial page whose
 * chunk could not be loaded, so re-find the last read position from the root
 * and leave the locator on the next tuple to return.
 */
static void
iterator_refind_partial_leaf(BTreeIterator *it)
{
	OBTreeFindPageContext *context = &it->context;
	BTreeDescr *desc = context->desc;
	BTreePageItemLocator *loc;
	OTuple		tup;
	bool		match;

	Assert(!it->combinedResult);

	/*
	 * A partial (FETCH) read just failed because the backing page changed
	 * under us.  Switch this scan to whole-page images (IMAGE) for the rest
	 * of its life before re-finding: a full page read is a consistent
	 * snapshot, so the re-find below and every subsequent fetch are immune to
	 * concurrent page changes and never touch the partial-read or fastpath
	 * descent paths again.  (This is the same FETCH->IMAGE transition the
	 * page-count heuristic makes; doing it here just makes it
	 * failure-driven.)
	 */
	BTREE_PAGE_FIND_UNSET(context, FETCH);
	BTREE_PAGE_FIND_SET(context, IMAGE);

	/*
	 * If we have not handed out any tuple yet, there is no resume key:
	 * re-find the scan start from the original key/kind, mirroring the
	 * positioning in o_btree_iterator_create().  (This happens when the very
	 * first partial read fails -- e.g. the start position landed past the end
	 * of the first page and the backing page changed before the next page
	 * could be read.)
	 */
	if (!it->curKeySet)
	{
		(void) find_page(context, it->startKey, it->startKind, 0);

		loc = &context->items[context->index].locator;
		if (it->startKey != NULL && IT_IS_BACKWARD(it) &&
			BTREE_PAGE_LOCATOR_IS_VALID(context->img, loc))
		{
			bool		make_dec = false;

			if (BTREE_PAGE_LOCATOR_GET_OFFSET(context->img, loc) ==
				BTREE_PAGE_ITEMS_COUNT(context->img))
				make_dec = true;
			else
			{
				BTREE_PAGE_READ_LEAF_TUPLE(tup, context->img, loc);
				if (o_btree_cmp(desc, it->startKey, it->startKind,
								&tup, BTreeKeyLeafTuple) < 0)
					make_dec = true;
			}
			if (make_dec)
				BTREE_PAGE_LOCATOR_PREV(context->img, loc);
		}
		return;
	}

	/*
	 * find_page() sets up a fresh partial read for the current page change
	 * count and loads the target locator's chunk (retrying internally on its
	 * own partial-read failures), so the located tuple is always safe to
	 * read.
	 */
	(void) find_page(context, &it->curKey.tuple, BTreeKeyNonLeafKey, 0);

	loc = &context->items[context->index].locator;
	if (!BTREE_PAGE_LOCATOR_IS_VALID(context->img, loc))
		return;

	/*
	 * find_page() leaves the locator on the first tuple >= curKey (its chunk
	 * loaded), so it is safe to read here.
	 */
	BTREE_PAGE_READ_LEAF_TUPLE(tup, context->img, loc);
	match = o_btree_cmp(desc, &tup, BTreeKeyLeafTuple,
						&it->curKey.tuple, BTreeKeyNonLeafKey) == 0;

	if (IT_IS_FORWARD(it))
	{
		/*
		 * The locator already sits on the first tuple >= curKey.  When curKey
		 * was already returned and is still present, step past it to its
		 * successor; otherwise (curKey not yet returned, or gone) the locator
		 * is already on the next tuple to read.
		 */
		if (match && it->curKeyReturned)
			BTREE_PAGE_LOCATOR_NEXT(context->img, loc);
	}
	else
	{
		/*
		 * Backward: the next tuple to read is the largest one <= curKey.  If
		 * curKey is present and not yet returned, that is curKey itself
		 * (stay); otherwise (already returned, or gone) step left of the
		 * first tuple >= curKey to reach the largest one strictly below it.
		 */
		if (it->curKeyReturned || !match)
			BTREE_PAGE_LOCATOR_PREV(context->img, loc);
	}
}

/*
 * Advance the leaf locator by one item in the scan direction.
 *
 * In IMAGE mode the whole page (including the chunk-descriptor array) is in
 * context->img, so the plain BTREE_PAGE_LOCATOR_NEXT/PREV macros can cross
 * chunk boundaries by reading the descriptors from the image.
 *
 * In FETCH mode the leaf is partial and the descriptor array is NOT loaded
 * eagerly (loadHikeys = !fastpath in find_page()).  Moving inside the current
 * chunk needs only the locator's cached per-chunk counts.  But crossing a
 * chunk boundary needs the descriptor array, so we load the hikeys chunk (which
 * contains it) on demand first; that is a no-op once it is loaded, so a
 * multi-chunk leaf pays for it only on the first crossing.  After that the
 * regular BTREE_PAGE_LOCATOR_NEXT/PREV macros cross using the descriptors now
 * present in the image, and the loop in the caller loads the destination
 * chunk's data before reading from it.
 *
 * Returns false if the hikeys chunk could not be loaded because the backing
 * page changed; the caller must re-find.  On end-of-page in the scan direction
 * the locator is left invalid (BTREE_PAGE_LOCATOR_IS_VALID() == false).
 */
static inline bool
iterator_advance_leaf(BTreeIterator *it, BTreePageItemLocator *loc)
{
	OBTreeFindPageContext *context = &it->context;

	if (BTREE_PAGE_FIND_IS(context, FETCH))
	{
		bool		crossing;

		if (IT_IS_FORWARD(it))
			crossing = loc->itemOffset + 1 >= loc->chunkItemsCount;
		else
			crossing = loc->itemOffset == 0;

		if (crossing &&
			!partial_load_hikeys_chunk(&context->partial, context->img))
			return false;
	}

	IT_NEXT_OFFSET(it, loc);
	return true;
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
											  &it->oSnapshot, tupleCsn,
											  it->tupleCxt,
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
											  &it->oSnapshot, tupleCsn,
											  it->tupleCxt,
											  it->fetchCallback,
											  it->fetchCallbackArg);

				UNDO_IT_NEXT_OFFSET(&it->undoIt, &it->undoLoc);

				if (!O_TUPLE_IS_NULL(result))
					return result;
			}
		}
		else
		{
			OTuple		posTup;

			/* In FETCH mode the leaf is partial; load this tuple's chunk. */
			if (BTREE_PAGE_FIND_IS(context, FETCH) &&
				!partial_load_chunk(&context->partial, context->img,
									leaf_item->locator.chunkOffset, NULL))
			{
				/*
				 * The backing page changed since we set up the partial read.
				 * Never read from it; re-find our position and retry.
				 */
				iterator_refind_partial_leaf(it);
				continue;
			}

			/*
			 * Remember this position in case the page changes later.  We are
			 * about to consume this tuple, so mark it as returned: a later
			 * re-find resumes after it.
			 */
			BTREE_PAGE_READ_LEAF_TUPLE(posTup, context->img, &leaf_item->locator);
			copy_fixed_key(desc, &it->curKey, posTup);
			it->curKeySet = true;
			it->curKeyReturned = true;

			result = o_find_tuple_version(desc, context->img,
										  &leaf_item->locator,
										  &it->oSnapshot, tupleCsn,
										  it->tupleCxt,
										  it->fetchCallback,
										  it->fetchCallbackArg);

			if (!iterator_advance_leaf(it, &leaf_item->locator))
				iterator_refind_partial_leaf(it);

			if (!O_TUPLE_IS_NULL(result))
				return result;
		}
	}

	O_TUPLE_SET_NULL(result);
	return result;				/* unreachable */
}

/*
 * Switch the iterator from partial reads (FETCH) to whole-page images (IMAGE)
 * once it has visited enough pages.  No-op for combined scans (always IMAGE).
 */
static inline void
iterator_maybe_switch_to_image(BTreeIterator *it)
{
	if (BTREE_PAGE_FIND_IS(&it->context, FETCH) && ++it->pageCount >= 3)
	{
		BTREE_PAGE_FIND_UNSET(&it->context, FETCH);
		BTREE_PAGE_FIND_SET(&it->context, IMAGE);
	}
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
	BTreeDescr *desc = context->desc;
	OFixedKey	key_buf;

	if (o_btree_interator_can_fetch_from_undo(context->desc, it))
		return true;

	while (!BTREE_PAGE_LOCATOR_IS_VALID(img, &context->items[context->index].locator))
	{
		bool		step_result;
		BTreePageHeader *header;

		if (IS_LAST_PAGE(img, it))
			return false;

		iterator_maybe_switch_to_image(it);

		/*
		 * find_right_page() reads the current leaf's hikey to locate its
		 * right sibling.  With hikeys loaded on demand (loadHikeys =
		 * !fastpath), a FETCH leaf reached via the fastpath has no hikeys
		 * chunk yet -- load it now.  partial_load_hikeys_chunk() is a no-op
		 * once the chunk (or the whole page, in IMAGE mode) is present; on a
		 * backing-page change recover through the standard re-find.  Backward
		 * stepping uses the lokey, never the current leaf's hikey.
		 */
		if (IT_IS_FORWARD(it) &&
			!partial_load_hikeys_chunk(&context->partial, context->img))
		{
			iterator_refind_partial_leaf(it);
			continue;
		}

		if (IT_IS_FORWARD(it))
			step_result = find_right_page(context, &key_buf);
		else
			step_result = find_left_page(context, &key_buf);

		if (!step_result)
			return false;

		header = (BTreePageHeader *) context->img;

		if (it->combinedResult && header->csn >= it->oSnapshot.csn)
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

	/*
	 * Nothing to do if we're not on the combined page.  All the undo items
	 * corresponding to the data page key range must happen while we're on
	 * that data page.
	 */
	if (!it->combinedPage)
		return false;

	Assert(it->combinedResult && header->csn >= it->oSnapshot.csn);

	while (!BTREE_PAGE_LOCATOR_IS_VALID(hImg, &it->undoLoc))
	{
		/* switch to next history page if we can */
		if (undo_it_next_page(it->context.desc, &it->undoIt) ||
			undo_it_switch(desc, &it->undoIt, header->undoLocation))
		{
			if (IT_IS_FORWARD(it))
				BTREE_PAGE_LOCATOR_FIRST(hImg, &it->undoLoc);
			else
				BTREE_PAGE_LOCATOR_LAST(hImg, &it->undoLoc);
		}
		else
		{
			break;
		}
	}

	return can_fetch_from_undo(it);
}

/*
 * Check if `historicalImg` still contains more tuples corresponding to
 * the `img` key range.
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

static OTuple
btree_iterate_raw_internal(BTreeIterator *it, void *end, BTreeKeyType endKind,
						   bool endInclude, bool *scanEnd,
						   BTreeLocationHint *hint, bool deleted_as_null,
						   BTreeLeafTuphdr **tupHdr)
{
	BTreeLeafTuphdr *localTupHdr;
	OBTreeFindPageContext *context = &it->context;
	Page		img = context->img;
	OTuple		result;
	OFixedKey	key_buf;

	if (!tupHdr)
		tupHdr = &localTupHdr;

	*scanEnd = false;

	while (true)
	{
		BTreePageItemLocator *loc = &context->items[context->index].locator;

		if (BTREE_PAGE_LOCATOR_IS_VALID(img, loc))
		{
			/* In FETCH mode the leaf is partial; load this tuple's chunk. */
			if (BTREE_PAGE_FIND_IS(context, FETCH) &&
				!partial_load_chunk(&context->partial, context->img,
									loc->chunkOffset, NULL))
			{
				/*
				 * The backing page changed since we set up the partial read.
				 * Never read from it; re-find our position and retry.
				 */
				iterator_refind_partial_leaf(it);
				continue;
			}
			BTREE_PAGE_READ_LEAF_ITEM(*tupHdr, result, context->img, loc);

			/*
			 * Apply the end-key bound before mutating any iterator state.
			 * A rejected tuple must leave curKey/curKeyReturned and the
			 * locator untouched: a later partial-read refind would
			 * otherwise treat it as already handed out and step past it,
			 * silently dropping the row on a resumed scan.
			 */
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

			/*
			 * Remember this position in case the page changes later.  The
			 * tuple is being consumed, so mark it returned: a later re-find
			 * resumes after it.
			 */
			copy_fixed_key(context->desc, &it->curKey, result);
			it->curKeySet = true;
			it->curKeyReturned = true;

			if (!iterator_advance_leaf(it, loc))
			{
				iterator_refind_partial_leaf(it);
				loc = &context->items[context->index].locator;
			}

			if (!deleted_as_null ||
				(*tupHdr)->deleted == BTreeLeafTupleNonDeleted)
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

		iterator_maybe_switch_to_image(it);

		/*
		 * Load the current leaf's hikeys on demand for find_right_page() (see
		 * the matching comment in btree_iterator_check_load_next_page()).
		 */
		if (IT_IS_FORWARD(it) &&
			!partial_load_hikeys_chunk(&context->partial, context->img))
		{
			iterator_refind_partial_leaf(it);
			continue;
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
 * Iterate over leaf page tuples without considering undo log.  Deleted tuples
 * are reported as NULLs.  So, the separate `*end` flag indicates finish of
 * iterations.
 */
OTuple
btree_iterate_raw(BTreeIterator *it, void *end, BTreeKeyType endKind,
				  bool endInclude, bool *scanEnd, BTreeLocationHint *hint)
{
	return btree_iterate_raw_internal(it, end, endKind, endInclude, scanEnd,
									  hint, true, NULL);
}

/*
 * Iterate over leaf page tuples without considering undo log. Deleted tuples
 * also returned.
 */
OTuple
btree_iterate_all(BTreeIterator *it, void *end, BTreeKeyType endKind,
				  bool endInclude, bool *scanEnd, BTreeLocationHint *hint,
				  BTreeLeafTuphdr **tupHdr)
{
	return btree_iterate_raw_internal(it, end, endKind, endInclude, scanEnd,
									  hint, false, tupHdr);
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
 * Tries to switch to next undo page from the same baseLoc
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

	if (!UndoLocationIsValid(location) || undoIt->baseLoc == location)
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
	BTreeDescr *desc = undoIt->it->context.desc;
	UndoLogType undoType PG_USED_FOR_ASSERTS_ONLY = GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType);
	UndoLocation rec_undo_loc,
				undoLocation;
	bool		left,
				right;

	undoLocation = undoIt->baseLoc;
	undoIt->leftmost = true;
	undoIt->rightmost = true;

	/*
	 * Seed the working image with the live data leaf before walking the
	 * chain. Differential page-level undo images (UndoPageImage*Diff) do not
	 * store page bytes; they reconstruct the historical page by transforming
	 * the newer page in place.  Since each undo_it_find_internal() call
	 * restarts the walk from baseLoc, it must restart from the live leaf --
	 * mirroring the histImg re-seed in load_first_historical_page() /
	 * load_next_historical_page(). For full images this memcpy is harmless:
	 * they overwrite the image whole.
	 */
	memcpy(undoIt->image, undoIt->it->context.img, ORIOLEDB_BLCKSZ);

	while (true)
	{
		/* Load the next page item from page-level undo item */
		if (undoIt->it->scanDir == ForwardScanDirection)
			get_page_from_undo(desc, undoLocation, key, kind,
							   undoIt->image, &left, &right, NULL, NULL, NULL);
		else
			get_page_from_undo(desc, undoLocation, key, kind,
							   undoIt->image, &left, &right, &undoIt->lokey, NULL, NULL);

		undoIt->rightmost = (undoIt->rightmost && right) || O_PAGE_IS(undoIt->image, RIGHTMOST);
		undoIt->leftmost = (undoIt->leftmost && left) || O_PAGE_IS(undoIt->image, LEFTMOST);
		undoIt->imageUndoLoc = O_UNDO_GET_IMAGE_LOCATION(undoLocation, left);
		Assert(UNDO_REC_EXISTS(undoType, undoLocation));

		header = (BTreePageHeader *) undoIt->image;
		rec_csn = header->csn;
		rec_undo_loc = header->undoLocation;

		/* Check if we need to visit next page-level undo item */
		if (COMMITSEQNO_IS_NORMAL(rec_csn) && rec_csn >= undoIt->it->oSnapshot.csn)
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

#ifdef IS_DEV
/* ------------------------------------------------------------------------
 * C-side test helpers.  Compiled only when IS_DEV is defined (-DIS_DEV);
 * the SQL bindings are registered in the 1.8->1.9 dev migration.  See
 * test/sql/iterator.sql for the test driver.
 * ------------------------------------------------------------------------ */

#include "access/relation.h"
#include "fmgr.h"
#include "tuple/format.h"
#include "utils/builtins.h"

/*
 * Whitebox test: prove that the premature it->curKeyReturned = true at
 * iterator.c:1358 (set before the end-key bound check at :1372) can leak
 * into a partial-read refind and silently drop a tuple from a resumed scan.
 *
 * 1. Open a forward iterator at start_at.
 * 2. iterate_raw with end == start_at exclusive: reads the tuple at start_at,
 *    sets curKey=start_at and curKeyReturned=true, then rejects it via the
 *    end bound and returns NULL with scanEnd=true.  State now claims
 *    start_at was handed out, even though only NULL went to the caller.
 * 3. Force iterator_refind_partial_leaf(), the path partial-read failures
 *    take when the backing page changes.  With the bug it steps past
 *    curKey=start_at; with the fix (no spurious "returned") it stays at it.
 * 4. Resume with a wide end and drain the iterator.  Return the PKs as an
 *    int4 array so the caller can `unnest` them: row count tells the story.
 *
 * Expected (post-fix): N rows starting at start_at.
 * Bug (current code):  N-1 rows starting at start_at + 1 -- start_at lost.
 *
 * Assumes the relation's primary index has a single int4 key column.
 */
PG_FUNCTION_INFO_V1(orioledb_test_endkey_returned_skip);
Datum
orioledb_test_endkey_returned_skip(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		start_at = PG_GETARG_INT32(1);
	Relation	rel;
	OTableDescr *descr;
	OIndexDescr *primary;
	OBTreeKeyBound startBound,
				narrowEnd,
				wideEnd;
	BTreeIterator *it;
	OTuple		result;
	bool		scanEnd = false;
	bool		isnull;
	Datum	   *elems;
	int			nelems = 0;
	int			alloc = 16;
	int			dims[1],
				lbs[1];

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);
	if (!descr)
		elog(ERROR, "relation is not an orioledb table");
	primary = GET_PRIMARY(descr);
	if (primary->nKeyFields < 1 || primary->fields[0].inputtype != INT4OID)
		elog(ERROR, "test requires a single int4 primary key column");

	startBound.nkeys = 1;
	startBound.n_row_keys = 0;
	startBound.row_keys = NULL;
	startBound.keys[0].type = INT4OID;
	startBound.keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
	startBound.keys[0].comparator = primary->fields[0].comparator;
	startBound.keys[0].exclusion_fn = NULL;
	startBound.keys[0].value = Int32GetDatum(start_at);

	narrowEnd = startBound;
	wideEnd = startBound;
	wideEnd.keys[0].value = Int32GetDatum(PG_INT32_MAX);

	it = o_btree_iterator_create(&primary->desc, (Pointer) &startBound,
								 BTreeKeyBound, &o_in_progress_snapshot,
								 ForwardScanDirection);

	/* Drive the iterator into the buggy state. */
	result = btree_iterate_raw(it, (Pointer) &narrowEnd, BTreeKeyBound,
							   false, &scanEnd, NULL);
	if (!O_TUPLE_IS_NULL(result) || !scanEnd)
		elog(ERROR, "unexpected first iterate_raw: scanEnd=%d, tuple is %s",
			 scanEnd, O_TUPLE_IS_NULL(result) ? "null" : "non-null");

	/* Simulate the partial-read failure that forces a refind. */
	iterator_refind_partial_leaf(it);

	/* Resume with a wide end and collect every PK the iterator now emits. */
	elems = (Datum *) palloc(alloc * sizeof(Datum));
	for (;;)
	{
		scanEnd = false;
		result = btree_iterate_raw(it, (Pointer) &wideEnd, BTreeKeyBound,
								   true, &scanEnd, NULL);
		if (O_TUPLE_IS_NULL(result))
		{
			if (!scanEnd)
				elog(ERROR, "iterate_raw returned NULL without scanEnd");
			break;
		}
		if (nelems == alloc)
		{
			alloc *= 2;
			elems = (Datum *) repalloc(elems, alloc * sizeof(Datum));
		}
		elems[nelems++] = o_fastgetattr(result, 1, primary->leafTupdesc,
										&primary->leafSpec, &isnull);
		if (isnull)
			elog(ERROR, "PK column unexpectedly NULL");
	}

	btree_iterator_free(it);
	relation_close(rel, AccessShareLock);

	dims[0] = nelems;
	lbs[0] = 1;
	PG_RETURN_ARRAYTYPE_P(construct_md_array(elems, NULL, 1, dims, lbs,
											 INT4OID, sizeof(int32), true,
											 TYPALIGN_INT));
}

/*
 * Whitebox test: prove that iterator_refind_partial_leaf()'s curKeySet
 * branch bails out (iterator.c:911) without decrementing the locator when
 * find_page() lands past-end on a backward scan.  The seed path and the
 * !curKeySet path both decrement in this case; only this branch doesn't.
 * If find_page legitimately lands on a leaf with no tuple >= curKey
 * (e.g. curKey got deleted and the leaf's max < curKey), the backward
 * scan silently drops the leaf's tail.
 *
 * 1. Open a backward iterator past the leaf's max.  The seed lands the
 *    locator on the leaf's largest tuple and copies it into curKey.
 * 2. Overwrite curKey with a forged tuple whose PK is fake_curkey (chosen
 *    > all tuples in the table) so find_page() lands past-end on refind.
 * 3. iterator_refind_partial_leaf() -- the partial-read-failure path.
 *    With the bug, curKeySet branch returns early; the locator stays
 *    invalid and the caller's next find_left_page from the leftmost leaf
 *    ends the scan.  With the fix (mirror seed/!curKeySet behaviour) the
 *    locator decrements to the leaf's tail and the drain reads it.
 * 4. Drain backward and return the PKs as int4[].
 *
 * Expected (post-fix): every row in the table, descending.
 * Bug (current code): empty array -- the leaf's tail is silently dropped.
 *
 * Assumes the relation's primary index has a single int4 key column.
 */
PG_FUNCTION_INFO_V1(orioledb_test_back_refind_skip_tail);
Datum
orioledb_test_back_refind_skip_tail(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		fake_curkey = PG_GETARG_INT32(1);
	Relation	rel;
	OTableDescr *descr;
	OIndexDescr *primary;
	OBTreeKeyBound startBound;
	BTreeIterator *it;
	OTuple		result;
	OTuple		fakeTup;
	Datum		fakeValue;
	bool		fakeNull = false;
	bool		scanEnd = false;
	bool		isnull;
	Datum	   *elems;
	int			nelems = 0;
	int			alloc = 16;
	int			dims[1],
				lbs[1];

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);
	if (!descr)
		elog(ERROR, "relation is not an orioledb table");
	primary = GET_PRIMARY(descr);
	if (primary->nKeyFields < 1 || primary->fields[0].inputtype != INT4OID)
		elog(ERROR, "test requires a single int4 primary key column");

	startBound.nkeys = 1;
	startBound.n_row_keys = 0;
	startBound.row_keys = NULL;
	startBound.keys[0].type = INT4OID;
	startBound.keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
	startBound.keys[0].comparator = primary->fields[0].comparator;
	startBound.keys[0].exclusion_fn = NULL;
	startBound.keys[0].value = Int32GetDatum(fake_curkey);

	it = o_btree_iterator_create(&primary->desc, (Pointer) &startBound,
								 BTreeKeyBound, &o_in_progress_snapshot,
								 BackwardScanDirection);

	/*
	 * Forge curKey: a leaf-format tuple whose PK is fake_curkey.  Once
	 * copied into it->curKey, find_page() on refind will land past-end.
	 */
	fakeValue = Int32GetDatum(fake_curkey);
	fakeTup = o_form_tuple(primary->leafTupdesc, &primary->leafSpec, 0,
						   &fakeValue, &fakeNull, NULL);
	copy_fixed_key(&primary->desc, &it->curKey, fakeTup);
	pfree(fakeTup.data);
	it->curKeySet = true;
	it->curKeyReturned = false;

	/* Force the partial-read-failure refind path. */
	iterator_refind_partial_leaf(it);

	/* Drain backward; collect every PK the iterator now emits. */
	elems = (Datum *) palloc(alloc * sizeof(Datum));
	for (;;)
	{
		scanEnd = false;
		result = btree_iterate_raw(it, NULL, BTreeKeyNone, false,
								   &scanEnd, NULL);
		if (O_TUPLE_IS_NULL(result))
		{
			if (!scanEnd)
				elog(ERROR, "iterate_raw returned NULL without scanEnd");
			break;
		}
		if (nelems == alloc)
		{
			alloc *= 2;
			elems = (Datum *) repalloc(elems, alloc * sizeof(Datum));
		}
		elems[nelems++] = o_fastgetattr(result, 1, primary->leafTupdesc,
										&primary->leafSpec, &isnull);
		if (isnull)
			elog(ERROR, "PK column unexpectedly NULL");
	}

	btree_iterator_free(it);
	relation_close(rel, AccessShareLock);

	dims[0] = nelems;
	lbs[0] = 1;
	PG_RETURN_ARRAYTYPE_P(construct_md_array(elems, NULL, 1, dims, lbs,
											 INT4OID, sizeof(int32), true,
											 TYPALIGN_INT));
}

#endif							/* IS_DEV */
