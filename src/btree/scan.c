/*-------------------------------------------------------------------------
 *
 * scan.c
 *		Routines for sequential scan of orioledb B-tree
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/scan.c
 *
 * ALGORITHM
 *
 *		The big picture algorithm of sequential scan is following.
 *		1. Scan all the internal pages with level == 1. The total amount of
 *		   internal pages are expected to be small. So, it should be OK to
 *		   scan them in logical order.
 *		   1.1. Immediately scan children's leaves and return their contents.
 *		   1.2. Edge cases are handled using iterators. They are expected to
 *		   be very rare.
 *		   1.3. Collect on-disk downlinks into an array together with CSN at
 *		   the moment of the corresponding internal page read.
 *		2. Ascending sort array of downlinks providing as sequential access
 *		   pattern as possible.
 *		3. Scan sorted downlink and apply the corresponding CSN.
 *
 * PARALLEL SCAN
 *
 *		The parallel sequential scan is implemented as follows.
 *		1. The scan leader creates a shared DSM array for on-disk downlinks,
 *		   initially sized to TREE_NUM_LEAF_PAGES.
 *		2. Two internal page images (level == 1) are kept in shared memory.
 *		3. Workers are iterating the downlinks of these pages in parallel
 *		   one by one.  On-disk downlinks are written directly to the shared
 *		   DSM array.  If the array is full, it is reallocated under lock.
 *		4. Once the internal page is finished, one worker loads the next page in
 *		   its place.  Other workers continue to process the downlink of the
 *		   remaining page.
 *		5. Once internal page processing is finished, one worker sorts the
 *		   shared on-disk downlinks array under lock.
 *		6. Workers process on-disk downlinks in parallel one by one.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/io.h"
#include "btree/iterator.h"
#include "btree/page_chunks.h"
#include "btree/scan.h"
#include "btree/undo.h"
#include "tableam/descr.h"
#include "transam/oxid.h"
#include "tableam/descr.h"
#include "tuple/slot.h"
#include "utils/page_pool.h"
#include "utils/resowner.h"
#include "utils/sampling.h"
#include "utils/stopevent.h"

#include "miscadmin.h"
#include "utils/wait_event.h"

typedef enum
{
	BTreeSeqScanInMemory,
	BTreeSeqScanDisk,
	BTreeSeqScanFinished
} BTreeSeqScanStatus;

typedef struct
{
	uint64		downlink;
	CommitSeqNo csn;
} BTreeSeqScanDiskDownlink;

struct BTreeSeqScan
{
	BTreeDescr *desc;

	char		leafImg[ORIOLEDB_BLCKSZ];
	char		histImg[ORIOLEDB_BLCKSZ];

	/*
	 * FETCH-mode partial reads for bitmap scans.  When `fetch` is set (bitmap
	 * callbacks present and non-parallel scan) the in-memory leaf page is
	 * read partially into leafImg: only the chunks actually visited by the
	 * bitmap's key-driven iteration are materialized from the live shared
	 * page via `leafPartial`.  `leafPartialFailed` is raised when a chunk
	 * load detects a concurrent modification; the caller then falls back to
	 * an iterator over the current key range.  Parallel scans keep whole-page
	 * IMAGE reads (they share the page image through DSM, which a partial
	 * image can't back), and pages carrying historical/undo data are fully
	 * materialized (IMAGE).
	 */
	bool		fetch;
	PartialPageState leafPartial;
	bool		leafPartialFailed;

	/*
	 * FETCH-mode partial read of the level-1 internal page.  When `fetch` is
	 * set the internal page is read partially into scan->context.img via
	 * scan->context.partial (find_page FETCH mode): only the hikeys chunk
	 * plus the chunks holding downlinks the bitmap walk actually visits are
	 * materialized.  The bitmap-driven walk skips whole chunks of downlinks
	 * via their chunk hikeys (get_next_downlink ->
	 * internal_skip_to_next_key), exactly mirroring the leaf's
	 * adjust_location_with_next_key(). intPartialFailed is raised when an
	 * on-demand chunk load loses the race with a concurrent page
	 * modification; load_next_internal_page then re-reads the whole page
	 * (IMAGE) and the walk continues on the full image.
	 */
	bool		intPartialFailed;

	/*
	 * High key of the last downlink get_next_downlink() returned (the low
	 * bound of where the walk should resume).  When an on-demand
	 * internal-chunk load fails mid-walk, get_next_downlink() re-reads the
	 * internal page whole (IMAGE) by descending to this key, so
	 * already-returned downlinks are not revisited.  haveIntResumeKey is
	 * false before the first downlink of a scan.
	 */
	bool		haveIntResumeKey;
	OFixedKey	intResumeKey;

	/*
	 * Key of the last leaf tuple emitted from the current partially-read leaf
	 * (valid only while haveLastLeafKey).  When a partial chunk load loses
	 * the race with a concurrent modification after some tuples of the leaf
	 * were already emitted, the fallback iterator must resume strictly after
	 * this key rather than re-read the whole leaf range (which would emit
	 * those tuples a second time).  iterSkipKey asks the iterator to drop a
	 * leading tuple that is <= this key (the resume point is inclusive).
	 */
	bool		haveLastLeafKey;
	OFixedKey	lastLeafKey;
	bool		iterSkipKey;
	OFixedKey	iterSkipKeyVal;

	bool		initialized;
	bool		checkpointNumberSet;
	OSnapshot	oSnapshot;
	OBTreeFindPageContext context;
	OFixedKey	prevHikey;
	BTreeLocationHint hint;

	BTreePageItemLocator intLoc;

	/*
	 * The page offset we started with according to `prevHikey`;
	 */
	OffsetNumber intStartOffset;

	BTreePageItemLocator leafLoc;

	bool		haveHistImg;
	BTreePageItemLocator histLoc;

	BTreeSeqScanStatus status;
	MemoryContext mctx;

	BTreeSeqScanDiskDownlink *diskDownlinks;
	int64		downlinksCount; /* Used only for serial scan */
	int64		downlinkIndex;
	int64		allocatedDownlinks;

	BTreeIterator *iter;
	OTuple		iterEnd;

	/*
	 * Number of the last completed checkpoint when scan was started.  We need
	 * on-disk pages of this checkpoint to be not overridden until scan
	 * finishes.  This means we shouldn't start using free blocks of later
	 * checkpoints before this scan is finished.
	 */
	uint32		checkpointNumber;

	BTreeMetaPage *metaPageBlkno;
	dlist_node	listNode;

	OFixedKey	nextKey;

	bool		needSampling;
	BlockSampler sampler;
	BlockNumber samplingNumber;
	BlockNumber samplingNext;

	BTreeSeqScanCallbacks *cb;
	void	   *arg;
	bool		isSingleLeafPage;	/* Scan couldn't read first internal page */
	OFixedKey	keyRangeLow,
				keyRangeHigh;
	bool		firstPageIsLoaded;

	/* Private parallel worker info in a backend */
	ParallelOScanDesc poscan;
	int			workerNumber;
	dsm_segment *dsmSeg;

	/* Ensures scan cleanup on transaction abort or resource owner release */
	ResourceOwner resowner;
};

static dlist_head listOfScans = DLIST_STATIC_INIT(listOfScans);

static void scan_make_iterator(BTreeSeqScan *scan, OTuple startKey, OTuple keyRangeHigh);
static void get_next_key(BTreeSeqScan *scan, BTreePageItemLocator *intLoc, OFixedKey *nextKey, Page page);
static void ResourceOwnerRememberBTreeSeqScan(ResourceOwner owner, BTreeSeqScan *scan);
static void ResourceOwnerForgetBTreeSeqScan(ResourceOwner owner, BTreeSeqScan *scan);

/*
 * Resource owner integration for BTreeSeqScan.
 *
 * Previously seq_scans_cleanup() only ran after transaction finish, so seq
 * scans were not released correctly on subtransaction finish, release of
 * prepared statements, etc.  Binding seq scans to ResourceOwner solves this.
 *
 * PG >= 17 uses custom ResourceOwner resources.  PG 16 uses a release
 * callback.
 */
#if PG_VERSION_NUM >= 170000
static void ResOwnerReleaseBTreeSeqScan(Datum res);
static char *ResOwnerPrintBTreeSeqScan(Datum res);

static const ResourceOwnerDesc btree_seq_scan_resowner_desc =
{
	.name = "OrioleDB BTreeSeqScans",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	.release_priority = RELEASE_PRIO_RELCACHE_REFS - 1,
	.ReleaseResource = ResOwnerReleaseBTreeSeqScan,
	.DebugPrint = ResOwnerPrintBTreeSeqScan
};
#endif

BTreeScanShmem *btreeScanShmem;

Size
btree_scan_shmem_needs(void)
{
	return CACHELINEALIGN(sizeof(BTreeScanShmem));
}

void
btree_scan_init_shmem(Pointer ptr, bool found)
{
	btreeScanShmem = (BTreeScanShmem *) ptr;

	if (!found)
	{
		btreeScanShmem->pageLoadTrancheId = LWLockNewTrancheId();
		btreeScanShmem->downlinksPublishTrancheId = LWLockNewTrancheId();
	}

	LWLockRegisterTranche(btreeScanShmem->pageLoadTrancheId,
						  "OBTreeScanPageLoadTrancheId");
	LWLockRegisterTranche(btreeScanShmem->downlinksPublishTrancheId,
						  "OBTreeScanDownlinksPublishTrancheId");
}


/*
 * Materialize the data chunk that `loc` points into for a partially-read leaf
 * image (bitmap FETCH scan).
 *
 * For a whole-page image -- every non-fetch scan, the historical image, and any
 * leaf we fully materialized because it carried undo -- leafPartial.isPartial
 * is false and this is a no-op.  Otherwise it copies the chunk from the live
 * shared page (rechecking the page change count); the chunk boundaries live in
 * the hikeys chunk, which o_btree_try_read_page already loaded, so the
 * locator's item offset is preserved.  A concurrent modification raises
 * scan->leafPartialFailed and returns false, and the caller must abandon the
 * partial image and re-read the current key range through an iterator (the
 * bitmap fetch layer rechecks each tuple, so over-reading the range is safe).
 */
static inline bool
seq_leaf_partial_ensure(BTreeSeqScan *scan, Page p, BTreePageItemLocator *loc)
{
	if (!scan->leafPartial.isPartial || p != scan->leafImg)
		return true;

	if (!partial_load_chunk(&scan->leafPartial, p, loc->chunkOffset, NULL))
	{
		scan->leafPartialFailed = true;
		return false;
	}
	return true;
}

/*
 * Materialize the hikeys chunk of the partially-read internal page (bitmap
 * FETCH scan).  The chunk-descriptor array and the chunk hikeys used to skip
 * downlinks live in it.  No-op for a whole-page image.  Sets
 * scan->intPartialFailed and returns false on a concurrent modification.
 */
static inline bool
seq_int_partial_ensure_hikeys(BTreeSeqScan *scan)
{
	if (!scan->context.partial.isPartial ||
		scan->context.partial.hikeysChunkIsLoaded)
		return true;

	if (!partial_load_hikeys_chunk(&scan->context.partial, scan->context.img))
	{
		scan->intPartialFailed = true;
		return false;
	}
	return true;
}

/*
 * Materialize the data chunk that `loc` points into for the partially-read
 * internal page in scan->context.img.  No-op for a whole-page image (every
 * parallel scan and any page re-read in IMAGE mode after a partial failure).
 * Sets scan->intPartialFailed and returns false on a concurrent modification.
 */
static inline bool
seq_int_partial_ensure(BTreeSeqScan *scan, BTreePageItemLocator *loc)
{
	if (!scan->context.partial.isPartial)
		return true;

	if (!partial_load_chunk(&scan->context.partial, scan->context.img,
							loc->chunkOffset, NULL))
	{
		scan->intPartialFailed = true;
		return false;
	}
	return true;
}

static void
load_first_historical_page(BTreeSeqScan *scan)
{
	BTreePageHeader *header = (BTreePageHeader *) scan->leafImg;
	Pointer		key = NULL;
	BTreeKeyType kind = BTreeKeyNone;
	OFixedKey	lokey,
			   *lokeyPtr = &lokey;
	OFixedKey	hikey;

	scan->haveHistImg = false;
	if (!COMMITSEQNO_IS_NORMAL(scan->oSnapshot.csn))
		return;

	if (!O_PAGE_IS(scan->leafImg, RIGHTMOST))
		copy_fixed_hikey(scan->desc, &hikey, scan->leafImg);
	else
		O_TUPLE_SET_NULL(hikey.tuple);
	O_TUPLE_SET_NULL(lokey.tuple);

	/*
	 * Seed histImg with the live page: differential page-level undo images
	 * (UndoPageImage*Diff) reconstruct the historical page by transforming
	 * the newer page in place, so the chain walk must start from the live
	 * page.
	 */
	memcpy(scan->histImg, scan->leafImg, ORIOLEDB_BLCKSZ);

	while (COMMITSEQNO_IS_NORMAL(header->csn) &&
		   header->csn >= scan->oSnapshot.csn)
	{
		if (!UNDO_REC_EXISTS(GET_PAGE_LEVEL_UNDO_TYPE(scan->desc->undoType),
							 header->undoLocation))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("snapshot too old")));
		}

		(void) get_page_from_undo(scan->desc, header->undoLocation, key, kind,
								  scan->histImg, NULL, NULL, NULL,
								  lokeyPtr, &hikey.tuple);

		if (!O_PAGE_IS(scan->histImg, RIGHTMOST))
			copy_fixed_hikey(scan->desc, &hikey, scan->histImg);
		else
			O_TUPLE_SET_NULL(hikey.tuple);

		scan->haveHistImg = true;
		header = (BTreePageHeader *) scan->histImg;
		if (!O_TUPLE_IS_NULL(lokey.tuple))
		{
			key = (Pointer) &lokey.tuple;
			kind = BTreeKeyNonLeafKey;
			lokeyPtr = NULL;
		}
	}

	if (!scan->haveHistImg)
		return;

	if (!O_TUPLE_IS_NULL(lokey.tuple))
	{
		(void) btree_page_search(scan->desc, scan->histImg,
								 (Pointer) &lokey.tuple,
								 BTreeKeyNonLeafKey, NULL,
								 &scan->histLoc);
		(void) page_locator_find_real_item(scan->histImg, NULL, &scan->histLoc);
	}
	else
	{
		BTREE_PAGE_LOCATOR_FIRST(scan->histImg, &scan->histLoc);
	}

}

static void
load_next_historical_page(BTreeSeqScan *scan)
{
	BTreePageHeader *header = (BTreePageHeader *) scan->leafImg;
	OFixedKey	prevHikey;

	copy_fixed_hikey(scan->desc, &prevHikey, scan->histImg);

	/*
	 * Re-seed histImg with the live page before reconstructing the next
	 * historical sub-page.  Differential undo images transform the page in
	 * place, so each reconstruction must restart from the live (merged) page
	 * rather than the previous sub-page left in histImg.  prevHikey above was
	 * captured from that previous sub-page first.
	 */
	memcpy(scan->histImg, scan->leafImg, ORIOLEDB_BLCKSZ);

	while (COMMITSEQNO_IS_NORMAL(header->csn) &&
		   header->csn >= scan->oSnapshot.csn)
	{
		if (!UNDO_REC_EXISTS(GET_PAGE_LEVEL_UNDO_TYPE(scan->desc->undoType),
							 header->undoLocation))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("snapshot too old")));
		}
		(void) get_page_from_undo(scan->desc, header->undoLocation,
								  (Pointer) &prevHikey.tuple, BTreeKeyNonLeafKey,
								  scan->histImg, NULL, NULL, NULL,
								  NULL, NULL);
		header = (BTreePageHeader *) scan->histImg;
	}
	BTREE_PAGE_LOCATOR_FIRST(scan->histImg, &scan->histLoc);
}

static Jsonb *
btree_lokey_stopevent_params(BTreeDescr *desc, OTuple lokey,
							 bool prevIsLeftmostOrNone)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;
	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(desc, &state);
	jsonb_push_key(&state, "lokey");
	(void) o_btree_key_to_jsonb(desc, lokey, &state);
	jsonb_push_bool_key(&state, "prevIsLeftmostOrNone", prevIsLeftmostOrNone);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

/*
 * Loads next internal page and. Outputs page, start locator and offset.
 *
 * In case of parallel scan the caller should hold a lock preventing the other workers from modifying
 * a page in a shared state and updating prevHikey.
 */
static bool
load_next_internal_page(BTreeSeqScan *scan, OTuple prevHikey,
						Page page,
						BTreePageItemLocator *intLoc,
						OffsetNumber *startOffset,
						const bool prevIsLeftmostOrNone,
						bool isBitmapJump)
{
	bool		has_next = false;
	OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;

	CHECK_FOR_INTERRUPTS();
	elog(DEBUG3, "load_next_internal_page");

	/*
	 * A non-parallel bitmap (FETCH) scan reads the level-1 internal page
	 * partially: find_page() in FETCH mode leaves it in scan->context.img
	 * with scan->context.partial tracking the loaded chunks, and the
	 * bitmap-driven walk below skips whole chunks of downlinks it does not
	 * need.  Parallel scans (page != NULL) share the whole page through DSM,
	 * and a page whose partial read lost a race (intPartialFailed) is re-read
	 * whole; both use IMAGE.  The retry loop below re-reads whole if
	 * materializing the hikeys chunk (needed to iterate/skip) loses a race
	 * after the partial read.
	 */
	while (true)
	{
		bool		useFetch = scan->fetch && !page && !scan->intPartialFailed;

		if (useFetch)
		{
			BTREE_PAGE_FIND_UNSET(&scan->context, IMAGE);
			BTREE_PAGE_FIND_SET(&scan->context, FETCH);
		}
		else
		{
			BTREE_PAGE_FIND_UNSET(&scan->context, FETCH);
			BTREE_PAGE_FIND_SET(&scan->context, IMAGE);
		}
		scan->context.flags |= BTREE_PAGE_FIND_DOWNLINK_LOCATION;

		if (!O_TUPLE_IS_NULL(prevHikey))
		{
			STOPEVENT(STOPEVENT_SEQ_SCAN_LOAD_INTERNAL_PAGE,
					  btree_lokey_stopevent_params(scan->desc, prevHikey, prevIsLeftmostOrNone));
			findResult = find_page(&scan->context, &prevHikey, BTreeKeyNonLeafKey, 1);
		}
		else
		{
			findResult = find_page(&scan->context, NULL, BTreeKeyNone, 1);
		}
		Assert(findResult == OFindPageResultSuccess);

		if (scan->context.partial.isPartial)
		{
			/*
			 * find_page() returns a page below targetLevel when the tree is
			 * shallower than requested (a single leaf, no level-1 page).  The
			 * level-0 branch below copies the whole page into leafImg, so
			 * re-read it in IMAGE mode.  (The fixed page header is
			 * materialized by the partial read, so PAGE_GET_LEVEL is valid
			 * here.)
			 */
			if (PAGE_GET_LEVEL(scan->context.img) != 1)
			{
				scan->intPartialFailed = true;
				continue;
			}

			/*
			 * A partial read needs the hikeys chunk materialized before we
			 * touch item offsets, iterate, or skip.  If that loses a race
			 * with a concurrent modification, fall back to a whole-page IMAGE
			 * read.
			 */
			if (!seq_int_partial_ensure_hikeys(scan))
			{
				Assert(scan->intPartialFailed);
				continue;
			}
		}
		break;
	}

	/*
	 * The page is now consistently loaded (partially in FETCH, whole after an
	 * IMAGE fallback).  Clear the failure latch so the next page can be tried
	 * partially again.
	 */
	scan->intPartialFailed = false;

	/* In case of parallel scan copy page image into shared state */
	if (page)
	{
		Assert(scan->poscan);
		Assert(!scan->context.partial.isPartial);
		memcpy(page, scan->context.img, ORIOLEDB_BLCKSZ);
	}
	else
	{
		Assert(!scan->poscan);
		scan->firstPageIsLoaded = true;
		page = scan->context.img;
	}

	if (PAGE_GET_LEVEL(page) == 1)
	{
		/*
		 * Check if the left bound of the found keyrange corresponds to the
		 * previous hikey.  Otherwise, use iterator to correct the situation.
		 */
		*intLoc = scan->context.items[scan->context.index].locator;
		*startOffset = BTREE_PAGE_LOCATOR_GET_OFFSET(page, intLoc);
		if (isBitmapJump)
		{
			/*
			 * A bitmap page skip is a fresh descent straight to the wanted
			 * key, so there is no continuity with a previous page to verify
			 * (and hence no iterator to build).  find_page() landed on the
			 * downlink covering the key; record the page's low key as
			 * prevHikey so get_current_downlink_key() derives the first
			 * downlink's low bound from it.
			 */
			if (O_TUPLE_IS_NULL(scan->context.lokey.tuple))
				clear_fixed_key(&scan->prevHikey);
			else
				copy_fixed_key(scan->desc, &scan->prevHikey,
							   scan->context.lokey.tuple);
		}
		else if (!O_TUPLE_IS_NULL(prevHikey))
		{
			OTuple		intTup;

			if (*startOffset > 0)
				BTREE_PAGE_READ_INTERNAL_TUPLE(intTup, page, intLoc);
			else
				intTup = scan->context.lokey.tuple;

			if (O_TUPLE_IS_NULL(intTup) ||
				o_btree_cmp(scan->desc,
							&prevHikey, BTreeKeyNonLeafKey,
							&intTup, BTreeKeyNonLeafKey) != 0)
			{
				get_next_key(scan, intLoc, &scan->keyRangeHigh, page);
				elog(DEBUG3, "scan_make_iterator");

				scan_make_iterator(scan, prevHikey, scan->keyRangeHigh.tuple);
			}
		}
		has_next = true;
	}
	else
	{
		Assert(PAGE_GET_LEVEL(page) == 0);
		memcpy(scan->leafImg, page, ORIOLEDB_BLCKSZ);
		/* A whole-page leaf: no partial-read state must leak into it. */
		scan->leafPartial.isPartial = false;
		scan->haveLastLeafKey = false;
		BTREE_PAGE_LOCATOR_FIRST(scan->leafImg, &scan->leafLoc);
		scan->hint.blkno = scan->context.items[0].blkno;
		scan->hint.pageChangeCount = scan->context.items[0].pageChangeCount;
		BTREE_PAGE_LOCATOR_SET_INVALID(&scan->intLoc);
		O_TUPLE_SET_NULL(scan->nextKey.tuple);
		load_first_historical_page(scan);
		has_next = false;
	}
	return has_next;
}

static void
add_on_disk_downlink(BTreeSeqScan *scan, uint64 downlink, CommitSeqNo csn)
{
	ParallelOScanDesc poscan = scan->poscan;

	if (!poscan)
	{
		/* Non-parallel: use local array */
		if (scan->downlinksCount >= scan->allocatedDownlinks)
		{
			scan->allocatedDownlinks *= 2;
			scan->diskDownlinks = (BTreeSeqScanDiskDownlink *) repalloc_huge(scan->diskDownlinks,
																			 sizeof(scan->diskDownlinks[0]) * scan->allocatedDownlinks);
		}
		scan->diskDownlinks[scan->downlinksCount].downlink = downlink;
		scan->diskDownlinks[scan->downlinksCount].csn = csn;
		scan->downlinksCount++;
	}
	else
	{
		/* Parallel: write directly to shared DSM array */
		while (true)
		{
			uint64		index;
			BTreeSeqScanDiskDownlink *shared;

			LWLockAcquire(&poscan->downlinksPublish, LW_SHARED);

			/* Re-attach to DSM if it was reallocated */
			if (scan->dsmSeg == NULL ||
				dsm_segment_handle(scan->dsmSeg) != poscan->dsmHandle)
			{
				if (scan->dsmSeg)
					dsm_detach(scan->dsmSeg);
				scan->dsmSeg = dsm_attach(poscan->dsmHandle);
			}

			index = pg_atomic_fetch_add_u64(&poscan->downlinksCount, 1);

			if (index < poscan->dsmAllocated)
			{
				shared = (BTreeSeqScanDiskDownlink *) dsm_segment_address(scan->dsmSeg);
				shared[index].downlink = downlink;
				shared[index].csn = csn;
				LWLockRelease(&poscan->downlinksPublish);
				return;
			}

			/* Over capacity: undo increment, grow under exclusive lock */
			pg_atomic_fetch_sub_u64(&poscan->downlinksCount, 1);
			LWLockRelease(&poscan->downlinksPublish);

			LWLockAcquire(&poscan->downlinksPublish, LW_EXCLUSIVE);

			/* Re-check: another worker may have already grown it */
			if (poscan->dsmAllocated <= (uint64) index)
			{
				dsm_segment *newSeg;
				uint64		newAllocated = poscan->dsmAllocated * 2;
				uint64		oldCount = pg_atomic_read_u64(&poscan->downlinksCount);

				newSeg = dsm_create(MAXALIGN(newAllocated * sizeof(BTreeSeqScanDiskDownlink)), DSM_CREATE_NULL_IF_MAXSEGMENTS);
				if (newSeg == NULL)
				{
					if (scan->dsmSeg)
						dsm_detach(scan->dsmSeg);
					LWLockRelease(&poscan->downlinksPublish);
					ereport(ERROR,
							(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
							 errmsg("parallel scan failed: too many dynamic shared memory segments")));
				}

				if (oldCount > 0)
					memcpy(dsm_segment_address(newSeg),
						   dsm_segment_address(scan->dsmSeg),
						   oldCount * sizeof(BTreeSeqScanDiskDownlink));

				if (scan->dsmSeg)
					dsm_detach(scan->dsmSeg);
				scan->dsmSeg = newSeg;
				poscan->dsmHandle = dsm_segment_handle(newSeg);
				poscan->dsmAllocated = newAllocated;
			}

			LWLockRelease(&poscan->downlinksPublish);
			/* Retry the insert */
		}
	}
}

static int
cmp_downlinks(const void *p1, const void *p2)
{
	uint64		d1 = ((BTreeSeqScanDiskDownlink *) p1)->downlink;
	uint64		d2 = ((BTreeSeqScanDiskDownlink *) p2)->downlink;

	if (d1 < d2)
		return -1;
	else if (d1 == d2)
		return 0;
	else
		return 1;
}

static void
switch_to_disk_scan(BTreeSeqScan *scan)
{
	ParallelOScanDesc poscan = scan->poscan;

	scan->status = BTreeSeqScanDisk;
	BTREE_PAGE_LOCATOR_SET_INVALID(&scan->leafLoc);
	if (!poscan)
	{
		/* Serial scan */
		qsort(scan->diskDownlinks,
			  scan->downlinksCount,
			  sizeof(scan->diskDownlinks[0]),
			  cmp_downlinks);
	}
	else
	{
		/* Parallel scan */

		/*
		 * Wait for any in-flight add_on_disk_downlink() calls to complete. A
		 * worker that already got a disk downlink from get_next_downlink()
		 * but hasn't finished writing it yet will have incremented
		 * downlinksWritersInProgress.
		 */
		while (pg_atomic_read_u32(&poscan->downlinksWritersInProgress) > 0)
		{
			pg_usleep(10L);
			CHECK_FOR_INTERRUPTS();
		}

		/*
		 * First worker to grab the exclusive lock sorts the shared downlinks
		 * array.  Other workers wait on the lock and then see the sorted
		 * flag.
		 */
		LWLockAcquire(&poscan->downlinksPublish, LW_EXCLUSIVE);

		if (!(poscan->flags & O_PARALLEL_DOWNLINKS_SORTED))
		{
			uint64		count = pg_atomic_read_u64(&poscan->downlinksCount);

			/* Re-attach to DSM if it was reallocated */
			if (scan->dsmSeg == NULL ||
				dsm_segment_handle(scan->dsmSeg) != poscan->dsmHandle)
			{
				if (scan->dsmSeg)
					dsm_detach(scan->dsmSeg);
				scan->dsmSeg = dsm_attach(poscan->dsmHandle);
			}

			if (count > 0)
			{
				qsort(dsm_segment_address(scan->dsmSeg), count,
					  sizeof(BTreeSeqScanDiskDownlink), cmp_downlinks);
			}

			pg_atomic_write_u64(&poscan->downlinkIndex, 0);
			pg_write_barrier();
			poscan->flags |= O_PARALLEL_DOWNLINKS_SORTED;
		}

		LWLockRelease(&poscan->downlinksPublish);

		/* Ensure attached to current DSM for disk scan phase */
		if (poscan->dsmHandle &&
			(scan->dsmSeg == NULL ||
			 dsm_segment_handle(scan->dsmSeg) != poscan->dsmHandle))
		{
			if (scan->dsmSeg)
				dsm_detach(scan->dsmSeg);
			scan->dsmSeg = dsm_attach(poscan->dsmHandle);
		}
	}
}

/*
 * Make an interator to read the key range from `startKey` to the next
 * downlink or hikey of internal page hikey if we're considering the last
 * downlink.
 */
static void
scan_make_iterator(BTreeSeqScan *scan, OTuple keyRangeLow, OTuple keyRangeHigh)
{
	MemoryContext mctx;

	mctx = MemoryContextSwitchTo(scan->mctx);
	if (!O_TUPLE_IS_NULL(keyRangeLow))
		scan->iter = o_btree_iterator_create(scan->desc, &keyRangeLow, BTreeKeyNonLeafKey,
											 &scan->oSnapshot, ForwardScanDirection);
	else
		scan->iter = o_btree_iterator_create(scan->desc, NULL, BTreeKeyNone,
											 &scan->oSnapshot, ForwardScanDirection);
	MemoryContextSwitchTo(mctx);

	BTREE_PAGE_LOCATOR_SET_INVALID(&scan->leafLoc);
	scan->haveHistImg = false;
	scan->iterEnd = keyRangeHigh;

	/*
	 * The iterator produces tuples directly; the partial leaf image (if any)
	 * is abandoned, so make on-demand chunk loads inert until the next leaf
	 * read.
	 */
	scan->leafPartial.isPartial = false;
	scan->leafPartialFailed = false;
	scan->haveLastLeafKey = false;
}

/* Output item downlink and key using provided page and current locator */
static void
get_current_downlink_key(BTreeSeqScan *scan,
						 BTreePageItemLocator *loc,
						 OffsetNumber startOffset,
						 OTuple prevHiKey,
						 OFixedKey *curKey,
						 uint64 *downlink,
						 Page page)
{
	BTreeNonLeafTuphdr *tuphdr;
	OTuple		tuple;

	/*
	 * Partial FETCH read of the internal page: materialize the chunk holding
	 * this downlink before reading it.  On a concurrent modification bail
	 * with intPartialFailed set; get_next_downlink() re-reads the page whole.
	 */
	if (!seq_int_partial_ensure(scan, loc))
	{
		*downlink = 0;
		clear_fixed_key(curKey);
		return;
	}

	STOPEVENT(STOPEVENT_STEP_DOWN, btree_downlink_stopevent_params(scan->desc,
																   page, loc));

	BTREE_PAGE_READ_INTERNAL_ITEM(tuphdr, tuple, page, loc);
	*downlink = tuphdr->downlink;

	if (BTREE_PAGE_LOCATOR_GET_OFFSET(page, loc) != startOffset)
	{
		copy_fixed_key(scan->desc, curKey, tuple);
	}
	else if (!O_PAGE_IS(page, LEFTMOST))
	{
		Assert(!O_TUPLE_IS_NULL(prevHiKey));
		copy_fixed_key(scan->desc, curKey, prevHiKey);
	}
	else
	{
		/*
		 * It might happen that due to concurrent page merge, we're visiting
		 * the leftmost page the second time.  In this case, prevHiKey is not
		 * NULL, so there is no assertion here.
		 */
		clear_fixed_key(curKey);
	}
}

/* Output next key and locator on a provided internal page */
static void
get_next_key(BTreeSeqScan *scan, BTreePageItemLocator *intLoc, OFixedKey *nextKey, Page page)
{
	BTREE_PAGE_LOCATOR_NEXT(page, intLoc);
	if (BTREE_PAGE_LOCATOR_IS_VALID(page, intLoc))
	{
		/*
		 * The chunk-descriptor array in the (loaded) hikeys chunk lets
		 * BTREE_PAGE_LOCATOR_NEXT step across a chunk boundary without the
		 * data chunk; materialize the landed chunk before reading its key.
		 */
		if (!seq_int_partial_ensure(scan, intLoc))
		{
			clear_fixed_key(nextKey);
			return;
		}
		copy_fixed_page_key(scan->desc, nextKey, page, intLoc);
	}
	else if (!O_PAGE_IS(page, RIGHTMOST))
		copy_fixed_hikey(scan->desc, nextKey, page);
	else
		clear_fixed_key(nextKey);
}

/*
 * Bitmap FETCH within-page skip.  On entry scan->intLoc points at the downlink
 * immediately following the one just returned (its low bound is `boundary`).
 * Advance scan->intLoc to the downlink covering the next bitmap key at or after
 * `boundary`, skipping whole chunks of unwanted downlinks -- btree_page_search()
 * jumps via the chunk hikeys and materializes only the target chunk, so the
 * skipped chunks are never copied out of the shared page.  Mirrors the leaf's
 * adjust_location_with_next_key().
 *
 * Leaves scan->intLoc invalid (caller loads/descends to the next page) when the
 * next wanted key is beyond this page's hikey or the bitmap is exhausted.  On a
 * concurrent modification sets scan->intPartialFailed and returns; scan->intLoc
 * is then unusable and the next get_next_downlink() call re-reads the page whole
 * before touching it.
 */
static void
internal_skip_to_next_key(BTreeSeqScan *scan, Page page,
						  BTreePageItemLocator *intLoc, OTuple boundary)
{
	OFixedKey	probe;

	/* A leftmost gap has no key to probe with; fall back to sequential. */
	if (O_TUPLE_IS_NULL(boundary))
		return;

	copy_fixed_key(scan->desc, &probe, boundary);
	if (!scan->cb->getNextKey(&probe, BTreeKeyNonLeafKey, true, scan->arg))
	{
		/* Nothing left in the bitmap anywhere. */
		BTREE_PAGE_LOCATOR_SET_INVALID(intLoc);
		return;
	}

	/* Beyond this page: let the caller descend to the covering page. */
	if (!O_PAGE_IS(page, RIGHTMOST))
	{
		OFixedKey	hikey;

		copy_fixed_hikey(scan->desc, &hikey, page);
		if (o_btree_cmp(scan->desc, &probe.tuple, BTreeKeyNonLeafKey,
						&hikey.tuple, BTreeKeyNonLeafKey) >= 0)
		{
			BTREE_PAGE_LOCATOR_SET_INVALID(intLoc);
			return;
		}
	}

	/*
	 * Within this page: position intLoc on the downlink covering the key.
	 * btree_page_search() lands one past the covering downlink (on the first
	 * separator strictly greater than the key), so step back and materialize
	 * the landed chunk -- exactly as page_find_downlink() does during
	 * descent.
	 */
	if (!btree_page_search(scan->desc, page, (Pointer) &probe.tuple,
						   BTreeKeyNonLeafKey, &scan->context.partial, intLoc))
	{
		scan->intPartialFailed = true;
		return;
	}
	BTREE_PAGE_LOCATOR_PREV(page, intLoc);
	if (scan->context.partial.isPartial &&
		!partial_load_chunk(&scan->context.partial, page, intLoc->chunkOffset,
							NULL))
		scan->intPartialFailed = true;
}

/*
 * Gets the next downlink with it's keyrange (low and high keys of the
 * keyrange).
 *
 * Returns true on success.  False result can be caused by one of three reasons:
 * 1) The rightmost internal page is processed;
 * 2) There is just single leaf page in the tree (and it's loaded into
 *    scan->context.img);
 * 3) There is scan->iter to be processed before we can get downlinks from the
 *    current internal page.
 */
static bool
get_next_downlink(BTreeSeqScan *scan, uint64 *downlink,
				  OFixedKey *keyRangeLow, OFixedKey *keyRangeHigh)
{
	ParallelOScanDesc poscan = scan->poscan;

	if (!poscan)
	{
		/* Non-parallel case */
		bool		pageIsLoaded = scan->firstPageIsLoaded;
		bool		prevIsLeftmostOrNone = true;

		while (true)
		{

			/* Try to load next internal page if needed */
			if (!pageIsLoaded)
			{
				/*
				 * A bitmap scan can skip whole internal pages that hold no
				 * wanted keys: ask the callback for the next needed key at or
				 * after the finished page's hikey and descend straight to the
				 * page covering it, instead of stepping to the immediate next
				 * page.  The landed downlink is fed to the same in-memory /
				 * on-disk handling below, so the per-leaf FETCH walk still
				 * applies.
				 */
				if (scan->cb && scan->cb->getNextKey)
				{
					OFixedKey	jumpKey;

					if (scan->firstPageIsLoaded)
						copy_fixed_hikey(scan->desc, &jumpKey, scan->context.img);
					else
						clear_fixed_key(&jumpKey);

					if (!scan->cb->getNextKey(&jumpKey, BTreeKeyNonLeafKey,
											  true, scan->arg))
					{
						/* Nothing left in the bitmap. */
						clear_fixed_key(keyRangeLow);
						clear_fixed_key(keyRangeHigh);
						return false;
					}

					if (!load_next_internal_page(scan, jumpKey.tuple,
												 NULL,
												 &scan->intLoc,
												 &scan->intStartOffset,
												 true,
												 true))
					{
						/* Single leaf page (tree has no internal level). */
						scan->isSingleLeafPage = true;
						clear_fixed_key(keyRangeLow);
						clear_fixed_key(keyRangeHigh);
						return false;
					}

					/* A jump is a fresh descent; it never builds an iterator. */
					Assert(!scan->iter);
				}
				else
				{
					if (scan->firstPageIsLoaded)
					{
						Assert(!O_PAGE_IS(scan->context.img, RIGHTMOST));
						if (scan->context.img)
							prevIsLeftmostOrNone = O_PAGE_IS(scan->context.img, LEFTMOST);
						copy_fixed_hikey(scan->desc, &scan->prevHikey, scan->context.img);
					}

					if (!load_next_internal_page(scan, scan->prevHikey.tuple,
												 NULL,
												 &scan->intLoc,
												 &scan->intStartOffset,
												 prevIsLeftmostOrNone,
												 false))
					{
						/* first page only */
						Assert(O_PAGE_IS(scan->context.img, LEFTMOST));
						scan->isSingleLeafPage = true;
						clear_fixed_key(keyRangeLow);
						clear_fixed_key(keyRangeHigh);
						return false;
					}

					if (scan->iter)
						return false;
				}
			}

			/*
			 * A prior call's within-page skip lost the race with a concurrent
			 * modification after it had already returned its downlink,
			 * leaving scan->intLoc unusable.  Re-read the internal page whole
			 * (IMAGE) by descending to the resume boundary before touching
			 * it; load_next_internal_page() clears intPartialFailed and lands
			 * scan->intLoc on the downlink covering intResumeKey.
			 */
			if (pageIsLoaded && scan->intPartialFailed)
			{
				OTuple		resumeKey;

				if (scan->haveIntResumeKey)
					resumeKey = scan->intResumeKey.tuple;
				else
					O_TUPLE_SET_NULL(resumeKey);

				if (!load_next_internal_page(scan, resumeKey, NULL,
											 &scan->intLoc, &scan->intStartOffset,
											 true, true))
				{
					scan->isSingleLeafPage = true;
					clear_fixed_key(keyRangeLow);
					clear_fixed_key(keyRangeHigh);
					return false;
				}
				Assert(!scan->intPartialFailed);
			}

			if (BTREE_PAGE_LOCATOR_IS_VALID(scan->context.img, &scan->intLoc))
			{
				get_current_downlink_key(scan, &scan->intLoc, scan->intStartOffset,
										 scan->prevHikey.tuple, keyRangeLow,
										 downlink, scan->context.img);

				/*
				 * construct fixed hikey of internal item and get next
				 * internal locator
				 */
				if (!scan->intPartialFailed)
					get_next_key(scan, &scan->intLoc, keyRangeHigh, scan->context.img);

				if (scan->intPartialFailed)
				{
					/*
					 * A partial chunk load for the current downlink lost the
					 * race.  Re-read the page whole from the resume boundary
					 * and recompute this downlink (it has not been returned
					 * yet).
					 */
					OTuple		resumeKey;

					if (scan->haveIntResumeKey)
						resumeKey = scan->intResumeKey.tuple;
					else
						O_TUPLE_SET_NULL(resumeKey);

					if (!load_next_internal_page(scan, resumeKey, NULL,
												 &scan->intLoc, &scan->intStartOffset,
												 true, true))
					{
						scan->isSingleLeafPage = true;
						clear_fixed_key(keyRangeLow);
						clear_fixed_key(keyRangeHigh);
						return false;
					}
					Assert(!scan->intPartialFailed);
					continue;
				}

				/* Remember where the walk resumes (this downlink's high key). */
				scan->haveIntResumeKey = !O_TUPLE_IS_NULL(keyRangeHigh->tuple);
				if (scan->haveIntResumeKey)
					copy_fixed_key(scan->desc, &scan->intResumeKey,
								   keyRangeHigh->tuple);

				/*
				 * Bitmap-driven within-page skip: advance scan->intLoc
				 * straight to the downlink covering the next wanted key,
				 * skipping whole chunks of downlinks the bitmap does not
				 * need.
				 */
				if (scan->fetch && scan->cb && scan->cb->getNextKey &&
					BTREE_PAGE_LOCATOR_IS_VALID(scan->context.img, &scan->intLoc))
					internal_skip_to_next_key(scan, scan->context.img,
											  &scan->intLoc, keyRangeHigh->tuple);

				return true;
			}

			if (O_PAGE_IS(scan->context.img, RIGHTMOST))
				return false;

			pageIsLoaded = false;
		}
	}
	else
	{
		/* Parallel case */
		while (true)
		{
			BTreeIntPageParallelData *curPage;
			BTreeIntPageParallelData *nextPage;
			BTreePageItemLocator loc;

			SpinLockAcquire(&poscan->intpageAccess);
			curPage = CUR_PAGE(poscan);
			nextPage = NEXT_PAGE(poscan);

			if (poscan->flags & O_PARALLEL_IS_SINGLE_LEAF_PAGE)
			{
				SpinLockRelease(&poscan->intpageAccess);
				scan->haveHistImg = false;
				BTREE_PAGE_LOCATOR_SET_INVALID(&scan->leafLoc);
				return false;
			}

			if (curPage->status == OParallelScanPageInvalid)
			{
				bool		next_loaded;

				Assert(nextPage->status == OParallelScanPageInvalid);

				if (!(poscan->flags & O_PARALLEL_FIRST_PAGE_LOADED))
				{
					clear_fixed_shmem_key(&curPage->prevHikey);
				}
				else
				{
					Assert(O_PAGE_IS(nextPage->img, RIGHTMOST));
					SpinLockRelease(&poscan->intpageAccess);
					return false;
				}
				curPage->status = OParallelScanPageInProgress;
				LWLockAcquire(&poscan->intpageLoad, LW_EXCLUSIVE);
				SpinLockRelease(&poscan->intpageAccess);

				next_loaded = load_next_internal_page(scan,
													  fixed_shmem_key_get_tuple(&curPage->prevHikey),
													  curPage->img,
													  &loc,
													  &curPage->startOffset,
													  false,
													  false);
				if (!next_loaded)
				{
					SpinLockAcquire(&poscan->intpageAccess);
					poscan->flags |= O_PARALLEL_IS_SINGLE_LEAF_PAGE;
					clear_fixed_key(keyRangeLow);
					clear_fixed_key(keyRangeHigh);
					SpinLockRelease(&poscan->intpageAccess);
					LWLockRelease(&poscan->intpageLoad);
					return false;
				}

				SpinLockAcquire(&poscan->intpageAccess);
				curPage->imgReadCsn = scan->context.imgReadCsn;
				curPage->offset = BTREE_PAGE_LOCATOR_GET_OFFSET(curPage->img, &loc);
				curPage->status = OParallelScanPageValid;
				poscan->flags |= O_PARALLEL_FIRST_PAGE_LOADED;
				SpinLockRelease(&poscan->intpageAccess);
				LWLockRelease(&poscan->intpageLoad);

				if (scan->iter)
					return false;
				continue;
			}
			else if (curPage->status == OParallelScanPageInProgress)
			{
				SpinLockRelease(&poscan->intpageAccess);
				if (LWLockAcquireOrWait(&poscan->intpageLoad, LW_EXCLUSIVE))
					LWLockRelease(&poscan->intpageLoad);
				continue;
			}

			if (nextPage->status == OParallelScanPageInvalid &&
				!O_PAGE_IS(curPage->img, RIGHTMOST))
			{
				bool		next_loaded PG_USED_FOR_ASSERTS_ONLY;

				copy_fixed_shmem_hikey(scan->desc, &nextPage->prevHikey, curPage->img);
				nextPage->status = OParallelScanPageInProgress;
				LWLockAcquire(&poscan->intpageLoad, LW_EXCLUSIVE);
				SpinLockRelease(&poscan->intpageAccess);

				next_loaded = load_next_internal_page(scan,
													  fixed_shmem_key_get_tuple(&nextPage->prevHikey),
													  nextPage->img,
													  &loc,
													  &nextPage->startOffset,
													  false,
													  false);
				Assert(next_loaded);

				SpinLockAcquire(&poscan->intpageAccess);
				nextPage->imgReadCsn = scan->context.imgReadCsn;
				nextPage->offset = BTREE_PAGE_LOCATOR_GET_OFFSET(nextPage->img, &loc);
				nextPage->status = OParallelScanPageValid;
				SpinLockRelease(&poscan->intpageAccess);
				LWLockRelease(&poscan->intpageLoad);

				if (scan->iter)
					return false;
				continue;
			}

			BTREE_PAGE_OFFSET_GET_LOCATOR(curPage->img, curPage->offset, &loc);

			if (BTREE_PAGE_LOCATOR_IS_VALID(curPage->img, &loc))	/* inside int page */
			{
				get_current_downlink_key(scan, &loc, curPage->startOffset,
										 fixed_shmem_key_get_tuple(&curPage->prevHikey),
										 keyRangeLow, downlink, curPage->img);
				/* Get next internal page locator and next internal item hikey */
				get_next_key(scan, &loc, keyRangeHigh, curPage->img);

				/* Push next internal item page offset into shared state */
				curPage->offset = BTREE_PAGE_LOCATOR_GET_OFFSET(curPage->img, &loc);
				scan->context.imgReadCsn = curPage->imgReadCsn;

				/*
				 * Become the shared downlink writer. This is to be cleared by
				 * the caller: immediately for in-memory and in IO downlinks,
				 * after downlink is written to shared DSM array for disk
				 * downlinks.
				 */
				pg_atomic_fetch_add_u32(&poscan->downlinksWritersInProgress, 1);

				SpinLockRelease(&poscan->intpageAccess);
				return true;
			}
			else
			{
				curPage->status = OParallelScanPageInvalid;
				poscan->flags ^= O_PARALLEL_CURRENT_PAGE;
				SpinLockRelease(&poscan->intpageAccess);
			}
		}
	}
}

/*
 * Checks if loaded leaf page matches downlink of internal page.  Makes iterator
 * to read the considered key range if check failed.
 *
 * Hikey of leaf page should match to next downlink or internal page hikey if
 * we're considering the last downlink.
 */
static void
check_in_memory_leaf_page(BTreeSeqScan *scan, OTuple keyRangeLow, OTuple keyRangeHigh)
{
	OTuple		leafHikey;
	bool		result = false;

	if (!O_PAGE_IS(scan->leafImg, RIGHTMOST))
		BTREE_PAGE_GET_HIKEY(leafHikey, scan->leafImg);
	else
		O_TUPLE_SET_NULL(leafHikey);

	if (O_TUPLE_IS_NULL(keyRangeHigh) && O_TUPLE_IS_NULL(leafHikey))
		return;

	if (O_TUPLE_IS_NULL(keyRangeHigh) || O_TUPLE_IS_NULL(leafHikey))
	{
		result = true;
	}
	else
	{
		if (o_btree_cmp(scan->desc,
						&keyRangeHigh, BTreeKeyNonLeafKey,
						&leafHikey, BTreeKeyNonLeafKey) != 0)
			result = true;
	}

	if (result)
	{
		elog(DEBUG3, "scan_make_iterator 2");
		scan_make_iterator(scan, keyRangeLow, keyRangeHigh);
	}
}


/*
 * Interates the internal page till we either:
 *  - Successfully read the next in-memory leaf page;
 *  - Made an iterator to read key range, which belongs to current downlink;
 *  - Reached the end of internal page.
 */
static bool
iterate_internal_page(BTreeSeqScan *scan)
{
	uint64		downlink = 0;

	while (get_next_downlink(scan, &downlink, &scan->keyRangeLow, &scan->keyRangeHigh))
	{
		bool		valid_downlink = true;

		if (scan->cb && scan->cb->isRangeValid)
			valid_downlink = scan->cb->isRangeValid(scan->keyRangeLow.tuple, scan->keyRangeHigh.tuple,
													scan->arg);
		else if (scan->needSampling)
		{
			if (scan->samplingNumber < scan->samplingNext)
			{
				valid_downlink = false;
			}
			else
			{
				if (BlockSampler_HasMore(scan->sampler))
					scan->samplingNext = BlockSampler_Next(scan->sampler);
				else
					scan->samplingNext = InvalidBlockNumber;
			}
			scan->samplingNumber++;
		}

		if (valid_downlink)
		{
			if (DOWNLINK_IS_ON_DISK(downlink))
			{
				add_on_disk_downlink(scan, downlink, scan->context.imgReadCsn);
				if (scan->poscan)
					pg_atomic_fetch_sub_u32(&scan->poscan->downlinksWritersInProgress, 1);
			}
			else if (DOWNLINK_IS_IN_MEMORY(downlink))
			{
				ReadPageResult result;
				PartialPageState *leafPartial = NULL;

				if (scan->poscan)
					pg_atomic_fetch_sub_u32(&scan->poscan->downlinksWritersInProgress, 1);

				if (scan->fetch)
				{
					/*
					 * Read the leaf partially: only the chunks the bitmap's
					 * key-driven walk actually visits are copied from the
					 * live shared page (on demand, via
					 * seq_leaf_partial_ensure).
					 */
					scan->leafPartial.isPartial = true;
					scan->leafPartial.hikeysChunkIsLoaded = false;
					memset(scan->leafPartial.chunkIsLoaded, 0,
						   sizeof(scan->leafPartial.chunkIsLoaded));
					scan->leafPartialFailed = false;
					scan->haveLastLeafKey = false;
					leafPartial = &scan->leafPartial;
				}

				result = o_btree_try_read_page(scan->desc,
											   DOWNLINK_GET_IN_MEMORY_BLKNO(downlink),
											   DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(downlink),
											   scan->leafImg,
											   scan->context.imgReadCsn,
											   NULL,
											   BTreeKeyNone,
											   leafPartial,
											   true,
											   NULL);

				if (result == ReadPageResultOK)
				{
					check_in_memory_leaf_page(scan, scan->keyRangeLow.tuple, scan->keyRangeHigh.tuple);
					if (scan->iter)
						return true;

					/*
					 * A leaf carrying historical/undo data is read whole: the
					 * historical merge in btree_seq_scan_getnext_internal()
					 * and load_first_historical_page() walk leaf items
					 * directly and seed histImg with the full page.
					 * o_btree_try_read_page() already materialized the page
					 * for its own undo replay when imgReadCsn required it;
					 * materialize explicitly against the scan snapshot too so
					 * a partial image never reaches those paths.  A
					 * concurrent change during materialization falls back to
					 * an iterator over the current key range.
					 */
					if (leafPartial && scan->leafPartial.isPartial &&
						COMMITSEQNO_IS_NORMAL(scan->oSnapshot.csn) &&
						((BTreePageHeader *) scan->leafImg)->csn >= scan->oSnapshot.csn)
					{
						if (!partial_load_full_page(&scan->leafPartial, scan->leafImg))
						{
							scan_make_iterator(scan, scan->keyRangeLow.tuple,
											   scan->keyRangeHigh.tuple);
							Assert(scan->iter);
							return true;
						}
					}

					scan->hint.blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(downlink);
					scan->hint.pageChangeCount = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(downlink);
					BTREE_PAGE_LOCATOR_FIRST(scan->leafImg, &scan->leafLoc);
					O_TUPLE_SET_NULL(scan->nextKey.tuple);
					load_first_historical_page(scan);
					return true;
				}
				else
				{
					scan_make_iterator(scan, scan->keyRangeLow.tuple, scan->keyRangeHigh.tuple);
					Assert(scan->iter);
					return true;
				}
			}
			else if (DOWNLINK_IS_IN_IO(downlink))
			{
				/*
				 * Downlink has currently IO in-progress.  Wait for IO
				 * completion and refind this downlink.
				 */
				int			ionum = DOWNLINK_GET_IO_LOCKNUM(downlink);

				if (scan->poscan)
					pg_atomic_fetch_sub_u32(&scan->poscan->downlinksWritersInProgress, 1);

				wait_for_io_completion(ionum);

				elog(DEBUG3, "DOWNLINK_IS_IN_IO");
				scan_make_iterator(scan, scan->keyRangeLow.tuple, scan->keyRangeHigh.tuple);
				Assert(scan->iter);
				return true;
			}
		}
		else if (scan->poscan)
		{
			pg_atomic_fetch_sub_u32(&scan->poscan->downlinksWritersInProgress, 1);
		}
	}

	if (scan->iter)
		return true;

	elog(DEBUG3, "Worker %d iterate_internal_page complete", scan->workerNumber);
	return false;
}

static bool
load_next_disk_leaf_page(BTreeSeqScan *scan)
{
	FileExtent	extent;
	bool		success;
	BTreePageHeader *header;
	BTreeSeqScanDiskDownlink downlink;
	ParallelOScanDesc poscan = scan->poscan;

	if (!poscan)
	{
		if (scan->downlinkIndex >= scan->downlinksCount)
			return false;

		downlink = scan->diskDownlinks[scan->downlinkIndex];
	}
	else
	{
		uint64		index = pg_atomic_fetch_add_u64(&poscan->downlinkIndex, 1);

		if (index >= pg_atomic_read_u64(&poscan->downlinksCount))
		{
			if (scan->dsmSeg)
			{
				dsm_detach(scan->dsmSeg);
				scan->dsmSeg = NULL;
			}
			return false;
		}
		downlink = ((BTreeSeqScanDiskDownlink *) dsm_segment_address(scan->dsmSeg))[index];
	}

	success = read_page_from_disk(scan->desc,
								  scan->leafImg,
								  downlink.downlink,
								  &extent);
	header = (BTreePageHeader *) scan->leafImg;
	if (header->csn >= downlink.csn)
		read_page_from_undo(scan->desc, scan->leafImg, header->undoLocation,
							downlink.csn, NULL, BTreeKeyNone, NULL);

	STOPEVENT(STOPEVENT_SCAN_DISK_PAGE,
			  btree_page_stopevent_params(scan->desc,
										  scan->leafImg));

	if (!success)
		elog(ERROR, "can not read leaf page from disk");

	/*
	 * A disk leaf is read whole into the private leafImg, so any partial-read
	 * state left over from a previous in-memory leaf must not leak into it
	 * (it would make seq_leaf_partial_ensure() re-read chunks from a stale
	 * source).
	 */
	scan->leafPartial.isPartial = false;
	scan->haveLastLeafKey = false;

	BTREE_PAGE_LOCATOR_FIRST(scan->leafImg, &scan->leafLoc);
	scan->downlinkIndex++;
	scan->hint.blkno = OInvalidInMemoryBlkno;
	scan->hint.pageChangeCount = InvalidOPageChangeCount;
	O_TUPLE_SET_NULL(scan->nextKey.tuple);
	load_first_historical_page(scan);
	return true;
}

static inline bool
single_leaf_page_rel(BTreeSeqScan *scan)
{
	if (scan->poscan)
		return (scan->poscan->flags & O_PARALLEL_IS_SINGLE_LEAF_PAGE) != 0;
	else
		return scan->isSingleLeafPage;
}

static void
init_checkpoit_number(BTreeSeqScan *scan)
{
	uint32		checkpointNumberBefore,
				checkpointNumberAfter;
	bool		checkpointConcurrent;
	BTreeMetaPage *metaPage;
	BTreeDescr *desc = scan->desc;

	o_btree_load_shmem(scan->desc);
	metaPage = BTREE_GET_META(scan->desc);

	START_CRIT_SECTION();

	/*
	 * Get the checkpoint number for the scan.  There is race condition with
	 * concurrent switching tree to the next checkpoint.  So, we have to
	 * workaround this with recheck-retry loop,
	 */
	checkpointNumberBefore = get_cur_checkpoint_number(&desc->oids,
													   desc->type,
													   &checkpointConcurrent);
	while (true)
	{
		(void) pg_atomic_fetch_add_u32(&metaPage->numSeqScans[checkpointNumberBefore % NUM_SEQ_SCANS_ARRAY_SIZE], 1);
		checkpointNumberAfter = get_cur_checkpoint_number(&desc->oids,
														  desc->type,
														  &checkpointConcurrent);
		if (checkpointNumberAfter == checkpointNumberBefore)
		{
			scan->checkpointNumber = checkpointNumberBefore;
			scan->checkpointNumberSet = true;
			break;
		}
		(void) pg_atomic_fetch_sub_u32(&metaPage->numSeqScans[checkpointNumberBefore % NUM_SEQ_SCANS_ARRAY_SIZE], 1);
		checkpointNumberBefore = checkpointNumberAfter;
	}
	END_CRIT_SECTION();
}

static void
init_btree_seq_scan(BTreeSeqScan *scan)
{
	ParallelOScanDesc poscan = scan->poscan;
	BlockSampler sampler = scan->sampler;
	BTreeDescr *desc = scan->desc;

	if (poscan)
	{
		/*
		 * Scan worker numbers are assigned by the order of workers init of
		 * local seqscan. In case of call seqscan in an index build worker,
		 * the numbers of scan workers, and who is a scan leader is not
		 * related to index build leader (who merges workers sort results
		 * after all workers completed their scans).
		 */
		SpinLockAcquire(&poscan->workerStart);
#ifdef USE_ASSERT_CHECKING
		for (scan->workerNumber = 0; poscan->worker_active[scan->workerNumber] == true; scan->workerNumber++)
		{
		}

		poscan->worker_active[scan->workerNumber] = true;
		poscan->nworkers = scan->workerNumber + 1;
#else
		scan->workerNumber = poscan->nworkers;
		poscan->nworkers++;
#endif
		/* Scan leader */
		if (scan->workerNumber == 0)
		{
			uint32		numLeafPages;
			uint64		allocSize;

			Assert(!(poscan->flags & O_PARALLEL_LEADER_STARTED));
			poscan->flags |= O_PARALLEL_LEADER_STARTED;
			init_checkpoit_number(scan);

			/*
			 * Create a shared DSM segment for on-disk downlinks upfront,
			 * sized to hold as many downlinks as there are leaf pages in the
			 * tree.
			 */
			numLeafPages = TREE_NUM_LEAF_PAGES(desc);
			if (numLeafPages < 16)
				numLeafPages = 16;
			allocSize = MAXALIGN((uint64) numLeafPages * sizeof(BTreeSeqScanDiskDownlink));
			scan->dsmSeg = dsm_create(allocSize, DSM_CREATE_NULL_IF_MAXSEGMENTS);
			if (scan->dsmSeg == NULL)
			{
				SpinLockRelease(&poscan->workerStart);
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						 errmsg("parallel scan failed: too many dynamic shared memory segments")));
			}
			poscan->dsmHandle = dsm_segment_handle(scan->dsmSeg);
			poscan->dsmAllocated = numLeafPages;
			pg_write_barrier();
			poscan->flags |= O_PARALLEL_DSM_CREATED;
		}
		SpinLockRelease(&poscan->workerStart);

		/* Non-leader workers: wait for DSM creation and attach */
		Assert(scan->workerNumber >= 0);
		if (scan->workerNumber > 0)
		{
			while (!(poscan->flags & O_PARALLEL_DSM_CREATED))
			{
				pg_usleep(100L);
				CHECK_FOR_INTERRUPTS();
			}
			pg_read_barrier();
			if (poscan->dsmHandle)
				scan->dsmSeg = dsm_attach(poscan->dsmHandle);
		}

		elog(DEBUG3, "init_btree_seq_scan. %s %d started", poscan ? "Parallel worker" : "Worker", scan->workerNumber);
	}
	else
	{
		scan->workerNumber = -1;
		init_checkpoit_number(scan);
	}

	if (sampler)
	{
		scan->needSampling = true;
		if (BlockSampler_HasMore(scan->sampler))
			scan->samplingNext = BlockSampler_Next(scan->sampler);
		else
			scan->samplingNext = InvalidBlockNumber;
	}
	else
	{
		scan->needSampling = false;
		scan->samplingNext = InvalidBlockNumber;
	}

	O_TUPLE_SET_NULL(scan->nextKey.tuple);

	init_page_find_context(&scan->context, desc, scan->oSnapshot.csn,
						   BTREE_PAGE_FIND_IMAGE |
						   BTREE_PAGE_FIND_KEEP_LOKEY |
						   BTREE_PAGE_FIND_READ_CSN);
	clear_fixed_key(&scan->prevHikey);
	clear_fixed_key(&scan->keyRangeHigh);
	clear_fixed_key(&scan->keyRangeLow);
	scan->isSingleLeafPage = false;
	o_btree_load_shmem(desc);
	if (!iterate_internal_page(scan) && !single_leaf_page_rel(scan))
	{
		switch_to_disk_scan(scan);
		if (!load_next_disk_leaf_page(scan))
			scan->status = BTreeSeqScanFinished;
	}

	scan->initialized = true;
}

static BTreeSeqScan *
make_btree_seq_scan_internal(BTreeDescr *desc, OSnapshot *oSnapshot,
							 BTreeSeqScanCallbacks *cb, void *arg,
							 BlockSampler sampler, ParallelOScanDesc poscan)
{
	BTreeSeqScan *scan = (BTreeSeqScan *) MemoryContextAlloc(btree_seqscan_context,
															 sizeof(BTreeSeqScan));

	scan->poscan = poscan;
	scan->desc = desc;
	if (!IS_SYS_TREE_OIDS(desc->oids))
		((OIndexDescr *) desc->arg)->refcnt++;
	scan->oSnapshot = *oSnapshot;
	scan->status = BTreeSeqScanInMemory;
	scan->allocatedDownlinks = 16;
	scan->downlinksCount = 0;
	scan->downlinkIndex = 0;
	scan->diskDownlinks = (BTreeSeqScanDiskDownlink *) MemoryContextAlloc(btree_seqscan_context,
																		  sizeof(scan->diskDownlinks[0]) * scan->allocatedDownlinks);
	scan->mctx = CurrentMemoryContext;
	scan->iter = NULL;
	scan->cb = cb;
	scan->arg = arg;

	/*
	 * A bitmap scan (callbacks with getNextKey) that isn't parallel drives a
	 * key-directed walk of each leaf, so we can read leaves partially.  See
	 * the comment on BTreeSeqScan.fetch.
	 */
	scan->fetch = (cb != NULL && cb->getNextKey != NULL && poscan == NULL);
	scan->leafPartial.isPartial = false;
	scan->leafPartial.src = NULL;
	scan->leafPartialFailed = false;
	scan->haveLastLeafKey = false;
	scan->iterSkipKey = false;
	scan->intPartialFailed = false;
	scan->haveIntResumeKey = false;

	scan->firstPageIsLoaded = false;
	scan->intStartOffset = 0;
	scan->samplingNumber = 0;
	scan->sampler = sampler;
	scan->dsmSeg = NULL;
	scan->initialized = false;
	scan->checkpointNumberSet = false;
	scan->haveHistImg = false;
	BTREE_PAGE_LOCATOR_SET_INVALID(&scan->leafLoc);

	dlist_push_tail(&listOfScans, &scan->listNode);
	scan->resowner = NULL;
#if PG_VERSION_NUM >= 170000
	ResourceOwnerEnlarge(CurrentResourceOwner);
#endif
	ResourceOwnerRememberBTreeSeqScan(CurrentResourceOwner, scan);
	scan->resowner = CurrentResourceOwner;

	return scan;
}

BTreeSeqScan *
make_btree_seq_scan(BTreeDescr *desc, OSnapshot *oSnapshot, void *poscan)
{
	return make_btree_seq_scan_internal(desc, oSnapshot, NULL, NULL, NULL, poscan);
}

BTreeSeqScan *
make_btree_seq_scan_cb(BTreeDescr *desc, OSnapshot *oSnapshot,
					   BTreeSeqScanCallbacks *cb, void *arg)
{
	return make_btree_seq_scan_internal(desc, oSnapshot, cb, arg, NULL, NULL);
}

BTreeSeqScan *
make_btree_sampling_scan(BTreeDescr *desc, BlockSampler sampler)
{
	return make_btree_seq_scan_internal(desc, &o_in_progress_snapshot,
										NULL, NULL, sampler, NULL);
}

static OTuple
btree_seq_scan_get_tuple_from_iterator(BTreeSeqScan *scan,
									   CommitSeqNo *tupleCsn,
									   BTreeLocationHint *hint)
{
	OTuple		result;

	while (true)
	{
		if (!O_TUPLE_IS_NULL(scan->iterEnd))
			result = o_btree_iterator_fetch(scan->iter, tupleCsn,
											&scan->iterEnd, BTreeKeyNonLeafKey,
											false, hint);
		else
			result = o_btree_iterator_fetch(scan->iter, tupleCsn,
											NULL, BTreeKeyNone,
											false, hint);

		/*
		 * When a partial-read fallback resumed the scan at (inclusive) the
		 * last already-emitted key, drop that leading duplicate so count/rows
		 * aren't doubled.
		 */
		if (scan->iterSkipKey && !O_TUPLE_IS_NULL(result) &&
			o_btree_cmp(scan->desc, &result, BTreeKeyLeafTuple,
						&scan->iterSkipKeyVal.tuple, BTreeKeyNonLeafKey) <= 0)
			continue;
		scan->iterSkipKey = false;
		break;
	}

	if (O_TUPLE_IS_NULL(result))
	{
		btree_iterator_free(scan->iter);
		scan->iter = NULL;
		scan->haveHistImg = false;
	}
	return result;
}

static bool
adjust_location_with_next_key(BTreeSeqScan *scan,
							  Page p, BTreePageItemLocator *loc)
{
	BTreeDescr *desc = scan->desc;
	BTreePageHeader *header = (BTreePageHeader *) p;
	int			cmp;
	OTuple		key;

	if (!BTREE_PAGE_LOCATOR_IS_VALID(p, loc))
		return false;

	if (!seq_leaf_partial_ensure(scan, p, loc))
		return false;
	BTREE_PAGE_READ_LEAF_TUPLE(key, p, loc);

	cmp = o_btree_cmp(desc, &key, BTreeKeyLeafTuple,
					  &scan->nextKey.tuple, BTreeKeyNonLeafKey);
	if (cmp == 0)
		return true;
	if (cmp > 0)
		return false;

	while (true)
	{
		if (loc->chunkOffset == (header->chunksCount - 1))
			break;

		key.formatFlags = header->chunkDesc[loc->chunkOffset].hikeyFlags;
		key.data = (Pointer) p + SHORT_GET_LOCATION(header->chunkDesc[loc->chunkOffset].hikeyShortLocation);
		cmp = o_btree_cmp(desc, &key, BTreeKeyNonLeafKey,
						  &scan->nextKey.tuple, BTreeKeyNonLeafKey);
		if (cmp > 0)
			break;
		loc->itemOffset = loc->chunkItemsCount;
		if (!page_locator_next_chunk(p, loc))
		{
			BTREE_PAGE_LOCATOR_SET_INVALID(loc);
			return false;
		}
	}

	while (BTREE_PAGE_LOCATOR_IS_VALID(p, loc))
	{
		/*
		 * The chunk-skip loop above navigates via chunk hikeys (in the loaded
		 * hikeys chunk), and BTREE_PAGE_LOCATOR_NEXT below can step across a
		 * chunk boundary when nextKey falls in the gap between a chunk's last
		 * item and its hikey; materialize the current chunk before reading.
		 */
		if (!seq_leaf_partial_ensure(scan, p, loc))
			return false;
		BTREE_PAGE_READ_LEAF_TUPLE(key, p, loc);
		cmp = o_btree_cmp(desc,
						  &key, BTreeKeyLeafTuple,
						  &scan->nextKey.tuple, BTreeKeyNonLeafKey);
		if (cmp == 0)
			return true;
		if (cmp > 0)
			break;
		BTREE_PAGE_LOCATOR_NEXT(p, loc);
	}

	return false;
}

static void
apply_next_key(BTreeSeqScan *scan)
{
	BTreeDescr *desc = scan->desc;

	Assert(BTREE_PAGE_LOCATOR_IS_VALID(scan->leafImg, &scan->leafLoc) ||
		   (scan->haveHistImg && BTREE_PAGE_LOCATOR_IS_VALID(scan->histImg, &scan->histLoc)));

	while (true)
	{
		OTuple		key;
		bool		leafResult,
					histResult;

		if (BTREE_PAGE_LOCATOR_IS_VALID(scan->leafImg, &scan->leafLoc))
		{
			if (!seq_leaf_partial_ensure(scan, scan->leafImg, &scan->leafLoc))
				return;
			BTREE_PAGE_READ_LEAF_TUPLE(key, scan->leafImg, &scan->leafLoc);
		}
		else
			O_TUPLE_SET_NULL(key);

		if (scan->haveHistImg &&
			BTREE_PAGE_LOCATOR_IS_VALID(scan->histImg, &scan->histLoc))
		{
			if (O_TUPLE_IS_NULL(key))
			{
				BTREE_PAGE_READ_LEAF_TUPLE(key, scan->histImg, &scan->histLoc);
			}
			else
			{
				OTuple		histKey;

				BTREE_PAGE_READ_LEAF_TUPLE(histKey, scan->histImg, &scan->histLoc);
				if (o_btree_cmp(desc,
								&key, BTreeKeyLeafTuple,
								&histKey, BTreeKeyNonLeafKey) > 0)
					key = histKey;
			}
		}

		scan->nextKey.tuple = key;
		if (O_TUPLE_IS_NULL(key) ||
			!scan->cb->getNextKey(&scan->nextKey, BTreeKeyLeafTuple, true,
								  scan->arg))
		{
			BTREE_PAGE_LOCATOR_SET_INVALID(&scan->leafLoc);
			return;
		}

		leafResult = adjust_location_with_next_key(scan,
												   scan->leafImg,
												   &scan->leafLoc);
		if (scan->haveHistImg)
		{
			histResult = adjust_location_with_next_key(scan,
													   scan->histImg,
													   &scan->histLoc);
			if (leafResult || histResult)
				return;
		}
		else if (leafResult)
			return;

		if (!BTREE_PAGE_LOCATOR_IS_VALID(scan->leafImg, &scan->leafLoc) &&
			!(scan->haveHistImg &&
			  BTREE_PAGE_LOCATOR_IS_VALID(scan->histImg, &scan->histLoc)))
			return;
	}
}

static OTuple
btree_seq_scan_getnext_internal(BTreeSeqScan *scan, MemoryContext mctx,
								CommitSeqNo *tupleCsn, BTreeLocationHint *hint)
{
	OTuple		tuple;

	if (scan->iter)
	{
		tuple = btree_seq_scan_get_tuple_from_iterator(scan, tupleCsn, hint);
		if (!O_TUPLE_IS_NULL(tuple))
			return tuple;
	}

	while (true)
	{
		while (scan->haveHistImg)
		{
			OTuple		histTuple;

			while (!BTREE_PAGE_LOCATOR_IS_VALID(scan->histImg, &scan->histLoc))
			{
				if (O_PAGE_IS(scan->histImg, RIGHTMOST))
				{
					scan->haveHistImg = false;
					break;
				}
				if (!O_PAGE_IS(scan->leafImg, RIGHTMOST))
				{
					OTuple		leafHikey,
								histHikey;

					BTREE_PAGE_GET_HIKEY(leafHikey, scan->leafImg);
					BTREE_PAGE_GET_HIKEY(histHikey, scan->histImg);
					if (o_btree_cmp(scan->desc,
									&histHikey, BTreeKeyNonLeafKey,
									&leafHikey, BTreeKeyNonLeafKey) >= 0)
					{
						scan->haveHistImg = false;
						break;
					}
				}
				load_next_historical_page(scan);
			}

			if (!scan->haveHistImg)
				break;

			if (scan->cb && scan->cb->getNextKey)
				apply_next_key(scan);

			if (!BTREE_PAGE_LOCATOR_IS_VALID(scan->histImg, &scan->histLoc))
				continue;

			BTREE_PAGE_READ_LEAF_TUPLE(histTuple, scan->histImg,
									   &scan->histLoc);
			if (!BTREE_PAGE_LOCATOR_IS_VALID(scan->leafImg, &scan->leafLoc))
			{
				OTuple		leafHikey;

				if (!O_PAGE_IS(scan->leafImg, RIGHTMOST))
				{
					BTREE_PAGE_GET_HIKEY(leafHikey, scan->leafImg);
					if (o_btree_cmp(scan->desc,
									&histTuple, BTreeKeyLeafTuple,
									&leafHikey, BTreeKeyNonLeafKey) >= 0)
					{
						scan->haveHistImg = false;
						break;
					}
				}
			}
			else
			{
				BTreeLeafTuphdr *tuphdr;
				OTuple		leafTuple;
				int			cmp;

				BTREE_PAGE_READ_LEAF_ITEM(tuphdr, leafTuple,
										  scan->leafImg, &scan->leafLoc);

				cmp = o_btree_cmp(scan->desc,
								  &histTuple, BTreeKeyLeafTuple,
								  &leafTuple, BTreeKeyLeafTuple);
				if (cmp > 0)
					break;

				if (cmp == 0)
				{
					if (XACT_INFO_OXID_IS_CURRENT(tuphdr->xactInfo))
					{
						BTREE_PAGE_LOCATOR_NEXT(scan->histImg, &scan->histLoc);
						break;
					}
					else
					{
						BTREE_PAGE_LOCATOR_NEXT(scan->leafImg, &scan->leafLoc);
					}
				}
			}

			tuple = o_find_tuple_version(scan->desc,
										 scan->histImg,
										 &scan->histLoc,
										 &scan->oSnapshot,
										 tupleCsn,
										 mctx,
										 NULL,
										 NULL);
			BTREE_PAGE_LOCATOR_NEXT(scan->histImg, &scan->histLoc);
			if (!O_TUPLE_IS_NULL(tuple))
			{
				if (hint)
					*hint = scan->hint;
				return tuple;
			}
		}

		if (scan->cb && scan->cb->getNextKey &&
			BTREE_PAGE_LOCATOR_IS_VALID(scan->leafImg, &scan->leafLoc))
			apply_next_key(scan);

		if (scan->leafPartialFailed)
		{
			OTuple		resumeLow;

			/*
			 * A partial leaf chunk load lost its race with a concurrent page
			 * modification.  Abandon the partial image and re-read the rest
			 * of the current downlink's key range through an iterator; the
			 * bitmap fetch layer rechecks every tuple, so over-reading is
			 * safe as long as we don't re-emit tuples already returned.  If
			 * we already emitted tuples from this leaf, resume from the last
			 * of them (inclusive) and let the iterator drop that leading
			 * duplicate; otherwise re-read the whole range.
			 */
			scan->leafPartialFailed = false;
			if (scan->haveLastLeafKey)
			{
				resumeLow = scan->lastLeafKey.tuple;
				copy_fixed_key(scan->desc, &scan->iterSkipKeyVal,
							   scan->lastLeafKey.tuple);
				scan->iterSkipKey = true;
			}
			else
				resumeLow = scan->keyRangeLow.tuple;
			scan_make_iterator(scan, resumeLow, scan->keyRangeHigh.tuple);
			tuple = btree_seq_scan_get_tuple_from_iterator(scan, tupleCsn, hint);
			if (!O_TUPLE_IS_NULL(tuple))
				return tuple;
			continue;
		}

		if (!BTREE_PAGE_LOCATOR_IS_VALID(scan->leafImg, &scan->leafLoc))
		{
			if (scan->status == BTreeSeqScanInMemory)
			{
				if (iterate_internal_page(scan))
				{
					if (scan->iter)
					{
						tuple = btree_seq_scan_get_tuple_from_iterator(scan,
																	   tupleCsn,
																	   hint);
						if (!O_TUPLE_IS_NULL(tuple))
							return tuple;
					}
				}
				else
				{
					switch_to_disk_scan(scan);
				}
			}
			if (scan->status == BTreeSeqScanDisk)
			{
				if (!load_next_disk_leaf_page(scan))
				{
					scan->status = BTreeSeqScanFinished;
					O_TUPLE_SET_NULL(tuple);
					return tuple;
				}
			}
			continue;
		}

		/*
		 * While reading a leaf partially, remember the key we're about to
		 * process so a later partial-read failure can resume just after it
		 * instead of re-reading (and re-emitting) the whole leaf.
		 * scan->nextKey is the bitmap key positioned by apply_next_key(),
		 * i.e. the key of the tuple at leafLoc, already in the non-leaf key
		 * form comparisons use.
		 */
		if (scan->leafPartial.isPartial && !O_TUPLE_IS_NULL(scan->nextKey.tuple))
		{
			copy_fixed_key(scan->desc, &scan->lastLeafKey, scan->nextKey.tuple);
			scan->haveLastLeafKey = true;
		}

		tuple = o_find_tuple_version(scan->desc,
									 scan->leafImg,
									 &scan->leafLoc,
									 &scan->oSnapshot,
									 tupleCsn,
									 mctx,
									 NULL,
									 NULL);
		BTREE_PAGE_LOCATOR_NEXT(scan->leafImg, &scan->leafLoc);
		if (!O_TUPLE_IS_NULL(tuple))
		{
			if (hint)
				*hint = scan->hint;
			return tuple;
		}
	}

	/* keep compiler quiet */
	O_TUPLE_SET_NULL(tuple);
	return tuple;
}

OTuple
btree_seq_scan_getnext(BTreeSeqScan *scan, MemoryContext mctx,
					   CommitSeqNo *tupleCsn, BTreeLocationHint *hint)
{
	OTuple		tuple;

	Assert(scan);
	if (!scan->initialized)
		init_btree_seq_scan(scan);

	if (scan->status == BTreeSeqScanInMemory ||
		scan->status == BTreeSeqScanDisk)
	{
		tuple = btree_seq_scan_getnext_internal(scan, mctx, tupleCsn, hint);

		if (!O_TUPLE_IS_NULL(tuple))
			return tuple;
	}
	Assert(scan->status == BTreeSeqScanFinished);

	O_TUPLE_SET_NULL(tuple);
	return tuple;
}

static OTuple
btree_seq_scan_get_tuple_from_iterator_raw(BTreeSeqScan *scan,
										   bool *end,
										   BTreeLocationHint *hint)
{
	OTuple		result;

	if (!O_TUPLE_IS_NULL(scan->iterEnd))
		result = btree_iterate_raw(scan->iter, &scan->iterEnd, BTreeKeyNonLeafKey,
								   false, end, hint);
	else
		result = btree_iterate_raw(scan->iter, NULL, BTreeKeyNone,
								   false, end, hint);

	if (*end)
	{
		btree_iterator_free(scan->iter);
		scan->iter = NULL;
		scan->haveHistImg = false;
	}
	return result;
}

static OTuple
btree_seq_scan_getnext_raw_internal(BTreeSeqScan *scan, MemoryContext mctx,
									BTreeLocationHint *hint)
{
	BTreeLeafTuphdr *tupHdr;
	OTuple		tuple;

	if (scan->iter)
	{
		bool		end;

		tuple = btree_seq_scan_get_tuple_from_iterator_raw(scan, &end, hint);
		if (!end)
			return tuple;
	}

	while (!BTREE_PAGE_LOCATOR_IS_VALID(scan->leafImg, &scan->leafLoc))
	{
		if (scan->status == BTreeSeqScanInMemory)
		{
			elog(DEBUG3, "load_next_in_memory_leaf_page START3");
			if (iterate_internal_page(scan))
			{
				if (scan->iter)
				{
					bool		end;

					tuple = btree_seq_scan_get_tuple_from_iterator_raw(scan, &end, hint);
					if (!end)
						return tuple;
				}
			}
			else
			{
				switch_to_disk_scan(scan);
			}
		}
		if (scan->status == BTreeSeqScanDisk)
		{
			if (!load_next_disk_leaf_page(scan))
			{
				scan->status = BTreeSeqScanFinished;
				O_TUPLE_SET_NULL(tuple);
				return tuple;
			}
		}
	}

	BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tuple, scan->leafImg, &scan->leafLoc);
	BTREE_PAGE_LOCATOR_NEXT(scan->leafImg, &scan->leafLoc);

	if (!tupHdr->deleted)
	{
		if (hint)
			*hint = scan->hint;

		return tuple;
	}
	else
	{
		O_TUPLE_SET_NULL(tuple);
		return tuple;
	}
}

OTuple
btree_seq_scan_getnext_raw(BTreeSeqScan *scan, MemoryContext mctx,
						   bool *end, BTreeLocationHint *hint)
{
	OTuple		tuple;

	if (!scan->initialized)
		init_btree_seq_scan(scan);

	if (scan->status == BTreeSeqScanInMemory ||
		scan->status == BTreeSeqScanDisk)
	{
		tuple = btree_seq_scan_getnext_raw_internal(scan, mctx, hint);
		if (scan->status == BTreeSeqScanInMemory ||
			scan->status == BTreeSeqScanDisk)
		{
			*end = false;
			return tuple;
		}
	}
	Assert(scan->status == BTreeSeqScanFinished);

	O_TUPLE_SET_NULL(tuple);
	*end = true;
	return tuple;
}

/*
 * Internal cleanup for a sequential scan: decrements the numSeqScans counter
 * and completes deferred meta page free if this was the last scan.  Called
 * from both the normal free path and the resource owner release callback.
 */
static void
free_btree_seq_scan_internal(BTreeSeqScan *scan, bool fromResowner)
{
	BTreeDescr *desc = scan->desc;

	START_CRIT_SECTION();

	if (scan->resowner)
	{
		ResourceOwnerForgetBTreeSeqScan(scan->resowner, scan);
		scan->resowner = NULL;
	}

	if (scan->checkpointNumberSet && OInMemoryBlknoIsValid(desc->rootInfo.metaPageBlkno))
	{
		BTreeMetaPage *metaPage = BTREE_GET_META(scan->desc);

		(void) pg_atomic_fetch_sub_u32(&metaPage->numSeqScans[scan->checkpointNumber % NUM_SEQ_SCANS_ARRAY_SIZE], 1);

		/* Complete deferred meta page free if this was the last scan. */
		if (metaPage->toBeFreedOnSeqScanRelease && meta_page_get_num_seq_scans(desc->rootInfo.metaPageBlkno) == 0)
			ppool_free_page(desc->ppool, desc->rootInfo.metaPageBlkno, false);

		scan->checkpointNumberSet = false;
	}

	if (scan->dsmSeg)
	{
		/*
		 * Skip dsm_detach when called from ResourceOwner release: the DSM
		 * segment is also registered as a resource and will be detached by
		 * ResourceOwner independently.  Calling dsm_detach here would attempt
		 * ResourceOwnerForget on a DSM that may have already been released.
		 */
		if (!fromResowner)
			dsm_detach(scan->dsmSeg);
		scan->dsmSeg = NULL;
	}

	if (scan->iter)
	{
		btree_iterator_free(scan->iter);
		scan->iter = NULL;
	}

	if (scan->diskDownlinks)
	{
		pfree(scan->diskDownlinks);
		scan->diskDownlinks = NULL;
	}

	if (!IS_SYS_TREE_OIDS(desc->oids))
		((OIndexDescr *) desc->arg)->refcnt--;
	scan->status = BTreeSeqScanFinished;

	if (!fromResowner)
	{
		dlist_delete_from_thoroughly(&listOfScans, &scan->listNode);
		pfree(scan);
	}

	END_CRIT_SECTION();
}

void
free_btree_seq_scan(BTreeSeqScan *scan)
{
	free_btree_seq_scan_internal(scan, false);
}

/*
 * Error cleanup for sequential scans.  No scans survives the error, but they
 * aren't cleaned up individually.  Thus, we have to walk through all the scans
 * and revert changes made to the metaPageBlkno->numSeqScans.
 */
void
seq_scans_cleanup(void)
{
	START_CRIT_SECTION();
	while (!dlist_is_empty(&listOfScans))
	{
		BTreeSeqScan *scan = dlist_head_element(BTreeSeqScan, listNode, &listOfScans);

		free_btree_seq_scan_internal(scan, false);
	}
	END_CRIT_SECTION();
}

/*
 * Return the total number of active sequential scans across all checkpoint
 * number slots for the given meta page.
 */
int
meta_page_get_num_seq_scans(OInMemoryBlkno metaPageBlkno)
{
	BTreeMetaPage *metaPage = (BTreeMetaPage *) O_GET_IN_MEMORY_PAGE(metaPageBlkno);
	int			result = 0;
	int			i;

	for (i = 0; i < NUM_SEQ_SCANS_ARRAY_SIZE; i++)
		result += pg_atomic_read_u32(&metaPage->numSeqScans[i]);

	return result;
}

#if PG_VERSION_NUM >= 170000

static void
ResourceOwnerRememberBTreeSeqScan(ResourceOwner owner, BTreeSeqScan *scan)
{
	ResourceOwnerRemember(owner, PointerGetDatum(scan), &btree_seq_scan_resowner_desc);
}
static void
ResourceOwnerForgetBTreeSeqScan(ResourceOwner owner, BTreeSeqScan *scan)
{
	ResourceOwnerForget(owner, PointerGetDatum(scan), &btree_seq_scan_resowner_desc);
}

static void
ResOwnerReleaseBTreeSeqScan(Datum res)
{
	BTreeSeqScan *scan = (BTreeSeqScan *) DatumGetPointer(res);

	scan->resowner = NULL;
	free_btree_seq_scan_internal(scan, true);
}

static char *
ResOwnerPrintBTreeSeqScan(Datum res)
{
	BTreeSeqScan *scan = (BTreeSeqScan *) DatumGetPointer(res);
	ORelOids	oids = scan->desc->oids;

	return psprintf("OrioleDB BTreeSeqScans (%u, %u, %u)",
					oids.datoid, oids.reloid, oids.relnode);
}

#else

/*
 * PG16 lacks the per-owner ResourceOwnerRemember API, so we fall back to
 * RegisterResourceReleaseCallback.  The callback fires for every
 * ResourceOwner release, so it filters by scan->resowner to only free the
 * scan when its own binding owner is being released.
 */
static void
ResOwnerReleaseBTreeSeqScanCallback(ResourceReleasePhase phase,
									bool isCommit, bool isTopLevel, void *arg)
{
	BTreeSeqScan *scan = (BTreeSeqScan *) arg;

	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;
	if (scan->resowner != CurrentResourceOwner)
		return;

	/*
	 * Unregister this callback before letting free_btree_seq_scan_internal
	 * clear scan->resowner.  The scan itself is not pfreed here (fromResowner
	 * skips dlist_delete/pfree and leaves cleanup to seq_scans_cleanup), and
	 * that later pfree would leave a dangling arg pointer in the global
	 * callback list if we did not drop the entry now.  Self-removal during
	 * the release walk is safe: resowner.c captures the next pointer before
	 * invoking each callback.
	 */
	UnregisterResourceReleaseCallback(ResOwnerReleaseBTreeSeqScanCallback,
									  scan);
	scan->resowner = NULL;
	free_btree_seq_scan_internal(scan, true);
}

static void
ResourceOwnerRememberBTreeSeqScan(ResourceOwner owner, BTreeSeqScan *scan)
{
	RegisterResourceReleaseCallback(ResOwnerReleaseBTreeSeqScanCallback, scan);
}

static void
ResourceOwnerForgetBTreeSeqScan(ResourceOwner owner, BTreeSeqScan *scan)
{
	UnregisterResourceReleaseCallback(ResOwnerReleaseBTreeSeqScanCallback, scan);
}

#endif
