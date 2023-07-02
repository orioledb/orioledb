/*-------------------------------------------------------------------------
 *
 * page_contents.c
 *		Low-level routines for working with b-tree page contents.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/page_contents.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/ucm.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/proc.h"
#include "storage/proclist.h"
#include "storage/s_lock.h"
#include "utils/memdebug.h"

/*
 * Navigates and reads the page image from undo log according to `key` of
 * `keyType` and `csn`.  Saves lokey of the page to lokey if *lokey != NULL.
 */
UndoLocation
read_page_from_undo(BTreeDescr *desc, Page img, UndoLocation undo_loc,
					CommitSeqNo csn, void *key, BTreeKeyType keyType,
					OFixedKey *lokey)
{
	BTreePageHeader *header;
	CommitSeqNo page_csn;
	UndoLocation rec_undo_location;
	bool		is_left = true;

	Assert(UndoLocationIsValid(undo_loc));

	while (true)
	{
		/* Read page image from page-level undo item */
		get_page_from_undo(desc, undo_loc, key, keyType, img,
						   &is_left, NULL, lokey, NULL, NULL);

		header = (BTreePageHeader *) img;
		page_csn = header->csn;
		rec_undo_location = header->undoLocation;

		/* Page-level undo item should be retained */
		Assert(UNDO_REC_EXISTS(undo_loc));

		/* Continue traversing undo chain if needed */
		if (COMMITSEQNO_IS_NORMAL(page_csn) && page_csn >= csn)
		{
			undo_loc = rec_undo_location;
			continue;
		}
		else
		{
			break;
		}
	}

	/* Page-level undo item should be retained */
	Assert(UNDO_REC_EXISTS(undo_loc));

	return O_UNDO_GET_IMAGE_LOCATION(undo_loc, is_left);
}

/*
 * Try to copy consistent image of page with page number = blkno to dest.
 */
static inline ReadPageResult
try_copy_page(OInMemoryBlkno blkno, uint32 pageChangeCount, Page dest,
			  PartialPageState *partial, CommitSeqNo *readCsn)
{
	UsageCountMap *ucm;
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	uint32		state1,
				state2;
	bool		hiKeysEndOK PG_USED_FOR_ASSERTS_ONLY = true;

	state1 = pg_atomic_read_u32(&(O_PAGE_HEADER(p)->state));
	if (O_PAGE_STATE_READ_IS_BLOCKED(state1))
		return ReadPageResultFailed;

	pg_read_barrier();

	if (partial)
	{
		BTreePageHeader *header = (BTreePageHeader *) p;
		LocationIndex hikeysEnd = header->hikeysEnd;

		pg_read_barrier();

		if (hikeysEnd >= sizeof(BTreePageHeader) && hikeysEnd < ORIOLEDB_BLCKSZ)
			memcpy(dest, p, hikeysEnd);
		else
			hiKeysEndOK = false;

		partial->isPartial = true;
		partial->src = p;
		memset(&partial->chunkIsLoaded, 0, sizeof(bool) * BTREE_PAGE_MAX_CHUNKS);
	}
	else
		memcpy(dest, p, ORIOLEDB_BLCKSZ);

	if (readCsn)
		*readCsn = pg_atomic_read_u64(&ShmemVariableCache->nextCommitSeqNo);

	pg_read_barrier();
	state2 = pg_atomic_read_u32(&(O_PAGE_HEADER(p)->state));

	if ((state1 & PAGE_STATE_CHANGE_COUNT_MASK) != (state2 & PAGE_STATE_CHANGE_COUNT_MASK) ||
		O_PAGE_STATE_READ_IS_BLOCKED(state2))
		return ReadPageResultFailed;

	if (pageChangeCount != InvalidOPageChangeCount && O_PAGE_GET_CHANGE_COUNT(p) != pageChangeCount)
		return ReadPageResultWrongPageChangeCount;

	Assert(hiKeysEndOK);

	ucm = &(get_ppool_by_blkno(blkno)->ucm);
	page_inc_usage_count(ucm, blkno,
						 pg_atomic_read_u32(&((OrioleDBPageHeader *) dest)->usageCount),
						 false);

	return ReadPageResultOK;
}

/*
 * Copy consistent image of page with page number = blkno to dest.
 */
static inline void
copy_page(OInMemoryBlkno blkno, Page dest, PartialPageState *partial,
		  CommitSeqNo *readCsn)
{
	while (try_copy_page(blkno, InvalidOPageChangeCount, dest,
						 partial, readCsn) != ReadPageResultOK)
	{
		(void) page_wait_for_read_enable(blkno);
	}
}

/*
 * Read in-memory page number `blkno` into `img`.  Check expected
 * `pageChangeCount` until it is InvalidOPageChangeCount.  Lookup for undo
 * page according to `csn` when `key` of `keyType`.
 */
bool
o_btree_read_page(BTreeDescr *desc, OInMemoryBlkno blkno,
				  uint32 pageChangeCount, Page img,
				  CommitSeqNo csn, void *key, BTreeKeyType keyType,
				  OFixedKey *lokey, PartialPageState *partial,
				  UndoLocation *undoLocation, CommitSeqNo *readCsn)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	CommitSeqNo headerCsn;
	UndoLocation headerUndoLocation;
	bool		read_undo = O_PAGE_IS(p, LEAF);

	EA_READ_INC(blkno);

	/*---
	 * Check if we need to load page image from undo?
	 *
	 * We do this check without holding a page lock or even usage of state
	 * protocol.  Istead we ensure correctenss of this check in a following
	 * way.
	 *
	 * 1. We read csn before undo location (ensured with memory barriers).
	 *    We write csn after undo location (also ensured with memory barriers).
	 *    Thus, undo location we read is probably more recent than csn.  That could
	 *    lead to traverse of extra step of undo chain, which is not a problem.
	 *    Also that could lead to miss the need of reading undo, but that would
	 *    be catched by subsequent check.
	 * 2. We check page change count after reading csn and undo location.  That
	 *    ensures page wasn't reused for something while reading csn and undo
	 *    location.  Note, that there is at least one memory barrier between
	 *    increasing page change count and reusing the page during page unlock.
	 */
	headerCsn = header->csn;
	if (read_undo && COMMITSEQNO_IS_NORMAL(csn) && headerCsn >= csn)
	{
		UndoLocation pageUndoLoc;

		pg_read_barrier();
		headerUndoLocation = header->undoLocation;
		if (pageChangeCount != InvalidOPageChangeCount)
		{
			pg_read_barrier();
			if (header->o_header.pageChangeCount != pageChangeCount)
				return false;
		}
		else
		{
			pageChangeCount = header->o_header.pageChangeCount;
		}

		pageUndoLoc = read_page_from_undo(desc, img, headerUndoLocation, csn,
										  key, keyType, lokey);
		header = (BTreePageHeader *) img;
		header->o_header.pageChangeCount = pageChangeCount;
		if (partial)
			partial->isPartial = false;
		if (undoLocation)
			*undoLocation = pageUndoLoc;
		if (readCsn)
			*readCsn = header->csn;
		return true;
	}

	copy_page(blkno, img, partial, readCsn);

	/* If that is the required page according to page change count */
	header = (BTreePageHeader *) img;
	if (pageChangeCount != InvalidOPageChangeCount)
	{
		if (header->o_header.pageChangeCount != pageChangeCount)
			return false;
	}
	else
	{
		pageChangeCount = header->o_header.pageChangeCount;
	}

	/* Re-try reading page-level undo item due to concurrent changes */
	if (read_undo && COMMITSEQNO_IS_NORMAL(csn) && header->csn >= csn)
	{
		UndoLocation pageUndoLoc;

		pageUndoLoc = read_page_from_undo(desc, img, header->undoLocation, csn,
										  key, keyType, lokey);
		header = (BTreePageHeader *) img;
		header->o_header.pageChangeCount = pageChangeCount;
		if (partial)
			partial->isPartial = false;
		if (undoLocation)
			*undoLocation = pageUndoLoc;
		if (readCsn)
			*readCsn = header->csn;
		return true;
	}

	if (undoLocation)
		*undoLocation = InvalidUndoLocation;

	return true;
}

/*
 * Try to read page with concurrent changes.  Returns true on success.
 */
ReadPageResult
o_btree_try_read_page(BTreeDescr *desc, OInMemoryBlkno blkno, uint32 pageChangeCount, Page img,
					  CommitSeqNo csn, Pointer key, BTreeKeyType keyType,
					  PartialPageState *partial, CommitSeqNo *readCsn)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	uint32		tmpPageChangeCount;
	bool		read_undo = O_PAGE_IS(p, LEAF);
	ReadPageResult result;

	EA_READ_INC(blkno);

	/* Check pointer to page-level undo item */
	if (read_undo && COMMITSEQNO_IS_NORMAL(csn) && header->csn >= csn)
	{
		tmpPageChangeCount = header->o_header.pageChangeCount;
		pg_read_barrier();
		if (pageChangeCount != InvalidOPageChangeCount && tmpPageChangeCount != pageChangeCount)
			return ReadPageResultWrongPageChangeCount;

		read_page_from_undo(desc, img, header->undoLocation, csn,
							key, keyType, NULL);
		header = (BTreePageHeader *) img;
		header->o_header.pageChangeCount = tmpPageChangeCount;
		if (readCsn)
			*readCsn = header->csn;
		return ReadPageResultOK;
	}

	result = try_copy_page(blkno, pageChangeCount, img, partial, readCsn);
	if (result != ReadPageResultOK)
		return result;

	/* Re-try reading page-level undo item due to concurrent changes */
	header = (BTreePageHeader *) img;
	if (read_undo && COMMITSEQNO_IS_NORMAL(csn) && header->csn >= csn)
	{
		tmpPageChangeCount = header->o_header.pageChangeCount;
		pg_read_barrier();

		Assert(pageChangeCount == InvalidOPageChangeCount || tmpPageChangeCount == pageChangeCount);

		read_page_from_undo(desc, img, header->undoLocation, csn,
							key, keyType, NULL);
		header = (BTreePageHeader *) img;
		header->o_header.pageChangeCount = tmpPageChangeCount;
		if (readCsn)
			*readCsn = header->csn;
	}

	return ReadPageResultOK;
}

void
init_new_btree_page(BTreeDescr *desc, OInMemoryBlkno blkno, uint16 flags,
					uint16 level, bool noLock)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;

	if (!noLock)
	{
		lock_page(blkno);
		page_block_reads(blkno);
	}

	page_desc->oids = desc->oids;
	page_desc->type = desc->type;
	page_desc->fileExtent.len = InvalidFileExtentLen;
	page_desc->fileExtent.off = InvalidFileExtentOff;
	header->flags = flags;
	if (flags & O_BTREE_FLAG_LEAF)
	{
		header->field1 = 0;
		PAGE_SET_N_VACATED(p, 0);
	}
	else
	{
		PAGE_SET_LEVEL(p, level);
		PAGE_SET_N_ONDISK(p, 0);
	}
	header->rightLink = InvalidRightLink;
	header->csn = COMMITSEQNO_FROZEN;
	header->undoLocation = InvalidUndoLocation;
	header->checkpointNum = 0;
	header->itemsCount = 0;
	header->prevInsertOffset = MaxOffsetNumber;
	header->maxKeyLen = 0;
	page_change_usage_count(&desc->ppool->ucm, blkno,
							(pg_atomic_read_u32(desc->ppool->ucm.epoch) + 2) % UCM_USAGE_LEVELS);

	memset(p + offsetof(BTreePageHeader, chunkDesc),
		   0,
		   ORIOLEDB_BLCKSZ - offsetof(BTreePageHeader, chunkDesc));
}

void
init_meta_page(OInMemoryBlkno blkno, uint32 leafPagesNum)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	BTreeMetaPage *metaPageBlkno = (BTreeMetaPage *) p;
	int			i,
				j;

	memset(p + O_PAGE_HEADER_SIZE, 0, ORIOLEDB_BLCKSZ - O_PAGE_HEADER_SIZE);
	pg_atomic_init_u32(&metaPageBlkno->leafPagesNum, leafPagesNum);
	pg_atomic_init_u64(&metaPageBlkno->numFreeBlocks, 0);
	pg_atomic_init_u64(&metaPageBlkno->datafileLength[0], 0);
	pg_atomic_init_u64(&metaPageBlkno->datafileLength[1], 0);
	pg_atomic_init_u64(&metaPageBlkno->ctid, 0);
	for (i = 0; i < NUM_SEQ_SCANS_ARRAY_SIZE; i++)
		pg_atomic_init_u32(&metaPageBlkno->numSeqScans[i], 0);

	LWLockInitialize(&metaPageBlkno->copyBlknoLock,
					 checkpoint_state->copyBlknoTrancheId);
	LWLockInitialize(&metaPageBlkno->metaLock,
					 checkpoint_state->oMetaTrancheId);

	page_desc->type = oIndexInvalid;
	page_desc->oids.datoid = InvalidOid;
	page_desc->oids.reloid = InvalidOid;
	page_desc->oids.relnode = InvalidOid;
	page_desc->fileExtent.len = InvalidFileExtentLen;
	page_desc->fileExtent.off = InvalidFileExtentOff;

	for (i = 0; i < 2; i++)
	{
		metaPageBlkno->freeBuf.pages[i] = OInvalidInMemoryBlkno;
		for (j = 0; j < 2; j++)
		{
			metaPageBlkno->nextChkp[j].pages[i] = OInvalidInMemoryBlkno;
			metaPageBlkno->tmpBuf[j].pages[i] = OInvalidInMemoryBlkno;
		}

		metaPageBlkno->partsInfo[i].writeMaxLocation = 0;
		for (j = 0; j < MAX_NUM_DIRTY_PARTS; j++)
		{
			metaPageBlkno->partsInfo[i].dirtyParts[j].segNum = -1;
			metaPageBlkno->partsInfo[i].dirtyParts[j].partNum = -1;
		}
	}
}

/*
 * Estimate vacated space in the page.
 */
LocationIndex
page_get_vacated_space(BTreeDescr *desc, Page p, CommitSeqNo csn)
{
	LocationIndex vacated_bytes = 0;
	BTreePageItemLocator loc;

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		BTreeLeafTuphdr *header;
		OTuple		tuple;

		BTREE_PAGE_READ_LEAF_ITEM(header, tuple, p, &loc);
		if (XACT_INFO_FINISHED_FOR_EVERYBODY(header->xactInfo))
		{
			if (header->deleted)
			{
				if (COMMITSEQNO_IS_INPROGRESS(csn) || XACT_INFO_MAP_CSN(header->xactInfo) < csn)
					vacated_bytes += BTREE_PAGE_GET_ITEM_SIZE(p, &loc);
			}
			else
			{
				vacated_bytes += BTREE_PAGE_GET_ITEM_SIZE(p, &loc) -
					(BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tuple, OTupleLength)));
			}
		}
	}

	return vacated_bytes;
}

/*
 * Sets to 0 unused space on the page.
 */
void
null_unused_bytes(Page img)
{
	BTreePageHeader *header = (BTreePageHeader *) img;

	memset((Pointer) img + header->dataSize, 0,
		   ORIOLEDB_BLCKSZ - header->dataSize);
}

void
page_cut_first_key(Page node)
{
	BTreeNonLeafTuphdr *tuphdr,
				tmp;
	BTreePageItemLocator loc;

	Assert(!O_PAGE_IS(node, LEAF));
	BTREE_PAGE_LOCATOR_FIRST(node, &loc);
	Assert(BTREE_PAGE_GET_ITEM_SIZE(node, &loc) > BTreeNonLeafTuphdrSize);

	tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(node, &loc);
	tmp = *tuphdr;

	page_locator_resize_item(node, &loc, BTreeNonLeafTuphdrSize);

	tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(node, &loc);
	*tuphdr = tmp;
}

void
put_page_image(OInMemoryBlkno blkno, Page img)
{
	Page		page = O_GET_IN_MEMORY_PAGE(blkno);

	pg_write_barrier();

	memcpy(page + O_PAGE_HEADER_SIZE,
		   (char *) img + O_PAGE_HEADER_SIZE,
		   ORIOLEDB_BLCKSZ - O_PAGE_HEADER_SIZE);
}

/*
 * Calculates number of vacated bytes for leaf pages and number of
 * disk downlinks for non-leaf pages.
 */
void
o_btree_page_calculate_statistics(BTreeDescr *desc, Pointer p)
{
	BTreePageItemLocator loc;

	if (O_PAGE_IS(p, LEAF))
	{
		int			nVacated = 0;

		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			BTreeLeafTuphdr *tupHdr;
			OTuple		tuple;

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tuple, p, &loc);

			if (tupHdr->deleted)
				nVacated += BTREE_PAGE_GET_ITEM_SIZE(p, &loc);
			else
				nVacated += BTREE_PAGE_GET_ITEM_SIZE(p, &loc) -
					(BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tuple, OTupleLength)));
		}
		PAGE_SET_N_VACATED(p, nVacated);
	}
	else
	{
		int			nOnDisk = 0;

		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			BTreeNonLeafTuphdr *tupHdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			if (DOWNLINK_IS_ON_DISK(tupHdr->downlink))
				nOnDisk++;
		}
		PAGE_SET_N_ONDISK(p, nOnDisk);
	}
}

void
copy_fixed_tuple(BTreeDescr *desc, OFixedTuple *dst, OTuple src)
{
	int			tuplen;

	if (O_TUPLE_IS_NULL(src))
	{
		clear_fixed_tuple(dst);
		return;
	}

	tuplen = o_btree_len(desc, src, OTupleLength);
	Assert(tuplen <= sizeof(dst->fixedData));
	dst->tuple.formatFlags = src.formatFlags;
	dst->tuple.data = dst->fixedData;
	memcpy(dst->fixedData, src.data, tuplen);
	if (tuplen != MAXALIGN(tuplen))
		memset(&dst->fixedData[tuplen], 0, MAXALIGN(tuplen) - tuplen);
}

static void
copy_fixed_key_with_len(OFixedKey *dst, OTuple src, int tuplen)
{
	if (O_TUPLE_IS_NULL(src))
	{
		clear_fixed_key(dst);
		return;
	}

	dst->tuple.formatFlags = src.formatFlags;
	dst->tuple.data = dst->fixedData;
	memcpy(dst->fixedData, src.data, tuplen);
	if (tuplen != MAXALIGN(tuplen))
		memset(&dst->fixedData[tuplen], 0, MAXALIGN(tuplen) - tuplen);
}

void
copy_fixed_key(BTreeDescr *desc, OFixedKey *dst, OTuple src)
{
	int			tuplen;

	if (O_TUPLE_IS_NULL(src))
	{
		clear_fixed_key(dst);
		return;
	}

	tuplen = o_btree_len(desc, src, OKeyLength);
	Assert(tuplen <= sizeof(dst->fixedData));
	copy_fixed_key_with_len(dst, src, tuplen);
}

void
copy_fixed_page_key(BTreeDescr *desc, OFixedKey *dst,
					Page p, BTreePageItemLocator *loc)
{
	OTuple		src;

	BTREE_PAGE_READ_TUPLE(src, p, loc);
	copy_fixed_key(desc, dst, src);
}

void
copy_fixed_hikey(BTreeDescr *desc, OFixedKey *dst, Page p)
{
	OTuple		src;

	BTREE_PAGE_GET_HIKEY(src, p);
	copy_fixed_key(desc, dst, src);
}

void
clear_fixed_tuple(OFixedTuple *dst)
{
	dst->tuple.formatFlags = 0;
	dst->tuple.data = NULL;
}

void
clear_fixed_key(OFixedKey *dst)
{
	dst->tuple.formatFlags = 0;
	dst->tuple.data = NULL;
}

void
copy_from_fixed_shmem_key(OFixedKey *dst, OFixedShmemKey *src)
{
	if (!src->notNull)
	{
		clear_fixed_key(dst);
		return;
	}

	memcpy(dst->fixedData, src->data.fixedData, src->len);
	dst->tuple.data = dst->fixedData;
	dst->tuple.formatFlags = src->formatFlags;
}

void
copy_fixed_shmem_key(BTreeDescr *desc, OFixedShmemKey *dst, OTuple src)
{
	if (O_TUPLE_IS_NULL(src))
	{
		clear_fixed_shmem_key(dst);
		return;
	}

	dst->len = o_btree_len(desc, src, OKeyLength);
	Assert(dst->len <= sizeof(dst->data.fixedData));
	memcpy(dst->data.fixedData, src.data, dst->len);
	dst->notNull = true;
	dst->formatFlags = src.formatFlags;
}

void
copy_fixed_shmem_page_key(BTreeDescr *desc, OFixedShmemKey *dst,
						  Page p, BTreePageItemLocator *loc)
{
	OTuple		src;

	BTREE_PAGE_READ_TUPLE(src, p, loc);
	copy_fixed_shmem_key(desc, dst, src);
}

void
copy_fixed_shmem_hikey(BTreeDescr *desc, OFixedShmemKey *dst, Page p)
{
	OTuple		src;

	BTREE_PAGE_GET_HIKEY(src, p);
	copy_fixed_shmem_key(desc, dst, src);
}

void
clear_fixed_shmem_key(OFixedShmemKey *dst)
{
	dst->notNull = false;
	dst->formatFlags = 0;
	dst->len = 0;
}

OTuple
fixed_shmem_key_get_tuple(OFixedShmemKey *src)
{
	OTuple		result;

	if (src->notNull)
	{
		result.data = src->data.fixedData;
		result.formatFlags = src->formatFlags;
	}
	else
	{
		result.data = NULL;
		result.formatFlags = 0;
	}
	return result;
}

OTuple
page_get_hikey(Page p)
{
	BTreePageChunkDesc *chunkDesc;
	BTreePageHeader *header = (BTreePageHeader *) p;
	OTuple		result;

	Assert(!O_PAGE_IS(p, RIGHTMOST));

	chunkDesc = &header->chunkDesc[header->chunksCount - 1];

	result.formatFlags = chunkDesc->hikeyFlags;
	result.data = (Pointer) p + SHORT_GET_LOCATION(chunkDesc->hikeyShortLocation);

	return result;
}

int
page_get_hikey_size(Page p)
{
	BTreePageChunkDesc *chunkDesc;
	BTreePageHeader *header = (BTreePageHeader *) p;

	Assert(!O_PAGE_IS(p, RIGHTMOST));
	chunkDesc = &header->chunkDesc[header->chunksCount - 1];

	return (header->hikeysEnd - SHORT_GET_LOCATION(chunkDesc->hikeyShortLocation));
}

void
page_set_hikey_flags(Page p, uint8 flags)
{
	BTreePageChunkDesc *chunkDesc;
	BTreePageHeader *header = (BTreePageHeader *) p;

	Assert(!O_PAGE_IS(p, RIGHTMOST));
	chunkDesc = &header->chunkDesc[header->chunksCount - 1];
	chunkDesc->hikeyFlags = flags;
}

bool
page_fits_hikey(Page p, LocationIndex newHikeySize)
{
	BTreePageHeader *header = (BTreePageHeader *) p;
	LocationIndex dataShift,
				hikeyLocation,
				dataLocation;

	Assert(newHikeySize = MAXALIGN(newHikeySize));
	Assert(header->chunksCount == 1);

	hikeyLocation = SHORT_GET_LOCATION(header->chunkDesc[0].hikeyShortLocation);
	dataLocation = SHORT_GET_LOCATION(header->chunkDesc[0].shortLocation);
	if (hikeyLocation + newHikeySize <= dataLocation)
		return true;

	dataShift = hikeyLocation + newHikeySize - dataLocation;
	return (header->dataSize + dataShift <= ORIOLEDB_BLCKSZ);
}

void
page_resize_hikey(Page p, LocationIndex newHikeySize)
{
	BTreePageHeader *header = (BTreePageHeader *) p;
	LocationIndex dataShift,
				hikeyLocation,
				dataLocation;

	Assert(newHikeySize = MAXALIGN(newHikeySize));
	Assert(header->chunksCount == 1);

	hikeyLocation = SHORT_GET_LOCATION(header->chunkDesc[0].hikeyShortLocation);
	dataLocation = SHORT_GET_LOCATION(header->chunkDesc[0].shortLocation);
	if (hikeyLocation + newHikeySize <= dataLocation)
	{
		/* Fits */
		header->hikeysEnd = hikeyLocation + newHikeySize;
		return;
	}

	dataShift = hikeyLocation + newHikeySize - dataLocation;
	Assert(header->dataSize + dataShift <= ORIOLEDB_BLCKSZ);
	memmove((Pointer) p + dataLocation + dataShift,
			(Pointer) p + dataLocation,
			header->dataSize - dataLocation);
	header->chunkDesc[0].shortLocation += LOCATION_GET_SHORT(dataShift);
	header->hikeysEnd = hikeyLocation + newHikeySize;
	header->dataSize += dataShift;
}

void
btree_page_update_max_key_len(BTreeDescr *desc, Page p)
{
	LocationIndex maxKeyLen;
	BTreePageHeader *header = (BTreePageHeader *) p;
	BTreePageItemLocator loc;

	if (!O_PAGE_IS(p, RIGHTMOST))
		maxKeyLen = BTREE_PAGE_GET_HIKEY_SIZE(p);
	else
		maxKeyLen = 0;


	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		LocationIndex keyLen;

		if (!O_PAGE_IS(p, LEAF))
		{
			keyLen = BTREE_PAGE_GET_ITEM_SIZE(p, &loc) -
				BTreeNonLeafTuphdrSize;
		}
		else
		{
			OTuple		tuple;

			BTREE_PAGE_READ_TUPLE(tuple, p, &loc);
			keyLen = o_btree_len(desc, tuple, OTupleKeyLengthNoVersion);
		}
		maxKeyLen = Max(maxKeyLen, keyLen);
	}
	header->maxKeyLen = maxKeyLen;
}
