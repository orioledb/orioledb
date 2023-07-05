/*-------------------------------------------------------------------------
 *
 * print.c
 *		Routines for printing orioledb B-tree structure and contents.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/print.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/page_chunks.h"
#include "btree/print.h"
#include "btree/undo.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "tuple/format.h"
#include "utils/page_pool.h"

#include "access/transam.h"
#include "utils/builtins.h"
#include "miscadmin.h"
#include "utils/memutils.h"

typedef struct
{
	BTreePrintOptions *options;
	/* Page number in NLR tree traversal */
	OInMemoryBlkno NLRPageNumber;
	uint32		minCheckpointNum;
	CommitSeqNo minCsn;
	UndoLocation minUndoLoc;
	/* Used for saving backend id number during NLR traversal. */
	BackendId	backendIdInTraversal;
	bool		hasCsn;
	/* hash mapping of the backend id with the number of   */
	HTAB	   *backendIdHash;

	/*
	 * hash mapping of the page number in memory with number in the NLR tree
	 * traversal
	 */
	HTAB	   *pageHash;
	/* sorted list of unique undo locations in ascending order */
	List	   *undosList;
} BTreePrintData;

typedef BackendId BackendIdHashKey;
typedef OInMemoryBlkno PageHashKey;

typedef struct
{
	BackendIdHashKey backendId;
	BackendId	backendIdInTraversal;
} BackendIdHashEntry;

typedef struct
{
	PageHashKey inMemoryPageNumber;
	OInMemoryBlkno NLRPageNumber;
} PageHashEntry;

static void print_page_contents_recursive(BTreeDescr *desc,
										  OInMemoryBlkno blkno,
										  PrintFunc keyPrintFunc,
										  PrintFunc tuplePrintFunc,
										  Pointer printArg,
										  BTreePrintData *printData,
										  int depthLeft, StringInfo outbuf);
static void btree_calculate_min_values(OInMemoryBlkno blkno,
									   BTreePrintData *printData);
static bool btree_print_csn(CommitSeqNo csn, StringInfo outbuf,
							BTreePrintData *printData, bool addComma);
static void btree_print_backend_id(OXid oxid, StringInfo outbuf,
								   BTreePrintData *printData);
static uint64 lundo_location(List *list, UndoLocation location);
static bool btree_print_undo_location(UndoLocation undoLocation,
									  StringInfo outbuf,
									  BTreePrintData *printData,
									  bool addComma);
static bool btree_print_format_flags(int formatFlags, StringInfo outbuf,
									 BTreePrintData *printData, bool addComma);
static void btree_print_page_number(OInMemoryBlkno blkno, StringInfo outbuf,
									BTreePrintData *printData);
static void btree_print_orioledb_downlink(uint64 downlink, StringInfo outbuf,
										  BTreePrintData *printData);
static void btree_print_rightlink(OInMemoryBlkno rightlink, StringInfo outbuf,
								  BTreePrintData *printData);
static void pdata_set_min_csn(BTreePrintData *printData,
							  CommitSeqNo csn);
static List *ladd_unique_undo(List *list,
							  UndoLocation location);


/*
 * Recursively print contents of B-tree pages with given depth.  Uses
 * callbacks for printing keys and tuples.
 */
void
o_print_btree_pages(BTreeDescr *desc, StringInfo outbuf,
					PrintFunc keyPrintFunc, PrintFunc tuplePrintFunc,
					Pointer printArg, BTreePrintOptions *options, int depth)
{
	HASHCTL		ctl;
	BTreePrintData printData = {0};

	if (options->undoLogLocationPrintType != BTreeNotPrint)
		update_min_undo_locations(false, true);
	Assert(OInMemoryBlknoIsValid(desc->rootInfo.rootPageBlkno) &&
		   OInMemoryBlknoIsValid(desc->rootInfo.metaPageBlkno));

	printData.options = options;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(BackendIdHashKey);
	ctl.entrysize = sizeof(BackendIdHashEntry);
	ctl.hcxt = CurrentMemoryContext;
	printData.backendIdHash = hash_create("backend id hash", GetMaxBackends(), &ctl,
										  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ctl.keysize = sizeof(PageHashKey);
	ctl.entrysize = sizeof(PageHashEntry);
	ctl.hcxt = CurrentMemoryContext;
	printData.pageHash = hash_create("page hash", 8, &ctl,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* calculate minimal values only if one of the options set */
	if (printData.options->pagePrintType == BTreePrintRelative ||
		printData.options->csnPrintType == BTreePrintRelative ||
		printData.options->undoLogLocationPrintType == BTreePrintRelative ||
		printData.options->backendIdPrintType == BTreePrintAbsolute)
	{
		printData.minCheckpointNum = UINT32_MAX;
		printData.hasCsn = false;
		printData.minUndoLoc = InvalidUndoLocation;
		printData.backendIdInTraversal = 0;
		printData.NLRPageNumber = 0;
		printData.undosList = NIL;
		btree_calculate_min_values(desc->rootInfo.rootPageBlkno, &printData);
	}
	printData.NLRPageNumber = 0;
	print_page_contents_recursive(desc, desc->rootInfo.rootPageBlkno, keyPrintFunc, tuplePrintFunc,
								  printArg, &printData, depth, outbuf);

	list_free_deep(printData.undosList);
	hash_destroy(printData.pageHash);
	hash_destroy(printData.backendIdHash);
}

/*
 * Print contents of give B-tree page.  If non-leaf page is given, recursively
 * print childredn.
 */
static void
print_page_contents_recursive(BTreeDescr *desc, OInMemoryBlkno blkno,
							  PrintFunc keyPrintFunc,
							  PrintFunc tuplePrintFunc,
							  Pointer printArg,
							  BTreePrintData *printData,
							  int depthLeft, StringInfo outbuf)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	BTreePageItemLocator loc;
	OffsetNumber i,
				j,
				k;

	if (depthLeft <= 0)
		return;

	btree_print_page_number(blkno, outbuf, printData);
	appendStringInfo(outbuf, "level = %d, maxKeyLen = %d",
					 PAGE_GET_LEVEL(p),
					 header->maxKeyLen);
	btree_print_csn(header->csn, outbuf, printData, true);

	if (UndoLocationIsValid(header->undoLocation))
	{
		btree_print_undo_location(header->undoLocation, outbuf, printData, true);
	}

	if (O_PAGE_IS(p, LEAF))
		appendStringInfo(outbuf, ", nVacatedBytes = %u", PAGE_GET_N_VACATED(p));

	appendStringInfo(outbuf, "\n");

	if (printData->options->printStateValue)
		appendStringInfo(outbuf, "state = %u", pg_atomic_read_u32(&(O_PAGE_HEADER(p)->state)));
	else
	{
		uint32		state = pg_atomic_read_u32(&(O_PAGE_HEADER(p)->state));

		if (O_PAGE_STATE_READ_IS_BLOCKED(state))
			appendStringInfo(outbuf, "state = modify");
		else if (O_PAGE_STATE_IS_LOCKED(state))
			appendStringInfo(outbuf, "state = locked");
		else
			appendStringInfo(outbuf, "state = free");
	}

	if (printData->options->changeCountPrintType == BTreePrintAbsolute)
		appendStringInfo(outbuf, ", pageChangeCount = %u", O_PAGE_HEADER(p)->pageChangeCount);

	appendStringInfo(outbuf, ", datoid ");
	if (printData->options->idsPrintType == BTreePrintRelative)
		appendStringInfo(outbuf, "%sequal", page_desc->oids.datoid == desc->oids.datoid ? "" : "not ");
	else
		appendStringInfo(outbuf, "= %u", page_desc->oids.datoid);

	appendStringInfo(outbuf, ", relnode ");
	if (printData->options->idsPrintType == BTreePrintRelative)
		appendStringInfo(outbuf, "%sequal", page_desc->oids.relnode == desc->oids.relnode ? "" : "not ");
	else
		appendStringInfo(outbuf, "= %u", page_desc->oids.relnode);

	switch (desc->type)
	{
		case oIndexInvalid:
			appendStringInfo(outbuf, ", ix_type = invalid");
			break;
		case oIndexToast:
			appendStringInfo(outbuf, ", ix_type = toast");
			break;
		case oIndexPrimary:
			appendStringInfo(outbuf, ", ix_type = primary");
			break;
		case oIndexUnique:
			appendStringInfo(outbuf, ", ix_type = unique");
			break;
		case oIndexRegular:
			appendStringInfo(outbuf, ", ix_type = regular");
			break;
		default:
			appendStringInfo(outbuf, ", ix_type = wrong");
			break;
	}

	if (OCompressIsValid(desc->compress))
		appendStringInfo(outbuf, ", compression = %d", desc->compress);

	if (IS_DIRTY(blkno))
	{
		if (!IS_DIRTY_CONCURRENT(blkno))
			appendStringInfo(outbuf, ", dirty, concurrent IO");
		else
			appendStringInfo(outbuf, ", dirty");
	}
	else
		appendStringInfo(outbuf, ", clean");

	if (printData->options->printFileOffset)
	{
		if (FileExtentIsValid(page_desc->fileExtent))
			appendStringInfo(outbuf, ", fileOffset = %lu", (long unsigned) page_desc->fileExtent.off);
		else
			appendStringInfo(outbuf, ", fileOffset is invalid");
	}

	if (printData->options->checkpointNumPrintType == BTreePrintAbsolute)
		appendStringInfo(outbuf, ", checkpointNum = %u", header->checkpointNum);
	else if (printData->options->checkpointNumPrintType == BTreePrintRelative)
		appendStringInfo(outbuf, ", checkpointNum = %u", header->checkpointNum - printData->minCheckpointNum);

	appendStringInfo(outbuf, "\n");

	appendStringInfo(outbuf, O_PAGE_IS(p, LEFTMOST) ? "    Leftmost, " : "    ");
	if (!O_PAGE_IS(p, RIGHTMOST))
	{
		OTuple		hikey;

		btree_print_rightlink(RIGHTLINK_GET_BLKNO(header->rightLink), outbuf, printData);
		BTREE_PAGE_GET_HIKEY(hikey, p);
		appendStringInfo(outbuf, "    Hikey: offset = %d, key = ",
						 (int) ((Pointer) hikey.data - (Pointer) p));
		keyPrintFunc(desc, outbuf, hikey, printArg);
		appendStringInfo(outbuf, "\n");
	}
	else
	{
		appendStringInfo(outbuf, "Rightmost\n");
	}

	i = 0;
	j = 0;
	for (j = 0; j < header->chunksCount; j++)
	{
		appendStringInfo(outbuf, "  Chunk %i: offset = %u, location = %u, hikey location = %u",
						 j,
						 header->chunkDesc[j].offset,
						 SHORT_GET_LOCATION(header->chunkDesc[j].shortLocation),
						 SHORT_GET_LOCATION(header->chunkDesc[j].hikeyShortLocation));
		if (!O_PAGE_IS(p, RIGHTMOST) || j < header->chunksCount - 1)
		{
			OTuple		hikey;

			hikey.formatFlags = header->chunkDesc[j].hikeyFlags;
			hikey.data = (Pointer) p + SHORT_GET_LOCATION(header->chunkDesc[j].hikeyShortLocation);
			appendStringInfo(outbuf, ", hikey = ");
			keyPrintFunc(desc, outbuf, hikey, printArg);
		}
		appendStringInfo(outbuf, "\n");
		page_chunk_fill_locator(p, j, &loc);
		for (k = 0; k < loc.chunkItemsCount; k++)
		{
			loc.itemOffset = k;
			if (O_PAGE_IS(p, LEAF))
			{
				BTreeLeafTuphdr tuphdr,
						   *pageTuphdr;
				OTuple		tuple;
				bool		inUndo = false;

				BTREE_PAGE_READ_LEAF_ITEM(pageTuphdr, tuple, p, &loc);
				tuphdr = *pageTuphdr;
				appendStringInfo(outbuf, "    Item %i: ", i);

				while (true)
				{
					bool		needsComma = false;

					if (inUndo)
						appendStringInfo(outbuf, "      Undo item: ");

					if (XACT_INFO_IS_FINISHED(tuphdr.xactInfo))
					{
						needsComma = btree_print_csn(XACT_INFO_MAP_CSN(tuphdr.xactInfo), outbuf, printData, false);
					}
					else
					{
						int			lockMode = XACT_INFO_GET_LOCK_MODE(tuphdr.xactInfo);

						if (XACT_INFO_IS_LOCK_ONLY(tuphdr.xactInfo))
						{
							appendStringInfo(outbuf, "lock only, ");
						}
						switch (lockMode)
						{
							case RowLockKeyShare:
								appendStringInfo(outbuf, "mode = keyShare");
								break;
							case RowLockShare:
								appendStringInfo(outbuf, "mode = share");
								break;
							case RowLockNoKeyUpdate:
								appendStringInfo(outbuf, "mode = noKeyUpdate");
								break;
							case RowLockUpdate:
								appendStringInfo(outbuf, "mode = update");
								break;
							default:
								elog(ERROR, "Invalid lock mode: %u", lockMode);
								break;
						}
						needsComma = true;
						btree_print_backend_id(XACT_INFO_GET_OXID(tuphdr.xactInfo), outbuf, printData);
					}

					needsComma |= btree_print_format_flags(tuphdr.formatFlags,
														   outbuf, printData,
														   needsComma);

					if (tuphdr.deleted != BTreeLeafTupleNonDeleted)
					{
						if (needsComma)
							appendStringInfo(outbuf, ", ");
						else
							needsComma = true;
						if (tuphdr.deleted == BTreeLeafTupleDeleted)
							appendStringInfo(outbuf, "deleted");
						else if (tuphdr.deleted == BTreeLeafTupleMovedPartitions)
							appendStringInfo(outbuf, "moved partitions");
						else if (tuphdr.deleted == BTreeLeafTuplePKChanged)
							appendStringInfo(outbuf, "PK changed");
					}
					needsComma |= btree_print_undo_location((UndoLocation) tuphdr.undoLocation, outbuf, printData, needsComma);

					if (!inUndo)
					{
						if (needsComma)
							appendStringInfo(outbuf, ", ");
						else
							needsComma = true;
						appendStringInfo(outbuf, "offset = %u",
										 BTREE_PAGE_GET_ITEM_OFFSET(p, &loc));
					}

					if (tuphdr.chainHasLocks)
					{
						if (needsComma)
							appendStringInfo(outbuf, ", ");
						else
							needsComma = true;
						appendStringInfo(outbuf, "chainHasLocks");
					}

					if (!O_TUPLE_IS_NULL(tuple))
					{
						if (needsComma)
							appendStringInfo(outbuf, ", ");
						else
							needsComma = true;
						appendStringInfo(outbuf, "tuple = ");
						tuplePrintFunc(desc, outbuf, tuple, printArg);
					}
					appendStringInfo(outbuf, "\n");

					if ((!XACT_INFO_IS_FINISHED(tuphdr.xactInfo) || tuphdr.chainHasLocks) &&
						UndoLocationIsValid(tuphdr.undoLocation))
					{
						Assert(UNDO_REC_EXISTS(tuphdr.undoLocation));
						if (inUndo && !O_TUPLE_IS_NULL(tuple))
							pfree(tuple.data);
						if (!tuphdr.deleted && !XACT_INFO_IS_LOCK_ONLY(tuphdr.xactInfo))
						{
							get_prev_leaf_header_and_tuple_from_undo(&tuphdr, &tuple, 0);
						}
						else
						{
							get_prev_leaf_header_from_undo(&tuphdr, false);
							O_TUPLE_SET_NULL(tuple);
						}
						inUndo = true;
					}
					else
					{
						break;
					}
				}
				if (inUndo && !O_TUPLE_IS_NULL(tuple))
					pfree(tuple.data);
			}
			else
			{
				BTreeNonLeafTuphdr *tuphdr;
				OTuple		tuple;

				BTREE_PAGE_READ_INTERNAL_ITEM(tuphdr, tuple, p, &loc);

				appendStringInfo(outbuf, "    Item %i: ", i);
				appendStringInfo(outbuf, "offset = %u",
								 BTREE_PAGE_GET_ITEM_OFFSET(p, &loc));
				if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
					btree_print_orioledb_downlink(tuphdr->downlink, outbuf, printData);
				else if (DOWNLINK_IS_IN_IO(tuphdr->downlink))
					appendStringInfo(outbuf, ", in-progress (%u)",
									 DOWNLINK_GET_IO_LOCKNUM(tuphdr->downlink));
				else
					appendStringInfo(outbuf, ", downlink = on-disk (%lu, %u)",
									 DOWNLINK_GET_DISK_OFF(tuphdr->downlink),
									 DOWNLINK_GET_DISK_LEN(tuphdr->downlink));
				if (i != 0)
				{
					appendStringInfo(outbuf, ", key = ");
					keyPrintFunc(desc, outbuf, tuple, printArg);
				}
				appendStringInfo(outbuf, "\n");
			}
			i++;
		}
	}
	appendStringInfo(outbuf, "\n");

	printData->NLRPageNumber++;

	if (!O_PAGE_IS(p, LEAF))
	{
		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			Pointer		ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
			BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) ptr;

			if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
				print_page_contents_recursive(desc,
											  DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink),
											  keyPrintFunc, tuplePrintFunc, printArg, printData,
											  depthLeft - 1, outbuf);
		}
	}

	blkno = RIGHTLINK_GET_BLKNO(BTREE_PAGE_GET_RIGHTLINK(p));
	if (OInMemoryBlknoIsValid(blkno))
		print_page_contents_recursive(desc, blkno, keyPrintFunc, tuplePrintFunc,
									  printArg, printData, depthLeft, outbuf);
}

/*
 * Calculate values needed for printing page positions in NLR traversal,
 * relative CSNs and Undo locations.
 */
static void
btree_calculate_min_values(OInMemoryBlkno blkno, BTreePrintData *printData)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	PageHashEntry *pageHashEntry;
	BackendIdHashEntry *backendIdHashEntry;
	BTreePageItemLocator loc;
	bool		found;


	/* if page number is not in hash, then add new value to hash */
	pageHashEntry = (PageHashEntry *) hash_search(printData->pageHash,
												  &blkno, HASH_ENTER, &found);
	if (!found)
	{
		pageHashEntry->NLRPageNumber = printData->NLRPageNumber;
		printData->NLRPageNumber++;
	}

	printData->minCheckpointNum = Min(printData->minCheckpointNum,
									  header->checkpointNum);
	printData->undosList = ladd_unique_undo(printData->undosList,
											header->undoLocation);

	/* Iterate over the child nodes */
	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		Pointer		ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

		if (O_PAGE_IS(p, LEAF))
		{
			BTreeLeafTuphdr tuphdr = *((BTreeLeafTuphdr *) ptr);

			while (true)
			{
				printData->undosList = ladd_unique_undo(printData->undosList,
														tuphdr.undoLocation);

				if (XACT_INFO_IS_FINISHED(tuphdr.xactInfo))
				{
					pdata_set_min_csn(printData, XACT_INFO_MAP_CSN(tuphdr.xactInfo));
				}
				else
				{
					OXid		oxid = XACT_INFO_GET_OXID(tuphdr.xactInfo);
					uint32		procnum = oxid_get_procnum(oxid);

					backendIdHashEntry = (BackendIdHashEntry *) hash_search(printData->backendIdHash,
																			&procnum,
																			HASH_ENTER,
																			&found);

					/*
					 * if backend id wasn't in hash that means it first
					 * appearence of backend saving id in traversal to hash
					 * needed
					 */
					if (!found)
					{
						backendIdHashEntry->backendIdInTraversal = printData->backendIdInTraversal;
						printData->backendIdInTraversal++;
					}
				}
				if ((!XACT_INFO_IS_FINISHED(tuphdr.xactInfo) || tuphdr.chainHasLocks) &&
					UndoLocationIsValid(tuphdr.undoLocation))
					get_prev_leaf_header_from_undo(&tuphdr, false);
				else
					break;
			}
		}
		else
		{
			BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) ptr;

			/* If tuple is downlink in memory */
			if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
			{
				/* recursively traverse to every child */
				btree_calculate_min_values(DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink),
										   printData);
			}
		}
	}

	/* If node has valid rightlink also traverse to it */
	blkno = RIGHTLINK_GET_BLKNO(BTREE_PAGE_GET_RIGHTLINK(p));
	if (OInMemoryBlknoIsValid(blkno))
		btree_calculate_min_values(blkno, printData);
}

/*
 * Print in memory downlink for child node
 */
static bool
btree_print_csn(CommitSeqNo csn, StringInfo outbuf, BTreePrintData *printData, bool addComma)
{
	CommitSeqNo printedCsn = csn;

	/* print csn if option has another value then BTreePrintAbsolute */
	if (printData->options->csnPrintType != BTreeNotPrint)
	{
		if (addComma)
			appendStringInfo(outbuf, ", ");
		appendStringInfo(outbuf, "csn = ");
		if (COMMITSEQNO_IS_FROZEN(printedCsn))
			appendStringInfo(outbuf, "FROZEN");
		else if (COMMITSEQNO_IS_INPROGRESS(printedCsn))
			appendStringInfo(outbuf, "INPROGRESS");
		else
		{
			/*
			 * If relative csn option set, then substract min csn from
			 * absolute node csn
			 */
			if (printData->options->csnPrintType == BTreePrintRelative)
				printedCsn = csn - printData->minCsn;
			appendStringInfo(outbuf, UINT64_FORMAT, printedCsn);
		}
		return true;
	}
	return false;
}

/*
 * Print node backend id
 */
static void
btree_print_backend_id(OXid oxid, StringInfo outbuf, BTreePrintData *printData)
{
	BackendId	backendId = oxid_get_procnum(oxid);
	BackendIdHashEntry *hentry;
	bool		found;

	if (printData->options->backendIdPrintType != BTreeNotPrint)
	{
		/* find backend id in traversal by backend id */
		hentry = (BackendIdHashEntry *) hash_search(printData->backendIdHash, &backendId, HASH_FIND, &found);
		Assert(found);
		appendStringInfo(outbuf, ", backend = %d", hentry->backendIdInTraversal);
	}
}

static uint64
lundo_location(List *list, UndoLocation location)
{
	ListCell   *lc;
	uint64		i = 0;

	foreach(lc, list)
	{
		if (*((UndoLocation *) lfirst(lc)) == location)
			break;
		i++;
	}
	return i;
}

static bool
btree_print_undo_location(UndoLocation undoLocation, StringInfo outbuf,
						  BTreePrintData *printData, bool addComma)
{
	UndoLocation printedUndoLoc = undoLocation;
	BTreePrintOption printType = printData->options->undoLogLocationPrintType;

	if (printType != BTreeNotPrint)
	{
		/* print undo location only if it is valid */
		if (UndoLocationIsValid(undoLocation) &&
			(((UNDO_REC_EXISTS(undoLocation) && printType == BTreePrintAbsolute)) ||
			 ((UNDO_REC_XACT_RETAIN(undoLocation) && printType == BTreePrintRelative))))
		{
			/*
			 * if ascending number option set, then it gets number of undo
			 * location in sorted list
			 */
			if (printData->options->undoLogLocationPrintType ==
				BTreePrintRelative)
			{
				printedUndoLoc = lundo_location(printData->undosList, undoLocation);
				Assert(!printedUndoLoc ||
					   printedUndoLoc != list_length(printData->undosList));
			}
			if (addComma)
				appendStringInfo(outbuf, ", ");
			appendStringInfo(outbuf, "undoLocation = " UINT64_FORMAT, printedUndoLoc);
			return true;
		}
	}
	return false;
}

static bool
btree_print_format_flags(int formatFlags, StringInfo outbuf,
						 BTreePrintData *printData, bool addComma)
{
	if (printData->options->printFormatFlags)
	{
		if (addComma)
			appendStringInfo(outbuf, ", ");
		appendStringInfo(outbuf, "format = %sFIXED",
						 formatFlags == O_TUPLE_FLAGS_FIXED_FORMAT ? "" :
						 "NOT ");
		return true;
	}
	return false;
}

/*
 * Print page number for node
 */
static void
btree_print_page_number(OInMemoryBlkno blkno, StringInfo outbuf, BTreePrintData *printData)
{
	PageHashEntry *hentry;
	bool		found;
	OInMemoryBlkno printedPageNumber = blkno;

	/* print page number in NLR traverse only if corresponding option set */
	if (printData->options->pagePrintType == BTreePrintRelative)
	{
		/* find the corresponding page number in NLR traversal */
		hentry = (PageHashEntry *) hash_search(printData->pageHash, &blkno, HASH_FIND, &found);
		Assert(found);
		printedPageNumber = hentry->NLRPageNumber;
	}
	appendStringInfo(outbuf, "Page %u: ", printedPageNumber);
}

/*
 * Print in memory downlink for child node
 */
static void
btree_print_orioledb_downlink(uint64 downlink, StringInfo outbuf, BTreePrintData *printData)
{
	PageHashEntry *hentry;
	bool		found;
	OInMemoryBlkno printedPageNumber = DOWNLINK_GET_IN_MEMORY_BLKNO(downlink);

	/* print page number in NLR traverse only if corresponding option set */
	if (printData->options->pagePrintType == BTreePrintRelative)
	{
		/* find the corresponding page number in NLR traversal */
		hentry = (PageHashEntry *) hash_search(printData->pageHash, &printedPageNumber, HASH_FIND, &found);
		Assert(found);
		printedPageNumber = hentry->NLRPageNumber;
	}
	appendStringInfo(outbuf, ", downlink = %u", printedPageNumber);
	if (printData->options->changeCountPrintType == BTreePrintAbsolute)
		appendStringInfo(outbuf, " (%u)", DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(downlink));
}

/*
 * Print rightlink for node
 */
static void
btree_print_rightlink(OInMemoryBlkno rightlink, StringInfo outbuf, BTreePrintData *printData)
{
	PageHashEntry *hentry;
	bool		found;
	OInMemoryBlkno printedPageNumber = rightlink;

	/*
	 * print rightlink page number in NLR traverse only if corresponding
	 * option set
	 */
	if (OInMemoryBlknoIsValid(rightlink))
	{
		if (printData->options->pagePrintType == BTreePrintRelative)
		{
			/* find the corresponding page number in NLR traversal */
			hentry = (PageHashEntry *) hash_search(printData->pageHash, &printedPageNumber, HASH_FIND, &found);
			Assert(found);
			printedPageNumber = hentry->NLRPageNumber;
		}
		appendStringInfo(outbuf, "Rightlink = %u\n", printedPageNumber);
	}
	else
		appendStringInfo(outbuf, "Rightlink is invalid\n");
}

static void
pdata_set_min_csn(BTreePrintData *printData, CommitSeqNo csn)
{
	if (!COMMITSEQNO_IS_NORMAL(csn))
		return;

	if (!printData->hasCsn || printData->minCsn > csn)
	{
		printData->hasCsn = true;
		printData->minCsn = csn;
	}
}

/* adds unique undo location in ascending order */
static List *
ladd_unique_undo(List *list, UndoLocation location)
{
	ListCell   *lc;
	UndoLocation lLoc,
			   *copyLoc;
	int			insertAt = -1,
				i;

	if (!UndoLocationIsValid(location) || !UNDO_REC_XACT_RETAIN(location))
		return list;

	copyLoc = palloc(sizeof(UndoLocation));
	*copyLoc = location;

	/* lappend_cell does not work with NIL list */
	if (list == NIL)
		return lappend(list, copyLoc);

	lLoc = *((UndoLocation *) linitial(list));
	if (lLoc > location)
		return lcons(copyLoc, list);

	i = 0;
	foreach(lc, list)
	{
		lLoc = *((UndoLocation *) lfirst(lc));
		if (lLoc == location)
		{
			pfree(copyLoc);
			return list;
		}
		if (lLoc > location)
			break;
		insertAt = i;
		i++;
	}
	Assert(insertAt >= 0);
	list = list_insert_nth(list, insertAt, copyLoc);
	return list;
}
