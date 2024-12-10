/*-------------------------------------------------------------------------
 *
 * tuple_chunk.c
 *		OrioleDB implementation of chunk API over tuple chunks.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/tuple_chunk.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "btree/btree.h"

#include "btree/chunk_ops.h"
#include "btree/find.h"
#include "btree/tuple_chunk.h"

static void allocate_chunk_item(BTreeChunkDesc * desc, OffsetNumber itemOffset,
								uint16 itemSize);
static void resize_chunk_item(BTreeChunkDesc * desc, OffsetNumber itemOffset,
							  uint16 newItemSize);
static void delete_chunk_item(BTreeChunkDesc * desc, OffsetNumber itemOffset);

static bool load_chunk_partially(BTreeChunkDesc * desc, PartialPageState *partial,
								 Page page);

static uint16 get_tuple_chunk_header_size(BTreeChunkType chunkType);
static uint16 get_tuple_chunk_tuple_size(BTreeDescr *treeDesc,
										 BTreeChunkType chunkType, OTuple tuple);

static void add_builder_item(BTreeChunkBuilderData * builderData,
							 Pointer tupleHeader, uint16 tupleHeaderSize,
							 OTuple tuple, uint16 tupleSize);

int32
tuple_chunk_estimate_change(BTreeChunkDesc * desc, OffsetNumber itemOffset,
							BTreeChunkOperationType operation,
							Pointer tupleHeader, OTuple tuple)
{
	int32		itemsShift = 0,
				sizeNeeded = 0;

	if (operation == BTreeChunkOperationInsert)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * (desc->chunkItemsCount + 1)) -
			MAXALIGN(sizeof(LocationIndex) * desc->chunkItemsCount);

		sizeNeeded = get_tuple_chunk_header_size(desc->chunkType) +
			MAXALIGN(get_tuple_chunk_tuple_size(desc->treeDesc,
												desc->chunkType, tuple)) +
			itemsShift;
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		sizeNeeded = get_tuple_chunk_item_size(desc, itemOffset) -
			(get_tuple_chunk_header_size(desc->chunkType) +
			 MAXALIGN(get_tuple_chunk_tuple_size(desc->treeDesc,
												 desc->chunkType, tuple)));
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * desc->chunkItemsCount) -
			MAXALIGN(sizeof(LocationIndex) * (desc->chunkItemsCount - 1));

		sizeNeeded = (-1) * (get_tuple_chunk_item_size(desc, itemOffset) +
							 itemsShift);
	}
	else
		Assert(false);

	return sizeNeeded;
}

void
tuple_chunk_perform_change(BTreeChunkDesc * desc, OffsetNumber itemOffset,
						   BTreeChunkOperationType operation,
						   Pointer tupleHeader, OTuple tuple)
{
	Pointer		itemPtr;
	uint16		tupleHeaderSize,
				tupleSize;

	if (operation == BTreeChunkOperationInsert ||
		operation == BTreeChunkOperationUpdate)
	{
		tupleHeaderSize = get_tuple_chunk_header_size(desc->chunkType);
		tupleSize = get_tuple_chunk_tuple_size(desc->treeDesc,
											   desc->chunkType, tuple);
	}

	if (operation == BTreeChunkOperationInsert)
	{
		/* Allocate space for the new item, move other items if necessary */
		allocate_chunk_item(desc, itemOffset,
							tupleHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = get_tuple_chunk_item(desc, itemOffset);

		memcpy(itemPtr, tupleHeader, tupleHeaderSize);
		itemPtr += tupleHeaderSize;
		memcpy(itemPtr, tuple.data, tupleSize);

		set_tuple_chunk_item_flags(desc, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		resize_chunk_item(desc, itemOffset,
						  tupleHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = get_tuple_chunk_item(desc, itemOffset);

		memcpy(itemPtr, tupleHeader, tupleHeaderSize);
		itemPtr += tupleHeaderSize;
		memcpy(itemPtr, tuple.data, tupleSize);

		set_tuple_chunk_item_flags(desc, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		delete_chunk_item(desc, itemOffset);
	}
	else
		elog(ERROR, "invalid BTreeChunkOperationType: %d", operation);
}

bool
tuple_chunk_cmp(BTreeChunkDesc * desc, PartialPageState *partial,
				Page page, OffsetNumber itemOffset,
				void *key, BTreeKeyType keyType, int *result)
{
	OTuple		tuple2;
	BTreeKeyType keyType2;
	OBTreeKeyCmp cmpFunc = desc->treeDesc->ops->cmp;

	if (partial && !load_chunk_partially(desc, partial, page))
		return false;

	Assert(itemOffset < desc->chunkItemsCount);

	keyType2 = O_PAGE_IS(page, LEAF) ? BTreeKeyLeafTuple : BTreeKeyNonLeafKey;
	tuple2 = read_tuple_chunk_tuple(desc, page, itemOffset);

	*result = cmpFunc(desc->treeDesc, key, keyType, &tuple2, keyType2);
	return true;
}

bool
tuple_chunk_search(BTreeChunkDesc * desc, PartialPageState *partial,
				   Page page, void *key, BTreeKeyType keyType,
				   OffsetNumber *itemOffset)
{
	bool		isLeaf = O_PAGE_IS(page, LEAF),
				nextKey;
	BTreeKeyType midKeyType;
	OffsetNumber low,
				mid,
				high;
	OBTreeKeyCmp cmpFunc = desc->treeDesc->ops->cmp;
	int			targetCmpVal,
				result;

	if (partial && !load_chunk_partially(desc, partial, page))
		return false;

	if (desc->chunkItemsCount == 0)
	{
		*itemOffset = 0;
		return true;
	}

	midKeyType = isLeaf ? BTreeKeyLeafTuple : BTreeKeyNonLeafKey;

	low = 0;
	high = desc->chunkItemsCount - 1;
	nextKey = (!isLeaf && keyType != BTreeKeyPageHiKey);

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

	targetCmpVal = nextKey ? 0 : 1; /* a target value of cmpFunc() */

	/*
	 * Don't pass BTreeHiKey to comparison function, we've set nextkey flag
	 * instead.
	 */
	if (keyType == BTreeKeyPageHiKey)
		keyType = BTreeKeyNonLeafKey;

	while (high > low)
	{
		mid = low + ((high - low) / 2);

		if (!isLeaf && mid == 0 && *itemOffset == 0)
			result = 1;
		else
		{
			OTuple		midTuple;

			*itemOffset = mid;
			midTuple = read_tuple_chunk_tuple(desc, page, *itemOffset);
			result = cmpFunc(desc->treeDesc, key, keyType, &midTuple, midKeyType);
		}

		if (result >= targetCmpVal)
			low = mid + 1;
		else
			high = mid;
	}

	*itemOffset = low;

	return true;
}

bool
tuple_chunk_read_tuple(BTreeChunkDesc * desc, PartialPageState *partial,
					   Page page, OffsetNumber itemOffset,
					   Pointer *tupleHeader, OTuple *tuple, bool *needsFree)
{
	if (partial && !load_chunk_partially(desc, partial, page))
		return false;

	Assert(itemOffset < desc->chunkItemsCount);

	if (O_PAGE_IS(page, LEAF))
		read_tuple_chunk_leaf_item(desc, page, itemOffset,
								   (BTreeLeafTuphdr **) tupleHeader, tuple);
	else
		read_tuple_chunk_internal_item(desc, page, itemOffset,
									   (BTreeNonLeafTuphdr **) tupleHeader,
									   tuple);

	/* Set always to true in the current implementation */
	*needsFree = true;

	return true;
}

void
init_tuple_chunk(BTreeChunkDesc * desc, Page page, OffsetNumber chunkOffset)
{
	BTreePageHeader *header = (BTreePageHeader *) page;
	LocationIndex chunkLocation = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset].shortLocation);

	desc->chunkOffset = chunkOffset;
	desc->chunkData = (Pointer) page + chunkLocation;

	if (chunkOffset + 1 < header->chunksCount)
	{
		desc->chunkItemsCount = header->chunkDesc[chunkOffset + 1].offset -
			header->chunkDesc[chunkOffset].offset;
		desc->chunkDataSize = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset + 1].shortLocation) -
			chunkLocation;
	}
	else
	{
		desc->chunkItemsCount = header->itemsCount -
			header->chunkDesc[chunkOffset].offset;
		desc->chunkDataSize = header->dataSize - chunkLocation;
	}
}

static void
allocate_chunk_item(BTreeChunkDesc * desc, OffsetNumber itemOffset,
					uint16 itemSize)
{
	LocationIndex itemsShift,
				dataShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;

	Assert(itemSize == MAXALIGN(itemSize));

	/* Calculate the change of (maxaligned) item array size */
	itemsShift = MAXALIGN(sizeof(LocationIndex) * (desc->chunkItemsCount + 1)) -
		MAXALIGN(sizeof(LocationIndex) * desc->chunkItemsCount);

	/* Calculate the shift of the data after new item is inserted */
	dataShift = itemsShift + itemSize;
	Assert(dataShift == MAXALIGN(dataShift));

	firstItemPtr = desc->chunkData +
		MAXALIGN(sizeof(LocationIndex) * desc->chunkItemsCount);

	if (itemOffset < desc->chunkItemsCount)
		itemPtr = get_tuple_chunk_item(desc, itemOffset);
	else
	{
		Assert(itemOffset == desc->chunkItemsCount);

		itemPtr = desc->chunkData + desc->chunkDataSize;
	}

	endPtr = desc->chunkData + desc->chunkDataSize;

	/* Data should still fit to the page */
	Assert(endPtr + dataShift <= (Pointer) ORIOLEDB_BLCKSZ);

	/* Shift the data after insert location */
	Assert(itemPtr <= endPtr);

	memmove(itemPtr + dataShift, itemPtr, endPtr - itemPtr);

	desc->chunkItemsCount++;
	desc->chunkDataSize += dataShift;

	if (itemsShift != 0)
	{
		/*
		 * If items array size is changed, then we have to also move the items
		 * before insert location and adjust those locations in the items
		 * array.
		 */
		memmove(firstItemPtr + itemsShift, firstItemPtr, itemPtr - firstItemPtr);
		for (OffsetNumber i = 0; i < itemOffset; i++)
			desc->chunkItems->items[i] += itemsShift;
	}

	/* Add the new item to the items array  */
	for (OffsetNumber i = desc->chunkItemsCount; i > itemOffset; i--)
		desc->chunkItems->items[i] = desc->chunkItems->items[i - 1] + dataShift;
	desc->chunkItems->items[itemOffset] = itemPtr - desc->chunkData + itemsShift;
}

static void
resize_chunk_item(BTreeChunkDesc * desc, OffsetNumber itemOffset,
				  uint16 newItemSize)
{
	LocationIndex dataShift;
	Pointer		nextItemPtr,
				endPtr;

	Assert(newItemSize == MAXALIGN(newItemSize));

	/* Calculate the shift of the data after the item resize */
	dataShift = newItemSize - get_tuple_chunk_item_size(desc, itemOffset);
	Assert(dataShift == MAXALIGN(dataShift));

	if (dataShift <= 0)
		return;

	Assert(itemOffset < desc->chunkItemsCount);
	if (itemOffset + 1 < desc->chunkItemsCount)
		nextItemPtr = get_tuple_chunk_item(desc, itemOffset + 1);
	else
		nextItemPtr = desc->chunkData + desc->chunkDataSize;

	endPtr = desc->chunkData + desc->chunkDataSize;

	/* Data should still fit to the page */
	Assert(endPtr + dataShift <= (Pointer) ORIOLEDB_BLCKSZ);

	/* Shift the data after the item */
	memmove(nextItemPtr + dataShift, nextItemPtr, endPtr - nextItemPtr);

	/* Adjust the items array */
	for (OffsetNumber i = itemOffset + 1; i < desc->chunkItemsCount; i++)
		desc->chunkItems->items[i] += dataShift;

	desc->chunkDataSize += dataShift;
}

static void
delete_chunk_item(BTreeChunkDesc * desc, OffsetNumber itemOffset)
{
	uint16		itemSize;
	LocationIndex itemsShift,
				dataShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;

	itemSize = get_tuple_chunk_item_size(desc, itemOffset);
	Assert(itemSize == MAXALIGN(itemSize));

	Assert(desc->chunkItemsCount > 0);
	/* Calculate the change of (maxaligned) item array size */
	itemsShift = MAXALIGN(sizeof(LocationIndex) * desc->chunkItemsCount) -
		MAXALIGN(sizeof(LocationIndex) * (desc->chunkItemsCount - 1));

	/* Calculate the shift of the data after the item is deleted */
	dataShift = itemsShift + itemSize;
	Assert(dataShift == MAXALIGN(dataShift));

	firstItemPtr = desc->chunkData +
		MAXALIGN(sizeof(LocationIndex) * desc->chunkItemsCount);

	Assert(itemOffset < desc->chunkItemsCount);
	itemPtr = get_tuple_chunk_item(desc, itemOffset);

	endPtr = desc->chunkData + desc->chunkDataSize;
	Assert(endPtr - dataShift >= itemPtr - itemsShift);

	/*
	 * Adjust the items array.  We should do this first to prevent it been
	 * overridden by the data when it's shorten.
	 */
	for (OffsetNumber i = itemOffset; i < desc->chunkItemsCount - 1; i++)
		desc->chunkItems->items[i] = desc->chunkItems->items[i + 1] - dataShift;

	if (itemsShift != 0)
	{
		/* Shift the data before deleted item when items arrays is shorten. */
		memmove(firstItemPtr - itemsShift, firstItemPtr, itemPtr - firstItemPtr);

		/* Shift item pointers of those items */
		for (OffsetNumber i = 0; i < itemOffset; i++)
			desc->chunkItems->items[i] -= itemsShift;
	}

	/* Move the data after deleted item */
	memmove(itemPtr - itemsShift, itemPtr + itemSize, endPtr - itemPtr - itemSize);

	desc->chunkItemsCount--;
	desc->chunkDataSize -= dataShift;
}

static bool
load_chunk_partially(BTreeChunkDesc * desc, PartialPageState *partial, Page page)
{
	Page		sourcePage = partial->src;
	uint32		pageState,
				sourceState;
	BTreePageHeader *header = (BTreePageHeader *) page;
	LocationIndex chunkLocation =
		SHORT_GET_LOCATION(header->chunkDesc[desc->chunkOffset].shortLocation);

	Assert(partial != NULL);

	if (!partial->isPartial || partial->chunkIsLoaded[desc->chunkOffset])
		return true;

	init_tuple_chunk(desc, page, desc->chunkOffset);

	memcpy((Pointer) page + chunkLocation,
		   (Pointer) sourcePage + chunkLocation, desc->chunkDataSize);

	pg_read_barrier();

	pageState = pg_atomic_read_u32(&(O_PAGE_HEADER(page)->state));
	sourceState = pg_atomic_read_u32(&(O_PAGE_HEADER(sourcePage)->state));
	if ((pageState & PAGE_STATE_CHANGE_COUNT_MASK) != (sourceState & PAGE_STATE_CHANGE_COUNT_MASK) ||
		O_PAGE_STATE_READ_IS_BLOCKED(sourceState))
		return false;

	if (O_PAGE_GET_CHANGE_COUNT(page) != O_PAGE_GET_CHANGE_COUNT(sourcePage))
		return false;

	partial->chunkIsLoaded[desc->chunkOffset] = true;

	return true;
}

static inline uint16
get_tuple_chunk_header_size(BTreeChunkType chunkType)
{
	if (chunkType == BTreeChunkLeaf)
		return sizeof(BTreeLeafTuphdr);
	else if (chunkType == BTreeChunkNonLeaf ||
			 chunkType == BTreeChunkHiKeys)
		return sizeof(BTreeNonLeafTuphdr);
	else
		Assert(false);
}

static inline uint16
get_tuple_chunk_tuple_size(BTreeDescr *treeDesc, BTreeChunkType chunkType,
						   OTuple tuple)
{
	if (chunkType == BTreeChunkLeaf)
		return o_btree_len(treeDesc, tuple, OTupleLength);
	else if (chunkType == BTreeChunkNonLeaf ||
			 chunkType == BTreeChunkHiKeys)
		return o_btree_len(treeDesc, tuple, OKeyLength);
	else
		Assert(false);
}

void
tuple_chunk_builder_init(BTreeChunkBuilderData * builderData,
						 BTreeDescr *treeDesc, BTreeChunkType chunkType)
{
	builderData->treeDesc = treeDesc;
	builderData->chunkType = chunkType;

	builderData->chunkData = NULL;
	builderData->chunkDataMaxSize = 0;
	builderData->chunkDataSize = 0;

	builderData->chunkItemsCount = 0;
}

int32
tuple_chunk_builder_estimate(BTreeChunkBuilderData * builderData,
							 OTuple tuple)
{
	LocationIndex itemsShift;

	Assert(builderData != NULL);

	itemsShift = MAXALIGN(sizeof(LocationIndex) * (builderData->chunkItemsCount + 1)) -
		MAXALIGN(sizeof(LocationIndex) * builderData->chunkItemsCount);

	return itemsShift + MAXALIGN(
								 get_tuple_chunk_tuple_size(builderData->treeDesc,
															builderData->chunkType, tuple)) +
		get_tuple_chunk_header_size(builderData->chunkType);
}

void
tuple_chunk_builder_add(BTreeChunkBuilderData * builderData, OTuple tuple,
						uint64 downlink)
{
	Size		tupleHeaderSize,
				tupleSize;

	Assert(builderData != NULL);
	Assert(builderData->chunkDataMaxSize > 0);
	Assert(builderData->chunkDataSize < builderData->chunkDataMaxSize);

	tupleHeaderSize = get_tuple_chunk_header_size(builderData->chunkType);
	tupleSize = get_tuple_chunk_tuple_size(builderData->treeDesc,
										   builderData->chunkType, tuple);

	if (builderData->chunkType == BTreeChunkLeaf)
	{
		BTreeLeafTuphdr leaf_header = {0};

		leaf_header.deleted = BTreeLeafTupleNonDeleted;
		leaf_header.undoLocation = InvalidUndoLocation;
		leaf_header.xactInfo = OXID_GET_XACT_INFO(BootstrapTransactionId, RowLockUpdate, false);

		add_builder_item(builderData, (Pointer) &leaf_header, tupleHeaderSize,
						 tuple, tupleSize);
	}
	else if (builderData->chunkType == BTreeChunkNonLeaf ||
			 builderData->chunkType == BTreeChunkHiKeys)
	{
		BTreeNonLeafTuphdr internal_header = {0};

		internal_header.downlink = downlink;

		add_builder_item(builderData, (Pointer) &internal_header, tupleHeaderSize,
						 tuple, tupleSize);
	}
	else
		Assert(false);
}

void
tuple_chunk_builder_finish(BTreeChunkBuilderData * builderData,
						   BTreeChunkDesc * chunkDesc)
{
	uint16		chunkItemsSize;
	Pointer		itemPtr;

	chunkDesc->treeDesc = builderData->treeDesc;
	chunkDesc->chunkType = builderData->chunkType;

	chunkDesc->chunkData = builderData->chunkData;
	chunkDesc->chunkDataSize = builderData->chunkDataSize;

	chunkDesc->chunkItemsCount = builderData->chunkItemsCount;

	/* Move items and put chunkItems into the beginning of chunkData */

	chunkItemsSize = MAXALIGN(sizeof(LocationIndex) * chunkDesc->chunkItemsCount);
	itemPtr = chunkDesc->chunkData + chunkItemsSize;

	memmove(itemPtr, chunkDesc->chunkData, chunkDesc->chunkDataSize);

	memcpy(chunkDesc->chunkData, builderData->chunkItems,
		   sizeof(LocationIndex) * builderData->chunkItemsCount);
	chunkDesc->chunkItems = (BTreePageChunk *) chunkDesc->chunkData;

	/* Adjust items offets */
	for (OffsetNumber i = 0; i < chunkDesc->chunkItemsCount; i++)
		chunkDesc->chunkItems->items[i] += chunkItemsSize;
}

static void
add_builder_item(BTreeChunkBuilderData * builderData,
				 Pointer tupleHeader, uint16 tupleHeaderSize,
				 OTuple tuple, uint16 tupleSize)
{
	Pointer		itemPtr;
	uint16		itemSize = tupleHeaderSize + MAXALIGN(tupleSize);

	Assert(builderData->chunkDataMaxSize >= builderData->chunkDataSize + itemSize);

	itemPtr = builderData->chunkData + builderData->chunkDataSize;

	/* Copy the header and the tuple */
	memcpy(itemPtr, tupleHeader, tupleHeaderSize);
	itemPtr += tupleHeaderSize;
	memcpy(itemPtr, tuple.data, tupleSize);

	/* Adjust the item offset */
	builderData->chunkItems->items[builderData->chunkItemsCount] =
		builderData->chunkDataSize;
	builderData->chunkItems->items[builderData->chunkItemsCount] =
		ITEM_SET_FLAGS(builderData->chunkItems->items[builderData->chunkItemsCount],
					   tuple.formatFlags);

	/* Ajust the size */
	builderData->chunkDataSize = builderData->chunkDataSize + itemSize;
	builderData->chunkItemsCount++;
}
