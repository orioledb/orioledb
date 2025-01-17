/*-------------------------------------------------------------------------
 *
 * tuple_chunk.h
 *		OrioleDB implementation of chunk API over tuple chunks.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/tuple_chunk.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_TUPLE_CHUNK_H__
#define __BTREE_TUPLE_CHUNK_H__

#include "btree/chunk_ops.h"

/* A tuple chunk routines */

extern int32 tuple_chunk_estimate_change(BTreeChunkDesc *desc,
										 OffsetNumber itemOffset,
										 BTreeChunkOperationType operation,
										 OTuple tuple);
extern void tuple_chunk_perform_change(BTreeChunkDesc *desc,
									   OffsetNumber itemOffset,
									   BTreeChunkOperationType operation,
									   Pointer tupleHeader, OTuple tuple);
extern bool tuple_chunk_cmp(BTreeChunkDesc *desc, PartialPageState *partial,
							Page page, OffsetNumber itemOffset,
							void *key, BTreeKeyType keyType, int *result);
extern bool tuple_chunk_search(BTreeChunkDesc *desc, PartialPageState *partial,
							   Page page, void *key, BTreeKeyType keyType,
							   OffsetNumber *itemOffset);
extern bool tuple_chunk_read_tuple(BTreeChunkDesc *desc, PartialPageState *partial,
								   Page page, OffsetNumber itemOffset,
								   Pointer *tupleHeader, OTuple *tuple,
								   bool *needsFree);

extern void init_tuple_chunk(BTreeChunkDesc *desc, Page page,
							 OffsetNumber chunkOffset);

/* Builder routines */

extern void tuple_chunk_builder_init(BTreeChunkBuilderData *builderData,
									 BTreeDescr *treeDesc,
									 BTreeChunkType chunkType);
extern int32 tuple_chunk_builder_estimate(BTreeChunkBuilderData *builderData,
										  OTuple tuple);
extern void tuple_chunk_builder_add(BTreeChunkBuilderData *builderData,
									OTuple tuple, uint64 downlink);
extern void tuple_chunk_builder_finish(BTreeChunkBuilderData *builderData,
									   BTreeChunkDesc *chunkDesc);


static inline uint8
get_tuple_chunk_item_flags(BTreeChunkDesc *desc, OffsetNumber itemOffset)
{
	Assert(itemOffset < desc->chunkItemsCount);

	return ITEM_GET_FLAGS(desc->chunkItems->items[itemOffset]);
}

static inline void
set_tuple_chunk_item_flags(BTreeChunkDesc *desc, OffsetNumber itemOffset,
						   uint8 flags)
{
	Assert(itemOffset < desc->chunkItemsCount);

	desc->chunkItems->items[itemOffset] =
		ITEM_SET_FLAGS(desc->chunkItems->items[itemOffset], flags);
}

static inline uint16
get_tuple_chunk_item_size(BTreeChunkDesc *desc, OffsetNumber itemOffset)
{
	LocationIndex itemLocation,
				nextItemLocation;

	Assert(desc->chunkItemsCount > itemOffset);

	/* Calculate offset form the beginning of the chunk */
	itemLocation = ITEM_GET_OFFSET(desc->chunkItems->items[itemOffset]);

	if (itemOffset + 1 < desc->chunkItemsCount)
	{
		nextItemLocation = ITEM_GET_OFFSET(desc->chunkItems->items[itemOffset + 1]);
		return nextItemLocation - itemLocation;
	}
	else
		return desc->chunkDataSize - itemLocation;
}

static inline Pointer
get_tuple_chunk_item(BTreeChunkDesc *desc, OffsetNumber itemOffset)
{
	Assert(itemOffset < desc->chunkItemsCount);

	return desc->chunkData + ITEM_GET_OFFSET(desc->chunkItems->items[itemOffset]);
}

static inline void
read_tuple_chunk_leaf_item(BTreeChunkDesc *desc, OffsetNumber itemOffset,
						   BTreeLeafTuphdr **tupleHeader, OTuple *tuple)
{
	Pointer		item = get_tuple_chunk_item(desc, itemOffset);

	*tupleHeader = (BTreeLeafTuphdr *) item;
	tuple->data = item + BTreeLeafTuphdrSize;
	tuple->formatFlags = get_tuple_chunk_item_flags(desc, itemOffset);
}

static inline void
read_tuple_chunk_internal_item(BTreeChunkDesc *desc, OffsetNumber itemOffset,
							   BTreeNonLeafTuphdr **tupleHeader, OTuple *tuple)
{
	Pointer		item = get_tuple_chunk_item(desc, itemOffset);

	*tupleHeader = (BTreeNonLeafTuphdr *) item;
	tuple->data = item + BTreeNonLeafTuphdrSize;
	tuple->formatFlags = get_tuple_chunk_item_flags(desc, itemOffset);
}

static inline OTuple
read_tuple_chunk_tuple(BTreeChunkDesc *desc, OffsetNumber itemOffset)
{
	Pointer		item = get_tuple_chunk_item(desc, itemOffset);
	OTuple		result;

	result.formatFlags = get_tuple_chunk_item_flags(desc, itemOffset);
	if (desc->chunkType == BTreeChunkLeaf)
		result.data = item + BTreeLeafTuphdrSize;
	else
		result.data = item + BTreeNonLeafTuphdrSize;

	return result;
}

#endif							/* __BTREE_TUPLE_CHUNK_H__ */
