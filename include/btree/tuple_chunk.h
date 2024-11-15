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

extern int32 tuple_chunk_estimate_change(BTreeChunkDesc *chunk,
										 OffsetNumber itemOffset,
										 BTreeChunkOperationType operation,
										 OTuple tuple);
extern void tuple_chunk_perform_change(BTreeChunkDesc *chunk,
									   OffsetNumber itemOffset,
									   BTreeChunkOperationType operation,
									   Pointer tupleHeader, OTuple tuple);
extern void tuple_chunk_compact(BTreeChunkDesc *chunk);
extern bool tuple_chunk_cmp(BTreeChunkDesc *chunk, PartialPageState *partial,
							Page page, OffsetNumber itemOffset,
							void *key, BTreeKeyType keyType, int *result);
extern bool tuple_chunk_search(BTreeChunkDesc *chunk, PartialPageState *partial,
							   Page page, void *key, BTreeKeyType keyType,
							   OffsetNumber *itemOffset);
extern bool tuple_chunk_read_tuple(BTreeChunkDesc *chunk, PartialPageState *partial,
								   Page page, OffsetNumber itemOffset,
								   Pointer *tupleHeader, OTuple *tuple,
								   bool *needsFree);

extern void init_tuple_chunk(BTreeChunkDesc *chunk, Page page,
							 OffsetNumber chunkOffset);

/* Builder routines */

extern void tuple_chunk_builder_init(BTreeChunkBuilderData *chunkBuilder,
									 BTreeDescr *treeDesc,
									 BTreeChunkType chunkType);
extern int32 tuple_chunk_builder_estimate(BTreeChunkBuilderData *chunkBuilder,
										  OTuple tuple);
extern void tuple_chunk_builder_add(BTreeChunkBuilderData *chunkBuilder,
									OTuple tuple, uint64 downlink);
extern void tuple_chunk_builder_finish(BTreeChunkBuilderData *chunkBuilder,
									   BTreeChunkDesc *chunk);


static inline uint8
get_tuple_chunk_item_flags(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < chunk->chunkItemsCount);

	return ITEM_GET_FLAGS(chunk->chunkItems->items[itemOffset]);
}

static inline void
set_tuple_chunk_item_flags(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
						   uint8 flags)
{
	Assert(itemOffset < chunk->chunkItemsCount);

	chunk->chunkItems->items[itemOffset] =
		ITEM_SET_FLAGS(chunk->chunkItems->items[itemOffset], flags);
}

static inline uint16
get_tuple_chunk_item_size(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	LocationIndex itemLocation,
				nextItemLocation;

	Assert(chunk->chunkItemsCount > itemOffset);

	/* Calculate offset form the beginning of the chunk */
	itemLocation = ITEM_GET_OFFSET(chunk->chunkItems->items[itemOffset]);

	if (itemOffset + 1 < chunk->chunkItemsCount)
	{
		nextItemLocation = ITEM_GET_OFFSET(chunk->chunkItems->items[itemOffset + 1]);
		return nextItemLocation - itemLocation;
	}
	else
		return chunk->chunkDataSize - itemLocation;
}

static inline Pointer
get_tuple_chunk_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < chunk->chunkItemsCount);

	return chunk->chunkData + ITEM_GET_OFFSET(chunk->chunkItems->items[itemOffset]);
}

static inline void
read_tuple_chunk_leaf_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
						   BTreeLeafTuphdr **tupleHeader, OTuple *tuple)
{
	Pointer		item = get_tuple_chunk_item(chunk, itemOffset);

	*tupleHeader = (BTreeLeafTuphdr *) item;
	tuple->data = item + BTreeLeafTuphdrSize;
	tuple->formatFlags = get_tuple_chunk_item_flags(chunk, itemOffset);
}

static inline void
read_tuple_chunk_internal_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
							   BTreeNonLeafTuphdr **tupleHeader, OTuple *tuple)
{
	Pointer		item = get_tuple_chunk_item(chunk, itemOffset);

	*tupleHeader = (BTreeNonLeafTuphdr *) item;
	tuple->data = item + BTreeNonLeafTuphdrSize;
	tuple->formatFlags = get_tuple_chunk_item_flags(chunk, itemOffset);
}

static inline OTuple
read_tuple_chunk_tuple(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	Pointer		item = get_tuple_chunk_item(chunk, itemOffset);
	OTuple		result;

	result.formatFlags = get_tuple_chunk_item_flags(chunk, itemOffset);
	if (chunk->chunkType == BTreeChunkLeaf)
		result.data = item + BTreeLeafTuphdrSize;
	else
		result.data = item + BTreeNonLeafTuphdrSize;

	return result;
}

#endif							/* __BTREE_TUPLE_CHUNK_H__ */
