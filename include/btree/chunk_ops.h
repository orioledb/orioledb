/*-------------------------------------------------------------------------
 *
 * chunk_ops.h
 *		OrioleDB abstract page chunk access API.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/chunk_ops.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_CHUNK_OPS_H__
#define __BTREE_CHUNK_OPS_H__

#include "btree/btree.h"
#include "btree/page_contents.h"

typedef enum BTreeChunkOperationType
{
	BTreeChunkOperationInsert,
	BTreeChunkOperationUpdate,
	BTreeChunkOperationDelete
} BTreeChunkOperationType;

typedef struct PartialPageState PartialPageState;

typedef struct BTreeChunkOps BTreeChunkOps;

typedef struct BTreeChunkBuilderItem
{
	Pointer		data;
	uint16		size;
	uint8		flags;
} BTreeChunkBuilderItem;

typedef struct BTreeChunkBuilder
{
	BTreeDescr *treeDesc;
	const BTreeChunkOps *const ops;

	/* Array of offsets of chunk items */
	BTreeChunkBuilderItem chunkItems[BTREE_PAGE_MAX_CHUNK_ITEMS];
	/* Number of chunk items */
	uint16		chunkItemsCount;
} BTreeChunkBuilder;

typedef struct BTreeChunkDesc
{
	BTreeDescr *treeDesc;
	const BTreeChunkOps *const ops;

	/* Page which contains the chunk */
	Page		page;
	/* Offset of the chunk within the page */
	OffsetNumber chunkOffset;
} BTreeChunkDesc;

typedef struct BTreeChunkOps
{
	uint16		itemHeaderSize;
	BTreeKeyType itemKeyType;
	OLengthType itemLengthType;

	/*
	 * Main API functions.
	 */

	/*
	 * Initialize the chunk using the given page and optional chunk offset.
	 */
	void		(*init) (BTreeChunkDesc *chunk, Page page,
						 OffsetNumber chunkOffset);

	/*
	 * Estimate the new chunk size after the operation.
	 */
	int32		(*estimate_change) (BTreeChunkDesc *chunk, OffsetNumber itemOffset,
									BTreeChunkOperationType operation,
									OTuple tuple);

	/*
	 * Perform the operation.  A caller should take care about the size of the
	 * chunk.  Extension of the chunk should be performed before
	 * "perform_change", and shrinking of the chunk should be performed after
	 * "perform_change".
	 */
	void		(*perform_change) (BTreeChunkDesc *chunk, OffsetNumber itemOffset,
								   BTreeChunkOperationType operation,
								   Pointer tupleHeader, OTuple tuple);

	/*
	 * Performa compaction of the chunk by moving items and releasing gaps.  A
	 * caller is responsible to take care of deleted and non-visible tuples in
	 * the chunk.
	 */
	void		(*compact) (BTreeChunkDesc *chunk);

	/*
	 * Get available size for compaction.
	 */
	uint16		(*get_available_size) (BTreeChunkDesc *chunk, CommitSeqNo csn);

	/*
	 * Compare given key to the chunk tuple.  Return false if the chunk was
	 * changed concurrently.
	 */
	bool		(*cmp) (BTreeChunkDesc *chunk, PartialPageState *partial,
						Page page, OffsetNumber itemOffset,
						void *key, BTreeKeyType keyType, int *result);

	/*
	 * Search for the given key in the chunk tuple.  Return false if the chunk
	 * was changed concurrently.
	 */
	bool		(*search) (BTreeChunkDesc *chunk, PartialPageState *partial,
						   Page page, void *key, BTreeKeyType keyType,
						   OffsetNumber *itemOffset);

	/*
	 * Read the tuple from the chunk.  Return false if the chunk was changed
	 * concurrently.
	 */
	bool		(*read_tuple) (BTreeChunkDesc *chunk, PartialPageState *partial,
							   Page page, OffsetNumber itemOffset,
							   Pointer *tupleHeader, OTuple *tuple,
							   bool *isCopy);

	/*
	 * Chunk builder functions.
	 */

	/*
	 * Initialize the chunk builder.
	 */
	void		(*builder_init) (BTreeChunkBuilder *chunkBuilder);

	/*
	 * Estimate the chunk size after adding the tuple to the builder.
	 */
	int32		(*builder_estimate) (BTreeChunkBuilder *chunkBuilder, OTuple tuple);

	/*
	 * Add the tuple to the builder.
	 */
	void		(*builder_add) (BTreeChunkBuilder *chunkBuilder,
								BTreeChunkBuilderItem *chunkItem);

	/*
	 * Finish building the chunk.
	 */
	void		(*builder_finish) (BTreeChunkBuilder *chunkBuilder,
								   BTreeChunkDesc *chunk);
} BTreeChunkOps;

extern const BTreeChunkOps BTreeLeafTupleChunkOps;
extern const BTreeChunkOps BTreeInternalTupleChunkOps;
extern const BTreeChunkOps BTreeHiKeyChunkOps;

/*
 * Chunk initialization functions.
 */

extern void init_btree_chunk_desc(BTreeChunkDesc *desc,
								  const BTreeChunkOps *ops,
								  BTreeDescr *treeDesc, Page page,
								  OffsetNumber chunkOffset);

extern void init_btree_chunk_builder(BTreeChunkBuilder *chunkBuilder,
									 const BTreeChunkOps *ops,
									 BTreeDescr *treeDesc);

#endif							/* __BTREE_CHUNK_OPS_H__ */
