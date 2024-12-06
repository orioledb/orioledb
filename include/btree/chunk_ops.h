/*-------------------------------------------------------------------------
 *
 * chunk_ops.h
 *		OrioleDB abstract page chunk access API.
 *
 * Copyright (c) 2024, Oriole DB Inc.
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

typedef enum BTreeChunkType
{
	BTreeChunkHiKeys,
	BTreeChunkNonLeaf,
	BTreeChunkLeaf
} BTreeChunkType;

typedef struct BTreeChunkBuilderData
{
	BTreeDescr *treeDesc;
	BTreeChunkType chunkType;

	Pointer		chunkData;
	Size		chunkDataMaxSize;
	Size		chunkDataSize;

	/* Array of offsets of chunk items */
	BTreePageChunk chunkItems[BTREE_PAGE_MAX_CHUNK_ITEMS];
	/* Number of chunk items */
	uint16		chunkItemsCount;
} BTreeChunkBuilderData;

typedef struct PartialPageState PartialPageState;

typedef struct BTreeChunkDesc BTreeChunkDesc;

typedef struct BTreeChunkOps
{
	/*
	 * Estimate the new chunk size after the operation.
	 */
	int32		(*estimate_change) (BTreeChunkDesc *desc, OffsetNumber itemOffset,
									BTreeChunkOperationType operation,
									Pointer tupleHeader, OTuple tuple);

	/*
	 * Perform the operation.  A caller should take care about the size of the
	 * chunk.  Extension of the chunk should be performed before "perform_change",
	 * and shrinking of the chunk should be performed after "perform_change".
	 */
	void		(*perform_change) (BTreeChunkDesc *desc, OffsetNumber itemOffset,
								   BTreeChunkOperationType operation,
								   Pointer tupleHeader, OTuple tuple);

	/*
	 * Compare given key to the chunk tuple.  Return false if the chunk was
	 * changed concurrently.
	 */
	bool		(*cmp) (BTreeChunkDesc *desc, PartialPageState *partial,
						Page page, OffsetNumber itemOffset,
						void *key, BTreeKeyType keyType, int *result);

	/*
	 * Search for the given key in the chunk tuple.  Return false if the chunk
	 * was changed concurrently.
	 */
	bool		(*search) (BTreeChunkDesc *desc, PartialPageState *partial,
						   Page page, void *key, BTreeKeyType keyType,
						   OffsetNumber *itemOffset);

	/*
	 * Read the tuple from the chunk.  Return false if the chunk was changed
	 * concurrently.
	 */
	bool		(*read_tuple) (BTreeChunkDesc *desc, PartialPageState *partial,
							   Page page, OffsetNumber itemOffset,
							   Pointer *tupleHeader, OTuple *tuple,
							   bool *needsFree);

	/*
	 * Initialize the chunk builder.
	 */
	void		(*builder_init) (BTreeChunkBuilderData *builderData,
								 BTreeDescr *treeDesc,
								 BTreeChunkType chunkType, Size size);

	/*
	 * Estimate the chunk size after adding the tuple to the builder.
	 */
	int32		(*builder_estimate) (BTreeChunkBuilderData *builderData, OTuple tuple);

	/*
	 * Add the tuple to the builder.
	 */
	void		(*builder_add) (BTreeChunkBuilderData *builderData, OTuple tuple);

	/*
	 * Finish building the chunk.
	 */
	void		(*builder_finish) (BTreeChunkBuilderData *builderData,
								   Pointer chunkData);
} BTreeChunkOps;

typedef struct BTreeChunkDesc
{
	BTreeDescr *treeDesc;
	BTreeChunkOps *ops;
	BTreeChunkType chunkType;

	Pointer		chunkData;
	/* Size of the chunkData in bytes */
	uint16		chunkDataSize;
	/* Offset number of the chunk within BTreePageHeader->chunkDesc */
	OffsetNumber chunkOffset;

	/* Array of offsets of chunk items */
	BTreePageChunk *chunkItems;
	/* Number of chunk items */
	uint16		chunkItemsCount;
} BTreeChunkDesc;

#endif							/* __BTREE_CHUNK_OPS_H__ */
