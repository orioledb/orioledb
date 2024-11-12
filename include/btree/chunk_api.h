/*-------------------------------------------------------------------------
 *
 * chunk_api.h
 * 		Abstract chunk storage API
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/chunk_api.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_CHUNK_API_H__
#define __BTREE_CHUNK_API_H__

#include "btree.h"
#include "page_contents.h"

typedef enum BTreeChunkOperationType
{
	BTreeChunkOperationInsert,
	BTreeChunkOperationUpdate,
	BTreeChunkOperationDelete
}			BTreeChunkOperationType;

typedef enum BTreeChunkType
{
	BTreeHiKeysChunk,
	BTreeNonLeafChunk,
	BTreeLeafChunk
}			BTreeChunkType;


typedef struct BTreeChunkDesc BTreeChunkDesc;

/*
 * API for a chunk storage abstraction.
 */
typedef struct
{
	/* Amount of memory to pre-allocated for the chunk builder */
	int			builderMemSize;

	/*
	 * Estimate the chunk size change after operation.
	 */
	int			(*estimate_change) (BTreeChunkDesc *desc,
									OffsetNumber offset,
									BTreeChunkOperationType operation,
									Pointer tupHdr,
									OTuple tuple);

	/*
	 * Perform the operation.  Caller should take care about size change
	 * according to result of 'estimate_change' method.  Extension should be
	 * performed before 'perform_change', shrink should be performed after
	 * 'perform_change'.
	 */
	void		(*perform_change) (BTreeChunkDesc *desc,
								   OffsetNumber offset,
								   BTreeChunkOperationType operation,
								   Pointer tupHdr,
								   OTuple tuple);

	/*
	 * Compare given key to the chunk tuple.  Return false if the chunk was
	 * changed concurrently.
	 */
	bool		(*cmp) (BTreeChunkDesc *desc,
						PartialPageState *partial,
						uint32 lastState,
						OffsetNumber offset,
						void *p, BTreeKeyType k,
						int *result);

	/*
	 * Search for the given key to the chunk tuple.  Return false if the chunk
	 * was changed concurrently.
	 */
	bool		(*search) (BTreeChunkDesc *desc,
						   PartialPageState *partial,
						   uint32 lastState,
						   void *p, BTreeKeyType k,
						   OffsetNumber *offset);

	/*
	 * Read tuple from chunk.  Return false if the chunk was changed
	 * concurrently.
	 */
	bool		(*read_tuple) (BTreeChunkDesc *desc,
							   PartialPageState *partial,
							   uint32 lastState,
							   OffsetNumber offset,
							   OTuple *tuple,
							   Pointer tupHdr);

	/*
	 * Initialize chunk empty builder
	 */
	void		(*builder_init) (Pointer builderData,
								 BTreeChunkType chunkType);

	/*
	 * Estimate result chunk size after adding the next tuple to the builder.
	 */
	int			(*builder_estimate) (Pointer builderData,
									 OTuple tuple);

	/*
	 * Add the next tuple to the builder.
	 */
	void		(*builder_add) (Pointer builderData,
								OTuple tuple);

	/*
	 * Finish building the chunk.
	 */
	void		(*builder_finish) (Pointer builderData,
								   Pointer chunkData);
}			BTreeChunkOps;

struct BTreeChunkDesc
{
	Pointer		chunkData;
	BTreeChunkOps *ops;
	void	   *arg;
};

#endif							/* __BTREE_CHUNK_API_H__ */
