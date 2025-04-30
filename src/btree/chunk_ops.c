/*-------------------------------------------------------------------------
 *
 * chunk_ops.c
 *		Utility functions for OrioleDB abstract page chunk access API.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/chunk_ops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/chunk_ops.h"

#include "utils/memdebug.h"

/*
 * Chunk initialization functions.
 */

BTreeChunkDesc *
make_btree_chunk_desc(BTreeDescr *treeDesc, const BTreeChunkOps *ops,
					  Page page, OffsetNumber chunkOffset)
{
	BTreeChunkDesc *res;

	res = palloc0(ops->chunkDescSize);

	res->treeDesc = treeDesc;
	*((const BTreeChunkOps **) &res->ops) = ops;
	ops->init(res, page, chunkOffset);

	return res;
}

void
release_btree_chunk_desc(BTreeChunkDesc *desc)
{
	desc->ops->release(desc);
}

BTreeChunkBuilder *
make_btree_chunk_builder(BTreeDescr *treeDesc, const BTreeChunkOps *ops)
{
	BTreeChunkBuilder *res;

	res = palloc0(sizeof(BTreeChunkBuilder));

	res->treeDesc = treeDesc;
	*((const BTreeChunkOps **) &res->ops) = ops;
	ops->builder_init(res);

	return res;
}

void
release_btree_chunk_builder(BTreeChunkBuilder *chunkBuilder)
{
}

/*
 * Page context utility functions.
 */

void
btree_page_context_init(BTreePageContext *pageContext, BTreeDescr *treeDesc)
{
	memset(pageContext, 0, sizeof(*pageContext));

	pageContext->treeDesc = treeDesc;
	pageContext->mctx = AllocSetContextCreate(CurrentMemoryContext,
											  "orioledb BTreePageContext context",
											  ALLOCSET_DEFAULT_SIZES);
}

void
btree_page_context_release(BTreePageContext *pageContext)
{
	pageContext->hikeyChunk = NULL;
	pageContext->tupleChunks = NULL;

	if (pageContext->mctx != NULL)
		MemoryContextReset(pageContext->mctx);
}

void
btree_page_context_set(BTreePageContext *pageContext, Page page)
{
	btree_page_context_release(pageContext);
	pageContext->page = page;
}

void
btree_page_context_invalidate(BTreePageContext *pageContext)
{
	btree_page_context_release(pageContext);
}

void
btree_page_context_hikey_init(BTreePageContext *pageContext,
							  const BTreeChunkOps *ops)
{
	Assert(pageContext->hikeyChunk == NULL || pageContext->hikeyChunk->ops == ops);

	if (pageContext->hikeyChunk == NULL)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(pageContext->mctx);

		pageContext->hikeyChunk = make_btree_chunk_desc(pageContext->treeDesc,
														ops,
														pageContext->page, 0);

		MemoryContextSwitchTo(oldctx);

		VALGRIND_CHECK_MEM_IS_DEFINED(pageContext->hikeyChunk->chunkData,
									  pageContext->hikeyChunk->chunkDataSize);
	}
}

void
btree_page_context_tuple_init(BTreePageContext *pageContext,
							  const BTreeChunkOps *ops,
							  OffsetNumber chunkOffset)
{
	if (pageContext->tupleChunks == NULL)
	{
		BTreePageHeader *header = (BTreePageHeader *) pageContext->page;

		pageContext->tupleChunks =
			(BTreeChunkDesc **) palloc0(sizeof(BTreeChunkDesc *) *
										header->chunksCount);
	}

	if (pageContext->tupleChunks[chunkOffset] == NULL)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(pageContext->mctx);

		pageContext->tupleChunks[chunkOffset] =
			make_btree_chunk_desc(pageContext->treeDesc, ops,
								  pageContext->page, chunkOffset);

		MemoryContextSwitchTo(oldctx);
	}
	else
	{
		MemoryContext oldctx = MemoryContextSwitchTo(pageContext->mctx);

		Assert(pageContext->tupleChunks[chunkOffset]->ops == ops);

		pageContext->tupleChunks[chunkOffset]->ops->init(
			pageContext->tupleChunks[chunkOffset],
			pageContext->page, chunkOffset);

		MemoryContextSwitchTo(oldctx);
	}

	VALGRIND_CHECK_MEM_IS_DEFINED(pageContext->tupleChunk->chunkData,
								  pageContext->tupleChunk->chunkDataSize);
}

/*
 * Utility functions
 */

uint16
btree_get_available_size(BTreeChunkDesc *chunk, CommitSeqNo csn)
{
	return chunk->ops->get_available_size(chunk, csn);
}
