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

/*
 * Chunk initialization functions.
 */

BTreeChunkDesc *
make_chunk_desc(BTreeDescr *treeDesc, const BTreeChunkOps *ops,
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
release_chunk_desc(BTreeChunkDesc *desc)
{
	desc->ops->release(desc);
}

BTreeChunkBuilder *
make_chunk_builder(BTreeDescr *treeDesc, const BTreeChunkOps *ops)
{
	BTreeChunkBuilder *res;

	res = palloc0(sizeof(BTreeChunkBuilder));

	res->treeDesc = treeDesc;
	*((const BTreeChunkOps **) &res->ops) = ops;
	ops->builder_init(res);

	return res;
}

void
release_chunk_builder(BTreeChunkBuilder *chunkBuilder)
{
}

/*
 * Page context utility functions.
 */

void
page_context_init(BTreePageContext *pageContext, BTreeDescr *treeDesc)
{
	memset(pageContext, 0, sizeof(*pageContext));

	pageContext->treeDesc = treeDesc;
	pageContext->mctx = CurrentMemoryContext;
}

void
page_context_release(BTreePageContext *pageContext)
{
	if (pageContext->hikeyChunk != NULL)
	{
		release_chunk_desc(pageContext->hikeyChunk);
		pfree(pageContext->hikeyChunk);
		pageContext->hikeyChunk = NULL;
	}

	if (pageContext->tupleChunk != NULL)
	{
		release_chunk_desc(pageContext->tupleChunk);
		pfree(pageContext->tupleChunk);
		pageContext->tupleChunk = NULL;
	}
}

void
page_context_set_page(BTreePageContext *pageContext, Page page)
{
	page_context_release(pageContext);
	pageContext->page = page;
}

void
page_context_set_invalid(BTreePageContext *pageContext)
{
	page_context_release(pageContext);
}

void
page_context_hikey_init(BTreePageContext *pageContext, const BTreeChunkOps *ops)
{
	Assert(pageContext->hikeyChunk == NULL || pageContext->hikeyChunk->ops == ops);

	if (pageContext->hikeyChunk == NULL)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(pageContext->mctx);

		pageContext->hikeyChunk = make_chunk_desc(pageContext->treeDesc,
												  ops,
												  pageContext->page, 0);

		MemoryContextSwitchTo(oldctx);
	}
}

void
page_context_tuple_init(BTreePageContext *pageContext, const BTreeChunkOps *ops,
						OffsetNumber chunkOffset)
{
	if (pageContext->tupleChunk == NULL)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(pageContext->mctx);

		pageContext->tupleChunk = make_chunk_desc(pageContext->treeDesc,
												  ops,
												  pageContext->page, chunkOffset);

		MemoryContextSwitchTo(oldctx);
	}
	else
	{
		MemoryContext oldctx = MemoryContextSwitchTo(pageContext->mctx);

		Assert(pageContext->tupleChunk->ops == ops);

		pageContext->tupleChunk->ops->init(pageContext->tupleChunk,
										   pageContext->page, chunkOffset);

		MemoryContextSwitchTo(oldctx);
	}
}
