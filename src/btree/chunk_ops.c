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
	}
	pageContext->hikeyChunk = NULL;
	pageContext->isInitialized = false;
}

void
page_context_set_page(BTreePageContext *pageContext, Page page)
{
	page_context_set_invalid(pageContext);
	pageContext->page = page;
}

void
page_context_set_invalid(BTreePageContext *pageContext)
{
	if (pageContext->isInitialized)
	{
		pageContext->hikeyChunk->ops->release(pageContext->hikeyChunk);
	}
	pageContext->isInitialized = false;
}

void
page_context_ensure_chunk_init(BTreePageContext *pageContext)
{
	if (!pageContext->isInitialized)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(pageContext->mctx);

		if (pageContext->hikeyChunk == NULL)
			pageContext->hikeyChunk = make_chunk_desc(pageContext->treeDesc,
													  &HiKeyChunkOps,
													  pageContext->page, 0);
		else
			pageContext->hikeyChunk->ops->init(pageContext->hikeyChunk,
											   pageContext->page, 0);

		MemoryContextSwitchTo(oldctx);
		pageContext->isInitialized = true;
	}
}
