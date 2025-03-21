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
	pageContext->mctx = CurrentMemoryContext;
}

void
btree_page_context_release(BTreePageContext *pageContext)
{
	if (pageContext->hikeyChunk != NULL)
	{
		release_btree_chunk_desc(pageContext->hikeyChunk);
		pfree(pageContext->hikeyChunk);
	}
	pageContext->hikeyChunk = NULL;
	pageContext->isInitialized = false;
}

void
btree_page_context_set(BTreePageContext *pageContext, Page page)
{
	btree_page_context_invalidate(pageContext);
	pageContext->page = page;
}

void
btree_page_context_invalidate(BTreePageContext *pageContext)
{
	if (pageContext->isInitialized)
	{
		pageContext->hikeyChunk->ops->release(pageContext->hikeyChunk);
	}
	pageContext->isInitialized = false;
}

void
btree_page_context_hikey_init(BTreePageContext *pageContext)
{
	if (!pageContext->isInitialized)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(pageContext->mctx);

		if (pageContext->hikeyChunk == NULL)
			pageContext->hikeyChunk = make_btree_chunk_desc(pageContext->treeDesc,
															&BTreeHiKeyChunkOps,
															pageContext->page, 0);
		else
			pageContext->hikeyChunk->ops->init(pageContext->hikeyChunk,
											   pageContext->page, 0);

		MemoryContextSwitchTo(oldctx);
		pageContext->isInitialized = true;

		VALGRIND_CHECK_MEM_IS_DEFINED(pageContext->hikeyChunk->chunkData,
									  pageContext->hikeyChunk->chunkDataSize);
	}
}
