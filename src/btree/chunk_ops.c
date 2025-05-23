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

void
init_btree_chunk_desc(BTreeChunkDesc *desc, const BTreeChunkOps *ops,
					  BTreeDescr *treeDesc, Page page,
					  OffsetNumber chunkOffset)
{
	desc->treeDesc = treeDesc;
	*((const BTreeChunkOps **) &desc->ops) = ops;
	ops->init(desc, page, chunkOffset);
}

void
init_btree_chunk_builder(BTreeChunkBuilder *chunkBuilder,
						 const BTreeChunkOps *ops, BTreeDescr *treeDesc)
{
	chunkBuilder->treeDesc = treeDesc;
	*((const BTreeChunkOps **) &chunkBuilder->ops) = ops;
	ops->builder_init(chunkBuilder);
}

/*
 * Page context utility functions.
 */

void
btree_page_locator_init(BTreePageLocator *pageLocator, BTreeDescr *treeDesc,
						Page page)
{
	memset(pageLocator, 0, sizeof(*pageLocator));
	ASAN_UNPOISON_MEMORY_REGION(pageLocator, sizeof(*pageLocator));

	pageLocator->treeDesc = treeDesc;
	pageLocator->page = page;

	btree_page_locator_hikey_init(pageLocator, &BTreeHiKeyChunkOps);
}

void
btree_page_locator_hikey_init(BTreePageLocator *pageLocator,
							  const BTreeChunkOps *ops)
{
	init_btree_chunk_desc(&pageLocator->hikeyChunk, ops, pageLocator->treeDesc,
						  pageLocator->page, 0);

	VALGRIND_CHECK_MEM_IS_DEFINED(pageContext->hikeyChunk->chunkData,
								  pageContext->hikeyChunk->chunkDataSize);
}
