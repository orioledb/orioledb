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
