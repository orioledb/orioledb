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
	pfree(desc);
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
	pfree(chunkBuilder);
}
