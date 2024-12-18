/*-------------------------------------------------------------------------
 *
 * tuple_chunk.c
 *		OrioleDB implementation of chunk API over tuple chunks.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/tuple_chunk.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "btree/page_chunks.h"
#include "btree/tuple_chunk.h"
#include "tableam/descr.h"

#include "access/relation.h"

typedef struct TupleChunkTestDesc
{
	BTreeChunkDesc chunkDesc;

	char		page[ORIOLEDB_BLCKSZ];
}			TupleChunkTestDesc;

/*
 * tuple_chunk tests:
 *   - Initialize BTreeDescr and BTreeChunkDesc
 *   - Add items: estimate change, perform change
 *   - Update items: estimate change, perform change
 *   - Delete items: estimate change, performa change
 *   - Search items: cmp, search
 *   - Initialize builder
 *   - Estimate builder size
 *   - Add items to the builder
 *   - Finalize the builder
 */

PG_FUNCTION_INFO_V1(test_tuple_chunk_initialize);

Datum
test_tuple_chunk_initialize(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	ORelOids	oids;
	Relation	rel;
	OTableDescr *desc;
	TupleChunkTestDesc *testDesc;
	MemoryContext ctx;

	/* Get information about the table to get BTreeDescr */

	rel = relation_open(relid, AccessShareLock);
	ORelOidsSetFromRel(oids, rel);
	relation_close(rel, AccessShareLock);

	desc = o_fetch_table_descr(oids);
	if (desc->nIndices == 0)
		elog(ERROR, "relation %d doesn't have indexes", relid);

	ctx = MemoryContextSwitchTo(desc->indices[0]->index_mctx);

	testDesc = palloc0(sizeof(TupleChunkTestDesc));
	testDesc->chunkDesc.treeDesc = desc->indices[0]->desc;

	init_page_first_chunk(testDesc->chunkDesc.treeDesc, testDesc->page, 0);
	init_tuple_chunk(&testDesc->chunkDesc, testDesc->page, 0);

	MemoryContextSwitchTo(ctx);

	PG_RETURN_POINTER(testDesc);
}
