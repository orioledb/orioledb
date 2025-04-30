/*-------------------------------------------------------------------------
 *
 * tuple_chunk.c
 *		OrioleDB implementation of chunk API over tuple chunks.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/tuple_chunk.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/chunk_ops.h"
#include "btree/page_chunks.h"
#include "btree/print.h"
#include "tableam/descr.h"

#include "access/relation.h"
#include "funcapi.h"
#include "utils/lsyscache.h"

typedef struct TupleChunkTestDesc
{
	OTableDescr tableDesc;
	OIndexDescr *indexDesc;

	BTreeChunkDesc *chunk;
	BTreeChunkBuilder *chunkBuilder;

	TuplePrintOpaque printOpaque;
	StringInfoData buf;

	char		page[ORIOLEDB_BLCKSZ];
} TupleChunkTestDesc;

PG_FUNCTION_INFO_V1(test_btree_leaf_tuple_chunk);
PG_FUNCTION_INFO_V1(test_btree_leaf_tuple_chunk_builder);
PG_FUNCTION_INFO_V1(test_btree_hikey_chunk);
PG_FUNCTION_INFO_V1(test_btree_hikey_chunk_builder);

/*
 * Utility functions.
 */

static void
init_test_desc(TupleChunkTestDesc *testDesc, Oid relid)
{
	ORelOids	oids;
	OTable	   *oTable;
	Relation	rel;

	MemSet(testDesc, 0, sizeof(TupleChunkTestDesc));

	/* Get information about the table to get BTreeDescr */

	rel = relation_open(relid, AccessShareLock);
	ORelOidsSetFromRel(oids, rel);
	relation_close(rel, AccessShareLock);

	oTable = o_tables_get(oids);
	if (oTable->nindices == 0)
		elog(ERROR, "relation %d doesn't have indexes", relid);

	o_fill_tmp_table_descr(&testDesc->tableDesc, oTable);

	/* Initialize resulting BTreeChunkDesc and TupleChunkTestDesc */

	testDesc->indexDesc = GET_PRIMARY(&testDesc->tableDesc);

	pfree(oTable);
}

static void
free_test_desc(TupleChunkTestDesc *testDesc)
{
	o_free_tmp_table_descr(&testDesc->tableDesc);
	if (testDesc->chunk != NULL)
	{
		release_btree_chunk_desc(testDesc->chunk);
		pfree(testDesc->chunk);
	}
	if (testDesc->chunkBuilder != NULL)
	{
		release_btree_chunk_builder(testDesc->chunkBuilder);
		pfree(testDesc->chunkBuilder);
	}
}

static void
init_print_opaque(TupleChunkTestDesc *testDesc)
{
	TuplePrintOpaque *printOpaque = &testDesc->printOpaque;

	printOpaque->desc = testDesc->indexDesc->leafTupdesc;
	printOpaque->spec = &testDesc->indexDesc->leafSpec;
	printOpaque->keyDesc = testDesc->indexDesc->nonLeafTupdesc;
	printOpaque->keySpec = &testDesc->indexDesc->nonLeafSpec;
	printOpaque->values = (Datum *) palloc(sizeof(Datum) * printOpaque->desc->natts);
	printOpaque->nulls = (bool *) palloc(sizeof(bool) * printOpaque->desc->natts);
	printOpaque->outputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * printOpaque->desc->natts);
	printOpaque->keyOutputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * printOpaque->desc->natts);
	printOpaque->printRowVersion = false;

	for (int i = 0; i < printOpaque->desc->natts; i++)
	{
		Oid			output;
		bool		varlena;

		getTypeOutputInfo(printOpaque->desc->attrs[i].atttypid, &output, &varlena);
		fmgr_info(output, &printOpaque->outputFns[i]);
	}

	for (int i = 0; i < printOpaque->keyDesc->natts; i++)
	{
		Oid			output;
		bool		varlena;

		getTypeOutputInfo(printOpaque->keyDesc->attrs[i].atttypid, &output, &varlena);
		fmgr_info(output, &printOpaque->keyOutputFns[i]);
	}
}

static OTuple
make_otuple(TupleChunkTestDesc *testDesc, Datum *values, bool *nulls, bool isLeaf)
{
	TupleDesc	tupleDesc;
	OTupleFixedFormatSpec *spec;

	if (isLeaf)
	{
		tupleDesc = testDesc->indexDesc->leafTupdesc;
		spec = &testDesc->indexDesc->leafSpec;
	}
	else
	{
		tupleDesc = testDesc->indexDesc->nonLeafTupdesc;
		spec = &testDesc->indexDesc->nonLeafSpec;
	}

	/* Make the result OTuple */
	return o_form_tuple(tupleDesc, spec, 0, values, nulls, NULL);
}

static void
print_otuple(TupleChunkTestDesc *testDesc, OTuple tuple, bool isLeaf)
{
	TuplePrintOpaque *printOpaque = &testDesc->printOpaque;
	TupleDesc	tupleDesc;
	OTupleFixedFormatSpec *spec;
	FmgrInfo   *outputFns;

	if (isLeaf)
	{
		tupleDesc = printOpaque->desc;
		spec = printOpaque->spec;
		outputFns = printOpaque->outputFns;
	}
	else
	{
		tupleDesc = printOpaque->keyDesc;
		spec = printOpaque->keySpec;
		outputFns = printOpaque->keyOutputFns;
	}

	o_tuple_print(tupleDesc, spec, outputFns, &testDesc->buf, tuple,
				  printOpaque->values, printOpaque->nulls, false);
}

static void
test_estimate_change(TupleChunkTestDesc *testDesc, OffsetNumber itemOffset,
					 BTreeChunkOperationType operation,
					 Datum *values, bool *nulls, bool isLeaf)
{
	OTuple		tuple = {0};
	int32		estimate;

	if (operation != BTreeChunkOperationDelete)
		tuple = make_otuple(testDesc, values, nulls, isLeaf);

	estimate = testDesc->chunk->ops->estimate_change(testDesc->chunk, itemOffset,
													 operation, tuple);

	if (operation != BTreeChunkOperationDelete)
	{
		resetStringInfo(&testDesc->buf);
		print_otuple(testDesc, tuple, isLeaf);
	}

	if (operation == BTreeChunkOperationInsert)
		elog(INFO, "Estimate INSERT operation %s at position %d: %d",
			 testDesc->buf.data, itemOffset, estimate);
	else if (operation == BTreeChunkOperationUpdate)
		elog(INFO, "Estimate UPDATE operation %s at position %d: %d",
			 testDesc->buf.data, itemOffset, estimate);
	else if (operation == BTreeChunkOperationDelete)
		elog(INFO, "Estimate DELETE operation at position %d: %d",
			 itemOffset, estimate);
	else
		elog(ERROR, "Unexpected operation type: %d", operation);

	if (operation != BTreeChunkOperationDelete)
		pfree(tuple.data);
}

static void
test_perform_change(TupleChunkTestDesc *testDesc, OffsetNumber itemOffset,
					BTreeChunkOperationType operation,
					Datum *values, bool *nulls, Pointer header, bool isLeaf)
{
	OTuple		tuple = {0};

	if (operation != BTreeChunkOperationDelete)
		tuple = make_otuple(testDesc, values, nulls, isLeaf);

	testDesc->chunk->ops->perform_change(testDesc->chunk, itemOffset,
										 operation, header, tuple);

	if (operation != BTreeChunkOperationDelete)
	{
		resetStringInfo(&testDesc->buf);
		print_otuple(testDesc, tuple, isLeaf);
	}

	if (operation == BTreeChunkOperationInsert)
		elog(INFO, "Perform INSERT operation %s at position %d",
			 testDesc->buf.data, itemOffset);
	else if (operation == BTreeChunkOperationUpdate)
		elog(INFO, "Perform UPDATE operation %s at position %d",
			 testDesc->buf.data, itemOffset);
	else if (operation == BTreeChunkOperationDelete)
		elog(INFO, "Perform DELETE operation at position %d",
			 itemOffset);
	else
		elog(ERROR, "Unexpected operation type: %d", operation);

	if (operation != BTreeChunkOperationDelete)
		pfree(tuple.data);
}

static void
test_read_tuple(TupleChunkTestDesc *testDesc, OffsetNumber itemOffset,
				bool isLeaf)
{
	Pointer		header = NULL;
	OTuple		tuple = {0};
	bool		needsFree;

	testDesc->chunk->ops->read_tuple(testDesc->chunk, NULL, NULL, itemOffset,
									 &header, &tuple, &needsFree);
	Assert(!needsFree);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, tuple, isLeaf);

	if (isLeaf)
	{
		BTreeLeafTuphdr *leafHeader = (BTreeLeafTuphdr *) header;

		if (leafHeader->deleted != BTreeLeafTupleNonDeleted)
		{
			if (leafHeader->deleted == BTreeLeafTupleDeleted)
				appendStringInfoString(&testDesc->buf, ", deleted");
			else if (leafHeader->deleted == BTreeLeafTupleMovedPartitions)
				appendStringInfoString(&testDesc->buf, ", moved partitions");
			else if (leafHeader->deleted == BTreeLeafTuplePKChanged)
				appendStringInfoString(&testDesc->buf, ", PK changed");
		}
	}

	elog(INFO, "Read tuple at position %d: %s", itemOffset, testDesc->buf.data);
}

static void
test_read_hikey(TupleChunkTestDesc *testDesc, OffsetNumber itemOffset)
{
	OTuple		tuple = {0};
	bool		needsFree;

	testDesc->chunk->ops->read_tuple(testDesc->chunk, NULL, NULL, itemOffset,
									 NULL, &tuple, &needsFree);
	Assert(!needsFree);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, tuple, false);

	elog(INFO, "Read tuple at position %d: %s", itemOffset, testDesc->buf.data);
}

static void
test_cmp_tuple(TupleChunkTestDesc *testDesc, OffsetNumber itemOffset,
			   Datum *values, bool *nulls, bool isLeaf)
{
	OTuple		tuple = {0};
	int			cmp = 0;

	tuple = make_otuple(testDesc, values, nulls, isLeaf);

	testDesc->chunk->ops->cmp(testDesc->chunk, NULL, NULL, itemOffset, &tuple,
							  testDesc->chunk->ops->itemKeyType, &cmp);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, tuple, isLeaf);

	elog(INFO, "Compare tuple %s at position %d: %d", testDesc->buf.data,
		 itemOffset, cmp);

	pfree(tuple.data);
}

static void
test_search_tuple(TupleChunkTestDesc *testDesc, Datum *values, bool *nulls,
				  bool isLeaf)
{
	OTuple		tuple = {0};
	OffsetNumber itemOffset;

	tuple = make_otuple(testDesc, values, nulls, isLeaf);

	testDesc->chunk->ops->search(testDesc->chunk, NULL, NULL, &tuple,
								 testDesc->chunk->ops->itemKeyType, &itemOffset);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, tuple, isLeaf);

	elog(INFO, "Search for tuple %s: %d", testDesc->buf.data, itemOffset);

	pfree(tuple.data);
}

static void
test_builder_estimate(TupleChunkTestDesc *testDesc, Datum *values, bool *nulls,
					  bool isLeaf)
{
	OTuple		tuple = {0};
	int32		estimate;

	tuple = make_otuple(testDesc, values, nulls, isLeaf);

	estimate = testDesc->chunkBuilder->ops->builder_estimate(testDesc->chunkBuilder,
															 tuple);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, tuple, isLeaf);

	elog(INFO, "Estimate adding tuple %s to builder: %d", testDesc->buf.data,
		 estimate);

	pfree(tuple.data);
}

static void
test_builder_add(TupleChunkTestDesc *testDesc, Datum *values, bool *nulls,
				 Pointer header, bool isLeaf)
{
	const BTreeChunkOps *ops = testDesc->chunkBuilder->ops;
	OTuple		tuple;
	uint16		tupleSize;
	char	   *tupleData;
	BTreeChunkBuilderItem chunkItem = {0};

	tuple = make_otuple(testDesc, values, nulls, isLeaf);
	tupleSize = o_btree_len(testDesc->chunkBuilder->treeDesc, tuple,
							ops->itemLengthType);

	chunkItem.flags = tuple.formatFlags;
	if (header != NULL)
		chunkItem.size = ops->itemHeaderSize + MAXALIGN(tupleSize);
	else
		chunkItem.size = MAXALIGN(tupleSize);

	/* Do not pfree the tuple since we only copy pointer to the tuple */
	tupleData = palloc0(chunkItem.size);

	if (header != NULL)
	{
		Assert(ops->itemHeaderSize > 0);

		memcpy(tupleData, header, ops->itemHeaderSize);
		memcpy(tupleData + ops->itemHeaderSize, tuple.data, tupleSize);
	}
	else
		memcpy(tupleData, tuple.data, tupleSize);

	chunkItem.data = (Pointer) tupleData;
	ops->builder_add(testDesc->chunkBuilder, &chunkItem);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, tuple, isLeaf);

	elog(INFO, "Perform adding tuple %s to builder", testDesc->buf.data);

	pfree(tuple.data);
}

/*
 * Utility functions for leaf tuple chunks testing.
 */

static void
ltc_init_desc(TupleChunkTestDesc *testDesc)
{
	init_page_first_chunk(&testDesc->indexDesc->desc, testDesc->page, 0);

	testDesc->chunk = make_btree_chunk_desc(&testDesc->indexDesc->desc,
											&BTreeLeafTupleChunkOps,
											testDesc->page, 0);
}

static void
ltc_test_compact(TupleChunkTestDesc *testDesc)
{
	BTreeChunkDesc *chunk = testDesc->chunk;
	uint16		sizeBefore = chunk->chunkDataSize,
				itemsCountBefore = chunk->chunkItemsCount;

	chunk->ops->compact(chunk);

	elog(INFO, "Compaction: before: chunkDataSize %d, chunkItemsCount %d, "
		 "after: chunkDataSize %d, chunkItemsCount %d",
		 sizeBefore, itemsCountBefore,
		 chunk->chunkDataSize, chunk->chunkItemsCount);
}

static void
ltc_init_builder(TupleChunkTestDesc *testDesc)
{
	testDesc->chunkBuilder = make_btree_chunk_builder(&testDesc->indexDesc->desc,
													  &BTreeLeafTupleChunkOps);
}

/*
 * Utility functions for hikey chunks testing.
 */

static void
htc_init_desc(TupleChunkTestDesc *testDesc)
{
	init_page_first_chunk(&testDesc->indexDesc->desc, testDesc->page, 0);

	testDesc->chunk = make_btree_chunk_desc(&testDesc->indexDesc->desc,
											&BTreeHiKeyChunkOps,
											testDesc->page, 0);
	testDesc->chunk->chunkItemsCount = 0;
}

static void
htc_init_builder(TupleChunkTestDesc *testDesc)
{
	testDesc->chunkBuilder = make_btree_chunk_builder(&testDesc->indexDesc->desc,
													  &BTreeHiKeyChunkOps);
}

/*
 * Main test functions.
 */

Datum
test_btree_leaf_tuple_chunk(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TupleChunkTestDesc testDesc;
	BTreeChunkDesc *chunk;

	init_test_desc(&testDesc, relid);
	ltc_init_desc(&testDesc);
	init_print_opaque(&testDesc);
	initStringInfo(&testDesc.buf);

	chunk = testDesc.chunk;

	/* Test - Estimate INSERT operation */
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationInsert;
		Datum	   *values = (Datum[]) {Int32GetDatum(1), CStringGetTextDatum("value 1")};
		bool	   *nulls = (bool[]) {false, false};

		test_estimate_change(&testDesc, itemOffset, operation, values, nulls,
							 true);
	}

	/* Test - Perform INSERT operation */
	for (OffsetNumber itemOffset = 0; itemOffset < 50; itemOffset++)
	{
		BTreeChunkOperationType operation = BTreeChunkOperationInsert;
		int32		intVal = itemOffset + 1;
		char	   *stringVal = psprintf("value %d", intVal);

		Datum	   *values =
		(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum(stringVal)};
		bool	   *nulls = (bool[]) {false, false};
		BTreeLeafTuphdr header = {0};

		header.deleted = BTreeLeafTupleNonDeleted;
		test_perform_change(&testDesc, itemOffset, operation, values, nulls,
							(Pointer) &header, true);

		pfree(stringVal);
		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset, true);
	}

	/* Test - Compare a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		int32		intVal = itemOffset;

		Datum	   *values =
		(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum("value")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls, true);

		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Compare a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		int32		intVal = itemOffset + 1;

		Datum	   *values =
		(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum("value")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls, true);

		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Compare a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		int32		intVal = itemOffset + 2;

		Datum	   *values =
		(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum("value")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls, true);

		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Search for a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount + 2; itemOffset++)
	{
		int32		intVal = itemOffset;

		Datum	   *values =
		(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum("value")};
		bool	   *nulls = (bool[]) {false, false};

		test_search_tuple(&testDesc, values, nulls, true);

		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Estimate UPDATE operation */
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationUpdate;
		Datum	   *values = (Datum[]) {Int32GetDatum(1), CStringGetTextDatum("value 2")};
		bool	   *nulls = (bool[]) {false, false};

		test_estimate_change(&testDesc, itemOffset, operation, values, nulls,
							 true);
	}

	/* Test - Estimate DELETE operation */
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationDelete;

		test_estimate_change(&testDesc, itemOffset, operation, NULL, NULL,
							 true);
	}

	/* Test - Perform UPDATE operation */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		BTreeChunkOperationType operation = BTreeChunkOperationUpdate;
		int32		intVal = itemOffset + 100;
		char	   *stringVal = psprintf("value %d", intVal);

		Datum	   *values =
		(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum(stringVal)};
		bool	   *nulls = (bool[]) {false, false};
		BTreeLeafTuphdr header = {0};

		header.deleted = BTreeLeafTupleNonDeleted;
		test_perform_change(&testDesc, itemOffset, operation, values, nulls,
							(Pointer) &header, true);

		pfree(stringVal);
		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset, true);
	}

	/* Test - Compare a tuple */
	{
		OffsetNumber itemOffset = 0;
		Datum	   *values = (Datum[]) {Int32GetDatum(100), CStringGetTextDatum("value 100")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls, true);
	}

	/* Test - Search for a tuple */
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(100), CStringGetTextDatum("value 100")};
		bool	   *nulls = (bool[]) {false, false};

		test_search_tuple(&testDesc, values, nulls, true);
	}

	/* Test - Perform UPDATE operation with smaller values */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		BTreeChunkOperationType operation = BTreeChunkOperationUpdate;
		int32		intVal = itemOffset + 100;
		char	   *stringVal = psprintf("%d", intVal);

		Datum	   *values =
		(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum(stringVal)};
		bool	   *nulls = (bool[]) {false, false};
		BTreeLeafTuphdr header = {0};

		header.deleted = BTreeLeafTupleNonDeleted;
		test_perform_change(&testDesc, itemOffset, operation, values, nulls,
							(Pointer) &header, true);

		pfree(stringVal);
		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Perform DELETE operation */
	for (int i = 0; i < 10; i++)
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationDelete;

		test_perform_change(&testDesc, itemOffset, operation, NULL, NULL,
							NULL, true);
	}

	/* Test - Perform DELETE operation */
	for (int i = 0; i < 10; i++)
	{
		OffsetNumber itemOffset = chunk->chunkItemsCount - 1;
		BTreeChunkOperationType operation = BTreeChunkOperationDelete;

		test_perform_change(&testDesc, itemOffset, operation, NULL, NULL,
							NULL, true);
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset, true);
	}

	/* Test - Compare a tuple */
	{
		OffsetNumber itemOffset = 0;
		Datum	   *values = (Datum[]) {Int32GetDatum(100), CStringGetTextDatum("value 100")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls, true);
	}

	/* Test - Search for a tuple */
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(100), CStringGetTextDatum("value 100")};
		bool	   *nulls = (bool[]) {false, false};

		test_search_tuple(&testDesc, values, nulls, true);
	}

	/* Test - Compaction */
	ltc_test_compact(&testDesc);

	/* Test - Perform soft delete */
	for (OffsetNumber itemOffset = 10; itemOffset < 20; itemOffset++)
	{
		BTreeChunkOperationType operation = BTreeChunkOperationUpdate;
		int32		intVal = itemOffset + 100;
		char	   *stringVal = psprintf("%d", intVal);

		Datum	   *values =
		(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum(stringVal)};
		bool	   *nulls = (bool[]) {false, false};
		BTreeLeafTuphdr header = {0};

		header.deleted = BTreeLeafTupleDeleted;
		test_perform_change(&testDesc, itemOffset, operation, values, nulls,
							(Pointer) &header, true);

		pfree(stringVal);
		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset, true);
	}

	/* Test - Compaction */
	ltc_test_compact(&testDesc);

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset, true);
	}

	free_test_desc(&testDesc);

	PG_RETURN_VOID();
}

Datum
test_btree_leaf_tuple_chunk_builder(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TupleChunkTestDesc testDesc;

	init_test_desc(&testDesc, relid);
	ltc_init_desc(&testDesc);
	ltc_init_builder(&testDesc);
	init_print_opaque(&testDesc);
	initStringInfo(&testDesc.buf);

	/* Test - Estimate new tuple */
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(1), CStringGetTextDatum("value 1")};
		bool	   *nulls = (bool[]) {false, false};

		test_builder_estimate(&testDesc, values, nulls, true);
	}

	/* Test - Add new tuple */
	for (OffsetNumber itemOffset = 0; itemOffset < 50; itemOffset++)
	{
		int32		intVal = itemOffset + 1;
		char	   *stringVal = psprintf("value %d", intVal);

		Datum	   *values =
		(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum(stringVal)};
		bool	   *nulls = (bool[]) {false, false};
		BTreeLeafTuphdr header = {0};

		test_builder_add(&testDesc, values, nulls, (Pointer) &header, true);

		pfree(stringVal);
		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Finish building */
	{
		testDesc.chunkBuilder->ops->builder_finish(testDesc.chunkBuilder,
												   testDesc.chunk);
		elog(INFO, "Finished building tuple chunk");
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < testDesc.chunk->chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset, true);
	}

	free_test_desc(&testDesc);

	PG_RETURN_VOID();
}

Datum
test_btree_hikey_chunk(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TupleChunkTestDesc testDesc;
	BTreeChunkDesc *chunk;

	init_test_desc(&testDesc, relid);
	htc_init_desc(&testDesc);
	init_print_opaque(&testDesc);
	initStringInfo(&testDesc.buf);

	chunk = testDesc.chunk;

	/* Test - Estimate INSERT operation */
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationInsert;
		Datum	   *values = (Datum[]) {Int32GetDatum(1)};
		bool	   *nulls = (bool[]) {false};

		test_estimate_change(&testDesc, itemOffset, operation, values, nulls,
							 false);
	}

	/* Test - Perform INSERT operation */
	for (OffsetNumber itemOffset = 0; itemOffset < BTREE_PAGE_MAX_CHUNKS; itemOffset++)
	{
		BTreeChunkOperationType operation = BTreeChunkOperationInsert;
		Datum	   *values = (Datum[]) {Int32GetDatum(itemOffset + 1)};
		bool	   *nulls = (bool[]) {false};

		test_perform_change(&testDesc, itemOffset, operation, values, nulls,
							NULL, false);
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		test_read_hikey(&testDesc, itemOffset);
	}

	/* Test - Compare a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(itemOffset)};
		bool	   *nulls = (bool[]) {false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls, false);
	}

	/* Test - Compare a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(itemOffset + 1)};
		bool	   *nulls = (bool[]) {false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls, false);
	}

	/* Test - Search for a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount + 2; itemOffset++)
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(itemOffset)};
		bool	   *nulls = (bool[]) {false};

		test_search_tuple(&testDesc, values, nulls, false);
	}

	/* Test - Estimate UPDATE operation */
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationUpdate;
		Datum	   *values = (Datum[]) {Int32GetDatum(1)};
		bool	   *nulls = (bool[]) {false};

		test_estimate_change(&testDesc, itemOffset, operation, values, nulls,
							 false);
	}

	/* Test - Estimate DELETE operation */
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationDelete;

		test_estimate_change(&testDesc, itemOffset, operation, NULL, NULL,
							 false);
	}

	/* Test - Perform UPDATE operation */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		BTreeChunkOperationType operation = BTreeChunkOperationUpdate;

		Datum	   *values = (Datum[]) {Int32GetDatum(itemOffset + 100)};
		bool	   *nulls = (bool[]) {false};

		test_perform_change(&testDesc, itemOffset, operation, values, nulls,
							NULL, false);
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		test_read_hikey(&testDesc, itemOffset);
	}

	/* Test - Compare a tuple */
	{
		OffsetNumber itemOffset = 0;
		Datum	   *values = (Datum[]) {Int32GetDatum(100)};
		bool	   *nulls = (bool[]) {false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls, false);
	}

	/* Test - Search for a tuple */
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(100)};
		bool	   *nulls = (bool[]) {false};

		test_search_tuple(&testDesc, values, nulls, false);
	}

	/* Test - Perform DELETE operation */
	for (int i = 0; i < 10; i++)
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationDelete;

		test_perform_change(&testDesc, itemOffset, operation, NULL, NULL,
							NULL, false);
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		test_read_hikey(&testDesc, itemOffset);
	}

	/* Test - Compare a tuple */
	{
		OffsetNumber itemOffset = 0;
		Datum	   *values = (Datum[]) {Int32GetDatum(100)};
		bool	   *nulls = (bool[]) {false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls, false);
	}

	/* Test - Search for a tuple */
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(100)};
		bool	   *nulls = (bool[]) {false};

		test_search_tuple(&testDesc, values, nulls, false);
	}

	free_test_desc(&testDesc);

	PG_RETURN_VOID();
}

Datum
test_btree_hikey_chunk_builder(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TupleChunkTestDesc testDesc;

	init_test_desc(&testDesc, relid);
	htc_init_desc(&testDesc);
	htc_init_builder(&testDesc);
	init_print_opaque(&testDesc);
	initStringInfo(&testDesc.buf);

	/* Test - Estimate new tuple */
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(1)};
		bool	   *nulls = (bool[]) {false};

		test_builder_estimate(&testDesc, values, nulls, false);
	}

	/* Test - Add new tuple */
	for (OffsetNumber itemOffset = 0; itemOffset < BTREE_PAGE_MAX_CHUNKS; itemOffset++)
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(itemOffset + 1)};
		bool	   *nulls = (bool[]) {false};

		test_builder_add(&testDesc, values, nulls, NULL, false);
	}

	/* Test - Finish building */
	{
		testDesc.chunkBuilder->ops->builder_finish(testDesc.chunkBuilder,
												   testDesc.chunk);
		elog(INFO, "Finished building tuple chunk");
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		 itemOffset < testDesc.chunk->chunkItemsCount; itemOffset++)
	{
		test_read_hikey(&testDesc, itemOffset);
	}

	free_test_desc(&testDesc);

	PG_RETURN_VOID();
}
