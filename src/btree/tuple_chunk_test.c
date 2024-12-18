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

#include "btree/page_chunks.h"
#include "btree/print.h"
#include "btree/tuple_chunk.h"
#include "tableam/descr.h"

#include "access/relation.h"
#include "funcapi.h"
#include "utils/lsyscache.h"

typedef struct TupleChunkTestDesc
{
	OTableDescr tableDesc;
	OIndexDescr *indexDesc;

	BTreeChunkDesc chunkDesc;
	BTreeChunkBuilderData builderData;

	TuplePrintOpaque printOpaque;
	StringInfoData buf;

	char		page[ORIOLEDB_BLCKSZ];
} TupleChunkTestDesc;

PG_FUNCTION_INFO_V1(test_tuple_chunk_changes);
PG_FUNCTION_INFO_V1(test_tuple_chunk_builder);

static void init_test_desc(TupleChunkTestDesc *testDesc, Oid relid);
static void init_chunk_desc(TupleChunkTestDesc *testDesc,
							BTreeChunkType chunkType);
static void init_chunk_builder(TupleChunkTestDesc *testDesc,
							   BTreeChunkType chunkType);
static void free_test_desc(TupleChunkTestDesc *testDesc);

static void init_print_opaque(TupleChunkTestDesc *testDesc);

static void test_estimate_change(TupleChunkTestDesc *testDesc,
								 OffsetNumber itemOffset,
								 BTreeChunkOperationType operation,
								 Datum *values, bool *nulls);
static void test_perform_change(TupleChunkTestDesc *testDesc,
								OffsetNumber itemOffset,
								BTreeChunkOperationType operation,
								Datum *values, bool *nulls);
static void test_read_tuple(TupleChunkTestDesc *testDesc,
							OffsetNumber itemOffset);
static void test_cmp_tuple(TupleChunkTestDesc *testDesc,
						   OffsetNumber itemOffset,
						   Datum *values, bool *nulls);
static void test_search_tuple(TupleChunkTestDesc *testDesc,
							  Datum *values, bool *nulls);

static void test_builder_estimate(TupleChunkTestDesc *testDesc,
								  Datum *values, bool *nulls);
static void test_builder_add(TupleChunkTestDesc *testDesc,
							 Datum *values, bool *nulls, uint64 downlink);

static OTuple make_otuple(TupleChunkTestDesc *testDesc,
						  BTreeChunkType chunkType,
						  Datum *values, bool *nulls);
static void print_otuple(TupleChunkTestDesc *testDesc,
						 BTreeChunkType chunkType, OTuple tuple);

Datum
test_tuple_chunk_changes(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TupleChunkTestDesc testDesc;

	init_test_desc(&testDesc, relid);
	init_chunk_desc(&testDesc, BTreeChunkLeaf);
	init_print_opaque(&testDesc);
	initStringInfo(&testDesc.buf);

	/* Test - Estimate INSERT operation */
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationInsert;
		Datum	   *values = (Datum[]) {Int32GetDatum(1), CStringGetTextDatum("value 1")};
		bool	   *nulls = (bool[]) {false, false};

		test_estimate_change(&testDesc, itemOffset, operation, values, nulls);
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

		test_perform_change(&testDesc, itemOffset, operation, values, nulls);

		pfree(stringVal);
		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		itemOffset < testDesc.chunkDesc.chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset);
	}

	/* Test - Compare a tuple */
	for (OffsetNumber itemOffset = 0;
		itemOffset < testDesc.chunkDesc.chunkItemsCount; itemOffset++)
	{
		int32		intVal = itemOffset;

		Datum	   *values =
			(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum("value")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls);

		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Compare a tuple */
	for (OffsetNumber itemOffset = 0;
		itemOffset < testDesc.chunkDesc.chunkItemsCount; itemOffset++)
	{
		int32		intVal = itemOffset + 1;

		Datum	   *values =
			(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum("value")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls);

		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Compare a tuple */
	for (OffsetNumber itemOffset = 0;
		itemOffset < testDesc.chunkDesc.chunkItemsCount; itemOffset++)
	{
		int32		intVal = itemOffset + 2;

		Datum	   *values =
			(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum("value")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls);

		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Search for a tuple */
	for (OffsetNumber itemOffset = 0;
		itemOffset < testDesc.chunkDesc.chunkItemsCount + 2; itemOffset++)
	{
		int32		intVal = itemOffset;

		Datum	   *values =
			(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum("value")};
		bool	   *nulls = (bool[]) {false, false};

		test_search_tuple(&testDesc, values, nulls);

		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Estimate UPDATE operation */
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationUpdate;
		Datum	   *values = (Datum[]) {Int32GetDatum(1), CStringGetTextDatum("value 2")};
		bool	   *nulls = (bool[]) {false, false};

		test_estimate_change(&testDesc, itemOffset, operation, values, nulls);
	}

	/* Test - Estimate DELETE operation */
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationDelete;

		test_estimate_change(&testDesc, itemOffset, operation, NULL, NULL);
	}

	/* Test - Perform UPDATE operation */
	for (OffsetNumber itemOffset = 0;
		itemOffset < testDesc.chunkDesc.chunkItemsCount; itemOffset++)
	{
		BTreeChunkOperationType operation = BTreeChunkOperationUpdate;
		int32		intVal = itemOffset + 100;
		char	   *stringVal = psprintf("value %d", intVal);

		Datum	   *values =
			(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum(stringVal)};
		bool	   *nulls = (bool[]) {false, false};

		test_perform_change(&testDesc, itemOffset, operation, values, nulls);

		pfree(stringVal);
		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		itemOffset < testDesc.chunkDesc.chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset);
	}

	/* Test - Compare a tuple */
	{
		OffsetNumber itemOffset = 0;
		Datum	   *values = (Datum[]) {Int32GetDatum(100), CStringGetTextDatum("value 100")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls);
	}

	/* Test - Search for a tuple */
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(100), CStringGetTextDatum("value 100")};
		bool	   *nulls = (bool[]) {false, false};

		test_search_tuple(&testDesc, values, nulls);
	}

	/* Test - Perform DELETE operation */
	for (int i = 0; i < 10; i++)
	{
		OffsetNumber itemOffset = 0;
		BTreeChunkOperationType operation = BTreeChunkOperationDelete;

		test_perform_change(&testDesc, itemOffset, operation, NULL, NULL);
	}

	/* Test - Perform DELETE operation */
	for (int i = 0; i < 10; i++)
	{
		OffsetNumber itemOffset = testDesc.chunkDesc.chunkItemsCount - 1;
		BTreeChunkOperationType operation = BTreeChunkOperationDelete;

		test_perform_change(&testDesc, itemOffset, operation, NULL, NULL);
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		itemOffset < testDesc.chunkDesc.chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset);
	}

	/* Test - Compare a tuple */
	{
		OffsetNumber itemOffset = 0;
		Datum	   *values = (Datum[]) {Int32GetDatum(100), CStringGetTextDatum("value 100")};
		bool	   *nulls = (bool[]) {false, false};

		test_cmp_tuple(&testDesc, itemOffset, values, nulls);
	}

	/* Test - Search for a tuple */
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(100), CStringGetTextDatum("value 100")};
		bool	   *nulls = (bool[]) {false, false};

		test_search_tuple(&testDesc, values, nulls);
	}

	free_test_desc(&testDesc);

	PG_RETURN_VOID();
}

Datum
test_tuple_chunk_builder(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TupleChunkTestDesc testDesc;

	init_test_desc(&testDesc, relid);
	init_chunk_builder(&testDesc, BTreeChunkLeaf);
	init_print_opaque(&testDesc);
	initStringInfo(&testDesc.buf);

	/* Test - Estimate new tuple */
	{
		Datum	   *values = (Datum[]) {Int32GetDatum(1), CStringGetTextDatum("value 1")};
		bool	   *nulls = (bool[]) {false, false};

		test_builder_estimate(&testDesc, values, nulls);
	}

	/* Test - Add new tuple */
	for (OffsetNumber itemOffset = 0; itemOffset < 50; itemOffset++)
	{
		int32		intVal = itemOffset + 1;
		char	   *stringVal = psprintf("value %d", intVal);

		Datum	   *values =
			(Datum[]) {Int32GetDatum(intVal), CStringGetTextDatum(stringVal)};
		bool	   *nulls = (bool[]) {false, false};

		test_builder_add(&testDesc, values, nulls, 0);

		pfree(stringVal);
		pfree(DatumGetPointer(values[1]));
	}

	/* Test - Finish building */
	{
		tuple_chunk_builder_finish(&testDesc.builderData, &testDesc.chunkDesc);
		elog(INFO, "Finished building tuple chunk");
	}

	/* Test - Read a tuple */
	for (OffsetNumber itemOffset = 0;
		itemOffset < testDesc.chunkDesc.chunkItemsCount; itemOffset++)
	{
		test_read_tuple(&testDesc, itemOffset);
	}

	free_test_desc(&testDesc);

	PG_RETURN_VOID();
}

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

	testDesc->indexDesc = testDesc->tableDesc.indices[0];

	pfree(oTable);
}

static void
init_chunk_desc(TupleChunkTestDesc *testDesc, BTreeChunkType chunkType)
{
	testDesc->chunkDesc.treeDesc = &testDesc->indexDesc->desc;

	init_page_first_chunk(testDesc->chunkDesc.treeDesc, testDesc->page, 0);
	init_tuple_chunk(&testDesc->chunkDesc, testDesc->page, 0);

	testDesc->chunkDesc.chunkType = chunkType;
}

static void
init_chunk_builder(TupleChunkTestDesc *testDesc, BTreeChunkType chunkType)
{
	tuple_chunk_builder_init(&testDesc->builderData,
							 &testDesc->indexDesc->desc, chunkType);

	testDesc->builderData.chunkData = (Pointer) testDesc->page;
	testDesc->builderData.chunkDataMaxSize = ORIOLEDB_BLCKSZ;
}

static void
free_test_desc(TupleChunkTestDesc *testDesc)
{
	o_free_tmp_table_descr(&testDesc->tableDesc);
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

static void
test_estimate_change(TupleChunkTestDesc *testDesc, OffsetNumber itemOffset,
					 BTreeChunkOperationType operation,
					 Datum *values, bool *nulls)
{
	OTuple		tuple = {0};
	int32		estimate;

	if (operation != BTreeChunkOperationDelete)
		tuple = make_otuple(testDesc, testDesc->chunkDesc.chunkType,
							values, nulls);

	estimate = tuple_chunk_estimate_change(&testDesc->chunkDesc, itemOffset,
										   operation, tuple);

	if (operation != BTreeChunkOperationDelete)
	{
		resetStringInfo(&testDesc->buf);
		print_otuple(testDesc, testDesc->chunkDesc.chunkType, tuple);
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
					Datum *values, bool *nulls)
{
	OTuple		tuple = {0};

	if (operation != BTreeChunkOperationDelete)
		tuple = make_otuple(testDesc, testDesc->chunkDesc.chunkType,
							values, nulls);

	if (testDesc->chunkDesc.chunkType == BTreeChunkLeaf)
	{
		BTreeLeafTuphdr header = {0};

		tuple_chunk_perform_change(&testDesc->chunkDesc, itemOffset, operation,
								   (Pointer) &header, tuple);
	}
	else
	{
		BTreeNonLeafTuphdr header = {0};

		tuple_chunk_perform_change(&testDesc->chunkDesc, itemOffset, operation,
								   (Pointer) &header, tuple);
	}

	if (operation != BTreeChunkOperationDelete)
	{
		resetStringInfo(&testDesc->buf);
		print_otuple(testDesc, testDesc->chunkDesc.chunkType, tuple);
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
test_read_tuple(TupleChunkTestDesc *testDesc, OffsetNumber itemOffset)
{
	Pointer		header = NULL;
	OTuple		tuple = {0};
	bool		needsFree;

	tuple_chunk_read_tuple(&testDesc->chunkDesc, NULL, NULL, itemOffset,
						   &header, &tuple, &needsFree);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, testDesc->chunkDesc.chunkType, tuple);

	elog(INFO, "Read tuple at position %d: %s", itemOffset, testDesc->buf.data);
}

static void
test_cmp_tuple(TupleChunkTestDesc *testDesc, OffsetNumber itemOffset,
			   Datum *values, bool *nulls)
{
	OTuple		tuple = {0};
	int			cmp = 0;

	tuple = make_otuple(testDesc, testDesc->chunkDesc.chunkType,
						values, nulls);

	tuple_chunk_cmp(&testDesc->chunkDesc, NULL, NULL, itemOffset, &tuple,
					testDesc->chunkDesc.chunkType == BTreeChunkLeaf ?
					BTreeKeyLeafTuple : BTreeKeyNonLeafKey,
					&cmp);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, testDesc->chunkDesc.chunkType, tuple);

	elog(INFO, "Compare tuple %s at position %d: %d", testDesc->buf.data,
		 itemOffset, cmp);

	pfree(tuple.data);
}

static void
test_search_tuple(TupleChunkTestDesc *testDesc, Datum *values, bool *nulls)
{
	OTuple		tuple = {0};
	OffsetNumber itemOffset;

	tuple = make_otuple(testDesc, testDesc->chunkDesc.chunkType,
						values, nulls);

	tuple_chunk_search(&testDesc->chunkDesc, NULL, NULL, &tuple,
					   testDesc->chunkDesc.chunkType == BTreeChunkLeaf ?
					   BTreeKeyLeafTuple : BTreeKeyNonLeafKey,
					   &itemOffset);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, testDesc->chunkDesc.chunkType, tuple);

	elog(INFO, "Search for tuple %s: %d", testDesc->buf.data, itemOffset);

	pfree(tuple.data);
}

static void
test_builder_estimate(TupleChunkTestDesc *testDesc, Datum *values, bool *nulls)
{
	OTuple		tuple = {0};
	int32		estimate;

	tuple = make_otuple(testDesc, testDesc->builderData.chunkType,
						values, nulls);

	estimate = tuple_chunk_builder_estimate(&testDesc->builderData, tuple);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, testDesc->builderData.chunkType, tuple);

	elog(INFO, "Estimate adding tuple %s to builder: %d", testDesc->buf.data,
		 estimate);

	pfree(tuple.data);
}

static void
test_builder_add(TupleChunkTestDesc *testDesc, Datum *values, bool *nulls,
				 uint64 downlink)
{
	OTuple		tuple = {0};

	tuple = make_otuple(testDesc, testDesc->builderData.chunkType,
						values, nulls);

	tuple_chunk_builder_add(&testDesc->builderData, tuple, downlink);

	resetStringInfo(&testDesc->buf);
	print_otuple(testDesc, testDesc->builderData.chunkType, tuple);

	elog(INFO, "Perform adding tuple %s to builder", testDesc->buf.data);

	pfree(tuple.data);
}

static OTuple
make_otuple(TupleChunkTestDesc *testDesc, BTreeChunkType chunkType,
			Datum *values, bool *nulls)
{
	TupleDesc	tupleDesc;
	OTupleFixedFormatSpec *spec;

	if (chunkType == BTreeChunkLeaf)
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
	return o_form_tuple(tupleDesc, spec, 0, values, nulls);
}

static void
print_otuple(TupleChunkTestDesc *testDesc, BTreeChunkType chunkType,
			 OTuple tuple)
{
	TuplePrintOpaque *printOpaque = &testDesc->printOpaque;
	TupleDesc	tupleDesc;
	OTupleFixedFormatSpec *spec;
	FmgrInfo   *outputFns;

	if (chunkType == BTreeChunkLeaf)
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
