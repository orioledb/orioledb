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

#include "orioledb.h"

#include "btree/page_chunks.h"
#include "btree/tuple_chunk.h"
#include "tableam/descr.h"

#include "access/relation.h"
#include "funcapi.h"

typedef struct TupleChunkTestDesc
{
	OTableDescr tableDesc;
	OIndexDescr *indexDesc;
	BTreeChunkDesc chunkDesc;

	char		page[ORIOLEDB_BLCKSZ];
} TupleChunkTestDesc;

/*
 * This variable is used across all test functions.  To use the variable it is
 * necessary to call test_tuple_chunk_initialize() first.
 */
static TupleChunkTestDesc *testDesc = NULL;

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
PG_FUNCTION_INFO_V1(test_tuple_chunk_estimate_change);
PG_FUNCTION_INFO_V1(test_tuple_chunk_perform_change);
PG_FUNCTION_INFO_V1(test_tuple_chunk_cmp);
PG_FUNCTION_INFO_V1(test_tuple_chunk_search);
PG_FUNCTION_INFO_V1(test_tuple_chunk_read_tuple);
PG_FUNCTION_INFO_V1(test_tuple_chunk_free);

static OTuple make_otuple(HeapTupleHeader rec);
static HeapTuple make_heap_tuple(TupleDesc recDesc, OTuple tuple);

Datum
test_tuple_chunk_initialize(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	BTreeChunkType chunkType = PG_GETARG_INT16(1);
	ORelOids	oids;
	OTable	   *oTable;
	Relation	rel;
	MemoryContext ctx;

	/* Get information about the table to get BTreeDescr */

	rel = relation_open(relid, AccessShareLock);
	ORelOidsSetFromRel(oids, rel);
	relation_close(rel, AccessShareLock);

	oTable = o_tables_get(oids);
	if (oTable->nindices == 0)
		elog(ERROR, "relation %d doesn't have indexes", relid);

	ctx = MemoryContextSwitchTo(TopMemoryContext);

	testDesc = palloc0(sizeof(TupleChunkTestDesc));
	o_fill_tmp_table_descr(&testDesc->tableDesc, oTable);

	/* Initialize resulting BTreeChunkDesc and TupleChunkTestDesc */

	testDesc->indexDesc = testDesc->tableDesc.indices[0];
	testDesc->chunkDesc.treeDesc = &testDesc->indexDesc->desc;

	init_page_first_chunk(testDesc->chunkDesc.treeDesc, testDesc->page, 0);
	init_tuple_chunk(&testDesc->chunkDesc, testDesc->page, 0);

	testDesc->chunkDesc.chunkType = chunkType;

	MemoryContextSwitchTo(ctx);

	pfree(oTable);

	PG_RETURN_VOID();
}

Datum
test_tuple_chunk_estimate_change(PG_FUNCTION_ARGS)
{
	OffsetNumber itemOffset = PG_GETARG_INT16(0);
	BTreeChunkOperationType operation = PG_GETARG_INT16(1);
	OTuple		tuple = {0};
	int32		res = 0;

	if (testDesc == NULL)
		elog(LOG, "TupleChunkTestDesc is not initialized");

	if (!PG_ARGISNULL(2))
	{
		HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(2);

		tuple = make_otuple(rec);

		PG_FREE_IF_COPY(rec, 2);
	}

	res = tuple_chunk_estimate_change(&testDesc->chunkDesc, itemOffset,
									  operation, tuple);

	if (tuple.data != NULL)
		pfree(tuple.data);

	PG_RETURN_INT32(res);
}

Datum
test_tuple_chunk_perform_change(PG_FUNCTION_ARGS)
{
	OffsetNumber itemOffset = PG_GETARG_INT16(0);
	BTreeChunkOperationType operation = PG_GETARG_INT16(1);
	OTuple		tuple = {0};

	if (testDesc == NULL)
		elog(ERROR, "TupleChunkTestDesc is not initialized");

	if (itemOffset >= testDesc->chunkDesc.chunkItemsCount &&
		operation != BTreeChunkOperationInsert)
		elog(ERROR, "itemOffset %d is greater than chunkItemsCount %d",
			 itemOffset, testDesc->chunkDesc.chunkItemsCount);

	if (!PG_ARGISNULL(2))
	{
		HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(2);

		tuple = make_otuple(rec);

		PG_FREE_IF_COPY(rec, 2);
	}

	if (testDesc->chunkDesc.chunkType == BTreeChunkLeaf)
	{
		BTreeLeafTuphdr header = {0};

		tuple_chunk_perform_change(&testDesc->chunkDesc, itemOffset,
								   operation, (Pointer) &header, tuple);
	}
	else
	{
		BTreeNonLeafTuphdr header = {0};

		tuple_chunk_perform_change(&testDesc->chunkDesc, itemOffset,
								   operation, (Pointer) &header, tuple);
	}

	if (tuple.data != NULL)
		pfree(tuple.data);

	PG_RETURN_VOID();
}

Datum
test_tuple_chunk_cmp(PG_FUNCTION_ARGS)
{
	OffsetNumber itemOffset = PG_GETARG_INT16(0);
	HeapTupleHeader key;
	OTuple		keyTuple;
	bool		cmpResult;
	int			cmp = 0;

	if (testDesc == NULL)
		elog(ERROR, "TupleChunkTestDesc is not initialized");

	if (itemOffset >= testDesc->chunkDesc.chunkItemsCount)
		elog(ERROR, "itemOffset %d is greater than chunkItemsCount %d",
			 itemOffset, testDesc->chunkDesc.chunkItemsCount);

	key = PG_GETARG_HEAPTUPLEHEADER(1);

	keyTuple = make_otuple(key);

	cmpResult = tuple_chunk_cmp(&testDesc->chunkDesc, NULL, NULL, itemOffset,
								&keyTuple,
								testDesc->chunkDesc.chunkType == BTreeChunkLeaf ?
								BTreeKeyLeafTuple : BTreeKeyNonLeafKey,
								&cmp);

	PG_FREE_IF_COPY(key, 1);
	pfree(keyTuple.data);

	if (!cmpResult)
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(cmp);
}

Datum
test_tuple_chunk_search(PG_FUNCTION_ARGS)
{
	HeapTupleHeader key;
	OTuple		keyTuple;
	bool		searchResult;
	OffsetNumber itemOffset;

	if (testDesc == NULL)
		elog(ERROR, "TupleChunkTestDesc is not initialized");

	key = PG_GETARG_HEAPTUPLEHEADER(0);

	keyTuple = make_otuple(key);

	searchResult = tuple_chunk_search(&testDesc->chunkDesc, NULL, NULL, &keyTuple,
									  testDesc->chunkDesc.chunkType == BTreeChunkLeaf ?
									  BTreeKeyLeafTuple : BTreeKeyNonLeafKey,
									  &itemOffset);

	PG_FREE_IF_COPY(key, 0);
	pfree(keyTuple.data);

	if (!searchResult)
		PG_RETURN_NULL();
	else
		PG_RETURN_INT16(itemOffset);
}

Datum
test_tuple_chunk_read_tuple(PG_FUNCTION_ARGS)
{
	OffsetNumber itemOffset = PG_GETARG_INT16(0);
	OTuple		tuple = {0};
	Pointer    *header = NULL;
	bool		needsFree = false;
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (testDesc == NULL)
		elog(ERROR, "TupleChunkTestDesc is not initialized");

	if (itemOffset >= testDesc->chunkDesc.chunkItemsCount)
		elog(ERROR, "itemOffset %d is greater than chunkItemsCount %d",
			 itemOffset, testDesc->chunkDesc.chunkItemsCount);

	if (!tuple_chunk_read_tuple(&testDesc->chunkDesc, NULL, NULL, itemOffset,
								(Pointer *) &header, &tuple, &needsFree))
		PG_RETURN_VOID();

	PG_RETURN_DATUM(HeapTupleGetDatum(make_heap_tuple(tupdesc, tuple)));
}

Datum
test_tuple_chunk_free(PG_FUNCTION_ARGS)
{
	o_free_tmp_table_descr(&testDesc->tableDesc);
	pfree(testDesc);

	testDesc = NULL;

	PG_RETURN_VOID();
}

static OTuple
make_otuple(HeapTupleHeader rec)
{
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	recDesc;
	HeapTupleData tuple;
	TupleDesc	tupleDesc;
	OTupleFixedFormatSpec *spec;
	Datum	   *values;
	bool	   *nulls;
	OTuple		result;

	Assert(testDesc != NULL);

	if (testDesc->chunkDesc.chunkType == BTreeChunkLeaf)
	{
		tupleDesc = testDesc->indexDesc->leafTupdesc;
		spec = &testDesc->indexDesc->leafSpec;
	}
	else
	{
		tupleDesc = testDesc->indexDesc->nonLeafTupdesc;
		spec = &testDesc->indexDesc->nonLeafSpec;
	}

	/* Extract type info from the tuple itself */
	tupType = HeapTupleHeaderGetTypeId(rec);
	tupTypmod = HeapTupleHeaderGetTypMod(rec);

	recDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	if (recDesc->natts != tupleDesc->natts)
		elog(ERROR, "unexpected number of attributes %d, expected %d",
			 recDesc->natts, tupleDesc->natts);

	/* Build a temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;

	values = (Datum *) palloc(recDesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(recDesc->natts * sizeof(bool));

	/* Break down the tuple into fields */
	heap_deform_tuple(&tuple, recDesc, values, nulls);

	/* Make the result OTuple */
	result = o_form_tuple(tupleDesc, spec, 0, values, nulls);

	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(recDesc);

	return result;
}

static HeapTuple
make_heap_tuple(TupleDesc recDesc, OTuple tuple)
{
	TupleDesc	tupleDesc;
	OTupleFixedFormatSpec *spec;
	Datum	   *values;
	bool	   *nulls;
	HeapTuple	heapTuple;

	Assert(testDesc != NULL);

	if (testDesc->chunkDesc.chunkType == BTreeChunkLeaf)
	{
		tupleDesc = testDesc->indexDesc->leafTupdesc;
		spec = &testDesc->indexDesc->leafSpec;
	}
	else
	{
		tupleDesc = testDesc->indexDesc->nonLeafTupdesc;
		spec = &testDesc->indexDesc->nonLeafSpec;
	}

	if (recDesc->natts != tupleDesc->natts)
		elog(ERROR, "unexpected number of attributes %d, expected %d",
			 recDesc->natts, tupleDesc->natts);

	values = palloc(recDesc->natts * sizeof(Datum));
	nulls = palloc(recDesc->natts * sizeof(bool));

	o_deform_tuple(tupleDesc, spec, tuple, values, nulls);

	heapTuple = heap_form_tuple(recDesc, values, nulls);

	pfree(values);
	pfree(nulls);

	return heapTuple;
}
