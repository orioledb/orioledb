/*-------------------------------------------------------------------------
 *
 * bitmap_scan.c
 *		Routines for bitmap scan of orioledb table
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/bitmap_scan.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "btree/iterator.h"
#include "btree/page_chunks.h"
#include "tableam/bitmap_scan.h"
#include "tableam/index_scan.h"
#include "tuple/slot.h"

#include "access/relation.h"
#include "access/table.h"
#include "catalog/pg_type.h"
#include "executor/nodeIndexscan.h"
#include "lib/rbtree.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"

#include <math.h>

typedef struct OBitmapScan
{
	OTableDescr *tbl_desc;
	ScanState  *ss;
	OSnapshot	oSnapshot;
	MemoryContext cxt;
	RBTree	   *saved_bitmap;
	Oid			typeoid;
	BTreeSeqScan *seq_scan;
} OBitmapScan;

static bool o_bitmap_is_range_valid(OTuple low, OTuple high, void *arg);
static bool o_bitmap_get_next_key(OFixedKey *key, bool inclusive, void *arg);

BTreeSeqScanCallbacks bitmap_seq_scan_callbacks = {
	.isRangeValid = o_bitmap_is_range_valid,
	.getNextKey = o_bitmap_get_next_key
};

#define UINT64_HIGH_BIT (UINT64CONST(1) << 63)

static uint64
int64_to_uint64(int64 val)
{
	if (val >= 0)
		return (uint64) val | UINT64_HIGH_BIT;
	else
		return UINT64_HIGH_BIT - (uint64) (-val);
}

static int64
uint64_to_int64(uint64 val)
{
	if (val & UINT64_HIGH_BIT)
		return val & (~UINT64_HIGH_BIT);
	else
		return -(int64) (UINT64_HIGH_BIT - val);
}

static uint64
val_get_uint64(Datum val, Oid typeoid)
{
	ItemPointer iptr;

	switch (typeoid)
	{
		case INT4OID:
			return int64_to_uint64(DatumGetInt32(val));
		case INT8OID:
			return int64_to_uint64(DatumGetInt64(val));
		case TIDOID:
			iptr = DatumGetItemPointer(val);
			return (ItemPointerGetBlockNumberNoCheck(iptr) << 16) +
				ItemPointerGetOffsetNumberNoCheck(iptr);
		default:
			elog(ERROR, "Unsupported keybitmap type");
			return 0;
	}
}

static void
uint64_get_val(uint64 val, Oid typeoid, Pointer ptr)
{
	ItemPointer iptr;

	switch (typeoid)
	{
		case INT4OID:
			*((int32 *) ptr) = uint64_to_int64(val);
			break;
		case INT8OID:
			*((int64 *) ptr) = uint64_to_int64(val);
			break;
		case TIDOID:
			iptr = (ItemPointer) ptr;
			ItemPointerSetBlockNumber(iptr, val >> 16);
			ItemPointerSetOffsetNumber(iptr, val & 0xFFFF);
			break;
		default:
			elog(ERROR, "Unsupported keybitmap type");
			break;
	}
}

static uint64
seconary_tuple_get_pk_data(OTuple tuple, OIndexDescr *ix_descr)
{
	AttrNumber	attnum;
	FormData_pg_attribute *attr;
	Datum		val;

	Assert(ix_descr->nPrimaryFields == 1);
	Assert(!O_TUPLE_IS_NULL(tuple));

	attnum = ix_descr->primaryFieldsAttnums[0];
	attr = &ix_descr->leafTupdesc->attrs[attnum - 1];
	val = o_toast_nocachegetattr(tuple, attnum, ix_descr->leafTupdesc,
								 &ix_descr->leafSpec);
	return val_get_uint64(val, attr->atttypid);
}

static uint64
primary_tuple_get_data(OTuple tuple, OIndexDescr *primary, bool onlyPkey)
{
	AttrNumber	attnum;
	FormData_pg_attribute *attr;
	Datum		val;

	Assert(primary->nFields == 1);

	Assert(!O_TUPLE_IS_NULL(tuple));

	attnum = primary->fields[0].tableAttnum;
	attr = &primary->leafTupdesc->attrs[attnum - 1];
	if (onlyPkey)
		attnum = 1;
	val = o_toast_nocachegetattr(tuple, attnum, primary->leafTupdesc,
								 &primary->leafSpec);
	return val_get_uint64(val, attr->atttypid);
}

static double
o_index_getbitmap(OBitmapHeapPlanState *bitmap_state,
				  BitmapIndexScanState *node, RBTree *bitmap)
{
	OScanState	ostate = {0};
	OTableDescr *descr;
	OIndexDescr *indexDescr = NULL;
	OIndexNumber ix_num;
	Relation	index,
				table;
	BitmapIndexScan *bitmap_ix_scan = ((BitmapIndexScan *) node->ss.ps.plan);
	OTuple		tuple = {0};
	ExprContext *econtext = bitmap_state->scan->ss->ps.ps_ExprContext;
	MemoryContext mcxt = bitmap_state->scan->ss->ss_ScanTupleSlot->tts_mcxt;
	double		nTuples = 0;
	OEACallsCounters *prev_ea_counters = ea_counters;

	index = index_open(bitmap_ix_scan->indexid, AccessShareLock);
	table = table_open(index->rd_index->indrelid, AccessShareLock);
	descr = relation_get_descr(table);
	Assert(descr);
	relation_close(table, AccessShareLock);
	for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
	{
		indexDescr = descr->indices[ix_num];
		if (indexDescr->oids.reloid == bitmap_ix_scan->indexid)
			break;
	}
	Assert(ix_num < descr->nIndices && indexDescr != NULL);
	ostate.ixNum = ix_num;
	ostate.scanDir = ForwardScanDirection;
	ostate.indexQuals = bitmap_ix_scan->indexqual;
	ResetExprContext(econtext);
	init_index_scan_state(&bitmap_state->o_plan_state, &ostate, index, econtext,
						  &node->biss_RuntimeKeys,
						  &node->biss_NumRuntimeKeys,
						  &node->biss_ScanKeys,
						  &node->biss_NumScanKeys);
	relation_close(index, AccessShareLock);

	if (node->biss_NumRuntimeKeys != 0)
	{
		ResetExprContext(node->biss_RuntimeContext);
		ExecIndexEvalRuntimeKeys(node->biss_RuntimeContext,
								 node->biss_RuntimeKeys,
								 node->biss_NumRuntimeKeys);
		node->biss_RuntimeKeysReady = true;
	}

	if ((node->biss_NumRuntimeKeys == 0 && node->biss_NumArrayKeys == 0) ||
		(node->biss_RuntimeKeysReady))
	{
		btrescan(&ostate.scandesc, node->biss_ScanKeys,
				 node->biss_NumScanKeys, NULL, 0);
		ostate.numPrefixExactKeys = o_get_num_prefix_exact_keys(node->biss_ScanKeys, node->biss_NumScanKeys);
	}


	if (is_explain_analyze(&node->ss.ps))
	{
		ea_counters = &bitmap_state->eaCounters[ix_num];
	}
	else
		ea_counters = NULL;

	ostate.oSnapshot = bitmap_state->oSnapshot;
	ostate.onlyCurIx = true;
	ostate.cxt = bitmap_state->cxt;

	ostate.curKeyRangeIsLoaded = false;
	ostate.curKeyRange.empty = true;
	ostate.curKeyRange.low.n_row_keys = 0;
	ostate.curKeyRange.high.n_row_keys = 0;

	if (!ostate.curKeyRangeIsLoaded)
	{
		BTScanOpaque so = (BTScanOpaque) ostate.scandesc.opaque;

		_bt_preprocess_keys(&ostate.scandesc);
		if (so->numArrayKeys)
			_bt_start_array_keys(&ostate.scandesc, ForwardScanDirection);
		ostate.curKeyRange.empty = true;
	}

	o_btree_load_shmem(&indexDescr->desc);
	do
	{
		tuple = o_iterate_index(indexDescr, &ostate, NULL, mcxt, NULL);

		if (!O_TUPLE_IS_NULL(tuple))
		{
			uint64		data;

			data = seconary_tuple_get_pk_data(tuple, indexDescr);
			o_keybitmap_insert(bitmap, data);
			nTuples += 1;
		}
	} while (!O_TUPLE_IS_NULL(tuple));

	if (ostate.iterator)
		btree_iterator_free(ostate.iterator);
	MemoryContextReset(ostate.cxt);

	ea_counters = prev_ea_counters;
	return nTuples;
}

static RBTree *
o_exec_bitmapqual(OBitmapHeapPlanState *bitmap_state, PlanState *planstate)
{
	RBTree	   *result = NULL;

	switch (nodeTag(planstate))
	{
		case T_BitmapAndState:
			{
				BitmapAndState *node = (BitmapAndState *) planstate;
				int			i;
				Instrumentation *instrument = node->ps.instrument;

				if (instrument)
					InstrStartNode(instrument);

				for (i = 0; i < node->nplans; i++)
				{
					PlanState  *subnode = node->bitmapplans[i];
					RBTree	   *subresult = o_exec_bitmapqual(bitmap_state,
															  subnode);

					if (result == NULL)
						result = subresult; /* first subplan */
					else
					{
						o_keybitmap_intersect(result, subresult);
						o_keybitmap_free(subresult);
					}

					/*
					 * If at any stage we have a completely empty bitmap, we
					 * can fall out without evaluating the remaining subplans,
					 * since ANDing them can no longer change the result.
					 * (Note: the fact that indxpath.c orders the subplans by
					 * selectivity should make this case more likely to
					 * occur.)
					 */
					if (o_keybitmap_is_empty(result))
						break;
				}
				if (instrument)
					InstrStopNode(instrument, 0);
				break;
			}
		case T_BitmapOrState:
			{
				BitmapOrState *node = (BitmapOrState *) planstate;
				int			i;
				Instrumentation *instrument = node->ps.instrument;

				if (instrument)
					InstrStartNode(instrument);

				for (i = 0; i < node->nplans; i++)
				{
					PlanState  *subnode = node->bitmapplans[i];
					RBTree	   *subresult;

					if (IsA(subnode, BitmapIndexScanState))
					{
						if (result == NULL) /* first subplan */
						{
							result = o_keybitmap_create();
						}

						bitmap_state->scan->saved_bitmap = result;
						subresult = o_exec_bitmapqual(bitmap_state, subnode);
						Assert(result == subresult);
					}
					else
					{
						/* standard implementation */
						subresult = o_exec_bitmapqual(bitmap_state, subnode);

						if (result == NULL)
							result = subresult; /* first subplan */
						else
						{
							o_keybitmap_union(result, subresult);
							o_keybitmap_free(subresult);
						}
					}
				}
				if (instrument)
					InstrStopNode(instrument, 0);
				break;
			}
		case T_BitmapIndexScanState:
			{
				double		nTuples = 0;
				BitmapIndexScanState *node;
				Instrumentation *instrument;

				node = (BitmapIndexScanState *) planstate;
				instrument = node->ss.ps.instrument;

				if (instrument)
					InstrStartNode(instrument);

				if (bitmap_state->scan->saved_bitmap)
				{
					result = bitmap_state->scan->saved_bitmap;
					/* reset for next time */
					bitmap_state->scan->saved_bitmap = NULL;
				}
				else
				{
					result = o_keybitmap_create();
				}

				nTuples = o_index_getbitmap(bitmap_state, node, result);
				if (instrument)
					InstrStopNode(instrument, nTuples);
				break;
			}
		default:
			elog(ERROR, "%s: unrecognized node type: %d",
				 PG_FUNCNAME_MACRO, (int) nodeTag(planstate));
			break;
	}

	return result;
}

OBitmapScan *
o_make_bitmap_scan(OBitmapHeapPlanState *bitmap_state, ScanState *ss,
				   PlanState *bitmapqualplanstate, Relation rel,
				   Oid typeoid, OSnapshot *oSnapshot,
				   MemoryContext cxt)
{
	OBitmapScan *scan = palloc0(sizeof(OBitmapScan));

	scan->typeoid = typeoid;
	scan->oSnapshot = *oSnapshot;
	scan->cxt = cxt;
	scan->ss = ss;
	scan->tbl_desc = relation_get_descr(rel);
	bitmap_state->scan = scan;
	scan->saved_bitmap = o_exec_bitmapqual(bitmap_state, bitmapqualplanstate);
	scan->seq_scan = make_btree_seq_scan_cb(&GET_PRIMARY(scan->tbl_desc)->desc,
											&scan->oSnapshot,
											&bitmap_seq_scan_callbacks, scan);
	return scan;
}

TupleTableSlot *
o_exec_bitmap_fetch(OBitmapScan *scan, CustomScanState *node)
{
	bool		valid = false;
	TupleTableSlot *slot = NULL;

	do
	{
		OTuple		tuple;
		BTreeLocationHint hint;
		MemoryContext tupleCxt = node->ss.ss_ScanTupleSlot->tts_mcxt;
		CommitSeqNo tupleCsn;

		tuple = btree_seq_scan_getnext(scan->seq_scan, tupleCxt, &tupleCsn,
									   &hint);

		if (O_TUPLE_IS_NULL(tuple))
		{
			slot = ExecClearTuple(node->ss.ss_ScanTupleSlot);
			valid = true;
		}
		else
		{
			OTableDescr *descr;
			uint64		value;

			descr = relation_get_descr(node->ss.ss_currentRelation);
			value = primary_tuple_get_data(tuple, GET_PRIMARY(descr), false);

			if (o_keybitmap_test(scan->saved_bitmap, value))
			{
				TupleTableSlot *scan_slot;
				MemoryContext oldcxt;

				slot = node->ss.ss_ScanTupleSlot;
				oldcxt = MemoryContextSwitchTo(slot->tts_mcxt);
				scan_slot = MakeSingleTupleTableSlot(descr->tupdesc,
													 &TTSOpsOrioleDB);
				MemoryContextSwitchTo(oldcxt);
				tts_orioledb_store_tuple(scan_slot, tuple,
										 descr, tupleCsn,
										 PrimaryIndexNumber,
										 true, &hint);

				slot = scan_slot;
				valid = true;
			}
		}

		if (!valid)
			InstrCountFiltered2(node, 1);
		else if (!TupIsNull(slot) && !o_exec_qual(node->ss.ps.ps_ExprContext,
												  node->ss.ps.qual, slot))
			InstrCountFiltered1(node, 1);

	} while (!valid || (!TupIsNull(slot) &&
						!o_exec_qual(node->ss.ps.ps_ExprContext,
									 node->ss.ps.qual, slot)));
	return slot;
}

void
o_free_bitmap_scan(OBitmapScan *scan)
{
	free_btree_seq_scan(scan->seq_scan);
	o_keybitmap_free(scan->saved_bitmap);
	pfree(scan);
}

static bool
o_bitmap_is_range_valid(OTuple low, OTuple high, void *arg)
{
	OBitmapScan *bitmap_scan = (OBitmapScan *) arg;
	OIndexDescr *primary = GET_PRIMARY(bitmap_scan->tbl_desc);
	uint64		lowValue,
				highValue;

	if (!O_TUPLE_IS_NULL(low))
		lowValue = primary_tuple_get_data(low, primary, true);
	else
		lowValue = 0;

	if (!O_TUPLE_IS_NULL(high))
		highValue = primary_tuple_get_data(high, primary, true);
	else
		highValue = UINT64_MAX;

	return o_keybitmap_range_is_valid(bitmap_scan->saved_bitmap,
									  lowValue, highValue);
}

static bool
o_bitmap_get_next_key(OFixedKey *key, bool inclusive, void *arg)
{
	OBitmapScan *bitmap_scan = (OBitmapScan *) arg;
	bool		found;
	uint64		prev_value = 0;
	uint64		res_value;
	OTupleHeader tuphdr;
	OIndexDescr *primary = GET_PRIMARY(bitmap_scan->tbl_desc);

	if (!O_TUPLE_IS_NULL(key->tuple))
	{
		prev_value = primary_tuple_get_data(key->tuple, primary, false);
		if (!inclusive)
		{
			if (prev_value == UINT64_MAX)
				return false;
			prev_value++;
		}
	}

	res_value = o_keybitmap_get_next(bitmap_scan->saved_bitmap, prev_value,
									 &found);

	if (found)
	{
		FormData_pg_attribute *attr = TupleDescAttr(primary->nonLeafTupdesc, 0);

		Assert(primary->nFields == 1);
		tuphdr = (OTupleHeader) key->fixedData;
		tuphdr->hasnulls = false;
		tuphdr->natts = 1;
		tuphdr->len = SizeOfOTupleHeader + attr->attlen;
		uint64_get_val(res_value,
					   attr->atttypid,
					   &key->fixedData[SizeOfOTupleHeader]);
		key->tuple.data = key->fixedData;
		key->tuple.formatFlags = 0;
	}
	else
	{
		O_TUPLE_SET_NULL(key->tuple);
	}

	return found;
}
