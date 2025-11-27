/*-------------------------------------------------------------------------
 *
 * bitmap_scan.c
 *		Routines for bitmap scan of orioledb table
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
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
#include "tableam/tree.h"
#include "tuple/slot.h"

#include "access/relation.h"
#include "access/table.h"
#include "catalog/pg_type.h"
#include "tuple/format.h"
#include "executor/nodeIndexscan.h"
#include "lib/rbtree.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"

#include <math.h>


typedef struct BitmapSeqScanArg
{
	OTableDescr *tbl_desc;
	RBTree	   *bitmap;
	TIDBitmap  *bridged_bitmap;
} BitmapSeqScanArg;

typedef struct OBitmapScan
{
	ScanState  *ss;
	OSnapshot	oSnapshot;
	MemoryContext cxt;
	Oid			typeoid;
	TBMIterator *tbmiterator;
	TBMIterateResult *tbmres;
	int			cur_tuple;
	int			page_ntuples;
	
	/* Bitmap scanning state */
	uint64		current_bitmap_value; 	/* Current position in bitmap */
	bool		bitmap_exhausted; 		/* True when bitmap is fully scanned */
	BTreeIterator *range_iterator;		/* For dense ranges */
	uint64		range_end_value;		/* End of current dense range */
	bool		using_range_scan;		/* True when scanning a dense range */
	
	BitmapSeqScanArg arg;
} OBitmapScan;

/*
 * Adaptive bitmap scan strategy:
 * - For sparse bitmaps: use direct tuple lookups (efficient for scattered tuples)
 * - For dense bitmaps: use range iteration (efficient for clustered tuples)
 * 
 * We detect density by checking how many consecutive values exist in the bitmap.
 * If we find DENSE_BITMAP_THRESHOLD or more consecutive values, we switch to
 * range iteration mode.
 */
#define DENSE_BITMAP_THRESHOLD 16  		/* Min consecutive tuples to use range scan */
#define DENSE_BITMAP_GAP_THRESHOLD 4  	/* Max gap between tuples in dense region */


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
	bool		is_null;

	Assert(ix_descr->nPrimaryFields == 1);
	Assert(!O_TUPLE_IS_NULL(tuple));

	/*
	 * Currently bitmap scan works only for first field with int4, int8 or
	 * ctid type
	 */
	attnum = ix_descr->primaryFieldsAttnums[0];
	attr = &ix_descr->leafTupdesc->attrs[attnum - 1];
	val = o_toast_nocachegetattr(tuple, attnum, ix_descr->leafTupdesc,
								 &ix_descr->leafSpec, &is_null);
	return val_get_uint64(val, attr->atttypid);
}

static uint64
primary_tuple_get_data(OTuple tuple, OIndexDescr *primary, bool onlyPkey)
{
	AttrNumber	attnum;
	FormData_pg_attribute *attr;
	Datum		val;
	bool		is_null;
	BTreeKeyType keyType = onlyPkey ? BTreeKeyNonLeafKey : BTreeKeyLeafTuple;
	TupleDesc	tupdesc = onlyPkey ? primary->nonLeafTupdesc : primary->leafTupdesc;
	OTupleFixedFormatSpec *spec = onlyPkey ? &primary->nonLeafSpec : &primary->leafSpec;

	Assert(primary->nFields == 1);

	Assert(!O_TUPLE_IS_NULL(tuple));

	attnum = OIndexKeyAttnumToTupleAttnum(keyType, primary, 1);
	attr = &tupdesc->attrs[attnum - 1];
	val = o_toast_nocachegetattr(tuple, attnum, tupdesc, spec, &is_null);
	return val_get_uint64(val, attr->atttypid);
}

/*
 * Fill OBTreeKeyBound from a uint64 bitmap value for primary key lookup.
 */
static void
fill_key_bound_from_uint64(OBTreeKeyBound *bound, OIndexDescr *primary, uint64 value,
							ItemPointerData *iptr_storage)
{
	FormData_pg_attribute *attr;
	Datum		datum_value = 0;

	Assert(primary->nFields == 1);
	Assert(iptr_storage != NULL);
	attr = TupleDescAttr(primary->nonLeafTupdesc, 0);
	
	/* Convert uint64 value to datum of appropriate type */
	if (attr->atttypid == TIDOID)
	{
		uint64_get_val(value, attr->atttypid, (Pointer) iptr_storage);
		datum_value = ItemPointerGetDatum(iptr_storage);
	}
	else
		uint64_get_val(value, attr->atttypid, (Pointer)&datum_value);

	/* Fill the bound structure */
	bound->nkeys = 1;
	bound->keys[0].value = datum_value;
	bound->keys[0].type = attr->atttypid;
	bound->keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
	bound->keys[0].comparator = primary->fields[0].comparator;
	bound->n_row_keys = 0;
	bound->row_keys = NULL;
}

static double
o_index_getbitmap(OBitmapHeapPlanState *bitmap_state,
				  BitmapIndexScanState *node,
				  RBTree *bitmap, TIDBitmap *tbm_result)
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

	bitmap_state->o_plan_state.plan_state = &node->ss.ps;

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
		if (!so->qual_ok)
			return nTuples;
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

			if (!tbm_result)
			{
				data = seconary_tuple_get_pk_data(tuple, indexDescr);
				o_keybitmap_insert(bitmap, data);
			}
			else
			{
				if (indexDescr->desc.type != oIndexPrimary)
				{
					OBTreeKeyBound bound;
					OTuple		ptup;
					OIndexDescr *primary = GET_PRIMARY(descr);
					AttrNumber	attnum;
					Datum		val;
					bool		is_null;
					TupleDesc	tupdesc = primary->leafTupdesc;
					OTupleFixedFormatSpec *spec = &primary->leafSpec;
					ItemPointer bridge_iptr;

					/* fetch primary index key from tuple and search raw tuple */
					o_fill_pindex_tuple_key_bound(&indexDescr->desc, tuple, &bound);

					o_btree_load_shmem(&primary->desc);
					ptup = o_btree_find_tuple_by_key(&primary->desc,
													 (Pointer) &bound, BTreeKeyBound,
													 &ostate.oSnapshot, NULL,
													 mcxt, NULL);

					/*
					 * in concurrent DELETE/UPDATE it might happen, we should
					 * to try fetch next tuple
					 */
					if (!O_TUPLE_IS_NULL(ptup))
					{
						attnum = primary->primaryIsCtid ? 2 : 1;
						val = o_toast_nocachegetattr(ptup, attnum, tupdesc, spec, &is_null);
						Assert(!is_null);
						bridge_iptr = DatumGetItemPointer(val);
						tbm_add_tuples(tbm_result, bridge_iptr, 1, false);
						pfree(tuple.data);
						tuple = ptup;
					}
				}
				else
				{
					Assert(false);
				}
			}
			nTuples += 1;
		}
	} while (!O_TUPLE_IS_NULL(tuple));

	if (ostate.iterator)
		btree_iterator_free(ostate.iterator);
	MemoryContextReset(ostate.cxt);

	ea_counters = prev_ea_counters;
	return nTuples;
}

static void
exec_bitmap_index_state(OBitmapHeapPlanState *bitmap_state, PlanState *planstate,
						RBTree **rbt_result, TIDBitmap **tbm_result)
{
	double		nTuples = 0;
	BitmapIndexScanState *node;
	Instrumentation *instrument;
	OBTOptions *options;
	ExprContext *econtext = bitmap_state->scan->ss->ps.ps_ExprContext;

	node = (BitmapIndexScanState *) planstate;
	instrument = node->ss.ps.instrument;
	options = (OBTOptions *) node->biss_RelationDesc->rd_options;

	if (node->biss_NumRuntimeKeys != 0)
		ExecIndexEvalRuntimeKeys(econtext,
								 node->biss_RuntimeKeys,
								 node->biss_NumRuntimeKeys);
	if (node->biss_NumArrayKeys != 0)
		node->biss_RuntimeKeysReady =
			ExecIndexEvalArrayKeys(econtext,
								   node->biss_ArrayKeys,
								   node->biss_NumArrayKeys);
	else
		node->biss_RuntimeKeysReady = true;

	/* reset index scan */
	if (node->biss_RuntimeKeysReady)
		index_rescan(node->biss_ScanDesc,
					 node->biss_ScanKeys, node->biss_NumScanKeys,
					 NULL, 0);

	if (instrument)
		InstrStartNode(instrument);

	if (node->biss_RelationDesc->rd_rel->relam != BTREE_AM_OID ||
		(options && !options->orioledb_index))
	{
		bool		doscan;
		IndexScanDesc scandesc;

		if (*tbm_result == NULL)
			*tbm_result = tbm_create(work_mem * 1024L, NULL);

		/*
		 * extract necessary information from index scan node
		 */
		scandesc = node->biss_ScanDesc;

		/*
		 * If we have runtime keys and they've not already been set up, do it
		 * now. Array keys are also treated as runtime keys; note that if
		 * ExecReScan returns with biss_RuntimeKeysReady still false, then
		 * there is an empty array key so we should do nothing.
		 */
		if (!node->biss_RuntimeKeysReady &&
			(node->biss_NumRuntimeKeys != 0 || node->biss_NumArrayKeys != 0))
		{
			ExecReScan((PlanState *) node);
			doscan = node->biss_RuntimeKeysReady;
		}
		else
			doscan = true;

		while (doscan)
		{
			nTuples += (double) index_getbitmap(scandesc, *tbm_result);

			CHECK_FOR_INTERRUPTS();

			doscan = ExecIndexAdvanceArrayKeys(node->biss_ArrayKeys,
											   node->biss_NumArrayKeys);
			if (doscan)			/* reset index scan */
				index_rescan(node->biss_ScanDesc,
							 node->biss_ScanKeys, node->biss_NumScanKeys,
							 NULL, 0);
		}
	}
	else
	{
		if (*tbm_result == NULL && *rbt_result == NULL)
			*rbt_result = o_keybitmap_create();
		nTuples = o_index_getbitmap(bitmap_state, node, *rbt_result, *tbm_result);
	}
	if (instrument)
		InstrStopNode(instrument, nTuples);
}

/*
 * add_rbt_to_tbm
 *
 * Iterate over all entries stored in an RBTree of key/bitmap nodes and add
 * the corresponding physical tuple item pointers (ctid/bridge ItemPointer)
 * into the provided TIDBitmap.
 */
static void
add_rbt_to_tbm(OBitmapHeapPlanState *bitmap_state, TIDBitmap *tbm, RBTree *rbt)
{
	RBTreeIterator iter;
	OKeyBitmapRBTNode *node;
	OIndexDescr *primary = GET_PRIMARY(bitmap_state->scan->arg.tbl_desc);
	OBTreeKeyBound bound;

	/* Iterate through all entries in the bitmap */
	rbt_begin_iterate(rbt, LeftRightWalk, &iter);
	while ((node = (OKeyBitmapRBTNode *) rbt_iterate(&iter)) != NULL)
	{
		OTuple		tuple;
		uint64		start_value,
					end_value;
		ItemPointerData iptr_storage;
		
		/* Determine the range of values for this node */
		if (node->bitmap)
		{
			/* Node has a bitmap, iterate through set bits */
			start_value = node->key;
			end_value = node->key + BITMAP_SIZE * 8;
		}
		else
		{
			/* Node has a single value */
			start_value = node->key;
			end_value = node->key + 1;
		}

		/* Fetch each tuple indicated by the bitmap */
		for (uint64 cur_value = start_value; cur_value < end_value; cur_value++)
		{
			AttrNumber	attnum;
			Datum		val;
			bool		is_null;
			TupleDesc	tupdesc = primary->leafTupdesc;
			OTupleFixedFormatSpec *spec = &primary->leafSpec;
			ItemPointer bridge_iptr;

			/* Skip if this value is not in the bitmap */
			if (node->bitmap)
			{
				int offset = (cur_value & LOW_PART_MASK);
				if (!(node->bitmap[offset >> 3] & (1 << (offset & 7))))
					continue;
			}
			else if (cur_value != node->key)
			{
				continue;
			}

			/* Fill bound structure from uint64 value */
			fill_key_bound_from_uint64(&bound, primary, cur_value, &iptr_storage);

			/* Lookup the tuple directly */
			o_btree_load_shmem(&primary->desc);
			tuple = o_btree_find_tuple_by_key(&primary->desc,
											 (Pointer) &bound, BTreeKeyBound,
											 &bitmap_state->scan->oSnapshot, NULL,
											 bitmap_state->scan->cxt, NULL);

			if (!O_TUPLE_IS_NULL(tuple))
			{
				attnum = primary->primaryIsCtid ? 2 : 1;
				val = o_toast_nocachegetattr(tuple, attnum, tupdesc, spec, &is_null);
				Assert(!is_null);
				bridge_iptr = DatumGetItemPointer(val);
				tbm_add_tuples(tbm, bridge_iptr, 1, false);
				pfree(tuple.data);
			}
		}
	}
}

static void
o_exec_bitmapqual(OBitmapHeapPlanState *bitmap_state, PlanState *planstate,
				  RBTree **rbt_result, TIDBitmap **tbm_result)
{
	Assert(rbt_result && tbm_result);
	Assert(*rbt_result == NULL || *tbm_result == NULL);

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
					RBTree	   *rbt_subresult = NULL;
					TIDBitmap  *tbm_subresult = NULL;

					o_exec_bitmapqual(bitmap_state, subnode, &rbt_subresult, &tbm_subresult);

					Assert(rbt_subresult || tbm_subresult);

					if (tbm_subresult != NULL)
					{
						if (*tbm_result == NULL)
						{
							*tbm_result = tbm_subresult;	/* first subplan */
						}
						else
						{
							tbm_intersect(*tbm_result, tbm_subresult);
							tbm_free(tbm_subresult);
						}
					}
					else if (rbt_subresult != NULL)
					{
						if (*tbm_result == NULL)
						{
							if (*rbt_result == NULL)
							{
								*rbt_result = rbt_subresult;	/* first subplan */
							}
							else if (*rbt_result != NULL)
							{
								o_keybitmap_intersect(*rbt_result, rbt_subresult);
								o_keybitmap_free(rbt_subresult);
							}
						}
						else
						{
							TIDBitmap  *temp_bitmap = tbm_create(work_mem * 1024L, NULL);

							Assert(*rbt_result == NULL);

							add_rbt_to_tbm(bitmap_state, temp_bitmap, rbt_subresult);
							tbm_intersect(*tbm_result, temp_bitmap);
							tbm_free(temp_bitmap);
						}
					}

					if (*tbm_result != NULL && *rbt_result != NULL)
					{
						TIDBitmap  *temp_bitmap = tbm_create(work_mem * 1024L, NULL);

						add_rbt_to_tbm(bitmap_state, temp_bitmap, *rbt_result);
						tbm_intersect(*tbm_result, temp_bitmap);
						tbm_free(temp_bitmap);
					}

					/*
					 * If at any stage we have a completely empty bitmap, we
					 * can fall out without evaluating the remaining subplans,
					 * since ANDing them can no longer change the result.
					 * (Note: the fact that indxpath.c orders the subplans by
					 * selectivity should make this case more likely to
					 * occur.)
					 */
					if ((*rbt_result && o_keybitmap_is_empty(*rbt_result)) ||
						(*tbm_result && tbm_is_empty(*tbm_result)))
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
					RBTree	   *rbt_subresult = NULL;
					TIDBitmap  *tbm_subresult = NULL;

					if (IsA(subnode, BitmapIndexScanState))
					{
						rbt_subresult = *rbt_result;
						tbm_subresult = *tbm_result;
						Assert(!(rbt_subresult && tbm_subresult));
						o_exec_bitmapqual(bitmap_state, subnode, &rbt_subresult, &tbm_subresult);

						/*
						 * In other situations union should be already made
						 * inside of o_exec_bitmapqual
						 */
						if (*rbt_result == NULL && rbt_subresult != NULL)
							*rbt_result = rbt_subresult;
						if (*tbm_result == NULL && tbm_subresult != NULL)
							*tbm_result = tbm_subresult;
					}
					else
					{
						/* standard implementation */
						o_exec_bitmapqual(bitmap_state, subnode, &rbt_subresult, &tbm_subresult);

						if (tbm_subresult != NULL)
						{
							if (*tbm_result == NULL)
								*tbm_result = tbm_subresult;	/* first subplan */
							else
							{
								tbm_union(*tbm_result, tbm_subresult);
								tbm_free(tbm_subresult);
							}
						}
						else if (rbt_subresult != NULL)
						{
							if (*rbt_result == NULL)
							{
								*rbt_result = rbt_subresult;	/* first subplan */
							}
							else if (*tbm_result == NULL)
							{
								o_keybitmap_union(*rbt_result, rbt_subresult);
								o_keybitmap_free(rbt_subresult);
							}
							else
							{
								add_rbt_to_tbm(bitmap_state, *tbm_result, rbt_subresult);
								o_keybitmap_free(rbt_subresult);
							}
						}
					}

					if (*tbm_result != NULL && *rbt_result != NULL)
					{
						add_rbt_to_tbm(bitmap_state, *tbm_result, *rbt_result);
						o_keybitmap_free(*rbt_result);
						*rbt_result = NULL;
					}
				}
				if (instrument)
					InstrStopNode(instrument, 0);
				break;
			}
		case T_BitmapIndexScanState:
			exec_bitmap_index_state(bitmap_state, planstate, rbt_result, tbm_result);
			break;
		default:
			elog(ERROR, "%s: unrecognized node type: %d",
				 PG_FUNCNAME_MACRO, (int) nodeTag(planstate));
			break;
	}
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
	scan->arg.tbl_desc = relation_get_descr(rel);
	scan->current_bitmap_value = 0;
	scan->bitmap_exhausted = false;
	scan->range_iterator = NULL;
	scan->range_end_value = 0;
	scan->using_range_scan = false;
	bitmap_state->scan = scan;
	o_exec_bitmapqual(bitmap_state, bitmapqualplanstate,
					  &scan->arg.bitmap,
					  &scan->arg.bridged_bitmap);

	if (scan->arg.bitmap)
	{
		/* Bitmap iteration starts from 0 */
		scan->current_bitmap_value = 0;
		scan->bitmap_exhausted = false;
	}
	else
	{
		Assert(scan->arg.bridged_bitmap);
		scan->tbmiterator = tbm_begin_iterate(scan->arg.bridged_bitmap);
	}

	return scan;
}

static void
o_tbmiterator_next_page(OBitmapScan *scan, OBitmapHeapPlanState *bitmap_state)
{
	OIndexDescr *bridge = scan->arg.tbl_desc->bridge;

	scan->cur_tuple = 0;
	scan->page_ntuples = 0;

	/* Clear old bitmap and create new one for this page */
	if (scan->arg.bitmap)
		o_keybitmap_free(scan->arg.bitmap);
	scan->arg.bitmap = o_keybitmap_create();
	if (scan->tbmres->ntuples >= 0)
	{
		/*
		 * Bitmap is non-lossy, so we just look through the offsets listed in
		 * tbmres; but we have to follow any HOT chain starting at each such
		 * offset.
		 */
		int			curoff;

		scan->page_ntuples = scan->tbmres->ntuples;
		for (curoff = 0; curoff < scan->tbmres->ntuples; curoff++)
		{
			OffsetNumber offnum = scan->tbmres->offsets[curoff];
			ItemPointerData iptr;
			OBTreeKeyBound bridge_bound;
			OTuple		bridge_tup;
			uint64		data;
			CommitSeqNo tupleCsn;

			ItemPointerSet(&iptr, scan->tbmres->blockno, offnum);

			bridge_bound.nkeys = 1;
			bridge_bound.keys[0].value = ItemPointerGetDatum(&iptr);
			bridge_bound.keys[0].type = TIDOID;
			bridge_bound.keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
			bridge_bound.keys[0].comparator = NULL;
			bridge_bound.n_row_keys = 0;
			bridge_bound.row_keys = NULL;

			o_btree_load_shmem(&bridge->desc);

			bridge_tup = o_btree_find_tuple_by_key(&bridge->desc,
												   (Pointer) &bridge_bound, BTreeKeyBound,
												   &o_in_progress_snapshot, &tupleCsn,
												   CurrentMemoryContext, NULL);

			if (!O_TUPLE_IS_NULL(bridge_tup))
			{
				data = seconary_tuple_get_pk_data(bridge_tup, bridge);
				o_keybitmap_insert(scan->arg.bitmap, data);

				pfree(bridge_tup.data);
			}
		}
	}
	else
	{
		/*
		 * Bitmap is lossy, so we must examine each line pointer on the page.
		 */

		OTableDescr *tbl_descr = scan->arg.tbl_desc;
		BTreeIterator *it;
		ItemPointerData start_iptr;
		ItemPointerData end_iptr;
		OBTreeKeyBound start_bound;
		OBTreeKeyBound end_bound;
		TupleTableSlot *primarySlot;
		ExprContext *tup_econtext = bitmap_state->scan->ss->ps.ps_ExprContext;
		CommitSeqNo tupleCsn;

		ItemPointerSet(&start_iptr, scan->tbmres->blockno, 0);
		start_bound.nkeys = 1;
		start_bound.keys[0].value = ItemPointerGetDatum(&start_iptr);
		start_bound.keys[0].type = TIDOID;
		start_bound.keys[0].flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_INCLUSIVE | O_VALUE_BOUND_COERCIBLE;
		start_bound.keys[0].comparator = bridge->fields[0].comparator;
		start_bound.n_row_keys = 0;
		start_bound.row_keys = NULL;

		ItemPointerSet(&end_iptr, scan->tbmres->blockno, MaxOffsetNumber);
		end_bound.nkeys = 1;
		end_bound.keys[0].value = ItemPointerGetDatum(&end_iptr);
		end_bound.keys[0].type = TIDOID;
		end_bound.keys[0].flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_INCLUSIVE | O_VALUE_BOUND_COERCIBLE;
		end_bound.keys[0].comparator = bridge->fields[0].comparator;
		end_bound.n_row_keys = 0;
		end_bound.row_keys = NULL;

		o_btree_load_shmem(&bridge->desc);
		it = o_btree_iterator_create(&bridge->desc, (Pointer) &start_bound, BTreeKeyBound,
									 &o_in_progress_snapshot, ForwardScanDirection);
		primarySlot = MakeSingleTupleTableSlot(tbl_descr->tupdesc, &TTSOpsOrioleDB);

		do
		{
			OTuple		tup = o_btree_iterator_fetch(it, &tupleCsn,
													 (Pointer) &end_bound,
													 BTreeKeyBound, true,
													 NULL);
			uint64		data;

			if (O_TUPLE_IS_NULL(tup))
				break;

			data = seconary_tuple_get_pk_data(tup, bridge);
			o_keybitmap_insert(scan->arg.bitmap, data);
			scan->page_ntuples++;

			pfree(tup.data);
			ExecClearTuple(primarySlot);
			MemoryContextReset(tup_econtext->ecxt_per_tuple_memory);
		} while (true);

		ExecDropSingleTupleTableSlot(primarySlot);
		btree_iterator_free(it);
	}
}

/*
 * Check if we should use range iteration for the next set of tuples.
 * Returns true if we find a dense region, and sets *range_start and *range_end.
 */
static bool
should_use_range_iteration(OIndexDescr *primary, RBTree *bitmap, uint64 current_value,
                           uint64 *range_start, uint64 *range_end)
{
    uint64		next_value;
    bool		found;
    int			consecutive_count = 0;
    uint64		first_value = 0;
    uint64		last_value = 0;
    uint64		check_value = current_value;

    /* Look ahead to see if there's a dense region */
    while (true)
    {
        next_value = o_keybitmap_get_next(bitmap, check_value, &found);
		
        if (!found)
            break;

        if (first_value == 0)
		{
            first_value = next_value;
		}

        last_value = next_value;

        /* Check if values are close together (within reasonable range for same page) */
		if ((next_value - check_value) > DENSE_BITMAP_GAP_THRESHOLD)
        {
            /* Gap too large, not a dense region */
            break;
        }

        consecutive_count++;
        check_value = next_value + 1;
    }

    if (consecutive_count >= DENSE_BITMAP_THRESHOLD)
    {
        *range_start = first_value;
        *range_end = last_value;
        return true;
    }
	
    return false;
}

/*
 * o_bitmap_store_tuple
 * Store and optionally recheck a tuple found by a bitmap heap scan.
 */
static bool
o_bitmap_store_tuple(OBitmapScan *scan, CustomScanState *node, TupleTableSlot ** slot,
				     OTuple *tuple, CommitSeqNo tupleCsn,
				     BTreeLocationHint *hint, OBitmapHeapPlanState *bitmap_state)
{
	OTableDescr *descr;
	TupleTableSlot *scan_slot;
	MemoryContext oldcxt;
	bool		fetched = false;
	
	Assert(slot);

	descr = relation_get_descr(node->ss.ss_currentRelation);
	*slot = node->ss.ss_ScanTupleSlot;
	oldcxt = MemoryContextSwitchTo((*slot)->tts_mcxt);
	scan_slot = MakeSingleTupleTableSlot(descr->tupdesc,
											&TTSOpsOrioleDB);
	MemoryContextSwitchTo(oldcxt);
	tts_orioledb_store_tuple(scan_slot, *tuple,
								descr, tupleCsn,
								PrimaryIndexNumber,
								true, hint);

	if (scan->tbmres && scan->tbmres->recheck)
	{
		ExprContext *tup_econtext = bitmap_state->scan->ss->ps.ps_ExprContext;
		ExprState  *bitmapqualorig_state;

		bitmapqualorig_state = ExecInitQual(bitmap_state->bitmapqualorig, NULL);

		slot_getallattrs(scan_slot);
		tup_econtext->ecxt_scantuple = scan_slot;

		if (!ExecQual((ExprState *) bitmapqualorig_state, tup_econtext))
		{
			pfree(tuple->data);
			*slot = ExecClearTuple(node->ss.ss_ScanTupleSlot);
		}
		else
		{
			*slot = scan_slot;
			fetched = true;
		}
	}
	else
	{
		*slot = scan_slot;
		fetched = true;
	}
	
	return fetched;
}

TupleTableSlot *
o_exec_bitmap_fetch(OBitmapScan *scan, CustomScanState *node)
{
	bool		fetched;
	TupleTableSlot *slot = NULL;
	OCustomScanState *ocstate = (OCustomScanState *) node;
	OBitmapHeapPlanState *bitmap_state =
		(OBitmapHeapPlanState *) ocstate->o_plan_state;

	do
	{
		OTuple		tuple;
		BTreeLocationHint hint;
		MemoryContext tupleCxt = node->ss.ss_ScanTupleSlot->tts_mcxt;
		CommitSeqNo tupleCsn;

		fetched = false;

		if (scan->tbmiterator != NULL && scan->tbmres == NULL)
		{
			scan->tbmres = tbm_iterate(scan->tbmiterator);

			if (scan->tbmres == NULL)
			{
				slot = ExecClearTuple(node->ss.ss_ScanTupleSlot);
				fetched = true;
			}

			if (!fetched)
				o_tbmiterator_next_page(scan, bitmap_state);
		}

		if (!fetched)
		{
			ItemPointerData iptr_storage;

			/* Adaptive bitmap scan strategy */
			if (scan->arg.bitmap && !scan->bitmap_exhausted)
			{
				OIndexDescr *primary = GET_PRIMARY(scan->arg.tbl_desc);
				
				/* Check if we should use range iteration for dense regions */
				if (!scan->using_range_scan)
				{
					uint64 range_start, range_end;
					
					if (should_use_range_iteration(primary,
												   scan->arg.bitmap, 
												   scan->current_bitmap_value,
												   &range_start, &range_end))
					{
						/* Start range iteration for dense region */
						OBTreeKeyBound start_bound;
						
						fill_key_bound_from_uint64(&start_bound, primary, range_start, &iptr_storage);
						
						scan->range_iterator = o_btree_iterator_create(&primary->desc,
																	 (Pointer) &start_bound, BTreeKeyBound,
																	 &scan->oSnapshot, ForwardScanDirection);
						scan->using_range_scan = true;
						scan->range_end_value = range_end;
						scan->current_bitmap_value = range_start;
					}
				}
				
				/* If using range iteration, fetch from iterator */
				if (scan->using_range_scan)
				{
					OBTreeKeyBound end_bound;
					/* Build end bound for iterator */
					fill_key_bound_from_uint64(&end_bound, primary, scan->range_end_value, &iptr_storage);
					
					/* zero out hint */
					{
						hint.blkno = OInvalidInMemoryBlkno;
						hint.pageChangeCount = InvalidOPageChangeCount;
					}
					/* Fetch next tuple from iterator */
					tuple = o_btree_iterator_fetch(scan->range_iterator, &tupleCsn,
												   (Pointer) &end_bound, BTreeKeyBound,
												   true, &hint);
					
					if (O_TUPLE_IS_NULL(tuple))
					{
						/* End of range, switch back to direct lookup */
						btree_iterator_free(scan->range_iterator);
						scan->range_iterator = NULL;
						scan->using_range_scan = false;
						scan->current_bitmap_value = scan->range_end_value + 1;
						continue;
					}
					else
					{
						uint64 tuple_value = primary_tuple_get_data(tuple, primary, false);
						
						/* Check if this tuple is in the bitmap */
						if (o_keybitmap_test(scan->arg.bitmap, tuple_value))
						{
							scan->current_bitmap_value = tuple_value + 1;
						
							/* Save tuple in slot */
							fetched = o_bitmap_store_tuple(scan, node, &slot,
														 &tuple, tupleCsn,
														 &hint, bitmap_state);
						}
						else
						{
							/* Tuple not in bitmap, continue */
							pfree(tuple.data);
						}
					}
				}
				else
				{
					/* Use direct lookup for sparse regions */
					OBTreeKeyBound bound;
					uint64		next_value;
					bool		found;

					/* Get next value from bitmap */
					next_value = o_keybitmap_get_next(scan->arg.bitmap,
													  scan->current_bitmap_value,
													  &found);

					if (!found)
					{
						/* No more entries in bitmap */
						scan->bitmap_exhausted = true;
						slot = ExecClearTuple(node->ss.ss_ScanTupleSlot);
						fetched = true;
					}
					else
					{
						/* Update current position for next iteration */
						scan->current_bitmap_value = next_value + 1;
						
						/* Fill bound structure from uint64 value */
						fill_key_bound_from_uint64(&bound, primary, next_value, &iptr_storage);

						/* Lookup the tuple directly */
						o_btree_load_shmem(&primary->desc);
						
						/* zero out hint */
						{
							hint.blkno = OInvalidInMemoryBlkno;
							hint.pageChangeCount = InvalidOPageChangeCount;
						}
	
						tuple = o_btree_find_tuple_by_key(&primary->desc,
														 (Pointer) &bound, BTreeKeyBound,
														 &scan->oSnapshot, &tupleCsn,
														 tupleCxt, &hint);

						if (O_TUPLE_IS_NULL(tuple))
						{
							/* Tuple not found, skip to next */
							continue;
						}
						else
						{							
							/* Save tuple in slot */
							fetched = o_bitmap_store_tuple(scan, node, &slot,
														 &tuple, tupleCsn,
														 &hint, bitmap_state);
						}

						if (scan->tbmres)
						{
							scan->cur_tuple++;
							if (scan->cur_tuple >= scan->page_ntuples)
								scan->tbmres = NULL;
						}
					}
				}
			}
			else
			{
				/* Bitmap exhausted or not using bitmap */
				slot = ExecClearTuple(node->ss.ss_ScanTupleSlot);
				fetched = true;
			}
		}

		if (!fetched)
			InstrCountFiltered2(node, 1);
		else if (!TupIsNull(slot) && !o_exec_qual(node->ss.ps.ps_ExprContext,
												  node->ss.ps.qual, slot))
			InstrCountFiltered1(node, 1);

	} while (!fetched || (!TupIsNull(slot) &&
						  !o_exec_qual(node->ss.ps.ps_ExprContext,
									   node->ss.ps.qual, slot)));
	return slot;
}

void
o_free_bitmap_scan(OBitmapScan *scan)
{
	if (scan->range_iterator)
		btree_iterator_free(scan->range_iterator);
	if (scan->arg.bitmap)
		o_keybitmap_free(scan->arg.bitmap);
	if (scan->tbmiterator)
		tbm_end_iterate(scan->tbmiterator);
	if (scan->arg.bridged_bitmap)
		tbm_free(scan->arg.bridged_bitmap);
	pfree(scan);
}
