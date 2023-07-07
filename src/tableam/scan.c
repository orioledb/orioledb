/*-------------------------------------------------------------------------
 *
 * scan.c
 *		Scan Provider for orioledb tables.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/scan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/iterator.h"
#include "btree/scan.h"
#include "recovery/recovery.h"
#include "tableam/bitmap_scan.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "tableam/index_scan.h"
#include "tableam/scan.h"
#include "tuple/slot.h"
#include "utils/stopevent.h"

#include "access/relation.h"
#include "access/table.h"
#include "common/hashfn.h"
#include "executor/nodeModifyTable.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/spccache.h"

#include <math.h>

typedef enum OPathTag
{
	O_IndexPath,
	O_BitmapHeapPath,
} OPathTag;

typedef struct OPath
{
	OPathTag	type;
} OPath;

typedef struct OIndexPath
{
	OPath		o_path;
	ScanDirection scandir;
	OIndexNumber ix_num;
} OIndexPath;

typedef struct OBitmapHeapPath
{
	OPath		o_path;
} OBitmapHeapPath;

typedef struct OIndexPlanState
{
	OPlanState	o_plan_state;
	OScanState	ostate;
	/* Used only in o_explain_custom_scan */
	List	   *stripped_indexquals;
	bool		onlyCurIx;
} OIndexPlanState;

typedef struct OCustomScanState
{
	CustomScanState css;
	bool		useEaCounters;
	OEACallsCounters eaCounters;
	OPlanState *o_plan_state;
} OCustomScanState;

set_rel_pathlist_hook_type old_set_rel_pathlist_hook = NULL;
OEACallsCounters *ea_counters = NULL;

/* custom scan */
static Plan *o_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
								CustomPath *best_path, List *tlist,
								List *clauses, List *custom_plans);
static void o_begin_custom_scan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *o_exec_custom_scan(CustomScanState *node);
static void o_end_custom_scan(CustomScanState *node);
static void o_rescan_custom_scan(CustomScanState *node);
static void o_explain_custom_scan(CustomScanState *node, List *ancestors,
								  ExplainState *es);
static Node *o_create_custom_scan_state(CustomScan *cscan);

static CustomPathMethods o_path_methods =
{
	.CustomName = "o_path",
	.PlanCustomPath = o_plan_custom_path
};

CustomScanMethods o_scan_methods =
{
	"o_scan",
	o_create_custom_scan_state
};

static CustomExecMethods o_scan_exec_methods =
{
	"o_exec_scan",
	o_begin_custom_scan,
	o_exec_custom_scan,
	o_end_custom_scan,
	o_rescan_custom_scan,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	o_explain_custom_scan
};

bool
is_o_custom_scan(CustomScan *scan)
{
	return scan->methods == &o_scan_methods;
}

bool
is_o_custom_scan_state(CustomScanState *scan)
{
	return scan->methods == &o_scan_exec_methods;
}

static Path *
transform_path(Path *src_path, OTableDescr *descr)
{
	CustomPath *result;

	Assert(IsA(src_path, IndexPath) || IsA(src_path, Path) ||
		   IsA(src_path, BitmapHeapPath));

	result = makeNode(CustomPath);
	result->path.pathtype = T_CustomScan;
	result->path.parent = src_path->parent;
	result->path.pathtarget = src_path->pathtarget;
	result->path.param_info = src_path->param_info;
	result->path.rows = src_path->rows;
	result->path.startup_cost = src_path->startup_cost;
	result->path.total_cost = src_path->total_cost;
	result->path.pathkeys = src_path->pathkeys;
	result->path.parallel_aware = src_path->parallel_aware;
	result->path.parallel_safe = src_path->parallel_safe;
	result->path.parallel_workers = src_path->parallel_workers;
	result->methods = &o_path_methods;
	result->custom_paths = list_make1(src_path);

	if (IsA(src_path, Path))
	{
		OIndexPath *new_path = palloc0(sizeof(OIndexPath));

		new_path->o_path.type = O_IndexPath;
		new_path->ix_num = PrimaryIndexNumber;
		new_path->scandir = ForwardScanDirection;
		result->custom_private = list_make1(new_path);
	}
	else if (IsA(src_path, IndexPath))
	{
		IndexPath  *ix_path = (IndexPath *) src_path;
		OIndexNumber ix_num;
		OIndexDescr *index_descr;
		OIndexPath *new_path = palloc0(sizeof(OIndexPath));

		/* Find ix_num */
		for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
		{
			index_descr = descr->indices[ix_num];
			if (index_descr->oids.reloid == ix_path->indexinfo->indexoid)
				break;
		}
		Assert(ix_num < descr->nIndices);
		new_path->o_path.type = O_IndexPath;
		new_path->ix_num = ix_num;
		new_path->scandir = ix_path->indexscandir;
		result->custom_private = list_make1(new_path);
	}
	else if (IsA(src_path, BitmapHeapPath))
	{
		OBitmapHeapPath *new_path = palloc0(sizeof(OBitmapHeapPath));

		new_path->o_path.type = O_BitmapHeapPath;
		result->custom_private = list_make1(new_path);
	}
	return &result->path;
}

bool
orioledb_set_plain_rel_pathlist_hook(PlannerInfo *root, RelOptInfo *rel,
									 RangeTblEntry *rte)
{
	bool		result = true;

	if (rte->rtekind == RTE_RELATION &&
		(rte->relkind == RELKIND_RELATION || rte->relkind == RELKIND_MATVIEW))
	{
		Relation	relation = relation_open(rte->relid, NoLock);

		if (is_orioledb_rel(relation))
		{
			ListCell   *lc;
			int			i;
			int			nfields;
			ORelOids	oids;
			OTable	   *o_table;

			ORelOidsSetFromRel(oids, relation);
			o_table = o_tables_get(oids);

			Assert(o_table);

			if (o_table->has_primary)
			{
				/*
				 * Additional pkey fields are added to index target list so
				 * that the index only scan is selected
				 */
				nfields = o_table->indices[PrimaryIndexNumber].nfields;

				for (i = 0; i < nfields; i++)
				{
					OTableIndexField *pk_field;

					pk_field = &o_table->indices[PrimaryIndexNumber].fields[i];

					foreach(lc, rel->indexlist)
					{
						IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);
						int			col;
						bool		member = false;

						for (col = 0; col < index->ncolumns; col++)
						{
							if (pk_field->attnum + 1 == index->indexkeys[col])
							{
								member = true;
								break;
							}
						}

						if (!member)
						{
							Expr	   *indexvar;
							const FormData_pg_attribute *att_tup;

							index->ncolumns++;
							index->indexkeys = (int *)
								repalloc(index->indexkeys,
										 sizeof(int) * index->ncolumns);
							index->indexkeys[index->ncolumns - 1] =
								pk_field->attnum + 1;
							index->canreturn = (bool *)
								repalloc(index->canreturn,
										 sizeof(bool) * index->ncolumns);
							index->canreturn[index->ncolumns - 1] = true;

							att_tup = TupleDescAttr(relation->rd_att,
													pk_field->attnum);

							indexvar = (Expr *) makeVar(index->rel->relid,
														pk_field->attnum + 1,
														att_tup->atttypid,
														att_tup->atttypmod,
														att_tup->attcollation,
														0);

							index->indextlist =
								lappend(index->indextlist,
										makeTargetEntry(indexvar,
														index->ncolumns,
														NULL,
														false));
						}
					}
				}

				foreach(lc, rel->indexlist)
				{
					IndexClauseSet rclauseset;
					IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

					if (index->indpred != NIL && !index->predOK)
						continue;

					MemSet(&rclauseset, 0, sizeof(rclauseset));
					match_restriction_clauses_to_index(root, index, &rclauseset);

					result = !rclauseset.nonempty;
				}
			}
		}
		relation_close(relation, NoLock);
	}

	return result;
}

/*
 * Removes all index and base relation scan paths for a orioledb TableAm table.
 */
void
orioledb_set_rel_pathlist_hook(PlannerInfo *root, RelOptInfo *rel,
							   Index rti, RangeTblEntry *rte)
{
	if (rte->rtekind == RTE_RELATION &&
		(rte->relkind == RELKIND_RELATION || rte->relkind == RELKIND_MATVIEW))
	{
		Relation	relation = table_open(rte->relid, NoLock);

		/* orioledb relation */
		if (is_orioledb_rel(relation))
		{
			OTableDescr *descr;
			bool		seq_path_needed;
			int			i;

			descr = relation_get_descr(relation);
			Assert(descr != NULL);

			/*
			 * transform all postgres scans to custom scans
			 */
			seq_path_needed = list_length(rel->pathlist) == 1 ||
				descr->indices[PrimaryIndexNumber]->primaryIsCtid;
			i = 0;
			while (i < list_length(rel->pathlist))
			{
				Path	   *path = list_nth(rel->pathlist, i);

				if (IsA(path, IndexPath) ||
					IsA(path, Path) ||
					IsA(path, BitmapHeapPath))
				{
					if (IsA(path, Path) && path->pathtype == T_SampleScan)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("orioledb table \"%s\" does not "
										"support TABLESAMPLE",
										RelationGetRelationName(relation))),
								errdetail("Sample scan is not supported for "
										  "OrioleDB tables yet. Please send a "
										  "bug report."));
					}

					if (!IsA(path, Path))
						rel->pathlist = list_delete_nth_cell(rel->pathlist, i);
					else
						i++;
					if (seq_path_needed || !IsA(path, Path))
					{
						Path	   *custom_path = transform_path(path, descr);

						rel->pathlist = list_insert_nth(rel->pathlist, i,
														custom_path);
						i++;
					}
				}
				else
				{
					i++;
				}
			}
		}

		if (relation != NULL)
			table_close(relation, NoLock);
	}

	/*
	 * else it is not relation: nothing to do
	 */

	if (old_set_rel_pathlist_hook != NULL)
		old_set_rel_pathlist_hook(root, rel, rti, rte);
}

/*
 * Creates orioledb CustomScan plan from orioledb CustomPath.
 */
static Plan *
o_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
				   CustomPath *best_path, List *tlist,
				   List *clauses, List *custom_plans)
{
	OPath	   *o_path = linitial(best_path->custom_private);
	CustomScan *custom_scan = makeNode(CustomScan);
	Plan	   *plan = &custom_scan->scan.plan;
	List	   *qpqual = NIL;
	RangeTblEntry *rte;
	Oid			reloid;
	Relation	relation;
	OTableDescr *descr;
	Plan	   *custom_plan;

	rte = planner_rt_fetch(rel->relid, root);
	Assert(rte->rtekind == RTE_RELATION);
	reloid = rte->relid;

	relation = table_open(reloid, NoLock);
	Assert(relation != NULL);
	descr = relation_get_descr(relation);
	Assert(descr != NULL);

	plan->lefttree = NULL;
	plan->righttree = NULL;
	plan->initPlan = NULL;
	/* plan costs will be filled by create_customscan_plan */

	custom_scan->scan.scanrelid = rel->relid;
	custom_scan->flags = best_path->flags;
	custom_scan->methods = &o_scan_methods;
	custom_scan->custom_plans = custom_plans;
	/* custom_scan->custom_relids will be filled by create_customscan_plan */
	custom_scan->custom_relids = NULL;

	Assert(custom_plans);
	custom_plan = (Plan *) linitial(custom_plans);
	if (IsA(custom_plan, Result))
		custom_plan = outerPlan(custom_plan);

	if (o_path->type == O_IndexPath)
	{
		OIndexPath *ix_path = (OIndexPath *) o_path;
		bool		onlyCurIx = IsA(custom_plan, IndexOnlyScan);

		if (custom_plans && IsA(custom_plan, IndexScan))
		{
			IndexScan  *ix_scan = (IndexScan *) custom_plan;

			plan->targetlist = ix_scan->scan.plan.targetlist;
			custom_scan->custom_scan_tlist = NIL;
			qpqual = ix_scan->scan.plan.qual;
		}
		else if (custom_plans && IsA(custom_plan, IndexOnlyScan))
		{
			IndexOnlyScan *ixo_scan = (IndexOnlyScan *) custom_plan;

			plan->targetlist = ixo_scan->scan.plan.targetlist;
			custom_scan->custom_scan_tlist = ixo_scan->indextlist;
			qpqual = ixo_scan->scan.plan.qual;
		}

		custom_scan->custom_exprs = NIL;
		custom_scan->custom_private =
			list_make4(makeInteger(O_IndexPlan),
					   makeInteger(ix_path->ix_num),
					   makeInteger(ix_path->scandir),
					   makeInteger(onlyCurIx));
	}
	else
	{
		BitmapHeapScan *bh_scan = (BitmapHeapScan *) custom_plan;
		OIndexDescr *primary = GET_PRIMARY(descr);

		custom_scan->scan.plan.targetlist =
			copyObject(bh_scan->scan.plan.targetlist);
		qpqual = bh_scan->scan.plan.qual;

		Assert(primary->nFields == 1);
		custom_scan->custom_private =
			list_make2(makeInteger(O_BitmapHeapPlan),
					   makeInteger(primary->fields[0].inputtype));
	}

	table_close(relation, NoLock);
	plan->qual = qpqual;
	return (Plan *) custom_scan;
}

/*
 * Custom scan.
 */

/*
 * Creates OCustomScanState.
 */
Node *
o_create_custom_scan_state(CustomScan *cscan)
{
	Node	   *node = palloc0fast(sizeof(OCustomScanState));
	OCustomScanState *ocstate = (OCustomScanState *) node;
	OPlanTag	plan_tag = intVal(linitial(cscan->custom_private));
	Plan	   *custom_plan;

	node->type = T_CustomScanState;
	ocstate->css.methods = &o_scan_exec_methods;
	ocstate->css.slotOps = &TTSOpsOrioleDB;

	Assert(cscan->custom_plans);
	custom_plan = (Plan *) linitial(cscan->custom_plans);
	if (IsA(custom_plan, Result))
		custom_plan = outerPlan(custom_plan);

	if (plan_tag == O_IndexPlan)
	{
		OIndexPlanState *ix_plan_state =
			(OIndexPlanState *) palloc0(sizeof(OIndexPlanState));

		ix_plan_state->ostate.ixNum = intVal(lsecond(cscan->custom_private));
		ix_plan_state->ostate.scanDir = intVal(lthird(cscan->custom_private));
		ix_plan_state->ostate.iterator = NULL;
		ix_plan_state->ostate.curKeyRangeIsLoaded = false;
		ix_plan_state->ostate.curKeyRange.empty = true;
		ix_plan_state->ostate.curKeyRange.low.n_row_keys = 0;
		ix_plan_state->ostate.curKeyRange.high.n_row_keys = 0;

		if (IsA(custom_plan, IndexScan))
		{
			IndexScan  *ix_scan = (IndexScan *) custom_plan;

			ix_plan_state->stripped_indexquals =
				copyObject(ix_scan->indexqualorig);
			ix_plan_state->ostate.indexQuals = copyObject(ix_scan->indexqual);
		}
		else if (IsA(custom_plan, IndexOnlyScan))
		{
			IndexOnlyScan *ixo_scan = (IndexOnlyScan *) custom_plan;

			ix_plan_state->stripped_indexquals =
				copyObject(ixo_scan->indexqual);
			ix_plan_state->ostate.indexQuals = copyObject(ixo_scan->indexqual);
		}
		else
		{
			Assert(false);
		}
		ix_plan_state->ostate.onlyCurIx =
			intVal(lfourth(cscan->custom_private));

		ocstate->o_plan_state = (OPlanState *) ix_plan_state;
	}
	else if (plan_tag == O_BitmapHeapPlan)
	{
		OBitmapHeapPlanState *bitmap_state =
			(OBitmapHeapPlanState *) palloc0(sizeof(OBitmapHeapPlanState));
		BitmapHeapScan *bh_scan = (BitmapHeapScan *) custom_plan;

		bitmap_state->typeoid = intVal(lsecond(cscan->custom_private));
		bitmap_state->bitmapqualplan = copyObject(bh_scan->scan.plan.lefttree);
		bitmap_state->bitmapqualorig = copyObject(bh_scan->bitmapqualorig);
		ocstate->o_plan_state = (OPlanState *) bitmap_state;
	}
	else
	{
		Assert(false);
	}
	ocstate->o_plan_state->type = plan_tag;

	return node;
}

/*
 * Initializes OCustomScanState and prepares for scan.
 */
void
o_begin_custom_scan(CustomScanState *node, EState *estate, int eflags)
{
	OCustomScanState *ocstate = (OCustomScanState *) node;

	ocstate->useEaCounters = is_explain_analyze(&node->ss.ps);

	if (ocstate->useEaCounters)
	{
		OTableDescr *descr;

		descr = relation_get_descr(node->ss.ss_currentRelation);
		eanalyze_counters_init(&ocstate->eaCounters, descr);
	}

	if (ocstate->o_plan_state->type == O_IndexPlan)
	{
		OIndexPlanState *ix_plan_state =
			(OIndexPlanState *) ocstate->o_plan_state;
		OScanState *scan_state = &ix_plan_state->ostate;

		scan_state->csn = estate->es_snapshot->snapshotcsn;

		ix_plan_state->ostate.cxt = AllocSetContextCreate(estate->es_query_cxt,
														  "orioledb_cs plan data",
														  ALLOCSET_DEFAULT_SIZES);
	}
	else if (ocstate->o_plan_state->type == O_BitmapHeapPlan)
	{
		OBitmapHeapPlanState *bitmap_state =
			(OBitmapHeapPlanState *) ocstate->o_plan_state;

		bitmap_state->bitmapqualplanstate =
			ExecInitNode(bitmap_state->bitmapqualplan, estate, eflags);

		if (ocstate->useEaCounters)
		{
			int			i;
			OTableDescr *descr;

			descr = relation_get_descr(node->ss.ss_currentRelation);
			bitmap_state->eaCounters = palloc0(sizeof(OEACallsCounters) *
											   descr->nIndices);
			for (i = 0; i < descr->nIndices; i++)
			{
				eanalyze_counters_init(&bitmap_state->eaCounters[i], descr);
			}
		}

		bitmap_state->csn = estate->es_snapshot->snapshotcsn;
		bitmap_state->cxt = AllocSetContextCreate(estate->es_query_cxt,
												  "orioledb_cs plan data",
												  ALLOCSET_DEFAULT_SIZES);
	}
}

/*
 * Iterates custom scan.
 */
TupleTableSlot *
o_exec_custom_scan(CustomScanState *node)
{
	OCustomScanState *ocstate = (OCustomScanState *) node;
	EPQState   *epqstate;
	CommitSeqNo csn;
	TupleTableSlot *slot = NULL;

	if (ocstate->useEaCounters)
		ea_counters = &ocstate->eaCounters;
	else
		ea_counters = NULL;

	if (ocstate->o_plan_state->type == O_IndexPlan)
	{
		OIndexPlanState *ix_plan_state =
			(OIndexPlanState *) ocstate->o_plan_state;

		csn = ix_plan_state->ostate.csn;

		epqstate = node->ss.ps.state->es_epq_active;
		if (epqstate)
		{
			Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;

			if (epqstate->relsubs_done[scanrelid - 1])
			{
				/*
				 * Return empty slot, as we already performed an EPQ
				 * substitution for this relation.
				 */

				slot = node->ss.ss_ScanTupleSlot;

				/* Return empty slot, as we already returned a tuple */
				return ExecClearTuple(slot);
			}
			else if (epqstate->relsubs_slot[scanrelid - 1] != NULL)
			{
				/*
				 * Return replacement tuple provided by the EPQ caller.
				 */

				slot = epqstate->relsubs_slot[scanrelid - 1];

				Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);

				/* Mark to remember that we shouldn't return more */
				epqstate->relsubs_done[scanrelid - 1] = true;

				/* Return empty slot if we haven't got a test tuple */
				if (TupIsNull(slot))
					return NULL;

				if (!o_exec_qual(node->ss.ps.ps_ExprContext,
								 node->ss.ps.qual, slot))
					return NULL;

				slot = o_exec_project(node->ss.ps.ps_ProjInfo,
									  node->ss.ps.ps_ExprContext, slot, NULL);

				return slot;
			}
		}

		if (ix_plan_state->ostate.scandesc == NULL)
		{
			Relation	rel = node->ss.ss_currentRelation;
			OTableDescr *descr = relation_get_descr(rel);
			OIndexNumber ix_num = ix_plan_state->ostate.ixNum;
			OIndexDescr *ix_descr = descr->indices[ix_num];
			Relation	index = index_open(ix_descr->oids.reloid,
										   AccessShareLock);

			init_index_scan_state(&ix_plan_state->ostate, index,
								  node->ss.ps.ps_ExprContext);
			index_close(index, AccessShareLock);
		}

		slot = o_exec_fetch(&ix_plan_state->ostate, &node->ss, csn);
	}
	else if (ocstate->o_plan_state->type == O_BitmapHeapPlan)
	{
		OBitmapHeapPlanState *bitmap_state =
			(OBitmapHeapPlanState *) ocstate->o_plan_state;

		if (bitmap_state->scan == NULL)
		{
			PlanState  *bitmapqualplanstate = bitmap_state->bitmapqualplanstate;
			Relation	rel = node->ss.ss_currentRelation;

			bitmap_state->scan = o_make_bitmap_scan(bitmap_state,
													&node->ss,
													bitmapqualplanstate,
													rel,
													bitmap_state->typeoid,
													bitmap_state->csn,
													bitmap_state->cxt);
		}

		slot = o_exec_bitmap_fetch(bitmap_state->scan, node);
	}

	slot = o_exec_project(node->ss.ps.ps_ProjInfo, node->ss.ps.ps_ExprContext,
						  slot, NULL);

	return slot;
}

/*
 * Restarts the scan.
 */
void
o_rescan_custom_scan(CustomScanState *node)
{
	OCustomScanState *ocstate = (OCustomScanState *) node;

	if (ocstate->o_plan_state->type == O_IndexPlan)
	{
		OIndexPlanState *ix_plan_state =
			(OIndexPlanState *) ocstate->o_plan_state;

		if (node->ss.ps.chgParam != NULL)
		{
			MemoryContextReset(ix_plan_state->ostate.cxt);
		}
		else if (ix_plan_state->ostate.iterator != NULL)
		{
			btendscan(ix_plan_state->ostate.scandesc);
			btree_iterator_free(ix_plan_state->ostate.iterator);
		}

		ix_plan_state->ostate.curKeyRangeIsLoaded = false;
		ix_plan_state->ostate.curKeyRange.empty = true;
		ix_plan_state->ostate.curKeyRange.low.n_row_keys = 0;
		ix_plan_state->ostate.curKeyRange.high.n_row_keys = 0;
		ix_plan_state->ostate.iterator = NULL;
		ix_plan_state->ostate.scandesc = NULL;
	}
	else if (ocstate->o_plan_state->type == O_BitmapHeapPlan)
	{
		OBitmapHeapPlanState *bitmap_state;

		bitmap_state = (OBitmapHeapPlanState *) ocstate->o_plan_state;

		if (bitmap_state->scan)
			o_free_bitmap_scan(bitmap_state->scan);

		if (ocstate->useEaCounters)
			pfree(bitmap_state->eaCounters);
		bitmap_state->scan = NULL;
	}
}

/*
 * Ends custom scan.
 */
void
o_end_custom_scan(CustomScanState *node)
{
	OCustomScanState *ocstate = (OCustomScanState *) node;

	STOPEVENT(STOPEVENT_SCAN_END, NULL);

	if (ocstate->o_plan_state->type == O_IndexPlan)
	{
		OIndexPlanState *ix_plan_state;

		ix_plan_state = (OIndexPlanState *) ocstate->o_plan_state;

		if (ix_plan_state->ostate.iterator != NULL)
			btree_iterator_free(ix_plan_state->ostate.iterator);
		MemoryContextDelete(ix_plan_state->ostate.cxt);
		ix_plan_state->ostate.cxt = NULL;
	}
	else if (ocstate->o_plan_state->type == O_BitmapHeapPlan)
	{
		OBitmapHeapPlanState *bitmap_state =
			(OBitmapHeapPlanState *) ocstate->o_plan_state;

		if (bitmap_state->bitmapqualplanstate)
			ExecEndNode(bitmap_state->bitmapqualplanstate);
		MemoryContextReset(bitmap_state->cxt);
		if (bitmap_state->scan)
			o_free_bitmap_scan(bitmap_state->scan);
		if (ocstate->useEaCounters)
			pfree(bitmap_state->eaCounters);
	}
	ea_counters = NULL;
}

typedef struct OExplainContext
{
	List	   *ancestors;
	ExplainState *es;
	OCustomScanState *ocstate;
	OTableDescr *descr;
} OExplainContext;

static bool
o_explain_node(PlanState *planstate, OExplainContext *ec)
{
	bool		result;

	if (planstate == NULL)
		return false;

	switch (planstate->type)
	{
		case T_BitmapOrState:
			{
				BitmapOrState *node = (BitmapOrState *) planstate;
				int			saved_nplans = node->nplans;

				node->nplans = 0;
				ExplainNode(planstate, ec->ancestors, "Outer", NULL, ec->es);
				ec->es->indent += 3;
				node->nplans = saved_nplans;
				break;
			}
		case T_BitmapAndState:
			{
				BitmapAndState *node = (BitmapAndState *) planstate;
				int			saved_nplans = node->nplans;

				node->nplans = 0;
				ExplainNode(planstate, ec->ancestors, "Outer", NULL, ec->es);
				ec->es->indent += 3;
				node->nplans = saved_nplans;
				break;
			}
		case T_BitmapIndexScanState:
			{
				OCustomScanState *ocstate = ec->ocstate;

				ExplainNode(planstate, ec->ancestors, "Outer", NULL, ec->es);
				switch (ec->es->format)
				{
					case EXPLAIN_FORMAT_TEXT:
						ec->es->indent += 3;
						break;
					case EXPLAIN_FORMAT_JSON:
						{
							int			i;

							ec->es->str->len--;
							for (i = ec->es->str->len; i > 0; i--)
							{
								if (ec->es->str->data[i - 1] == '\n')
									break;
							}
							ec->es->str->len -= (ec->es->str->len - i) + 1;
							for (i = ec->es->str->len; i > 0; i--)
							{
								if (ec->es->str->data[i - 1] != ' ')
									break;
							}
							ec->es->indent++;
						}
						break;
					case EXPLAIN_FORMAT_XML:
						{
							int			i;

							ec->es->str->len--;
							for (i = ec->es->str->len; i > 0; i--)
							{
								if (ec->es->str->data[i - 1] == '\n')
									break;
							}
							ec->es->str->len -= (ec->es->str->len - i);
							ec->es->indent++;
						}
						break;
					case EXPLAIN_FORMAT_YAML:
						ec->es->indent++;
						break;
				}
				if (ocstate->useEaCounters)
				{
					OIndexNumber ix_num;
					BitmapIndexScan *bm_scan;
					OBitmapHeapPlanState *o_bm_state;
					OTableDescr *descr = ec->descr;
					OEACallsCounters *eaCounters;

					bm_scan = ((BitmapIndexScan *) planstate->plan);
					o_bm_state = (OBitmapHeapPlanState *) ocstate->o_plan_state;
					eaCounters = o_bm_state->eaCounters;
					for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
					{
						OIndexDescr *indexDescr = descr->indices[ix_num];

						if (indexDescr->oids.reloid == bm_scan->indexid)
							break;
					}
					Assert(ix_num < descr->nIndices);
					eanalyze_counters_explain(descr, &eaCounters[ix_num], ec->es);
				}
				switch (ec->es->format)
				{
					case EXPLAIN_FORMAT_TEXT:
						ec->es->indent -= 3;
						break;
					case EXPLAIN_FORMAT_JSON:
					case EXPLAIN_FORMAT_XML:
					case EXPLAIN_FORMAT_YAML:
						ExplainCloseGroup("Plan", "Plan", true, ec->es);
						break;
				}
				break;
			}
		default:
			elog(ERROR, "can't explain node: %d", planstate->type);
			break;
	}

	result = planstate_tree_walker(planstate, o_explain_node, ec);
	switch (planstate->type)
	{
		case T_BitmapOrState:
			{
				ec->es->indent -= 3;
				break;
			}
		case T_BitmapAndState:
			ec->es->indent -= 3;
			break;
		default:
			break;
	}
	return result;
}

/*
 * Explains custom scan.
 */
void
o_explain_custom_scan(CustomScanState *node, List *ancestors, ExplainState *es)
{
	OCustomScanState *ocstate = (OCustomScanState *) node;
	OTableDescr *descr;
	char	   *indexName;
	StringInfoData title;

	descr = relation_get_descr(node->ss.ss_currentRelation);

	if (ocstate->o_plan_state->type == O_IndexPlan)
	{
		OIndexPlanState *ix_plan_state =
			(OIndexPlanState *) ocstate->o_plan_state;
		bool		backward = (ix_plan_state->ostate.scanDir ==
								BackwardScanDirection);
		char	   *direction = !backward ? "Forward" : "Backward";

		initStringInfo(&title);
		indexName = descr->indices[ix_plan_state->ostate.ixNum]->name.data;

		switch (es->format)
		{
			case EXPLAIN_FORMAT_TEXT:
				appendStringInfo(&title, "%s index %sscan of", direction,
								 ix_plan_state->ostate.onlyCurIx ? "only " : "");
				ExplainPropertyText(title.data, indexName, es);
				show_scan_qual(ix_plan_state->stripped_indexquals, "Conds",
							   &node->ss.ps, ancestors, es);
				break;

			case EXPLAIN_FORMAT_XML:
			case EXPLAIN_FORMAT_YAML:
			case EXPLAIN_FORMAT_JSON:
				ExplainPropertyText("Scan Direction", direction, es);
				ExplainPropertyText("Index Name", indexName, es);
				if (ix_plan_state->ostate.onlyCurIx)
					ExplainPropertyText("Custom Scan Subtype", "Index Only Scan",
										es);
				else
					ExplainPropertyText("Custom Scan Subtype", "Index Scan", es);
				show_scan_qual(ix_plan_state->stripped_indexquals, "Index Cond",
							   &node->ss.ps, ancestors, es);
				break;
		}

		if (ix_plan_state->stripped_indexquals)
			show_instrumentation_count("Rows Removed by Index Recheck", 2,
									   &node->ss.ps, es);
		if (node->ss.ps.qual)
			show_instrumentation_count("Rows Removed by Filter", 1,
									   &node->ss.ps, es);
	}
	else if (ocstate->o_plan_state->type == O_BitmapHeapPlan)
	{
		OBitmapHeapPlanState *bitmap_state =
			(OBitmapHeapPlanState *) ocstate->o_plan_state;

		switch (es->format)
		{
			case EXPLAIN_FORMAT_TEXT:
				appendStringInfoSpaces(es->str, es->indent * 2);
				appendStringInfoString(es->str, "Bitmap heap scan\n");
				break;

			case EXPLAIN_FORMAT_XML:
			case EXPLAIN_FORMAT_YAML:
			case EXPLAIN_FORMAT_JSON:
				ExplainPropertyText("Custom Scan Subtype", "Bitmap Heap Scan", es);
				break;
		}

		show_scan_qual(bitmap_state->bitmapqualorig, "Recheck Cond",
					   &node->ss.ps, ancestors, es);
		if (bitmap_state->bitmapqualorig)
			show_instrumentation_count("Rows Removed by Index Recheck", 2,
									   &node->ss.ps, es);
		if (node->ss.ps.qual)
			show_instrumentation_count("Rows Removed by Filter", 1,
									   &node->ss.ps, es);

		if (bitmap_state->bitmapqualplanstate)
		{
			OExplainContext ec;

			ExplainOpenGroup("Plans", "Plans", false, es);
			ec.ancestors = list_copy(ancestors);
			ec.es = es;
			ec.ocstate = ocstate;
			ec.descr = descr;
			o_explain_node(bitmap_state->bitmapqualplanstate, &ec);
			ExplainCloseGroup("Plans", "Plans", false, es);
		}
	}
	if (ocstate->useEaCounters)
		eanalyze_counters_explain(descr, &ocstate->eaCounters, es);
}
