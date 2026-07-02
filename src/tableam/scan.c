/*-------------------------------------------------------------------------
 *
 * scan.c
 *		Scan Provider for orioledb tables.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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
#include "transam/oxid.h"
#include "tuple/slot.h"
#include "utils/stopevent.h"

#include "access/relation.h"
#include "access/table.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/nodeIndexscan.h"
#include "executor/nodeModifyTable.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#endif
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "utils/json.h"
#include "utils/datum.h"
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
static bool o_extract_row_array_keys(OIndexDescr *primary, Index scanrelid,
									 bool prefixUsesIndexVars,
									 bool qpqualUsesIndexVars,
									 List *indexqual, List *qpqual,
									 List **out_key_exprs, int *out_ntuples,
									 int *out_nkeys, List **out_key_types);
static void o_maybe_inject_row_array_path(PlannerInfo *root, RelOptInfo *rel,
										  OTableDescr *descr);

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

/* No existing callers */
bool
is_o_custom_scan(CustomScan *scan)
{
	return scan->methods == &o_scan_methods;
}

/* No existing callers */
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

/*
 * If the rel has a row-array-IN on a contiguous prefix of the primary key but
 * no primary index path was generated to serve it pointwise (e.g. a covering
 * secondary index whose leading column matches only the common prefix wins, or
 * a parameterized/generic plan where no common leading value is known), inject
 * a full primary index scan path.  The row-array-IN OR (and any planner-derived
 * leading equality) stays in baserestrictinfo and becomes the scan's qpqual,
 * which o_plan_custom_path turns into N per-tuple probes.
 *
 * This works for SELECT and for UPDATE/DELETE: the primary index scan is
 * transformed into an o_scan that already emits the row identity (rowid) the
 * ModifyTable node needs, exactly like a planner-generated primary index
 * UPDATE/DELETE scan.
 */
static bool
o_rel_is_rowmarked(PlannerInfo *root, Index relid)
{
	ListCell   *lc;

	foreach(lc, root->rowMarks)
	{
		PlanRowMark *rc = lfirst_node(PlanRowMark, lc);

		if (rc->rti == relid)
			return true;
	}
	return false;
}

static void
o_maybe_inject_row_array_path(PlannerInfo *root, RelOptInfo *rel,
							  OTableDescr *descr)
{
	OIndexDescr *primary = GET_PRIMARY(descr);
	IndexOptInfo *primaryIndex = NULL;
	ListCell   *lc;
	bool		matched = false;
	List	   *dummyExprs;
	int			dummyNTuples,
				dummyNKeys;
	List	   *dummyTypes;

	if (o_rel_is_rowmarked(root, rel->relid))
		return;

	if (primary->primaryIsCtid || primary->nUniqueFields <= 1)
		return;

	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

		if (index->indexoid == primary->oids.reloid)
		{
			primaryIndex = index;
			break;
		}
	}
	if (primaryIndex == NULL)
		return;

	/*
	 * Split the base restrict clauses into the row-array-IN OR and the plain
	 * equalities the planner may have derived alongside it (e.g. a common
	 * "o_w_id = const" pulled out of the OR).  Those equalities pin the
	 * shared leading prefix, so pass them as the "prefix"
	 * (indexqual-equivalent) with table-var resolution; the OR is the qpqual.
	 */
	{
		List	   *orClauses = NIL;
		List	   *eqClauses = NIL;

		foreach(lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
			Node	   *clause = (Node *) rinfo->clause;

			if (IsA(clause, BoolExpr) &&
				((BoolExpr *) clause)->boolop == OR_EXPR)
				orClauses = lappend(orClauses, clause);
			else if (IsA(clause, OpExpr))
				eqClauses = lappend(eqClauses, clause);
		}

		if (list_length(orClauses) == 1 &&
			!o_rel_is_rowmarked(root, rel->relid) &&
			o_extract_row_array_keys(primary, rel->relid, false, false,
									 eqClauses, orClauses,
									 &dummyExprs, &dummyNTuples,
									 &dummyNKeys, &dummyTypes))
			matched = true;
	}
	if (!matched)
		return;

	{
		IndexPath  *ipath;
		double		nprobes = (double) dummyNTuples;
		Cost		per_probe = 2.0 * cpu_operator_cost * primary->nUniqueFields
			+ random_page_cost;

		ipath = create_index_path(root, primaryIndex,
								  NIL, NIL, NIL, NIL,
								  ForwardScanDirection,
								  false,
								  rel->lateral_relids, 1.0, false);

		if (ipath != NULL)
		{
			ipath->path.startup_cost = 0.0;
			ipath->path.total_cost = nprobes * per_probe +
				ipath->path.rows * cpu_tuple_cost;
			add_path(rel, (Path *) ipath);
		}
	}
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
			int			nkeyfields;
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
				nkeyfields = o_table->indices[PrimaryIndexNumber].nkeyfields;

				for (i = 0; i < nkeyfields; i++)
				{
					OTableIndexField *pk_field;

					pk_field = &o_table->indices[PrimaryIndexNumber].fields[i];

					foreach(lc, rel->indexlist)
					{
						IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);
						int			col;
						bool		member = false;
						int			ix_num = InvalidIndexNumber;

						/* Don't add additional pkey fields to bridged indices */
						for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
						{
							if (o_table->indices[ix_num].oids.reloid == index->indexoid)
								break;
						}

						if (ix_num == InvalidIndexNumber || ix_num == o_table->nindices)
							continue;

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
			o_table_free(o_table);
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
			int			i;

			descr = relation_get_descr(relation);
			Assert(descr != NULL);

			o_maybe_inject_row_array_path(root, rel, descr);

			/*
			 * transform all postgres scans to custom scans
			 */
			i = 0;
			while (i < list_length(rel->pathlist))
			{
				Path	   *path = list_nth(rel->pathlist, i);

				if (IsA(path, Path) ||
					IsA(path, IndexPath) ||
					IsA(path, BitmapHeapPath))
				{
					bool		replace = !IsA(path, Path);

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

					if (IsA(path, IndexPath))
					{
						IndexPath  *ix_path = (IndexPath *) path;
						OIndexDescr *primary = GET_PRIMARY(descr);

						replace = primary->oids.reloid == ix_path->indexinfo->indexoid;
					}

					if (replace)
					{
						Path	   *custom_path = transform_path(path, descr);

						rel->pathlist = list_delete_nth_cell(rel->pathlist, i);

						rel->pathlist = list_insert_nth(rel->pathlist, i,
														custom_path);
					}
				}
				i++;
			}

			i = 0;
			while (i < list_length(rel->partial_pathlist))
			{
				Path	   *path = list_nth(rel->partial_pathlist, i);

				/*
				 * TODO: Remove when parallel bitmap heap scan will be
				 * implemented
				 */
				if (!IsA(path, Path))
					rel->partial_pathlist = list_delete_nth_cell(rel->partial_pathlist, i);
				else
					i++;
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
 * Detect a "row-array-IN" qual that pins the same contiguous leading prefix
 * of the primary key across every OR arm, so the scan can iterate the N
 * tuples as N per-tuple probes instead of one whole-index prefix scan.
 *
 * Given key columns (c1..cn), we handle "(c1..cj) IN (list)" for any leading
 * prefix length j (1 <= j <= n) where every OR arm pins the SAME contiguous
 * leading prefix c1..cj to exact equality values.  Each resulting per-tuple
 * range sets the first j key columns exact and leaves the rest unbounded:
 *   - j == n -> a point lookup (ostate->exact);
 *   - j <  n -> a prefix range scan (iterator path), exactly like
 *               "WHERE c1=.. AND .. AND cj=..".
 *
 * We look for exactly one top-level BoolExpr(OR) among the qpqual clauses
 * where every arm is a conjunction of btree-equalities "keycol = value".
 * Equalities may also come from the leading index cond (indexqual); those are
 * shared by all arms.  Value expressions must not reference the scan relation
 * itself (they must be evaluable as runtime keys).
 *
 * On success returns true and sets *out_key_exprs to a flat List (arm-major,
 * key-position order) of value Exprs of length ntuples*j, *out_ntuples = N,
 * *out_nkeys = j (the pinned prefix length), and *out_key_types to a List of j
 * Oid (Integer nodes), one per pinned key position.  On any mismatch returns
 * false and leaves the outputs unchanged (caller keeps existing behavior).
 *
 * The OR clause is intentionally left in the qpqual as a cheap recheck so
 * results are guaranteed identical for all inputs.
 */
static bool
o_extract_row_array_keys(OIndexDescr *primary, Index scanrelid,
						 bool prefixUsesIndexVars, bool qpqualUsesIndexVars,
						 List *indexqual, List *qpqual,
						 List **out_key_exprs, int *out_ntuples,
						 int *out_nkeys, List **out_key_types)
{
	int			nkeys = primary->nUniqueFields;
	int		   *keyForAttnum;
	int			maxAttnum;
	Expr	  **prefixExpr;
	Oid		   *prefixType;
	bool	   *prefixSet;
	int			prefixLen;		/* contiguous prefix pinned by index cond */
	BoolExpr   *orExpr = NULL;
	ListCell   *lc;
	List	   *armKeyExprs = NIL;
	List	   *keyTypes = NIL;
	int			ntuples;
	int			coverLen = -1;	/* pinned prefix length, shared by all arms */
	int			i;
	bool		ok = true;

	if (nkeys <= 1 || nkeys > INDEX_MAX_KEYS)
		return false;
	/* Only the plain user-defined primary key, not a surrogate ctid pkey */
	if (primary->primaryIsCtid)
		return false;

	/* Find exactly one top-level OR BoolExpr in the qpqual. */
	foreach(lc, qpqual)
	{
		Node	   *clause = (Node *) lfirst(lc);

		if (IsA(clause, BoolExpr) && ((BoolExpr *) clause)->boolop == OR_EXPR)
		{
			if (orExpr != NULL)
				return false;	/* more than one OR: bail */
			orExpr = (BoolExpr *) clause;
		}
	}
	if (orExpr == NULL || list_length(orExpr->args) < 2)
		return false;

	/*
	 * Build the table-attnum -> key-position map for the primary key columns.
	 * tableAttnums[keypos] is the (1-based) table attnum for key position
	 * keypos.
	 */
	maxAttnum = primary->maxTableAttnum;
	if (maxAttnum <= 0)
		return false;
	keyForAttnum = (int *) palloc(sizeof(int) * (maxAttnum + 1));
	for (i = 0; i <= maxAttnum; i++)
		keyForAttnum[i] = -1;
	for (i = 0; i < nkeys; i++)
	{
		AttrNumber	tattno = primary->tableAttnums[i];

		if (tattno <= 0 || tattno > maxAttnum)
			return false;
		keyForAttnum[tattno] = i;
	}

	prefixExpr = (Expr **) palloc0(sizeof(Expr *) * nkeys);
	prefixType = (Oid *) palloc0(sizeof(Oid) * nkeys);
	prefixSet = (bool *) palloc0(sizeof(bool) * nkeys);

	/*
	 * Collect leading equalities pinned by the index cond.  These use
	 * INDEX_VAR with varattno = index attribute number; for the primary index
	 * the index attribute i maps to key position i-1.  We only accept plain
	 * scalar equalities (single constant/param RHS) here; SAOP index conds
	 * are not eligible for the pointwise expansion.
	 */
	foreach(lc, indexqual)
	{
		Node	   *clause = (Node *) lfirst(lc);
		OpExpr	   *op;
		Node	   *lhs,
				   *rhs;
		Var		   *var;
		int			keypos;

		if (!IsA(clause, OpExpr))
			return false;		/* e.g. SAOP: not a clean pointwise prefix */
		op = (OpExpr *) clause;
		if (list_length(op->args) != 2)
			return false;
		lhs = (Node *) linitial(op->args);
		rhs = (Node *) lsecond(op->args);
		if (!IsA(lhs, Var))
			return false;
		var = (Var *) lhs;
		if (prefixUsesIndexVars)
		{
			if (var->varno != INDEX_VAR)
				return false;
			keypos = var->varattno - 1;
			if (keypos < 0 || keypos >= nkeys)
				return false;
		}
		else
		{
			if (var->varno != scanrelid || var->varattno <= 0 ||
				var->varattno > maxAttnum)
				return false;
			keypos = keyForAttnum[var->varattno];
			if (keypos < 0)
				return false;
		}
		if (get_op_opfamily_strategy(op->opno,
									 primary->fields[keypos].opfamily)
			!= BTEqualStrategyNumber)
			return false;
		/* value must not depend on the scan relation */
		if (bms_is_member(scanrelid, pull_varnos(NULL, rhs)))
			return false;
		if (prefixSet[keypos])
			return false;
		prefixExpr[keypos] = (Expr *) rhs;
		prefixType[keypos] = exprType(rhs);
		prefixSet[keypos] = true;
	}

	/*
	 * The index-cond equalities must themselves form a contiguous leading
	 * prefix (they always do for the OrioleDB scan-key builder, but verify).
	 */
	prefixLen = 0;
	while (prefixLen < nkeys && prefixSet[prefixLen])
		prefixLen++;
	for (i = prefixLen; i < nkeys; i++)
	{
		if (prefixSet[i])
			return false;		/* gap in the index-cond prefix */
	}

	/*
	 * Walk the OR arms.  Each arm's equalities, together with the shared
	 * index-cond prefix, must pin the SAME contiguous leading prefix c1..cj.
	 */
	ntuples = list_length(orExpr->args);
	foreach(lc, orExpr->args)
	{
		Node	   *arm = (Node *) lfirst(lc);
		List	   *conj;
		ListCell   *lc2;
		Expr	  **armExpr = (Expr **) palloc0(sizeof(Expr *) * nkeys);
		Oid		   *armType = (Oid *) palloc0(sizeof(Oid) * nkeys);
		bool	   *armSet = (bool *) palloc0(sizeof(bool) * nkeys);
		int			j;
		int			k;

		if (IsA(arm, BoolExpr) && ((BoolExpr *) arm)->boolop == AND_EXPR)
			conj = ((BoolExpr *) arm)->args;
		else
			conj = list_make1(arm);

		foreach(lc2, conj)
		{
			Node	   *clause = (Node *) lfirst(lc2);
			OpExpr	   *op;
			Node	   *lhs,
					   *rhs;
			Var		   *var;
			int			keypos;
			Relids		rhs_relids;

			if (!IsA(clause, OpExpr))
			{
				ok = false;
				break;
			}
			op = (OpExpr *) clause;
			if (list_length(op->args) != 2)
			{
				ok = false;
				break;
			}
			lhs = (Node *) linitial(op->args);
			rhs = (Node *) lsecond(op->args);
			if (!IsA(lhs, Var))
			{
				ok = false;
				break;
			}
			var = (Var *) lhs;

			/*
			 * Arm vars reference either the scan relation (plain IndexScan
			 * qpqual, by table attnum) or INDEX_VAR (IndexOnlyScan qpqual, by
			 * index attribute = key position + 1).
			 */
			if (qpqualUsesIndexVars)
			{
				if (var->varno != INDEX_VAR)
				{
					ok = false;
					break;
				}
				keypos = var->varattno - 1;
				if (keypos < 0 || keypos >= nkeys)
				{
					ok = false;
					break;
				}
			}
			else
			{
				if (var->varno != scanrelid || var->varattno <= 0 ||
					var->varattno > maxAttnum)
				{
					ok = false;
					break;
				}
				keypos = keyForAttnum[var->varattno];
				if (keypos < 0)
				{
					ok = false;
					break;
				}
			}
			if (get_op_opfamily_strategy(op->opno,
										 primary->fields[keypos].opfamily)
				!= BTEqualStrategyNumber)
			{
				ok = false;
				break;
			}
			/* value must not depend on the scan relation */
			rhs_relids = pull_varnos(NULL, rhs);
			if (bms_is_member(scanrelid, rhs_relids))
			{
				ok = false;
				break;
			}
			if (armSet[keypos] || prefixSet[keypos])
			{
				/* duplicate / conflicts with the fixed prefix */
				ok = false;
				break;
			}
			armExpr[keypos] = (Expr *) rhs;
			armType[keypos] = exprType(rhs);
			armSet[keypos] = true;
		}

		if (!ok)
			break;

		/* Merge the shared prefix into this arm's coverage */
		for (k = 0; k < prefixLen; k++)
		{
			armExpr[k] = prefixExpr[k];
			armType[k] = prefixType[k];
			armSet[k] = true;
		}

		/* Combined coverage must be a contiguous leading prefix c1..cj */
		j = 0;
		while (j < nkeys && armSet[j])
			j++;
		if (j == 0)
		{
			ok = false;
			break;
		}
		for (k = j; k < nkeys; k++)
		{
			if (armSet[k])
			{
				ok = false;		/* gap: not a contiguous leading prefix */
				break;
			}
		}
		if (!ok)
			break;

		/* All arms must pin the same prefix length */
		if (coverLen == -1)
			coverLen = j;
		else if (coverLen != j)
		{
			ok = false;
			break;
		}

		for (k = 0; k < j; k++)
			armKeyExprs = lappend(armKeyExprs, armExpr[k]);
	}

	if (!ok || coverLen <= 0)
		return false;

	/* Build the per-key type list (coverLen positions) from the first arm. */
	for (i = 0; i < coverLen; i++)
	{
		Expr	   *e = (Expr *) list_nth(armKeyExprs, i);

		keyTypes = lappend(keyTypes, makeInteger((int) exprType((Node *) e)));
	}

	*out_key_exprs = armKeyExprs;
	*out_ntuples = ntuples;
	*out_nkeys = coverLen;
	*out_key_types = keyTypes;
	return true;
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
		List	   *rowArrayExprs = NIL;
		int			rowArrayNTuples = 0;
		int			rowArrayNKeys = 0;
		List	   *rowArrayKeyTypes = NIL;

		if (custom_plans && IsA(custom_plan, IndexScan))
		{
			IndexScan  *ix_scan = (IndexScan *) custom_plan;

			plan->targetlist = ix_scan->scan.plan.targetlist;
			custom_scan->custom_scan_tlist = NIL;
			qpqual = ix_scan->scan.plan.qual;

			/*
			 * Try to recognize a row-array-IN on the primary key so we can do
			 * N exact point lookups instead of a whole-index prefix scan.
			 */
			if (ix_path->ix_num == PrimaryIndexNumber)
			{
				OIndexDescr *primary = GET_PRIMARY(descr);

				o_extract_row_array_keys(primary, rel->relid, true, false,
										 ix_scan->indexqual, qpqual,
										 &rowArrayExprs, &rowArrayNTuples,
										 &rowArrayNKeys, &rowArrayKeyTypes);
			}
		}
		else if (custom_plans && IsA(custom_plan, IndexOnlyScan))
		{
			IndexOnlyScan *ixo_scan = (IndexOnlyScan *) custom_plan;

			plan->targetlist = ixo_scan->scan.plan.targetlist;
			custom_scan->custom_scan_tlist = ixo_scan->indextlist;
			qpqual = ixo_scan->scan.plan.qual;

			/*
			 * The index-only scan qpqual references the scan relation by
			 * table attnum (like a plain IndexScan qpqual).  Its indexqual,
			 * however, is expressed in INDEX_VAR terms.
			 */
			if (ix_path->ix_num == PrimaryIndexNumber)
			{
				OIndexDescr *primary = GET_PRIMARY(descr);

				o_extract_row_array_keys(primary, rel->relid, true, false,
										 ixo_scan->indexqual, qpqual,
										 &rowArrayExprs, &rowArrayNTuples,
										 &rowArrayNKeys, &rowArrayKeyTypes);
			}
		}

		custom_scan->custom_exprs = rowArrayExprs;
		custom_scan->custom_private =
		/* cppcheck-suppress unknownEvaluationOrder */
			list_make4(makeInteger(O_IndexPlan),
					   makeInteger(ix_path->ix_num),
					   makeInteger(ix_path->scandir),
					   makeInteger(onlyCurIx));
		custom_scan->custom_private =
			lappend(custom_scan->custom_private,
					makeInteger(rowArrayNTuples));
		custom_scan->custom_private =
			lappend(custom_scan->custom_private,
					makeInteger(rowArrayNKeys));
		custom_scan->custom_private =
			lappend(custom_scan->custom_private, rowArrayKeyTypes);
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
		/* cppcheck-suppress unknownEvaluationOrder */
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
static Node *
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

		/*
		 * Row-array-IN metadata (custom_private positions 5..7 and the value
		 * expressions in custom_exprs).  rowArrayNTuples == 0 => inactive.
		 */
		ix_plan_state->rowArrayNTuples =
			intVal(list_nth(cscan->custom_private, 4));
		ix_plan_state->rowArrayNKeys =
			intVal(list_nth(cscan->custom_private, 5));
		if (ix_plan_state->rowArrayNTuples > 0)
		{
			List	   *typeList = (List *) list_nth(cscan->custom_private, 6);
			int			k;
			ListCell   *tlc;

			Assert(list_length(typeList) == ix_plan_state->rowArrayNKeys);
			ix_plan_state->rowArrayKeyTypes = (Oid *)
				palloc(sizeof(Oid) * ix_plan_state->rowArrayNKeys);
			k = 0;
			foreach(tlc, typeList)
				ix_plan_state->rowArrayKeyTypes[k++] = (Oid) intVal(lfirst(tlc));

			ix_plan_state->rowArrayKeyExprList = copyObject(cscan->custom_exprs);
			Assert(list_length(ix_plan_state->rowArrayKeyExprList) ==
				   ix_plan_state->rowArrayNTuples *
				   ix_plan_state->rowArrayNKeys);
		}

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
static void
o_begin_custom_scan(CustomScanState *node, EState *estate, int eflags)
{
	OTableDescr *descr = relation_get_descr(node->ss.ss_currentRelation);
	OCustomScanState *ocstate = (OCustomScanState *) node;

	ocstate->o_plan_state->plan_state = &node->ss.ps;

	if (is_explain_analyze(ocstate->o_plan_state->plan_state))
	{
		ResourceOwnerRememberOTableDescr(CurrentResourceOwner, descr);
		eanalyze_counters_init(&ocstate->eaCounters, descr);
	}

	if (ocstate->o_plan_state->type == O_IndexPlan)
	{
		OIndexPlanState *ix_plan_state =
			(OIndexPlanState *) ocstate->o_plan_state;
		OScanState *scan_state = &ix_plan_state->ostate;
		OIndexNumber ix_num = ix_plan_state->ostate.ixNum;
		OIndexDescr *ix_descr = descr->indices[ix_num];
		Relation	index;

		O_LOAD_SNAPSHOT(&scan_state->oSnapshot, estate->es_snapshot);
		ix_plan_state->ostate.cxt = AllocSetContextCreate(estate->es_query_cxt,
														  "orioledb_cs plan data",
														  ALLOCSET_DEFAULT_SIZES);
		index = index_open(ix_descr->oids.reloid, AccessShareLock);

		init_index_scan_state(&ix_plan_state->o_plan_state, &ix_plan_state->ostate,
							  index, node->ss.ps.ps_ExprContext,
#if PG_VERSION_NUM >= 180000
							  estate->es_snapshot,
#endif
							  &ix_plan_state->iss_RuntimeKeys,
							  &ix_plan_state->iss_NumRuntimeKeys,
							  &ix_plan_state->iss_ScanKeys,
							  &ix_plan_state->iss_NumScanKeys);

		/*
		 * Keep the index relation open until o_end_custom_scan.
		 * ostate.scandesc.indexRelation points into it; if we closed here, a
		 * relcache invalidation could evict the entry before
		 * _bt_preprocess_keys reads rd_opfamily / rd_indoption through that
		 * pointer.
		 */
		ix_plan_state->indexRelation = index;

		ix_plan_state->iss_RuntimeContext = CreateExprContext(estate);

		/*
		 * Set up the row-array-IN value expressions and per-tuple buffers.
		 * The expressions are evaluated at rescan in iss_RuntimeContext (they
		 * reference only constants/outer params, never the scan tuple).
		 */
		if (ix_plan_state->rowArrayNTuples > 0)
		{
			int			ntotal = ix_plan_state->rowArrayNTuples *
				ix_plan_state->rowArrayNKeys;
			int			k;
			ListCell   *elc;

			scan_state->rowArrayNTuples = ix_plan_state->rowArrayNTuples;
			scan_state->rowArrayNKeys = ix_plan_state->rowArrayNKeys;
			scan_state->rowArrayKeyTypes = ix_plan_state->rowArrayKeyTypes;
			scan_state->rowArrayKeyExprs = (ExprState **)
				palloc(sizeof(ExprState *) * ntotal);
			scan_state->rowArrayValues = (Datum *) palloc(sizeof(Datum) * ntotal);
			scan_state->rowArrayNulls = (bool *) palloc(sizeof(bool) * ntotal);
			scan_state->rowArrayOrder = (int *)
				palloc(sizeof(int) * scan_state->rowArrayNTuples);
			scan_state->rowArrayNValid = 0;

			k = 0;
			foreach(elc, ix_plan_state->rowArrayKeyExprList)
			{
				Expr	   *e = (Expr *) lfirst(elc);

				scan_state->rowArrayKeyExprs[k++] =
					ExecInitExpr(e, &node->ss.ps);
			}
		}

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and
		 * pass the scankeys to the index AM.
		 */
		if (ix_plan_state->iss_NumRuntimeKeys == 0 || ix_plan_state->iss_RuntimeKeysReady)
			o_rescan_custom_scan(node);
	}
	else if (ocstate->o_plan_state->type == O_BitmapHeapPlan)
	{
		OBitmapHeapPlanState *bitmap_state =
			(OBitmapHeapPlanState *) ocstate->o_plan_state;

		bitmap_state->bitmapqualplanstate =
			ExecInitNode(bitmap_state->bitmapqualplan, estate, eflags);

		if (is_explain_analyze(ocstate->o_plan_state->plan_state))
		{
			int			i;

			bitmap_state->eaCounters = palloc0(sizeof(OEACallsCounters) *
											   descr->nIndices);
			for (i = 0; i < descr->nIndices; i++)
			{
				eanalyze_counters_init(&bitmap_state->eaCounters[i], descr);
			}
		}

		O_LOAD_SNAPSHOT(&bitmap_state->oSnapshot, estate->es_snapshot);
		bitmap_state->cxt = AllocSetContextCreate(estate->es_query_cxt,
												  "orioledb_cs plan data",
												  ALLOCSET_DEFAULT_SIZES);
	}
}

/*
 * Iterates custom scan.
 */
static TupleTableSlot *
o_exec_custom_scan(CustomScanState *node)
{
	OCustomScanState *ocstate = (OCustomScanState *) node;
	EPQState   *epqstate;
	TupleTableSlot *slot = NULL;

	if (is_explain_analyze(ocstate->o_plan_state->plan_state))
		ea_counters = &ocstate->eaCounters;
	else
		ea_counters = NULL;

	if (ocstate->o_plan_state->type == O_IndexPlan)
	{
		OIndexPlanState *ix_plan_state =
			(OIndexPlanState *) ocstate->o_plan_state;

		/*
		 * If we have runtime keys and they've not already been set up, do it
		 * now.
		 */
		if (!ix_plan_state->iss_RuntimeKeysReady)
			o_rescan_custom_scan(node);

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

		slot = o_exec_fetch(&ix_plan_state->ostate, &node->ss);
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
													&bitmap_state->oSnapshot,
													bitmap_state->cxt);
		}

		slot = o_exec_bitmap_fetch(bitmap_state->scan, node);
	}

	slot = o_exec_project(node->ss.ps.ps_ProjInfo, node->ss.ps.ps_ExprContext,
						  slot, NULL);

	return slot;
}

/*
 * Compare two row-array-IN arms by their pinned key columns, using the index
 * field comparators (with the same coercion fallback as is_tuple_valid).
 * Returns <0, 0, >0.
 */
static int
o_row_array_cmp(OScanState *ostate, OIndexDescr *id, int a, int b)
{
	int			k;
	int			nkeys = ostate->rowArrayNKeys;

	for (k = 0; k < nkeys; k++)
	{
		Datum		va = ostate->rowArrayValues[a * nkeys + k];
		Datum		vb = ostate->rowArrayValues[b * nkeys + k];
		OIndexField *field = &id->fields[k];
		Oid			type = ostate->rowArrayKeyTypes[k];
		int			cmp;

		if (!OidIsValid(type))
			type = field->inputtype;

		/*
		 * If the arm value type is binary-coercible to the field input type
		 * we compare with the field comparator directly, otherwise use a
		 * type-specific comparator (mirrors o_bound_is_coercible /
		 * key_range).
		 */
		if (type == field->opclass || type == field->inputtype ||
			IsBinaryCoercible(type, field->inputtype))
		{
			cmp = o_call_comparator(field->comparator, va, vb);
		}
		else
		{
			OComparator *c = o_find_comparator(field->opfamily, type,
											   field->inputtype,
											   field->collation);

			cmp = o_call_comparator(c, va, vb);
		}

		if (cmp != 0)
			return field->ascending ? cmp : -cmp;
	}
	return 0;
}

/*
 * Evaluate the row-array-IN value expressions (runtime keys) and build the
 * ordering of arms in ascending key order, skipping any arm whose key
 * contains a NULL (equality never matches NULL).
 */
static void
o_eval_row_array_keys(CustomScanState *node, OIndexPlanState *ix_plan_state)
{
	OScanState *ostate = &ix_plan_state->ostate;
	OTableDescr *descr = relation_get_descr(node->ss.ss_currentRelation);
	OIndexDescr *id = descr->indices[ostate->ixNum];
	ExprContext *econtext = ix_plan_state->iss_RuntimeContext;
	MemoryContext oldcxt;
	int			nkeys = ostate->rowArrayNKeys;
	int			t;
	int			nvalid = 0;

	/*
	 * Evaluate in the per-scan context so the resulting Datums survive for
	 * the lifetime of the scan (they are read on every switch_to_next_range).
	 */
	ResetExprContext(econtext);
	oldcxt = MemoryContextSwitchTo(ostate->cxt);

	for (t = 0; t < ostate->rowArrayNTuples; t++)
	{
		int			k;
		bool		hasnull = false;

		for (k = 0; k < nkeys; k++)
		{
			int			idx = t * nkeys + k;
			ExprState  *es = ostate->rowArrayKeyExprs[idx];
			Datum		val;
			bool		isnull;

			val = ExecEvalExpr(es, econtext, &isnull);
			if (isnull)
			{
				hasnull = true;
				ostate->rowArrayValues[idx] = (Datum) 0;
				ostate->rowArrayNulls[idx] = true;
			}
			else
			{
				int16		typlen;
				bool		typbyval;

				/*
				 * Copy the value into the per-scan context; the runtime
				 * context is reset on every rescan.
				 */
				get_typlenbyval(ostate->rowArrayKeyTypes[k], &typlen, &typbyval);
				ostate->rowArrayValues[idx] = datumCopy(val, typbyval, typlen);
				ostate->rowArrayNulls[idx] = false;
			}
		}

		if (!hasnull)
			ostate->rowArrayOrder[nvalid++] = t;
	}

	ostate->rowArrayCurTuple = 0;

	/* Insertion sort the valid arm indices by ascending key order. */
	for (t = 1; t < nvalid; t++)
	{
		int			cur = ostate->rowArrayOrder[t];
		int			s = t - 1;

		while (s >= 0 &&
			   o_row_array_cmp(ostate, id, ostate->rowArrayOrder[s], cur) > 0)
		{
			ostate->rowArrayOrder[s + 1] = ostate->rowArrayOrder[s];
			s--;
		}
		ostate->rowArrayOrder[s + 1] = cur;
	}

	/*
	 * Drop duplicate keys.  The original whole-index scan visits each
	 * physical tuple once regardless of how many times its key appears in the
	 * IN list, so a duplicate arm must not yield the tuple again.  After
	 * sorting, equal keys are adjacent.
	 */
	if (nvalid > 1)
	{
		int			w = 1;

		for (t = 1; t < nvalid; t++)
		{
			if (o_row_array_cmp(ostate, id, ostate->rowArrayOrder[w - 1],
								ostate->rowArrayOrder[t]) != 0)
				ostate->rowArrayOrder[w++] = ostate->rowArrayOrder[t];
		}
		nvalid = w;
	}
	ostate->rowArrayNValid = nvalid;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Restarts the scan.
 */
static void
o_rescan_custom_scan(CustomScanState *node)
{
	OCustomScanState *ocstate = (OCustomScanState *) node;

	if (ocstate->o_plan_state->type == O_IndexPlan)
	{
		OIndexPlanState *ix_plan_state =
			(OIndexPlanState *) ocstate->o_plan_state;

		if (ix_plan_state->iss_NumRuntimeKeys != 0)
		{
			ExprContext *econtext = ix_plan_state->iss_RuntimeContext;

			ResetExprContext(econtext);
			ExecIndexEvalRuntimeKeys(econtext,
									 ix_plan_state->iss_RuntimeKeys,
									 ix_plan_state->iss_NumRuntimeKeys);
		}
		ix_plan_state->iss_RuntimeKeysReady = true;

		btrescan(&ix_plan_state->ostate.scandesc, ix_plan_state->iss_ScanKeys,
				 ix_plan_state->iss_NumScanKeys, NULL, 0);

		if (ix_plan_state->ostate.iterator != NULL)
		{
			btree_iterator_free(ix_plan_state->ostate.iterator);
			ix_plan_state->ostate.iterator = NULL;
		}
		if (node->ss.ps.chgParam != NULL)
		{
			MemoryContextReset(ix_plan_state->ostate.cxt);
		}

		ix_plan_state->ostate.curKeyRangeIsLoaded = false;
		ix_plan_state->ostate.numPrefixExactKeys = o_get_num_prefix_exact_keys(ix_plan_state->iss_ScanKeys, ix_plan_state->iss_NumScanKeys);
		ix_plan_state->ostate.curKeyRange.empty = true;
		ix_plan_state->ostate.curKeyRange.low.n_row_keys = 0;
		ix_plan_state->ostate.curKeyRange.high.n_row_keys = 0;
		ix_plan_state->ostate.iterator = NULL;

		/* Evaluate row-array-IN value expressions and order the tuples. */
		if (ix_plan_state->ostate.rowArrayNTuples > 0)
			o_eval_row_array_keys(node, ix_plan_state);
	}
	else if (ocstate->o_plan_state->type == O_BitmapHeapPlan)
	{
		OBitmapHeapPlanState *bitmap_state;

		bitmap_state = (OBitmapHeapPlanState *) ocstate->o_plan_state;

		if (bitmap_state->scan)
			o_free_bitmap_scan(bitmap_state->scan);

		if (is_explain_analyze(ocstate->o_plan_state->plan_state))
			pfree(bitmap_state->eaCounters);
		bitmap_state->scan = NULL;
	}
}

/*
 * Ends custom scan.
 */
static void
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
		index_close(ix_plan_state->indexRelation, AccessShareLock);
		ix_plan_state->indexRelation = NULL;
	}
	else if (ocstate->o_plan_state->type == O_BitmapHeapPlan)
	{
		OBitmapHeapPlanState *bitmap_state =
			(OBitmapHeapPlanState *) ocstate->o_plan_state;

		if (bitmap_state->bitmapqualplanstate)
			ExecEndNode(bitmap_state->bitmapqualplanstate);
		if (bitmap_state->scan)
			o_free_bitmap_scan(bitmap_state->scan);
		if (is_explain_analyze(ocstate->o_plan_state->plan_state))
			pfree(bitmap_state->eaCounters);
		MemoryContextDelete(bitmap_state->cxt);
		bitmap_state->cxt = NULL;
	}
	if (is_explain_analyze(ocstate->o_plan_state->plan_state))
		ResourceOwnerForgetOTableDescr(CurrentResourceOwner,
									   ocstate->eaCounters.descr);
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
				if (is_explain_analyze(ocstate->o_plan_state->plan_state))
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
					if (ix_num >= descr->nIndices)
					{
#ifdef USE_ASSERT_CHECKING
						Relation	index = index_open(bm_scan->indexid, AccessShareLock);

						if (index->rd_rel->relam == BTREE_AM_OID &&
							!(index->rd_options && !((OBTOptions *) index->rd_options)->orioledb_index))
						{
							Assert(false);
						}

						index_close(index, AccessShareLock);
#endif
					}
					else
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
			ec.ancestors = lcons(node->ss.ps.plan, ec.ancestors);
			ec.es = es;
			ec.ocstate = ocstate;
			ec.descr = descr;
			o_explain_node(bitmap_state->bitmapqualplanstate, &ec);
			ExplainCloseGroup("Plans", "Plans", false, es);
		}
	}
	if (is_explain_analyze(ocstate->o_plan_state->plan_state))
		eanalyze_counters_explain(descr, &ocstate->eaCounters, es);
}
