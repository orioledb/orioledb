/*-------------------------------------------------------------------------
 *
 * handler.c
 *		Implementation of index access method handler
 *
 * Copyright (c) 2021-2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/indexam/handler.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/indices.h"
#include "catalog/o_tables.h"
#include "tuple/slot.h"
#include "utils/compress.h"
#include "utils/planner.h"

#include "access/amapi.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "tableam/index_scan.h"
#include "utils/index_selfuncs.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#define DEFAULT_PAGE_CPU_MULTIPLIER 50.0

#define LOG_AM_CALLS true // TODO: Remove
#undef LOG_AM_CALLS // TODO: Remove

static IndexBuildResult *orioledb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo);
static void orioledb_ambuildempty(Relation index);
static bool orioledb_aminsert(Relation rel, Datum *values, bool *isnull,
							  ItemPointer ht_ctid, Relation heapRel,
							  IndexUniqueCheck checkUnique,
							  bool indexUnchanged,
							  IndexInfo *indexInfo);
static IndexBulkDeleteResult *orioledb_ambulkdelete(IndexVacuumInfo *info,
													IndexBulkDeleteResult *stats,
													IndexBulkDeleteCallback callback,
													void *callback_state);
static IndexBulkDeleteResult *orioledb_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
static bool orioledb_amcanreturn(Relation index, int attno);
static void orioledb_amcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
									Cost *indexStartupCost, Cost *indexTotalCost,
									Selectivity *indexSelectivity, double *indexCorrelation,
									double *indexPages);
static bytea *orioledb_amoptions(Datum reloptions, bool validate);
static bool orioledb_amproperty(Oid index_oid, int attno, IndexAMProperty prop,
								const char *propname, bool *res, bool *isnull);
static char *orioledb_ambuildphasename(int64 phasenum);
static bool orioledb_amvalidate(Oid opclassoid);
static void orioledb_amadjustmembers(Oid opfamilyoid, Oid opclassoid,
									 List *operators, List *functions);
static IndexScanDesc orioledb_ambeginscan(Relation rel, int nkeys, int norderbys);
static void orioledb_amrescan(IndexScanDesc scan, ScanKey scankey,
							  int nscankeys, ScanKey orderbys, int norderbys);
static bool orioledb_amgettuple(IndexScanDesc scan, ScanDirection dir);
static int64 orioledb_amgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
static void orioledb_amendscan(IndexScanDesc scan);
static void orioledb_ammarkpos(IndexScanDesc scan);
static void orioledb_amrestrpos(IndexScanDesc scan);
static Size orioledb_amestimateparallelscan(void);
static void orioledb_aminitparallelscan(void *target);
static void orioledb_amparallelrescan(IndexScanDesc scan);

Datum orioledb_indexam_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(orioledb_indexam_handler);

Datum orioledb_indexam_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
	orioledb_check_shmem();

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = true;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = true;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = true;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amsummarizing = false;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = orioledb_ambuild;
	amroutine->ambuildempty = orioledb_ambuildempty;
	amroutine->aminsert = orioledb_aminsert;
	amroutine->ambulkdelete = orioledb_ambulkdelete;
	amroutine->amvacuumcleanup = orioledb_amvacuumcleanup;
	amroutine->amcanreturn = orioledb_amcanreturn;
	amroutine->amcostestimate = orioledb_amcostestimate;
	amroutine->amoptions = orioledb_amoptions;
	amroutine->amproperty = orioledb_amproperty;
	amroutine->ambuildphasename = orioledb_ambuildphasename;
	amroutine->amvalidate = orioledb_amvalidate;
	amroutine->amadjustmembers = orioledb_amadjustmembers;
	amroutine->ambeginscan = orioledb_ambeginscan;
	amroutine->amrescan = orioledb_amrescan;
	amroutine->amgettuple = orioledb_amgettuple;
	amroutine->amgetbitmap = orioledb_amgetbitmap;
	amroutine->amendscan = orioledb_amendscan;
	amroutine->ammarkpos = orioledb_ammarkpos;
	amroutine->amrestrpos = orioledb_amrestrpos;
	amroutine->amestimateparallelscan = orioledb_amestimateparallelscan;
	amroutine->aminitparallelscan = orioledb_aminitparallelscan;
	amroutine->amparallelrescan = orioledb_amparallelrescan;

	PG_RETURN_POINTER(amroutine);
}

IndexBuildResult *
orioledb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult	   *result;

#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	o_define_index_validate(heap, index);
	o_define_index(heap, index);

	result->heap_tuples = 0.0;
	result->index_tuples = 0.0;

	return result;
}

void
orioledb_ambuildempty(Relation index)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
}

bool
orioledb_aminsert(Relation rel, Datum *values, bool *isnull,
				  ItemPointer ht_ctid, Relation heapRel,
				  IndexUniqueCheck checkUnique,
				  bool indexUnchanged,
				  IndexInfo *indexInfo)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif

	return false;
}

IndexBulkDeleteResult *
orioledb_ambulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
					  IndexBulkDeleteCallback callback, void *callback_state)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
	return stats;
}

IndexBulkDeleteResult *
orioledb_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
	return stats;
}

bool
orioledb_amcanreturn(Relation index, int attno)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
	return true;
}

/* TODO: Rewrite to be more orioledb-specific */
void
orioledb_amcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
						Cost *indexStartupCost, Cost *indexTotalCost,
						Selectivity *indexSelectivity, double *indexCorrelation,
						double *indexPages)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif

	IndexOptInfo *index = path->indexinfo;
	GenericCosts costs = {0};
	Oid			relid;
	AttrNumber	colnum;
	VariableStatData vardata = {0};
	double		numIndexTuples;
	Cost		descentCost;
	List	   *indexBoundQuals;
	int			indexcol;
	bool		eqQualHere;
	bool		found_saop;
	bool		found_is_null_op;
	double		num_sa_scans;
	ListCell   *lc;

	/*
	 * For a btree scan, only leading '=' quals plus inequality quals for the
	 * immediately next attribute contribute to index selectivity (these are
	 * the "boundary quals" that determine the starting and stopping points of
	 * the index scan).  Additional quals can suppress visits to the heap, so
	 * it's OK to count them in indexSelectivity, but they should not count
	 * for estimating numIndexTuples.  So we must examine the given indexquals
	 * to find out which ones count as boundary quals.  We rely on the
	 * knowledge that they are given in index column order.
	 *
	 * For a RowCompareExpr, we consider only the first column, just as
	 * rowcomparesel() does.
	 *
	 * If there's a ScalarArrayOpExpr in the quals, we'll actually perform N
	 * index scans not one, but the ScalarArrayOpExpr's operator can be
	 * considered to act the same as it normally does.
	 */
	indexBoundQuals = NIL;
	indexcol = 0;
	eqQualHere = false;
	found_saop = false;
	found_is_null_op = false;
	num_sa_scans = 1;
	foreach(lc, path->indexclauses)
	{
		IndexClause *iclause = lfirst_node(IndexClause, lc);
		ListCell   *lc2;

		if (indexcol != iclause->indexcol)
		{
			/* Beginning of a new column's quals */
			if (!eqQualHere)
				break;			/* done if no '=' qual for indexcol */
			eqQualHere = false;
			indexcol++;
			if (indexcol != iclause->indexcol)
				break;			/* no quals at all for indexcol */
		}

		/* Examine each indexqual associated with this index clause */
		foreach(lc2, iclause->indexquals)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc2);
			Expr	   *clause = rinfo->clause;
			Oid			clause_op = InvalidOid;
			int			op_strategy;

			if (IsA(clause, OpExpr))
			{
				OpExpr	   *op = (OpExpr *) clause;

				clause_op = op->opno;
			}
			else if (IsA(clause, RowCompareExpr))
			{
				RowCompareExpr *rc = (RowCompareExpr *) clause;

				clause_op = linitial_oid(rc->opnos);
			}
			else if (IsA(clause, ScalarArrayOpExpr))
			{
				ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
				Node	   *other_operand = (Node *) lsecond(saop->args);
				int			alength = estimate_array_length(other_operand);

				clause_op = saop->opno;
				found_saop = true;
				/* count number of SA scans induced by indexBoundQuals only */
				if (alength > 1)
					num_sa_scans *= alength;
			}
			else if (IsA(clause, NullTest))
			{
				NullTest   *nt = (NullTest *) clause;

				if (nt->nulltesttype == IS_NULL)
				{
					found_is_null_op = true;
					/* IS NULL is like = for selectivity purposes */
					eqQualHere = true;
				}
			}
			else
				elog(ERROR, "unsupported indexqual type: %d",
					 (int) nodeTag(clause));

			/* check for equality operator */
			if (OidIsValid(clause_op))
			{
				op_strategy = get_op_opfamily_strategy(clause_op,
													   index->opfamily[indexcol]);
				Assert(op_strategy != 0);	/* not a member of opfamily?? */
				if (op_strategy == BTEqualStrategyNumber)
					eqQualHere = true;
			}

			indexBoundQuals = lappend(indexBoundQuals, rinfo);
		}
	}

	/*
	 * If index is unique and we found an '=' clause for each column, we can
	 * just assume numIndexTuples = 1 and skip the expensive
	 * clauselist_selectivity calculations.  However, a ScalarArrayOp or
	 * NullTest invalidates that theory, even though it sets eqQualHere.
	 */
	if (index->unique &&
		indexcol == index->nkeycolumns - 1 &&
		eqQualHere &&
		!found_saop &&
		!found_is_null_op)
		numIndexTuples = 1.0;
	else
	{
		List	   *selectivityQuals;
		Selectivity btreeSelectivity;

		/*
		 * If the index is partial, AND the index predicate with the
		 * index-bound quals to produce a more accurate idea of the number of
		 * rows covered by the bound conditions.
		 */
		selectivityQuals = add_predicate_to_index_quals(index, indexBoundQuals);

		btreeSelectivity = clauselist_selectivity(root, selectivityQuals,
												  index->rel->relid,
												  JOIN_INNER,
												  NULL);
		numIndexTuples = btreeSelectivity * index->rel->tuples;

		/*
		 * As in genericcostestimate(), we have to adjust for any
		 * ScalarArrayOpExpr quals included in indexBoundQuals, and then round
		 * to integer.
		 */
		numIndexTuples = rint(numIndexTuples / num_sa_scans);
	}

	/*
	 * Now do generic index cost estimation.
	 */
	costs.numIndexTuples = numIndexTuples;

	genericcostestimate(root, path, loop_count, &costs);

	/*
	 * Add a CPU-cost component to represent the costs of initial btree
	 * descent.  We don't charge any I/O cost for touching upper btree levels,
	 * since they tend to stay in cache, but we still have to do about log2(N)
	 * comparisons to descend a btree of N leaf tuples.  We charge one
	 * cpu_operator_cost per comparison.
	 *
	 * If there are ScalarArrayOpExprs, charge this once per SA scan.  The
	 * ones after the first one are not startup cost so far as the overall
	 * plan is concerned, so add them only to "total" cost.
	 */
	if (index->tuples > 1)		/* avoid computing log(0) */
	{
		descentCost = ceil(log(index->tuples) / log(2.0)) * cpu_operator_cost;
		costs.indexStartupCost += descentCost;
		costs.indexTotalCost += costs.num_sa_scans * descentCost;
	}

	/*
	 * Even though we're not charging I/O cost for touching upper btree pages,
	 * it's still reasonable to charge some CPU cost per page descended
	 * through.  Moreover, if we had no such charge at all, bloated indexes
	 * would appear to have the same search cost as unbloated ones, at least
	 * in cases where only a single leaf page is expected to be visited.  This
	 * cost is somewhat arbitrarily set at 50x cpu_operator_cost per page
	 * touched.  The number of such pages is btree tree height plus one (ie,
	 * we charge for the leaf page too).  As above, charge once per SA scan.
	 */
	descentCost = (index->tree_height + 1) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
	costs.indexStartupCost += descentCost;
	costs.indexTotalCost += costs.num_sa_scans * descentCost;

	/*
	 * If we can get an estimate of the first column's ordering correlation C
	 * from pg_statistic, estimate the index correlation as C for a
	 * single-column index, or C * 0.75 for multiple columns. (The idea here
	 * is that multiple columns dilute the importance of the first column's
	 * ordering, but don't negate it entirely.  Before 8.0 we divided the
	 * correlation by the number of columns, but that seems too strong.)
	 */
	if (index->indexkeys[0] != 0)
	{
		/* Simple variable --- look to stats for the underlying table */
		RangeTblEntry *rte = planner_rt_fetch(index->rel->relid, root);

		Assert(rte->rtekind == RTE_RELATION);
		relid = rte->relid;
		Assert(relid != InvalidOid);
		colnum = index->indexkeys[0];

		if (get_relation_stats_hook &&
			(*get_relation_stats_hook) (root, rte, colnum, &vardata))
		{
			/*
			 * The hook took control of acquiring a stats tuple.  If it did
			 * supply a tuple, it'd better have supplied a freefunc.
			 */
			if (HeapTupleIsValid(vardata.statsTuple) &&
				!vardata.freefunc)
				elog(ERROR, "no function provided to release variable stats with");
		}
		else
		{
			vardata.statsTuple = SearchSysCache3(STATRELATTINH,
												 ObjectIdGetDatum(relid),
												 Int16GetDatum(colnum),
												 BoolGetDatum(rte->inh));
			vardata.freefunc = ReleaseSysCache;
		}
	}
	else
	{
		/* Expression --- maybe there are stats for the index itself */
		relid = index->indexoid;
		colnum = 1;

		if (get_index_stats_hook &&
			(*get_index_stats_hook) (root, relid, colnum, &vardata))
		{
			/*
			 * The hook took control of acquiring a stats tuple.  If it did
			 * supply a tuple, it'd better have supplied a freefunc.
			 */
			if (HeapTupleIsValid(vardata.statsTuple) &&
				!vardata.freefunc)
				elog(ERROR, "no function provided to release variable stats with");
		}
		else
		{
			vardata.statsTuple = SearchSysCache3(STATRELATTINH,
												 ObjectIdGetDatum(relid),
												 Int16GetDatum(colnum),
												 BoolGetDatum(false));
			vardata.freefunc = ReleaseSysCache;
		}
	}

	if (HeapTupleIsValid(vardata.statsTuple))
	{
		Oid			sortop;
		AttStatsSlot sslot;

		sortop = get_opfamily_member(index->opfamily[0],
									 index->opcintype[0],
									 index->opcintype[0],
									 BTLessStrategyNumber);
		if (OidIsValid(sortop) &&
			get_attstatsslot(&sslot, vardata.statsTuple,
							 STATISTIC_KIND_CORRELATION, sortop,
							 ATTSTATSSLOT_NUMBERS))
		{
			double		varCorrelation;

			Assert(sslot.nnumbers == 1);
			varCorrelation = sslot.numbers[0];

			if (index->reverse_sort[0])
				varCorrelation = -varCorrelation;

			if (index->nkeycolumns > 1)
				costs.indexCorrelation = varCorrelation * 0.75;
			else
				costs.indexCorrelation = varCorrelation;

			free_attstatsslot(&sslot);
		}
	}

	ReleaseVariableStats(vardata);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

static void
validate_index_compress(const char *value)
{
	if (value)
		validate_compress(o_parse_compress(value), "Index");
}

bytea *
orioledb_amoptions(Datum reloptions, bool validate)
{
	static bool relopts_set = false;
	static local_relopts relopts = {0};

	if (!relopts_set)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		init_local_reloptions(&relopts, sizeof(OBTOptions));

		/* Options from default_reloptions */
		add_local_int_reloption(&relopts, "fillfactor",
								"Packs btree index pages only to "
								"this percentage",
								BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR,
								100,
								offsetof(OBTOptions, bt_options) +
								offsetof(BTOptions, fillfactor));
		add_local_real_reloption(&relopts, "vacuum_cleanup_index_scale_factor",
								 "Deprecated B-Tree parameter.",
								 -1, 0.0, 1e10,
								 offsetof(OBTOptions, bt_options) +
								 offsetof(BTOptions,
										  vacuum_cleanup_index_scale_factor));
		add_local_bool_reloption(&relopts, "deduplicate_items",
								 "Enables \"deduplicate items\" feature for "
								 "this btree index",
								 true,
								 offsetof(OBTOptions, bt_options) +
								 offsetof(BTOptions, deduplicate_items));

		/* Options for orioledb tables */
		add_local_string_reloption(&relopts, "compress",
								   "Compression level of a particular index",
								   NULL, validate_index_compress, NULL,
								   offsetof(OBTOptions, compress_offset));
		MemoryContextSwitchTo(oldcxt);
		relopts_set = true;
	}

	return (bytea *) build_local_reloptions(&relopts, reloptions, validate);
}

bool
orioledb_amproperty(Oid index_oid, int attno, IndexAMProperty prop,
					const char *propname, bool *res, bool *isnull)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif

	switch (prop)
	{
		case AMPROP_RETURNABLE:
			/* answer only for columns, not AM or whole index */
			if (attno == 0)
				return false;
			/* otherwise, btree can always return data */
			*res = true;
			return true;

		default:
			return false;		/* punt to generic code */
	}
}

char *
orioledb_ambuildphasename(int64 phasenum)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif

	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN:
			return "scanning table";
		case PROGRESS_BTREE_PHASE_PERFORMSORT_1:
			return "sorting live tuples";
		case PROGRESS_BTREE_PHASE_PERFORMSORT_2:
			return "sorting dead tuples";
		case PROGRESS_BTREE_PHASE_LEAF_LOAD:
			return "loading tuples in tree";
		default:
			return NULL;
	}
}

bool orioledb_amvalidate(Oid opclassoid)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
	return true;
}

void
orioledb_amadjustmembers(Oid opfamilyoid, Oid opclassoid, List *operators,
						 List *functions)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
}

IndexScanDesc
orioledb_ambeginscan(Relation rel, int nkeys, int norderbys)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
	OScanState	*o_scan;
	IndexScanDesc scan;
	ORelOids	oids;
	OIndexType	ix_type;
	OIndexDescr *index_descr;
	OTableDescr *descr;
	OIndexNumber ix_num;

	o_scan = (OScanState *) palloc0(sizeof(OScanState));

	/* get the scan */
	scan = btbeginscan(rel, nkeys, norderbys);
	o_scan->scandesc = *scan;
	pfree(scan);

	scan = &o_scan->scandesc;

	scan->parallel_scan = NULL;
	scan->xs_temp_snap = false;
	scan->xs_want_rowid = true;

	oids.datoid = MyDatabaseId;
	oids.reloid = rel->rd_rel->oid;
	oids.relnode = rel->rd_rel->relfilenode;
	if (rel->rd_index->indisprimary)
		ix_type = oIndexPrimary;
	else if (rel->rd_index->indisunique)
		ix_type = oIndexUnique;
	else
		ix_type = oIndexRegular;
	index_descr = o_fetch_index_descr(oids, ix_type, false, NULL);
	Assert(index_descr != NULL);
	descr = o_fetch_table_descr(index_descr->tableOids);
	Assert(descr != NULL);
	/* Find ix_num */
	for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
	{
		OIndexDescr *index;
		index = descr->indices[ix_num];
		if (index->oids.reloid == rel->rd_rel->oid)
			break;
	}
	Assert(ix_num < descr->nIndices);
	o_scan->ixNum = ix_num;

	o_scan->cxt = AllocSetContextCreate(CurrentMemoryContext,
										"orioledb_cs plan data",
										ALLOCSET_DEFAULT_SIZES);

	return scan;
}

void
orioledb_amrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
				  ScanKey orderbys, int norderbys)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
	OScanState	*o_scan = (OScanState *) scan;

	MemoryContextReset(o_scan->cxt);
	btrescan(scan, scankey, nscankeys, orderbys, norderbys);
}

bool
orioledb_amgettuple(IndexScanDesc scan, ScanDirection dir)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
	bool		res;
	OScanState	*o_scan = (OScanState *) scan;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	OTableDescr *descr;
	OTuple		tuple;
	bool		scan_primary;
	MemoryContext tupleCxt = CurrentMemoryContext;
	BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};
	CommitSeqNo tupleCsn;
	OIndexNumber ix_num;

	o_scan->scanDir = dir;
	o_scan->csn = scan->xs_snapshot->snapshotcsn;

	/* btree indexes are never lossy */
	scan->xs_recheck = false;

	/*
	 * If we have any array keys, initialize them during first call for a
	 * scan.  We can't do this in btrescan because we don't know the scan
	 * direction at that time.
	 */
	if (so->numArrayKeys && !BTScanPosIsValid(so->currPos))
	{
		/* punt if we have any unsatisfiable array keys */
		if (so->numArrayKeys < 0)
			return false;

		_bt_start_array_keys(scan, dir);
	}
	if (!BTScanPosIsValid(so->currPos))
	{
		_bt_preprocess_keys(scan);
	}

	descr = relation_get_descr(scan->heapRelation);
	scan_primary = o_scan->ixNum == PrimaryIndexNumber || !scan->xs_want_itup;

	if (!o_scan->curKeyRangeIsLoaded)
		o_scan->curKeyRange.empty = true;

	tuple = o_index_scan_getnext(descr, o_scan, &tupleCsn, scan_primary,
								 tupleCxt, &hint);

	if (O_TUPLE_IS_NULL(tuple))
	{
		scan->xs_rowid.isnull = true;
		res = false;
	}
	else
	{
		TupleTableSlot *slot;

		ix_num = scan_primary ? PrimaryIndexNumber : o_scan->ixNum;
		if (scan_primary)
			scan->xs_hitupdesc = descr->tupdesc;
		else
		{
			OIndexDescr *index_descr = descr->indices[ix_num];
			int nfields = index_descr->nFields - index_descr->nPrimaryFields;
			int			i;

			scan->xs_hitupdesc = CreateTemplateTupleDesc(nfields);
			for (i = 0; i < nfields; i++)
			{
				TupleDescCopyEntry(scan->xs_hitupdesc, i + 1, index_descr->leafTupdesc, i + 1);
			}
		}
		slot = MakeSingleTupleTableSlot(scan->xs_hitupdesc, &TTSOpsOrioleDB);
		tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn, ix_num,
								 true, &hint);
		scan->xs_rowid.value = slot_getsysattr(slot, RowIdAttributeNumber, &scan->xs_rowid.isnull);
		scan->xs_hitup = ExecCopySlotHeapTuple(slot);

		res = true;
	}

	return res;
}

int64
orioledb_amgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
	return 0;
}

void
orioledb_amendscan(IndexScanDesc scan)
{
	OScanState	*o_scan = (OScanState *) scan;

#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif

	MemoryContextDelete(o_scan->cxt);
}

void
orioledb_ammarkpos(IndexScanDesc scan)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
}

void
orioledb_amrestrpos(IndexScanDesc scan)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
}

Size
orioledb_amestimateparallelscan(void)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
	return sizeof(uint8);
}

void
orioledb_aminitparallelscan(void *target)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
}

void
orioledb_amparallelrescan(IndexScanDesc scan)
{
#ifdef LOG_AM_CALLS
	elog(WARNING, "%s", PG_FUNCNAME_MACRO);
#endif
}
