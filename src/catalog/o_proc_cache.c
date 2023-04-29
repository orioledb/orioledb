/*-------------------------------------------------------------------------
 *
 *  o_proc_cache.c
 *		Routines for orioledb proc cache.
 *
 * proc_cache is tree that contains cached metadata from pg_proc.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_proc_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_tables.h"
#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"

#include "access/htup_details.h"
#if PG_VERSION_NUM >= 140000
#include "access/toast_compression.h"
#endif
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif
#include "catalog/pg_am.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/functions.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM < 140000
#include "nodes/print.h"
#endif
#include "rewrite/rewriteHandler.h"
#include "pgstat.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgrtab.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/planner.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

static OSysCache *proc_cache = NULL;

typedef struct sql_func_data
{
	char	   *src;
	bool		returnsTuple;	/* true if returning whole tuple result */
	bool		readonly_func;	/* true to run in "read only" mode */
	bool		lazyEval;		/* true if using lazyEval for result query */
	int			jf_natts;
	int			nnqtlists;

	bool		has_argnames;

	List	   *jf_targetList;
	Oid		   *jf_atts;

	char	  **argnames;

	int		   *nqtlists;
	Node	 ***qtlists;
} sql_func_data;

struct OProc
{
	OSysCacheKey1 key;
	Oid			rettype;		/* actual return type */
	bool		strict;			/* T if function is "strict" */
	bool		retset;			/* T if function returns a set */
	Oid			prolang;
	Oid			proowner;
	int16		nargs;

	char	   *proname;
	char	   *prosrc;
	char	   *probin;
	Oid		   *argtypes;
	sql_func_data *sql_func;

	MemoryContext cxt;
};

#if PG_VERSION_NUM < 140000
/*
 * Data structure needed by the parser callback hooks to resolve parameter
 * references during parsing of a SQL function's body.  This is separate from
 * SQLFunctionCache since we sometimes do parsing separately from execution.
 */
typedef struct SQLFunctionParseInfo
{
	char	   *fname;			/* function's name */
	int			nargs;			/* number of input arguments */
	Oid		   *argtypes;		/* resolved types of input arguments */
	char	  **argnames;		/* names of input arguments; NULL if none */
	/* Note that argnames[i] can be NULL, if some args are unnamed */
	Oid			collation;		/* function's input collation, if known */
} SQLFunctionParseInfo;


/*
 * Perform rewriting of a query produced by parse analysis.
 *
 * Note: query must just have come from the parser, because we do not do
 * AcquireRewriteLocks() on it.
 */
static List *
pg_rewrite_query(Query *query)
{
	List	   *querytree_list;

	if (Debug_print_parse)
		elog_node_display(LOG, "parse tree", query,
						  Debug_pretty_print);

	if (log_parser_stats)
		ResetUsage();

	if (query->commandType == CMD_UTILITY)
	{
		/* don't rewrite utilities, just dump 'em into result list */
		querytree_list = list_make1(query);
	}
	else
	{
		/* rewrite regular queries */
		querytree_list = QueryRewrite(query);
	}

	if (log_parser_stats)
		ShowUsage("REWRITER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass querytree through copyObject() */
	{
		List	   *new_list;

		new_list = copyObject(querytree_list);
		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_list, querytree_list))
			elog(WARNING, "copyObject() failed to produce equal parse tree");
		else
			querytree_list = new_list;
	}
#endif

#ifdef WRITE_READ_PARSE_PLAN_TREES
	/* Optional debugging check: pass querytree through outfuncs/readfuncs */
	{
		List	   *new_list = NIL;
		ListCell   *lc;

		/*
		 * We currently lack outfuncs/readfuncs support for most utility
		 * statement types, so only attempt to write/read non-utility queries.
		 */
		foreach(lc, querytree_list)
		{
			Query	   *query = castNode(Query, lfirst(lc));

			if (query->commandType != CMD_UTILITY)
			{
				char	   *str = nodeToString(query);
				Query	   *new_query = stringToNodeWithLocations(str);

				/*
				 * queryId is not saved in stored rules, but we must preserve
				 * it here to avoid breaking pg_stat_statements.
				 */
				new_query->queryId = query->queryId;

				new_list = lappend(new_list, new_query);
				pfree(str);
			}
			else
				new_list = lappend(new_list, query);
		}

		/* This checks both outfuncs/readfuncs and the equal() routines... */
		if (!equal(new_list, querytree_list))
			elog(WARNING, "outfuncs/readfuncs failed to produce equal parse tree");
		else
			querytree_list = new_list;
	}
#endif

	if (Debug_print_rewritten)
		elog_node_display(LOG, "rewritten parse tree", querytree_list,
						  Debug_pretty_print);

	return querytree_list;
}
#elif PG_VERSION_NUM >= 150000
#define pg_analyze_and_rewrite_params pg_analyze_and_rewrite_withcb
#endif

/*
 * An SQLFunctionCache record is built during the first call,
 * and linked to from the fn_extra field of the FmgrInfo struct.
 *
 * Note that currently this has only the lifespan of the calling query.
 * Someday we should rewrite this code to use plancache.c to save parse/plan
 * results for longer than that.
 *
 * Physically, though, the data has the lifespan of the FmgrInfo that's used
 * to call the function, and there are cases (particularly with indexes)
 * where the FmgrInfo might survive across transactions.  We cannot assume
 * that the parse/plan trees are good for longer than the (sub)transaction in
 * which parsing was done, so we must mark the record with the LXID/subxid of
 * its creation time, and regenerate everything if that's obsolete.  To avoid
 * memory leakage when we do have to regenerate things, all the data is kept
 * in a sub-context of the FmgrInfo's fn_mcxt.
 */
typedef struct
{
	char	   *fname;			/* function name (for error msgs) */
	char	   *src;			/* function body text (for error msgs) */

	SQLFunctionParseInfoPtr pinfo;	/* data for parser callback hooks */

	Oid			rettype;		/* actual return type */
	int16		typlen;			/* length of the return type */
	bool		typbyval;		/* true if return type is pass by value */
	bool		returnsSet;		/* true if returning multiple rows */
	bool		returnsTuple;	/* true if returning whole tuple result */
	bool		shutdown_reg;	/* true if registered shutdown callback */
	bool		readonly_func;	/* true to run in "read only" mode */
	bool		lazyEval;		/* true if using lazyEval for result query */

	ParamListInfo paramLI;		/* Param list representing current args */

	Tuplestorestate *tstore;	/* where we accumulate result tuples */

	JunkFilter *junkFilter;		/* will be NULL if function returns VOID */

	/*
	 * func_state is a List of execution_state records, each of which is the
	 * first for its original parsetree, with any additional records chained
	 * to it via the "next" fields.  This sublist structure is needed to keep
	 * track of where the original query boundaries are.
	 */
	List	   *func_state;

	MemoryContext fcontext;		/* memory context holding this struct and all
								 * subsidiary data */

	LocalTransactionId lxid;	/* lxid in which cache was made */
	SubTransactionId subxid;	/* subxid in which cache was made */
} SQLFunctionCache;

typedef SQLFunctionCache *SQLFunctionCachePtr;

static void init_sql_fcache(FunctionCallInfo fcinfo, Oid collation,
							bool lazyEvalOK, sql_func_data *out_sql_func,
							MemoryContext out_sql_func_cxt);
static void sql_exec_error_callback(void *arg);

static Pointer o_proc_cache_serialize_entry(Pointer entry, int *len);
static Pointer o_proc_cache_deserialize_entry(MemoryContext mcxt, Pointer data,
											  Size length);
static void o_proc_cache_free_entry(Pointer entry);
static void o_proc_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									Pointer arg);

O_SYS_CACHE_FUNCS(proc_cache, OProc, 1);

static OSysCacheFuncs proc_cache_funcs =
{
	.free_entry = o_proc_cache_free_entry,
	.fill_entry = o_proc_cache_fill_entry,
	.toast_serialize_entry = o_proc_cache_serialize_entry,
	.toast_deserialize_entry = o_proc_cache_deserialize_entry,
};

/*
 * Initializes the proc sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(proc_cache)
{
	Oid			keytypes[] = {OIDOID};

	proc_cache = o_create_sys_cache(SYS_TREES_PROC_CACHE,
									true, false,
									ProcedureOidIndexId, PROCOID, 1,
									keytypes, fastcache,
									mcxt,
									&proc_cache_funcs);
}

void
o_proc_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	HeapTuple	proctup;
	Form_pg_proc procform;
	OProc	   *o_proc = (OProc *) *entry_ptr;
	Oid		   *fncollation = (Oid *) arg;
	bool		lazyEvalOK = false;
	SQLFunctionCachePtr fcache;
	MemoryContext oldcontext;
	int			i;
	Oid			procoid = DatumGetObjectId(key->keys[0]);

	proctup = SearchSysCache1(PROCOID, key->keys[0]);
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", procoid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	if (o_proc != NULL)			/* Existed o_proc updated */
	{
		Assert(false);
	}
	else
	{
		o_proc = palloc0(sizeof(OProc));
		o_proc->cxt = AllocSetContextCreate(proc_cache->mcxt, "o_proc mcxt",
											ALLOCSET_DEFAULT_SIZES);
		*entry_ptr = (Pointer) o_proc;
	}

	o_proc->rettype = procform->prorettype;
	o_proc->strict = procform->proisstrict;
	o_proc->retset = procform->proretset;
	o_proc->prolang = procform->prolang;
	o_proc->proowner = procform->proowner;
	o_proc->nargs = procform->pronargs;

	oldcontext = MemoryContextSwitchTo(o_proc->cxt);
	o_proc->proname = pstrdup(NameStr(procform->proname));

	o_proc->nargs = procform->pronargs;
	o_proc->argtypes = palloc0(o_proc->nargs * sizeof(Oid));
	memcpy(o_proc->argtypes, procform->proargtypes.values,
		   o_proc->nargs * sizeof(Oid));
	for (i = 0; i < o_proc->nargs; i++)
	{
		o_type_cache_add_if_needed(key->common.datoid, o_proc->argtypes[i],
								   key->common.lsn, NULL);
	}

	fmgr_symbol(procoid, &o_proc->probin, &o_proc->prosrc);

	if (o_proc->prolang == SQLlanguageId)
	{
		FmgrInfo   *finfo;
		FunctionCallInfo fcinfo;
		OClassArg	arg = {.sys_table = true};
		XLogRecPtr	cur_lsn;
		Oid			datoid;

		finfo = palloc0(sizeof(FmgrInfo));
		fcinfo = palloc0(SizeForFunctionCallInfo(2));
		fmgr_info(procoid, finfo);
		InitFunctionCallInfoData(*fcinfo, finfo, 2,
								 *fncollation, NULL, NULL);

		/* Check call context */
		if (fcinfo->flinfo->fn_retset)
		{
			ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;

			/*
			 * For simplicity, we require callers to support both set eval
			 * modes. There are cases where we must use one or must use the
			 * other, and it's not really worthwhile to postpone the check
			 * till we know. But note we do not require caller to provide an
			 * expectedDesc.
			 */
			if (!rsi || !IsA(rsi, ReturnSetInfo) ||
				(rsi->allowedModes & SFRM_ValuePerCall) == 0 ||
				(rsi->allowedModes & SFRM_Materialize) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("set-valued function called in context "
								"that cannot accept a set")));
			lazyEvalOK = !(rsi->allowedModes & SFRM_Materialize_Preferred);
		}
		else
		{
			lazyEvalOK = true;
		}

		o_proc->sql_func = palloc0(sizeof(sql_func_data));
		init_sql_fcache(fcinfo, fcinfo->fncollation, lazyEvalOK,
						o_proc->sql_func, o_proc->cxt);

		o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
		o_class_cache_add_if_needed(datoid, TypeRelationId, cur_lsn,
									(Pointer) &arg);
		for (i = 0; i < o_proc->sql_func->nnqtlists; i++)
		{
			int			j;

			for (j = 0; j < o_proc->sql_func->nqtlists[i]; j++)
			{
				PlannedStmt *pstmt;

				pstmt = castNode(PlannedStmt, o_proc->sql_func->qtlists[i][j]);

				o_collect_functions_pstmt(pstmt);
			}
		}
		fcache = (SQLFunctionCachePtr) fcinfo->flinfo->fn_extra;
		pfree(fcache);
		pfree(fcinfo);
		pfree(finfo);
	}
	MemoryContextSwitchTo(oldcontext);
	ReleaseSysCache(proctup);
}

void
o_proc_cache_free_entry(Pointer entry)
{
	OProc	   *o_proc = (OProc *) entry;

	MemoryContextDelete(o_proc->cxt);
	pfree(o_proc);
}

Pointer
o_proc_cache_serialize_entry(Pointer entry, int *len)
{
	int			i,
				j;
	StringInfoData str;
	OProc	   *o_proc = (OProc *) entry;

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_proc,
						   offsetof(OProc, proname));

	o_serialize_string(o_proc->proname, &str);
	o_serialize_string(o_proc->prosrc, &str);
	o_serialize_string(o_proc->probin, &str);
	appendBinaryStringInfo(&str, (Pointer) o_proc->argtypes,
						   sizeof(Oid) * o_proc->nargs);

	if (o_proc->prolang == SQLlanguageId)
	{
		sql_func_data *sql_func = o_proc->sql_func;

		o_serialize_string(sql_func->src, &str);
		appendBinaryStringInfo(&str, ((Pointer) sql_func) +
							   offsetof(sql_func_data, returnsTuple),
							   offsetof(sql_func_data, jf_targetList) -
							   offsetof(sql_func_data, returnsTuple));
		o_serialize_node((Node *) sql_func->jf_targetList, &str);
		appendBinaryStringInfo(&str, (Pointer) sql_func->jf_atts,
							   sizeof(Oid) * sql_func->jf_natts);

		if (sql_func->has_argnames)
			for (i = 0; i < o_proc->nargs; i++)
			{
				o_serialize_string(sql_func->argnames[i], &str);
			}

		appendBinaryStringInfo(&str, (Pointer) sql_func->nqtlists,
							   sizeof(int) * sql_func->nnqtlists);

		for (i = 0; i < sql_func->nnqtlists; i++)
		{
			for (j = 0; j < sql_func->nqtlists[i]; j++)
			{
				o_serialize_node(sql_func->qtlists[i][j], &str);
			}
		}
	}

	*len = str.len;
	return str.data;
}

Pointer
o_proc_cache_deserialize_entry(MemoryContext mcxt, Pointer data, Size length)
{
	Pointer		ptr = data;
	OProc	   *o_proc;
	int			len;
	MemoryContext old_mcxt;
	int			i,
				j;

	o_proc = (OProc *) palloc0(sizeof(OProc));
	len = offsetof(OProc, proname);
	Assert((ptr - data) + len <= length);
	memcpy(o_proc, ptr, len);
	ptr += len;

	o_proc->cxt = AllocSetContextCreate(mcxt, "o_proc mcxt",
										ALLOCSET_DEFAULT_SIZES);

	old_mcxt = MemoryContextSwitchTo(o_proc->cxt);
	o_proc->proname = o_deserialize_string(&ptr);
	o_proc->prosrc = o_deserialize_string(&ptr);
	o_proc->probin = o_deserialize_string(&ptr);

	len = sizeof(Oid) * o_proc->nargs;
	Assert((ptr - data) + len <= length);
	o_proc->argtypes = palloc0(len);
	memcpy(o_proc->argtypes, ptr, len);
	ptr += len;

	if (o_proc->prolang == SQLlanguageId)
	{
		sql_func_data *sql_func;

		sql_func = (sql_func_data *) palloc0(sizeof(sql_func_data));
		o_proc->sql_func = sql_func;

		sql_func->src = o_deserialize_string(&ptr);

		len = offsetof(sql_func_data, jf_targetList) -
			offsetof(sql_func_data, returnsTuple);
		Assert((ptr - data) + len <= length);
		memcpy(((Pointer) sql_func) + offsetof(sql_func_data, returnsTuple),
			   ptr, len);
		ptr += len;

		sql_func->jf_targetList = (List *) o_deserialize_node(&ptr);

		len = sizeof(Oid) * sql_func->jf_natts;
		Assert((ptr - data) + len <= length);
		sql_func->jf_atts = palloc0(len);
		memcpy(sql_func->jf_atts, ptr, len);
		ptr += len;

		len = sizeof(char *) * o_proc->nargs;
		if (sql_func->has_argnames)
		{
			sql_func->argnames = palloc0(len);
			for (i = 0; i < o_proc->nargs; i++)
			{
				sql_func->argnames[i] = o_deserialize_string(&ptr);
			}
		}

		len = sizeof(int) * sql_func->nnqtlists;
		Assert((ptr - data) + len <= length);
		sql_func->nqtlists = palloc0(len);
		memcpy(sql_func->nqtlists, ptr, len);
		ptr += len;
		sql_func->qtlists = palloc0(sql_func->nnqtlists * sizeof(Node **));
		for (i = 0; i < sql_func->nnqtlists; i++)
		{
			sql_func->qtlists[i] = palloc0(sql_func->nqtlists[i] *
										   sizeof(Node *));
			for (j = 0; j < sql_func->nqtlists[i]; j++)
			{
				sql_func->qtlists[i][j] = o_deserialize_node(&ptr);
			}
		}
	}

	MemoryContextSwitchTo(old_mcxt);

	return (Pointer) o_proc;
}

/*
 * Specialized DestReceiver for collecting query output in a SQL function
 */
typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Tuplestorestate *tstore;	/* where to put result tuples */
	MemoryContext cxt;			/* context containing tstore */
	JunkFilter *filter;			/* filter to convert tuple type */
} DR_sqlfunction;

/*
 * We have an execution_state record for each query in a function.  Each
 * record contains a plantree for its query.  If the query is currently in
 * F_EXEC_RUN state then there's a QueryDesc too.
 *
 * The "next" fields chain together all the execution_state records generated
 * from a single original parsetree.  (There will only be more than one in
 * case of rule expansion of the original parsetree.)
 */
typedef enum
{
	F_EXEC_START, F_EXEC_RUN, F_EXEC_DONE
} ExecStatus;

typedef struct execution_state
{
	struct execution_state *next;
	ExecStatus	status;
	bool		setsResult;		/* true if this query produces func's result */
	bool		lazyEval;		/* true if should fetch one row at a time */
	PlannedStmt *stmt;			/* plan for this query */
	QueryDesc  *qd;				/* null unless status == RUN */
} execution_state;

/*
 * Set up the per-query execution_state records for a SQL function.
 *
 * The input is a List of Lists of parsed and rewritten, but not planned,
 * querytrees.  The sublist structure denotes the original query boundaries.
 */
static List *
init_execution_state(List *queryTree_list,
					 SQLFunctionCachePtr fcache,
					 bool lazyEvalOK)
{
	List	   *eslist = NIL;
	execution_state *lasttages = NULL;
	ListCell   *lc1;

	foreach(lc1, queryTree_list)
	{
		List	   *qtlist = lfirst_node(List, lc1);
		execution_state *firstes = NULL;
		execution_state *preves = NULL;
		ListCell   *lc2;

		foreach(lc2, qtlist)
		{
			Query	   *queryTree = lfirst_node(Query, lc2);
			PlannedStmt *stmt;
			execution_state *newes;

			/* Plan the query if needed */
			if (queryTree->commandType == CMD_UTILITY)
			{
				/* Utility commands require no planning. */
				stmt = makeNode(PlannedStmt);
				stmt->commandType = CMD_UTILITY;
				stmt->canSetTag = queryTree->canSetTag;
				stmt->utilityStmt = queryTree->utilityStmt;
				stmt->stmt_location = queryTree->stmt_location;
				stmt->stmt_len = queryTree->stmt_len;
			}
			else
				stmt = pg_plan_query(queryTree,
									 fcache->src,
									 0,
									 NULL);

			/*
			 * Precheck all commands for validity in a function.  This should
			 * generally match the restrictions spi.c applies.
			 */
			if (stmt->commandType == CMD_UTILITY)
			{
				if (IsA(stmt->utilityStmt, CopyStmt) &&
					((CopyStmt *) stmt->utilityStmt)->filename == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot COPY to/from client in an SQL function")));

				if (IsA(stmt->utilityStmt, TransactionStmt))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					/* translator: %s is a SQL statement name */
							 errmsg("%s is not allowed in an SQL function",
									CreateCommandName(stmt->utilityStmt))));
			}

			if (fcache->readonly_func && !CommandIsReadOnly(stmt))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				/* translator: %s is a SQL statement name */
						 errmsg("%s is not allowed in a non-volatile function",
								CreateCommandName((Node *) stmt))));

			/* OK, build the execution_state for this query */
			newes = (execution_state *) palloc(sizeof(execution_state));
			if (preves)
				preves->next = newes;
			else
				firstes = newes;

			newes->next = NULL;
			newes->status = F_EXEC_START;
			newes->setsResult = false;	/* might change below */
			newes->lazyEval = false;	/* might change below */
			newes->stmt = stmt;
			newes->qd = NULL;

			if (queryTree->canSetTag)
				lasttages = newes;

			preves = newes;
		}

		eslist = lappend(eslist, firstes);
	}

	/*
	 * Mark the last canSetTag query as delivering the function result; then,
	 * if it is a plain SELECT, mark it for lazy evaluation. If it's not a
	 * SELECT we must always run it to completion.
	 *
	 * Note: at some point we might add additional criteria for whether to use
	 * lazy eval.  However, we should prefer to use it whenever the function
	 * doesn't return set, since fetching more than one row is useless in that
	 * case.
	 *
	 * Note: don't set setsResult if the function returns VOID, as evidenced
	 * by not having made a junkfilter.  This ensures we'll throw away any
	 * output from the last statement in such a function.
	 */
	if (lasttages && fcache->junkFilter)
	{
		lasttages->setsResult = true;
		if (lazyEvalOK &&
			lasttages->stmt->commandType == CMD_SELECT &&
			!lasttages->stmt->hasModifyingCTE)
			fcache->lazyEval = lasttages->lazyEval = true;
	}

	return eslist;
}

static void
jf_cleanTupType_init_entry(TupleDesc desc,
						   AttrNumber attributeNumber,
						   const char *attributeName,
						   Oid oidtypeid,
						   int32 typmod,
						   int attdim,
						   Oid jf_atttypid)
{
	Form_pg_attribute att;

	/*
	 * sanity checks
	 */
	Assert(PointerIsValid(desc));
	Assert(attributeNumber >= 1);
	Assert(attributeNumber <= desc->natts);

	/*
	 * initialize the attribute fields
	 */
	att = TupleDescAttr(desc, attributeNumber - 1);

	att->attrelid = 0;			/* dummy value */

	/*
	 * Note: attributeName can be NULL, because the planner doesn't always
	 * fill in valid resname values in targetlists, particularly for resjunk
	 * attributes. Also, do nothing if caller wants to re-use the old attname.
	 */
	if (attributeName == NULL)
		MemSet(NameStr(att->attname), 0, NAMEDATALEN);
	else if (attributeName != NameStr(att->attname))
		namestrcpy(&(att->attname), attributeName);

	att->attstattarget = -1;
	att->attcacheoff = -1;
	att->atttypmod = typmod;

	att->attnum = attributeNumber;
	att->attndims = attdim;

	att->attnotnull = false;
	att->atthasdef = false;
	att->atthasmissing = false;
	att->attidentity = '\0';
	att->attgenerated = '\0';
	att->attisdropped = false;
	att->attislocal = true;
	att->attinhcount = 0;
	/* attacl, attoptions and attfdwoptions are not present in tupledescs */

	att->atttypid = oidtypeid;
	o_type_cache_fill_info(jf_atttypid, &att->attlen, &att->attbyval,
						   &att->attalign, &att->attstorage,
						   &att->attcollation);
#if PG_VERSION_NUM >= 140000
	att->attcompression = InvalidCompressionMethod;
#endif
}

/*
 * Prepare the SQLFunctionParseInfo struct for parsing a SQL function body
 *
 * This includes resolving actual types of polymorphic arguments.
 *
 * call_expr can be passed as NULL, but then we will fail if there are any
 * polymorphic arguments.
 */
static SQLFunctionParseInfoPtr
o_prepare_sql_fn_parse_info(OProc *o_proc, Node *call_expr, Oid inputCollation)
{
	SQLFunctionParseInfoPtr pinfo;
	int			nargs;
	sql_func_data *sql_func = o_proc->sql_func;

	pinfo = (SQLFunctionParseInfoPtr) palloc0(sizeof(SQLFunctionParseInfo));

	/* Function's name (only) can be used to qualify argument names */
	pinfo->fname = pstrdup(o_proc->proname);

	/* Save the function's input collation */
	pinfo->collation = inputCollation;

	/*
	 * Copy input argument types from the pg_proc entry, then resolve any
	 * polymorphic types.
	 */
	pinfo->nargs = nargs = o_proc->nargs;
	if (nargs > 0)
	{
		Oid		   *argOidVect;
		int			argnum;

		argOidVect = (Oid *) palloc(nargs * sizeof(Oid));
		memcpy(argOidVect, o_proc->argtypes, nargs * sizeof(Oid));

		for (argnum = 0; argnum < nargs; argnum++)
		{
			Oid			argtype = argOidVect[argnum];

			if (IsPolymorphicType(argtype))
			{
				argtype = get_call_expr_argtype(call_expr, argnum);
				if (argtype == InvalidOid)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("could not determine actual type of argument declared %s",
									format_type_be(argOidVect[argnum]))));
				argOidVect[argnum] = argtype;
			}
		}

		pinfo->argtypes = argOidVect;
	}

	/*
	 * Collect names of arguments, too, if any
	 */
	if (nargs > 0)
	{
		pinfo->argnames = sql_func->argnames;
	}
	else
		pinfo->argnames = NULL;

	return pinfo;
}

/*
 * Initialize the SQLFunctionCache for a SQL function
 */
static void
init_sql_fcache(FunctionCallInfo fcinfo, Oid collation, bool lazyEvalOK,
				sql_func_data *out_sql_func, MemoryContext out_sql_func_cxt)
{
	FmgrInfo   *finfo = fcinfo->flinfo;
	Oid			foid = finfo->fn_oid;
	MemoryContext fcontext;
	MemoryContext oldcontext;
	Oid			rettype;
	TupleDesc	rettupdesc;
	HeapTuple	procedureTuple = NULL;
	Form_pg_proc procedureStruct;
	SQLFunctionCachePtr fcache;
	List	   *queryTree_list = NIL;
	List	   *resulttlist;
	Datum		tmp;
	bool		isNull;
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OProc	   *o_proc;
	sql_func_data *sql_func = NULL;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_proc = o_proc_cache_search(datoid, fcinfo->flinfo->fn_oid, cur_lsn,
								 proc_cache->nkeys);
	if (o_proc)
		sql_func = o_proc->sql_func;

	/*
	 * Create memory context that holds all the SQLFunctionCache data.  It
	 * must be a child of whatever context holds the FmgrInfo.
	 */
	fcontext = AllocSetContextCreate(finfo->fn_mcxt,
									 "SQL function",
									 ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(fcontext);

	/*
	 * Create the struct proper, link it to fcontext and fn_extra.  Once this
	 * is done, we'll be able to recover the memory after failure, even if the
	 * FmgrInfo is long-lived.
	 */
	fcache = (SQLFunctionCachePtr) palloc0(sizeof(SQLFunctionCache));
	fcache->fcontext = fcontext;
	finfo->fn_extra = (void *) fcache;

	if (o_proc)
	{
		fcache->fname = pstrdup(o_proc->proname);
	}
	else
	{
		/*
		 * get the procedure tuple corresponding to the given function Oid
		 */
		procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(foid));
		if (!HeapTupleIsValid(procedureTuple))
			elog(ERROR, "cache lookup failed for function %u", foid);
		procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

		/*
		 * copy function name immediately for use by error reporting callback,
		 * and for use as memory context identifier
		 */
		fcache->fname = pstrdup(NameStr(procedureStruct->proname));
		MemoryContextSetIdentifier(fcontext, fcache->fname);
	}

	if (o_proc)
	{
		rettype = o_proc->rettype;
	}
	else
	{
		/*
		 * Resolve any polymorphism, obtaining the actual result type, and the
		 * corresponding tupdesc if it's a rowtype.
		 */
		(void) get_call_result_type(fcinfo, &rettype, &rettupdesc);
	}
	fcache->rettype = rettype;

	if (o_proc)
	{
		o_type_cache_fill_info(o_proc->rettype,
							   &fcache->typlen, &fcache->typbyval,
							   NULL, NULL, NULL);
		fcache->returnsSet = o_proc->retset;
		fcache->readonly_func = sql_func->readonly_func;
	}
	else
	{
		/* Fetch the typlen and byval info for the result type */
		get_typlenbyval(rettype, &fcache->typlen, &fcache->typbyval);

		/* Remember whether we're returning setof something */
		fcache->returnsSet = procedureStruct->proretset;


		/* Remember if function is STABLE/IMMUTABLE */
		fcache->readonly_func =
			(procedureStruct->provolatile != PROVOLATILE_VOLATILE);
	}

	if (o_proc)
	{
		/*
		 * We need the actual argument types to pass to the parser.  Also make
		 * sure that parameter symbols are considered to have the function's
		 * resolved input collation.
		 */
		fcache->pinfo = o_prepare_sql_fn_parse_info(o_proc,
													finfo->fn_expr,
													collation);
	}
	else
	{
		/*
		 * We need the actual argument types to pass to the parser.  Also make
		 * sure that parameter symbols are considered to have the function's
		 * resolved input collation.
		 */
		fcache->pinfo = prepare_sql_fn_parse_info(procedureTuple,
												  finfo->fn_expr,
												  collation);
	}

	if (o_proc)
	{
		fcache->src = pstrdup(sql_func->src);
	}
	else
	{
		/*
		 * And of course we need the function body text.
		 */
		tmp = SysCacheGetAttr(PROCOID,
							  procedureTuple,
							  Anum_pg_proc_prosrc,
							  &isNull);
		if (isNull)
			elog(ERROR, "null prosrc for function %u", foid);
		fcache->src = TextDatumGetCString(tmp);

#if PG_VERSION_NUM < 140000
		isNull = true;
#elif PG_VERSION_NUM >= 140000
		/* If we have prosqlbody, pay attention to that not prosrc. */
		tmp = SysCacheGetAttr(PROCOID,
							  procedureTuple,
							  Anum_pg_proc_prosqlbody,
							  &isNull);
#endif

		/*
		 * Parse and rewrite the queries in the function text.  Use sublists
		 * to keep track of the original query boundaries.
		 *
		 * Note: since parsing and planning is done in fcontext, we will
		 * generate a lot of cruft that lives as long as the fcache does. This
		 * is annoying but we'll not worry about it until the module is
		 * rewritten to use plancache.c.
		 */
		queryTree_list = NIL;
		if (!isNull)
		{
			Node	   *n;
			List	   *stored_query_list;
			ListCell   *lc;

			n = stringToNode(TextDatumGetCString(tmp));
			if (IsA(n, List))
				stored_query_list = linitial_node(List, castNode(List, n));
			else
				stored_query_list = list_make1(n);

			foreach(lc, stored_query_list)
			{
				Query	   *parsetree = lfirst_node(Query, lc);
				List	   *queryTree_sublist;

				AcquireRewriteLocks(parsetree, true, false);
				queryTree_sublist = pg_rewrite_query(parsetree);
				queryTree_list = lappend(queryTree_list, queryTree_sublist);
			}
		}
		else
		{
			List	   *raw_parsetree_list;
			ListCell   *lc;

			raw_parsetree_list = pg_parse_query(fcache->src);

			foreach(lc, raw_parsetree_list)
			{
				RawStmt    *parsetree = lfirst_node(RawStmt, lc);
				List	   *queryTree_sublist;

				queryTree_sublist = pg_analyze_and_rewrite_params(
																  parsetree,
																  fcache->src,
																  (ParserSetupHook) sql_fn_parser_setup,
																  fcache->pinfo,
																  NULL);
				queryTree_list = lappend(queryTree_list, queryTree_sublist);
			}
		}

		/*
		 * Check that there are no statements we don't want to allow.
		 */
		check_sql_fn_statements(queryTree_list);
	}

	if (o_proc)
	{
		fcache->returnsTuple = sql_func->returnsTuple;
	}
	else
	{
		/*
		 * Check that the function returns the type it claims to.  Although in
		 * simple cases this was already done when the function was defined,
		 * we have to recheck because database objects used in the function's
		 * queries might have changed type.  We'd have to recheck anyway if
		 * the function had any polymorphic arguments.  Moreover,
		 * check_sql_fn_retval takes care of injecting any required column
		 * type coercions.  (But we don't ask it to insert nulls for dropped
		 * columns; the junkfilter handles that.)
		 *
		 * Note: we set fcache->returnsTuple according to whether we are
		 * returning the whole tuple result or just a single column.  In the
		 * latter case we clear returnsTuple because we need not act different
		 * from the scalar result case, even if it's a rowtype column.
		 * (However, we have to force lazy eval mode in that case; otherwise
		 * we'd need extra code to expand the rowtype column into multiple
		 * columns, since we have no way to notify the caller that it should
		 * do that.)
		 */
		fcache->returnsTuple = check_sql_fn_retval(queryTree_list,
												   rettype,
												   rettupdesc,
												   false,
												   &resulttlist);
	}

	/*
	 * Construct a JunkFilter we can use to coerce the returned rowtype to the
	 * desired form, unless the result type is VOID, in which case there's
	 * nothing to coerce to.  (XXX Frequently, the JunkFilter isn't doing
	 * anything very interesting, but much of this module expects it to be
	 * there anyway.)
	 */
	if (rettype != VOIDOID)
	{
		if (o_proc)
		{
			int			cleanLength;
			AttrNumber *cleanMap;
			TupleTableSlot *slot;
			int			len;
			TupleDesc	typeInfo;
			int			cur_resno = 1;
			ListCell   *l;

			fcache->junkFilter = makeNode(JunkFilter);
			fcache->junkFilter->jf_targetList = sql_func->jf_targetList;

			len = ExecCleanTargetListLength(sql_func->jf_targetList);
			typeInfo = CreateTemplateTupleDesc(len);
			foreach(l, sql_func->jf_targetList)
			{
				TargetEntry *tle = lfirst(l);

				if (tle->resjunk)
					continue;

				jf_cleanTupType_init_entry(typeInfo,
										   cur_resno,
										   tle->resname,
										   exprType((Node *) tle->expr),
										   exprTypmod((Node *) tle->expr),
										   0,
										   sql_func->jf_atts[cur_resno - 1]);
				TupleDescInitEntryCollation(typeInfo,
											cur_resno,
											exprCollation((Node *) tle->expr));
				cur_resno++;
			}

			fcache->junkFilter->jf_cleanTupType = typeInfo;

			/*
			 * Now calculate the mapping between the original tuple's
			 * attributes and the "clean" tuple's attributes.
			 *
			 * The "map" is an array of "cleanLength" attribute numbers, i.e.
			 * one entry for every attribute of the "clean" tuple. The value
			 * of this entry is the attribute number of the corresponding
			 * attribute of the "original" tuple.  (Zero indicates a NULL
			 * output attribute, but we do not use that feature in this
			 * routine.)
			 */
			cleanLength = typeInfo->natts;
			if (cleanLength > 0)
			{
				AttrNumber	cleanResno;
				ListCell   *t;

				cleanMap = (AttrNumber *) palloc(cleanLength * sizeof(AttrNumber));
				cleanResno = 0;
				foreach(t, sql_func->jf_targetList)
				{
					TargetEntry *tle = lfirst(t);

					if (!tle->resjunk)
					{
						cleanMap[cleanResno] = tle->resno;
						cleanResno++;
					}
				}
				Assert(cleanResno == cleanLength);
			}
			else
				cleanMap = NULL;

			slot = MakeSingleTupleTableSlot(NULL, &TTSOpsMinimalTuple);
			ExecSetSlotDescriptor(slot, typeInfo);
			fcache->junkFilter->jf_cleanMap = cleanMap;
			fcache->junkFilter->jf_resultSlot = slot;
		}
		else
		{
			TupleTableSlot *slot = MakeSingleTupleTableSlot(NULL,
															&TTSOpsMinimalTuple);

			/*
			 * If the result is composite, *and* we are returning the whole
			 * tuple result, we need to insert nulls for any dropped columns.
			 * In the single-column-result case, there might be dropped
			 * columns within the composite column value, but it's not our
			 * problem here.  There should be no resjunk entries in
			 * resulttlist, so in the second case the JunkFilter is certainly
			 * a no-op.
			 */
			if (rettupdesc && fcache->returnsTuple)
				fcache->junkFilter = ExecInitJunkFilterConversion(resulttlist,
																  rettupdesc,
																  slot);
			else
				fcache->junkFilter = ExecInitJunkFilter(resulttlist, slot);
		}
	}

	if (fcache->returnsTuple)
	{
		/* Make sure output rowtype is properly blessed */
		BlessTupleDesc(fcache->junkFilter->jf_resultSlot->tts_tupleDescriptor);
	}
	else if (fcache->returnsSet && type_is_rowtype(fcache->rettype))
	{
		/*
		 * Returning rowtype as if it were scalar --- materialize won't work.
		 * Right now it's sufficient to override any caller preference for
		 * materialize mode, but to add more smarts in init_execution_state
		 * about this, we'd probably need a three-way flag instead of bool.
		 */
		lazyEvalOK = true;
	}

	if (o_proc)
	{
		execution_state *lastes = NULL;
		int			i,
					j;

		fcache->func_state = NIL;
		fcache->lazyEval = sql_func->lazyEval;
		for (i = 0; i < sql_func->nnqtlists; i++)
		{
			execution_state *firstes = NULL;
			execution_state *preves = NULL;

			for (j = 0; j < sql_func->nqtlists[i]; j++)
			{
				execution_state *newes;

				newes = (execution_state *) palloc0(sizeof(execution_state));
				if (preves)
					preves->next = newes;
				else
					firstes = newes;

				newes->next = NULL;
				newes->status = F_EXEC_START;
				newes->setsResult = false;	/* might change below */
				newes->lazyEval = false;	/* might change below */
				newes->stmt = (PlannedStmt *)
					copyObject(sql_func->qtlists[i][j]);
				newes->qd = NULL;

				preves = newes;
				lastes = newes;
			}
			fcache->func_state = lappend(fcache->func_state, firstes);
		}
		if (lastes && fcache->junkFilter)
		{
			lastes->setsResult = true;
			lastes->lazyEval = fcache->lazyEval;
		}
	}
	else
	{
		/* Finally, plan the queries */
		fcache->func_state = init_execution_state(queryTree_list,
												  fcache,
												  lazyEvalOK);
	}

	/* Mark fcache with time of creation to show it's valid */
	fcache->lxid = MyProc->lxid;
	fcache->subxid = GetCurrentSubTransactionId();

	if (!o_proc)
	{
		ReleaseSysCache(procedureTuple);
	}

	MemoryContextSwitchTo(oldcontext);

	if (out_sql_func)
	{
		int			i,
					j;
		ListCell   *l,
				   *lc;
		int			cur_resno = 1;
		JunkFilter *jf;

		oldcontext = MemoryContextSwitchTo(out_sql_func_cxt);

		out_sql_func->src = pstrdup(fcache->src);
		out_sql_func->returnsTuple = fcache->returnsTuple;
		out_sql_func->readonly_func = fcache->readonly_func;
		out_sql_func->lazyEval = fcache->lazyEval;
		jf = fcache->junkFilter;
		if (jf)
			out_sql_func->jf_targetList = copyObject(jf->jf_targetList);
		Assert(procedureTuple);
		procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);
		if (fcache->pinfo->argnames)
		{
			out_sql_func->argnames = palloc0(procedureStruct->pronargs *
											 sizeof(char *));
			for (i = 0; i < procedureStruct->pronargs; i++)
			{
				if (fcache->pinfo->argnames[i])
					out_sql_func->argnames[i] =
						pstrdup(fcache->pinfo->argnames[i]);
			}
			out_sql_func->has_argnames = true;
		}
		else
		{
			out_sql_func->argnames = NULL;
			out_sql_func->has_argnames = false;
		}

		if (jf)
		{
			out_sql_func->jf_natts = jf->jf_cleanTupType->natts;
			out_sql_func->jf_atts = palloc0(sizeof(Oid) *
											out_sql_func->jf_natts);
			foreach(l, out_sql_func->jf_targetList)
			{
				TargetEntry *tle = lfirst(l);
				FormData_pg_attribute *att;

				if (tle->resjunk)
					continue;

				att = &jf->jf_cleanTupType->attrs[cur_resno - 1];
				out_sql_func->jf_atts[cur_resno - 1] = att->atttypid;
				o_type_cache_add_if_needed(datoid, att->atttypid, cur_lsn,
										   NULL);
				cur_resno++;
			}
		}

		out_sql_func->nnqtlists = list_length(fcache->func_state);
		out_sql_func->nqtlists = palloc0(sizeof(int) *
										 out_sql_func->nnqtlists);
		out_sql_func->qtlists = palloc0(sizeof(Node **) *
										out_sql_func->nnqtlists);

		lc = list_head(fcache->func_state);
		for (i = 0; i < out_sql_func->nnqtlists; i++)
		{
			int			len;
			execution_state *head;
			execution_state *cur;

			head = lfirst(lc);
			cur = head;
			len = 0;
			while (cur)
			{
				len++;
				cur = cur->next;
			}

			out_sql_func->nqtlists[i] = len;
			out_sql_func->qtlists[i] = palloc0(sizeof(Node *) * len);
			cur = head;
			for (j = 0; j < len; j++)
			{
				out_sql_func->qtlists[i][j] = (Node *) copyObject(cur->stmt);
				cur = cur->next;
			}
			lc = lnext(fcache->func_state, lc);
		}

		MemoryContextSwitchTo(oldcontext);
	}
}

/* Start up execution of one execution_state node */
static void
postquel_start(execution_state *es, SQLFunctionCachePtr fcache)
{
	DestReceiver *dest;

	Assert(es->qd == NULL);

	/*
	 * If this query produces the function result, send its output to the
	 * tuplestore; else discard any output.
	 */
	if (es->setsResult)
	{
		DR_sqlfunction *myState;

		dest = CreateDestReceiver(DestSQLFunction);
		/* pass down the needed info to the dest receiver routines */
		myState = (DR_sqlfunction *) dest;
		Assert(myState->pub.mydest == DestSQLFunction);
		myState->tstore = fcache->tstore;
		myState->cxt = CurrentMemoryContext;
		myState->filter = fcache->junkFilter;
	}
	else
		dest = None_Receiver;

	es->qd = CreateQueryDesc(es->stmt,
							 fcache->src,
							 InvalidSnapshot,
							 InvalidSnapshot,
							 dest,
							 fcache->paramLI,
							 es->qd ? es->qd->queryEnv : NULL,
							 0);

	/* Utility commands don't need Executor. */
	if (es->qd->operation != CMD_UTILITY)
	{
		/*
		 * In lazyEval mode, do not let the executor set up an AfterTrigger
		 * context.  This is necessary not just an optimization, because we
		 * mustn't exit from the function execution with a stacked
		 * AfterTrigger level still active.  We are careful not to select
		 * lazyEval mode for any statement that could possibly queue triggers.
		 */
		int			eflags;

		if (es->lazyEval)
			eflags = EXEC_FLAG_SKIP_TRIGGERS;
		else
			eflags = 0;			/* default run-to-completion flags */
		ExecutorStart(es->qd, eflags);
	}

	es->status = F_EXEC_RUN;
}

/* Run one execution_state; either to completion or to first result row */
/* Returns true if we ran to completion */
static bool
postquel_getnext(execution_state *es, SQLFunctionCachePtr fcache)
{
	bool		result;

	if (es->qd->operation == CMD_UTILITY)
	{
		ProcessUtility(es->qd->plannedstmt,
					   fcache->src,
#if PG_VERSION_NUM >= 140000
					   false,
#endif
					   PROCESS_UTILITY_QUERY,
					   es->qd->params,
					   es->qd->queryEnv,
					   es->qd->dest,
					   NULL);
		result = true;			/* never stops early */
	}
	else
	{
		/* Run regular commands to completion unless lazyEval */
		uint64		count = (es->lazyEval) ? 1 : 0;

		ExecutorRun(es->qd, ForwardScanDirection, count, !fcache->returnsSet || !es->lazyEval);

		/*
		 * If we requested run to completion OR there was no tuple returned,
		 * command must be complete.
		 */
		result = (count == 0 || es->qd->estate->es_processed == 0);
	}

	return result;
}

/* Shut down execution of one execution_state node */
static void
postquel_end(execution_state *es)
{
	/* mark status done to ensure we don't do ExecutorEnd twice */
	es->status = F_EXEC_DONE;

	/* Utility commands don't need Executor. */
	if (es->qd->operation != CMD_UTILITY)
	{
		ExecutorFinish(es->qd);
		ExecutorEnd(es->qd);
	}

	es->qd->dest->rDestroy(es->qd->dest);

	FreeQueryDesc(es->qd);
	es->qd = NULL;
}

/* Build ParamListInfo array representing current arguments */
static void
postquel_sub_params(SQLFunctionCachePtr fcache,
					FunctionCallInfo fcinfo)
{
	int			nargs = fcinfo->nargs;

	if (nargs > 0)
	{
		ParamListInfo paramLI;

		if (fcache->paramLI == NULL)
		{
			paramLI = makeParamList(nargs);
			fcache->paramLI = paramLI;
		}
		else
		{
			paramLI = fcache->paramLI;
			Assert(paramLI->numParams == nargs);
		}

		for (int i = 0; i < nargs; i++)
		{
			ParamExternData *prm = &paramLI->params[i];

			prm->value = fcinfo->args[i].value;
			prm->isnull = fcinfo->args[i].isnull;
			prm->pflags = 0;
			prm->ptype = fcache->pinfo->argtypes[i];
		}
	}
	else
		fcache->paramLI = NULL;
}

/*
 * Extract the SQL function's value from a single result row.  This is used
 * both for scalar (non-set) functions and for each row of a lazy-eval set
 * result.
 */
static Datum
postquel_get_single_result(TupleTableSlot *slot,
						   FunctionCallInfo fcinfo,
						   SQLFunctionCachePtr fcache,
						   MemoryContext resultcontext)
{
	Datum		value;
	MemoryContext oldcontext;

	/*
	 * Set up to return the function value.  For pass-by-reference datatypes,
	 * be sure to allocate the result in resultcontext, not the current memory
	 * context (which has query lifespan).  We can't leave the data in the
	 * TupleTableSlot because we intend to clear the slot before returning.
	 */
	oldcontext = MemoryContextSwitchTo(resultcontext);

	if (fcache->returnsTuple)
	{
		/* We must return the whole tuple as a Datum. */
		fcinfo->isnull = false;
		value = ExecFetchSlotHeapTupleDatum(slot);
	}
	else
	{
		/*
		 * Returning a scalar, which we have to extract from the first column
		 * of the SELECT result, and then copy into result context if needed.
		 */
		value = slot_getattr(slot, 1, &(fcinfo->isnull));

		if (!fcinfo->isnull)
			value = datumCopy(value, fcache->typbyval, fcache->typlen);
	}

	MemoryContextSwitchTo(oldcontext);

	return value;
}

/*
 * callback function in case a function-returning-set needs to be shut down
 * before it has been run to completion
 */
static void
ShutdownSQLFunction(Datum arg)
{
	SQLFunctionCachePtr fcache = (SQLFunctionCachePtr) DatumGetPointer(arg);
	execution_state *es;
	ListCell   *lc;

	foreach(lc, fcache->func_state)
	{
		es = (execution_state *) lfirst(lc);
		while (es)
		{
			/* Shut down anything still running */
			if (es->status == F_EXEC_RUN)
			{
				/* Re-establish active snapshot for any called functions */
				if (!fcache->readonly_func)
					PushActiveSnapshot(es->qd->snapshot);

				postquel_end(es);

				if (!fcache->readonly_func)
					PopActiveSnapshot();
			}

			/* Reset states to START in case we're called again */
			es->status = F_EXEC_START;
			es = es->next;
		}
	}

	/* Release tuplestore if we have one */
	if (fcache->tstore)
		tuplestore_end(fcache->tstore);
	fcache->tstore = NULL;

	/* execUtils will deregister the callback... */
	fcache->shutdown_reg = false;
}

/*
 * o_fmgr_sql: function call manager for SQL functions
 */
Datum
o_fmgr_sql(PG_FUNCTION_ARGS)
{
	SQLFunctionCachePtr fcache;
	ErrorContextCallback sqlerrcontext;
	MemoryContext oldcontext;
	bool		randomAccess;
	bool		lazyEvalOK;
	bool		is_first;
	bool		pushed_snapshot;
	execution_state *es;
	TupleTableSlot *slot;
	Datum		result;
	List	   *eslist;
	ListCell   *eslc;

	/*
	 * Setup error traceback support for ereport()
	 */
	sqlerrcontext.callback = sql_exec_error_callback;
	sqlerrcontext.arg = fcinfo->flinfo;
	sqlerrcontext.previous = error_context_stack;
	error_context_stack = &sqlerrcontext;

	/* Check call context */
	if (fcinfo->flinfo->fn_retset)
	{
		ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;

		/*
		 * For simplicity, we require callers to support both set eval modes.
		 * There are cases where we must use one or must use the other, and
		 * it's not really worthwhile to postpone the check till we know. But
		 * note we do not require caller to provide an expectedDesc.
		 */
		if (!rsi || !IsA(rsi, ReturnSetInfo) ||
			(rsi->allowedModes & SFRM_ValuePerCall) == 0 ||
			(rsi->allowedModes & SFRM_Materialize) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("set-valued function called in context that cannot accept a set")));
		randomAccess = rsi->allowedModes & SFRM_Materialize_Random;
		lazyEvalOK = !(rsi->allowedModes & SFRM_Materialize_Preferred);
	}
	else
	{
		randomAccess = false;
		lazyEvalOK = true;
	}

	/*
	 * Initialize fcache (build plans) if first time through; or re-initialize
	 * if the cache is stale.
	 */
	fcache = (SQLFunctionCachePtr) fcinfo->flinfo->fn_extra;

	if (fcache != NULL)
	{
		if (fcache->lxid != MyProc->lxid ||
			!SubTransactionIsActive(fcache->subxid))
		{
			/* It's stale; unlink and delete */
			fcinfo->flinfo->fn_extra = NULL;
			MemoryContextDelete(fcache->fcontext);
			fcache = NULL;
		}
	}

	if (fcache == NULL)
	{
		init_sql_fcache(fcinfo, fcinfo->fncollation, lazyEvalOK, NULL, NULL);
		fcache = (SQLFunctionCachePtr) fcinfo->flinfo->fn_extra;
	}

	/*
	 * Switch to context in which the fcache lives.  This ensures that our
	 * tuplestore etc will have sufficient lifetime.  The sub-executor is
	 * responsible for deleting per-tuple information.  (XXX in the case of a
	 * long-lived FmgrInfo, this policy represents more memory leakage, but
	 * it's not entirely clear where to keep stuff instead.)
	 */
	oldcontext = MemoryContextSwitchTo(fcache->fcontext);

	/*
	 * Find first unfinished query in function, and note whether it's the
	 * first query.
	 */
	eslist = fcache->func_state;
	es = NULL;
	is_first = true;
	foreach(eslc, eslist)
	{
		es = (execution_state *) lfirst(eslc);

		while (es && es->status == F_EXEC_DONE)
		{
			is_first = false;
			es = es->next;
		}

		if (es)
			break;
	}

	/*
	 * Convert params to appropriate format if starting a fresh execution. (If
	 * continuing execution, we can re-use prior params.)
	 */
	if (is_first && es && es->status == F_EXEC_START)
		postquel_sub_params(fcache, fcinfo);

	/*
	 * Build tuplestore to hold results, if we don't have one already. Note
	 * it's in the query-lifespan context.
	 */
	if (!fcache->tstore)
		fcache->tstore = tuplestore_begin_heap(randomAccess, false, work_mem);

	/*
	 * Execute each command in the function one after another until we either
	 * run out of commands or get a result row from a lazily-evaluated SELECT.
	 *
	 * Notes about snapshot management:
	 *
	 * In a read-only function, we just use the surrounding query's snapshot.
	 *
	 * In a non-read-only function, we rely on the fact that we'll never
	 * suspend execution between queries of the function: the only reason to
	 * suspend execution before completion is if we are returning a row from a
	 * lazily-evaluated SELECT.  So, when first entering this loop, we'll
	 * either start a new query (and push a fresh snapshot) or re-establish
	 * the active snapshot from the existing query descriptor.  If we need to
	 * start a new query in a subsequent execution of the loop, either we need
	 * a fresh snapshot (and pushed_snapshot is false) or the existing
	 * snapshot is on the active stack and we can just bump its command ID.
	 */
	pushed_snapshot = false;
	while (es)
	{
		bool		completed;

		if (es->status == F_EXEC_START)
		{

			/*
			 * If not read-only, be sure to advance the command counter for
			 * each command, so that all work to date in this transaction is
			 * visible.  Take a new snapshot if we don't have one yet,
			 * otherwise just bump the command ID in the existing snapshot.
			 */
			if (!fcache->readonly_func)
			{
				CommandCounterIncrement();
				if (!pushed_snapshot)
				{
					PushActiveSnapshot(GetTransactionSnapshot());
					pushed_snapshot = true;
				}
				else
					UpdateActiveSnapshotCommandId();
			}

			postquel_start(es, fcache);
		}
		else if (!fcache->readonly_func && !pushed_snapshot)
		{
			/* Re-establish active snapshot when re-entering function */
			PushActiveSnapshot(es->qd->snapshot);
			pushed_snapshot = true;
		}

		completed = postquel_getnext(es, fcache);

		/*
		 * If we ran the command to completion, we can shut it down now. Any
		 * row(s) we need to return are safely stashed in the tuplestore, and
		 * we want to be sure that, for example, AFTER triggers get fired
		 * before we return anything.  Also, if the function doesn't return
		 * set, we can shut it down anyway because it must be a SELECT and we
		 * don't care about fetching any more result rows.
		 */
		if (completed || !fcache->returnsSet)
			postquel_end(es);

		/*
		 * Break from loop if we didn't shut down (implying we got a
		 * lazily-evaluated row).  Otherwise we'll press on till the whole
		 * function is done, relying on the tuplestore to keep hold of the
		 * data to eventually be returned.  This is necessary since an
		 * INSERT/UPDATE/DELETE RETURNING that sets the result might be
		 * followed by additional rule-inserted commands, and we want to
		 * finish doing all those commands before we return anything.
		 */
		if (es->status != F_EXEC_DONE)
			break;

		/*
		 * Advance to next execution_state, which might be in the next list.
		 */
		es = es->next;
		while (!es)
		{
			eslc = lnext(eslist, eslc);
			if (!eslc)
				break;			/* end of function */

			es = (execution_state *) lfirst(eslc);

			/*
			 * Flush the current snapshot so that we will take a new one for
			 * the new query list.  This ensures that new snaps are taken at
			 * original-query boundaries, matching the behavior of interactive
			 * execution.
			 */
			if (pushed_snapshot)
			{
				PopActiveSnapshot();
				pushed_snapshot = false;
			}
		}
	}

	/*
	 * The tuplestore now contains whatever row(s) we are supposed to return.
	 */
	if (fcache->returnsSet)
	{
		ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;

		if (es)
		{
			/*
			 * If we stopped short of being done, we must have a lazy-eval
			 * row.
			 */
			Assert(es->lazyEval);
			/* Re-use the junkfilter's output slot to fetch back the tuple */
			Assert(fcache->junkFilter);
			slot = fcache->junkFilter->jf_resultSlot;
			if (!tuplestore_gettupleslot(fcache->tstore, true, false, slot))
				elog(ERROR, "failed to fetch lazy-eval tuple");
			/* Extract the result as a datum, and copy out from the slot */
			result = postquel_get_single_result(slot, fcinfo,
												fcache, oldcontext);
			/* Clear the tuplestore, but keep it for next time */
			/* NB: this might delete the slot's content, but we don't care */
			tuplestore_clear(fcache->tstore);

			/*
			 * Let caller know we're not finished.
			 */
			rsi->isDone = ExprMultipleResult;

			/*
			 * Ensure we will get shut down cleanly if the exprcontext is not
			 * run to completion.
			 */
			if (!fcache->shutdown_reg)
			{
				RegisterExprContextCallback(rsi->econtext,
											ShutdownSQLFunction,
											PointerGetDatum(fcache));
				fcache->shutdown_reg = true;
			}
		}
		else if (fcache->lazyEval)
		{
			/*
			 * We are done with a lazy evaluation.  Clean up.
			 */
			tuplestore_clear(fcache->tstore);

			/*
			 * Let caller know we're finished.
			 */
			rsi->isDone = ExprEndResult;

			fcinfo->isnull = true;
			result = (Datum) 0;

			/* Deregister shutdown callback, if we made one */
			if (fcache->shutdown_reg)
			{
				UnregisterExprContextCallback(rsi->econtext,
											  ShutdownSQLFunction,
											  PointerGetDatum(fcache));
				fcache->shutdown_reg = false;
			}
		}
		else
		{
			/*
			 * We are done with a non-lazy evaluation.  Return whatever is in
			 * the tuplestore.  (It is now caller's responsibility to free the
			 * tuplestore when done.)
			 */
			rsi->returnMode = SFRM_Materialize;
			rsi->setResult = fcache->tstore;
			fcache->tstore = NULL;
			/* must copy desc because execSRF.c will free it */
			if (fcache->junkFilter)
				rsi->setDesc = CreateTupleDescCopy(fcache->junkFilter->jf_cleanTupType);

			fcinfo->isnull = true;
			result = (Datum) 0;

			/* Deregister shutdown callback, if we made one */
			if (fcache->shutdown_reg)
			{
				UnregisterExprContextCallback(rsi->econtext,
											  ShutdownSQLFunction,
											  PointerGetDatum(fcache));
				fcache->shutdown_reg = false;
			}
		}
	}
	else
	{
		/*
		 * Non-set function.  If we got a row, return it; else return NULL.
		 */
		if (fcache->junkFilter)
		{
			/* Re-use the junkfilter's output slot to fetch back the tuple */
			slot = fcache->junkFilter->jf_resultSlot;
			if (tuplestore_gettupleslot(fcache->tstore, true, false, slot))
				result = postquel_get_single_result(slot, fcinfo,
													fcache, oldcontext);
			else
			{
				fcinfo->isnull = true;
				result = (Datum) 0;
			}
		}
		else
		{
			/* Should only get here for VOID functions and procedures */
			Assert(fcache->rettype == VOIDOID);
			fcinfo->isnull = true;
			result = (Datum) 0;
		}

		/* Clear the tuplestore, but keep it for next time */
		tuplestore_clear(fcache->tstore);
	}

	/* Pop snapshot if we have pushed one */
	if (pushed_snapshot)
		PopActiveSnapshot();

	/*
	 * If we've gone through every command in the function, we are done. Reset
	 * the execution states to start over again on next call.
	 */
	if (es == NULL)
	{
		foreach(eslc, fcache->func_state)
		{
			es = (execution_state *) lfirst(eslc);
			while (es)
			{
				es->status = F_EXEC_START;
				es = es->next;
			}
		}
	}

	error_context_stack = sqlerrcontext.previous;

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * error context callback to let us supply a call-stack traceback
 */
static void
sql_exec_error_callback(void *arg)
{
	FmgrInfo   *flinfo = (FmgrInfo *) arg;
	SQLFunctionCachePtr fcache = (SQLFunctionCachePtr) flinfo->fn_extra;
	int			syntaxerrposition;

	/*
	 * We can do nothing useful if init_sql_fcache() didn't get as far as
	 * saving the function name
	 */
	if (fcache == NULL || fcache->fname == NULL)
		return;

	/*
	 * If there is a syntax error position, convert to internal syntax error
	 */
	syntaxerrposition = geterrposition();
	if (syntaxerrposition > 0 && fcache->src != NULL)
	{
		errposition(0);
		internalerrposition(syntaxerrposition);
		internalerrquery(fcache->src);
	}

	/*
	 * Try to determine where in the function we failed.  If there is a query
	 * with non-null QueryDesc, finger it.  (We check this rather than looking
	 * for F_EXEC_RUN state, so that errors during ExecutorStart or
	 * ExecutorEnd are blamed on the appropriate query; see postquel_start and
	 * postquel_end.)
	 */
	if (fcache->func_state)
	{
		execution_state *es;
		int			query_num;
		ListCell   *lc;

		es = NULL;
		query_num = 1;
		foreach(lc, fcache->func_state)
		{
			es = (execution_state *) lfirst(lc);
			while (es)
			{
				if (es->qd)
				{
					errcontext("SQL function \"%s\" statement %d",
							   fcache->fname, query_num);
					break;
				}
				es = es->next;
			}
			if (es)
				break;
			query_num++;
		}
		if (es == NULL)
		{
			/*
			 * couldn't identify a running query; might be function entry,
			 * function exit, or between queries.
			 */
			errcontext("SQL function \"%s\"", fcache->fname);
		}
	}
	else
	{
		/*
		 * Assume we failed during init_sql_fcache().  (It's possible that the
		 * function actually has an empty body, but in that case we may as
		 * well report all errors as being "during startup".)
		 */
		errcontext("SQL function \"%s\" during startup", fcache->fname);
	}
}

void
o_proc_cache_validate_add(Oid datoid, Oid procoid, Oid fncollation,
						  char *func_type, char *used_for)
{
	StringInfoData str;

	initStringInfo(&str);
	appendStringInfo(&str, " should be used as B-tree %s support function "
					 "in operator class used for %s "
					 "of orioledb index", func_type, used_for);
	o_validate_function_by_oid(procoid, str.data);

	o_collect_function_by_oid(procoid, fncollation);
	pfree(str.data);
}

void
o_proc_cache_fill_finfo(FmgrInfo *finfo, Oid procoid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OProc	   *o_proc = NULL;
	const FmgrBuiltin *fbp;

	memset(finfo, 0, sizeof(FmgrInfo));

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_proc = o_proc_cache_search(datoid, procoid, cur_lsn,
								 proc_cache->nkeys);

	Assert(o_proc);

	if ((fbp = fmgr_isbuiltin(procoid)) != NULL)
	{
		/*
		 * Fast path for builtin functions: don't bother consulting pg_proc
		 */
		finfo->fn_stats = TRACK_FUNC_ALL;	/* ie, never track */
		finfo->fn_addr = fbp->func;
	}
	else if (o_proc->prolang == INTERNALlanguageId)
	{
		fbp = fmgr_lookupByName(o_proc->prosrc);
		if (fbp == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("internal function \"%s\" is not in "
							"internal lookup table",
							o_proc->prosrc)));
		finfo->fn_stats = TRACK_FUNC_ALL;	/* ie, never track */
		finfo->fn_addr = fbp->func;
	}
	else if (o_proc->prolang == ClanguageId)
	{
		finfo->fn_stats = TRACK_FUNC_PL;
		finfo->fn_addr = load_external_function(o_proc->probin,
												o_proc->prosrc,
												false,
												NULL);
	}
	else if (o_proc->prolang == SQLlanguageId)
	{
		if (o_is_syscache_hooks_set())
			finfo->fn_addr = o_fmgr_sql;
		else
			finfo->fn_addr = fmgr_sql;
		finfo->fn_stats = TRACK_FUNC_PL;	/* ie, track if ALL */
	}
	else
	{
		/* TODO: Add another language support */
		elog(ERROR, "Function language is not supported");
	}

	finfo->fn_oid = procoid;
	finfo->fn_nargs = o_proc->nargs;
	finfo->fn_strict = o_proc->strict;
	finfo->fn_retset = o_proc->retset;
	finfo->fn_mcxt = CurrentMemoryContext;
	finfo->fn_extra = NULL;
	finfo->fn_expr = NULL;
}

HeapTuple
o_proc_cache_search_htup(TupleDesc tupdesc, Oid procoid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_proc] = {0};
	bool		nulls[Natts_pg_proc] = {0};
	OProc	   *o_proc;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_proc = o_proc_cache_search(datoid, procoid, cur_lsn, proc_cache->nkeys);
	if (o_proc)
	{
		oidvector  *parameterTypes;
		NameData	procname;

		parameterTypes = buildoidvector(o_proc->argtypes, o_proc->nargs);

		namestrcpy(&procname, o_proc->proname);
		values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(o_proc->rettype);
		values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(parameterTypes);
		values[Anum_pg_proc_proname - 1] = NameGetDatum(&procname);
		values[Anum_pg_proc_proowner - 1] = ObjectIdGetDatum(o_proc->proowner);
		values[Anum_pg_proc_prolang - 1] = ObjectIdGetDatum(o_proc->prolang);
		if (o_proc->prosrc)
			values[Anum_pg_proc_prosrc - 1] =
				CStringGetTextDatum(o_proc->prosrc);
		else
			nulls[Anum_pg_proc_prosrc - 1] = true;
		if (o_proc->probin)
			values[Anum_pg_proc_probin - 1] =
				CStringGetTextDatum(o_proc->probin);
		else
			nulls[Anum_pg_proc_probin - 1] = true;

		nulls[Anum_pg_proc_proallargtypes - 1] = true;
		nulls[Anum_pg_proc_proargmodes - 1] = true;
		nulls[Anum_pg_proc_proargnames - 1] = true;
		nulls[Anum_pg_proc_proargdefaults - 1] = true;
		nulls[Anum_pg_proc_protrftypes - 1] = true;
#if PG_VERSION_NUM >= 140000
		nulls[Anum_pg_proc_prosqlbody - 1] = true;
#endif
		nulls[Anum_pg_proc_proconfig - 1] = true;
		nulls[Anum_pg_proc_proacl - 1] = true;
		result = heap_form_tuple(tupdesc, values, nulls);
		ItemPointerSet(&(result->t_self), 0, FirstOffsetNumber);
	}
	return result;
}
