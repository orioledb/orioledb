/*-------------------------------------------------------------------------
 *
 * planner.c
 * 		Routines for query processing.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/utils/planner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"
#include "utils/planner.h"

#include "access/genam.h"
#include "access/hash.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_language.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "executor/functions.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "parser/analyze.h"
#include "parser/parse_target.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

typedef struct
{
	char	   *proname;
	char	   *prosrc;
} validate_error_callback_arg;

typedef struct
{
	char	   *hint_msg;
	char	   *proname;
} validate_function_arg;

typedef bool (*WalkerFunc) (Node *node, void *context);

static bool validate_function(Node *node, void *context);
static Node *o_wrap_top_funcexpr(Node *node);
static void o_collect_function_walker(Oid functionId, Oid inputcollid,
									  List *args, void *context);
static bool plan_tree_walker(Plan *plan, WalkerFunc, void *context);

#if PG_VERSION_NUM >= 150000
#define pg_analyze_and_rewrite_params pg_analyze_and_rewrite_withcb
#endif

 /*
  * error context callback to let us supply a call-stack traceback
  */
static void
sql_validate_error_callback(void *arg)
{
	validate_error_callback_arg *callback_arg;
	int			syntaxerrposition;

	callback_arg = (validate_error_callback_arg *) arg;

	/* If it's a syntax error, convert to internal syntax error report */
	syntaxerrposition = geterrposition();
	if (syntaxerrposition > 0)
	{
		errposition(0);
		internalerrposition(syntaxerrposition);
		internalerrquery(callback_arg->prosrc);
	}

	errcontext("SQL function \"%s\" during body validation",
			   callback_arg->proname);
}

static void
o_process_sql_function(HeapTuple procedureTuple, WalkerFunc walker,
					   void *context, Oid functionId, Oid inputcollid,
					   List *args)
{
	Form_pg_proc procedureStruct;
	MemoryContext mycxt,
				oldcxt;
	ErrorContextCallback sqlerrcontext;
	validate_error_callback_arg callback_arg;
	Datum		proc_body;
	bool		isNull;
	bool		haspolyarg;
	List	   *querytree_list;
	int			i;

	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	/*
	 * Make a temporary memory context, so that we don't leak all the stuff
	 * that parsing might create.
	 */
	mycxt = AllocSetContextCreate(CurrentMemoryContext,
								  "inline_function",
								  ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(mycxt);

	haspolyarg = false;
	for (i = 0; i < procedureStruct->pronargs; i++)
	{
		if (get_typtype(procedureStruct->proargtypes.values[i]) == TYPTYPE_PSEUDO)
		{
			if (IsPolymorphicType(procedureStruct->proargtypes.values[i]))
				haspolyarg = true;
		}
	}

	/*
	 * Setup error traceback support for ereport(). This is so that we can
	 * finger the function that bad information came from.
	 */
	callback_arg.proname = NameStr(procedureStruct->proname);

	/* Fetch the function body */
	proc_body = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_prosrc,
								&isNull);
	if (isNull)
		elog(ERROR, "null prosrc for function %u", functionId);
	callback_arg.prosrc = TextDatumGetCString(proc_body);

	sqlerrcontext.callback = sql_validate_error_callback;
	sqlerrcontext.arg = (void *) &callback_arg;
	sqlerrcontext.previous = error_context_stack;
	error_context_stack = &sqlerrcontext;

#if PG_VERSION_NUM >= 140000
	/* If we have prosqlbody, pay attention to that not prosrc */
	proc_body = SysCacheGetAttr(PROCOID, procedureTuple,
								Anum_pg_proc_prosqlbody, &isNull);
	if (!isNull)
	{
		ListCell   *lc;
		Node	   *n;
		List	   *stored_query_list;

		n = stringToNode(TextDatumGetCString(proc_body));
		if (IsA(n, List))
			stored_query_list = linitial_node(List, castNode(List, n));
		else
			stored_query_list = list_make1(n);

		querytree_list = NIL;
		foreach(lc, stored_query_list)
		{
			Query	   *parsetree = lfirst_node(Query, lc);
			List	   *querytree_sublist;

			AcquireRewriteLocks(parsetree, true, false);
			querytree_sublist = pg_rewrite_query(parsetree);
			querytree_list = lappend(querytree_list, querytree_sublist);
		}
	}
	else
#endif
	{
		List	   *raw_parsetree_list;

		raw_parsetree_list = pg_parse_query(callback_arg.prosrc);
		querytree_list = NIL;

		if (!haspolyarg)
		{
			ListCell   *lc;
			SQLFunctionParseInfoPtr pinfo;

			pinfo = prepare_sql_fn_parse_info(procedureTuple, NULL,
											  InvalidOid);
			foreach(lc, raw_parsetree_list)
			{
				RawStmt    *parsetree = lfirst_node(RawStmt, lc);
				List	   *querytree_sublist;

				querytree_sublist = pg_analyze_and_rewrite_params(parsetree,
																  callback_arg.prosrc,
																  (ParserSetupHook) sql_fn_parser_setup,
																  pinfo,
																  NULL);
				querytree_list = lappend(querytree_list,
										 querytree_sublist);
			}
		}
	}


	/*
	 * The single command must be a simple "SELECT expression".
	 *
	 * Note: if you change the tests involved in this, see also plpgsql's
	 * exec_simple_check_plan().  That generally needs to have the same idea
	 * of what's a "simple expression", so that inlining a function that
	 * previously wasn't inlined won't change plpgsql's conclusion.
	 */
	if (!haspolyarg)
	{
		Oid			rettype;
		TupleDesc	rettupdesc;
		ListCell   *lc;
		List	   *resulttlist;

		foreach(lc, querytree_list)
		{
			List	   *sublist = lfirst_node(List, lc);
			ListCell   *lc2;

			foreach(lc2, sublist)
			{
				Query	   *query = lfirst_node(Query, lc2);
				Query	   *new_query;
				List	   *colnames;
				RangeTblEntry *rte;
				ListCell   *lc3;

				MemoryContextSwitchTo(oldcxt);
				new_query = makeNode(Query);
				new_query->commandType = CMD_SELECT;
				new_query->canSetTag = true;

				/*
				 * We need a moderately realistic colnames list for the
				 * subquery RTE
				 */
				colnames = NIL;
				foreach(lc3, query->targetList)
				{
					TargetEntry *tle = (TargetEntry *) lfirst(lc3);

					if (tle->resjunk)
						continue;
					colnames = lappend(colnames,
									   makeString(tle->resname ? tle->resname : ""));
				}

				rte = makeNode(RangeTblEntry);
				rte->rtekind = RTE_SUBQUERY;
				rte->subquery = query;
				rte->eref = rte->alias = makeAlias("*SELECT*", colnames);
				rte->lateral = false;
				rte->inh = false;
				rte->inFromCl = true;
				new_query->rtable = list_make1(rte);

				query_tree_walker(new_query, walker, context, 0);
				MemoryContextSwitchTo(mycxt);
			}
		}

		check_sql_fn_statements(querytree_list);

		(void) get_func_result_type(procedureStruct->oid, &rettype,
									&rettupdesc);

		(void) check_sql_fn_retval(querytree_list, rettype, rettupdesc, false,
								   &resulttlist);
	}

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(mycxt);
	error_context_stack = sqlerrcontext.previous;
}

Node *
o_wrap_top_funcexpr(Node *node)
{
	static NamedArgExpr named_arg = {.xpr = {.type = T_NamedArgExpr}};

	named_arg.arg = (Expr *) node;
	return (Node *) &named_arg;
}

/*
 *	o_process_functions_in_node -
 *	  apply checker() to each function OID contained in given expression node
 *
 * Returns true if the checker() function does; for nodes representing more
 * than one function call, returns true if the checker() function does so
 * for any of those functions.  Returns false if node does not invoke any
 * SQL-visible function.  Caller must not pass node == NULL.
 *
 * This function examines only the given node; it does not recurse into any
 * sub-expressions.  Callers typically prefer to keep control of the recursion
 * for themselves, in case additional checks should be made, or because they
 * have special rules about which parts of the tree need to be visited.
 *
 * Note: we ignore MinMaxExpr, SQLValueFunction, XmlExpr, CoerceToDomain,
 * and NextValueExpr nodes, because they do not contain SQL function OIDs.
 * However, they can invoke SQL-visible functions, so callers should take
 * thought about how to treat them.
 */
static void
o_process_functions_in_node(Node *node,
							void (*func_walker) (Oid functionId,
												 Oid inputcollid,
												 List *args,
												 void *context),
							void *context)
{
	Oid			functionId = InvalidOid;
	Oid			inputcollid;
	List	   *args;

	switch (nodeTag(node))
	{
		case T_Aggref:
			{
				Aggref	   *expr = (Aggref *) node;

				functionId = expr->aggfnoid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_WindowFunc:
			{
				WindowFunc *expr = (WindowFunc *) node;

				functionId = expr->winfnoid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *expr = (FuncExpr *) node;

				functionId = expr->funcid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				OpExpr	   *expr = (OpExpr *) node;

				/* Set opfuncid if it wasn't set already */
				set_opfuncid(expr);

				functionId = expr->opfuncid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

				set_sa_opfuncid(expr);
				functionId = expr->opfuncid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_CoerceViaIO:
			{
				CoerceViaIO *expr = (CoerceViaIO *) node;
				Oid			iofunc;
				Oid			typioparam;
				bool		typisvarlena;

				/* check the result type's input function */
				getTypeInputInfo(expr->resulttype,
								 &iofunc, &typioparam);

				functionId = iofunc;
				inputcollid = InvalidOid;
				args = list_make1(expr->arg);

				func_walker(functionId, inputcollid, args, context);

				/* check the input type's output function */
				getTypeOutputInfo(exprType((Node *) expr->arg),
								  &iofunc, &typisvarlena);

				functionId = iofunc;
				inputcollid = InvalidOid;
				args = list_make1(expr->arg);

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;
				ListCell   *opid;
				ListCell   *collid;
				ListCell   *larg;
				ListCell   *rarg;

				forfour(opid, rcexpr->opnos,
						collid, rcexpr->inputcollids,
						larg, rcexpr->largs,
						rarg, rcexpr->rargs)
				{
					functionId = get_opcode(lfirst_oid(opid));
					inputcollid = lfirst_oid(collid);
					args = list_make2(lfirst(larg),
									  lfirst(rarg));

					func_walker(functionId, inputcollid, args, context);
				}
			}
			break;
		default:
			break;
	}
}

static void
validate_function_walker(Oid functionId, Oid inputcollid, List *args,
						 void *context)
{
	HeapTuple	procedureTuple;
	Form_pg_proc procedureStruct;
	validate_function_arg *arg = (validate_function_arg *) context;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
	if (!HeapTupleIsValid(procedureTuple))
		elog(ERROR, "cache lookup failed for function %u", functionId);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	arg->proname = pstrdup(procedureStruct->proname.data);

	if (procedureStruct->prolang > SQLlanguageId)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function \"%s\" cannot be used here",
						arg->proname),
				 errhint("only C and SQL functions%s",
						 arg->hint_msg)));
	if (procedureStruct->provolatile != PROVOLATILE_IMMUTABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function \"%s\" cannot be used here",
						arg->proname),
				 errhint("only immutable functions%s",
						 arg->hint_msg)));

	if (procedureStruct->prolang == SQLlanguageId &&
		procedureStruct->prokind == PROKIND_FUNCTION)
	{
		o_process_sql_function(procedureTuple, validate_function,
							   context, functionId, inputcollid, args);
	}
	ReleaseSysCache(procedureTuple);
}

static bool
validate_function(Node *node, void *context)
{
	validate_function_arg *arg = (validate_function_arg *) context;

	if (node == NULL)
		return false;

	if (IsA(node, NextValueExpr))
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function \"%s\" cannot be used here",
						FigureColname(node)),
				 errhint("only immutable functions%s",
						 arg->hint_msg)));
	}
	o_process_functions_in_node(node, validate_function_walker, context);

	/* Recurse to check arguments */
	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;
		ListCell   *rtable;

		foreach(rtable, query->rtable)
		{
			RangeTblEntry *rte = lfirst(rtable);

			if (rte->rtekind == RTE_RELATION)
			{
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("function \"%s\" cannot be used here",
								arg->proname),
						 errhint("only queries without relation "
								 "references%s", arg->hint_msg)));

			}
		}
		(void) query_tree_walker(query, validate_function, context, 0);
	}
	else
		(void) expression_tree_walker(node, validate_function,
									  (void *) context);
	return false;
}

void
o_validate_funcexpr(Node *node, char *hint_msg)
{
	validate_function_arg arg = {.hint_msg = hint_msg};

	if (!node)
		return;

	expression_tree_walker(o_wrap_top_funcexpr(node),
						   validate_function, &arg);

	if (arg.proname)
		pfree(arg.proname);
}

void
o_validate_function_by_oid(Oid procoid, char *hint_msg)
{
	FuncExpr   *fexpr;
	HeapTuple	procedureTuple;
	Form_pg_proc procedureStruct;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(procoid));
	if (!HeapTupleIsValid(procedureTuple))
		elog(ERROR, "cache lookup failed for function %u", procoid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	fexpr = makeNode(FuncExpr);
	fexpr->funcid = procoid;
	fexpr->funcresulttype = procedureStruct->prorettype;
	fexpr->funcretset = procedureStruct->proretset;
	fexpr->funcvariadic = procedureStruct->provariadic;
	fexpr->funcformat = COERCE_EXPLICIT_CALL;	/* doesn't matter */
	fexpr->funccollid = InvalidOid; /* doesn't matter */
	fexpr->inputcollid = InvalidOid;
	fexpr->args = NIL;
	fexpr->location = -1;

	o_validate_funcexpr((Node *) fexpr, hint_msg);

	ReleaseSysCache(procedureTuple);
}

static inline bool
is_a_plan(Node *node)
{
#if PG_VERSION_NUM >= 160000
	return (nodeTag(node) >= T_Result) && (nodeTag(node) <= T_Limit);
#else
	return (nodeTag(node) >= T_Plan) && (nodeTag(node) <= T_Limit);
#endif
}

static bool
o_collect_function(Node *node, void *context)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OClassArg	arg = {.sys_table = true};

	if (node == NULL)
		return false;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_class_cache_add_if_needed(datoid, TypeRelationId, cur_lsn,
								(Pointer) &arg);
	switch (nodeTag(node))
	{
		case T_Aggref:
		case T_WindowFunc:
		case T_FuncExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_ScalarArrayOpExpr:
		case T_CoerceViaIO:
		case T_RowCompareExpr:
		case T_FunctionScan:
			o_class_cache_add_if_needed(datoid, ProcedureRelationId,
										cur_lsn, (Pointer) &arg);
			break;
		default:
			break;
	}

	o_process_functions_in_node(node, o_collect_function_walker, context);

	switch (nodeTag(node))
	{
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
			{
				OpExpr	   *opexpr = (OpExpr *) node;

				o_class_cache_add_if_needed(datoid, OperatorRelationId, cur_lsn,
											(Pointer) &arg);
				o_operator_cache_add_if_needed(datoid, opexpr->opno,
											   cur_lsn, NULL);
				o_type_cache_add_if_needed(datoid, opexpr->opresulttype,
										   cur_lsn, NULL);
			}
			break;
		case T_Aggref:
			{
				Aggref	   *aggref = (Aggref *) node;
				ListCell   *lc;

				o_class_cache_add_if_needed(datoid, AggregateRelationId, cur_lsn,
											(Pointer) &arg);
				o_class_cache_add_if_needed(datoid, OperatorRelationId, cur_lsn,
											(Pointer) &arg);
				o_aggregate_cache_add_if_needed(datoid, aggref->aggfnoid,
												cur_lsn, NULL);
				o_type_cache_add_if_needed(datoid, aggref->aggtype,
										   cur_lsn, NULL);

				foreach(lc, aggref->aggargtypes)
				{
					o_type_cache_add_if_needed(datoid, lfirst_oid(lc),
											   cur_lsn, NULL);
				}
			}
			break;
		case T_Agg:
			{
				Agg		   *agg = (Agg *) node;
				int			i;

				for (i = 0; i < agg->numCols; i++)
				{
					Oid			eq_opr = agg->grpOperators[i];
					CatCList   *catlist;
					int			j;

					o_class_cache_add_if_needed(datoid, OperatorRelationId,
												cur_lsn, (Pointer) &arg);
					o_operator_cache_add_if_needed(datoid, eq_opr, cur_lsn, NULL);

					/*
					 * Search pg_amop to see if the target operator is
					 * registered as the "=" operator of any hash opfamily. If
					 * the operator is registered in multiple opfamilies,
					 * assume we can use any one.
					 */
					catlist = SearchSysCacheList1(AMOPOPID,
												  ObjectIdGetDatum(eq_opr));

					for (j = 0; j < catlist->n_members; j++)
					{
						HeapTuple	tuple = &catlist->members[j]->tuple;
						Form_pg_amop aform = (Form_pg_amop) GETSTRUCT(tuple);

						o_class_cache_add_if_needed(datoid,
													AccessMethodOperatorRelationId,
													cur_lsn, (Pointer) &arg);
						o_amop_cache_add_if_needed(datoid, aform->amopopr,
												   aform->amoppurpose,
												   aform->amopfamily, cur_lsn,
												   NULL);

						if (aform->amopmethod == HASH_AM_OID &&
							aform->amopstrategy == HTEqualStrategyNumber)
						{
							Oid			result;

							/*
							 * Get the matching support function(s).  Failure
							 * probably shouldn't happen --- it implies a
							 * bogus opfamily --- but continue looking if so.
							 */
							result = get_opfamily_proc(aform->amopfamily,
													   aform->amoplefttype,
													   aform->amoplefttype,
													   HASHSTANDARD_PROC);
							if (!OidIsValid(result))
								continue;

							o_class_cache_add_if_needed(datoid,
														AccessMethodProcedureRelationId,
														cur_lsn, (Pointer) &arg);
							o_amproc_cache_add_if_needed(datoid, aform->amopfamily,
														 aform->amoplefttype,
														 aform->amoplefttype,
														 HASHSTANDARD_PROC,
														 cur_lsn, NULL);

							/*
							 * Only one lookup needed if given operator is
							 * single-type
							 */
							if (aform->amoplefttype == aform->amoprighttype)
								break;
							result = get_opfamily_proc(aform->amopfamily,
													   aform->amoprighttype,
													   aform->amoprighttype,
													   HASHSTANDARD_PROC);
							if (!OidIsValid(result))
								continue;
							o_amproc_cache_add_if_needed(datoid, aform->amopfamily,
														 aform->amoprighttype,
														 aform->amoprighttype,
														 HASHSTANDARD_PROC,
														 cur_lsn, NULL);
							break;
						}
					}

					ReleaseSysCacheList(catlist);
				}
			}
			break;
		case T_WindowFunc:
			{
				WindowFunc *window_func = (WindowFunc *) node;

				o_class_cache_add_if_needed(datoid, AggregateRelationId, cur_lsn,
											(Pointer) &arg);
				o_class_cache_add_if_needed(datoid, OperatorRelationId, cur_lsn,
											(Pointer) &arg);
				o_aggregate_cache_add_if_needed(datoid, window_func->winfnoid,
												cur_lsn, NULL);
				o_type_cache_add_if_needed(datoid, window_func->wintype,
										   cur_lsn, NULL);
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *func_expr = (FuncExpr *) node;

				o_type_cache_add_if_needed(datoid, func_expr->funcresulttype,
										   cur_lsn, NULL);
			}
			break;
		case T_MinMaxExpr:
			{
				MinMaxExpr *minmaxexpr = (MinMaxExpr *) node;

				o_type_cache_add_if_needed(datoid, minmaxexpr->minmaxtype,
										   cur_lsn, NULL);
			}
			break;
		case T_CoerceViaIO:
			{
				CoerceViaIO *iocoerce = (CoerceViaIO *) node;

				o_type_cache_add_if_needed(datoid,
										   exprType((Node *) iocoerce->arg),
										   cur_lsn, NULL);
				o_type_cache_add_if_needed(datoid, iocoerce->resulttype,
										   cur_lsn, NULL);
			}
			break;
		case T_RowExpr:
			{
				RowExpr    *row_expr = (RowExpr *) node;

				o_type_cache_add_if_needed(datoid, row_expr->row_typeid,
										   cur_lsn, NULL);
			}
			break;
		default:
			break;
	}

	/* Recurse to check arguments */
	if (IsA(node, Query))
		(void) query_tree_walker((Query *) node, o_collect_function,
								 context, 0);
	else if (is_a_plan(node))
		(void) plan_tree_walker((Plan *) node, o_collect_function, context);
	else
		(void) expression_tree_walker(node, o_collect_function, context);
	return false;
}

void
o_collect_funcexpr(Node *node)
{
	if (!node)
		return;

	expression_tree_walker(o_wrap_top_funcexpr(node), o_collect_function,
						   NULL);
}

void
o_collect_function_walker(Oid functionId, Oid inputcollid, List *args,
						  void *context)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OClassArg	arg = {.sys_table = true};
	HeapTuple	procedureTuple;
	Form_pg_proc procedureStruct;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);
	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_proc_cache_add_if_needed(datoid, functionId, cur_lsn,
							   (Pointer) &inputcollid);
	o_class_cache_add_if_needed(datoid, AuthIdRelationId, cur_lsn,
								(Pointer) &arg);

	if (procedureStruct->prolang == SQLlanguageId &&
		procedureStruct->prokind == PROKIND_FUNCTION)
	{
		o_process_sql_function(procedureTuple, o_collect_function,
							   context, functionId, inputcollid, args);
	}
	ReleaseSysCache(procedureTuple);
}

void
o_collect_function_by_oid(Oid procoid, Oid inputcollid)
{
	FuncExpr   *fexpr;
	HeapTuple	procedureTuple;
	Form_pg_proc procedureStruct;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(procoid));
	if (!HeapTupleIsValid(procedureTuple))
		elog(ERROR, "cache lookup failed for function %u", procoid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	fexpr = makeNode(FuncExpr);
	fexpr->funcid = procoid;
	fexpr->funcresulttype = procedureStruct->prorettype;
	fexpr->funcretset = procedureStruct->proretset;
	fexpr->funcvariadic = procedureStruct->provariadic;
	fexpr->funcformat = COERCE_EXPLICIT_CALL;	/* doesn't matter */
	fexpr->funccollid = InvalidOid; /* doesn't matter */
	fexpr->inputcollid = inputcollid;
	fexpr->args = NIL;
	fexpr->location = -1;

	expression_tree_walker(o_wrap_top_funcexpr((Node *) fexpr),
						   o_collect_function, NULL);

	ReleaseSysCache(procedureTuple);
}

static bool
plan_tree_walker(Plan *plan, WalkerFunc walker, void *context)
{
	ListCell   *lc;

	if (plan == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex plan trees */
	check_stack_depth();

	if (expression_tree_walker((Node *) plan->targetlist,
							   walker, context))
		return true;
	if (expression_tree_walker((Node *) plan->qual,
							   walker, context))
		return true;

	/* lefttree */
	if (outerPlan(plan))
	{
		if (walker((Node *) outerPlan(plan), context))
			return true;
	}

	/* righttree */
	if (innerPlan(plan))
	{
		if (walker((Node *) innerPlan(plan), context))
			return true;
	}

	switch (nodeTag(plan))
	{
			/*
			 * control nodes
			 */
		case T_Result:
			{
				Result	   *result = (Result *) plan;

				if (expression_tree_walker((Node *) result->resconstantqual,
										   walker, context))
					return true;
			}
			break;

		case T_ModifyTable:
			{
				ModifyTable *modify_table = (ModifyTable *) plan;

				if (expression_tree_walker((Node *) modify_table->withCheckOptionLists,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) modify_table->returningLists,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) modify_table->onConflictSet,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) modify_table->onConflictWhere,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) modify_table->exclRelTlist,
										   walker, context))
					return true;
			}
			break;

		case T_Append:
			{
				Append	   *append = (Append *) plan;

				foreach(lc, append->appendplans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc),
										 walker, context))
						return true;
				}
			}
			break;

		case T_MergeAppend:
			{
				MergeAppend *merge_append = (MergeAppend *) plan;

				foreach(lc, merge_append->mergeplans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc),
										 walker, context))
						return true;
				}
			}
			break;

		case T_BitmapAnd:
			{
				BitmapAnd  *bitmap_and = (BitmapAnd *) plan;

				foreach(lc, bitmap_and->bitmapplans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc),
										 walker, context))
						return true;
				}
			}
			break;

		case T_BitmapOr:
			{
				BitmapOr   *bitmap_or = (BitmapOr *) plan;

				foreach(lc, bitmap_or->bitmapplans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc),
										 walker, context))
						return true;
				}
			}
			break;

		case T_SampleScan:
			{
				SampleScan *sample_scan = (SampleScan *) plan;

				if (expression_tree_walker((Node *) sample_scan->tablesample,
										   walker, context))
					return true;
			}
			break;

		case T_IndexScan:
			{
				IndexScan  *index_scan = (IndexScan *) plan;

				if (expression_tree_walker((Node *) index_scan->indexqual,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_scan->indexqualorig,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_scan->indexorderby,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_scan->indexorderbyorig,
										   walker, context))
					return true;
			}
			break;

		case T_IndexOnlyScan:
			{
				IndexOnlyScan *index_only_scan = (IndexOnlyScan *) plan;

				if (expression_tree_walker((Node *) index_only_scan->recheckqual,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_only_scan->indexqual,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_only_scan->indexorderby,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_only_scan->indextlist,
										   walker, context))
					return true;
			}
			break;

		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bitmap_index_scan = (BitmapIndexScan *) plan;

				if (expression_tree_walker((Node *) bitmap_index_scan->indexqual,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) bitmap_index_scan->indexqualorig,
										   walker, context))
					return true;
			}
			break;

		case T_BitmapHeapScan:
			{
				BitmapHeapScan *bitmap_heap_scan = (BitmapHeapScan *) plan;

				if (expression_tree_walker((Node *) bitmap_heap_scan->bitmapqualorig,
										   walker, context))
					return true;
			}
			break;

		case T_TidScan:
			{
				TidScan    *tid_scan = (TidScan *) plan;

				if (expression_tree_walker((Node *) tid_scan->tidquals,
										   walker, context))
					return true;
			}
			break;

#if PG_VERSION_NUM >= 140000
		case T_TidRangeScan:
			{
				TidRangeScan *tid_range_scan = (TidRangeScan *) plan;

				if (expression_tree_walker((Node *) tid_range_scan->tidrangequals,
										   walker, context))
					return true;
			}
			break;
#endif

		case T_SubqueryScan:
			{
				SubqueryScan *subquery_scan = (SubqueryScan *) plan;

				if (plan_tree_walker((Plan *) subquery_scan->subplan,
									 walker, context))
					return true;
			}
			break;

		case T_FunctionScan:
			{
				FunctionScan *function_scan = (FunctionScan *) plan;

				if (expression_tree_walker((Node *) function_scan->functions,
										   walker, context))
					return true;
			}
			break;

		case T_TableFuncScan:
			{
				TableFuncScan *table_func_scan = (TableFuncScan *) plan;

				if (expression_tree_walker((Node *) table_func_scan->tablefunc,
										   walker, context))
					return true;
			}
			break;

		case T_ValuesScan:
			{
				ValuesScan *values_scan = (ValuesScan *) plan;

				if (expression_tree_walker((Node *) values_scan->values_lists,
										   walker, context))
					return true;
			}
			break;

		case T_ForeignScan:
			{
				ForeignScan *foreign_scan = (ForeignScan *) plan;

				if (expression_tree_walker((Node *) foreign_scan->fdw_exprs,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) foreign_scan->fdw_recheck_quals,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) foreign_scan->fdw_scan_tlist,
										   walker, context))
					return true;
			}
			break;

		case T_CustomScan:
			{
				CustomScan *custom_scan = (CustomScan *) plan;

				if (expression_tree_walker((Node *) custom_scan->custom_scan_tlist,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) custom_scan->custom_exprs,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) custom_scan->custom_scan_tlist,
										   walker, context))
					return true;

				foreach(lc, custom_scan->custom_plans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc), walker, context))
						return true;
				}
			}
			break;

		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			{
				Join	   *join = (Join *) plan;

				if (expression_tree_walker((Node *) join->joinqual,
										   walker, context))
					return true;

				if (IsA(join, NestLoop))
				{
					NestLoop   *nl = (NestLoop *) join;

					foreach(lc, nl->nestParams)
					{
						NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);

						if (expression_tree_walker((Node *) nlp->paramval,
												   walker, context))
							return true;
					}
				}
				else if (IsA(join, MergeJoin))
				{
					MergeJoin  *mj = (MergeJoin *) join;

					if (expression_tree_walker((Node *) mj->mergeclauses,
											   walker, context))
						return true;
				}
				else if (IsA(join, HashJoin))
				{
					HashJoin   *hj = (HashJoin *) join;

					if (expression_tree_walker((Node *) hj->hashclauses,
											   walker, context))
						return true;
					if (expression_tree_walker((Node *) hj->hashkeys,
											   walker, context))
						return true;
				}
			}
			break;

#if PG_VERSION_NUM >= 140000
		case T_Memoize:
			{
				Memoize    *memoize = (Memoize *) plan;

				if (expression_tree_walker((Node *) memoize->param_exprs,
										   walker, context))
					return true;
			}
			break;
#endif

		case T_WindowAgg:
			{
				WindowAgg  *window_agg = (WindowAgg *) plan;

				if (expression_tree_walker((Node *) window_agg->startOffset,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) window_agg->endOffset,
										   walker, context))
					return true;
			}
			break;

		case T_Hash:
			{
				Hash	   *hash = (Hash *) plan;

				if (expression_tree_walker((Node *) hash->hashkeys,
										   walker, context))
					return true;
			}
			break;

		case T_Limit:
			{
				Limit	   *limit = (Limit *) plan;

				if (expression_tree_walker((Node *) limit->limitOffset,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) limit->limitCount,
										   walker, context))
					return true;
			}
			break;

		case T_Agg:
		case T_CteScan:
		case T_Gather:
		case T_GatherMerge:
		case T_Group:
		case T_IncrementalSort:
		case T_LockRows:
		case T_Material:
		case T_NamedTuplestoreScan:
		case T_ProjectSet:
		case T_RecursiveUnion:
		case T_SeqScan:
		case T_SetOp:
		case T_Sort:
		case T_Unique:
		case T_WorkTableScan:
			break;

		default:
			elog(ERROR, "%s: unrecognized node type: %d", PG_FUNCNAME_MACRO,
				 (int) nodeTag(plan));
			break;
	}

	foreach(lc, plan->initPlan)
	{
		if (walker((Node *) plan->initPlan, context))
			return true;
	}

	return false;
}

static bool
plannedstatement_tree_walker(PlannedStmt *pstmt,
							 WalkerFunc walker,
							 void *context)
{
	Plan	   *plan = pstmt->planTree;
	ListCell   *lc;
	ProjectSet *project_set;

	/* Guard against stack overflow due to overly complex plan trees */
	check_stack_depth();

	project_set = makeNode(ProjectSet);
	project_set->plan.lefttree = plan;

	plan_tree_walker((Plan *) project_set, walker, context);

	/* subPlan-s */
	foreach(lc, pstmt->subplans)
	{
		Plan	   *sp = (Plan *) lfirst(lc);

		if (sp && plan_tree_walker(sp, walker, context))
			return true;
	}

	return false;
}

void
o_collect_functions_pstmt(PlannedStmt *pstmt)
{
	plannedstatement_tree_walker(pstmt, o_collect_function, NULL);
}
