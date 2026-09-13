/*-------------------------------------------------------------------------
 *
 * setprogram.c
 *		Recognizing a SET clause another process can apply.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/setprogram.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "tableam/setprogram.h"

#include "common/int.h"
#include "executor/executor.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

static ExecutorStart_hook_type next_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type next_ExecutorEnd_hook = NULL;

/*
 * The one program in flight.  A statement that qualifies is by construction
 * the only thing its transaction runs, so there is never a second one to
 * keep; a nested executor (a function body, say) simply finds the slot taken
 * and leaves it alone, which costs it nothing but delegation.
 */
static OSetProgram currentProgram;
static Oid	currentProgramRelid = InvalidOid;
static int	currentProgramDepth = 0;
static int	executorDepth = 0;

OSetProgram *
o_current_set_program(Oid relid)
{
	if (!OidIsValid(currentProgramRelid) || currentProgramRelid != relid)
		return NULL;
	return &currentProgram;
}

/*
 * Is this the addition of a constant we can carry out ourselves, without
 * fmgr, without allocating and without any way to raise an error?  Only those
 * qualify: the holder applies a program with a page locked, where a longjmp
 * would leave the page behind.
 */
/*
 * The arithmetic we are able to carry out ourselves: no fmgr, no allocation,
 * and no way to raise an error.  Only those qualify, because the holder
 * applies a program with a page locked, where a longjmp would leave the page
 * behind.
 *
 * Reports the width of each operand.  The pairs listed are the ones whose
 * result is as wide as their left operand, which is what lets the outcome go
 * back into the column it came from without a cast the compiler would then
 * have to recognize.
 */
static bool
supported_add_op(Oid opfuncid, int *leftWidth, int *rightWidth, bool *subtract)
{
	switch (opfuncid)
	{
		case F_INT2PL:
			*leftWidth = 2, *rightWidth = 2, *subtract = false;
			return true;
		case F_INT2MI:
			*leftWidth = 2, *rightWidth = 2, *subtract = true;
			return true;
		case F_INT4PL:
			*leftWidth = 4, *rightWidth = 4, *subtract = false;
			return true;
		case F_INT4MI:
			*leftWidth = 4, *rightWidth = 4, *subtract = true;
			return true;
		case F_INT42PL:
			*leftWidth = 4, *rightWidth = 2, *subtract = false;
			return true;
		case F_INT42MI:
			*leftWidth = 4, *rightWidth = 2, *subtract = true;
			return true;
		case F_INT8PL:
			*leftWidth = 8, *rightWidth = 8, *subtract = false;
			return true;
		case F_INT8MI:
			*leftWidth = 8, *rightWidth = 8, *subtract = true;
			return true;
		case F_INT82PL:
			*leftWidth = 8, *rightWidth = 2, *subtract = false;
			return true;
		case F_INT82MI:
			*leftWidth = 8, *rightWidth = 2, *subtract = true;
			return true;
		case F_INT84PL:
			*leftWidth = 8, *rightWidth = 4, *subtract = false;
			return true;
		case F_INT84MI:
			*leftWidth = 8, *rightWidth = 4, *subtract = true;
			return true;
		default:
			return false;
	}
}

/*
 * Compile one target list entry into an assignment, or report that it is
 * beyond us.
 */
static bool
compile_set_op(Expr *expr, AttrNumber attnum, OSetOp *op)
{
	op->attnum = attnum;

	if (IsA(expr, Const))
	{
		Const	   *con = (Const *) expr;

		/*
		 * Only by-value constants travel: the program is copied into shared
		 * memory verbatim, so a Datum that is really a pointer would mean
		 * nothing in the process that applies it.
		 */
		if (!con->constbyval)
			return false;

		op->kind = OSetOpConst;
		op->opfuncid = InvalidOid;
		op->value = con->constvalue;
		op->isnull = con->constisnull;
		return true;
	}

	if (IsA(expr, OpExpr))
	{
		OpExpr	   *opexpr = (OpExpr *) expr;
		Node	   *left,
				   *right;
		Var		   *var;
		Const	   *con;
		int			leftWidth,
					rightWidth;
		bool		subtract;
		AttrNumber	varattno;

		if (list_length(opexpr->args) != 2 ||
			!supported_add_op(opexpr->opfuncid, &leftWidth, &rightWidth,
							  &subtract))
			return false;

		left = (Node *) linitial(opexpr->args);
		right = (Node *) lsecond(opexpr->args);

		/*
		 * Left to right only.  Subtraction is not commutative, and for
		 * addition the reversed form is rare enough not to be worth a second
		 * shape to get wrong.
		 */
		if (!IsA(left, Var) || !IsA(right, Const))
			return false;

		var = (Var *) left;
		con = (Const *) right;

		/*
		 * The column must read itself.  Referring to another column would
		 * make the result depend on where in the batch this assignment runs,
		 * and the whole point is that it does not.
		 *
		 * By this stage set_plan_references() has rewritten the Var to point
		 * at whatever the scan node produces -- for an index scan, a column of
		 * the index tuple -- so the relation's own attribute number is the one
		 * kept aside in varattnosyn.
		 */
		varattno = (var->varno == INDEX_VAR || var->varno == OUTER_VAR ||
					var->varno == INNER_VAR) ? var->varattnosyn : var->varattno;
		if (varattno != attnum)
			return false;

		if (!con->constbyval || con->constisnull)
			return false;

		op->kind = OSetOpAddConst;
		op->opfuncid = opexpr->opfuncid;
		op->value = con->constvalue;
		op->isnull = false;
		return true;
	}

	return false;
}

/*
 * Compile the statement's SET clause if it is one we can hand over.
 *
 * The expressions are taken from the subplan's target list paired with the
 * update column numbers -- the same two things ExecBuildUpdateProjection()
 * consumes -- so what we compile is what EvalPlanQual() would re-evaluate.
 */
static void
compile_current_program(QueryDesc *queryDesc)
{
	PlannedStmt *stmt = queryDesc->plannedstmt;
	ModifyTable *node;
	Plan	   *subplan;
	List	   *updateColnos;
	ListCell   *ltl,
			   *lcol;
	RangeTblEntry *rte;
	Index		rti;
	int			nops = 0;

	if (queryDesc->operation != CMD_UPDATE)
		return;
	if (!IsA(stmt->planTree, ModifyTable))
		return;

	node = (ModifyTable *) stmt->planTree;

	/*
	 * One relation, plainly updated.  Anything that makes the statement mean
	 * more than "assign these columns" -- routing, ON CONFLICT, RETURNING, a
	 * WITH CHECK OPTION -- is somebody else's business to run.
	 */
	if (list_length(node->resultRelations) != 1 ||
		list_length(node->updateColnosLists) != 1 ||
		node->onConflictAction != ONCONFLICT_NONE ||
		node->returningLists != NIL ||
		node->withCheckOptionLists != NIL ||
		node->rootRelation != 0)
		return;

	subplan = outerPlan(node);
	if (subplan == NULL)
		return;

	updateColnos = (List *) linitial(node->updateColnosLists);
	if (list_length(updateColnos) > O_SET_PROGRAM_MAX_OPS)
		return;

	forboth(ltl, subplan->targetlist, lcol, updateColnos)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, ltl);
		AttrNumber	attnum = lfirst_int(lcol);

		if (tle->resjunk)
			return;				/* the lists would be out of step */

		if (!compile_set_op(tle->expr, attnum, &currentProgram.ops[nops]))
			return;
		nops++;
	}

	if (nops == 0)
		return;

	rti = linitial_int(node->resultRelations);
	rte = list_nth_node(RangeTblEntry, stmt->rtable, rti - 1);
	currentProgram.nops = nops;
	currentProgramRelid = rte->relid;
	currentProgramDepth = executorDepth;
}

static void
o_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	executorDepth++;

	if (next_ExecutorStart_hook)
		next_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/* The outermost statement gets the slot; a nested one does without. */
	if (!OidIsValid(currentProgramRelid) && !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		compile_current_program(queryDesc);
}

static void
o_ExecutorEnd(QueryDesc *queryDesc)
{
	if (OidIsValid(currentProgramRelid) && currentProgramDepth == executorDepth)
	{
		currentProgramRelid = InvalidOid;
		currentProgram.nops = 0;
	}
	executorDepth--;

	if (next_ExecutorEnd_hook)
		next_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

void
o_setprogram_init(void)
{
	next_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = o_ExecutorStart;
	next_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = o_ExecutorEnd;
}

/*
 * Add a constant to a value, in the widths the operator says the two sides
 * are.  Returns false on overflow, which is the caller's cue to leave the
 * update alone rather than to raise: we may be holding a page.
 */
static bool
add_const(Oid opfuncid, Datum cur, Datum add, Datum *result)
{
	int			leftWidth,
				rightWidth;
	bool		subtract;
	int64		a,
				b,
				out;

	if (!supported_add_op(opfuncid, &leftWidth, &rightWidth, &subtract))
		return false;

	a = (leftWidth == 2) ? (int64) DatumGetInt16(cur) :
		(leftWidth == 4) ? (int64) DatumGetInt32(cur) : DatumGetInt64(cur);
	b = (rightWidth == 2) ? (int64) DatumGetInt16(add) :
		(rightWidth == 4) ? (int64) DatumGetInt32(add) : DatumGetInt64(add);

	if (subtract ? pg_sub_s64_overflow(a, b, &out)
		: pg_add_s64_overflow(a, b, &out))
		return false;

	/* The result goes back into the left operand's column, so it must fit. */
	switch (leftWidth)
	{
		case 2:
			if (out < PG_INT16_MIN || out > PG_INT16_MAX)
				return false;
			*result = Int16GetDatum((int16) out);
			return true;
		case 4:
			if (out < PG_INT32_MIN || out > PG_INT32_MAX)
				return false;
			*result = Int32GetDatum((int32) out);
			return true;
		default:
			*result = Int64GetDatum(out);
			return true;
	}
}

bool
o_set_program_apply(const OSetProgram *program,
					Datum *values, bool *isnull, int natts)
{
	int			i;

	for (i = 0; i < program->nops; i++)
	{
		const OSetOp *op = &program->ops[i];
		int			idx = op->attnum - 1;

		if (idx < 0 || idx >= natts)
			return false;

		switch (op->kind)
		{
			case OSetOpConst:
				values[idx] = op->value;
				isnull[idx] = op->isnull;
				break;

			case OSetOpAddConst:
				if (isnull[idx])
					break;		/* NULL + anything is still NULL */
				if (!add_const(op->opfuncid, values[idx], op->value,
							   &values[idx]))
					return false;
				break;
		}
	}

	return true;
}
