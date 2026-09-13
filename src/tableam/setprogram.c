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
 * A constant operand, whether it was written as a literal or bound as a
 * parameter.  A prepared statement is the normal shape for the workloads
 * worth combining, and by the time ExecutorStart runs its parameters are
 * bound, so the two are the same thing to us.
 */
static bool
resolve_constant(Node *expr, ParamListInfo params,
				 Datum *value, bool *isnull, Oid *type)
{
	if (IsA(expr, Const))
	{
		Const	   *con = (Const *) expr;

		*value = con->constvalue;
		*isnull = con->constisnull;
		*type = con->consttype;
		return true;
	}

	if (IsA(expr, Param))
	{
		Param	   *param = (Param *) expr;
		ParamExternData *prm;

		if (param->paramkind != PARAM_EXTERN || params == NULL ||
			param->paramid <= 0 || param->paramid > params->numParams)
			return false;

		/*
		 * Only parameters already sitting in the list.  A paramFetch hook
		 * could run arbitrary code, and this has to stay a plain read.
		 */
		if (params->paramFetch != NULL)
			return false;

		prm = &params->params[param->paramid - 1];
		if (!OidIsValid(prm->ptype))
			return false;

		*value = prm->value;
		*isnull = prm->isnull;
		*type = prm->ptype;
		return true;
	}

	return false;
}

/*
 * Put a constant into the op, copying it into the program's own store when it
 * is held by reference.  Returns false if it does not fit or is of a kind we
 * cannot copy.
 */
static bool
store_constant(OSetProgram *program, OSetOp *op,
			   Datum value, bool isnull, Oid type)
{
	int16		typlen;
	bool		typbyval;
	Size		size;
	Pointer		src;
	int			offset;

	op->isnull = isnull;
	if (isnull)
	{
		op->byval = true;
		op->value = (Datum) 0;
		return true;
	}

	get_typlenbyval(type, &typlen, &typbyval);

	if (typbyval)
	{
		op->byval = true;
		op->value = value;
		return true;
	}

	if (typlen == -1)
	{
		struct varlena *vl;

		/*
		 * Detoast now, in the process that can: what goes into the store has
		 * to be readable by whoever applies it, with no catalog and no I/O.
		 * Keep the four-byte header rather than the packed one, so that every
		 * reader downstream sees an ordinary varlena.
		 */
		vl = pg_detoast_datum((struct varlena *) DatumGetPointer(value));
		src = (Pointer) vl;
		size = VARSIZE(vl);
	}
	else if (typlen > 0)
	{
		src = DatumGetPointer(value);
		size = (Size) typlen;
	}
	else
		return false;			/* cstring and friends: not worth the care */

	offset = MAXALIGN(program->constUsed);
	if (offset + size > O_SET_PROGRAM_CONST_SPACE)
		return false;

	memcpy(&program->consts[offset], src, size);
	program->constUsed = offset + size;

	op->byval = false;
	op->valueOffset = offset;
	op->valueLen = size;
	return true;
}

/*
 * Compile one target list entry into an assignment, or report that it is
 * beyond us.
 */
static bool
compile_set_op(OSetProgram *program, ParamListInfo params,
			   Expr *expr, AttrNumber attnum, OSetOp *op)
{
	Datum		value;
	bool		isnull;
	Oid			type;

	/*
	 * Start from zeroes.  The program is copied whole into shared memory, and
	 * a field this op does not use should not carry whatever the last
	 * statement left there.
	 */
	memset(op, 0, sizeof(*op));
	op->attnum = attnum;

	if (resolve_constant((Node *) expr, params, &value, &isnull, &type))
	{
		op->kind = OSetOpConst;
		op->opfuncid = InvalidOid;
		return store_constant(program, op, value, isnull, type);
	}

	if (IsA(expr, OpExpr))
	{
		OpExpr	   *opexpr = (OpExpr *) expr;
		Node	   *left,
				   *right;
		Var		   *var;
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
		if (!IsA(left, Var))
			return false;
		if (!resolve_constant(right, params, &value, &isnull, &type))
			return false;

		var = (Var *) left;

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

		/* The amount to add is arithmetic, so it is always by value. */
		if (isnull)
			return false;

		op->kind = OSetOpAddConst;
		op->opfuncid = opexpr->opfuncid;
		op->byval = true;
		op->value = value;
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

	currentProgram.constUsed = 0;

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

		if (!compile_set_op(&currentProgram, queryDesc->params, tle->expr,
							attnum, &currentProgram.ops[nops]))
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
o_set_program_apply(OSetProgram *program,
					Datum *values, bool *isnull, int natts)
{
	int			i;

	for (i = 0; i < program->nops; i++)
	{
		OSetOp	   *op = &program->ops[i];
		int			idx = op->attnum - 1;

		if (idx < 0 || idx >= natts)
			return false;

		switch (op->kind)
		{
			case OSetOpConst:
				isnull[idx] = op->isnull;
				if (op->isnull)
					values[idx] = (Datum) 0;
				else if (op->byval)
					values[idx] = op->value;
				else
					values[idx] = PointerGetDatum(&program->consts[op->valueOffset]);
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
