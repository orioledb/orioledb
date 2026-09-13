/*-------------------------------------------------------------------------
 *
 * setprogram.h
 *		A SET clause in a form another process can apply.
 *
 * The page holder can only finish an update for a waiting backend if it can
 * work out what the new row should be.  Under contention the row it finds is
 * rarely the one the waiter read, so the waiter's precomputed tuple is no use:
 * what has to travel is the assignment itself.
 *
 * Only a narrow shape of UPDATE qualifies, described by OSetOp below.  The
 * expressions come from the same place ExecBuildUpdateProjection() takes
 * them -- the subplan's target list paired with the update column numbers --
 * so what is compiled here is exactly what EvalPlanQual() would have
 * re-evaluated, minus everything we cannot promise to reproduce.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/setprogram.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_SETPROGRAM_H__
#define __TABLEAM_SETPROGRAM_H__

#include "postgres.h"

#include "access/attnum.h"
#include "executor/execdesc.h"
#include "nodes/execnodes.h"

/*
 * The assignments we are able to carry.  Anything else means the statement is
 * not delegatable, which costs nothing but the usual path.
 */
typedef enum
{
	OSetOpConst,				/* col := <const> */
	OSetOpAddConst				/* col := col + <const> */
} OSetOpKind;

typedef struct
{
	AttrNumber	attnum;			/* 1-based attribute of the relation */
	OSetOpKind	kind;
	Oid			opfuncid;		/* the addition, for OSetOpAddConst */
	Datum		value;			/* by value only: this crosses shared memory */
	bool		isnull;
} OSetOp;

/*
 * Eight covers the shapes worth combining and keeps the whole program inside
 * a waiter's shared state without a length prefix to get wrong.
 */
#define O_SET_PROGRAM_MAX_OPS	8

typedef struct
{
	int			nops;			/* 0 means "nothing here" */
	OSetOp		ops[O_SET_PROGRAM_MAX_OPS];
} OSetProgram;

/*
 * The program compiled for the statement now running, if it has one.  Valid
 * only between ExecutorStart and ExecutorEnd, and only ever describes a
 * single result relation.
 */
extern OSetProgram *o_current_set_program(Oid relid);

extern void o_setprogram_init(void);

/*
 * Apply a program to a tuple's values.  Returns false when an assignment
 * cannot be carried out on these inputs, which the caller must treat as "do
 * it the ordinary way" rather than as an error.
 */
extern bool o_set_program_apply(const OSetProgram *program,
								Datum *values, bool *isnull, int natts);

#endif							/* __TABLEAM_SETPROGRAM_H__ */
