/*-------------------------------------------------------------------------
 *
 * planner.h
 *		Routines for query processing.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PLANNER_H__
#define __PLANNER_H__

extern void o_validate_funcexpr(Node *node, char *hint_msg);
extern void o_validate_function_by_oid(Oid procoid, char *hint_msg);

extern void o_collect_funcexpr(Node *node);
extern void o_collect_function_by_oid(Oid procoid, Oid inputcollid);
extern void o_collect_op_by_oid(Oid opoid);

extern void o_collect_functions_pstmt(PlannedStmt *pstmt);

#endif
