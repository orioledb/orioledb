/*-------------------------------------------------------------------------
 *
 * scan.h
 *		Scan Provider for orioledb tables.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/scan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_SCAN_H__
#define __TABLEAM_SCAN_H__

#include "postgres.h"

#include "nodes/extensible.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"

typedef enum OPlanTag
{
	O_IndexPlan,
	O_BitmapHeapPlan,
} OPlanTag;

typedef struct OPlanState
{
	OPlanTag	type;
	PlanState  *plan_state;
} OPlanState;

typedef struct OCustomScanState
{
	CustomScanState css;
	OEACallsCounters eaCounters;
	OPlanState *o_plan_state;
} OCustomScanState;

extern set_rel_pathlist_hook_type old_set_rel_pathlist_hook;

extern void orioledb_set_rel_pathlist_hook(PlannerInfo *root, RelOptInfo *rel,
										   Index rti, RangeTblEntry *rte);
extern bool orioledb_set_plain_rel_pathlist_hook(PlannerInfo *root,
												 RelOptInfo *rel,
												 RangeTblEntry *rte);

extern bool is_o_custom_scan(CustomScan *scan);
extern bool is_o_custom_scan_state(CustomScanState *scan);

#endif							/* __TABLEAM_SCAN_H__ */
