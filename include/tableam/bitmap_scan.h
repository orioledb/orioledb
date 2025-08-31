/*-------------------------------------------------------------------------
 *
 * bitmap_scan.h
 *		Declarations for bitmap scan of OrioleDB table.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/bitmap_scan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_BITMAP_SCAN_H__
#define __TABLEAM_BITMAP_SCAN_H__

#include "btree/scan.h"
#include "tableam/handler.h"
#include "tableam/scan.h"

#include "lib/rbtree.h"

typedef struct OBitmapScan OBitmapScan;

typedef struct OBitmapHeapPlanState
{
	OPlanState	o_plan_state;
	Plan	   *bitmapqualplan;
	PlanState  *bitmapqualplanstate;
	/* index quals, in standard expr form */
	List	   *bitmapqualorig;
	Oid			typeoid;
	OSnapshot	oSnapshot;
	MemoryContext cxt;
	OBitmapScan *scan;
	OEACallsCounters *eaCounters;
} OBitmapHeapPlanState;

extern OBitmapScan *o_make_bitmap_scan(OBitmapHeapPlanState *bitmap_state,
									   ScanState *ss,
									   PlanState *bitmapqualplanstate,
									   Relation rel, Oid typeoid,
									   OSnapshot *oSnapshot, MemoryContext cxt);
extern TupleTableSlot *o_exec_bitmap_fetch(OBitmapScan *scan,
										   CustomScanState *node);
extern void o_free_bitmap_scan(OBitmapScan *scan);

extern RBTree *o_keybitmap_create(void);
extern void o_keybitmap_insert(RBTree *rbtree, uint64 value);
extern void o_keybitmap_intersect(RBTree *a, RBTree *b);
extern void o_keybitmap_union(RBTree *a, RBTree *b);
extern void o_keybitmap_free(RBTree *tree);
extern bool o_keybitmap_is_empty(RBTree *rbtree);
extern bool o_keybitmap_test(RBTree *rbtree, uint64 value);
extern bool o_keybitmap_range_is_valid(RBTree *rbtree, uint64 low, uint64 high);
extern uint64 o_keybitmap_get_next(RBTree *rbtree, uint64 prev, bool *found);

#endif							/* __TABLEAM_BITMAP_SCAN_H__ */
