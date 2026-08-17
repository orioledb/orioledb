/*-------------------------------------------------------------------------
 *
 * bitmap_scan.h
 *		Declarations for bitmap scan of OrioleDB table.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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

#include "executor/executor.h"
#include "storage/condition_variable.h"
#include "storage/spin.h"
#include "utils/dsa.h"

/*
 * Opaque set of uint64 keys backing the orioledb bitmap scan.  Implemented in
 * key_bitmap.c on top of the adaptive radix tree.
 */
typedef struct OKeyBitmap OKeyBitmap;

typedef struct OBitmapScan OBitmapScan;

/*
 * Shared (DSM) state coordinating a parallel bitmap heap scan.  The bitmap is
 * built once by the first worker to arrive and serialized into es_query_dsa;
 * the others wait on `cv` and then attach the same buffer read-only.  The
 * primary tree is then fetched cooperatively through `poscan`.
 */
typedef enum OBitmapParallelStage
{
	OBITMAP_PARALLEL_NEW = 0,	/* nobody has started building yet */
	OBITMAP_PARALLEL_BUILDING,	/* one worker is building the bitmap */
	OBITMAP_PARALLEL_READY,		/* bitmap serialized (or empty); fetch may run */
} OBitmapParallelStage;

typedef struct ParallelOBitmapScanDesc
{
	ParallelOScanDescData poscan;	/* cooperative primary fetch scan */
	slock_t		mutex;			/* protects the fields below */
	ConditionVariable cv;		/* waited on until stage == READY */
	OBitmapParallelStage stage;
	bool		empty;			/* built bitmap has no keys (skip fetch) */
	bool		is_bridge;		/* built a bridge TIDBitmap, not a key bitmap */
	dsa_pointer bitmap_dsa;		/* serialized key bitmap in es_query_dsa */
	Size		bitmap_size;
	dsa_pointer tbm_iter;		/* bridge: shared TIDBitmap iterator */
} ParallelOBitmapScanDesc;

typedef ParallelOBitmapScanDesc *ParallelOBitmapScan;

typedef struct OBitmapHeapPlanState
{
	OPlanState	o_plan_state;
	Plan	   *bitmapqualplan;
	PlanState  *bitmapqualplanstate;
	/* index quals, in standard expr form */
	List	   *bitmapqualorig;
	/* initialized ExprState for bitmapqualorig, reused across tuples */
	ExprState  *bitmapqualorig_state;
	Oid			typeoid;
	OSnapshot	oSnapshot;
	MemoryContext cxt;
	OBitmapScan *scan;
	OEACallsCounters *eaCounters;
	/* parallel scan: DSM coordination state + the query DSA (NULL if serial) */
	ParallelOBitmapScan pbitmap;
	dsa_area   *dsa;
} OBitmapHeapPlanState;

extern OBitmapScan *o_make_bitmap_scan(OBitmapHeapPlanState *bitmap_state,
									   ScanState *ss,
									   PlanState *bitmapqualplanstate,
									   Relation rel, Oid typeoid,
									   OSnapshot *oSnapshot, MemoryContext cxt);
extern TupleTableSlot *o_exec_bitmap_fetch(OBitmapScan *scan,
										   CustomScanState *node);
extern void o_free_bitmap_scan(OBitmapScan *scan);

/*
 * Maximum length, in bytes, of an order-preserving encoded composite primary
 * key handled by the fixed-key bitmap.  Keys shorter than this are encoded
 * right-aligned with a zero high-pad; wider primary keys are not eligible for
 * a bitmap scan.
 */
#define OKBM_FIXED_BYTES 24

extern OKeyBitmap *o_keybitmap_create(void);
extern void o_keybitmap_insert(OKeyBitmap *bm, uint64 value);
extern void o_keybitmap_intersect(OKeyBitmap *a, OKeyBitmap *b);
extern void o_keybitmap_union(OKeyBitmap *a, OKeyBitmap *b);
extern void o_keybitmap_free(OKeyBitmap *bm);
extern bool o_keybitmap_is_empty(OKeyBitmap *bm);
extern bool o_keybitmap_test(OKeyBitmap *bm, uint64 value);
extern bool o_keybitmap_emit(OKeyBitmap *bm, uint64 value);
extern bool o_keybitmap_range_is_valid(OKeyBitmap *bm, uint64 low, uint64 high);
extern uint64 o_keybitmap_get_next(OKeyBitmap *bm, uint64 prev, bool *found);

/*
 * Which bitmap encoding a table's primary key is eligible for.  NONE means the
 * primary key can't back a bitmap scan (the planner then won't offer one).
 */
typedef enum OKeyBitmapMode
{
	O_KEYBITMAP_NONE,
	O_KEYBITMAP_UINT64,			/* single int4/int8/ctid field, densified */
	O_KEYBITMAP_FIXED			/* composite of small ints, order-preserving */
} OKeyBitmapMode;

/*
 * Classify a table's primary key.  On O_KEYBITMAP_FIXED, *fixedKeyLen (if not
 * NULL) receives the meaningful encoded length in bytes.  Defined in
 * bitmap_scan.c; used both by the planner (get_relation_info_hook) and the
 * executor so the two always agree.
 */
extern OKeyBitmapMode o_keybitmap_pk_mode(OIndexDescr *primary,
										  int *fixedKeyLen);

/* Fixed-key (composite primary key) variant. */
extern OKeyBitmap *o_keybitmap_create_fixed(void);
extern void o_keybitmap_insert_key(OKeyBitmap *bm, const uint8 *key);
extern bool o_keybitmap_test_key(OKeyBitmap *bm, const uint8 *key);
extern bool o_keybitmap_emit_key(OKeyBitmap *bm, const uint8 *key);
extern bool o_keybitmap_range_is_valid_key(OKeyBitmap *bm, const uint8 *low,
										   const uint8 *high);
extern bool o_keybitmap_get_next_key(OKeyBitmap *bm, const uint8 *prev,
									 uint8 *result);

/*
 * Serialize a finalized bitmap into a flat, pointerless buffer (for sharing
 * across parallel workers), and attach a read-only wrapper over such a buffer.
 */
extern Size o_keybitmap_serialized_size(OKeyBitmap *bm);
extern void o_keybitmap_serialize(OKeyBitmap *bm, void *buf);
extern OKeyBitmap *o_keybitmap_attach(void *buf, MemoryContext cxt);

#endif							/* __TABLEAM_BITMAP_SCAN_H__ */
