/*-------------------------------------------------------------------------
 *
 * fastpath.h
 *		Declarations for fastpath intra-page navigation in B-tree.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/fastpath.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_FASTPATH_H__
#define __BTREE_FASTPATH_H__

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/page_contents.h"

#define FASTPATH_FIND_DOWNLINK_MAX_KEYS (4)

/*
 * Per-key flags directing the array narrowing.  Unlike a raw value, these name
 * a *storage position* (the on-page slot order), so they are correct for both
 * ASC and DESC key columns: FIRST collapses the search range to its lowest
 * storage slot, LAST to its highest.  find_downlink_get_keys() resolves value
 * infinities, unbounded columns, and NULLs (via the column's ASC/DESC and
 * NULLS FIRST/LAST ordering) into these storage directions.
 */
#define FASTPATH_FIND_DOWNLINK_FLAG_FIRST (1)
#define FASTPATH_FIND_DOWNLINK_FLAG_LAST (2)

/*
 * Array-search routine over a fixed-stride column.  "ascending" mirrors
 * OIndexField.ascending: false selects the DESC-ordered variant (values stored
 * high-to-low) so the same routines serve both index orderings.
 */
typedef void (*ArraySearchFunc) (Pointer p, int stride,
								 int *lower, int *upper, Datum keyDatum,
								 bool ascending);

typedef struct
{
	bool		enabled;
	bool		inclusive;
	int			numKeys;
	int			length;

	Datum		offsets[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	ArraySearchFunc funcs[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	Datum		values[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	uint8		flags[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	bool		ascending[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
} FastpathFindDownlinkMeta;

typedef enum
{
	OBTreeFastPathFindOK,
	OBTreeFastPathFindRetry,
	OBTreeFastPathFindFailure,
	OBTreeFastPathFindSlowpath
} OBTreeFastPathFindResult;

extern void can_fastpath_find_downlink(OBTreeFindPageContext *context,
									   void *key,
									   BTreeKeyType keyType,
									   FastpathFindDownlinkMeta *meta);
extern OBTreeFastPathFindResult fastpath_find_chunk(Pointer pagePtr,
													OInMemoryBlkno blkno,
													FastpathFindDownlinkMeta *meta,
													int *chunkIndex);
extern OBTreeFastPathFindResult fastpath_find_downlink(Pointer pagePtr,
													   OInMemoryBlkno blkno,
													   FastpathFindDownlinkMeta *meta,
													   BTreePageItemLocator *loc,
													   BTreeNonLeafTuphdr **tuphdrPtr);

#endif							/* __BTREE_FASTPATH_H__ */
