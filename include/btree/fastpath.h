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

/*
 * What of a fast-path search depends on the index alone, built on first use
 * by can_fastpath_find_downlink().  Filling a descriptor zeroes it, which
 * leaves it unbuilt.
 */
typedef struct
{
	bool		built;
	/* leading key columns the array searches serve */
	int			natts;
	/* of those, the leading ones that are also the leaf tuple's */
	int			leafNatts;
	/* the first float column (raw order misplaces NaN), or natts */
	int			firstInexact;
	int			length;
	int			leafStride;
	Datum		offsets[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	ArraySearchFunc funcs[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	bool		ascending[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	Oid			types[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
} FastpathIndexMeta;

typedef struct FastpathFindDownlinkMeta
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

	/*
	 * The same search applies to a leaf chunk of fixed-format items, which
	 * lie at a fixed stride with the key columns leading: see
	 * fastpath_search_leaf_items().
	 */
	/* Raw values of the key types order exactly as their opclasses do */
	bool		rawOrderExact;

	bool		leafSearch;
	int			leafStride;

	/*
	 * Per key column with a value: where an exclusive bound lies against the
	 * run of items equal to that value -- after it (1), before it (-1), or,
	 * for an inclusive bound, not decided there (0).
	 */
	int8		leafExclusive[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
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
extern bool fastpath_search_leaf_items(FastpathFindDownlinkMeta *meta,
									   BTreePageItemLocator *loc);
#define FASTPATH_PREFETCH_MAX 16

/* A leaf's chunk of a key as the prefetch found it: index -1 if it did not */
typedef struct FastpathLeafChunk
{
	int			index;
	uint64		state;
} FastpathLeafChunk;

extern bool fastpath_rebind_meta(const FastpathFindDownlinkMeta *base,
								 const void *baseKey, const void *key,
								 FastpathFindDownlinkMeta *meta);
extern bool fastpath_prepare_prefetch(BTreeDescr *desc, void *key,
									  BTreeKeyType keyType,
									  FastpathFindDownlinkMeta *meta);
extern void fastpath_prefetch_leaves(BTreeDescr *desc, int n,
									 FastpathFindDownlinkMeta *metas,
									 BTreeLocationHint *hints,
									 FastpathLeafChunk *chunks);
extern OBTreeFastPathFindResult fastpath_load_leaf_item(FastpathFindDownlinkMeta *meta,
														PartialPageState *partial,
														Page img,
														OffsetNumber chunkOffset,
														BTreePageItemLocator *loc);
extern bool fastpath_fetch_leaf_tuple(BTreeDescr *desc,
									  FastpathFindDownlinkMeta *meta,
									  BTreeLocationHint *hint,
									  FastpathLeafChunk *known,
									  OSnapshot *snapshot,
									  MemoryContext mcxt, OTuple *result,
									  CommitSeqNo *tupleCsn);
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
