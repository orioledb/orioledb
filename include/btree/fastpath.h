/*-------------------------------------------------------------------------
 *
 * fastpath.h
 *		Declarations for fastpath intra-page navigation in B-tree.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
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
#define FASTPATH_FIND_DOWNLINK_FLAG_MINUS_INF (1)
#define FASTPATH_FIND_DOWNLINK_FLAG_PLUS_INF (2)

typedef void (*ArraySearchFunc) (Pointer p, int stride,
								 int *lower, int *upper, Datum keyDatum);

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
