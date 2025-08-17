/*-------------------------------------------------------------------------
 *
 * ucm.h
 *		Declarations of OrioleDB usage count map (USM).
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/ucm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __UCM_H__
#define __UCM_H__

#include "btree/page_state.h"

#define UCM_INVALID_LEVEL (0xF)
#define UCM_USAGE_LEVELS	(0x7)
#define UCM_FREE_PAGES_LEVEL (0x7)
#define UCM_LEVELS			(0x8)

typedef struct UsageCountMap
{
	pg_atomic_uint32 *epoch;
	pg_atomic_uint32 *ucm;
	OInMemoryBlkno offset;
	OInMemoryBlkno size;
	int			total;
	int			nonLeaf;
	int			rootFactor;
	uint32		usageCounter;
} UsageCountMap;

extern Size estimate_ucm_space(UsageCountMap *map, OInMemoryBlkno offset, OInMemoryBlkno size);
extern void init_ucm(UsageCountMap *map, Pointer ptr, bool found);
extern void ucm_inc(UsageCountMap *map, OInMemoryBlkno blkno, int prev, int next);
extern void page_inc_usage_count(UsageCountMap *map, OInMemoryBlkno blkno);
extern void page_change_usage_count(UsageCountMap *map, OInMemoryBlkno blkno, uint32 usageCount);
extern bool ucm_check_map(UsageCountMap *map);
extern bool ucm_epoch_needs_shift(UsageCountMap *map);
extern void ucm_epoch_shift(UsageCountMap *map);
extern OInMemoryBlkno ucm_next_blkno(UsageCountMap *map, OInMemoryBlkno init_blkno, uint32 mask_src);
extern OInMemoryBlkno ucm_occupy_free_page(UsageCountMap *map);
extern void set_skip_ucm(void);
extern void unset_skip_ucm(void);

static inline uint64
ucm_update_state(UsageCountMap *map, OInMemoryBlkno blkno, uint64 state)
{
	uint32		epoch = pg_atomic_read_u32(map->epoch),
				mask;
	uint32		usageCount = O_PAGE_STATE_GET_USAGE_COUNT(state);

	if (usageCount == UCM_INVALID_LEVEL ||
		usageCount == UCM_FREE_PAGES_LEVEL)
		return state;

	Assert(usageCount < UCM_USAGE_LEVELS);

	map->usageCounter++;

	mask = (1 << ((UCM_USAGE_LEVELS + usageCount - epoch) % UCM_USAGE_LEVELS)) - 1;

	if ((map->usageCounter & mask) == 0 && (usageCount + 1) % UCM_USAGE_LEVELS != epoch)
		return O_PAGE_STATE_SET_USAGE_COUNT(state, (usageCount + 1) % UCM_USAGE_LEVELS);
	else
		return state;
}

static inline void
ucm_after_update_state(UsageCountMap *map, OInMemoryBlkno blkno,
					   uint64 oldState, uint64 newState)
{
	uint32		oldUsageCount = O_PAGE_STATE_GET_USAGE_COUNT(oldState);
	uint32		newUsageCount = O_PAGE_STATE_GET_USAGE_COUNT(newState);

	if (oldUsageCount != newUsageCount)
		ucm_inc(map, blkno - map->offset, oldUsageCount, newUsageCount);
}

#endif							/* __UCM_H__ */
