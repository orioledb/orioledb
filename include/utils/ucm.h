/*-------------------------------------------------------------------------
 *
 * ucm.h
 *		Declarations of OrioleDB usage count map (USM).
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/ucm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __UCM_H__
#define __UCM_H__

#define InvalidUsageCount 0xFF
#define UCM_USAGE_LEVELS	7
#define UCM_FREE_PAGES_LEVEL 7
#define UCM_LEVELS			8

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
extern void page_inc_usage_count(UsageCountMap *map, OInMemoryBlkno blkno,
								 uint32 usageCount, bool no_skip);
extern void page_change_usage_count(UsageCountMap *map, OInMemoryBlkno blkno, uint32 usageCount);
extern bool ucm_check_map(UsageCountMap *map);
extern bool ucm_epoch_needs_shift(UsageCountMap *map);
extern void ucm_epoch_shift(UsageCountMap *map);
extern OInMemoryBlkno ucm_next_blkno(UsageCountMap *map, OInMemoryBlkno init_blkno, uint32 mask_src);
extern OInMemoryBlkno ucm_occupy_free_page(UsageCountMap *map);
extern void set_skip_ucm(void);
extern void unset_skip_ucm(void);

#endif							/* __UCM_H__ */
