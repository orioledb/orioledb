/*-------------------------------------------------------------------------
 *
 * ucm.c
 *		OrioleDB usage count map (UCM) implementation.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/utils/ucm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "utils/ucm.h"

#define UCM_BRANCH_FACTOR	15
#define UCM_LEVEL_BITS		4
#define UCM_LEVEL_MASK		0xF

static bool skip_ucm = false;

static int	init_ucm_non_leaf_recursive(UsageCountMap *map, int i);
static void ucm_inc_recursive(UsageCountMap *map, int i, int prev, int next);
static bool ucm_check_recursive(UsageCountMap *map, int i);
static inline void ucm_inc(UsageCountMap *map, OInMemoryBlkno blkno, int prev, int next);

/*
 * Estimate shaed memory space for UCM data structure.
 */
Size
estimate_ucm_space(UsageCountMap *map, OInMemoryBlkno offset, OInMemoryBlkno size)
{
	int			n_leaf_groups;
	int			n_leaf_vars;
	int			n_non_leaf_vars;
	int			n;

	map->offset = offset;
	map->size = size;
	n_leaf_groups = (map->size + UCM_BRANCH_FACTOR - 1) / UCM_BRANCH_FACTOR;
	n_leaf_vars = n_leaf_groups;

	n_non_leaf_vars = 0;
	n = n_leaf_vars;
	map->rootFactor = UCM_BRANCH_FACTOR;
	while (n > UCM_BRANCH_FACTOR)
	{
		n_non_leaf_vars += 1;
		n_non_leaf_vars *= UCM_BRANCH_FACTOR;
		n += UCM_BRANCH_FACTOR - 1;
		n /= UCM_BRANCH_FACTOR;
		map->rootFactor *= UCM_BRANCH_FACTOR;
	}

	map->total = n_non_leaf_vars + n_leaf_vars;
	map->nonLeaf = n_non_leaf_vars;
	return PG_CACHE_LINE_SIZE + sizeof(pg_atomic_uint32) * map->total;
}

static int
get_value_frame(uint32 value)
{
	int			i;
	uint32		mask = UCM_LEVEL_MASK,
				one = 1,
				result = 0;

	for (i = 0; i < UCM_LEVELS; i++)
	{
		if (value & mask)
			result += one;

		one <<= UCM_LEVEL_BITS;
		mask <<= UCM_LEVEL_BITS;
	}

	return result;
}

static int
init_ucm_non_leaf_recursive(UsageCountMap *map, int i)
{
	if (i < map->nonLeaf)
	{
		int			j;
		uint32		value;

		value = 0;
		for (j = (i + 1) * UCM_BRANCH_FACTOR; j < (i + 2) * UCM_BRANCH_FACTOR; j++)
		{
			value += get_value_frame(init_ucm_non_leaf_recursive(map, j));
		}
		pg_atomic_init_u32(&map->ucm[i], value);
		return value;
	}
	else if (i < map->total)
	{
		return pg_atomic_read_u32(&map->ucm[i]);
	}
	else
	{
		return 0;
	}
}

/*
 * Initialize UCM shared memory.
 */
void
init_ucm(UsageCountMap *map, Pointer ptr, bool found)
{
	int			i;
	OInMemoryBlkno blkno;

	map->epoch = (pg_atomic_uint32 *) ptr;
	ptr += PG_CACHE_LINE_SIZE;

	map->ucm = (pg_atomic_uint32 *) ptr;

	if (found)
		return;

	pg_atomic_init_u32(map->epoch, 0);

	/* Init leaf variables */
	blkno = 0;
	for (i = map->nonLeaf; i < map->total; i++)
	{
		uint32		pagesCount = Min(map->size - blkno, UCM_BRANCH_FACTOR);

		pg_atomic_init_u32(&map->ucm[i],
						   pagesCount << (UCM_FREE_PAGES_LEVEL * UCM_LEVEL_BITS));
		blkno += UCM_BRANCH_FACTOR;
	}

	/* Recursively inin non-leaf variables */
	for (i = 0; i < UCM_BRANCH_FACTOR; i++)
		init_ucm_non_leaf_recursive(map, i);
}

/*
 * Worker function, which recursively increments value of ucm map.
 */
static void
ucm_inc_recursive(UsageCountMap *map, int i, int32 prev, int32 next)
{
	uint32		val,
				new_val,
				prev_mask,
				next_mask,
				prev_one,
				next_one;

	Assert(prev < UCM_LEVELS || prev == InvalidUsageCount);
	Assert(next < UCM_LEVELS || next == InvalidUsageCount);

	if (prev != InvalidUsageCount)
	{
		prev_mask = UCM_LEVEL_MASK << (prev * UCM_LEVEL_BITS);
		prev_one = 1 << (prev * UCM_LEVEL_BITS);
	}
	else
	{
		prev_mask = 0;
		prev_one = 0;
	}

	if (next != InvalidUsageCount)
	{
		next_mask = UCM_LEVEL_MASK << (next * UCM_LEVEL_BITS),
			next_one = 1 << (next * UCM_LEVEL_BITS);
	}
	else
	{
		next_mask = 0;
		next_one = 0;
	}

	val = pg_atomic_read_u32(&map->ucm[i]);
	while (true)
	{
		if ((val & prev_mask) < prev_one || (val & next_mask) > (next_mask - next_one))
		{
			SpinDelayStatus delayStatus;

			init_local_spin_delay(&delayStatus);

			while ((val & prev_mask) < prev_one || (val & next_mask) > (next_mask - next_one))
			{
				perform_spin_delay(&delayStatus);
				val = pg_atomic_read_u32(&map->ucm[i]);
			}
			finish_spin_delay(&delayStatus);
		}

		new_val = val - prev_one + next_one;

		if (pg_atomic_compare_exchange_u32(&map->ucm[i], &val, new_val))
			break;
	}

	if (i >= UCM_BRANCH_FACTOR)
		ucm_inc_recursive(map, (i / UCM_BRANCH_FACTOR) - 1,
						  ((new_val & prev_mask) == 0) ? prev : InvalidUsageCount,
						  ((val & next_mask) == 0) ? next : InvalidUsageCount);
}

static inline void
ucm_inc(UsageCountMap *map, OInMemoryBlkno blkno, int prev, int next)
{
	ucm_inc_recursive(map, map->nonLeaf + blkno / UCM_BRANCH_FACTOR, prev, next);
}

void
page_inc_usage_count(UsageCountMap *map, OInMemoryBlkno blkno,
					 uint32 usageCount, bool no_skip)
{
	uint32		epoch = pg_atomic_read_u32(map->epoch),
				mask;

	if (usageCount == InvalidUsageCount ||
		usageCount == UCM_FREE_PAGES_LEVEL ||
		(!no_skip && skip_ucm))
		return;

	Assert(usageCount < UCM_USAGE_LEVELS);

	map->usageCounter++;

	mask = (1 << ((UCM_USAGE_LEVELS + usageCount - epoch) % UCM_USAGE_LEVELS)) - 1;

	if ((map->usageCounter & mask) == 0 && (usageCount + 1) % UCM_USAGE_LEVELS != epoch)
	{
		Page		p = O_GET_IN_MEMORY_PAGE(blkno);

		if (pg_atomic_compare_exchange_u32(&(O_PAGE_HEADER(p)->usageCount),
										   &usageCount,
										   (usageCount + 1) % UCM_USAGE_LEVELS))
		{
			ucm_inc(map, blkno - map->offset, usageCount, (usageCount + 1) % UCM_USAGE_LEVELS);
		}
	}
}

void
page_change_usage_count(UsageCountMap *map, OInMemoryBlkno blkno, uint32 usageCount)
{
	uint32		prev_usagecount;
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);

	prev_usagecount = pg_atomic_exchange_u32(&(O_PAGE_HEADER(p)->usageCount),
											 usageCount);
	ucm_inc(map, blkno - map->offset, prev_usagecount, usageCount);
}

static bool
page_try_change_usage_count(UsageCountMap *map, OInMemoryBlkno blkno,
							uint32 old_usagecount, uint32 new_usagecount)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);

	if (pg_atomic_compare_exchange_u32(&(O_PAGE_HEADER(p)->usageCount),
									   &old_usagecount,
									   new_usagecount))
	{
		ucm_inc(map, blkno - map->offset, old_usagecount, new_usagecount);
		return true;
	}
	else
	{
		return false;
	}
}

static bool
ucm_check_recursive(UsageCountMap *map, int i)
{
	if (i < map->nonLeaf)
	{
		/* Non-leaf */
		int			j,
					j_max;
		uint32		expected = 0,
					value;
		bool		result = true;

		value = pg_atomic_read_u32(&map->ucm[i]);
		j_max = Min((i + 2) * UCM_BRANCH_FACTOR, map->total);
		for (j = (i + 1) * UCM_BRANCH_FACTOR; j < j_max; j++)
		{
			result = result && ucm_check_recursive(map, j);
			expected += get_value_frame(pg_atomic_read_u32(&map->ucm[j]));
		}

		if (value != expected)
		{
			elog(NOTICE, "wrong value of internal ucm[%d]: expected %x, have %x",
				 i, expected, value);
			result = false;
		}
		return result;
	}
	else if (i < map->total)
	{
		int			group_num = i - map->nonLeaf,
					blkno,
					blkno_max;
		bool		result = true;
		uint32		expected = 0,
					value,
					usageCount;

		value = pg_atomic_read_u32(&map->ucm[i]);
		blkno_max = Min((group_num + 1) * UCM_BRANCH_FACTOR, map->size);
		for (blkno = group_num * UCM_BRANCH_FACTOR; blkno < blkno_max; blkno++)
		{
			Page		p = O_GET_IN_MEMORY_PAGE(blkno + map->offset);

			usageCount = pg_atomic_read_u32(&(O_PAGE_HEADER(p)->usageCount));

			if (usageCount < UCM_LEVELS)
			{
				expected += (1 << (UCM_LEVEL_BITS * usageCount));
			}
			else if (usageCount != InvalidUsageCount)
			{
				elog(NOTICE, "wrong value of ucm[%d]: expected %x, have %x",
					 i, expected, value);
				result = false;
			}
		}

		if (value != expected)
		{
			elog(NOTICE, "wrong value of leaf ucm[%d]: expected %x, have %x",
				 i, expected, value);
			result = false;
		}

		return result;
	}
	else
	{
		return 0;
	}
}

bool
ucm_check_map(UsageCountMap *map)
{
	bool		result = true;
	int			i;

	for (i = 0; i < UCM_BRANCH_FACTOR; i++)
		result = result && ucm_check_recursive(map, i);

	return result;
}

bool
ucm_epoch_needs_shift(UsageCountMap *map)
{
	uint32		mask,
				epoch;
	int			i;

	epoch = pg_atomic_read_u32(map->epoch);
	mask = 0xFFFFFFFF;
	for (i = UCM_USAGE_LEVELS - 2; i < UCM_USAGE_LEVELS; i++)
	{
		int			shift = ((i + epoch) % UCM_USAGE_LEVELS) * UCM_LEVEL_BITS;

		mask &= ~(UCM_LEVEL_MASK << shift);
	}

	for (i = 0; i < UCM_BRANCH_FACTOR; i++)
	{
		if (pg_atomic_read_u32(&map->ucm[i]) & mask)
			return false;
	}
	return true;
}

void
ucm_epoch_shift(UsageCountMap *map)
{
	uint32		epoch,
				next_epoch;

	epoch = pg_atomic_read_u32(map->epoch);
	if (epoch == UCM_USAGE_LEVELS - 1)
		next_epoch = 0;
	else
		next_epoch = epoch + 1;
	pg_atomic_compare_exchange_u32(map->epoch, &epoch, next_epoch);
}

OInMemoryBlkno
ucm_next_blkno(UsageCountMap *map, OInMemoryBlkno init_blkno,
			   uint32 mask_src)
{
	int64		location;
	int64		i;
	int64		factor,
				base;
	int64		num_iterations;
	uint32		mask;
	uint32		epoch;

	epoch = pg_atomic_read_u32(map->epoch);

retry:

	mask = 0;
	for (i = 0; i < UCM_USAGE_LEVELS; i++)
	{
		if (mask_src & (1 << i))
		{
			int			shift = ((i + epoch) % UCM_USAGE_LEVELS) * UCM_LEVEL_BITS;

			mask |= UCM_LEVEL_MASK << shift;
		}
	}

	location = init_blkno - map->offset;
	factor = map->rootFactor;
	base = 0;
	num_iterations = 0;
	while (true)
	{
		i = base + (location / factor) % UCM_BRANCH_FACTOR;

		if (factor == 1 && location < map->size)
		{
			/* Work with pages themselves */
			OrioleDBPageHeader *header = (OrioleDBPageHeader *) O_GET_IN_MEMORY_PAGE(location + map->offset);
			uint32		usageCount;

			usageCount = pg_atomic_read_u32(&header->usageCount);
			if (usageCount < UCM_LEVELS)
			{
				int			j = (UCM_LEVELS + usageCount - epoch) % UCM_LEVELS;

				if (mask_src & (1 << j))
				{
					page_inc_usage_count(map, location + map->offset, usageCount, true);
					return location + map->offset;
				}
			}
		}

		if (i < map->total && (pg_atomic_read_u32(&map->ucm[i]) & mask))
		{
			/* Required usage counts should be here, so step into */
			base = (i + 1) * UCM_BRANCH_FACTOR;
			factor /= UCM_BRANCH_FACTOR;
			num_iterations = 0;
		}
		else
		{
			/* Not found, so step over */
			int64		j;

			if (num_iterations > 2 * UCM_BRANCH_FACTOR)
			{
				/*
				 * Made two rounds and didn't found required usage counts.  So
				 * give up and retry at upper level.
				 */
				if (base == 0)
				{
					uint32		next_epoch;

					if (epoch == UCM_USAGE_LEVELS - 1)
						next_epoch = 0;
					else
						next_epoch = epoch + 1;

					pg_atomic_compare_exchange_u32(map->epoch,
												   &epoch,
												   next_epoch);
					goto retry;
				}
				factor *= UCM_BRANCH_FACTOR;
				i = (i / UCM_BRANCH_FACTOR) - 1;
				base = (i / UCM_BRANCH_FACTOR) * UCM_BRANCH_FACTOR;
				num_iterations = 0;
			}

			j = (location / factor) % UCM_BRANCH_FACTOR;
			location = (location / factor) * factor;
			location += ((j + 1) % UCM_BRANCH_FACTOR - j) * factor;
			num_iterations++;
		}
	}
}

OInMemoryBlkno
ucm_occupy_free_page(UsageCountMap *map)
{
	int64		location;
	int64		i;
	int64		factor,
				base;
	int64		num_iterations;
	uint32		mask;

	mask = UCM_LEVEL_MASK << (UCM_FREE_PAGES_LEVEL * UCM_LEVEL_BITS);
	location = 0;
	factor = map->rootFactor;
	base = 0;
	num_iterations = 0;
	while (true)
	{
		Assert(factor > 0);

		i = base + (location / factor) % UCM_BRANCH_FACTOR;

		if (factor == 1 && location < map->size)
		{
			/* Work with pages themselves */
			OInMemoryBlkno blkno = location + map->offset;
			OrioleDBPageHeader *header = (OrioleDBPageHeader *) O_GET_IN_MEMORY_PAGE(blkno);
			uint32		usageCount;

			usageCount = pg_atomic_read_u32(&header->usageCount);
			if (usageCount == UCM_FREE_PAGES_LEVEL &&
				page_try_change_usage_count(map, blkno,
											UCM_FREE_PAGES_LEVEL, InvalidUsageCount))
			{
				return blkno;
			}
		}

		if (i < map->total && (pg_atomic_read_u32(&map->ucm[i]) & mask))
		{
			/* Required usage counts should be here, so step into */
			base = (i + 1) * UCM_BRANCH_FACTOR;
			factor /= UCM_BRANCH_FACTOR;
			num_iterations = 0;
		}
		else
		{
			/* Not found, so step over */
			int64		j;

			if (num_iterations > 2 * UCM_BRANCH_FACTOR && base != 0)
			{
				/*
				 * Made two rounds and didn't found required usage counts.  So
				 * give up and retry at upper level.
				 */
				factor *= UCM_BRANCH_FACTOR;
				i = (i / UCM_BRANCH_FACTOR) - 1;
				base = (i / UCM_BRANCH_FACTOR) * UCM_BRANCH_FACTOR;
				num_iterations = 0;
			}

			j = (location / factor) % UCM_BRANCH_FACTOR;
			location = (location / factor) * factor;
			location += ((j + 1) % UCM_BRANCH_FACTOR - j) * factor;
			num_iterations++;
		}
	}
}

void
set_skip_ucm(void)
{
	skip_ucm = true;
}

void
unset_skip_ucm(void)
{
	skip_ucm = false;
}
