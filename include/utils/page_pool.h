/*-------------------------------------------------------------------------
 *
 * page_pool.h
 *		Declarations for OrioleDB logical page pool implementation.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/page_pool.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PAGE_POOL_H__
#define __PAGE_POOL_H__

#if PG_VERSION_NUM >= 150000
#include "common/pg_prng.h"
#endif

#include "utils/ucm.h"

/*
 * ucm may not work correctly with lesser page pool size
 */
#define PPOOL_MIN_SIZE			(1024)
#define PPOOL_MIN_SIZE_BLCKS	(PPOOL_MIN_SIZE * ORIOLEDB_BLCKSZ / BLCKSZ)
#define PPOOL_RESERVE_META 0
#define PPOOL_RESERVE_INSERT	1
#define PPOOL_RESERVE_FIND	2
#define PPOOL_RESERVE_SHARED_INFO_INSERT	3
#define PPOOL_RESERVE_COUNT	4

#define PPOOL_KIND_GET_MASK(kind) (1 << (kind))

#define PPOOL_RESERVE_META_MASK PPOOL_KIND_GET_MASK(PPOOL_RESERVE_META)
#define PPOOL_RESERVE_INSERT_MASK PPOOL_KIND_GET_MASK(PPOOL_RESERVE_INSERT)
#define PPOOL_RESERVE_FIND_MASK PPOOL_KIND_GET_MASK(PPOOL_RESERVE_FIND)
#define PPOOL_RESERVE_SHARED_INFO_INSERT_MASK PPOOL_KIND_GET_MASK(PPOOL_RESERVE_SHARED_INFO_INSERT)
#define PPOOL_RESERVE_MASK_ALL (PPOOL_RESERVE_META_MASK | PPOOL_RESERVE_INSERT_MASK \
								| PPOOL_RESERVE_FIND_MASK | PPOOL_RESERVE_SHARED_INFO_INSERT_MASK)

struct OPagePool
{
	/* count of available to reserve pages in the pool */
	pg_atomic_uint64 *availablePagesCount;
	/* count of dirty pages in the pool */
	pg_atomic_uint32 *dirtyPagesCount;
	/* init position for the ucm */
	OInMemoryBlkno location;
	/* offset of the pool in the o_shared_buffers */
	OInMemoryBlkno offset;
	/* size of the pool */
	OInMemoryBlkno size;
	/* reserved pages count by type array */
	OInMemoryBlkno numPagesReserved[PPOOL_RESERVE_COUNT];
	/* usage counter map and their size in shared memory */
	UsageCountMap ucm;
	Size		ucmShmemSize;
	/* seed for random values */
#if PG_VERSION_NUM >= 150000
	pg_prng_state prngSeed;
#else
	unsigned short xseed[3];
#endif
};

extern Size ppool_estimate_space(OPagePool *pool, OInMemoryBlkno offset, OInMemoryBlkno size, bool debug);
extern void ppool_shmem_init(OPagePool *pool, Pointer ptr, bool found);
extern OInMemoryBlkno ppool_free_pages_count(OPagePool *pool);
extern OInMemoryBlkno ppool_dirty_pages_count(OPagePool *pool);
extern void ppool_run_clock(OPagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested);

extern void ppool_reserve_pages(OPagePool *pool, int kind, int count);
extern void ppool_release_reserved(OPagePool *pool, uint32 mask);
extern void ppool_release_all_pages(void);
extern OInMemoryBlkno ppool_get_metapage(OPagePool *pool);
extern OInMemoryBlkno ppool_get_page(OPagePool *pool, int kind);
extern void ppool_free_page(OPagePool *pool, OInMemoryBlkno blkno, bool haveLock);

#define PAGE_DESC_FLAG_DIRTY			1	/* Modified since the the last
											 * time being written out */
#define PAGE_DESC_FLAG_CONCURRENT_DIRTY	2	/* Second "dirty" flag used to
											 * detect changes concurrent to
											 * write operatorions */
#define PAGE_DESC_FLAG_BOTH_DIRTY		(PAGE_DESC_FLAG_DIRTY | PAGE_DESC_FLAG_CONCURRENT_DIRTY)
#define IS_DIRTY(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags & PAGE_DESC_FLAG_DIRTY)
#define IS_DIRTY_CONCURRENT(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags & PAGE_DESC_FLAG_CONCURRENT_DIRTY)
#define CLEAN_DIRTY_CONCURRENT(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags &= ~PAGE_DESC_FLAG_CONCURRENT_DIRTY)

#define MARK_DIRTY(pool, blkno) \
	do \
	{ \
		if (!IS_DIRTY(blkno)) { \
			O_GET_IN_MEMORY_PAGEDESC(blkno)->flags |= PAGE_DESC_FLAG_BOTH_DIRTY; \
			pg_atomic_fetch_add_u32(pool->dirtyPagesCount, 1); \
		} \
		else if (!IS_DIRTY_CONCURRENT(blkno)) \
		{ \
			O_GET_IN_MEMORY_PAGEDESC(blkno)->flags |= PAGE_DESC_FLAG_CONCURRENT_DIRTY; \
		} \
	} \
	while (0); \

#define CLEAN_DIRTY(pool, blkno) \
	if (IS_DIRTY(blkno)) { \
		O_GET_IN_MEMORY_PAGEDESC(blkno)->flags &= ~PAGE_DESC_FLAG_BOTH_DIRTY; \
		pg_atomic_fetch_sub_u32(pool->dirtyPagesCount, 1); \
	}

#define FREE_PAGE_IF_VALID(pool, blkno) \
	if (OInMemoryBlknoIsValid((blkno))) \
	{ \
		CLEAN_DIRTY((pool), (blkno)); \
		ppool_free_page((pool), (blkno), false); \
		(blkno) = OInvalidInMemoryBlkno; \
	} \

#endif							/* __PAGE_POOL_H__ */
