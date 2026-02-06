/*-------------------------------------------------------------------------
 *
 * page_pool.h
 *		Declarations for OrioleDB logical page pool implementation.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/page_pool.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PAGE_POOL_H__
#define __PAGE_POOL_H__

#include "common/pg_prng.h"
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

typedef struct PagePool PagePool;

typedef struct PagePoolConfig PagePoolConfig;

/*
 * Page pool operations - implemented by each pool type
 */
typedef struct PagePoolOps
{
	/* Page allocation/deallocation */
	OInMemoryBlkno (*alloc_page) (PagePool *pool, int pageReserveKind);
	OInMemoryBlkno (*alloc_metapage) (PagePool *pool);
	void		(*free_page) (PagePool *pool, OInMemoryBlkno blkno, bool haveLock);

	/* Page reservation system */
	void		(*reserve_pages) (PagePool *pool, int pageReserveKind, int count);
	void		(*release_reserved) (PagePool *pool, uint32 kind_mask);

	OInMemoryBlkno (*free_pages_count) (PagePool *pool);
	OInMemoryBlkno (*dirty_pages_count) (PagePool *pool);
	void		(*run_clock) (PagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested);
	OInMemoryBlkno (*size) (PagePool *pool);

	/* Usage tracking */
	void		(*ucm_inc_usage) (PagePool *pool, OInMemoryBlkno blkno);
	void		(*ucm_change_usage) (PagePool *pool, OInMemoryBlkno blkno, uint32 usageCount);
	uint32		(*ucm_get_epoch) (PagePool *pool);
	bool		(*ucm_epoch_needs_shift) (PagePool *pool);
	void		(*ucm_epoch_shift) (PagePool *pool);
	uint64		(*ucm_update_state) (PagePool *pool, OInMemoryBlkno blkno, uint64 state);
	void		(*ucm_after_update_state) (PagePool *pool, OInMemoryBlkno blkno, uint64 oldState, uint64 newState);

	uint64		(*write_build_page) (PagePool *pool, BTreeDescr *desc, Page img,
									 FileExtent *extent, BTreeMetaPage *metaPage);
} PagePoolOps;

typedef struct PagePool
{
	const PagePoolOps *ops;
} PagePool;

extern void ppool_release_all_pages(void);

/* Shared memory based page pool handle */
typedef struct OPagePool
{
	PagePool	base;
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
	pg_prng_state prngSeed;
} OPagePool;

/* Shared memory based page pool operations */

extern Size o_ppool_estimate_space(OPagePool *pool, OInMemoryBlkno offset, OInMemoryBlkno size, bool debug);
extern void o_ppool_shmem_init(OPagePool *pool, Pointer ptr, bool found);

/* Local memory page pool handler */
typedef struct LocalPagePool
{
	PagePool	base;
	MemoryContext slab_context;
	uint32		size;
	uint32		current_slot;
} LocalPagePool;


extern void local_ppool_init(LocalPagePool *pool);

#define PAGE_DESC_FLAG_DIRTY			1	/* Modified since the the last
											 * time being written out */
#define PAGE_DESC_FLAG_CONCURRENT_DIRTY	2	/* Second "dirty" flag used to
											 * detect changes concurrent to
											 * write operatorions */
#define PAGE_DESC_FLAG_BOTH_DIRTY		(PAGE_DESC_FLAG_DIRTY | PAGE_DESC_FLAG_CONCURRENT_DIRTY)
#define IS_DIRTY(blkno) ((O_GET_IN_MEMORY_PAGEDESC(blkno)->flags & PAGE_DESC_FLAG_DIRTY) && !O_PAGE_IS_LOCAL(blkno))
#define IS_DIRTY_CONCURRENT(blkno) ((O_GET_IN_MEMORY_PAGEDESC(blkno)->flags & PAGE_DESC_FLAG_CONCURRENT_DIRTY) && !O_PAGE_IS_LOCAL(blkno))
#define CLEAN_DIRTY_CONCURRENT(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags &= ~PAGE_DESC_FLAG_CONCURRENT_DIRTY)

/*  Local page can never be dirty as it's never synced with disk */
#define MARK_DIRTY_EXTENDED(desc, blkno, skipMeta) \
	do \
	{ \
	    if (O_PAGE_IS_LOCAL(blkno)) break; \
		if (!(skipMeta)) \
		{ \
			BTREE_GET_META(desc)->dirtyFlag1 = true; \
			BTREE_GET_META(desc)->dirtyFlag2 = true; \
		} \
		if (!IS_DIRTY(blkno)) { \
			O_GET_IN_MEMORY_PAGEDESC(blkno)->flags |= PAGE_DESC_FLAG_BOTH_DIRTY; \
			pg_atomic_fetch_add_u32(((OPagePool*)(desc)->ppool)->dirtyPagesCount, 1); \
		} \
		else if (!IS_DIRTY_CONCURRENT(blkno)) \
		{ \
			O_GET_IN_MEMORY_PAGEDESC(blkno)->flags |= PAGE_DESC_FLAG_CONCURRENT_DIRTY; \
		} \
	} \
	while (0);

#define MARK_DIRTY(desc, blkno) \
	MARK_DIRTY_EXTENDED(desc, blkno, false)

/*  Local page can never be dirty as it's never synced with disk */
#define CLEAN_DIRTY(pool, blkno) \
	if (IS_DIRTY(blkno)) { \
		O_GET_IN_MEMORY_PAGEDESC(blkno)->flags &= ~PAGE_DESC_FLAG_BOTH_DIRTY; \
		pg_atomic_fetch_sub_u32(((OPagePool*)(pool))->dirtyPagesCount, 1); \
	}

#define FREE_PAGE_IF_VALID(pool, blkno) \
	if (OInMemoryBlknoIsValid((blkno))) \
	{ \
        CLEAN_DIRTY((pool), (blkno)); \
		(*(pool)->ops->free_page)((pool), (blkno), false); \
		(blkno) = OInvalidInMemoryBlkno; \
	} \


#endif							/* __PAGE_POOL_H__ */
