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
	void		(*run_maintenance) (PagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested);
	OInMemoryBlkno (*size) (PagePool *pool);

	/* Usage tracking */
	void		(*ucm_inc_usage) (PagePool *pool, OInMemoryBlkno blkno);
	void		(*ucm_init) (PagePool *pool, OInMemoryBlkno blkno);
} PagePoolOps;

typedef struct PagePool
{
	const PagePoolOps *ops;
	/* reserved pages count by type array */
	uint32		numPagesReserved[PPOOL_RESERVE_COUNT];
} PagePool;

/* Inline dispatch wrappers over PagePoolOps */

static inline OInMemoryBlkno
ppool_alloc_page(PagePool *pool, int pageReserveKind)
{
	return pool->ops->alloc_page(pool, pageReserveKind);
}

static inline OInMemoryBlkno
ppool_alloc_metapage(PagePool *pool)
{
	return pool->ops->alloc_metapage(pool);
}

static inline void
ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock)
{
	pool->ops->free_page(pool, blkno, haveLock);
}

static inline void
ppool_reserve_pages(PagePool *pool, int pageReserveKind, int count)
{
	pool->ops->reserve_pages(pool, pageReserveKind, count);
}

static inline void
ppool_release_reserved(PagePool *pool, uint32 kind_mask)
{
	pool->ops->release_reserved(pool, kind_mask);
}

static inline OInMemoryBlkno
ppool_free_pages_count(PagePool *pool)
{
	return pool->ops->free_pages_count(pool);
}

static inline OInMemoryBlkno
ppool_dirty_pages_count(PagePool *pool)
{
	return pool->ops->dirty_pages_count(pool);
}

static inline void
ppool_run_maintenance(PagePool *pool, bool evict,
					  volatile sig_atomic_t *shutdown_requested)
{
	pool->ops->run_maintenance(pool, evict, shutdown_requested);
}

static inline OInMemoryBlkno
ppool_size(PagePool *pool)
{
	return pool->ops->size(pool);
}

static inline void
ppool_ucm_inc_usage(PagePool *pool, OInMemoryBlkno blkno)
{
	pool->ops->ucm_inc_usage(pool, blkno);
}

static inline void
ppool_ucm_init(PagePool *pool, OInMemoryBlkno blkno)
{
	pool->ops->ucm_init(pool, blkno);
}

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
	uint32		alloc_current_slot;
	uint32		evict_current_slot;
	/* count of available to reserve pages in the pool */
	uint32		availablePagesCount;
	/* count of dirty pages in the pool */
	uint32		dirtyPagesCount;
	uint32	   *usage_count;
} LocalPagePool;

extern void local_ppool_init(LocalPagePool *pool);

#define PAGE_DESC_FLAG_DIRTY			1	/* Modified since the the last
											 * time being written out */
#define PAGE_DESC_FLAG_CONCURRENT_DIRTY	2	/* Second "dirty" flag used to
											 * detect changes concurrent to
											 * write operatorions */
#define PAGE_DESC_FLAG_BOTH_DIRTY		(PAGE_DESC_FLAG_DIRTY | PAGE_DESC_FLAG_CONCURRENT_DIRTY)
#define IS_DIRTY(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags & PAGE_DESC_FLAG_DIRTY)
#define IS_DIRTY_CONCURRENT(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags & PAGE_DESC_FLAG_CONCURRENT_DIRTY)
#define CLEAN_DIRTY_CONCURRENT(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags &= ~PAGE_DESC_FLAG_CONCURRENT_DIRTY)
#define BLKNO_LOCAL_BIT 0x80000000

#define MARK_DIRTY_EXTENDED(desc, blkno, skipMeta) \
	do \
	{ \
		if (!(skipMeta)) \
		{ \
			BTREE_GET_META(desc)->dirtyFlag1 = true; \
			BTREE_GET_META(desc)->dirtyFlag2 = true; \
		} \
		if (!IS_DIRTY(blkno)) { \
			O_GET_IN_MEMORY_PAGEDESC(blkno)->flags |= PAGE_DESC_FLAG_BOTH_DIRTY; \
			if(O_PAGE_IS_LOCAL(blkno)) { \
				((LocalPagePool*)(desc)->ppool)->dirtyPagesCount++; \
			} else { \
				pg_atomic_fetch_add_u32(((OPagePool*)(desc)->ppool)->dirtyPagesCount, 1); \
			} \
		} \
		else if (!IS_DIRTY_CONCURRENT(blkno)) \
		{ \
			O_GET_IN_MEMORY_PAGEDESC(blkno)->flags |= PAGE_DESC_FLAG_CONCURRENT_DIRTY; \
		} \
	} \
	while (0);

#define MARK_DIRTY(desc, blkno) \
	MARK_DIRTY_EXTENDED(desc, blkno, false)

#define CLEAN_DIRTY(pool, blkno) \
	if (IS_DIRTY(blkno)) { \
		O_GET_IN_MEMORY_PAGEDESC(blkno)->flags &= ~PAGE_DESC_FLAG_BOTH_DIRTY; \
		if(O_PAGE_IS_LOCAL(blkno)) { \
			((LocalPagePool*)(pool))->dirtyPagesCount--; \
		} else { \
			pg_atomic_fetch_sub_u32(((OPagePool*)(pool))->dirtyPagesCount, 1); \
		} \
	}

#define FREE_PAGE_IF_VALID(pool, blkno) \
	if (OInMemoryBlknoIsValid((blkno))) \
	{ \
        CLEAN_DIRTY((pool), (blkno)); \
		ppool_free_page((pool), (blkno), false); \
		(blkno) = OInvalidInMemoryBlkno; \
	} \


#endif							/* __PAGE_POOL_H__ */
