/*-------------------------------------------------------------------------
 *
 * page_pool.h
 *		Declarations for OrioleDB logical page pool implementation.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/page_pool.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PAGE_POOL_H__
#define __PAGE_POOL_H__

#include "common/pg_prng.h"
#include "storage/bufpage.h"
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

typedef char * SmallPagePointer;

typedef struct PagePoolConfig PagePoolConfig;

typedef enum PagePointerType {
	PPTR_TYPE_PTR,
	PPTR_TYPE_BLKNO
} PagePointerType;

typedef struct PagePointer {
    PagePointerType type;
    union {
    	SmallPagePointer ptr;
    	OInMemoryBlkno blkno;
    } ptr;
} PagePointer;

static inline bool pptr_eq(PagePointer pptr1, PagePointer pptr2)
{
	return pptr1.type == pptr2.type &&
		(pptr1.type == PPTR_TYPE_PTR ? pptr1.ptr.ptr  == pptr2.ptr.ptr : pptr1.ptr.blkno == pptr2.ptr.blkno);
}

static inline Page pptr_get_page(PagePointer pptr)
{
    if (pptr.type == PPTR_TYPE_BLKNO)
        return O_GET_IN_MEMORY_PAGE(pptr.ptr.blkno);
    else
        return (Page) pptr.ptr.ptr;
}

/*
 * Page pool operations - implemented by each pool type
 */
typedef struct PagePoolOps
{
	/* Page allocation/deallocation */
	PagePointer (*alloc_page)(PagePool *pool, int pageReserveKind);
	void (*free_page)(PagePool *pool, PagePointer ptr);
	
	/* Page reservation system */
	bool (*reserve_pages)(PagePool *pool, int pageReserveKind, uint32 count);
	void (*release_reserved)(PagePool *pool, uint32 kind_mask);
	uint32 (*get_reserved_count)(PagePool *pool, int pageReserveKind);
	
	/* Page state management */
	void (*mark_dirty)(PagePool *pool, PagePointer ptr);
	void (*mark_clean)(PagePool *pool, PagePointer ptr);
	bool (*is_dirty)(PagePool *pool, PagePointer ptr);
	
	/* Usage tracking */
	void (*inc_usage)(PagePool *pool, PagePointer ptr);
	void (*dec_usage)(PagePool *pool, PagePointer ptr);
	uint32 (*get_usage)(PagePool *pool, PagePointer ptr);
    
	void (*lock_page)(PagePointer ptr);
	void (*unlock_page)(PagePointer ptr);
        /* ... */
} PagePoolOps;

typedef struct PagePool {
    const PagePoolOps *ops;
} PagePool;

typedef struct OPagePool
{
    PagePool *base;
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

extern Size o_ppool_estimate_space(PagePool *pool, OInMemoryBlkno offset, OInMemoryBlkno size, bool debug);
extern void o_ppool_shmem_init(PagePool *pool, Pointer ptr, bool found);
extern OInMemoryBlkno o_ppool_free_pages_count(PagePool *pool);
extern OInMemoryBlkno o_ppool_dirty_pages_count(PagePool *pool);
extern void o_ppool_run_clock(PagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested);

extern void o_ppool_reserve_pages(PagePool *pool, int kind, int count);
extern void o_ppool_release_reserved(PagePool *pool, uint32 mask);
extern void o_ppool_release_all_pages(void);
extern OInMemoryBlkno o_ppool_get_metapage(PagePool *pool);
extern PagePointer o_ppool_get_page(PagePool *pool, int kind);
extern void o_ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock);

#define PAGE_DESC_FLAG_DIRTY			1	/* Modified since the the last
											 * time being written out */
#define PAGE_DESC_FLAG_CONCURRENT_DIRTY	2	/* Second "dirty" flag used to
											 * detect changes concurrent to
											 * write operatorions */
#define PAGE_DESC_FLAG_BOTH_DIRTY		(PAGE_DESC_FLAG_DIRTY | PAGE_DESC_FLAG_CONCURRENT_DIRTY)
#define IS_DIRTY(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags & PAGE_DESC_FLAG_DIRTY)
#define IS_DIRTY_CONCURRENT(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags & PAGE_DESC_FLAG_CONCURRENT_DIRTY)
#define CLEAN_DIRTY_CONCURRENT(blkno) (O_GET_IN_MEMORY_PAGEDESC(blkno)->flags &= ~PAGE_DESC_FLAG_CONCURRENT_DIRTY)

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
			pg_atomic_fetch_add_u32((desc)->ppool->dirtyPagesCount, 1); \
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
		pg_atomic_fetch_sub_u32((pool)->dirtyPagesCount, 1); \
	}

#define FREE_PAGE_IF_VALID(pool, blkno) \
	if (OInMemoryBlknoIsValid((blkno))) \
	{ \
		CLEAN_DIRTY((pool), (blkno)); \
		ppool_free_page((pool), (blkno), false); \
		(blkno) = OInvalidInMemoryBlkno; \
	} \
	
const PagePoolOps o_page_pool_ops = {
    .alloc_page = o_ppool_get_page,
};

#endif							/* __PAGE_POOL_H__ */

