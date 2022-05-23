/*-------------------------------------------------------------------------
 *
 * page_pool.c
 *		OrioleDB logical page pool implementation.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/utils/page_pool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "btree/page_contents.h"
#include "btree/undo.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/ucm.h"

#include "utils/memdebug.h"

/*
 * Calculates shared memory space needed for a page pool. Be careful,
 * it prepares local memory structures to initialize.
 */
Size
ppool_estimate_space(OPagePool *pool, OInMemoryBlkno offset, OInMemoryBlkno size, bool debug)
{
	Size		result = 0;

	if (!debug)
		Assert(size >= PPOOL_MIN_SIZE);

	pool->offset = offset;
	pool->size = size;

	result += CACHELINEALIGN(sizeof(pg_atomic_uint64));
	result += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	pool->ucmShmemSize = estimate_ucm_space(&pool->ucm, offset, size);

	result += pool->ucmShmemSize;
	return result;
}

/*
 * Initializes data in shared memory for the page pool. ppool_estimate_space()
 * must be already called for the pool.
 */
void
ppool_shmem_init(OPagePool *pool, Pointer ptr, bool found)
{
	pool->availablePagesCount = (pg_atomic_uint64 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint64));

	pool->dirtyPagesCount = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	if (!found)
	{
		pg_atomic_init_u64(pool->availablePagesCount, pool->size);
		pg_atomic_init_u32(pool->dirtyPagesCount, 0);
	}

	init_ucm(&pool->ucm, ptr, found);

#if PG_VERSION_NUM >= 150000
	pg_prng_seed(&pool->prngSeed, MyBackendId);
	pool->location = pg_prng_uint64_range(&pool->prngSeed,
										  pool->offset,
										  pool->offset + pool->size - 1);
#else
	pool->xseed[0] = MyBackendId;
	pool->xseed[1] = MyBackendId >> 16;
	pool->xseed[2] = 0;
	pool->location = pool->offset + (uint64) pg_jrand48(pool->xseed) % pool->size;
#endif
}

/*
 * Reserve pages for further allocation.  Reserving pages might require running
 * clock algorithm with page eviction.  It shouldn't be called while holding
 * a page lock for two reasons.
 *
 * 1) Searching and eviction of page might take too long time for holding a
 *    page lock.
 * 2) Eviction of page places page locks itself.  And it's hard to guarantee
 *    there is no deadlocks assuming that we might evict almost any page.
 *
 * This is why one should reserve enough amount of pages _before_ taking a page
 * lock, and then allocate them using ucm_occupy_free_page().
 */
void
ppool_reserve_pages(OPagePool *pool, int kind, int count)
{
	uint64		val;

	Assert(!have_locked_pages());

	count -= pool->numPagesReserved[kind];
	if (count <= 0)
		return;

	val = pg_atomic_sub_fetch_u64(pool->availablePagesCount, count);
	while (val & (UINT64CONST(1) << 63))
	{
		ppool_run_clock(pool, true, NULL);
		val = pg_atomic_read_u64(pool->availablePagesCount);
	}

	pool->numPagesReserved[kind] += count;
}

/*
 * Release previously reserved pages according to mask (multiple kinds can be
 * released in one call).
 */
void
ppool_release_reserved(OPagePool *pool, uint32 mask)
{
	int			sum = 0,
				kind;

	for (kind = 0; kind < PPOOL_RESERVE_COUNT; kind++)
	{
		if (mask & (1 << kind))
		{
			sum += pool->numPagesReserved[kind];
			pool->numPagesReserved[kind] = 0;
		}
	}
	if (sum != 0)
		pg_atomic_add_fetch_u64(pool->availablePagesCount, sum);
}

/*
 * Release all reserved pages in all the pools.
 */
void
ppool_release_all_pages(void)
{
	int			i;

	for (i = 0; i < (int) OPagePoolTypesCount; i++)
	{
		OPagePool  *pool = get_ppool((OPagePoolType) i);

		ppool_release_reserved(pool, PPOOL_RESERVE_MASK_ALL);
	}
}

/*
 * Reserves and allocate page for metadata. Metadata pages are typically
 * allocated without holding any page locks.
 */
OInMemoryBlkno
ppool_get_metapage(OPagePool *pool)
{
	ppool_reserve_pages(pool, PPOOL_RESERVE_META, 1);
	return ppool_get_page(pool, PPOOL_RESERVE_META);
}

/*
 * Get next free page from the pool.
 *
 * Free page should be previously reserved by o_pool_reserve_pages().
 */
OInMemoryBlkno
ppool_get_page(OPagePool *pool, int kind)
{
	OInMemoryBlkno result;

	Assert(pool->numPagesReserved[kind] > 0);
	pool->numPagesReserved[kind]--;

	result = ucm_occupy_free_page(&pool->ucm);
	Assert(pool->offset <= result && result < pool->offset + pool->size);

	VALGRIND_CHECK_MEM_IS_DEFINED(O_GET_IN_MEMORY_PAGE(result), ORIOLEDB_BLCKSZ);

	return result;
}

/*
 * Return free page to the pool.
 */
void
ppool_free_page(OPagePool *pool, OInMemoryBlkno blkno, bool haveLock)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);

	Assert(pool->offset <= blkno && blkno < pool->offset + pool->size);

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);
	Assert(!IS_DIRTY(blkno));

	/*
	 * Reset page header and descriptor.  Do this while holding a page lock in
	 * order to prevent race condition with walk_page().
	 */
	if (!haveLock)
		lock_page(blkno);
	O_PAGE_CHANGE_COUNT_INC(p);
	page_desc->oids.datoid = InvalidOid;
	page_desc->oids.relnode = InvalidOid;
	page_desc->oids.reloid = InvalidOid;
	page_desc->type = 0;
	page_desc->fileExtent.off = InvalidFileExtentOff;
	page_desc->fileExtent.len = InvalidFileExtentLen;
	unlock_page(blkno);

	page_change_usage_count(&pool->ucm, blkno, UCM_FREE_PAGES_LEVEL);

	pg_atomic_add_fetch_u64(pool->availablePagesCount, 1);
}

/*
 * Return count of free pages in the pool.
 */
OInMemoryBlkno
ppool_free_pages_count(OPagePool *pool)
{
	uint64		count = pg_atomic_read_u64(pool->availablePagesCount);

	if (count & (UINT64CONST(1) << 63))
		return 0;
	else
		return (OInMemoryBlkno) count;
}

/*
 * Return count of dirty pages in the pool.
 */
OInMemoryBlkno
ppool_dirty_pages_count(OPagePool *pool)
{
	return pg_atomic_read_u32(pool->dirtyPagesCount);
}

/*
 * Run clock replacement algorithm until we evict at least one page.
 */
void
ppool_run_clock(OPagePool *pool, bool evict,
				volatile sig_atomic_t *shutdown_requested)
{
	uint64		blkno;
	Size		undoSize = get_reserved_undo_size(UndoReserveTxn);
	bool		haveRetainLoc = have_retained_undo_location();

#if PG_VERSION_NUM >= 150000
	blkno = pg_prng_uint64_range(&pool->prngSeed,
								 pool->offset,
								 pool->offset + pool->size - 1);
#else
	blkno = pool->offset + (uint64) pg_jrand48(pool->xseed) % pool->size;
#endif

	/*
	 * Shouldn't be called while holding a page lock: one should reserve the
	 * pages in advance.
	 */
	Assert(!have_locked_pages());

	/* We might need to merge pages */
	reserve_undo_size(UndoReserveTxn, 2 * O_MERGE_UNDO_IMAGE_SIZE);

	Assert(blkno >= pool->offset && blkno < pool->offset + pool->size);
	/* Our attempts to evict pages shouldn't themselves affect UCM */
	set_skip_ucm();

	while (true)
	{
		if (shutdown_requested != NULL && *shutdown_requested)
			break;

		blkno = ucm_next_blkno(&pool->ucm, blkno, 1);

		Assert(blkno >= pool->offset && blkno < pool->offset + pool->size);
		if (walk_page(blkno, evict) != OWalkPageSkipped)
		{
			Assert(!have_locked_pages());
			break;
		}
		Assert(!have_locked_pages());
		blkno++;
		if (blkno >= pool->offset + pool->size)
			blkno = pool->offset;
	}

	unset_skip_ucm();

	/*
	 * The caller might have the undo location reserved.  We need to carefully
	 * put the undo location back.
	 */
	if (haveRetainLoc)
	{
		if (undoSize > 0)
			reserve_undo_size(UndoReserveTxn, undoSize);
	}
	else
	{
		release_undo_size(UndoReserveTxn);
		free_retained_undo_location();
		if (undoSize > 0)
			reserve_undo_size(UndoReserveTxn, undoSize);
	}
}
