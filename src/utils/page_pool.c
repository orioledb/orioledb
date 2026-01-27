/*-------------------------------------------------------------------------
 *
 * page_pool.c
 *		OrioleDB logical page pool implementation.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
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
#include "checkpoint/checkpoint.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/ucm.h"

#include "utils/memdebug.h"

#define LOCAL_PPOOL_INIT_SIZE 1024

/* Shared memory based page pool operations */

OInMemoryBlkno o_ppool_get_page(PagePool *pool, int kind);
OInMemoryBlkno o_ppool_get_metapage(PagePool *pool);
void		o_ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock);

void		o_ppool_reserve_pages(PagePool *pool, int kind, int count);
void		o_ppool_release_reserved(PagePool *pool, uint32 mask);

OInMemoryBlkno o_ppool_free_pages_count(PagePool *pool);
OInMemoryBlkno o_ppool_dirty_pages_count(PagePool *pool);
void		o_ppool_run_clock(PagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested);
OInMemoryBlkno o_ppool_size(PagePool *pool);

void		o_ucm_inc_usage(PagePool *pool, OInMemoryBlkno blkno);
void		o_ucm_change_usage(PagePool *pool, OInMemoryBlkno blkno, uint32 usageCount);
uint32		o_ucm_get_epoch(PagePool *pool);
bool		o_ucm_epoch_needs_shift(PagePool *pool);
void		o_ucm_epoch_shift(PagePool *pool);
uint64		o_ucm_update_state(PagePool *pool, OInMemoryBlkno blkno, uint64 state);
void		o_ucm_after_update_state(PagePool *pool, OInMemoryBlkno blkno, uint64 oldState, uint64 newState);

/* PagePoolOps for a shared memory based page pool */
static const PagePoolOps o_page_pool_ops = {
	.alloc_page = o_ppool_get_page,
	.alloc_metapage = o_ppool_get_metapage,
	.free_page = o_ppool_free_page,

	.reserve_pages = o_ppool_reserve_pages,
	.release_reserved = o_ppool_release_reserved,

	.free_pages_count = o_ppool_free_pages_count,
	.dirty_pages_count = o_ppool_dirty_pages_count,
	.run_clock = o_ppool_run_clock,
	.size = o_ppool_size,

	.ucm_inc_usage = o_ucm_inc_usage,
	.ucm_change_usage = o_ucm_change_usage,
	.ucm_get_epoch = o_ucm_get_epoch,
	.ucm_epoch_needs_shift = o_ucm_epoch_needs_shift,
	.ucm_epoch_shift = o_ucm_epoch_shift,
	.ucm_update_state = o_ucm_update_state,
	.ucm_after_update_state = o_ucm_after_update_state,
};

/* Shared local memory based page pool operations */

OInMemoryBlkno local_ppool_alloc_page(PagePool *pool, int kind);
OInMemoryBlkno local_ppool_alloc_metapage(PagePool *pool);
void		local_ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock);

void		local_ppool_reserve_pages(PagePool *pool, int kind, int count);
void		local_ppool_release_reserved(PagePool *pool, uint32 mask);

OInMemoryBlkno local_ppool_free_pages_count(PagePool *pool);
OInMemoryBlkno local_ppool_dirty_pages_count(PagePool *pool);
void		local_ppool_run_clock(PagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested);
OInMemoryBlkno local_ppool_size(PagePool *pool);

void		local_ucm_inc_usage(PagePool *pool, OInMemoryBlkno blkno);
void		local_ucm_change_usage(PagePool *pool, OInMemoryBlkno blkno, uint32 usageCount);
uint32		local_ucm_get_epoch(PagePool *pool);
bool		local_ucm_epoch_needs_shift(PagePool *pool);
void		local_ucm_epoch_shift(PagePool *pool);
uint64		local_ucm_update_state(PagePool *pool, OInMemoryBlkno blkno, uint64 state);
void		local_ucm_after_update_state(PagePool *pool, OInMemoryBlkno blkno, uint64 oldState, uint64 newState);

/* PagePoolOps for a local memory based page pool */
static const PagePoolOps local_ppool_ops = {
	.alloc_page = local_ppool_alloc_page,
	.alloc_metapage = local_ppool_alloc_metapage,
	.free_page = local_ppool_free_page,

	.reserve_pages = local_ppool_reserve_pages,
	.release_reserved = local_ppool_release_reserved,

	.free_pages_count = local_ppool_free_pages_count,
	.dirty_pages_count = local_ppool_dirty_pages_count,
	.run_clock = local_ppool_run_clock,
	.size = local_ppool_size,

	.ucm_inc_usage = local_ucm_inc_usage,
	.ucm_change_usage = local_ucm_change_usage,
	.ucm_get_epoch = local_ucm_get_epoch,
	.ucm_epoch_needs_shift = local_ucm_epoch_needs_shift,
	.ucm_epoch_shift = local_ucm_epoch_shift,
	.ucm_update_state = local_ucm_update_state,
	.ucm_after_update_state = local_ucm_after_update_state,
};

/*
 * Calculates shared memory space needed for a page pool. Be careful,
 * it prepares local memory structures to initialize.
 */
Size
o_ppool_estimate_space(OPagePool *pool, OInMemoryBlkno offset, OInMemoryBlkno size, bool debug)
{
	Size		result = 0;

	if (!debug)
		Assert(size >= PPOOL_MIN_SIZE);
	/* TODO: check for ppool max size */

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
o_ppool_shmem_init(OPagePool *pool, Pointer ptr, bool found)
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

	pg_prng_seed(&pool->prngSeed, MyBackendId);
	pool->location = pg_prng_uint64_range(&pool->prngSeed,
										  pool->offset,
										  pool->offset + pool->size - 1);
	pool->base.ops = &o_page_pool_ops;
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
o_ppool_reserve_pages(PagePool *pool, int kind, int count)
{
	uint64		val;
	OPagePool  *o_pool = (OPagePool *) pool;

	Assert(!have_locked_pages());

	count -= o_pool->numPagesReserved[kind];
	if (count <= 0)
		return;

	val = pg_atomic_sub_fetch_u64(o_pool->availablePagesCount, count);
	while (val & (UINT64CONST(1) << 63))
	{
		(*pool->ops->run_clock) (pool, true, NULL);
		val = pg_atomic_read_u64(o_pool->availablePagesCount);
	}

	o_pool->numPagesReserved[kind] += count;
}

/*
 * Release previously reserved pages according to mask (multiple kinds can be
 * released in one call).
 */
void
o_ppool_release_reserved(PagePool *pool, uint32 mask)
{
	int			sum = 0,
				kind;
	OPagePool  *o_pool = (OPagePool *) pool;

	for (kind = 0; kind < PPOOL_RESERVE_COUNT; kind++)
	{
		if (mask & (1 << kind))
		{
			sum += o_pool->numPagesReserved[kind];
			o_pool->numPagesReserved[kind] = 0;
		}
	}
	if (sum != 0)
		pg_atomic_add_fetch_u64(o_pool->availablePagesCount, sum);
}

/*
 * Release all reserved pages in all the shared memory pools.
 */
void
ppool_release_all_pages(void)
{
	int			i;

	for (i = 0; i < (int) OPagePoolTypesCount; i++)
	{
		PagePool   *pool = get_ppool((OPagePoolType) i);

		(*pool->ops->release_reserved) (pool, PPOOL_RESERVE_MASK_ALL);
	}
}

/*
 * Reserves and allocate page for metadata. Metadata pages are typically
 * allocated without holding any page locks.
 */
/*  THOUGHT: can be shared for both ppool impls */
OInMemoryBlkno
o_ppool_get_metapage(PagePool *pool)
{
	(*pool->ops->reserve_pages) (pool, PPOOL_RESERVE_META, 1);
	return (*pool->ops->alloc_page) (pool, PPOOL_RESERVE_META);
}

/*
 * Get next free page from the pool.
 *
 * Free page should be previously reserved by o_pool_reserve_pages().
 */
OInMemoryBlkno
o_ppool_get_page(PagePool *pool, int kind)
{
	OPagePool  *o_pool = (OPagePool *) pool;
	OInMemoryBlkno result;

	Assert(o_pool->numPagesReserved[kind] > 0);
	o_pool->numPagesReserved[kind]--;

	result = ucm_occupy_free_page(&o_pool->ucm);
	Assert(o_pool->offset <= result && result < o_pool->offset + o_pool->size);

	VALGRIND_CHECK_MEM_IS_DEFINED(O_GET_IN_MEMORY_PAGE(result), ORIOLEDB_BLCKSZ);

	return result;
}

/*
 * Return free page to the pool.
 */
void
o_ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	OPagePool  *o_pool = (OPagePool *) pool;

	Assert(o_pool->offset <= blkno && blkno < o_pool->offset + o_pool->size);

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);
	Assert(!IS_DIRTY(blkno));

	/*
	 * Reset page header and descriptor.  Do this while holding a page lock in
	 * order to prevent race condition with walk_page().
	 */
	if (!haveLock)
		lock_page(blkno);
	O_PAGE_CHANGE_COUNT_INC(p);
	ORelOidsSetInvalid(page_desc->oids);
	page_desc->type = 0;
	page_desc->fileExtent.off = InvalidFileExtentOff;
	page_desc->fileExtent.len = InvalidFileExtentLen;
	unlock_page(blkno);

	page_change_usage_count(&o_pool->ucm, blkno, UCM_FREE_PAGES_LEVEL);

	pg_atomic_add_fetch_u64(o_pool->availablePagesCount, 1);
}

/*
 * Return count of free pages in the pool.
 */
OInMemoryBlkno
o_ppool_free_pages_count(PagePool *pool)
{
	OPagePool  *o_pool = (OPagePool *) pool;
	uint64		count = pg_atomic_read_u64(o_pool->availablePagesCount);

	if (count & (UINT64CONST(1) << 63))
		return 0;
	else
		return (OInMemoryBlkno) count;
}

/*
 * Return count of dirty pages in the pool.
 */
OInMemoryBlkno
o_ppool_dirty_pages_count(PagePool *pool)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	return pg_atomic_read_u32(o_pool->dirtyPagesCount);
}

/*
 * Run clock replacement algorithm until we evict at least one page.
 */
void
o_ppool_run_clock(PagePool *pool, bool evict,
				  volatile sig_atomic_t *shutdown_requested)
{
	uint64		blkno;
	Size		undoRegularSize = get_reserved_undo_size(UndoLogRegularPageLevel);
	Size		undoSystemSize = get_reserved_undo_size(UndoLogSystem);
	bool		haveRetainRegularLoc = undo_type_has_retained_location(UndoLogRegularPageLevel);
	bool		haveRetainSystemLoc = undo_type_has_retained_location(UndoLogSystem);
	OPagePool  *o_pool = (OPagePool *) pool;

	blkno = pg_prng_uint64_range(&o_pool->prngSeed,
								 o_pool->offset,
								 o_pool->offset + o_pool->size - 1);

	/*
	 * Shouldn't be called while holding a page lock: one should reserve the
	 * pages in advance.
	 */
	Assert(!have_locked_pages());

	/* We might need to merge pages */
	reserve_undo_size(UndoLogRegularPageLevel, 2 * O_MERGE_UNDO_IMAGE_SIZE);
	reserve_undo_size(UndoLogSystem, 2 * O_MERGE_UNDO_IMAGE_SIZE);

	Assert(blkno >= o_pool->offset && blkno < o_pool->offset + o_pool->size);
	/* Our attempts to evict pages shouldn't themselves affect UCM */
	set_skip_ucm();

	while (true)
	{
		if (shutdown_requested != NULL && *shutdown_requested)
			break;

		blkno = ucm_next_blkno(&o_pool->ucm, blkno, 1);

		Assert(blkno >= o_pool->offset && blkno < o_pool->offset + o_pool->size);
		if (walk_page(blkno, evict) != OWalkPageSkipped)
		{
			Assert(!have_locked_pages());
			break;
		}
		Assert(!have_locked_pages());
		blkno++;
		if (blkno >= o_pool->offset + o_pool->size)
			blkno = o_pool->offset;
	}

	unset_skip_ucm();

	/*
	 * The caller might have the undo location reserved.  We need to carefully
	 * put the undo location back.
	 */
	if (undoRegularSize > 0)
		reserve_undo_size(UndoLogRegularPageLevel, undoRegularSize);
	else
		release_undo_size(UndoLogRegularPageLevel);

	if (undoSystemSize > 0)
		reserve_undo_size(UndoLogSystem, undoSystemSize);
	else
		release_undo_size(UndoLogSystem);

	if (!haveRetainRegularLoc)
		free_retained_undo_location(UndoLogRegularPageLevel);
	if (!haveRetainSystemLoc)
		free_retained_undo_location(UndoLogSystem);
}

/*
 * Return the size of the page pool.
 */
OInMemoryBlkno
o_ppool_size(PagePool *pool)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	return o_pool->size;
}

void
o_ucm_inc_usage(PagePool *pool, OInMemoryBlkno blkno)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	page_inc_usage_count(&o_pool->ucm, blkno);
}

void
o_ucm_change_usage(PagePool *pool, OInMemoryBlkno blkno, uint32 usageCount)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	page_change_usage_count(&o_pool->ucm, blkno, usageCount);
}

uint32
o_ucm_get_epoch(PagePool *pool)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	return pg_atomic_read_u32(o_pool->ucm.epoch);
}

bool
o_ucm_epoch_needs_shift(PagePool *pool)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	return ucm_epoch_needs_shift(&o_pool->ucm);
}

void
o_ucm_epoch_shift(PagePool *pool)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	ucm_epoch_shift(&o_pool->ucm);
}

uint64
o_ucm_update_state(PagePool *pool, OInMemoryBlkno blkno, uint64 state)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	return ucm_update_state(&o_pool->ucm, blkno, state);
}

void
o_ucm_after_update_state(PagePool *pool, OInMemoryBlkno blkno, uint64 oldState, uint64 newState)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	ucm_after_update_state(&o_pool->ucm, blkno, oldState, newState);
}

void
local_ppool_init(LocalPagePool *pool)
{
	local_ppool_pages = calloc(LOCAL_PPOOL_INIT_SIZE, sizeof(Page));
	local_ppool_page_descs = calloc(LOCAL_PPOOL_INIT_SIZE, sizeof(OrioleDBPageDesc));
	if (!local_ppool_pages || !local_ppool_page_descs)
		ereport(ERROR, errmsg("Failed to allocate memory for local page pool"));
	pool->size = LOCAL_PPOOL_INIT_SIZE;
	pool->current_slot = 0;
	pool->slab_context = SlabContextCreate(TopMemoryContext, "oriole local page pool", ORIOLEDB_BLCKSZ * 16, ORIOLEDB_BLCKSZ);
	pool->base.ops = &local_ppool_ops;
}

OInMemoryBlkno
local_ppool_alloc_page(PagePool *pool, int kind)
{
	LocalPagePool *local_pool = (LocalPagePool *) pool;

	int			start = local_pool->current_slot;
	int			i = start;
	int			old_size = local_pool->size;
	int			new_size;
	Page	   *new_pages;
	OrioleDBPageDesc *new_page_descs;

	/* Iterate through local_pool->pages to find a free slot */
	do
	{
		i++;
		if (i >= local_pool->size)
			i = 0;
		if (local_ppool_pages[i] == NULL)
		{
			local_ppool_pages[i] = (Page) MemoryContextAllocZero(local_pool->slab_context, ORIOLEDB_BLCKSZ);
			local_pool->current_slot = i;
			/* Set the local page bit */
			return i | 0x80000000;
		}
	} while (i != start);
	
	/* Failed to find a free slot - increase pages array size */
	
	new_size = local_pool->size * 2;
	new_pages = realloc(local_ppool_pages, new_size * sizeof(Page));
	new_page_descs = realloc(local_ppool_page_descs, new_size * sizeof(OrioleDBPageDesc));
	
	if (!new_pages || !new_page_descs)
	{
		/* Original pointers remain valid if their realloc failed, keeping state consistent. */
		ereport(ERROR, errmsg("Failed to allocate memory for local page pool"));
	}
	
	local_ppool_pages = new_pages;
	local_ppool_page_descs = new_page_descs;
	local_pool->size = new_size;
	memset(local_ppool_pages + old_size, 0, old_size * sizeof(Page));
	memset(local_ppool_page_descs + old_size, 0, old_size * sizeof(OrioleDBPageDesc));
	
	local_pool->current_slot = old_size;
	local_ppool_pages[old_size] = (Page) MemoryContextAllocZero(local_pool->slab_context, ORIOLEDB_BLCKSZ);
	
	/* Set the local page bit */
	return old_size | 0x80000000;
}

OInMemoryBlkno
local_ppool_alloc_metapage(PagePool *pool)
{
	/* Kind is not used */
	return local_ppool_alloc_page(pool, 0);
}

void
local_ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock)
{
	int			i = blkno & O_BLKNO_MASK;

	pfree(local_ppool_pages[i]);
	local_ppool_pages[i] = NULL;
}

void
local_ppool_reserve_pages(PagePool *pool, int kind, int count)
{
	/* Stub: do nothing */
}

void
local_ppool_release_reserved(PagePool *pool, uint32 mask)
{
	/* Stub: do nothing */
}

OInMemoryBlkno
local_ppool_free_pages_count(PagePool *pool)
{
	return UINT32_MAX;
}

OInMemoryBlkno
local_ppool_dirty_pages_count(PagePool *pool)
{
	return 0;
}

void
local_ppool_run_clock(PagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested)
{
	/* Stub: do nothing */
}

OInMemoryBlkno
local_ppool_size(PagePool *pool)
{
    LocalPagePool  *o_pool = (LocalPagePool *) pool;
    
	return o_pool->size;
}

void
local_ucm_inc_usage(PagePool *pool, OInMemoryBlkno blkno)
{
	/* Stub: do nothing */
}

void
local_ucm_change_usage(PagePool *pool, OInMemoryBlkno blkno, uint32 usageCount)
{
	/* Stub: do nothing */
}

uint32
local_ucm_get_epoch(PagePool *pool)
{
	return 0;
}

bool
local_ucm_epoch_needs_shift(PagePool *pool)
{
	return false;
}

void
local_ucm_epoch_shift(PagePool *pool)
{
	/* Stub: do nothing */
}

uint64
local_ucm_update_state(PagePool *pool, OInMemoryBlkno blkno, uint64 state)
{
	return state;
}

void
local_ucm_after_update_state(PagePool *pool, OInMemoryBlkno blkno, uint64 oldState, uint64 newState)
{
	/* Stub: do nothing */
}
