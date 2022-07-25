/*-------------------------------------------------------------------------
 *
 * page_state.c
 *		OrioleDB B-tree page locking, waiting, reading etc.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/page_state.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/io.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"
#include "utils/ucm.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/proc.h"
#include "storage/proclist.h"
#include "storage/s_lock.h"
#include "utils/memdebug.h"

/* Maximum simultaneously locked pages per process */
#define MAX_PAGES_PER_PROCESS 8

/*
 * Enable this to recheck page starts and struct on every unlock.
 */
/* #define CHECK_PAGE_STATS */
/* #define CHECK_PAGE_STRUCT */

typedef struct
{
	OInMemoryBlkno blkno;
	uint32		state;
} MyLockedPage;

static MyLockedPage myLockedPages[MAX_PAGES_PER_PROCESS];
static OInMemoryBlkno myInProgressSplitPages[ORIOLEDB_MAX_DEPTH * 2];
static int	numberOfMyLockedPages = 0;
static int	numberOfMyInProgressSplitPages = 0;


#ifdef CHECK_PAGE_STRUCT
static void o_check_page_struct(BTreeDescr *desc, Page p);
#endif
#ifdef CHECK_PAGE_STATS
static void o_check_btree_page_statistics(BTreeDescr *desc, Pointer p);
#endif

static int
get_my_locked_page_index(OInMemoryBlkno blkno)
{
	int			i;

	for (i = 0; i < numberOfMyLockedPages; i++)
		if (myLockedPages[i].blkno == blkno)
			return i;
	return -1;
}

static void
my_locked_page_add(OInMemoryBlkno blkno, uint32 state)
{
	Assert(get_my_locked_page_index(blkno) < 0);
	Assert(numberOfMyLockedPages < MAX_PAGES_PER_PROCESS);
	myLockedPages[numberOfMyLockedPages].blkno = blkno;
	myLockedPages[numberOfMyLockedPages++].state = state;
}

static uint32
my_locked_page_del(OInMemoryBlkno blkno)
{
	int			i = get_my_locked_page_index(blkno);
	uint32		state;

	Assert(i >= 0);
	state = myLockedPages[i].state;
	myLockedPages[i] = myLockedPages[--numberOfMyLockedPages];

	return state;
}

static uint32
my_locked_page_get_state(OInMemoryBlkno blkno)
{
	int			i = get_my_locked_page_index(blkno);

	Assert(i >= 0);
	return myLockedPages[i].state;
}

static uint32
lock_page_or_list(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	uint32		state;
	SpinDelayStatus status;

	init_local_spin_delay(&status);
	state = pg_atomic_read_u32(&header->state);
	while (true)
	{
		uint32		newState;

		if (!O_PAGE_STATE_IS_LOCKED(state))
		{
			newState = O_PAGE_STATE_LOCK(state);
		}
		else if (!(state & PAGE_STATE_LIST_LOCKED_FLAG))
		{
			newState = state | (PAGE_STATE_LIST_LOCKED_FLAG | PAGE_STATE_HAS_WAITERS_FLAG);
		}
		else
		{
			perform_spin_delay(&status);
			state = pg_atomic_read_u32(&header->state);
			continue;
		}

		if (pg_atomic_compare_exchange_u32(&header->state, &state, newState))
			break;
	}
	finish_spin_delay(&status);

	return state;
}

/*
 * This function finishes when page is enable to read or we managed to lock
 * the page list.
 */
static uint32
read_enabled_or_lock_page_list(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	uint32		state;
	SpinDelayStatus status;

	init_local_spin_delay(&status);
	state = pg_atomic_read_u32(&header->state);
	while (true)
	{
		uint32		newState;

		if (!O_PAGE_STATE_READ_IS_BLOCKED(state))
		{
			break;
		}
		else if (!(state & PAGE_STATE_LIST_LOCKED_FLAG))
		{
			newState = state | (PAGE_STATE_LIST_LOCKED_FLAG | PAGE_STATE_HAS_WAITERS_FLAG);
		}
		else
		{
			perform_spin_delay(&status);
			state = pg_atomic_read_u32(&header->state);
			continue;
		}

		if (pg_atomic_compare_exchange_u32(&header->state, &state, newState))
			break;
	}
	finish_spin_delay(&status);

	return state;
}

static uint32
lock_page_list(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	uint32		oldState;
	SpinDelayStatus status;

	init_local_spin_delay(&status);
	while (true)
	{
		oldState = pg_atomic_fetch_or_u32(&header->state, PAGE_STATE_LIST_LOCKED_FLAG);

		if (!(oldState & PAGE_STATE_LIST_LOCKED_FLAG))
			break;
		else
			perform_spin_delay(&status);
	}
	finish_spin_delay(&status);

	return oldState;
}

static uint32
dequeue_self(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	proclist_mutable_iter iter;
	uint32		mask;
	uint32		state;
	bool		found = false;

	(void) lock_page_list(blkno);

	proclist_foreach_modify(iter, &O_GET_IN_MEMORY_PAGEDESC(blkno)->waitersList, lwWaitLink)
	{
		if (iter.cur == MyProc->pgprocno)
		{
			proclist_delete(&O_GET_IN_MEMORY_PAGEDESC(blkno)->waitersList, iter.cur, lwWaitLink);
			found = true;
			break;
		}
	}

	mask = ~PAGE_STATE_LIST_LOCKED_FLAG;
	if (proclist_is_empty(&O_GET_IN_MEMORY_PAGEDESC(blkno)->waitersList))
		mask &= ~PAGE_STATE_HAS_WAITERS_FLAG;

	state = pg_atomic_fetch_and_u32(&header->state, mask);
	state &= mask;

	if (found)
	{
		MyProc->lwWaiting = false;
	}
	else
	{
		int			extraWaits = 0;

		/*
		 * Now wait for the scheduled wakeup, otherwise our ->lwWaiting would
		 * get reset at some inconvenient point later. Most of the time this
		 * will immediately return.
		 */
		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (!MyProc->lwWaiting)
				break;
			extraWaits++;
		}

		/*
		 * Fix the process wait semaphore's count for any absorbed wakeups.
		 */
		while (extraWaits-- > 0)
			PGSemaphoreUnlock(MyProc->sem);
	}

	return state;
}

/*
 * Place exclusive lock on the page.  Doesn't block readers before
 * page_block_reads() is called.
 */
void
lock_page(OInMemoryBlkno blkno)
{
	UsageCountMap *ucm = &(get_ppool_by_blkno(blkno)->ucm);
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	uint32		prevState;
	int			extraWaits = 0;

	Assert(get_my_locked_page_index(blkno) < 0);

	EA_LOCK_INC(blkno);

	page_inc_usage_count(ucm, blkno,
						 pg_atomic_read_u32(&header->usageCount), false);

	while (true)
	{
		prevState = lock_page_or_list(blkno);
		if (!O_PAGE_STATE_IS_LOCKED(prevState))
			break;

		proclist_push_tail(&O_GET_IN_MEMORY_PAGEDESC(blkno)->waitersList,
						   MyProc->pgprocno,
						   lwWaitLink);
		MyProc->lwWaiting = true;
		MyProc->lwWaitMode = LW_EXCLUSIVE;
		prevState = pg_atomic_fetch_and_u32(&header->state, ~PAGE_STATE_LIST_LOCKED_FLAG);
		if (!O_PAGE_STATE_IS_LOCKED(prevState))
		{
			prevState = pg_atomic_fetch_or_u32(&header->state, PAGE_STATE_LOCKED_FLAG);
			if (!O_PAGE_STATE_IS_LOCKED(prevState))
			{
				prevState = dequeue_self(blkno);
				break;
			}
		}

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (!MyProc->lwWaiting)
				break;
			extraWaits++;
		}

		pgstat_report_wait_end();
	}

	my_locked_page_add(blkno, prevState | PAGE_STATE_LOCKED_FLAG);

	/*
	 * Fix the process wait semaphore's count for any absorbed wakeups.
	 */
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(MyProc->sem);
}

void
page_wait_for_read_enable(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	uint32		prevState;
	int			extraWaits = 0;

	while (true)
	{
		prevState = read_enabled_or_lock_page_list(blkno);
		if (!(prevState & PAGE_STATE_NO_READ_FLAG))
			break;

		proclist_push_tail(&O_GET_IN_MEMORY_PAGEDESC(blkno)->waitersList,
						   MyProc->pgprocno,
						   lwWaitLink);
		MyProc->lwWaiting = true;
		MyProc->lwWaitMode = LW_SHARED;
		prevState = pg_atomic_fetch_and_u32(&header->state, ~PAGE_STATE_LIST_LOCKED_FLAG);

		if (!(prevState & PAGE_STATE_NO_READ_FLAG))
		{
			dequeue_self(blkno);
			return;
		}

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (!MyProc->lwWaiting)
				break;
			extraWaits++;
		}

		pgstat_report_wait_end();
	}

	/*
	 * Fix the process wait semaphore's count for any absorbed wakeups.
	 */
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(MyProc->sem);

	return;
}

static uint32
page_wait_for_changecount(OInMemoryBlkno blkno, uint32 state)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	uint32		curState;
	int			extraWaits = 0;

	while (true)
	{
		bool		exit_loop = false;

		curState = lock_page_list(blkno);
		if ((curState & PAGE_STATE_CHANGE_COUNT_MASK) !=
			(state & PAGE_STATE_CHANGE_COUNT_MASK))
		{
			curState = pg_atomic_fetch_and_u32(&header->state, ~PAGE_STATE_LIST_LOCKED_FLAG);
			return curState;
		}
		(void) pg_atomic_fetch_or_u32(&header->state, PAGE_STATE_HAS_WAITERS_FLAG);

		proclist_push_tail(&O_GET_IN_MEMORY_PAGEDESC(blkno)->waitersList,
						   MyProc->pgprocno,
						   lwWaitLink);
		MyProc->lwWaiting = true;
		MyProc->lwWaitMode = LW_SHARED;
		curState = pg_atomic_fetch_and_u32(&header->state, ~PAGE_STATE_LIST_LOCKED_FLAG);

		if ((curState & PAGE_STATE_CHANGE_COUNT_MASK) !=
			(state & PAGE_STATE_CHANGE_COUNT_MASK))
		{
			return dequeue_self(blkno);
		}

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (!MyProc->lwWaiting)
			{
				curState = pg_atomic_read_u32(&header->state);
				if ((curState & PAGE_STATE_CHANGE_COUNT_MASK) !=
					(state & PAGE_STATE_CHANGE_COUNT_MASK))
					exit_loop = true;
				break;
			}
			extraWaits++;
		}
		if (exit_loop)
			break;

		pgstat_report_wait_end();
	}

	/*
	 * Fix the process wait semaphore's count for any absorbed wakeups.
	 */
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(MyProc->sem);

	return curState;
}

bool
have_locked_pages(void)
{
	return (numberOfMyLockedPages > 0);
}

/* Wait for a change of the page and lock it. */
void
relock_page(OInMemoryBlkno blkno)
{
	UsageCountMap *ucm = &(get_ppool_by_blkno(blkno)->ucm);
	OrioleDBPageHeader *header;
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	uint32		state;

	state = my_locked_page_get_state(blkno);
	unlock_page(blkno);

	STOPEVENT(STOPEVENT_RELOCK_PAGE, NULL);

	header = (OrioleDBPageHeader *) p;
	page_inc_usage_count(ucm, blkno,
						 pg_atomic_read_u32(&header->usageCount), false);

	page_wait_for_changecount(blkno, state);
	lock_page(blkno);
}

/*
 * Try to lock the given page from concurrent changes.  Returns true on success.
 */
bool
try_lock_page(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	uint32		state;

	state = pg_atomic_fetch_or_u32(&(O_PAGE_HEADER(p)->state),
								   PAGE_STATE_LOCKED_FLAG);

	if (O_PAGE_STATE_IS_LOCKED(state))
		return false;

	EA_LOCK_INC(blkno);
	my_locked_page_add(blkno, state | PAGE_STATE_LOCKED_FLAG);
	return true;
}

/*
 * Declare newly created page as already locked by our process.
 */
void
delare_page_as_locked(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);

	my_locked_page_add(blkno, pg_atomic_read_u32(&(O_PAGE_HEADER(p)->state)));
}

/*
 * Check if page is locked.
 */
bool
page_is_locked(OInMemoryBlkno blkno)
{
	return (get_my_locked_page_index(blkno) >= 0);
}

/*
 * Block reads on locked page to prepare it for the modification.
 */
void
page_block_reads(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	uint32		state;
	int			i = get_my_locked_page_index(blkno);

	Assert((myLockedPages[i].state & PAGE_STATE_CHANGE_NON_WAITERS_MASK) ==
		   (pg_atomic_read_u32(&(O_PAGE_HEADER(p)->state)) & PAGE_STATE_CHANGE_NON_WAITERS_MASK));

	state = pg_atomic_fetch_or_u32(&(O_PAGE_HEADER(p)->state), PAGE_STATE_NO_READ_FLAG);
	Assert((state & PAGE_STATE_LOCKED_FLAG));
	myLockedPages[i].state = state | PAGE_STATE_NO_READ_FLAG;
}

static void
wakeup_waiters(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	proclist_head wakeup;
	proclist_mutable_iter iter;
	bool		wokeup_exclusive = false;
	uint32		mask;

	proclist_init(&wakeup);

	lock_page_list(blkno);

	proclist_foreach_modify(iter, &O_GET_IN_MEMORY_PAGEDESC(blkno)->waitersList, lwWaitLink)
	{
		PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

		if (wokeup_exclusive && waiter->lwWaitMode == LW_EXCLUSIVE)
			continue;

		proclist_delete(&O_GET_IN_MEMORY_PAGEDESC(blkno)->waitersList, iter.cur, lwWaitLink);
		proclist_push_tail(&wakeup, iter.cur, lwWaitLink);
		if (waiter->lwWaitMode == LW_EXCLUSIVE)
			wokeup_exclusive = true;
	}

	mask = ~PAGE_STATE_LIST_LOCKED_FLAG;
	if (proclist_is_empty(&O_GET_IN_MEMORY_PAGEDESC(blkno)->waitersList))
		mask &= ~PAGE_STATE_HAS_WAITERS_FLAG;
	pg_atomic_fetch_and_u32(&(O_PAGE_HEADER(p)->state), mask);

	/* Awaken any waiters I removed from the queue. */
	proclist_foreach_modify(iter, &wakeup, lwWaitLink)
	{
		PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

		proclist_delete(&wakeup, iter.cur, lwWaitLink);

		/*
		 * Guarantee that lwWaiting being unset only becomes visible once the
		 * unlink from the link has completed. Otherwise the target backend
		 * could be woken up for other reason and enqueue for a new lock - if
		 * that happens before the list unlink happens, the list would end up
		 * being corrupted.
		 *
		 * The barrier pairs with the LWLockWaitListLock() when enqueuing for
		 * another lock.
		 */
		pg_write_barrier();
		waiter->lwWaiting = false;
		PGSemaphoreUnlock(waiter->sem);
	}
}

/*
 * Unlock the page.  Page should be already locked.
 */
void
unlock_page(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	uint32		state;

#ifdef CHECK_PAGE_STRUCT
	if (O_GET_IN_MEMORY_PAGEDESC(blkno)->type != oIndexInvalid)
		o_check_page_struct(NULL, p);
#endif

#ifdef CHECK_PAGE_STATS

	/*
	 * XXX: index_oids_get_btree_descr() might expand a hash table under
	 * critical section.
	 */
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);

	if (O_PAGE_IS(p, LEAF) && page_desc->type != oIndexInvalid)
	{
		ORelOids	oids = page_desc->oids;

		if (!IS_SYS_TREE_OIDS(oids))
		{
			BTreeDescr *desc;

			desc = index_oids_get_btree_descr(oids, page_desc->type);

			if (desc)
				o_check_btree_page_statistics(desc, p);
		}
	}
#endif

	state = my_locked_page_del(blkno);

#ifdef USE_ASSERT_CHECKING
	if (!O_PAGE_IS(p, LEAF) && OidIsValid(O_GET_IN_MEMORY_PAGEDESC(blkno)->oids.reloid))
	{
		int			on_disk = 0;
		BTreePageItemLocator loc;

		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			if (DOWNLINK_IS_ON_DISK(tuphdr->downlink))
				on_disk++;
		}
		Assert(on_disk == PAGE_GET_N_ONDISK(p));
	}
#endif

	VALGRIND_CHECK_MEM_IS_DEFINED(O_GET_IN_MEMORY_PAGE(blkno), ORIOLEDB_BLCKSZ);

	Assert((state & PAGE_STATE_CHANGE_NON_WAITERS_MASK) == (pg_atomic_read_u32(&(O_PAGE_HEADER(p)->state)) & PAGE_STATE_CHANGE_NON_WAITERS_MASK));

	if (state & PAGE_STATE_NO_READ_FLAG)
	{
		state = pg_atomic_add_fetch_u32(&(O_PAGE_HEADER(p)->state),
										PAGE_STATE_CHANGE_COUNT_ONE - (state & (PAGE_STATE_LOCKED_FLAG | PAGE_STATE_NO_READ_FLAG)));
	}
	else
	{
		state = pg_atomic_fetch_and_u32(&(O_PAGE_HEADER(p)->state), ~PAGE_STATE_LOCKED_FLAG);
		state &= ~PAGE_STATE_LOCKED_FLAG;
	}

	if (state & PAGE_STATE_HAS_WAITERS_FLAG)
		wakeup_waiters(blkno);
}

/*
 * Release all previously acquired page locks one-by-one.
 */
void
release_all_page_locks(void)
{
	pg_write_barrier();

	while (numberOfMyLockedPages > 0)
		unlock_page(myLockedPages[0].blkno);
}

/*
 * Register in-progress split.  This split will be marked as incomplete on
 * errer cleanup unless it's unregistered before.
 *
 * Must be called within critical section.
 */
void
btree_register_inprogress_split(OInMemoryBlkno left_blkno)
{
#ifdef USE_ASSERT_CHECKING
	int			i;

	for (i = 0; i < numberOfMyInProgressSplitPages; i++)
		Assert(myInProgressSplitPages[i] != left_blkno);
#endif
	Assert(CritSectionCount > 0);
	Assert((numberOfMyInProgressSplitPages + 1) <= sizeof(myInProgressSplitPages) / sizeof(myInProgressSplitPages[0]));
	myInProgressSplitPages[numberOfMyInProgressSplitPages++] = left_blkno;
}

/*
 * Unregister in-progress split.
 *
 * Must be calles within critical section.
 */
void
btree_unregister_inprogress_split(OInMemoryBlkno left_blkno)
{
	int			i;

	Assert(CritSectionCount > 0);
	Assert(numberOfMyInProgressSplitPages > 0);
	for (i = 0; i < numberOfMyInProgressSplitPages; i++)
	{
		if (myInProgressSplitPages[i] == left_blkno)
		{
			numberOfMyInProgressSplitPages--;
			myInProgressSplitPages[i] = myInProgressSplitPages[numberOfMyInProgressSplitPages];
			return;
		}
	}
	Assert(false);
}

/*
 * Marks all in-progress splits as incomplete.
 */
void
btree_mark_incomplete_splits(void)
{
	int			i;

	for (i = 0; i < numberOfMyInProgressSplitPages; i++)
		btree_split_mark_finished(myInProgressSplitPages[i], true, false);
	numberOfMyInProgressSplitPages = 0;
}

/*
 * Marks the split as finished.
 *
 * It sets O_BTREE_FLAG_BROKEN_SPLIT if success = false or removes rightlink
 * on the left page.
 *
 * It does not call modify_page if use_lock = false.
 */
void
btree_split_mark_finished(OInMemoryBlkno left_blkno, bool use_lock, bool success)
{
	BTreePageHeader *header;

	if (use_lock)
	{
		lock_page(left_blkno);
		page_block_reads(left_blkno);
	}

	header = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(left_blkno);

	Assert(RightLinkIsValid(header->rightLink));
	Assert(!use_lock || !O_PAGE_IS(O_GET_IN_MEMORY_PAGE(left_blkno), BROKEN_SPLIT));

	if (success)
	{
		header->flags &= ~O_BTREE_FLAG_BROKEN_SPLIT;
		header->rightLink = InvalidRightLink;
	}
	else
	{
		header->flags |= O_BTREE_FLAG_BROKEN_SPLIT;
	}

	if (use_lock)
		unlock_page(left_blkno);
}

#ifdef CHECK_PAGE_STRUCT
/*
 * Check if page has a consistent structure.
 */
void
o_check_page_struct(BTreeDescr *desc, Page p)
{
	BTreePageHeader *header = (BTreePageHeader *) p;
	int			i,
				j,
				itemsCount;
	LocationIndex endLocation,
				chunkSize;

	Assert(header->dataSize <= ORIOLEDB_BLCKSZ);
	Assert(header->hikeysEnd <= header->dataSize);

	for (i = 0; i < header->chunksCount; i++)
	{
		BTreePageChunkDesc *chunk = &header->chunkDesc[i];
		BTreePageChunk *chunkData;

		if (i > 0)
		{
			BTreePageChunkDesc *prevChunk = &header->chunkDesc[i - 1] PG_USED_FOR_ASSERTS_ONLY;

			Assert(chunk->shortLocation >= prevChunk->shortLocation);
			Assert(chunk->offset >= prevChunk->offset);
			Assert(chunk->hikeyShortLocation > prevChunk->hikeyShortLocation);
			Assert(SHORT_GET_LOCATION(chunk->hikeyShortLocation) <= header->hikeysEnd);
			Assert(SHORT_GET_LOCATION(chunk->shortLocation) <= header->dataSize);
			Assert(chunk->offset <= header->itemsCount);
		}
		else
		{
			Assert(SHORT_GET_LOCATION(chunk->shortLocation) == header->hikeysEnd || SHORT_GET_LOCATION(chunk->shortLocation) == BTREE_PAGE_HIKEYS_END(NULL, p));
			Assert(chunk->offset == 0);
			Assert(SHORT_GET_LOCATION(chunk->hikeyShortLocation) == MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * header->chunksCount));
		}

		if (i == header->chunksCount - 1)
		{
			if (!O_PAGE_IS(p, RIGHTMOST))
				Assert(SHORT_GET_LOCATION(chunk->hikeyShortLocation) < header->hikeysEnd);
			itemsCount = header->itemsCount - chunk->offset;
			endLocation = header->dataSize;
		}
		else
		{
			Assert(header->chunkDesc[i + 1].offset <= header->itemsCount);
			Assert(header->chunkDesc[i + 1].offset >= chunk->offset);
			itemsCount = header->chunkDesc[i + 1].offset - chunk->offset;
			endLocation = SHORT_GET_LOCATION(header->chunkDesc[i + 1].shortLocation);
			Assert(endLocation <= header->dataSize);
		}

		chunkData = (BTreePageChunk *) ((Pointer) p + SHORT_GET_LOCATION(chunk->shortLocation));
		chunkSize = endLocation - SHORT_GET_LOCATION(chunk->shortLocation);
		Assert(MAXALIGN(sizeof(LocationIndex) * itemsCount) <= chunkSize);

		for (j = 0; j < itemsCount; j++)
		{
			Assert(ITEM_GET_OFFSET(chunkData->items[j]) >= MAXALIGN(sizeof(LocationIndex) * itemsCount));
			Assert(ITEM_GET_OFFSET(chunkData->items[j]) <= chunkSize);
			if (j > 0)
				Assert(ITEM_GET_OFFSET(chunkData->items[j]) >= ITEM_GET_OFFSET(chunkData->items[j - 1]));
			if (j < itemsCount - 1 && O_PAGE_IS(p, LEAF) && ITEM_GET_FLAGS(chunkData->items[j]) == 0)
				Assert(ITEM_GET_OFFSET(chunkData->items[j]) < ITEM_GET_OFFSET(chunkData->items[j + 1]));
			if (desc)
			{
				OTuple		tuple;
				int			len;

				tuple.formatFlags = ITEM_GET_FLAGS(chunkData->items[j]);
				if (O_PAGE_IS(p, LEAF))
				{
					tuple.data = (Pointer) chunkData + ITEM_GET_OFFSET(chunkData->items[j]) + BTreeLeafTuphdrSize;
					len = BTreeLeafTuphdrSize + o_btree_len(desc, tuple, OTupleLength);
				}
				else
				{
					if (i == 0 && j == 0)
					{
						len = BTreeNonLeafTuphdrSize;
					}
					else
					{
						tuple.data = (Pointer) chunkData + ITEM_GET_OFFSET(chunkData->items[j]) + BTreeNonLeafTuphdrSize;
						len = BTreeNonLeafTuphdrSize + o_btree_len(desc, tuple, OKeyLength);
					}
				}

				if (j < itemsCount - 1)
					Assert(ITEM_GET_OFFSET(chunkData->items[j]) + len <= ITEM_GET_OFFSET(chunkData->items[j + 1]));
				else
					Assert(ITEM_GET_OFFSET(chunkData->items[j]) + len <= chunkSize);

			}
		}
	}

}
#endif

#ifdef CHECK_PAGE_STATS

/*
 * Check if precalculated number of vacated bytes for leaf pages and number
 * of disk downlinks for non-leaf pages is correct.
 */
static void
o_check_btree_page_statistics(BTreeDescr *desc, Pointer p)
{
	if (O_PAGE_IS(p, LEAF))
	{
		int			nVacatedBytes;

		nVacatedBytes = PAGE_GET_N_VACATED(p);
		o_btree_page_calculate_statistics(desc, p);

		Assert(nVacatedBytes == PAGE_GET_N_VACATED(p));
	}
	else
	{
		int			nDiskDownlinks;

		nDiskDownlinks = PAGE_GET_N_ONDISK(p);
		o_btree_page_calculate_statistics(desc, p);

		Assert(nDiskDownlinks == PAGE_GET_N_ONDISK(p));
	}
}
#endif
