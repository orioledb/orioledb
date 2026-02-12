/*-------------------------------------------------------------------------
 *
 * page_state.c
 *		OrioleDB B-tree page locking, waiting, reading etc.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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
#include "storage/itemptr.h"
#include "tableam/descr.h"
#include "tableam/key_range.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/dsa.h"
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
 * Enable this to recheck page stats on every unlock.
 */
/* #define CHECK_PAGE_STATS */

typedef struct
{
	OInMemoryBlkno blkno;
	uint64		state;
} MyLockedPage;

static MyLockedPage myLockedPages[MAX_PAGES_PER_PROCESS];
static OInMemoryBlkno myInProgressSplitPages[ORIOLEDB_MAX_DEPTH * 2];
static int	numberOfMyLockedPages = 0;
static int	numberOfMyInProgressSplitPages = 0;

OPageWaiterShmemState *lockerStates = NULL;

#ifdef CHECK_PAGE_STATS
static void o_check_btree_page_statistics(BTreeDescr *desc, Pointer p);
#endif

Size
page_state_shmem_needs(void)
{
	return CACHELINEALIGN(sizeof(OPageWaiterShmemState) * max_procs);
}

void
page_state_shmem_init(Pointer buf, bool found)
{
	Pointer		ptr = buf;

	lockerStates = (OPageWaiterShmemState *) ptr;
}

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
my_locked_page_add(OInMemoryBlkno blkno, uint64 state)
{
	Assert(get_my_locked_page_index(blkno) < 0);
	Assert(numberOfMyLockedPages < MAX_PAGES_PER_PROCESS);

	Assert(pg_atomic_read_u64(&((OrioleDBPageHeader *) O_GET_IN_MEMORY_PAGE(blkno))->state) & PAGE_STATE_LOCKED_FLAG);
	myLockedPages[numberOfMyLockedPages].blkno = blkno;
	myLockedPages[numberOfMyLockedPages++].state = state;
}

static uint64
my_locked_page_del(OInMemoryBlkno blkno)
{
	int			i = get_my_locked_page_index(blkno);
	uint64		state;

	Assert(i >= 0 && i < MAX_PAGES_PER_PROCESS);
	state = myLockedPages[i].state;
	myLockedPages[i] = myLockedPages[--numberOfMyLockedPages];

	return state;
}

static uint64
my_locked_page_get_state(OInMemoryBlkno blkno)
{
	int			i = get_my_locked_page_index(blkno);

	Assert(i >= 0 && i < MAX_PAGES_PER_PROCESS);
	return myLockedPages[i].state;
}

static uint64
lock_page_or_queue(OInMemoryBlkno blkno, uint32 pgprocnum)
{
	UsageCountMap *ucm = &(get_ppool_by_blkno(blkno)->ucm);
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	uint64		state;
	OPageWaiterShmemState *lockerState = &lockerStates[pgprocnum];
	bool		ucmUpdateTried = false;

	Assert(pgprocnum < max_procs);

	state = pg_atomic_read_u64(&header->state);
	while (true)
	{
		uint64		newState;

		if (!O_PAGE_STATE_IS_LOCKED(state))
		{
			newState = O_PAGE_STATE_LOCK(state);
		}
		else
		{
			Assert((state & PAGE_STATE_LIST_TAIL_MASK) != pgprocnum);
			lockerState->status = OPageWaitExclusive;
			lockerState->next = (state & PAGE_STATE_LIST_TAIL_MASK);
			newState = state & (~PAGE_STATE_LIST_TAIL_MASK);
			newState |= pgprocnum;
		}

		if (!ucmUpdateTried)
		{
			newState = ucm_update_state(ucm, blkno, newState);
			ucmUpdateTried = true;
		}

		if (pg_atomic_compare_exchange_u64(&header->state, &state, newState))
		{
			ucm_after_update_state(ucm, blkno, state, newState);
			break;
		}
	}

	return state;
}

typedef struct
{
	char		img[8192];
	PartialPageState partial;
	bool		load;
} PageImg;

typedef enum
{
	LockPageResultLocked = 1,
	LockPageResultQueued = 2,
	LockPageResultSplitDetected = 3
} LockPageResult;

static LockPageResult
lock_page_or_queue_or_split_detect(BTreeDescr *desc, OInMemoryBlkno *blkno,
								   uint32 *pageChangeCount, uint32 pgprocnum,
								   PageImg *img, OTupleXactInfo xactInfo,
								   OTuple tuple, uint64 *prevState,
								   bool *keySerialized)
{
	UsageCountMap *ucm = &(get_ppool_by_blkno(*blkno)->ucm);
	Page		p = O_GET_IN_MEMORY_PAGE(*blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	OrioleDBPageHeader *imgHeader = (OrioleDBPageHeader *) img->img;
	uint64		state;
	OPageWaiterShmemState *lockerState = &lockerStates[pgprocnum];
	bool		ucmUpdateTried = false;

	Assert(pgprocnum < max_procs);

	state = pg_atomic_read_u64(&header->state);
	while (true)
	{
		uint64		newState;

		if (!O_PAGE_STATE_IS_LOCKED(state))
		{
			newState = O_PAGE_STATE_LOCK(state);
		}
		else
		{
			if (!img->load ||
				(state & PAGE_STATE_CHANGE_COUNT_MASK) != (pg_atomic_read_u64(&imgHeader->state) & PAGE_STATE_CHANGE_COUNT_MASK))
			{
				if (!o_btree_read_page(desc, *blkno, *pageChangeCount, img->img,
									   COMMITSEQNO_INPROGRESS, NULL, BTreeKeyNone, NULL,
									   &img->partial, true, NULL, NULL))
				{
					return LockPageResultSplitDetected;
				}
				img->load = true;

				if (!O_PAGE_IS(img->img, RIGHTMOST))
				{
					OTuple		hikey;

					BTREE_PAGE_GET_HIKEY(hikey, img->img);

					if (o_btree_cmp(desc, &tuple, BTreeKeyLeafTuple,
									&hikey, BTreeKeyNonLeafKey) >= 0)
					{
						uint64		rightlink = BTREE_PAGE_GET_RIGHTLINK(img->img);

						if (OInMemoryBlknoIsValid(RIGHTLINK_GET_BLKNO(rightlink)))
						{
							*blkno = RIGHTLINK_GET_BLKNO(rightlink);
							*pageChangeCount = RIGHTLINK_GET_CHANGECOUNT(rightlink);
							p = O_GET_IN_MEMORY_PAGE(*blkno);
							header = (OrioleDBPageHeader *) p;
							Assert(get_my_locked_page_index(*blkno) < 0);
							state = pg_atomic_read_u64(&header->state);
							continue;
						}
						else
						{
							return LockPageResultSplitDetected;
						}
					}
				}
			}

			if (!*keySerialized)
			{
				BTreeLeafTuphdr tuphdr;
				int			tuplen;

				tuphdr.deleted = false;
				tuphdr.undoLocation = InvalidUndoLocation;
				tuphdr.formatFlags = 0;
				tuphdr.chainHasLocks = false;
				tuphdr.xactInfo = xactInfo;

				lockerState->reloids = desc->oids;
				if (desc->undoType != UndoLogNone)
					lockerState->reservedUndoSize = get_reserved_undo_size(desc->undoType);
				else
					lockerState->reservedUndoSize = 0;
				lockerState->tupleFlags = tuple.formatFlags;
				memcpy(lockerState->tupleData.fixedData,
					   &tuphdr,
					   BTreeLeafTuphdrSize);
				tuplen = o_btree_len(desc, tuple, OTupleLength);
				memcpy(&lockerState->tupleData.fixedData[BTreeLeafTuphdrSize],
					   tuple.data,
					   tuplen);
				if (tuplen != MAXALIGN(tuplen))
					memset(&lockerState->tupleData.fixedData[BTreeLeafTuphdrSize + tuplen],
						   0, MAXALIGN(tuplen) - tuplen);
				*keySerialized = true;
			}

			Assert((state & PAGE_STATE_LIST_TAIL_MASK) != pgprocnum);
			lockerState->status = OPageWaitInsert;
			lockerState->undoLocation = InvalidUndoLocation;
			lockerState->pageChangeCount = *pageChangeCount;
			Assert(!lockerState->inserted);
			lockerState->next = (state & PAGE_STATE_LIST_TAIL_MASK);
			newState = state & (~PAGE_STATE_LIST_TAIL_MASK);
			newState |= pgprocnum;
		}

		if (!ucmUpdateTried)
		{
			newState = ucm_update_state(ucm, *blkno, newState);
			ucmUpdateTried = true;
		}

		if (pg_atomic_compare_exchange_u64(&header->state, &state, newState))
		{
			ucm_after_update_state(ucm, *blkno, state, newState);
			break;
		}
	}

	*prevState = state;

	if (!O_PAGE_STATE_IS_LOCKED(state))
		return LockPageResultLocked;
	else
		return LockPageResultQueued;
}

/*
 * This function finishes when page is enable to read or we managed to lock
 * the page list.
 */
static uint64
read_enabled_or_queue(OInMemoryBlkno blkno, uint32 pgprocnum)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	uint64		state;
	OPageWaiterShmemState *lockerState = &lockerStates[pgprocnum];

	state = pg_atomic_read_u64(&header->state);
	while (true)
	{
		uint64		newState;

		if (!O_PAGE_STATE_READ_IS_BLOCKED(state))
		{
			break;
		}
		else
		{
			Assert((state & PAGE_STATE_LIST_TAIL_MASK) != pgprocnum);
			lockerState->status = OPageWaitNonExclusive;
			lockerState->next = (state & PAGE_STATE_LIST_TAIL_MASK);
			newState = state & (~PAGE_STATE_LIST_TAIL_MASK);
			newState |= pgprocnum;
		}

		if (pg_atomic_compare_exchange_u64(&header->state, &state, newState))
			break;
	}

	return state;
}

static uint64
state_changed_or_queue(OInMemoryBlkno blkno, uint32 pgprocnum,
					   uint64 oldState)
{
	UsageCountMap *ucm = &(get_ppool_by_blkno(blkno)->ucm);
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	uint64		state;
	OPageWaiterShmemState *lockerState = &lockerStates[pgprocnum];
	bool		ucmUpdateTried = false;

	state = pg_atomic_read_u64(&header->state);
	while (true)
	{
		uint64		newState;

		if ((state & PAGE_STATE_CHANGE_COUNT_MASK) !=
			(oldState & PAGE_STATE_CHANGE_COUNT_MASK))
		{
			break;
		}
		else
		{
			Assert((state & PAGE_STATE_LIST_TAIL_MASK) != pgprocnum);
			lockerState->status = OPageWaitNonExclusive;
			lockerState->next = (state & PAGE_STATE_LIST_TAIL_MASK);
			newState = state & (~PAGE_STATE_LIST_TAIL_MASK);
			newState |= pgprocnum;
		}

		if (!ucmUpdateTried)
		{
			newState = ucm_update_state(ucm, blkno, newState);
			ucmUpdateTried = true;
		}

		if (pg_atomic_compare_exchange_u64(&header->state, &state, newState))
		{
			ucm_after_update_state(ucm, blkno, state, newState);
			break;
		}
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
	OPageWaiterShmemState *lockerState = &lockerStates[MYPROCNUMBER];
	uint64		prevState;
	int			extraWaits = 0;

	Assert(get_my_locked_page_index(blkno) < 0);

	EA_LOCK_INC(blkno);

	while (true)
	{
		prevState = lock_page_or_queue(blkno, MYPROCNUMBER);

		if (!O_PAGE_STATE_IS_LOCKED(prevState))
			break;

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (lockerState->status == OPageWaitWakeUp)
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

/*
 * Place exclusive lock on the page.  Doesn't block readers before
 * page_block_reads() is called.
 */
OLockPageWithTupleResult
lock_page_with_tuple(BTreeDescr *desc,
					 OInMemoryBlkno *blkno, uint32 *pageChangeCount,
					 OTupleXactInfo xactInfo, OTuple tuple)
{
	uint64		prevState;
	int			extraWaits = 0;
	OPageWaiterShmemState *lockerState = &lockerStates[MYPROCNUMBER];
	bool		keySerialized = false;
	PageImg		img;


	img.load = false;
	Assert(get_my_locked_page_index(*blkno) < 0);

	while (true)
	{
		LockPageResult lockResult;

		lockResult = lock_page_or_queue_or_split_detect(desc, blkno,
														pageChangeCount,
														MYPROCNUMBER,
														&img, xactInfo,
														tuple, &prevState,
														&keySerialized);

		if (lockResult == LockPageResultLocked)
		{
			break;
		}
		else if (lockResult == LockPageResultSplitDetected)
		{
			return OLockPageWithTupleResultRefindNeeded;
		}
		Assert(lockResult == LockPageResultQueued);

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (lockerState->status == OPageWaitWakeUp)
				break;
			extraWaits++;
		}
		pgstat_report_wait_end();

		/*
		 * Fix the process wait semaphore's count for any absorbed wakeups.
		 */
		while (extraWaits-- > 0)
			PGSemaphoreUnlock(MyProc->sem);

		if (lockerState->inserted)
		{
			UndoLogType undoType = desc->undoType;

			Assert(keySerialized);
			lockerState->inserted = false;
			if (undoType != UndoLogNone)
			{
				giveup_reserved_undo_size(undoType);
				if (UndoLocationIsValid(lockerState->undoLocation) &&
					!UndoLocationIsValid(curRetainUndoLocations[undoType]))
					curRetainUndoLocations[undoType] = lockerState->undoLocation;
			}

			return OLockPageWithTupleResultInserted;
		}
	}

	EA_LOCK_INC(*blkno);

	my_locked_page_add(*blkno, prevState | PAGE_STATE_LOCKED_FLAG);

	return OLockPageWithTupleResultLocked;
}

void
page_wait_for_read_enable(OInMemoryBlkno blkno)
{
	uint32		prevState;
	int			extraWaits = 0;
	OPageWaiterShmemState *lockerState = &lockerStates[MYPROCNUMBER];

	while (true)
	{
		prevState = read_enabled_or_queue(blkno, MYPROCNUMBER);

		if (!(prevState & PAGE_STATE_NO_READ_FLAG))
			break;

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (lockerState->status == OPageWaitWakeUp)
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
	uint64		curState;
	int			extraWaits = 0;
	OPageWaiterShmemState *lockerState = &lockerStates[MYPROCNUMBER];

	while (true)
	{
		bool		exit_loop = false;

		curState = state_changed_or_queue(blkno, MYPROCNUMBER, state);
		if ((curState & PAGE_STATE_CHANGE_COUNT_MASK) !=
			(state & PAGE_STATE_CHANGE_COUNT_MASK))
		{
			return curState;
		}

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (lockerState->status == OPageWaitWakeUp)
			{
				curState = pg_atomic_read_u64(&header->state);
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
	uint64		state;

	state = my_locked_page_get_state(blkno);
	unlock_page(blkno);

	STOPEVENT(STOPEVENT_RELOCK_PAGE, NULL);

	page_wait_for_changecount(blkno, state);
	lock_page(blkno);
}

/*
 * Try to lock the given page from concurrent changes.  Returns true on success.
 */
bool
try_lock_page(OInMemoryBlkno blkno)
{
	UsageCountMap *ucm = &(get_ppool_by_blkno(blkno)->ucm);
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	uint64		state;

	state = pg_atomic_fetch_or_u64(&(O_PAGE_HEADER(p)->state),
								   PAGE_STATE_LOCKED_FLAG);

	if (O_PAGE_STATE_IS_LOCKED(state))
		return false;

	EA_LOCK_INC(blkno);
	my_locked_page_add(blkno, state | PAGE_STATE_LOCKED_FLAG);
	page_inc_usage_count(ucm, blkno);

	return true;
}

/*
 * Declare newly created page as already locked by our process.
 */
void
delare_page_as_locked(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);

	my_locked_page_add(blkno, pg_atomic_read_u64(&(O_PAGE_HEADER(p)->state)));
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
	uint64		state;
	int			i = get_my_locked_page_index(blkno);

	Assert((myLockedPages[i].state & PAGE_STATE_CHANGE_NON_WAITERS_MASK) ==
		   (pg_atomic_read_u64(&(O_PAGE_HEADER(p)->state)) & PAGE_STATE_CHANGE_NON_WAITERS_MASK));

	state = pg_atomic_fetch_or_u64(&(O_PAGE_HEADER(p)->state), PAGE_STATE_NO_READ_FLAG);
	Assert((state & PAGE_STATE_LOCKED_FLAG));
	myLockedPages[i].state = state | PAGE_STATE_NO_READ_FLAG;
}

int
get_waiters_with_tuples(BTreeDescr *desc,
						OInMemoryBlkno blkno,
						int result[BTREE_PAGE_MAX_SPLIT_ITEMS])
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	uint32		pgprocnum;
	int			count = 0;

	pgprocnum = pg_atomic_read_u64(&(O_PAGE_HEADER(p)->state)) & PAGE_STATE_LIST_TAIL_MASK;

	while (pgprocnum != PAGE_STATE_INVALID_PROCNO)
	{
		OPageWaiterShmemState *lockerState = &lockerStates[pgprocnum];

		if (lockerState->status == OPageWaitInsert &&
			lockerState->pageChangeCount == O_PAGE_HEADER(p)->pageChangeCount &&
			ORelOidsIsEqual(desc->oids, lockerState->reloids))
		{
			result[count++] = pgprocnum;
			if (count >= BTREE_PAGE_MAX_SPLIT_ITEMS)
			{
				Assert(count == BTREE_PAGE_MAX_SPLIT_ITEMS);
				break;
			}
		}

		pgprocnum = lockerState->next;
	}

	return count;
}

void
mark_waiter_tuples_inserted(int procnums[BTREE_PAGE_MAX_SPLIT_ITEMS],
							int count)
{
	int			i;

	Assert(count > 0);

	for (i = 0; i < count; i++)
		lockerStates[procnums[i]].inserted = true;

}

/*
 * Check page before unlocking.
 */
static void
unlock_check_page(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);

#ifdef CHECK_PAGE_STRUCT
	if (O_GET_IN_MEMORY_PAGEDESC(blkno)->type != oIndexInvalid)
		o_check_page_struct(NULL, p);
#else
	if (O_GET_IN_MEMORY_PAGEDESC(blkno)->type != oIndexInvalid)
	{
		BTreePageHeader *header = (BTreePageHeader *) p;
		BTreePageChunkDesc *lastChunk = &header->chunkDesc[header->chunksCount - 1];

		if (SHORT_GET_LOCATION(lastChunk->shortLocation) > header->dataSize ||
			header->dataSize > ORIOLEDB_BLCKSZ)
			elog(PANIC, "broken page: (blkno: %u, p: %p, lastChunk: %u, dataSize: %u)",
				 blkno, p, SHORT_GET_LOCATION(lastChunk->shortLocation),
				 header->dataSize);
	}
#endif

#ifdef CHECK_PAGE_STATS
	{
		/*
		 * XXX: index_oids_get_btree_descr() might expand a hash table under
		 * critical section.
		 */
		OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);

		if (O_PAGE_IS(p, LEAF) && page_desc->type != oIndexInvalid)
		{
			ORelOids	oids = page_desc->oids;
			BTreeDescr *desc;

			if (!IS_SYS_TREE_OIDS(oids))
				desc = index_oids_get_btree_descr(oids, page_desc->type);
			else
				desc = get_sys_tree_no_init(oids.reloid);
			if (desc)
				o_check_btree_page_statistics(desc, p);
		}
	}
#endif

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
}

/*
 * unlock_page_internal -- release a previously locked in‑memory page and wake
 *						   any backends that can now proceed.
 *
 * The waiters are stored in a lock‑less, singly‑linked list.  The tail
 * (newest waiter) PGPROC number is packed into the low bits of the 64-bit
 * page‑state word.  A successful unlock therefore needs to:
 *	 1. Walk that list;
 *	 2. Move every suitable waiter (see `shouldWake`) and at most one
 *		exclusive waiter to a private wake list;
 *	 3. Patch the shared list so that the removed waiters vanish from it;
 *	 4. Publish a new page‑state word with the updated tail via atomic CAS;
 *	 5. If the CAS fails, process the newly added waiters (if any) and retry;
 *	 6. Finally, wake up all backends we collected on our private list.
 *
 * The two auxiliary variables `prevTail` and `prevTailPatch` are the key to
 * the logic: if we fail the CAS, the list may already contain our previous
 * patch (i.e. `prevTail->next` now points somewhere else).  We detect that
 * and re‑apply the patch in the next iteration instead of trying to start
 * from scratch (the latter is not possible, because we might already have
 * modified the list).
 */
static void
unlock_page_internal(OInMemoryBlkno blkno, bool split)
{
	Page		page = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *hdr = (OrioleDBPageHeader *) page;

	/* Head of our private stack of waiters to wake once the page is unlocked */
	uint32		wakeListHead = PAGE_STATE_INVALID_PROCNO;

	/* Bookkeeping needed when the CAS fails and we must retry */
	uint32		prevTail = PAGE_STATE_INVALID_PROCNO;
	uint32		prevTailPatch = PAGE_STATE_INVALID_PROCNO;

	/* We may wake **one** exclusive waiter per unlock attempt */
	bool		exclusiveAlreadyWoken = false;
	uint64		state;

	unlock_check_page(blkno);

	state = pg_atomic_read_u64(&hdr->state);

	for (;;)
	{
		/* Snapshot the tail encoded in the state word */
		uint32		tail = state & PAGE_STATE_LIST_TAIL_MASK;
		uint32		cur = tail;
		uint32		prev = PAGE_STATE_INVALID_PROCNO;
		uint64		newState;

		uint32		newTail = tail; /* will become the new list tail */

		/* Remember the first exclusive waiter we may decide to wake */
		uint32		exclusive = PAGE_STATE_INVALID_PROCNO;
		uint32		exclusivePrev = PAGE_STATE_INVALID_PROCNO;

		/* --------------------------------------------------------------
		 * 1. Walk the waiter list, unlinking suitable lockers on the fly
		 * --------------------------------------------------------------*/
		while (cur != prevTail) /* stop before the node we patched during the
								 * previous (failed) iteration */
		{
			OPageWaiterShmemState *lock = &lockerStates[cur];

			bool		shouldWake =
				lock->inserted ||
				lock->status == OPageWaitNonExclusive ||
				(split && lock->status == OPageWaitInsert);

			if (shouldWake)
			{
				uint32		next = lock->next;

				/* Unlink waiter from shared waiter list */
				if (prev == PAGE_STATE_INVALID_PROCNO)
					newTail = next; /* removed the first element */
				else
					lockerStates[prev].next = next;

				/* Push waiter onto our private wake list */
				lock->next = wakeListHead;
				wakeListHead = cur;

				cur = next;
				continue;		/* stay on the same `prev` */
			}

			/* Remember the first (oldest) exclusive waiter */
			if (!exclusiveAlreadyWoken && exclusive == PAGE_STATE_INVALID_PROCNO)
			{
				exclusive = cur;
				exclusivePrev = prev;
			}

			prev = cur;
			cur = lock->next;
		}

		/* ----------------------------------------------------------------
		 * 2. Optionally move the first exclusive waiter to the wake list
		 * ----------------------------------------------------------------*/
		if (exclusive != PAGE_STATE_INVALID_PROCNO && !exclusiveAlreadyWoken)
		{
			OPageWaiterShmemState *lock = &lockerStates[exclusive];

			exclusiveAlreadyWoken = true;

			if (exclusivePrev == PAGE_STATE_INVALID_PROCNO)
				newTail = lock->next;	/* exclusive was the first node */
			else
				lockerStates[exclusivePrev].next = lock->next;

			/* push to wake list */
			lock->next = wakeListHead;
			wakeListHead = exclusive;

			if (prev == exclusive)
				prev = exclusivePrev;
		}

		/* ----------------------------------------------------------------
		 * 3. Re‑apply the patch from the previous failed CAS attempt
		 * ----------------------------------------------------------------*/
		if (prevTail != prevTailPatch)
		{
			Assert(prevTail != PAGE_STATE_INVALID_PROCNO);

			if (prev == PAGE_STATE_INVALID_PROCNO)
				newTail = prevTailPatch;	/* new head is different */
			else
			{
				Assert(prev != prevTailPatch);
				lockerStates[prev].next = prevTailPatch;
			}
		}

		/* ----------------------------------------------------------------
		 * 4. Compose and try to publish the new page‑state word
		 * ----------------------------------------------------------------*/
		newState = state &
			~(PAGE_STATE_LIST_TAIL_MASK |
			  PAGE_STATE_LOCKED_FLAG |
			  PAGE_STATE_NO_READ_FLAG);

		/* Bump change‑counter if reads had been blocked */
		if (O_PAGE_STATE_READ_IS_BLOCKED(state))
		{
			uint64		changeCount = (newState & PAGE_STATE_CHANGE_COUNT_MASK);

			newState &= ~PAGE_STATE_CHANGE_COUNT_MASK;
			changeCount += PAGE_STATE_CHANGE_COUNT_ONE;
			changeCount &= PAGE_STATE_CHANGE_COUNT_MASK;
			newState |= changeCount;
		}

		newState |= newTail;

		if (pg_atomic_compare_exchange_u64(&hdr->state, &state, newState))
			break;				/* Success!  Exit retry loop */

		/* ----------------------------------------------------------------
		 * 5. CAS failed – remember what we did and retry
		 * ----------------------------------------------------------------*/
		prevTail = tail;
		prevTailPatch = newTail;
		/* `state` now holds the value returned by the failed CAS */
	}

	/* Cleanup the local list of locked pages */
	my_locked_page_del(blkno);

	/* --------------------------------------------------------------------
	 * 6. Waking collected waiters
	 * --------------------------------------------------------------------*/
	pg_write_barrier();			/* ensure list modifications are visible */

	for (uint32 procno = wakeListHead;
		 procno != PAGE_STATE_INVALID_PROCNO;)
	{
		OPageWaiterShmemState *lockState = &lockerStates[procno];
		uint32		next;
		PGPROC	   *proc = GetPGProcByNumber(procno);

		next = lockState->next;
		lockState->status = OPageWaitWakeUp;

		/*
		 * Ensure memory access ordering.  The effect of statements above must
		 * materialize before waking up the waiter, which must see
		 * lockState->pageWaiting == false and can modify lockState->next.
		 */
		pg_memory_barrier();

		PGSemaphoreUnlock(proc->sem);

		procno = next;
	}
}

void
unlock_page(OInMemoryBlkno blkno)
{
	unlock_page_internal(blkno, false);
}

/*
 * Unlock the page after page split.  Page should be locked before.
 */
void
unlock_page_after_split(OInMemoryBlkno blkno)
{
	unlock_page_internal(blkno, true);
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
btree_register_inprogress_split(OInMemoryBlkno rightBlkno)
{
#ifdef USE_ASSERT_CHECKING
	int			i;

	for (i = 0; i < numberOfMyInProgressSplitPages; i++)
		Assert(myInProgressSplitPages[i] != rightBlkno);
#endif
	Assert(CritSectionCount > 0);
	Assert((numberOfMyInProgressSplitPages + 1) <= sizeof(myInProgressSplitPages) / sizeof(myInProgressSplitPages[0]));
	myInProgressSplitPages[numberOfMyInProgressSplitPages++] = rightBlkno;
}

/*
 * Unregister in-progress split.
 *
 * Must be calles within critical section.
 */
void
btree_unregister_inprogress_split(OInMemoryBlkno rightBlkno)
{
	int			i;

	Assert(CritSectionCount > 0);
	Assert(numberOfMyInProgressSplitPages > 0);
	for (i = 0; i < numberOfMyInProgressSplitPages; i++)
	{
		if (myInProgressSplitPages[i] == rightBlkno)
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
btree_split_mark_finished(OInMemoryBlkno rightBlkno, bool use_lock, bool success)
{
	BTreePageHeader *leftHeader;
	BTreePageHeader *rightHeader;
	OrioleDBPageDesc *rightPageDesc = O_GET_IN_MEMORY_PAGEDESC(rightBlkno);
	OInMemoryBlkno leftBlkno;

	leftBlkno = rightPageDesc->leftBlkno;
	Assert(OInMemoryBlknoIsValid(leftBlkno));

	/*
	 * Still need to lock th left page even if we're going to just set
	 * BROKEN_SPLIT on the right page, because we need to notify waiters in
	 * o_btree_split_is_incomplete().
	 */
	if (use_lock)
	{
		while (true)
		{
			lock_page(leftBlkno);

			if (rightPageDesc->leftBlkno == leftBlkno)
				break;

			unlock_page(leftBlkno);
			leftBlkno = rightPageDesc->leftBlkno;
			Assert(OInMemoryBlknoIsValid(leftBlkno));
		}
	}

	lock_page(rightBlkno);

	if (use_lock)
		page_block_reads(leftBlkno);
	page_block_reads(rightBlkno);

	START_CRIT_SECTION();

	leftHeader = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(leftBlkno);
	rightHeader = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(rightBlkno);

	Assert(RightLinkIsValid(leftHeader->rightLink));
	Assert(use_lock || success);

	if (success)
	{
		rightHeader->flags &= ~O_BTREE_FLAG_BROKEN_SPLIT;
		leftHeader->rightLink = InvalidRightLink;
		rightPageDesc->leftBlkno = OInvalidInMemoryBlkno;
	}
	else
	{
		Assert(!O_PAGE_IS(O_GET_IN_MEMORY_PAGE(rightBlkno), BROKEN_SPLIT));
		rightHeader->flags |= O_BTREE_FLAG_BROKEN_SPLIT;
	}

	END_CRIT_SECTION();

	unlock_page(rightBlkno);

	if (use_lock)
		unlock_page(leftBlkno);
}

#ifdef CHECK_PAGE_STRUCT

extern void log_btree(BTreeDescr *desc);

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
	OTuple		prevChunkHikey;

	Assert(header->dataSize <= ORIOLEDB_BLCKSZ);
	Assert(header->hikeysEnd <= header->dataSize);

	O_TUPLE_SET_NULL(prevChunkHikey);

	for (i = 0; i < header->chunksCount; i++)
	{
		BTreePageChunkDesc *chunk = &header->chunkDesc[i];
		BTreePageChunk *chunkData;
		OTuple		chunkHikey;

		if (O_PAGE_IS(p, RIGHTMOST) && i == header->chunksCount - 1)
		{
			O_TUPLE_SET_NULL(chunkHikey);
		}
		else
		{
			chunkHikey.formatFlags = header->chunkDesc[i].hikeyFlags;
			chunkHikey.data = p + SHORT_GET_LOCATION(header->chunkDesc[i].hikeyShortLocation);
		}

		if (!O_PAGE_IS(p, RIGHTMOST) || i < header->chunksCount - 1)
		{
			Assert((chunk->hikeyFlags & O_TUPLE_FLAGS_FIXED_FORMAT) || !(header->flags & O_BTREE_FLAG_HIKEYS_FIXED));
		}

		if (i > 0)
		{
			BTreePageChunkDesc *prevChunk = &header->chunkDesc[i - 1] PG_USED_FOR_ASSERTS_ONLY;

			Assert(chunk->shortLocation >= prevChunk->shortLocation);
			Assert(chunk->offset >= prevChunk->offset);
			Assert(O_PAGE_IS(p, RIGHTMOST) || chunk->hikeyShortLocation > prevChunk->hikeyShortLocation);
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
			if (!(i == 0 && j == 0 && !O_PAGE_IS(p, LEAF)))
			{
				Assert((ITEM_GET_FLAGS(chunkData->items[j]) & O_TUPLE_FLAGS_FIXED_FORMAT) || (chunk->chunkKeysFixed == 0));
			}
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
					if (!O_TUPLE_IS_NULL(chunkHikey))
						Assert(o_btree_cmp(desc, &tuple, BTreeKeyLeafTuple, &chunkHikey, BTreeKeyNonLeafKey) < 0);
					if (!O_TUPLE_IS_NULL(prevChunkHikey))
						Assert(o_btree_cmp(desc, &tuple, BTreeKeyLeafTuple, &prevChunkHikey, BTreeKeyNonLeafKey) >= 0);
				}
				else
				{
#ifdef ORIOLEDB_CUT_FIRST_KEY
					if (i == 0 && j == 0)
					{
						len = BTreeNonLeafTuphdrSize;
						O_TUPLE_SET_NULL(tuple);
					}
					else
#endif
					{
						tuple.data = (Pointer) chunkData + ITEM_GET_OFFSET(chunkData->items[j]) + BTreeNonLeafTuphdrSize;
						len = BTreeNonLeafTuphdrSize + o_btree_len(desc, tuple, OKeyLength);
					}
					if (!O_TUPLE_IS_NULL(chunkHikey) && !O_TUPLE_IS_NULL(tuple))
						Assert(o_btree_cmp(desc, &tuple, BTreeKeyNonLeafKey, &chunkHikey, BTreeKeyNonLeafKey) < 0);
					if (!O_TUPLE_IS_NULL(prevChunkHikey) && !O_TUPLE_IS_NULL(tuple))
						Assert(o_btree_cmp(desc, &tuple, BTreeKeyNonLeafKey, &prevChunkHikey, BTreeKeyNonLeafKey) >= 0);
				}

				if (j < itemsCount - 1)
					Assert(ITEM_GET_OFFSET(chunkData->items[j]) + len <= ITEM_GET_OFFSET(chunkData->items[j + 1]));
				else
					Assert(ITEM_GET_OFFSET(chunkData->items[j]) + len <= chunkSize);

			}
		}

		prevChunkHikey = chunkHikey;
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
