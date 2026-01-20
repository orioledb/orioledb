/*-------------------------------------------------------------------------
 *
 * btree.c
 *		Routines for OrioleDB B-tree initialization and cleanup.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/btree.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "catalog/pg_index.h"
#include "btree/find.h"
#include "btree/insert.h"
#include "btree/io.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "catalog/o_tables.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/tree.h"
#include "transam/undo.h"
#include "transam/oxid.h"
#include "tuple/format.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/hsearch.h"

/* Shared memory hash table for validation boundaries */
static HTAB *validation_boundary_htab = NULL;
static LWLock *validation_boundary_lock = NULL;
#define VALIDATION_BOUNDARY_NON  0x0000	/* Minimum value of the first byte of serialized PK tuple for validation boundary */
#define VALIDATION_BOUNDARY_FULL 0xFFFF	/* Maximum value of the first byte of serialized PK tuple for validation boundary */


LWLockPadded *unique_locks;
int			num_unique_locks;

void
o_btree_init_unique_lwlocks(void)
{
	num_unique_locks = max_procs * 4;
	unique_locks = GetNamedLWLockTranche("orioledb_unique_locks");
}

void
o_btree_init(BTreeDescr *desc)
{
	init_new_btree_page(desc, desc->rootInfo.rootPageBlkno,
						O_BTREE_FLAGS_ROOT_INIT, 0, false);
	init_page_first_chunk(desc, O_GET_IN_MEMORY_PAGE(desc->rootInfo.rootPageBlkno), 0);
	unlock_page(desc->rootInfo.rootPageBlkno);
	init_meta_page(desc->rootInfo.metaPageBlkno, 1);

	/*
	 * Don't mark the root page dirty by default to skip checkpointing of the
	 * empty trees.  Except for the system trees, which are checkpointed every
	 * time.
	 */
	if (IS_SYS_TREE_OIDS(desc->oids))
		MARK_DIRTY(desc, desc->rootInfo.rootPageBlkno);
}

static bool
get_page_children(OInMemoryBlkno blkno, uint32 pageChangeCount,
				  OInMemoryBlkno childPageNumbers[BTREE_PAGE_MAX_CHUNK_ITEMS],
				  uint32 childPageChangeCounts[BTREE_PAGE_MAX_CHUNK_ITEMS],
				  int *childPagesCount)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageDesc *desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	BTreePageItemLocator loc;
	int			ionum;

retry:
	lock_page(blkno);
	if (desc->ionum >= 0)
	{
		ionum = desc->ionum;
		unlock_page(blkno);

		wait_for_io_completion(ionum);
		goto retry;
	}
	*childPagesCount = 0;

	if (O_PAGE_GET_CHANGE_COUNT(p) != pageChangeCount)
	{
		/*
		 * It seems that page has been evicted concurrently.  So, nothing to
		 * do.
		 */
		unlock_page(blkno);
		return false;
	}

	if (!O_PAGE_IS(p, LEAF))
	{
		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			if (DOWNLINK_IS_IN_IO(tuphdr->downlink))
			{
				ionum = DOWNLINK_GET_IO_LOCKNUM(tuphdr->downlink);
				unlock_page(blkno);

				wait_for_io_completion(ionum);
				goto retry;
			}
			else if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
			{
				childPageNumbers[*childPagesCount] = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
				childPageChangeCounts[*childPagesCount] = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(tuphdr->downlink);
				(*childPagesCount)++;
			}
		}
	}
	return true;
}

/*
 * Recursively sets O_BTREE_FLAG_PRE_CLEANUP to the given page and all its
 * children.
 */
static void
mark_page_pre_cleanup(OInMemoryBlkno blkno, uint32 pageChangeCount)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	OInMemoryBlkno childPageNumbers[BTREE_PAGE_MAX_CHUNK_ITEMS];
	uint32		childPageChangeCounts[BTREE_PAGE_MAX_CHUNK_ITEMS];
	int			childPagesCount;
	int			i,
				ionum;

	if (!get_page_children(blkno, pageChangeCount,
						   childPageNumbers, childPageChangeCounts,
						   &childPagesCount))
		return;

	page_block_reads(blkno);
	header->flags |= O_BTREE_FLAG_PRE_CLEANUP;
	ionum = O_GET_IN_MEMORY_PAGEDESC(blkno)->ionum;
	unlock_page(blkno);

	if (ionum >= 0)
		wait_for_io_completion(ionum);

	for (i = 0; i < childPagesCount; i++)
		mark_page_pre_cleanup(childPageNumbers[i],
							  childPageChangeCounts[i]);
}

/*
 * Frees given page and all of its children recursively.
 */
static void
free_page(OPagePool *pool, OInMemoryBlkno blkno, uint32 pageChangeCount)
{
	OInMemoryBlkno childPageNumbers[BTREE_PAGE_MAX_CHUNK_ITEMS];
	uint32		childPageChangeCounts[BTREE_PAGE_MAX_CHUNK_ITEMS];
	int			childPagesCount;
	int			i;

	if (!get_page_children(blkno, pageChangeCount,
						   childPageNumbers, childPageChangeCounts,
						   &childPagesCount))
		return;
	Assert(O_PAGE_IS(O_GET_IN_MEMORY_PAGE(blkno), PRE_CLEANUP));
	Assert(O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(blkno)) == pageChangeCount);
	Assert(O_GET_IN_MEMORY_PAGEDESC(blkno)->ionum < 0);
	unlock_page(blkno);

	for (i = 0; i < childPagesCount; i++)
		free_page(pool,
				  childPageNumbers[i],
				  childPageChangeCounts[i]);

	lock_page(blkno);
	Assert(O_PAGE_IS(O_GET_IN_MEMORY_PAGE(blkno), PRE_CLEANUP));
	Assert(O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(blkno)) == pageChangeCount);
	Assert(O_GET_IN_MEMORY_PAGEDESC(blkno)->ionum < 0);
	page_block_reads(blkno);
	CLEAN_DIRTY(pool, blkno);
	ppool_free_page(pool, blkno, true);

}

static inline void
free_meta_page(OPagePool *pool, OInMemoryBlkno metaPageBlkno)
{
	BTreeMetaPage *meta_page;
	int			i,
				j;

	meta_page = (BTreeMetaPage *) O_GET_IN_MEMORY_PAGE(metaPageBlkno);
	for (i = 0; i < 2; i++)
	{
		FREE_PAGE_IF_VALID(pool, meta_page->freeBuf.pages[i]);
		for (j = 0; j < 2; j++)
		{
			FREE_PAGE_IF_VALID(pool, meta_page->nextChkp[j].pages[i]);
			FREE_PAGE_IF_VALID(pool, meta_page->tmpBuf[j].pages[i]);
		}
	}
	ppool_free_page(pool, metaPageBlkno, NULL);
}

/*
 * Two phase algorithm for pages cleanup, which can run concurrently
 * to walk_page().
 *
 * The first phase sets O_BTREE_FLAG_PRE_CLEANUP preventing walk_page() from
 * evicting or writing these pages.
 *
 * The second phase cleans pages previously marked with
 * O_BTREE_FLAG_PRE_CLEANUP flag from bottom to top.
 *
 * Therefore walk_page() never gets in trouble trying to find parent page
 * using find_page().
 */
void
o_btree_cleanup_pages(OInMemoryBlkno rootPageBlkno, OInMemoryBlkno metaPageBlkno, uint32 rootPageChangeCount)
{
	OPagePool  *pool = get_ppool_by_blkno(rootPageBlkno);

	Assert(OInMemoryBlknoIsValid(rootPageBlkno));
	Assert(OInMemoryBlknoIsValid(metaPageBlkno));
	Assert(pool != NULL);

	mark_page_pre_cleanup(rootPageBlkno, rootPageChangeCount);
	free_page(pool, rootPageBlkno, rootPageChangeCount);

	free_meta_page(pool, metaPageBlkno);
}

void
o_btree_check_size_of_tuple(int len, char *relation_name, bool index)
{
	if (len > O_BTREE_MAX_TUPLE_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("index row size %d exceeds orioledb maximum %zu for %s \"%s\"",
						len,
						O_BTREE_MAX_TUPLE_SIZE,
						index ? "index" : "table",
						relation_name)));
}

ItemPointerData
btree_ctid_get_and_inc(BTreeDescr *desc)
{
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	ItemPointerData result;
	uint64		ctid = pg_atomic_fetch_add_u64(&metaPageBlkno->ctid, 1);

	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));
	Assert(ctid / (MaxOffsetNumber - FirstOffsetNumber) < InvalidBlockNumber);

	ItemPointerSet(&result,
				   (uint32) (ctid / (MaxOffsetNumber - FirstOffsetNumber)),
				   (OffsetNumber) (ctid % (MaxOffsetNumber - FirstOffsetNumber) + FirstOffsetNumber));
	return result;
}

void
btree_ctid_update_if_needed(BTreeDescr *desc, ItemPointerData ctid)
{
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	uint64		old_ctid,
				new_ctid;

	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));
	new_ctid = (uint64) ItemPointerGetBlockNumber(&ctid) * (MaxOffsetNumber - FirstOffsetNumber);
	new_ctid += ctid.ip_posid - FirstOffsetNumber;
	Assert(new_ctid < (uint64) (MaxOffsetNumber - FirstOffsetNumber) * (uint64) InvalidBlockNumber);

	new_ctid++;
	do
	{
		old_ctid = pg_atomic_read_u64(&metaPageBlkno->ctid);
		if (old_ctid >= new_ctid)
			break;
	} while (!pg_atomic_compare_exchange_u64(&metaPageBlkno->ctid, &old_ctid, new_ctid));
}

ItemPointerData
btree_bridge_ctid_get_and_inc(BTreeDescr *desc, bool *overflow)
{
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	ItemPointerData result;
	uint64		ctid = pg_atomic_fetch_add_u64(&metaPageBlkno->bridge_ctid, 1);

	BlockNumber max_block_number = MaxBlockNumber;

	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));

	if (BlockNumberIsValid(max_bridge_ctid_blkno))
		max_block_number = max_bridge_ctid_blkno;

	*overflow = ctid / MaxHeapTuplesPerPage >= max_block_number;

	ItemPointerSet(&result,
				   (uint32) (ctid / MaxHeapTuplesPerPage % max_block_number),
				   (OffsetNumber) (ctid % MaxHeapTuplesPerPage + FirstOffsetNumber));
	return result;
}

static inline OIndexDescr *
o_get_tree_def(BTreeDescr *desc)
{
	return desc->arg;
}

void
btree_desc_stopevent_params_internal(BTreeDescr *desc, JsonbParseState **state)
{
	jsonb_push_int8_key(state, "datoid", desc->oids.datoid);
	jsonb_push_int8_key(state, "reloid", desc->oids.reloid);
	jsonb_push_int8_key(state, "relnode", desc->oids.relnode);

	if (IS_SYS_TREE_OIDS(desc->oids))
		jsonb_push_string_key(state, "treeName", "sys_tree");
	else if (desc->type == oIndexToast)
		jsonb_push_string_key(state, "treeName", "toast");
	else
		jsonb_push_string_key(state, "treeName", o_get_tree_def(desc)->name.data);
}

void
btree_page_stopevent_params_internal(BTreeDescr *desc, Page p,
									 JsonbParseState **state)
{
	jsonb_push_int8_key(state, "level", PAGE_GET_LEVEL(p));
	jsonb_push_int8_key(state, "pageChangeCount", O_PAGE_GET_CHANGE_COUNT(p));

	jsonb_push_key(state, "hikey");
	if (!O_PAGE_IS(p, RIGHTMOST))
	{
		OTuple		hikey;

		BTREE_PAGE_GET_HIKEY(hikey, p);
		(void) o_btree_key_to_jsonb(desc, hikey, state);
	}
	else
	{
		JsonbValue	jval;

		jval.type = jbvNull;
		(void) pushJsonbValue(state, WJB_VALUE, &jval);
	}
}

Jsonb *
btree_page_stopevent_params(BTreeDescr *desc, Page p)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;
	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(desc, &state);
	btree_page_stopevent_params_internal(desc, p, &state);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

Jsonb *
btree_downlink_stopevent_params(BTreeDescr *desc, Page p, BTreePageItemLocator *loc)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;
	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);
	BTreeNonLeafTuphdr *internal_ptr;

	internal_ptr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, loc);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(desc, &state);
	btree_page_stopevent_params_internal(desc, p, &state);

	jsonb_push_key(&state, "downlink");
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(&state, "blkno", DOWNLINK_GET_IN_MEMORY_BLKNO(internal_ptr->downlink));
	jsonb_push_int8_key(&state, "pageChangeCount", DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(internal_ptr->downlink));
	jsonb_push_key(&state, "key");
	if (BTREE_PAGE_LOCATOR_GET_OFFSET(p, loc) > 0)
	{
		OTuple		key;

		BTREE_PAGE_READ_INTERNAL_TUPLE(key, p, loc);
		(void) o_btree_key_to_jsonb(desc, key, &state);
	}
	else
	{
		JsonbValue	jval;

		jval.type = jbvNull;
		(void) pushJsonbValue(&state, WJB_VALUE, &jval);
	}
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

/*
 * Calculate shared memory needed for validation boundary hash table.
 */
Size
btree_validation_shmem_needs(void)
{
	Size		size;

	/* Size for hash table: 128 concurrent index builds should be enough */
	size = hash_estimate_size(128, sizeof(ValidationBoundaryEntry));
	
	return size;
}

/*
 * Initialize shared memory for validation boundary hash table.
 */
void
btree_validation_shmem_init(Pointer ptr, bool found)
{
	HASHCTL		info;

	if (!found)
	{
		/* Initialize hash table */
		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(ORelOids);
		info.entrysize = sizeof(ValidationBoundaryEntry);
		
		validation_boundary_htab = ShmemInitHash("Validation Boundary Hash",
												 128, 128,
												 &info,
												 HASH_ELEM | HASH_BLOBS);
	}
	else
	{
		/* Attach to existing hash table */
		validation_boundary_htab = (HTAB *) ptr;
	}
	
	/* Get the single lock for the hash table */
	validation_boundary_lock = &(GetNamedLWLockTranche("orioledb_validation_boundary")->lock);
}

/*
 * Set the validation boundary for concurrent index builds.
 * The boundary is stored as an OBTreeKeyBound in the meta page.
 */
void
btree_set_validation_boundary(OIndexDescr *idx, OTuple heapTuple)
{
	ValidationBoundaryEntry *entry;
	int			boundaryLen;
	BTreeDescr *desc = &idx->desc;
	bool allocated = false;
	bool		found;

	Assert(desc != NULL);
	Assert(!O_TUPLE_IS_NULL(heapTuple));
	
	OTuple boundary = o_tuple_make_key(desc, heapTuple, NULL, false, &allocated);

	boundaryLen = o_btree_len(desc, boundary, OTupleLength);
	if (boundaryLen > O_BTREE_MAX_KEY_SIZE)
		elog(ERROR, "validation boundary tuple too large: %d bytes (maximum: %ld bytes)",
			 boundaryLen, O_BTREE_MAX_KEY_SIZE);

	/* Acquire single global lock */
	LWLockAcquire(validation_boundary_lock, LW_EXCLUSIVE);
	/* Insert or update entry in hash table */
	entry = (ValidationBoundaryEntry *) hash_search(validation_boundary_htab,
													&desc->oids,
													HASH_ENTER,
													&found);
	memcpy(entry->tupleData, boundary.data, boundaryLen);
	entry->tupleLen = boundaryLen;
	entry->formatFlags = boundary.formatFlags;
	LWLockRelease(validation_boundary_lock);
}

/*
 * Get the current validation boundary.
 * Returns true if a boundary is set, false otherwise.
 * The boundary OTuple will be allocated in the current memory context.
 */
bool
btree_get_validation_boundary(BTreeDescr *desc, OTuple *boundary)
{
	char	   *data;
	ValidationBoundaryEntry *entry;

	Assert(desc != NULL);
	Assert(boundary != NULL);
	Assert(validation_boundary_htab != NULL);
	
	/* Acquire shared lock for reading */
	LWLockAcquire(validation_boundary_lock, LW_SHARED);

	/* Look up entry in hash table */
	entry = (ValidationBoundaryEntry *) hash_search(validation_boundary_htab,
													&desc->oids,
													HASH_FIND,
													NULL);

	if (entry == NULL)
	{
		O_TUPLE_SET_NULL(*boundary);
		LWLockRelease(validation_boundary_lock);
		return false;
	}

	/* Allocate and copy the boundary tuple */
	data = (char *) palloc(entry->tupleLen);
	memcpy(data, entry->tupleData, entry->tupleLen);
	
	boundary->data = data;
	boundary->formatFlags = entry->formatFlags;
	
	LWLockRelease(validation_boundary_lock);
	return true;
}

uint16
btree_is_validation_boundary_get_len(BTreeDescr *desc)
{
	uint16		len;
	ValidationBoundaryEntry *entry;

	Assert(desc != NULL);
	
	LWLockAcquire(validation_boundary_lock, LW_SHARED);

	/* Look up entry in hash table */
	entry = (ValidationBoundaryEntry *) hash_search(validation_boundary_htab,
													&desc->oids,
													HASH_FIND,
													NULL);
													
	len = entry->tupleLen;
	
	LWLockRelease(validation_boundary_lock);

	return len;
}

/*
 * Set validation boundary to non visible.
 */
void
btree_set_validation_boundary_non_visible(OIndexDescr *idx)
{
	ValidationBoundaryEntry *entry;
	BTreeDescr *desc = &idx->desc;
	bool found;

	Assert(desc != NULL);
	
	LWLockAcquire(validation_boundary_lock, LW_EXCLUSIVE);

	/* Insert or update entry in hash table */
	entry = (ValidationBoundaryEntry *) hash_search(validation_boundary_htab,
													&desc->oids,
													HASH_ENTER,
													&found);
	entry->tupleLen = VALIDATION_BOUNDARY_NON;
	
	LWLockRelease(validation_boundary_lock);
}

/*
 * Set validation boundary to full visible (no boundary).
 */
void
btree_set_validation_boundary_full_visible(OIndexDescr *idx)
{
	ValidationBoundaryEntry *entry;
	BTreeDescr *desc = &idx->desc;

	Assert(desc != NULL);
	
	LWLockAcquire(validation_boundary_lock, LW_EXCLUSIVE);
	/* Insert or update entry in hash table */
	entry = (ValidationBoundaryEntry *) hash_search(validation_boundary_htab,
													&desc->oids,
													HASH_ENTER,
													NULL);
	entry->tupleLen = VALIDATION_BOUNDARY_FULL;
	
	LWLockRelease(validation_boundary_lock);
}

void
btree_remove_validation_boundary(OIndexDescr *idx)
{
	BTreeDescr *desc = &idx->desc;

	Assert(desc != NULL);
	
	LWLockAcquire(validation_boundary_lock, LW_EXCLUSIVE);

	/* Remove entry from hash table */
	hash_search(validation_boundary_htab, &desc->oids, HASH_REMOVE, NULL);
	
	LWLockRelease(validation_boundary_lock);
}

/*
 * Check if a primary key satisfies the validation boundary.
 * Returns true if:
 * - No validation is in progress (boundary not set), OR
 * - The PK is less than or equal to the boundary
 * Returns false if PK is greater than the boundary.
 */
bool
btree_pk_satisfies_validation_boundary(BTreeDescr *desc, OTuple pk)
{
	OTuple		boundary;
	ValidationBoundaryEntry *entry;
	char		boundaryData[O_BTREE_MAX_KEY_SIZE];
	int			cmp;

	Assert(desc != NULL);
	Assert(!O_TUPLE_IS_NULL(pk));
		
	LWLockAcquire(validation_boundary_lock, LW_SHARED);
	
	/* Look up entry in hash table */
	entry = (ValidationBoundaryEntry *) hash_search(validation_boundary_htab,
													&desc->oids,
													HASH_FIND,
													NULL);
	if (entry == NULL)
	{
		LWLockRelease(validation_boundary_lock);
		/* No boundary set, so PK satisfies it */
		return true;
	}
										
	if (entry->tupleLen == VALIDATION_BOUNDARY_NON)
	{
		LWLockRelease(validation_boundary_lock);
		/* Boundary is non visible, so PK does not satisfy it */
		return false;
	}
	else if (entry->tupleLen == VALIDATION_BOUNDARY_FULL)
	{
		LWLockRelease(validation_boundary_lock);
		/* Boundary is full visible, so PK satisfies it */
		return true;
	}
	
	memcpy(boundaryData, entry->tupleData, entry->tupleLen);

	boundary.data = boundaryData;
	boundary.formatFlags = entry->formatFlags;
	
	LWLockRelease(validation_boundary_lock);

	/* Compare PK with boundary */
	cmp = o_btree_cmp(desc, &pk, BTreeKeyNonLeafKey,
					  &boundary, BTreeKeyNonLeafKey);
	
	/* Return true if PK <= boundary */
	return (cmp <= 0);
}

/*
 * Check if an index is "ready but not valid" by querying pg_index.
 * This indicates the index is being built concurrently.
 *
 * Returns true if indisready=true and indisvalid=false, false otherwise.
 */
bool
btree_index_is_ready_not_valid(Oid indexRelOid)
{
	HeapTuple	indexTuple;
	Form_pg_index indexForm;
	bool		result;
	
	/* Get the index tuple from pg_index */
	indexTuple = SearchSysCache1(INDEXRELID, indexRelOid);
	
	if (!HeapTupleIsValid(indexTuple))
	{
		/* Index not found in pg_index */
		return false;
	}

	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

	/* Check if index is ready but not valid */
	result = (indexForm->indisready && !indexForm->indisvalid);

	ReleaseSysCache(indexTuple);

	return result;
}
