/*-------------------------------------------------------------------------
 *
 * btree.c
 *		Routines for OrioleDB B-tree initilization and cleanup.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/btree.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

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
	MARK_DIRTY(desc->ppool, desc->rootInfo.rootPageBlkno);
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
				 errmsg("index row size %d orioledb maximum %zu for %s \"%s\"",
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
