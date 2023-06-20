/*-------------------------------------------------------------------------
 *
 * descr.c
 *		Routines for handling descriptors of orioledb trees.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/descr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "btree/iterator.h"
#include "btree/modify.h"
#include "checkpoint/checkpoint.h"
#include "catalog/free_extents.h"
#include "catalog/o_indices.h"
#include "catalog/o_sys_cache.h"
#include "catalog/o_tables.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"
#include "tableam/toast.h"
#include "tableam/tree.h"
#include "tuple/slot.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"

#include "access/nbtree.h"
#include "catalog/pg_opfamily.h"
#include "common/hashfn.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "pgstat.h"

static OIndexDescr *get_index_descr(ORelOids ixOids, OIndexType ixType,
									bool miss_ok);
static void o_table_descr_fill_indices(OTableDescr *descr, OTable *table);
static void init_shared_root_info(OPagePool *pool,
								  SharedRootInfo *sharedRootInfo);
static void cleanup_shared_root_info_pages(OPagePool *pool,
										   SharedRootInfo *sharedRootInfo);
static bool o_tree_init_free_extents(BTreeDescr *desc);
static OComparator *o_find_opclass_comparator(OOpclass *opclass, Oid collation);
static inline OComparator *o_find_cached_comparator(OComparatorKey *key);
static inline OComparator *o_add_comparator_to_cache(OComparator *comparator);
static bool recreate_table_descr(OTableDescr *descr);
static void recreate_index_descr(OIndexDescr *descr);

PG_FUNCTION_INFO_V1(orioledb_get_table_descrs);
PG_FUNCTION_INFO_V1(orioledb_get_index_descrs);

struct OComparatorKey
{
	Oid			opfamily;
	Oid			lefttype;
	Oid			righttype;
	Oid			collation;
};

struct OComparator
{
	OComparatorKey key;
	bool		haveSortSupport;

	/* Filled when haveSortSupport == false */
	FmgrInfo	finfo;

	/* Filled when haveSortSupport == true */
	MemoryContext ssup_cxt;
	void	   *ssup_extra;
	int			(*ssup_comparator) (Datum x, Datum y, SortSupport ssup);
};

static HTAB *oTableDescrHash;
static HTAB *oIndexDescrHash;
static HTAB *comparatorCache;
static OComparatorKey lastkey = {0};
static OComparator *lastcmp = NULL;
static MemoryContext descrCxt = NULL;

static void o_find_toastable_attrs(OTableDescr *tableDescr);


/*
 * Creates shared root info.  But insertion into shared cache is performed by
 * table_descr_init_tree function.
 */
static SharedRootInfo *
create_shared_root_info(OPagePool *pool, SharedRootInfoKey *key)
{
	SharedRootInfo *sharedRootInfo;

	sharedRootInfo = palloc0(sizeof(SharedRootInfo));
	sharedRootInfo->key = *key;
	init_shared_root_info(pool, sharedRootInfo);
	return sharedRootInfo;
}

EvictedTreeData *
read_evicted_data(ORelOids oids, uint32 chkp_num)
{
	char	   *filename;
	File		eviction_file;
	EvictedTreeData *evicted_tree_data = NULL;

	filename = get_eviction_filename(oids, chkp_num);
	eviction_file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);

	if (eviction_file != -1)
	{
		evicted_tree_data = palloc(sizeof(EvictedTreeData));
		if (OFileRead(eviction_file, (Pointer) evicted_tree_data, sizeof(EvictedTreeData), 0, WAIT_EVENT_DATA_FILE_READ) !=
			sizeof(EvictedTreeData))
		{
			pfree(evicted_tree_data);
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not read evicted tree data from file %s",
								   filename)));
		}
		FileClose(eviction_file);
	}
	pfree(filename);

	return evicted_tree_data;
}

/*
 * OTableDescr* BTrees are created without shared memory initialization.
 * Sequence buffers files, data rootInfo file are not initialized too. There are
 * reasons for it:
 *
 * 1. Long queries may do not use all indices.
 * 2. In some cases no sense to initialize BTree memory if it not exists.
 *
 * We can load shared memory in-place, in low-level BTree code
 * but it more complicated approach. It will be harder to understand and debug.
 *
 * To avoid concurrency problems with eviction/cleanup table this call must be
 * under AccessShareLock (See o_tables.h/o_tables_rel_lock()).
 */
static bool
o_btree_load_shmem_internal(BTreeDescr *desc, bool checkpoint)
{
	SharedRootInfoKey key;
	SharedRootInfo *sharedRootInfo = NULL;
	bool		was_evicted,
				is_compressed,
				init_extents,
				inserted PG_USED_FOR_ASSERTS_ONLY;
	int			lockNo;
	bool		hasLock = false;

	Assert(desc != NULL);
	if (!ORelOidsIsValid(desc->oids) || IS_SYS_TREE_OIDS(desc->oids))
		return true;

	/* easy case: shared memory is initialized */
	if (ORootPageIsValid(desc) && OMetaPageIsValid(desc))
		return true;

	memset(&key, 0, sizeof(SharedRootInfoKey));
	key.datoid = desc->oids.datoid;
	key.relnode = desc->oids.relnode;

	sharedRootInfo = o_find_shared_root_info(&key);
	if (sharedRootInfo == NULL)
	{
		lockNo = tag_hash(&key, sizeof(key)) % SHARED_ROOT_INFO_INSERT_NUM_LOCKS;
		LWLockAcquire(&checkpoint_state->oSharedRootInfoInsertLocks[lockNo],
					  LW_EXCLUSIVE);
		hasLock = true;
		sharedRootInfo = o_find_shared_root_info(&key);
	}

	if (sharedRootInfo && sharedRootInfo->placeholder)
	{
		if (hasLock)
			LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[lockNo]);
		pfree(sharedRootInfo);
		return false;
	}

	if (sharedRootInfo == NULL)
	{
		OTuple		sharedRootInfoTuple;

		/* tries to create SharedRootInfo */
		sharedRootInfo = create_shared_root_info(desc->ppool, &key);
		desc->rootInfo = sharedRootInfo->rootInfo;

		checkpointable_tree_init(desc, true, &was_evicted);
		is_compressed = OCompressIsValid(desc->compress);
		desc->rootInfo = sharedRootInfo->rootInfo;

		init_extents = false;
		if (is_compressed && !was_evicted)
		{
			init_extents = true;

			/*
			 * We should prevent iteration through free extentents list by the
			 * checkpointer until free extents is not completely initialized
			 * yet.
			 */
			LWLockAcquire(&BTREE_GET_META(desc)->copyBlknoLock, LW_SHARED);
		}

		sharedRootInfoTuple.data = (Pointer) sharedRootInfo;
		sharedRootInfoTuple.formatFlags = 0;
		inserted = o_btree_autonomous_insert(get_sys_tree(SYS_TREES_SHARED_ROOT_INFO),
											 sharedRootInfoTuple);
		Assert(inserted);

		if (init_extents)
		{
			/*
			 * The loader of an index fills the free extents list.
			 */
			if (!o_tree_init_free_extents(desc))
			{
				LWLockRelease(&BTREE_GET_META(desc)->copyBlknoLock);
				elog(FATAL,
					 "unable to read free extents file %s",
					 get_seq_buf_filename(&desc->freeBuf.tag));
			}
			LWLockRelease(&BTREE_GET_META(desc)->copyBlknoLock);
		}
	}
	else
	{
		/*
		 * o_btree_load_shmem() must be called only under relation locks, in
		 * this state BTree can not be evicted and removed from ShareDescr
		 * cache because AccessExclusiveLock needed for this actions.
		 */
		Assert(OInMemoryBlknoIsValid(sharedRootInfo->rootInfo.rootPageBlkno));
		Assert(OInMemoryBlknoIsValid(sharedRootInfo->rootInfo.metaPageBlkno));

		desc->rootInfo = sharedRootInfo->rootInfo;
		(void) checkpointable_tree_init(desc, false, NULL);
	}

	if (hasLock)
		LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[lockNo]);

	Assert(sharedRootInfo != NULL);
	Assert(!sharedRootInfo->placeholder);
	pfree(sharedRootInfo);
	return true;
}

void
o_btree_load_shmem(BTreeDescr *desc)
{
	bool		result PG_USED_FOR_ASSERTS_ONLY;

	result = o_btree_load_shmem_internal(desc, false);
	Assert(result == true);
}

bool
o_btree_load_shmem_checkpoint(BTreeDescr *desc)
{
	return o_btree_load_shmem_internal(desc, true);
}

/*
 * Returns false if BTree does not exist in shared memory.
 *
 * Same to o_btree_load_shmem() but it does not create a BTree in shared
 * memory. Must be called under relation locks too.
 */
bool
o_btree_try_use_shmem(BTreeDescr *desc)
{
	Assert(ORelOidsIsValid(desc->oids));

	if (!ORootPageIsValid(desc) || !OMetaPageIsValid(desc))
	{
		SharedRootInfoKey key;
		SharedRootInfo *shared = NULL;

		key.datoid = desc->oids.datoid;
		key.relnode = desc->oids.relnode;

		shared = o_find_shared_root_info(&key);
		if (shared == NULL)
			return false;

		Assert(!shared->placeholder);
		Assert(OInMemoryBlknoIsValid(shared->rootInfo.rootPageBlkno));
		Assert(OInMemoryBlknoIsValid(shared->rootInfo.metaPageBlkno));

		desc->rootInfo = shared->rootInfo;

		(void) checkpointable_tree_init(desc, false, NULL);
		pfree(shared);
	}
	return true;
}

/*
 * Appends extents from free blocks file to the free extents list.
 */
static bool
o_tree_init_free_extents(BTreeDescr *desc)
{
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	uint64		num_free_blocks = pg_atomic_read_u64(&metaPageBlkno->numFreeBlocks);
	File		file;
	char	   *filename;

	Assert(OCompressIsValid(desc->compress));

	if (num_free_blocks == 0)
		return true;

	filename = get_seq_buf_filename(&desc->freeBuf.tag);
	file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
	pfree(filename);

	if (file >= 0)
	{
		FileExtent *extent;
		off_t		offset = sizeof(CheckpointFileHeader),
					bytes_read,
					i;
		char		buf[ORIOLEDB_BLCKSZ];

		do
		{
			bytes_read = OFileRead(file, buf, ORIOLEDB_BLCKSZ, offset,
								   WAIT_EVENT_DATA_FILE_READ);
			offset += bytes_read;
			if (bytes_read % sizeof(FileExtent) > 0)
				break;

			for (i = 0; i < bytes_read; i += sizeof(FileExtent))
			{
				extent = (FileExtent *) (buf + i);
				if (extent->len > 1)
					pg_atomic_fetch_add_u64(&metaPageBlkno->numFreeBlocks, (uint64) extent->len - 1);

				free_extent(desc, *extent);
				num_free_blocks--;
			}
		} while (num_free_blocks > 0 && bytes_read == ORIOLEDB_BLCKSZ);
		FileClose(file);

		return num_free_blocks == 0;
	}

	return false;
}

static void
index_descr_free(OIndexDescr *tree)
{
	if (tree->leafTupdesc)
		FreeTupleDesc(tree->leafTupdesc);
	if (tree->nonLeafTupdesc)
		FreeTupleDesc(tree->nonLeafTupdesc);
	if (tree->econtext)
		FreeExprContext(tree->econtext, false);
	if (tree->index_mctx)
	{
		MemoryContextDelete(tree->index_mctx);
	}
	checkpointable_tree_free(&tree->desc);
}

static void
index_descr_delete_from_hash(OIndexDescr *tree)
{
	bool		found;

	index_descr_free(tree);

	elog(DEBUG3, "index descr hash delete index (%u, %u, %u)",
		 tree->oids.datoid,
		 tree->oids.reloid,
		 tree->oids.relnode);

	(void) hash_search(oIndexDescrHash, &tree->oids,
					   HASH_REMOVE, &found);
	Assert(found);
}

static void
table_descr_free(OTableDescr *descr)
{
	int			i;

	elog(DEBUG3, "index descr hash delete for (%u, %u, %u)",
		 descr->oids.datoid,
		 descr->oids.reloid,
		 descr->oids.relnode);

	if (descr->toast)
	{
		descr->toast->refcnt--;
		if (!descr->toast->valid)
			index_descr_delete_from_hash(descr->toast);
	}

	if (descr->indices)
	{
		for (i = 0; i < descr->nIndices; i++)
			if (descr->indices[i])
			{
				descr->indices[i]->refcnt--;
				if (!descr->indices[i]->valid)
					index_descr_delete_from_hash(descr->indices[i]);
			}
		pfree(descr->indices);
	}

	if (descr->oldTuple)
		ExecDropSingleTupleTableSlot(descr->oldTuple);
	if (descr->newTuple)
		ExecDropSingleTupleTableSlot(descr->newTuple);
	if (descr->tupdesc)
		FreeTupleDesc(descr->tupdesc);
}


void
o_free_tmp_table_descr(OTableDescr *descr)
{
	int			i;

	if (descr->toast)
	{
		index_descr_free(descr->toast);
		pfree(descr->toast);
	}

	if (descr->indices)
	{
		for (i = 0; i < descr->nIndices; i++)
		{
			index_descr_free(descr->indices[i]);
			pfree(descr->indices[i]);
		}
		pfree(descr->indices);
	}

	if (descr->oldTuple)
		ExecDropSingleTupleTableSlot(descr->oldTuple);
	if (descr->newTuple)
		ExecDropSingleTupleTableSlot(descr->newTuple);
	if (descr->tupdesc)
		FreeTupleDesc(descr->tupdesc);
}


static void
table_descr_delete_from_hash(OTableDescr *descr)
{
	bool		found;

	table_descr_free(descr);
	(void) hash_search(oTableDescrHash, &descr->oids,
					   HASH_REMOVE, &found);
	Assert(found);
}

static void
fill_table_descr_common_fields(OTableDescr *descr, OTable *o_table)
{
	MemoryContext old_context;

	memset(descr, 0, sizeof(OTableDescr));
	old_context = MemoryContextSwitchTo(descrCxt);
	descr->refcnt = 0;
	descr->oids = o_table->oids;
	descr->tupdesc = o_table_tupdesc(o_table);
	descr->oldTuple = MakeSingleTupleTableSlot(descr->tupdesc,
											   &TTSOpsOrioleDB);
	descr->newTuple = MakeSingleTupleTableSlot(descr->tupdesc,
											   &TTSOpsOrioleDB);
	o_set_sys_cache_search_datoid(o_table->oids.datoid);
	MemoryContextSwitchTo(old_context);
}

static void
fill_table_descr(OTableDescr *descr, OTable *o_table)
{
	MemoryContext old_context;

	fill_table_descr_common_fields(descr, o_table);

	old_context = MemoryContextSwitchTo(descrCxt);
	o_table_descr_fill_indices(descr, o_table);
	MemoryContextSwitchTo(old_context);

	o_table_free(o_table);
}

void
o_fill_tmp_table_descr(OTableDescr *descr, OTable *o_table)
{
	MemoryContext old_context;
	OIndexNumber cur_ix;
	OIndex	   *index;
	OIndexDescr *indexDescr;

	fill_table_descr_common_fields(descr, o_table);

	old_context = MemoryContextSwitchTo(descrCxt);

	descr->nIndices = o_table->nindices;
	if (!o_table->has_primary)
		descr->nIndices++;

	descr->indices = (OIndexDescr **) palloc0(sizeof(OIndexDescr *) * descr->nIndices);
	for (cur_ix = 0; cur_ix < descr->nIndices; cur_ix++)
	{
		index = make_o_index(o_table, cur_ix);
		indexDescr = palloc0(sizeof(OIndexDescr));
		o_index_fill_descr(indexDescr, index, o_table);
		index_btree_desc_init(&indexDescr->desc, indexDescr->compress,
							  indexDescr->oids, index->indexType,
							  index->createOxid, indexDescr);
		free_o_index(index);
		descr->indices[cur_ix] = indexDescr;
	}

	index = make_o_index(o_table, TOASTIndexNumber);
	indexDescr = palloc0(sizeof(OIndexDescr));
	o_index_fill_descr(indexDescr, index, o_table);
	index_btree_desc_init(&indexDescr->desc, indexDescr->compress,
						  indexDescr->oids, index->indexType,
						  index->createOxid, indexDescr);
	free_o_index(index);
	descr->toast = indexDescr;

	o_find_toastable_attrs(descr);
	MemoryContextSwitchTo(old_context);
}

static OTableDescr *
create_table_descr(ORelOids oids)
{
	OTableDescr *descr;
	bool		found;
	OTable	   *o_table;
	bool		old_enable_stopevents;

	old_enable_stopevents = enable_stopevents;
	enable_stopevents = false;

	o_table = o_tables_get(oids);

	if (o_table == NULL)
	{
		enable_stopevents = old_enable_stopevents;
		return NULL;
	}

	descr = hash_search(oTableDescrHash,
						&o_table->oids,
						HASH_ENTER,
						&found);
	Assert(!found);

	fill_table_descr(descr, o_table);

	enable_stopevents = old_enable_stopevents;
	return descr;
}

/*
 * Finds tree with given oids in private table descriptor.
 */
OIndexNumber
find_tree_in_descr(OTableDescr *descr, ORelOids oids)
{
	int			i;

	for (i = 0; i < descr->nIndices; i++)
	{
		if (descr->indices[i]->oids.datoid == oids.datoid &&
			descr->indices[i]->oids.reloid == oids.reloid &&
			descr->indices[i]->oids.relnode == oids.relnode)
		{
			return i;
		}
	}

	if (descr->toast->oids.datoid == oids.datoid &&
		descr->toast->oids.reloid == oids.reloid &&
		descr->toast->oids.relnode == oids.relnode)
		return TOASTIndexNumber;

	return InvalidIndexNumber;
}

/*
 * o_fetch_table_descr fetches OTableDescr from cache, or creates a new one.
 */
OTableDescr *
o_fetch_table_descr(ORelOids oids)
{
	OTableDescr *table_descr;
	bool		found;

	table_descr = hash_search(oTableDescrHash, &oids, HASH_FIND, &found);

	if (!found)
		table_descr = create_table_descr(oids);

	return table_descr;
}

/*
 * o_fetch_index_descr fetches OIndexDescr for particular tree from cache, or
 * creates a new one.
 */
OIndexDescr *
o_fetch_index_descr(ORelOids oids, OIndexType type, bool lock, bool *nested)
{
	OIndexDescr *index_descr = NULL;

	if (lock)
		o_tables_rel_lock_extended(&oids, AccessShareLock, true);

	index_descr = get_index_descr(oids, type, true);

	if (!index_descr && lock)
	{
		o_tables_rel_unlock_extended(&oids, AccessShareLock, true);
	}

	return index_descr;
}


static void
cleanup_shared_root_info_pages(OPagePool *pool, SharedRootInfo *sharedRootInfo)
{
	BTreeRootInfo *rootInfo = &sharedRootInfo->rootInfo;

	FREE_PAGE_IF_VALID(pool, rootInfo->rootPageBlkno);
	if (OInMemoryBlknoIsValid(rootInfo->metaPageBlkno))
	{
		int			blkno,
					bufnum;
		BTreeMetaPage *meta_page = (BTreeMetaPage *) O_GET_IN_MEMORY_PAGE(rootInfo->metaPageBlkno);

		for (blkno = 0; blkno < 2; blkno++)
		{
			FREE_PAGE_IF_VALID(pool, meta_page->freeBuf.pages[blkno]);
			for (bufnum = 0; bufnum < 2; bufnum++)
			{
				FREE_PAGE_IF_VALID(pool, meta_page->nextChkp[bufnum].pages[blkno]);
				FREE_PAGE_IF_VALID(pool, meta_page->tmpBuf[bufnum].pages[blkno]);
			}
		}
	}
	FREE_PAGE_IF_VALID(pool, rootInfo->metaPageBlkno);
}

static void
init_shared_root_info(OPagePool *pool, SharedRootInfo *sharedRootInfo)
{
	BTreeMetaPage *meta_page;
	BTreeRootInfo *rootInfo = &sharedRootInfo->rootInfo;
	int			blkno,
				bufnum;

	sharedRootInfo->placeholder = false;
	rootInfo->rootPageBlkno = ppool_get_metapage(pool);
	rootInfo->metaPageBlkno = ppool_get_metapage(pool);
	rootInfo->rootPageChangeCount = O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(rootInfo->rootPageBlkno));

	if (!OInMemoryBlknoIsValid(rootInfo->rootPageBlkno) ||
		!OInMemoryBlknoIsValid(rootInfo->metaPageBlkno))
	{
		if (rootInfo->metaPageBlkno != OInvalidInMemoryBlkno)
		{
			ppool_free_page(pool, rootInfo->metaPageBlkno, NULL);
			rootInfo->metaPageBlkno = OInvalidInMemoryBlkno;
		}
		cleanup_shared_root_info_pages(pool, sharedRootInfo);
		elog(ERROR, "out of space for shared cache");
	}

	meta_page = (BTreeMetaPage *) O_GET_IN_MEMORY_PAGE(rootInfo->metaPageBlkno);
	for (blkno = 0; blkno < 2; blkno++)
	{
		meta_page->freeBuf.pages[blkno] = OInvalidInMemoryBlkno;
		for (bufnum = 0; bufnum < 2; bufnum++)
		{
			meta_page->nextChkp[bufnum].pages[blkno] = OInvalidInMemoryBlkno;
			meta_page->tmpBuf[bufnum].pages[blkno] = OInvalidInMemoryBlkno;
		}
	}
}

void
o_invalidate_descrs(Oid datoid, Oid reloid, Oid relfilenode)
{
	HASH_SEQ_STATUS scan_status;
	OTableDescr *tableDescr;
	OIndexDescr *indexDescr;

	if (!OidIsValid(datoid) || !OidIsValid(reloid) || !OidIsValid(relfilenode))
	{
		Assert(!OidIsValid(datoid) && !OidIsValid(reloid) && !OidIsValid(relfilenode));
		hash_seq_init(&scan_status, oTableDescrHash);
		while ((tableDescr = (OTableDescr *) hash_seq_search(&scan_status)) != NULL)
		{
			bool		delete = tableDescr->refcnt == 0;

			if (!delete)
				delete = !recreate_table_descr(tableDescr);

			if (delete)
				table_descr_delete_from_hash(tableDescr);
		}

		hash_seq_init(&scan_status, oIndexDescrHash);
		while ((indexDescr = (OIndexDescr *) hash_seq_search(&scan_status)) != NULL)
		{
			if (indexDescr->refcnt == 0)
				index_descr_delete_from_hash(indexDescr);
			else
				recreate_index_descr(indexDescr);
		}
	}
	else
	{
		ORelOids	oids = {datoid, reloid, relfilenode};
		bool		found;

		tableDescr = hash_search(oTableDescrHash, &oids, HASH_FIND, &found);
		if (found)
		{
			bool		delete = tableDescr->refcnt == 0;

			if (!delete)
				delete = !recreate_table_descr(tableDescr);

			if (delete)
				table_descr_delete_from_hash(tableDescr);
		}

		indexDescr = hash_search(oIndexDescrHash, &oids, HASH_FIND, &found);
		if (found)
		{
			if (indexDescr->refcnt == 0)
				index_descr_delete_from_hash(indexDescr);
			else
				recreate_index_descr(indexDescr);
		}
	}
}

SharedRootInfo *
o_find_shared_root_info(SharedRootInfoKey *key)
{
	OTuple		key_tuple,
				result_tuple;

	key_tuple.data = (Pointer) key;
	key_tuple.formatFlags = 0;

	result_tuple = o_btree_find_tuple_by_key(get_sys_tree(SYS_TREES_SHARED_ROOT_INFO),
											 &key_tuple, BTreeKeyNonLeafKey,
											 COMMITSEQNO_INPROGRESS, NULL,
											 CurrentMemoryContext, NULL);

	return (SharedRootInfo *) result_tuple.data;
}

void
o_insert_shared_root_placeholder(Oid datoid, Oid relnode)
{
	OTuple		sharedRootInfoTuple;
	SharedRootInfo sharedRootInfo;
	bool		inserted PG_USED_FOR_ASSERTS_ONLY;

	sharedRootInfoTuple.formatFlags = 0;
	sharedRootInfoTuple.data = (Pointer) &sharedRootInfo;

	memset(&sharedRootInfo, 0, sizeof(sharedRootInfo));
	sharedRootInfo.key.datoid = datoid;
	sharedRootInfo.key.relnode = relnode;
	sharedRootInfo.placeholder = true;
	sharedRootInfo.rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
	sharedRootInfo.rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
	sharedRootInfo.rootInfo.rootPageChangeCount = 0;

	inserted = o_btree_autonomous_insert(get_sys_tree(SYS_TREES_SHARED_ROOT_INFO),
										 sharedRootInfoTuple);
	Assert(inserted);
}

void
cleanup_btree(Oid datoid, Oid relnode, bool files)
{
	SharedRootInfoKey key;
	SharedRootInfo *shared = NULL;

	key.datoid = datoid;
	key.relnode = relnode;

	shared = o_find_shared_root_info(&key);

	if (shared)
	{
		bool		drop_result PG_USED_FOR_ASSERTS_ONLY;

		drop_result = o_drop_shared_root_info(datoid, relnode);
		Assert(drop_result);
		if (!shared->placeholder)
			o_btree_cleanup_pages(shared->rootInfo.rootPageBlkno,
								  shared->rootInfo.metaPageBlkno,
								  shared->rootInfo.rootPageChangeCount);
		pfree(shared);
	}
	if (files)
		cleanup_btree_files(key.datoid, key.relnode);
}

bool
o_drop_shared_root_info(Oid datoid, Oid relnode)
{
	SharedRootInfoKey key;
	OTuple		key_tuple;

	key.datoid = datoid;
	key.relnode = relnode;
	key_tuple.data = (Pointer) &key;
	key_tuple.formatFlags = 0;

	return o_btree_autonomous_delete(get_sys_tree(SYS_TREES_SHARED_ROOT_INFO),
									 key_tuple, BTreeKeyNonLeafKey, NULL);
}

static OIndexDescr *
get_index_descr(ORelOids ixOids, OIndexType ixType, bool miss_ok)
{
	bool		found;
	OIndexDescr *result;
	OIndex	   *oIndex;
	MemoryContext mcxt;

	result = hash_search(oIndexDescrHash, &ixOids, HASH_ENTER, &found);
	if (found)
		return result;

	oIndex = o_indices_get(ixOids, ixType);
	Assert(oIndex || miss_ok);
	if (!oIndex && miss_ok)
	{
		(void) hash_search(oIndexDescrHash, &ixOids, HASH_REMOVE, &found);
		Assert(found);
		return NULL;
	}
	mcxt = MemoryContextSwitchTo(descrCxt);
	o_index_fill_descr(result, oIndex, NULL);
	MemoryContextSwitchTo(mcxt);
	index_btree_desc_init(&result->desc, result->compress, result->oids,
						  oIndex->indexType, oIndex->createOxid, result);
	free_o_index(oIndex);

	return result;
}

static void
recreate_index_descr(OIndexDescr *descr)
{
	OIndex	   *oIndex;
	int			refcnt;
	MemoryContext mcxt;

	oIndex = o_indices_get(descr->oids, descr->desc.type);
	if (!oIndex)
	{
		descr->valid = false;
		return;
	}
	refcnt = descr->refcnt;
	index_descr_free(descr);
	mcxt = MemoryContextSwitchTo(descrCxt);
	o_index_fill_descr(descr, oIndex, NULL);
	MemoryContextSwitchTo(mcxt);
	index_btree_desc_init(&descr->desc, descr->compress, descr->oids,
						  oIndex->indexType, oIndex->createOxid, descr);
	descr->refcnt = refcnt;
	free_o_index(oIndex);
}

static void
o_table_descr_fill_indices(OTableDescr *descr, OTable *table)
{
	OIndexNumber cur_ix,
				ix_off = 0;

	descr->nIndices = table->nindices;
	if (!table->has_primary)
	{
		descr->nIndices++;
		ix_off = 1;
	}

	descr->indices = (OIndexDescr **) palloc0(sizeof(OIndexDescr *) * descr->nIndices);
	for (cur_ix = 0; cur_ix < descr->nIndices; cur_ix++)
	{
		ORelOids	ixOids;
		OIndexType	ixType;

		if (!table->has_primary && cur_ix == 0)
		{
			ixOids = table->oids;
			ixType = oIndexPrimary;
		}
		else
		{
			ixOids = table->indices[cur_ix - ix_off].oids;
			ixType = table->indices[cur_ix - ix_off].type;
		}

		descr->indices[cur_ix] = get_index_descr(ixOids, ixType, false);
		descr->indices[cur_ix]->refcnt++;
	}

	if (ORelOidsIsValid(table->toast_oids))
	{
		descr->toast = get_index_descr(table->toast_oids, oIndexToast, false);
		descr->toast->refcnt++;
	}
	else
		descr->toast = NULL;

	o_find_toastable_attrs(descr);
}

static bool
is_pk_attnum(OTableDescr *tableDescr, AttrNumber attnum)
{
	OIndexDescr *pk = GET_PRIMARY(tableDescr);
	int			i;

	for (i = 0; i < pk->nFields; i++)
	{
		if (attnum == pk->fields[i].tableAttnum)
			return true;
	}
	return false;
}

static void
o_find_toastable_attrs(OTableDescr *tableDescr)
{
	OIndexDescr *pk = GET_PRIMARY(tableDescr);
	TupleDesc	tupdesc = pk->leafTupdesc;
	List	   *toastable = NIL;
	ListCell   *lc;
	int			i,
				ctid_off = pk->primaryIsCtid ? 1 : 0;

	toastable = NIL;

	for (i = ctid_off; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (!att->attisdropped && att->attlen <= 0 && att->attstorage != 'p' &&
			!is_pk_attnum(tableDescr, i + 1))
			toastable = lappend_int(toastable, i);
	}

	if (toastable != NIL)
	{
		tableDescr->ntoastable = list_length(toastable);
		tableDescr->toastable = palloc(sizeof(AttrNumber) * tableDescr->ntoastable);
		i = 0;
		foreach(lc, toastable)
		{
			tableDescr->toastable[i] = lfirst_int(lc);
			i++;
		}
		list_free(toastable);
	}
	else
	{
		tableDescr->toastable = NULL;
		tableDescr->ntoastable = 0;
	}
}

/* fills field opclass fields and finds comparator for it */
void
oFillFieldOpClassAndComparator(OIndexField *field, Oid datoid, Oid opclassoid)
{
	OOpclass   *opclass;

	o_set_sys_cache_search_datoid(datoid);
	opclass = o_opclass_get(opclassoid);
	field->opclass = opclassoid;
	field->inputtype = opclass->inputtype;
	field->opfamily = opclass->opfamily;
	field->comparator = o_find_opclass_comparator(opclass, field->collation);

	Assert(field->comparator != NULL);
}

/*
 * Find opfamily omparator for given datatypes and collation.  Throws error
 * if not found.
 */
OComparator *
o_find_comparator(Oid opfamily, Oid lefttype, Oid righttype, Oid collation)
{
	OComparatorKey key = {
		.opfamily = opfamily,
		.lefttype = lefttype,
		.righttype = righttype,
		.collation = collation
	};
	OComparator *result;
	OComparator comparator;
	Oid			procOid;

	/*
	 * At first, try to find existing comparator in cache.
	 */
	if ((result = o_find_cached_comparator(&key)) != NULL)
		return result;

	/*
	 * If comparator isn't cached, then look for comparator with sort support
	 * function.
	 */
	Assert(OidIsValid(lefttype));
	Assert(OidIsValid(righttype));
	procOid =
		get_opfamily_proc(opfamily, lefttype, righttype, BTSORTSUPPORT_PROC);
	memset(&comparator, 0, sizeof(comparator));
	comparator.key = key;
	if (OidIsValid(procOid))
	{
		SortSupportData ssup;

		memset(&ssup, 0, sizeof(ssup));
		ssup.ssup_cxt = descrCxt;
		ssup.ssup_collation = collation;
		ssup.abbreviate = false;
		OidFunctionCall1(procOid, PointerGetDatum(&ssup));
		if (ssup.comparator != NULL)
		{
			comparator.haveSortSupport = true;
			comparator.ssup_cxt = ssup.ssup_cxt;
			comparator.ssup_extra = ssup.ssup_extra;
			comparator.ssup_comparator = ssup.comparator;
		}
	}

	/*
	 * Finally, look for plain comparison function.  Throw erro if not found.
	 */
	if (!comparator.haveSortSupport)
	{
		procOid =
			get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC);
		if (!OidIsValid(procOid))
		{
			HeapTuple	tup;
			Form_pg_opfamily opfamilyForm;

			tup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamily));
			Assert(HeapTupleIsValid(tup));
			opfamilyForm = (Form_pg_opfamily) GETSTRUCT(tup);

			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("opfamily %s doesn't contain comparison function for types %s and %s",
							NameStr(opfamilyForm->opfname),
							format_type_be(lefttype),
							format_type_be(righttype))));
		}
		fmgr_info(procOid, &comparator.finfo);
	}

	return o_add_comparator_to_cache(&comparator);
}

/*
 * Find opclass comparator in cache or create new one.
 */
static OComparator *
o_find_opclass_comparator(OOpclass *opclass, Oid collation)
{
	OComparatorKey key = {
		.opfamily = opclass->opfamily,
		.lefttype = opclass->inputtype,
		.righttype = opclass->inputtype,
		.collation = collation
	};
	OComparator *result;
	OComparator comparator;

	/*
	 * At first, try to find existing comparator in cache.
	 */
	if ((result = o_find_cached_comparator(&key)) != NULL)
		return result;

	memset(&comparator, 0, sizeof(comparator));
	comparator.key = key;

	/*
	 * If comparator isn't cached, then look for comparator with sort support
	 * function.
	 */
	Assert(OidIsValid(opclass->key.common.datoid)); /* ssup may use SysCache */
	o_set_syscache_hooks();
	if (MyDatabaseId == opclass->key.common.datoid &&
		OidIsValid(opclass->ssupOid))
	{
		SortSupportData ssup;
		FmgrInfo	finfo;

		memset(&finfo, 0, sizeof(FmgrInfo));
		memset(&ssup, 0, sizeof(ssup));
		ssup.ssup_cxt = descrCxt;
		ssup.ssup_collation = collation;
		ssup.abbreviate = false;

		o_proc_cache_fill_finfo(&finfo, opclass->ssupOid);

		FunctionCall1(&finfo, PointerGetDatum(&ssup));

		if (ssup.comparator != NULL)
		{
			comparator.haveSortSupport = true;
			comparator.ssup_cxt = ssup.ssup_cxt;
			comparator.ssup_extra = ssup.ssup_extra;
			comparator.ssup_comparator = ssup.comparator;
		}
	}

	/*
	 * Finally, look for plain comparison function.
	 */
	if (!comparator.haveSortSupport)
		o_proc_cache_fill_finfo(&comparator.finfo, opclass->cmpOid);
	o_reset_syscache_hooks();

	return o_add_comparator_to_cache(&comparator);
}

/*
 * Tries to find a comparator in the cache.
 */
static inline OComparator *
o_find_cached_comparator(OComparatorKey *key)
{
	OComparator *result;
	bool		found;

	/* compares with previous search */
	if (memcmp(key, &lastkey, sizeof(OComparatorKey)) == 0)
		return lastcmp;

	/* try to find in the cache */
	result = hash_search(comparatorCache, key, HASH_FIND, &found);
	if (found)
	{
		memcpy(&lastkey, key, sizeof(OComparatorKey));
		lastcmp = result;
		return result;
	}

	return NULL;
}

/*
 * Adds the comparator to the cache.
 */
static inline OComparator *
o_add_comparator_to_cache(OComparator *comparator)
{
	OComparator *cached;

	cached = hash_search(comparatorCache, &comparator->key, HASH_ENTER, NULL);
	memcpy(cached, comparator, sizeof(OComparator));

	memcpy(&lastkey, &comparator->key, sizeof(OComparatorKey));
	lastcmp = cached;

	return cached;
}

void
o_invalidate_comparator_cache(Oid opfamily, Oid lefttype, Oid righttype)
{
	OComparator *comparator;
	HASH_SEQ_STATUS scan_status;
	OComparatorKey key = {
		.opfamily = opfamily,
		.lefttype = lefttype,
		.righttype = righttype
	};

	if (key.opfamily == lastkey.opfamily &&
		key.lefttype == lastkey.lefttype &&
		key.righttype == lastkey.righttype)
		lastcmp = NULL;

	hash_seq_init(&scan_status, comparatorCache);
	while ((comparator = (OComparator *) hash_seq_search(&scan_status)) != NULL)
	{
		if (key.opfamily == comparator->key.opfamily &&
			key.lefttype == comparator->key.lefttype &&
			key.righttype == comparator->key.righttype)
		{
			Oid			collation = comparator->key.collation;

			if (comparator->ssup_extra)
				pfree(comparator->ssup_extra);
			key.collation = collation;
			(void) hash_search(comparatorCache, &key, HASH_REMOVE, NULL);
		}
	}
}

int
o_call_comparator(OComparator *comparator, Datum left, Datum right)
{
	int			ret;

	if (comparator->haveSortSupport)
	{
		SortSupportData ssup;

		memset(&ssup, 0, sizeof(ssup));
		ssup.ssup_cxt = comparator->ssup_cxt;
		ssup.ssup_collation = comparator->key.collation;
		ssup.ssup_extra = comparator->ssup_extra;
		ssup.abbreviate = false;
		ret = comparator->ssup_comparator(left, right, &ssup);
		comparator->ssup_extra = ssup.ssup_extra;
	}
	else
	{
		Datum		cmp;

		cmp = FunctionCall2Coll(&comparator->finfo, comparator->key.collation,
								left, right);
		ret = DatumGetInt32(cmp);
	}

	return ret;
}

/* Info needed to use an old-style comparison function as a sort comparator */
typedef struct
{
	FmgrInfo	flinfo;			/* lookup data for comparison function */
	FunctionCallInfoBaseData fcinfo;	/* reusable callinfo structure */
} SortShimExtra;

#define SizeForSortShimExtra(nargs) (offsetof(SortShimExtra, fcinfo) + SizeForFunctionCallInfo(nargs))

/*
 * Shim function for calling an old-style comparator
 *
 * This is essentially an inlined version of FunctionCall2Coll(), except
 * we assume that the FunctionCallInfoBaseData was already mostly set up by
 * PrepareSortSupportComparisonShim.
 */
static int
comparison_shim(Datum x, Datum y, SortSupport ssup)
{
	SortShimExtra *extra = (SortShimExtra *) ssup->ssup_extra;
	Datum		result;

	extra->fcinfo.args[0].value = x;
	extra->fcinfo.args[1].value = y;

	/* just for paranoia's sake, we reset isnull each time */
	extra->fcinfo.isnull = false;

	result = FunctionCallInvoke(&extra->fcinfo);

	/* Check for null result, since caller is clearly not expecting one */
	if (extra->fcinfo.isnull)
		elog(ERROR, "function %u returned NULL", extra->flinfo.fn_oid);

	return result;
}

void
o_finish_sort_support_function(OComparator *comparator, SortSupport ssup)
{
	if (comparator->haveSortSupport)
	{
		ssup->comparator = comparator->ssup_comparator;
		ssup->ssup_extra = comparator->ssup_extra;
	}
	else
	{
		SortShimExtra *extra;

		extra = (SortShimExtra *) MemoryContextAlloc(ssup->ssup_cxt,
													 SizeForSortShimExtra(2));

		memcpy(&extra->flinfo, &comparator->finfo, sizeof(FmgrInfo));

		/* We can initialize the callinfo just once and re-use it */
		InitFunctionCallInfoData(extra->fcinfo, &extra->flinfo, 2,
								 ssup->ssup_collation, NULL, NULL);
		extra->fcinfo.args[0].isnull = false;
		extra->fcinfo.args[1].isnull = false;

		ssup->ssup_extra = extra;
		ssup->comparator = comparison_shim;
	}
}

void
o_tableam_descr_init(void)
{
	HASHCTL		ctl;

	descrCxt = AllocSetContextCreate(TopMemoryContext,
									 "OrioleDB descriptors",
									 ALLOCSET_DEFAULT_SIZES);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ORelOids);
	ctl.entrysize = sizeof(OTableDescr);
	ctl.hcxt = descrCxt;
	oTableDescrHash = hash_create("OrioleDB table descriptors", 8,
								  &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ORelOids);
	ctl.entrysize = sizeof(OIndexDescr);
	ctl.hcxt = descrCxt;
	oIndexDescrHash = hash_create("OrioleDB index descriptors", 8,
								  &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OComparatorKey);
	ctl.entrysize = sizeof(OComparator);
	ctl.hcxt = descrCxt;
	comparatorCache = hash_create("OrioleDB comparators", 8,
								  &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static bool
recreate_table_descr(OTableDescr *descr)
{
	OTable	   *o_table;
	bool		old_enable_stopevents;
	int			refcnt;

	old_enable_stopevents = enable_stopevents;
	enable_stopevents = false;

	o_table = o_tables_get(descr->oids);
	if (!o_table)
		return false;

	refcnt = descr->refcnt;
	table_descr_free(descr);
	fill_table_descr(descr, o_table);
	descr->refcnt = refcnt;

	enable_stopevents = old_enable_stopevents;
	return true;
}

void
recreate_table_descr_by_oids(ORelOids oids)
{
	OTableDescr *descr;
	bool		found;

	descr = hash_search(oTableDescrHash, &oids, HASH_FIND, &found);

	if (found)
	{
		OIndexDescr *indexDescr;

		indexDescr = hash_search(oIndexDescrHash, &oids, HASH_FIND, &found);
		if (found)
			index_descr_delete_from_hash(indexDescr);
		recreate_table_descr(descr);
	}
	else
		(void) create_table_descr(oids);
}

void
table_descr_inc_refcnt(OTableDescr *descr)
{
	descr->refcnt++;
}

void
table_descr_dec_refcnt(OTableDescr *descr)
{
	Assert(descr->refcnt > 0);
	descr->refcnt--;
}

Datum
orioledb_get_table_descrs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS scan_status;
	OTableDescr *tableDescr;

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	hash_seq_init(&scan_status, oTableDescrHash);
	while ((tableDescr = (OTableDescr *) hash_seq_search(&scan_status)) != NULL)
	{
		Datum		values[4];
		bool		nulls[4] = {false};

		values[0] = tableDescr->oids.datoid;
		values[1] = tableDescr->oids.reloid;
		values[2] = tableDescr->oids.relnode;
		values[3] = tableDescr->refcnt;
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

Datum
orioledb_get_index_descrs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS scan_status;
	OIndexDescr *indexDescr;

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	hash_seq_init(&scan_status, oIndexDescrHash);
	while ((indexDescr = (OIndexDescr *) hash_seq_search(&scan_status)) != NULL)
	{
		Datum		values[4];
		bool		nulls[4] = {false};

		values[0] = indexDescr->oids.datoid;
		values[1] = indexDescr->oids.reloid;
		values[2] = indexDescr->oids.relnode;
		values[3] = indexDescr->refcnt;
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

void
o_invalidate_undo_item_callback(UndoLocation location, UndoStackItem *baseItem,
								OXid oxid, bool abort, bool changeCountsValid)
{
	InvalidateUndoStackItem *invalidateItem = (InvalidateUndoStackItem *) baseItem;

	if (abort && !(invalidateItem->flags & O_INVALIDATE_OIDS_ON_ABORT))
		return;

	if (!abort && !(invalidateItem->flags & O_INVALIDATE_OIDS_ON_COMMIT))
		return;

	o_invalidate_oids(invalidateItem->oids);
}

void
o_add_invalidate_undo_item(ORelOids oids, uint32 flags)
{
	UndoLocation location;
	InvalidateUndoStackItem *item;
	LocationIndex size;

	size = sizeof(InvalidateUndoStackItem);
	item = (InvalidateUndoStackItem *) get_undo_record_unreserved(UndoReserveTxn,
																  &location,
																  MAXALIGN(size));
	item->oids = oids;
	item->flags = flags;
	item->header.base.type = InvalidateUndoItemType;
	item->header.base.indexType = oIndexPrimary;
	item->header.base.itemSize = size;

	add_new_undo_stack_item(location);
	release_undo_size(UndoReserveTxn);
}
