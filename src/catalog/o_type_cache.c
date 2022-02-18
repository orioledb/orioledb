/*-------------------------------------------------------------------------
 *
 * o_type_cache.c
 *		Generic interface for type cache duplicate trees.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_type_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/modify.h"
#include "catalog/o_type_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/wal.h"
#include "transam/oxid.h"
#include "tuple/toast.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

Oid			o_type_cmp_datoid = InvalidOid;

static Pointer o_type_cache_get_from_tree(OTypeCache *type_cache, Oid datoid,
										  Oid enumoid, XLogRecPtr cur_lsn);
static Pointer o_type_cache_get_from_toast_tree(OTypeCache *type_cache,
												Oid datoid, Oid oid,
												XLogRecPtr cur_lsn);
static bool o_type_cache_add(OTypeCache *type_cache, Oid datoid, Oid oid,
							 XLogRecPtr insert_lsn, Pointer entry);
static bool o_type_cache_update(OTypeCache *type_cache, Pointer updated_entry);

static BTreeDescr *oTypeCacheToastGetBTreeDesc(void *arg);
static uint32 oTypeCacheToastGetMaxChunkSize(void *key, void *arg);
static void oTypeCacheToastUpdateKey(void *key, uint32 offset, void *arg);
static void *oTypeCacheToastGetNextKey(void *key, void *arg);
static OTuple oTypeCacheToastCreateTuple(void *key, Pointer data,
										 uint32 offset, int length,
										 void *arg);
static OTuple oTypeCacheToastCreateKey(void *key, uint32 offset, void *arg);
static Pointer oTypeCacheToastGetTupleData(OTuple tuple, void *arg);
static uint32 oTypeCacheToastGetTupleOffset(OTuple tuple, void *arg);
static uint32 oTypeCacheToastGetTupleDataSize(OTuple tuple, void *arg);


ToastAPI	oTypeCacheToastAPI = {
	.getBTreeDesc = oTypeCacheToastGetBTreeDesc,
	.getMaxChunkSize = oTypeCacheToastGetMaxChunkSize,
	.updateKey = oTypeCacheToastUpdateKey,
	.getNextKey = oTypeCacheToastGetNextKey,
	.createTuple = oTypeCacheToastCreateTuple,
	.createKey = oTypeCacheToastCreateKey,
	.getTupleData = oTypeCacheToastGetTupleData,
	.getTupleOffset = oTypeCacheToastGetTupleOffset,
	.getTupleDataSize = oTypeCacheToastGetTupleDataSize,
	.deleteLogFullTuple = true,
	.versionCallback = NULL
};

static MemoryContext typecacheCxt = NULL;
static HTAB *typecache_fastcache;

/*
 * Initializes the enum B-tree memory.
 */
void
o_typecaches_init(void)
{
	HASHCTL		ctl;

	typecacheCxt = AllocSetContextCreate(TopMemoryContext,
										 "OrioleDB typecaches fastcache context",
										 ALLOCSET_DEFAULT_SIZES);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OTypeHashKey);
	ctl.entrysize = sizeof(OTypeHashEntry);
	ctl.hcxt = typecacheCxt;
	typecache_fastcache = hash_create("OrioleDB typecaches fastcache", 8,
									  &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	o_enum_cache_init(typecacheCxt, typecache_fastcache);
	o_enumoid_cache_init(typecacheCxt, typecache_fastcache);
	o_range_cache_init(typecacheCxt, typecache_fastcache);
	o_type_element_cache_init(typecacheCxt, typecache_fastcache);
	o_record_cache_init(typecacheCxt, typecache_fastcache);
}

/*
 * Initializes the enum B-tree memory.
 */
OTypeCache *
o_create_type_cache(int sys_tree_num, bool is_toast,
					Oid classoid, HTAB *fast_cache,
					MemoryContext mcxt, OTypeCacheFuncs *funcs)
{
	OTypeCache *type_cache;

	Assert(fast_cache);
	Assert(funcs);

	type_cache = MemoryContextAllocZero(mcxt, sizeof(OTypeCache));
	type_cache->sys_tree_num = sys_tree_num;
	type_cache->is_toast = is_toast;
	type_cache->classoid = classoid;
	type_cache->fast_cache = fast_cache;
	type_cache->mcxt = mcxt;
	type_cache->funcs = funcs;

#ifdef USE_ASSERT_CHECKING
	Assert(type_cache->funcs->free_entry);
	Assert(type_cache->funcs->fill_entry);
	if (type_cache->is_toast)
	{
		Assert(type_cache->funcs->toast_serialize_entry);
		Assert(type_cache->funcs->toast_deserialize_entry);
	}
#endif

	return type_cache;
}

/*
 * GetSysCacheHashValue for OID without SysCache usage
 */
static inline OTypeHashKey
o_type_cache_GetSysCacheHashValue(Oid oid)
{
	uint32		hashValue = 0;
	uint32		oneHash;

	oneHash = murmurhash32(oid);

	hashValue ^= oneHash;

	return hashValue;
}

static void
invalidate_fastcache_entry(int cacheid, uint32 hashvalue)
{
	bool		found;
	OTypeHashEntry *fast_cache_entry;

	fast_cache_entry = (OTypeHashEntry *) hash_search(typecache_fastcache,
													  &hashvalue,
													  HASH_REMOVE,
													  &found);

	if (found)
	{
		ListCell   *lc;

		foreach(lc, fast_cache_entry->tree_entries)
		{
			OTypeHashTreeEntry *tree_entry =
			(OTypeHashTreeEntry *) lfirst(lc);

			if (tree_entry->type_cache)
			{
				OTypeCache *type_cache = tree_entry->type_cache;

				if (!memcmp(&type_cache->last_fast_cache_key,
							&fast_cache_entry->key,
							sizeof(OTypeHashKey)))
				{
					memset(&type_cache->last_fast_cache_key, 0,
						   sizeof(OTypeHashKey));
					type_cache->last_fast_cache_entry = NULL;
				}
				tree_entry->type_cache->funcs->free_entry(tree_entry->entry);
			}
			if (tree_entry->link != 0)
			{
				invalidate_fastcache_entry(cacheid, tree_entry->link);
			}
		}
		list_free_deep(fast_cache_entry->tree_entries);
	}
}

void
orioledb_syscache_type_hook(Datum arg, int cacheid, uint32 hashvalue)
{
	if (typecache_fastcache)
		invalidate_fastcache_entry(cacheid, hashvalue);
}

Pointer
o_type_cache_search(OTypeCache *type_cache, Oid datoid,
					Oid oid, XLogRecPtr cur_lsn)
{
	bool		found;
	OTypeHashKey cur_fast_cache_key;
	OTypeHashEntry *fast_cache_entry;
	Pointer		tree_entry;
	MemoryContext prev_context;
	OTypeHashTreeEntry *new_entry;

	cur_fast_cache_key = o_type_cache_GetSysCacheHashValue(oid);

	/* fast search */
	if (!memcmp(&cur_fast_cache_key, &type_cache->last_fast_cache_key,
				sizeof(OTypeHashKey)) &&
		type_cache->last_fast_cache_entry)
	{
		OTypeKey   *entry_type_key =
		(OTypeKey *) type_cache->last_fast_cache_entry;

		if (entry_type_key->datoid == datoid && entry_type_key->oid == oid)
			return type_cache->last_fast_cache_entry;
	}

	/* cache search */
	fast_cache_entry = (OTypeHashEntry *) hash_search(type_cache->fast_cache,
													  &cur_fast_cache_key, HASH_ENTER, &found);
	if (found)
	{
		ListCell   *lc;

		foreach(lc, fast_cache_entry->tree_entries)
		{
			OTypeHashTreeEntry *tree_entry =
			(OTypeHashTreeEntry *) lfirst(lc);

			if (tree_entry->type_cache == type_cache)
			{
				OTypeKey   *entry_type_key = (OTypeKey *) tree_entry->entry;

				if (entry_type_key->datoid == datoid &&
					entry_type_key->oid == oid)
				{
					memcpy(&type_cache->last_fast_cache_key,
						   &cur_fast_cache_key,
						   sizeof(OTypeHashKey));
					type_cache->last_fast_cache_entry = tree_entry->entry;
					return type_cache->last_fast_cache_entry;
				}
			}
		}
	}
	else
		fast_cache_entry->tree_entries = NIL;

	prev_context = MemoryContextSwitchTo(type_cache->mcxt);
	if (type_cache->is_toast)
		tree_entry = o_type_cache_get_from_toast_tree(type_cache, datoid,
													  oid, cur_lsn);
	else
		tree_entry = o_type_cache_get_from_tree(type_cache, datoid,
												oid, cur_lsn);
	if (tree_entry == NULL)
	{
		MemoryContextSwitchTo(prev_context);
		return NULL;
	}
	new_entry = palloc0(sizeof(OTypeHashTreeEntry));
	new_entry->type_cache = type_cache;
	new_entry->entry = tree_entry;
	if (type_cache->funcs->fastcache_get_link_oid)
	{
		Oid			link_oid = type_cache->funcs->fastcache_get_link_oid(tree_entry);
		OTypeHashKey link_key = o_type_cache_GetSysCacheHashValue(link_oid);
		OTypeHashTreeEntry *link_entry;
		OTypeHashEntry *link_cache_entry;

		link_entry = palloc0(sizeof(OTypeHashTreeEntry));
		link_entry->type_cache = NULL;
		link_entry->link = cur_fast_cache_key;
		new_entry->link = link_key;
		memcpy(&new_entry->link,
			   &link_key,
			   sizeof(OTypeHashKey));

		link_cache_entry = (OTypeHashEntry *)
			hash_search(type_cache->fast_cache, &link_key, HASH_ENTER, &found);
		if (!found)
			link_cache_entry->tree_entries = NIL;
		link_cache_entry->tree_entries = lappend(link_cache_entry->tree_entries,
												 link_entry);
	}

	fast_cache_entry->tree_entries = lappend(fast_cache_entry->tree_entries,
											 new_entry);

	MemoryContextSwitchTo(prev_context);

	memcpy(&type_cache->last_fast_cache_key,
		   &cur_fast_cache_key,
		   sizeof(OTypeHashKey));
	type_cache->last_fast_cache_entry = new_entry->entry;
	return type_cache->last_fast_cache_entry;
}

static TupleFetchCallbackResult
o_type_cache_get_by_lsn_callback(OTuple tuple, OXid tupOxid, CommitSeqNo csn,
								 void *arg,
								 TupleFetchCallbackCheckType check_type)
{
	OTypeToastChunkKey *tuple_key = (OTypeToastChunkKey *) tuple.data;
	XLogRecPtr *cur_lsn = (XLogRecPtr *) arg;

	if (check_type != OTupleFetchCallbackKeyCheck)
		return OTupleFetchNext;

	if (tuple_key->type_key.insert_lsn < *cur_lsn)
		return OTupleFetchMatch;
	else
		return OTupleFetchNext;
}

Pointer
o_type_cache_get_from_toast_tree(OTypeCache *type_cache, Oid datoid,
								 Oid oid, XLogRecPtr cur_lsn)
{
	Pointer		data;
	Size		dataLength;
	Pointer		result = NULL;
	BTreeDescr *td = get_sys_tree(type_cache->sys_tree_num);
	OTypeToastKeyBound toast_key = {
		.chunk_key = {.type_key = {.datoid = datoid,
				.oid = oid,
		.insert_lsn = cur_lsn},
		.offset = 0},
		.lsn_cmp = false
	};

	data = generic_toast_get_any_with_callback(&oTypeCacheToastAPI,
											   (Pointer) &toast_key,
											   &dataLength,
											   COMMITSEQNO_NON_DELETED,
											   td,
											   o_type_cache_get_by_lsn_callback,
											   &cur_lsn);
	if (data == NULL)
		return NULL;
	result = type_cache->funcs->toast_deserialize_entry(type_cache->mcxt,
														data, dataLength);
	pfree(data);

	return result;
}

Pointer
o_type_cache_get_from_tree(OTypeCache *type_cache, Oid datoid,
						   Oid oid, XLogRecPtr cur_lsn)
{
	BTreeDescr *td = get_sys_tree(type_cache->sys_tree_num);
	BTreeIterator *it;
	OTuple		last_tup;
	OTypeKeyBound key;

	key.datoid = datoid;
	key.oid = oid;

	it = o_btree_iterator_create(td, (Pointer) &key, BTreeKeyBound,
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	O_TUPLE_SET_NULL(last_tup);
	do
	{
		OTuple		tup = o_btree_iterator_fetch(it, NULL, (Pointer) &key,
												 BTreeKeyBound, true, NULL);
		OTypeKey   *type_key;

		if (O_TUPLE_IS_NULL(tup))
			break;

		if (!O_TUPLE_IS_NULL(last_tup))
			pfree(last_tup.data);

		type_key = (OTypeKey *) tup.data;
		if (type_key->insert_lsn > cur_lsn)
			break;
		last_tup = tup;
	} while (true);

	btree_iterator_free(it);

	return last_tup.data;
}

static inline void
o_type_cache_fill_locktag(LOCKTAG *tag, Oid datoid, Oid oid, Oid classoid,
						  int lockmode)
{
	Assert(lockmode == AccessShareLock || lockmode == AccessExclusiveLock);
	memset(tag, 0, sizeof(LOCKTAG));
	SET_LOCKTAG_OBJECT(*tag, datoid, classoid, oid, 0);
}

static void
o_type_cache_lock(Oid datoid, Oid oid, Oid classoid, int lockmode)
{
	LOCKTAG		locktag;

	o_type_cache_fill_locktag(&locktag, datoid, oid, classoid, lockmode);

	LockAcquire(&locktag, lockmode, false, false);
}

static void
o_type_cache_unlock(Oid datoid, Oid oid, Oid classoid, int lockmode)
{
	LOCKTAG		locktag;

	o_type_cache_fill_locktag(&locktag, datoid, oid, classoid, lockmode);

	if (!LockRelease(&locktag, lockmode, false))
	{
		elog(ERROR, "Can not release %s typecache lock on datoid = %d, "
			 "oid = %d",
			 lockmode == AccessShareLock ? "share" : "exclusive",
			 datoid, oid);
	}
}

/* Non-key fields of entry should be filled before call */
bool
o_type_cache_add(OTypeCache *type_cache, Oid datoid, Oid oid,
				 XLogRecPtr insert_lsn, Pointer entry)
{
	bool		inserted;
	OTypeKey   *type_key = (OTypeKey *) entry;
	BTreeDescr *desc = get_sys_tree(type_cache->sys_tree_num);

	type_key->datoid = datoid;
	type_key->oid = oid;
	type_key->insert_lsn = insert_lsn;
	type_key->deleted = false;

	if (!type_cache->is_toast)
	{
		OTuple		tup;

		tup.formatFlags = 0;
		tup.data = entry;
		inserted = o_btree_autonomous_insert(desc, tup);
	}
	else
	{
		Pointer		data;
		int			len;
		OTypeToastKeyBound toast_key = {0};
		OAutonomousTxState state;

		toast_key.chunk_key.type_key = *type_key;
		toast_key.chunk_key.offset = 0;
		toast_key.lsn_cmp = true;

		data = type_cache->funcs->toast_serialize_entry(entry, &len);

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			inserted = generic_toast_insert(&oTypeCacheToastAPI,
											(Pointer) &toast_key,
											data, len,
											get_current_oxid(),
											COMMITSEQNO_INPROGRESS,
											desc);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
		pfree(data);
	}
	return inserted;
}

static OBTreeWaitCallbackAction
o_type_cache_wait_callback(BTreeDescr *descr,
						   OTuple tup, OTuple *newtup, OXid oxid, OTupleXactInfo xactInfo,
						   RowLockMode *lock_mode, BTreeLocationHint *hint,
						   void *arg)
{
	return OBTreeCallbackActionXidWait;
}

static OBTreeModifyCallbackAction
o_type_cache_update_callback(BTreeDescr *descr,
							 OTuple tup, OTuple *newtup, OXid oxid, OTupleXactInfo xactInfo,
							 RowLockMode *lock_mode, BTreeLocationHint *hint,
							 void *arg)
{
	return OBTreeCallbackActionUpdate;
}


static BTreeModifyCallbackInfo callbackInfo =
{
	.waitCallback = o_type_cache_wait_callback,
	.modifyCallback = o_type_cache_update_callback,
	.insertToDeleted = o_type_cache_update_callback,
	.arg = NULL
};

bool
o_type_cache_update(OTypeCache *type_cache, Pointer updated_entry)
{
	bool		result;
	OTypeKeyBound key;
	OTypeKey   *type_key;
	BTreeDescr *desc = get_sys_tree(type_cache->sys_tree_num);

	type_key = (OTypeKey *) updated_entry;

	key.datoid = type_key->datoid;
	key.oid = type_key->oid;

	if (!type_cache->is_toast)
	{
		OAutonomousTxState state;
		OTuple		tup;

		tup.formatFlags = 0;
		tup.data = updated_entry;

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = o_btree_modify(desc, BTreeOperationUpdate,
									tup, BTreeKeyLeafTuple,
									(Pointer) &key, BTreeKeyBound,
									get_current_oxid(), COMMITSEQNO_INPROGRESS,
									RowLockNoKeyUpdate, NULL,
									&callbackInfo) == OBTreeModifyResultUpdated;
			if (result)
				o_wal_update(desc, tup);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
	}
	else
	{
		Pointer		data;
		int			len;
		OTypeToastKeyBound toast_key = {0};
		OAutonomousTxState state;

		toast_key.chunk_key.type_key = *type_key;
		toast_key.chunk_key.offset = 0;
		toast_key.lsn_cmp = true;

		data = type_cache->funcs->toast_serialize_entry(updated_entry, &len);

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = generic_toast_update(&oTypeCacheToastAPI,
										  (Pointer) &toast_key,
										  data, len,
										  get_current_oxid(),
										  COMMITSEQNO_INPROGRESS,
										  desc);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
	}
	return result;
}

void
o_type_cache_add_if_needed(OTypeCache *type_cache, Oid datoid,
						   Oid oid, XLogRecPtr insert_lsn, Pointer arg)
{
	Pointer		entry = NULL;
	bool		inserted PG_USED_FOR_ASSERTS_ONLY;

	o_type_cache_lock(datoid, oid, type_cache->classoid,
					  AccessExclusiveLock);

	entry = o_type_cache_search(type_cache, datoid, oid, insert_lsn);
	if (entry != NULL)
	{
		/* it's already exist in B-tree */
		return;
	}

	type_cache->funcs->fill_entry(&entry, datoid, oid, insert_lsn, arg);

	Assert(entry);

	/*
	 * All done, now try to insert into B-tree.
	 */
	inserted = o_type_cache_add(type_cache, datoid, oid,
								insert_lsn, entry);
	Assert(inserted);
	o_type_cache_unlock(datoid, oid, type_cache->classoid,
						AccessExclusiveLock);
	type_cache->funcs->free_entry(entry);
}

void
o_type_cache_update_if_needed(OTypeCache *type_cache,
							  Oid datoid, Oid oid, Pointer arg)
{
	Pointer		entry = NULL;
	XLogRecPtr	cur_lsn = GetXLogWriteRecPtr();
	OTypeKey   *type_key;
	bool		updated PG_USED_FOR_ASSERTS_ONLY;

	o_type_cache_lock(datoid, oid, type_cache->classoid,
					  AccessExclusiveLock);

	entry = o_type_cache_search(type_cache, datoid, oid, cur_lsn);
	if (entry == NULL)
	{
		/* it's not exist in B-tree */
		return;
	}

	type_key = (OTypeKey *) entry;
	type_cache->funcs->fill_entry(&entry, datoid, oid,
								  type_key->insert_lsn, arg);

	updated = o_type_cache_update(type_cache, entry);
	Assert(updated);
	o_type_cache_unlock(datoid, oid, type_cache->classoid,
						AccessExclusiveLock);
}

bool
o_type_cache_delete(OTypeCache *type_cache, Oid datoid, Oid oid)
{
	Pointer		entry;
	OTypeKey   *type_key;


	entry = o_type_cache_search(type_cache, datoid, oid,
								GetXLogWriteRecPtr());

	if (entry == NULL)
		return false;

	if (type_cache->funcs->delete_hook)
		type_cache->funcs->delete_hook(entry);

	type_key = (OTypeKey *) entry;
	type_key->deleted = true;

	return o_type_cache_update(type_cache, entry);
}

void
o_type_cache_delete_by_lsn(OTypeCache *type_cache, XLogRecPtr lsn)
{
	BTreeIterator *it;
	BTreeDescr *td = get_sys_tree(type_cache->sys_tree_num);

	it = o_btree_iterator_create(td, NULL, BTreeKeyNone,
								 COMMITSEQNO_NON_DELETED, ForwardScanDirection);

	do
	{
		bool		end;
		BTreeLocationHint hint;
		OTuple		tup = btree_iterate_raw(it, NULL, BTreeKeyNone,
											false, &end, &hint);
		OTypeKey   *type_key;
		OTuple		key_tup;

		if (O_TUPLE_IS_NULL(tup))
		{
			if (end)
				break;
			else
				continue;
		}

		type_key = (OTypeKey *) tup.data;
		key_tup.formatFlags = 0;
		key_tup.data = (Pointer) type_key;

		if (type_key->insert_lsn < lsn && type_key->deleted)
		{
			bool		result PG_USED_FOR_ASSERTS_ONLY;

			if (!type_cache->is_toast)
			{
				result = o_btree_autonomous_delete(td, key_tup, BTreeKeyNonLeafKey, &hint);
			}
			else
			{
				OTypeToastKeyBound toast_key = {0};
				OAutonomousTxState state;

				toast_key.chunk_key.type_key = *type_key;
				toast_key.chunk_key.offset = 0;
				toast_key.lsn_cmp = true;

				start_autonomous_transaction(&state);
				PG_TRY();
				{
					result = generic_toast_delete(&oTypeCacheToastAPI,
												  (Pointer) &toast_key,
												  get_current_oxid(),
												  COMMITSEQNO_NON_DELETED,
												  td);
				}
				PG_CATCH();
				{
					abort_autonomous_transaction(&state);
					PG_RE_THROW();
				}
				PG_END_TRY();
				finish_autonomous_transaction(&state);
			}

			Assert(result);
		}
	} while (true);

	btree_iterator_free(it);
}


static BTreeDescr *
oTypeCacheToastGetBTreeDesc(void *arg)
{
	BTreeDescr *desc = (BTreeDescr *) arg;

	return desc;
}

static uint32
oTypeCacheToastGetMaxChunkSize(void *key, void *arg)
{
	uint32		max_chunk_size;

	max_chunk_size =
		MAXALIGN_DOWN((O_BTREE_MAX_TUPLE_SIZE * 3 -
					   MAXALIGN(sizeof(OTypeToastChunkKey))) / 3) -
		offsetof(OTypeToastChunk, data);

	return max_chunk_size;
}

static void
oTypeCacheToastUpdateKey(void *key, uint32 offset, void *arg)
{
	OTypeToastKeyBound *ckey = (OTypeToastKeyBound *) key;

	ckey->chunk_key.offset = offset;
}

static void *
oTypeCacheToastGetNextKey(void *key, void *arg)
{
	OTypeToastKeyBound *ckey = (OTypeToastKeyBound *) key;
	static OTypeToastKeyBound nextKey;

	nextKey = *ckey;
	nextKey.chunk_key.type_key.oid++;
	nextKey.chunk_key.offset = 0;

	return (Pointer) &nextKey;
}

static OTuple
oTypeCacheToastCreateTuple(void *key, Pointer data, uint32 offset,
						   int length, void *arg)
{
	OTypeToastKeyBound *bound = (OTypeToastKeyBound *) key;
	OTypeToastChunk *chunk;
	OTuple		result;

	bound->chunk_key.offset = offset;

	chunk = (OTypeToastChunk *) palloc0(offsetof(OTypeToastChunk, data) +
										length);
	chunk->key = bound->chunk_key;
	chunk->dataLength = length;
	memcpy(chunk->data, data + offset, length);

	result.data = (Pointer) chunk;
	result.formatFlags = 0;

	return result;
}

static OTuple
oTypeCacheToastCreateKey(void *key, uint32 offset, void *arg)
{
	OTypeToastChunkKey *ckey = (OTypeToastChunkKey *) key;
	OTypeToastChunkKey *ckey_copy;
	OTuple		result;

	ckey_copy = (OTypeToastChunkKey *) palloc(sizeof(OTypeToastChunkKey));
	*ckey_copy = *ckey;

	result.data = (Pointer) ckey_copy;
	result.formatFlags = 0;

	return result;
}

static Pointer
oTypeCacheToastGetTupleData(OTuple tuple, void *arg)
{
	OTypeToastChunk *chunk = (OTypeToastChunk *) tuple.data;

	return chunk->data;
}

static uint32
oTypeCacheToastGetTupleOffset(OTuple tuple, void *arg)
{
	OTypeToastChunk *chunk = (OTypeToastChunk *) tuple.data;

	return chunk->key.offset;
}

static uint32
oTypeCacheToastGetTupleDataSize(OTuple tuple, void *arg)
{
	OTypeToastChunk *chunk = (OTypeToastChunk *) tuple.data;

	return chunk->dataLength;
}

/*
 * Gets prosrc and probin strings for the procedure from cache and stores
 * it to the procedure.
 */
void
o_type_procedure_fill(Oid procoid, OProcedure *proc)
{
	char	   *prosrc = NULL,
			   *probin = NULL;

	fmgr_symbol(procoid, &probin, &prosrc);

	if (!prosrc && !probin)
		elog(ERROR, "function %u not found", procoid);

	if (prosrc != NULL)
	{
		strlcpy(proc->prosrc, prosrc, O_OPCLASS_PROSRC_MAXLEN);
		pfree(prosrc);
	}

	if (probin != NULL)
	{
		strlcpy(proc->probin, probin, O_OPCLASS_PROSRC_MAXLEN);
		pfree(probin);
	}
}

void
o_type_procedure_fill_finfo(FmgrInfo *finfo, OProcedure *cmp_proc,
							Oid cmp_oid, short fn_args)
{
	memset(finfo, 0, sizeof(FmgrInfo));

	if (strlen(cmp_proc->probin) == 0)
	{
		Oid			cmpoid;

		cmpoid = fmgr_internal_function(cmp_proc->prosrc);
		finfo->fn_oid = cmpoid;
		finfo->fn_addr = fmgr_builtins[fmgr_builtin_oid_index[cmpoid]].func;
		finfo->fn_stats = TRACK_FUNC_ALL;
	}
	else
	{
		finfo->fn_oid = cmp_oid;
		finfo->fn_addr = load_external_function(cmp_proc->probin,
												cmp_proc->prosrc,
												false,
												NULL);
		finfo->fn_stats = TRACK_FUNC_OFF;
	}
	finfo->fn_nargs = fn_args;
	finfo->fn_strict = true;
	finfo->fn_retset = false;
	finfo->fn_mcxt = CurrentMemoryContext;
}

void
custom_type_add_if_needed(Oid datoid, Oid typoid, XLogRecPtr insert_lsn)
{
	Form_pg_type typeform;
	HeapTuple	tuple = NULL;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	Assert(tuple);
	typeform = (Form_pg_type) GETSTRUCT(tuple);

	switch (typeform->typtype)
	{
		case TYPTYPE_COMPOSITE:
			if (typeform->typtypmod == -1)
			{
				o_record_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
											 NULL);
			}
			break;
		case TYPTYPE_RANGE:
			o_range_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
										NULL);
			break;
		case TYPTYPE_ENUM:
			o_enum_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
									   NULL);
			break;
		case TYPTYPE_DOMAIN:
			custom_type_add_if_needed(datoid, typeform->typbasetype,
									  insert_lsn);
			break;
		default:
			if (typeform->typcategory == TYPCATEGORY_ARRAY)
			{
				o_type_element_cache_add_if_needed(datoid, typeform->typelem,
												   insert_lsn, NULL);
				custom_type_add_if_needed(datoid, typeform->typelem,
										  insert_lsn);
			}
			break;
	}
	if (tuple != NULL)
		ReleaseSysCache(tuple);
}

/*
 * Inserts type elements for all fields of the o_table to the typecache.
 */
void
custom_types_add_all(OTable *o_table)
{
	int			cur_field;
	XLogRecPtr	cur_lsn = GetXLogWriteRecPtr();

	for (cur_field = 0; cur_field < o_table->nfields; cur_field++)
		custom_type_add_if_needed(o_table->oids.datoid,
								  o_table->fields[cur_field].typid,
								  cur_lsn);
}
