/*-------------------------------------------------------------------------
 *
 * o_sys_cache.c
 *		Generic interface for sys cache duplicate trees.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_sys_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "access/hash.h"
#include "btree/btree.h"
#include "btree/modify.h"
#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "transam/oxid.h"
#include "tuple/toast.h"
#include "utils/planner.h"

#include "access/heaptoast.h"
#include "commands/defrem.h"
#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif
#include "catalog/pg_aggregate.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_range.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "executor/functions.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

typedef struct OSysCacheHashTreeEntry
{
	OSysCache  *sys_cache;		/* If NULL only link stored */
	Pointer		entry;
} OSysCacheHashTreeEntry;
typedef struct OSysCacheHashEntry
{
	OSysCacheHashKey key;
	List	   *tree_entries;	/* list of OSysCacheHashTreeEntry-s that used
								 * because we store entries for all sys caches
								 * in same fastcache for simpler invalidation
								 * of dependent objects */
} OSysCacheHashEntry;

typedef struct OCacheIdMapEntry
{
	int			cacheId;
	OSysCache  *sys_cache;
} OCacheIdMapEntry;

static Pointer o_sys_cache_get_from_tree(OSysCache *sys_cache,
										 int nkeys,
										 OSysCacheKey *key);
static Pointer o_sys_cache_get_from_toast_tree(OSysCache *sys_cache,
											   OSysCacheKey *key);
static bool o_sys_cache_add(OSysCache *sys_cache, OSysCacheKey *key,
							Pointer entry);
static bool o_sys_cache_update(OSysCache *sys_cache, Pointer updated_entry);
static int	o_sys_cache_key_cmp(OSysCache *sys_cache, int nkeys,
								OSysCacheKey *key1, OSysCacheKey *key2);
static void o_sys_cache_keys_to_str(StringInfo buf, OSysCache *sys_cache,
									OSysCacheKey *key);

static BTreeDescr *oSysCacheToastGetBTreeDesc(void *arg);
static uint32 oSysCacheToastGetMaxChunkSize(void *key, void *arg);
static void oSysCacheToastUpdateKey(void *key, uint32 offset, void *arg);
static void *oSysCacheToastGetNextKey(void *key, void *arg);
static OTuple oSysCacheToastCreateTuple(void *key, Pointer data,
										uint32 offset, int length,
										void *arg);
static OTuple oSysCacheToastCreateKey(void *key, uint32 offset, void *arg);
static Pointer oSysCacheToastGetTupleData(OTuple tuple, void *arg);
static uint32 oSysCacheToastGetTupleOffset(OTuple tuple, void *arg);
static uint32 oSysCacheToastGetTupleDataSize(OTuple tuple, void *arg);

static HeapTuple o_auth_cache_search_htup(TupleDesc tupdesc, Oid authoid);


ToastAPI	oSysCacheToastAPI = {
	.getBTreeDesc = oSysCacheToastGetBTreeDesc,
	.getMaxChunkSize = oSysCacheToastGetMaxChunkSize,
	.updateKey = oSysCacheToastUpdateKey,
	.getNextKey = oSysCacheToastGetNextKey,
	.createTuple = oSysCacheToastCreateTuple,
	.createKey = oSysCacheToastCreateKey,
	.getTupleData = oSysCacheToastGetTupleData,
	.getTupleOffset = oSysCacheToastGetTupleOffset,
	.getTupleDataSize = oSysCacheToastGetTupleDataSize,
	.deleteLogFullTuple = false,
	.versionCallback = NULL
};

Oid			o_sys_cache_search_datoid = InvalidOid;

static MemoryContext sys_cache_cxt = NULL;
static HTAB *sys_cache_fastcache;
static HTAB *sys_caches;

static ResourceOwner my_owner = NULL;
static Oid	save_userid;
static int	save_sec_context;

/*
 * Initializes the enum B-tree memory.
 */
void
o_sys_caches_init(void)
{
	HASHCTL		ctl;

	sys_cache_cxt = AllocSetContextCreate(TopMemoryContext,
										  "OrioleDB sys_caches fastcache context",
										  ALLOCSET_DEFAULT_SIZES);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OSysCacheHashKey);
	ctl.entrysize = sizeof(OSysCacheHashEntry);
	ctl.hcxt = sys_cache_cxt;
	sys_cache_fastcache = hash_create("OrioleDB sys_caches fastcache", 8,
									  &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int);
	ctl.entrysize = sizeof(OCacheIdMapEntry);
	ctl.hcxt = sys_cache_cxt;
	sys_caches = hash_create("OrioleDB sys_tree_num to sys_cache map", 8, &ctl,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	o_aggregate_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_amop_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_amop_strat_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_amproc_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_enum_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_enumoid_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_class_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_opclass_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_operator_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_proc_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_range_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_type_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_collation_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_database_cache_init(sys_cache_cxt, sys_cache_fastcache);
#if PG_VERSION_NUM >= 140000
	o_multirange_cache_init(sys_cache_cxt, sys_cache_fastcache);
#endif
	orioledb_setup_syscache_hooks();
}

static uint32
charhashfast(OSysCacheKey *key, int att_num)
{
	return murmurhash32((int32) DatumGetChar(key->keys[att_num]));
}

static uint32
namehashfast(OSysCacheKey *key, int att_num)
{
	char	   *name;

	name = NameStr(*O_KEY_GET_NAME(key, att_num));
	return hash_any((unsigned char *) name, strlen(name));
}

static uint32
int2hashfast(OSysCacheKey *key, int att_num)
{
	return murmurhash32((int32) DatumGetInt16(key->keys[att_num]));
}

static uint32
int4hashfast(OSysCacheKey *key, int att_num)
{
	return murmurhash32((int32) DatumGetInt32(key->keys[att_num]));
}

static uint32
texthashfast(OSysCacheKey *key, int att_num)
{
	/*
	 * The use of DEFAULT_COLLATION_OID is fairly arbitrary here.  We just
	 * want to take the fast "deterministic" path in texteq().
	 */
	return DatumGetInt32(DirectFunctionCall1Coll(hashtext,
												 DEFAULT_COLLATION_OID,
												 key->keys[att_num]));
}

static uint32
oidvectorhashfast(OSysCacheKey *key, int att_num)
{
	return DatumGetInt32(
						 DirectFunctionCall1(hashoidvector, key->keys[att_num]));
}

static void
set_hash_func(Oid keytype, O_CCHashFN *hashfunc)
{

	switch (keytype)
	{
		case BOOLOID:
			*hashfunc = charhashfast;
			break;
		case CHAROID:
			*hashfunc = charhashfast;
			break;
		case NAMEOID:
			*hashfunc = namehashfast;
			break;
		case INT2OID:
			*hashfunc = int2hashfast;
			break;
		case INT4OID:
			*hashfunc = int4hashfast;
			break;
		case TEXTOID:
			*hashfunc = texthashfast;
			break;
		case OIDOID:
		case REGPROCOID:
		case REGPROCEDUREOID:
		case REGOPEROID:
		case REGOPERATOROID:
		case REGCLASSOID:
		case REGTYPEOID:
		case REGCOLLATIONOID:
		case REGCONFIGOID:
		case REGDICTIONARYOID:
		case REGROLEOID:
		case REGNAMESPACEOID:
			*hashfunc = int4hashfast;
			break;
		case OIDVECTOROID:
			*hashfunc = oidvectorhashfast;
			break;
		default:
			elog(FATAL, "type %u not supported as catcache key", keytype);
			*hashfunc = NULL;	/* keep compiler quiet */
			break;
	}
}

/*
 * Initializes the enum B-tree memory.
 */
OSysCache *
o_create_sys_cache(int sys_tree_num, bool is_toast,
				   Oid cc_indexoid, int cacheId, int nkeys,
				   Oid *keytypes, int data_len, HTAB *fast_cache,
				   MemoryContext mcxt, OSysCacheFuncs *funcs)
{
	OSysCache  *sys_cache;
	int			i;
	OCacheIdMapEntry *entry;

	Assert(fast_cache);
	Assert(funcs);

	sys_cache = MemoryContextAllocZero(mcxt, sizeof(OSysCache));
	sys_cache->sys_tree_num = sys_tree_num;
	sys_cache->is_toast = is_toast;
	sys_cache->cc_indexoid = cc_indexoid;
	sys_cache->cacheId = cacheId;
	sys_cache->nkeys = nkeys;
	memcpy(sys_cache->keytypes, keytypes, sizeof(Oid) * sys_cache->nkeys);
	sys_cache->data_len = data_len;
	sys_cache->fast_cache = fast_cache;
	sys_cache->mcxt = mcxt;
	sys_cache->funcs = funcs;

#ifdef USE_ASSERT_CHECKING
	Assert(sys_cache->funcs->free_entry);
	Assert(sys_cache->funcs->fill_entry);
	if (sys_cache->is_toast)
	{
		Assert(sys_cache->funcs->toast_serialize_entry);
		Assert(sys_cache->funcs->toast_deserialize_entry);
	}
#endif

	for (i = 0; i < sys_cache->nkeys; i++)
	{
		set_hash_func(keytypes[i], &sys_cache->cc_hashfunc[i]);
	}

	entry = hash_search(sys_caches, &cacheId, HASH_ENTER, NULL);
	entry->sys_cache = sys_cache;
	sys_tree_set_extra(sys_tree_num, (Pointer) sys_cache);
	return sys_cache;
}

/*
 *		CatalogCacheComputeHashValue
 *
 * Compute the hash value associated with a given set of lookup keys
 */
static OSysCacheHashKey
compute_hash_value(O_CCHashFN *cc_hashfunc, int nkeys, OSysCacheKey *key)
{
	uint32		hashValue = 0;
	uint32		oneHash;

	switch (nkeys)
	{
		case 4:
			oneHash = (cc_hashfunc[3]) (key, 3);

			hashValue ^= oneHash << 24;
			hashValue ^= oneHash >> 8;
			/* FALLTHROUGH */
		case 3:
			oneHash = (cc_hashfunc[2]) (key, 2);

			hashValue ^= oneHash << 16;
			hashValue ^= oneHash >> 16;
			/* FALLTHROUGH */
		case 2:
			oneHash = (cc_hashfunc[1]) (key, 1);

			hashValue ^= oneHash << 8;
			hashValue ^= oneHash >> 24;
			/* FALLTHROUGH */
		case 1:
			oneHash = (cc_hashfunc[0]) (key, 0);

			hashValue ^= oneHash;
			break;
		default:
			elog(FATAL, "wrong number of hash keys: %d", nkeys);
			break;
	}

	return hashValue;
}

static void
invalidate_fastcache_entry(int cacheid, uint32 hashvalue)
{
	bool		found;
	OSysCacheHashEntry *fast_cache_entry;

	fast_cache_entry = (OSysCacheHashEntry *) hash_search(sys_cache_fastcache,
														  &hashvalue,
														  HASH_REMOVE,
														  &found);

	if (found)
	{
		ListCell   *lc;

		foreach(lc, fast_cache_entry->tree_entries)
		{
			OSysCacheHashTreeEntry *tree_entry;

			tree_entry = (OSysCacheHashTreeEntry *) lfirst(lc);

			if (tree_entry->sys_cache)
			{
				OSysCache  *sys_cache = tree_entry->sys_cache;

				if (!memcmp(&sys_cache->last_fast_cache_key,
							&fast_cache_entry->key,
							sizeof(OSysCacheHashKey)))
				{
					memset(&sys_cache->last_fast_cache_key, 0,
						   sizeof(OSysCacheHashKey));
					sys_cache->last_fast_cache_entry = NULL;
				}
				tree_entry->sys_cache->funcs->free_entry(tree_entry->entry);
			}
		}
		list_free_deep(fast_cache_entry->tree_entries);
	}
}

static void
orioledb_syscache_hook(Datum arg, int cacheid, uint32 hashvalue)
{
	if (sys_cache_fastcache)
		invalidate_fastcache_entry(cacheid, hashvalue);
}

void
orioledb_setup_syscache_hooks(void)
{
	HASH_SEQ_STATUS hash_seq;
	OCacheIdMapEntry *entry;

	hash_seq_init(&hash_seq, sys_caches);

	while ((entry = (OCacheIdMapEntry *) hash_seq_search(&hash_seq)) != NULL)
	{
		OSysCache  *sys_cache = entry->sys_cache;

		CacheRegisterSyscacheCallback(sys_cache->cacheId,
									  orioledb_syscache_hook,
									  PointerGetDatum(NULL));
	}
}

Pointer
o_sys_cache_search(OSysCache *sys_cache, int nkeys, OSysCacheKey *key)
{
	bool		found = false;
	OSysCacheHashKey cur_fast_cache_key;
	OSysCacheHashEntry *fast_cache_entry;
	Pointer		tree_entry;
	MemoryContext prev_context;
	OSysCacheHashTreeEntry *new_entry;

	cur_fast_cache_key = compute_hash_value(sys_cache->cc_hashfunc,
											sys_cache->nkeys, key);

	/* fast search */
	if (!memcmp(&cur_fast_cache_key, &sys_cache->last_fast_cache_key,
				sizeof(OSysCacheHashKey)) &&
		sys_cache->last_fast_cache_entry)
	{
		OSysCacheKey *sys_cache_key;

		sys_cache_key = (OSysCacheKey *) sys_cache->last_fast_cache_entry;

		if (sys_cache_key->common.datoid == key->common.datoid &&
			o_sys_cache_key_cmp(sys_cache, sys_cache->nkeys, sys_cache_key,
								key) == 0)
			return sys_cache->last_fast_cache_entry;
	}

	/* cache search */
	fast_cache_entry = (OSysCacheHashEntry *)
		hash_search(sys_cache->fast_cache, &cur_fast_cache_key, HASH_ENTER,
					&found);
	if (found)
	{
		ListCell   *lc;

		foreach(lc, fast_cache_entry->tree_entries)
		{
			OSysCacheHashTreeEntry *tree_entry;

			tree_entry = (OSysCacheHashTreeEntry *) lfirst(lc);

			if (tree_entry->sys_cache == sys_cache)
			{
				OSysCacheKey *sys_cache_key;

				sys_cache_key = (OSysCacheKey *) tree_entry->entry;

				if (sys_cache_key->common.datoid == key->common.datoid &&
					o_sys_cache_key_cmp(sys_cache, sys_cache->nkeys,
										sys_cache_key, key) == 0)
				{
					memcpy(&sys_cache->last_fast_cache_key,
						   &cur_fast_cache_key,
						   sizeof(OSysCacheHashKey));
					sys_cache->last_fast_cache_entry = tree_entry->entry;
					return sys_cache->last_fast_cache_entry;
				}
			}
		}
	}
	else
		fast_cache_entry->tree_entries = NIL;

	prev_context = MemoryContextSwitchTo(sys_cache->mcxt);
	if (sys_cache->is_toast)
		tree_entry = o_sys_cache_get_from_toast_tree(sys_cache, key);
	else
		tree_entry = o_sys_cache_get_from_tree(sys_cache, nkeys, key);
	if (tree_entry == NULL)
	{
		MemoryContextSwitchTo(prev_context);
		return NULL;
	}
	new_entry = palloc0(sizeof(OSysCacheHashTreeEntry));
	new_entry->sys_cache = sys_cache;
	new_entry->entry = tree_entry;

	fast_cache_entry->tree_entries = lappend(fast_cache_entry->tree_entries,
											 new_entry);

	MemoryContextSwitchTo(prev_context);

	memcpy(&sys_cache->last_fast_cache_key,
		   &cur_fast_cache_key,
		   sizeof(OSysCacheHashKey));
	sys_cache->last_fast_cache_entry = new_entry->entry;
	return sys_cache->last_fast_cache_entry;
}

static TupleFetchCallbackResult
o_sys_cache_get_by_lsn_callback(OTuple tuple, OXid tupOxid, CommitSeqNo csn,
								void *arg,
								TupleFetchCallbackCheckType check_type)
{
	OSysCacheToastChunkKey *tuple_key = (OSysCacheToastChunkKey *) tuple.data;
	XLogRecPtr *cur_lsn = (XLogRecPtr *) arg;

	if (check_type != OTupleFetchCallbackKeyCheck)
		return OTupleFetchNext;

	if (tuple_key->sys_cache_key.common.lsn < *cur_lsn)
		return OTupleFetchMatch;
	else
		return OTupleFetchNext;
}

Pointer
o_sys_cache_get_from_toast_tree(OSysCache *sys_cache, OSysCacheKey *key)
{
	Pointer		data;
	Size		dataLength;
	Pointer		result = NULL;
	BTreeDescr *td = get_sys_tree(sys_cache->sys_tree_num);
	OSysCacheToastKeyBound toast_key = {.common = {.offset = 0},
	.key = key,.lsn_cmp = false};

	data = generic_toast_get_any_with_callback(&oSysCacheToastAPI,
											   (Pointer) &toast_key,
											   &dataLength,
											   COMMITSEQNO_NON_DELETED,
											   td,
											   o_sys_cache_get_by_lsn_callback,
											   &key->common.lsn);
	if (data == NULL)
		return NULL;
	result = sys_cache->funcs->toast_deserialize_entry(sys_cache->mcxt,
													   data, dataLength);
	pfree(data);

	return result;
}

Pointer
o_sys_cache_get_from_tree(OSysCache *sys_cache, int nkeys, OSysCacheKey *key)
{
	BTreeDescr *td = get_sys_tree(sys_cache->sys_tree_num);
	BTreeIterator *it;
	OTuple		last_tup;
	OSysCacheBound bound = {.key = key,.nkeys = nkeys};

	it = o_btree_iterator_create(td, (Pointer) &bound, BTreeKeyBound,
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	O_TUPLE_SET_NULL(last_tup);
	do
	{
		OTuple		tup = o_btree_iterator_fetch(it, NULL,
												 (Pointer) &bound,
												 BTreeKeyBound, true,
												 NULL);
		OSysCacheKey *sys_cache_key;

		if (O_TUPLE_IS_NULL(tup))
			break;

		if (!O_TUPLE_IS_NULL(last_tup))
			pfree(last_tup.data);

		sys_cache_key = (OSysCacheKey *) tup.data;
		if (sys_cache_key->common.lsn > key->common.lsn)
			break;
		last_tup = tup;
	} while (true);

	btree_iterator_free(it);

	return last_tup.data;
}

static inline void
o_sys_cache_fill_locktag(LOCKTAG *tag, Oid datoid, Oid classoid,
						 OSysCacheHashKey key_hash, int lockmode)
{
	Assert(lockmode == AccessShareLock || lockmode == AccessExclusiveLock);
	memset(tag, 0, sizeof(LOCKTAG));
	SET_LOCKTAG_OBJECT(*tag, datoid, classoid, key_hash, 0);
	tag->locktag_type = LOCKTAG_USERLOCK;
}

static void
o_sys_cache_lock(OSysCache *sys_cache, OSysCacheKey *key, int lockmode)
{
	LOCKTAG		locktag;
	OSysCacheHashKey key_hash;

	key_hash = compute_hash_value(sys_cache->cc_hashfunc, sys_cache->nkeys,
								  key);

	o_sys_cache_fill_locktag(&locktag, key->common.datoid, key_hash,
							 sys_cache->cc_indexoid, lockmode);

	LockAcquire(&locktag, lockmode, false, false);
}

static void
o_sys_cache_unlock(OSysCache *sys_cache, OSysCacheKey *key, int lockmode)
{
	LOCKTAG		locktag;
	OSysCacheHashKey key_hash;

	key_hash = compute_hash_value(sys_cache->cc_hashfunc, sys_cache->nkeys,
								  key);

	o_sys_cache_fill_locktag(&locktag, key->common.datoid, key_hash,
							 sys_cache->cc_indexoid, lockmode);

	if (!LockRelease(&locktag, lockmode, false))
	{
		StringInfo	str = makeStringInfo();

		o_sys_cache_keys_to_str(str, sys_cache, key);
		elog(ERROR, "Can not release %s catalog cache lock on datoid = %d, "
			 "key = %s", lockmode == AccessShareLock ? "share" : "exclusive",
			 key->common.datoid, str->data);
		pfree(str->data);
		pfree(str);
	}
}

static

/* Non-key fields of entry should be filled before call */
bool
o_sys_cache_add(OSysCache *sys_cache, OSysCacheKey *key, Pointer entry)
{
	bool		inserted;
	OSysCacheKey *entry_key = (OSysCacheKey *) entry;
	BTreeDescr *desc = get_sys_tree(sys_cache->sys_tree_num);
	int			i;
	bool		allocated = false;
	OTuple		entry_tuple = {.data = entry};
	int			key_len = o_btree_len(desc, entry_tuple, OTupleKeyLength);
	int			entry_len = o_btree_len(desc, entry_tuple, OTupleLength);

	entry_key->common = key->common;
	entry_key->common.dataLength = 0;
	for (i = 0; i < sys_cache->nkeys; i++)
	{
		switch (sys_cache->keytypes[i])
		{
			case NAMEOID:
				{
					Pointer		new_entry;
					int			new_entry_len;

					new_entry_len = entry_len + sizeof(NameData);
					new_entry = palloc0(new_entry_len);
					memcpy(new_entry, entry, key_len);
					memcpy(new_entry + key_len,
						   NameStr(*DatumGetName(key->keys[i])),
						   sizeof(NameData));
					memcpy(new_entry + key_len + sizeof(NameData),
						   entry + key_len,
						   entry_len - key_len);
					entry_key = (OSysCacheKey *) new_entry;
					entry_key->keys[i] = key_len;
					entry_key->common.dataLength += sizeof(NameData);

					key_len += sizeof(NameData);
					if (allocated)
						pfree(entry);
					entry = new_entry;
					allocated = true;
				}
				break;

			default:
				entry_key->keys[i] = key->keys[i];
				break;
		}
	}

	if (!sys_cache->is_toast)
	{
		OTuple		tup = {0};

		tup.formatFlags = 0;
		tup.data = entry;
		inserted = o_btree_autonomous_insert(desc, tup);
	}
	else
	{
		Pointer		data;
		int			len;
		OSysCacheToastKeyBound toast_key = {0};
		OAutonomousTxState state;

		toast_key.key = entry_key;
		toast_key.common.offset = 0;
		toast_key.lsn_cmp = true;

		data = sys_cache->funcs->toast_serialize_entry(entry, &len);

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			inserted = generic_toast_insert(&oSysCacheToastAPI,
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
	if (allocated)
		pfree(entry);
	return inserted;
}

static OBTreeWaitCallbackAction
o_sys_cache_wait_callback(BTreeDescr *descr,
						  OTuple tup, OTuple *newtup, OXid oxid,
						  OTupleXactInfo xactInfo, UndoLocation location,
						  RowLockMode *lock_mode, BTreeLocationHint *hint,
						  void *arg)
{
	return OBTreeCallbackActionXidWait;
}

static OBTreeModifyCallbackAction
o_sys_cache_update_callback(BTreeDescr *descr,
							OTuple tup, OTuple *newtup, OXid oxid,
							OTupleXactInfo xactInfo, UndoLocation location,
							RowLockMode *lock_mode, BTreeLocationHint *hint,
							void *arg)
{
	return OBTreeCallbackActionUpdate;
}

static OBTreeModifyCallbackAction
o_sys_cache_update_deleted_callback(BTreeDescr *descr,
									OTuple tup, OTuple *newtup, OXid oxid,
									OTupleXactInfo xactInfo,
									BTreeLeafTupleDeletedStatus deleted,
									UndoLocation location,
									RowLockMode *lock_mode, BTreeLocationHint *hint,
									void *arg)
{
	return OBTreeCallbackActionUpdate;
}


static BTreeModifyCallbackInfo callbackInfo =
{
	.waitCallback = o_sys_cache_wait_callback,
	.modifyCallback = o_sys_cache_update_callback,
	.modifyDeletedCallback = o_sys_cache_update_deleted_callback,
	.arg = NULL
};

bool
o_sys_cache_update(OSysCache *sys_cache, Pointer updated_entry)
{
	bool		result;
	OSysCacheKey *sys_cache_key;
	BTreeDescr *desc = get_sys_tree(sys_cache->sys_tree_num);
	OSysCacheBound bound = {.nkeys = sys_cache->nkeys};

	sys_cache_key = (OSysCacheKey *) updated_entry;
	bound.key = sys_cache_key;

	if (!sys_cache->is_toast)
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
									(Pointer) &bound, BTreeKeyBound,
									get_current_oxid(), COMMITSEQNO_INPROGRESS,
									RowLockNoKeyUpdate, NULL,
									&callbackInfo) ==
				OBTreeModifyResultUpdated;
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
		OSysCacheToastKeyBound toast_key = {0};
		OAutonomousTxState state;

		toast_key.key = sys_cache_key;
		toast_key.common.offset = 0;
		toast_key.lsn_cmp = true;

		data = sys_cache->funcs->toast_serialize_entry(updated_entry, &len);

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = generic_toast_update(&oSysCacheToastAPI,
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
o_sys_cache_add_if_needed(OSysCache *sys_cache, OSysCacheKey *key, Pointer arg)
{
	Pointer		entry = NULL;
	bool		inserted PG_USED_FOR_ASSERTS_ONLY;
	bool		found = false;

	o_sys_cache_lock(sys_cache, key, AccessExclusiveLock);

	entry = o_sys_cache_search(sys_cache, sys_cache->nkeys, key);
	found = entry != NULL;

	if (found)
	{
		o_sys_cache_unlock(sys_cache, key, AccessExclusiveLock);
		return;
	}

	sys_cache->funcs->fill_entry(&entry, key, arg);

	Assert(entry);

	/*
	 * All done, now try to insert into B-tree.
	 */
	inserted = o_sys_cache_add(sys_cache, key, entry);
	Assert(inserted);
	o_sys_cache_unlock(sys_cache, key, AccessExclusiveLock);
	sys_cache->funcs->free_entry(entry);
}

void
o_sys_cache_update_if_needed(OSysCache *sys_cache, OSysCacheKey *key,
							 Pointer arg)
{
	Pointer		entry = NULL;
	OSysCacheKey *sys_cache_key;
	bool		updated PG_USED_FOR_ASSERTS_ONLY;

	o_sys_cache_lock(sys_cache, key, AccessExclusiveLock);

	o_sys_cache_set_datoid_lsn(&key->common.lsn, NULL);
	entry = o_sys_cache_search(sys_cache, sys_cache->nkeys, key);
	if (entry == NULL)
	{
		/* it's not exist in B-tree */
		return;
	}

	sys_cache_key = (OSysCacheKey *) entry;
	sys_cache->funcs->fill_entry(&entry, sys_cache_key, arg);

	updated = o_sys_cache_update(sys_cache, entry);
	Assert(updated);
	o_sys_cache_unlock(sys_cache, key, AccessExclusiveLock);
}

bool
o_sys_cache_delete(OSysCache *sys_cache, OSysCacheKey *key)
{
	Pointer		entry;
	OSysCacheKey *sys_cache_key;

	o_sys_cache_set_datoid_lsn(&key->common.lsn, NULL);
	entry = o_sys_cache_search(sys_cache, sys_cache->nkeys, key);

	if (entry == NULL)
		return false;

	sys_cache_key = (OSysCacheKey *) entry;
	sys_cache_key->common.deleted = true;

	return o_sys_cache_update(sys_cache, entry);
}

static void
o_sys_cache_delete_by_lsn(OSysCache *sys_cache, XLogRecPtr lsn)
{
	BTreeIterator *it;
	BTreeDescr *td = get_sys_tree(sys_cache->sys_tree_num);

	it = o_btree_iterator_create(td, NULL, BTreeKeyNone,
								 COMMITSEQNO_NON_DELETED,
								 ForwardScanDirection);

	do
	{
		bool		end;
		BTreeLocationHint hint;
		OTuple		tup = btree_iterate_raw(it, NULL, BTreeKeyNone,
											false, &end, &hint);
		OSysCacheKey *sys_cache_key;
		OTuple		key_tup;

		if (O_TUPLE_IS_NULL(tup))
		{
			if (end)
				break;
			else
				continue;
		}

		if (sys_cache->is_toast)
			sys_cache_key = (OSysCacheKey *)
				(tup.data + offsetof(OSysCacheToastChunkKey, sys_cache_key));
		else
			sys_cache_key = (OSysCacheKey *) tup.data;
		key_tup.formatFlags = 0;
		key_tup.data = (Pointer) sys_cache_key;

		if (sys_cache_key->common.lsn < lsn && sys_cache_key->common.deleted)
		{
			bool		result PG_USED_FOR_ASSERTS_ONLY;

			if (!sys_cache->is_toast)
			{
				result = o_btree_autonomous_delete(td, key_tup,
												   BTreeKeyNonLeafKey, &hint);
			}
			else
			{
				OSysCacheToastKeyBound toast_key = {0};
				OAutonomousTxState state;

				toast_key.key = sys_cache_key;
				toast_key.common.offset = 0;
				toast_key.lsn_cmp = true;

				start_autonomous_transaction(&state);
				PG_TRY();
				{
					result = generic_toast_delete(&oSysCacheToastAPI,
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

void
o_sys_caches_delete_by_lsn(XLogRecPtr checkPointRedo)
{
	HASH_SEQ_STATUS hash_seq;
	OCacheIdMapEntry *entry;

	hash_seq_init(&hash_seq, sys_caches);
	while ((entry = (OCacheIdMapEntry *) hash_seq_search(&hash_seq)) != NULL)
	{
		OSysCache  *sys_cache = entry->sys_cache;

		o_sys_cache_delete_by_lsn(sys_cache, checkPointRedo);
	}
}


static BTreeDescr *
oSysCacheToastGetBTreeDesc(void *arg)
{
	BTreeDescr *desc = (BTreeDescr *) arg;

	return desc;
}

static uint32
oSysCacheToastGetMaxChunkSize(void *key, void *arg)
{
	BTreeDescr *desc = (BTreeDescr *) arg;
	uint32		chunk_key_len;
	uint32		max_chunk_size;
	OTuple		tup = {0};

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);

	max_chunk_size = MAXALIGN_DOWN((O_BTREE_MAX_TUPLE_SIZE * 3 -
									MAXALIGN(chunk_key_len)) /
								   3) -
		(chunk_key_len + sizeof(OSysCacheToastChunkCommon));

	return max_chunk_size;
}

static void
oSysCacheToastUpdateKey(void *key, uint32 offset, void *arg)
{
	OSysCacheToastKeyBound *ckey = (OSysCacheToastKeyBound *) key;

	ckey->common.offset = offset;
}

static inline int
nkeys_for_desc(BTreeDescr *desc)
{
	OTuple		tup = {0};
	int			key_len;
	bool		toast = desc->ops->cmp == o_sys_cache_toast_cmp;
	int			nkeys;

	if (toast)
	{
		int			chunk_key_len;

		chunk_key_len = o_btree_len(desc, tup, OKeyLength);
		key_len = chunk_key_len -
			offsetof(OSysCacheToastChunkKey, sys_cache_key);
	}
	else
	{
		key_len = o_btree_len(desc, tup, OKeyLength);
	}
	nkeys = (key_len - offsetof(OSysCacheKey, keys)) / sizeof(Datum);

	return nkeys;
}

static void *
oSysCacheToastGetNextKey(void *key, void *arg)
{
	BTreeDescr *desc = (BTreeDescr *) arg;
	OSysCacheToastKeyBound *ckey = (OSysCacheToastKeyBound *) key;
	static OSysCacheKey4 nextKey = {0};
	static OSysCacheToastKeyBound nextKeyBound = {.key =
	(OSysCacheKey *) &nextKey};
	int			nkeys;
	int			key_len;

	nkeys = nkeys_for_desc(desc);

	key_len = offsetof(OSysCacheKey, keys) + sizeof(Datum) * nkeys;

	nextKeyBound.common.offset = 0;
	memcpy(nextKeyBound.key, ckey->key, key_len);
	nextKeyBound.key->keys[nkeys - 1]++;

	return (Pointer) &nextKeyBound;
}

static OTuple
oSysCacheToastCreateTuple(void *key, Pointer data, uint32 offset,
						  int length, void *arg)
{
	OSysCacheToastKeyBound *bound = (OSysCacheToastKeyBound *) key;
	Pointer		chunk;
	OTuple		result;
	OTuple		tup = {0};
	BTreeDescr *desc = (BTreeDescr *) arg;
	int			key_len;
	int			chunk_key_len;
	OSysCacheToastChunkKey *chunk_key;
	OSysCacheToastChunkCommon *common;

	bound->common.offset = offset;

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);
	key_len = chunk_key_len - offsetof(OSysCacheToastChunkKey, sys_cache_key);

	chunk = palloc0(chunk_key_len + sizeof(OSysCacheToastChunkCommon) +
					length);

	common = (OSysCacheToastChunkCommon *) (chunk + chunk_key_len);
	common->dataLength = length;
	chunk_key = (OSysCacheToastChunkKey *) chunk;
	chunk_key->common = bound->common;
	memcpy(&chunk_key->sys_cache_key, bound->key, key_len);
	memcpy(chunk + chunk_key_len + sizeof(OSysCacheToastChunkCommon),
		   data + offset, length);

	result.data = (Pointer) chunk;
	result.formatFlags = 0;

	return result;
}

static OTuple
oSysCacheToastCreateKey(void *key, uint32 offset, void *arg)
{
	OSysCacheToastChunkKey *ckey = (OSysCacheToastChunkKey *) key;
	OSysCacheToastChunkKey *ckey_copy;
	OTuple		result;

	ckey_copy = (OSysCacheToastChunkKey *) palloc(sizeof(OSysCacheToastChunkKey));
	*ckey_copy = *ckey;

	result.data = (Pointer) ckey_copy;
	result.formatFlags = 0;

	return result;
}

static Pointer
oSysCacheToastGetTupleData(OTuple tuple, void *arg)
{
	BTreeDescr *desc = (BTreeDescr *) arg;
	int			chunk_key_len;
	OTuple		tup = {0};
	Pointer		chunk = tuple.data;

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);

	return chunk + chunk_key_len + sizeof(OSysCacheToastChunkCommon);
}

static uint32
oSysCacheToastGetTupleOffset(OTuple tuple, void *arg)
{
	Pointer		chunk = tuple.data;
	OSysCacheToastChunkKey *chunk_key;

	chunk_key = (OSysCacheToastChunkKey *) chunk;

	return chunk_key->common.offset;
}

static uint32
oSysCacheToastGetTupleDataSize(OTuple tuple, void *arg)
{
	Pointer		chunk = tuple.data;
	OSysCacheToastChunkCommon *common;
	BTreeDescr *desc = (BTreeDescr *) arg;
	int			chunk_key_len;
	OTuple		tup = {0};

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);

	common = (OSysCacheToastChunkCommon *) (chunk + chunk_key_len);

	return common->dataLength;
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
				OClassArg	arg = {.sys_table = false};

				o_class_cache_add_if_needed(datoid, typeform->typrelid,
											insert_lsn, (Pointer) &arg);
				o_type_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
										   NULL);
			}
			break;
		case TYPTYPE_RANGE:
			{
				XLogRecPtr	sys_lsn;
				Oid			sys_datoid;
				OClassArg	class_arg = {.sys_table = true};

				o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
				o_class_cache_add_if_needed(sys_datoid, RangeRelationId,
											sys_lsn, (Pointer) &class_arg);
				o_range_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
											NULL);
				o_type_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
										   NULL);
				o_range_cache_add_rngsubopc(datoid, typeform->oid, insert_lsn);
			}
			break;
#if PG_VERSION_NUM >= 140000
		case TYPTYPE_MULTIRANGE:
			{
				XLogRecPtr	sys_lsn;
				Oid			sys_datoid;
				OClassArg	class_arg = {.sys_table = true};

				o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
				o_class_cache_add_if_needed(sys_datoid, RangeRelationId,
											sys_lsn, (Pointer) &class_arg);
				o_multirange_cache_add_if_needed(datoid, typeform->oid,
												 insert_lsn, NULL);
				o_type_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
										   NULL);
			}
			break;
#endif
		case TYPTYPE_ENUM:
			{
				XLogRecPtr	sys_lsn;
				Oid			sys_datoid;
				OClassArg	class_arg = {.sys_table = true};

				o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
				o_class_cache_add_if_needed(sys_datoid, EnumRelationId, sys_lsn,
											(Pointer) &class_arg);
				o_type_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
										   NULL);
				o_enum_cache_add_all(datoid, typeform->oid, insert_lsn);
			}
			break;
		case TYPTYPE_DOMAIN:
			custom_type_add_if_needed(datoid, typeform->typbasetype,
									  insert_lsn);
			break;
		default:
			if (typeform->typcategory == TYPCATEGORY_ARRAY)
			{
				o_composite_type_element_save(datoid, typeform->typelem,
											  insert_lsn);
			}
			break;
	}
	if (tuple != NULL)
		ReleaseSysCache(tuple);
}

/*
 * Inserts type elements for all fields of the o_table to the orioledb sys
 * cache.
 */
void
custom_types_add_all(OTable *o_table, OTableIndex *o_table_index)
{
	int			cur_field;
	XLogRecPtr	cur_lsn;
	int			expr_field = 0;

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	for (cur_field = 0; cur_field < o_table_index->nfields; cur_field++)
	{
		int			attnum = o_table_index->fields[cur_field].attnum;
		Oid			typid;

		if (attnum != EXPR_ATTNUM)
			typid = o_table->fields[attnum].typid;
		else
			typid = o_table_index->exprfields[expr_field++].typid;
		custom_type_add_if_needed(o_table->oids.datoid, typid, cur_lsn);
	}
}

void
o_composite_type_element_save(Oid datoid, Oid typoid, XLogRecPtr insert_lsn)
{
	Oid			default_btree_opclass;
	Oid			default_hash_opclass;

	custom_type_add_if_needed(datoid, typoid, insert_lsn);
	o_type_cache_add_if_needed(datoid, typoid, insert_lsn, NULL);
	default_btree_opclass = o_type_cache_default_opclass(typoid, BTREE_AM_OID);
	if (OidIsValid(default_btree_opclass))
	{
		XLogRecPtr	sys_lsn;
		Oid			sys_datoid;
		OClassArg	class_arg = {.sys_table = true};
		Oid			btree_opf;
		Oid			btree_opintype;

		btree_opf = get_opclass_family(default_btree_opclass);
		btree_opintype = get_opclass_input_type(default_btree_opclass);

		o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
		o_class_cache_add_if_needed(sys_datoid, OperatorClassRelationId,
									sys_lsn, (Pointer) &class_arg);
		o_opclass_cache_add_if_needed(datoid, default_btree_opclass,
									  insert_lsn, NULL);
		o_class_cache_add_if_needed(sys_datoid,
									AccessMethodProcedureRelationId, sys_lsn,
									(Pointer) &class_arg);
		o_amproc_cache_add_if_needed(datoid, btree_opf, btree_opintype,
									 btree_opintype, BTORDER_PROC, insert_lsn,
									 NULL);
		o_class_cache_add_if_needed(sys_datoid, AccessMethodOperatorRelationId,
									sys_lsn, (Pointer) &class_arg);
		o_amop_strat_cache_add_if_needed(datoid, btree_opf, btree_opintype,
										 btree_opintype, BTLessStrategyNumber,
										 insert_lsn, NULL);
		o_amop_strat_cache_add_if_needed(datoid, btree_opf, btree_opintype,
										 btree_opintype,
										 BTLessEqualStrategyNumber, insert_lsn,
										 NULL);
		o_amop_strat_cache_add_if_needed(datoid, btree_opf, btree_opintype,
										 btree_opintype, BTEqualStrategyNumber,
										 insert_lsn, NULL);
	}
	default_hash_opclass = o_type_cache_default_opclass(typoid, HASH_AM_OID);
	if (OidIsValid(default_hash_opclass))
	{
		XLogRecPtr	sys_lsn;
		Oid			sys_datoid;
		OClassArg	class_arg = {.sys_table = true};
		Oid			hash_opf;
		Oid			hash_opintype;

		hash_opf = get_opclass_family(default_hash_opclass);
		hash_opintype = get_opclass_input_type(default_hash_opclass);

		o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
		o_class_cache_add_if_needed(sys_datoid, OperatorClassRelationId,
									sys_lsn, (Pointer) &class_arg);
		o_opclass_cache_add_if_needed(datoid, default_hash_opclass, insert_lsn,
									  NULL);
		o_class_cache_add_if_needed(sys_datoid,
									AccessMethodProcedureRelationId, sys_lsn,
									(Pointer) &class_arg);
		o_amproc_cache_add_if_needed(datoid, hash_opf, hash_opintype,
									 hash_opintype, HASHSTANDARD_PROC,
									 insert_lsn, NULL);
		o_amproc_cache_add_if_needed(datoid, hash_opf, hash_opintype,
									 hash_opintype, HASHEXTENDED_PROC,
									 insert_lsn, NULL);

		o_class_cache_add_if_needed(sys_datoid, AccessMethodOperatorRelationId,
									sys_lsn, (Pointer) &class_arg);
		o_amop_strat_cache_add_if_needed(datoid, hash_opf, hash_opintype,
										 hash_opintype, HTEqualStrategyNumber,
										 insert_lsn, NULL);
	}
}

static CatCTup *
heap_to_catctup(CatCache *cache, TupleDesc cc_tupdesc, HeapTuple tuple,
				bool refcount)
{
	CatCTup    *ct;
	HeapTuple	dtp;
	MemoryContext oldcxt;
	int			i;

	/*
	 * If there are any out-of-line toasted fields in the tuple, expand them
	 * in-line. This saves cycles during later use of the catcache entry, and
	 * also protects us against the possibility of the toast tuples being
	 * freed before we attempt to fetch them, in case of something using a
	 * slightly stale catcache entry.
	 */
	if (HeapTupleHasExternal(tuple))
		dtp = toast_flatten_tuple(tuple, cc_tupdesc);
	else
		dtp = tuple;

	/* Allocate memory for CatCTup and the cached tuple in one go */
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	ct = (CatCTup *) palloc0(sizeof(CatCTup) + MAXIMUM_ALIGNOF + dtp->t_len);
	ct->tuple.t_len = dtp->t_len;
	ct->tuple.t_self = dtp->t_self;
	ct->tuple.t_tableOid = dtp->t_tableOid;
	ct->tuple.t_data = (HeapTupleHeader) MAXALIGN(((char *) ct) +
												  sizeof(CatCTup));
	/* copy tuple contents */
	memcpy((char *) ct->tuple.t_data, (const char *) dtp->t_data,
		   dtp->t_len);
	MemoryContextSwitchTo(oldcxt);

	if (dtp != tuple)
		heap_freetuple(dtp);

	/* extract keys - they'll point into the tuple if not by-value */
	for (i = 0; i < cache->cc_nkeys; i++)
	{
		Datum		atp;
		bool		isnull;

		atp = heap_getattr(&ct->tuple, cache->cc_keyno[i], cc_tupdesc,
						   &isnull);
		Assert(!isnull);
		ct->keys[i] = atp;
	}

	/*
	 * Finish initializing the CatCTup header, and add it to the cache's
	 * linked list and counts.
	 */
	ct->ct_magic = CT_MAGIC;
	ct->my_cache = cache;

	/* immediately set the refcount to 1 */
	if (refcount)
	{
		ResourceOwnerEnlargeCatCacheRefs(CurrentResourceOwner);
		ct->refcount++;
		ResourceOwnerRememberCatCacheRef(CurrentResourceOwner, &ct->tuple);
	}
	return ct;
}

static CatCTup *
o_SearchCatCacheInternal_hook(CatCache *cache, int nkeys, Datum v1, Datum v2,
							  Datum v3, Datum v4)
{
	CatCTup    *result = NULL;
	TupleDesc	tupdesc = NULL;
	HeapTuple	hook_tuple = NULL;

	switch (cache->cc_indexoid)
	{
		case AggregateFnoidIndexId:
		case AccessMethodOperatorIndexId:
		case AccessMethodStrategyIndexId:
		case AccessMethodProcedureIndexId:
		case AuthIdOidIndexId:
		case CollationOidIndexId:
		case EnumOidIndexId:
		case EnumTypIdLabelIndexId:
		case OpclassOidIndexId:
		case OperatorOidIndexId:
		case ProcedureOidIndexId:
		case RangeTypidIndexId:
#if PG_VERSION_NUM >= 140000
		case RangeMultirangeTypidIndexId:
#endif
		case TypeOidIndexId:
			if (cache->cc_tupdesc)
				tupdesc = cache->cc_tupdesc;
			else
				tupdesc = o_class_cache_search_tupdesc(cache->cc_reloid);
			break;
		default:
			break;
	}

	switch (cache->cc_indexoid)
	{
		case AggregateFnoidIndexId:
			{
				Oid			aggfnoid;

				aggfnoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_aggregate_cache_search_htup(tupdesc, aggfnoid);
			}
			break;
		case AccessMethodOperatorIndexId:
			{
				Oid			amopopr;
				char		amoppurpose;
				Oid			amopfamily;

				amopopr = DatumGetObjectId(v1);
				amoppurpose = DatumGetChar(v2);
				amopfamily = DatumGetObjectId(v3);

				Assert(tupdesc);

				hook_tuple = o_amop_cache_search_htup(tupdesc, amopopr,
													  amoppurpose, amopfamily);
			}
			break;
		case AccessMethodStrategyIndexId:
			{
				Oid			amopfamily;
				Oid			amoplefttype;
				Oid			amoprighttype;
				int16		amopstrategy;

				amopfamily = DatumGetObjectId(v1);
				amoplefttype = DatumGetObjectId(v2);
				amoprighttype = DatumGetObjectId(v3);
				amopstrategy = DatumGetChar(v4);

				Assert(tupdesc);

				hook_tuple =
					o_amop_strat_cache_search_htup(tupdesc, amopfamily,
												   amoplefttype, amoprighttype,
												   amopstrategy);
			}
			break;
		case AccessMethodProcedureIndexId:
			{
				Oid			amprocfamily;
				Oid			amproclefttype;
				Oid			amprocrighttype;
				int16		amprocnum;

				amprocfamily = DatumGetObjectId(v1);
				amproclefttype = DatumGetObjectId(v2);
				amprocrighttype = DatumGetObjectId(v3);
				amprocnum = DatumGetChar(v4);

				Assert(tupdesc);

				hook_tuple = o_amproc_cache_search_htup(tupdesc, amprocfamily,
														amproclefttype,
														amprocrighttype,
														amprocnum);
			}
			break;
		case AuthIdOidIndexId:
			{
				Oid			authoid;

				authoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_auth_cache_search_htup(tupdesc, authoid);
			}
			break;
		case CollationOidIndexId:
			{
				Oid			colloid;

				colloid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_collation_cache_search_htup(tupdesc, colloid);
			}
			break;
		case EnumOidIndexId:
			{
				Oid			enum_oid;

				enum_oid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_enumoid_cache_search_htup(tupdesc, enum_oid);
			}
			break;
		case EnumTypIdLabelIndexId:
			{
				Oid			enumtypid;
				Name		enumlabel;

				enumtypid = DatumGetObjectId(v1);
				enumlabel = DatumGetName(v1);

				Assert(tupdesc);

				hook_tuple =
					o_enum_cache_search_htup(tupdesc, enumtypid, enumlabel);
			}
			break;
		case OpclassOidIndexId:
			{
				Oid			opclassoid;

				opclassoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_opclass_cache_search_htup(tupdesc, opclassoid);
			}
			break;
		case OperatorOidIndexId:
			{
				Oid			operoid;

				operoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_operator_cache_search_htup(tupdesc, operoid);
			}
			break;
		case ProcedureOidIndexId:
			{
				Oid			procoid;

				procoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_proc_cache_search_htup(tupdesc, procoid);
			}
			break;
		case RangeTypidIndexId:
			{
				Oid			rngtypid;

				rngtypid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_range_cache_search_htup(tupdesc, rngtypid);
			}
			break;
#if PG_VERSION_NUM >= 140000
		case RangeMultirangeTypidIndexId:
			{
				Oid			rngmultitypid;

				rngmultitypid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_multirange_cache_search_htup(tupdesc,
															rngmultitypid);
			}
			break;
#endif
		case TypeOidIndexId:
			{
				Oid			typeoid;

				typeoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_type_cache_search_htup(tupdesc, typeoid);
			}
			break;

		default:
			break;
	}

	if (hook_tuple)
		result = heap_to_catctup(cache, tupdesc, hook_tuple, true);

	if (tupdesc && tupdesc != cache->cc_tupdesc)
		FreeTupleDesc(tupdesc);

	return result;
}

static CatCList *
o_SearchCatCacheList_hook(CatCache *cache, int nkeys, Datum v1, Datum v2,
						  Datum v3)
{
	CatCList   *cl = NULL;

	switch (cache->cc_indexoid)
	{
		case AccessMethodOperatorIndexId:
			{
				TupleDesc	tupdesc = NULL;
				List	   *htup_list;
				int			nmembers;
				Oid			amopopr;
				int			i;
				ListCell   *lc;
				MemoryContext oldcxt;

				if (cache->cc_tupdesc)
					tupdesc = cache->cc_tupdesc;
				else
					tupdesc = o_class_cache_search_tupdesc(cache->cc_reloid);

				Assert(nkeys == 1);
				amopopr = DatumGetObjectId(v1);

				Assert(tupdesc);

				htup_list = o_amop_cache_search_htup_list(tupdesc, amopopr);
				if (htup_list != NIL)
				{
					nmembers = list_length(htup_list);

					oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
					cl = (CatCList *)
						palloc0(offsetof(CatCList, members) +
								nmembers * sizeof(CatCTup *));
					MemoryContextSwitchTo(oldcxt);

					cl->cl_magic = CL_MAGIC;
					cl->my_cache = cache;
					cl->n_members = nmembers;

					ResourceOwnerEnlargeCatCacheListRefs(CurrentResourceOwner);
					i = 0;
					foreach(lc, htup_list)
					{
						HeapTuple	ht = lfirst(lc);
						CatCTup    *ct;

						ct = heap_to_catctup(cache, tupdesc, ht, false);
						cl->members[i++] = ct;
						ct->c_list = cl;
					}
					Assert(i == nmembers);

					cl->refcount++;
					ResourceOwnerRememberCatCacheListRef(CurrentResourceOwner, cl);
				}

				if (tupdesc && tupdesc != cache->cc_tupdesc)
					FreeTupleDesc(tupdesc);
			}
			break;
		default:
			break;
	}

	return cl;
}

static TupleDesc
o_SysCacheGetAttr_hook(CatCache *SysCache)
{
	TupleDesc	tupdesc = NULL;

	switch (SysCache->cc_indexoid)
	{
		case AggregateFnoidIndexId:
		case AccessMethodOperatorIndexId:
		case AccessMethodProcedureIndexId:
		case AuthIdOidIndexId:
		case CollationOidIndexId:
		case OpclassOidIndexId:
		case OperatorOidIndexId:
		case ProcedureOidIndexId:
		case TypeOidIndexId:
			if (SysCache->cc_tupdesc)
				tupdesc = SysCache->cc_tupdesc;
			else
				tupdesc = o_class_cache_search_tupdesc(SysCache->cc_reloid);
			break;
		default:
			break;
	}

	return tupdesc;
}

static uint32
o_GetCatCacheHashValue_hook(CatCache *cache, int nkeys, Datum v1, Datum v2,
							Datum v3, Datum v4)
{
	OSysCacheKey4 key = {.keys = {v1, v2, v3, v4}};
	OCacheIdMapEntry *entry;

	entry = hash_search(sys_caches, &cache->id, HASH_ENTER, NULL);
	Assert(entry);
	return compute_hash_value(entry->sys_cache->cc_hashfunc, nkeys,
							  (OSysCacheKey *) &key);
}

static void
o_load_typcache_tupdesc_hook(TypeCacheEntry *typentry)
{
	typentry->tupDesc = o_class_cache_search_tupdesc(typentry->typrelid);
	typentry->tupDesc->tdrefcount++;
}

static int
o_sys_cache_key_cmp(OSysCache *sys_cache, int nkeys, OSysCacheKey *key1,
					OSysCacheKey *key2)
{
	int			i;
	int			cmp = 0;

	for (i = 0; i < nkeys; i++)
	{
		Oid			keytype = sys_cache->keytypes[i];

		switch (keytype)
		{
			case NAMEOID:
				{
					char	   *arg1;
					char	   *arg2;

					arg1 = NameStr(*O_KEY_GET_NAME(key1, i));
					arg2 = NameStr(*O_KEY_GET_NAME(key2, i));
					cmp = strncmp(arg1, arg2, NAMEDATALEN);
				}
				break;
			default:
				cmp = key1->keys[i] - key2->keys[i];
				break;
		}
		if (cmp != 0)
			break;
	}
	return cmp;
}

static inline OSysCache *
get_o_sys_cache(int sys_tree_num)
{
	return (OSysCache *) sys_tree_get_extra(sys_tree_num);
}

int
o_sys_cache_key_length(BTreeDescr *desc, OTuple tuple)
{
	Pointer		data = tuple.data;
	OSysCacheKeyCommon *common;
	OSysCache  *sys_cache;
	int			key_len;

	sys_cache = get_o_sys_cache(desc->oids.reloid);
	key_len = offsetof(OSysCacheKey, keys) + sizeof(Datum) * sys_cache->nkeys;

	common = (OSysCacheKeyCommon *) data;

	return key_len + common->dataLength;
}

int
o_sys_cache_tup_length(BTreeDescr *desc, OTuple tuple)
{
	OSysCache  *sys_cache;
	int			key_len;
	int			data_len;

	key_len = o_sys_cache_key_length(desc, tuple);
	sys_cache = get_o_sys_cache(desc->oids.reloid);
	data_len = sys_cache->data_len;

	return key_len + data_len;
}

/*
 * Comparison function for non-TOAST sys cache B-tree.
 *
 * If none of the arguments is BTreeKeyBound it comparses by both
 * oid and lsn. It make possible to insert values with same oid.
 * Else it comparses only by oid, which is used by other operations than
 * insert, to find all rows with exact oid.
 * If key kind is not BTreeKeyBound it expects that OTuple passed.
 */
int
o_sys_cache_cmp(BTreeDescr *desc, void *p1, BTreeKeyType k1, void *p2,
				BTreeKeyType k2)
{
	OSysCacheKey *key1;
	OSysCacheKey *key2;
	bool		lsn_cmp = true;
	int			nkeys;
	int			cmp;
	OSysCache  *sys_cache;

	sys_cache = get_o_sys_cache(desc->oids.reloid);
	nkeys = sys_cache->nkeys;

	if (k1 == BTreeKeyBound)
	{
		OSysCacheBound *bound = (OSysCacheBound *) p1;

		key1 = bound->key;
		nkeys = bound->nkeys;
		lsn_cmp = false;
	}
	else
		key1 = (OSysCacheKey *) (((OTuple *) p1)->data);

	if (k2 == BTreeKeyBound)
	{
		OSysCacheBound *bound = (OSysCacheBound *) p2;

		key2 = bound->key;
		nkeys = bound->nkeys;
		lsn_cmp = false;
	}
	else
		key2 = (OSysCacheKey *) (((OTuple *) p2)->data);

	if (key1->common.datoid != key2->common.datoid)
		return key1->common.datoid < key2->common.datoid ? -1 : 1;

	cmp = o_sys_cache_key_cmp(sys_cache, nkeys, key1, key2);
	if (cmp != 0)
		return cmp;

	if (lsn_cmp)
		if (key1->common.lsn != key2->common.lsn)
			return key1->common.lsn < key2->common.lsn ? -1 : 1;

	return 0;
}

static void
o_sys_cache_keys_to_str(StringInfo buf, OSysCache *sys_cache,
						OSysCacheKey *key)
{
	int			i;

	appendStringInfo(buf, "(");
	for (i = 0; i < sys_cache->nkeys; i++)
	{
		if (i != 0)
			appendStringInfo(buf, ", ");
		switch (sys_cache->keytypes[i])
		{
			case NAMEOID:
				{
					char	   *name = NameStr(*O_KEY_GET_NAME(key, i));

					appendStringInfo(buf, "\"%s\"", name);
				}
				break;

			default:
				appendStringInfo(buf, "%lu", key->keys[i]);
				break;
		}
	}
	appendStringInfo(buf, ")");
}

/*
 * Generic non-TOAST sys cache key print function for o_print_btree_pages()
 */
void
o_sys_cache_key_print(BTreeDescr *desc, StringInfo buf, OTuple key_tup,
					  Pointer arg)
{
	OSysCacheKey *key = (OSysCacheKey *) key_tup.data;
	uint32		id,
				off;

	/* Decode ID and offset */
	id = (uint32) (key->common.lsn >> 32);
	off = (uint32) key->common.lsn;

	appendStringInfo(buf, "(%u, ", key->common.datoid);
	o_sys_cache_keys_to_str(buf, get_o_sys_cache(desc->oids.reloid), key);
	appendStringInfo(buf, ", %X/%X, %c)", id, off,
					 key->common.deleted ? 'Y' : 'N');
}

static void
o_sys_cache_keys_push_to_jsonb_state(OSysCache *sys_cache,
									 OSysCacheKey *key,
									 JsonbParseState **state)
{
	int			i;

	jsonb_push_key(state, "keys");
	(void) pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);
	for (i = 0; i < sys_cache->nkeys; i++)
	{
		switch (sys_cache->keytypes[i])
		{
			case NAMEOID:
				{
					JsonbValue	jval;
					char	   *name;

					name = NameStr(*O_KEY_GET_NAME(key, i));

					jval.type = jbvString;
					jval.val.string.len = strlen(name);
					jval.val.string.val = name;
					(void) pushJsonbValue(state, WJB_ELEM, &jval);
				}
				break;

			default:
				{
					Datum		res;
					JsonbValue	jval;

					res = DirectFunctionCall1(int8_numeric,
											  Int64GetDatum(key->keys[i]));

					jval.type = jbvNumeric;
					jval.val.numeric = DatumGetNumeric(res);
					(void) pushJsonbValue(state, WJB_ELEM, &jval);
				}
				break;
		}
	}
	(void) pushJsonbValue(state, WJB_END_ARRAY, NULL);
}

static void
o_sys_cache_key_push_to_jsonb_state(BTreeDescr *desc, OSysCacheKey *key,
									JsonbParseState **state)
{
	StringInfo	str;

	jsonb_push_int8_key(state, "datoid", key->common.datoid);
	jsonb_push_int8_key(state, "lsn", key->common.lsn);
	jsonb_push_bool_key(state, "deleted", key->common.deleted);

	str = makeStringInfo();
	o_sys_cache_keys_push_to_jsonb_state(get_o_sys_cache(desc->oids.reloid),
										 key, state);
	pfree(str->data);
	pfree(str);
}

JsonbValue *
o_sys_cache_key_to_jsonb(BTreeDescr *desc, OTuple tup, JsonbParseState **state)
{
	OSysCacheKey *key = (OSysCacheKey *) tup.data;

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	o_sys_cache_key_push_to_jsonb_state(desc, key, state);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

int
o_sys_cache_toast_chunk_length(BTreeDescr *desc, OTuple tuple)
{
	Pointer		chunk = tuple.data;
	int			chunk_key_len;
	OTuple		tup = {0};
	OSysCacheToastChunkCommon *common;

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);

	common = (OSysCacheToastChunkCommon *) (chunk + chunk_key_len);

	return chunk_key_len + sizeof(OSysCacheToastChunkCommon) +
		common->dataLength;
}

/*
 * Comparison function for TOAST sys cache B-tree.
 *
 * If key kind BTreeKeyBound it expects OSysCacheToastKeyBound.
 * Otherwise it expects that OTuple passed.
 * It wraps OSysCacheToastChunkKey to OTuple to pass it to o_sys_cache_cmp.
 */
int
o_sys_cache_toast_cmp(BTreeDescr *desc, void *p1, BTreeKeyType k1,
					  void *p2, BTreeKeyType k2)
{
	uint32		offset1,
				offset2;
	OSysCacheKey *key1 = NULL;
	OSysCacheKey *key2 = NULL;
	OSysCacheKey4 _key = {0};
	OSysCacheBound _bound = {.key = (OSysCacheKey *) &_key};
	OTuple		key_tuple1 = {0},
				key_tuple2 = {0};
	Pointer		sys_cache_key_cmp_arg1 = NULL,
				sys_cache_key_cmp_arg2 = NULL;
	int			sys_cache_key_cmp_result;
	int			nkeys;

	nkeys = nkeys_for_desc(desc);
	_bound.nkeys = nkeys;

	if (k1 == BTreeKeyBound)
	{
		OSysCacheToastKeyBound *kb1 = (OSysCacheToastKeyBound *) p1;

		Assert(k2 != BTreeKeyBound);
		key1 = (OSysCacheKey *) &_key;
		key1->common = kb1->key->common;
		offset1 = kb1->common.offset;
		memcpy(key1->keys, kb1->key->keys, sizeof(Datum) * nkeys);
		if (kb1->lsn_cmp)
			k1 = BTreeKeyNonLeafKey;	/* make o_sys_cache_cmp to compare by
										 * lsn */
		else
			sys_cache_key_cmp_arg1 = (Pointer) &_bound;
	}
	else
	{
		OSysCacheToastChunkKey *chunk_key =
			((OSysCacheToastChunkKey *) ((OTuple *) p1)->data);

		key1 = &chunk_key->sys_cache_key;
		offset1 = chunk_key->common.offset;
	}

	if (!sys_cache_key_cmp_arg1)
	{
		key_tuple1.data = (Pointer) key1;
		sys_cache_key_cmp_arg1 = (Pointer) &key_tuple1;
	}

	if (k2 == BTreeKeyBound)
	{
		OSysCacheToastKeyBound *kb2 = (OSysCacheToastKeyBound *) p2;

		Assert(k1 != BTreeKeyBound);
		key2 = (OSysCacheKey *) &_key;
		key2->common = kb2->key->common;
		offset2 = kb2->common.offset;
		memcpy(key2->keys, kb2->key->keys, sizeof(Datum) * nkeys);
		if (kb2->lsn_cmp)
			k2 = BTreeKeyNonLeafKey;	/* make o_sys_cache_cmp to compare by
										 * lsn */
		else
			sys_cache_key_cmp_arg2 = (Pointer) &_bound;
	}
	else
	{
		OSysCacheToastChunkKey *chunk_key =
			((OSysCacheToastChunkKey *) ((OTuple *) p2)->data);

		key2 = &chunk_key->sys_cache_key;
		offset2 = chunk_key->common.offset;
	}

	if (!sys_cache_key_cmp_arg2)
	{
		key_tuple2.data = (Pointer) key2;
		sys_cache_key_cmp_arg2 = (Pointer) &key_tuple2;
	}

	sys_cache_key_cmp_result = o_sys_cache_cmp(desc,
											   sys_cache_key_cmp_arg1, k1,
											   sys_cache_key_cmp_arg2, k2);

	if (sys_cache_key_cmp_result != 0)
		return sys_cache_key_cmp_result;

	if (offset1 != offset2)
		return offset1 < offset2 ? -1 : 1;

	return 0;
}

/*
 * Generic TOAST sys cache key print function for o_print_btree_pages()
 */
void
o_sys_cache_toast_key_print(BTreeDescr *desc, StringInfo buf,
							OTuple tup, Pointer arg)
{
	OTuple		key_tup = {0};
	OSysCacheToastChunkKey *key = (OSysCacheToastChunkKey *) tup.data;

	appendStringInfo(buf, "(");
	key_tup.data = (Pointer) &key->sys_cache_key;
	o_sys_cache_key_print(desc, buf, key_tup, arg);
	appendStringInfo(buf, ", %u)",
					 key->common.offset);
}

JsonbValue *
o_sys_cache_toast_key_to_jsonb(BTreeDescr *desc, OTuple tup,
							   JsonbParseState **state)
{
	OSysCacheToastChunkKey *key = (OSysCacheToastChunkKey *) tup.data;

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	o_sys_cache_key_push_to_jsonb_state(desc, &key->sys_cache_key, state);
	jsonb_push_int8_key(state, "offset", key->common.offset);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_sys_cache_toast_tup_print(BTreeDescr *desc, StringInfo buf,
							OTuple tup, Pointer arg)
{
	OTuple		key_tup = {0};
	Pointer		chunk = tup.data;
	OSysCacheToastChunkCommon *common;
	int			chunk_key_len;

	chunk_key_len = o_btree_len(desc, key_tup, OKeyLength);

	common = (OSysCacheToastChunkCommon *) (chunk + chunk_key_len);

	appendStringInfo(buf, "(");
	key_tup.data = chunk;
	o_sys_cache_toast_key_print(desc, buf, key_tup, arg);
	appendStringInfo(buf, ", %u)", common->dataLength);
}

HeapTuple
o_auth_cache_search_htup(TupleDesc tupdesc, Oid authoid)
{
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_authid] = {0};
	bool		nulls[Natts_pg_authid] = {0};
	NameData	oname;

	Assert(authoid == BOOTSTRAP_SUPERUSERID);

	values[Anum_pg_authid_oid - 1] = ObjectIdGetDatum(BOOTSTRAP_SUPERUSERID);
	namestrcpy(&oname, "");
	values[Anum_pg_authid_rolname - 1] = NameGetDatum(&oname);
	values[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(true);

	nulls[Anum_pg_authid_rolpassword - 1] = true;
	nulls[Anum_pg_authid_rolvaliduntil - 1] = true;

	result = heap_form_tuple(tupdesc, values, nulls);
	return result;
}

bool
o_is_syscache_hooks_set()
{
	return SearchCatCacheInternal_hook == o_SearchCatCacheInternal_hook;
}

void
o_set_syscache_hooks()
{
	if (!IsTransactionState())
	{
		if (!CurrentResourceOwner)
		{
			if (!my_owner)
				my_owner = ResourceOwnerCreate(NULL, "orioledb o_fmgr_sql");
			CurrentResourceOwner = my_owner;
		}

		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
							   save_sec_context |
							   SECURITY_LOCAL_USERID_CHANGE);
		SearchCatCacheInternal_hook = o_SearchCatCacheInternal_hook;
		SearchCatCacheList_hook = o_SearchCatCacheList_hook;
		SysCacheGetAttr_hook = o_SysCacheGetAttr_hook;
		GetCatCacheHashValue_hook = o_GetCatCacheHashValue_hook;
		GetDefaultOpClass_hook = o_type_cache_default_opclass;
		load_typcache_tupdesc_hook = o_load_typcache_tupdesc_hook;
		load_enum_cache_data_hook = o_load_enum_cache_data_hook;
	}
}

void
o_reset_syscache_hooks()
{
	if (SearchCatCacheInternal_hook != NULL)
	{
		SearchCatCacheInternal_hook = NULL;
		SearchCatCacheList_hook = NULL;
		SysCacheGetAttr_hook = NULL;
		GetCatCacheHashValue_hook = NULL;
		GetDefaultOpClass_hook = NULL;
		load_typcache_tupdesc_hook = NULL;
		load_enum_cache_data_hook = NULL;
		SetUserIdAndSecContext(save_userid, save_sec_context);
		if (CurrentResourceOwner == my_owner)
		{
			CurrentResourceOwner = NULL;
		}
	}
}
