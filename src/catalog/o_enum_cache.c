/*-------------------------------------------------------------------------
 *
 * o_enum_cache.c
 *		Routines for orioledb enum and enumoid system caches.
 *
 * enum_cache is TOAST tree that contains cached enum and its values
 * metadata from pg_type.
 * enumoid_cache is tree that contains typeoids for enumoids,
 * that used to get type from enumoid in o_enum_cmp_internal_hook.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_enum_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/iterator.h"
#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"

#include "access/table.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

static OSysCache *enum_cache = NULL;
static OSysCache *enumoid_cache = NULL;

static void o_enum_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									Pointer arg);
static void o_enum_cache_free_entry(Pointer entry);

static void o_enumoid_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									   Pointer arg);
static void o_enumoid_cache_free_entry(Pointer entry);

/* Copied from typecache.c */
typedef struct
{
	Oid			enum_oid;		/* OID of one enum value */
	float4		sort_order;		/* its sort position */
} EnumItem;

/* Copied from typecache.c */
typedef struct TypeCacheEnumData
{
	Oid			bitmap_base;	/* OID corresponding to bit 0 of bitmapset */
	Bitmapset  *sorted_values;	/* Set of OIDs known to be in order */
	int			num_values;		/* total number of values in enum */
	EnumItem	enum_values[FLEXIBLE_ARRAY_MEMBER];
} TypeCacheEnumData;

O_SYS_CACHE_FUNCS(enum_cache, OEnum, 2);
O_SYS_CACHE_FUNCS(enumoid_cache, OEnumOid, 1);

static OSysCacheFuncs enum_cache_funcs =
{
	.free_entry = o_enum_cache_free_entry,
	.fill_entry = o_enum_cache_fill_entry
};

static OSysCacheFuncs enumoid_cache_funcs =
{
	.free_entry = o_enumoid_cache_free_entry,
	.fill_entry = o_enumoid_cache_fill_entry
};

/*
 * Initializes the enum B-tree memory.
 */
O_SYS_CACHE_INIT_FUNC(enum_cache)
{
	Oid			keytypes[] = {OIDOID, NAMEOID};
	int			data_len = sizeof(OEnumData);

	enum_cache = o_create_sys_cache(SYS_TREES_ENUM_CACHE, false,
									EnumTypIdLabelIndexId, ENUMTYPOIDNAME, 2,
									keytypes, data_len, fastcache, mcxt,
									&enum_cache_funcs);
}

O_SYS_CACHE_INIT_FUNC(enumoid_cache)
{
	Oid			keytypes[] = {OIDOID};

	enumoid_cache = o_create_sys_cache(SYS_TREES_ENUMOID_CACHE, false,
									   EnumOidIndexId, ENUMOID, 1, keytypes, 0,
									   fastcache, mcxt, &enumoid_cache_funcs);
}

void
o_enum_cache_add_all(Oid datoid, Oid enum_oid, XLogRecPtr insert_lsn)
{
	Relation	enum_rel;
	SysScanDesc enum_scan;
	HeapTuple	enum_tuple;
	ScanKeyData skey;

	/* Scan pg_enum for the members of the target enum type. */
	ScanKeyInit(&skey, Anum_pg_enum_enumtypid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(enum_oid));

	enum_rel = table_open(EnumRelationId, AccessShareLock);
	enum_scan = systable_beginscan(enum_rel, EnumTypIdLabelIndexId, true, NULL,
								   1, &skey);

	while (HeapTupleIsValid(enum_tuple = systable_getnext(enum_scan)))
	{
		Form_pg_enum en = (Form_pg_enum) GETSTRUCT(enum_tuple);

		o_enum_cache_add_if_needed(datoid, ObjectIdGetDatum(en->enumtypid),
								   NameGetDatum(&en->enumlabel), insert_lsn,
								   NULL);
		o_enumoid_cache_add_if_needed(datoid, ObjectIdGetDatum(en->oid),
									  insert_lsn, NULL);
	}

	systable_endscan(enum_scan);
	table_close(enum_rel, AccessShareLock);
}

void
o_enum_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	HeapTuple	enumtup;
	Form_pg_enum enumform;
	OEnum	   *o_enum = (OEnum *) *entry_ptr;
	MemoryContext prev_context;
	Oid			enumtypid;
	Name		enumlabel;

	enumtypid = DatumGetObjectId(key->keys[0]);
	enumlabel = DatumGetName(key->keys[1]);

	enumtup = SearchSysCache2(ENUMTYPOIDNAME, key->keys[0], key->keys[1]);
	if (!HeapTupleIsValid(enumtup))
		elog(ERROR, "cache lookup failed for enum (%u %s)", enumtypid,
			 enumlabel->data);
	enumform = (Form_pg_enum) GETSTRUCT(enumtup);

	prev_context = MemoryContextSwitchTo(enum_cache->mcxt);
	if (o_enum != NULL)			/* Existed o_enum updated */
	{
		Assert(false);
	}
	else
	{
		o_enum = palloc0(sizeof(OEnum));
		*entry_ptr = (Pointer) o_enum;
	}

	o_enum->data.oid = enumform->oid;
	o_enum->data.enumsortorder = enumform->enumsortorder;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(enumtup);
}

void
o_enumoid_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	HeapTuple	enumtup;
	Form_pg_enum enumform;
	OEnumOid   *o_enumoid = (OEnumOid *) *entry_ptr;
	MemoryContext prev_context;
	Oid			enum_oid;

	enum_oid = DatumGetObjectId(key->keys[0]);

	enumtup = SearchSysCache1(ENUMOID, key->keys[0]);
	if (!HeapTupleIsValid(enumtup))
		elog(ERROR, "cache lookup failed for enum (%u)", enum_oid);
	enumform = (Form_pg_enum) GETSTRUCT(enumtup);

	prev_context = MemoryContextSwitchTo(enumoid_cache->mcxt);
	if (o_enumoid != NULL)		/* Existed o_enum updated */
	{
		Assert(false);
	}
	else
	{
		o_enumoid = palloc0(sizeof(OEnumOid));
		*entry_ptr = (Pointer) o_enumoid;
	}

	o_enumoid->enumtypid = enumform->enumtypid;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(enumtup);
}

void
o_enum_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

void
o_enumoid_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_enum_cache_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup,
					   Pointer arg)
{
	OSysCacheKey *key = (OSysCacheKey *) tup.data;
	OEnumData  *o_enum_data;

	o_enum_data = (OEnumData *) (tup.data + offsetof(OEnum, data) +
								 key->common.dataLength);
	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", oid: %u, enumsortorder: %f)",
					 o_enum_data->oid,
					 o_enum_data->enumsortorder);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_enumoid_cache_tup_print(BTreeDescr *desc, StringInfo buf,
						  OTuple tup, Pointer arg)
{
	OEnumOid   *o_enumoid = (OEnumOid *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", %u)", o_enumoid->enumtypid);
}

void
o_enum_cache_delete_all(Oid datoid, Oid enum_oid)
{
	BTreeDescr *td = get_sys_tree(enum_cache->sys_tree_num);
	BTreeIterator *it;
	OSysCacheKey2 key = {0};
	OSysCacheBound bound = {.key = (OSysCacheKey *) &key,
	.nkeys = 1};

	o_sys_cache_set_datoid_lsn(&key.common.lsn, &key.common.datoid);
	key.keys[0] = ObjectIdGetDatum(enum_oid);

	it = o_btree_iterator_create(td, (Pointer) &bound, BTreeKeyBound,
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	do
	{
		OTuple		tup = o_btree_iterator_fetch(it, NULL,
												 (Pointer) &bound,
												 BTreeKeyBound, true,
												 NULL);
		OEnum	   *o_enum = (OEnum *) tup.data;
		OEnumData  *o_enum_data;

		if (O_TUPLE_IS_NULL(tup))
			break;

		if (o_enum->key.common.lsn > key.common.lsn)
			break;

		o_enum_data =
			(OEnumData *) (((Pointer) o_enum) + offsetof(OEnum, data) +
						   o_enum->key.common.dataLength);
		o_enum_cache_delete(datoid, o_enum->key.keys[0],
							NameGetDatum(O_KEY_GET_NAME(&o_enum->key, 1)));
		o_enumoid_cache_delete(datoid, o_enum_data->oid);
	} while (true);
}

HeapTuple
o_enumoid_cache_search_htup(TupleDesc tupdesc, Oid enum_oid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_enum] = {0};
	bool		nulls[Natts_pg_enum] = {0};
	OEnumOid   *o_enumoid;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_enumoid = o_enumoid_cache_search(datoid, enum_oid, cur_lsn,
									   enumoid_cache->nkeys);
	if (o_enumoid)
	{
		NameData	enumlabel;

		values[Anum_pg_enum_oid - 1] = o_enumoid->key.keys[0];
		values[Anum_pg_enum_enumtypid - 1] =
			ObjectIdGetDatum(o_enumoid->enumtypid);
		namestrcpy(&enumlabel, "");
		values[Anum_pg_enum_enumlabel - 1] = NameGetDatum(&enumlabel);
		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}

HeapTuple
o_enum_cache_search_htup(TupleDesc tupdesc, Oid enumtypid, Name enumlabel)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_enum] = {0};
	bool		nulls[Natts_pg_enum] = {0};
	OEnum	   *o_enum;
	OEnumData  *o_enum_data;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_enum = o_enum_cache_search(datoid, enumtypid, NameGetDatum(enumlabel),
								 cur_lsn, enum_cache->nkeys);
	if (o_enum)
	{
		NameData	enumlabel;

		o_enum_data =
			(OEnumData *) (((Pointer) o_enum) + offsetof(OEnum, data) +
						   o_enum->key.common.dataLength);
		values[Anum_pg_enum_enumtypid - 1] = o_enum->key.keys[0];
		namestrcpy(&enumlabel, DatumGetName(o_enum->key.keys[0])->data);
		values[Anum_pg_enum_enumlabel - 1] = NameGetDatum(&enumlabel);
		values[Anum_pg_enum_oid - 1] = ObjectIdGetDatum(o_enum_data->oid);
		values[Anum_pg_enum_enumsortorder - 1] =
			Float4GetDatum(o_enum_data->enumsortorder);
		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}

/*
 * qsort comparison function for OID-ordered EnumItems
 */
static int
enum_oid_cmp(const void *left, const void *right)
{
	const EnumItem *l = (const EnumItem *) left;
	const EnumItem *r = (const EnumItem *) right;

	if (l->enum_oid < r->enum_oid)
		return -1;
	else if (l->enum_oid > r->enum_oid)
		return 1;
	else
		return 0;
}

/*
 * Load (or re-load) the enumData member of the typcache entry.
 */
void
o_load_enum_cache_data_hook(TypeCacheEntry *tcache)
{
	TypeCacheEnumData *enumdata;
	BTreeDescr *td = get_sys_tree(enum_cache->sys_tree_num);
	BTreeIterator *it;
	OSysCacheKey2 key = {0};
	OSysCacheBound bound = {.key = (OSysCacheKey *) &key,
	.nkeys = 1};
	EnumItem   *items;
	int			numitems;
	int			maxitems;
	Oid			bitmap_base;
	Bitmapset  *bitmap;
	MemoryContext oldcxt;
	int			bm_size,
				start_pos;

	/* Check that this is actually an enum */
	if (tcache->typtype != TYPTYPE_ENUM)
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s is not an enum",
							   format_type_be(tcache->type_id))));

	/*
	 * Read all the information for members of the enum type.  We collect the
	 * info in working memory in the caller's context, and then transfer it to
	 * permanent memory in CacheMemoryContext.  This minimizes the risk of
	 * leaking memory from CacheMemoryContext in the event of an error partway
	 * through.
	 */
	maxitems = 64;
	items = (EnumItem *) palloc(sizeof(EnumItem) * maxitems);
	numitems = 0;

	o_sys_cache_set_datoid_lsn(&key.common.lsn, &key.common.datoid);
	key.keys[0] = ObjectIdGetDatum(tcache->type_id);

	it = o_btree_iterator_create(td, (Pointer) &bound, BTreeKeyBound,
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	do
	{
		OTuple		tup = o_btree_iterator_fetch(it, NULL,
												 (Pointer) &bound,
												 BTreeKeyBound, true,
												 NULL);
		OEnum	   *o_enum = (OEnum *) tup.data;
		OEnumData  *o_enum_data;

		if (O_TUPLE_IS_NULL(tup))
			break;

		if (o_enum->key.common.lsn > key.common.lsn)
			break;

		if (numitems >= maxitems)
		{
			maxitems *= 2;
			items = (EnumItem *) repalloc(items, sizeof(EnumItem) * maxitems);
		}
		o_enum_data =
			(OEnumData *) (((Pointer) o_enum) + offsetof(OEnum, data) +
						   o_enum->key.common.dataLength);
		items[numitems].enum_oid = o_enum_data->oid;
		items[numitems].sort_order = o_enum_data->enumsortorder;
		numitems++;
	} while (true);

	/* Sort the items into OID order */
	qsort(items, numitems, sizeof(EnumItem), enum_oid_cmp);

	/*
	 * Here, we create a bitmap listing a subset of the enum's OIDs that are
	 * known to be in order and can thus be compared with just OID comparison.
	 *
	 * The point of this is that the enum's initial OIDs were certainly in
	 * order, so there is some subset that can be compared via OID comparison;
	 * and we'd rather not do binary searches unnecessarily.
	 *
	 * This is somewhat heuristic, and might identify a subset of OIDs that
	 * isn't exactly what the type started with.  That's okay as long as the
	 * subset is correctly sorted.
	 */
	bitmap_base = InvalidOid;
	bitmap = NULL;
	bm_size = 1;				/* only save sets of at least 2 OIDs */

	for (start_pos = 0; start_pos < numitems - 1; start_pos++)
	{
		/*
		 * Identify longest sorted subsequence starting at start_pos
		 */
		Bitmapset  *this_bitmap = bms_make_singleton(0);
		int			this_bm_size = 1;
		Oid			start_oid = items[start_pos].enum_oid;
		float4		prev_order = items[start_pos].sort_order;
		int			i;

		for (i = start_pos + 1; i < numitems; i++)
		{
			Oid			offset;

			offset = items[i].enum_oid - start_oid;
			/* quit if bitmap would be too large; cutoff is arbitrary */
			if (offset >= 8192)
				break;
			/* include the item if it's in-order */
			if (items[i].sort_order > prev_order)
			{
				prev_order = items[i].sort_order;
				this_bitmap = bms_add_member(this_bitmap, (int) offset);
				this_bm_size++;
			}
		}

		/* Remember it if larger than previous best */
		if (this_bm_size > bm_size)
		{
			bms_free(bitmap);
			bitmap_base = start_oid;
			bitmap = this_bitmap;
			bm_size = this_bm_size;
		}
		else
			bms_free(this_bitmap);

		/*
		 * Done if it's not possible to find a longer sequence in the rest of
		 * the list.  In typical cases this will happen on the first
		 * iteration, which is why we create the bitmaps on the fly instead of
		 * doing a second pass over the list.
		 */
		if (bm_size >= (numitems - start_pos - 1))
			break;
	}

	/* OK, copy the data into CacheMemoryContext */
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
	enumdata = (TypeCacheEnumData *)
		palloc(offsetof(TypeCacheEnumData, enum_values) +
			   numitems * sizeof(EnumItem));
	enumdata->bitmap_base = bitmap_base;
	enumdata->sorted_values = bms_copy(bitmap);
	enumdata->num_values = numitems;
	memcpy(enumdata->enum_values, items, numitems * sizeof(EnumItem));
	MemoryContextSwitchTo(oldcxt);

	pfree(items);
	bms_free(bitmap);

	/* And link the finished cache struct into the typcache */
	if (tcache->enumData != NULL)
		pfree(tcache->enumData);
	tcache->enumData = enumdata;
}
