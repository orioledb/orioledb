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

#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"

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
#include "utils/syscache.h"

static OSysCache *enum_cache = NULL;
static OSysCache *enumoid_cache = NULL;

static void o_enum_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									Pointer arg);
static Pointer o_enum_cache_serialize_entry(Pointer entry,
											int *len);
static Pointer o_enum_cache_deserialize_entry(MemoryContext mcxt, Pointer data,
											  Size length);
static void o_enum_cache_free_entry(Pointer entry);
static void o_enum_cache_delete_hook(Pointer entry);

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

/* Copied fields of TypeCacheEnumData */
struct OEnum
{
	OSysCacheKey1 key;
	Oid			bitmap_base;	/* OID corresponding to bit 0 of bitmapset */
	Bitmapset  *sorted_values;	/* Set of OIDs known to be in order */
	int			num_values;		/* total number of values in enum */
	EnumItem   *enum_values;
};

O_SYS_CACHE_FUNCS(enum_cache, OEnum, 1);
O_SYS_CACHE_FUNCS(enumoid_cache, OEnumOid, 1);

static OSysCacheFuncs enum_cache_funcs =
{
	.free_entry = o_enum_cache_free_entry,
	.fill_entry = o_enum_cache_fill_entry,
	.toast_serialize_entry = o_enum_cache_serialize_entry,
	.toast_deserialize_entry = o_enum_cache_deserialize_entry,
	.delete_hook = o_enum_cache_delete_hook
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
	Oid			keytypes[] = {OIDOID};

	enum_cache = o_create_sys_cache(SYS_TREES_ENUM_CACHE,
									true, false,
									TypeOidIndexId, TYPEOID, 1,
									keytypes, fastcache,
									mcxt,
									&enum_cache_funcs);
}

O_SYS_CACHE_INIT_FUNC(enumoid_cache)
{
	Oid			keytypes[] = {OIDOID};

	enumoid_cache = o_create_sys_cache(SYS_TREES_ENUMOID_CACHE,
									   false, false,
									   EnumOidIndexId, ENUMOID, 1,
									   keytypes, fastcache,
									   mcxt,
									   &enumoid_cache_funcs);
}

void
o_enum_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	MemoryContext prev_context;
	TypeCacheEntry *typcache;
	int			i;
	Size		enum_vals_len;
	OEnum	   *o_enum = (OEnum *) *entry_ptr;
	Oid			typoid = DatumGetObjectId(key->keys[0]);

	typcache = lookup_type_cache(typoid, 0);
	load_enum_cache_data(typcache);

	prev_context = MemoryContextSwitchTo(enum_cache->mcxt);
	enum_vals_len = typcache->enumData->num_values * sizeof(EnumItem);
	if (o_enum != NULL)			/* Existed o_enum updated */
	{
		bms_free(o_enum->sorted_values);
		o_enum->enum_values = repalloc(o_enum->enum_values, enum_vals_len);
	}
	else
	{
		o_enum = palloc0(sizeof(OEnum));
		*entry_ptr = (Pointer) o_enum;
		o_enum->enum_values = palloc0(enum_vals_len);
	}
	o_enum->num_values = typcache->enumData->num_values;
	o_enum->bitmap_base = typcache->enumData->bitmap_base;
	o_enum->sorted_values = bms_copy(typcache->enumData->sorted_values);

	for (i = 0; i < o_enum->num_values; i++)
	{
		EnumItem   *o_enum_value = &o_enum->enum_values[i],
				   *typecache_value = &typcache->enumData->enum_values[i];

		o_enum_value->enum_oid = typecache_value->enum_oid;
		o_enum_value->sort_order = typecache_value->sort_order;

		o_enumoid_cache_add_if_needed(key->common.datoid,
									  o_enum_value->enum_oid,
									  key->common.lsn, (Pointer) &typoid);
	}
	MemoryContextSwitchTo(prev_context);
}

void
o_enumoid_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	OEnumOid   *o_enumoid = (OEnumOid *) *entry_ptr;
	Oid		   *typoid = (Oid *) arg;

	if (o_enumoid == NULL)
	{
		o_enumoid = palloc0(sizeof(OEnumOid));
		*entry_ptr = (Pointer) o_enumoid;
	}

	o_enumoid->enumtypid = *typoid;
}

void
o_enum_cache_free_entry(Pointer entry)
{
	OEnum	   *o_enum = (OEnum *) entry;

	bms_free(o_enum->sorted_values);
	pfree(o_enum->enum_values);
	pfree(o_enum);
}

void
o_enum_cache_delete_hook(Pointer entry)
{
	int			i;
	OEnum	   *o_enum = (OEnum *) entry;

	for (i = 0; i < o_enum->num_values; i++)
	{
		Oid			enumoid = o_enum->enum_values[i].enum_oid;

		o_enumoid_cache_delete(o_enum->key.common.datoid, enumoid);
	}
}

void
o_enumoid_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

Pointer
o_enum_cache_serialize_entry(Pointer entry, int *len)
{
	StringInfoData str;
	OEnum	   *o_enum = (OEnum *) entry;

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_enum,
						   offsetof(OEnum, sorted_values));
	appendBinaryStringInfo(&str, (Pointer) o_enum->sorted_values,
						   offsetof(Bitmapset, words));
	appendBinaryStringInfo(&str, (Pointer) &o_enum->sorted_values->words,
						   o_enum->sorted_values->nwords *
						   sizeof(bitmapword));
	appendBinaryStringInfo(&str, (Pointer) &o_enum->num_values,
						   sizeof(int));
	appendBinaryStringInfo(&str, (Pointer) o_enum->enum_values,
						   o_enum->num_values * sizeof(EnumItem));

	*len = str.len;
	return str.data;
}

Pointer
o_enum_cache_deserialize_entry(MemoryContext mcxt, Pointer data, Size length)
{
	Pointer		ptr = data;
	OEnum	   *o_enum = (OEnum *) data;
	int			len;

	o_enum = (OEnum *) palloc(sizeof(OEnum));
	len = offsetof(OEnum, sorted_values);
	Assert((ptr - data) + len <= length);
	memcpy(o_enum, ptr, len);
	ptr += len;

	len = offsetof(Bitmapset, words) +
		((Bitmapset *) ptr)->nwords * sizeof(bitmapword);
	o_enum->sorted_values = MemoryContextAlloc(mcxt, len);
	Assert((ptr - data) + len <= length);
	memcpy(o_enum->sorted_values, ptr, len);
	ptr += len;

	len = sizeof(int);
	Assert((ptr - data) + len <= length);
	memcpy(&o_enum->num_values, ptr, len);
	ptr += len;

	len = o_enum->num_values * sizeof(EnumItem);
	o_enum->enum_values = MemoryContextAlloc(mcxt, len);
	Assert((ptr - data) + len == length);
	memcpy(o_enum->enum_values, ptr, len);
	ptr += len;

	return (Pointer) o_enum;
}

TypeCacheEntry *
o_enum_cmp_internal_hook(Oid enum_oid, MemoryContext mcxt)
{
	TypeCacheEntry *typcache = NULL;
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OEnum	   *o_enum;
	MemoryContext prev_context;
	OEnumOid   *enumoid;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	enumoid = o_enumoid_cache_search(datoid, enum_oid, cur_lsn,
									 enumoid_cache->nkeys);
	o_enum = o_enum_cache_search(datoid, enumoid->enumtypid, cur_lsn,
								 enum_cache->nkeys);
	if (o_enum)
	{
		TypeCacheEnumData *enumData;
		Size		len;

		prev_context = MemoryContextSwitchTo(mcxt);
		typcache = palloc0(sizeof(TypeCacheEntry));
		typcache->type_id = enumoid->enumtypid;
		len = o_enum->num_values * sizeof(EnumItem);
		enumData = palloc0(offsetof(TypeCacheEnumData, enum_values) + len);
		enumData->bitmap_base = o_enum->bitmap_base;
		enumData->sorted_values = bms_copy(o_enum->sorted_values);
		enumData->num_values = o_enum->num_values;
		memcpy(&enumData->enum_values, o_enum->enum_values, len);
		typcache->enumData = enumData;
		MemoryContextSwitchTo(prev_context);
	}

	return typcache;
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
	appendStringInfo(buf, ", %d)", o_enumoid->enumtypid);
}
