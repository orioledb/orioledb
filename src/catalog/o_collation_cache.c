/*-------------------------------------------------------------------------
 *
 *  o_collation_cache.c
 *		Routines for orioledb collate cache.
 *
 * collate_cache is tree that contains cached metadata from pg_collate.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_collation_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"
#include "catalog/pg_collation.h"
#include "utils/syscache.h"

static OSysCache *collation_cache = NULL;

typedef struct OCollation
{
	OSysCacheKey1 key;
	uint16		data_version;
	char		collprovider;
	bool		collisdeterministic;
	NameData	collname;
	char	   *collcollate;
	char	   *collctype;
	char	   *colliculocale;
	char	   *collicurules;
	char	   *collversion;
} OCollation;

static void o_collation_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
										 Pointer arg);
static void o_collation_cache_free_entry(Pointer entry);
static Pointer o_collation_cache_serialize_entry(Pointer entry, int *len);
static Pointer o_collation_cache_deserialize_entry(MemoryContext mcxt,
												   Pointer data, Size length);

O_SYS_CACHE_FUNCS(collation_cache, OCollation, 1);

static OSysCacheFuncs collation_cache_funcs =
{
	.free_entry = o_collation_cache_free_entry,
	.fill_entry = o_collation_cache_fill_entry,
	.toast_serialize_entry = o_collation_cache_serialize_entry,
	.toast_deserialize_entry = o_collation_cache_deserialize_entry,
};

/*
 * Initializes the collation sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(collation_cache)
{
	Oid			keytypes[] = {OIDOID};

	collation_cache = o_create_sys_cache(SYS_TREES_COLLATION_CACHE, true,
										 CollationOidIndexId, COLLOID, 1,
										 keytypes, 0, fastcache, mcxt,
										 &collation_cache_funcs);
}

void
o_collation_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
							 Pointer arg)
{
	HeapTuple	collationtup;
	Form_pg_collation collform;
	OCollation *o_collation = (OCollation *) *entry_ptr;
	MemoryContext prev_context;
	Oid			colloid;
	Datum		datum;
	bool		isNull;
	bool		valid;

	colloid = DatumGetObjectId(key->keys[0]);

	collationtup = SearchSysCache1(COLLOID, key->keys[0]);
	if (!HeapTupleIsValid(collationtup))
		elog(ERROR, "cache lookup failed for collation (%u)", colloid);
	collform = (Form_pg_collation) GETSTRUCT(collationtup);

#if PG_VERSION_NUM < 180000
	valid = collform->collprovider == COLLPROVIDER_ICU ||
		lc_collate_is_c(colloid);
#else
	valid = collform->collprovider == COLLPROVIDER_ICU ||
		pg_newlocale_from_collation(colloid)->collate_is_c;
#endif

	valid = valid || (colloid == DEFAULT_COLLATION_OID &&
					  pg_newlocale_from_collation(DEFAULT_COLLATION_OID) != NULL &&
					  pg_newlocale_from_collation(DEFAULT_COLLATION_OID)->provider == COLLPROVIDER_ICU);
	if (!valid)
		elog(ERROR,
			 "Only C, POSIX and ICU collations supported for orioledb tables");

	prev_context = MemoryContextSwitchTo(collation_cache->mcxt);
	if (o_collation != NULL)	/* Existed o_collation updated */
	{
		Assert(false);
	}
	else
	{
		o_collation = palloc0(sizeof(OCollation));
		*entry_ptr = (Pointer) o_collation;
	}

	o_collation->data_version = ORIOLEDB_SYS_TREE_VERSION;
	o_collation->collname = collform->collname;
	o_collation->collprovider = collform->collprovider;
	o_collation->collisdeterministic = collform->collisdeterministic;

	datum = SysCacheGetAttr(COLLOID, collationtup,
							Anum_pg_collation_collcollate, &isNull);
	if (!isNull)
		o_collation->collcollate = TextDatumGetCString(datum);
	else
		o_collation->collcollate = NULL;

	datum = SysCacheGetAttr(COLLOID, collationtup,
							Anum_pg_collation_collctype, &isNull);
	if (!isNull)
		o_collation->collctype = TextDatumGetCString(datum);
	else
		o_collation->collctype = NULL;

#if PG_VERSION_NUM >= 170000
	datum = SysCacheGetAttr(COLLOID, collationtup,
							Anum_pg_collation_colllocale, &isNull);
#else
	datum = SysCacheGetAttr(COLLOID, collationtup,
							Anum_pg_collation_colliculocale, &isNull);
#endif
	if (!isNull)
		o_collation->colliculocale = TextDatumGetCString(datum);
	else
		o_collation->colliculocale = NULL;

	datum = SysCacheGetAttr(COLLOID, collationtup,
							Anum_pg_collation_collicurules, &isNull);
	if (!isNull)
		o_collation->collicurules = TextDatumGetCString(datum);
	else
		o_collation->collicurules = NULL;
	datum = SysCacheGetAttr(COLLOID, collationtup,
							Anum_pg_collation_collversion, &isNull);
	if (!isNull)
		o_collation->collversion = TextDatumGetCString(datum);
	else
		o_collation->collversion = NULL;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(collationtup);
}

void
o_collation_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

Pointer
o_collation_cache_serialize_entry(Pointer entry, int *len)
{
	StringInfoData str;
	OCollation *o_collation = (OCollation *) entry;

	if (o_collation->data_version != ORIOLEDB_SYS_TREE_VERSION)
		elog(FATAL,
			 "ORIOLEDB_SYS_TREE_VERSION %u of OrioleDB cluster is not among supported for conversion from %u",
			 o_collation->data_version, ORIOLEDB_SYS_TREE_VERSION);

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_collation,
						   offsetof(OCollation, collcollate));

	o_serialize_string(o_collation->collcollate, &str);
	o_serialize_string(o_collation->collctype, &str);
	o_serialize_string(o_collation->colliculocale, &str);
	o_serialize_string(o_collation->collicurules, &str);
	o_serialize_string(o_collation->collversion, &str);

	*len = str.len;
	return str.data;
}

Pointer
o_collation_cache_deserialize_entry(MemoryContext mcxt, Pointer data,
									Size length)
{
	Pointer		ptr = data;
	OCollation *o_collation;
	int			len;

	o_collation = (OCollation *) palloc0(sizeof(OCollation));
	len = offsetof(OCollation, collcollate);
	Assert((ptr - data) + len <= length);
	memcpy(o_collation, ptr, len);
	ptr += len;
	if (o_collation->data_version != ORIOLEDB_SYS_TREE_VERSION)
		elog(FATAL,
			 "ORIOLEDB_SYS_TREE_VERSION %u of OrioleDB cluster is not among supported for conversion to %u",
			 o_collation->data_version, ORIOLEDB_SYS_TREE_VERSION);

	o_collation->collcollate = o_deserialize_string(&ptr);
	o_collation->collctype = o_deserialize_string(&ptr);
	o_collation->colliculocale = o_deserialize_string(&ptr);
	o_collation->collicurules = o_deserialize_string(&ptr);
	o_collation->collversion = o_deserialize_string(&ptr);

	return (Pointer) o_collation;
}

HeapTuple
o_collation_cache_search_htup(TupleDesc tupdesc, Oid colloid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_collation] = {0};
	bool		nulls[Natts_pg_collation] = {0};
	OCollation *o_collation;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_collation = o_collation_cache_search(datoid, colloid, cur_lsn,
										   collation_cache->nkeys);
	if (o_collation)
	{
		values[Anum_pg_collation_oid - 1] = ObjectIdGetDatum(colloid);
		values[Anum_pg_collation_collname - 1] =
			NameGetDatum(&o_collation->collname);
		values[Anum_pg_collation_collprovider - 1] =
			CharGetDatum(o_collation->collprovider);
		values[Anum_pg_collation_collisdeterministic - 1] =
			BoolGetDatum(o_collation->collisdeterministic);

		nulls[Anum_pg_collation_collversion - 1] = true;
		if (o_collation->collcollate)
			values[Anum_pg_collation_collcollate - 1] =
				CStringGetTextDatum(o_collation->collcollate);
		else
			nulls[Anum_pg_collation_collcollate - 1] = true;

		if (o_collation->collctype)
			values[Anum_pg_collation_collctype - 1] =
				CStringGetTextDatum(o_collation->collctype);
		else
			nulls[Anum_pg_collation_collctype - 1] = true;

#if PG_VERSION_NUM >= 170000
		if (o_collation->colliculocale)
			values[Anum_pg_collation_colllocale - 1] =
				CStringGetTextDatum(o_collation->colliculocale);
		else
			nulls[Anum_pg_collation_colllocale - 1] = true;
#else
		if (o_collation->colliculocale)
			values[Anum_pg_collation_colliculocale - 1] =
				CStringGetTextDatum(o_collation->colliculocale);
		else
			nulls[Anum_pg_collation_colliculocale - 1] = true;
#endif
		if (o_collation->collicurules)
			values[Anum_pg_collation_collicurules - 1] =
				CStringGetTextDatum(o_collation->collicurules);
		else
			nulls[Anum_pg_collation_collicurules - 1] = true;
		if (o_collation->collversion)
			values[Anum_pg_collation_collversion - 1] =
				CStringGetTextDatum(o_collation->collversion);
		else
			nulls[Anum_pg_collation_collversion - 1] = true;

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}

void
orioledb_save_collation(Oid colloid)
{
	if (OidIsValid(colloid))
	{
		XLogRecPtr	cur_lsn;
		Oid			datoid;
		OClassArg	arg = {.sys_table = true};

		o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
		o_class_cache_add_if_needed(datoid, CollationRelationId, cur_lsn,
									(Pointer) &arg);
		o_collation_cache_add_if_needed(datoid, colloid, cur_lsn, NULL);
	}
}
