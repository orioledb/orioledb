/*-------------------------------------------------------------------------
 *
 *  o_database_cache.c
 *		Routines for orioledb database cache.
 *
 * database_cache is tree that contains cached metadata from pg_database.
 *
 * Copyright (c) 2021-2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_database_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "utils/syscache.h"
#include "mb/pg_wchar.h"

static OSysCache *database_cache = NULL;

typedef struct ODatabase
{
	OSysCacheKey1 key;
	int32		encoding;
	char		datlocprovider;
#if PG_VERSION_NUM >= 170000
	char	   *datlocale;
	char	   *daticurules;
#endif
} ODatabase;

static void o_database_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
										Pointer arg);
static void o_database_cache_free_entry(Pointer entry);
static Pointer o_database_cache_serialize_entry(Pointer entry, int *len);
static Pointer o_database_cache_deserialize_entry(MemoryContext mcxt,
												  Pointer data, Size length);

O_SYS_CACHE_FUNCS(database_cache, ODatabase, 1);

static OSysCacheFuncs database_cache_funcs =
{
	.free_entry = o_database_cache_free_entry,
	.fill_entry = o_database_cache_fill_entry,
	.toast_serialize_entry = o_database_cache_serialize_entry,
	.toast_deserialize_entry = o_database_cache_deserialize_entry,
};

/*
 * Initializes the database sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(database_cache)
{
	Oid			keytypes[] = {OIDOID};

	database_cache = o_create_sys_cache(SYS_TREES_DATABASE_CACHE, true,
										DatabaseOidIndexId, DATABASEOID, 1,
										keytypes, 0, fastcache, mcxt,
										&database_cache_funcs);
}

void
o_database_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
							Pointer arg)
{
	HeapTuple	databasetup;
	Form_pg_database dbform;
	ODatabase  *o_database = (ODatabase *) *entry_ptr;
	MemoryContext prev_context;
	Oid			dboid;
#if PG_VERSION_NUM >= 170000
	Datum		datum;
	bool		isNull;
#endif

	dboid = DatumGetObjectId(key->keys[0]);

	databasetup = SearchSysCache1(DATABASEOID, key->keys[0]);
	if (!HeapTupleIsValid(databasetup))
		elog(ERROR, "cache lookup failed for database (%u)", dboid);
	dbform = (Form_pg_database) GETSTRUCT(databasetup);

	prev_context = MemoryContextSwitchTo(database_cache->mcxt);
	if (o_database != NULL)		/* Existed o_database updated */
	{
		Assert(false);
	}
	else
	{
		o_database = palloc0(sizeof(ODatabase));
		*entry_ptr = (Pointer) o_database;
	}

	o_database->encoding = dbform->encoding;
	o_database->datlocprovider = dbform->datlocprovider;

#if PG_VERSION_NUM >= 170000
	datum = SysCacheGetAttr(DATABASEOID, databasetup,
							Anum_pg_database_datlocale, &isNull);
	if (!isNull)
		o_database->datlocale = TextDatumGetCString(datum);
	else
		o_database->datlocale = NULL;

	datum = SysCacheGetAttr(DATABASEOID, databasetup,
							Anum_pg_database_daticurules, &isNull);
	if (!isNull)
		o_database->daticurules = TextDatumGetCString(datum);
	else
		o_database->daticurules = NULL;
#endif

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(databasetup);
}

void
o_database_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

int32
o_database_cache_get_database_encoding()
{
	XLogRecPtr	cur_lsn;
	ODatabase  *o_database;

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	o_database = o_database_cache_search(Template1DbOid, Template1DbOid, cur_lsn,
										 database_cache->nkeys);
	return o_database ? o_database->encoding : PG_SQL_ASCII;
}

void
o_database_cache_set_database_encoding()
{
	int32		encoding = o_database_cache_get_database_encoding();

	SetDatabaseEncoding(encoding);
}

#if PG_VERSION_NUM >= 170000
void
o_database_cache_set_default_locale_provider()
{
	XLogRecPtr	cur_lsn;
	ODatabase  *o_database;

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	o_database = o_database_cache_search(Template1DbOid, Template1DbOid, cur_lsn,
										 database_cache->nkeys);
	if (o_database)
	{
		if (o_database->datlocprovider == COLLPROVIDER_BUILTIN)
		{
			default_locale.info.builtin.locale = MemoryContextStrdup(TopMemoryContext,
																	 o_database->datlocale);
		}
		else if (o_database->datlocprovider == COLLPROVIDER_ICU)
		{
			make_icu_collator(o_database->datlocale, o_database->daticurules, &default_locale);
		}

		default_locale.provider = o_database->datlocprovider;
		default_locale.deterministic = true;
	}
	else
	{
		default_locale.provider = COLLPROVIDER_DEFAULT;
		default_locale.deterministic = true;
	}
}
#endif

Pointer
o_database_cache_serialize_entry(Pointer entry, int *len)
{
	StringInfoData str;
	ODatabase  *o_database = (ODatabase *) entry;

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_database,
						   offsetof(ODatabase, datlocprovider));

#if PG_VERSION_NUM >= 170000
	appendBinaryStringInfo(&str, ((Pointer) o_database) + offsetof(ODatabase, datlocprovider),
						   offsetof(ODatabase, datlocale) - offsetof(ODatabase, datlocprovider));
	o_serialize_string(o_database->datlocale, &str);
	o_serialize_string(o_database->daticurules, &str);
#else
	appendBinaryStringInfo(&str, ((Pointer) o_database) + offsetof(ODatabase, datlocprovider),
						   sizeof(ODatabase) - offsetof(ODatabase, datlocprovider));
#endif

	*len = str.len;
	return str.data;
}

Pointer
o_database_cache_deserialize_entry(MemoryContext mcxt, Pointer data,
								   Size length)
{
	Pointer		ptr = data;
	ODatabase  *o_database;
	int			len;

	o_database = (ODatabase *) palloc0(sizeof(ODatabase));
	len = offsetof(ODatabase, datlocprovider);
	Assert((ptr - data) + len <= length);
	memcpy(o_database, ptr, len);
	ptr += len;

#if PG_VERSION_NUM >= 170000
	len = offsetof(ODatabase, datlocale) - offsetof(ODatabase, datlocprovider);
#else
	len = sizeof(ODatabase) - offsetof(ODatabase, datlocprovider);
#endif
	Assert((ptr - data) + len <= length);
	memcpy(((Pointer) o_database) + offsetof(ODatabase, datlocprovider), ptr, len);
	ptr += len;

#if PG_VERSION_NUM >= 170000
	o_database->datlocale = o_deserialize_string(&ptr);
	o_database->daticurules = o_deserialize_string(&ptr);
#endif

	return (Pointer) o_database;
}
