/*-------------------------------------------------------------------------
 *
 *  o_record_cache.c
 *		Routines for orioledb record type cache.
 *
 * record_cache is tree that contains cached range metadata from pg_type.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_record_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_type_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static OTypeCache *record_cache = NULL;

static Pointer o_record_cache_serialize_entry(Pointer entry,
											  int *len);
static Pointer o_record_cache_deserialize_entry(MemoryContext mcxt,
												Pointer data,
												Size length);
static void o_record_cache_free_entry(Pointer entry);
static Oid	o_record_cache_get_link_oid(Pointer entry);
static void fill_o_record(Pointer *entry_ptr, Oid datoid, Oid typoid,
						  XLogRecPtr insert_lsn, Pointer arg);

struct ORecord
{
	OTypeKey	key;
	Oid			typrelid;
	int			natts;
	FormData_pg_attribute *attrs;
};

O_TYPE_CACHE_FUNCS(record_cache, ORecord);

static OTypeCacheFuncs record_cache_funcs =
{
	.free_entry = o_record_cache_free_entry,
	.fill_entry = fill_o_record,
	.toast_serialize_entry = o_record_cache_serialize_entry,
	.toast_deserialize_entry = o_record_cache_deserialize_entry,
	.fastcache_get_link_oid = o_record_cache_get_link_oid
};

/*
 * Initializes the record type cache memory.
 */
O_TYPE_CACHE_INIT_FUNC(record_cache)
{
	record_cache = o_create_type_cache(SYS_TREES_RECORD_CACHE,
									   true,
									   TypeRelationId,
									   fastcache,
									   mcxt,
									   &record_cache_funcs);
}

void
fill_o_record(Pointer *entry_ptr, Oid datoid, Oid typoid,
			  XLogRecPtr insert_lsn, Pointer arg)
{
	MemoryContext prev_context;
	TypeCacheEntry *typcache;
	int			i;
	Size		len;
	ORecord    *o_record = (ORecord *) *entry_ptr;

	typcache = lookup_type_cache(typoid, TYPECACHE_TUPDESC);

	prev_context = MemoryContextSwitchTo(record_cache->mcxt);
	len = typcache->tupDesc->natts * sizeof(FormData_pg_attribute);
	if (o_record != NULL)		/* Existed o_record updated */
	{
		o_record->attrs = (FormData_pg_attribute *) repalloc(o_record->attrs,
															 len);
		memset(o_record->attrs, 0, len);
	}
	else
	{
		o_record = palloc0(sizeof(ORecord));
		*entry_ptr = (Pointer) o_record;
		o_record->attrs = (FormData_pg_attribute *) palloc0(len);
	}
	o_record->typrelid = typcache->typrelid;
	o_record->natts = typcache->tupDesc->natts;
	for (i = 0; i < o_record->natts; i++)
	{
		FormData_pg_attribute *record_attr = &o_record->attrs[i],
				   *typcache_attr =
		&typcache->tupDesc->attrs[i];

		record_attr->attrelid = typcache_attr->attrelid;
		record_attr->attname = typcache_attr->attname;
		record_attr->atttypid = typcache_attr->atttypid;
		record_attr->attstattarget = typcache_attr->attstattarget;
		record_attr->attlen = typcache_attr->attlen;
		record_attr->attnum = typcache_attr->attnum;
		record_attr->attndims = typcache_attr->attndims;
		record_attr->attcacheoff = typcache_attr->attcacheoff;
		record_attr->atttypmod = typcache_attr->atttypmod;
		record_attr->attbyval = typcache_attr->attbyval;
		record_attr->attstorage = typcache_attr->attstorage;
		record_attr->attalign = typcache_attr->attalign;
		record_attr->attnotnull = typcache_attr->attnotnull;
		record_attr->atthasdef = typcache_attr->atthasdef;
		record_attr->atthasmissing = typcache_attr->atthasmissing;
		record_attr->attidentity = typcache_attr->attidentity;
		record_attr->attgenerated = typcache_attr->attgenerated;
		record_attr->attisdropped = typcache_attr->attisdropped;
		record_attr->attislocal = typcache_attr->attislocal;
		record_attr->attinhcount = typcache_attr->attinhcount;
		record_attr->attcollation = typcache_attr->attcollation;

		if (!record_attr->attisdropped)
		{
			o_type_element_cache_add_if_needed(datoid,
											   record_attr->atttypid,
											   insert_lsn, NULL);
			custom_type_add_if_needed(datoid,
									  record_attr->atttypid,
									  insert_lsn);
		}
	}
	MemoryContextSwitchTo(prev_context);
}

void
o_record_cache_free_entry(Pointer entry)
{
	ORecord    *o_record = (ORecord *) entry;

	pfree(o_record->attrs);
	pfree(o_record);
}

/*
 * Records typerelid also added to cache to invalidate record pg_class cached
 * entry on record type alter.
 */
Oid
o_record_cache_get_link_oid(Pointer entry)
{
	ORecord    *o_record = (ORecord *) entry;

	return o_record->typrelid;
}

Pointer
o_record_cache_serialize_entry(Pointer entry, int *len)
{
	StringInfoData str;
	ORecord    *o_record = (ORecord *) entry;

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_record,
						   offsetof(ORecord, attrs));
	appendBinaryStringInfo(&str, (Pointer) o_record->attrs,
						   o_record->natts * sizeof(FormData_pg_attribute));

	*len = str.len;
	return str.data;

}

Pointer
o_record_cache_deserialize_entry(MemoryContext mcxt, Pointer data, Size length)
{
	Pointer		ptr = data;
	ORecord    *o_record = (ORecord *) data;
	int			len;

	o_record = (ORecord *) palloc(sizeof(ORecord));
	len = offsetof(ORecord, attrs);
	Assert((ptr - data) + len <= length);
	memcpy(o_record, ptr, len);
	ptr += len;

	len = o_record->natts * sizeof(FormData_pg_attribute);
	o_record->attrs = MemoryContextAlloc(mcxt, len);
	Assert((ptr - data) + len == length);
	memcpy(o_record->attrs, ptr, len);
	ptr += len;

	return (Pointer) o_record;
}

TupleDesc
o_record_cmp_hook(Oid type_id, MemoryContext mcxt)
{
	TupleDesc	result = NULL;
	XLogRecPtr	cur_lsn = is_recovery_in_progress() ?
	GetXLogReplayRecPtr(NULL) :
	GetXLogWriteRecPtr();
	Oid			datoid;
	ORecord    *o_record;
	MemoryContext prev_context = MemoryContextSwitchTo(mcxt);

	if (OidIsValid(MyDatabaseId))
	{
		datoid = MyDatabaseId;
	}
	else
	{
		Assert(OidIsValid(o_type_cmp_datoid));
		datoid = o_type_cmp_datoid;
	}

	o_record = o_record_cache_search(datoid, type_id, cur_lsn);
	if (o_record)
	{
		result = CreateTemplateTupleDesc(o_record->natts);
		memcpy(&result->attrs, o_record->attrs,
			   o_record->natts * sizeof(FormData_pg_attribute));
	}
	MemoryContextSwitchTo(prev_context);

	return result;
}
