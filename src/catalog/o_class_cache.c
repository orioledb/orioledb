/*-------------------------------------------------------------------------
 *
 *  o_class_cache.c
 *		Routines for orioledb class sys cache.
 *
 * class_cache is tree that contains cached range metadata from pg_type.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_class_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"

#include "access/htup_details.h"
#include "access/relation.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/catcache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static OSysCache *class_cache = NULL;

static Pointer o_class_cache_serialize_entry(Pointer entry,
											 int *len);
static Pointer o_class_cache_deserialize_entry(MemoryContext mcxt,
											   Pointer data,
											   Size length);
static void o_class_cache_free_entry(Pointer entry);
static void o_class_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									 Pointer arg);

struct OClass
{
	OSysCacheKey1 key;
	Oid			reltype;
	int			natts;
	FormData_pg_attribute *attrs;
};

O_SYS_CACHE_FUNCS(class_cache, OClass, 1);

static OSysCacheFuncs class_cache_funcs =
{
	.free_entry = o_class_cache_free_entry,
	.fill_entry = o_class_cache_fill_entry,
	.toast_serialize_entry = o_class_cache_serialize_entry,
	.toast_deserialize_entry = o_class_cache_deserialize_entry
};

/*
 * Initializes the record sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(class_cache)
{
	Oid			keytypes[] = {OIDOID};

	class_cache = o_create_sys_cache(SYS_TREES_CLASS_CACHE,
									 true, true,
									 ClassOidIndexId, RELOID, 1,
									 keytypes, fastcache,
									 mcxt,
									 &class_cache_funcs);
}

void
o_class_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	Relation	rel;
	MemoryContext prev_context;
	int			i;
	Size		len;
	OClass	   *o_class = (OClass *) *entry_ptr;
	OClassArg  *carg = (OClassArg *) arg;
	bool		sys_cache;
	Oid			classoid = DatumGetObjectId(key->keys[0]);

	sys_cache = carg && !carg->column_drop && carg->sys_table;

	rel = relation_open(classoid, AccessShareLock);

	prev_context = MemoryContextSwitchTo(class_cache->mcxt);
	len = rel->rd_att->natts * sizeof(FormData_pg_attribute);
	if (o_class != NULL)		/* Existed o_class updated */
	{
		o_class->attrs = (FormData_pg_attribute *) repalloc(o_class->attrs,
															len);
		memset(o_class->attrs, 0, len);
	}
	else
	{
		o_class = palloc0(sizeof(OClass));
		*entry_ptr = (Pointer) o_class;
		o_class->attrs = (FormData_pg_attribute *) palloc0(len);
	}
	o_class->reltype = rel->rd_rel->reltype;
	o_class->natts = rel->rd_att->natts;
	for (i = 0; i < o_class->natts; i++)
	{
		bool		process;
		FormData_pg_attribute *class_attr,
				   *typcache_attr;

		class_attr = &o_class->attrs[i];
		typcache_attr = &rel->rd_att->attrs[i];

		class_attr->attrelid = typcache_attr->attrelid;
		class_attr->attname = typcache_attr->attname;
		class_attr->atttypid = typcache_attr->atttypid;
		class_attr->attstattarget = typcache_attr->attstattarget;
		class_attr->attlen = typcache_attr->attlen;
		class_attr->attnum = typcache_attr->attnum;
		class_attr->attndims = typcache_attr->attndims;
		class_attr->attcacheoff = typcache_attr->attcacheoff;
		class_attr->atttypmod = typcache_attr->atttypmod;
		class_attr->attbyval = typcache_attr->attbyval;
		class_attr->attstorage = typcache_attr->attstorage;
		class_attr->attalign = typcache_attr->attalign;
		class_attr->attnotnull = typcache_attr->attnotnull;
		class_attr->atthasdef = typcache_attr->atthasdef;
		class_attr->atthasmissing = typcache_attr->atthasmissing;
		class_attr->attidentity = typcache_attr->attidentity;
		class_attr->attgenerated = typcache_attr->attgenerated;
		class_attr->attislocal = typcache_attr->attislocal;
		class_attr->attinhcount = typcache_attr->attinhcount;
		class_attr->attcollation = typcache_attr->attcollation;

		class_attr->attisdropped = typcache_attr->attisdropped ||
			(carg && carg->column_drop &&
			 carg->dropped - 1 == i);

		process = !class_attr->attisdropped && !sys_cache;
		if (process)
			o_composite_type_element_save(key->common.datoid,
										  class_attr->atttypid,
										  key->common.lsn);
	}
	MemoryContextSwitchTo(prev_context);
	relation_close(rel, AccessShareLock);
}

void
o_class_cache_free_entry(Pointer entry)
{
	OClass	   *o_class = (OClass *) entry;

	pfree(o_class->attrs);
	pfree(o_class);
}

Pointer
o_class_cache_serialize_entry(Pointer entry, int *len)
{
	StringInfoData str;
	OClass	   *o_class = (OClass *) entry;

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_class,
						   offsetof(OClass, attrs));
	appendBinaryStringInfo(&str, (Pointer) o_class->attrs,
						   o_class->natts * sizeof(FormData_pg_attribute));

	*len = str.len;
	return str.data;

}

Pointer
o_class_cache_deserialize_entry(MemoryContext mcxt, Pointer data, Size length)
{
	Pointer		ptr = data;
	OClass	   *o_class = (OClass *) data;
	int			len;

	o_class = (OClass *) palloc(sizeof(OClass));
	len = offsetof(OClass, attrs);
	Assert((ptr - data) + len <= length);
	memcpy(o_class, ptr, len);
	ptr += len;

	len = o_class->natts * sizeof(FormData_pg_attribute);
	o_class->attrs = MemoryContextAlloc(mcxt, len);
	Assert((ptr - data) + len == length);
	memcpy(o_class->attrs, ptr, len);
	ptr += len;

	return (Pointer) o_class;
}

TupleDesc
o_class_cache_search_tupdesc(Oid cc_reloid)
{
	TupleDesc	result = NULL;
	Oid			datoid;
	XLogRecPtr	cur_lsn;
	OClass	   *o_class;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);

	o_class = o_class_cache_search(datoid, cc_reloid, cur_lsn,
								   class_cache->nkeys);
	if (o_class)
	{
		result = CreateTemplateTupleDesc(o_class->natts);
		memcpy(&result->attrs, o_class->attrs,
			   o_class->natts * sizeof(FormData_pg_attribute));
	}
	return result;
}

TupleDesc
o_record_cmp_hook(Oid type_id, MemoryContext mcxt)
{
	TupleDesc	result = NULL;
	Oid			reloid;
	MemoryContext prev_context = MemoryContextSwitchTo(mcxt);

	reloid = o_type_cache_get_typrelid(type_id);

	Assert(OidIsValid(reloid));

	result = o_class_cache_search_tupdesc(reloid);
	MemoryContextSwitchTo(prev_context);

	return result;
}
