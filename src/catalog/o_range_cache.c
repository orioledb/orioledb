/*-------------------------------------------------------------------------
 *
 * o_range_cache.c
 *		Routines for orioledb range type cache.
 *
 * range_cache is tree that contains cached range metadata from pg_type.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_range_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_type_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"

#include "access/htup_details.h"
#include "catalog/pg_range.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/fmgrtab.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static OTypeCache *range_cache = NULL;

static void o_range_cache_free_entry(Pointer entry);
static void o_range_cache_fill_entry(Pointer *entry_ptr, Oid datoid,
									 Oid typoid, XLogRecPtr insert_lsn,
									 Pointer arg);

O_TYPE_CACHE_FUNCS(range_cache, ORangeType);

static OTypeCacheFuncs range_cache_funcs =
{
	.free_entry = o_range_cache_free_entry,
	.fill_entry = o_range_cache_fill_entry
};

/*
 * Initializes the range type cache memory.
 */
O_TYPE_CACHE_INIT_FUNC(range_cache)
{
	range_cache = o_create_type_cache(SYS_TREES_RANGE_CACHE,
									  false,
									  TypeRelationId,
									  fastcache,
									  mcxt,
									  &range_cache_funcs);
}

void
o_range_cache_fill_entry(Pointer *entry_ptr, Oid datoid, Oid typoid,
						 XLogRecPtr insert_lsn, Pointer arg)
{
	TypeCacheEntry *typcache;
	ORangeType *o_range_type = (ORangeType *) *entry_ptr;

	/*
	 * find typecache entry
	 */
	typcache = lookup_type_cache(typoid, TYPECACHE_RANGE_INFO);
	if (typcache->rngelemtype == NULL)
		elog(ERROR, "type %u is not a range type", typoid);

	if (o_range_type == NULL)
	{
		o_range_type = palloc0(sizeof(ORangeType));
		*entry_ptr = (Pointer) o_range_type;
	}

	custom_type_add_if_needed(datoid,
							  typcache->rngelemtype->type_id,
							  insert_lsn);

	o_range_type->elem_typlen = typcache->rngelemtype->typlen;
	o_range_type->elem_typbyval = typcache->rngelemtype->typbyval;
	o_range_type->elem_typalign = typcache->rngelemtype->typalign;
	o_range_type->rng_collation = typcache->rng_collation;
	o_type_procedure_fill(typcache->rng_cmp_proc_finfo.fn_oid,
						  &o_range_type->rng_cmp_proc);
	o_range_type->rng_cmp_oid = typcache->rng_cmp_proc_finfo.fn_oid;
}

void
o_range_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

TypeCacheEntry *
o_range_cmp_hook(FunctionCallInfo fcinfo, Oid rngtypid,
				 MemoryContext mcxt)
{
	TypeCacheEntry *typcache = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;

	if (typcache == NULL ||
		typcache->type_id != rngtypid)
	{
		XLogRecPtr	cur_lsn = is_recovery_in_progress() ?
		GetXLogReplayRecPtr(NULL) :
		GetXLogWriteRecPtr();
		Oid			datoid;
		ORangeType *range_type;
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

		range_type = o_range_cache_search(datoid, rngtypid, cur_lsn);
		if (range_type)
		{
			typcache = palloc0(sizeof(TypeCacheEntry));
			typcache->type_id = rngtypid;
			typcache->rngelemtype = palloc0(sizeof(TypeCacheEntry));
			typcache->rngelemtype->typlen = range_type->elem_typlen;
			typcache->rngelemtype->typbyval = range_type->elem_typbyval;
			typcache->rngelemtype->typalign = range_type->elem_typalign;
			typcache->rng_collation = range_type->rng_collation;

			o_type_procedure_fill_finfo(&typcache->rng_cmp_proc_finfo,
										&range_type->rng_cmp_proc,
										range_type->rng_cmp_oid,
										2);

			fcinfo->flinfo->fn_extra = (void *) typcache;
			MemoryContextSwitchTo(prev_context);
		}
		else
			typcache = NULL;
	}

	return typcache;
}
