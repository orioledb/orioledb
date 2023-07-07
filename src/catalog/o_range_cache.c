/*-------------------------------------------------------------------------
 *
 * o_range_cache.c
 *		Routines for orioledb range sys cache.
 *
 * range_cache is tree that contains cached range metadata from pg_type.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_range_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"

#include "access/hash.h"
#include "access/htup_details.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_range.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/fmgrtab.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static OSysCache *range_cache = NULL;

static void o_range_cache_free_entry(Pointer entry);
static void o_range_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									 Pointer arg);

O_SYS_CACHE_FUNCS(range_cache, ORange, 1);

static OSysCacheFuncs range_cache_funcs =
{
	.free_entry = o_range_cache_free_entry,
	.fill_entry = o_range_cache_fill_entry
};

/*
 * Initializes the range sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(range_cache)
{
	Oid			keytypes[] = {OIDOID};

	range_cache = o_create_sys_cache(SYS_TREES_RANGE_CACHE, false,
									 RangeTypidIndexId, RANGETYPE, 1, keytypes,
									 0, fastcache, mcxt, &range_cache_funcs);
}

void
o_range_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	HeapTuple	rangetup;
	Form_pg_range rangeform;
	ORange	   *o_range = (ORange *) *entry_ptr;
	MemoryContext prev_context;
	Oid			rngtypid;

	rngtypid = DatumGetObjectId(key->keys[0]);

	rangetup = SearchSysCache1(RANGETYPE, key->keys[0]);
	if (!HeapTupleIsValid(rangetup))
		elog(ERROR, "cache lookup failed for range (%u)", rngtypid);
	rangeform = (Form_pg_range) GETSTRUCT(rangetup);

	prev_context = MemoryContextSwitchTo(range_cache->mcxt);
	if (o_range == NULL)
	{
		o_range = palloc0(sizeof(ORange));
		*entry_ptr = (Pointer) o_range;
	}

	o_range->rngsubtype = rangeform->rngsubtype;
	o_range->rngsubopc = rangeform->rngsubopc;
	o_range->rngcollation = rangeform->rngcollation;
	custom_type_add_if_needed(key->common.datoid, o_range->rngsubtype,
							  key->common.lsn);
	o_type_cache_add_if_needed(key->common.datoid, o_range->rngsubtype,
							   key->common.lsn, NULL);
	o_opclass_cache_add_if_needed(key->common.datoid, o_range->rngsubopc,
								  key->common.lsn, NULL);
	if (OidIsValid(o_range->rngcollation))
		o_collation_cache_add_if_needed(key->common.datoid,
										o_range->rngcollation, key->common.lsn,
										NULL);

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(rangetup);
}


void
o_range_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_range_cache_tup_print(BTreeDescr *desc, StringInfo buf,
						OTuple tup, Pointer arg)
{
	ORange	   *o_range = (ORange *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", rngsubtype: %u, rngcollation: %d, "
					 "rngsubopc: %u)",
					 o_range->rngsubtype, o_range->rngcollation,
					 o_range->rngsubopc);
}

HeapTuple
o_range_cache_search_htup(TupleDesc tupdesc, Oid rngtypid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_range] = {0};
	bool		nulls[Natts_pg_range] = {0};
	ORange	   *o_range;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_range =
		o_range_cache_search(datoid, rngtypid, cur_lsn, range_cache->nkeys);
	if (o_range)
	{
		values[Anum_pg_range_rngtypid - 1] = o_range->key.keys[0];
		values[Anum_pg_range_rngcollation - 1] =
			ObjectIdGetDatum(o_range->rngcollation);
		values[Anum_pg_range_rngsubopc - 1] =
			ObjectIdGetDatum(o_range->rngsubopc);
		values[Anum_pg_range_rngsubtype - 1] =
			ObjectIdGetDatum(o_range->rngsubtype);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}

void
o_range_cache_add_rngsubopc(Oid datoid, Oid rngtypid, XLogRecPtr insert_lsn)
{
	ORange	   *o_range;
	XLogRecPtr	sys_lsn;
	Oid			sys_datoid;
	OClassArg	class_arg = {.sys_table = true};
	Oid			btree_opf;
	Oid			btree_opintype;

	o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
	o_range =
		o_range_cache_search(datoid, rngtypid, insert_lsn, range_cache->nkeys);
	Assert(o_range);
	o_class_cache_add_if_needed(sys_datoid, OperatorClassRelationId, sys_lsn,
								(Pointer) &class_arg);
	o_class_cache_add_if_needed(sys_datoid, AccessMethodProcedureRelationId,
								sys_lsn, (Pointer) &class_arg);
	btree_opf = get_opclass_family(o_range->rngsubopc);
	btree_opintype = get_opclass_input_type(o_range->rngsubopc);
	o_amproc_cache_add_if_needed(datoid, btree_opf, btree_opintype,
								 btree_opintype, BTORDER_PROC, insert_lsn,
								 NULL);
}

#if PG_VERSION_NUM >= 140000

static OSysCache *multirange_cache = NULL;

static void o_multirange_cache_free_entry(Pointer entry);
static void o_multirange_cache_fill_entry(Pointer *entry_ptr,
										  OSysCacheKey *key,
										  Pointer arg);

O_SYS_CACHE_FUNCS(multirange_cache, OMultiRange, 1);
static OSysCacheFuncs multirange_cache_funcs =
{
	.free_entry = o_multirange_cache_free_entry,
	.fill_entry = o_multirange_cache_fill_entry
};

O_SYS_CACHE_INIT_FUNC(multirange_cache)
{
	Oid			keytypes[] = {OIDOID};

	multirange_cache = o_create_sys_cache(SYS_TREES_MULTIRANGE_CACHE, false,
										  RangeMultirangeTypidIndexId,
										  RANGEMULTIRANGE, 1, keytypes, 0,
										  fastcache, mcxt,
										  &multirange_cache_funcs);
}

void
o_multirange_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
							  Pointer arg)
{
	HeapTuple	rangetup;
	Form_pg_range rangeform;
	OMultiRange *o_multirange = (OMultiRange *) *entry_ptr;
	MemoryContext prev_context;
	Oid			rngtypid;

	rngtypid = DatumGetObjectId(key->keys[0]);

	rangetup = SearchSysCache1(RANGEMULTIRANGE, key->keys[0]);
	if (!HeapTupleIsValid(rangetup))
		elog(ERROR, "cache lookup failed for multirange (%u)", rngtypid);
	rangeform = (Form_pg_range) GETSTRUCT(rangetup);

	prev_context = MemoryContextSwitchTo(range_cache->mcxt);
	if (o_multirange == NULL)
	{
		o_multirange = palloc0(sizeof(OMultiRange));
		*entry_ptr = (Pointer) o_multirange;
	}

	o_multirange->rngtypid = rangeform->rngtypid;
	custom_type_add_if_needed(key->common.datoid, o_multirange->rngtypid,
							  key->common.lsn);

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(rangetup);
}

void
o_multirange_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

void
o_multirange_cache_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup,
							 Pointer arg)
{
	OMultiRange *o_multirange = (OMultiRange *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", rngtypid: %u)", o_multirange->rngtypid);
}

HeapTuple
o_multirange_cache_search_htup(TupleDesc tupdesc, Oid rngmultitypid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_range] = {0};
	bool		nulls[Natts_pg_range] = {0};
	OMultiRange *o_multirange;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_multirange = o_multirange_cache_search(datoid, rngmultitypid, cur_lsn,
											 multirange_cache->nkeys);
	if (o_multirange)
	{
		values[Anum_pg_range_rngtypid - 1] = o_multirange->key.keys[0];
		values[Anum_pg_range_rngtypid - 1] =
			ObjectIdGetDatum(o_multirange->rngtypid);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}
#endif
