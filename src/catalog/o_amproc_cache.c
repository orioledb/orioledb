/*-------------------------------------------------------------------------
 *
 *  o_amproc_cache.c
 *		Routines for orioledb amproc cache.
 *
 * amproc_cache is tree that contains cached metadata from pg_amproc.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_amproc_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"

#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif
#include "catalog/pg_amproc.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

static OSysCache *amproc_cache = NULL;

static void o_amproc_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									  Pointer arg);
static void o_amproc_cache_free_entry(Pointer entry);

O_SYS_CACHE_FUNCS(amproc_cache, OAmProc, 4);

static OSysCacheFuncs amproc_cache_funcs =
{
	.free_entry = o_amproc_cache_free_entry,
	.fill_entry = o_amproc_cache_fill_entry
};

/*
 * Initializes the type sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(amproc_cache)
{
	Oid			keytypes[] = {OIDOID, OIDOID, OIDOID, INT2OID};

	amproc_cache = o_create_sys_cache(SYS_TREES_AMPROC_CACHE,
									  false, false,
									  AccessMethodProcedureIndexId, AMPROCNUM,
									  4, keytypes, fastcache, mcxt,
									  &amproc_cache_funcs);
}


void
o_amproc_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	HeapTuple	amproctup;
	Form_pg_amproc amprocform;
	OAmProc    *o_amproc = (OAmProc *) *entry_ptr;
	MemoryContext prev_context;
	Oid			amprocfamily;
	Oid			amproclefttype;
	Oid			amprocrighttype;
	int16		amprocnum;

	amprocfamily = DatumGetObjectId(key->keys[0]);
	amproclefttype = DatumGetObjectId(key->keys[1]);
	amprocrighttype = DatumGetObjectId(key->keys[2]);
	amprocnum = DatumGetChar(key->keys[3]);

	amproctup = SearchSysCache4(AMPROCNUM, key->keys[0], key->keys[1],
								key->keys[2], key->keys[3]);
	if (!HeapTupleIsValid(amproctup))
		elog(ERROR, "cache lookup failed for amproc (%u %u %u %d)",
			 amprocfamily, amproclefttype, amprocrighttype, amprocnum);
	amprocform = (Form_pg_amproc) GETSTRUCT(amproctup);

	prev_context = MemoryContextSwitchTo(amproc_cache->mcxt);
	if (o_amproc != NULL)		/* Existed o_amproc updated */
	{
		Assert(false);
	}
	else
	{
		o_amproc = palloc0(sizeof(OAmProc));
		*entry_ptr = (Pointer) o_amproc;
	}

	o_amproc->amproc = amprocform->amproc;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(amproctup);
}

void
o_amproc_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

HeapTuple
o_amproc_cache_search_htup(TupleDesc tupdesc, Oid amprocfamily,
						   Oid amproclefttype, Oid amprocrighttype,
						   int16 amprocnum)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_amproc] = {0};
	bool		nulls[Natts_pg_amproc] = {0};
	OAmProc    *o_amproc;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_amproc = o_amproc_cache_search(datoid, amprocfamily, amproclefttype,
									 amprocrighttype, amprocnum, cur_lsn,
									 amproc_cache->nkeys);
	if (o_amproc)
	{
		values[Anum_pg_amproc_amproc - 1] = ObjectIdGetDatum(o_amproc->amproc);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_amproc_cache_tup_print(BTreeDescr *desc, StringInfo buf,
						 OTuple tup, Pointer arg)
{
	OAmProc    *o_amproc = (OAmProc *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", amproc: %u)",
					 o_amproc->amproc);
}
