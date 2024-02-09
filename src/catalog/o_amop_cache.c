/*-------------------------------------------------------------------------
 *
 *  o_amop_cache.c
 *		Routines for orioledb amop cache.
 *
 * amop_cache is tree that contains cached metadata from pg_amop.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_amop_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/iterator.h"
#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"

#include "catalog/pg_amop.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

static OSysCache *amop_cache = NULL;
static OSysCache *amop_strat_cache = NULL;

static void o_amop_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									Pointer arg);
static void o_amop_cache_free_entry(Pointer entry);

static void o_amop_strat_cache_fill_entry(Pointer *entry_ptr,
										  OSysCacheKey *key, Pointer arg);
static void o_amop_strat_cache_free_entry(Pointer entry);

O_SYS_CACHE_FUNCS(amop_cache, OAmOp, 3);
O_SYS_CACHE_FUNCS(amop_strat_cache, OAmOpStrat, 4);

static OSysCacheFuncs amop_cache_funcs =
{
	.free_entry = o_amop_cache_free_entry,
	.fill_entry = o_amop_cache_fill_entry
};

static OSysCacheFuncs amop_strat_cache_funcs =
{
	.free_entry = o_amop_strat_cache_free_entry,
	.fill_entry = o_amop_strat_cache_fill_entry
};

/*
 * Initializes the type sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(amop_cache)
{
	Oid			keytypes[] = {OIDOID, CHAROID, OIDOID};

	amop_cache = o_create_sys_cache(SYS_TREES_AMOP_CACHE, false,
									AccessMethodOperatorIndexId, AMOPOPID, 3,
									keytypes, 0, fastcache, mcxt,
									&amop_cache_funcs);
}

/*
 * Initializes the type sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(amop_strat_cache)
{
	Oid			keytypes[] = {OIDOID, OIDOID, OIDOID, INT2OID};

	amop_strat_cache = o_create_sys_cache(SYS_TREES_AMOP_STRAT_CACHE, false,
										  AccessMethodStrategyIndexId,
										  AMOPSTRATEGY, 4, keytypes, 0,
										  fastcache, mcxt,
										  &amop_strat_cache_funcs);
}


void
o_amop_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	HeapTuple	amoptup;
	Form_pg_amop amopform;
	OAmOp	   *o_amop = (OAmOp *) *entry_ptr;
	MemoryContext prev_context;
	Oid			amopopr;
	char		amoppurpose;
	Oid			amopfamily;

	amopopr = DatumGetObjectId(key->keys[0]);
	amoppurpose = DatumGetChar(key->keys[1]);
	amopfamily = DatumGetObjectId(key->keys[2]);

	amoptup = SearchSysCache3(AMOPOPID, key->keys[0], key->keys[1],
							  key->keys[2]);
	if (!HeapTupleIsValid(amoptup))
		elog(ERROR, "cache lookup failed for amop (%u, %c, %u)", amopopr,
			 amoppurpose, amopfamily);
	amopform = (Form_pg_amop) GETSTRUCT(amoptup);

	prev_context = MemoryContextSwitchTo(amop_cache->mcxt);
	if (o_amop != NULL)			/* Existed o_amop updated */
	{
		Assert(false);
	}
	else
	{
		o_amop = palloc0(sizeof(OAmOp));
		*entry_ptr = (Pointer) o_amop;
	}

	o_amop->amopmethod = amopform->amopmethod;
	o_amop->amopstrategy = amopform->amopstrategy;
	o_amop->amopfamily = amopform->amopfamily;
	o_amop->amoplefttype = amopform->amoplefttype;
	o_amop->amoprighttype = amopform->amoprighttype;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(amoptup);
}

void
o_amop_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

void
o_amop_strat_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
							  Pointer arg)
{
	HeapTuple	amoptup;
	Form_pg_amop amopform;
	OAmOpStrat *o_amop_strat = (OAmOpStrat *) *entry_ptr;
	MemoryContext prev_context;
	Oid			amopfamily;
	Oid			amoplefttype;
	Oid			amoprighttype;
	int16		amopstrategy;

	amopfamily = DatumGetObjectId(key->keys[0]);
	amoplefttype = DatumGetObjectId(key->keys[1]);
	amoprighttype = DatumGetObjectId(key->keys[2]);
	amopstrategy = DatumGetChar(key->keys[3]);

	amoptup = SearchSysCache4(AMOPSTRATEGY, key->keys[0], key->keys[1],
							  key->keys[2], key->keys[3]);
	if (!HeapTupleIsValid(amoptup))
		elog(ERROR, "cache lookup failed for amop strategy (%u, %u, %u, %u)",
			 amopfamily, amoplefttype, amoprighttype, amopstrategy);
	amopform = (Form_pg_amop) GETSTRUCT(amoptup);

	prev_context = MemoryContextSwitchTo(amop_cache->mcxt);
	if (o_amop_strat != NULL)	/* Existed o_amop_strat updated */
	{
		Assert(false);
	}
	else
	{
		o_amop_strat = palloc0(sizeof(OAmOpStrat));
		*entry_ptr = (Pointer) o_amop_strat;
	}

	o_amop_strat->amopopr = amopform->amopopr;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(amoptup);
}

void
o_amop_strat_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

static HeapTuple
o_amop_to_htup(OAmOp *o_amop, TupleDesc tupdesc)
{
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_amop] = {0};
	bool		nulls[Natts_pg_amop] = {0};

	if (o_amop)
	{
		values[Anum_pg_amop_oid - 1] = o_amop->key.keys[0];
		values[Anum_pg_amop_amopmethod - 1] =
			ObjectIdGetDatum(o_amop->amopmethod);
		values[Anum_pg_amop_amopstrategy - 1] =
			Int16GetDatum(o_amop->amopstrategy);
		values[Anum_pg_amop_amopfamily - 1] =
			ObjectIdGetDatum(o_amop->amopfamily);
		values[Anum_pg_amop_amoplefttype - 1] =
			ObjectIdGetDatum(o_amop->amoplefttype);
		values[Anum_pg_amop_amoprighttype - 1] =
			ObjectIdGetDatum(o_amop->amoprighttype);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}

HeapTuple
o_amop_cache_search_htup(TupleDesc tupdesc, Oid amopopr, char amoppurpose,
						 Oid amopfamily)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OAmOp	   *o_amop;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_amop = o_amop_cache_search(datoid, amopopr, amoppurpose, amopfamily,
								 cur_lsn, amop_cache->nkeys);
	return o_amop_to_htup(o_amop, tupdesc);
}

List *
o_amop_cache_search_htup_list(TupleDesc tupdesc, Oid amopopr)
{
	List	   *result = NIL;
	BTreeDescr *td = get_sys_tree(amop_cache->sys_tree_num);
	BTreeIterator *it;
	OSysCacheKey3 key = {0};
	OSysCacheBound bound = {.key = (OSysCacheKey *) &key,.nkeys = 1};

	o_sys_cache_set_datoid_lsn(&key.common.lsn, &key.common.datoid);
	key.keys[0] = ObjectIdGetDatum(amopopr);

	it = o_btree_iterator_create(td, (Pointer) &bound, BTreeKeyBound,
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	do
	{
		OTuple		tup = o_btree_iterator_fetch(it, NULL,
												 (Pointer) &bound,
												 BTreeKeyBound, true,
												 NULL);
		OAmOp	   *o_amop = (OAmOp *) tup.data;

		if (O_TUPLE_IS_NULL(tup))
			break;

		if (o_amop->key.common.lsn > key.common.lsn)
			break;

		result = lappend(result, o_amop_to_htup(o_amop, tupdesc));
	} while (true);

	btree_iterator_free(it);
	return result;
}

static HeapTuple
o_amop_strat_to_htup(OAmOpStrat *o_amop_strat, TupleDesc tupdesc)
{
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_amop] = {0};
	bool		nulls[Natts_pg_amop] = {0};

	if (o_amop_strat)
	{
		values[Anum_pg_amop_amopfamily - 1] = o_amop_strat->key.keys[0];
		values[Anum_pg_amop_amoplefttype - 1] = o_amop_strat->key.keys[1];
		values[Anum_pg_amop_amoprighttype - 1] = o_amop_strat->key.keys[2];
		values[Anum_pg_amop_amopstrategy - 1] = o_amop_strat->key.keys[3];
		values[Anum_pg_amop_amopopr - 1] =
			ObjectIdGetDatum(o_amop_strat->amopopr);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}

HeapTuple
o_amop_strat_cache_search_htup(TupleDesc tupdesc, Oid amopfamily,
							   Oid amoplefttype, Oid amoprighttype,
							   int16 amopstrategy)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OAmOpStrat *o_amop_strat;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_amop_strat = o_amop_strat_cache_search(datoid, amopfamily, amoplefttype,
											 amoprighttype, amopstrategy,
											 cur_lsn, amop_cache->nkeys);
	return o_amop_strat_to_htup(o_amop_strat, tupdesc);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_amop_cache_tup_print(BTreeDescr *desc, StringInfo buf,
					   OTuple tup, Pointer arg)
{
	OAmOp	   *o_amop = (OAmOp *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", amopmethod: %u, amopstrategy: %d, amopfamily: %u"
					 ", amoplefttype: %u, amoprighttype: %u)",
					 o_amop->amopmethod,
					 o_amop->amopstrategy,
					 o_amop->amopfamily,
					 o_amop->amoplefttype,
					 o_amop->amoprighttype);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_amop_strat_cache_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup,
							 Pointer arg)
{
	OAmOpStrat *o_amop_strat = (OAmOpStrat *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf,
					 ", amopopr: %u)",
					 o_amop_strat->amopopr);
}
