/*-------------------------------------------------------------------------
 *
 *  o_aggregate_cache.c
 *		Routines for orioledb aggregate cache.
 *
 * aggregate_cache is tree that contains cached metadata from pg_aggregate.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_aggregate_cache.c
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
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

static OSysCache *aggregate_cache = NULL;

struct OAggregate
{
	OSysCacheKey1 key;
	regproc		aggfinalfn;
	regproc		aggserialfn;
	regproc		aggdeserialfn;
	bool		aggfinalextra;
	regproc		aggcombinefn;
	regproc		aggtransfn;
	char		aggfinalmodify;
	bool		aggmfinalextra;
	regproc		aggmfinalfn;
	char		aggmfinalmodify;
	regproc		aggminvtransfn;
	regproc		aggmtransfn;
	Oid			aggmtranstype;
	Oid			aggtranstype;
	bool		has_initval;
	bool		has_minitval;

	char	   *agginitval;
	char	   *aggminitval;
};

static void o_aggregate_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
										 Pointer arg);
static void o_aggregate_cache_free_entry(Pointer entry);
static Pointer o_aggregate_cache_serialize_entry(Pointer entry, int *len);
static Pointer o_aggregate_cache_deserialize_entry(MemoryContext mcxt,
												   Pointer data, Size length);

O_SYS_CACHE_FUNCS(aggregate_cache, OAggregate, 1);

static OSysCacheFuncs aggregate_cache_funcs =
{
	.free_entry = o_aggregate_cache_free_entry,
	.fill_entry = o_aggregate_cache_fill_entry,
	.toast_serialize_entry = o_aggregate_cache_serialize_entry,
	.toast_deserialize_entry = o_aggregate_cache_deserialize_entry,
};

/*
 * Initializes the type sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(aggregate_cache)
{
	Oid			keytypes[] = {OIDOID};

	aggregate_cache = o_create_sys_cache(SYS_TREES_AGG_CACHE,
										 true, false,
										 AggregateFnoidIndexId, AGGFNOID, 1,
										 keytypes, fastcache,
										 mcxt,
										 &aggregate_cache_funcs);
}


void
o_aggregate_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
							 Pointer arg)
{
	HeapTuple	aggtup;
	Form_pg_aggregate aggform;
	OAggregate *o_agg = (OAggregate *) *entry_ptr;
	MemoryContext prev_context;
	Datum		textInitVal;
	bool		initValueIsNull;
	Oid			aggfnoid = DatumGetObjectId(key->keys[0]);

	aggtup = SearchSysCache1(AGGFNOID, key->keys[0]);
	if (!HeapTupleIsValid(aggtup))
		elog(ERROR, "cache lookup failed for aggregate function %u", aggfnoid);
	aggform = (Form_pg_aggregate) GETSTRUCT(aggtup);

	prev_context = MemoryContextSwitchTo(aggregate_cache->mcxt);
	if (o_agg != NULL)			/* Existed o_agg updated */
	{
		Assert(false);
	}
	else
	{
		o_agg = palloc0(sizeof(OAggregate));
		*entry_ptr = (Pointer) o_agg;
	}

	o_agg->aggfinalfn = aggform->aggfinalfn;
	o_agg->aggserialfn = aggform->aggserialfn;
	o_agg->aggdeserialfn = aggform->aggdeserialfn;
	o_agg->aggfinalextra = aggform->aggfinalextra;
	o_agg->aggcombinefn = aggform->aggcombinefn;
	o_agg->aggtransfn = aggform->aggtransfn;
	o_agg->aggfinalmodify = aggform->aggfinalmodify;
	o_agg->aggmfinalextra = aggform->aggmfinalextra;
	o_agg->aggmfinalfn = aggform->aggmfinalfn;
	o_agg->aggmfinalmodify = aggform->aggmfinalmodify;
	o_agg->aggminvtransfn = aggform->aggminvtransfn;
	o_agg->aggmtransfn = aggform->aggmtransfn;
	o_agg->aggmtranstype = aggform->aggmtranstype;
	o_agg->aggtranstype = aggform->aggtranstype;

	o_type_cache_add_if_needed(key->common.datoid, o_agg->aggtranstype,
							   key->common.lsn, NULL);
	o_type_cache_add_if_needed(key->common.datoid, o_agg->aggmtranstype,
							   key->common.lsn, NULL);

	textInitVal = SysCacheGetAttr(AGGFNOID, aggtup,
								  Anum_pg_aggregate_agginitval,
								  &initValueIsNull);
	if (!initValueIsNull)
	{
		o_agg->has_initval = true;
		o_agg->agginitval = TextDatumGetCString(textInitVal);
	}

	textInitVal = SysCacheGetAttr(AGGFNOID, aggtup,
								  Anum_pg_aggregate_aggminitval,
								  &initValueIsNull);
	if (!initValueIsNull)
	{
		o_agg->has_minitval = true;
		o_agg->aggminitval = TextDatumGetCString(textInitVal);
	}

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(aggtup);
}

void
o_aggregate_cache_free_entry(Pointer entry)
{
	OAggregate *o_agg = (OAggregate *) entry;

	if (o_agg->has_initval)
		pfree(o_agg->agginitval);
	if (o_agg->has_minitval)
		pfree(o_agg->aggminitval);
	pfree(o_agg);
}

Pointer
o_aggregate_cache_serialize_entry(Pointer entry, int *len)
{
	StringInfoData str;
	OAggregate *o_agg = (OAggregate *) entry;

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_agg,
						   offsetof(OAggregate, agginitval));
	if (o_agg->has_initval)
		o_serialize_string(o_agg->agginitval, &str);
	if (o_agg->has_minitval)
		o_serialize_string(o_agg->aggminitval, &str);

	*len = str.len;
	return str.data;
}

Pointer
o_aggregate_cache_deserialize_entry(MemoryContext mcxt, Pointer data,
									Size length)
{
	Pointer		ptr = data;
	OAggregate *o_agg;
	int			len;

	o_agg = (OAggregate *) palloc(sizeof(OAggregate));
	len = offsetof(OAggregate, agginitval);
	Assert((ptr - data) + len <= length);
	memcpy(o_agg, ptr, len);
	ptr += len;

	if (o_agg->has_initval)
		o_agg->agginitval = o_deserialize_string(&ptr);
	if (o_agg->has_minitval)
		o_agg->aggminitval = o_deserialize_string(&ptr);

	return (Pointer) o_agg;
}

HeapTuple
o_aggregate_cache_search_htup(TupleDesc tupdesc, Oid aggfnoid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_aggregate] = {0};
	bool		nulls[Natts_pg_aggregate] = {0};
	OAggregate *o_agg;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_agg = o_aggregate_cache_search(datoid, aggfnoid, cur_lsn,
									 aggregate_cache->nkeys);
	if (o_agg)
	{
		values[Anum_pg_aggregate_aggfinalfn - 1] =
			ObjectIdGetDatum(o_agg->aggfinalfn);
		values[Anum_pg_aggregate_aggserialfn - 1] =
			ObjectIdGetDatum(o_agg->aggserialfn);
		values[Anum_pg_aggregate_aggdeserialfn - 1] =
			ObjectIdGetDatum(o_agg->aggdeserialfn);
		values[Anum_pg_aggregate_aggfinalextra - 1] =
			ObjectIdGetDatum(o_agg->aggfinalextra);
		values[Anum_pg_aggregate_aggcombinefn - 1] =
			ObjectIdGetDatum(o_agg->aggcombinefn);
		values[Anum_pg_aggregate_aggtransfn - 1] =
			ObjectIdGetDatum(o_agg->aggtransfn);
		values[Anum_pg_aggregate_aggfinalmodify - 1] =
			CharGetDatum(o_agg->aggfinalmodify);
		values[Anum_pg_aggregate_aggmfinalextra - 1] =
			BoolGetDatum(o_agg->aggmfinalextra);
		values[Anum_pg_aggregate_aggmfinalfn - 1] =
			ObjectIdGetDatum(o_agg->aggmfinalfn);
		values[Anum_pg_aggregate_aggmfinalmodify - 1] =
			CharGetDatum(o_agg->aggmfinalmodify);
		values[Anum_pg_aggregate_aggminvtransfn - 1] =
			ObjectIdGetDatum(o_agg->aggminvtransfn);
		values[Anum_pg_aggregate_aggmtransfn - 1] =
			ObjectIdGetDatum(o_agg->aggmtransfn);
		values[Anum_pg_aggregate_aggmtranstype - 1] =
			ObjectIdGetDatum(o_agg->aggmtranstype);
		values[Anum_pg_aggregate_aggtranstype - 1] =
			ObjectIdGetDatum(o_agg->aggtranstype);

		if (o_agg->has_initval)
			values[Anum_pg_aggregate_agginitval - 1] =
				CStringGetTextDatum(o_agg->agginitval);
		else
			nulls[Anum_pg_aggregate_agginitval - 1] = true;

		if (o_agg->has_minitval)
			values[Anum_pg_aggregate_aggminitval - 1] =
				CStringGetTextDatum(o_agg->aggminitval);
		else
			nulls[Anum_pg_aggregate_aggminitval - 1] = true;
		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}
