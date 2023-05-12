/*-------------------------------------------------------------------------
 *
 *  o_operator_cache.c
 *		Routines for orioledb operator cache.
 *
 * operator_cache is tree that contains cached metadata from pg_operator.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_operator_cache.c
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
#include "catalog/pg_operator.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

static OSysCache *operator_cache = NULL;

static void o_operator_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
										Pointer arg);
static void o_operator_cache_free_entry(Pointer entry);

O_SYS_CACHE_FUNCS(operator_cache, OOperator, 1);

static OSysCacheFuncs operator_cache_funcs =
{
	.free_entry = o_operator_cache_free_entry,
	.fill_entry = o_operator_cache_fill_entry
};

/*
 * Initializes the type sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(operator_cache)
{
	Oid			keytypes[] = {OIDOID};

	operator_cache = o_create_sys_cache(SYS_TREES_OPER_CACHE, false,
										OperatorOidIndexId, OPEROID, 1,
										keytypes, 0, fastcache, mcxt,
										&operator_cache_funcs);
}


void
o_operator_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	HeapTuple	opertup;
	Form_pg_operator operform;
	OOperator  *o_operator = (OOperator *) *entry_ptr;
	MemoryContext prev_context;
	Oid			operoid = DatumGetObjectId(key->keys[0]);

	opertup = SearchSysCache1(OPEROID, key->keys[0]);
	if (!HeapTupleIsValid(opertup))
		elog(ERROR, "cache lookup failed for operator %u", operoid);
	operform = (Form_pg_operator) GETSTRUCT(opertup);

	prev_context = MemoryContextSwitchTo(operator_cache->mcxt);
	if (o_operator != NULL)		/* Existed o_operator updated */
	{
		Assert(false);
	}
	else
	{
		o_operator = palloc0(sizeof(OOperator));
		*entry_ptr = (Pointer) o_operator;
	}

	o_operator->oprcode = operform->oprcode;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(opertup);
}

void
o_operator_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

HeapTuple
o_operator_cache_search_htup(TupleDesc tupdesc, Oid operoid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_operator] = {0};
	bool		nulls[Natts_pg_operator] = {0};
	OOperator  *o_operator;
	NameData	oname;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_operator = o_operator_cache_search(datoid, operoid, cur_lsn,
										 operator_cache->nkeys);
	if (o_operator)
	{
		values[Anum_pg_operator_oid - 1] = o_operator->key.keys[0];
		namestrcpy(&oname, "");
		values[Anum_pg_operator_oprname - 1] = NameGetDatum(&oname);
		values[Anum_pg_operator_oprcode - 1] =
			ObjectIdGetDatum(o_operator->oprcode);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_operator_cache_tup_print(BTreeDescr *desc, StringInfo buf,
						   OTuple tup, Pointer arg)
{
	OOperator  *o_operator = (OOperator *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", oprcode: %u)",
					 o_operator->oprcode);
}
