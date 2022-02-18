/*-------------------------------------------------------------------------
 *
 * o_type_element_cache.c
 *		Routines for orioledb type elements cache.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_type_element_cache.c
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
#include "utils/builtins.h"
#include "utils/syscache.h"

static OTypeCache *type_element_cache = NULL;

static void o_type_element_cache_free_entry(Pointer entry);
static void o_type_element_cache_fill_entry(Pointer *entry_ptr, Oid datoid,
											Oid typoid, XLogRecPtr insert_lsn,
											Pointer arg);

O_TYPE_CACHE_FUNCS(type_element_cache, OTypeElement);

static OTypeCacheFuncs type_element_cache_funcs =
{
	.free_entry = o_type_element_cache_free_entry,
	.fill_entry = o_type_element_cache_fill_entry
};

/*
 * Initializes the type elements cache memory.
 */
O_TYPE_CACHE_INIT_FUNC(type_element_cache)
{
	type_element_cache = o_create_type_cache(SYS_TREES_TYPE_ELEMENT_CACHE,
											 false,
											 TypeRelationId,
											 fastcache,
											 mcxt,
											 &type_element_cache_funcs);
}

void
o_type_element_cache_fill_entry(Pointer *entry_ptr, Oid datoid, Oid typoid,
								XLogRecPtr insert_lsn, Pointer arg)
{
	TypeCacheEntry *typcache;
	OTypeElement *o_type_element = (OTypeElement *) *entry_ptr;

	/*
	 * find typecache entry
	 */
	typcache = lookup_type_cache(typoid, TYPECACHE_CMP_PROC_FINFO);

	if (o_type_element == NULL)
	{
		o_type_element = palloc0(sizeof(OTypeElement));
		*entry_ptr = (Pointer) o_type_element;
	}

	o_type_element->typlen = typcache->typlen;
	o_type_element->typbyval = typcache->typbyval;
	o_type_element->typalign = typcache->typalign;

	if (!OidIsValid(typcache->cmp_proc_finfo.fn_oid))
		ereport(
				ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg(
						"could not identify a comparison function for type %s",
						format_type_be(typoid))));

	o_type_procedure_fill(typcache->cmp_proc_finfo.fn_oid,
						  &o_type_element->cmp_proc);
	o_type_element->cmp_oid = typcache->cmp_proc_finfo.fn_oid;
}

void
o_type_element_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

TypeCacheEntry *
o_type_elements_cmp_hook(Oid elemtype, MemoryContext mcxt)
{
	TypeCacheEntry *typcache = NULL;

	XLogRecPtr	cur_lsn = is_recovery_in_progress() ?
	GetXLogReplayRecPtr(NULL) :
	GetXLogWriteRecPtr();
	Oid			datoid;
	OTypeElement *type_element;
	MemoryContext prev_context;

	if (OidIsValid(MyDatabaseId))
	{
		datoid = MyDatabaseId;
	}
	else
	{
		Assert(OidIsValid(o_type_cmp_datoid));
		datoid = o_type_cmp_datoid;
	}

	type_element = o_type_element_cache_search(datoid, elemtype, cur_lsn);
	if (type_element)
	{
		prev_context = MemoryContextSwitchTo(mcxt);
		typcache = palloc0(sizeof(TypeCacheEntry));
		typcache->type_id = elemtype;
		typcache->typlen = type_element->typlen;
		typcache->typbyval = type_element->typbyval;
		typcache->typalign = type_element->typalign;

		o_type_procedure_fill_finfo(&typcache->cmp_proc_finfo,
									&type_element->cmp_proc,
									type_element->cmp_oid,
									2);
		MemoryContextSwitchTo(prev_context);
	}

	return typcache;
}
