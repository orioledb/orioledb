/*-------------------------------------------------------------------------
 *
 *  o_type_cache.c
 *		Routines for orioledb type cache.
 *
 * type_cache is tree that contains cached metadata from pg_type.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_type_cache.c
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
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_range.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

static OSysCache *type_cache = NULL;

static void o_type_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									Pointer arg);
static void o_type_cache_free_entry(Pointer entry);

O_SYS_CACHE_FUNCS(type_cache, OType, 1);

static OSysCacheFuncs type_cache_funcs =
{
	.free_entry = o_type_cache_free_entry,
	.fill_entry = o_type_cache_fill_entry
};

/*
 * Initializes the type sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(type_cache)
{
	Oid			keytypes[] = {OIDOID};

	type_cache = o_create_sys_cache(SYS_TREES_TYPE_CACHE, false,
									TypeOidIndexId, TYPEOID, 1, keytypes, 0,
									fastcache, mcxt, &type_cache_funcs);
}

void
o_type_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	HeapTuple	typetup;
	Form_pg_type typeform;
	OType	   *o_type = (OType *) *entry_ptr;
	Oid			typeoid = DatumGetObjectId(key->keys[0]);

	typetup = SearchSysCache1(TYPEOID, key->keys[0]);
	if (!HeapTupleIsValid(typetup))
		elog(ERROR, "cache lookup failed for type %u", typeoid);
	typeform = (Form_pg_type) GETSTRUCT(typetup);

	if (o_type != NULL)			/* Existed o_type updated */
	{
		Assert(false);
	}
	else
	{
		o_type = palloc0(sizeof(OType));
		*entry_ptr = (Pointer) o_type;
	}

	o_type->typname = typeform->typname;
	o_type->typlen = typeform->typlen;
	o_type->typbyval = typeform->typbyval;
	o_type->typalign = typeform->typalign;
	o_type->typstorage = typeform->typstorage;
	o_type->typcollation = typeform->typcollation;
	o_type->typrelid = typeform->typrelid;
	o_type->typtype = typeform->typtype;
	o_type->typcategory = typeform->typcategory;
	o_type->typispreferred = typeform->typispreferred;
	o_type->typisdefined = typeform->typisdefined;
	o_type->typinput = typeform->typinput;
	o_type->typoutput = typeform->typoutput;
	o_type->typreceive = typeform->typreceive;
	o_type->typsend = typeform->typsend;
	o_type->typelem = typeform->typelem;
	o_type->typdelim = typeform->typdelim;
	o_type->typbasetype = typeform->typbasetype;
	o_type->typtypmod = typeform->typtypmod;
#if PG_VERSION_NUM >= 140000
	o_type->typsubscript = typeform->typsubscript;
#endif

	o_type->default_btree_opclass = GetDefaultOpClass(typeoid, BTREE_AM_OID);
	if (!OidIsValid(o_type->default_btree_opclass) &&
		typeform->typtype != TYPTYPE_PSEUDO)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify a comparison function for type %s",
						format_type_be(typeoid))));
	o_type->default_hash_opclass = GetDefaultOpClass(typeoid, HASH_AM_OID);

	if (OidIsValid(o_type->typelem))
		o_type_cache_add_if_needed(key->common.datoid, o_type->typelem,
								   key->common.lsn, NULL);

	ReleaseSysCache(typetup);
}

void
o_type_cache_free_entry(Pointer entry)
{
	OType	   *o_type = (OType *) entry;

	pfree(o_type);
}

HeapTuple
o_type_cache_search_htup(TupleDesc tupdesc, Oid typeoid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	typetup = NULL;
	Datum		values[Natts_pg_type] = {0};
	bool		nulls[Natts_pg_type] = {0};
	OType	   *o_type;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_type = o_type_cache_search(datoid, typeoid, cur_lsn, type_cache->nkeys);
	if (o_type)
	{
		values[Anum_pg_type_oid - 1] = o_type->key.keys[0];
		values[Anum_pg_type_typname - 1] = NameGetDatum(&o_type->typname);
		values[Anum_pg_type_typlen - 1] = Int16GetDatum(o_type->typlen);
		values[Anum_pg_type_typbyval - 1] = BoolGetDatum(o_type->typbyval);
		values[Anum_pg_type_typalign - 1] = CharGetDatum(o_type->typalign);
		values[Anum_pg_type_typstorage - 1] = CharGetDatum(o_type->typstorage);
		values[Anum_pg_type_typcollation - 1] =
			ObjectIdGetDatum(o_type->typcollation);
		values[Anum_pg_type_typrelid - 1] = ObjectIdGetDatum(o_type->typrelid);
		values[Anum_pg_type_typtype - 1] = CharGetDatum(o_type->typtype);
		values[Anum_pg_type_typcategory - 1] =
			CharGetDatum(o_type->typcategory);
		values[Anum_pg_type_typispreferred - 1] =
			BoolGetDatum(o_type->typispreferred);
		values[Anum_pg_type_typisdefined - 1] =
			BoolGetDatum(o_type->typisdefined);
		values[Anum_pg_type_typinput - 1] = ObjectIdGetDatum(o_type->typinput);
		values[Anum_pg_type_typoutput - 1] =
			ObjectIdGetDatum(o_type->typoutput);
		values[Anum_pg_type_typreceive - 1] =
			ObjectIdGetDatum(o_type->typreceive);
		values[Anum_pg_type_typsend - 1] = ObjectIdGetDatum(o_type->typsend);
		values[Anum_pg_type_typelem - 1] = ObjectIdGetDatum(o_type->typelem);
		values[Anum_pg_type_typdelim - 1] = CharGetDatum(o_type->typdelim);
		values[Anum_pg_type_typbasetype - 1] =
			ObjectIdGetDatum(o_type->typbasetype);
		values[Anum_pg_type_typtypmod - 1] = Int32GetDatum(o_type->typtypmod);
#if PG_VERSION_NUM >= 140000
		values[Anum_pg_type_typsubscript - 1] =
			ObjectIdGetDatum(o_type->typsubscript);
#endif

		nulls[Anum_pg_type_typdefault - 1] = true;
		nulls[Anum_pg_type_typdefaultbin - 1] = true;
		nulls[Anum_pg_type_typacl - 1] = true;
		typetup = heap_form_tuple(tupdesc, values, nulls);
	}
	return typetup;
}

Oid
o_type_cache_get_typrelid(Oid typeoid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OType	   *o_type;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_type = o_type_cache_search(datoid, typeoid, cur_lsn, type_cache->nkeys);
	Assert(o_type);
	return o_type->typrelid;
}

Oid
o_type_cache_default_opclass(Oid typeoid, Oid am_id)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OType	   *o_type;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_type = o_type_cache_search(datoid, typeoid, cur_lsn, type_cache->nkeys);
	Assert(o_type);
	Assert(am_id == BTREE_AM_OID || am_id == HASH_AM_OID);
	if (am_id == BTREE_AM_OID)
		return o_type->default_btree_opclass;
	else
		return o_type->default_hash_opclass;
}

void
o_type_cache_fill_info(Oid typeoid, int16 *typlen, bool *typbyval,
					   char *typalign, char *typstorage, Oid *typcollation)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	OType	   *o_type;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_type = o_type_cache_search(datoid, typeoid, cur_lsn, type_cache->nkeys);

	Assert(o_type);
	if (typlen)
		*typlen = o_type->typlen;
	if (typbyval)
		*typbyval = o_type->typbyval;
	if (typalign)
		*typalign = o_type->typalign;
	if (typstorage)
		*typstorage = o_type->typstorage;
	if (typcollation)
		*typcollation = o_type->typcollation;
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_type_cache_tup_print(BTreeDescr *desc, StringInfo buf,
					   OTuple tup, Pointer arg)
{
	OType	   *o_type = (OType *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", typname: %s"
					 ", typlen: %d"
					 ", typbyval: %c"
					 ", typalign: '%c'"
					 ", typstorage: '%c'"
					 ", typcollation: %u"
					 ", typrelid: %u"
					 ", typtype: '%c'"
					 ", typcategory: '%c'"
					 ", typispreferred: %c"
					 ", typisdefined: %c"
					 ", typinput: %u"
					 ", typoutput: %u"
					 ", typreceive: %u"
					 ", typsend: %u"
					 ", typelem: %u"
					 ", typdelim: '%c'"
					 ", typbasetype: %u"
					 ", typtypmod: %d"
#if PG_VERSION_NUM >= 140000
					 ", typsubscript: %u"
#endif
					 ", default_btree_opclass: %u"
					 ", default_hash_opclass: %u"
					 ")",
					 o_type->typname.data,
					 o_type->typlen,
					 o_type->typbyval ? 'Y' : 'N',
					 o_type->typalign,
					 o_type->typstorage,
					 o_type->typcollation,
					 o_type->typrelid,
					 o_type->typtype,
					 o_type->typcategory,
					 o_type->typispreferred ? 'Y' : 'N',
					 o_type->typisdefined ? 'Y' : 'N',
					 o_type->typinput,
					 o_type->typoutput,
					 o_type->typreceive,
					 o_type->typsend,
					 o_type->typelem,
					 o_type->typdelim,
					 o_type->typbasetype,
					 o_type->typtypmod,
#if PG_VERSION_NUM >= 140000
					 o_type->typsubscript,
#endif
					 o_type->default_btree_opclass,
					 o_type->default_hash_opclass);
}
