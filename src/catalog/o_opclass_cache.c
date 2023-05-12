/*-------------------------------------------------------------------------
 *
 * o_opclass.c
 *		Routines for orioledb operator classes sys cache.
 *
 * Operator class B-tree stores data used by comparator and field initialization
 * for orioledb engine tables.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_opclass.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/iterator.h"
#include "btree/modify.h"
#include "catalog/o_sys_cache.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "utils/planner.h"
#include "utils/stopevent.h"

#include "access/nbtree.h"
#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static OSysCache *opclass_cache = NULL;

static void o_opclass_cache_free_entry(Pointer entry);
static void o_opclass_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
									   Pointer arg);

O_SYS_CACHE_FUNCS(opclass_cache, OOpclass, 1);

static OSysCacheFuncs opclass_cache_funcs =
{
	.free_entry = o_opclass_cache_free_entry,
	.fill_entry = o_opclass_cache_fill_entry
};

/*
 * Initializes the opclass sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(opclass_cache)
{
	Oid			keytypes[] = {OIDOID};

	opclass_cache = o_create_sys_cache(SYS_TREES_OPCLASS_CACHE, false,
									   OpclassOidIndexId, CLAOID, 1, keytypes,
									   0, fastcache, mcxt,
									   &opclass_cache_funcs);
}

static void
add_btree_opclass(Oid datoid, Oid opclassoid, XLogRecPtr insert_lsn)
{
	XLogRecPtr	sys_lsn;
	Oid			sys_datoid;
	OClassArg	class_arg = {.sys_table = true};
	Oid			btree_opf;
	Oid			btree_opintype;

	btree_opf = get_opclass_family(opclassoid);
	btree_opintype = get_opclass_input_type(opclassoid);

	o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
	o_class_cache_add_if_needed(sys_datoid, OperatorClassRelationId, sys_lsn,
								(Pointer) &class_arg);
	o_opclass_cache_add_if_needed(datoid, opclassoid, insert_lsn,
								  NULL);
	o_class_cache_add_if_needed(sys_datoid, AccessMethodProcedureRelationId,
								sys_lsn, (Pointer) &class_arg);
	o_amproc_cache_add_if_needed(datoid, btree_opf, btree_opintype,
								 btree_opintype, BTORDER_PROC, insert_lsn,
								 NULL);
	o_class_cache_add_if_needed(sys_datoid, AccessMethodOperatorRelationId,
								sys_lsn, (Pointer) &class_arg);

	if (get_opfamily_member(btree_opf, btree_opintype, btree_opintype,
							BTLessStrategyNumber))
		o_amop_strat_cache_add_if_needed(datoid, btree_opf, btree_opintype,
										 btree_opintype, BTLessStrategyNumber,
										 insert_lsn, NULL);
	if (get_opfamily_member(btree_opf, btree_opintype, btree_opintype,
							BTLessEqualStrategyNumber))
		o_amop_strat_cache_add_if_needed(datoid, btree_opf, btree_opintype,
										 btree_opintype,
										 BTLessEqualStrategyNumber, insert_lsn,
										 NULL);
	if (get_opfamily_member(btree_opf, btree_opintype, btree_opintype,
							BTEqualStrategyNumber))
		o_amop_strat_cache_add_if_needed(datoid, btree_opf, btree_opintype,
										 btree_opintype, BTEqualStrategyNumber,
										 insert_lsn, NULL);
}

/*
 * Inserts opclasses for all fields of the o_table to the opclass B-tree.
 */
void
o_opclass_cache_add_table(OTable *o_table)
{
	int			cur_ix;
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	XLogRecPtr	sys_lsn;
	Oid			sys_datoid;
	OClassArg	class_arg = {.sys_table = true};

	o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
	o_class_cache_add_if_needed(sys_datoid, OperatorClassRelationId, sys_lsn,
								(Pointer) &class_arg);

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	datoid = o_table->oids.datoid;

	/*
	 * Inserts opclasses for TOAST index.
	 */
	o_opclass_cache_add_if_needed(datoid,
								  GetDefaultOpClass(INT2OID, BTREE_AM_OID),
								  cur_lsn, NULL);
	add_btree_opclass(datoid, GetDefaultOpClass(INT2OID, BTREE_AM_OID),
					  cur_lsn);
	o_opclass_cache_add_if_needed(datoid,
								  GetDefaultOpClass(INT4OID, BTREE_AM_OID),
								  cur_lsn, NULL);
	add_btree_opclass(datoid, GetDefaultOpClass(INT4OID, BTREE_AM_OID),
					  cur_lsn);

	/*
	 * Inserts opclass for default index if there is no unique index.
	 */
	if (o_table->nindices == 0 || o_table->indices[0].type == oIndexRegular)
	{
		o_opclass_cache_add_if_needed(datoid,
									  GetDefaultOpClass(TIDOID, BTREE_AM_OID),
									  cur_lsn, NULL);
		add_btree_opclass(datoid, GetDefaultOpClass(TIDOID, BTREE_AM_OID),
						  cur_lsn);
	}

	for (cur_ix = 0; cur_ix < o_table->nindices; cur_ix++)
	{
		OTableIndex *index = &o_table->indices[cur_ix];
		int			cur_field;

		for (cur_field = 0; cur_field < index->nkeyfields; cur_field++)
		{
			o_opclass_cache_add_if_needed(datoid,
										  index->fields[cur_field].opclass,
										  cur_lsn, NULL);
			add_btree_opclass(datoid, index->fields[cur_field].opclass,
							  cur_lsn);
		}
	}
}

/*
 * Finds and returns OOpclass.
 */
OOpclass *
o_opclass_get(Oid opclassoid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	return o_opclass_cache_search(datoid, opclassoid, cur_lsn,
								  opclass_cache->nkeys);
}

HeapTuple
o_opclass_cache_search_htup(TupleDesc tupdesc, Oid opclassoid)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	result = NULL;
	Datum		values[Natts_pg_opclass] = {0};
	bool		nulls[Natts_pg_opclass] = {0};
	OOpclass   *o_opclass;
	NameData	oname;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_opclass = o_opclass_cache_search(datoid, opclassoid, cur_lsn,
									   opclass_cache->nkeys);
	if (o_opclass)
	{
		values[Anum_pg_opclass_oid - 1] = o_opclass->key.keys[0];
		namestrcpy(&oname, "");
		values[Anum_pg_opclass_opcname - 1] = NameGetDatum(&oname);
		values[Anum_pg_opclass_opcfamily - 1] =
			ObjectIdGetDatum(o_opclass->opfamily);
		values[Anum_pg_opclass_opcintype - 1] =
			ObjectIdGetDatum(o_opclass->inputtype);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	return result;
}

void
o_opclass_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	HeapTuple	opclasstuple;
	Form_pg_opclass opclassform;
	OOpclass   *o_opclass = (OOpclass *) *entry_ptr;
	Oid			opclassoid = DatumGetObjectId(key->keys[0]);
	Oid			inputtype;

	/*
	 * find typecache entry
	 */
	opclasstuple = SearchSysCache1(CLAOID, key->keys[0]);
	if (!HeapTupleIsValid(opclasstuple))
		elog(ERROR, "cache lookup failed for opclass %u", opclassoid);
	opclassform = (Form_pg_opclass) GETSTRUCT(opclasstuple);

	if (o_opclass == NULL)
	{
		o_opclass = palloc0(sizeof(OOpclass));
		*entry_ptr = (Pointer) o_opclass;
	}
	else
	{
		Assert(false);
	}

	o_opclass->opfamily = opclassform->opcfamily;
	o_opclass->inputtype = opclassform->opcintype;

	inputtype = o_opclass->inputtype;
	o_opclass->ssupOid = get_opfamily_proc(o_opclass->opfamily, inputtype,
										   inputtype, BTSORTSUPPORT_PROC);
	if (OidIsValid(o_opclass->ssupOid))
		o_proc_cache_validate_add(key->common.datoid,
								  o_opclass->ssupOid, InvalidOid,
								  "sort", "field");

	o_opclass->cmpOid = get_opfamily_proc(o_opclass->opfamily, inputtype,
										  inputtype, BTORDER_PROC);
	o_proc_cache_validate_add(key->common.datoid, o_opclass->cmpOid,
							  InvalidOid, "comparsion", "field");
	ReleaseSysCache(opclasstuple);
}

void
o_opclass_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_opclass_cache_tup_print(BTreeDescr *desc, StringInfo buf,
						  OTuple tup, Pointer arg)
{
	OOpclass   *o_opclass = (OOpclass *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", opfamily: %u, inputtype: %d, "
					 "cmpOid: %u, ssupOid: %u)",
					 o_opclass->opfamily, o_opclass->inputtype,
					 o_opclass->cmpOid, o_opclass->ssupOid);
}
