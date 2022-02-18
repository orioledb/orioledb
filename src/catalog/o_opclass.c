/*-------------------------------------------------------------------------
 *
 * o_opclass.c
 *		Routines for orioledb operator classes list.
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
#include "catalog/o_opclass.h"
#include "checkpoint/checkpoint.h"
#include "recovery/wal.h"
#include "utils/stopevent.h"

#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static HTAB *opclass_cache;

static inline OOpclass *o_opclass_cached_search(Oid datoid, Oid opclass, bool must_found);
static OOpclass *o_opclass_get_from_tree(OOpclassKey *key);
static bool o_opclass_insert(OOpclass *opclass);
static void o_opclass_add_if_needed(Oid datoid, Oid opclassoid);


/*
 * Initializes the opclass B-tree memory and opclass cache.
 */
void
o_opclass_init(void)
{
	HASHCTL		ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OOpclassKey);
	ctl.entrysize = sizeof(OOpclass);
	ctl.hcxt = TopMemoryContext;
	opclass_cache = hash_create("orioledb opclasses", 16,
								&ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Inserts opclasses for all fields of the o_table to the opclass B-tree.
 */
void
o_opclass_add_all(OTable *o_table)
{
	int			cur_ix;

	/*
	 * Inserts opclasses for TOAST index.
	 */
	o_opclass_add_if_needed(o_table->oids.datoid,
							GetDefaultOpClass(INT2OID, BTREE_AM_OID));
	o_opclass_add_if_needed(o_table->oids.datoid,
							GetDefaultOpClass(INT4OID, BTREE_AM_OID));

	/*
	 * Inserts opclass for default index if there is no unique index.
	 */
	if (o_table->nindices == 0 || o_table->indices[0].type == oIndexRegular)
	{
		o_opclass_add_if_needed(o_table->oids.datoid,
								GetDefaultOpClass(TIDOID, BTREE_AM_OID));
	}

	for (cur_ix = 0; cur_ix < o_table->nindices; cur_ix++)
	{
		OTableIndex *index = &o_table->indices[cur_ix];
		int			cur_field;

		for (cur_field = 0; cur_field < index->nfields; cur_field++)
		{
			o_opclass_add_if_needed(o_table->oids.datoid,
									index->fields[cur_field].opclass);
		}
	}
}

/*
 * Finds and returns OOpclass.
 */
OOpclass *
o_opclass_get(Oid datoid, Oid opclass)
{
	return o_opclass_cached_search(datoid, opclass, true);
}

/*
 * Cached OOpclass search.
 */
static inline OOpclass *
o_opclass_cached_search(Oid datoid, Oid opclass, bool must_found)
{
	static OOpclassKey lastkey =
	{
		0, 0
	};
	static OOpclass *lastclass = NULL;
	OOpclassKey key;
	OOpclass   *class,
			   *new_class;
	bool		found;

	memset(&key, 0, sizeof(OOpclassKey));
	key.datoid = datoid;
	key.opclassoid = opclass;

	/* fast search */
	if (memcmp(&key, &lastkey, sizeof(OOpclassKey)) == 0)
		return lastclass;

	/* cache search */
	class = hash_search(opclass_cache, &key, HASH_FIND, &found);
	if (found)
	{
		lastkey = key;
		lastclass = class;
		return class;
	}

	class = o_opclass_get_from_tree(&key);
	if (class == NULL)
	{
		if (must_found)
		{
			elog(ERROR, "Unable to found internal opclass %u for datoid %u.",
				 key.opclassoid, key.datoid);
		}
		else
		{
			return NULL;
		}
	}

	/* now we need to add opclass into the cache */
	new_class = hash_search(opclass_cache, &key, HASH_ENTER, NULL);
	Assert(new_class != NULL);
	memcpy(new_class, class, sizeof(OOpclass));
	memcpy(&lastkey, &key, sizeof(OOpclassKey));
	lastclass = new_class;
	pfree(class);
	return new_class;
}

/*
 * Return OOpclass tuple from the opclass B-tree.
 */
static OOpclass *
o_opclass_get_from_tree(OOpclassKey *key)
{
	OTuple		result;
	bool		old_enable_stopevents = enable_stopevents;
	OTuple		tup;

	enable_stopevents = false;
	tup.formatFlags = 0;
	tup.data = (Pointer) key;
	result = o_btree_find_tuple_by_key(get_sys_tree(SYS_TREES_OPCLASSES),
									   (Pointer) &tup, BTreeKeyNonLeafKey,
									   COMMITSEQNO_INPROGRESS, NULL,
									   CurrentMemoryContext, NULL);
	enable_stopevents = old_enable_stopevents;

	return (OOpclass *) result.data;
}

/*
 * Inserts the opclass to the opclass B-tree.
 */
static bool
o_opclass_insert(OOpclass *opclass)
{
	OOpclass   *founded;
	bool		inserted;
	bool		old_enable_stopevents;
	BTreeDescr *desc = get_sys_tree(SYS_TREES_OPCLASSES);
	OTuple		tup;

	tup.formatFlags = 0;
	tup.data = (Pointer) opclass;

	if ((founded = o_opclass_get_from_tree(&opclass->key)) != NULL)
	{
		pfree(founded);
		return false;
	}

	old_enable_stopevents = enable_stopevents;
	enable_stopevents = false;
	inserted = o_btree_autonomous_insert(desc, tup);
	enable_stopevents = old_enable_stopevents;

	return inserted;
}

/*
 * Adds an opclass to the opclass B-tree if it is not exist.
 */
static void
o_opclass_add_if_needed(Oid datoid, Oid opclassoid)
{
	Form_pg_opclass opclass;
	HeapTuple	opclasstuple;
	OOpclass   *o_opclass;
	Oid			base_type,
				cmpoid;

	o_opclass = o_opclass_cached_search(datoid, opclassoid, false);
	if (o_opclass != NULL)
	{
		/* it's already exist in B-tree */
		return;
	}

	/*
	 * we need to create OOpclass structure.
	 */
	o_opclass = palloc0(sizeof(OOpclass));
	o_opclass->key.datoid = datoid;
	o_opclass->key.opclassoid = opclassoid;

	/*
	 * find opclass in the database catalog
	 */
	opclasstuple = SearchSysCache1(CLAOID,
								   ObjectIdGetDatum(opclassoid));

	if (!HeapTupleIsValid(opclasstuple))
		elog(ERROR, "cache lookup failed for opclass %u", opclassoid);

	opclass = (Form_pg_opclass) GETSTRUCT(opclasstuple);
	o_opclass->opfamily = opclass->opcfamily;
	o_opclass->inputtype = opclass->opcintype;

	ReleaseSysCache(opclasstuple);

	/* find and fill prosrc and probin */
	base_type = getBaseType(o_opclass->inputtype);

	cmpoid = get_opfamily_proc(o_opclass->opfamily, base_type, base_type,
							   BTSORTSUPPORT_PROC);
	if (OidIsValid(cmpoid))
	{
		/* sort support is optional */
		o_type_procedure_fill(cmpoid, &o_opclass->ssupProc);
		o_opclass->hasSsup = true;
	}
	else
	{
		o_opclass->hasSsup = false;
	}
	o_opclass->ssupOid = cmpoid;

	cmpoid = get_opfamily_proc(o_opclass->opfamily, base_type, base_type,
							   BTORDER_PROC);
	if (!OidIsValid(cmpoid))
	{
		/* but compare function should exist */
		elog(ERROR, "Can't find comparator for opclass %u", opclassoid);
	}
	o_type_procedure_fill(cmpoid, &o_opclass->cmpProc);
	o_opclass->cmpOid = cmpoid;

	/*
	 * All done, now try to insert into B-tree.
	 */
	(void) o_opclass_insert(o_opclass);
	pfree(o_opclass);
}
