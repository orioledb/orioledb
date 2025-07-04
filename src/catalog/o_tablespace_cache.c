/*-------------------------------------------------------------------------
 *
 * o_tablespace_cache.c
 * 		Routines for orioledb relnode/datoid to tablespace map tree.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_tablespace_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"

#include "catalog/pg_tablespace_d.h"
#include "common/relpath.h"
#include "utils/syscache.h"

static OSysCache *tablespace_cache = NULL;

O_SYS_CACHE_FUNCS(tablespace_cache, OTablespace, 1);

static void o_tablespace_cache_free_entry(Pointer entry);
static void o_tablespace_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg);

static OSysCacheFuncs tablespace_cache_funcs =
{
	.free_entry = o_tablespace_cache_free_entry,
	.fill_entry = o_tablespace_cache_fill_entry,
};

typedef struct OTablespaceArg
{
	Oid			tablespace;
} OTablespaceArg;

O_SYS_CACHE_INIT_FUNC(tablespace_cache)
{
	Oid			keytypes[] = {OIDOID};
	int			data_len = sizeof(OTablespace);

	tablespace_cache = o_create_sys_cache(SYS_TREES_TABLESPACE_CACHE, false,
										  TableSpaceRelationId, TABLESPACEOID, 1,
										  keytypes, data_len, fastcache, mcxt,
										  &tablespace_cache_funcs);
}

void
o_tablespace_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key, Pointer arg)
{
	OTablespaceArg *otarg = (OTablespaceArg *) arg;
	OTablespace *o_tablespace = (OTablespace *) *entry_ptr;
	MemoryContext prev_context;
	char	   *prefix;
	char	   *db_prefix;

	prev_context = MemoryContextSwitchTo(tablespace_cache->mcxt);
	if (o_tablespace != NULL)	/* Existed o_tablespace updated */
	{
		Assert(false);
	}
	else
	{
		o_tablespace = palloc0(sizeof(OTablespace));
		*entry_ptr = (Pointer) o_tablespace;
	}

	if (OidIsValid(otarg->tablespace))
		o_tablespace->tablespace = otarg->tablespace;
	else
		o_tablespace->tablespace = MyDatabaseTableSpace;

	o_get_prefixes_for_tablespace(key->common.datoid, o_tablespace->tablespace, &prefix, &db_prefix);
	o_verify_dir_exists_or_create(prefix, NULL, NULL);
	o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
	pfree(db_prefix);

	MemoryContextSwitchTo(prev_context);
}

void
o_tablespace_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

void
o_tablespace_cache_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup,
							 Pointer arg)
{
	OTablespace *o_relnode_tablespace = (OTablespace *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", tablespace: %u)", o_relnode_tablespace->tablespace);
}

void
o_get_prefixes_for_tablespace(Oid datoid, Oid tablespace, char **prefix, char **db_prefix)
{
	static char pathbuf[MAXPGPATH];
	Datum		path_datum;
	text	   *path;
	char	   *path_str;

	path_datum = DirectFunctionCall1(pg_tablespace_location, ObjectIdGetDatum(tablespace));
	path = DatumGetTextP(path_datum);
	path_str = text_to_cstring(path);

	if (path_str[0] == '\0')
		snprintf(pathbuf, sizeof(pathbuf), "%s", ORIOLEDB_DATA_DIR);
	else
		snprintf(pathbuf, sizeof(pathbuf), "%s/" TABLESPACE_VERSION_DIRECTORY "/%s", path_str, ORIOLEDB_DATA_DIR);
	pfree(path_str);
	pfree(path);
	if (prefix)
		*prefix = pathbuf;
	if (db_prefix)
		*db_prefix = psprintf("%s/%u", pathbuf, datoid);
}

void
o_get_prefixes_for_relnode(Oid datoid, Oid relnode, char **prefix, char **db_prefix)
{
	XLogRecPtr	cur_lsn;
	Oid			tablespace = DEFAULTTABLESPACE_OID;

	if (datoid != SYS_TREES_DATOID)
	{
		OTablespace *o_tablespace = NULL;

		o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
		o_tablespace = o_tablespace_cache_search(datoid, relnode, cur_lsn, tablespace_cache->nkeys);
		if (o_tablespace)
			tablespace = o_tablespace->tablespace;
		else
			tablespace = DEFAULTTABLESPACE_OID;
	}

	o_get_prefixes_for_tablespace(datoid, tablespace, prefix, db_prefix);
}

void
o_tablespace_cache_add_relnode(Oid datoid, Oid relnode, Oid tablespace)
{
	OTablespaceArg arg = {.tablespace = tablespace};
	XLogRecPtr	cur_lsn;

	if ((!OidIsValid(tablespace) && MyDatabaseTableSpace == DEFAULTTABLESPACE_OID) ||
		tablespace == DEFAULTTABLESPACE_OID)
	{
		char	   *prefix;
		char	   *db_prefix;

		o_get_prefixes_for_tablespace(datoid, DEFAULTTABLESPACE_OID, &prefix, &db_prefix);
		o_verify_dir_exists_or_create(prefix, NULL, NULL);
		o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
		pfree(db_prefix);
		return;
	}

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	o_tablespace_cache_add_if_needed(datoid, relnode, cur_lsn, (Pointer) &arg);
}

void
o_tablespace_cache_add_datoid(Oid datoid, Oid tablespace)
{
	o_tablespace_cache_add_relnode(datoid, datoid, tablespace);
}

void
o_tablespace_cache_add_table(OTable *o_table)
{
	int			i;

	o_tablespace_cache_add_relnode(o_table->oids.datoid,
								   o_table->oids.relnode,
								   o_table->tablespace);
	for (i = 0; i < o_table->nindices; i++)
	{
		o_tablespace_cache_add_relnode(o_table->indices[i].oids.datoid,
									   o_table->indices[i].oids.relnode,
									   o_table->indices[i].tablespace);
	}
	o_tablespace_cache_add_relnode(o_table->toast_oids.datoid,
								   o_table->toast_oids.relnode,
								   o_table->tablespace);
	if (ORelOidsIsValid(o_table->bridge_oids))
		o_tablespace_cache_add_relnode(o_table->bridge_oids.datoid,
									   o_table->bridge_oids.relnode,
									   o_table->tablespace);
}
