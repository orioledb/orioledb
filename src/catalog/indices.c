/*-------------------------------------------------------------------------
 *
 * indices.c
 *		Indices routines
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/indices.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/build.h"
#include "btree/io.h"
#include "btree/undo.h"
#include "checkpoint/checkpoint.h"
#include "catalog/indices.h"
#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/operations.h"
#include "transam/oxid.h"
#include "tuple/slot.h"
#include "tuple/sort.h"
#include "tuple/toast.h"
#include "utils/compress.h"
#include "utils/planner.h"

#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_utilcmd.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

/* copied from tablecmds.c */
typedef struct NewColumnValue
{
	AttrNumber	attnum;			/* which column */
	Expr	   *expr;			/* expression to compute */
	ExprState  *exprstate;		/* execution state */
	bool		is_generated;	/* is it a GENERATED expression? */
} NewColumnValue;

bool		in_indexes_rebuild = false;

static void add_primary_fields(Relation indexRelation, OIndexDescr *idx_descr,
							   OTableIndex *index, OTable *o_table,
							   Relation pg_attribute, Form_pg_class class_form,
							   Form_pg_index index_form, Relation pg_index,
							   HeapTuple *index_tuple);
static void remove_primary_fields(Form_pg_index index_form,
								  Form_pg_class class_form, Oid reloid,
								  Relation pg_attribute,
								  OTableIndex *table_index);

bool
is_in_indexes_rebuild(void)
{
	return in_indexes_rebuild;
}

void
assign_new_oids(OTable *oTable, Relation rel)
{
	Oid			heap_relid,
				toast_relid;
#if PG_VERSION_NUM >= 140000
	ReindexParams params;
#endif
	CheckTableForSerializableConflictIn(rel);

	toast_relid = rel->rd_rel->reltoastrelid;
	if (OidIsValid(toast_relid))
	{
		Relation	toastrel = relation_open(toast_relid,
											 AccessExclusiveLock);

		RelationSetNewRelfilenode(toastrel,
								  toastrel->rd_rel->relpersistence);
		table_close(toastrel, NoLock);
	}

	heap_relid = RelationGetRelid(rel);

	PG_TRY();
	{
		in_indexes_rebuild = true;
#if PG_VERSION_NUM >= 140000
	params.options = 0;
	params.tablespaceOid = InvalidOid;
	reindex_relation(heap_relid, REINDEX_REL_PROCESS_TOAST, &params);
#else
	reindex_relation(heap_relid, REINDEX_REL_PROCESS_TOAST, 0);
#endif
		RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
	}
	PG_CATCH();
	{
		in_indexes_rebuild = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
	in_indexes_rebuild = false;
	o_table_fill_oids(oTable, rel, &rel->rd_node);
	orioledb_free_rd_amcache(rel);
}

void
recreate_o_table(OTable *old_o_table, OTable *o_table)
{
	CommitSeqNo csn;
	OXid		oxid;
	int			oldTreeOidsNum,
				newTreeOidsNum;
	ORelOids	oldOids = old_o_table->oids,
			   *oldTreeOids,
				newOids = o_table->oids,
			   *newTreeOids;

	fill_current_oxid_csn(&oxid, &csn);

	oldTreeOids = o_table_make_index_oids(old_o_table, &oldTreeOidsNum);
	newTreeOids = o_table_make_index_oids(o_table, &newTreeOidsNum);

	o_tables_drop_by_oids(old_o_table->oids, oxid, csn);
	o_tables_add(o_table, oxid, csn);
	add_invalidate_wal_record(o_table->oids, old_o_table->oids.relnode);

	add_undo_truncate_relnode(oldOids, oldTreeOids, oldTreeOidsNum,
							  newOids, newTreeOids, newTreeOidsNum);
	pfree(oldTreeOids);
	pfree(newTreeOids);
}

static void
o_validate_index_elements(OTable *o_table, OIndexType type, List *index_elems,
						  Node *whereClause)
{
	ListCell   *field_cell;

	if (whereClause)
		o_validate_funcexpr(whereClause, " are supported in "
							"orioledb index predicate");

	foreach(field_cell, index_elems)
	{
		OTableField *field;
		IndexElem  *ielem = castNode(IndexElem, lfirst(field_cell));

		if (!ielem->expr)
		{
			int			attnum = o_table_fieldnum(o_table, ielem->name);

			if (attnum == o_table->nfields)
			{
				elog(ERROR, "indexed field %s is not found in orioledb table",
					 ielem->name);
			}
			field = &o_table->fields[attnum];

			if (type == oIndexPrimary && !field->notnull)
			{
				elog(ERROR, "primary key should include only NOT NULL columns, "
					 "but column %s is nullable", ielem->name);
			}

			if (type_is_collatable(field->typid))
			{
				if (!OidIsValid(field->collation))
					ereport(ERROR,
							(errcode(ERRCODE_INDETERMINATE_COLLATION),
							 errmsg("could not determine which collation to use for index expression"),
							 errhint("Use the COLLATE clause to set the collation explicitly.")));
			}
			else
			{
				if (OidIsValid(field->collation))
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("collations are not supported by type %s",
									format_type_be(field->typid))));
			}
		}
		else
		{
			o_validate_funcexpr(ielem->expr, " are supported in "
								"orioledb index expressions");
		}
	}
}

void
o_define_index_validate(Relation rel, IndexStmt *stmt, bool skip_build,
						ODefineIndexContext **arg)
{
	int							nattrs;
	Oid							myrelid = RelationGetRelid(rel);
	ORelOids					oids = {MyDatabaseId,
										myrelid,
										rel->rd_node.relNode};
	OIndexType					ix_type;
	static ODefineIndexContext	context;
	OTable					   *o_table;
	bool						reuse = OidIsValid(stmt->oldNode);

	*arg = &context;

	context.oldNode = stmt->oldNode;

	if (!reuse)
	{
		if (strcmp(stmt->accessMethod, "btree") != 0)
			ereport(ERROR, errmsg("'%s' access method is not supported", stmt->accessMethod),
					errhint("Only 'btree' access method supported now "
							"for indices on orioledb tables."));

		if (stmt->concurrent)
			elog(ERROR, "concurrent indexes are not supported.");

		if (stmt->tableSpace != NULL)
			elog(ERROR, "tablespaces aren't supported");

		o_table = o_tables_get(oids);
		if (o_table == NULL)
		{
			elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
				(unsigned) oids.datoid, (unsigned) oids.reloid,
				(unsigned) oids.relnode);
		}

		/* check index type */
		if (stmt->primary)
			ix_type = oIndexPrimary;
		else if (stmt->unique)
			ix_type = oIndexUnique;
		else
			ix_type = oIndexRegular;

		/* check index fields number */
		nattrs = list_length(stmt->indexParams);
		if (ix_type == oIndexPrimary && !skip_build)
		{
			if (o_table->nindices > 0)
			{
				int nattrs_max = 0,
					ix;

				if (o_table->has_primary)
					elog(ERROR, "table already has primary index");

				for (ix = 0; ix < o_table->nindices; ix++)
					nattrs_max = Max(nattrs_max, o_table->indices[ix].nfields);

				if (nattrs_max + nattrs > INDEX_MAX_KEYS)
				{
					elog(ERROR, "too many fields in the primary index for exiting indices");
				}
			}
		}
		else
		{
			if (o_table->nindices > 0 &&
				o_table->indices[0].type != oIndexRegular &&
				nattrs + o_table->indices[0].nfields > INDEX_MAX_KEYS)
			{
				elog(ERROR, "too many fields in the index");
			}
		}

		if (stmt->idxname == NULL)
		{
			List   *allIndexParams;
			List   *indexColNames;

			if (ix_type != oIndexPrimary && o_table->has_primary &&
				stmt->indexIncludingParams)
			{
				int				npkeyfields;
				int				nfields;
				int				nkeyfields;
				int				i;
				ListCell	   *lc;
				OTableIndex	   *primary;
				Bitmapset	   *pkfields = NULL;

				allIndexParams = list_concat_copy(stmt->indexParams,
												  stmt->indexIncludingParams);

				primary = &o_table->indices[PrimaryIndexNumber];

				npkeyfields = primary->nfields;
				nfields = list_length(allIndexParams);
				nkeyfields = list_length(stmt->indexParams);
				foreach(lc, allIndexParams)
				{
					int				pknum;
					int				fieldnum;
					AttrNumber		attnum;
					bool			pkey_started = false;
					IndexElem	   *elem = lfirst(lc);

					if (!elem->expr)
					{
						fieldnum = foreach_current_index(lc);
						attnum = o_table_fieldnum(o_table, elem->name);
						for (pknum = 0; pknum < primary->nfields; pknum++)
						{
							OTableIndexField *pkfield;

							pkfield = &primary->fields[pknum];

							if (AttributeNumberIsValid(attnum) &&
								attnum == pkfield->attnum)
							{
								OTableField *table_field;
								Oid opclass;

								table_field =
									o_table_field_by_name(o_table, elem->name);

								opclass = ResolveOpClass(elem->opclass,
														 table_field->typid,
														 "btree",
														 BTREE_AM_OID);

								if ((opclass == pkfield->opclass) &&
									!bms_is_member(attnum, pkfields))
								{
									pkfields = bms_add_member(pkfields,
															  attnum);

									if ((fieldnum >= nkeyfields) &&
										((nfields - fieldnum) == npkeyfields))
										pkey_started = true;
									else
										npkeyfields--;
									break;
								}
							}
						}
					}
					if (pkey_started)
						break;
				}
				bms_free(pkfields);

				for (i = npkeyfields; i > 0; i--)
				{
					int				pknum;
					IndexElem	   *elem;
					bool			member = false;

					elem = llast(stmt->indexIncludingParams);

					for (pknum = 0; pknum < primary->nfields; pknum++)
					{
						OTableIndexField   *pk_field = &primary->fields[i];
						AttrNumber			attnum;

						attnum = o_table_fieldnum(o_table, elem->name);
						if (attnum == pk_field->attnum)
						{
							member = true;
							break;
						}
					}
					if (!member)
						break;
					stmt->indexIncludingParams =
						list_delete_last(stmt->indexIncludingParams);
				}
			}
			allIndexParams = list_concat_copy(stmt->indexParams,
											  stmt->indexIncludingParams);
			indexColNames = ChooseIndexColumnNames(allIndexParams);

			stmt->idxname = ChooseIndexName(RelationGetRelationName(rel),
											RelationGetNamespace(rel),
											indexColNames,
											stmt->excludeOpNames,
											stmt->primary,
											stmt->isconstraint);
		}

		/*
		 * Add primary key fields, because otherwise, when planning a query with a
		 * where clause consisting only of index fields and primary key fields, an
		 * index-only scan is not selected.
		 */
		if (ix_type != oIndexPrimary && o_table->has_primary)
		{
			int i;
			int nfields;

			nfields = o_table->indices[PrimaryIndexNumber].nfields;

			for (i = 0; i < nfields; i++)
			{
				OTableIndexField   *pk_field;
				OTableField		   *table_field;
				bool				member = false;
				ListCell		   *lc;

				pk_field = &o_table->indices[PrimaryIndexNumber].fields[i];
				table_field = &o_table->fields[pk_field->attnum];

				foreach(lc, stmt->indexParams)
				{
					IndexElem *elem = lfirst(lc);

					if (!elem->expr &&
						strcmp(elem->name, table_field->name.data) == 0)
					{
						member = true;
						break;
					}
				}

				if (!member)
				{
					foreach(lc, stmt->indexIncludingParams)
					{
						IndexElem *elem = lfirst(lc);

						if (strcmp(elem->name,
									table_field->name.data) == 0)
						{
							member = true;
							break;
						}
					}
				}

				if (!member)
				{
					IndexElem   *iparam = makeNode(IndexElem);

					iparam->name = pstrdup(table_field->name.data);
					iparam->expr = NULL;
					iparam->indexcolname = NULL;
					iparam->collation = NIL;
					iparam->opclass = NIL;
					iparam->opclassopts = NIL;

					stmt->indexIncludingParams =
						lappend(stmt->indexIncludingParams, iparam);
				}
			}
		}

		/* check index fields */
		o_validate_index_elements(o_table, ix_type,
								  stmt->indexParams, stmt->whereClause);
	}
}

static void
o_update_pg_atttributes(OTable *o_table, OTable *old_o_table,
						OIndexNumber ix_num, OTableDescr *descr)
{
	OTableIndex	   *table_index = &o_table->indices[ix_num];
	int				ctid_off = o_table->has_primary ? 0 : 1;
	OIndexDescr	   *idx_descr = descr->indices[ix_num + ctid_off];

	if (table_index->type != oIndexPrimary)
	{
		Relation		indexRelation;
		Oid				reloid;
		Relation		pg_class;
		Relation		pg_index;
		Relation		pg_attribute;
		Form_pg_class	class_form;
		Form_pg_index	index_form;
		HeapTuple		class_tuple,
						index_tuple;

		indexRelation = index_open(table_index->oids.reloid,
								   AccessExclusiveLock);
		reloid = RelationGetRelid(indexRelation);

		pg_class = table_open(RelationRelationId, RowExclusiveLock);
		class_tuple = SearchSysCacheCopy1(RELOID,
										  ObjectIdGetDatum(reloid));
		if (!HeapTupleIsValid(class_tuple))
			elog(ERROR, "could not find pg_class for relation %u",
				 reloid);
		class_form = (Form_pg_class)GETSTRUCT(class_tuple);

		pg_index = table_open(IndexRelationId, RowExclusiveLock);
		index_tuple = SearchSysCacheCopy1(INDEXRELID,
										  ObjectIdGetDatum(reloid));
		if (!HeapTupleIsValid(index_tuple))
			elog(ERROR, "could not find pg_index for relation %u",
				 reloid);
		index_form = (Form_pg_index)GETSTRUCT(index_tuple);

		pg_attribute = table_open(AttributeRelationId,
								  RowExclusiveLock);

		if (o_table->has_primary)
			add_primary_fields(indexRelation, idx_descr, table_index,
							   o_table, pg_attribute, class_form,
							   index_form, pg_index, &index_tuple);
		else
		{
			OTableIndex *old_table_index;
			old_table_index = &old_o_table->indices[ix_num + 1];
			remove_primary_fields(index_form, class_form,
								  reloid, pg_attribute,
								  old_table_index);
		}

		CatalogTupleUpdate(pg_class, &class_tuple->t_self,
						   class_tuple);
		CatalogTupleUpdate(pg_index, &index_tuple->t_self,
						   index_tuple);
		heap_freetuple(class_tuple);
		heap_freetuple(index_tuple);
		table_close(pg_attribute, RowExclusiveLock);
		table_close(pg_class, RowExclusiveLock);
		table_close(pg_index, RowExclusiveLock);
		index_close(indexRelation, AccessExclusiveLock);
	}
}

void
o_define_index(Relation rel, Oid indoid, bool reindex,
			   bool skip_constraint_checks, bool skip_build,
			   ODefineIndexContext *context)
{
	Relation		index_rel;
	OTable		   *old_o_table = NULL;
	OTable		   *new_o_table;
	OTable		   *o_table;
	OIndexNumber	ix_num;
	OTableIndex	   *index;
	OTableDescr	   *old_descr = NULL;
	bool			reuse = false;
	bool			is_build = false;
	Oid				myrelid = RelationGetRelid(rel);
	ORelOids		oids = {MyDatabaseId, myrelid, rel->rd_node.relNode};
	OIndexType		ix_type;
	OCompress		compress = InvalidOCompress;
	int16			indnatts;
	int16			indnkeyatts;
	OBTOptions	   *options;

	index_rel = index_open(indoid, AccessShareLock);
	if (context)
		reuse = OidIsValid(context->oldNode);

	options = (OBTOptions *) index_rel->rd_options;

	if (options)
	{
		if (options->compress_offset > 0)
		{
			char   *str;

			str = (char *)(((Pointer) options) + options->compress_offset);
			if (str)
				compress = o_parse_compress(str);
		}
	}

	if (index_rel->rd_index->indisprimary)
		ix_type = oIndexPrimary;
	else if (index_rel->rd_index->indisunique)
		ix_type = oIndexUnique;
	else
		ix_type = oIndexRegular;

	indnatts = index_rel->rd_index->indnatts;
	indnkeyatts = index_rel->rd_index->indnkeyatts;

	index_close(index_rel, AccessShareLock);

	old_o_table = o_tables_get(oids);
	if (old_o_table == NULL)
	{
		elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
			 (unsigned) oids.datoid, (unsigned) oids.reloid,
			 (unsigned) oids.relnode);
	}
	o_table = old_o_table;

	if (!reuse && skip_build)
		return;

	if (!reuse)
	{
		if (reindex)
		{
			int	i;

			ix_num = InvalidIndexNumber;
			for (i = 0; i < o_table->nindices; i++)
			{
				if (o_table->indices[i].oids.reloid == indoid)
					ix_num = i;
			}
			reindex = ix_num != InvalidIndexNumber &&
					  ix_num < o_table->nindices;
		}

		if (reindex)
		{
			o_index_drop(rel, ix_num);

			if (ix_type == oIndexPrimary)
			{
				o_table_free(old_o_table);
				oids.relnode = rel->rd_node.relNode;
				old_o_table = o_tables_get(oids);
				if (old_o_table == NULL)
				{
					elog(FATAL, "orioledb table does not exists "
								"for oids = %u, %u, %u",
						 oids.datoid, oids.reloid, oids.relnode);
				}
				o_table = old_o_table;
				reindex = false;
			}
		}

		if (!reindex)
		{
			ORelOids	primary_oids;

			primary_oids = ix_type == oIndexPrimary ||
						   !old_o_table->has_primary ?
							   old_o_table->oids :
							   old_o_table->indices[PrimaryIndexNumber].oids;
			is_build = tbl_data_exists(&primary_oids);

			/* Rebuild, assign new oids */
			if (ix_type == oIndexPrimary)
			{
				new_o_table = o_tables_get(oids);
				o_table = new_o_table;
				assign_new_oids(new_o_table, rel);
				oids = new_o_table->oids;
			}

			if (ix_type == oIndexPrimary)
			{
				ix_num = 0; /* place first */
				o_table->has_primary = true;
				o_table->primary_init_nfields = o_table->nfields;
			}
			else
			{
				ix_num = o_table->nindices;
			}
			o_table->indices = (OTableIndex *)
				repalloc(o_table->indices, sizeof(OTableIndex) *
											(o_table->nindices + 1));

			/* move indices if needed */
			if (ix_type == oIndexPrimary && o_table->nindices > 0)
			{
				memmove(&o_table->indices[1], &o_table->indices[0],
						o_table->nindices * (sizeof(OTableIndex)));
			}
			o_table->nindices++;

			index = &o_table->indices[ix_num];

			memset(index, 0, sizeof(OTableIndex));

			index->type = ix_type;
			index->nfields = indnatts;
			index->nkeyfields = indnkeyatts;
			index->npkeyfields = 0;
			if (ix_type != oIndexPrimary && o_table->has_primary)
			{
				int				pknum;
				OTableIndex	   *primary;

				primary = &o_table->indices[PrimaryIndexNumber];
				for (pknum = 0; pknum < primary->nfields; pknum++)
				{
					OTableIndexField   *pkfield = &primary->fields[pknum];
					int					fieldnum;
					bool				member = false;
					int					pkey_start = index->nfields;

					for (fieldnum = 0; fieldnum < pkey_start; fieldnum++)
					{
						AttrNumber		attnum;

						attnum = index_rel->rd_index->indkey.values[fieldnum];
						if (AttributeNumberIsValid(attnum) &&
							attnum == pkfield->attnum + 1)
						{
							member = true;
							break;
						}
						pkey_start--;
					}

					if (!member)
						index->npkeyfields++;
				}
			} else if (ix_type == oIndexPrimary)
			{
				index->npkeyfields = indnatts;
			}

			if (OCompressIsValid(compress))
				index->compress = compress;
			else if (ix_type == oIndexPrimary)
				index->compress = o_table->primary_compress;
			else
				index->compress = o_table->default_compress;
		}
		else
		{
			is_build = true;
		}
	}
	else
	{
		int	i;

		ix_num = InvalidIndexNumber;
		for (i = 0; i < o_table->nindices; i++)
		{
			if (o_table->indices[i].oids.relnode == context->oldNode)
				ix_num = i;
		}
		Assert(ix_num != InvalidIndexNumber);
	}

	index_rel = index_open(indoid, AccessShareLock);
	index = &o_table->indices[ix_num];
	if (!reuse)
		memcpy(&index->name, &index_rel->rd_rel->relname,
			   sizeof(NameData));
	index->oids.relnode = index_rel->rd_rel->relfilenode;

	/* fill index fields */
	if (!reuse)
	{
		index->type = ix_type;
		o_table_fill_index(o_table, ix_num, index_rel);
	}

	index_close(index_rel, AccessShareLock);

	index->oids.datoid = MyDatabaseId;
	index->oids.reloid = indoid;

	is_build = is_build && !skip_build;

	if (!reuse)
	{
		o_opclass_cache_add_table(o_table);
		custom_types_add_all(o_table, index);

		/* update o_table */
		if (old_o_table)
			old_descr = o_fetch_table_descr(old_o_table->oids);

		/* create orioledb index from exist data */
		if (is_build)
		{
			OTableDescr tmpDescr;

			if (index->type == oIndexPrimary)
			{
				int	new_ix;
				Assert(old_o_table);

				for (new_ix = 0; new_ix < o_table->nindices; new_ix++)
				{
					OTableIndex	   *new_index = &o_table->indices[new_ix];
					if (new_index->type != oIndexPrimary)
					{
						int	pknum;

						new_index->npkeyfields = 0;

						for (pknum = 0; pknum < index->nfields; pknum++)
						{
							OTableIndexField *pkfield = &index->fields[pknum];
							int fieldnum;
							bool member = false;

							for (fieldnum = 0; fieldnum < new_index->nfields; fieldnum++)
							{
								OTableIndexField *field = &new_index->fields[fieldnum];
								if (field->attnum == pkfield->attnum)
								{
									member = true;
									break;
								}
							}

							if (!member)
								new_index->npkeyfields++;
						}

						new_index->nfields += new_index->npkeyfields;
					}
				}
				o_fill_tmp_table_descr(&tmpDescr, o_table);
				rebuild_indices(old_o_table, old_descr, o_table, &tmpDescr);

				if (!is_recovery_in_progress())
				{
					int i;

					for (i = 0; i < o_table->nindices; i++)
					{
						o_update_pg_atttributes(o_table, old_o_table, i,
												&tmpDescr);
					}

					/* Make the updated catalog row versions visible */
					CommandCounterIncrement();
				}
				o_free_tmp_table_descr(&tmpDescr);
			}
			else
			{
				o_fill_tmp_table_descr(&tmpDescr, o_table);
				build_secondary_index(o_table, &tmpDescr, ix_num);
				o_free_tmp_table_descr(&tmpDescr);
			}
		}
		else if (index->type == oIndexPrimary && !is_recovery_in_progress())
		{
			OTableDescr	tmpDescr;
			int			i;

			for (i = 0; i < o_table->nindices; i++)
			{
				OTableIndex	   *new_index = &o_table->indices[i];
				if (new_index->type != oIndexPrimary)
				{
					int	pknum;

					new_index->npkeyfields = 0;

					for (pknum = 0; pknum < index->nfields; pknum++)
					{
						OTableIndexField *pkfield = &index->fields[pknum];
						int fieldnum;
						bool member = false;

						for (fieldnum = 0; fieldnum < new_index->nfields; fieldnum++)
						{
							OTableIndexField *field = &new_index->fields[fieldnum];
							if (field->attnum == pkfield->attnum)
							{
								member = true;
								break;
							}
						}

						if (!member)
							new_index->npkeyfields++;
					}

					new_index->nfields += new_index->npkeyfields;
				}
			}

			o_fill_tmp_table_descr(&tmpDescr, o_table);

			for (i = 0; i < o_table->nindices; i++)
			{
				o_update_pg_atttributes(o_table, old_o_table, i,
										&tmpDescr);
			}
			/* Make the updated catalog row versions visible */
			CommandCounterIncrement();
			o_free_tmp_table_descr(&tmpDescr);
		}
	}

	if (!reuse && index->type == oIndexPrimary)
	{
		CommitSeqNo csn;
		OXid oxid;

		Assert(old_o_table);
		fill_current_oxid_csn(&oxid, &csn);
		recreate_o_table(old_o_table, o_table);
	}
	else
	{
		CommitSeqNo csn;
		OXid		oxid;

		fill_current_oxid_csn(&oxid, &csn);
		o_tables_update(o_table, oxid, csn);
		add_undo_create_relnode(o_table->oids, &index->oids, 1);
		recreate_table_descr_by_oids(oids);
	}

	if (reindex)
	{
		o_invalidate_oids(index->oids);
		o_add_invalidate_undo_item(index->oids, O_INVALIDATE_OIDS_ON_ABORT);
	}

	if (old_o_table)
		o_table_free(old_o_table);
	if (o_table != old_o_table)
		o_table_free(o_table);

	if (is_build)
		LWLockRelease(&checkpoint_state->oTablesAddLock);
}

void
build_secondary_index(OTable *o_table, OTableDescr *descr, OIndexNumber ix_num)
{
	BTreeIterator *iter;
	OIndexDescr *primary,
			   *idx;
	Tuplesortstate *sortstate;
	TupleTableSlot *primarySlot;
	Relation	tableRelation,
				indexRelation = NULL;
	double		heap_tuples,
				index_tuples;
	uint64		ctid;
	CheckpointFileHeader fileHeader;

	idx = descr->indices[o_table->has_primary ? ix_num : ix_num + 1];

	primary = GET_PRIMARY(descr);

	o_btree_load_shmem(&primary->desc);

	if (!is_recovery_in_progress())
		indexRelation = index_open(idx->oids.reloid, AccessShareLock);
	sortstate = tuplesort_begin_orioledb_index(idx, work_mem, false, NULL);
	if (indexRelation)
		index_close(indexRelation, AccessShareLock);

	iter = o_btree_iterator_create(&primary->desc, NULL, BTreeKeyNone,
								   COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	primarySlot = MakeSingleTupleTableSlot(descr->tupdesc, &TTSOpsOrioleDB);

	heap_tuples = 0;
	index_tuples = 0;
	ctid = 1;
	while (true)
	{
		OTuple		primaryTup;
		OTuple		secondaryTup;
		MemoryContext oldContext;

		primaryTup = o_btree_iterator_fetch(iter, NULL, NULL,
											BTreeKeyNone, true, NULL);

		if (O_TUPLE_IS_NULL(primaryTup))
			break;

		tts_orioledb_store_tuple(primarySlot, primaryTup, descr,
								 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
								 true, NULL);

		slot_getallattrs(primarySlot);

		heap_tuples++;

		if (o_is_index_predicate_satisfied(idx, primarySlot, idx->econtext))
		{
			oldContext = MemoryContextSwitchTo(sortstate->tuplecontext);
			secondaryTup = tts_orioledb_make_secondary_tuple(primarySlot,
															 idx, true);
			MemoryContextSwitchTo(oldContext);

			index_tuples++;

			o_btree_check_size_of_tuple(o_tuple_size(secondaryTup,
													 &idx->leafSpec),
										idx->name.data, true);
			tuplesort_putotuple(sortstate, secondaryTup);
		}

		ExecClearTuple(primarySlot);
	}
	ExecDropSingleTupleTableSlot(primarySlot);
	pfree(iter);

	tuplesort_performsort(sortstate);

	btree_write_index_data(&idx->desc, idx->leafTupdesc, sortstate,
						   ctid, &fileHeader);
	tuplesort_end_orioledb_index(sortstate);

	/*
	 * We hold oTablesAddLock till o_tables_update().  So, checkpoint number
	 * in the data file will be consistent with o_tables metadata.
	 */
	LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);

	btree_write_file_header(&idx->desc, &fileHeader);

	if (!is_recovery_in_progress())
	{
		tableRelation = table_open(o_table->oids.reloid, AccessExclusiveLock);
		indexRelation = index_open(o_table->indices[ix_num].oids.reloid,
								   AccessExclusiveLock);
		index_update_stats(tableRelation,
						   true,
						   heap_tuples);

		index_update_stats(indexRelation,
						   false,
						   index_tuples);

		/* Make the updated catalog row versions visible */
		CommandCounterIncrement();
		table_close(tableRelation, AccessExclusiveLock);
		index_close(indexRelation, AccessExclusiveLock);
	}
}

static void
add_primary_fields(Relation indexRelation, OIndexDescr *idx_descr,
				   OTableIndex *index, OTable *o_table, Relation pg_attribute,
				   Form_pg_class class_form, Form_pg_index index_form,
				   Relation pg_index, HeapTuple *index_tuple)
{
	Oid			reloid = RelationGetRelid(indexRelation);
	int2vector *indkey;
	int attnum;
	Datum values[Natts_pg_index] = {0};
	bool nulls[Natts_pg_index] = {0};
	bool replaces[Natts_pg_index] = {0};
	HeapTuple old_index_tuple;
	int nsupport;
	int indkey_ix;
	int pkey_start = index->nfields - index->npkeyfields;

	for (attnum = 0; attnum < index->npkeyfields; attnum++)
	{
		FormData_pg_attribute attribute;
#if PG_VERSION_NUM >= 140000
		FormData_pg_attribute *aattr[] = {&attribute};
		TupleDesc tupdesc;
#endif
		OIndexField *idx_field = &idx_descr->fields[pkey_start + attnum];
		OTableField *table_field = &o_table->fields[idx_field->tableAttnum - 1];

		attribute.attrelid = reloid;
		namestrcpy(&(attribute.attname), table_field->name.data);

		attribute.atttypid = table_field->typid;
		attribute.attstattarget = 0;
		attribute.attlen = table_field->typlen;
		attribute.attnum = pkey_start + attnum + 1;
		attribute.attndims = table_field->ndims;
		attribute.atttypmod = table_field->typmod;
		attribute.attbyval = table_field->byval;
		attribute.attalign = table_field->align;
		attribute.attstorage = table_field->storage;
#if PG_VERSION_NUM >= 140000
		attribute.attcompression = table_field->compression;
#endif
		attribute.attnotnull = table_field->notnull;
		attribute.atthasdef = false;
		attribute.atthasmissing = false;
		attribute.attidentity = '\0';
		attribute.attgenerated = '\0';
		attribute.attisdropped = false;
		attribute.attislocal = true;
		attribute.attinhcount = 0;
		attribute.attcollation = table_field->collation;

#if PG_VERSION_NUM >= 140000
		tupdesc = CreateTupleDesc(lengthof(aattr),
								  (FormData_pg_attribute **) &aattr);
		InsertPgAttributeTuples(pg_attribute, tupdesc, reloid, NULL, NULL);
#else
		InsertPgAttributeTuple(pg_attribute, &attribute, (Datum)0, NULL);
#endif
	}

	if (indexRelation->rd_opcoptions)
	{
		int relatt;
		for (relatt = 0; relatt < class_form->relnatts; relatt++)
		{
			if (indexRelation->rd_opcoptions[relatt])
				pfree(indexRelation->rd_opcoptions[relatt]);
		}
		pfree(indexRelation->rd_opcoptions);
		indexRelation->rd_opcoptions = NULL;
	}

	if (indexRelation->rd_support)
		pfree(indexRelation->rd_support);
	if (indexRelation->rd_supportinfo)
		pfree(indexRelation->rd_supportinfo);

	class_form->relnatts += index->npkeyfields;
	index_form->indnatts += index->npkeyfields;

	nsupport = index_form->indnatts *
			   indexRelation->rd_indam->amsupport;
	indexRelation->rd_support = (RegProcedure *)
		MemoryContextAllocZero(indexRelation->rd_indexcxt,
							   nsupport * sizeof(RegProcedure));
	indexRelation->rd_supportinfo = (FmgrInfo *)
		MemoryContextAllocZero(indexRelation->rd_indexcxt,
							   nsupport * sizeof(FmgrInfo));

	indkey = buildint2vector(NULL, index_form->indnatts);
	for (indkey_ix = 0; indkey_ix < pkey_start; indkey_ix++)
	{
		indkey->values[indkey_ix] = index_form->indkey.values[indkey_ix];
	}
	for (indkey_ix = 0; indkey_ix < index->npkeyfields; indkey_ix++)
	{
		OIndexField *idx_field = &idx_descr->fields[pkey_start + indkey_ix];

		indkey->values[pkey_start + indkey_ix] = idx_field->tableAttnum;
	}

	replaces[Anum_pg_index_indkey - 1] = true;
	values[Anum_pg_index_indkey - 1] = PointerGetDatum(indkey);

	old_index_tuple = *index_tuple;
	*index_tuple = heap_modify_tuple(old_index_tuple,
									 RelationGetDescr(pg_index), values,
									 nulls, replaces);
	heap_freetuple(old_index_tuple);
}

static void
remove_primary_fields(Form_pg_index index_form, Form_pg_class class_form,
					  Oid reloid, Relation pg_attribute,
					  OTableIndex *table_index)
{
	int attnum;
	int pkey_start = table_index->nfields - table_index->npkeyfields;

	for (attnum = 0; attnum < table_index->npkeyfields; attnum++)
	{
		HeapTuple attr_tuple;

		attr_tuple = SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(reloid),
										 Int16GetDatum(pkey_start +
													   attnum + 1));

		if (!HeapTupleIsValid(attr_tuple))
			elog(ERROR, "could not find pg_attribute for "
						"relation %u",
				 reloid);

		CatalogTupleDelete(pg_attribute, &attr_tuple->t_self);
	}
	class_form->relnatts = pkey_start;
	index_form->indnatts = pkey_start;
}

void
rebuild_indices(OTable *old_o_table, OTableDescr *old_descr,
				OTable *o_table, OTableDescr *descr)
{
	BTreeIterator		   *iter;
	OIndexDescr			   *primary,
						   *idx;
	Tuplesortstate		  **sortstates;
	Tuplesortstate		   *toastSortState;
	TupleTableSlot		   *primarySlot;
	int						i;
	Relation				tableRelation;
	double					heap_tuples,
						   *index_tuples;
	uint64					ctid;
	CheckpointFileHeader   *fileHeaders;
	CheckpointFileHeader	toastFileHeader;

	primary = GET_PRIMARY(old_descr);
	o_btree_load_shmem(&primary->desc);

	sortstates = (Tuplesortstate **) palloc(sizeof(Tuplesortstate *) *
											descr->nIndices);
	fileHeaders = (CheckpointFileHeader *) palloc(sizeof(CheckpointFileHeader) *
												  descr->nIndices);

	for (i = 0; i < descr->nIndices; i++)
	{
		idx = descr->indices[i];
		sortstates[i] = tuplesort_begin_orioledb_index(idx, work_mem, false, NULL);
	}
	primarySlot = MakeSingleTupleTableSlot(old_descr->tupdesc, &TTSOpsOrioleDB);

	btree_open_smgr(&descr->toast->desc);
	toastSortState = tuplesort_begin_orioledb_toast(descr->toast,
													descr->indices[0],
													work_mem, false, NULL);

	iter = o_btree_iterator_create(&primary->desc, NULL, BTreeKeyNone,
								   COMMITSEQNO_INPROGRESS, ForwardScanDirection);
	heap_tuples = 0;
	ctid = 0;
	index_tuples = palloc0(sizeof(double) * descr->nIndices);
	while (true)
	{
		OTuple		primaryTup;

		primaryTup = o_btree_iterator_fetch(iter, NULL, NULL,
											BTreeKeyNone, true, NULL);

		if (O_TUPLE_IS_NULL(primaryTup))
			break;

		tts_orioledb_store_tuple(primarySlot, primaryTup, old_descr,
								 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
								 true, NULL);

		slot_getallattrs(primarySlot);
		tts_orioledb_detoast(primarySlot);
		tts_orioledb_toast(primarySlot, descr);

		for (i = 0; i < descr->nIndices; i++)
		{
			OTuple		newTup;
			MemoryContext oldContext;

			idx = descr->indices[i];

			if (!o_is_index_predicate_satisfied(idx, primarySlot,
												idx->econtext))
				continue;

			index_tuples[i]++;

			oldContext = MemoryContextSwitchTo(sortstates[i]->tuplecontext);
			if (i == 0)
			{
				if (idx->primaryIsCtid)
				{
					primarySlot->tts_tid.ip_posid = (OffsetNumber) ctid;
					BlockIdSet(&primarySlot->tts_tid.ip_blkid,
							   (uint32) (ctid >> 16));
					ctid++;
				}
				newTup = tts_orioledb_form_orphan_tuple(primarySlot, descr);
			}
			else
			{
				newTup = tts_orioledb_make_secondary_tuple(primarySlot,
														   idx, true);
			}
			MemoryContextSwitchTo(oldContext);
			o_btree_check_size_of_tuple(o_tuple_size(newTup, &idx->leafSpec),
										idx->name.data, true);
			tuplesort_putotuple(sortstates[i], newTup);
		}

		tts_orioledb_toast_sort_add(primarySlot, descr, toastSortState);

		ExecClearTuple(primarySlot);
		heap_tuples++;
	}

	ExecDropSingleTupleTableSlot(primarySlot);
	btree_iterator_free(iter);

	for (i = 0; i < descr->nIndices; i++)
	{
		idx = descr->indices[i];
		tuplesort_performsort(sortstates[i]);
		btree_write_index_data(&idx->desc, idx->leafTupdesc, sortstates[i],
							   (idx->primaryIsCtid &&
								i == PrimaryIndexNumber) ? ctid : 0,
							   &fileHeaders[i]);
		tuplesort_end_orioledb_index(sortstates[i]);
	}
	pfree(sortstates);

	tuplesort_performsort(toastSortState);
	btree_write_index_data(&descr->toast->desc, descr->toast->leafTupdesc,
						   toastSortState, 0, &toastFileHeader);
	tuplesort_end_orioledb_index(toastSortState);

	/*
	 * We hold oTablesAddLock till o_tables_update().  So, checkpoint number
	 * in the data file will be consistent with o_tables metadata.
	 */
	LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);

	for (i = 0; i < descr->nIndices; i++)
		btree_write_file_header(&descr->indices[i]->desc, &fileHeaders[i]);
	btree_write_file_header(&descr->toast->desc, &toastFileHeader);

	pfree(fileHeaders);

	if (!is_recovery_in_progress())
	{
		tableRelation = table_open(o_table->oids.reloid, AccessExclusiveLock);
		index_update_stats(tableRelation, true, heap_tuples);

		for (i = 0; i < o_table->nindices; i++)
		{
			OTableIndex *table_index = &o_table->indices[i];
			Relation	indexRelation;

			indexRelation = index_open(table_index->oids.reloid,
									   AccessExclusiveLock);

			index_update_stats(indexRelation, false, index_tuples[i]);
			index_close(indexRelation, AccessExclusiveLock);
		}

		/* Make the updated catalog row versions visible */
		CommandCounterIncrement();
		table_close(tableRelation, AccessExclusiveLock);
	}
	pfree(index_tuples);
}

static void
drop_primary_index(Relation rel, OTable *o_table)
{
	OTable		   *old_o_table;
	OTableDescr		tmp_descr;
	OTableDescr	   *old_descr;
	int				ix_num;

	Assert(o_table->indices[PrimaryIndexNumber].type == oIndexPrimary);

	old_o_table = o_table;
	o_table = o_tables_get(o_table->oids);
	assign_new_oids(o_table, rel);

	memmove(&o_table->indices[0],
			&o_table->indices[1],
			(o_table->nindices - 1) * sizeof(OTableIndex));
	o_table->nindices--;
	o_table->has_primary = false;
	o_table->primary_init_nfields = o_table->nfields + 1;	/* + ctid field */

	for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
	{
		OTableIndex	   *index = &o_table->indices[ix_num];
		if (index->type != oIndexPrimary)
			index->nfields -= index->npkeyfields;
		index->npkeyfields = 0;
	}

	old_descr = o_fetch_table_descr(old_o_table->oids);

	o_fill_tmp_table_descr(&tmp_descr, o_table);
	rebuild_indices(old_o_table, old_descr, o_table, &tmp_descr);
	for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
	{
		o_update_pg_atttributes(o_table, old_o_table, ix_num, &tmp_descr);
	}
	/* Make the updated catalog row versions visible */
	CommandCounterIncrement();
	o_free_tmp_table_descr(&tmp_descr);

	recreate_o_table(old_o_table, o_table);

	LWLockRelease(&checkpoint_state->oTablesAddLock);

}

static void
drop_secondary_index(OTable *o_table, OIndexNumber ix_num)
{
	CommitSeqNo csn;
	OXid		oxid;
	ORelOids	deletedOids;

	Assert(o_table->indices[ix_num].type != oIndexInvalid);

	deletedOids = o_table->indices[ix_num].oids;
	o_table->nindices--;
	if (o_table->nindices > 0)
	{
		memmove(&o_table->indices[ix_num],
				&o_table->indices[ix_num + 1],
				(o_table->nindices - ix_num) * sizeof(OTableIndex));
	}

	/* update o_table */
	fill_current_oxid_csn(&oxid, &csn);
	o_tables_update(o_table, oxid, csn);
	add_undo_drop_relnode(o_table->oids, &deletedOids, 1);
	recreate_table_descr_by_oids(o_table->oids);
}

void
o_index_drop(Relation tbl, OIndexNumber ix_num)
{
	ORelOids	oids = {MyDatabaseId, tbl->rd_rel->oid,
	tbl->rd_node.relNode};
	OTable	   *o_table;

	o_table = o_tables_get(oids);

	if (o_table == NULL)
	{
		elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
			 (unsigned) oids.datoid, (unsigned) oids.reloid, (unsigned) oids.relnode);
	}

	if (o_table->indices[ix_num].type == oIndexPrimary)
		drop_primary_index(tbl, o_table);
	else
		drop_secondary_index(o_table, ix_num);
	o_table_free(o_table);

}

OIndexNumber
o_find_ix_num_by_name(OTableDescr *descr, char *ix_name)
{
	OIndexNumber result = InvalidIndexNumber;
	int			i;

	for (i = 0; i < descr->nIndices; i++)
	{
		if (strcmp(descr->indices[i]->name.data, ix_name) == 0)
		{
			result = i;
			break;
		}
	}
	return result;
}
