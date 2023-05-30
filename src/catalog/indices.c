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
}			NewColumnValue;

bool		in_indexes_rebuild = false;

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
	o_table_fill_oids(oTable, rel, &RelGetNode(rel));
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
	int			nattrs;
	ORelOids	oids;
	OIndexType	ix_type;
	static ODefineIndexContext context;
	OTable	   *o_table;
	bool		reuse = OidIsValid(IndexStmtGetOldNode(stmt));

	ORelOidsSetFromRel(oids, rel);
	*arg = &context;

	context.oldNode = IndexStmtGetOldNode(stmt);

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

		if (stmt->excludeOpNames)
			elog(ERROR, "exclusion indices are not supported.");

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
				int			nattrs_max = 0,
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
			List	   *allIndexParams;
			List	   *indexColNames;

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

		/* check index fields */
		o_validate_index_elements(o_table, ix_type,
								  stmt->indexParams, stmt->whereClause);
	}
}

void
rebuild_indices_insert_placeholders(OTableDescr *descr)
{
	int			i;

	for (i = 0; i < descr->nIndices; i++)
		o_insert_shared_root_placeholder(descr->indices[i]->desc.oids.datoid,
										 descr->indices[i]->desc.oids.relnode);
	o_insert_shared_root_placeholder(descr->toast->desc.oids.datoid,
									 descr->toast->desc.oids.relnode);
}

/*--
 * We build indices in three phases.
 *
 * 1. Update meta-information in system trees and insert shared root info
 *    placeholders for new trees (under oTablesMetaLock).
 * 2. Write the new trees data.
 * 3. Insert new tree headers and delete place holders (under oTablesMetaLock).
 *
 * So, checkpointer can't reach the new tree until the meta-information is
 * complete.  Also checkpoint numer in tree headers is valid.
 */
void
o_define_index(Relation rel, Oid indoid, bool reindex,
			   bool skip_constraint_checks, bool skip_build,
			   ODefineIndexContext *context)
{
	Relation	index_rel;
	OTable	   *old_o_table = NULL;
	OTable	   *new_o_table;
	OTable	   *o_table;
	OIndexNumber ix_num;
	OTableIndex *index;
	OTableDescr *old_descr = NULL,
			   *descr = NULL;
	bool		reuse = false;
	bool		is_build = false;
	ORelOids	oids;
	OIndexType	ix_type;
	OCompress	compress = InvalidOCompress;
	int16		indnatts;
	int16		indnkeyatts;
	OBTOptions *options;

	if (!OidIsValid(indoid))
		return;

	ORelOidsSetFromRel(oids, rel);

	index_rel = index_open(indoid, AccessShareLock);
	if (context)
		reuse = OidIsValid(context->oldNode);

	options = (OBTOptions *) index_rel->rd_options;

	if (options)
	{
		if (options->compress_offset > 0)
		{
			char	   *str;

			str = (char *) (((Pointer) options) + options->compress_offset);
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
			int			i;

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
				ORelOidsSetFromRel(oids, rel);
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
				o_table->has_primary = true;
				o_table->primary_init_nfields = o_table->nfields;
				ix_num = 0;		/* place first */
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
		int			i;

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
#if PG_VERSION_NUM >= 150000
		index->nulls_not_distinct = index_rel->rd_index->indnullsnotdistinct;
#endif
		o_table_fill_index(o_table, ix_num, index_rel);
	}

	index_close(index_rel, AccessShareLock);

	index->oids.datoid = MyDatabaseId;
	index->oids.reloid = indoid;

	is_build = is_build && !skip_build;

	o_tables_meta_lock();

	o_opclass_cache_add_table(o_table);
	custom_types_add_all(o_table, index);
	if (!reuse && index->type == oIndexPrimary)
	{
		CommitSeqNo csn;
		OXid		oxid;

		Assert(old_o_table);
		old_descr = o_fetch_table_descr(old_o_table->oids);
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

	descr = o_fetch_table_descr(o_table->oids);

	if (!reuse && is_build)
	{
		if (index->type == oIndexPrimary)
			rebuild_indices_insert_placeholders(descr);
		else
			o_insert_shared_root_placeholder(index->oids.datoid,
											 index->oids.relnode);
	}

	if (reindex)
	{
		o_invalidate_oids(index->oids);
		o_add_invalidate_undo_item(index->oids, O_INVALIDATE_OIDS_ON_ABORT);
	}

	if (!reuse && is_build)
	{
		ORelOids	invalidOids = {InvalidOid, InvalidOid, InvalidOid};

		o_tables_meta_unlock(invalidOids, InvalidOid);

		if (index->type == oIndexPrimary)
		{
			Assert(old_o_table);
			rebuild_indices(old_o_table, old_descr, o_table, descr);
		}
		else
		{
			build_secondary_index(o_table, descr, ix_num);
		}
	}
	else
	{
		o_tables_meta_unlock(o_table->oids,
							 (index->type == oIndexPrimary) ? old_o_table->oids.relnode : InvalidOid);
	}

	if (old_o_table)
		o_table_free(old_o_table);
	if (o_table != old_o_table)
		o_table_free(o_table);
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
			secondaryTup = tts_orioledb_make_secondary_tuple(primarySlot,
															 idx, true);
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
	tuplesort_end(sortstate);

	/*
	 * Write the file header.  We need to write the correct checkpoint number,
	 * meta lock will prevent checkpointer from walking through.  Remove
	 * shared root info placeholder to let checkpointer process this tree when
	 * we release the lock.
	 */
	o_tables_meta_lock();

	btree_write_file_header(&idx->desc, &fileHeader);
	o_drop_shared_root_info(idx->desc.oids.datoid,
							idx->desc.oids.relnode);

	o_tables_meta_unlock(o_table->oids, InvalidOid);

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

void
rebuild_indices(OTable *old_o_table, OTableDescr *old_descr,
				OTable *o_table, OTableDescr *descr)
{
	BTreeIterator *iter;
	OIndexDescr *primary,
			   *idx;
	Tuplesortstate **sortstates;
	Tuplesortstate *toastSortState;
	TupleTableSlot *primarySlot;
	int			i;
	Relation	tableRelation;
	double		heap_tuples,
			   *index_tuples;
	uint64		ctid;
	CheckpointFileHeader *fileHeaders;
	CheckpointFileHeader toastFileHeader;

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

			idx = descr->indices[i];

			if (!o_is_index_predicate_satisfied(idx, primarySlot,
												idx->econtext))
				continue;

			index_tuples[i]++;

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
		tuplesort_end(sortstates[i]);
	}
	pfree(sortstates);

	tuplesort_performsort(toastSortState);
	btree_write_index_data(&descr->toast->desc, descr->toast->leafTupdesc,
						   toastSortState, 0, &toastFileHeader);
	tuplesort_end(toastSortState);

	/*
	 * Write the file headers.  We need to write the correct checkpoint
	 * number, meta lock will prevent checkpointer from walking through.
	 * Remove shared root info placeholders to let checkpointer process these
	 * trees when we release the lock.
	 */
	o_tables_meta_lock();

	for (i = 0; i < descr->nIndices; i++)
	{
		btree_write_file_header(&descr->indices[i]->desc, &fileHeaders[i]);
		o_drop_shared_root_info(descr->indices[i]->desc.oids.datoid,
								descr->indices[i]->desc.oids.relnode);
	}
	btree_write_file_header(&descr->toast->desc, &toastFileHeader);
	o_drop_shared_root_info(descr->toast->desc.oids.datoid,
							descr->toast->desc.oids.relnode);

	o_tables_meta_unlock(o_table->oids, old_o_table->oids.relnode);

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
	OTable	   *old_o_table;
	OTableDescr *descr;
	OTableDescr *old_descr;
	ORelOids	invalidOids = {InvalidOid, InvalidOid, InvalidOid};

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

	o_tables_meta_lock();
	old_descr = o_fetch_table_descr(old_o_table->oids);
	recreate_o_table(old_o_table, o_table);
	descr = o_fetch_table_descr(o_table->oids);
	rebuild_indices_insert_placeholders(descr);
	o_tables_meta_unlock(invalidOids, InvalidOid);

	rebuild_indices(old_o_table, old_descr, o_table, descr);

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
	o_tables_meta_lock();
	o_tables_update(o_table, oxid, csn);
	o_tables_meta_unlock(o_table->oids, InvalidOid);
	add_undo_drop_relnode(o_table->oids, &deletedOids, 1);
	recreate_table_descr_by_oids(o_table->oids);
}

void
o_index_drop(Relation tbl, OIndexNumber ix_num)
{
	ORelOids	oids;
	OTable	   *o_table;

	ORelOidsSetFromRel(oids, tbl);
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
