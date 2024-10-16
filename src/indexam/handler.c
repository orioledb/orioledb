/*-------------------------------------------------------------------------
 *
 * handler.c
 *		Implementation of index access method handler
 *
 * Copyright (c) 2021-2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/indexam/handler.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/modify.h"
#include "catalog/indices.h"
#include "catalog/o_tables.h"
#include "indexam/handler.h"
#include "tableam/operations.h"
#include "tableam/tree.h"
#include "tuple/slot.h"
#include "utils/compress.h"
#include "utils/planner.h"
#include "utils/stopevent.h"

#include "access/amapi.h"
#include "access/relation.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "tableam/index_scan.h"
#include "utils/fmgroids.h"
#include "utils/index_selfuncs.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include <math.h>

#define DEFAULT_PAGE_CPU_MULTIPLIER 50.0

static IndexBuildResult *orioledb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo);
static void orioledb_ambuildempty(Relation index);
static bool orioledb_aminsert(Relation rel, Datum *values, bool *isnull,
							  Datum tupleid, Relation heapRel,
							  IndexUniqueCheck checkUnique,
							  bool indexUnchanged,
							  IndexInfo *indexInfo);
static bool orioledb_amupdate(Relation rel, bool new_valid, bool old_valid,
							  Datum *values, bool *isnull, Datum tupleid,
							  Datum *valuesOld, bool *isnullOld,
							  Datum oldTupleid,
							  Relation heapRel,
							  IndexUniqueCheck checkUnique,
							  IndexInfo *indexInfo);
static bool orioledb_amdelete(Relation rel,
							  Datum *values, bool *isnull,
							  Datum tupleid,
							  Relation heapRel,
							  IndexInfo *indexInfo);
static IndexBulkDeleteResult *orioledb_ambulkdelete(IndexVacuumInfo *info,
													IndexBulkDeleteResult *stats,
													IndexBulkDeleteCallback callback,
													void *callback_state);
static IndexBulkDeleteResult *orioledb_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
static bool orioledb_amcanreturn(Relation index, int attno);
static void orioledb_amcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
									Cost *indexStartupCost, Cost *indexTotalCost,
									Selectivity *indexSelectivity, double *indexCorrelation,
									double *indexPages);
static bytea *orioledb_amoptions(Datum reloptions, bool validate);
static bool orioledb_amproperty(Oid index_oid, int attno, IndexAMProperty prop,
								const char *propname, bool *res, bool *isnull);
static char *orioledb_ambuildphasename(int64 phasenum);
static bool orioledb_amvalidate(Oid opclassoid);
static void orioledb_amadjustmembers(Oid opfamilyoid, Oid opclassoid,
									 List *operators, List *functions);
static IndexScanDesc orioledb_ambeginscan(Relation rel, int nkeys, int norderbys);
static void orioledb_amrescan(IndexScanDesc scan, ScanKey scankey,
							  int nscankeys, ScanKey orderbys, int norderbys);
static bool orioledb_amgettuple(IndexScanDesc scan, ScanDirection dir);
static int64 orioledb_amgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
static void orioledb_amendscan(IndexScanDesc scan);
static void orioledb_ammarkpos(IndexScanDesc scan);
static void orioledb_amrestrpos(IndexScanDesc scan);
static Size orioledb_amestimateparallelscan(void);
static void orioledb_aminitparallelscan(void *target);
static void orioledb_amparallelrescan(IndexScanDesc scan);

static IndexAmRoutine *
orioledb_get_indexam_handler(void)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	orioledb_check_shmem();

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = true;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = true;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amsummarizing = false;
	amroutine->ammvccaware = true;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = orioledb_ambuild;
	amroutine->ambuildempty = orioledb_ambuildempty;
	amroutine->aminsert = NULL;
	amroutine->aminsertextended = orioledb_aminsert;
	amroutine->amupdate = orioledb_amupdate;
	amroutine->amdelete = orioledb_amdelete;
	amroutine->ambulkdelete = orioledb_ambulkdelete;
	amroutine->amvacuumcleanup = orioledb_amvacuumcleanup;
	amroutine->amcanreturn = orioledb_amcanreturn;
	amroutine->amcostestimate = orioledb_amcostestimate;
	amroutine->amoptions = orioledb_amoptions;
	amroutine->amproperty = orioledb_amproperty;
	amroutine->ambuildphasename = orioledb_ambuildphasename;
	amroutine->amvalidate = orioledb_amvalidate;
	amroutine->amadjustmembers = orioledb_amadjustmembers;
	amroutine->ambeginscan = orioledb_ambeginscan;
	amroutine->amrescan = orioledb_amrescan;
	amroutine->amgettuple = orioledb_amgettuple;
	amroutine->amgetbitmap = orioledb_amgetbitmap;
	amroutine->amendscan = orioledb_amendscan;
	amroutine->ammarkpos = orioledb_ammarkpos;
	amroutine->amrestrpos = orioledb_amrestrpos;
	amroutine->amestimateparallelscan = orioledb_amestimateparallelscan;
	amroutine->aminitparallelscan = orioledb_aminitparallelscan;
	amroutine->amparallelrescan = orioledb_amparallelrescan;

	return amroutine;
}

IndexAmRoutine *
orioledb_indexam_routine_hook(Oid tamoid, Oid amhandler)
{
	static Oid	orioledb_tam_oid = InvalidOid;

	if (tamoid == HEAP_TABLE_AM_OID)
		return NULL;

	if (!OidIsValid(orioledb_tam_oid))
		orioledb_tam_oid = GetSysCacheOid1(AMNAME, Anum_pg_am_oid,
										   CStringGetDatum("orioledb"));

	if (tamoid == orioledb_tam_oid && amhandler == F_BTHANDLER)
		return orioledb_get_indexam_handler();

	return NULL;
}


/* Check if name is used */


IndexBuildResult *
orioledb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	bool		reindex = false;
	IndexBuildResult *result;
	String	   *relname;

	relname = makeString(index->rd_rel->relname.data);
	if (list_member(reindex_list, relname))
	{
		reindex = true;
		reindex_list = list_delete(reindex_list, relname);
	}

	(void) btbuild(heap, index, indexInfo);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = 0.0;
	result->index_tuples = 0.0;

	if (!index->rd_index->indisprimary && !OidIsValid(o_saved_relrewrite))
	{
		ORelOids	tbl_oids;

		ORelOidsSetFromRel(tbl_oids, heap);
		o_define_index_validate(tbl_oids, index, indexInfo, NULL);
		o_define_index(heap, index, InvalidOid, reindex, InvalidIndexNumber, result);
	}

	return result;
}

void
orioledb_ambuildempty(Relation index)
{
	btbuildempty(index);
}

static OBTreeModifyCallbackAction
o_insert_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
				  OXid oxid, OTupleXactInfo xactInfo,
				  BTreeLeafTupleDeletedStatus deleted,
				  UndoLocation location, RowLockMode *lock_mode,
				  BTreeLocationHint *hint, void *arg)
{
	OTableSlot *oslot = (OTableSlot *) arg;

	if (descr->type == oIndexPrimary &&
		XACT_INFO_OXID_IS_CURRENT(xactInfo))
	{
		OIndexDescr *id = (OIndexDescr *) descr->arg;

		o_tuple_set_version(&id->leafSpec, newtup,
							o_tuple_get_version(tup) + 1);
		oslot->tuple = *newtup;
	}
	return OBTreeCallbackActionUpdate;
}

static void
o_report_duplicate(Relation rel, OIndexDescr *id, TupleTableSlot *slot)
{
	bool		is_ctid = id->primaryIsCtid;
	bool		is_primary = id->desc.type == oIndexPrimary;

	if (is_primary && is_ctid)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("ctid index key duplicate.")));
	}
	else
	{
		StringInfo	str = makeStringInfo();
		int			i;

		appendStringInfo(str, "(");
		for (i = 0; i < id->nKeyFields; i++)
		{
			if (i != 0)
				appendStringInfo(str, ", ");
			appendStringInfo(str, "%s",
							 id->nonLeafTupdesc->attrs[i].attname.data);
		}
		appendStringInfo(str, ")=");

		slot_getallattrs(slot);

		appendStringInfo(str, "(");
		for (i = 0; i < id->nUniqueFields; i++)
		{
			Datum		value = slot->tts_values[i];
			bool		isnull = slot->tts_isnull[i];

			if (i != 0)
				appendStringInfo(str, ", ");
			if (isnull)
				appendStringInfo(str, "null");
			else
			{
				Oid			typoutput;
				bool		typisvarlena;
				char	   *res;

				getTypeOutputInfo(id->nonLeafTupdesc->attrs[i].atttypid,
								  &typoutput, &typisvarlena);
				res = OidOutputFunctionCall(typoutput, value);
				appendStringInfo(str, "'%s'", res);
			}
		}
		appendStringInfo(str, ")");

		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("duplicate key value violates unique "
						"constraint \"%s\"", id->name.data),
				 errdetail("Key %s already exists.", str->data),
				 errtableconstraint(rel, id->desc.type == oIndexPrimary ?
									"pk" : "sk")));
	}
}


static void
append_rowid_values(OIndexDescr *id,
					TupleDesc pk_tupdesc, OTupleFixedFormatSpec *pk_spec,
					Datum pkDatum, Datum *values, bool *isnull,
					CommitSeqNo *csn, uint32 *version)
{
	bytea	   *rowid;
	Pointer		p;

	if (!id->primaryIsCtid)
	{
		OTuple		tuple;
		ORowIdAddendumNonCtid *add;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumNonCtid *) p;
		p += MAXALIGN(sizeof(ORowIdAddendumNonCtid));

		tuple.data = p;
		tuple.formatFlags = add->flags;
		*csn = add->csn;
		*version = o_tuple_get_version(tuple);

		if (id->nPrimaryFields < id->nFields)
		{
			int			i;
			int			pk_from;

			pk_from = id->nFields - id->nPrimaryFields;

			/* Amount of index fields checked in o_define_index_validate */
			for (i = 0; i < id->nPrimaryFields; i++)
			{
				AttrNumber	attnum = id->primaryFieldsAttnums[i] - 1;

				if (attnum >= pk_from)
				{
					values[attnum] = o_fastgetattr(tuple, i + 1, pk_tupdesc, pk_spec, &isnull[attnum]);
				}
			}
		}
	}
	else
	{
		ORowIdAddendumCtid *add;
		AttrNumber	attnum = id->nFields - 1;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumCtid *) p;
		*csn = add->csn;
		*version = add->version;
		p += MAXALIGN(sizeof(ORowIdAddendumCtid));

		values[attnum] = PointerGetDatum(p);
		isnull[attnum] = false;
	}
}

static void
detoast_passed_values(OIndexDescr *index_descr, Datum *values, bool *isnull, bool *vfree)
{
	int			i;
	int			pk_from;

	pk_from = index_descr->nFields - index_descr->nPrimaryFields;

	for (i = 0; i < pk_from; i++)
	{
		Form_pg_attribute att = TupleDescAttr(index_descr->nonLeafTupdesc, i);
		Datum		tmp;

		if (!isnull[i] && att->attlen == -1 &&
			VARATT_IS_EXTENDED(values[i]))
		{
			tmp = PointerGetDatum(PG_DETOAST_DATUM(values[i]));
			Assert(values[i] != tmp);
			values[i] = tmp;
			vfree[i] = true;
		}
	}
}

bool
orioledb_aminsert(Relation rel, Datum *values, bool *isnull,
				  Datum tupleid, Relation heapRel,
				  IndexUniqueCheck checkUnique,
				  bool indexUnchanged,
				  IndexInfo *indexInfo)
{
	ORelOids	oids;
	OIndexType	ix_type;
	OIndexDescr *index_descr;
	OTableDescr *descr;
	OIndexNumber ix_num;
	bool		success;
	BTreeModifyCallbackInfo callbackInfo =
	{
		.waitCallback = NULL,
		.modifyDeletedCallback = o_insert_callback,
		.modifyCallback = NULL,
		.needsUndoForSelfCreated = true
	};
	OSnapshot	o_snapshot;
	OXid		oxid;
	TupleTableSlot *slot;
	uint32		version;
	OTuple		tuple;
	int			skipped = 0;
	CommitSeqNo csn;

	if (OidIsValid(rel->rd_rel->relrewrite))
		return true;

	if (rel->rd_index->indisprimary)
		return true;

	ORelOidsSetFromRel(oids, rel);
	if (rel->rd_index->indisprimary)
		ix_type = oIndexPrimary;
	else if (rel->rd_index->indisunique)
		ix_type = oIndexUnique;
	else
		ix_type = oIndexRegular;
	index_descr = o_fetch_index_descr(oids, ix_type, false, NULL);
	Assert(index_descr != NULL);
	descr = o_fetch_table_descr(index_descr->tableOids);
	Assert(descr != NULL);
	/* Find ix_num */
	for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
	{
		OIndexDescr *index;

		index = descr->indices[ix_num];
		if (index->oids.reloid == rel->rd_rel->oid)
			break;
	}
	Assert(ix_num < descr->nIndices);

	/* TODO: Run this only when fields amount differs */
	/* Remove duplicates like we do in orioledb tables */
	for (int copy_from = 0; copy_from < rel->rd_att->natts; copy_from++)
	{
		Form_pg_attribute orig_attr = &rel->rd_att->attrs[copy_from];
		Form_pg_attribute idx_attr;

		if (copy_from - skipped >= index_descr->leafTupdesc->natts)
			break;

		idx_attr = &index_descr->leafTupdesc->attrs[copy_from - skipped];

		if (strncmp(orig_attr->attname.data, idx_attr->attname.data, NAMEDATALEN) == 0)
		{
			if (skipped > 0)
				values[copy_from - skipped] = values[copy_from];
		}
		else
			skipped++;
	}
	append_rowid_values(index_descr,
						GET_PRIMARY(descr)->nonLeafTupdesc,
						&GET_PRIMARY(descr)->nonLeafSpec,
						tupleid, values, isnull,
						&csn, &version);

	tuple = o_form_tuple(index_descr->leafTupdesc, &index_descr->leafSpec, version, values, isnull);
	slot = index_descr->old_leaf_slot;
	tts_orioledb_store_tuple(slot, tuple, descr, csn, ix_num, false, NULL);
	callbackInfo.arg = slot;

	fill_current_oxid_osnapshot(&oxid, &o_snapshot);

	success = (o_tbl_index_insert(descr, descr->indices[ix_num], &tuple, slot,
								  oxid, o_snapshot.csn, &callbackInfo) == OBTreeModifyResultInserted);

	if (!success)
	{
		o_report_duplicate(heapRel, descr->indices[ix_num], slot);
	}
	return success;
}

bool
orioledb_amupdate(Relation rel, bool new_valid, bool old_valid,
				  Datum *values, bool *isnull, Datum tupleid,
				  Datum *valuesOld, bool *isnullOld, Datum oldTupleid,
				  Relation heapRel,
				  IndexUniqueCheck checkUnique,
				  IndexInfo *indexInfo)
{
	OTableModifyResult result;
	ORelOids	oids;
	OIndexType	ix_type;
	OIndexDescr *index_descr;
	OTableDescr *descr;
	OIndexNumber ix_num;
	CommitSeqNo csn;
	OSnapshot	oSnapshot;
	OXid		oxid;
	TupleTableSlot *new_slot;
	TupleTableSlot *old_slot;
	uint32		version;
	OTuple		new_tuple;
	OTuple		old_tuple;
	bool	   *vfree;
	int			i;

	if (rel->rd_index->indisprimary)
		return true;

	ORelOidsSetFromRel(oids, rel);
	if (rel->rd_index->indisprimary)
		ix_type = oIndexPrimary;
	else if (rel->rd_index->indisunique)
		ix_type = oIndexUnique;
	else
		ix_type = oIndexRegular;
	index_descr = o_fetch_index_descr(oids, ix_type, false, NULL);
	Assert(index_descr != NULL);
	descr = o_fetch_table_descr(index_descr->tableOids);
	Assert(descr != NULL);

	/* Find ix_num */
	for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
	{
		OIndexDescr *index;

		index = descr->indices[ix_num];
		if (index->oids.reloid == rel->rd_rel->oid)
			break;
	}
	Assert(ix_num < descr->nIndices);

	append_rowid_values(index_descr,
						GET_PRIMARY(descr)->nonLeafTupdesc,
						&GET_PRIMARY(descr)->nonLeafSpec,
						oldTupleid, valuesOld, isnullOld,
						&csn, &version);
	vfree = palloc0(sizeof(bool) * index_descr->leafTupdesc->natts);
	/* TODO: Probably there is a better way than detoasting here */
	detoast_passed_values(index_descr, valuesOld, isnullOld, vfree);
	old_tuple = o_form_tuple(index_descr->leafTupdesc, &index_descr->leafSpec,
							 version, valuesOld, isnullOld);
	old_slot = index_descr->old_leaf_slot;
	tts_orioledb_store_non_leaf_tuple(old_slot, old_tuple, descr, csn, ix_num, false, NULL);

	append_rowid_values(index_descr,
						GET_PRIMARY(descr)->nonLeafTupdesc,
						&GET_PRIMARY(descr)->nonLeafSpec,
						tupleid, values, isnull,
						&csn, &version);
	new_tuple = o_form_tuple(index_descr->leafTupdesc, &index_descr->leafSpec, version, values, isnull);
	new_slot = index_descr->new_leaf_slot;
	tts_orioledb_store_non_leaf_tuple(new_slot, new_tuple, descr, csn, ix_num, false, NULL);

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	result = o_update_secondary_index(index_descr, ix_num,
									  new_valid, old_valid,
									  new_slot, new_tuple,
									  old_slot, oxid, oSnapshot.csn);

	for (i = 0; i < index_descr->leafTupdesc->natts; i++)
	{
		if (vfree[i])
			pfree(DatumGetPointer(valuesOld[i]));
	}
	pfree(vfree);

	if (!result.success)
	{
		switch (result.action)
		{
			case BTreeOperationUpdate:
				{
					StringInfo	str = makeStringInfo();

					if (result.failedIxNum == PrimaryIndexNumber)
						break;	/* it is ok */

					appendStringInfo(str, "(");
					for (i = 0; i < index_descr->nUniqueFields; i++)
					{
						if (i != 0)
							appendStringInfo(str, ", ");
						if (isnull[i])
							appendStringInfo(str, "null");
						else
						{
							Oid			typoutput;
							bool		typisvarlena;
							char	   *res;

							getTypeOutputInfo(index_descr->leafTupdesc->attrs[i].atttypid,
											  &typoutput, &typisvarlena);
							res = OidOutputFunctionCall(typoutput, valuesOld[i]);
							appendStringInfo(str, "'%s'", res);
						}
					}
					appendStringInfo(str, ")");
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("unable to remove tuple from secondary index in \"%s\"",
									RelationGetRelationName(rel)),
							 errdetail("Unable to remove %s from index \"%s\"",
									   str->data,
									   index_descr->name.data),
							 errtableconstraint(rel, "sk")));
					break;
				}
			case BTreeOperationInsert:
				o_report_duplicate(heapRel, index_descr, new_slot);
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Unsupported BTreeOperationType.")));
				break;
		}
	}

	return result.success;
}
bool
orioledb_amdelete(Relation rel, Datum *values, bool *isnull,
				  Datum tupleid, Relation heapRel, IndexInfo *indexInfo)
{
	OTableModifyResult result;
	ORelOids	oids;
	OIndexType	ix_type;
	OIndexDescr *index_descr;
	OTableDescr *descr;
	OIndexNumber ix_num;
	CommitSeqNo csn;
	OSnapshot	oSnapshot;
	OXid		oxid;
	uint32		version;
	TupleTableSlot *slot;
	OTuple		tuple;
	bool	   *vfree;
	int			i;

	if (rel->rd_index->indisprimary)
		return true;

	ORelOidsSetFromRel(oids, rel);
	if (rel->rd_index->indisprimary)
		ix_type = oIndexPrimary;
	else if (rel->rd_index->indisunique)
		ix_type = oIndexUnique;
	else
		ix_type = oIndexRegular;
	index_descr = o_fetch_index_descr(oids, ix_type, false, NULL);
	Assert(index_descr != NULL);
	descr = o_fetch_table_descr(index_descr->tableOids);
	Assert(descr != NULL);

	/* Find ix_num */
	for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
	{
		OIndexDescr *index;

		index = descr->indices[ix_num];
		if (index->oids.reloid == rel->rd_rel->oid)
			break;
	}
	Assert(ix_num < descr->nIndices);

	slot = index_descr->old_leaf_slot;
	append_rowid_values(index_descr,
						GET_PRIMARY(descr)->nonLeafTupdesc,
						&GET_PRIMARY(descr)->nonLeafSpec,
						tupleid, values, isnull,
						&csn, &version);
	vfree = palloc0(sizeof(bool) * index_descr->nonLeafTupdesc->natts);
	detoast_passed_values(index_descr, values, isnull, vfree);
	tuple = o_form_tuple(index_descr->leafTupdesc, &index_descr->leafSpec,
						 version, values, isnull);
	tts_orioledb_store_tuple(slot, tuple, descr, csn, ix_num, false, NULL);

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	result = o_tbl_index_delete(index_descr, ix_num, slot, oxid, oSnapshot.csn);
	for (i = 0; i < index_descr->nonLeafTupdesc->natts; i++)
	{
		if (vfree[i])
			pfree(DatumGetPointer(values[i]));
	}
	pfree(vfree);

	if (!result.success)
	{
		switch (result.action)
		{
			case BTreeOperationUpdate:
				{
					StringInfo	str = makeStringInfo();

					if (result.failedIxNum == PrimaryIndexNumber)
						break;	/* it is ok */

					appendStringInfo(str, "(");
					for (i = 0; i < index_descr->nUniqueFields; i++)
					{
						if (i != 0)
							appendStringInfo(str, ", ");
						if (isnull[i])
							appendStringInfo(str, "null");
						else
						{
							Oid			typoutput;
							bool		typisvarlena;
							char	   *res;

							getTypeOutputInfo(index_descr->nonLeafTupdesc->attrs[i].atttypid,
											  &typoutput, &typisvarlena);
							res = OidOutputFunctionCall(typoutput, values[i]);
							appendStringInfo(str, "'%s'", res);
						}
					}
					appendStringInfo(str, ")");
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("unable to remove tuple from secondary index in \"%s\"",
									RelationGetRelationName(rel)),
							 errdetail("Unable to remove %s from index \"%s\"",
									   str->data,
									   index_descr->name.data),
							 errtableconstraint(rel, "sk")));
					break;
				}
			case BTreeOperationInsert:
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Unsupported BTreeOperationType.")));
				break;
		}
	}

	return result.success;
}

IndexBulkDeleteResult *
orioledb_ambulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
					  IndexBulkDeleteCallback callback, void *callback_state)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return stats;
}

IndexBulkDeleteResult *
orioledb_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	return stats;
}

bool
orioledb_amcanreturn(Relation index, int attno)
{
	return true;
}

/* TODO: Rewrite to be more orioledb-specific */
void
orioledb_amcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
						Cost *indexStartupCost, Cost *indexTotalCost,
						Selectivity *indexSelectivity, double *indexCorrelation,
						double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	GenericCosts costs = {0};
	Oid			relid;
	AttrNumber	colnum;
	VariableStatData vardata = {0};
	double		numIndexTuples;
	Cost		descentCost;
	List	   *indexBoundQuals;
	int			indexcol;
	bool		eqQualHere;
	bool		found_saop;
	bool		found_is_null_op;
	double		num_sa_scans;
	ListCell   *lc;

	/*
	 * For a btree scan, only leading '=' quals plus inequality quals for the
	 * immediately next attribute contribute to index selectivity (these are
	 * the "boundary quals" that determine the starting and stopping points of
	 * the index scan).  Additional quals can suppress visits to the heap, so
	 * it's OK to count them in indexSelectivity, but they should not count
	 * for estimating numIndexTuples.  So we must examine the given indexquals
	 * to find out which ones count as boundary quals.  We rely on the
	 * knowledge that they are given in index column order.
	 *
	 * For a RowCompareExpr, we consider only the first column, just as
	 * rowcomparesel() does.
	 *
	 * If there's a ScalarArrayOpExpr in the quals, we'll actually perform N
	 * index scans not one, but the ScalarArrayOpExpr's operator can be
	 * considered to act the same as it normally does.
	 */
	indexBoundQuals = NIL;
	indexcol = 0;
	eqQualHere = false;
	found_saop = false;
	found_is_null_op = false;
	num_sa_scans = 1;
	foreach(lc, path->indexclauses)
	{
		IndexClause *iclause = lfirst_node(IndexClause, lc);
		ListCell   *lc2;

		if (indexcol != iclause->indexcol)
		{
			/* Beginning of a new column's quals */
			if (!eqQualHere)
				break;			/* done if no '=' qual for indexcol */
			eqQualHere = false;
			indexcol++;
			if (indexcol != iclause->indexcol)
				break;			/* no quals at all for indexcol */
		}

		/* Examine each indexqual associated with this index clause */
		foreach(lc2, iclause->indexquals)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc2);
			Expr	   *clause = rinfo->clause;
			Oid			clause_op = InvalidOid;
			int			op_strategy;

			if (IsA(clause, OpExpr))
			{
				OpExpr	   *op = (OpExpr *) clause;

				clause_op = op->opno;
			}
			else if (IsA(clause, RowCompareExpr))
			{
				RowCompareExpr *rc = (RowCompareExpr *) clause;

				clause_op = linitial_oid(rc->opnos);
			}
			else if (IsA(clause, ScalarArrayOpExpr))
			{
				ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
				Node	   *other_operand = (Node *) lsecond(saop->args);
				int			alength = estimate_array_length(other_operand);

				clause_op = saop->opno;
				found_saop = true;
				/* count number of SA scans induced by indexBoundQuals only */
				if (alength > 1)
					num_sa_scans *= alength;
			}
			else if (IsA(clause, NullTest))
			{
				NullTest   *nt = (NullTest *) clause;

				if (nt->nulltesttype == IS_NULL)
				{
					found_is_null_op = true;
					/* IS NULL is like = for selectivity purposes */
					eqQualHere = true;
				}
			}
			else
				elog(ERROR, "unsupported indexqual type: %d",
					 (int) nodeTag(clause));

			/* check for equality operator */
			if (OidIsValid(clause_op))
			{
				op_strategy = get_op_opfamily_strategy(clause_op,
													   index->opfamily[indexcol]);
				Assert(op_strategy != 0);	/* not a member of opfamily?? */
				if (op_strategy == BTEqualStrategyNumber)
					eqQualHere = true;
			}

			indexBoundQuals = lappend(indexBoundQuals, rinfo);
		}
	}

	/*
	 * If index is unique and we found an '=' clause for each column, we can
	 * just assume numIndexTuples = 1 and skip the expensive
	 * clauselist_selectivity calculations.  However, a ScalarArrayOp or
	 * NullTest invalidates that theory, even though it sets eqQualHere.
	 */
	if (index->unique &&
		indexcol == index->nkeycolumns - 1 &&
		eqQualHere &&
		!found_saop &&
		!found_is_null_op)
		numIndexTuples = 1.0;
	else
	{
		List	   *selectivityQuals;
		Selectivity btreeSelectivity;

		/*
		 * If the index is partial, AND the index predicate with the
		 * index-bound quals to produce a more accurate idea of the number of
		 * rows covered by the bound conditions.
		 */
		selectivityQuals = add_predicate_to_index_quals(index, indexBoundQuals);

		btreeSelectivity = clauselist_selectivity(root, selectivityQuals,
												  index->rel->relid,
												  JOIN_INNER,
												  NULL);
		numIndexTuples = btreeSelectivity * index->rel->tuples;

		/*
		 * As in genericcostestimate(), we have to adjust for any
		 * ScalarArrayOpExpr quals included in indexBoundQuals, and then round
		 * to integer.
		 */
		numIndexTuples = rint(numIndexTuples / num_sa_scans);
	}

	/*
	 * Now do generic index cost estimation.
	 */
	costs.numIndexTuples = numIndexTuples;

	genericcostestimate(root, path, loop_count, &costs);

	/*
	 * Add a CPU-cost component to represent the costs of initial btree
	 * descent.  We don't charge any I/O cost for touching upper btree levels,
	 * since they tend to stay in cache, but we still have to do about log2(N)
	 * comparisons to descend a btree of N leaf tuples.  We charge one
	 * cpu_operator_cost per comparison.
	 *
	 * If there are ScalarArrayOpExprs, charge this once per SA scan.  The
	 * ones after the first one are not startup cost so far as the overall
	 * plan is concerned, so add them only to "total" cost.
	 */
	if (index->tuples > 1)		/* avoid computing log(0) */
	{
		descentCost = ceil(log(index->tuples) / log(2.0)) * cpu_operator_cost;
		costs.indexStartupCost += descentCost;
		costs.indexTotalCost += costs.num_sa_scans * descentCost;
	}

	/*
	 * Even though we're not charging I/O cost for touching upper btree pages,
	 * it's still reasonable to charge some CPU cost per page descended
	 * through.  Moreover, if we had no such charge at all, bloated indexes
	 * would appear to have the same search cost as unbloated ones, at least
	 * in cases where only a single leaf page is expected to be visited.  This
	 * cost is somewhat arbitrarily set at 50x cpu_operator_cost per page
	 * touched.  The number of such pages is btree tree height plus one (ie,
	 * we charge for the leaf page too).  As above, charge once per SA scan.
	 */
	descentCost = (index->tree_height + 1) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
	costs.indexStartupCost += descentCost;
	costs.indexTotalCost += costs.num_sa_scans * descentCost;

	/*
	 * If we can get an estimate of the first column's ordering correlation C
	 * from pg_statistic, estimate the index correlation as C for a
	 * single-column index, or C * 0.75 for multiple columns. (The idea here
	 * is that multiple columns dilute the importance of the first column's
	 * ordering, but don't negate it entirely.  Before 8.0 we divided the
	 * correlation by the number of columns, but that seems too strong.)
	 */
	if (index->indexkeys[0] != 0)
	{
		/* Simple variable --- look to stats for the underlying table */
		RangeTblEntry *rte = planner_rt_fetch(index->rel->relid, root);

		Assert(rte->rtekind == RTE_RELATION);
		relid = rte->relid;
		Assert(relid != InvalidOid);
		colnum = index->indexkeys[0];

		if (get_relation_stats_hook &&
			(*get_relation_stats_hook) (root, rte, colnum, &vardata))
		{
			/*
			 * The hook took control of acquiring a stats tuple.  If it did
			 * supply a tuple, it'd better have supplied a freefunc.
			 */
			if (HeapTupleIsValid(vardata.statsTuple) &&
				!vardata.freefunc)
				elog(ERROR, "no function provided to release variable stats with");
		}
		else
		{
			vardata.statsTuple = SearchSysCache3(STATRELATTINH,
												 ObjectIdGetDatum(relid),
												 Int16GetDatum(colnum),
												 BoolGetDatum(rte->inh));
			vardata.freefunc = ReleaseSysCache;
		}
	}
	else
	{
		/* Expression --- maybe there are stats for the index itself */
		relid = index->indexoid;
		colnum = 1;

		if (get_index_stats_hook &&
			(*get_index_stats_hook) (root, relid, colnum, &vardata))
		{
			/*
			 * The hook took control of acquiring a stats tuple.  If it did
			 * supply a tuple, it'd better have supplied a freefunc.
			 */
			if (HeapTupleIsValid(vardata.statsTuple) &&
				!vardata.freefunc)
				elog(ERROR, "no function provided to release variable stats with");
		}
		else
		{
			vardata.statsTuple = SearchSysCache3(STATRELATTINH,
												 ObjectIdGetDatum(relid),
												 Int16GetDatum(colnum),
												 BoolGetDatum(false));
			vardata.freefunc = ReleaseSysCache;
		}
	}

	if (HeapTupleIsValid(vardata.statsTuple))
	{
		Oid			sortop;
		AttStatsSlot sslot;

		sortop = get_opfamily_member(index->opfamily[0],
									 index->opcintype[0],
									 index->opcintype[0],
									 BTLessStrategyNumber);
		if (OidIsValid(sortop) &&
			get_attstatsslot(&sslot, vardata.statsTuple,
							 STATISTIC_KIND_CORRELATION, sortop,
							 ATTSTATSSLOT_NUMBERS))
		{
			double		varCorrelation;

			Assert(sslot.nnumbers == 1);
			varCorrelation = sslot.numbers[0];

			if (index->reverse_sort[0])
				varCorrelation = -varCorrelation;

			if (index->nkeycolumns > 1)
				costs.indexCorrelation = varCorrelation * 0.75;
			else
				costs.indexCorrelation = varCorrelation;

			free_attstatsslot(&sslot);
		}
	}

	ReleaseVariableStats(vardata);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

static void
validate_index_compress(const char *value)
{
	if (value)
		validate_compress(o_parse_compress(value), "Index");
}

bytea *
orioledb_amoptions(Datum reloptions, bool validate)
{
	static bool relopts_set = false;
	static local_relopts relopts = {0};

	if (!relopts_set)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		init_local_reloptions(&relopts, sizeof(OBTOptions));

		/* Options from default_reloptions */
		add_local_int_reloption(&relopts, "fillfactor",
								"Packs btree index pages only to "
								"this percentage",
								BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR,
								100,
								offsetof(OBTOptions, bt_options) +
								offsetof(BTOptions, fillfactor));
		add_local_real_reloption(&relopts, "vacuum_cleanup_index_scale_factor",
								 "Deprecated B-Tree parameter.",
								 -1, 0.0, 1e10,
								 offsetof(OBTOptions, bt_options) +
								 offsetof(BTOptions,
										  vacuum_cleanup_index_scale_factor));
		add_local_bool_reloption(&relopts, "deduplicate_items",
								 "Enables \"deduplicate items\" feature for "
								 "this btree index",
								 true,
								 offsetof(OBTOptions, bt_options) +
								 offsetof(BTOptions, deduplicate_items));

		/* Options for orioledb tables */
		add_local_string_reloption(&relopts, "compress",
								   "Compression level of a particular index",
								   NULL, validate_index_compress, NULL,
								   offsetof(OBTOptions, compress_offset));
		MemoryContextSwitchTo(oldcxt);
		relopts_set = true;
	}

	return (bytea *) build_local_reloptions(&relopts, reloptions, validate);
}

bool
orioledb_amproperty(Oid index_oid, int attno, IndexAMProperty prop,
					const char *propname, bool *res, bool *isnull)
{
	switch (prop)
	{
		case AMPROP_RETURNABLE:
			/* answer only for columns, not AM or whole index */
			if (attno == 0)
				return false;
			/* otherwise, btree can always return data */
			*res = true;
			return true;

		default:
			return false;		/* punt to generic code */
	}
}

char *
orioledb_ambuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN:
			return "scanning table";
		case PROGRESS_BTREE_PHASE_PERFORMSORT_1:
			return "sorting live tuples";
		case PROGRESS_BTREE_PHASE_PERFORMSORT_2:
			return "sorting dead tuples";
		case PROGRESS_BTREE_PHASE_LEAF_LOAD:
			return "loading tuples in tree";
		default:
			return NULL;
	}
}

bool
orioledb_amvalidate(Oid opclassoid)
{
	return true;
}

void
orioledb_amadjustmembers(Oid opfamilyoid, Oid opclassoid, List *operators,
						 List *functions)
{
}

IndexScanDesc
orioledb_ambeginscan(Relation rel, int nkeys, int norderbys)
{
	OScanState *o_scan;
	IndexScanDesc scan;
	ORelOids	oids;
	OIndexType	ix_type;
	OIndexDescr *index_descr;
	OTableDescr *descr;
	OIndexNumber ix_num;

	o_scan = (OScanState *) palloc0(sizeof(OScanState));

	/* get the scan */
	scan = btbeginscan(rel, nkeys, norderbys);
	o_scan->scandesc = *scan;
	pfree(scan);

	scan = &o_scan->scandesc;

	scan->parallel_scan = NULL;
	scan->xs_temp_snap = false;
	scan->xs_want_rowid = true;

	ORelOidsSetFromRel(oids, rel);
	if (rel->rd_index->indisprimary)
		ix_type = oIndexPrimary;
	else if (rel->rd_index->indisunique)
		ix_type = oIndexUnique;
	else
		ix_type = oIndexRegular;
	index_descr = o_fetch_index_descr(oids, ix_type, false, NULL);
	Assert(index_descr != NULL);
	descr = o_fetch_table_descr(index_descr->tableOids);
	Assert(descr != NULL);
	/* Find ix_num */
	for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
	{
		OIndexDescr *index;

		index = descr->indices[ix_num];
		if (index->oids.reloid == rel->rd_rel->oid)
			break;
	}
	Assert(ix_num < descr->nIndices);
	o_scan->ixNum = ix_num;

	o_scan->cxt = AllocSetContextCreate(CurrentMemoryContext,
										"orioledb_cs plan data",
										ALLOCSET_DEFAULT_SIZES);
	return scan;
}

void
orioledb_amrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
				  ScanKey orderbys, int norderbys)
{
	OScanState *o_scan = (OScanState *) scan;

	MemoryContextReset(o_scan->cxt);
	o_scan->iterator = NULL;
	o_scan->curKeyRangeIsLoaded = false;
	btrescan(scan, scankey, nscankeys, orderbys, norderbys);
}

static void
fill_hitup(IndexScanDesc scan, OTuple tuple, OTableDescr *descr,
		   CommitSeqNo tupleCsn, BTreeLocationHint *hint)
{
	TupleTableSlot *slot;

	scan->xs_hitupdesc = descr->tupdesc;
	slot = descr->oldTuple;
	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn, PrimaryIndexNumber, false, hint);
	scan->xs_rowid.value = slot_getsysattr(slot, RowIdAttributeNumber, &scan->xs_rowid.isnull);
	scan->xs_hitup = ExecCopySlotHeapTuple(slot);
}

/* TODO: Rewrite */
static void
fill_itup(IndexScanDesc scan, OTuple tuple, OTableDescr *descr,
		  CommitSeqNo tupleCsn, BTreeLocationHint *hint)
{
	OScanState *o_scan = (OScanState *) scan;
	TupleTableSlot *slot;
	bytea	   *rowid;
	OIndexDescr *index_descr = descr->indices[o_scan->ixNum];
	TupleDesc	pk_tupdesc;
	OTupleFixedFormatSpec *pk_spec;
	int			result_size,
				tuple_size;
	Pointer		ptr;

	slot = index_descr->index_slot;
	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn, o_scan->ixNum, false, hint);
	slot_getallattrs(slot);

	/*
	 * moving values from duplicate field places that will be filled during
	 * index_form_tuple
	 */
	if (index_descr->itupdesc->natts > index_descr->leafTupdesc->natts)
	{
		int			skipped = index_descr->itupdesc->natts - index_descr->leafTupdesc->natts;

		for (int copy_to = index_descr->itupdesc->natts - 1; copy_to >= 0; copy_to--)
		{
			Form_pg_attribute idx_attr = &index_descr->itupdesc->attrs[copy_to];
			Form_pg_attribute slot_attr = &index_descr->leafTupdesc->attrs[copy_to - skipped];

			if (strncmp(slot_attr->attname.data, idx_attr->attname.data, NAMEDATALEN) == 0)
			{
				if (skipped == 0)
					break;
				slot->tts_values[copy_to] = slot->tts_values[copy_to - skipped];
				slot->tts_isnull[copy_to] = slot->tts_isnull[copy_to - skipped];
			}
			else
			{
				slot->tts_values[copy_to] = 0;
				slot->tts_isnull[copy_to] = true;
				skipped--;
			}
		}
	}

	if (o_scan->ixNum == PrimaryIndexNumber)
	{
		OIndexDescr *primary = descr->indices[o_scan->ixNum];

		pk_tupdesc = primary->nonLeafTupdesc;
		pk_spec = &primary->nonLeafSpec;
	}
	else
	{
		pk_tupdesc = GET_PRIMARY(descr)->nonLeafTupdesc;
		pk_spec = &GET_PRIMARY(descr)->nonLeafSpec;
	}

	if (index_descr->primaryIsCtid)
	{
		OTableSlot *oslot = (OTableSlot *) slot;
		ORowIdAddendumCtid addCtid;

		addCtid.hint = *hint;
		addCtid.csn = tupleCsn;
		addCtid.version = oslot->version;

		/* Ctid primary key: give hint + tid as rowid */
		result_size = MAXALIGN(VARHDRSZ) +
			MAXALIGN(sizeof(ORowIdAddendumCtid)) +
			sizeof(ItemPointerData);
		rowid = (bytea *) MemoryContextAllocZero(slot->tts_mcxt, result_size);
		SET_VARSIZE(rowid, result_size);
		ptr = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		memcpy(ptr, &addCtid, sizeof(ORowIdAddendumCtid));
		ptr += MAXALIGN(sizeof(ORowIdAddendumCtid));
		memcpy(ptr, &slot->tts_tid, sizeof(ItemPointerData));
	}
	else
	{
		ORowIdAddendumNonCtid addNonCtid;
		Datum	   *rowid_values;
		bool	   *rowid_isnull;
		Datum		temp_rowid_values[2 * INDEX_MAX_KEYS];
		bool		temp_rowid_isnull[2 * INDEX_MAX_KEYS];
		int			i;

		addNonCtid.hint = *hint;
		addNonCtid.csn = tupleCsn;
		addNonCtid.flags = tuple.formatFlags;

		/*
		 * Amount of index fields checked in o_define_index_validate
		 */
		for (i = 0; i < index_descr->nPrimaryFields; i++)
		{
			AttrNumber	attnum = index_descr->primaryFieldsAttnums[i] - 1;

			temp_rowid_values[i] = slot->tts_values[attnum];
			temp_rowid_isnull[i] = slot->tts_isnull[attnum];
		}

		if (o_scan->ixNum == PrimaryIndexNumber)
		{
			rowid_values = slot->tts_values;
			rowid_isnull = slot->tts_isnull;
		}
		else
		{
			rowid_values = temp_rowid_values;
			rowid_isnull = temp_rowid_isnull;
		}

		tuple_size = o_new_tuple_size(pk_tupdesc, pk_spec, NULL, 0, rowid_values, rowid_isnull, NULL);
		result_size = MAXALIGN(VARHDRSZ) + MAXALIGN(sizeof(ORowIdAddendumNonCtid));
		result_size += tuple_size;
		rowid = (bytea *) MemoryContextAllocZero(slot->tts_mcxt, result_size);
		SET_VARSIZE(rowid, result_size);
		ptr = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		memcpy(ptr, &addNonCtid, sizeof(ORowIdAddendumNonCtid));
		tuple.data = ptr + MAXALIGN(sizeof(ORowIdAddendumNonCtid));

		o_tuple_fill(pk_tupdesc, pk_spec, &tuple, tuple_size, NULL,
					 0, rowid_values, rowid_isnull, NULL);
	}

	scan->xs_rowid.isnull = false;
	scan->xs_rowid.value = PointerGetDatum(rowid);

	scan->xs_itupdesc = index_descr->itupdesc;
	scan->xs_itup = index_form_tuple(index_descr->itupdesc, slot->tts_values, slot->tts_isnull);

	ItemPointerCopy(&slot->tts_tid, &scan->xs_itup->t_tid);
}

bool
orioledb_amgettuple(IndexScanDesc scan, ScanDirection dir)
{
	bool		res;
	OScanState *o_scan = (OScanState *) scan;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	OTableDescr *descr;
	OTuple		tuple;
	bool		scan_primary;
	MemoryContext tupleCxt = CurrentMemoryContext;
	BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};
	CommitSeqNo csn;

	o_scan->scanDir = dir;

	if (scan->xs_snapshot->snapshot_type == SNAPSHOT_DIRTY)
		o_scan->o_snapshot = o_in_progress_snapshot;
	else if (scan->xs_snapshot->snapshot_type == SNAPSHOT_NON_VACUUMABLE)
		o_scan->o_snapshot = o_non_deleted_snapshot;
	else
		O_LOAD_SNAPSHOT(&o_scan->o_snapshot, scan->xs_snapshot);

	/* btree indexes are never lossy */
	scan->xs_recheck = false;

	/*
	 * If we have any array keys, initialize them during first call for a
	 * scan.  We can't do this in btrescan because we don't know the scan
	 * direction at that time.
	 */
	if (so->numArrayKeys && !o_scan->curKeyRangeIsLoaded)
	{
		/* punt if we have any unsatisfiable array keys */
		if (so->numArrayKeys < 0)
			return false;

		_bt_start_array_keys(scan, dir);
	}
	if (!o_scan->curKeyRangeIsLoaded)
	{
		_bt_preprocess_keys(scan);
		o_scan->curKeyRange.empty = true;
	}

	descr = relation_get_descr(scan->heapRelation);
	scan_primary = o_scan->ixNum == PrimaryIndexNumber || !scan->xs_want_itup;

	tuple = o_index_scan_getnext(descr, o_scan, &csn, scan_primary,
								 tupleCxt, &hint);

	if (O_TUPLE_IS_NULL(tuple))
	{
		scan->xs_rowid.isnull = true;
		res = false;
	}
	else
	{
		if (scan->xs_want_itup)
			fill_itup(scan, tuple, descr, csn, &hint);
		else
			fill_hitup(scan, tuple, descr, csn, &hint);
		res = true;
	}
	return res;
}

int64
orioledb_amgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	return 0;
}

void
orioledb_amendscan(IndexScanDesc scan)
{
	OScanState *o_scan = (OScanState *) scan;

	STOPEVENT(STOPEVENT_SCAN_END, NULL);

	MemoryContextDelete(o_scan->cxt);
}

void
orioledb_ammarkpos(IndexScanDesc scan)
{
}

void
orioledb_amrestrpos(IndexScanDesc scan)
{
}

Size
orioledb_amestimateparallelscan(void)
{
	return sizeof(uint8);
}

void
orioledb_aminitparallelscan(void *target)
{
}

void
orioledb_amparallelrescan(IndexScanDesc scan)
{
}
