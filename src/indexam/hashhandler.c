/*-------------------------------------------------------------------------
 *
 * hashhandler.c
 *		Implementation of hash index access method handler
 *
 * Copyright (c) 2021-2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/indexam/hashhandler.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/modify.h"
#include "catalog/indices.h"
#include "catalog/o_tables.h"
#include "indexam/handler.h"
#include "tableam/handler.h"

#include "access/hash.h"
#include "commands/vacuum.h"
#include "utils/index_selfuncs.h"

static IndexBuildResult *orioledb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo);
static bool orioledb_aminsert(Relation rel, Datum *values, bool *isnull,
							  Datum tupleid, Relation heapRel,
							  IndexUniqueCheck checkUnique,
							  bool indexUnchanged,
							  IndexInfo *indexInfo);
static IndexScanDesc orioledb_ambeginscan(Relation rel, int nkeys, int norderbys);

IndexAmRoutine *
orioledb_hash_handler(void)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = HTMaxStrategyNumber;
	amroutine->amsupport = HASHNProcs;
	amroutine->amoptsprocnum = HASHOPTIONS_PROC;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = false;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false;
	amroutine->amcanbuildparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amsummarizing = false;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = INT4OID;

	amroutine->ambuild = orioledb_ambuild;
	amroutine->ambuildempty = hashbuildempty;
	amroutine->aminsert = NULL;
	amroutine->aminsertextended = orioledb_aminsert;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = hashbulkdelete;
	amroutine->amvacuumcleanup = hashvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = hashcostestimate;
	amroutine->amoptions = hashoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = hashvalidate;
	amroutine->amadjustmembers = hashadjustmembers;
	amroutine->ambeginscan = orioledb_ambeginscan;
	amroutine->amrescan = hashrescan;
	amroutine->amgettuple = hashgettuple;
	amroutine->amgetbitmap = hashgetbitmap;
	amroutine->amendscan = hashendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	return amroutine;
}

IndexBuildResult *
orioledb_ambuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	OTableDescr		   *descr;

	descr = relation_get_descr(heap);
	/* During rewrite we are ignoring first ambuild,
	 * because we need descr to exist in orioledb_index_build_range_scan,
	 * but descr for table created later.
	 * So we performing new reindex_index in redefine_indices after descr created.
	 */
	if (descr == NULL)
	{
		result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

		result->heap_tuples = 0.0;
		result->index_tuples = 0.0;

		return result;
	}
	else
		return hashbuild(heap, index, indexInfo);
}

bool
orioledb_aminsert(Relation rel, Datum *values, bool *isnull,
				  Datum tupleid, Relation heapRel,
				  IndexUniqueCheck checkUnique,
				  bool indexUnchanged,
				  IndexInfo *indexInfo)
{
	ORelOids	oids;
	OTableDescr *descr;
	bytea	   *rowid;
	Pointer		p;

	ORelOidsSetFromRel(oids, heapRel);

	descr = o_fetch_table_descr(oids);
	Assert(descr != NULL);

	rowid = DatumGetByteaP(tupleid);
	p = (Pointer) rowid + MAXALIGN(VARHDRSZ);

	if (!GET_PRIMARY(descr)->primaryIsCtid)
	{
		p += MAXALIGN(sizeof(ORowIdAddendumNonCtid));

		tupleid = PointerGetDatum(p);
	}
	else
	{
		p += MAXALIGN(sizeof(ORowIdAddendumCtid));
		p += MAXALIGN(sizeof(ItemPointerData));
		tupleid = PointerGetDatum(p);
	}

	return hashinsert(rel, values, isnull, tupleid, heapRel,
					  checkUnique, indexUnchanged, indexInfo);
}

IndexScanDesc
orioledb_ambeginscan(Relation rel, int nkeys, int norderbys)
{
	o_current_index = rel;
	return hashbeginscan(rel, nkeys, norderbys);
}
