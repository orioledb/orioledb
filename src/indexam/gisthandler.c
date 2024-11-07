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

#include "indexam/handler.h"

#include "access/amapi.h"
#include "access/gistscan.h"
#include "access/gist_private.h"
#include "commands/vacuum.h"
#include "utils/index_selfuncs.h"

IndexAmRoutine *
orioledb_gist_indexam_handler(void)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	orioledb_check_shmem();

	amroutine->amstrategies = 0;
	amroutine->amsupport = GISTNProcs;
	amroutine->amoptsprocnum = GIST_OPTIONS_PROC;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = true;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false;
	amroutine->amcanbuildparallel = false;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amsummarizing = false;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = gistbuild;
	amroutine->ambuildempty = gistbuildempty;
	amroutine->aminsert = NULL;
	amroutine->aminsertextended = gistinsert;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = gistbulkdelete;
	amroutine->amvacuumcleanup = gistvacuumcleanup;
	amroutine->amcanreturn = gistcanreturn;
	amroutine->amcostestimate = gistcostestimate;
	amroutine->amoptions = gistoptions;
	amroutine->amproperty = gistproperty;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = gistvalidate;
	amroutine->amadjustmembers = gistadjustmembers;
	amroutine->ambeginscan = gistbeginscan;
	amroutine->amrescan = gistrescan;
	amroutine->amgettuple = gistgettuple;
	amroutine->amgetbitmap = gistgetbitmap;
	amroutine->amendscan = gistendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	return amroutine;
}
