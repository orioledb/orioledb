/*-------------------------------------------------------------------------
 *
 * indices.c
 *		Indices routines
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
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
#include "btree/scan.h"
#include "checkpoint/checkpoint.h"
#include "catalog/indices.h"
#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"
#include "recovery/internal.h"
#include "recovery/wal.h"
#include "tableam/operations.h"
#include "transam/oxid.h"
#include "tuple/slot.h"
#include "tuple/sort.h"
#include "tuple/toast.h"
#include "utils/compress.h"
#include "utils/planner.h"
#include "utils/stopevent.h"
#include "workers/interrupt.h"

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
#include "commands/progress.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_utilcmd.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

/* copied from nbtsort.c with modifications*/

/* Magic numbers for parallel state sharing */
#define PARALLEL_KEY_BTREE_SHARED		UINT64CONST(0xA000000000000001)
#define PARALLEL_KEY_WAL_USAGE			UINT64CONST(0xA000000000000005)
#define PARALLEL_KEY_BUFFER_USAGE		UINT64CONST(0xA000000000000006)
#define PARALLEL_KEY_TUPLESORT			UINT64CONST(0xA000000000000007)
/* up to INDEX_MAX_KEYS tuplesorts following PARALLEL_KEY_TUPLESORT */

/*
 * DISABLE_LEADER_PARTICIPATION disables the leader's participation in
 * parallel index builds.  This may be useful as a debugging aid.
#undef DISABLE_LEADER_PARTICIPATION
 */

/*
 * Status for leader participation in parallel index build.
 * It is inherited from bt_leader unmodified, not joined with oIdxBuildState which
 * is also used only on leader to preserve similarity with parallel btree build code.
 */
typedef struct oIdxLeader
{
	/* parallel context itself */
	ParallelContext *pcxt;

	/* Recovery parallel context of recovery workers */
	ParallelRecoveryContext *recoveryContext;

	/*
	 * nparticipanttuplesorts is the exact number of worker processes
	 * successfully launched, plus one leader process if it participates as a
	 * worker (only DISABLE_LEADER_PARTICIPATION builds avoid leader
	 * participating as a worker).
	 */
	int			nparticipanttuplesorts;

	/*
	 * Leader process convenience pointers to shared state (leader avoids TOC
	 * lookups).
	 *
	 * btshared is the shared state for entire build.  sharedsort is the
	 * shared, tuplesort-managed state passed to each process tuplesort.
	 */
	oIdxShared *btshared;
	Sharedsort **sharedsort;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
} oIdxLeader;

/*
 * Working state for parallel build used only for leader. It is used
 * to fill oIdxShared for workers in shmem or recovery_shmem and
 * a oIdxLeader for leader participation in parallel scan.
 */
typedef struct oIdxBuildState
{
	bool		isunique;
	Relation	heap;
	oIdxSpool  *spool;
	double		reltuples;

	/* Oriole-specific */
	oIdxLeader *btleader;
	void		(*worker_heap_sort_fn) (oIdxSpool *, void *, Sharedsort **, int sortmem, bool progress);
	OIndexNumber ix_num;
	bool		isrebuild;
} oIdxBuildState;

static void _o_index_end_parallel(oIdxLeader *btleader);
static void _o_index_leader_participate_as_worker(oIdxBuildState *buildstate);
static void build_secondary_index_worker_sort(oIdxSpool *btspool, void *btshared,
											  Sharedsort **sharedsort, int sortmem,
											  bool progress);
static void build_secondary_index_worker_heap_scan(OTableDescr *descr, OIndexDescr *idx, ParallelOScanDesc poscan, Tuplesortstate **sortstates, bool progress, double *heap_tuples, double *index_tuples[]);
static void rebuild_indices_worker_sort(oIdxSpool *btspool, void *bt_shared,
										Sharedsort **sharedsort, int sortmem,
										bool progress);
static void rebuild_indices_worker_heap_scan(OTableDescr *old_descr, OTableDescr *descr, ParallelOScanDesc poscan,
											 Tuplesortstate **sortstates, bool progress, double *heap_tuples,
											 double *index_tuples[], uint64 *ctid, uint64 *bridge_ctid);


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
assign_new_oids(OTable *oTable, Relation rel, bool drop_pkey)
{
	Oid			toast_relid;
	ReindexParams params;

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

	if (oTable->index_bridging)
	{
		oTable->bridge_oids.datoid = MyDatabaseId;
		oTable->bridge_oids.relnode = GetNewRelFileNumber(MyDatabaseTableSpace, NULL,
														  rel->rd_rel->relpersistence);
		oTable->bridge_oids.reloid = oTable->bridge_oids.relnode;
	}

	PG_TRY();
	{
		List	   *indexIds;
		char		persistence;
		ListCell   *indexId;

		in_indexes_rebuild = true;
		params.options = 0;
		params.tablespaceOid = InvalidOid;

		/* not using simple reindex_relation here anymore, */
		/* because we hold a lock on relation already */
		indexIds = RelationGetIndexList(rel);

		persistence = rel->rd_rel->relpersistence;

		/* Reindex all the indexes. */
		foreach(indexId, indexIds)
		{
			Oid			indexOid = lfirst_oid(indexId);
			Relation	iRel = index_open(indexOid, AccessExclusiveLock);
			OBTOptions *options = (OBTOptions *) iRel->rd_options;

			if (iRel->rd_rel->relam == BTREE_AM_OID && !(options && !options->orioledb_index))
				RelationSetNewRelfilenode(iRel, persistence);
			index_close(iRel, AccessExclusiveLock);
		}

		/*
		 * If the relation has a secondary toast rel, reindex that too while
		 * we still hold the lock on the main table.
		 */
		if (OidIsValid(toast_relid))
		{
			params.options &= ~(REINDEXOPT_MISSING_OK);
#if PG_VERSION_NUM >= 170000
			reindex_relation(NULL, toast_relid, REINDEX_REL_PROCESS_TOAST, &params);
#else
			reindex_relation(toast_relid, REINDEX_REL_PROCESS_TOAST, &params);
#endif
		}
		RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
	}
	PG_CATCH();
	{
		in_indexes_rebuild = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
	in_indexes_rebuild = false;
	o_table_fill_oids(oTable, rel, &RelGetNode(rel), drop_pkey);
	orioledb_free_rd_amcache(rel);
}

void
recreate_o_table(OTable *old_o_table, OTable *o_table)
{
	OSnapshot	oSnapshot;
	OXid		oxid;
	int			oldTreeOidsNum,
				newTreeOidsNum;
	ORelOids	oldOids,
			   *oldTreeOids,
				newOids,
			   *newTreeOids;

	Assert(old_o_table != NULL && o_table != NULL);

	oldOids = old_o_table->oids;
	newOids = o_table->oids;

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	oldTreeOids = o_table_make_index_oids(old_o_table, &oldTreeOidsNum);
	newTreeOids = o_table_make_index_oids(o_table, &newTreeOidsNum);

	o_tables_drop_by_oids(old_o_table->oids, oxid, oSnapshot.csn);
	o_tables_add(o_table, oxid, oSnapshot.csn);

	add_undo_truncate_relnode(oldOids, oldTreeOids, oldTreeOidsNum,
							  newOids, newTreeOids, newTreeOidsNum);
	pfree(oldTreeOids);
	pfree(newTreeOids);
}

static void
o_validate_index_elements(List *expressions, List *predicate)
{
	o_validate_funcexpr((Node *) predicate, " are supported in "
						"orioledb index predicate");
	o_validate_funcexpr((Node *) expressions, " are supported in "
						"orioledb index expressions");
}

void
o_define_index_validate(ORelOids oids, Relation index, IndexInfo *indexInfo, OTable *o_table)
{
	int			nattrs;
	OIndexType	ix_type;

	if (index->rd_index->indisexclusion)
		elog(ERROR, "exclusion indices are not supported.");

	if (o_table == NULL)
	{
		o_table = o_tables_get(oids);
		if (o_table == NULL)
		{
			elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
				 (unsigned) oids.datoid, (unsigned) oids.reloid,
				 (unsigned) oids.relnode);
		}
	}

	/* check index type */
	if (index->rd_index->indisprimary)
		ix_type = oIndexPrimary;
	else if (index->rd_index->indisunique)
		ix_type = oIndexUnique;
	else
		ix_type = oIndexRegular;

	/* check index fields number */
	nattrs = index->rd_index->indnatts;
	if (ix_type == oIndexPrimary)
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

	/* check index fields */
	o_validate_index_elements(RelationGetIndexExpressions(index),
							  RelationGetIndexPredicate(index));
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

static Jsonb *
index_build_params(OTableIndex *index)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(&state, "datoid", index->oids.datoid);
	jsonb_push_int8_key(&state, "reloid", index->oids.reloid);
	jsonb_push_int8_key(&state, "relnode", index->oids.relnode);
	jsonb_push_string_key(&state, "treeName", index->name.data);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
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
o_define_index(Relation heap, Relation index, Oid indoid, bool reindex,
			   OIndexNumber old_ix_num, bool set_tablespace, IndexBuildResult *result)
{
	OTable	   *old_o_table = NULL;
	OTable	   *new_o_table;
	OTable	   *o_table;
	OIndexNumber ix_num;
	OTableIndex *table_index;
	OTableDescr *old_descr = NULL,
			   *descr = NULL;
	bool		reuse_relnode = old_ix_num != InvalidIndexNumber;
	bool		is_build = false;
	ORelOids	oids;
	OIndexType	ix_type;
	int16		indnatts;
	int16		indnkeyatts;
	Oid			tablespace;
	OCompress	compress = InvalidOCompress;
	uint8		fillfactor = BTREE_DEFAULT_FILLFACTOR;
	OBTOptions *options;

	Assert(index == NULL || !(OidIsValid(indoid)));

	if (OidIsValid(indoid))
		index = index_open(indoid, AccessShareLock);

	ORelOidsSetFromRel(oids, heap);

	Assert(index != NULL);

	options = (OBTOptions *) index->rd_options;

	if (options)
	{
		if (options->compress_offset > 0)
		{
			char	   *str;

			str = (char *) (((Pointer) options) + options->compress_offset);
			if (str)
				compress = o_parse_compress(str);
		}

		fillfactor = options->bt_options.fillfactor;
		Assert(options->orioledb_index);
	}

	if (index->rd_index->indisprimary)
		ix_type = oIndexPrimary;
	else if (index->rd_index->indisunique)
		ix_type = oIndexUnique;
	else
		ix_type = oIndexRegular;

	indnatts = index->rd_index->indnatts;
	indnkeyatts = index->rd_index->indnkeyatts;
	tablespace = index->rd_rel->reltablespace;

	if (OidIsValid(indoid))
		index_close(index, AccessShareLock);

	old_o_table = o_tables_get(oids);
	if (old_o_table == NULL)
	{
		elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
			 (unsigned) oids.datoid, (unsigned) oids.reloid,
			 (unsigned) oids.relnode);
	}
	o_table = old_o_table;

	if (!reuse_relnode)
	{
		if (reindex)
		{
			int			i;

			ix_num = InvalidIndexNumber;
			for (i = 0; i < o_table->nindices; i++)
			{
				if (o_table->indices[i].oids.reloid == index->rd_rel->oid)
					ix_num = i;
			}
			reindex = ix_num != InvalidIndexNumber &&
				ix_num < o_table->nindices;

			o_index_drop(heap, ix_num);

			if (ix_type == oIndexPrimary)
			{
				o_table_free(old_o_table);
				ORelOidsSetFromRel(oids, heap);
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
			is_build = true;
			table_index = &o_table->indices[ix_num];
		}
		else
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
				assign_new_oids(new_o_table, heap, false);
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

			table_index = &o_table->indices[ix_num];

			memset(table_index, 0, sizeof(OTableIndex));

			table_index->type = ix_type;
			table_index->nfields = indnatts;
			table_index->nkeyfields = indnkeyatts;

			if (OCompressIsValid(compress))
				table_index->compress = compress;
			else if (ix_type == oIndexPrimary)
				table_index->compress = o_table->primary_compress;
			else
				table_index->compress = o_table->default_compress;

			table_index->fillfactor = fillfactor;
		}
	}
	else
	{
		ix_num = old_ix_num;
		table_index = &o_table->indices[ix_num];
	}

	if (!reuse_relnode)
		memcpy(&table_index->name, &index->rd_rel->relname,
			   sizeof(NameData));
	table_index->oids.relnode = index->rd_rel->relfilenode;

	/* fill index fields */
	if (!reuse_relnode)
	{
		table_index->nulls_not_distinct = index->rd_index->indnullsnotdistinct;
		o_table_fill_index(o_table, ix_num, index);
	}

	table_index->oids.datoid = MyDatabaseId;
	table_index->oids.reloid = index->rd_rel->oid;
	table_index->tablespace = tablespace;

	if (!reuse_relnode && is_build)
		o_tables_table_meta_lock(NULL);
	else
		o_tables_table_meta_lock(o_table);

	o_opclass_cache_add_table(o_table);
	custom_types_add_all(o_table, table_index);
	if (!reuse_relnode && table_index->type == oIndexPrimary)
	{
		Assert(old_o_table);
		old_descr = o_fetch_table_descr(old_o_table->oids);

		recreate_o_table(old_o_table, o_table);
	}
	else
	{
		OSnapshot	oSnapshot;
		OXid		oxid;

		fill_current_oxid_osnapshot(&oxid, &oSnapshot);
		o_tables_update(o_table, oxid, oSnapshot.csn);
		if (!reuse_relnode)
			add_undo_create_relnode(o_table->oids, &table_index->oids, 1);
		recreate_table_descr_by_oids(oids);
	}

	descr = o_fetch_table_descr(o_table->oids);

	if (!reuse_relnode && is_build)
	{
		if (table_index->type == oIndexPrimary)
		{
			if (!set_tablespace)
			{
				o_tablespace_cache_add_table(o_table);
				rebuild_indices_insert_placeholders(descr);
			}
		}
		else if (!set_tablespace)
		{
			o_tablespace_cache_add_relnode(table_index->oids.datoid, table_index->oids.relnode, tablespace);
			o_insert_shared_root_placeholder(table_index->oids.datoid,
											 table_index->oids.relnode);
		}
	}

	if (reindex)
	{
		o_invalidate_oids(table_index->oids);
		o_add_invalidate_undo_item(table_index->oids, O_INVALIDATE_OIDS_ON_ABORT);
	}

	if (!reuse_relnode && is_build)
	{
		o_tables_table_meta_unlock(NULL, InvalidOid);
		if (STOPEVENTS_ENABLED())
		{
			Jsonb	   *params;

			params = index_build_params(table_index);
			STOPEVENT(STOPEVENT_BUILD_INDEX_PLACEHOLDER_INSERTED, params);
		}

		if (table_index->type == oIndexPrimary)
		{
			Assert(old_o_table);
			rebuild_indices(old_o_table, old_descr, o_table, descr, false, result);
		}
		else
		{
			Assert(!is_recovery_in_progress());
			build_secondary_index(o_table, descr, ix_num, false, set_tablespace, result);
		}
	}
	else
	{
		o_tables_table_meta_unlock(o_table, (table_index->type == oIndexPrimary) ?
								   old_o_table->oids.relnode :
								   InvalidOid);
	}

	if (old_o_table)
		o_table_free(old_o_table);
	if (o_table != old_o_table)
		o_table_free(o_table);
}

/*
 * Invoke workers for leader. For non-recovery create parallel context and launch
 * parallel workers. For recovery mode signal the existing Orioledb recovery
 * workers to join as parallel workers.
 *
 * buildstate argument should be initialized (with the exception of the
 * tuplesort state in spools, which may later be created based on shared
 * state initially set up here).
 *
 * request is the target number of parallel worker processes to launch.
 *
 * Sets buildstate's oIdxLeader, which caller must use to shut down parallel
 * mode by passing it to _o_index_end_parallel() at the very end of its index
 * build.  If not even a single worker process can be launched, this is
 * never set, and caller should proceed with a serial index build.
 */
static void
_o_index_begin_parallel(oIdxBuildState *buildstate, bool isconcurrent, int request)
{
	ParallelContext *pcxt = NULL;
	ParallelRecoveryContext *recoveryContext;
	int			nworkers;
	shm_toc_estimator *estimator;
	shm_toc    *toc;
	dsm_segment *seg;
	int			scantuplesortstates;
	Size		estbtshared;
	Size		estsort = 0;
	oIdxShared *btshared;
	Sharedsort **sharedsort;
	oIdxSpool  *btspool = buildstate->spool;
	oIdxLeader *btleader = (oIdxLeader *) palloc0(sizeof(oIdxLeader));
	WalUsage   *walusage = NULL;
	BufferUsage *bufferusage = NULL;
	bool		leaderparticipates = true;
	int			o_table_size = 0;
	Pointer		o_table_serialized;
	int			old_o_table_size = 0;
	Pointer		old_o_table_serialized = NULL;
	int			i;
	int			nallindices = buildstate->spool->descr->nIndices + 1;
	bool		in_recovery = is_recovery_in_progress();
#ifdef DISABLE_LEADER_PARTICIPATION
	leaderparticipates = false;
#endif

	/*
	 * Currently we expect that only client backends and recovery workers
	 * build indexes in parallel.
	 */
	Assert(MyBackendType == B_BACKEND || in_recovery);

	if (btspool->descr->bridge)
		nallindices += 1;

	/* Create parallel context */
	if (!in_recovery)
	{
		/*
		 * Enter parallel mode, and create context for parallel build of btree
		 * index
		 */
		EnterParallelMode();
		Assert(request > 0);
		pcxt = CreateParallelContext("orioledb", "_o_index_parallel_build_main",
									 request);
		estimator = &pcxt->estimator;
		nworkers = pcxt->nworkers;
	}
	else
	{
		recoveryContext = CreateParallelRecoveryContext(*recovery_single_process ?
														0 : (recovery_idx_pool_size_guc - 1));
		estimator = &recoveryContext->estimator;
		nworkers = recoveryContext->nworkers;
	}

	/* Estimate DSM size */
	o_table_serialized = serialize_o_table(btspool->o_table, &o_table_size);
	if (buildstate->isrebuild)	/* Rebuild */
	{
		old_o_table_serialized = serialize_o_table(btspool->old_o_table, &old_o_table_size);
	}

	scantuplesortstates = leaderparticipates ? nworkers + 1 : nworkers;

	/*
	 * Estimate size for our own PARALLEL_KEY_BTREE_SHARED workspace, and
	 * PARALLEL_KEY_TUPLESORT tuplesort workspace
	 */
	/* Calls orioledb_parallelscan_estimate via tableam handler */
	estbtshared = _o_index_parallel_estimate_shared(o_table_size + old_o_table_size);
	shm_toc_estimate_chunk(estimator, estbtshared);
	shm_toc_estimate_keys(estimator, 1);

	estsort = tuplesort_estimate_shared(scantuplesortstates);
	if (buildstate->isrebuild)	/* Rebuild indices */
	{
		/* All indices plus TOAST sort states */
		shm_toc_estimate_chunk(estimator, mul_size(estsort, nallindices));
		shm_toc_estimate_keys(estimator, nallindices);
	}
	else						/* Add secondary index */
	{
		shm_toc_estimate_chunk(estimator, estsort);
		shm_toc_estimate_keys(estimator, 1);
	}

	if (!in_recovery)
	{
		/*
		 * Estimate space for WalUsage and BufferUsage --
		 * PARALLEL_KEY_WAL_USAGE and PARALLEL_KEY_BUFFER_USAGE.
		 *
		 * If there are no extensions loaded that care, we could skip this. We
		 * have no way of knowing whether anyone's looking at pgWalUsage or
		 * pgBufferUsage, so do it unconditionally.
		 */
		shm_toc_estimate_chunk(estimator,
							   mul_size(sizeof(WalUsage), nworkers));
		shm_toc_estimate_keys(estimator, 1);
		shm_toc_estimate_chunk(estimator,
							   mul_size(sizeof(BufferUsage), nworkers));
		shm_toc_estimate_keys(estimator, 1);
	}

	/* Initialize parallel DSM */
	if (!in_recovery)
	{
		InitializeParallelDSM(pcxt);
		/* If no DSM segment was available, back out (do serial build) */
		if (pcxt->seg == NULL)
		{
			pfree(o_table_serialized);
			if (old_o_table_serialized != NULL)
				pfree(old_o_table_serialized);
			pfree(btleader);

			DestroyParallelContext(pcxt);
			ExitParallelMode();
			return;
		}

		toc = pcxt->toc;
		seg = pcxt->seg;
	}
	else
	{
		InitializeParallelRecoveryDSM(recoveryContext);
		/* If no DSM segment was available, back out (do serial build) */
		if (recoveryContext->seg == NULL)
		{
			pfree(o_table_serialized);
			if (old_o_table_serialized != NULL)
				pfree(old_o_table_serialized);
			pfree(btleader);

			DestroyParallelRecoveryContext(recoveryContext);
			return;
		}

		toc = recoveryContext->toc;
		seg = recoveryContext->seg;
	}

	/* Put shared data into DSM */
	btshared = (oIdxShared *) shm_toc_allocate(toc, estbtshared);

	memmove(&btshared->o_table_serialized, o_table_serialized, o_table_size);
	if (buildstate->isrebuild)
	{
		Assert(old_o_table_serialized);
		memmove(((Pointer) &btshared->o_table_serialized) + o_table_size,
				old_o_table_serialized, old_o_table_size);

		sharedsort = (Sharedsort **) palloc0(sizeof(Sharedsort *) * nallindices);
		for (i = 0; i < nallindices; i++)
		{
			sharedsort[i] = (Sharedsort *) shm_toc_allocate(toc, estsort);
		}
	}
	else
	{
		sharedsort = (Sharedsort **) palloc0(sizeof(Sharedsort *));
		sharedsort[0] = (Sharedsort *) shm_toc_allocate(toc, estsort);
	}

	pfree(o_table_serialized);
	if (old_o_table_serialized != NULL)
		pfree(old_o_table_serialized);

	Assert((buildstate->isrebuild && buildstate->ix_num == InvalidIndexNumber) || (!buildstate->isrebuild && buildstate->ix_num != InvalidIndexNumber));
	/* Initialize immutable state */
	btshared->isunique = btspool->isunique;
	btshared->isconcurrent = isconcurrent;
	btshared->isrebuild = buildstate->isrebuild;
	btshared->ix_num = buildstate->ix_num;
	btshared->scantuplesortstates = scantuplesortstates;
	btshared->worker_heap_sort_fn = buildstate->worker_heap_sort_fn;
	btshared->o_table_size = o_table_size;
	btshared->old_o_table_size = old_o_table_size;

	/* Initialize mutable state */
	ConditionVariableInit(&btshared->workersdonecv);
	SpinLockInit(&btshared->mutex);
	if (in_recovery)
		ConditionVariableInit(&btshared->recoveryjoinedcv);
	btshared->nrecoveryworkersjoined = 0;
	btshared->nparticipantsdone = 0;
	btshared->reltuples = 0.0;
	memset(btshared->indtuples, 0, INDEX_MAX_KEYS * sizeof(double));
	orioledb_parallelscan_initialize_inner((ParallelTableScanDesc) &(btshared->poscan));

	shm_toc_insert(toc, PARALLEL_KEY_BTREE_SHARED, btshared);

	/*
	 * Store shared tuplesort-private state, for which we reserved space.
	 * Then, initialize opaque state using tuplesort routine.
	 */
	if (buildstate->isrebuild)
	{
		for (i = 0; i < nallindices; i++)
		{
			tuplesort_initialize_shared(sharedsort[i], scantuplesortstates, seg);
			shm_toc_insert(toc, PARALLEL_KEY_TUPLESORT + i, sharedsort[i]);
		}
	}
	else						/* Add secondary index */
	{
		tuplesort_initialize_shared(sharedsort[0], scantuplesortstates, seg);
		shm_toc_insert(toc, PARALLEL_KEY_TUPLESORT, sharedsort[0]);
	}

	if (!in_recovery)
	{
		/*
		 * Allocate space for each worker's WalUsage and BufferUsage; no need
		 * to initialize.
		 */
		walusage = shm_toc_allocate(pcxt->toc,
									mul_size(sizeof(WalUsage), pcxt->nworkers));
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, walusage);
		bufferusage = shm_toc_allocate(pcxt->toc,
									   mul_size(sizeof(BufferUsage), pcxt->nworkers));
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage);

		/* Launch workers, saving status for leader/caller */
		LaunchParallelWorkers(pcxt);
		btleader->pcxt = pcxt;
		btleader->nparticipanttuplesorts = leaderparticipates ? pcxt->nworkers_launched + 1 : pcxt->nworkers_launched;

		elog(DEBUG1, "parallel index build launched %d workers", pcxt->nworkers_launched);
	}
	else
	{
		btleader->recoveryContext = recoveryContext;
		btleader->nparticipanttuplesorts = btshared->scantuplesortstates;

		if (recoveryContext->nworkers != 0)
			recovery_send_worker_oids(dsm_segment_handle(recoveryContext->seg));

		elog(DEBUG1, "parallel index build uses %d recovery workers", recoveryContext->nworkers);
	}

	btleader->btshared = btshared;
	btleader->sharedsort = sharedsort;
	btleader->walusage = walusage;
	btleader->bufferusage = bufferusage;

	/* If no workers were successfully launched, back out (do serial build) */
	if (!in_recovery)
	{
		if (pcxt->nworkers_launched == 0)
		{
			_o_index_end_parallel(btleader);
			pfree(btleader);
			return;
		}
	}
	else
	{
		if (recoveryContext->nworkers == 0)
		{
			_o_index_end_parallel(btleader);
			pfree(btleader);
			return;
		}
	}

	/* Save leader state now that it's clear build will be parallel */
	buildstate->btleader = btleader;

	/* Join heap scan ourselves */
	if (leaderparticipates)
		_o_index_leader_participate_as_worker(buildstate);

	Assert(in_recovery == is_recovery_in_progress());

	/*
	 * Caller needs to wait for all launched workers when we return.  Make
	 * sure that the failure-to-start case will not hang forever.
	 */
	if (!in_recovery)
		WaitForParallelWorkersToAttach(pcxt);
	else
	{
		while (btshared->nrecoveryworkersjoined < btleader->nparticipanttuplesorts)
		{
			o_worker_handle_interrupts();

			/*
			 * We wait on a condition variable that will wake us as soon as
			 * the pause ends, but we use a timeout so we can check the
			 * ShutdownRequestPending periodically too.
			 */
			ConditionVariableTimedSleep(&btshared->recoveryjoinedcv, 1000,
										WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
		}

		ConditionVariableCancelSleep();
	}
}

/*
 * Shut down workers, destroy parallel context, and end parallel mode.
 */
static void
_o_index_end_parallel(oIdxLeader *btleader)
{
	if (!is_recovery_in_progress())
	{
		/* Shutdown worker processes */
		WaitForParallelWorkersToFinish(btleader->pcxt);

		/*
		 * Next, accumulate WAL usage.  (This must wait for the workers to
		 * finish, or we might get incomplete data.)
		 */
		for (int i = 0; i < btleader->pcxt->nworkers_launched; i++)
			InstrAccumParallelQuery(&btleader->bufferusage[i], &btleader->walusage[i]);

		DestroyParallelContext(btleader->pcxt);
		btleader->pcxt = NULL;

		ExitParallelMode();
	}
	else
	{
		DestroyParallelRecoveryContext(btleader->recoveryContext);
		btleader->recoveryContext = NULL;
	}
}

/*
 * Returns size of shared memory required to store state for a parallel
 * OrioleDB index build based on the snapshot its parallel scan will use.
 */
Size
_o_index_parallel_estimate_shared(Size o_table_size)
{
	Size		size = add_size(BUFFERALIGN(sizeof(oIdxShared)), o_table_size);

	/* c.f. shm_toc_allocate as to why BUFFERALIGN is used */
	return size;
}

/*
 * Within leader, wait for end of workers heap scans and sorts.
 *
 * When called, parallel heap scan started by _o_index_begin_parallel() will
 * already be underway within worker processes (when leader participates
 * as a worker, we should end up here just as workers are finishing).
 *
 */
static void
_o_index_parallel_heapscan(oIdxBuildState *buildstate)
{
	oIdxShared *btshared = buildstate->btleader->btshared;
	int			nparticipanttuplesorts;
	bool		in_recovery = is_recovery_in_progress();

	/*
	 * Currently we expect that only client backends and recovery workers
	 * build indexes in parallel.
	 */
	Assert(MyBackendType == B_BACKEND || in_recovery);

	nparticipanttuplesorts = buildstate->btleader->nparticipanttuplesorts;
	for (;;)
	{
		SpinLockAcquire(&btshared->mutex);
		if (btshared->nparticipantsdone == nparticipanttuplesorts)
		{
			SpinLockRelease(&btshared->mutex);
			break;
		}
		SpinLockRelease(&btshared->mutex);

		if (in_recovery)
		{
			o_worker_handle_interrupts();

			/*
			 * We wait on a condition variable that will wake us as soon as
			 * the pause ends, but we use a timeout so we can check the
			 * ShutdownRequestPending periodically too.
			 */
			ConditionVariableTimedSleep(&btshared->workersdonecv, 1000,
										WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
		}
		else
			ConditionVariableSleep(&btshared->workersdonecv,
								   WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}

	ConditionVariableCancelSleep();
}

static void
_o_index_leader_participate_as_worker(oIdxBuildState *buildstate)
{
	oIdxLeader *btleader = buildstate->btleader;
	oIdxSpool  *leaderworker;
	int			sortmem;

	/* Allocate memory and initialize private spool */
	leaderworker = (oIdxSpool *) palloc0(sizeof(oIdxSpool));
	leaderworker->index = buildstate->spool->index;
	leaderworker->isunique = buildstate->spool->isunique;
	leaderworker->o_table = buildstate->spool->o_table;
	leaderworker->descr = buildstate->spool->descr;
	leaderworker->old_o_table = buildstate->spool->old_o_table;
	leaderworker->old_descr = buildstate->spool->old_descr;

	/*
	 * Might as well use reliable figure when doling out maintenance_work_mem
	 * (when requested number of workers were not launched, this will be
	 * somewhat higher than it is for other workers).
	 */
	sortmem = maintenance_work_mem / btleader->nparticipanttuplesorts;

	/* Perform work common to all participants */
	buildstate->worker_heap_sort_fn(leaderworker, btleader->btshared, btleader->sharedsort,
									sortmem, true);

	pfree(leaderworker);
#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("BTREE BUILD (Leader Partial Spool) STATISTICS");
		ResetUsage();
	}
#endif							/* BTREE_BUILD_STATS */
}

/*
 * Wrapper to be called from parallel context when not in recovery.
 */
void
_o_index_parallel_build_main(dsm_segment *seg, shm_toc *toc)
{
	_o_index_parallel_build_inner(seg, toc, NULL, NULL);
}

/*
 * Perform work within a launched parallel process.
 * For recovery attaches to recovery shared memory and gets
 * serialized o_table as an explicit argument.
 */
void
_o_index_parallel_build_inner(dsm_segment *seg, shm_toc *toc,
							  OTable *recovery_o_table, OTable *recovery_old_o_table)
{
	oIdxSpool  *btspool;
	oIdxShared *btshared;
	Sharedsort **sharedsort;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	int			sortmem;
	int			i;
	int			nallindices;


#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
		ResetUsage();
#endif							/* BTREE_BUILD_STATS */

	/* Initialize worker's own spool */
	btspool = (oIdxSpool *) palloc0(sizeof(oIdxSpool));

	/*
	 * btshared and sharedsort are allocated in DSM shared state. btshared is
	 * allocated to contain serialized o_table
	 */
	btshared = shm_toc_lookup(toc, PARALLEL_KEY_BTREE_SHARED, false);
	Assert(btshared->o_table_size > 0);
	btspool->o_table = deserialize_o_table((Pointer) (&btshared->o_table_serialized),
										   btshared->o_table_size);
	if (btshared->isrebuild)
	{
		/*
		 * old_o_table_serialized is placed just after o_table_serialized in
		 * btshared
		 */
		Assert(btshared->old_o_table_size > 0);
		btspool->old_o_table = deserialize_o_table(((Pointer) (&btshared->o_table_serialized)) + btshared->o_table_size,
												   btshared->old_o_table_size);
	}

	if (btshared->isrebuild)
		elog(DEBUG1, "worker joined parallel index rebuild: old table (%u, %u, %u), table (%u, %u, %u)",
			 btspool->old_o_table->oids.datoid, btspool->old_o_table->oids.reloid,
			 btspool->old_o_table->oids.relnode,
			 btspool->o_table->oids.datoid, btspool->o_table->oids.reloid,
			 btspool->o_table->oids.relnode);
	else
		elog(DEBUG1, "worker joined parallel index build: table (%u, %u, %u), ix_num %u",
			 btspool->o_table->oids.datoid, btspool->o_table->oids.reloid,
			 btspool->o_table->oids.relnode, btshared->ix_num);

	btspool->isunique = btshared->isunique;
	btspool->descr = (OTableDescr *) palloc0(sizeof(OTableDescr));
	o_fill_tmp_table_descr(btspool->descr, btspool->o_table);

	if (btshared->isrebuild)
	{
		btspool->old_descr = (OTableDescr *) palloc0(sizeof(OTableDescr));
		o_fill_tmp_table_descr(btspool->old_descr, btspool->old_o_table);
	}

	nallindices = btspool->descr->nIndices + 1;

	if (btspool->descr->bridge)
		nallindices += 1;

	if (btshared->isrebuild)
	{
		sharedsort = (Sharedsort **) palloc0(sizeof(Sharedsort *) * nallindices);
		for (i = 0; i < nallindices; i++)
		{
			sharedsort[i] = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT + i, false);
			tuplesort_attach_shared(sharedsort[i], seg);
		}
	}
	else
	{
		sharedsort = (Sharedsort **) palloc0(sizeof(Sharedsort *));
		sharedsort[0] = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false);
		tuplesort_attach_shared(sharedsort[0], seg);
	}

	/* Prepare to track buffer usage during parallel execution */
	InstrStartParallelQuery();

	/* Perform sorting of spool */
	sortmem = maintenance_work_mem / btshared->scantuplesortstates;
	btshared->worker_heap_sort_fn(btspool, btshared, sharedsort,
								  sortmem, false);

	pfree((void *) sharedsort);

	o_free_tmp_table_descr(btspool->descr);
	pfree(btspool->descr);

	if (btspool->old_descr)
	{
		o_free_tmp_table_descr(btspool->old_descr);
		pfree(btspool->old_descr);
	}

	pfree(btspool);

	if (!is_recovery_in_progress())
	{
		/* Report WAL/buffer usage during parallel execution */
		bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
		walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false);
		InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
							  &walusage[ParallelWorkerNumber]);
	}

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("BTREE BUILD (Worker Partial Spool) STATISTICS");
		ResetUsage();
	}
#endif							/* BTREE_BUILD_STATS */
}

/*
 * Perform a worker's portion of a parallel sort for secondary index build
 *
 * This generates a tuplesort for passed btspool.  All
 * other spool fields should already be set when this is called.
 *
 * sortmem is the amount of working memory to use within each worker,
 * expressed in KBs.
 */
static void
build_secondary_index_worker_sort(oIdxSpool *btspool, void *bt_shared, Sharedsort **sharedsort,
								  int sortmem, bool progress)
{
	SortCoordinate coordinate;
	double	   *indtuples,
				heaptuples;
	oIdxShared *btshared = (oIdxShared *) bt_shared;
	ParallelOScanDesc poscan = &btshared->poscan;
	OTable	   *o_table;
	OIndexDescr *idx;

	indtuples = palloc0(sizeof(double));
	/* Initialize local tuplesort coordination state */
	coordinate = palloc0(sizeof(SortCoordinateData));
	coordinate->isWorker = true;
	coordinate->nParticipants = -1;
	coordinate->sharedsort = sharedsort[0];

	o_table = btspool->o_table;
	idx = btspool->descr->indices[o_table->has_primary ? btshared->ix_num : btshared->ix_num + 1];

	if (is_recovery_in_progress() && !(*recovery_single_process))
	{
		/* Track recovery workers joined parallel operation */
		SpinLockAcquire(&btshared->mutex);
		btshared->nrecoveryworkersjoined++;
		SpinLockRelease(&btshared->mutex);
		ConditionVariableBroadcast(&btshared->recoveryjoinedcv);
	}

	/* Begin "partial" tuplesort */
	btspool->sortstates = palloc0(sizeof(Pointer));
	btspool->sortstates[0] = tuplesort_begin_orioledb_index(idx, work_mem, false, coordinate);

	build_secondary_index_worker_heap_scan(btspool->descr, idx, poscan, btspool->sortstates, progress, &heaptuples, &indtuples);

	/* Execute this worker's part of the sort */
	if (progress)
		pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
									 PROGRESS_BTREE_PHASE_PERFORMSORT_1);
	o_set_syscache_hooks();
	tuplesort_performsort(btspool->sortstates[0]);
	o_unset_syscache_hooks();

	/*
	 * Done.  Record ambuild statistics, and whether we encountered a broken
	 * HOT chain.
	 */
	SpinLockAcquire(&btshared->mutex);
	btshared->nparticipantsdone++;
	elog(DEBUG3, "Worker %d finished scan and local sort", btshared->nparticipantsdone);

	btshared->reltuples += heaptuples;
	btshared->indtuples[0] += indtuples[0];
	SpinLockRelease(&btshared->mutex);

	/* Notify leader */
	ConditionVariableSignal(&btshared->workersdonecv);

	pfree(indtuples);
	/* We can end tuplesorts immediately */
	tuplesort_end(btspool->sortstates[0]);
	pfree(btspool->sortstates);
}

/* Get next tuple and store all its attributes to a slot */
static inline
bool
scan_getnextslot_allattrs(BTreeSeqScan *scan, OTableDescr *descr,
						  TupleTableSlot *slot, double *ntuples)
{
	OTuple		tup;
	BTreeLocationHint hint;
	CommitSeqNo tupleCsn;

	tup = btree_seq_scan_getnext(scan, slot->tts_mcxt, &tupleCsn, &hint);

	if (O_TUPLE_IS_NULL(tup))
		return false;

	tts_orioledb_store_tuple(slot, tup, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot_getallattrs(slot);
	(*ntuples)++;
	return true;
}

/*
 * Make a local heapscan in a worker, in a leader, or sequentially
 * for building secomndary index. Put result into provided sortstate
 */
static void
build_secondary_index_worker_heap_scan(OTableDescr *descr, OIndexDescr *idx,
									   ParallelOScanDesc poscan,
									   Tuplesortstate **sortstates,
									   bool progress, double *heap_tuples,
									   double *index_tuples[])
{
	void	   *sscan;
	TupleTableSlot *primarySlot;

	sscan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, &o_in_progress_snapshot, poscan);
	primarySlot = MakeSingleTupleTableSlot(descr->tupdesc, &TTSOpsOrioleDB);

	*heap_tuples = 0;
	*index_tuples[0] = 0;
	while (scan_getnextslot_allattrs(sscan, descr, primarySlot, heap_tuples))
	{
		OTuple		secondaryTup;

		if (o_is_index_predicate_satisfied(idx, primarySlot, idx->econtext))
		{
			secondaryTup = tts_orioledb_make_secondary_tuple(primarySlot,
															 idx, true);
			(*index_tuples[0])++;

			o_btree_check_size_of_tuple(o_tuple_size(secondaryTup,
													 &idx->leafSpec),
										idx->name.data, true);
			tuplesort_putotuple(sortstates[0], secondaryTup);
			pfree(secondaryTup.data);
		}

		ExecClearTuple(primarySlot);
	}
	ExecDropSingleTupleTableSlot(primarySlot);
	free_btree_seq_scan(sscan);
}

void
build_secondary_index(OTable *o_table, OTableDescr *descr, OIndexNumber ix_num,
					  bool in_dedicated_recovery_worker, bool set_tablespace,
					  IndexBuildResult *result)
{
	Tuplesortstate **sortstates;
	Relation	tableRelation,
				indexRelation = NULL;
	CheckpointFileHeader fileHeader;

	/* Infrastructure for parallel build corresponds to _bt_spools_heapscan */
	oIdxSpool  *btspool = NULL;
	oIdxBuildState buildstate = {0};
	SortCoordinate coordinate = NULL;
	uint64		ctid;
	double		heap_tuples;
	double	   *index_tuples;
	OIndexDescr *idx;

	index_tuples = palloc0(sizeof(double));
	ctid = 1;
	idx = descr->indices[o_table->has_primary ? ix_num : ix_num + 1];
	buildstate.btleader = NULL;

	/* Attempt to launch parallel worker scan when required */
	if (in_dedicated_recovery_worker || (ActiveSnapshotSet() && max_parallel_maintenance_workers > 0))
	{
		btspool = (oIdxSpool *) palloc0(sizeof(oIdxSpool));
		btspool->o_table = o_table;
		btspool->descr = descr;

		buildstate.worker_heap_sort_fn = &build_secondary_index_worker_sort;
		buildstate.ix_num = ix_num;
		buildstate.spool = btspool;
		buildstate.isrebuild = false;

		_o_index_begin_parallel(&buildstate, false, max_parallel_maintenance_workers);
	}

	if (buildstate.btleader)
		elog(DEBUG1, "parallel index build: table (%u, %u, %u), ix_num %u",
			 o_table->oids.datoid, o_table->oids.reloid, o_table->oids.relnode,
			 ix_num);
	else
		elog(DEBUG1, "serial index build: table (%u, %u, %u), ix_num %u",
			 o_table->oids.datoid, o_table->oids.reloid, o_table->oids.relnode,
			 ix_num);

	/*
	 * If parallel build requested and at least one worker process was
	 * successfully launched, set up coordination state
	 */
	if (buildstate.btleader)
	{
		coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
		coordinate->isWorker = false;
		coordinate->nParticipants =
			buildstate.btleader->nparticipanttuplesorts;
		coordinate->sharedsort = buildstate.btleader->sharedsort[0];
	}

	/* Begin serial/leader tuplesort */
	sortstates = (Tuplesortstate **) palloc0(sizeof(Pointer));
	sortstates[0] = tuplesort_begin_orioledb_index(idx, work_mem, false, coordinate);

	/* Fill spool using either serial or parallel heap scan */
	if (!buildstate.btleader)
	{
		/* Serial build */
		build_secondary_index_worker_heap_scan(descr, idx, NULL, sortstates,
											   false, &heap_tuples, &index_tuples);
	}
	else
	{
		/* We are on leader. Wait until workers end their scans */
		_o_index_parallel_heapscan(&buildstate);
		index_tuples[0] = buildstate.btleader->btshared->indtuples[0];
		heap_tuples = buildstate.btleader->btshared->reltuples;
	}

	o_set_syscache_hooks();
	tuplesort_performsort(sortstates[0]);
	o_unset_syscache_hooks();

	btree_write_index_data(&idx->desc, idx->leafTupdesc, sortstates[0],
						   ctid, 0, &fileHeader);
	/* End serial/leader sort */
	tuplesort_end(sortstates[0]);
	pfree((void *) sortstates);

	if (buildstate.btleader)
	{
		pfree(btspool);
		_o_index_end_parallel(buildstate.btleader);
	}

	/*
	 * Write the file header.  We need to write the correct checkpoint number,
	 * meta lock will prevent checkpointer from walking through.  Remove
	 * shared root info placeholder to let checkpointer process this tree when
	 * we release the lock.
	 */
	o_tables_table_meta_lock(o_table);

	btree_write_file_header(&idx->desc, &fileHeader);
	if (!set_tablespace)
		o_drop_shared_root_info(idx->desc.oids.datoid,
								idx->desc.oids.relnode);

	o_tables_table_meta_unlock(o_table, InvalidOid);

	if (result)
	{
		result->heap_tuples = heap_tuples;
		result->index_tuples = index_tuples[0];
	}
	else if (!is_recovery_in_progress())
	{
		tableRelation = table_open(o_table->oids.reloid, AccessExclusiveLock);
		indexRelation = index_open(o_table->indices[ix_num].oids.reloid,
								   AccessExclusiveLock);
		index_update_stats(tableRelation,
						   true,
						   heap_tuples);

		index_update_stats(indexRelation,
						   false,
						   index_tuples[0]);

		/* Make the updated catalog row versions visible */
		CommandCounterIncrement();
		table_close(tableRelation, AccessExclusiveLock);
		index_close(indexRelation, AccessExclusiveLock);
	}

	pfree(index_tuples);
}

/*
 * Perform a worker's portion of a parallel sort for all indexes rebuild
 *
 * This generates a tuplesorts for all indexes for passed btspool.  All
 * other spool fields should already be set when this is called.
 *
 * sortmem is the amount of working memory to use within each worker,
 * expressed in KBs.
 */
static void
rebuild_indices_worker_sort(oIdxSpool *btspool, void *bt_shared,
							Sharedsort **sharedsort, int sortmem,
							bool progress)
{
	SortCoordinate coordinate;
	double	   *indtuples,
				heaptuples;
	oIdxShared *btshared = (oIdxShared *) bt_shared;
	ParallelOScanDesc poscan = &btshared->poscan;
	int			i;
	int			nIndices = btspool->descr->nIndices;
	int			nallindices = nIndices + 1;

	if (btspool->descr->bridge)
		nallindices += 1;

	indtuples = palloc0(sizeof(double) * nIndices);
	coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData) * nallindices);

	/* Initialize local tuplesort coordination states */
	for (i = PrimaryIndexNumber; i < nallindices; i++)
	{
		coordinate[i].isWorker = true;
		coordinate[i].nParticipants = -1;
		coordinate[i].sharedsort = sharedsort[i];
	}

	if (is_recovery_in_progress() && !(*recovery_single_process))
	{
		/* Track recovery workers joined parallel operation */
		SpinLockAcquire(&btshared->mutex);
		btshared->nrecoveryworkersjoined++;
		SpinLockRelease(&btshared->mutex);
		ConditionVariableBroadcast(&btshared->recoveryjoinedcv);
	}

	/* Begin "partial" tuplesorts for all indexes to be rebuilt */
	btspool->sortstates = palloc0(sizeof(Pointer) * nallindices);
	for (i = PrimaryIndexNumber; i < nIndices; i++)
	{
		btspool->sortstates[i] = tuplesort_begin_orioledb_index(btspool->descr->indices[i], work_mem, false, &(coordinate[i]));
	}
	btspool->sortstates[nIndices] = tuplesort_begin_orioledb_toast(btspool->descr->toast,
																   btspool->descr->indices[PrimaryIndexNumber],
																   work_mem, false, &(coordinate[nIndices]));
	if (btspool->descr->bridge)
	{
		btspool->sortstates[nIndices + 1] = tuplesort_begin_orioledb_index(btspool->descr->bridge, work_mem, false, &(coordinate[nIndices + 1]));
	}

	rebuild_indices_worker_heap_scan(btspool->old_descr, btspool->descr,
									 poscan, btspool->sortstates, false,
									 &heaptuples, &indtuples, NULL, NULL);

	/* Execute this worker's part of the sort */
	if (progress)
		pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
									 PROGRESS_BTREE_PHASE_PERFORMSORT_1);
	for (i = PrimaryIndexNumber; i < nallindices; i++)
	{
		tuplesort_performsort(btspool->sortstates[i]);
	}

	/* Done, record statistics. */
	SpinLockAcquire(&btshared->mutex);
	btshared->nparticipantsdone++;
	elog(DEBUG3, "Worker %d finished scan and local sort", btshared->nparticipantsdone);

	btshared->reltuples += heaptuples;
	for (i = PrimaryIndexNumber; i < nIndices; i++)
	{
		btshared->indtuples[i] += indtuples[i];
	}
	SpinLockRelease(&btshared->mutex);

	/* Notify leader */
	ConditionVariableSignal(&btshared->workersdonecv);

	pfree(indtuples);

	/* We can end tuplesorts immediately */
	for (i = PrimaryIndexNumber; i < nallindices; i++)
	{
		tuplesort_end(btspool->sortstates[i]);
	}
	pfree(btspool->sortstates);
}

static void
rebuild_indices_worker_heap_scan(OTableDescr *old_descr, OTableDescr *descr,
								 ParallelOScanDesc poscan, Tuplesortstate **sortstates,
								 bool progress, double *heap_tuples, double *index_tuples[],
								 uint64 *ctid, uint64 *bridge_ctid)
{
	void	   *sscan;
	OIndexDescr *idx;
	int			i;
	TupleTableSlot *primarySlot;

	primarySlot = MakeSingleTupleTableSlot(old_descr->tupdesc, &TTSOpsOrioleDB);


	sscan = make_btree_seq_scan(&GET_PRIMARY(old_descr)->desc, &o_in_progress_snapshot, poscan);

	*heap_tuples = 0;
	for (i = PrimaryIndexNumber; i < descr->nIndices; i++)
	{
		(*index_tuples)[i] = 0;
	}

	while (scan_getnextslot_allattrs(sscan, old_descr, primarySlot, heap_tuples))
	{
		tts_orioledb_detoast(primarySlot);
		tts_orioledb_toast(primarySlot, descr);

		for (i = PrimaryIndexNumber; i < descr->nIndices; i++)
		{
			OTuple		newTup;

			idx = descr->indices[i];

			if (!o_is_index_predicate_satisfied(idx, primarySlot,
												idx->econtext))
				continue;

			(*index_tuples)[i]++;

			if (i == PrimaryIndexNumber)
			{
				if (idx->primaryIsCtid)
				{
					Assert(ctid);
					primarySlot->tts_tid.ip_posid = (OffsetNumber) (*ctid + 1);
					BlockIdSet(&primarySlot->tts_tid.ip_blkid,
							   (uint32) ((*ctid + 1) >> 16));
					(*ctid)++;
				}
				if (idx->bridging && bridge_ctid)
				{
					OTableSlot *o_slot = (OTableSlot *) primarySlot;
					BlockNumber max_block_number = MaxBlockNumber;

					if (BlockNumberIsValid(max_bridge_ctid_blkno))
						max_block_number = max_bridge_ctid_blkno;

					ItemPointerSet(&o_slot->bridge_ctid,
								   (uint32) ((*bridge_ctid) / MaxHeapTuplesPerPage % max_block_number),
								   (OffsetNumber) ((*bridge_ctid) % MaxHeapTuplesPerPage + FirstOffsetNumber));
					(*bridge_ctid)++;
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
			pfree(newTup.data);
		}

		tts_orioledb_toast_sort_add(primarySlot, descr, sortstates[descr->nIndices]);

		if (descr->bridge)
		{
			OTuple		newTup = tts_orioledb_make_secondary_tuple(primarySlot, descr->bridge, true);

			tuplesort_putotuple(sortstates[descr->nIndices + 1], newTup);
			pfree(newTup.data);
		}

		ExecClearTuple(primarySlot);
	}

	ExecDropSingleTupleTableSlot(primarySlot);
	free_btree_seq_scan(sscan);
}

void
rebuild_indices(OTable *old_o_table, OTableDescr *old_descr,
				OTable *o_table, OTableDescr *descr,
				bool in_dedicated_recovery_worker,
				IndexBuildResult *result)
{
	Tuplesortstate **sortstates;
	int			i;
	Relation	tableRelation;
	double		heap_tuples;
	double	   *index_tuples;
	uint64		ctid;
	uint64		bridge_ctid;
	CheckpointFileHeader *fileHeaders;
	oIdxBuildState buildstate = {0};
	oIdxSpool  *btspool = NULL;
	SortCoordinate *coordinate = NULL;
	S3TaskLocation maxLocation = 0,
				location = 0;
	BTreeDescr *old_td;
	BTreeMetaPage *meta;
	int			nallindices = descr->nIndices + 1;

	if (descr->bridge)
		nallindices += 1;

	sortstates = (Tuplesortstate **) palloc(sizeof(Tuplesortstate *) * nallindices);
	fileHeaders = (CheckpointFileHeader *) palloc(sizeof(CheckpointFileHeader) * nallindices);
	coordinate = (SortCoordinate *) palloc0(sizeof(SortCoordinate *) * nallindices);
	index_tuples = (double *) palloc0(sizeof(double) * descr->nIndices);

	ctid = 0;
	old_td = &GET_PRIMARY(old_descr)->desc;
	o_btree_load_shmem(old_td);
	meta = BTREE_GET_META(old_td);
	if (descr->bridge && old_descr->bridge)
		bridge_ctid = pg_atomic_read_u64(&meta->bridge_ctid);
	else
		bridge_ctid = 0;

	buildstate.btleader = NULL;

	/* Attempt to launch parallel worker scan when required */
	if ((in_dedicated_recovery_worker || (ActiveSnapshotSet() && max_parallel_maintenance_workers > 0)) &&
		!descr->indices[PrimaryIndexNumber]->primaryIsCtid &&
		!(descr->bridge && !old_descr->bridge))
	{
		btspool = (oIdxSpool *) palloc0(sizeof(oIdxSpool));
		btspool->o_table = o_table;
		btspool->descr = descr;
		btspool->old_o_table = old_o_table;
		btspool->old_descr = old_descr;

		buildstate.worker_heap_sort_fn = &rebuild_indices_worker_sort;
		buildstate.ix_num = InvalidIndexNumber;
		buildstate.spool = btspool;
		buildstate.isrebuild = true;

		_o_index_begin_parallel(&buildstate, false, max_parallel_maintenance_workers);
	}

	if (buildstate.btleader)
		elog(DEBUG1, "parallel index rebuild: old table (%u, %u, %u), table (%u, %u, %u)",
			 old_o_table->oids.datoid, old_o_table->oids.reloid, old_o_table->oids.relnode,
			 o_table->oids.datoid, o_table->oids.reloid, o_table->oids.relnode);
	else
		elog(DEBUG1, "serial index rebuild: old table (%u, %u, %u), table (%u, %u, %u)",
			 old_o_table->oids.datoid, old_o_table->oids.reloid, old_o_table->oids.relnode,
			 o_table->oids.datoid, o_table->oids.reloid, o_table->oids.relnode);

	/*
	 * If parallel build requested and at least one worker process was
	 * successfully launched, set up coordination state
	 */
	if (buildstate.btleader)
	{
		for (i = PrimaryIndexNumber; i < nallindices; i++)
		{
			coordinate[i] = palloc0(sizeof(SortCoordinateData));
			coordinate[i]->isWorker = false;
			coordinate[i]->nParticipants = buildstate.btleader->nparticipanttuplesorts;
			coordinate[i]->sharedsort = buildstate.btleader->sharedsort[i];
		}
	}

	/* Begin serial/leader tuplesorts */
	for (i = PrimaryIndexNumber; i < descr->nIndices; i++)
	{
		sortstates[i] = tuplesort_begin_orioledb_index(descr->indices[i], work_mem, false, coordinate[i]);
	}

	btree_open_smgr(&descr->toast->desc);
	sortstates[descr->nIndices] = tuplesort_begin_orioledb_toast(descr->toast,
																 descr->indices[PrimaryIndexNumber],
																 work_mem, false, coordinate[descr->nIndices]);

	if (descr->bridge)
	{
		btree_open_smgr(&descr->bridge->desc);
		sortstates[descr->nIndices + 1] = tuplesort_begin_orioledb_index(descr->bridge, work_mem, false, coordinate[descr->nIndices + 1]);
	}

	/* Fill spool using either serial or parallel heap scan */
	if (!buildstate.btleader)
	{
		/* Serial build */
		rebuild_indices_worker_heap_scan(old_descr, descr, NULL, sortstates,
										 false, &heap_tuples, &index_tuples,
										 &ctid,
										 (descr->bridge && !old_descr->bridge) ? &bridge_ctid : NULL);
	}
	else
	{
		/* We are on leader. Wait until workers end their scans */
		_o_index_parallel_heapscan(&buildstate);
		index_tuples[0] = buildstate.btleader->btshared->indtuples[0];
		heap_tuples = buildstate.btleader->btshared->reltuples;
	}

	o_set_syscache_hooks();
	for (i = PrimaryIndexNumber; i < nallindices; i++)
	{
		tuplesort_performsort(sortstates[i]);
		if (i < descr->nIndices)	/* Indices sort states */
		{
			btree_write_index_data(&descr->indices[i]->desc, descr->indices[i]->leafTupdesc,
								   sortstates[i],
								   (i == PrimaryIndexNumber && descr->indices[PrimaryIndexNumber]->primaryIsCtid) ? ctid : 0,
								   (i == PrimaryIndexNumber) ? bridge_ctid : 0,
								   &fileHeaders[i]);
		}
		else if (i == descr->nIndices)	/* TOAST sort state */
		{
			btree_write_index_data(&descr->toast->desc, descr->toast->leafTupdesc,
								   sortstates[descr->nIndices], 0, 0, &fileHeaders[i]);
		}
		else if (i == descr->nIndices + 1 && descr->bridge) /* bridge_index sort
															 * state */
		{
			btree_write_index_data(&descr->bridge->desc, descr->bridge->leafTupdesc,
								   sortstates[descr->nIndices + 1], 0, 0, &fileHeaders[i]);
		}
		tuplesort_end(sortstates[i]);
	}
	o_unset_syscache_hooks();

	pfree((void *) sortstates);

	if (buildstate.btleader)
	{
		pfree(btspool);
		if (!is_recovery_in_progress())
		{
			for (i = 0; i < descr->nIndices; i++)
			{
				if (index_tuples[i] == 0)
					index_tuples[i] = buildstate.btleader->btshared->indtuples[i];
			}
		}
		_o_index_end_parallel(buildstate.btleader);
	}

	/*
	 * Write the file headers.  We need to write the correct checkpoint
	 * number, meta lock will prevent checkpointer from walking through.
	 * Remove shared root info placeholders to let checkpointer process these
	 * trees when we release the lock.
	 */
	o_tables_table_meta_lock(o_table);

	for (i = PrimaryIndexNumber; i < nallindices; i++)
	{
		if (i < descr->nIndices)	/* Indices sort states */
		{
			location = btree_write_file_header(&descr->indices[i]->desc, &fileHeaders[i]);
			o_drop_shared_root_info(descr->indices[i]->desc.oids.datoid,
									descr->indices[i]->desc.oids.relnode);
		}
		else if (i == descr->nIndices)	/* TOAST sort state */
		{
			location = btree_write_file_header(&descr->toast->desc, &fileHeaders[i]);
			o_drop_shared_root_info(descr->toast->desc.oids.datoid,
									descr->toast->desc.oids.relnode);
		}
		else if (i == descr->nIndices + 1 && descr->bridge) /* index_bridge sort
															 * state */
		{
			location = btree_write_file_header(&descr->bridge->desc, &fileHeaders[i]);
			o_drop_shared_root_info(descr->bridge->desc.oids.datoid,
									descr->bridge->desc.oids.relnode);
		}
		maxLocation = Max(maxLocation, location);
	}

	o_tables_table_meta_unlock(o_table, old_o_table->oids.relnode);

	if (orioledb_s3_mode)
		s3_queue_wait_for_location(maxLocation);

	pfree(fileHeaders);

	if (!is_recovery_in_progress())
	{
		tableRelation = table_open(o_table->oids.reloid, AccessExclusiveLock);
		index_update_stats(tableRelation, true, heap_tuples);

		for (i = PrimaryIndexNumber; i < o_table->nindices; i++)
		{
			if (i == 0 && result)
			{
				result->heap_tuples = heap_tuples;
				result->index_tuples = index_tuples[i];
			}
			if (i != 0 || o_table->has_primary)
			{
				OTableIndex *table_index = &o_table->indices[i];
				Relation	indexRelation;

				indexRelation = index_open(table_index->oids.reloid,
										   AccessExclusiveLock);

				index_update_stats(indexRelation, false, index_tuples[i]);
				index_close(indexRelation, AccessExclusiveLock);
			}
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

	Assert(o_table->indices[PrimaryIndexNumber].type == oIndexPrimary);

	old_o_table = o_table;
	o_table = o_tables_get(o_table->oids);
	assign_new_oids(o_table, rel, true);

	memmove(&o_table->indices[0],
			&o_table->indices[1],
			(o_table->nindices - 1) * sizeof(OTableIndex));
	o_table->nindices--;
	o_table->has_primary = false;
	o_table->primary_init_nfields = o_table->nfields + 1;	/* + ctid field */

	o_tables_table_meta_lock(NULL);
	old_descr = o_fetch_table_descr(old_o_table->oids);
	recreate_o_table(old_o_table, o_table);
	descr = o_fetch_table_descr(o_table->oids);
	o_tablespace_cache_add_table(o_table);
	rebuild_indices_insert_placeholders(descr);
	o_tables_table_meta_unlock(NULL, InvalidOid);

	rebuild_indices(old_o_table, old_descr, o_table, descr, false, NULL);

}

static void
drop_secondary_index(OTable *o_table, OIndexNumber ix_num)
{
	OSnapshot	oSnapshot;
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
	fill_current_oxid_osnapshot(&oxid, &oSnapshot);
	o_tables_table_meta_lock(o_table);
	o_tables_update(o_table, oxid, oSnapshot.csn);
	o_tables_table_meta_unlock(o_table, InvalidOid);
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

	Assert(ix_num != InvalidIndexNumber && ix_num < o_table->nindices);
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

	Assert(descr != NULL);

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
