/*-------------------------------------------------------------------------
 *
 * handler.h
 *		Declarations of table access method handler
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/handler.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_HANDLER_H__
#define __TABLEAM_HANDLER_H__

#include "btree/btree.h"
#include "catalog/o_tables.h"

#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "rewrite/rewriteHandler.h"

extern bool is_orioledb_rel(Relation rel);
extern OIndexNumber find_tree_in_descr(OTableDescr *descr, ORelOids oids);

/* EXPLAIN ANALYZE functions call counter */
typedef struct
{
	uint32		read;			/* o_btree_read_page() */
	uint32		write;			/* write_page() */
	uint32		load;			/* load_page() */
	uint32		lock;			/* lock_page() */
	uint32		evict;			/* evict_page() */
} OEACallsCounter;

#define EA_COUNTERS_NUM (5)		/* number of EXPLAIN ANALYZE counters */

/*
 * EXPLAIN ANALYZE counters for different trees involved in single executor
 * node.
 */
typedef struct
{
	/* Identifiers of table being analyzed */
	ORelOids	oids;
	/* Table descriptor */
	OTableDescr *descr;
	/* Counters for primary and secondary indices */
	int			nindices;
	OEACallsCounter *indices;
	/* Counters for TOAST */
	OEACallsCounter toast;
	/* Counters for indices of other tables */
	OEACallsCounter others;
} OEACallsCounters;

/*
 * EXPLAIN ANALYZE function call counters.
 * will be init and free in tableam_scan.c
 */
extern OEACallsCounters *ea_counters;

/* returns AnalyzeCallsCounter for specified index number */
static inline OEACallsCounter *
get_ea_counters(OrioleDBPageDesc *desc)
{
	OIndexNumber ix_num = find_tree_in_descr(ea_counters->descr, desc->oids);

	if (ix_num == InvalidIndexNumber)
		return &ea_counters->others;
	if (ix_num == TOASTIndexNumber)
		return &ea_counters->toast;
	return &ea_counters->indices[ix_num];
}

/* increases EXPLAIN_ANALYZE counter for o_btree_read_page() call */
#define EA_READ_INC(blkno)  \
	if (ea_counters != NULL)	\
	{	\
		OrioleDBPageDesc *desc = O_GET_IN_MEMORY_PAGEDESC(blkno);	\
		OEACallsCounter *ix_counter = get_ea_counters(desc); \
		if (ix_counter != NULL) \
			ix_counter->read++; \
	}

/* increases EXPLAIN_ANALYZE counter for write_read() call */
#define EA_WRITE_INC(blkno)  \
	if (ea_counters != NULL)	\
	{	\
		OrioleDBPageDesc *desc = O_GET_IN_MEMORY_PAGEDESC(blkno);	\
		OEACallsCounter *ix_counter = get_ea_counters(desc); \
		if (ix_counter != NULL) \
			ix_counter->write++; \
	}

/* increases EXPLAIN_ANALYZE counter for load_page() call */
#define EA_LOAD_INC(blkno)  \
	if (ea_counters != NULL)	\
	{	\
		OrioleDBPageDesc *desc = O_GET_IN_MEMORY_PAGEDESC(blkno);	\
		OEACallsCounter *ix_counter = get_ea_counters(desc); \
		if (ix_counter != NULL) \
			ix_counter->load++; \
	}

/* increases EXPLAIN_ANALYZE counter for lock_page() call */
#define EA_LOCK_INC(blkno)  \
	if (ea_counters != NULL)	\
	{	\
		OrioleDBPageDesc *desc = O_GET_IN_MEMORY_PAGEDESC(blkno);	\
		OEACallsCounter *ix_counter = get_ea_counters(desc); \
		if (ix_counter != NULL) \
			ix_counter->lock++; \
	}

/* increases EXPLAIN_ANALYZE counter for evict_page() call */
#define EA_EVICT_INC(blkno)  \
	if (ea_counters != NULL)	\
	{	\
		OrioleDBPageDesc *desc = O_GET_IN_MEMORY_PAGEDESC(blkno);	\
		OEACallsCounter *ix_counter = get_ea_counters(desc); \
		if (ix_counter != NULL) \
			ix_counter->evict++; \
	}

extern void cleanup_btree(Oid datoid, Oid relnode);
extern bool o_drop_shared_root_info(Oid datoid, Oid relnode);
extern void o_tableam_descr_init(void);
extern void o_invalidate_descrs(Oid datoid, Oid reloid, Oid relfilenode);
extern void init_print_options(BTreePrintOptions *printOptions, VarChar *optionsArg);
extern void orioledb_free_rd_amcache(Relation rel);
extern OTableDescr *relation_get_descr(Relation rel);
extern void table_descr_inc_refcnt(OTableDescr *descr);
extern void table_descr_dec_refcnt(OTableDescr *descr);

extern Size orioledb_parallelscan_estimate(Relation rel);
extern Size orioledb_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);
extern void orioledb_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);


typedef enum
{
	OParallelScanPageInvalid,
	OParallelScanPageValid,
	OParallelScanPageInProgress
} OParallelScanPageStatus;

/*
 * OrioleDB-specific shared state for parallel table scan.
 *
 * Each backend participating in a parallel table scan has its own BTreeSeqScan
 * in its memory, that contains a pointer to ParallelOScanDescData. The
 * information here is sufficient to properly initialize each new BTreeSeqScan
 * as workers join the scan, and to coordiate their scans.
 */

typedef struct BTreeIntPageParallelData
{
	char		img[ORIOLEDB_BLCKSZ];	/* internal page image */
	OFixedShmemKey prevHikey;	/* low key of internal page */
	OffsetNumber offset;
	OffsetNumber startOffset;	/* first offset on internal page */
	OParallelScanPageStatus status;
	int			pageno;			/* debug only */
	CommitSeqNo imgReadCsn;
} BTreeIntPageParallelData;

typedef BTreeIntPageParallelData *BTreeIntPageParallel;

#define O_PARALLEL_LEADER_STARTED		1
#define O_PARALLEL_FIRST_PAGE_LOADED	(1<<1)
#define O_PARALLEL_IS_SINGLE_LEAF_PAGE  (1<<2)
#define O_PARALLEL_CURRENT_PAGE			(1<<3)	/* If set then current
												 * internal page is in
												 * intPage[1], and next
												 * internal page is in
												 * intPage[0]. If not set -
												 * vice versa. */
#define O_PARALLEL_DISK_SCAN_STARTED 	(1<<4)

#define CUR_PAGE(poscan)	(&(poscan)->intPage[((poscan)->flags & O_PARALLEL_CURRENT_PAGE) ? 0 : 1])
#define NEXT_PAGE(poscan)	(&(poscan)->intPage[((poscan)->flags & O_PARALLEL_CURRENT_PAGE) ? 1 : 0])

typedef struct ParallelOScanDescData
{
	ParallelTableScanDescData phs_base; /* Shared AM-independent state for
										 * parallel table scan */
	BTreeIntPageParallelData intPage[2];
	slock_t		intpageAccess,
				workerStart,	/* for sequential workers joining */
				workerBeginDisk;	/* for sequential joining to disk read
									 * phase */
	LWLock		intpageLoad,	/* for sequential internal page loading */
				downlinksSubscribe, /* workers can get disk downlinks from
									 * shared state */
				downlinksPublish;	/* workers can put disk downlinks to
									 * shared state */
	uint64		downlinksCount; /* cumulative number of disk downlinks in all
								 * workers */
	pg_atomic_uint64 downlinkIndex;
	ConditionVariable downlinksCv;
	int			workersReportedCount;	/* number of workers that reported
										 * disk downlinks number */
	bits8		flags;
	int			nworkers;
	dsm_handle	dsmHandle;
	/* debug only */
	int			cur_int_pageno;
	bool		worker_active[10];
} ParallelOScanDescData;

typedef ParallelOScanDescData *ParallelOScanDesc;
#endif
