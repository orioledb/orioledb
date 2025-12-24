/*-------------------------------------------------------------------------
 *
 * index_scan.h
 *		Declarations for index scan of OrioleDB table.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/index_scan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_INDEX_SCAN_H__
#define __TABLEAM_INDEX_SCAN_H__

#include "tableam/key_range.h"
#include "tableam/scan.h"

#include "access/sdir.h"

/* maximum size of attribute map for fast index tuple build */
#define MAX_ITUP_ATTR_MAP_SIZE 4

/**
 * FastItupBuildState
 *
 * Describes which "fast" construction strategy (if any) can be used to produce
 * an index tuple (FastItup) for a particular index scan/tuple situation.
 *
 * Values:
 *  - UndefinedFastItupBuildState
 *      State has not been determined yet; callers should treat this as
 *      uninitialized and perform the necessary checks to compute the actual
 *      state before attempting a fast build.
 *
 *  - ZeroCopyIndexTuplePossible
 *      A zero-copy construction path is applicable: the index tuple can be
 *      produced by directly copying data from OTuple representation.
 *		This is the fastest construction strategy.
 *
 *  - MappingIndexTupleBuildPossible
 * 		A mapping-based construction strategy can be used, where the index tuple
 *  	has few attributes and we can build a small mapping table for it.
 * 		Using this table later allows to build index tuples faster
 * 		than the traditional way, but not as fast as zero-copy. 
 *      
 *
 *  - NoFastItupBuildPossible
 *      No fast construction strategy applies; a full, traditional index tuple
 *      build (materializing and copying necessary data) must be used. This is
 *      the most general but typically the least performant path.
 *
 */
typedef enum FastItupBuildState
{
	UndefinedFastItupBuildState = 0,
	ZeroCopyIndexTuplePossible,
	MappingIndexTupleBuildPossible,
	NoFastItupBuildPossible
} FastItupBuildState;

typedef struct OScanState
{
	IndexScanDescData scandesc;
	OIndexNumber ixNum;
	MemoryContext cxt;
	ScanDirection scanDir;
	bool		addJunk;
	/* is only current index can be used in scan */
	bool		onlyCurIx;
	bool		returning;
	bool		curKeyRangeIsLoaded;
	int			numPrefixExactKeys;
	bool		exact;
	OBTreeKeyRange curKeyRange;
	BTreeIterator *iterator;
	List	   *indexQuals;
	/* used only by direct modify functions */
	CmdType		cmd;
	OSnapshot	oSnapshot;

	/* fast index tuple build info */
	FastItupBuildState 	itup_can_zero_copy;	
	/* pre-computed data size for fixed-size indexes */					
	Size 				itup_fixed_data_size;
	/* attribute map for fast index tuples creation */
	uint8				itup_attr_map[MAX_ITUP_ATTR_MAP_SIZE];
} OScanState;

typedef struct OIndexPlanState
{
	OPlanState	o_plan_state;
	OScanState	ostate;
	/* Used only in o_explain_custom_scan */
	List	   *stripped_indexquals;
	bool		onlyCurIx;
	struct ScanKeyData *iss_ScanKeys;
	int			iss_NumScanKeys;
	IndexRuntimeKeyInfo *iss_RuntimeKeys;
	int			iss_NumRuntimeKeys;
	bool		iss_RuntimeKeysReady;
	ExprContext *iss_RuntimeContext;
} OIndexPlanState;

/*
 * iteration code.
 */
extern void init_index_scan_state(OPlanState *o_plan_state, OScanState *ostate, Relation index,
								  ExprContext *econtext, IndexRuntimeKeyInfo **runtimeKeys,
								  int *numRuntimeKeys, ScanKeyData **scanKeys, int *numScanKeys);
extern OTuple o_iterate_index(OIndexDescr *indexDescr, OScanState *ostate,
							  CommitSeqNo *tupleCsn, MemoryContext tupleCxt,
							  BTreeLocationHint *hint);
extern OTuple o_index_scan_getnext(OTableDescr *descr, OScanState *ostate,
								   CommitSeqNo *tupleCsn,
								   bool scan_primary, MemoryContext tupleCxt,
								   BTreeLocationHint *hint);
extern TupleTableSlot *o_exec_fetch(OScanState *ostate, ScanState *ss);
extern bool o_exec_qual(ExprContext *econtext, ExprState *qual,
						TupleTableSlot *slot);
extern TupleTableSlot *o_exec_project(ProjectionInfo *projInfo,
									  ExprContext *econtext,
									  TupleTableSlot *scanTuple,
									  TupleTableSlot *innerTuple);

/* explain analyze */
extern void eanalyze_counters_init(OEACallsCounters *eacc, OTableDescr *descr);
extern void eanalyze_counter_explain(OEACallsCounter *counter, char *label,
									 char *ix_name, ExplainState *es);
extern void eanalyze_counters_explain(OTableDescr *descr,
									  OEACallsCounters *counters,
									  ExplainState *es);

extern int	o_get_num_prefix_exact_keys(ScanKey scankey, int nscankeys);

#endif							/* __TABLEAM_INDEX_SCAN_H__ */
