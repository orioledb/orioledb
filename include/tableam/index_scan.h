/*-------------------------------------------------------------------------
 *
 * index_scan.h
 *		Declarations for index scan of OrioleDB table.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
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
