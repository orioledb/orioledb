/*-------------------------------------------------------------------------
 *
 * index_scan.h
 *		Declarations for index scan of OrioleDB table.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
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
	OIndexNumber ixNum;
	MemoryContext cxt;
	ScanDirection scanDir;
	bool		addJunk;
	/* is only current index can be used in scan */
	bool		onlyCurIx;
	bool		returning;
	bool		curKeyRangeIsLoaded;
	bool		exact;
	OBTreeKeyRange curKeyRange;
	BTreeIterator *iterator;
	IndexScanDescData *scandesc;
	List	   *indexQuals;
	/* used only by direct modify functions */
	CmdType		cmd;
	CommitSeqNo csn;
} OScanState;

/*
 * iteration code.
 */
extern void init_index_scan_state(OScanState *ostate, Relation index,
								  ExprContext *econtext);
extern OTuple o_iterate_index(OIndexDescr *indexDescr, OScanState *ostate,
							  CommitSeqNo csn, CommitSeqNo *tupleCsn,
							  MemoryContext tupleCxt, BTreeLocationHint *hint);
extern OTuple o_index_scan_getnext(OTableDescr *descr, OScanState *ostate,
								   CommitSeqNo csn,
								   CommitSeqNo *tupleCsn,
								   bool scan_primary, MemoryContext tupleCxt,
								   BTreeLocationHint *hint);
extern TupleTableSlot *o_exec_fetch(OScanState *ostate, ScanState *ss,
									CommitSeqNo csn);
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

#endif							/* __TABLEAM_INDEX_SCAN_H__ */
