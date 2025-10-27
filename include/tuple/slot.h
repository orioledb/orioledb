/*-------------------------------------------------------------------------
 *
 * slot.h
 * 		Declarations for orioledb tuple slot implementation
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tuple/slot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TUPLE_SLOT_H__
#define __TUPLE_SLOT_H__

#include "postgres.h"
#include "executor/tuptable.h"

#include "tableam/key_range.h"
#include "tuple/format.h"

typedef struct OTableSlot
{
	TupleTableSlot base;

	char	   *data;			/* data for materialized slots */
	char	   *to_toast;
	bool	   *vfree;
	Datum	   *detoasted;
	OTuple		tuple;
	OTableDescr *descr;
	bytea	   *rowid;
	CommitSeqNo csn;
	int			ixnum;
	bool		leafTuple;
	uint32		version;
	OTupleReaderState state;
	BTreeLocationHint hint;
	ItemPointerData bridge_ctid;
} OTableSlot;

#define ORIOLEDB_TO_TOAST_OFF ('\0')
#define ORIOLEDB_TO_TOAST_ON ('y')
#define ORIOLEDB_TO_TOAST_COMPRESSION_TRIED ('c')

extern PGDLLIMPORT const TupleTableSlotOps TTSOpsOrioleDB;

extern void tts_orioledb_detoast(TupleTableSlot *slot);
extern void tts_orioledb_store_tuple(TupleTableSlot *slot, OTuple tuple,
									 OTableDescr *descr, CommitSeqNo csn,
									 int ixnum, bool shouldfree,
									 BTreeLocationHint *hint);
extern void tts_orioledb_store_non_leaf_tuple(TupleTableSlot *slot,
											  OTuple tuple,
											  OTableDescr *descr,
											  CommitSeqNo csn,
											  int ixnum, bool shouldfree,
											  BTreeLocationHint *hint);
extern OTuple tts_orioledb_make_secondary_tuple(TupleTableSlot *slot,
												OIndexDescr *idx,
												bool leaf);
extern void tts_orioledb_fill_key_bound(TupleTableSlot *slot, OIndexDescr *idx,
										OBTreeKeyBound *bound);
extern char *tss_orioledb_print_idx_key(TupleTableSlot *slot, OIndexDescr *id);
extern void appendStringInfoIndexKey(StringInfo str, TupleTableSlot *slot,
									 OIndexDescr *id);
extern char *orioledb_print_idx_key(HeapTuple tuple, OIndexDescr *id);
extern void tts_orioledb_toast(TupleTableSlot *slot, OTableDescr *descr);
extern OTuple tts_orioledb_form_tuple(TupleTableSlot *slot,
									  OTableDescr *descr);
extern OTuple tts_orioledb_form_orphan_tuple(TupleTableSlot *slot,
											 OTableDescr *descr);
extern bool tts_orioledb_insert_toast_values(TupleTableSlot *slot,
											 OTableDescr *descr,
											 OXid oxid, CommitSeqNo csn);
extern void tts_orioledb_toast_sort_add(TupleTableSlot *slot,
										OTableDescr *descr,
										Tuplesortstate *sortstate);
extern bool tts_orioledb_remove_toast_values(TupleTableSlot *slot,
											 OTableDescr *descr,
											 OXid oxid, CommitSeqNo csn);
extern bool tts_orioledb_update_toast_values(TupleTableSlot *oldSlot,
											 TupleTableSlot *newSlot,
											 OTableDescr *descr,
											 OXid oxid, CommitSeqNo csn);
extern bool tts_orioledb_modified(TupleTableSlot *oldSlot,
								  TupleTableSlot *newSlot,
								  Bitmapset *attrs);
extern void tts_orioledb_set_ctid(TupleTableSlot *slot, ItemPointer iptr);
extern Datum o_get_tbl_att(TupleTableSlot *slot, int attnum, bool primaryIsCtid,
						   bool *isnull, Oid *typid);
Datum o_get_idx_expr_att(TupleTableSlot *slot, OIndexDescr *idx,
						 ExprState *exp_state, bool *isnull);

#endif							/* __TUPLE_SLOT_H__ */
