/*-------------------------------------------------------------------------
 *
 * slot.c
 * 		Implementation of orioledb tuple sorting
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tuple/sort.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "tableam/descr.h"
#include "tuple/format.h"
#include "tuple/sort.h"

#include "catalog/pg_collation_d.h"
#include "catalog/pg_opclass_d.h"
#include "utils/tuplesort.h"

typedef struct
{
	OIndexDescr *id;
} OIndexBuildSortArg;

#define COPYTUP(state,stup,tup) ((*(state)->copytup) (state, stup, tup))

static int
comparetup_orioledb_index(const SortTuple *a, const SortTuple *b, Tuplesortstate *state)
{
	SortSupport sortKey = state->sortKeys;
	OTuple		ltup;
	OTuple		rtup;
	TupleDesc	tupDesc;
	bool		equal_hasnull = false;
	int			nkey;
	int32		compare;
	AttrNumber	attno;
	Datum		datum1,
				datum2;
	bool		isnull1,
				isnull2;
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) state->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;


	/* Compare the leading sort key */
	compare = ApplySortComparator(a->datum1, a->isnull1,
								  b->datum1, b->isnull1,
								  sortKey);
	if (compare != 0)
		return compare;

	/* Compare additional sort keys */
	ltup.data = (Pointer) a->tuple;
	ltup.formatFlags = a->flags;
	rtup.data = (Pointer) b->tuple;
	rtup.formatFlags = b->flags;
	tupDesc = state->tupDesc;

	if (sortKey->abbrev_converter)
	{
		attno = sortKey->ssup_attno;

		datum1 = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull1);
		datum2 = o_fastgetattr(rtup, attno, tupDesc, spec, &isnull2);

		compare = ApplySortAbbrevFullComparator(datum1, isnull1,
												datum2, isnull2,
												sortKey);
		if (compare != 0)
			return compare;
	}

	/* they are equal, so we only need to examine one null flag */
	if (a->isnull1)
		equal_hasnull = true;

	sortKey++;
	for (nkey = 1; nkey < state->nKeys; nkey++, sortKey++)
	{
		attno = sortKey->ssup_attno;

		datum1 = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull1);
		datum2 = o_fastgetattr(rtup, attno, tupDesc, spec, &isnull2);

		compare = ApplySortComparator(datum1, isnull1,
									  datum2, isnull2,
									  sortKey);
		if (compare != 0)
			return compare;		/* done when we find unequal attributes */

		/* they are equal, so we only need to examine one null flag */
		if (isnull1)
			equal_hasnull = true;
	}

	/* FIXME: all orioledb indexes should be unique */

	/*
	 * If btree has asked us to enforce uniqueness, complain if two equal
	 * tuples are detected (unless there was at least one NULL field).
	 *
	 * It is sufficient to make the test here, because if two tuples are equal
	 * they *must* get compared at some stage of the sort --- otherwise the
	 * sort algorithm wouldn't have checked whether one must appear before the
	 * other.
	 */
	if (state->enforceUnique && !equal_hasnull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("could not create unique index \"%s\"",
						arg->id->name.data),
				 errdetail("Duplicate keys exist.")));
	}

	return 0;
}

static void
copytup_orioledb_index(Tuplesortstate *state, SortTuple *stup, void *tup)
{
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) state->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;

	/*
	 * We expect the passed "tup" to be a TupleTableSlot, and form a tuple
	 * using the exported interface for that.
	 */
	Datum		original;
	OTuple		tuple;

	tuple.data = (Pointer) tup;
	tuple.formatFlags = stup->flags;

	if (GetMemoryChunkContext(tup) == state->tuplecontext)
	{
		stup->tuple = tup;
	}
	else
	{
		int			tupsize;

		tupsize = o_tuple_size(tuple, spec);
		stup->tuple = MemoryContextAlloc(state->tuplecontext, tupsize);
		memcpy(stup->tuple, tup, tupsize);
		tuple.data = (Pointer) stup->tuple;
	}
	state->availMem -= GetMemoryChunkSpace(stup->tuple);

	original = o_fastgetattr(tuple,
							 state->sortKeys[0].ssup_attno,
							 state->tupDesc,
							 spec,
							 &stup->isnull1);

	if (!state->sortKeys->abbrev_converter || stup->isnull1)
	{
		/*
		 * Store ordinary Datum representation, or NULL value.  If there is a
		 * converter it won't expect NULL values, and cost model is not
		 * required to account for NULL, so in that case we avoid calling
		 * converter and just set datum1 to zeroed representation (to be
		 * consistent, and to support cheap inequality tests for NULL
		 * abbreviated keys).
		 */
		stup->datum1 = original;
	}
	else if (!consider_abort_common(state))
	{
		/* Store abbreviated key representation */
		stup->datum1 = state->sortKeys->abbrev_converter(original,
														 state->sortKeys);
	}
	else
	{
		/* Abort abbreviation */
		int			i;

		stup->datum1 = original;

		/*
		 * Set state to be consistent with never trying abbreviation.
		 *
		 * Alter datum1 representation in already-copied tuples, so as to
		 * ensure a consistent representation (current tuple was just
		 * handled).  It does not matter if some dumped tuples are already
		 * sorted on tape, since serialized tuples lack abbreviated keys
		 * (TSS_BUILDRUNS state prevents control reaching here in any case).
		 */
		for (i = 0; i < state->memtupcount; i++)
		{
			SortTuple  *mtup = &state->memtuples[i];

			tuple.data = (Pointer) mtup->tuple;
			tuple.formatFlags = mtup->flags;
			original = o_fastgetattr(tuple,
									 state->sortKeys[0].ssup_attno,
									 state->tupDesc,
									 spec,
									 &stup->isnull1);
		}
	}
}

#if PG_VERSION_NUM >= 150000
#define TAPEDECL LogicalTape *tape
#define TAPEREAD(ptr, len) \
	LogicalTapeReadExact(tape, (ptr), (len))
#define TAPEWRITE(ptr, len) \
	LogicalTapeWrite(tape, (ptr), (len))
#else
#define TAPEDECL int tapenum
#define TAPEREAD(ptr, len) \
	LogicalTapeReadExact(state->tapeset, tapenum, (ptr), (len))
#define TAPEWRITE(ptr, len) \
	LogicalTapeWrite(state->tapeset, tapenum, (ptr), (len))
#endif

static void
writetup_orioledb_index(Tuplesortstate *state, TAPEDECL, SortTuple *stup)
{
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) state->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;
	OTuple		tuple;
	int			tuplen;

	tuple.data = (Pointer) stup->tuple;
	tuple.formatFlags = stup->flags;
	tuplen = o_tuple_size(tuple, spec) + sizeof(int) + 1;

	TAPEWRITE((void *) &tuplen, sizeof(tuplen));
	TAPEWRITE((void *) tuple.data, o_tuple_size(tuple, spec));
	TAPEWRITE((void *) &tuple.formatFlags, 1);
	if (state->randomAccess)	/* need trailing length word? */
		TAPEWRITE((void *) &tuplen, sizeof(tuplen));

	if (!state->slabAllocatorUsed)
	{
		state->availMem += GetMemoryChunkSpace(tuple.data);
		pfree(tuple.data);
	}
}

static void
readtup_orioledb_index(Tuplesortstate *state, SortTuple *stup,
					   TAPEDECL, unsigned int len)
{
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) state->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;
	uint32		tuplen = len - sizeof(int) - 1;
	Pointer		tup = (Pointer) readtup_alloc(state, tuplen);
	OTuple		tuple;

	/* read in the tuple proper */
	TAPEREAD(tup, tuplen);
	TAPEREAD(&stup->flags, 1);
	if (state->randomAccess)	/* need trailing length word? */
		TAPEREAD(&tuplen, sizeof(tuplen));
	stup->tuple = (void *) tup;
	tuple.data = tup;
	tuple.formatFlags = stup->flags;
	/* set up first-column key value */
	stup->datum1 = o_fastgetattr(tuple,
								 state->sortKeys[0].ssup_attno,
								 state->tupDesc,
								 spec,
								 &stup->isnull1);
}

Tuplesortstate *
tuplesort_begin_orioledb_index(OIndexDescr *idx,
							   int workMem,
							   bool randomAccess,
							   SortCoordinate coordinate)
{
	Tuplesortstate *state;
	int			key_fields;
	int			i;
	OIndexBuildSortArg *arg;

	SortSupport sortKeys;

	key_fields = idx->nFields;
	if (idx->unique && ((key_fields - idx->nPrimaryFields) > 0))
		key_fields -= idx->nPrimaryFields;

	sortKeys = (SortSupport) palloc0(key_fields * sizeof(SortSupportData));
	arg = (OIndexBuildSortArg *) palloc0(sizeof(OIndexBuildSortArg));
	arg->id = idx;

	for (i = 0; i < key_fields; i++)
	{
		SortSupport sortKey = &sortKeys[i];

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = idx->fields[i].collation;
		sortKey->ssup_nulls_first = idx->fields[i].nullfirst;
		sortKey->ssup_attno = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple, idx, i + 1);
		sortKey->abbreviate = (i == 0);
		sortKey->ssup_reverse = !idx->fields[i].ascending;
		/* FIXME: no abbrev converter yet */
		o_finish_sort_support_function(idx->fields[i].comparator, sortKey);
	}

	state = tuplesort_begin_custom(idx->leafTupdesc, idx->unique,
								   key_fields, sortKeys,
								   workMem, coordinate, randomAccess,
								   comparetup_orioledb_index,
								   copytup_orioledb_index,
								   writetup_orioledb_index,
								   readtup_orioledb_index,
								   arg);

	return state;
}

Tuplesortstate *
tuplesort_begin_orioledb_toast(OIndexDescr *toast,
							   OIndexDescr *primary,
							   int workMem,
							   bool randomAccess,
							   SortCoordinate coordinate)
{
	Tuplesortstate *state;
	int			key_fields;
	int			i;
	OIndexBuildSortArg *arg;
	OIndexField field;
	SortSupport sortKey;
	SortSupport sortKeys;

	key_fields = primary->nonLeafTupdesc->natts;

	sortKeys = (SortSupport) palloc0((key_fields + 2) * sizeof(SortSupportData));
	arg = (OIndexBuildSortArg *) palloc0(sizeof(OIndexBuildSortArg));
	arg->id = primary;

	for (i = 0; i < key_fields; i++)
	{
		sortKey = &sortKeys[i];
		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = primary->fields[i].collation;
		sortKey->ssup_nulls_first = primary->fields[i].nullfirst;
		sortKey->ssup_attno = primary->nonLeafTupdesc->attrs[i].attnum;
		sortKey->abbreviate = (i == 0);
		sortKey->ssup_reverse = !primary->fields[i].ascending;
		/* FIXME: no abbrev converter yet */
		o_finish_sort_support_function(primary->fields[i].comparator, sortKey);
	}

	field.collation = DEFAULT_COLLATION_OID;

	/* ATTN_POS */
	sortKey = &sortKeys[key_fields];
	sortKey->ssup_cxt = CurrentMemoryContext;
	sortKey->ssup_collation = DEFAULT_COLLATION_OID;
	sortKey->ssup_nulls_first = false;
	sortKey->ssup_attno = key_fields + 1;
	sortKey->abbreviate = false;
	sortKey->ssup_reverse = false;
	oFillFieldOpClassAndComparator(&field, toast->oids.datoid,
								   INT2_BTREE_OPS_OID);
	o_finish_sort_support_function(field.comparator, sortKey);

	/* OFFSET_POS */
	sortKey = &sortKeys[key_fields + 1];
	sortKey->ssup_cxt = CurrentMemoryContext;
	sortKey->ssup_collation = DEFAULT_COLLATION_OID;
	sortKey->ssup_nulls_first = false;
	sortKey->ssup_attno = key_fields + 2;
	sortKey->abbreviate = false;
	sortKey->ssup_reverse = false;
	oFillFieldOpClassAndComparator(&field, toast->oids.datoid,
								   INT4_BTREE_OPS_OID);
	o_finish_sort_support_function(field.comparator, sortKey);

	state = tuplesort_begin_custom(toast->leafTupdesc, true,
								   key_fields + 2, sortKeys,
								   workMem, coordinate, randomAccess,
								   comparetup_orioledb_index,
								   copytup_orioledb_index,
								   writetup_orioledb_index,
								   readtup_orioledb_index,
								   arg);

	return state;
}

void
tuplesort_end_orioledb_index(Tuplesortstate *state)
{
	pfree(state->sortKeys);
	pfree(state->arg);
	tuplesort_end(state);
}

OTuple
tuplesort_getotuple(Tuplesortstate *state, bool forward)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	OTuple		result;
	SortTuple	stup;

	if (!tuplesort_gettuple_common(state, forward, &stup))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	result.data = stup.tuple;
	result.formatFlags = stup.flags;

	return result;
}

void
tuplesort_putotuple(Tuplesortstate *state, OTuple tup)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	/*
	 * Copy the given tuple into memory we control, and decrease availMem.
	 * Then call the common code.
	 */
	stup.flags = tup.formatFlags;
	COPYTUP(state, &stup, (void *) tup.data);

	tuplesort_puttuple_common(state, &stup);

	MemoryContextSwitchTo(oldcontext);
}
