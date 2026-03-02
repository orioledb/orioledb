/*-------------------------------------------------------------------------
 *
 * sort.c
 * 		Implementation of orioledb tuple sorting
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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
#include "tuple/toast.h"

#include "catalog/pg_collation_d.h"
#include "catalog/pg_opclass_d.h"
#include "utils/tuplesort.h"

typedef struct
{
	TupleDesc	tupDesc;
	OIndexDescr *id;
	bool		enforceUnique;
	bool		hasOxidField;	/* True if tuples have oxid appended at end */
} OIndexBuildSortArg;

static void
write_o_tuple(void *ptr, OTuple tup, int tupsize)
{
	Pointer		p = (Pointer) ptr;

	*((uint8 *) p) = tup.formatFlags;
	p += MAXIMUM_ALIGNOF;
	memcpy(p, tup.data, tupsize);
}

static OTuple
read_o_tuple(void *ptr)
{
	OTuple		tup;
	Pointer		p = (Pointer) ptr;

	tup.formatFlags = *((uint8 *) p);
	p += MAXIMUM_ALIGNOF;
	tup.data = p;

	return tup;
}

static int
comparetup_orioledb_index(const SortTuple *a, const SortTuple *b, Tuplesortstate *state)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	SortSupport sortKey = base->sortKeys;
	OTuple		ltup;
	OTuple		rtup;
	TupleDesc	tupDesc;
	bool		equal_hasnull = false;
	int			nkey;
	int32		compare;
	AttrNumber	attno;
	Datum		datum1 = 0,
				datum2 = 0;
	bool		isnull1 = false,
				isnull2 = false;
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;

	/* Compare the leading sort key */
	compare = ApplySortComparator(a->datum1, a->isnull1,
								  b->datum1, b->isnull1,
								  sortKey);
	if (compare != 0)
		return compare;

	/* Compare additional sort keys */
	ltup = read_o_tuple(a->tuple);
	rtup = read_o_tuple(b->tuple);
	tupDesc = arg->tupDesc;

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
	for (nkey = 1; nkey < base->nKeys; nkey++, sortKey++)
	{
		if (!OIgnoreColumn(arg->id, nkey))
		{
			attno = sortKey->ssup_attno;

			datum1 = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull1);
			datum2 = o_fastgetattr(rtup, attno, tupDesc, spec, &isnull2);

			compare = ApplySortComparator(datum1, isnull1,
										  datum2, isnull2,
										  sortKey);
			if (compare != 0)
				return compare; /* done when we find unequal attributes */

			/* they are equal, so we only need to examine one null flag */
			if (isnull1)
				equal_hasnull = true;
		}
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
	if (arg->enforceUnique && !(!arg->id->nulls_not_distinct && equal_hasnull))
	{
		StringInfo	str = makeStringInfo();
		int			i;

		appendStringInfo(str, "(");
		for (i = 0; i < arg->id->nKeyFields; i++)
		{
			if (i != 0)
				appendStringInfo(str, ", ");
			appendStringInfo(str, "%s",
							 arg->id->nonLeafTupdesc->attrs[i].attname.data);
		}
		appendStringInfo(str, ")=(");
		sortKey = base->sortKeys;
		for (i = 0; i < arg->id->nUniqueFields; i++, sortKey++)
		{
			bool		isnull;
			Datum		value;

			attno = sortKey->ssup_attno;
			value = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull);
			if (i != 0)
				appendStringInfo(str, ", ");
			if (isnull)
				appendStringInfo(str, "null");
			else
			{
				Oid			typoutput;
				bool		typisvarlena;
				char	   *res;

				getTypeOutputInfo(arg->id->nonLeafTupdesc->attrs[i].atttypid,
								  &typoutput, &typisvarlena);
				res = OidOutputFunctionCall(typoutput, value);
				appendStringInfo(str, "'%s'", res);
			}
		}
		appendStringInfo(str, ")");

		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("could not create unique index \"%s\"",
						arg->id->name.data),
				 errdetail("Duplicate key \"%s\" violates unique constraint.", str->data)));
	}

	return 0;
}

static void
writetup_orioledb_index(Tuplesortstate *state, LogicalTape *tape, SortTuple *stup)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;
	OTuple		tuple;
	int			tuplen;

	tuple = read_o_tuple(stup->tuple);
	tuplen = o_tuple_size(tuple, spec) + sizeof(int) + 1;

	/* Add oxid size for validation tuples */
	if (arg->hasOxidField)
		tuplen += sizeof(OXid);
		
	LogicalTapeWrite(tape, (void *) &tuplen, sizeof(tuplen));
	LogicalTapeWrite(tape, (void *) tuple.data, o_tuple_size(tuple, spec));
	/* Write oxid if present */
	if (arg->hasOxidField)
	{
		Pointer oxidPtr = tuple.data + o_tuple_size(tuple, spec);
		LogicalTapeWrite(tape, oxidPtr, sizeof(OXid));
	}
	LogicalTapeWrite(tape, (void *) &tuple.formatFlags, 1);
	if (base->sortopt & TUPLESORT_RANDOMACCESS) /* need trailing length word? */
		LogicalTapeWrite(tape, (void *) &tuplen, sizeof(tuplen));
}

static void
readtup_orioledb_index(Tuplesortstate *state, SortTuple *stup,
					   LogicalTape *tape, unsigned int len)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;
	uint32		tuplen = len - sizeof(int) - 1;
	Pointer		tup = (Pointer) tuplesort_readtup_alloc(state, MAXIMUM_ALIGNOF + tuplen);
	OTuple		tuple;

	/* read in the tuple proper */
	LogicalTapeReadExact(tape, tup + MAXIMUM_ALIGNOF, tuplen);
	LogicalTapeReadExact(tape, tup, 1);
	if (base->sortopt & TUPLESORT_RANDOMACCESS) /* need trailing length word? */
		LogicalTapeReadExact(tape, &tuplen, sizeof(tuplen));
	stup->tuple = (void *) tup;
	tuple = read_o_tuple(tup);
	/* set up first-column key value */
	stup->datum1 = o_fastgetattr(tuple,
								 base->sortKeys[0].ssup_attno,
								 arg->tupDesc,
								 spec,
								 &stup->isnull1);
}

static void
removeabbrev_orioledb_index(Tuplesortstate *state, SortTuple *stups,
							int count)
{
	int			i;
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;

	for (i = 0; i < count; i++)
	{
		SortTuple  *stup = &stups[i];
		OTuple		tup;

		tup = read_o_tuple(stup->tuple);

		stup->datum1 = o_fastgetattr(tup,
									 base->sortKeys[0].ssup_attno,
									 arg->tupDesc,
									 spec,
									 &stup->isnull1);
	}
}

Tuplesortstate *
tuplesort_begin_orioledb_index(OIndexDescr *idx,
							   int workMem,
							   bool randomAccess,
							   SortCoordinate coordinate)
{
	Tuplesortstate *state = tuplesort_begin_common(workMem, coordinate,
												   randomAccess);
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	MemoryContext oldcontext;
	OIndexBuildSortArg *arg;
	int			sort_fields;
	int			i;

	if (idx->unique)
		sort_fields = idx->nKeyFields;
	else
		sort_fields = idx->nFields;

	oldcontext = MemoryContextSwitchTo(base->maincontext);
	arg = (OIndexBuildSortArg *) palloc0(sizeof(OIndexBuildSortArg));
	arg->id = idx;
	arg->tupDesc = idx->leafTupdesc;
	arg->enforceUnique = idx->unique;
	arg->hasOxidField = true;

	base->sortKeys = (SortSupport) palloc0(sort_fields *
										   sizeof(SortSupportData));
	base->nKeys = sort_fields;

	base->removeabbrev = removeabbrev_orioledb_index;
	base->comparetup = comparetup_orioledb_index;
	base->writetup = writetup_orioledb_index;
	base->readtup = readtup_orioledb_index;
	base->arg = arg;

	for (i = 0; i < sort_fields; i++)
	{
		if (!OIgnoreColumn(idx, i))
		{
			SortSupport sortKey = &base->sortKeys[i];

			sortKey->ssup_cxt = CurrentMemoryContext;
			sortKey->ssup_collation = idx->fields[i].collation;
			sortKey->ssup_nulls_first = idx->fields[i].nullfirst;
			sortKey->ssup_attno =
				OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple, idx, i + 1);
			sortKey->abbreviate = (i == 0);
			sortKey->ssup_reverse = !idx->fields[i].ascending;
			/* FIXME: no abbrev converter yet */
			o_finish_sort_support_function(idx->fields[i].comparator, sortKey);
		}
	}

	MemoryContextSwitchTo(oldcontext);

	return state;
}

/*
 * tuplesort_begin_orioledb_index_secondary_pk - create tuplesort for secondary index validation
 *
 * This function creates a tuplesort for validating secondary indexes during concurrent
 * index builds. Unlike tuplesort_begin_orioledb_index which takes a single index descriptor,
 * this function takes both secondary and primary index descriptors.
 *
 * The tuples stored have a special layout:
 * - First: Secondary index key fields (only the actual key fields, not including PK)
 * - Then: Primary key fields
 *
 * The sorting is done ONLY by the primary key fields, which enables merge-join
 * validation with the primary index scan. The tupDesc describes all fields (secondary key + PK),
 * but the sortKeys only reference the PK portion for sorting.
 *
 * This is essential for functional secondary indexes where secondary key values cannot
 * be computed from PK tuples alone - we must store the actual secondary key values
 * from the index being validated.
 *
 * Parameters:
 *   secondary - The secondary index descriptor (provides secondary key field information)
 *   primary - The primary index descriptor (provides PK fields and sorting rules)
 *   workMem - Memory limit for in-memory sorting
 *   randomAccess - Whether random access to sorted data is required
 *   coordinate - Coordination info for parallel sort (can be NULL)
 *
 * Returns: Tuplesortstate configured for secondary index validation
 */
Tuplesortstate *
tuplesort_begin_orioledb_index_secondary_pk(OIndexDescr *secondary,
											 OIndexDescr *primary,
											 int workMem,
											 bool randomAccess,
											 SortCoordinate coordinate)
{
	Tuplesortstate *state = tuplesort_begin_common(workMem, coordinate,
												   randomAccess);
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	MemoryContext oldcontext;
	OIndexBuildSortArg *arg;
	int			secondary_key_fields;
	int			pk_fields;
	int			total_fields;
	int			i;
	TupleDesc	combinedTupDesc;

	/*
	 * Calculate field counts:
	 * - secondary_key_fields: only the key fields of secondary index (NOT including PK)
	 * - pk_fields: number of PK key fields
	 * - total_fields: secondary key fields + PK fields (no duplication)
	 */
	secondary_key_fields = secondary->nKeyFields;
	pk_fields = primary->nKeyFields;
	total_fields = secondary_key_fields + pk_fields;

	oldcontext = MemoryContextSwitchTo(base->maincontext);
	
	/*
	 * Create combined tuple descriptor with secondary key fields first, then PK fields.
	 * We use the primary index for comparison (stored in arg->id), but the tupDesc
	 * includes all fields.
	 */
	combinedTupDesc = CreateTemplateTupleDesc(total_fields);
	
	/* Add secondary key fields */
	for (i = 0; i < secondary_key_fields; i++)
	{
		TupleDescInitEntry(combinedTupDesc,
						   i + 1,
						   NameStr(secondary->leafTupdesc->attrs[i].attname),
						   secondary->leafTupdesc->attrs[i].atttypid,
						   secondary->leafTupdesc->attrs[i].atttypmod,
						   0);
		combinedTupDesc->attrs[i].attcollation =
			secondary->leafTupdesc->attrs[i].attcollation;
	}
	
	/* Add PK fields after secondary key fields */
	for (i = 0; i < pk_fields; i++)
	{
		TupleDescInitEntry(combinedTupDesc,
						   secondary_key_fields + i + 1,
						   NameStr(primary->leafTupdesc->attrs[i].attname),
						   primary->leafTupdesc->attrs[i].atttypid,
						   primary->leafTupdesc->attrs[i].atttypmod,
						   0);
		combinedTupDesc->attrs[secondary_key_fields + i].attcollation =
			primary->leafTupdesc->attrs[i].attcollation;
	}
	
	arg = (OIndexBuildSortArg *) palloc0(sizeof(OIndexBuildSortArg));
	arg->id = primary;  /* Use primary for comparison logic */
	arg->tupDesc = combinedTupDesc;
	arg->enforceUnique = false;  /* Secondary index validation doesn't enforce uniqueness on PK */
	arg->hasOxidField = true;  /* Validation tuples have oxid appended */
	/*
	 * Set up sort keys - we only sort by PK fields, which are at the end
	 * of our combined tuple descriptor
	 */
	base->sortKeys = (SortSupport) palloc0(pk_fields * sizeof(SortSupportData));
	base->nKeys = pk_fields;

	base->removeabbrev = removeabbrev_orioledb_index;
	base->comparetup = comparetup_orioledb_index;
	base->writetup = writetup_orioledb_index;
	base->readtup = readtup_orioledb_index;
	base->arg = arg;

	/*
	 * Configure sort keys to reference PK fields in the combined tuple descriptor.
	 * PK fields start at position (secondary_key_fields + 1) in our tuple descriptor.
	 */
	for (i = 0; i < pk_fields; i++)
	{
		SortSupport sortKey = &base->sortKeys[i];

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = primary->fields[i].collation;
		sortKey->ssup_nulls_first = primary->fields[i].nullfirst;
		/* Attribute number in the combined tuple descriptor */
		sortKey->ssup_attno = secondary_key_fields + i + 1;
		sortKey->abbreviate = (i == 0);
		sortKey->ssup_reverse = !primary->fields[i].ascending;
		/* FIXME: no abbrev converter yet */
		o_finish_sort_support_function(primary->fields[i].comparator, sortKey);
	}

	MemoryContextSwitchTo(oldcontext);

	return state;
}

Tuplesortstate *
tuplesort_begin_orioledb_toast(OIndexDescr *toast,
							   OIndexDescr *primary,
							   int workMem,
							   bool randomAccess,
							   SortCoordinate coordinate)
{
	Tuplesortstate *state = tuplesort_begin_common(workMem, coordinate,
												   randomAccess);
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	MemoryContext oldcontext;
	OIndexBuildSortArg *arg;
	SortSupport sortKey;
	OIndexField field;
	int			key_fields;
	int			i;

	key_fields = primary->nKeyFields;

	oldcontext = MemoryContextSwitchTo(base->maincontext);
	arg = (OIndexBuildSortArg *) palloc0(sizeof(OIndexBuildSortArg));
	arg->id = primary;
	arg->tupDesc = toast->leafTupdesc;
	arg->enforceUnique = true;
	arg->hasOxidField = false;

	base->sortKeys = (SortSupport)
		palloc0((key_fields + TOAST_NON_LEAF_FIELDS_NUM) *
				sizeof(SortSupportData));
	base->nKeys = key_fields + TOAST_NON_LEAF_FIELDS_NUM;

	base->removeabbrev = removeabbrev_orioledb_index;
	base->comparetup = comparetup_orioledb_index;
	base->writetup = writetup_orioledb_index;
	base->readtup = readtup_orioledb_index;
	base->arg = arg;

	for (i = 0; i < key_fields; i++)
	{
		sortKey = &base->sortKeys[i];

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = primary->fields[i].collation;
		sortKey->ssup_nulls_first = primary->fields[i].nullfirst;
		sortKey->ssup_attno = i + 1;
		sortKey->abbreviate = (i == 0);
		sortKey->ssup_reverse = !primary->fields[i].ascending;
		/* FIXME: no abbrev converter yet */
		o_finish_sort_support_function(primary->fields[i].comparator, sortKey);
	}

	field.collation = DEFAULT_COLLATION_OID;

	/* ATTN_POS */
	sortKey = &base->sortKeys[key_fields];
	sortKey->ssup_cxt = CurrentMemoryContext;
	sortKey->ssup_collation = DEFAULT_COLLATION_OID;
	sortKey->ssup_nulls_first = false;
	sortKey->ssup_attno = key_fields + 1;
	sortKey->abbreviate = false;
	sortKey->ssup_reverse = false;
	oFillFieldOpClassAndComparator(&field, toast->oids.datoid,
								   INT2_BTREE_OPS_OID,
								   InvalidOid);
	o_finish_sort_support_function(field.comparator, sortKey);

	/* CHUNKN_POS */
	sortKey = &base->sortKeys[key_fields + 1];
	sortKey->ssup_cxt = CurrentMemoryContext;
	sortKey->ssup_collation = DEFAULT_COLLATION_OID;
	sortKey->ssup_nulls_first = false;
	sortKey->ssup_attno = key_fields + 2;
	sortKey->abbreviate = false;
	sortKey->ssup_reverse = false;
	oFillFieldOpClassAndComparator(&field, toast->oids.datoid,
								   INT4_BTREE_OPS_OID,
								   InvalidOid);
	o_finish_sort_support_function(field.comparator, sortKey);

	MemoryContextSwitchTo(oldcontext);

	return state;
}

OTuple
tuplesort_getotuple(Tuplesortstate *state, bool forward)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(TuplesortstateGetPublic(state)->sortcontext);
	SortTuple	stup;
	OTuple		result;

	if (!tuplesort_gettuple_common(state, forward, &stup))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	if (stup.tuple)
	{
		result = read_o_tuple(stup.tuple);
	}
	else
	{
		result.data = NULL;
		result.formatFlags = 0;
	}

	return result;
}

void
tuplesort_putotuple(Tuplesortstate *state, OTuple tup)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;
	MemoryContext oldcontext = MemoryContextSwitchTo(base->tuplecontext);
	SortTuple	stup;
	int			tupsize;
	OTuple		written_tup;
#if PG_VERSION_NUM >= 170000
	Size		tuplen;
#endif

	/*
	 * Copy the given tuple into memory we control, and decrease availMem.
	 * Then call the common code.
	 */
	tupsize = o_tuple_size(tup, spec);
	
	/* Add oxid size for validation tuples */
	if (arg->hasOxidField)
		tupsize += sizeof(OXid);
	
	stup.tuple = MemoryContextAlloc(base->tuplecontext, MAXIMUM_ALIGNOF + tupsize);
	write_o_tuple(stup.tuple, tup, tupsize);
	written_tup = read_o_tuple(stup.tuple);

	stup.datum1 = o_fastgetattr(written_tup,
								base->sortKeys[0].ssup_attno,
								arg->tupDesc,
								spec,
								&stup.isnull1);
#if PG_VERSION_NUM >= 170000
	/* GetMemoryChunkSpace is not supported for bump contexts */
	if (TupleSortUseBumpTupleCxt(base->sortopt))
		tuplen = MAXALIGN(tupsize);
	else
		tuplen = GetMemoryChunkSpace(stup.tuple);

	tuplesort_puttuple_common(state, &stup,
							  base->sortKeys->abbrev_converter && !stup.isnull1, tuplen);
#else
	tuplesort_puttuple_common(state, &stup,
							  base->sortKeys->abbrev_converter && !stup.isnull1);
#endif
	MemoryContextSwitchTo(oldcontext);
}
