/*-------------------------------------------------------------------------
 *
 * fastpath.c
 *		Routines for fastpath intra-page navigation in B-tree.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/fastpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/fastpath.h"
#include "btree/find.h"
#include "tableam/key_range.h"

#include "catalog/pg_opclass_d.h"

static void
find_downlink_get_key(BTreeDescr *desc, void *key, BTreeKeyType keyType,
					  bool *inclusive, int numValues,
					  Datum *values, uint8 *flags)
{
	TupleDesc	tupdesc;
	OTupleFixedFormatSpec *spec;
	OIndexDescr *id;
	OTuple	   *tuple;
	int			i;

	Assert(!IS_SYS_TREE_OIDS(desc->oids));

	id = (OIndexDescr *) desc->arg;
	*inclusive = false;

	if (keyType == BTreeKeyNone ||
		keyType == BTreeKeyRightmost)
	{
		for (i = 0; i < numValues; i++)
		{
			flags[i] = (keyType == BTreeKeyNone) ? FASTPATH_FIND_DOWNLINK_FLAG_MINUS_INF : FASTPATH_FIND_DOWNLINK_FLAG_PLUS_INF;
			values[i] = (Datum) 0;
		}
		return;
	}

	if (keyType == BTreeKeyBound ||
		keyType == BTreeKeyUniqueLowerBound ||
		keyType == BTreeKeyUniqueUpperBound)
	{
		OBTreeKeyBound *bound = (OBTreeKeyBound *) key;

		Assert(numValues <= bound->nkeys);
		for (i = 0; i < numValues; i++)
		{
			uint8		f = bound->keys[i].flags;

			if (f & O_VALUE_BOUND_UNBOUNDED)
			{
				flags[i] = (f & O_VALUE_BOUND_LOWER) ? FASTPATH_FIND_DOWNLINK_FLAG_MINUS_INF : FASTPATH_FIND_DOWNLINK_FLAG_PLUS_INF;
				values[i] = (Datum) 0;
			}
			else
			{
				flags[i] = 0;
				values[i] = bound->keys[i].value;
			}
		}
		return;
	}

	Assert(keyType == BTreeKeyLeafTuple ||
		   keyType == BTreeKeyNonLeafKey ||
		   keyType == BTreeKeyPageHiKey);

	if (keyType == BTreeKeyPageHiKey)
		*inclusive = true;

	if (keyType == BTreeKeyLeafTuple)
	{
		tupdesc = id->leafTupdesc;
		spec = &id->leafSpec;
	}
	else
	{
		tupdesc = id->nonLeafTupdesc;
		spec = &id->nonLeafSpec;
	}

	tuple = (OTuple *) key;

	for (i = 0; i < numValues; i++)
	{
		bool		isnull;
		int			attnum;

		attnum = OIndexKeyAttnumToTupleAttnum(keyType, id, i + 1);
		values[i] = o_fastgetattr(*tuple, attnum, tupdesc, spec, &isnull);

		if (isnull)
			flags[i] = (id->fields[i].nullfirst) ? FASTPATH_FIND_DOWNLINK_FLAG_MINUS_INF : FASTPATH_FIND_DOWNLINK_FLAG_PLUS_INF;
		else
			flags[i] = 0;
	}
}

/*
 * int4_array_search
 *		Binary-search version that narrows the second pass whenever the first
 *		pass encounters a value > key.
 */
static void
int4_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum)
{
	char	   *base = (char *) p;
	int32		key = DatumGetInt32(keyDatum);

	int			lo = *lower;	/* inclusive  */
	int			hi = *upper;	/* exclusive  */
	int			upper_hint = hi;	/* best "first > key" seen so far */

	/* ------------------------------------------------------------------
	 * Pass 1: locate the first element >= key (lower bound),
	 *		   while remembering the left-most element > key.
	 * ------------------------------------------------------------------ */
	while (lo < hi)
	{
		int			mid = lo + ((hi - lo) >> 1);
		int32		val = *((int32 *) (base + mid * stride));

		if (val < key)
			lo = mid + 1;
		else
		{
			hi = mid;			/* val >= key */

			if (val > key && mid < upper_hint)
				upper_hint = mid;	/* tighten the eventual upper bound */
		}
	}
	*lower = lo;				/* potential match / insertion point */

	/* If the key is absent, we’re done. */
	if (lo == *upper ||
		*((int32 *) (base + lo * stride)) != key)
	{
		*upper = lo;
		return;
	}

	/* ------------------------------------------------------------------
	 * Pass 2: find first element  > key, searching only up to upper_hint.
	 * ------------------------------------------------------------------ */
	hi = upper_hint;
	while (lo < hi)
	{
		int			mid = lo + ((hi - lo) >> 1);
		int32		val = *((int32 *) (base + mid * stride));

		if (val <= key)
			lo = mid + 1;
		else
			hi = mid;
	}
	*upper = lo;				/* first element greater than key */
}

static void
int8_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum)
{
	int			i;
	bool		lowerSet = false;
	int64		key = DatumGetInt32(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		int64		value = *((int64 *) p);

		if (value == key && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (value > key)
		{
			if (!lowerSet)
				*lower = i;
			*upper = i;
			return;
		}

		p += stride;
	}
	if (!lowerSet)
		*lower = *upper;
}

static void
oid_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum)
{
	int			i;
	bool		lowerSet = false;
	Oid			key = DatumGetInt32(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		Oid			value = *((Oid *) p);

		if (value == key && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (value > key)
		{
			if (!lowerSet)
				*lower = i;
			*upper = i;
			return;
		}

		p += stride;
	}
	if (!lowerSet)
		*lower = *upper;
}

static void
float8_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum)
{
	int			i;
	bool		lowerSet = false;
	float8		key = DatumGetInt32(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		float8		value = *((float8 *) p);

		if (value == key && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (value > key)
		{
			if (!lowerSet)
				*lower = i;
			*upper = i;
			return;
		}

		p += stride;
	}
	if (!lowerSet)
		*lower = *upper;
}

typedef struct
{
	Oid			typeid;
	Oid			opcid;
	int			typlen;
	int			align;
	ArraySearchFunc func;
} ArraySearchDesc;

ArraySearchDesc arraySearchDescs[] = {
	{OIDOID, OID_BTREE_OPS_OID, sizeof(Oid), ALIGNOF_INT, oid_array_search},
	{INT4OID, INT4_BTREE_OPS_OID, sizeof(int32), ALIGNOF_INT, int4_array_search},
	{INT8OID, INT8_BTREE_OPS_OID, sizeof(int64), ALIGNOF_DOUBLE, int8_array_search},
	{FLOAT8OID, FLOAT8_BTREE_OPS_OID, sizeof(float8), ALIGNOF_DOUBLE, float8_array_search},
};

static ArraySearchDesc *
find_array_search_desc_by_typeid(Oid typeid)
{
	int			i;

	for (i = 0; i < sizeof(arraySearchDescs) / sizeof(ArraySearchDesc); i++)
	{
		if (arraySearchDescs[i].typeid == typeid)
			return &arraySearchDescs[i];
	}
	return NULL;
}

void
can_fastpath_find_downlink(OBTreeFindPageContext *context,
						   void *key,
						   BTreeKeyType keyType,
						   FastpathFindDownlinkMeta *meta)
{
	BTreeDescr *desc = context->desc;
	OIndexDescr *id;
	int			i;
	int			offset;

	ASAN_UNPOISON_MEMORY_REGION(meta, sizeof(*meta));

	if (!BTREE_PAGE_FIND_IS(context, FETCH) ||
		IS_SYS_TREE_OIDS(desc->oids))
	{
		meta->enabled = false;
		return;
	}

	id = (OIndexDescr *) desc->arg;

	if (id->nonLeafTupdesc->natts >= FASTPATH_FIND_DOWNLINK_MAX_KEYS ||
		id->nonLeafSpec.natts != id->nonLeafTupdesc->natts)
	{
		meta->enabled = false;
		return;
	}

	if (keyType == BTreeKeyUniqueLowerBound ||
		keyType == BTreeKeyUniqueUpperBound)
		meta->numKeys = id->nUniqueFields;
	else if (id->desc.type != oIndexToast && id->desc.type != oIndexBridge)
		meta->numKeys = id->nKeyFields;
	else
		meta->numKeys = id->nonLeafSpec.natts;

	offset = 0;
	for (i = 0; i < meta->numKeys; i++)
	{
		ArraySearchDesc *desc = find_array_search_desc_by_typeid(id->nonLeafTupdesc->attrs[0].atttypid);

		if (!desc)
		{
			meta->enabled = false;
			return;
		}

		offset = TYPEALIGN(desc->align, offset);
		meta->funcs[i] = desc->func;
		meta->offsets[i] = offset;

		offset += desc->typlen;
	}

	find_downlink_get_key(context->desc, key, keyType,
						  &meta->inclusive, meta->numKeys,
						  meta->values, meta->flags);
	meta->enabled = true;
	meta->length = MAXALIGN(id->nonLeafSpec.len);
}

OBTreeFastPathFindResult
fastpath_find_chunk(Pointer pagePtr,
					OInMemoryBlkno blkno,
					FastpathFindDownlinkMeta *meta,
					int *chunkIndex)
{
	BTreePageHeader *imgHdr = (BTreePageHeader *) pagePtr;
	BTreePageHeader *hdr = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(blkno);
	int			i;
	int			lower;
	int			upper;
	int			count;
	int			offset;
	Pointer		base;
	uint32		imageChangeCount = pg_atomic_read_u32(&imgHdr->o_header.state) & PAGE_STATE_CHANGE_COUNT_MASK;
	uint32		state;

	count = O_PAGE_IS(pagePtr, RIGHTMOST) ? imgHdr->chunksCount - 1 : imgHdr->chunksCount;

	offset = SHORT_GET_LOCATION(hdr->chunkDesc[0].hikeyShortLocation);

	pg_read_barrier();

	if (imgHdr->hikeysEnd - offset != count * meta->length)
		return OBTreeFastPathFindSlowpath;

	base = (Pointer) hdr + offset;
	lower = 0;
	upper = count;
	for (i = 0; lower < upper && i < meta->numKeys; i++)
	{
		if (meta->flags[i] == 0)
			meta->funcs[i] (base + meta->offsets[i],
							meta->length, &lower, &upper,
							meta->values[i]);
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_MINUS_INF)
			upper = lower;
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_PLUS_INF)
			lower = upper;
	}

	*chunkIndex = meta->inclusive ? lower : upper;

	pg_read_barrier();

	/* Possible we need to visit the rightlink */
	if (*chunkIndex >= count)
		return OBTreeFastPathFindSlowpath;

	state = pg_atomic_read_u32(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount)
		return OBTreeFastPathFindRetry;

	return OBTreeFastPathFindOK;
}

OBTreeFastPathFindResult
fastpath_find_downlink(Pointer pagePtr,
					   OInMemoryBlkno blkno,
					   FastpathFindDownlinkMeta *meta,
					   BTreePageItemLocator *loc,
					   BTreeNonLeafTuphdr **tuphdrPtr)
{
	BTreePageHeader *imgHdr = (BTreePageHeader *) pagePtr;
	BTreePageHeader *hdr = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(blkno);
	int			lower;
	int			upper;
	int			count;
	int			i;
	int			chunkIndex;
	int			itemIndex;
	BTreePageChunk *chunk;
	int			chunkSize,
				chunkItemsCount;
	Pointer		base;
	uint32		state;
	uint32		imageChangeCount = pg_atomic_read_u32(&imgHdr->o_header.state) & PAGE_STATE_CHANGE_COUNT_MASK;
	OBTreeFastPathFindResult result;
	static BTreeNonLeafTuphdr tuphdr;

	result = fastpath_find_chunk(pagePtr, blkno, meta, &chunkIndex);

	if (result != OBTreeFastPathFindOK)
		return result;

	chunk = (BTreePageChunk *) ((Pointer) hdr + SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation));
	if (chunkIndex < imgHdr->chunksCount - 1)
	{
		chunkSize = SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex + 1].shortLocation) - SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation);
		chunkItemsCount = hdr->chunkDesc[chunkIndex + 1].offset - hdr->chunkDesc[chunkIndex].offset;
	}
	else
	{
		chunkSize = imgHdr->dataSize - SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation);
		chunkItemsCount = imgHdr->itemsCount - hdr->chunkDesc[chunkIndex].offset;
	}

	pg_read_barrier();

	if (chunkIndex == 0)
	{
		count = chunkItemsCount - 1;
		base = (Pointer) chunk + MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) + MAXALIGN(sizeof(BTreeNonLeafTuphdr));
	}
	else
	{
		count = chunkItemsCount;
		base = (Pointer) chunk + MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);
	}

	if (chunkSize != MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) +
		MAXALIGN(sizeof(BTreeNonLeafTuphdr)) * chunkItemsCount +
		meta->length * count)
		return OBTreeFastPathFindSlowpath;

	lower = 0;
	upper = count;
	for (i = 0; lower < upper && i < meta->numKeys; i++)
	{
		if (meta->flags[i] == 0)
			meta->funcs[i] (base + MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->offsets[i],
							MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->length,
							&lower, &upper, meta->values[i]);
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_MINUS_INF)
			upper = lower;
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_PLUS_INF)
			lower = upper;
	}

	itemIndex = meta->inclusive ? lower : upper;

	pg_read_barrier();

	state = pg_atomic_read_u32(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount)
		return OBTreeFastPathFindRetry;

	if (chunkIndex == 0)
	{
		if (itemIndex == 0)
			tuphdr = *((BTreeNonLeafTuphdr *) (base - MAXALIGN(sizeof(BTreeNonLeafTuphdr))));
		else
			tuphdr = *((BTreeNonLeafTuphdr *) (base + (MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->length) * (itemIndex - 1)));
		*tuphdrPtr = &tuphdr;
		loc->chunk = chunk;
		loc->chunkItemsCount = chunkItemsCount;
		loc->chunkSize = chunkSize;
		loc->itemOffset = itemIndex;
		loc->chunkOffset = chunkIndex;
	}
	else
	{
		if (itemIndex > 0)
		{
			tuphdr = *((BTreeNonLeafTuphdr *) (base + (MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->length) * (itemIndex - 1)));
			*tuphdrPtr = &tuphdr;
			loc->chunk = chunk;
			loc->chunkItemsCount = chunkItemsCount;
			loc->chunkSize = chunkSize;
			loc->itemOffset = itemIndex - 1;
			loc->chunkOffset = chunkIndex;
		}
		else
		{
			chunkIndex--;
			chunk = (BTreePageChunk *) ((Pointer) hdr + SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation));
			if (chunkIndex < imgHdr->chunksCount - 1)
			{
				chunkSize = SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex + 1].shortLocation) - SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation);
				chunkItemsCount = hdr->chunkDesc[chunkIndex + 1].offset - hdr->chunkDesc[chunkIndex].offset;
			}
			else
			{
				chunkSize = imgHdr->dataSize - SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation);
				chunkItemsCount = imgHdr->itemsCount - hdr->chunkDesc[chunkIndex].offset;
			}

			pg_read_barrier();

			if (chunkIndex == 0)
			{
				count = chunkItemsCount - 1;
				base = (Pointer) chunk + MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) + MAXALIGN(sizeof(BTreeNonLeafTuphdr));
			}
			else
			{
				count = chunkItemsCount;
				base = (Pointer) chunk + MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);
			}

			if (chunkSize != MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) +
				MAXALIGN(sizeof(BTreeNonLeafTuphdr)) * chunkItemsCount +
				meta->length * count)
				return OBTreeFastPathFindSlowpath;

			itemIndex = chunkItemsCount - 1;

			if (chunkIndex == 0 && itemIndex == 0)
				tuphdr = *((BTreeNonLeafTuphdr *) (base - MAXALIGN(sizeof(BTreeNonLeafTuphdr))));
			else
				tuphdr = *((BTreeNonLeafTuphdr *) (base + (MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->length) * (count - 1)));
			*tuphdrPtr = &tuphdr;

			loc->chunk = chunk;
			loc->chunkItemsCount = chunkItemsCount;
			loc->chunkSize = chunkSize;
			loc->itemOffset = itemIndex;
			loc->chunkOffset = chunkIndex;
		}
	}

	pg_read_barrier();

	state = pg_atomic_read_u32(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount)
		return OBTreeFastPathFindRetry;

	/* elog(LOG, "fast path %u %u, ", loc->chunkOffset, loc->itemOffset); */

	return OBTreeFastPathFindOK;
}
