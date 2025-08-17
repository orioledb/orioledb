/*-------------------------------------------------------------------------
 *
 * fastpath.c
 *		Routines for fastpath intra-page navigation in B-tree.
 *
 *	The "fast path" navigation enables us to find a downlink (child pointer)
 *	without copying page chunks into local memory and performing a full
 *	binary search on the tuple array.  In certain cases, we can walk a
 *	cache-friendly, fixed-stride array of values that mirrors the page layout,
 *	thereby reducing memory copying, branch mispredictions, and memory
 *	dereferences when descending the tree.
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
#include "postgres_ext.h"
#include "storage/itemptr.h"
#include "tableam/key_range.h"

#include "catalog/pg_opclass_d.h"
#include "commands/defrem.h"

typedef struct
{
	Oid			typeid;
	Oid			opcid;
	int			typlen;
	int			align;
	ArraySearchFunc func;
} ArraySearchDesc;

static ArraySearchDesc *find_array_search_desc_by_typeid(Oid typeid);

static bool find_downlink_get_keys(BTreeDescr *desc,
								   void *key, BTreeKeyType keyType,
								   bool *inclusive, int numValues,
								   Oid *types, Datum *values, uint8 *flags);

static void oid_array_search(Pointer p, int stride, int *lower,
							 int *upper, Datum keyDatum);
static void int4_array_search(Pointer p, int stride, int *lower,
							  int *upper, Datum keyDatum);
static void int8_array_search(Pointer p, int stride, int *lower,
							  int *upper, Datum keyDatum);
static void float4_array_search(Pointer p, int stride, int *lower,
								int *upper, Datum keyDatum);
static void float8_array_search(Pointer p, int stride, int *lower,
								int *upper, Datum keyDatum);
static void tid_array_search(Pointer p, int stride, int *lower,
							 int *upper, Datum keyDatum);

ArraySearchDesc arraySearchDescs[] = {
	{OIDOID, OID_BTREE_OPS_OID, sizeof(Oid), ALIGNOF_INT, oid_array_search},
	{INT4OID, INT4_BTREE_OPS_OID, sizeof(int32), ALIGNOF_INT, int4_array_search},
	{INT8OID, INT8_BTREE_OPS_OID, sizeof(int64), ALIGNOF_DOUBLE, int8_array_search},
	{FLOAT4OID, InvalidOid, sizeof(float4), ALIGNOF_INT, float4_array_search},
	{FLOAT8OID, FLOAT8_BTREE_OPS_OID, sizeof(float8), ALIGNOF_DOUBLE, float8_array_search},
	{TIDOID, InvalidOid, sizeof(ItemPointerData), ALIGNOF_SHORT, tid_array_search}
};

/*
 * Checks if the "fast path" the navigation can be applied to the given search
 * and fills *meta structure if so.
 */
void
can_fastpath_find_downlink(OBTreeFindPageContext *context,
						   void *key,
						   BTreeKeyType keyType,
						   FastpathFindDownlinkMeta *meta)
{
	BTreeDescr *desc = context->desc;
	OIndexDescr *id;
	Oid			types[FASTPATH_FIND_DOWNLINK_MAX_KEYS] = {InvalidOid};
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
		OIndexField *field = &id->fields[i];

		if (!desc || desc->opcid != field->opclass)
		{
			meta->enabled = false;
			return;
		}

		offset = TYPEALIGN(desc->align, offset);
		meta->funcs[i] = desc->func;
		meta->offsets[i] = offset;
		types[i] = desc->typeid;

		offset += desc->typlen;
	}

	if (!find_downlink_get_keys(context->desc, key, keyType,
								&meta->inclusive, meta->numKeys, types,
								meta->values, meta->flags))
	{
		meta->enabled = false;
		return;
	}

	meta->enabled = true;
	meta->length = MAXALIGN(id->nonLeafSpec.len);
}

static ArraySearchDesc *
find_array_search_desc_by_typeid(Oid typeid)
{
	int			i;

	for (i = 0; i < sizeof(arraySearchDescs) / sizeof(ArraySearchDesc); i++)
	{
		if (arraySearchDescs[i].typeid == typeid)
		{
			if (!OidIsValid(arraySearchDescs[i].opcid))
				arraySearchDescs[i].opcid = GetDefaultOpClass(typeid, BTREE_AM_OID);
			return &arraySearchDescs[i];
		}
	}
	return NULL;
}

/*
 * Decompose search key into values for the "fast path" tree navigation.
 */
static bool
find_downlink_get_keys(BTreeDescr *desc, void *key, BTreeKeyType keyType,
					   bool *inclusive, int numValues, Oid *types,
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
		return true;
	}

	if (keyType == BTreeKeyBound ||
		keyType == BTreeKeyUniqueLowerBound ||
		keyType == BTreeKeyUniqueUpperBound)
	{
		OBTreeKeyBound *bound = (OBTreeKeyBound *) key;
		int			num = Min(numValues, bound->nkeys);

		for (i = 0; i < num; i++)
		{
			uint8		f = bound->keys[i].flags;

			if (bound->keys[i].type != types[i])
				return false;

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
		return true;
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
	return true;
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
	uint64		state;
	uint64		imageChangeCount = pg_atomic_read_u64(&imgHdr->o_header.state) & PAGE_STATE_CHANGE_COUNT_MASK;
	OBTreeFastPathFindResult result;
	static BTreeNonLeafTuphdr tuphdr;

	result = fastpath_find_chunk(pagePtr, blkno, meta, &chunkIndex);

	if (result != OBTreeFastPathFindOK)
		return result;

	if (!hdr->chunkDesc[chunkIndex].chunkKeysFixed)
		return OBTreeFastPathFindSlowpath;

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

	state = pg_atomic_read_u64(&hdr->o_header.state);
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
			if (!hdr->chunkDesc[chunkIndex].chunkKeysFixed)
				return OBTreeFastPathFindSlowpath;

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

	state = pg_atomic_read_u64(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount)
		return OBTreeFastPathFindRetry;

	return OBTreeFastPathFindOK;
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
	uint64		imageChangeCount = pg_atomic_read_u64(&imgHdr->o_header.state) & PAGE_STATE_CHANGE_COUNT_MASK;
	uint64		state;

	if (!O_PAGE_IS(pagePtr, HIKEYS_FIXED))
		return OBTreeFastPathFindSlowpath;

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

	state = pg_atomic_read_u64(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount)
		return OBTreeFastPathFindRetry;

	return OBTreeFastPathFindOK;
}

/*
 * Find the given value in the fixed-stride array of integers.  The functions
 * below do the same for other datatypes.
 */
static void
int4_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum)
{
	int			i;
	bool		lowerSet = false;
	int32		key = DatumGetInt32(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		int32		value = *((int32 *) p);

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
int8_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum)
{
	int			i;
	bool		lowerSet = false;
	int64		key = DatumGetInt64(keyDatum);

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
	Oid			key = DatumGetObjectId(keyDatum);

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
float4_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum)
{
	int			i;
	bool		lowerSet = false;
	float4		key = DatumGetFloat4(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		/* cppcheck-suppress invalidPointerCast */
		float4		value = *((float4 *) p);

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
	float8		key = DatumGetFloat8(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		/* cppcheck-suppress invalidPointerCast */
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

static int
tid_cmp(ItemPointer arg1, ItemPointer arg2)
{
	BlockNumber b1 = ItemPointerGetBlockNumberNoCheck(arg1);
	BlockNumber b2 = ItemPointerGetBlockNumberNoCheck(arg2);

	if (b1 < b2)
		return -1;
	else if (b1 > b2)
		return 1;
	else if (ItemPointerGetOffsetNumberNoCheck(arg1) <
			 ItemPointerGetOffsetNumberNoCheck(arg2))
		return -1;
	else if (ItemPointerGetOffsetNumberNoCheck(arg1) >
			 ItemPointerGetOffsetNumberNoCheck(arg2))
		return 1;
	else
		return 0;
}

static void
tid_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum)
{
	int			i;
	bool		lowerSet = false;
	ItemPointer key = DatumGetItemPointer(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		int			cmp = tid_cmp((ItemPointer) p, key);

		if (cmp == 0 && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (cmp > 0)
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
