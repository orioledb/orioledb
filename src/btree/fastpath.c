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
 * Copyright (c) 2025-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#define FASTPATH_USE_AVX2 1
#include <immintrin.h>
#endif

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
							 int *upper, Datum keyDatum, bool ascending);
static void int4_array_search(Pointer p, int stride, int *lower,
							  int *upper, Datum keyDatum, bool ascending);
static void int8_array_search(Pointer p, int stride, int *lower,
							  int *upper, Datum keyDatum, bool ascending);
static void float4_array_search(Pointer p, int stride, int *lower,
								int *upper, Datum keyDatum, bool ascending);
static void float8_array_search(Pointer p, int stride, int *lower,
								int *upper, Datum keyDatum, bool ascending);
static void tid_array_search(Pointer p, int stride, int *lower,
							 int *upper, Datum keyDatum, bool ascending);

static ArraySearchDesc arraySearchDescs[] = {
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
#ifdef FASTPATH_USE_AVX2
static void int4_array_search_avx2(Pointer p, int stride, int *lower,
								   int *upper, Datum keyDatum, bool ascending);
static void int8_array_search_avx2(Pointer p, int stride, int *lower,
								   int *upper, Datum keyDatum, bool ascending);
static void oid_array_search_avx2(Pointer p, int stride, int *lower,
								  int *upper, Datum keyDatum, bool ascending);
#endif

/*
 * The array search of a type, the vector one where the CPU has it.
 */
static ArraySearchFunc
array_search_func(ArraySearchDesc *searchDesc)
{
#ifdef FASTPATH_USE_AVX2
	static int	haveAvx2 = -1;

	if (haveAvx2 < 0)
		haveAvx2 = __builtin_cpu_supports("avx2") ? 1 : 0;
	if (haveAvx2)
	{
		if (searchDesc->func == int4_array_search)
			return int4_array_search_avx2;
		if (searchDesc->func == int8_array_search)
			return int8_array_search_avx2;
		if (searchDesc->func == oid_array_search)
			return oid_array_search_avx2;
	}
#endif
	return searchDesc->func;
}

/*
 * Build the part of a fast-path search that depends on the index alone.
 */
static void
build_fastpath_index_meta(OIndexDescr *id, FastpathIndexMeta *im)
{
	int			natts = id->nonLeafTupdesc->natts;
	int			offset = 0;
	int			i = 0;

	if (natts <= FASTPATH_FIND_DOWNLINK_MAX_KEYS &&
		id->nonLeafSpec.natts == natts)
	{
		for (i = 0; i < natts; i++)
		{
			ArraySearchDesc *searchDesc =
				find_array_search_desc_by_typeid(TupleDescAttr(id->nonLeafTupdesc, i)->atttypid);
			OIndexField *field = &id->fields[i];

			/*
			 * The array-search routines compare raw datums, so they require
			 * the field's btree opclass to match the default one they
			 * implement. DESC ordering is supported: the routine gets an
			 * "ascending" flag and find_downlink_get_keys() expresses bounds
			 * as storage directions, so a DESC field is handled by mirroring
			 * the comparison rather than bailing.
			 */
			if (!searchDesc || searchDesc->opcid != field->opclass)
				break;

			offset = TYPEALIGN(searchDesc->align, offset);
			im->funcs[i] = array_search_func(searchDesc);
			im->offsets[i] = offset;
			im->ascending[i] = field->ascending;
			im->types[i] = searchDesc->typeid;
			offset += searchDesc->typlen;
		}
	}
	im->natts = i;

	/* The float searches do not order NaN as the opclass does */
	for (i = 0; i < im->natts; i++)
	{
		if (im->types[i] == FLOAT4OID || im->types[i] == FLOAT8OID)
			break;
	}
	im->firstInexact = i;

	im->length = MAXALIGN(id->nonLeafSpec.len);

	/*
	 * A leaf item is fixed-format only when every attribute of the leaf tuple
	 * is, and then the key columns sit at the offsets just computed if they
	 * are its leading attributes.
	 */
	i = 0;
	if (id->leafSpec.natts == id->leafTupdesc->natts && !id->bridging)
	{
		for (i = 0; i < im->natts; i++)
		{
			if (OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple, id, i + 1) != i + 1 ||
				TupleDescAttr(id->leafTupdesc, i)->atttypid != im->types[i])
				break;
		}
	}
	im->leafNatts = i;
	im->leafStride = BTreeLeafTuphdrSize + MAXALIGN(id->leafSpec.len);
	im->built = true;
}

void
can_fastpath_find_downlink(OBTreeFindPageContext *context,
						   void *key,
						   BTreeKeyType keyType,
						   FastpathFindDownlinkMeta *meta)
{
	BTreeDescr *desc = context->desc;
	OIndexDescr *id;
	FastpathIndexMeta *im;
	int			i;
	int			numKeys;

	ASAN_UNPOISON_MEMORY_REGION(meta, sizeof(*meta));

	if (!BTREE_PAGE_FIND_IS(context, FETCH) ||
		IS_SYS_TREE_OIDS(desc->oids))
	{
		meta->enabled = false;
		return;
	}

	id = (OIndexDescr *) desc->arg;
	im = &id->fastpath;
	if (!im->built)
		build_fastpath_index_meta(id, im);

	if (keyType == BTreeKeyUniqueLowerBound ||
		keyType == BTreeKeyUniqueUpperBound)
		numKeys = id->nUniqueFields;
	else if (id->desc.type != oIndexToast && id->desc.type != oIndexBridge)

		/*
		 * Compare the whole tuple-identifying key, not just the user key
		 * fields.  A non-unique index appends the primary key to make every
		 * downlink/leaf key unique; comparing only the leading nKeyFields
		 * would treat duplicate user-key values as an ambiguous prefix and
		 * could descend into the wrong child (skipping earlier duplicates).
		 */
		numKeys = id->nUniqueFields;
	else
		numKeys = id->nonLeafSpec.natts;

	if (numKeys > im->natts)
	{
		meta->enabled = false;
		return;
	}
	meta->numKeys = numKeys;
	memcpy(meta->funcs, im->funcs, sizeof(meta->funcs));
	memcpy(meta->offsets, im->offsets, sizeof(meta->offsets));
	memcpy(meta->ascending, im->ascending, sizeof(meta->ascending));

	if (!find_downlink_get_keys(context->desc, key, keyType,
								&meta->inclusive, numKeys, im->types,
								meta->values, meta->flags))
	{
		meta->enabled = false;
		return;
	}

	meta->enabled = true;
	meta->length = im->length;
	meta->rawOrderExact = numKeys <= im->firstInexact;
	meta->leafSearch = (meta->rawOrderExact && keyType == BTreeKeyBound &&
						numKeys <= im->leafNatts);
	if (meta->leafSearch)
	{
		OBTreeKeyBound *bound = (OBTreeKeyBound *) key;

		/*
		 * Equal to an item's value, an exclusive bound sorts after it (lower)
		 * or before it (upper) whatever the columns after say, and an
		 * inclusive one leaves it to them: o_idx_cmp_range_key_to_value(). A
		 * NULL bound or an exclusion operator compares otherwise, so they
		 * take the item-by-item search.
		 */
		for (i = 0; meta->leafSearch && i < Min(numKeys, bound->nkeys); i++)
		{
			uint8		f = bound->keys[i].flags;

			meta->leafExclusive[i] = 0;
			if ((f & O_VALUE_BOUND_NULL) || bound->keys[i].exclusion_fn)
				meta->leafSearch = false;
			else if (!(f & O_VALUE_BOUND_INCLUSIVE))
			{
				if (f & O_VALUE_BOUND_LOWER)
					meta->leafExclusive[i] = 1;
				else if (f & O_VALUE_BOUND_UPPER)
					meta->leafExclusive[i] = -1;
			}
		}
	}
	meta->leafStride = im->leafStride;
}

/*
 * Search a loaded leaf chunk the way fastpath_find_downlink() searches an
 * internal one, for the first item the bound admits: possible when the
 * chunk holds only fixed-format items, which then lie one after another at
 * the same stride.  The chunk is the caller's copy, consistent already.
 * False when the chunk is not laid out like that, leaving the caller to
 * search it item by item.
 */
bool
fastpath_search_leaf_items(FastpathFindDownlinkMeta *meta,
						   BTreePageItemLocator *loc)
{
	BTreePageChunk *chunk = loc->chunk;
	int			count = loc->chunkItemsCount;
	int			first;
	Pointer		base;
	int			lower,
				upper,
				i;

	if (!meta->leafSearch || count == 0)
		return false;

	first = MAXALIGN(sizeof(LocationIndex) * count);
	if (loc->chunkSize != first + meta->leafStride * count)
		return false;
	for (i = 0; i < count; i++)
	{
		if (ITEM_GET_OFFSET(chunk->items[i]) != first + meta->leafStride * i ||
			!(ITEM_GET_FLAGS(chunk->items[i]) & O_TUPLE_FLAGS_FIXED_FORMAT))
			return false;
	}

	base = (Pointer) chunk + first + BTreeLeafTuphdrSize;
	lower = 0;
	upper = count;
	for (i = 0; lower < upper && i < meta->numKeys; i++)
	{
		if (meta->flags[i] == 0)
		{
			meta->funcs[i] (base + meta->offsets[i], meta->leafStride,
							&lower, &upper, meta->values[i],
							meta->ascending[i]);
			if (meta->leafExclusive[i] > 0)
				lower = upper;
			else if (meta->leafExclusive[i] < 0)
				upper = lower;
		}
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_FIRST)
			upper = lower;
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_LAST)
			lower = upper;
	}

	/* Items equal to every value of an inclusive bound do not sort below it */
	loc->itemOffset = lower;
	return true;
}

/*
 * Prepare *meta for fastpath_prefetch_leaves(); false if the key cannot take
 * the fast path.
 */
bool
fastpath_prepare_prefetch(BTreeDescr *desc, void *key, BTreeKeyType keyType,
						  FastpathFindDownlinkMeta *meta)
{
	OBTreeFindPageContext context;

	if (orioledb_debug_disable_downlink_fastpath)
		return false;
	init_page_find_context(&context, desc, COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_FETCH);
	can_fastpath_find_downlink(&context, key, keyType, meta);
	return meta->enabled;
}

static inline void
prefetch_lines(Pointer p, int n)
{
	int			i;

	for (i = 0; i < n; i++)
		__builtin_prefetch(p + i * PG_CACHE_LINE_SIZE);
}

/*
 * Find the leaves of several keys together and start loading them into
 * cache, so that the lookups wait for their pages at once instead of one
 * after another.  The keys go down level by level: each level's pages were
 * prefetched while the level above was searched for every key.  Last, each
 * leaf's high keys tell the chunk of its key, which gets prefetched too.
 *
 * Nothing read here is validated: it only decides what to prefetch, and the
 * downlink to each leaf becomes the hint its lookup starts from, which the
 * lookup validates.  A hint stays invalid where the fast path cannot tell the
 * leaf.
 */
void
fastpath_prefetch_leaves(BTreeDescr *desc, int n,
						 FastpathFindDownlinkMeta *metas,
						 BTreeLocationHint *hints)
{
	OInMemoryBlkno blknos[FASTPATH_PREFETCH_MAX];
	bool		live[FASTPATH_PREFETCH_MAX];
	OInMemoryBlkno root;
	bool		any;
	int			i,
				depth;

	Assert(n <= FASTPATH_PREFETCH_MAX);

	o_btree_load_shmem(desc);
	root = desc->rootInfo.rootPageBlkno;

	any = false;
	for (i = 0; i < n; i++)
	{
		hints[i].blkno = OInvalidInMemoryBlkno;
		hints[i].pageChangeCount = InvalidOPageChangeCount;
		blknos[i] = root;
		live[i] = metas[i].enabled && !O_PAGE_IS_LOCAL(root);
		any = any || live[i];
	}

	for (depth = 0; any && depth < ORIOLEDB_MAX_DEPTH; depth++)
	{
		any = false;
		for (i = 0; i < n; i++)
		{
			BTreePageHeader hdr;
			BTreePageItemLocator loc;
			BTreeNonLeafTuphdr *tuphdr;
			uint64		downlink;
			OInMemoryBlkno child;
			int			level;

			if (!live[i])
				continue;
			live[i] = false;

			/* The header copy fastpath_find_downlink() validates against */
			memcpy(&hdr, O_GET_IN_MEMORY_PAGE(blknos[i]),
				   offsetof(BTreePageHeader, chunkDesc));
			pg_read_barrier();
			level = PAGE_GET_LEVEL(&hdr);
			if (level == 0 ||
				fastpath_find_downlink((Pointer) &hdr, blknos[i], &metas[i],
									   &loc, &tuphdr) != OBTreeFastPathFindOK)
				continue;

			downlink = tuphdr->downlink;
			if (!DOWNLINK_IS_IN_MEMORY(downlink))
				continue;
			child = DOWNLINK_GET_IN_MEMORY_BLKNO(downlink);

			/* Read unlocked, the downlink may come from a reused page */
			if (O_PAGE_IS_LOCAL(child) || child >= orioledb_buffers_count)
				continue;

			/* The header and the high keys */
			prefetch_lines(O_GET_IN_MEMORY_PAGE(child), 4);
			blknos[i] = child;
			if (level == 1)
			{
				hints[i].blkno = child;
				hints[i].pageChangeCount =
					DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(downlink);
			}
			else
			{
				live[i] = true;
				any = true;
			}
		}
	}

	/* Each leaf's chunk of its key */
	for (i = 0; i < n; i++)
	{
		BTreePageHeader hdr;
		BTreePageHeader *leaf;
		int			chunkIndex;
		LocationIndex chunkBegin,
					chunkEnd;

		if (!OInMemoryBlknoIsValid(hints[i].blkno))
			continue;
		leaf = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(hints[i].blkno);
		memcpy(&hdr, leaf, offsetof(BTreePageHeader, chunkDesc));
		pg_read_barrier();
		if (PAGE_GET_LEVEL(&hdr) != 0 ||
			fastpath_find_chunk((Pointer) &hdr, hints[i].blkno, &metas[i],
								&chunkIndex) != OBTreeFastPathFindOK ||
			chunkIndex >= hdr.chunksCount ||
			hdr.chunksCount > BTREE_PAGE_MAX_CHUNKS)
			continue;

		chunkBegin = SHORT_GET_LOCATION(leaf->chunkDesc[chunkIndex].shortLocation);
		chunkEnd = chunkIndex + 1 < hdr.chunksCount ?
			SHORT_GET_LOCATION(leaf->chunkDesc[chunkIndex + 1].shortLocation) :
			hdr.dataSize;
		if (chunkBegin >= chunkEnd || chunkEnd > ORIOLEDB_BLCKSZ)
			continue;
		prefetch_lines((Pointer) leaf + chunkBegin,
					   Min((chunkEnd - chunkBegin + PG_CACHE_LINE_SIZE - 1) /
						   PG_CACHE_LINE_SIZE, 16));
	}
}

/*
 * Search a leaf chunk of a partially read page where it lies in shared
 * memory, and copy to the image only what a single-tuple fetch reads: the
 * item array and the item the bound lands on.  Copying the whole chunk costs
 * such a fetch more than its descent does.  Everything read is validated by
 * the page state, as partial_load_chunk() validates its copy.
 *
 * Slowpath when the chunk is not a run of fixed-format items; the caller
 * then loads it whole.
 */
OBTreeFastPathFindResult
fastpath_load_leaf_item(FastpathFindDownlinkMeta *meta,
						PartialPageState *partial, Page img,
						OffsetNumber chunkOffset, BTreePageItemLocator *loc)
{
	BTreePageHeader *imgHdr = (BTreePageHeader *) img;
	BTreePageHeader *srcHdr = (BTreePageHeader *) partial->src;
	uint64		imgState = pg_atomic_read_u64(&imgHdr->o_header.state);
	uint64		srcState;
	LocationIndex chunkBegin,
				chunkEnd;
	int			count,
				first;
	BTreePageItemLocator live;

	Assert(partial->isPartial && !partial->chunkIsLoaded[chunkOffset]);

	if (chunkOffset >= imgHdr->chunksCount)
		return OBTreeFastPathFindSlowpath;

	chunkBegin = SHORT_GET_LOCATION(srcHdr->chunkDesc[chunkOffset].shortLocation);
	if (chunkOffset + 1 < imgHdr->chunksCount)
	{
		chunkEnd = SHORT_GET_LOCATION(srcHdr->chunkDesc[chunkOffset + 1].shortLocation);
		count = srcHdr->chunkDesc[chunkOffset + 1].offset -
			srcHdr->chunkDesc[chunkOffset].offset;
	}
	else
	{
		chunkEnd = imgHdr->dataSize;
		count = imgHdr->itemsCount - srcHdr->chunkDesc[chunkOffset].offset;
	}

	pg_read_barrier();

	/* A descriptor torn by a concurrent change must not lead off the page */
	if (chunkBegin > chunkEnd || chunkEnd > ORIOLEDB_BLCKSZ ||
		count <= 0 || count > BTREE_PAGE_MAX_CHUNK_ITEMS)
		return OBTreeFastPathFindSlowpath;

	live.chunk = (BTreePageChunk *) ((Pointer) srcHdr + chunkBegin);
	live.chunkOffset = chunkOffset;
	live.chunkItemsCount = count;
	live.chunkSize = chunkEnd - chunkBegin;
	if (!fastpath_search_leaf_items(meta, &live))
		return OBTreeFastPathFindSlowpath;

	first = MAXALIGN(sizeof(LocationIndex) * count);
	memcpy((Pointer) img + chunkBegin, live.chunk, first);
	if (live.itemOffset < count)
		memcpy((Pointer) img + chunkBegin + first +
			   meta->leafStride * live.itemOffset,
			   (Pointer) live.chunk + first +
			   meta->leafStride * live.itemOffset,
			   meta->leafStride);

	pg_read_barrier();

	srcState = pg_atomic_read_u64(&srcHdr->o_header.state);
	if ((imgState & PAGE_STATE_CHANGE_COUNT_MASK) !=
		(srcState & PAGE_STATE_CHANGE_COUNT_MASK) ||
		O_PAGE_STATE_READ_IS_BLOCKED(srcState) ||
		O_PAGE_GET_CHANGE_COUNT(img) != O_PAGE_GET_CHANGE_COUNT(srcHdr))
		return OBTreeFastPathFindRetry;

	*loc = live;
	loc->chunk = (BTreePageChunk *) ((Pointer) img + chunkBegin);
	partial->itemOnlyChunk = chunkOffset;
	partial->itemOnlyOffset = live.itemOffset;
	return OBTreeFastPathFindOK;
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
			{
				bool		was_saving;

				was_saving = o_start_saving_inval_messages();
				arraySearchDescs[i].opcid = GetDefaultOpClass(typeid, BTREE_AM_OID);
				o_stop_saving_inval_messages(was_saving);
			}
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
			/*
			 * "None" is the leftmost (first storage slot), "Rightmost" the
			 * last one -- these are storage positions, independent of
			 * ASC/DESC.
			 */
			flags[i] = (keyType == BTreeKeyNone) ? FASTPATH_FIND_DOWNLINK_FLAG_FIRST : FASTPATH_FIND_DOWNLINK_FLAG_LAST;
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
		uint8		exclusiveFence = 0;

		for (i = 0; i < num; i++)
		{
			uint8		f = bound->keys[i].flags;

			if (exclusiveFence)
			{
				/*
				 * An exclusive value before this column has decided already:
				 * o_idx_cmp_range_key_to_value() does not look further once a
				 * value equals an exclusive bound.
				 */
				flags[i] = exclusiveFence;
				values[i] = (Datum) 0;
			}
			else if (f & O_VALUE_BOUND_UNBOUNDED)
			{
				/*
				 * An unbounded column's direction is already a storage
				 * direction: o_idx_cmp_key_bound_to_tuple() puts "lower"
				 * before every entry whatever the column's ASC/DESC.
				 */
				flags[i] = (f & O_VALUE_BOUND_LOWER) ?
					FASTPATH_FIND_DOWNLINK_FLAG_FIRST : FASTPATH_FIND_DOWNLINK_FLAG_LAST;
				values[i] = (Datum) 0;
			}
			else if (f & O_VALUE_BOUND_NULL)
			{
				/*
				 * A NULL bound sorts to one storage extreme according to the
				 * field's NULLS FIRST/LAST ordering, exactly as
				 * o_idx_cmp_range_key_to_value() resolves it.  NULLS FIRST
				 * puts NULLs in the first storage slot, NULLS LAST in the
				 * last -- independent of ASC/DESC.  Without this the bound
				 * would be searched as its (meaningless) raw value, sending
				 * the descent to the wrong end of the tree.
				 */
				flags[i] = id->fields[i].nullfirst ? FASTPATH_FIND_DOWNLINK_FLAG_FIRST : FASTPATH_FIND_DOWNLINK_FLAG_LAST;
				values[i] = (Datum) 0;
			}
			else
			{
				/*
				 * Only a value has a type to check: an unbounded or NULL
				 * bound carries none.
				 */
				if (bound->keys[i].type != types[i])
					return false;
				flags[i] = 0;
				values[i] = bound->keys[i].value;

				/*
				 * Equal to this value, an exclusive bound lies after the run
				 * of entries sharing it (lower) or before it (upper),
				 * whatever the columns after say: pin them there.
				 */
				if (!(f & O_VALUE_BOUND_INCLUSIVE))
				{
					if (f & O_VALUE_BOUND_LOWER)
						exclusiveFence = FASTPATH_FIND_DOWNLINK_FLAG_LAST;
					else if (f & O_VALUE_BOUND_UPPER)
						exclusiveFence = FASTPATH_FIND_DOWNLINK_FLAG_FIRST;
				}
			}
		}

		/*
		 * The bound may specify fewer columns than the key (e.g. "i = v" on a
		 * (i, pk) key).  Such a bound fences either just before or just after
		 * the whole run of entries sharing its specified prefix; represent
		 * that fence by pinning every unspecified trailing column to the
		 * run's first or last storage slot.  A lower-inclusive or
		 * upper-exclusive bound fences before the run (first slot), a
		 * lower-exclusive or upper-inclusive bound fences after it (last
		 * slot).  These are storage positions of the prefix run as a whole,
		 * so they do not depend on the trailing columns' ASC/DESC.  Leaving
		 * these columns unset would compare against garbage and could
		 * position the descent in the wrong child.
		 */
		if (num > 0 && num < numValues)
		{
			uint8		f = bound->keys[num - 1].flags;
			uint8		fence;

			if (exclusiveFence)
				fence = exclusiveFence;
			else if (((f & O_VALUE_BOUND_LOWER) != 0) == ((f & O_VALUE_BOUND_INCLUSIVE) != 0))
				fence = FASTPATH_FIND_DOWNLINK_FLAG_FIRST;
			else
				fence = FASTPATH_FIND_DOWNLINK_FLAG_LAST;

			for (i = num; i < numValues; i++)
			{
				flags[i] = fence;
				values[i] = (Datum) 0;
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
			flags[i] = (id->fields[i].nullfirst) ? FASTPATH_FIND_DOWNLINK_FLAG_FIRST : FASTPATH_FIND_DOWNLINK_FLAG_LAST;
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
	uint32		imagePageChangeCount = O_PAGE_GET_CHANGE_COUNT(imgHdr);
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
							&lower, &upper, meta->values[i], meta->ascending[i]);
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_FIRST)
			upper = lower;
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_LAST)
			lower = upper;
	}

	itemIndex = meta->inclusive ? lower : upper;

	pg_read_barrier();

	state = pg_atomic_read_u64(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount ||
		O_PAGE_GET_CHANGE_COUNT(hdr) != imagePageChangeCount)
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
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount ||
		O_PAGE_GET_CHANGE_COUNT(hdr) != imagePageChangeCount)
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
	uint32		imagePageChangeCount = O_PAGE_GET_CHANGE_COUNT(imgHdr);
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
							meta->values[i], meta->ascending[i]);
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_FIRST)
			upper = lower;
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_LAST)
			lower = upper;
	}

	*chunkIndex = meta->inclusive ? lower : upper;

	pg_read_barrier();

	/*
	 * Past every high key: on the rightmost page that is its last chunk,
	 * which has none; on any other the key may be past the page, and the
	 * rightlink may have to be visited.
	 */
	if (*chunkIndex >= count &&
		!(O_PAGE_IS(pagePtr, RIGHTMOST) && *chunkIndex == count &&
		  meta->rawOrderExact))
		return OBTreeFastPathFindSlowpath;

	state = pg_atomic_read_u64(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount ||
		O_PAGE_GET_CHANGE_COUNT(hdr) != imagePageChangeCount)
		return OBTreeFastPathFindRetry;

	return OBTreeFastPathFindOK;
}

/*
 * Narrow [*lower, *upper) of a sorted fixed-stride array of integers to the
 * run of items equal to the key.  The scan counts the items below the key
 * and those not above it over the whole range instead of stopping at the
 * first item above: no branch depends on the data, so there is no exit to
 * mispredict, and the loads, independent of each other, overlap.  The float
 * searches below keep their own scans for NaN ordering.
 */
#define ARRAY_COUNT_SEARCH_FUNC(fname, ctype, fromDatum) \
static void \
fname(Pointer p, int stride, int *lower, int *upper, Datum keyDatum, \
	  bool ascending) \
{ \
	ctype		key = fromDatum(keyDatum); \
	Pointer		q = p + (Size) *lower * stride; \
	Pointer		end = p + (Size) *upper * stride; \
	int			below = 0, \
				notAbove = 0; \
\
	if (ascending) \
	{ \
		for (; q < end; q += stride) \
		{ \
			ctype		v = *(ctype *) q; \
\
			below += (v < key); \
			notAbove += (v <= key); \
		} \
	} \
	else \
	{ \
		for (; q < end; q += stride) \
		{ \
			ctype		v = *(ctype *) q; \
\
			below += (v > key); \
			notAbove += (v >= key); \
		} \
	} \
	*upper = *lower + notAbove; \
	*lower += below; \
}

ARRAY_COUNT_SEARCH_FUNC(int4_array_search, int32, DatumGetInt32)
ARRAY_COUNT_SEARCH_FUNC(int8_array_search, int64, DatumGetInt64)
ARRAY_COUNT_SEARCH_FUNC(oid_array_search, Oid, DatumGetObjectId)

static void
float4_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum,
					bool ascending)
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
		else if (ascending ? value > key : value < key)
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
float8_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum,
					bool ascending)
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
		else if (ascending ? value > key : value < key)
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
tid_array_search(Pointer p, int stride, int *lower, int *upper, Datum keyDatum,
				 bool ascending)
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
		else if (ascending ? cmp > 0 : cmp < 0)
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

#ifdef FASTPATH_USE_AVX2

/*
 * The counting searches above, eight 32-bit items or four 64-bit ones at a
 * time: a gather picks them from their stride, and lanes past the range are
 * masked off, neither loaded nor counted.  Oids compare unsigned, which AVX2
 * does not: flipping the sign bit of both sides turns it into a signed
 * comparison.
 */
#define AVX2_COUNT32(fname, flip) \
__attribute__((target("avx2"))) \
static void \
fname(Pointer p, int stride, int *lower, int *upper, Datum keyDatum, \
	  bool ascending) \
{ \
	const __m256i lanes = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7); \
	const __m256i offsets = _mm256_mullo_epi32(lanes, _mm256_set1_epi32(stride)); \
	const __m256i bias = _mm256_set1_epi32((flip) ? (int32) 0x80000000 : 0); \
	__m256i		key = _mm256_xor_si256(_mm256_set1_epi32((int32) DatumGetUInt32(keyDatum)), bias); \
	int			lo = *lower, \
				hi = *upper, \
				i, \
				below = 0, \
				notAbove = 0; \
\
	for (i = lo; i < hi; i += 8) \
	{ \
		__m256i		valid = _mm256_cmpgt_epi32(_mm256_set1_epi32(hi - i), lanes); \
		__m256i		v = _mm256_mask_i32gather_epi32(_mm256_setzero_si256(), \
													(const int *) (p + (Size) i * stride), \
													offsets, valid, 1); \
		__m256i		lt, \
					gt; \
\
		v = _mm256_xor_si256(v, bias); \
		lt = _mm256_cmpgt_epi32(key, v); \
		gt = _mm256_cmpgt_epi32(v, key); \
		if (!ascending) \
		{ \
			__m256i		t = lt; \
\
			lt = gt; \
			gt = t; \
		} \
		below += __builtin_popcount(_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_and_si256(lt, valid)))); \
		notAbove += __builtin_popcount(_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_andnot_si256(gt, valid)))); \
	} \
	*upper = lo + notAbove; \
	*lower = lo + below; \
}

AVX2_COUNT32(int4_array_search_avx2, false)
AVX2_COUNT32(oid_array_search_avx2, true)

__attribute__((target("avx2")))
static void
int8_array_search_avx2(Pointer p, int stride, int *lower, int *upper,
					   Datum keyDatum, bool ascending)
{
	const		__m128i lanes = _mm_setr_epi32(0, 1, 2, 3);
	const		__m128i offsets = _mm_mullo_epi32(lanes, _mm_set1_epi32(stride));
	const		__m256i lanes64 = _mm256_setr_epi64x(0, 1, 2, 3);
	__m256i		key = _mm256_set1_epi64x(DatumGetInt64(keyDatum));
	int			lo = *lower,
				hi = *upper,
				i,
				below = 0,
				notAbove = 0;

	for (i = lo; i < hi; i += 4)
	{
		__m256i		valid = _mm256_cmpgt_epi64(_mm256_set1_epi64x(hi - i), lanes64);
		__m256i		v = _mm256_mask_i32gather_epi64(_mm256_setzero_si256(),
													(const long long *) (p + (Size) i * stride),
													offsets, valid, 1);
		__m256i		lt = _mm256_cmpgt_epi64(key, v),
					gt = _mm256_cmpgt_epi64(v, key);

		if (!ascending)
		{
			__m256i		t = lt;

			lt = gt;
			gt = t;
		}
		below += __builtin_popcount(_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_and_si256(lt, valid))));
		notAbove += __builtin_popcount(_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_andnot_si256(gt, valid))));
	}
	*upper = lo + notAbove;
	*lower = lo + below;
}

#endif							/* FASTPATH_USE_AVX2 */
