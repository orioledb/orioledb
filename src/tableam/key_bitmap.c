/*-------------------------------------------------------------------------
 *
 * key_bitmap.c
 *		Routines for bitmap scan of orioledb table.
 *
 *		The bitmap is a set of uint64 keys, stored densely: the key is split
 *		into a high part (the chunk, key >> OKBM_CHUNK_BITS) and a low part
 *		(OKBM_CHUNK_BITS bits).  Each chunk maps to a bitmap covering its
 *		OKBM_CHUNK_VALUES low-part offsets.  Chunks are held in an adaptive
 *		radix tree (include/lib/o_radixtree.h), which keeps them ordered so
 *		iteration and range queries are efficient.
 *
 *		Ordered seeks (o_keybitmap_range_is_valid / o_keybitmap_get_next) are
 *		served from a sorted array of chunk keys built lazily once the bitmap
 *		stops being mutated (the build phase always precedes the scan phase).
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/key_bitmap.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "tableam/bitmap_scan.h"

#include "utils/memutils.h"

#define OKBM_CHUNK_BITS		10
#define OKBM_CHUNK_VALUES	(UINT64CONST(1) << OKBM_CHUNK_BITS)
#define OKBM_LOW_MASK		(OKBM_CHUNK_VALUES - 1)
#define OKBM_BITMAP_BYTES	(OKBM_CHUNK_VALUES / 8)

typedef struct OKeyBitmapChunk
{
	uint8		bitmap[OKBM_BITMAP_BYTES];
} OKeyBitmapChunk;

#define RT_PREFIX okbm
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_VALUE_TYPE OKeyBitmapChunk
#include "lib/o_radixtree.h"

/*
 * Fixed-key mode: for composite / non-int primary keys the key is an
 * order-preserving byte string of OKBM_FIXED_BYTES bytes (see
 * include/tableam/bitmap_scan.h).  These are stored, un-densified, in a
 * fixed-length-key radix tree whose value carries no information.
 */
typedef struct OKbmDummy
{
	uint8		unused;
} OKbmDummy;

#define RT_PREFIX okbmf
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_KEY_SIZE OKBM_FIXED_BYTES
#define RT_VALUE_TYPE OKbmDummy
#include "lib/o_radixtree.h"

struct OKeyBitmap
{
	bool		fixed;			/* false: uint64 densified; true: fixed-key */

	okbm_radix_tree *tree;		/* uint64 mode */
	okbmf_radix_tree *ftree;	/* fixed-key mode */

	/* dedicated context owned by the radix tree (reset/freed by okbm*_free) */
	MemoryContext treeCxt;
	/* context holding this struct and the seek arrays */
	MemoryContext cxt;

	/*
	 * Sorted key arrays, built lazily by okbm_finalize() to serve ordered
	 * seeks.  Invalidated (finalized = false) on every mutation.  uint64 mode
	 * stores chunk keys in chunks[] and the matching per-chunk bitmaps in
	 * payloads[] (so the read path is self-contained -- no radix-tree lookup
	 * -- and the finalized form is a set of flat POD arrays that can be
	 * shared across parallel workers); fixed mode stores whole keys, each
	 * OKBM_FIXED_BYTES bytes, in fkeys[].
	 */
	uint64	   *chunks;
	OKeyBitmapChunk *payloads;
	uint8	   *fkeys;
	int			nchunks;
	int			chunksCapacity;
	bool		finalized;

	/*
	 * A shared (read-only) bitmap attached over a serialized buffer: chunks/
	 * payloads/fkeys point into that buffer, tree/ftree are NULL and
	 * finalized is true.  o_keybitmap_free() must not free the buffer-backed
	 * arrays.
	 */
	bool		shared;
};

static void okbm_finalize(OKeyBitmap *bm);
static int	okbm_lower_bound(OKeyBitmap *bm, uint64 target);
static int	okbmf_lower_bound(OKeyBitmap *bm, const uint8 *target);

/*
 * Return the first set bit offset >= minOffset within a chunk bitmap, or -1.
 */
static int
find_next_offset(const uint8 *bitmap, int minOffset)
{
	int			i;
	uint8		mask;

	i = minOffset >> 3;
	mask = 0xFF << (minOffset & 7);
	while (i < OKBM_BITMAP_BYTES)
	{
		mask &= bitmap[i];
		if (mask)
		{
			int			result;

			result = i << 3;
			while (!(mask & 1))
			{
				result++;
				mask >>= 1;
			}
			return result;
		}
		mask = 0xFF;
		i++;
	}
	return -1;
}

OKeyBitmap *
o_keybitmap_create(void)
{
	OKeyBitmap *bm = palloc0(sizeof(OKeyBitmap));

	/* okbm_memory_usage() is part of the generated API but unused here */
	(void) okbm_memory_usage;

	bm->cxt = CurrentMemoryContext;

	/*
	 * The radix tree owns the context it is created in: okbm_free() resets it
	 * and deletes its child contexts.  Give it a dedicated child context so
	 * that freeing the tree does not clobber this struct or the chunks array,
	 * which live in bm->cxt.
	 */
	bm->treeCxt = AllocSetContextCreate(bm->cxt, "o_keybitmap radix tree",
										ALLOCSET_SMALL_SIZES);
	bm->fixed = false;
	bm->tree = okbm_create(bm->treeCxt);
	bm->ftree = NULL;
	bm->chunks = NULL;
	bm->payloads = NULL;
	bm->fkeys = NULL;
	bm->nchunks = 0;
	bm->chunksCapacity = 0;
	bm->finalized = false;
	bm->shared = false;
	return bm;
}

OKeyBitmap *
o_keybitmap_create_fixed(void)
{
	OKeyBitmap *bm = palloc0(sizeof(OKeyBitmap));

	/* okbmf_memory_usage() is part of the generated API but unused here */
	(void) okbmf_memory_usage;

	bm->cxt = CurrentMemoryContext;
	bm->treeCxt = AllocSetContextCreate(bm->cxt, "o_keybitmap radix tree",
										ALLOCSET_SMALL_SIZES);
	bm->fixed = true;
	bm->tree = NULL;
	bm->ftree = okbmf_create(bm->treeCxt);
	bm->chunks = NULL;
	bm->payloads = NULL;
	bm->fkeys = NULL;
	bm->nchunks = 0;
	bm->chunksCapacity = 0;
	bm->finalized = false;
	bm->shared = false;
	return bm;
}

void
o_keybitmap_free(OKeyBitmap *bm)
{
	if (bm->shared)
	{
		/* Arrays live in the shared buffer; only the wrapper is ours. */
		pfree(bm);
		return;
	}

	if (bm->fixed)
		okbmf_free(bm->ftree);
	else
		okbm_free(bm->tree);
	MemoryContextDelete(bm->treeCxt);
	if (bm->chunks)
		pfree(bm->chunks);
	if (bm->payloads)
		pfree(bm->payloads);
	if (bm->fkeys)
		pfree(bm->fkeys);
	pfree(bm);
}

/* --- fixed-key mode helpers --- */

static inline okbmf_key
okbmf_mkkey(const uint8 *key)
{
	okbmf_key	k;

	memcpy(k.data, key, OKBM_FIXED_BYTES);
	return k;
}

void
o_keybitmap_insert_key(OKeyBitmap *bm, const uint8 *key)
{
	okbmf_key	k = okbmf_mkkey(key);

	Assert(bm->fixed);
	if (okbmf_find(bm->ftree, k) == NULL)
	{
		OKbmDummy	dummy = {0};

		(void) okbmf_set(bm->ftree, k, &dummy);
	}
	bm->finalized = false;
}

bool
o_keybitmap_test_key(OKeyBitmap *bm, const uint8 *key)
{
	int			idx;

	Assert(bm->fixed);
	okbm_finalize(bm);

	idx = okbmf_lower_bound(bm, key);
	return idx < bm->nchunks &&
		memcmp(bm->fkeys + (Size) idx * OKBM_FIXED_BYTES, key,
			   OKBM_FIXED_BYTES) == 0;
}

/*
 * Insert key and report whether it was newly added.  Used as a dedup
 * test-and-set while streaming a BitmapOr of primary scans: a true return
 * means this is the first time the key is seen (emit it), false means a prior
 * branch already produced it (skip).
 */
bool
o_keybitmap_emit_key(OKeyBitmap *bm, const uint8 *key)
{
	okbmf_key	k = okbmf_mkkey(key);
	OKbmDummy	dummy = {0};

	Assert(bm->fixed);
	if (okbmf_find(bm->ftree, k) != NULL)
		return false;
	(void) okbmf_set(bm->ftree, k, &dummy);
	bm->finalized = false;
	return true;
}

void
o_keybitmap_insert(OKeyBitmap *bm, uint64 value)
{
	uint64		chunk = value >> OKBM_CHUNK_BITS;
	int			offset = value & OKBM_LOW_MASK;
	OKeyBitmapChunk *entry = okbm_find(bm->tree, chunk);

	if (entry == NULL)
	{
		OKeyBitmapChunk newentry;

		memset(&newentry, 0, sizeof(newentry));
		newentry.bitmap[offset >> 3] |= (1 << (offset & 7));
		(void) okbm_set(bm->tree, chunk, &newentry);
	}
	else
		entry->bitmap[offset >> 3] |= (1 << (offset & 7));

	bm->finalized = false;
}

bool
o_keybitmap_test(OKeyBitmap *bm, uint64 value)
{
	uint64		chunk = value >> OKBM_CHUNK_BITS;
	int			offset = value & OKBM_LOW_MASK;
	int			idx;

	okbm_finalize(bm);

	idx = okbm_lower_bound(bm, chunk);
	if (idx >= bm->nchunks || bm->chunks[idx] != chunk)
		return false;

	return (bm->payloads[idx].bitmap[offset >> 3] & (1 << (offset & 7))) != 0;
}

/* uint64 variant of o_keybitmap_emit_key(): insert, return true if newly added. */
bool
o_keybitmap_emit(OKeyBitmap *bm, uint64 value)
{
	uint64		chunk = value >> OKBM_CHUNK_BITS;
	int			offset = value & OKBM_LOW_MASK;
	int			byte = offset >> 3;
	uint8		mask = 1 << (offset & 7);
	OKeyBitmapChunk *entry = okbm_find(bm->tree, chunk);

	if (entry == NULL)
	{
		OKeyBitmapChunk newentry;

		memset(&newentry, 0, sizeof(newentry));
		newentry.bitmap[byte] |= mask;
		(void) okbm_set(bm->tree, chunk, &newentry);
		bm->finalized = false;
		return true;
	}
	if (entry->bitmap[byte] & mask)
		return false;
	entry->bitmap[byte] |= mask;
	bm->finalized = false;
	return true;
}

bool
o_keybitmap_is_empty(OKeyBitmap *bm)
{
	bool		empty;

	if (bm->fixed)
	{
		okbmf_iter *iter = okbmf_begin_iterate(bm->ftree);
		okbmf_key	k;

		empty = (okbmf_iterate_next(iter, &k) == NULL);
		okbmf_end_iterate(iter);
	}
	else
	{
		okbm_iter  *iter = okbm_begin_iterate(bm->tree);
		uint64		chunk;

		empty = (okbm_iterate_next(iter, &chunk) == NULL);
		okbm_end_iterate(iter);
	}
	return empty;
}

void
o_keybitmap_union(OKeyBitmap *a, OKeyBitmap *b)
{
	okbm_iter  *iter;
	OKeyBitmapChunk *bentry;
	uint64		chunk;

	Assert(a->fixed == b->fixed);

	if (a->fixed)
	{
		okbmf_iter *fiter = okbmf_begin_iterate(b->ftree);
		okbmf_key	k;

		while (okbmf_iterate_next(fiter, &k) != NULL)
		{
			if (okbmf_find(a->ftree, k) == NULL)
			{
				OKbmDummy	dummy = {0};

				(void) okbmf_set(a->ftree, k, &dummy);
			}
		}
		okbmf_end_iterate(fiter);
		a->finalized = false;
		return;
	}

	iter = okbm_begin_iterate(b->tree);

	while ((bentry = okbm_iterate_next(iter, &chunk)) != NULL)
	{
		OKeyBitmapChunk *aentry = okbm_find(a->tree, chunk);

		if (aentry == NULL)
			(void) okbm_set(a->tree, chunk, bentry);
		else
		{
			int			i;

			for (i = 0; i < OKBM_BITMAP_BYTES; i++)
				aentry->bitmap[i] |= bentry->bitmap[i];
		}
	}
	okbm_end_iterate(iter);
	a->finalized = false;
}

void
o_keybitmap_intersect(OKeyBitmap *a, OKeyBitmap *b)
{
	okbm_iter  *iter;
	OKeyBitmapChunk *aentry;
	uint64		chunk;
	uint64	   *toDelete = NULL;
	int			nDelete = 0;
	int			deleteCap = 0;
	int			i;

	Assert(a->fixed == b->fixed);

	if (a->fixed)
	{
		okbmf_iter *fiter = okbmf_begin_iterate(a->ftree);
		okbmf_key	k;
		okbmf_key  *fdel = NULL;
		int			nfdel = 0;
		int			fcap = 0;

		while (okbmf_iterate_next(fiter, &k) != NULL)
		{
			if (okbmf_find(b->ftree, k) == NULL)
			{
				if (nfdel >= fcap)
				{
					fcap = fcap ? fcap * 2 : 16;
					if (fdel == NULL)
						fdel = MemoryContextAlloc(a->cxt, sizeof(okbmf_key) * fcap);
					else
						fdel = repalloc(fdel, sizeof(okbmf_key) * fcap);
				}
				fdel[nfdel++] = k;
			}
		}
		okbmf_end_iterate(fiter);

		for (i = 0; i < nfdel; i++)
			(void) okbmf_delete(a->ftree, fdel[i]);
		if (fdel)
			pfree(fdel);

		a->finalized = false;
		return;
	}

	iter = okbm_begin_iterate(a->tree);

	/*
	 * AND each of a's chunks in place with the matching chunk of b.  Chunks
	 * that become empty (or have no counterpart in b) are collected and
	 * removed after iteration; deleting during iteration would invalidate the
	 * iterator.  Modifying leaf values in place is safe.
	 */
	while ((aentry = okbm_iterate_next(iter, &chunk)) != NULL)
	{
		OKeyBitmapChunk *bentry = okbm_find(b->tree, chunk);
		bool		empty = true;

		if (bentry != NULL)
		{
			for (i = 0; i < OKBM_BITMAP_BYTES; i++)
			{
				aentry->bitmap[i] &= bentry->bitmap[i];
				if (aentry->bitmap[i] != 0)
					empty = false;
			}
		}

		if (empty)
		{
			if (nDelete >= deleteCap)
			{
				deleteCap = deleteCap ? deleteCap * 2 : 16;
				if (toDelete == NULL)
					toDelete = MemoryContextAlloc(a->cxt,
												  sizeof(uint64) * deleteCap);
				else
					toDelete = repalloc(toDelete, sizeof(uint64) * deleteCap);
			}
			toDelete[nDelete++] = chunk;
		}
	}
	okbm_end_iterate(iter);

	for (i = 0; i < nDelete; i++)
		(void) okbm_delete(a->tree, toDelete[i]);
	if (toDelete)
		pfree(toDelete);

	a->finalized = false;
}

/*
 * Build the sorted array of chunk keys.  okbm iteration already yields chunks
 * in ascending order, so we just collect them.  No-op if already finalized.
 */
static void
okbm_finalize(OKeyBitmap *bm)
{
	okbm_iter  *iter;
	uint64		chunk;

	if (bm->finalized)
		return;

	bm->nchunks = 0;

	if (bm->fixed)
	{
		okbmf_iter *fiter = okbmf_begin_iterate(bm->ftree);
		okbmf_key	k;

		while (okbmf_iterate_next(fiter, &k) != NULL)
		{
			if (bm->nchunks >= bm->chunksCapacity)
			{
				bm->chunksCapacity = bm->chunksCapacity ? bm->chunksCapacity * 2 : 64;
				if (bm->fkeys == NULL)
					bm->fkeys = MemoryContextAlloc(bm->cxt,
												   (Size) OKBM_FIXED_BYTES * bm->chunksCapacity);
				else
					bm->fkeys = repalloc(bm->fkeys,
										 (Size) OKBM_FIXED_BYTES * bm->chunksCapacity);
			}
			memcpy(bm->fkeys + (Size) bm->nchunks * OKBM_FIXED_BYTES,
				   k.data, OKBM_FIXED_BYTES);
			bm->nchunks++;
		}
		okbmf_end_iterate(fiter);
		bm->finalized = true;
		return;
	}

	iter = okbm_begin_iterate(bm->tree);
	{
		OKeyBitmapChunk *entry;

		while ((entry = okbm_iterate_next(iter, &chunk)) != NULL)
		{
			if (bm->nchunks >= bm->chunksCapacity)
			{
				bm->chunksCapacity = bm->chunksCapacity ? bm->chunksCapacity * 2 : 64;
				if (bm->chunks == NULL)
				{
					bm->chunks = MemoryContextAlloc(bm->cxt,
													sizeof(uint64) * bm->chunksCapacity);
					bm->payloads = MemoryContextAlloc(bm->cxt,
													  sizeof(OKeyBitmapChunk) * bm->chunksCapacity);
				}
				else
				{
					bm->chunks = repalloc(bm->chunks,
										  sizeof(uint64) * bm->chunksCapacity);
					bm->payloads = repalloc(bm->payloads,
											sizeof(OKeyBitmapChunk) * bm->chunksCapacity);
				}
			}
			bm->chunks[bm->nchunks] = chunk;
			memcpy(&bm->payloads[bm->nchunks], entry, sizeof(OKeyBitmapChunk));
			bm->nchunks++;
		}
	}
	okbm_end_iterate(iter);

	bm->finalized = true;
}

/* Index of the first chunk key >= target. */
static int
okbm_lower_bound(OKeyBitmap *bm, uint64 target)
{
	int			lo = 0,
				hi = bm->nchunks;

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (bm->chunks[mid] < target)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

bool
o_keybitmap_range_is_valid(OKeyBitmap *bm, uint64 low, uint64 high)
{
	uint64		chunkLow;
	uint64		chunkHigh;
	int			idx;

	if (high <= low)
		return false;

	okbm_finalize(bm);

	chunkLow = low >> OKBM_CHUNK_BITS;
	chunkHigh = (high - 1) >> OKBM_CHUNK_BITS;

	for (idx = okbm_lower_bound(bm, chunkLow); idx < bm->nchunks; idx++)
	{
		uint64		chunk = bm->chunks[idx];
		OKeyBitmapChunk *entry;
		int			iStart,
					iEnd,
					i;
		uint8		startMask,
					endMask;

		if (chunk > chunkHigh)
			break;

		entry = &bm->payloads[idx];

		if (chunk == chunkLow)
		{
			iStart = (low & OKBM_LOW_MASK) >> 3;
			startMask = 0xFF << (low & 7);
		}
		else
		{
			iStart = 0;
			startMask = 0xFF;
		}

		if (chunk == chunkHigh)
		{
			iEnd = ((high - 1) & OKBM_LOW_MASK) >> 3;
			endMask = 0xFF >> (7 - ((high - 1) & 7));
		}
		else
		{
			iEnd = OKBM_BITMAP_BYTES - 1;
			endMask = 0xFF;
		}

		for (i = iStart; i <= iEnd; i++)
		{
			uint8		mask = (i == iStart) ? startMask : 0xFF;

			if (i == iEnd)
				mask &= endMask;

			if (entry->bitmap[i] & mask)
				return true;
		}
	}

	return false;
}

uint64
o_keybitmap_get_next(OKeyBitmap *bm, uint64 prev, bool *found)
{
	uint64		chunkPrev = prev >> OKBM_CHUNK_BITS;
	int			offPrev = prev & OKBM_LOW_MASK;
	int			idx;

	okbm_finalize(bm);

	for (idx = okbm_lower_bound(bm, chunkPrev); idx < bm->nchunks; idx++)
	{
		uint64		chunk = bm->chunks[idx];
		OKeyBitmapChunk *entry = &bm->payloads[idx];
		int			startOff = (chunk == chunkPrev) ? offPrev : 0;
		int			nextOff;

		nextOff = find_next_offset(entry->bitmap, startOff);

		if (nextOff >= 0)
		{
			*found = true;
			return (chunk << OKBM_CHUNK_BITS) + nextOff;
		}
	}

	*found = false;
	return 0;
}

/* --- fixed-key mode ordered seeks --- */

/* Index of the first key >= target (memcmp order) in the finalized fkeys[]. */
static int
okbmf_lower_bound(OKeyBitmap *bm, const uint8 *target)
{
	int			lo = 0,
				hi = bm->nchunks;

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (memcmp(bm->fkeys + (Size) mid * OKBM_FIXED_BYTES, target,
				   OKBM_FIXED_BYTES) < 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

bool
o_keybitmap_range_is_valid_key(OKeyBitmap *bm, const uint8 *low, const uint8 *high)
{
	int			idx;

	Assert(bm->fixed);
	if (memcmp(low, high, OKBM_FIXED_BYTES) >= 0)
		return false;

	okbm_finalize(bm);

	idx = okbmf_lower_bound(bm, low);
	if (idx >= bm->nchunks)
		return false;

	/* the first key >= low is in range iff it is < high */
	return memcmp(bm->fkeys + (Size) idx * OKBM_FIXED_BYTES, high,
				  OKBM_FIXED_BYTES) < 0;
}

bool
o_keybitmap_get_next_key(OKeyBitmap *bm, const uint8 *prev, uint8 *result)
{
	int			idx;

	Assert(bm->fixed);
	okbm_finalize(bm);

	idx = okbmf_lower_bound(bm, prev);
	if (idx >= bm->nchunks)
		return false;

	memcpy(result, bm->fkeys + (Size) idx * OKBM_FIXED_BYTES, OKBM_FIXED_BYTES);
	return true;
}

/* --- serialization (for sharing a finalized bitmap across workers) --- */

/*
 * On-buffer header.  The flat arrays follow at MAXALIGN(sizeof(header)):
 *   uint64 mode: uint64 chunks[nchunks]; OKeyBitmapChunk payloads[nchunks];
 *   fixed mode:  uint8  fkeys[nchunks * OKBM_FIXED_BYTES];
 * The layout contains no pointers, so it can be copied into shared memory and
 * attached (o_keybitmap_attach) at a different mapping address per worker.
 */
typedef struct OKeyBitmapSerialHeader
{
	int32		nchunks;
	bool		fixed;
} OKeyBitmapSerialHeader;

#define OKBM_SERIAL_BODY_OFFSET MAXALIGN(sizeof(OKeyBitmapSerialHeader))

Size
o_keybitmap_serialized_size(OKeyBitmap *bm)
{
	Size		sz = OKBM_SERIAL_BODY_OFFSET;

	okbm_finalize(bm);

	if (bm->fixed)
		sz += (Size) bm->nchunks * OKBM_FIXED_BYTES;
	else
		sz += (Size) bm->nchunks * (sizeof(uint64) + sizeof(OKeyBitmapChunk));
	return sz;
}

void
o_keybitmap_serialize(OKeyBitmap *bm, void *buf)
{
	OKeyBitmapSerialHeader *hdr = (OKeyBitmapSerialHeader *) buf;
	char	   *body = (char *) buf + OKBM_SERIAL_BODY_OFFSET;

	okbm_finalize(bm);

	hdr->nchunks = bm->nchunks;
	hdr->fixed = bm->fixed;

	if (bm->fixed)
	{
		if (bm->nchunks > 0)
			memcpy(body, bm->fkeys, (Size) bm->nchunks * OKBM_FIXED_BYTES);
	}
	else if (bm->nchunks > 0)
	{
		memcpy(body, bm->chunks, (Size) bm->nchunks * sizeof(uint64));
		memcpy(body + (Size) bm->nchunks * sizeof(uint64), bm->payloads,
			   (Size) bm->nchunks * sizeof(OKeyBitmapChunk));
	}
}

/*
 * Attach a read-only bitmap over a buffer produced by o_keybitmap_serialize().
 * The wrapper is allocated in `cxt` and holds pointers into `buf` (valid for
 * the caller's mapping of the shared segment); o_keybitmap_free() drops only
 * the wrapper.
 */
OKeyBitmap *
o_keybitmap_attach(void *buf, MemoryContext cxt)
{
	OKeyBitmapSerialHeader *hdr = (OKeyBitmapSerialHeader *) buf;
	char	   *body = (char *) buf + OKBM_SERIAL_BODY_OFFSET;
	OKeyBitmap *bm = MemoryContextAllocZero(cxt, sizeof(OKeyBitmap));

	bm->cxt = cxt;
	bm->treeCxt = NULL;
	bm->tree = NULL;
	bm->ftree = NULL;
	bm->fixed = hdr->fixed;
	bm->nchunks = hdr->nchunks;
	bm->chunksCapacity = 0;
	bm->finalized = true;
	bm->shared = true;

	if (bm->fixed)
	{
		bm->chunks = NULL;
		bm->payloads = NULL;
		bm->fkeys = (uint8 *) body;
	}
	else
	{
		bm->chunks = (uint64 *) body;
		bm->payloads = (OKeyBitmapChunk *)
			(body + (Size) bm->nchunks * sizeof(uint64));
		bm->fkeys = NULL;
	}
	return bm;
}
