/*-------------------------------------------------------------------------
 *
 * radix_selftest.c
 *		Runtime self-test for the fixed-length-key variant of the vendored
 *		radix tree (include/lib/o_radixtree.h with RT_KEY_SIZE).
 *
 *		Exposed as SQL function orioledb_radixtree_selftest(nkeys int).
 *		Returns 'ok' on success or a description of the first failure.
 *		Dev-only helper; not part of the shipped SQL.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "fmgr.h"
#include "lib/qunique.h"
#include "tableam/bitmap_scan.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

/* Two instances: a multiple-of-8 length and an odd length. */
#define RT_PREFIX rtst12
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_KEY_SIZE 12
typedef struct
{
	uint32		id;
} RtstVal;
#define RT_VALUE_TYPE RtstVal
#include "lib/o_radixtree.h"

#define RT_PREFIX rtst5
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_KEY_SIZE 5
#define RT_VALUE_TYPE RtstVal
#include "lib/o_radixtree.h"

PG_FUNCTION_INFO_V1(orioledb_radixtree_selftest);
PG_FUNCTION_INFO_V1(orioledb_encode_selftest);

/*
 * Order-preserving big-endian encoding of an unsigned integer of nbytes into
 * out.  For signed values the caller flips the sign bit first, so that the
 * unsigned byte ordering matches signed ordering.
 */
static void
enc_be(uint64 u, int nbytes, uint8 *out)
{
	int			i;

	for (i = nbytes - 1; i >= 0; i--)
	{
		out[i] = (uint8) (u & 0xFF);
		u >>= 8;
	}
}

#define ENC_MAX 32

/* one test tuple: (int4 a, int8 b, int2 c) */
typedef struct
{
	int32		a;
	int64		b;
	int16		c;
} EncTuple;

static int
enc_tuple_cmp(const void *x, const void *y)
{
	const EncTuple *p = x;
	const EncTuple *q = y;

	if (p->a != q->a)
		return p->a < q->a ? -1 : 1;
	if (p->b != q->b)
		return p->b < q->b ? -1 : 1;
	if (p->c != q->c)
		return p->c < q->c ? -1 : 1;
	return 0;
}

/*
 * Encode (a,b,c) order-preservingly, right-aligned into a fixed ENC_MAX-byte
 * buffer with a zero high-pad, matching the composite-PK keybitmap scheme.
 */
static void
enc_tuple(const EncTuple *t, uint8 *out)
{
	int			off;

	memset(out, 0, ENC_MAX);
	off = ENC_MAX - (4 + 8 + 2);
	enc_be((uint32) t->a ^ UINT64CONST(0x80000000), 4, out + off);
	enc_be((uint64) t->b ^ UINT64CONST(0x8000000000000000), 8, out + off + 4);
	enc_be((uint16) t->c ^ 0x8000, 2, out + off + 12);
}

Datum
orioledb_encode_selftest(PG_FUNCTION_ARGS)
{
	int			nkeys = PG_GETARG_INT32(0);
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext,
											  "enc", ALLOCSET_DEFAULT_SIZES);
	MemoryContext old = MemoryContextSwitchTo(cxt);
	EncTuple   *tuples = palloc(sizeof(EncTuple) * nkeys);
	uint8	   *encs;
	uint64		rng = UINT64CONST(0xdeadbeefcafef00d);
	int			i;
	char	   *err = NULL;

	for (i = 0; i < nkeys; i++)
	{
		rng = rng * UINT64CONST(6364136223846793005) + 1;
		tuples[i].a = (int32) (rng >> 33) - 100;	/* include negatives */
		rng = rng * UINT64CONST(6364136223846793005) + 1;
		tuples[i].b = (int64) (rng >> 20) - 1000000;
		rng = rng * UINT64CONST(6364136223846793005) + 1;
		tuples[i].c = (int16) ((rng >> 40) % 200) - 100;
	}

	qsort(tuples, nkeys, sizeof(EncTuple), enc_tuple_cmp);

	encs = palloc(ENC_MAX * nkeys);
	for (i = 0; i < nkeys; i++)
		enc_tuple(&tuples[i], encs + (size_t) i * ENC_MAX);

	/*
	 * encodings must be non-decreasing, and strictly increasing iff tuples
	 * differ
	 */
	for (i = 1; i < nkeys; i++)
	{
		int			tc = enc_tuple_cmp(&tuples[i - 1], &tuples[i]);
		int			ec = memcmp(encs + (size_t) (i - 1) * ENC_MAX,
								encs + (size_t) i * ENC_MAX, ENC_MAX);

		if ((tc == 0 && ec != 0) || (tc < 0 && ec >= 0) || (tc > 0))
		{
			err = psprintf("order mismatch at %d: tuplecmp=%d enccmp=%d", i, tc, ec);
			break;
		}
	}

	if (err)
		err = MemoryContextStrdup(old, err);
	MemoryContextSwitchTo(old);
	MemoryContextDelete(cxt);

	PG_RETURN_TEXT_P(cstring_to_text(err ? err : "ok"));
}

static int
rtst_memcmp(const void *a, const void *b, void *arg)
{
	size_t		n = *((size_t *) arg);

	return memcmp(a, b, n);
}

/*
 * Generate `count` pseudo-random keys of `n` bytes into `buf` using a
 * deterministic LCG seeded from `seed`, so runs are reproducible.
 */
static void
rtst_gen(unsigned char *buf, int count, int n, uint64 seed)
{
	uint64		rng = seed;
	int			i,
				b;

	for (i = 0; i < count; i++)
	{
		for (b = 0; b < n; b++)
		{
			rng = rng * UINT64CONST(6364136223846793005) +
				UINT64CONST(1442695040888963407);
			buf[i * n + b] = (unsigned char) (rng >> 33);
		}
	}
}

/*
 * Body of the test for a given instance/length.  Returns NULL on success or a
 * palloc'd failure description.  Generated per (PREFIX, N) so the exact same
 * logic exercises every key length.
 */
#define DEFINE_RTST_RUN(PREFIX, N)											\
static char *																\
PREFIX##_run(int nkeys, uint64 seed)										\
{																			\
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext,			\
											  "rtst", ALLOCSET_DEFAULT_SIZES); \
	MemoryContext old = MemoryContextSwitchTo(cxt);							\
	PREFIX##_radix_tree *tree = PREFIX##_create(cxt);						\
	size_t		nsz = (N);												\
	unsigned char *keys = palloc((Size) nkeys * (N));					\
	unsigned char *sorted = palloc((Size) nkeys * (N));					\
	unsigned char prev[(N)];											\
	int			i;														\
	int			ndistinct;												\
	int			cnt;													\
	char	   *err = NULL;											\
	PREFIX##_iter *it;													\
	PREFIX##_key ik;													\
	RtstVal    *pv;														\
																			\
	/* silence unused-function for API entry points this test doesn't call */ \
	(void) PREFIX##_free;												\
	(void) PREFIX##_memory_usage;										\
	rtst_gen(keys, nkeys, (N), seed);									\
	for (i = 0; i < nkeys; i++)											\
	{																	\
		PREFIX##_key k;												\
		RtstVal		v;											\
		memcpy(k.data, keys + (size_t) i * (N), (N));				\
		v.id = i;													\
		(void) PREFIX##_set(tree, k, &v);							\
	}																	\
	/* find roundtrip */												\
	for (i = 0; i < nkeys; i++)											\
	{																	\
		PREFIX##_key k;												\
		memcpy(k.data, keys + (size_t) i * (N), (N));				\
		pv = PREFIX##_find(tree, k);								\
		if (pv == NULL)												\
		{ err = psprintf("N=%d find missing at %d", (N), i); goto done; } \
		if (memcmp(keys + (size_t) pv->id * (N), k.data, (N)) != 0)	\
		{ err = psprintf("N=%d find wrong value at %d", (N), i); goto done; } \
	}																	\
	/* build sorted-distinct reference */								\
	memcpy(sorted, keys, (size_t) nkeys * (N));						\
	qsort_arg(sorted, nkeys, (N), rtst_memcmp, &nsz);				\
	ndistinct = qunique_arg(sorted, nkeys, (N), rtst_memcmp, &nsz);	\
	/* iterate: strictly ascending, matches sorted-distinct exactly */	\
	it = PREFIX##_begin_iterate(tree);									\
	cnt = 0;															\
	while ((pv = PREFIX##_iterate_next(it, &ik)) != NULL)				\
	{																	\
		if (cnt > 0 && memcmp(prev, ik.data, (N)) >= 0)				\
		{ err = psprintf("N=%d iter not ascending at %d", (N), cnt); PREFIX##_end_iterate(it); goto done; } \
		if (memcmp(sorted + (size_t) cnt * (N), ik.data, (N)) != 0)	\
		{ err = psprintf("N=%d iter != sorted at %d", (N), cnt); PREFIX##_end_iterate(it); goto done; } \
		if (memcmp(keys + (size_t) pv->id * (N), ik.data, (N)) != 0)	\
		{ err = psprintf("N=%d iter value mismatch at %d", (N), cnt); PREFIX##_end_iterate(it); goto done; } \
		memcpy(prev, ik.data, (N));									\
		cnt++;														\
	}																	\
	PREFIX##_end_iterate(it);											\
	if (cnt != ndistinct)												\
	{ err = psprintf("N=%d iter count %d != distinct %d", (N), cnt, ndistinct); goto done; } \
	/* delete half, re-check find */									\
	for (i = 0; i < nkeys; i += 2)										\
	{																	\
		PREFIX##_key k;												\
		memcpy(k.data, keys + (size_t) i * (N), (N));				\
		(void) PREFIX##_delete(tree, k);							\
	}																	\
done:																	\
	if (err)															\
		err = MemoryContextStrdup(old, err);							\
	MemoryContextSwitchTo(old);											\
	MemoryContextDelete(cxt);											\
	return err;															\
}

DEFINE_RTST_RUN(rtst12, 12)
DEFINE_RTST_RUN(rtst5, 5)

Datum
orioledb_radixtree_selftest(PG_FUNCTION_ARGS)
{
	int			nkeys = PG_GETARG_INT32(0);
	char	   *err;

	err = rtst12_run(nkeys, UINT64CONST(0x9e3779b97f4a7c15));
	if (err)
		PG_RETURN_TEXT_P(cstring_to_text(err));
	err = rtst5_run(nkeys, UINT64CONST(0x123456789abcdef0));
	if (err)
		PG_RETURN_TEXT_P(cstring_to_text(err));

	PG_RETURN_TEXT_P(cstring_to_text("ok"));
}

PG_FUNCTION_INFO_V1(orioledb_keybitmap_selftest);

static int
fkey_cmp(const void *a, const void *b, void *arg)
{
	return memcmp(a, b, OKBM_FIXED_BYTES);
}

/* big-endian increment of an OKBM_FIXED_BYTES key; returns false on overflow */
static bool
fkey_inc(uint8 *key)
{
	int			i;

	for (i = OKBM_FIXED_BYTES - 1; i >= 0; i--)
	{
		if (++key[i] != 0)
			return true;
	}
	return false;
}

Datum
orioledb_keybitmap_selftest(PG_FUNCTION_ARGS)
{
	int			nkeys = PG_GETARG_INT32(0);
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext,
											  "kbm", ALLOCSET_DEFAULT_SIZES);
	MemoryContext old = MemoryContextSwitchTo(cxt);
	uint8	   *keys = palloc((Size) nkeys * OKBM_FIXED_BYTES);
	uint8	   *sorted;
	uint64		rng = UINT64CONST(0x51ed270b);
	OKeyBitmap *bm = o_keybitmap_create_fixed();
	OKeyBitmap *bm2 = o_keybitmap_create_fixed();
	size_t		nsz = OKBM_FIXED_BYTES;
	int			i,
				ndistinct,
				cnt;
	uint8		cur[OKBM_FIXED_BYTES];
	uint8		out[OKBM_FIXED_BYTES];
	char	   *err = NULL;

	/* random keys, using only the low 12 bytes so collisions/order both occur */
	for (i = 0; i < nkeys; i++)
	{
		int			b;

		memset(keys + (size_t) i * OKBM_FIXED_BYTES, 0, OKBM_FIXED_BYTES);
		for (b = OKBM_FIXED_BYTES - 12; b < OKBM_FIXED_BYTES; b++)
		{
			rng = rng * UINT64CONST(6364136223846793005) + 1;
			keys[(size_t) i * OKBM_FIXED_BYTES + b] = (uint8) (rng >> 40);
		}
		o_keybitmap_insert_key(bm, keys + (size_t) i * OKBM_FIXED_BYTES);
	}

	for (i = 0; i < nkeys; i++)
		if (!o_keybitmap_test_key(bm, keys + (size_t) i * OKBM_FIXED_BYTES))
		{
			err = psprintf("test_key missing at %d", i);
			goto done;
		}

	sorted = palloc((Size) nkeys * OKBM_FIXED_BYTES);
	memcpy(sorted, keys, (Size) nkeys * OKBM_FIXED_BYTES);
	qsort_arg(sorted, nkeys, OKBM_FIXED_BYTES, fkey_cmp, &nsz);
	ndistinct = qunique_arg(sorted, nkeys, OKBM_FIXED_BYTES, fkey_cmp, &nsz);

	/* ordered walk via get_next_key must reproduce sorted-distinct exactly */
	memset(cur, 0, OKBM_FIXED_BYTES);
	cnt = 0;
	while (o_keybitmap_get_next_key(bm, cur, out))
	{
		if (cnt >= ndistinct)
		{
			err = psprintf("walk overran distinct=%d", ndistinct);
			goto done;
		}
		if (memcmp(out, sorted + (size_t) cnt * OKBM_FIXED_BYTES, OKBM_FIXED_BYTES) != 0)
		{
			err = psprintf("walk != sorted at %d", cnt);
			goto done;
		}
		if (!o_keybitmap_range_is_valid_key(bm, out, (const uint8 *) "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"))
		{
			err = psprintf("range_is_valid false at %d", cnt);
			goto done;
		}
		cnt++;
		memcpy(cur, out, OKBM_FIXED_BYTES);
		if (!fkey_inc(cur))
			break;
	}
	if (cnt != ndistinct)
	{
		err = psprintf("walk count %d != distinct %d", cnt, ndistinct);
		goto done;
	}

	/* intersect with self is a no-op; union with empty is a no-op */
	o_keybitmap_intersect(bm, bm);
	for (i = 0; i < nkeys; i++)
		if (!o_keybitmap_test_key(bm, keys + (size_t) i * OKBM_FIXED_BYTES))
		{
			err = "self-intersect dropped a key";
			goto done;
		}

	/* intersect with disjoint (empty bm2) -> empty */
	o_keybitmap_intersect(bm, bm2);
	if (!o_keybitmap_is_empty(bm))
	{
		err = "intersect with empty not empty";
		goto done;
	}

done:
	if (err)
		err = MemoryContextStrdup(old, err);
	o_keybitmap_free(bm);
	o_keybitmap_free(bm2);
	MemoryContextSwitchTo(old);
	MemoryContextDelete(cxt);
	PG_RETURN_TEXT_P(cstring_to_text(err ? err : "ok"));
}
