/*-------------------------------------------------------------------------
 *
 * bitmap_scan.c
 *		Routines for bitmap scan of orioledb table
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/bitmap_scan.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "btree/iterator.h"
#include "btree/page_chunks.h"
#include "tableam/bitmap_scan.h"
#include "tableam/index_scan.h"
#include "tableam/tree.h"
#include "tuple/slot.h"

#include "access/relation.h"
#include "access/table.h"
#include "catalog/pg_type.h"
#include "executor/nodeIndexscan.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include <math.h>


typedef struct BitmapSeqScanArg
{
	OTableDescr *tbl_desc;
	OKeyBitmap *bitmap;
} BitmapSeqScanArg;

typedef struct BridgeIterator
{
	TIDBitmap  *tidbitmap;

#if PG_VERSION_NUM >= 180000
	TBMPrivateIterator *tbmiterator;
	TBMIterateResult tbmres;

	OffsetNumber offsets[TBM_MAX_TUPLES_PER_PAGE];
	int			iter_ntuples;
#else
	TBMIterator *tbmiterator;
	TBMIterateResult *tbmres;
#endif

	int			cur_tuple;
	int			page_ntuples;
} BridgeIterator;

#if PG_VERSION_NUM >= 180000
#define BRIDGE_RECHECK(iter) \
	(BlockNumberIsValid((iter)->tbmres.blockno) && (iter)->tbmres.recheck)
#define BRIDGE_NEXT_TUPLE(iter) \
	do \
	{ \
		if (BlockNumberIsValid((iter)->tbmres.blockno)) \
		{ \
			(iter)->cur_tuple++; \
			if ((iter)->cur_tuple >= (iter)->page_ntuples) \
				(iter)->tbmres.blockno = InvalidBlockNumber; \
		} \
	} while (0)
#define BRIDGE_ITER_ISLOSSY(iter) ((iter)->tbmres.lossy)
#define BRIDGE_ITER_NTUPLES(iter) ((iter)->iter_ntuples)
#else
#define BRIDGE_RECHECK(iter) \
	((iter)->tbmres && (iter)->tbmres->recheck)
#define BRIDGE_NEXT_TUPLE(iter) \
	do \
	{ \
		if ((iter)->tbmres) \
		{ \
			(iter)->cur_tuple++; \
			if ((iter)->cur_tuple >= (iter)->page_ntuples) \
				(iter)->tbmres = NULL; \
		} \
	} while (0)
#define BRIDGE_ITER_ISLOSSY(iter) ((iter)->tbmres->ntuples == -1)
#define BRIDGE_ITER_NTUPLES(iter) ((iter)->tbmres->ntuples)
#endif

/* One streamed primary index scan (a single BitmapIndexScan node). */
typedef struct OBitmapStreamChild
{
	OScanState	ostate;
	OIndexDescr *ix;
	Relation	index;			/* kept open for scandesc.indexRelation */
	bool		scandesc_ready; /* scandesc + cxt were initialized */
	bool		empty;			/* qual_ok false / empty array: no rows */
} OBitmapStreamChild;

typedef struct OBitmapScan
{
	ScanState  *ss;
	OSnapshot	oSnapshot;
	MemoryContext cxt;

	Oid			typeoid;

	BTreeSeqScan *seq_scan;
	BitmapSeqScanArg arg;

	BridgeIterator bridge_iter;

	/*
	 * Primary-scan streaming.  When the bitmap qual is a single
	 * BitmapIndexScan over the primary index -- or a BitmapOr of such scans
	 * (e.g. a row-array "(a,b,c) IN (...)" on a composite pk) -- there is
	 * nothing to intersect and the union only needs de-duplication.  Instead
	 * of materializing a key bitmap and re-reading the primary tree we drive
	 * the primary index scan(s) directly and hand their live tuples straight
	 * to the output (o_exec_fetch-style), keeping full orioledb row identity
	 * (csn / hint / rowid) for locking, EPQ and toast and skipping the second
	 * pass.  For a BitmapOr the children can produce the same pk (duplicate /
	 * overlapping branches), so a dedup bitmap carries the "already emitted"
	 * bit.
	 */
	bool		stream_primary;
	OBitmapStreamChild *stream_children;
	int			stream_nchildren;
	int			stream_cur;
	OKeyBitmap *stream_dedup;	/* NULL for a single child (no dup possible) */
} OBitmapScan;

static bool o_bitmap_is_range_valid(OTuple low, OTuple high, void *arg);
static bool o_bitmap_get_next_key(OFixedKey *key, BTreeKeyType keyType,
								  bool inclusive, void *arg);

static void bridge_begin_iterate(BridgeIterator *iter);
static bool bridge_iterate(BridgeIterator *iter);
static void bridge_next_page(OBitmapScan *scan,
							 OBitmapHeapPlanState *bitmap_state);

static BTreeSeqScanCallbacks bitmap_seq_scan_callbacks = {
	.isRangeValid = o_bitmap_is_range_valid,
	.getNextKey = o_bitmap_get_next_key
};

#define UINT64_HIGH_BIT (UINT64CONST(1) << 63)

static uint64
int64_to_uint64(int64 val)
{
	if (val >= 0)
		return (uint64) val | UINT64_HIGH_BIT;
	else
		return UINT64_HIGH_BIT - (uint64) (-val);
}

static int64
uint64_to_int64(uint64 val)
{
	if (val & UINT64_HIGH_BIT)
		return val & (~UINT64_HIGH_BIT);
	else
		return -(int64) (UINT64_HIGH_BIT - val);
}

static uint64
val_get_uint64(Datum val, Oid typeoid)
{
	ItemPointer iptr;

	switch (typeoid)
	{
		case INT4OID:
			return int64_to_uint64(DatumGetInt32(val));
		case INT8OID:
			return int64_to_uint64(DatumGetInt64(val));
		case TIDOID:
			iptr = DatumGetItemPointer(val);
			return (ItemPointerGetBlockNumberNoCheck(iptr) << 16) +
				ItemPointerGetOffsetNumberNoCheck(iptr);
		default:
			elog(ERROR, "Unsupported keybitmap type");
			return 0;
	}
}

static void
uint64_get_val(uint64 val, Oid typeoid, Pointer ptr)
{
	ItemPointer iptr;

	switch (typeoid)
	{
		case INT4OID:
			*((int32 *) ptr) = uint64_to_int64(val);
			break;
		case INT8OID:
			*((int64 *) ptr) = uint64_to_int64(val);
			break;
		case TIDOID:
			iptr = (ItemPointer) ptr;
			ItemPointerSetBlockNumber(iptr, val >> 16);
			ItemPointerSetOffsetNumber(iptr, val & 0xFFFF);
			break;
		default:
			elog(ERROR, "Unsupported keybitmap type");
			break;
	}
}

static uint64
seconary_tuple_get_pk_data(OTuple tuple, OIndexDescr *ix_descr)
{
	AttrNumber	attnum;
	Form_pg_attribute attr;
	Datum		val;
	bool		is_null;

	Assert(ix_descr->nPrimaryFields == 1);
	Assert(!O_TUPLE_IS_NULL(tuple));

	/*
	 * Currently bitmap scan works only for first field with int4, int8 or
	 * ctid type
	 */
	attnum = ix_descr->primaryFieldsAttnums[0];
	attr = TupleDescAttr(ix_descr->leafTupdesc, attnum - 1);
	val = o_toast_nocachegetattr(tuple, attnum, ix_descr->leafTupdesc,
								 &ix_descr->leafSpec, &is_null);
	return val_get_uint64(val, attr->atttypid);
}

static uint64
primary_tuple_get_data(OTuple tuple, OIndexDescr *primary, bool onlyPkey)
{
	AttrNumber	attnum;
	Form_pg_attribute attr;
	Datum		val;
	bool		is_null;
	BTreeKeyType keyType = onlyPkey ? BTreeKeyNonLeafKey : BTreeKeyLeafTuple;
	TupleDesc	tupdesc = onlyPkey ? primary->nonLeafTupdesc : primary->leafTupdesc;
	OTupleFixedFormatSpec *spec = onlyPkey ? &primary->nonLeafSpec : &primary->leafSpec;

	Assert(primary->nFields == 1);

	Assert(!O_TUPLE_IS_NULL(tuple));

	attnum = OIndexKeyAttnumToTupleAttnum(keyType, primary, 1);
	attr = TupleDescAttr(tupdesc, attnum - 1);
	val = o_toast_nocachegetattr(tuple, attnum, tupdesc, spec, &is_null);
	return val_get_uint64(val, attr->atttypid);
}

/* ---- composite (fixed-key) primary key encoding ---- */

/*
 * How one fixed-size attribute is turned into order-preserving key bytes.  The
 * bitmap's radix tree is walked in byte order to drive the primary-tree seek
 * (o_bitmap_get_next_key), so the encoding of every supported type must sort
 * bytewise exactly as the type's default btree opclass sorts values.
 */
typedef enum OPkEncKind
{
	PKENC_SINT,					/* signed integer: big-endian, sign bit
								 * flipped */
	PKENC_UINT,					/* unsigned integer / raw byte: big-endian */
	PKENC_FLOAT,				/* IEEE float: order-preserving bit transform */
	PKENC_RAW,					/* fixed-size by-ref, already memcmp-ordered */
} OPkEncKind;

typedef struct OPkFixedType
{
	Oid			typeoid;
	int8		width;
	OPkEncKind	kind;
} OPkFixedType;

/*
 * All built-in fixed-size types whose default btree ordering has an
 * order-preserving fixed-width byte encoding.  Types whose ordering is not a
 * bytewise function of a fixed-size value are intentionally absent (they fall
 * back to O_KEYBITMAP_NONE): xid/xid8 (modular), timetz/interval (multi-field
 * ordering), name (64 bytes > OKBM_FIXED_BYTES), and all varlena types.
 */
static const OPkFixedType o_pk_fixed_types[] = {
	{BOOLOID, 1, PKENC_UINT},
	{CHAROID, 1, PKENC_UINT},	/* "char" compares as unsigned byte */
	{INT2OID, 2, PKENC_SINT},
	{INT4OID, 4, PKENC_SINT},
	{DATEOID, 4, PKENC_SINT},
	{OIDOID, 4, PKENC_UINT},
	{FLOAT4OID, 4, PKENC_FLOAT},
	{INT8OID, 8, PKENC_SINT},
	{TIMEOID, 8, PKENC_SINT},	/* time-of-day, non-negative int64 */
	{TIMESTAMPOID, 8, PKENC_SINT},
	{TIMESTAMPTZOID, 8, PKENC_SINT},
	{MONEYOID, 8, PKENC_SINT},	/* Cash is a signed int64 */
	{FLOAT8OID, 8, PKENC_FLOAT},
	{MACADDROID, 6, PKENC_RAW},
	{MACADDR8OID, 8, PKENC_RAW},
	{UUIDOID, 16, PKENC_RAW},
};

static const OPkFixedType *
o_pk_fixed_lookup(Oid typeoid)
{
	int			i;

	for (i = 0; i < lengthof(o_pk_fixed_types); i++)
		if (o_pk_fixed_types[i].typeoid == typeoid)
			return &o_pk_fixed_types[i];
	return NULL;
}

/* Encoded width of a fixed-size key column, or -1 if the type is unsupported. */
static int
o_pk_fixed_width(Oid typeoid)
{
	const OPkFixedType *t = o_pk_fixed_lookup(typeoid);

	return t ? t->width : -1;
}

OKeyBitmapMode
o_keybitmap_pk_mode(OIndexDescr *primary, int *fixedKeyLen)
{
	int			i;
	int			len = 0;

	/*
	 * For the primary index descriptor the PK columns are its own key fields
	 * (nKeyFields; INCLUDE columns in nFields are not part of the ordering
	 * and must be ignored).  nPrimaryFields is zero here (it counts the PK
	 * columns appended to *secondary* indexes).
	 */

	/*
	 * uint64 densified mode: the historically supported single-field case.
	 * Gated on nFields (not nKeyFields) so a single-key PK with INCLUDE
	 * columns falls through to fixed-key mode rather than the uint64 helpers,
	 * whose asserts require nFields == 1.
	 */
	if (primary->nFields <= 1)
	{
		bool		ok = true;

		for (i = 0; i < primary->nFields; i++)
		{
			Oid			t = primary->fields[i].inputtype;

			if (!(t == INT4OID || t == INT8OID || t == TIDOID))
			{
				ok = false;
				break;
			}
		}
		if (ok)
			return O_KEYBITMAP_UINT64;
	}

	/*
	 * Fixed-key mode: composite of ascending fixed-size fields.  Classify by
	 * the stored attribute type (atttypid of the ordering tuple) -- exactly
	 * what o_pk_encode_nonleaf()/o_pk_decode_to_key() encode -- so the mode
	 * decision and the encoding can never disagree.
	 */
	if (primary->primaryIsCtid)
		return O_KEYBITMAP_NONE;
	for (i = 0; i < primary->nKeyFields; i++)
	{
		AttrNumber	attnum = OIndexKeyAttnumToTupleAttnum(BTreeKeyNonLeafKey,
														  primary, i + 1);
		Oid			typeoid = TupleDescAttr(primary->nonLeafTupdesc,
											attnum - 1)->atttypid;
		int			w = o_pk_fixed_width(typeoid);

		if (w < 0 || !primary->fields[i].ascending)
			return O_KEYBITMAP_NONE;
		len += w;
	}
	if (len == 0 || len > OKBM_FIXED_BYTES)
		return O_KEYBITMAP_NONE;
	if (fixedKeyLen)
		*fixedKeyLen = len;
	return O_KEYBITMAP_FIXED;
}

/*
 * Order-preserving transform masks for o_pk_encode_one()/o_pk_decode_one(),
 * named by the encoded width in bits.  A signed integer has its sign bit
 * flipped so negatives sort before positives; an IEEE float has its sign bit
 * flipped when non-negative and all bits flipped when negative (the all-ones
 * masks are PG_UINT{32,64}_MAX).
 */
#define OKBM_SIGNBIT16	UINT64CONST(0x8000)
#define OKBM_SIGNBIT32	0x80000000U
#define OKBM_SIGNBIT64	UINT64CONST(0x8000000000000000)
/* Canonical quiet-NaN bit patterns (sort highest after the float transform). */
#define OKBM_FLOAT4_NAN	0x7fc00000U
#define OKBM_FLOAT8_NAN	UINT64CONST(0x7ff8000000000000)

/*
 * Order-preserving encode of one fixed-size Datum into big-endian key bytes;
 * returns the width written.  See OPkEncKind for the per-kind transforms.
 */
static int
o_pk_encode_one(Datum val, Oid typeoid, uint8 *out)
{
	const OPkFixedType *t = o_pk_fixed_lookup(typeoid);
	int			i,
				w;
	uint64		u = 0;

	if (t == NULL)
		elog(ERROR, "unsupported fixed keybitmap type %u", typeoid);
	w = t->width;

	switch (t->kind)
	{
		case PKENC_RAW:
			/* already memcmp-ordered (uuid, macaddr*): copy raw bytes */
			memcpy(out, DatumGetPointer(val), w);
			return w;
		case PKENC_UINT:
			u = (w == 1) ? (uint8) DatumGetChar(val)
				: (uint32) DatumGetObjectId(val);
			break;
		case PKENC_SINT:
			if (w == 2)
				u = (uint16) DatumGetInt16(val) ^ OKBM_SIGNBIT16;
			else if (w == 4)
				u = (uint32) DatumGetInt32(val) ^ OKBM_SIGNBIT32;
			else
				u = (uint64) DatumGetInt64(val) ^ OKBM_SIGNBIT64;
			break;
		case PKENC_FLOAT:

			/*
			 * IEEE 754 -> sortable unsigned integer, the standard trick used
			 * for radix-sorting floats.  Reinterpret the value's bits as an
			 * unsigned integer, then: - non-negative (sign bit 0): set the
			 * sign bit.  Non-negatives keep their relative order and all sort
			 * above negatives. - negative (sign bit 1): flip every bit.  This
			 * both clears the sign bit (so negatives sort below
			 * non-negatives) and reverses the order among negatives, which is
			 * what we want: the IEEE magnitude fields increase as the value
			 * moves away from zero, so -1.0 has a smaller bit pattern than
			 * -2.0 and flipping restores -2.0 < -1.0.  Both cases collapse to
			 * one XOR: with all-ones when the sign bit is set, with just the
			 * sign bit otherwise.  o_pk_decode_one() applies the inverse.
			 *
			 * Portability: this only assumes IEEE 754 binary32/binary64,
			 * which PostgreSQL requires (see the float ordering in float.h /
			 * the configure checks), and that float and uint of the same
			 * width share the machine's byte order -- true on every supported
			 * platform. memcpy (not a pointer cast / union) reinterprets the
			 * bits without violating strict aliasing.  We work on the integer
			 * *value*, not the raw memory bytes, and
			 * o_pk_encode_one()/o_pk_decode_one() serialize/deserialize that
			 * value big-endian symmetrically, so the key is
			 * endianness-independent.  -0.0 and NaN are normalized above (to
			 * +0.0 and one canonical NaN) so equal-comparing values never get
			 * two distinct encodings.
			 */
			if (w == 4)
			{
				float		f = DatumGetFloat4(val);
				uint32		bits;

				if (isnan(f))
					bits = OKBM_FLOAT4_NAN;
				else if (f == 0.0f)
					bits = 0;	/* normalize -0 to +0 */
				else
					memcpy(&bits, &f, sizeof(bits));
				bits ^= (bits & OKBM_SIGNBIT32) ? PG_UINT32_MAX : OKBM_SIGNBIT32;
				u = bits;
			}
			else
			{
				double		d = DatumGetFloat8(val);
				uint64		bits;

				if (isnan(d))
					bits = OKBM_FLOAT8_NAN;
				else if (d == 0.0)
					bits = 0;	/* normalize -0 to +0 */
				else
					memcpy(&bits, &d, sizeof(bits));
				bits ^= (bits & OKBM_SIGNBIT64) ? PG_UINT64_MAX : OKBM_SIGNBIT64;
				u = bits;
			}
			break;
	}
	for (i = w - 1; i >= 0; i--)
	{
		out[i] = (uint8) u;
		u >>= BITS_PER_BYTE;
	}
	return w;
}

static Datum
o_pk_decode_one(const uint8 *in, Oid typeoid, int *width)
{
	const OPkFixedType *t = o_pk_fixed_lookup(typeoid);
	uint64		u = 0;
	int			i,
				w;

	if (t == NULL)
		elog(ERROR, "unsupported fixed keybitmap type %u", typeoid);
	w = t->width;
	*width = w;

	if (t->kind == PKENC_RAW)
	{
		Pointer		p = palloc(w);

		memcpy(p, in, w);
		return PointerGetDatum(p);
	}

	for (i = 0; i < w; i++)
		u = (u << BITS_PER_BYTE) | in[i];

	switch (t->kind)
	{
		case PKENC_UINT:
			return (w == 1) ? CharGetDatum((char) (uint8) u)
				: ObjectIdGetDatum((Oid) (uint32) u);
		case PKENC_SINT:
			if (w == 2)
				return Int16GetDatum((int16) (uint16) (u ^ OKBM_SIGNBIT16));
			else if (w == 4)
				return Int32GetDatum((int32) (uint32) (u ^ OKBM_SIGNBIT32));
			else
				return Int64GetDatum((int64) (u ^ OKBM_SIGNBIT64));
		case PKENC_FLOAT:

			/*
			 * Inverse of the o_pk_encode_one() float transform.  The encoded
			 * sign bit tells which case produced it: a set sign bit came from
			 * a non-negative value (encode set it), so clear it back with an
			 * XOR of the sign bit; a clear sign bit came from a negative
			 * value (encode flipped all bits), so restore it by flipping all
			 * bits again.  See o_pk_encode_one() for the ordering and
			 * portability rationale.
			 */
			if (w == 4)
			{
				uint32		bits = (uint32) u;
				float		f;

				bits ^= (bits & OKBM_SIGNBIT32) ? OKBM_SIGNBIT32 : PG_UINT32_MAX;
				memcpy(&f, &bits, sizeof(f));
				return Float4GetDatum(f);
			}
			else
			{
				uint64		bits = u;
				double		d;

				bits ^= (bits & OKBM_SIGNBIT64) ? OKBM_SIGNBIT64 : PG_UINT64_MAX;
				memcpy(&d, &bits, sizeof(d));
				return Float8GetDatum(d);
			}
		default:
			/* PKENC_RAW handled above */
			Assert(false);
			return (Datum) 0;
	}
}

/*
 * Encode the primary key held in a leaf tuple of index "id" (primary or a
 * secondary, which carries the pk fields) into a right-aligned, zero-high-
 * padded OKBM_FIXED_BYTES buffer.
 */
static void
o_pk_encode_leaf(OTuple tuple, OIndexDescr *id, uint8 *out)
{
	bool		isPrimary = (id->desc.type == oIndexPrimary);
	int			npk = isPrimary ? id->nKeyFields : id->nPrimaryFields;
	int			pk_from = id->nFields - id->nPrimaryFields;
	AttrNumber	attnums[INDEX_MAX_KEYS] = {0};
	Oid			types[INDEX_MAX_KEYS] = {0};
	int			i;
	int			len = 0;
	int			off;

	for (i = 0; i < npk; i++)
	{
		if (isPrimary)
			attnums[i] = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple, id, i + 1);
		else
			attnums[i] = id->primaryFieldsAttnums[i];
		types[i] = TupleDescAttr(id->leafTupdesc,
								 isPrimary ? attnums[i] - 1 : pk_from + i)->atttypid;
		len += o_pk_fixed_width(types[i]);
	}

	memset(out, 0, OKBM_FIXED_BYTES);
	off = OKBM_FIXED_BYTES - len;
	for (i = 0; i < npk; i++)
	{
		bool		isnull;
		Datum		val = o_fastgetattr(tuple, attnums[i], id->leafTupdesc,
										&id->leafSpec, &isnull);

		off += o_pk_encode_one(val, types[i], out + off);
	}
}

/* Encode the primary key held in a non-leaf (pk-only) tuple. */
static void
o_pk_encode_nonleaf(OTuple tuple, OIndexDescr *primary, uint8 *out)
{
	AttrNumber	attnums[INDEX_MAX_KEYS] = {0};
	Oid			types[INDEX_MAX_KEYS] = {0};
	int			i;
	int			len = 0;
	int			off;

	for (i = 0; i < primary->nKeyFields; i++)
	{
		attnums[i] = OIndexKeyAttnumToTupleAttnum(BTreeKeyNonLeafKey, primary, i + 1);
		types[i] = TupleDescAttr(primary->nonLeafTupdesc, attnums[i] - 1)->atttypid;
		len += o_pk_fixed_width(types[i]);
	}

	memset(out, 0, OKBM_FIXED_BYTES);
	off = OKBM_FIXED_BYTES - len;
	for (i = 0; i < primary->nKeyFields; i++)
	{
		bool		isnull;
		Datum		val = o_fastgetattr(tuple, attnums[i], primary->nonLeafTupdesc,
										&primary->nonLeafSpec, &isnull);

		off += o_pk_encode_one(val, types[i], out + off);
	}
}

/* Decode a fixed key back into a non-leaf pk tuple stored in key->fixedData. */
static void
o_pk_decode_to_key(const uint8 *keybytes, OIndexDescr *primary, OFixedKey *key)
{
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	AttrNumber	attnums[INDEX_MAX_KEYS] = {0};
	Oid			types[INDEX_MAX_KEYS] = {0};
	int			natts = primary->nonLeafTupdesc->natts;
	int			i;
	int			len = 0;
	int			off;
	OTuple		tup;

	/*
	 * Only the ordering key columns can be recovered from the encoded key.  A
	 * covering primary key also carries INCLUDE columns in nonLeafTupdesc,
	 * but they are not part of the ordering and can't be navigated by, so the
	 * seek key needs only the ordering columns; leave the rest NULL.  Marking
	 * any attribute NULL makes o_form_tuple() build a non-fixed tuple with a
	 * null bitmap and skip those attributes, so it never reads the
	 * uninitialized INCLUDE values (which would be undefined behavior -- e.g.
	 * a bogus by-ref box).  o_btree_cmp() on the result only touches the
	 * nKeyFields columns.
	 */
	for (i = 0; i < natts; i++)
		isnull[i] = true;

	for (i = 0; i < primary->nKeyFields; i++)
	{
		attnums[i] = OIndexKeyAttnumToTupleAttnum(BTreeKeyNonLeafKey, primary, i + 1);
		types[i] = TupleDescAttr(primary->nonLeafTupdesc, attnums[i] - 1)->atttypid;
		len += o_pk_fixed_width(types[i]);
	}

	off = OKBM_FIXED_BYTES - len;
	for (i = 0; i < primary->nKeyFields; i++)
	{
		int			w;

		values[attnums[i] - 1] = o_pk_decode_one(keybytes + off, types[i], &w);
		isnull[attnums[i] - 1] = false; /* ordering key column is present */
		off += w;
	}

	tup = o_form_tuple(primary->nonLeafTupdesc, &primary->nonLeafSpec, 0,
					   values, isnull, NULL);
	key->tuple.formatFlags = tup.formatFlags;
	memcpy(key->fixedData, tup.data, o_tuple_size(tup, &primary->nonLeafSpec));
	pfree(tup.data);
	key->tuple.data = key->fixedData;
}

static double
o_index_getbitmap(OBitmapHeapPlanState *bitmap_state,
				  BitmapIndexScanState *node,
				  OKeyBitmap *bitmap, TIDBitmap *tbm_result)
{
	OScanState	ostate = {0};
	OTableDescr *descr;
	OIndexDescr *indexDescr = NULL;
	OIndexNumber ix_num;
	Relation	index,
				table;
	BitmapIndexScan *bitmap_ix_scan = ((BitmapIndexScan *) node->ss.ps.plan);
	OTuple		tuple = {0};
	ExprContext *econtext = bitmap_state->scan->ss->ps.ps_ExprContext;
	MemoryContext mcxt = bitmap_state->scan->ss->ss_ScanTupleSlot->tts_mcxt;
	double		nTuples = 0;
	OEACallsCounters *prev_ea_counters = ea_counters;

	bitmap_state->o_plan_state.plan_state = &node->ss.ps;

	index = index_open(bitmap_ix_scan->indexid, AccessShareLock);
	table = table_open(index->rd_index->indrelid, AccessShareLock);
	descr = relation_get_descr(table);
	Assert(descr);
	relation_close(table, AccessShareLock);
	for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
	{
		indexDescr = descr->indices[ix_num];
		if (indexDescr->oids.reloid == bitmap_ix_scan->indexid)
			break;
	}
	Assert(ix_num < descr->nIndices && indexDescr != NULL);
	ostate.ixNum = ix_num;
	ostate.scanDir = ForwardScanDirection;
	ostate.indexQuals = bitmap_ix_scan->indexqual;
	ResetExprContext(econtext);

	/*
	 * ExecIndexBuildScanKeys() (called from init_index_scan_state below)
	 * palloc's fresh scan-key and runtime-key arrays and assigns them via the
	 * **scanKeys / **runtimeKeys output pointers, overwriting whatever was
	 * there before without pfree'ing it.  Each call to this function happens
	 * once per SubPlan execution, so leftover arrays from prior runs would
	 * accumulate in the per-query context for the lifetime of the query. Free
	 * the previous ones here.
	 */
	if (node->biss_ScanKeys)
	{
		pfree(node->biss_ScanKeys);
		node->biss_ScanKeys = NULL;
	}
	if (node->biss_RuntimeKeys)
	{
		pfree(node->biss_RuntimeKeys);
		node->biss_RuntimeKeys = NULL;
		node->biss_NumRuntimeKeys = 0;
	}

	init_index_scan_state(&bitmap_state->o_plan_state, &ostate, index, econtext,
#if PG_VERSION_NUM >= 180000
						  bitmap_state->bitmapqualplanstate->state->es_snapshot,
#endif
						  &node->biss_RuntimeKeys,
						  &node->biss_NumRuntimeKeys,
						  &node->biss_ScanKeys,
						  &node->biss_NumScanKeys);
	relation_close(index, AccessShareLock);

	if (node->biss_NumRuntimeKeys != 0)
	{
		ResetExprContext(node->biss_RuntimeContext);
		ExecIndexEvalRuntimeKeys(node->biss_RuntimeContext,
								 node->biss_RuntimeKeys,
								 node->biss_NumRuntimeKeys);
		node->biss_RuntimeKeysReady = true;
	}

	if ((node->biss_NumRuntimeKeys == 0 && node->biss_NumArrayKeys == 0) ||
		(node->biss_RuntimeKeysReady))
	{
		btrescan(&ostate.scandesc, node->biss_ScanKeys,
				 node->biss_NumScanKeys, NULL, 0);
		ostate.numPrefixExactKeys = o_get_num_prefix_exact_keys(node->biss_ScanKeys, node->biss_NumScanKeys);
	}


	if (is_explain_analyze(&node->ss.ps))
	{
		ea_counters = &bitmap_state->eaCounters[ix_num];
	}
	else
		ea_counters = NULL;

	ostate.oSnapshot = bitmap_state->oSnapshot;
	ostate.onlyCurIx = true;
	ostate.cxt = bitmap_state->cxt;

	ostate.curKeyRangeIsLoaded = false;
	ostate.curKeyRange.empty = true;
	ostate.curKeyRange.low.n_row_keys = 0;
	ostate.curKeyRange.high.n_row_keys = 0;

	if (!ostate.curKeyRangeIsLoaded)
	{
		BTScanOpaque so = (BTScanOpaque) ostate.scandesc.opaque;

		_bt_preprocess_keys(&ostate.scandesc);
		if (!so->qual_ok)
			return nTuples;
		ostate.numPrefixExactKeys =
			o_adjust_num_prefix_exact_keys(so, ostate.numPrefixExactKeys);
		if (so->numArrayKeys)
			_bt_start_array_keys(&ostate.scandesc, ForwardScanDirection);
		ostate.curKeyRange.empty = true;
	}

	do
	{
		tuple = o_iterate_index(indexDescr, &ostate, NULL, mcxt, NULL);

		if (!O_TUPLE_IS_NULL(tuple))
		{
			uint64		data;

			if (!tbm_result)
			{
				if (o_keybitmap_pk_mode(GET_PRIMARY(descr), NULL) == O_KEYBITMAP_FIXED)
				{
					uint8		key[OKBM_FIXED_BYTES];

					o_pk_encode_leaf(tuple, indexDescr, key);
					o_keybitmap_insert_key(bitmap, key);
				}
				else
				{
					/*
					 * The scanned index may be the primary index itself (e.g.
					 * a bitmap index scan over the primary for a row-array
					 * IN), in which case the pk is the tuple's own key rather
					 * than an appended secondary payload.
					 */
					if (indexDescr->desc.type == oIndexPrimary)
						data = primary_tuple_get_data(tuple, indexDescr, false);
					else
						data = seconary_tuple_get_pk_data(tuple, indexDescr);
					o_keybitmap_insert(bitmap, data);
				}
			}
			else
			{
				if (indexDescr->desc.type != oIndexPrimary)
				{
					OBTreeKeyBound bound;
					OTuple		ptup;
					OIndexDescr *primary = GET_PRIMARY(descr);
					AttrNumber	attnum;
					Datum		val;
					bool		is_null;
					TupleDesc	tupdesc = primary->leafTupdesc;
					OTupleFixedFormatSpec *spec = &primary->leafSpec;
					ItemPointer bridge_iptr;

					/* fetch primary index key from tuple and search raw tuple */
					o_fill_pindex_tuple_key_bound(&indexDescr->desc, tuple, &bound);

					ptup = o_btree_find_tuple_by_key(&primary->desc,
													 (Pointer) &bound, BTreeKeyBound,
													 &ostate.oSnapshot, NULL,
													 mcxt, NULL);

					/*
					 * in concurrent DELETE/UPDATE it might happen, we should
					 * to try fetch next tuple
					 */
					if (!O_TUPLE_IS_NULL(ptup))
					{
						attnum = primary->primaryIsCtid ? 2 : 1;
						val = o_toast_nocachegetattr(ptup, attnum, tupdesc, spec, &is_null);
						Assert(!is_null);
						bridge_iptr = DatumGetItemPointer(val);
						tbm_add_tuples(tbm_result, bridge_iptr, 1, false);
						pfree(tuple.data);
						tuple = ptup;
					}
				}
				else
				{
					Assert(false);
				}
			}
			nTuples += 1;

			/*
			 * o_iterate_index() palloc'd tuple.data in mcxt
			 * (ss_ScanTupleSlot->tts_mcxt = the per-query context).  We've
			 * already extracted everything we need into the bitmap / tbm
			 * result, so release the buffer; otherwise it would accumulate
			 * across all tuples this index scan visits and balloon
			 * ExecutorState on SubPlan-heavy queries that re-build the bitmap
			 * many times.
			 */
			pfree(tuple.data);
		}
	} while (!O_TUPLE_IS_NULL(tuple));

	if (ostate.iterator)
		btree_iterator_free(ostate.iterator);
	MemoryContextReset(ostate.cxt);

	/*
	 * init_index_scan_state() above called btbeginscan(), which palloc'd a
	 * BTScanOpaque (and, lazily, currTuples / arrayContext / keyData) and
	 * stashed it in ostate.scandesc.opaque.  The IndexScanDesc itself was
	 * pfree'd inside init_index_scan_state(), but its private workspace
	 * survives there.  Without a matching btendscan() that workspace —
	 * including the 16KB currTuples buffer btree allocates on demand —
	 * would accumulate in the per-query context for every call (one per
	 * SubPlan execution that goes through bitmap heap scan).
	 */
	btendscan(&ostate.scandesc);

	ea_counters = prev_ea_counters;
	return nTuples;
}

static void
exec_bitmap_index_state(OBitmapHeapPlanState *bitmap_state, PlanState *planstate,
						OKeyBitmap **rbt_result, TIDBitmap **tbm_result)
{
	double		nTuples = 0;
	BitmapIndexScanState *node;
	Instrumentation *instrument;
	OBTOptions *options;
	ExprContext *econtext = bitmap_state->scan->ss->ps.ps_ExprContext;

	node = (BitmapIndexScanState *) planstate;
	instrument = node->ss.ps.instrument;
	options = (OBTOptions *) node->biss_RelationDesc->rd_options;

	if (node->biss_NumRuntimeKeys != 0)
		ExecIndexEvalRuntimeKeys(econtext,
								 node->biss_RuntimeKeys,
								 node->biss_NumRuntimeKeys);
	if (node->biss_NumArrayKeys != 0)
		node->biss_RuntimeKeysReady =
			ExecIndexEvalArrayKeys(econtext,
								   node->biss_ArrayKeys,
								   node->biss_NumArrayKeys);
	else
		node->biss_RuntimeKeysReady = true;

	/* reset index scan */
	if (node->biss_RuntimeKeysReady)
		index_rescan(node->biss_ScanDesc,
					 node->biss_ScanKeys, node->biss_NumScanKeys,
					 NULL, 0);

	if (instrument)
		InstrStartNode(instrument);

	if (node->biss_RelationDesc->rd_rel->relam != BTREE_AM_OID ||
		(options && !options->orioledb_index))
	{
		bool		doscan;
		IndexScanDesc scandesc;

		if (*tbm_result == NULL)
			*tbm_result = tbm_create(work_mem * 1024L, NULL);

		/*
		 * extract necessary information from index scan node
		 */
		scandesc = node->biss_ScanDesc;

		/*
		 * If we have runtime keys and they've not already been set up, do it
		 * now. Array keys are also treated as runtime keys; note that if
		 * ExecReScan returns with biss_RuntimeKeysReady still false, then
		 * there is an empty array key so we should do nothing.
		 */
		if (!node->biss_RuntimeKeysReady &&
			(node->biss_NumRuntimeKeys != 0 || node->biss_NumArrayKeys != 0))
		{
			ExecReScan((PlanState *) node);
			doscan = node->biss_RuntimeKeysReady;
		}
		else
			doscan = true;

		while (doscan)
		{
			nTuples += (double) index_getbitmap(scandesc, *tbm_result);

			CHECK_FOR_INTERRUPTS();

			doscan = ExecIndexAdvanceArrayKeys(node->biss_ArrayKeys,
											   node->biss_NumArrayKeys);
			if (doscan)			/* reset index scan */
				index_rescan(node->biss_ScanDesc,
							 node->biss_ScanKeys, node->biss_NumScanKeys,
							 NULL, 0);
		}
	}
	else
	{
		if (*tbm_result == NULL && *rbt_result == NULL)
		{
			OIndexDescr *primary = GET_PRIMARY(bitmap_state->scan->arg.tbl_desc);

			if (o_keybitmap_pk_mode(primary, NULL) == O_KEYBITMAP_FIXED)
				*rbt_result = o_keybitmap_create_fixed();
			else
				*rbt_result = o_keybitmap_create();
		}
#if PG_VERSION_NUM >= 180000
		node->biss_Instrument.nsearches++;
#endif
		nTuples = o_index_getbitmap(bitmap_state, node, *rbt_result, *tbm_result);
	}
	if (instrument)
		InstrStopNode(instrument, nTuples);
}

static void
add_rbt_to_tbm(OBitmapHeapPlanState *bitmap_state, TIDBitmap *tbm, OKeyBitmap *rbt)
{
	BTreeSeqScan *seq_scan;
	BitmapSeqScanArg arg;
	OIndexDescr *primary = GET_PRIMARY(bitmap_state->scan->arg.tbl_desc);

	arg.tbl_desc = bitmap_state->scan->arg.tbl_desc;
	arg.bitmap = rbt;

	seq_scan = make_btree_seq_scan_cb(&primary->desc,
									  &bitmap_state->scan->oSnapshot,
									  &bitmap_seq_scan_callbacks, &arg);

	while (true)
	{
		OTuple		tuple;
		BTreeLocationHint hint;
		CommitSeqNo tupleCsn;

		tuple = btree_seq_scan_getnext(seq_scan, bitmap_state->scan->cxt, &tupleCsn,
									   &hint);

		if (O_TUPLE_IS_NULL(tuple))
		{
			break;
		}
		else
		{
			AttrNumber	attnum;
			Datum		val;
			bool		is_null;
			TupleDesc	tupdesc = primary->leafTupdesc;
			OTupleFixedFormatSpec *spec = &primary->leafSpec;
			ItemPointer bridge_iptr;

			Assert(primary->nFields == 1);

			attnum = primary->primaryIsCtid ? 2 : 1;
			val = o_toast_nocachegetattr(tuple, attnum, tupdesc, spec, &is_null);
			Assert(!is_null);
			bridge_iptr = DatumGetItemPointer(val);
			tbm_add_tuples(tbm, bridge_iptr, 1, false);
		}
	}
	free_btree_seq_scan(seq_scan);
}

static void
o_exec_bitmapqual(OBitmapHeapPlanState *bitmap_state, PlanState *planstate,
				  OKeyBitmap **rbt_result, TIDBitmap **tbm_result)
{
	Assert(rbt_result && tbm_result);
	Assert(*rbt_result == NULL || *tbm_result == NULL);

	switch (nodeTag(planstate))
	{
		case T_BitmapAndState:
			{
				BitmapAndState *node = (BitmapAndState *) planstate;
				int			i;
				Instrumentation *instrument = node->ps.instrument;

				if (instrument)
					InstrStartNode(instrument);

				for (i = 0; i < node->nplans; i++)
				{
					PlanState  *subnode = node->bitmapplans[i];
					OKeyBitmap *rbt_subresult = NULL;
					TIDBitmap  *tbm_subresult = NULL;

					o_exec_bitmapqual(bitmap_state, subnode, &rbt_subresult, &tbm_subresult);

					Assert(rbt_subresult || tbm_subresult);

					if (tbm_subresult != NULL)
					{
						if (*tbm_result == NULL)
						{
							*tbm_result = tbm_subresult;	/* first subplan */
						}
						else
						{
							tbm_intersect(*tbm_result, tbm_subresult);
							tbm_free(tbm_subresult);
						}
					}
					else if (rbt_subresult != NULL)
					{
						if (*tbm_result == NULL)
						{
							if (*rbt_result == NULL)
							{
								*rbt_result = rbt_subresult;	/* first subplan */
							}
							else if (*rbt_result != NULL)
							{
								o_keybitmap_intersect(*rbt_result, rbt_subresult);
								o_keybitmap_free(rbt_subresult);
							}
						}
						else
						{
							TIDBitmap  *temp_bitmap = tbm_create(work_mem * 1024L, NULL);

							Assert(*rbt_result == NULL);

							add_rbt_to_tbm(bitmap_state, temp_bitmap, rbt_subresult);
							tbm_intersect(*tbm_result, temp_bitmap);
							tbm_free(temp_bitmap);
						}
					}

					if (*tbm_result != NULL && *rbt_result != NULL)
					{
						TIDBitmap  *temp_bitmap = tbm_create(work_mem * 1024L, NULL);

						add_rbt_to_tbm(bitmap_state, temp_bitmap, *rbt_result);
						tbm_intersect(*tbm_result, temp_bitmap);
						tbm_free(temp_bitmap);
					}

					/*
					 * If at any stage we have a completely empty bitmap, we
					 * can fall out without evaluating the remaining subplans,
					 * since ANDing them can no longer change the result.
					 * (Note: the fact that indxpath.c orders the subplans by
					 * selectivity should make this case more likely to
					 * occur.)
					 */
					if ((*rbt_result && o_keybitmap_is_empty(*rbt_result)) ||
						(*tbm_result && tbm_is_empty(*tbm_result)))
						break;
				}
				if (instrument)
					InstrStopNode(instrument, 0);
				break;
			}
		case T_BitmapOrState:
			{
				BitmapOrState *node = (BitmapOrState *) planstate;
				int			i;
				Instrumentation *instrument = node->ps.instrument;

				if (instrument)
					InstrStartNode(instrument);

				for (i = 0; i < node->nplans; i++)
				{
					PlanState  *subnode = node->bitmapplans[i];
					OKeyBitmap *rbt_subresult = NULL;
					TIDBitmap  *tbm_subresult = NULL;

					if (IsA(subnode, BitmapIndexScanState))
					{
						rbt_subresult = *rbt_result;
						tbm_subresult = *tbm_result;
						Assert(!(rbt_subresult && tbm_subresult));
						o_exec_bitmapqual(bitmap_state, subnode, &rbt_subresult, &tbm_subresult);

						/*
						 * In other situations union should be already made
						 * inside of o_exec_bitmapqual
						 */
						if (*rbt_result == NULL && rbt_subresult != NULL)
							*rbt_result = rbt_subresult;
						if (*tbm_result == NULL && tbm_subresult != NULL)
							*tbm_result = tbm_subresult;
					}
					else
					{
						/* standard implementation */
						o_exec_bitmapqual(bitmap_state, subnode, &rbt_subresult, &tbm_subresult);

						if (tbm_subresult != NULL)
						{
							if (*tbm_result == NULL)
								*tbm_result = tbm_subresult;	/* first subplan */
							else
							{
								tbm_union(*tbm_result, tbm_subresult);
								tbm_free(tbm_subresult);
							}
						}
						else if (rbt_subresult != NULL)
						{
							if (*rbt_result == NULL)
							{
								*rbt_result = rbt_subresult;	/* first subplan */
							}
							else if (*tbm_result == NULL)
							{
								o_keybitmap_union(*rbt_result, rbt_subresult);
								o_keybitmap_free(rbt_subresult);
							}
							else
							{
								add_rbt_to_tbm(bitmap_state, *tbm_result, rbt_subresult);
								o_keybitmap_free(rbt_subresult);
							}
						}
					}

					if (*tbm_result != NULL && *rbt_result != NULL)
					{
						add_rbt_to_tbm(bitmap_state, *tbm_result, *rbt_result);
						o_keybitmap_free(*rbt_result);
						*rbt_result = NULL;
					}
				}
				if (instrument)
					InstrStopNode(instrument, 0);
				break;
			}
		case T_BitmapIndexScanState:
			exec_bitmap_index_state(bitmap_state, planstate, rbt_result, tbm_result);
			break;
		default:
			elog(ERROR, "%s: unrecognized node type: %d",
				 PG_FUNCNAME_MACRO, (int) nodeTag(planstate));
			break;
	}
}

/*
 * Set up one streamed primary index scan for a BitmapIndexScan node (mirroring
 * exec_bitmap_index_state() + o_index_getbitmap()'s setup, minus the collect
 * loop) that o_bitmap_stream_fetch() drives directly.  Returns false -- leaving
 * *child untouched apart from a closed index -- when this node is not a
 * streamable primary orioledb index scan, so the caller falls back to building
 * a key bitmap.
 */
static bool
setup_primary_stream(OBitmapHeapPlanState *bitmap_state, OBitmapScan *scan,
					 BitmapIndexScanState *node, OBitmapStreamChild *child)
{
	OScanState *ostate = &child->ostate;
	OTableDescr *descr = scan->arg.tbl_desc;
	OIndexDescr *indexDescr = NULL;
	OIndexNumber ix_num;
	Relation	index;
	BitmapIndexScan *bitmap_ix_scan = (BitmapIndexScan *) node->ss.ps.plan;
	ExprContext *econtext = scan->ss->ps.ps_ExprContext;
	OBTOptions *options = (OBTOptions *) node->biss_RelationDesc->rd_options;
	BTScanOpaque so;

	/* Non-orioledb (bridged) indexes go through the TIDBitmap path. */
	if (node->biss_RelationDesc->rd_rel->relam != BTREE_AM_OID ||
		(options && !options->orioledb_index))
		return false;

	index = index_open(bitmap_ix_scan->indexid, AccessShareLock);
	for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
	{
		indexDescr = descr->indices[ix_num];
		if (indexDescr->oids.reloid == bitmap_ix_scan->indexid)
			break;
	}
	Assert(ix_num < descr->nIndices && indexDescr != NULL);

	/* Only the primary index scan yields the table's own rows directly. */
	if (indexDescr->desc.type != oIndexPrimary)
	{
		index_close(index, AccessShareLock);
		return false;
	}

	child->index = index;
	child->ix = indexDescr;

	/* Evaluate runtime / array keys (cf. exec_bitmap_index_state()). */
	if (node->biss_NumRuntimeKeys != 0)
		ExecIndexEvalRuntimeKeys(econtext, node->biss_RuntimeKeys,
								 node->biss_NumRuntimeKeys);
	if (node->biss_NumArrayKeys != 0)
		node->biss_RuntimeKeysReady =
			ExecIndexEvalArrayKeys(econtext, node->biss_ArrayKeys,
								   node->biss_NumArrayKeys);
	else
		node->biss_RuntimeKeysReady = true;

	/* Empty array key: the scan yields nothing. */
	if (!node->biss_RuntimeKeysReady)
	{
		child->empty = true;
		return true;
	}

	/* Build the orioledb scan state (cf. o_index_getbitmap()). */
	memset(ostate, 0, sizeof(*ostate));
	ostate->ixNum = ix_num;
	ostate->scanDir = ForwardScanDirection;
	ostate->indexQuals = bitmap_ix_scan->indexqual;
	bitmap_state->o_plan_state.plan_state = &node->ss.ps;
	ResetExprContext(econtext);

	if (node->biss_ScanKeys)
	{
		pfree(node->biss_ScanKeys);
		node->biss_ScanKeys = NULL;
	}
	if (node->biss_RuntimeKeys)
	{
		pfree(node->biss_RuntimeKeys);
		node->biss_RuntimeKeys = NULL;
		node->biss_NumRuntimeKeys = 0;
	}

	init_index_scan_state(&bitmap_state->o_plan_state, ostate, index, econtext,
#if PG_VERSION_NUM >= 180000
						  bitmap_state->bitmapqualplanstate->state->es_snapshot,
#endif
						  &node->biss_RuntimeKeys, &node->biss_NumRuntimeKeys,
						  &node->biss_ScanKeys, &node->biss_NumScanKeys);

	if (node->biss_NumRuntimeKeys != 0)
	{
		ResetExprContext(node->biss_RuntimeContext);
		ExecIndexEvalRuntimeKeys(node->biss_RuntimeContext,
								 node->biss_RuntimeKeys,
								 node->biss_NumRuntimeKeys);
		node->biss_RuntimeKeysReady = true;
	}

	if ((node->biss_NumRuntimeKeys == 0 && node->biss_NumArrayKeys == 0) ||
		node->biss_RuntimeKeysReady)
	{
		btrescan(&ostate->scandesc, node->biss_ScanKeys,
				 node->biss_NumScanKeys, NULL, 0);
		ostate->numPrefixExactKeys =
			o_get_num_prefix_exact_keys(node->biss_ScanKeys, node->biss_NumScanKeys);
	}

	ostate->oSnapshot = scan->oSnapshot;
	ostate->onlyCurIx = true;
	ostate->cxt = AllocSetContextCreate(scan->cxt,
										"orioledb bitmap primary stream",
										ALLOCSET_DEFAULT_SIZES);
	ostate->curKeyRangeIsLoaded = false;
	ostate->curKeyRange.empty = true;
	ostate->curKeyRange.low.n_row_keys = 0;
	ostate->curKeyRange.high.n_row_keys = 0;
	child->scandesc_ready = true;

	so = (BTScanOpaque) ostate->scandesc.opaque;
	_bt_preprocess_keys(&ostate->scandesc);
	if (!so->qual_ok)
	{
		child->empty = true;
		return true;
	}
	ostate->numPrefixExactKeys =
		o_adjust_num_prefix_exact_keys(so, ostate->numPrefixExactKeys);
	if (so->numArrayKeys)
		_bt_start_array_keys(&ostate->scandesc, ForwardScanDirection);
	ostate->curKeyRange.empty = true;

	return true;
}

/*
 * Set up primary-scan streaming for the whole bitmap qual, if it is a single
 * BitmapIndexScan over the primary index or a BitmapOr of only such scans.
 * Returns false (having freed anything it opened) to fall back to a key bitmap.
 */
static bool
setup_primary_stream_qual(OBitmapHeapPlanState *bitmap_state, OBitmapScan *scan,
						  PlanState *qual)
{
	if (IsA(qual, BitmapIndexScanState))
	{
		scan->stream_children = MemoryContextAllocZero(scan->cxt,
													   sizeof(OBitmapStreamChild));
		if (!setup_primary_stream(bitmap_state, scan,
								  (BitmapIndexScanState *) qual,
								  &scan->stream_children[0]))
		{
			pfree(scan->stream_children);
			scan->stream_children = NULL;
			return false;
		}
		scan->stream_nchildren = 1;
		/* a single scan never yields the same pk twice: no dedup needed */
		return true;
	}
	else if (IsA(qual, BitmapOrState))
	{
		BitmapOrState *orstate = (BitmapOrState *) qual;
		int			i;

		/* Only when every branch is itself a plain BitmapIndexScan. */
		for (i = 0; i < orstate->nplans; i++)
			if (!IsA(orstate->bitmapplans[i], BitmapIndexScanState))
				return false;

		scan->stream_children = MemoryContextAllocZero(scan->cxt,
													   sizeof(OBitmapStreamChild) * orstate->nplans);
		for (i = 0; i < orstate->nplans; i++)
		{
			if (!setup_primary_stream(bitmap_state, scan,
									  (BitmapIndexScanState *) orstate->bitmapplans[i],
									  &scan->stream_children[i]))
			{
				/* tear down the children already set up, then fall back */
				int			j;

				for (j = 0; j <= i; j++)
				{
					OBitmapStreamChild *c = &scan->stream_children[j];

					if (c->scandesc_ready)
					{
						if (c->ostate.iterator)
							btree_iterator_free(c->ostate.iterator);
						btendscan(&c->ostate.scandesc);
						if (c->ostate.cxt)
							MemoryContextDelete(c->ostate.cxt);
					}
					if (c->index)
						index_close(c->index, AccessShareLock);
				}
				pfree(scan->stream_children);
				scan->stream_children = NULL;
				return false;
			}
			scan->stream_nchildren++;
		}

		/* Branches can overlap / duplicate pks: dedup emitted rows. */
		if (o_keybitmap_pk_mode(GET_PRIMARY(scan->arg.tbl_desc), NULL) == O_KEYBITMAP_FIXED)
			scan->stream_dedup = o_keybitmap_create_fixed();
		else
			scan->stream_dedup = o_keybitmap_create();
		return true;
	}

	return false;
}

/*
 * Fetch the next tuple of a primary-scan-streamed bitmap scan.  Mirrors
 * o_exec_fetch(): pull one live primary tuple from the current child scan and
 * hand it to the scan slot with full row identity, applying the node qual.  A
 * BitmapOr's branches are streamed in turn; a dedup bitmap drops any pk already
 * emitted by an earlier (overlapping / duplicate) branch.
 */
static TupleTableSlot *
o_bitmap_stream_fetch(OBitmapScan *scan, CustomScanState *node)
{
	ScanState  *ss = &node->ss;
	OTableDescr *descr = relation_get_descr(ss->ss_currentRelation);
	OIndexDescr *primary = GET_PRIMARY(scan->arg.tbl_desc);
	MemoryContext tupleCxt = ss->ss_ScanTupleSlot->tts_mcxt;
	TupleTableSlot *slot;

	while (scan->stream_cur < scan->stream_nchildren)
	{
		OBitmapStreamChild *child = &scan->stream_children[scan->stream_cur];
		BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};
		CommitSeqNo tupleCsn;
		OTuple		tuple;

		if (child->empty)
		{
			scan->stream_cur++;
			continue;
		}

		tuple = o_iterate_index(child->ix, &child->ostate, &tupleCsn, tupleCxt,
								&hint);
		if (O_TUPLE_IS_NULL(tuple))
		{
			scan->stream_cur++;
			continue;
		}

		/* Dedup across BitmapOr branches. */
		if (scan->stream_dedup)
		{
			bool		fresh;

			if (o_keybitmap_pk_mode(primary, NULL) == O_KEYBITMAP_FIXED)
			{
				uint8		key[OKBM_FIXED_BYTES];

				o_pk_encode_leaf(tuple, child->ix, key);
				fresh = o_keybitmap_emit_key(scan->stream_dedup, key);
			}
			else
				fresh = o_keybitmap_emit(scan->stream_dedup,
										 primary_tuple_get_data(tuple, child->ix, false));

			if (!fresh)
			{
				pfree(tuple.data);
				continue;		/* already emitted by an earlier branch */
			}
		}

		tts_orioledb_store_tuple(ss->ss_ScanTupleSlot, tuple, descr, tupleCsn,
								 PrimaryIndexNumber, true, &hint);
		slot = ss->ss_ScanTupleSlot;

		if (o_exec_qual(ss->ps.ps_ExprContext, ss->ps.qual, slot))
			return slot;
		/* qual failed: keep scanning */
	}

	return ExecClearTuple(ss->ss_ScanTupleSlot);
}

/*
 * Parallel build coordination: the first worker to arrive builds the whole
 * key bitmap (running the bitmap qual), serializes it into es_query_dsa and
 * publishes it; the others wait on the condition variable and then attach the
 * same buffer read-only.  Returns the attached (shared) bitmap, or NULL when
 * the built bitmap is empty.  The cooperative primary fetch then runs through
 * pbitmap->poscan.
 */
static OKeyBitmap *
o_bitmap_parallel_prepare(OBitmapHeapPlanState *bitmap_state, OBitmapScan *scan,
						  PlanState *bitmapqualplanstate)
{
	ParallelOBitmapScan pbitmap = bitmap_state->pbitmap;
	dsa_area   *dsa = bitmap_state->dsa;
	bool		build;

	SpinLockAcquire(&pbitmap->mutex);
	build = (pbitmap->stage == OBITMAP_PARALLEL_NEW);
	if (build)
		pbitmap->stage = OBITMAP_PARALLEL_BUILDING;
	SpinLockRelease(&pbitmap->mutex);

	if (build)
	{
		OKeyBitmap *local = NULL;
		TIDBitmap  *tbm = NULL;
		dsa_pointer dp = InvalidDsaPointer;
		Size		sz = 0;
		bool		empty = true;

		o_exec_bitmapqual(bitmap_state, bitmapqualplanstate, &local, &tbm);
		/* The planner only offers a parallel path for native-index quals. */
		Assert(tbm == NULL);

		if (local != NULL && !o_keybitmap_is_empty(local))
		{
			sz = o_keybitmap_serialized_size(local);
			dp = dsa_allocate(dsa, sz);
			o_keybitmap_serialize(local, dsa_get_address(dsa, dp));
			empty = false;
		}
		if (local != NULL)
			o_keybitmap_free(local);

		SpinLockAcquire(&pbitmap->mutex);
		pbitmap->bitmap_dsa = dp;
		pbitmap->bitmap_size = sz;
		pbitmap->empty = empty;
		pbitmap->stage = OBITMAP_PARALLEL_READY;
		SpinLockRelease(&pbitmap->mutex);
		ConditionVariableBroadcast(&pbitmap->cv);
	}
	else
	{
		ConditionVariablePrepareToSleep(&pbitmap->cv);
		for (;;)
		{
			OBitmapParallelStage stage;

			SpinLockAcquire(&pbitmap->mutex);
			stage = pbitmap->stage;
			SpinLockRelease(&pbitmap->mutex);
			if (stage == OBITMAP_PARALLEL_READY)
				break;
			ConditionVariableSleep(&pbitmap->cv,
								   WAIT_EVENT_PARALLEL_BITMAP_SCAN);
		}
		ConditionVariableCancelSleep();
	}

	if (pbitmap->empty)
		return NULL;

	return o_keybitmap_attach(dsa_get_address(dsa, pbitmap->bitmap_dsa),
							  scan->cxt);
}

OBitmapScan *
o_make_bitmap_scan(OBitmapHeapPlanState *bitmap_state, ScanState *ss,
				   PlanState *bitmapqualplanstate, Relation rel,
				   Oid typeoid, OSnapshot *oSnapshot,
				   MemoryContext cxt)
{
	OBitmapScan *scan = palloc0(sizeof(OBitmapScan));

	scan->typeoid = typeoid;
	scan->oSnapshot = *oSnapshot;
	scan->cxt = cxt;
	scan->ss = ss;
	scan->arg.tbl_desc = relation_get_descr(rel);
	bitmap_state->scan = scan;

	/*
	 * Parallel bitmap heap scan: build the key bitmap once (cooperatively),
	 * attach it read-only, and fetch the primary tree cooperatively through
	 * the shared poscan.  The streaming/bridge fast paths are not used here.
	 */
	if (bitmap_state->pbitmap != NULL)
	{
		scan->arg.bitmap = o_bitmap_parallel_prepare(bitmap_state, scan,
													 bitmapqualplanstate);
		if (scan->arg.bitmap != NULL)
			scan->seq_scan =
				make_btree_seq_scan_cb_parallel(&GET_PRIMARY(scan->arg.tbl_desc)->desc,
												&scan->oSnapshot,
												&bitmap_seq_scan_callbacks,
												&scan->arg,
												&bitmap_state->pbitmap->poscan);
		return scan;
	}

	/*
	 * Fast path: a single primary BitmapIndexScan, or a BitmapOr of only such
	 * scans, is executed as live primary index scan(s) -- no key bitmap, no
	 * second pass over the primary tree.
	 */
	if (setup_primary_stream_qual(bitmap_state, scan, bitmapqualplanstate))
	{
		scan->stream_primary = true;
		return scan;
	}

	o_exec_bitmapqual(bitmap_state, bitmapqualplanstate,
					  &scan->arg.bitmap,
					  &scan->bridge_iter.tidbitmap);

	if (scan->arg.bitmap)
	{
		scan->seq_scan = make_btree_seq_scan_cb(&GET_PRIMARY(scan->arg.tbl_desc)->desc,
												&scan->oSnapshot,
												&bitmap_seq_scan_callbacks, &scan->arg);
	}
	else
	{
		bridge_begin_iterate(&scan->bridge_iter);
	}

	return scan;
}

TupleTableSlot *
o_exec_bitmap_fetch(OBitmapScan *scan, CustomScanState *node)
{
	bool		fetched;
	TupleTableSlot *slot = NULL;
	OCustomScanState *ocstate = (OCustomScanState *) node;
	OBitmapHeapPlanState *bitmap_state =
		(OBitmapHeapPlanState *) ocstate->o_plan_state;
	BridgeIterator *bridge_iter = &scan->bridge_iter;

	if (scan->stream_primary)
		return o_bitmap_stream_fetch(scan, node);

	/* An empty (parallel) bitmap has no primary scan and no bridge iterator. */
	if (scan->seq_scan == NULL && bridge_iter->tbmiterator == NULL)
		return ExecClearTuple(node->ss.ss_ScanTupleSlot);

	do
	{
		OTuple		tuple;
		BTreeLocationHint hint;
		MemoryContext tupleCxt = node->ss.ss_ScanTupleSlot->tts_mcxt;
		CommitSeqNo tupleCsn;
#if PG_VERSION_NUM >= 180000
		bool		page_exhausted = !BlockNumberIsValid(bridge_iter->tbmres.blockno);
#else
		bool		page_exhausted = (bridge_iter->tbmres == NULL);
#endif

		fetched = false;

		/*
		 * Reset per-tuple memory before each iteration.  PG's ExecCustomScan
		 * just delegates to the AM's callback without wrapping in ExecScan,
		 * so the standard per-tuple reset that ExecScan performs between
		 * fetch attempts doesn't run here.  Without this, every qual
		 * evaluation (especially ones with SubPlans, e.g. the TPC-C
		 * consistency-check #10 join, where each candidate row triggers
		 * subplan executions) accumulates intermediate values in
		 * ps_ExprContext->ecxt_per_tuple_memory and ExecutorState grows for
		 * the whole duration of the scan node.
		 */
		ResetExprContext(node->ss.ps.ps_ExprContext);

		/* Path 1: Iterate using bridge bitmap */
		if (bridge_iter->tbmiterator != NULL && page_exhausted)
		{
			if (!bridge_iterate(bridge_iter))
			{
				/* No more pages in the bitmap */
				slot = ExecClearTuple(node->ss.ss_ScanTupleSlot);
				fetched = true;
			}

			if (!fetched)
				bridge_next_page(scan, bitmap_state);
		}

		/* Path 2: Iterate using OKeyBitmap bitmap with seq scan */
		if (!fetched)
		{
			Assert(scan->seq_scan);

			tuple = btree_seq_scan_getnext(scan->seq_scan, tupleCxt, &tupleCsn,
										   &hint);

			if (O_TUPLE_IS_NULL(tuple))
			{
				/*
				 * The per-page primary seq scan is exhausted.  In bridge mode
				 * the TIDBitmap may still hold more pages: dead bridge_ctids
				 * left by earlier UPDATEs make the keybitmap on a page
				 * resolve to fewer (or zero) live PKs than page_ntuples, so
				 * BRIDGE_NEXT_TUPLE never marks the page exhausted on its
				 * own.  Force the advance here and continue the outer loop.
				 */
				if (bridge_iter->tbmiterator != NULL)
				{
#if PG_VERSION_NUM >= 180000
					bridge_iter->tbmres.blockno = InvalidBlockNumber;
#else
					bridge_iter->tbmres = NULL;
#endif
					continue;	/* skip the InstrCountFiltered2 below */
				}
				else
				{
					slot = ExecClearTuple(node->ss.ss_ScanTupleSlot);
					fetched = true;
				}
			}
			else
			{
				OTableDescr *descr;
				OIndexDescr *primary;
				uint64		value;
				bool		in_bitmap;

				descr = relation_get_descr(node->ss.ss_currentRelation);
				primary = GET_PRIMARY(descr);

				if (o_keybitmap_pk_mode(primary, NULL) == O_KEYBITMAP_FIXED)
				{
					uint8		key[OKBM_FIXED_BYTES];

					o_pk_encode_leaf(tuple, primary, key);
					in_bitmap = o_keybitmap_test_key(scan->arg.bitmap, key);
				}
				else
				{
					value = primary_tuple_get_data(tuple, primary, false);
					in_bitmap = o_keybitmap_test(scan->arg.bitmap, value);
				}

				if (in_bitmap)
				{
					slot = node->ss.ss_ScanTupleSlot;
					tts_orioledb_store_tuple(slot, tuple,
											 descr, tupleCsn,
											 PrimaryIndexNumber,
											 true, &hint);
					if (BRIDGE_RECHECK(bridge_iter))
					{
						ExprContext *tup_econtext = bitmap_state->scan->ss->ps.ps_ExprContext;

						/*
						 * Initialize bitmapqualorig_state lazily on first
						 * recheck.  Plans without lossy bitmap pages never
						 * reach this branch, so we avoid building the
						 * ExprState for them entirely.
						 */
						if (bitmap_state->bitmapqualorig_state == NULL)
							bitmap_state->bitmapqualorig_state =
								ExecInitQual(bitmap_state->bitmapqualorig,
											 &node->ss.ps);

						slot_getallattrs(slot);
						tup_econtext->ecxt_scantuple = slot;

						if (!ExecQual(bitmap_state->bitmapqualorig_state, tup_econtext))
						{
							ExecClearTuple(slot);
						}
						else
						{
							fetched = true;
						}
					}
					else
					{
						fetched = true;
					}
				}
				else
				{
					/*
					 * Row's primary key is not in the bitmap, so this version
					 * isn't part of the result.  btree_seq_scan_getnext()
					 * palloc'd tuple.data in tupleCxt = ss_ScanTupleSlot's
					 * tts_mcxt (the per-query context) and we are not handing
					 * it to the slot, so it would otherwise accumulate there
					 * for every rejected primary row until end of query.
					 */
					pfree(tuple.data);
				}

				BRIDGE_NEXT_TUPLE(bridge_iter);
			}
		}

		if (!fetched)
			InstrCountFiltered2(node, 1);
		else if (!TupIsNull(slot) && !o_exec_qual(node->ss.ps.ps_ExprContext,
												  node->ss.ps.qual, slot))
			InstrCountFiltered1(node, 1);

	} while (!fetched || (!TupIsNull(slot) &&
						  !o_exec_qual(node->ss.ps.ps_ExprContext,
									   node->ss.ps.qual, slot)));
	return slot;
}

void
o_free_bitmap_scan(OBitmapScan *scan)
{
	if (scan->stream_primary)
	{
		int			i;

		for (i = 0; i < scan->stream_nchildren; i++)
		{
			OBitmapStreamChild *c = &scan->stream_children[i];

			if (c->scandesc_ready)
			{
				if (c->ostate.iterator)
					btree_iterator_free(c->ostate.iterator);
				btendscan(&c->ostate.scandesc);
				if (c->ostate.cxt)
					MemoryContextDelete(c->ostate.cxt);
			}
			if (c->index)
				index_close(c->index, AccessShareLock);
		}
		if (scan->stream_children)
			pfree(scan->stream_children);
		if (scan->stream_dedup)
			o_keybitmap_free(scan->stream_dedup);
		pfree(scan);
		return;
	}

	if (scan->seq_scan)
		free_btree_seq_scan(scan->seq_scan);
	if (scan->arg.bitmap)
		o_keybitmap_free(scan->arg.bitmap);
	if (scan->bridge_iter.tbmiterator)
#if PG_VERSION_NUM >= 180000
		tbm_end_private_iterate(scan->bridge_iter.tbmiterator);
#else
		tbm_end_iterate(scan->bridge_iter.tbmiterator);
#endif
	if (scan->bridge_iter.tidbitmap)
		tbm_free(scan->bridge_iter.tidbitmap);
	pfree(scan);
}

static bool
o_bitmap_is_range_valid(OTuple low, OTuple high, void *arg)
{
	BitmapSeqScanArg *barg = (BitmapSeqScanArg *) arg;
	OIndexDescr *primary = GET_PRIMARY(barg->tbl_desc);
	uint64		lowValue,
				highValue;

	if (o_keybitmap_pk_mode(primary, NULL) == O_KEYBITMAP_FIXED)
	{
		uint8		lowKey[OKBM_FIXED_BYTES];
		uint8		highKey[OKBM_FIXED_BYTES];

		if (!O_TUPLE_IS_NULL(low))
			o_pk_encode_nonleaf(low, primary, lowKey);
		else
			memset(lowKey, 0, OKBM_FIXED_BYTES);

		if (!O_TUPLE_IS_NULL(high))
			o_pk_encode_nonleaf(high, primary, highKey);
		else
			memset(highKey, 0xFF, OKBM_FIXED_BYTES);

		return o_keybitmap_range_is_valid_key(barg->bitmap, lowKey, highKey);
	}

	if (!O_TUPLE_IS_NULL(low))
		lowValue = primary_tuple_get_data(low, primary, true);
	else
		lowValue = 0;

	if (!O_TUPLE_IS_NULL(high))
		highValue = primary_tuple_get_data(high, primary, true);
	else
		highValue = UINT64_MAX;

	return o_keybitmap_range_is_valid(barg->bitmap,
									  lowValue, highValue);
}

/*
 * Rewrite key->tuple with the smallest bitmap key at or after the position it
 * carries (NULL tuple => from the start of the tree); return false when none
 * remains.  keyType selects how the incoming position is decoded and drives the
 * two levels of the bitmap-directed walk (see BTreeSeqScanCallbacks.getNextKey):
 *   - BTreeKeyLeafTuple: position is the current leaf tuple (per-tuple walk);
 *   - BTreeKeyNonLeafKey: position is an internal-page boundary -- a downlink
 *     separator or page hikey (skip whole pages / downlinks, always inclusive).
 * The two only differ in how the position's PK value is read (leaf vs non-leaf
 * layout); the value is looked up in the same bitmap and the result is built as
 * the same key either way.
 */
static bool
o_bitmap_get_next_key(OFixedKey *key, BTreeKeyType keyType, bool inclusive,
					  void *arg)
{
	BitmapSeqScanArg *barg = (BitmapSeqScanArg *) arg;
	bool		nonLeaf = (keyType == BTreeKeyNonLeafKey);
	bool		found;
	uint64		prev_value = 0;
	uint64		res_value;
	OTupleHeader tuphdr;
	OIndexDescr *primary = GET_PRIMARY(barg->tbl_desc);

	Assert(keyType == BTreeKeyLeafTuple || keyType == BTreeKeyNonLeafKey);

	if (o_keybitmap_pk_mode(primary, NULL) == O_KEYBITMAP_FIXED)
	{
		uint8		prevKey[OKBM_FIXED_BYTES];
		uint8		outKey[OKBM_FIXED_BYTES];

		if (!O_TUPLE_IS_NULL(key->tuple))
		{
			if (nonLeaf)
				o_pk_encode_nonleaf(key->tuple, primary, prevKey);
			else
				o_pk_encode_leaf(key->tuple, primary, prevKey);

			if (!inclusive)
			{
				int			i;

				/* smallest key strictly greater than prev */
				for (i = OKBM_FIXED_BYTES - 1; i >= 0; i--)
					if (++prevKey[i] != 0)
						break;
				if (i < 0)
				{
					O_TUPLE_SET_NULL(key->tuple);
					return false;
				}
			}
		}
		else
			memset(prevKey, 0, OKBM_FIXED_BYTES);

		if (o_keybitmap_get_next_key(barg->bitmap, prevKey, outKey))
		{
			o_pk_decode_to_key(outKey, primary, key);
			return true;
		}
		O_TUPLE_SET_NULL(key->tuple);
		return false;
	}

	if (!O_TUPLE_IS_NULL(key->tuple))
	{
		prev_value = primary_tuple_get_data(key->tuple, primary, nonLeaf);
		if (!inclusive)
		{
			if (prev_value == UINT64_MAX)
			{
				O_TUPLE_SET_NULL(key->tuple);
				return false;
			}
			prev_value++;
		}
	}

	res_value = o_keybitmap_get_next(barg->bitmap, prev_value,
									 &found);

	if (found)
	{
		FormData_pg_attribute *attr = TupleDescAttr(primary->nonLeafTupdesc, 0);

		Assert(primary->nFields == 1);
		tuphdr = (OTupleHeader) key->fixedData;
		tuphdr->hasnulls = false;
		tuphdr->natts = 1;
		tuphdr->len = SizeOfOTupleHeader + attr->attlen;
		uint64_get_val(res_value,
					   attr->atttypid,
					   &key->fixedData[SizeOfOTupleHeader]);
		key->tuple.data = key->fixedData;
		key->tuple.formatFlags = 0;
	}
	else
	{
		O_TUPLE_SET_NULL(key->tuple);
	}

	return found;
}

static void
bridge_begin_iterate(BridgeIterator *iter)
{
	Assert(iter->tidbitmap);

#if PG_VERSION_NUM >= 180000
	iter->tbmiterator = tbm_begin_private_iterate(iter->tidbitmap);
	iter->tbmres.blockno = InvalidBlockNumber;
#else
	iter->tbmiterator = tbm_begin_iterate(iter->tidbitmap);
	iter->tbmres = NULL;
#endif
}

static bool
bridge_iterate(BridgeIterator *iter)
{
#if PG_VERSION_NUM >= 180000
	if (!BlockNumberIsValid(iter->tbmres.blockno))
	{
		if (!tbm_private_iterate(iter->tbmiterator, &iter->tbmres))
			return false;
		if (!iter->tbmres.lossy)
			iter->iter_ntuples = tbm_extract_page_tuple(&iter->tbmres,
														iter->offsets,
														TBM_MAX_TUPLES_PER_PAGE);
	}
	return BlockNumberIsValid(iter->tbmres.blockno);
#else
	if (iter->tbmres == NULL)
		iter->tbmres = tbm_iterate(iter->tbmiterator);
	return iter->tbmres != NULL;
#endif
}

static void
bridge_next_page(OBitmapScan *scan, OBitmapHeapPlanState *bitmap_state)
{
	OIndexDescr *bridge = scan->arg.tbl_desc->bridge;
	BridgeIterator *iter;

	Assert(scan->bridge_iter.tbmiterator != NULL);
#if PG_VERSION_NUM >= 180000
	Assert(BlockNumberIsValid(scan->bridge_iter.tbmres.blockno));
#else
	Assert(scan->bridge_iter.tbmres != NULL);
#endif

	iter = &scan->bridge_iter;
	iter->cur_tuple = 0;
	iter->page_ntuples = 0;

	if (scan->arg.bitmap)
		o_keybitmap_free(scan->arg.bitmap);
	scan->arg.bitmap = o_keybitmap_create();
	if (scan->seq_scan)
		free_btree_seq_scan(scan->seq_scan);
	scan->seq_scan = make_btree_seq_scan_cb(&GET_PRIMARY(scan->arg.tbl_desc)->desc,
											&scan->oSnapshot,
											&bitmap_seq_scan_callbacks, &scan->arg);
	if (!BRIDGE_ITER_ISLOSSY(iter))
	{
		/*
		 * Bitmap is non-lossy, so we just look through the offsets listed in
		 * tbmres; but we have to follow any HOT chain starting at each such
		 * offset.
		 */
		int			curoff;

		iter->page_ntuples = BRIDGE_ITER_NTUPLES(iter);
		for (curoff = 0; curoff < BRIDGE_ITER_NTUPLES(iter); curoff++)
		{
#if PG_VERSION_NUM >= 180000
			OffsetNumber offnum = iter->offsets[curoff];
			BlockNumber blockno = iter->tbmres.blockno;
#else
			OffsetNumber offnum = iter->tbmres->offsets[curoff];
			BlockNumber blockno = iter->tbmres->blockno;
#endif
			ItemPointerData iptr;
			OBTreeKeyBound bridge_bound;
			OTuple		bridge_tup;
			uint64		data;
			CommitSeqNo tupleCsn;

			ItemPointerSet(&iptr, blockno, offnum);

			bridge_bound.nkeys = 1;
			bridge_bound.keys[0].value = ItemPointerGetDatum(&iptr);
			bridge_bound.keys[0].type = TIDOID;
			bridge_bound.keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
			bridge_bound.keys[0].comparator = NULL;
			bridge_bound.keys[0].exclusion_fn = NULL;
			bridge_bound.n_row_keys = 0;
			bridge_bound.row_keys = NULL;

			bridge_tup = o_btree_find_tuple_by_key(&bridge->desc,
												   (Pointer) &bridge_bound, BTreeKeyBound,
												   &o_in_progress_snapshot, &tupleCsn,
												   CurrentMemoryContext, NULL);

			if (!O_TUPLE_IS_NULL(bridge_tup))
			{
				data = seconary_tuple_get_pk_data(bridge_tup, bridge);
				o_keybitmap_insert(scan->arg.bitmap, data);

				pfree(bridge_tup.data);
			}
		}
	}
	else
	{
		/*
		 * Bitmap is lossy, so we must examine each line pointer on the page.
		 */

		OTableDescr *tbl_descr = scan->arg.tbl_desc;
		BTreeIterator *it;
		ItemPointerData start_iptr;
		ItemPointerData end_iptr;
		OBTreeKeyBound start_bound;
		OBTreeKeyBound end_bound;
		TupleTableSlot *primarySlot;
		ExprContext *tup_econtext = bitmap_state->scan->ss->ps.ps_ExprContext;
		CommitSeqNo tupleCsn;
#if PG_VERSION_NUM >= 180000
		BlockNumber blockno = iter->tbmres.blockno;
#else
		BlockNumber blockno = iter->tbmres->blockno;
#endif

		ItemPointerSet(&start_iptr, blockno, 0);
		start_bound.nkeys = 1;
		start_bound.keys[0].value = ItemPointerGetDatum(&start_iptr);
		start_bound.keys[0].type = TIDOID;
		start_bound.keys[0].flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_INCLUSIVE | O_VALUE_BOUND_COERCIBLE;
		start_bound.keys[0].comparator = bridge->fields[0].comparator;
		start_bound.keys[0].exclusion_fn = NULL;
		start_bound.n_row_keys = 0;
		start_bound.row_keys = NULL;

		ItemPointerSet(&end_iptr, blockno, MaxOffsetNumber);
		end_bound.nkeys = 1;
		end_bound.keys[0].value = ItemPointerGetDatum(&end_iptr);
		end_bound.keys[0].type = TIDOID;
		end_bound.keys[0].flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_INCLUSIVE | O_VALUE_BOUND_COERCIBLE;
		end_bound.keys[0].comparator = bridge->fields[0].comparator;
		end_bound.keys[0].exclusion_fn = NULL;
		end_bound.n_row_keys = 0;
		end_bound.row_keys = NULL;

		it = o_btree_iterator_create(&bridge->desc, (Pointer) &start_bound, BTreeKeyBound,
									 &o_in_progress_snapshot, ForwardScanDirection);
		primarySlot = MakeSingleTupleTableSlot(tbl_descr->tupdesc, &TTSOpsOrioleDB);

		do
		{
			OTuple		tup = o_btree_iterator_fetch(it, &tupleCsn,
													 (Pointer) &end_bound,
													 BTreeKeyBound, true,
													 NULL);
			uint64		data;

			if (O_TUPLE_IS_NULL(tup))
				break;

			data = seconary_tuple_get_pk_data(tup, bridge);
			o_keybitmap_insert(scan->arg.bitmap, data);
			iter->page_ntuples++;

			pfree(tup.data);
			ExecClearTuple(primarySlot);
			MemoryContextReset(tup_econtext->ecxt_per_tuple_memory);
		} while (true);

		ExecDropSingleTupleTableSlot(primarySlot);
		btree_iterator_free(it);
	}
}
