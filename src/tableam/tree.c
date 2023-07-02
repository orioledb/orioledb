/*-------------------------------------------------------------------------
 *
 * tree.c
 *		Implementation BTree interface methods for OrioleDB tables and
 *		related routines.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/tree.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/io.h"
#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"
#include "tableam/toast.h"
#include "tableam/tree.h"
#include "tuple/toast.h"
#include "utils/stopevent.h"

#include "access/nbtree.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

static uint32 o_idx_hash(BTreeDescr *desc, OTuple tuple, BTreeKeyType kind);
static uint32 o_toast_hash(BTreeDescr *desc, OTuple tuple, BTreeKeyType kind);
static uint32 o_idx_unique_hash(BTreeDescr *desc, OTuple tuple);
static int	o_idx_len(BTreeDescr *desc, OTuple tuple, OLengthType type);
static JsonbValue *o_key_to_jsonb(BTreeDescr *desc, OTuple key,
								  JsonbParseState **state);
static OTuple o_sidx_tuple_make_key(BTreeDescr *desc, OTuple tupl,
									Pointer data, bool keep_version,
									bool *allocated);
static OTuple o_tuple_make_key(BTreeDescr *desc, OTuple tuple,
							   Pointer data, bool keep_version,
							   bool *allocated);
static OTuple o_create_key_tuple(BTreeDescr *desc, OTuple tuple,
								 Pointer data, OIndexType type,
								 bool keep_version);
static bool pk_needs_undo(BTreeDescr *desc, BTreeOperationType action,
						  OTuple oldTuple, OTupleXactInfo oldXactInfo,
						  bool oldDeleted, OTuple newTuple, OXid newOxid);

static BTreeOps primaryOps = {
	.len = o_idx_len,
	.key_to_jsonb = o_key_to_jsonb,
	.tuple_make_key = o_tuple_make_key,
	.needs_undo = pk_needs_undo,
	.cmp = o_idx_cmp,
	.hash = o_idx_hash,
	.unique_hash = o_idx_unique_hash
},

			secondaryOps = {
	.len = o_idx_len,
	.key_to_jsonb = o_key_to_jsonb,
	.tuple_make_key = o_sidx_tuple_make_key,
	.needs_undo = NULL,
	.cmp = o_idx_cmp,
	.hash = o_idx_hash,
	.unique_hash = o_idx_unique_hash
},

			toastOps = {
	.len = o_idx_len,
	.key_to_jsonb = o_key_to_jsonb,
	.tuple_make_key = o_sidx_tuple_make_key,
	.needs_undo = o_toast_needs_undo,
	.cmp = o_toast_cmp,
	.hash = o_toast_hash,
	.unique_hash = NULL
};


void
index_btree_desc_init(BTreeDescr *desc, OCompress compress, ORelOids oids,
					  OIndexType type, bool temp_table,
					  OXid createOxid, void *arg)
{
	if (type == oIndexPrimary)
		desc->ops = &primaryOps;
	else if (type == oIndexToast)
		desc->ops = &toastOps;
	else
		desc->ops = &secondaryOps;

	desc->oids = oids;
	desc->arg = arg;
	desc->compress = compress;
	desc->type = type;
	desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
	desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
	desc->rootInfo.rootPageChangeCount = 0;
	btree_init_smgr(desc);
	desc->freeBuf.file = -1;
	desc->nextChkp[0].file = -1;
	desc->nextChkp[1].file = -1;
	desc->tmpBuf[0].file = -1;
	desc->tmpBuf[0].file = -1;
	desc->ppool = get_ppool(OPagePoolMain);
	if (temp_table)
		desc->storageType = BTreeStorageTemporary;
	else
		desc->storageType = BTreeStoragePersistence;
	desc->undoType = UndoReserveTxn;
	desc->createOxid = createOxid;
}

static inline OIndexDescr *
o_get_tree_def(BTreeDescr *desc)
{
	return desc->arg;
}

static int
o_get_key_len(BTreeDescr *desc, OTuple tuple, OIndexType type, bool keepVersion)
{
	OIndexDescr *id = o_get_tree_def(desc);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	int			i,
				len;

	for (i = 0; i < id->nonLeafTupdesc->natts; i++)
	{
		int			attnum = (type == oIndexPrimary) ? id->fields[i].tableAttnum : i + 1;

		Assert(attnum > 0);
		values[i] = o_fastgetattr(tuple, attnum, id->leafTupdesc, &id->leafSpec, &isnull[i]);
	}

	len = o_new_tuple_size(id->nonLeafTupdesc, &id->nonLeafSpec, NULL,
						   keepVersion ? o_tuple_get_version(tuple) : 0,
						   values, isnull, NULL);

	return len;
}

static int
o_idx_len(BTreeDescr *desc, OTuple tuple, OLengthType type)
{
	OIndexDescr *id = (OIndexDescr *) desc->arg;

	if (type == OTupleLength)
	{
		return o_tuple_size(tuple, &id->leafSpec);
	}
	else if (type == OKeyLength)
	{
		return o_tuple_size(tuple, &id->nonLeafSpec);
	}
	else
	{
		Assert(type == OTupleKeyLength || type == OTupleKeyLengthNoVersion);
		return o_get_key_len(desc, tuple, desc->type,
							 (type == OTupleKeyLength));
	}
}

/* creates index tuple from current index tuple */
static OTuple
o_create_key_tuple(BTreeDescr *desc, OTuple tuple, Pointer data,
				   OIndexType type, bool keep_version)
{
	OIndexDescr *id = o_get_tree_def(desc);
	Datum		key[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	int			i,
				len;
	OTuple		result;
	uint32		version = keep_version ? o_tuple_get_version(tuple) : 0;

	Assert(type == oIndexPrimary || type == oIndexRegular);

	for (i = 0; i < id->nonLeafTupdesc->natts; i++)
	{
		int			attnum = (type == oIndexPrimary) ? id->fields[i].tableAttnum : i + 1;

		Assert(attnum > 0);
		key[i] = o_fastgetattr(tuple, attnum, id->leafTupdesc, &id->leafSpec, &isnull[i]);
	}

	len = o_new_tuple_size(id->nonLeafTupdesc, &id->nonLeafSpec, NULL, version, key, isnull, NULL);
	if (data)
	{
		memset(data, 0, len);
		result.data = data;
	}
	else
	{
		result.data = (Pointer) palloc0(len);
	}
	o_tuple_fill(id->nonLeafTupdesc, &id->nonLeafSpec, &result, len, NULL, version, key, isnull, NULL);

	return result;
}

#define HASH_INITIAL (0x9e3779b9)

/*
 * Useful links:
 *
 * http://burtleburtle.net/bob/hash/index.html
 * http://burtleburtle.net/bob/hash/doobs.html
 */
static inline uint32
hash_combine_mix(char *key, uint32 len, uint32 hash)
{
	int			i;

	for (i = 0; i < len; ++i)
	{
		if (key[i] != 0)
		{
#ifdef WORDS_BIGENDIAN
			hash = (hash << 28) ^ (hash >> 4) ^ (uint32) key[i];
#else
			hash = (hash << 4) ^ (hash >> 28) ^ (uint32) key[i];
#endif
		}

		/*
		 * else helps us to get the same values for a key tuple without
		 * fetching datums from the tuple, see o_hash_key()
		 */
	}

	return hash;
}

static inline uint32
hash_final(uint32 hash)
{
#ifdef WORDS_BIGENDIAN
	return (hash ^ (hash >> 24) ^ (hash >> 16));
#else
	return (hash ^ (hash >> 8) ^ (hash >> 16));
#endif
}

/*
 * It's ok with inline and hash variable declaration as:
 *
 * register uint32 hash;
 *
 * Checked with gcc -O2
 */
static inline uint32
hash_combine_mix_field(TupleDesc tupdesc, OTupleFixedFormatSpec *spec,
					   OTuple tup, int attnum, uint32 hash)
{
	Form_pg_attribute att;
	Pointer		val_ptr = NULL;

	att = TupleDescAttr((tupdesc), (attnum) - 1);

	val_ptr = o_fastgetattr_ptr(tup, attnum, tupdesc, spec);

	if (val_ptr == NULL)
		return hash;

	if (att->attlen > 0)
	{
		hash = hash_combine_mix(val_ptr, att->attlen, hash);
	}
	else if (att->attlen == -1)
	{
		hash = hash_combine_mix(val_ptr, VARSIZE_ANY(val_ptr), hash);
	}
	else
	{
		Assert(att->attlen == -2);
		hash = hash_combine_mix((char *) (val_ptr),
								strlen((char *) (val_ptr)) + 1, hash);
	}

	return hash;
}

static uint32
o_hash_key(OIndexDescr *idx, OTuple key)
{
	register uint32 hash = HASH_INITIAL;
	int			size;
	Pointer		data;

	data = o_tuple_get_data(key, &size, &idx->nonLeafSpec);
	hash = hash_combine_mix(data, size, hash);
	hash = hash_final(hash);

	return hash;
}

static uint32
o_hash_key_from_tuple(OIndexDescr *idx, OTuple tuple)
{
	register uint32 hash = HASH_INITIAL;
	TupleDesc	tupdesc = idx->leafTupdesc;
	OTupleFixedFormatSpec *spec = &idx->leafSpec;
	int			i = 0;

	for (i = 0; i < idx->nonLeafTupdesc->natts; i++)
	{
		int			attnum;

		if (idx->desc.type == oIndexPrimary)
			attnum = idx->fields[i].tableAttnum;
		else
			attnum = i + 1;

		hash = hash_combine_mix_field(tupdesc, spec, tuple, attnum, hash);
	}

	hash = hash_final(hash);

	return hash;
}

static uint32
o_hash_key_from_toast_tuple(OIndexDescr *toast, OTuple tuple)
{
	register uint32 hash = HASH_INITIAL;
	TupleDesc	tupdesc = toast->leafTupdesc;
	OTupleFixedFormatSpec *spec = &toast->leafSpec;
	int			attnum,
				natts;

	natts = toast->nonLeafTupdesc->natts - TOAST_NON_LEAF_FIELDS_NUM;
	for (attnum = 1; attnum <= natts; attnum++)
		hash = hash_combine_mix_field(tupdesc, spec, tuple, attnum, hash);

	hash = hash_final(hash);

	return hash;
}

static uint32
o_hash_key_from_toast_key(OIndexDescr *toast, OTuple key)
{
	register uint32 hash = HASH_INITIAL;
	TupleDesc	tupdesc = toast->nonLeafTupdesc;
	OTupleFixedFormatSpec *spec = &toast->nonLeafSpec;
	int			attnum,
				natts;

	natts = tupdesc->natts - TOAST_NON_LEAF_FIELDS_NUM;
	for (attnum = 1; attnum <= natts; attnum++)
		hash = hash_combine_mix_field(tupdesc, spec, key, attnum, hash);

	hash = hash_final(hash);

	return hash;
}

static uint32
o_idx_hash(BTreeDescr *desc, OTuple tuple, BTreeKeyType kind)
{
	Assert(kind == BTreeKeyLeafTuple || kind == BTreeKeyNonLeafKey);

	if (kind == BTreeKeyLeafTuple)
		return o_hash_key_from_tuple((OIndexDescr *) desc->arg, tuple);
	else if (kind == BTreeKeyNonLeafKey)
		return o_hash_key((OIndexDescr *) desc->arg, tuple);
	else
		return 0;				/* keep compiler quiet */
}

static uint32
o_toast_hash(BTreeDescr *desc, OTuple tuple, BTreeKeyType kind)
{
	Assert(kind == BTreeKeyLeafTuple || kind == BTreeKeyNonLeafKey);

	if (kind == BTreeKeyLeafTuple)
		return o_hash_key_from_toast_tuple((OIndexDescr *) desc->arg, tuple);
	else if (kind == BTreeKeyNonLeafKey)
		return o_hash_key_from_toast_key((OIndexDescr *) desc->arg, tuple);
	else
		return 0;				/* keep compiler quiet */
}

/*
 * Provide hash for unique index insert.  It mixes tree oids with unique
 * fields.
 */
static uint32
o_idx_unique_hash(BTreeDescr *desc, OTuple tuple)
{
	OIndexDescr *idx = o_get_tree_def(desc);
	register uint32 hash = HASH_INITIAL;
	TupleDesc	tupdesc = idx->leafTupdesc;
	OTupleFixedFormatSpec *spec = &idx->leafSpec;
	int			i = 0,
				attnum;

	hash = hash_combine_mix((Pointer) &desc->oids, sizeof(desc->oids), hash);

	for (i = 0; i < idx->nUniqueFields; i++)
	{
		attnum = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple, idx, i + 1);
		hash = hash_combine_mix_field(tupdesc, spec, tuple, attnum, hash);
	}

	hash = hash_final(hash);

	return hash;
}

/* creates index tuple from table tuple for primary index */
static OTuple
o_tuple_make_key(BTreeDescr *desc, OTuple tuple, Pointer data,
				 bool keep_version, bool *allocated)
{
	*allocated = (data == NULL);
	return o_create_key_tuple(desc, tuple, data, oIndexPrimary, keep_version);
}

static OTuple
o_sidx_tuple_make_key(BTreeDescr *desc, OTuple tuple, Pointer data,
					  bool keep_version, bool *allocated)
{
	*allocated = (data == NULL);
	return o_create_key_tuple(desc, tuple, data, oIndexRegular, keep_version);
}


static inline bool
o_bound_is_coercible(OBTreeValueBound *bound, OIndexField *field)
{
	return (bound->flags & O_VALUE_BOUND_COERCIBLE) ||
		IsBinaryCoercible(bound->type, field->inputtype);
}

/* fills key bound from tuple or index tuple that belongs to current BTree */
void
o_fill_key_bound(OIndexDescr *id, OTuple tuple,
				 BTreeKeyType keyType, OBTreeKeyBound *bound)
{
	TupleDesc	tupdesc;
	OTupleFixedFormatSpec *spec;
	int			i,
				attnum;
	bool		isnull;

	Assert(keyType == BTreeKeyLeafTuple || keyType == BTreeKeyNonLeafKey);

	bound->nkeys = id->nonLeafTupdesc->natts;
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
	for (i = 0; i < id->nonLeafTupdesc->natts; i++)
	{
		attnum = OIndexKeyAttnumToTupleAttnum(keyType, id, i + 1);
		bound->keys[i].value = o_fastgetattr(tuple, attnum, tupdesc, spec, &isnull);
		bound->keys[i].type = tupdesc->attrs[attnum - 1].atttypid;
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = id->fields[i].comparator;
	}
}

/* fills secondary index key bound from primary index tuple */
void
o_fill_secondary_key_bound(BTreeDescr *primary, BTreeDescr *secondary,
						   OTuple tuple, TupleTableSlot *slot,
						   OBTreeKeyBound *bound)
{
	OIndexDescr *pt = o_get_tree_def(primary),
			   *st = o_get_tree_def(secondary);
	int			i,
				attnum;
	bool		isnull;
	ListCell   *indexpr_item = list_head(st->expressions_state);

	bound->nkeys = st->nonLeafTupdesc->natts;
	for (i = 0; i < st->nonLeafTupdesc->natts; i++)
	{
		attnum = st->fields[i].tableAttnum;
		if (attnum != EXPR_ATTNUM)
		{
			bound->keys[i].value = o_fastgetattr(tuple, attnum,
												 pt->leafTupdesc,
												 &pt->leafSpec, &isnull);
			bound->keys[i].type = pt->leafTupdesc->attrs[attnum - 1].atttypid;
		}
		else
		{
			ExprState  *expr_state = (ExprState *) lfirst(indexpr_item);

			st->econtext->ecxt_scantuple = slot;
			bound->keys[i].value = ExecEvalExprSwitchContext(expr_state,
															 st->econtext,
															 &isnull);
			bound->keys[i].type = st->nonLeafTupdesc->attrs[i].atttypid;
			indexpr_item = lnext(st->expressions_state, indexpr_item);
		}
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = st->fields[i].comparator;
	}
}

/* fills primary index key bound from tuple that belongs secondary index */
void
o_fill_pindex_tuple_key_bound(BTreeDescr *desc,
							  OTuple tup,
							  OBTreeKeyBound *bound)
{
	OIndexDescr *id = o_get_tree_def(desc);
	int			i;
	int			pk_from = id->nFields - id->nPrimaryFields;
	bool		isnull;

	bound->nkeys = id->nPrimaryFields;
	for (i = 0; i < id->nPrimaryFields; i++)
	{
		AttrNumber	attnum = id->primaryFieldsAttnums[i];

		bound->keys[i].value = o_fastgetattr(tup, attnum, id->leafTupdesc, &id->leafSpec, &isnull);
		bound->keys[i].type = id->leafTupdesc->attrs[pk_from + i].atttypid;
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = id->fields[pk_from + i].comparator;
	}
}

static int
cmp_inclusive(uint8 f)
{
	if ((f & O_VALUE_BOUND_LOWER))
		return (f & O_VALUE_BOUND_INCLUSIVE) ? -1 : 1;
	if ((f & O_VALUE_BOUND_UPPER))
		return (f & O_VALUE_BOUND_INCLUSIVE) ? 1 : -1;

	return 0;
}

static int
cmp_inclusive2(uint8 f1, uint8 f2)
{
	int			cmp1 = cmp_inclusive(f1),
				cmp2 = cmp_inclusive(f2);

	return cmp1 - cmp2;
}

int
o_idx_cmp_range_key_to_value(OBTreeValueBound *bound1, OIndexField *field,
							 Datum value, bool isnull)
{
	int			cmp;

	Assert(!(bound1->flags & O_VALUE_BOUND_UNBOUNDED));
	if (!(bound1->flags & O_VALUE_BOUND_NULL) && !isnull)
	{
		if ((bound1->flags & O_VALUE_BOUND_COERCIBLE) && bound1->value == value)
			cmp = 0;
		else if (o_bound_is_coercible(bound1, field))
			cmp = o_call_comparator(field->comparator, bound1->value, value);
		else
			cmp = o_call_comparator(bound1->comparator, bound1->value, value);

		if (!field->ascending)
			cmp = -cmp;

		if (cmp == 0 && !(bound1->flags & O_VALUE_BOUND_INCLUSIVE))
			cmp = cmp_inclusive(bound1->flags);

		return cmp;
	}
	else
	{
		Assert((bound1->flags & O_VALUE_BOUND_NULL) || isnull);
		if ((bound1->flags & O_VALUE_BOUND_NULL) && isnull)
			return (bound1->flags & O_VALUE_BOUND_INCLUSIVE) ? 0 : cmp_inclusive(bound1->flags);
		else if (isnull)
			return field->nullfirst ? 1 : -1;
		else
			return field->nullfirst ? -1 : 1;
	}
}

static int
o_idx_cmp_tuples(OIndexDescr *id,
				 OTuple *tuple1, BTreeKeyType keyType1,
				 OTuple *tuple2, BTreeKeyType keyType2)
{
	TupleDesc	tupdesc1,
				tupdesc2;
	OTupleFixedFormatSpec *spec1,
			   *spec2;
	int			i,
				n,
				attnum1,
				attnum2;
	Datum		value1,
				value2;
	bool		isnull1,
				isnull2;

	Assert(keyType1 == BTreeKeyLeafTuple || keyType1 == BTreeKeyNonLeafKey);
	if (keyType1 == BTreeKeyLeafTuple)
	{
		tupdesc1 = id->leafTupdesc;
		spec1 = &id->leafSpec;
	}
	else
	{
		tupdesc1 = id->nonLeafTupdesc;
		spec1 = &id->nonLeafSpec;
	}

	Assert(keyType2 == BTreeKeyLeafTuple || keyType2 == BTreeKeyNonLeafKey);
	if (keyType2 == BTreeKeyLeafTuple)
	{
		tupdesc2 = id->leafTupdesc;
		spec2 = &id->leafSpec;
	}
	else
	{
		tupdesc2 = id->nonLeafTupdesc;
		spec2 = &id->nonLeafSpec;
	}

	n = id->nonLeafTupdesc->natts;
	for (i = 0; i < n; i++)
	{
		if (!OIgnoreColumn(id, i))
		{
			OIndexField *field = &id->fields[i];
			int			cmp = 0;

			attnum1 = OIndexKeyAttnumToTupleAttnum(keyType1, id, i + 1);
			value1 = o_fastgetattr(*tuple1, attnum1, tupdesc1, spec1, &isnull1);
			attnum2 = OIndexKeyAttnumToTupleAttnum(keyType2, id, i + 1);
			value2 = o_fastgetattr(*tuple2, attnum2, tupdesc2, spec2, &isnull2);

			if (!isnull1 && !isnull2)
			{
				cmp = o_call_comparator(field->comparator, value1, value2);
				if (!field->ascending)
					cmp = -cmp;
			}
			else if (isnull1 && isnull2)
				cmp = 0;
			else if (isnull1)
				cmp = field->nullfirst ? -1 : 1;
			else if (isnull2)
				cmp = field->nullfirst ? 1 : -1;

			if (cmp != 0)
				return cmp;
		}
	}
	return 0;
}

static int
o_idx_cmp_key_bound_to_tuple(OIndexDescr *id,
							 OBTreeKeyBound *key1, BTreeKeyType keyType1,
							 OTuple *tuple2, BTreeKeyType keyType2)
{
	TupleDesc	tupdesc;
	OTupleFixedFormatSpec *spec;
	int			i,
				n,
				attnum;
	Datum		value;
	bool		isnull;

	Assert(keyType2 == BTreeKeyLeafTuple || keyType2 == BTreeKeyNonLeafKey);

	if (keyType2 == BTreeKeyLeafTuple)
	{
		tupdesc = id->leafTupdesc;
		spec = &id->leafSpec;
	}
	else
	{
		tupdesc = id->nonLeafTupdesc;
		spec = &id->nonLeafSpec;
	}
	if (keyType1 == BTreeKeyBound)
	{
		n = id->nonLeafTupdesc->natts;
	}
	else
	{
		Assert(keyType1 == BTreeKeyUniqueLowerBound ||
			   keyType1 == BTreeKeyUniqueUpperBound);
		n = id->nUniqueFields;
	}

	for (i = 0; i < n; i++)
	{
		if (!OIgnoreColumn(id, i))
		{
			uint8		flags = key1->keys[i].flags;
			int			cmp;

			if (flags & O_VALUE_BOUND_UNBOUNDED)
				return (flags & O_VALUE_BOUND_LOWER) ? -1 : 1;

			attnum = OIndexKeyAttnumToTupleAttnum(keyType2, id, i + 1);
			value = o_fastgetattr(*tuple2, attnum, tupdesc, spec, &isnull);

			cmp = o_idx_cmp_range_key_to_value(&key1->keys[i], &id->fields[i],
											   value, isnull);
			if (cmp != 0)
				return cmp;
		}
	}

	if (keyType1 == BTreeKeyUniqueLowerBound)
		return -1;
	else if (keyType1 == BTreeKeyUniqueUpperBound)
		return 1;
	return 0;
}

int
o_idx_cmp_value_bounds(OBTreeValueBound *bound1,
					   OBTreeValueBound *bound2,
					   OIndexField *field,
					   bool *equal)
{
	/* Keep clang analyzer quiet */
#ifndef __clang_analyzer__
	int			res;

	if (equal)
		*equal = false;

	if ((bound1->flags & O_VALUE_BOUND_NO_VALUE) == 0 &&
		(bound2->flags & O_VALUE_BOUND_NO_VALUE) == 0)
	{
		/* Handle normal values */
		if ((bound1->flags & bound2->flags & O_VALUE_BOUND_COERCIBLE) &&
			bound1->value == bound2->value)
		{
			res = 0;
		}
		else
		{
			bool		coercible1 = o_bound_is_coercible(bound1, field);
			bool		coercible2 = o_bound_is_coercible(bound2, field);

			if (coercible1 && coercible2)
				res = o_call_comparator(field->comparator, bound1->value,
										bound2->value);
			else if (coercible1)
				res = -o_call_comparator(bound2->comparator, bound2->value,
										 bound1->value);
			else if (coercible2)
				res = o_call_comparator(bound1->comparator, bound1->value,
										bound2->value);
			else
				res = o_call_comparator(o_find_comparator(field->opfamily,
														  bound1->type,
														  bound2->type,
														  field->collation),
										bound1->value,
										bound2->value);
		}

		if (!field->ascending)
			res = -res;

		if (res == 0)
		{
			res = cmp_inclusive2(bound1->flags, bound2->flags);
			if (equal &&
				(bound1->flags & O_VALUE_BOUND_INCLUSIVE) &&
				(bound2->flags & O_VALUE_BOUND_INCLUSIVE))
				*equal = true;

		}
	}
	else if ((bound1->flags & O_VALUE_BOUND_UNBOUNDED) ||
			 (bound2->flags & O_VALUE_BOUND_UNBOUNDED))
	{
		/* Handle infinities */
		if ((bound1->flags & O_VALUE_BOUND_UNBOUNDED) &&
			(bound2->flags & O_VALUE_BOUND_UNBOUNDED))
		{
			if ((bound1->flags & O_VALUE_BOUND_DIRECTIONS) ==
				(bound2->flags & O_VALUE_BOUND_DIRECTIONS))
				return 0;
			else
				return (bound1->flags & O_VALUE_BOUND_LOWER) ? -1 : 1;
		}
		else if (bound1->flags & O_VALUE_BOUND_UNBOUNDED)
			return (bound1->flags & O_VALUE_BOUND_LOWER) ? -1 : 1;
		else
			return (bound2->flags & O_VALUE_BOUND_LOWER) ? 1 : -1;

	}
	else if ((bound1->flags & O_VALUE_BOUND_NULL) ||
			 (bound2->flags & O_VALUE_BOUND_NULL))
	{
		/* Handle nulls */
		if ((bound1->flags & O_VALUE_BOUND_NULL) &&
			(bound2->flags & O_VALUE_BOUND_NULL))
			res = cmp_inclusive2(bound1->flags, bound2->flags);
		else if (bound1->flags & O_VALUE_BOUND_NULL)
			res = field->nullfirst ? -1 : 1;
		else
			res = field->nullfirst ? 1 : -1;
	}
	else
	{
		Assert(false);
		res = 0;
	}

	return res;
#else
	return 0;
#endif
}


int
o_idx_cmp(BTreeDescr *desc,
		  void *p1, BTreeKeyType keyType1,
		  void *p2, BTreeKeyType keyType2)
{
	/* Keep clang analyzer quiet */
#ifndef __clang_analyzer__
	OIndexDescr *id = o_get_tree_def(desc);
	OBTreeKeyBound *key1,
			   *key2;
	int			i,
				n;
	int			cmp;

	o_set_sys_cache_search_datoid(desc->oids.datoid);

	if (!IS_BOUND_KEY_TYPE(keyType1) || !IS_BOUND_KEY_TYPE(keyType2))
	{
		if (IS_BOUND_KEY_TYPE(keyType1))
			return o_idx_cmp_key_bound_to_tuple(id,
												(OBTreeKeyBound *) p1,
												keyType1,
												(OTuple *) p2,
												keyType2);
		if (IS_BOUND_KEY_TYPE(keyType2))
			return -o_idx_cmp_key_bound_to_tuple(id,
												 (OBTreeKeyBound *) p2,
												 keyType2,
												 (OTuple *) p1,
												 keyType1);
		return o_idx_cmp_tuples(id,
								(OTuple *) p1,
								keyType1,
								(OTuple *) p2,
								keyType2);
	}

	key1 = (OBTreeKeyBound *) p1;
	key2 = (OBTreeKeyBound *) p2;

	Assert(key1->nkeys == id->nonLeafTupdesc->natts);
	Assert(key2->nkeys == id->nonLeafTupdesc->natts);

	if (keyType1 != BTreeKeyBound || keyType2 != BTreeKeyBound)
		n = id->nUniqueFields;
	else
		n = key1->nkeys;

	for (i = 0; i < n; i++)
	{
		if (!OIgnoreColumn(id, i))
		{
			cmp = o_idx_cmp_value_bounds(&key1->keys[i],
										 &key2->keys[i],
										 &id->fields[i],
										 NULL);
			if (cmp)
				return cmp;
		}
	}
#endif

	if (keyType1 != keyType2)
	{
		if (keyType1 == BTreeKeyUniqueLowerBound || keyType2 == BTreeKeyUniqueUpperBound)
			return -1;
		if (keyType1 == BTreeKeyUniqueUpperBound || keyType2 == BTreeKeyUniqueLowerBound)
			return 1;
	}

	return 0;
}

static bool
pk_needs_undo(BTreeDescr *desc, BTreeOperationType action,
			  OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
			  OTuple newTuple, OXid newOxid)
{
	if (action == BTreeOperationDelete)
		return true;

	if (!XACT_INFO_OXID_EQ(oldXactInfo, newOxid))
		return false;

	if (oldDeleted && o_tuple_get_version(oldTuple) + 1 == o_tuple_get_version(newTuple))
		return false;

	if (!O_TUPLE_IS_NULL(newTuple) && is_recovery_process() &&
		o_tuple_get_version(oldTuple) >= o_tuple_get_version(newTuple))
		return false;

	return true;
}

static void
o_key_to_jsonb_internal(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
						int natts, OTuple key, JsonbParseState **state)
{
	int			i;

	for (i = 0; i < natts; i++)
	{
		Datum		value;
		bool		isnull;
		JsonbValue	jval;
		ItemPointer iptr;
		BlockNumber blkno;
		OffsetNumber offset;

		jsonb_push_key(state, TupleDescAttr(tupleDesc, i)->attname.data);

		value = o_fastgetattr(key, i + 1, tupleDesc, spec, &isnull);

		if (isnull)
		{
			jval.type = jbvNull;
			(void) pushJsonbValue(state, WJB_VALUE, &jval);
			continue;
		}

		switch (TupleDescAttr(tupleDesc, i)->atttypid)
		{
			case TEXTOID:
				jval.type = jbvString;
				jval.val.string.len = VARSIZE_ANY_EXHDR(value);
				jval.val.string.val = VARDATA_ANY(value);
				(void) pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case TIDOID:
				iptr = DatumGetItemPointer(value);
				blkno = ItemPointerGetOffsetNumberNoCheck(iptr);
				offset = ItemPointerGetOffsetNumberNoCheck(iptr);

				jval.type = jbvNumeric;
				(void) pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum((int64) blkno)));
				(void) pushJsonbValue(state, WJB_ELEM, &jval);
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int32GetDatum((int32) offset)));
				(void) pushJsonbValue(state, WJB_ELEM, &jval);
				(void) pushJsonbValue(state, WJB_END_ARRAY, NULL);
				break;

			case INT2OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int2_numeric, value));
				(void) pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case INT4OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, value));
				(void) pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case INT8OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, value));
				(void) pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case FLOAT4OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(float4_numeric, value));
				(void) pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case FLOAT8OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(float8_numeric, value));
				(void) pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			default:
				jval.type = jbvNull;
				(void) pushJsonbValue(state, WJB_VALUE, &jval);
				break;
		}
	}
}

static JsonbValue *
o_key_to_jsonb(BTreeDescr *desc, OTuple key, JsonbParseState **state)
{
	OIndexDescr *id = o_get_tree_def(desc);

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	o_key_to_jsonb_internal(id->nonLeafTupdesc,
							&id->nonLeafSpec,
							id->nonLeafTupdesc->natts,
							key, state);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}
