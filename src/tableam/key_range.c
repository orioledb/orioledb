/*-------------------------------------------------------------------------
 *
 * key_range.c
 *		Function dealing with key ranges for planning and execution stage
 *		in OrioleDB.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/key_range.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "tableam/key_range.h"
#include "tableam/tree.h"

#include "access/stratnum.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/lsyscache.h"

static bool o_key_range_is_unbounded(OBTreeKeyRange *range, int attnum);
static void o_fill_key_bounds(Datum v, Oid type,
							  OBTreeValueBound *low, OBTreeValueBound *high,
							  OIndexField *field);

static OBTreeValueBound *
o_fill_row_key_bound(OBTreeKeyBound *bound,
					 bool first_subkey, bool last_subkey,
					 AttrNumber subattnum, uint8 flags)
{
	OBtreeRowKeyBound *rowkey;
	OBTreeValueBound *result;

	if (first_subkey)
	{
		bound->n_row_keys++;
		if (bound->n_row_keys - 1 == 0)
			bound->row_keys = palloc0(sizeof(OBtreeRowKeyBound) *
									  bound->n_row_keys);
		else
			bound->row_keys = repalloc(bound->row_keys,
									   sizeof(OBtreeRowKeyBound) *
									   bound->n_row_keys);
	}
	rowkey = &bound->row_keys[bound->n_row_keys - 1];
	if (first_subkey)
		rowkey->nkeys = 0;
	rowkey->nkeys++;
	if (rowkey->nkeys - 1 == 0)
	{
		rowkey->keys = palloc0(sizeof(OBTreeValueBound) * rowkey->nkeys);
		rowkey->keynums = palloc0(sizeof(int) * rowkey->nkeys);
	}
	else
	{
		rowkey->keys = repalloc(rowkey->keys, sizeof(OBTreeValueBound) *
								rowkey->nkeys);
		rowkey->keynums = repalloc(rowkey->keynums, sizeof(int) *
								   rowkey->nkeys);
	}
	result = &rowkey->keys[rowkey->nkeys - 1];
	rowkey->keynums[rowkey->nkeys - 1] = subattnum;
	result->flags = flags;
	if (!last_subkey)
		result->flags |= O_VALUE_BOUND_INCLUSIVE;

	return result;
}

bool
o_key_data_to_key_range(OBTreeKeyRange *res, ScanKeyData *keyData,
						int numberOfKeys, BTArrayKeyInfo *arrayKeys,
						int numPrefixExactKeys,
						int resultNKeys, OIndexField *fields)
{
	int			i;
	bool		exact = true;

	res->empty = false;
	res->low.nkeys = resultNKeys;
	res->high.nkeys = resultNKeys;

	for (i = 0; i < resultNKeys; i++)
	{
		res->low.keys[i].flags = O_VALUE_BOUND_MINUS_INFINITY;
		res->high.keys[i].flags = O_VALUE_BOUND_PLUS_INFINITY;
	}

	elog(WARNING, "numberOfKeys: %d", numberOfKeys);
	for (i = 0; i < numberOfKeys; i++)
	{
		bool		setLow = false,
					setHigh = false;
		ScanKeyData *key = &keyData[i];
		AttrNumber	attnum = key->sk_attno - 1;
		OBTreeValueBound low = {0, 0, O_VALUE_BOUND_MINUS_INFINITY, NULL};
		OBTreeValueBound high = {0, 0, O_VALUE_BOUND_PLUS_INFINITY, NULL};
		OIndexField *field = &fields[attnum];

		if (field->exclusion_fn)
			elog(WARNING, "field[%d]: exclusion_fn: %u", attnum, field->exclusion_fn->finfo.fn_oid);
		elog(WARNING, "fn_oid[%d]: %u", i, key->sk_func.fn_oid);
		elog(WARNING, "sk_sybtype[%d]: %d", i, key->sk_subtype);
		elog(WARNING, "sk_strategy[%d]: %s", i,
			 key->sk_strategy == BTLessStrategyNumber ? "BTLessStrategyNumber" :
			 key->sk_strategy == BTLessEqualStrategyNumber ? "BTLessEqualStrategyNumber" :
			 key->sk_strategy == BTEqualStrategyNumber ? "BTEqualStrategyNumber" :
			 key->sk_strategy == BTGreaterStrategyNumber ? "BTGreaterStrategyNumber" :
			 key->sk_strategy == BTGreaterEqualStrategyNumber ? "BTGreaterEqualStrategyNumber" :
			 "WRONG");
		elog(WARNING, "sk_flags[%d]: %X: %s%s%s%s%s%s%s%s%s %s%s%s%s %s", i,
			 key->sk_flags,

			 key->sk_flags & SK_ISNULL ? "SK_ISNULL " : "",
			 key->sk_flags & SK_UNARY ? "SK_UNARY " : "",
			 key->sk_flags & SK_ROW_HEADER ? "SK_ROW_HEADER " : "",
			 key->sk_flags & SK_ROW_MEMBER ? "SK_ROW_MEMBER " : "",
			 key->sk_flags & SK_ROW_END ? "SK_ROW_END " : "",
			 key->sk_flags & SK_SEARCHARRAY ? "SK_SEARCHARRAY " : "",
			 key->sk_flags & SK_SEARCHNULL ? "SK_SEARCHNULL " : "",
			 key->sk_flags & SK_SEARCHNOTNULL ? "SK_SEARCHNOTNULL " : "",
			 key->sk_flags & SK_ORDER_BY ? "SK_ORDER_BY " : "",
			 
			 key->sk_flags & SK_BT_REQFWD ? "SK_BT_REQFWD " : "",
			 key->sk_flags & SK_BT_REQBKWD ? "SK_BT_REQBKWD " : "",
			 key->sk_flags & SK_BT_DESC ? "SK_BT_DESC " : "",
			 key->sk_flags & SK_BT_NULLS_FIRST ? "SK_BT_NULLS_FIRST " : "",
			 "");

		switch (key->sk_strategy)
		{
			case BTLessStrategyNumber:
			case BTLessEqualStrategyNumber:
				if (key->sk_flags & SK_SEARCHNOTNULL)
				{
					if (!field->nullfirst)
						high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_NULL;
				}
				else
				{
					setHigh = true;
					high.flags = O_VALUE_BOUND_UPPER;
					if (key->sk_strategy == BTLessEqualStrategyNumber)
						high.flags |= O_VALUE_BOUND_INCLUSIVE;
					if (field->nullfirst)
					{
						setLow = true;
						low.flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_NULL;
					}
				}
				break;
			case BTEqualStrategyNumber:
				if (key->sk_flags & SK_SEARCHNULL)
				{
					low.flags = O_VALUE_BOUND_LOWER |
						O_VALUE_BOUND_INCLUSIVE |
						O_VALUE_BOUND_NULL;
					high.flags = O_VALUE_BOUND_UPPER |
						O_VALUE_BOUND_INCLUSIVE |
						O_VALUE_BOUND_NULL;
				}
				else
				{
					if (field->exclusion_fn)
					{
						low.exclusion_fn = field->exclusion_fn;
						high.exclusion_fn = field->exclusion_fn;
					}
					else
					{
						low.flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_INCLUSIVE;
						high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_INCLUSIVE;
						setLow = true;
						setHigh = true;
					}
				}
				break;
			case BTGreaterStrategyNumber:
			case BTGreaterEqualStrategyNumber:
				if (key->sk_flags & SK_SEARCHNOTNULL)
				{
					if (field->nullfirst)
						low.flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_NULL;

				}
				else
				{
					setLow = true;
					low.flags = O_VALUE_BOUND_LOWER;
					if (key->sk_strategy == BTGreaterEqualStrategyNumber)
						low.flags |= O_VALUE_BOUND_INCLUSIVE;
					if (!field->nullfirst)
					{
						setHigh = true;
						high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_NULL;
					}
				}
				break;

			default:
				Assert(false);
				break;
		}

		if ((key->sk_flags & SK_SEARCHARRAY) &&
			key->sk_strategy == BTEqualStrategyNumber)
		{
			Assert(arrayKeys && arrayKeys->num_elems > 0);
			if (o_key_range_is_unbounded(res, attnum))
			{
				if (i < numPrefixExactKeys)
				{
					o_fill_key_bounds(arrayKeys->elem_values[arrayKeys->cur_elem],
									  key->sk_subtype,
									  setLow ? &low : NULL,
									  setHigh ? &high : NULL,
									  field);
				}
				else
				{
					o_fill_key_bounds(arrayKeys->elem_values[0],
									  key->sk_subtype,
									  setLow ? &low : NULL,
									  NULL,
									  field);
					o_fill_key_bounds(arrayKeys->elem_values[arrayKeys->num_elems - 1],
									  key->sk_subtype,
									  NULL,
									  setHigh ? &high : NULL,
									  field);
				}
				if (setLow)
					res->low.keys[attnum] = low;
				if (setHigh)
					res->high.keys[attnum] = high;
			}
			arrayKeys++;
		}
		else if (key->sk_flags & SK_ROW_HEADER)
		{
			ScanKeyData *subkey;
			bool		first_subkey = true;
			bool		last_subkey = false;

			subkey = (ScanKey) DatumGetPointer(key->sk_argument);

			while (!last_subkey)
			{
				AttrNumber	subattnum;
				OIndexField *subfield;
				OBTreeValueBound *sublow = NULL;
				OBTreeValueBound *subhigh = NULL;

				last_subkey = subkey->sk_flags & SK_ROW_END;

				Assert(subkey->sk_flags & SK_ROW_MEMBER);

				subattnum = subkey->sk_attno - 1;
				subfield = &fields[subattnum];

				if (setLow)
					sublow = o_fill_row_key_bound(&res->low,
												  first_subkey, last_subkey,
												  subattnum, low.flags);
				if (setHigh)
					subhigh = o_fill_row_key_bound(&res->high,
												   first_subkey, last_subkey,
												   subattnum, high.flags);

				o_fill_key_bounds(subkey->sk_argument, subkey->sk_subtype,
								  sublow, subhigh,
								  subfield);
				first_subkey = false;
				if (!last_subkey)
					subkey++;
			}
		}
		else
		{
			Oid			type = key->sk_subtype;

			if (!OidIsValid(type))
				type = field->inputtype;

			o_fill_key_bounds(key->sk_argument, type,
							  setLow ? &low : NULL,
							  setHigh ? &high : NULL,
							  field);
			if (o_idx_cmp_value_bounds(&low, &res->low.keys[attnum],
									   field, NULL) >= 0)
				res->low.keys[attnum] = low;
			if (o_idx_cmp_value_bounds(&high, &res->high.keys[attnum],
									   field, NULL) <= 0)
				res->high.keys[attnum] = high;
		}
	}

	for (i = 0; i < resultNKeys; i++)
	{
		bool		equals;

		if (o_idx_cmp_value_bounds(&res->low.keys[i],
								   &res->high.keys[i],
								   &fields[i],
								   &equals) >= 0)
		{
			res->empty = true;
			return false;
		}

		if (!equals)
			exact = false;
	}
	return exact;
}

static void
o_fill_key_bounds(Datum v, Oid type,
				  OBTreeValueBound *low, OBTreeValueBound *high,
				  OIndexField *field)
{
	bool		coercible = false;
	OComparator *comparator = NULL;

	if (!low && !high)
		return;

	if (type == field->opclass || type == field->inputtype ||
		IsBinaryCoercible(type, field->inputtype))
		coercible = true;
	else
		comparator = o_find_comparator(field->opfamily, type,
									   field->inputtype,
									   field->collation);

	if (low != NULL)
	{
		low->value = v;
		low->type = type;
		low->comparator = comparator;
		if (coercible)
			low->flags |= O_VALUE_BOUND_COERCIBLE;
	}
	if (high != NULL)
	{
		high->value = v;
		high->type = type;
		high->comparator = comparator;
		if (coercible)
			high->flags |= O_VALUE_BOUND_COERCIBLE;
	}
}

static bool
o_key_range_is_unbounded(OBTreeKeyRange *range, int attnum)
{
	if (range->low.keys[attnum].flags == O_VALUE_BOUND_MINUS_INFINITY &&
		range->high.keys[attnum].flags == O_VALUE_BOUND_PLUS_INFINITY)
		return true;
	else
		return false;
}
