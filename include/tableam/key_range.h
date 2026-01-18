/*-------------------------------------------------------------------------
 *
 * key_range.h
 *		Declarations of range of keys.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/key_range.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_KEY_RANGE_H__
#define __TABLEAM_KEY_RANGE_H__

#include "tableam/descr.h"

#include "parser/parse_coerce.h"

#define O_VALUE_BOUND_INCLUSIVE 0x01
#define O_VALUE_BOUND_NULL 0x02
#define O_VALUE_BOUND_UNBOUNDED 0x04
#define O_VALUE_BOUND_LOWER 0x08
#define O_VALUE_BOUND_UPPER 0x10
#define O_VALUE_BOUND_COERCIBLE 0x20
#define O_VALUE_BOUND_NON_COERCIBLE 0x40
#define O_VALUE_BOUND_DIRECTIONS (O_VALUE_BOUND_LOWER | O_VALUE_BOUND_UPPER)
#define O_VALUE_BOUND_NO_VALUE (O_VALUE_BOUND_NULL | O_VALUE_BOUND_UNBOUNDED)
#define O_VALUE_BOUND_MINUS_INFINITY (O_VALUE_BOUND_LOWER | O_VALUE_BOUND_UNBOUNDED)
#define O_VALUE_BOUND_PLUS_INFINITY (O_VALUE_BOUND_UPPER | O_VALUE_BOUND_UNBOUNDED)
#define O_VALUE_BOUND_PLAIN_VALUE (O_VALUE_BOUND_LOWER | O_VALUE_BOUND_INCLUSIVE | O_VALUE_BOUND_COERCIBLE)

typedef struct
{
	Datum		value;
	Oid			type;
	uint8		flags;

	/*
	 * We're going to do many comparisons between bound value and tuple
	 * values. It would be very slow to lookup for the comparator each time.
	 * So if types don't match, we do cache the comaparator.
	 */
	OComparator *comparator;
	OExclusionFn *exclusion_fn;
} OBTreeValueBound;

typedef struct OBtreeRowKeyBound
{
	int			nkeys;
	int		   *keynums;
	OBTreeValueBound *keys;
} OBtreeRowKeyBound;

typedef struct
{
	int			nkeys;
	OBTreeValueBound keys[INDEX_MAX_KEYS];
	int			n_row_keys;
	OBtreeRowKeyBound *row_keys;
} OBTreeKeyBound;

typedef struct
{
	bool		empty;
	OBTreeKeyBound low;
	OBTreeKeyBound high;
} OBTreeKeyRange;

static inline bool
o_bound_is_coercible(OBTreeValueBound *bound, OIndexField *field)
{
	bool		result;

	if (bound->flags & O_VALUE_BOUND_COERCIBLE)
		return true;
	if (bound->flags & O_VALUE_BOUND_NON_COERCIBLE)
		return false;
	/* Neither flag is set, compute and cache the result */
	result = IsBinaryCoercible(bound->type, field->inputtype);
	bound->flags |= result ? O_VALUE_BOUND_COERCIBLE : O_VALUE_BOUND_NON_COERCIBLE;
	return result;
}

extern bool o_key_data_to_key_range(OBTreeKeyRange *res,
									ScanKeyData *keyData,
									int numberOfKeys,
									BTArrayKeyInfo *arrayKeys,
									int numPrefixExactKeys,
									int resultNKeys,
									OIndexField *fields);

#endif
