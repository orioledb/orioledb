/*-------------------------------------------------------------------------
 *
 * format.h
 * 		Declarations for orioledb tuple format.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tuple/format.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TUPLE_FORMAT_H__
#define __TUPLE_FORMAT_H__

#include "postgres.h"

typedef struct
{
	TupleDesc	desc;
	char	   *tp;
	bits8	   *bp;
	uint32		off;
	uint16		attnum;
	uint16		natts;
	bool		hasnulls;
	bool		slow;
} OTupleReaderState;

typedef struct
{
	uint16		hasnulls:1,
				len:15;
	uint16		natts;
	uint32		version;
} OTupleHeaderData;

#define O_TUPLE_FLAGS_FIXED_FORMAT	0x1

/*
 * `len` is 15 bits, which everything stored in a B-tree fits into several
 * times over (O_BTREE_MAX_TUPLE_SIZE).  A tuple built for WAL alone has no
 * such bound -- the old tuple of a REPLICA IDENTITY FULL record carries its
 * TOASTed attributes inline -- so a longer one sets `len` to O_TUPLE_LEN_LONG
 * and puts its real length in a uint32 right after the header.  The payload
 * then starts SizeOfOTupleHeaderLong in.
 *
 * O_TUPLE_LEN_LONG is the largest value the field can hold, so a short tuple
 * can never be mistaken for a long one: a tuple of exactly that many bytes
 * simply takes the long form too.
 */
#define O_TUPLE_LEN_LONG			((1 << 15) - 1)
#define SizeOfOTupleHeaderLong		(SizeOfOTupleHeader + MAXALIGN(sizeof(uint32)))

#define OTupleHeaderIsLong(hdr)		((hdr)->len == O_TUPLE_LEN_LONG)
#define OTupleHeaderLongLen(hdr)	\
	(*((uint32 *) ((Pointer) (hdr) + SizeOfOTupleHeader)))
#define OTupleHeaderGetLen(hdr)		\
	(OTupleHeaderIsLong(hdr) ? OTupleHeaderLongLen(hdr) : (uint32) (hdr)->len)
#define OTupleHeaderDataOff(hdr)	\
	(OTupleHeaderIsLong(hdr) ? SizeOfOTupleHeaderLong : SizeOfOTupleHeader)

typedef struct
{
	uint16		natts;
	uint16		len;
} OTupleFixedFormatSpec;

typedef OTupleHeaderData *OTupleHeader;
#define SizeOfOTupleHeader MAXALIGN(sizeof(OTupleHeaderData))

typedef struct BridgeData
{
	bool		is_pkey;
	ItemPointer bridge_iptr;
	/* compared with InvalidAttrNumber, so should be greater than 0 */
	AttrNumber	attnum;
} BridgeData;

#if PG_VERSION_NUM < 180000
#define OTupleAttrCompact		FormData_pg_attribute
#define OTupleAttrFull			FormData_pg_attribute

#define OTupleDescAttrFast(tupdesc, i) (TupleDescAttr((tupdesc), (i)))
#define OTupleDescAttrSlow(tupdesc, i) (TupleDescAttr((tupdesc), (i)))

#define o_att_align_nominal(att, cur_offset) \
	(att_align_nominal(cur_offset, (att)->attalign))

#define o_att_align_pointer(att, cur_offset, attlen, attptr) \
	(att_align_pointer(cur_offset, (att)->attalign, attlen, attptr))
#else
#define OTupleAttrCompact		CompactAttribute
#define OTupleAttrFull			FormData_pg_attribute

#define OTupleDescAttrFast(tupdesc, i) (TupleDescCompactAttr((tupdesc), (i)))
#define OTupleDescAttrSlow(tupdesc, i) (TupleDescAttr((tupdesc), (i)))

#define o_att_align_nominal(att, cur_offset) \
	(att_nominal_alignby(cur_offset, (att)->attalignby))

#define o_att_align_pointer(att, cur_offset, attlen, attptr) \
	(att_pointer_alignby(cur_offset, (att)->attalignby, attlen, attptr))
#endif

/*
 * Works with orioledb table tuples in primary index. It can fetch
 * TOAST pointers from table tuple.
 */
#define o_fastgetattr(tup, attnum, tupleDesc, spec, isnull)			\
(																	\
	AssertMacro((attnum) > 0),										\
	(*(isnull) = false),											\
	((tup).formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) ?				\
	(																\
		((attnum) - 1 < (spec)->natts) ?							\
		(															\
			OTupleDescAttrFast((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				fetchatt(OTupleDescAttrFast((tupleDesc), (attnum) - 1), \
					(char *) (tup).data +							\
					OTupleDescAttrFast((tupleDesc), (attnum) - 1)->attcacheoff) \
			)														\
			:														\
				o_toast_nocachegetattr((tup), (attnum), (tupleDesc), (spec), (isnull)) \
		)															\
		:															\
		(															\
			(*(isnull) = true),										\
			(Datum) NULL											\
		)															\
	)																\
	:																\
	(																\
		(!(((OTupleHeader) (tup).data)->hasnulls)) ?				\
		(															\
			OTupleDescAttrFast((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				fetchatt(OTupleDescAttrFast((tupleDesc), (attnum)-1), \
					(char *) (tup).data +							\
					OTupleHeaderDataOff((OTupleHeader) (tup).data) +	\
					OTupleDescAttrFast((tupleDesc), (attnum) - 1)->attcacheoff) \
			)														\
			:														\
				o_toast_nocachegetattr((tup), (attnum), (tupleDesc), (spec), (isnull)) \
		)															\
		:															\
		(															\
			att_isnull((attnum) - 1, (bits8 *) ((tup).data +		\
				OTupleHeaderDataOff((OTupleHeader) (tup).data))) ?		\
			(														\
				(*(isnull) = true),									\
				(Datum) NULL										\
			)														\
			:														\
			(														\
				o_toast_nocachegetattr((tup), (attnum), (tupleDesc), (spec), (isnull)) \
			)														\
		)															\
	)																\
)

#define o_fastgetattr_ptr(tup, attnum, tupleDesc, spec)				\
(																	\
	AssertMacro((attnum) > 0),										\
	((tup).formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) ?				\
	(																\
		((attnum) - 1 < (spec)->natts) ?							\
		(															\
			OTupleDescAttrFast((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				(char *) (tup).data +									\
				OTupleDescAttrFast((tupleDesc), (attnum) - 1)->attcacheoff \
			)														\
			:														\
				o_toast_nocachegetattr_ptr((tup), (attnum), (tupleDesc), (spec)) \
		)															\
		:															\
		(															\
			NULL													\
		)															\
	)																\
	:																\
	(																\
		(!(((OTupleHeader) (tup).data)->hasnulls)) ?				\
		(															\
			OTupleDescAttrFast((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				(char *) (tup).data +									\
				OTupleHeaderDataOff((OTupleHeader) (tup).data) +		\
				OTupleDescAttrFast((tupleDesc), (attnum) - 1)->attcacheoff \
			)														\
			:														\
				o_toast_nocachegetattr_ptr((tup), (attnum), (tupleDesc), (spec)) \
		)															\
		:															\
		(															\
			att_isnull((attnum) - 1, (bits8 *) ((tup).data +		\
				OTupleHeaderDataOff((OTupleHeader) (tup).data))) ?		\
			(														\
				NULL												\
			)														\
			:														\
			(														\
				o_toast_nocachegetattr_ptr((tup), (attnum), (tupleDesc), (spec)) \
			)														\
		)															\
	)																\
)

#define o_tuple_size(tup, spec)										\
(																	\
	((tup).formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) ?				\
	(																\
		(uint32) (spec)->len										\
	)																\
	:																\
	(																\
		OTupleHeaderGetLen((OTupleHeader) (tup).data)				\
	)																\
)

#define o_has_nulls(tup)											\
(																	\
	((tup).formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) ?				\
	(																\
		false														\
	)																\
	:																\
	(																\
		((OTupleHeader) (tup).data)->hasnulls						\
	)																\
)

extern void o_tuple_init_reader(OTupleReaderState *state, OTuple tuple,
								TupleDesc desc, OTupleFixedFormatSpec *spec);
extern Datum o_tuple_read_next_field(OTupleReaderState *state, bool *isnull);
extern uint32 o_tuple_next_field_offset(OTupleReaderState *state,
										OTupleAttrCompact * att);
extern ItemPointer o_tuple_get_last_iptr(TupleDesc desc,
										 OTupleFixedFormatSpec *spec,
										 OTuple tuple, bool *isnull);
extern Datum o_toast_nocachegetattr(OTuple tuple, int attnum,
									TupleDesc tupleDesc,
									OTupleFixedFormatSpec *spec,
									bool *is_null);
extern Pointer o_toast_nocachegetattr_ptr(OTuple tuple, int attnum,
										  TupleDesc tupleDesc,
										  OTupleFixedFormatSpec *spec);
extern Pointer o_tuple_get_data(OTuple tuple, int *size, OTupleFixedFormatSpec *spec);
extern Size o_new_tuple_size(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
							 ItemPointer iptr, BridgeData *bridge_data, uint32 version,
							 Datum *values, bool *isnull, char *to_toast);
extern void o_tuple_fill(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
						 OTuple *tuple, Size tuple_size,
						 ItemPointer iptr, BridgeData *bridge_data, uint32 version,
						 Datum *values, bool *isnull, char *to_toast);
extern OTuple o_form_tuple(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
						   uint32 version, Datum *values, bool *isnull,
						   BridgeData *bridge_data);
extern uint32 o_tuple_get_version(OTuple tuple);
extern void o_tuple_set_version(OTupleFixedFormatSpec *spec, OTuple *tuple,
								uint32 version);
extern void o_tuple_set_ctid(OTuple tuple, ItemPointer iptr);

#endif							/* __TUPLE_FORMAT_H__ */
