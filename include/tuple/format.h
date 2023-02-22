/*-------------------------------------------------------------------------
 *
 * format.h
 * 		Declarations for orioledb tuple format.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
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

typedef struct
{
	uint16		natts;
	uint16		len;
} OTupleFixedFormatSpec;

typedef OTupleHeaderData *OTupleHeader;
#define SizeOfOTupleHeader MAXALIGN(sizeof(OTupleHeaderData))

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
			TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				fetchatt(TupleDescAttr((tupleDesc), (attnum) - 1),	\
					(char *) (tup).data +							\
					TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff) \
			)														\
			:														\
				o_toast_nocachegetattr((tup), (attnum), (tupleDesc), (spec)) \
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
			TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				fetchatt(TupleDescAttr((tupleDesc), (attnum)-1),	\
					(char *) (tup).data + SizeOfOTupleHeader +		\
					TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff) \
			)														\
			:														\
				o_toast_nocachegetattr((tup), (attnum), (tupleDesc), (spec)) \
		)															\
		:															\
		(															\
			att_isnull((attnum) - 1, (bits8 *) ((tup).data + SizeOfOTupleHeader)) ? \
			(														\
				(*(isnull) = true),									\
				(Datum) NULL										\
			)														\
			:														\
			(														\
				o_toast_nocachegetattr((tup), (attnum), (tupleDesc), (spec)) \
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
			TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				(char *) (tup).data +									\
				TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff \
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
			TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				(char *) (tup).data + SizeOfOTupleHeader +				\
				TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff \
			)														\
			:														\
				o_toast_nocachegetattr_ptr((tup), (attnum), (tupleDesc), (spec)) \
		)															\
		:															\
		(															\
			att_isnull((attnum) - 1, (bits8 *) ((tup).data + SizeOfOTupleHeader)) ? \
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
		(spec)->len													\
	)																\
	:																\
	(																\
		((OTupleHeader) (tup).data)->len							\
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
extern ItemPointer o_tuple_get_last_iptr(TupleDesc desc,
										 OTupleFixedFormatSpec *spec,
										 OTuple tuple, bool *isnull);
extern Datum o_toast_nocachegetattr(OTuple tuple, int attnum,
									TupleDesc tupleDesc,
									OTupleFixedFormatSpec *spec);
extern Pointer o_toast_nocachegetattr_ptr(OTuple tuple, int attnum,
										  TupleDesc tupleDesc,
										  OTupleFixedFormatSpec *spec);
extern Pointer o_tuple_get_data(OTuple tuple, int *size, OTupleFixedFormatSpec *spec);
extern Size o_new_tuple_size(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
							 ItemPointer iptr, uint32 version,
							 Datum *values, bool *isnull, bool *to_toast);
extern void o_tuple_fill(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
						 OTuple *tuple, Size tuple_size,
						 ItemPointer iptr, uint32 version,
						 Datum *values, bool *isnull, bool *to_toast);
extern OTuple o_form_tuple(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
						   uint32 version, Datum *values, bool *isnull);
extern uint32 o_tuple_get_version(OTuple tuple);
extern void o_tuple_set_version(OTupleFixedFormatSpec *spec, OTuple *tuple,
								uint32 version);
extern void o_tuple_set_ctid(OTuple tuple, ItemPointer iptr);

#endif							/* __TUPLE_FORMAT_H__ */
