/*-------------------------------------------------------------------------
 *
 * format.c
 * 		Routines for accessing tuples in orioledb format.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tuple/format.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "tableam/toast.h"
#include "tuple/toast.h"
#include "tuple/format.h"

#include "access/htup_details.h"

/* Does att's datatype allow packing into the 1-byte-header varlena format? */
#define ATT_IS_PACKABLE(att) \
	((att)->attlen == -1 && (att)->attstorage != 'p')

/* Use this if it's already known varlena */
#define VARLENA_ATT_IS_PACKABLE(att) \
	((att)->attstorage != 'p')

void
o_tuple_init_reader(OTupleReaderState *state, OTuple tuple, TupleDesc desc,
					OTupleFixedFormatSpec *spec)
{
	Pointer		data = tuple.data;
	OTupleHeader header = (OTupleHeader) data;

	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
	{
		state->bp = NULL;
		state->tp = (char *) data;
		state->hasnulls = false;
		state->natts = spec->natts;
	}
	else if (header->hasnulls)
	{
		state->bp = (bits8 *) (data + SizeOfOTupleHeader);
		state->tp = (char *) (data + SizeOfOTupleHeader + MAXALIGN(BITMAPLEN(header->natts)));
		state->hasnulls = true;
		state->natts = header->natts;
	}
	else
	{
		state->bp = NULL;
		state->tp = (char *) (data + SizeOfOTupleHeader);
		state->hasnulls = false;
		state->natts = header->natts;
	}
	state->off = 0;
	state->attnum = 0;
	state->desc = desc;
	state->slow = false;
}

static uint32
o_tuple_next_field_offset(OTupleReaderState *state, Form_pg_attribute att)
{
	uint32		off;

	if (!state->slow && att->attcacheoff >= 0)
	{
		state->off = att->attcacheoff;
	}
	else if (att->attlen == -1)
	{
		if (!state->slow &&
			state->off == att_align_nominal(state->off, att->attalign))
		{
			att->attcacheoff = state->off;
		}
		else
		{
			state->off = att_align_pointer(state->off, att->attalign, -1,
										   state->tp + state->off);
			state->slow = true;
		}
	}
	else
	{
		state->off = att_align_nominal(state->off, att->attalign);
		if (!state->slow)
			att->attcacheoff = state->off;
	}

	off = state->off;

	if (!att->attbyval && att->attlen < 0 &&
		IS_TOAST_POINTER(state->tp + state->off))
	{
		state->off += sizeof(OToastValue);
	}
	else
	{
		state->off = att_addlength_pointer(state->off,
										   att->attlen,
										   state->tp + state->off);
	}

	if (att->attlen <= 0)
		state->slow = true;

	state->attnum++;

	return off;
}

Datum
o_tuple_read_next_field(OTupleReaderState *state, bool *isnull)
{
	Form_pg_attribute att;
	Datum		result;
	uint32		off;

	if (state->attnum >= state->natts)
	{
		Form_pg_attribute attr = &state->desc->attrs[state->attnum];

		if (attr->atthasmissing)
		{
			result = getmissingattr(state->desc,
									state->attnum + 1,
									isnull);
			state->attnum++;
			return result;
		}
		else
		{
			*isnull = true;
			state->attnum++;
			return (Datum) 0;
		}
	}

	att = TupleDescAttr(state->desc, state->attnum);

	if (state->hasnulls && att_isnull(state->attnum, state->bp))
	{
		*isnull = true;
		state->slow = true;
		state->attnum++;
		return (Datum) 0;
	}

	*isnull = false;
	off = o_tuple_next_field_offset(state, att);

	return fetchatt(att, state->tp + off);
}

static Pointer
o_tuple_read_next_field_ptr(OTupleReaderState *state)
{
	Form_pg_attribute att;
	uint32		off;

	if (state->attnum >= state->natts)
		return NULL;

	att = TupleDescAttr(state->desc, state->attnum);

	if (state->hasnulls && att_isnull(state->attnum, state->bp))
	{
		state->attnum++;
		return NULL;
	}

	off = o_tuple_next_field_offset(state, att);

	return state->tp + off;
}

ItemPointer
o_tuple_get_last_iptr(TupleDesc desc, OTupleFixedFormatSpec *spec,
					  OTuple tuple, bool *isnull)
{
	if (!(tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
	{
		OTupleHeader	header = (OTupleHeader) tuple.data;
		uint8		   *bp = (uint8 *) (tuple.data + SizeOfOTupleHeader);

		if ((header->hasnulls) && att_isnull(desc->natts - 1, bp))
		{
			*isnull = true;
			return (ItemPointer) NULL;
		}

		*isnull = false;
		return (ItemPointer) ((char *) header + header->len - sizeof(ItemPointerData));
	}
	else
	{
		if (spec->natts < desc->natts)
		{
			*isnull = true;
			return (ItemPointer) NULL;
		}

		*isnull = false;
		return (ItemPointer) ((char *) tuple.data + spec->len - sizeof(ItemPointerData));
	}
}

/*
 * nocachegetattr analog for tuples that can consist
 * orioledb toast values (OToastValue). But return just pointer to field
 * in the tuple.
 */
Pointer
o_toast_nocachegetattr_ptr(OTuple tuple,
						   int attnum,
						   TupleDesc tupleDesc,
						   OTupleFixedFormatSpec *spec)
{
	OTupleHeader tup = (OTupleHeader) tuple.data;
	char	   *tp;				/* ptr to data part of tuple */
	bool		slow = false;	/* do we have to walk attrs? */
	int			i;
	OTupleReaderState reader;
	Pointer		result = NULL;

	/* ----------------
	 *	 Three cases:
	 *
	 *	 1: No nulls and no variable-width attributes.
	 *	 2: Has a null or a var-width AFTER att.
	 *	 3: Has nulls or var-widths BEFORE att.
	 * ----------------
	 */

	attnum--;

	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
	{
		tp = (char *) tuple.data;
	}
	else if (tup->hasnulls)
	{
		/*
		 * there's a null somewhere in the tuple
		 *
		 * check to see if any preceding bits are null...
		 */
		int			byte = attnum >> 3;
		int			finalbit = attnum & 0x07;
		bits8	   *bp = (bits8 *) (tuple.data + SizeOfOTupleHeader);

		/* check for nulls "before" final bit of last byte */
		if ((~bp[byte]) & ((1 << finalbit) - 1))
			slow = true;
		else
		{
			/* check for nulls in any "earlier" bytes */
			int			i;

			for (i = 0; i < byte; i++)
			{
				if (bp[i] != 0xFF)
				{
					slow = true;
					break;
				}
			}
		}
		tp = (char *) (tuple.data + SizeOfOTupleHeader + MAXALIGN(BITMAPLEN(tup->natts)));
	}
	else
	{
		tp = (char *) (tuple.data + SizeOfOTupleHeader);
	}

	if (!slow)
	{
		Form_pg_attribute att;

		/*
		 * If we get here, there are no nulls up to and including the target
		 * attribute.  If we have a cached offset, we can use it.
		 */
		att = TupleDescAttr(tupleDesc, attnum);
		if (att->attcacheoff >= 0)
			return tp + att->attcacheoff;
	}

	o_tuple_init_reader(&reader, tuple, tupleDesc, spec);
	for (i = 0; i <= attnum; i++)
		result = o_tuple_read_next_field_ptr(&reader);

	Assert(result != NULL);

	return result;
}


/*
 * nocachegetattr analog for tuples that can consist
 * orioledb toast values (OToastValue).
 */
Datum
o_toast_nocachegetattr(OTuple tuple,
					   int attnum,
					   TupleDesc tupleDesc,
					   OTupleFixedFormatSpec *spec)
{
	OTupleHeader tup = (OTupleHeader) tuple.data;
	char	   *tp;				/* ptr to data part of tuple */
	bool		slow = false;	/* do we have to walk attrs? */
	int			i;
	OTupleReaderState reader;
	Datum		result = (Datum) 0;
	bool		is_null = false;

	/* ----------------
	 *	 Three cases:
	 *
	 *	 1: No nulls and no variable-width attributes.
	 *	 2: Has a null or a var-width AFTER att.
	 *	 3: Has nulls or var-widths BEFORE att.
	 * ----------------
	 */

	attnum--;

	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
	{
		tp = (char *) tuple.data;
	}
	else if (tup->hasnulls)
	{
		/*
		 * there's a null somewhere in the tuple
		 *
		 * check to see if any preceding bits are null...
		 */
		int			byte = attnum >> 3;
		int			finalbit = attnum & 0x07;
		bits8	   *bp = (bits8 *) (tuple.data + SizeOfOTupleHeader);

		/* check for nulls "before" final bit of last byte */
		if ((~bp[byte]) & ((1 << finalbit) - 1))
			slow = true;
		else
		{
			/* check for nulls in any "earlier" bytes */
			int			i;

			for (i = 0; i < byte; i++)
			{
				if (bp[i] != 0xFF)
				{
					slow = true;
					break;
				}
			}
		}
		tp = (char *) (tuple.data + SizeOfOTupleHeader + MAXALIGN(BITMAPLEN(tup->natts)));
	}
	else
	{
		tp = (char *) (tuple.data + SizeOfOTupleHeader);
	}

	if (!slow)
	{
		Form_pg_attribute att;

		/*
		 * If we get here, there are no nulls up to and including the target
		 * attribute.  If we have a cached offset, we can use it.
		 */
		att = TupleDescAttr(tupleDesc, attnum);
		if (att->attcacheoff >= 0)
			return fetchatt(att, tp + att->attcacheoff);
	}

	o_tuple_init_reader(&reader, tuple, tupleDesc, spec);
	for (i = 0; i <= attnum; i++)
		result = o_tuple_read_next_field(&reader, &is_null);

	Assert(!is_null);

	return result;
}

Pointer
o_tuple_get_data(OTuple tuple, int *size, OTupleFixedFormatSpec *spec)
{
	if (!(tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
	{
		OTupleHeader header = (OTupleHeader) tuple.data;
		int			hoff = SizeOfOTupleHeader + header->hasnulls ? MAXALIGN(BITMAPLEN(header->natts)) : 0;

		*size = header->len - hoff;
		return (Pointer) tuple.data + hoff;
	}
	else
	{
		*size = spec->len;
		return tuple.data;
	}
}

/*
 * toast_compute_data_size
 *		Determine size of the data area of a tuple to be constructed
 */
static Size
o_tuple_compute_data_size(TupleDesc tupleDesc, ItemPointer iptr,
						  Datum *values, bool *isnull, bool *to_toast,
						  int natts)
{
	Size		data_length = 0;
	int			i,
				off = iptr ? 1 : 0;

	for (i = 0; i < natts; i++)
	{
		Datum		val;
		Form_pg_attribute atti;

		if (!(i == 0 && iptr))
		{
			if (to_toast != NULL && to_toast[i - off])
			{
				data_length += sizeof(OToastValue);
				continue;
			}

			if (isnull[i - off])
				continue;
			val = values[i - off];
		}
		else
		{
			val = PointerGetDatum(iptr);
		}

		atti = TupleDescAttr(tupleDesc, i);
		if (ATT_IS_PACKABLE(atti) &&
			VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
		{
			/*
			 * we're anticipating converting to a short varlena header, so
			 * adjust length and don't count any alignment
			 */
			data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
		}
		else if (atti->attlen == -1 &&
				 VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * we want to flatten the expanded value so that the constructed
			 * tuple doesn't depend on it
			 */
			data_length = att_align_nominal(data_length, atti->attalign);
			data_length += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			data_length = att_align_datum(data_length, atti->attalign,
										  atti->attlen, val);
			data_length = att_addlength_datum(data_length, atti->attlen,
											  val);
		}
	}

	return data_length;
}

Size
o_new_tuple_size(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
				 ItemPointer iptr, uint32 version,
				 Datum *values, bool *isnull, bool *to_toast)
{
	bool		hasnull = false;
	bool		fixedFormat = (version == 0);
	int			i,
				natts,
				off = iptr ? 1 : 0;
	Size		result;

	natts = tupleDesc->natts;

	/*
	 * Check for nulls
	 */
	for (i = off; i < natts; i++)
	{
		if (isnull[i - off])
		{
			fixedFormat = false;
			hasnull = true;
		}
		else if (i >= spec->natts)
			fixedFormat = false;
	}

	/*
	 * Determine total space needed
	 */
	if (!fixedFormat)
	{
		result = SizeOfOTupleHeader;
		if (hasnull)
			result += MAXALIGN(BITMAPLEN(natts));
	}
	else
	{
		result = 0;
		natts = spec->natts;
	}

	result += o_tuple_compute_data_size(tupleDesc, iptr, values,
										isnull, to_toast, natts);

	return result;
}

/*
 * Memory is expected to be already zeroed!
 */
void
o_tuple_fill(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
			 OTuple *tuple, Size tuple_size, ItemPointer iptr, uint32 version,
			 Datum *values, bool *isnull, bool *to_toast)
{
	OTupleHeader tup = (OTupleHeader) tuple->data;
	bits8	   *bitP;
	bits8		bitmask;
	int			i;
	int			natts = tupleDesc->natts;
	int			hoff;
	int			attOff = iptr ? 1 : 0;
	Size		len;
	bool		hasnull = false;
	bool		fixedFormat = (version == 0);
	Pointer		data;

	/*
	 * Check for nulls
	 */
	for (i = attOff; i < natts; i++)
	{
		if (isnull[i - attOff])
		{
			fixedFormat = false;
			hasnull = true;
		}
		else if (i >= spec->natts)
			fixedFormat = false;
	}

	if (!fixedFormat)
	{
		tup->hasnulls = hasnull;
		tup->len = tuple_size;
		tup->natts = natts;
		tup->version = version;
		len = SizeOfOTupleHeader;
		if (hasnull)
			len += MAXALIGN(BITMAPLEN(natts));
		hoff = len;
		if (hasnull)
		{
			bitP = (bits8 *) (tuple->data + SizeOfOTupleHeader - 1);
			bitmask = HIGHBIT;
		}
		else
		{
			/* just to keep compiler quiet */
			bitP = NULL;
			bitmask = 0;
		}
		tuple->formatFlags = 0;
	}
	else
	{
		bitP = NULL;
		bitmask = 0;
		len = 0;
		hoff = 0;
		natts = spec->natts;
		hasnull = false;
		tuple->formatFlags = O_TUPLE_FLAGS_FIXED_FORMAT;
	}

	data = tuple->data + hoff;

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDesc, i);
		Size		data_length = 0;
		Datum		value;
		bool		null;
		bool		cur_to_toast;

		if (!(i == 0 && iptr))
		{
			cur_to_toast = (to_toast != NULL && to_toast[i - attOff]);
			value = values[i - attOff];
			null = isnull[i - attOff];
		}
		else
		{
			cur_to_toast = false;
			value = PointerGetDatum(iptr);
			null = false;
		}

		if (cur_to_toast)
		{
			OToastValue toastValue;

			memset(&toastValue, 0, sizeof(toastValue));
			SET_TOAST_POINTER(&toastValue);
			toastValue.raw_size = o_get_raw_size(value);
			toastValue.toasted_size = o_get_src_size(value);

#if PG_VERSION_NUM >= 140000
			{
				if (VARATT_IS_COMPRESSED(value))
				{
					if (att->attcompression == InvalidCompressionMethod)
						att->attcompression = default_toast_compression;
					switch (att->attcompression)
					{
						case TOAST_PGLZ_COMPRESSION:
							toastValue.compression = TOAST_PGLZ_COMPRESSION_ID;
							break;
						case TOAST_LZ4_COMPRESSION:
							toastValue.compression = TOAST_LZ4_COMPRESSION_ID;
							break;
						default:
							toastValue.compression = TOAST_INVALID_COMPRESSION_ID;
					}
				}
				else
					toastValue.compression = TOAST_INVALID_COMPRESSION_ID;
			}
#endif

			data_length = sizeof(OToastValue);
			memcpy(data, &toastValue, data_length);
		}

		if (hasnull)
		{
			if (bitmask != HIGHBIT)
				bitmask <<= 1;
			else
			{
				bitP += 1;
				*bitP = 0x0;
				bitmask = 1;
			}

			if (null)
				continue;

			*bitP |= bitmask;
		}

		if (cur_to_toast)
		{
			data += data_length;
			continue;
		}


		/*
		 * XXX we use the att_align macros on the pointer value itself, not on
		 * an offset.  This is a bit of a hack.
		 */
		if (att->attbyval)
		{
			/* pass-by-value */
			data = (char *) att_align_nominal(data, att->attalign);
			store_att_byval(data, value, att->attlen);
			data_length = att->attlen;
		}
		else if (att->attlen == -1)
		{
			/* varlena */
			Pointer		val = DatumGetPointer(value);

			if (VARATT_IS_EXTERNAL(val))
			{
				if (VARATT_IS_EXTERNAL_EXPANDED(val))
				{
					/*
					 * we want to flatten the expanded value so that the
					 * constructed tuple doesn't depend on it
					 */
					ExpandedObjectHeader *eoh = DatumGetEOHP(value);

					data = (char *) att_align_nominal(data,
													  att->attalign);
					data_length = EOH_get_flat_size(eoh);
					EOH_flatten_into(eoh, data, data_length);
				}
				else
				{
					/* no alignment, since it's short by definition */
					data_length = VARSIZE_EXTERNAL(val);
					memcpy(data, val, data_length);
				}
			}
			else if (VARATT_IS_SHORT(val))
			{
				/* no alignment for short varlenas */
				data_length = VARSIZE_SHORT(val);
				memcpy(data, val, data_length);
			}
			else if (VARLENA_ATT_IS_PACKABLE(att) &&
					 VARATT_CAN_MAKE_SHORT(val))
			{
				/* convert to short varlena -- no alignment */
				data_length = VARATT_CONVERTED_SHORT_SIZE(val);
				SET_VARSIZE_SHORT(data, data_length);
				memcpy(data + 1, VARDATA(val), data_length - 1);
			}
			else
			{
				/* full 4-byte header varlena */
				data = (char *) att_align_nominal(data,
												  att->attalign);
				data_length = VARSIZE(val);
				memcpy(data, val, data_length);
			}
		}
		else if (att->attlen == -2)
		{
			/* cstring ... never needs alignment */
			Assert(att->attalign == 'c');
			data_length = strlen(DatumGetCString(value)) + 1;
			memcpy(data, DatumGetPointer(value), data_length);
		}
		else
		{
			/* fixed-length pass-by-reference */
			data = (char *) att_align_nominal(data, att->attalign);
			Assert(att->attlen > 0);
			data_length = att->attlen;
			memcpy(data, DatumGetPointer(value), data_length);
		}

		data += data_length;
	}

	Assert((data - tuple->data) == tuple_size);
}

OTuple
o_form_tuple(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
			 uint32 version, Datum *values, bool *isnull)
{
	OTuple		result;
	int			len;

	len = o_new_tuple_size(tupleDesc, spec, NULL, version, values, isnull, NULL);
	result.data = (Pointer) palloc0(len);
	o_tuple_fill(tupleDesc, spec, &result, len, NULL, version, values, isnull, NULL);
	return result;
}


uint32
o_tuple_get_version(OTuple tuple)
{
	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
		return 0;
	else
		return ((OTupleHeader) tuple.data)->version;
}

void
o_tuple_set_version(OTupleFixedFormatSpec *spec, OTuple *tuple,
					uint32 version)
{
	OTupleHeader header = (OTupleHeader) tuple->data;

	if (!(tuple->formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
	{
		header->version = version;
		if (header->version == 0 && !header->hasnulls && header->natts == spec->natts)
		{
			Assert(header->len = spec->len + sizeof(OTupleHeaderData));
			tuple->formatFlags |= O_TUPLE_FLAGS_FIXED_FORMAT;
			memmove(tuple->data, tuple->data + sizeof(OTupleHeaderData), spec->len);
		}
		return;
	}

	if (version == 0)
		return;

	tuple->data = (Pointer) repalloc(tuple->data, spec->len + sizeof(OTupleHeaderData));
	memmove(tuple->data + sizeof(OTupleHeaderData),
			tuple->data,
			spec->len);
	tuple->formatFlags &= ~O_TUPLE_FLAGS_FIXED_FORMAT;

	header = (OTupleHeaderData *) tuple->data;
	header->natts = spec->natts;
	header->len = sizeof(OTupleHeaderData) + spec->len;
	header->hasnulls = 0;
	header->version = version;
}

void
o_tuple_set_ctid(OTuple tuple, ItemPointer iptr)
{
	Pointer		data = tuple.data;
	OTupleHeader header = (OTupleHeader) data;

	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
	{
		*((ItemPointer) data) = *iptr;
	}
	else if (header->hasnulls)
	{
		*((ItemPointer) (data + SizeOfOTupleHeader + MAXALIGN(BITMAPLEN(header->natts)))) = *iptr;
	}
	else
	{
		*((ItemPointer) (data + SizeOfOTupleHeader)) = *iptr;
	}
}
