/*-------------------------------------------------------------------------
 *
 * wal_partial.c
 *		WAL update records carrying the changed fields only.
 *
 * Below wal_level = logical nothing but replay reads an update record, and
 * replay has the row at hand.  WAL_REC_UPDATE_PARTIAL then carries the
 * primary key of the row and the fields the update changed, instead of the
 * whole new tuple.
 *
 * The fields carry their new values, not a difference from the old ones:
 * whatever version of the row replay finds, the result has these fields
 * set.  That matters because a checkpoint image may hold a newer version of
 * the row than the one the record was made against.  Every later record
 * then sets the fields it changed in turn, so replay still ends with the row
 * the primary had.  For the same reason replay may skip a record when the
 * row is gone or holds something the primary's version could not: the image
 * is newer than the record, and the record's changes are already there.
 *
 * Payload layout.  Numbers are varints, seven bits per byte, low bits first:
 *
 *	varint		version of the table descriptor the new tuple was built with
 *	varint		row version in the new tuple's header, see o_tuple_get_version()
 *	varint		key length
 *	uint8		format flags of the key
 *	key bytes
 *	varint		number of fields
 *	for each field:
 *		varint	attribute number, starting from 0, shifted left by one, with
 *				the low bit set for NULL
 *		value bytes as stored in the new tuple, absent for NULL; the
 *				attribute or the varlena header tells their length
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/wal_partial.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/iterator.h"
#include "recovery/wal_partial.h"
#include "tableam/descr.h"
#include "tableam/toast.h"
#include "transam/oxid.h"
#include "tuple/format.h"

#include "access/tupmacs.h"

static void
append_varint(StringInfo buf, uint32 value)
{
	while (value >= 0x80)
	{
		appendStringInfoChar(buf, (char) ((value & 0x7F) | 0x80));
		value >>= 7;
	}
	appendStringInfoChar(buf, (char) value);
}

static uint32
read_varint(Pointer *ptr)
{
	uint32		value = 0;
	int			shift = 0;
	uint8		byte;

	do
	{
		byte = *((uint8 *) *ptr);
		(*ptr)++;
		value |= (uint32) (byte & 0x7F) << shift;
		shift += 7;
	} while (byte & 0x80);

	return value;
}

/*
 * Length of a field value, which may be unaligned in the payload.
 */
static uint32
field_length(Form_pg_attribute att, Pointer ptr)
{
	if (att->attlen > 0)
		return att->attlen;
	if (att->attlen == -1)
	{
		uint32		header;

		if (VARATT_IS_1B(ptr))
			return VARSIZE_1B(ptr);
		memcpy(&header, ptr, sizeof(header));
		return VARSIZE_4B(&header);
	}
	Assert(att->attlen == -2);
	return strlen(ptr) + 1;
}

/*
 * Locate the bytes of a field value.  A TOAST pointer has no length a
 * varlena header would tell, and o_form_tuple() could not copy it either, so
 * such a value makes the caller give up.
 */
static bool
field_bytes(Form_pg_attribute att, Datum value, char *byvalBuf,
			Pointer *ptr, uint32 *len)
{
	if (att->attbyval)
	{
		store_att_byval(byvalBuf, value, att->attlen);
		*ptr = byvalBuf;
	}
	else
	{
		*ptr = DatumGetPointer(value);
		if (att->attlen == -1 && IS_TOAST_POINTER(*ptr))
			return false;
	}
	*len = field_length(att, *ptr);
	return true;
}

/*
 * Build the payload of WAL_REC_UPDATE_PARTIAL for an update of the row from
 * oldTuple to newTuple, both primary index leaf tuples, the latter built with
 * version of the table descriptor.  Returns false when the update cannot be
 * described this way.
 */
bool
o_wal_partial_update_payload(OIndexDescr *primary, uint32 version,
							 OTuple oldTuple, OTuple newTuple, StringInfo buf)
{
	TupleDesc	tupdesc = primary->leafTupdesc;
	OTupleReaderState oldReader,
				newReader;
	StringInfoData fields;
	OTuple		key;
	bool		keyAllocated;
	uint32		keyLength;
	uint32		nfields = 0;
	int			i;

	Assert(primary->desc.type == oIndexPrimary);
	Assert(!primary->bridging && !primary->primaryIsCtid);

	initStringInfo(&fields);
	o_tuple_init_reader(&oldReader, oldTuple, tupdesc, &primary->leafSpec);
	o_tuple_init_reader(&newReader, newTuple, tupdesc, &primary->leafSpec);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Datum		oldValue,
					newValue;
		bool		oldNull,
					newNull;
		char		oldByval[sizeof(Datum)],
					newByval[sizeof(Datum)];
		Pointer		oldPtr = NULL,
					newPtr = NULL;
		uint32		oldLen = 0,
					newLen = 0;

		oldValue = o_tuple_read_next_field(&oldReader, &oldNull);
		newValue = o_tuple_read_next_field(&newReader, &newNull);

		if ((!oldNull &&
			 !field_bytes(att, oldValue, oldByval, &oldPtr, &oldLen)) ||
			(!newNull &&
			 !field_bytes(att, newValue, newByval, &newPtr, &newLen)))
		{
			pfree(fields.data);
			return false;
		}

		if (oldNull == newNull &&
			(newNull || (oldLen == newLen &&
						 memcmp(oldPtr, newPtr, newLen) == 0)))
			continue;

		append_varint(&fields, ((uint32) i << 1) | (newNull ? 1 : 0));
		if (!newNull)
			appendBinaryStringInfo(&fields, newPtr, newLen);
		nfields++;
	}

	append_varint(buf, version);
	append_varint(buf, o_tuple_get_version(newTuple));

	key = o_btree_tuple_make_key(&primary->desc, newTuple, NULL, false,
								 &keyAllocated);
	keyLength = o_btree_len(&primary->desc, key, OKeyLength);
	append_varint(buf, keyLength);
	appendStringInfoChar(buf, (char) key.formatFlags);
	appendBinaryStringInfo(buf, key.data, keyLength);
	if (keyAllocated)
		pfree(key.data);

	append_varint(buf, nfields);
	appendBinaryStringInfo(buf, fields.data, fields.len);
	pfree(fields.data);
	return true;
}

/*
 * The version of the table descriptor replay has to build the row with.
 */
uint32
o_wal_partial_update_get_version(Pointer payload)
{
	return read_varint(&payload);
}

/*
 * Copy the primary key out of the payload; *ptr is moved past it.  The copy
 * is aligned, as comparison and hashing functions expect.
 */
static OTuple
read_key(Pointer *ptr, uint32 *rowVersion)
{
	OTuple		key;
	uint32		keyLength;

	(void) read_varint(ptr);
	*rowVersion = read_varint(ptr);
	keyLength = read_varint(ptr);
	key.formatFlags = *((uint8 *) *ptr);
	(*ptr)++;
	key.data = palloc(keyLength);
	memcpy(key.data, *ptr, keyLength);
	*ptr += keyLength;
	return key;
}

OTuple
o_wal_partial_update_get_key(Pointer payload)
{
	uint32		rowVersion;

	return read_key(&payload, &rowVersion);
}

/*
 * Build the new version of the row from the version replay finds in the tree
 * and the fields of the payload.  Returns false, and builds nothing, when
 * there is no row or it is a newer version than the record's (see the file
 * header comment).
 */
bool
o_wal_partial_update_build(OIndexDescr *primary, Pointer payload,
						   OTuple *newTuple)
{
	TupleDesc	tupdesc = primary->leafTupdesc;
	OTuple		key,
				oldTuple;
	OTupleReaderState reader;
	Datum	   *values;
	bool	   *isnull;
	bool	   *changed;
	Pointer		ptr = payload;
	uint32		rowVersion;
	uint32		nfields;
	List	   *buffers = NIL;
	bool		result = true;
	int			i;

	Assert(primary->desc.type == oIndexPrimary);

	key = read_key(&ptr, &rowVersion);
	oldTuple = o_btree_find_tuple_by_key(&primary->desc, &key,
										 BTreeKeyNonLeafKey,
										 &o_in_progress_snapshot, NULL,
										 CurrentMemoryContext, NULL);
	pfree(key.data);
	if (O_TUPLE_IS_NULL(oldTuple))
		return false;

	values = palloc(sizeof(Datum) * tupdesc->natts);
	isnull = palloc(sizeof(bool) * tupdesc->natts);
	changed = palloc0(sizeof(bool) * tupdesc->natts);

	nfields = read_varint(&ptr);
	for (i = 0; i < nfields; i++)
	{
		uint32		field = read_varint(&ptr);
		uint32		attnum = field >> 1;
		Form_pg_attribute att;
		uint32		len;
		Pointer		value;

		if (attnum >= tupdesc->natts)
			elog(ERROR, "invalid attribute number %u in a partial update WAL record of a %d-column tree",
				 attnum, tupdesc->natts);

		changed[attnum] = true;
		isnull[attnum] = (field & 1) != 0;
		if (isnull[attnum])
		{
			values[attnum] = (Datum) 0;
			continue;
		}

		att = TupleDescAttr(tupdesc, attnum);
		len = field_length(att, ptr);
		value = palloc(len);
		memcpy(value, ptr, len);
		ptr += len;
		buffers = lappend(buffers, value);

		if (att->attbyval)
			values[attnum] = fetch_att(value, true, att->attlen);
		else
			values[attnum] = PointerGetDatum(value);
	}

	o_tuple_init_reader(&reader, oldTuple, tupdesc, &primary->leafSpec);
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Datum		value;
		bool		valueNull;

		value = o_tuple_read_next_field(&reader, &valueNull);
		if (changed[i])
			continue;

		/*
		 * The primary only writes this record when neither version of the row
		 * has a TOAST pointer, so a version that has one is newer.
		 */
		if (!valueNull && att->attlen == -1 &&
			IS_TOAST_POINTER(DatumGetPointer(value)))
		{
			result = false;
			break;
		}
		values[i] = value;
		isnull[i] = valueNull;
	}

	/*
	 * The row version decides how replay treats a version the same
	 * transaction wrote before, so the rebuilt row must carry the primary's.
	 */
	if (result)
		*newTuple = o_form_tuple(tupdesc, &primary->leafSpec, rowVersion,
								 values, isnull, NULL);

	list_free_deep(buffers);
	pfree(values);
	pfree(isnull);
	pfree(changed);
	pfree(oldTuple.data);
	return result;
}
