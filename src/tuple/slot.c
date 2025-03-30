/*-------------------------------------------------------------------------
 *
 * slot.c
 * 		Routines for orioledb tuple slot implementation
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tuple/slot.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "tableam/toast.h"
#include "tuple/toast.h"
#include "tuple/slot.h"

#include "access/detoast.h"
#include "access/toast_internals.h"
#include "catalog/heap.h"
#include "catalog/pg_type_d.h"
#include "storage/itemptr.h"
#include "utils/expandeddatum.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"

#include "nodes/nodeFuncs.h"

static void tts_orioledb_init_reader(TupleTableSlot *slot);
static void tts_orioledb_get_index_values(TupleTableSlot *slot,
										  OIndexDescr *idx, Datum *values,
										  bool *isnull, bool leaf);

static void
tts_orioledb_init(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	oslot->data = NULL;
	O_TUPLE_SET_NULL(oslot->tuple);
	oslot->descr = NULL;
	oslot->rowid = NULL;
	oslot->to_toast = NULL;
	oslot->version = 0;
	oslot->hint.blkno = OInvalidInMemoryBlkno;
	oslot->hint.pageChangeCount = 0;
}

static void
tts_orioledb_release(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	if (oslot->to_toast)
		pfree(oslot->to_toast);
}

static void
tts_orioledb_clear(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		if (!O_TUPLE_IS_NULL(oslot->tuple))
			pfree(oslot->tuple.data);
		if (oslot->data)
			pfree(oslot->data);
		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	if (oslot->to_toast)
	{
		int			i,
					natts = slot->tts_tupleDescriptor->natts;

		Assert(oslot->vfree);
		for (i = 0; i < natts; i++)
		{
			if (oslot->detoasted[i])
			{
				pfree(DatumGetPointer(oslot->detoasted[i]));
				oslot->detoasted[i] = (Datum) 0;
			}
			if (oslot->vfree[i])
				pfree(DatumGetPointer(slot->tts_values[i]));
		}
		memset(oslot->vfree, 0, natts * sizeof(bool));
		memset(oslot->to_toast, ORIOLEDB_TO_TOAST_OFF, natts * sizeof(bool));
	}

	oslot->data = NULL;
	O_TUPLE_SET_NULL(oslot->tuple);
	if (oslot->rowid)
	{
		pfree(oslot->rowid);
		oslot->rowid = NULL;
	}
	oslot->descr = NULL;
	oslot->hint.blkno = OInvalidInMemoryBlkno;
	oslot->hint.pageChangeCount = 0;

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

static OTuple
tts_orioledb_make_key(TupleTableSlot *slot, OTableDescr *descr)
{
	OIndexDescr *id = GET_PRIMARY(descr);
	Datum		key[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	int			i,
				ctid_off = id->primaryIsCtid ? 1 : 0;
	OTuple		result;

	for (i = 0; i < id->nonLeafTupdesc->natts; i++)
	{
		int			attnum = id->fields[i].tableAttnum;

		if (attnum == 1 && ctid_off == 1)
		{
			key[i] = PointerGetDatum(&slot->tts_tid);
			isnull[i] = false;
		}
		else
		{
			int			attindex = attnum - 1 - ctid_off;
#ifdef USE_ASSERT_CHECKING
			/* PK attributes shouldn't be external or compressed */
			Form_pg_attribute att;

			att = TupleDescAttr(slot->tts_tupleDescriptor,
								attnum - 1 - ctid_off);
			if (!slot->tts_isnull[attindex] && att->attlen < 0)
			{
				Assert(!VARATT_IS_EXTERNAL(slot->tts_values[attindex]));
				Assert(!VARATT_IS_COMPRESSED(slot->tts_values[attindex]));
			}
#endif
			key[i] = slot->tts_values[attindex];
			isnull[i] = slot->tts_isnull[attindex];
		}
	}

	result = o_form_tuple(id->nonLeafTupdesc, &id->nonLeafSpec,
						  ((OTableSlot *) slot)->version, key, isnull,
						  NULL);
	return result;
}

static OTuple
make_key_from_secondary_slot(TupleTableSlot *slot, OIndexDescr *idx, OTableDescr *descr)
{
	Datum		key[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	int			i;
	OTuple		result;

	for (i = 0; i < idx->nPrimaryFields; i++)
	{
		int			pk_attnum = idx->primaryFieldsAttnums[i];
		int			attindex = pk_attnum - 1;

#ifdef USE_ASSERT_CHECKING
		/* PK attributes shouldn't be external or compressed */
		Form_pg_attribute att;

		att = TupleDescAttr(slot->tts_tupleDescriptor, pk_attnum - 1);
		if (!slot->tts_isnull[attindex] && att->attlen < 0)
		{
			Assert(!VARATT_IS_EXTERNAL(slot->tts_values[attindex]));
			Assert(!VARATT_IS_COMPRESSED(slot->tts_values[attindex]));
		}
#endif
		key[i] = slot->tts_values[attindex];
		isnull[i] = slot->tts_isnull[attindex];
	}

	result = o_form_tuple(GET_PRIMARY(descr)->nonLeafTupdesc, &GET_PRIMARY(descr)->nonLeafSpec,
						  ((OTableSlot *) slot)->version, key, isnull, NULL);
	return result;
}

static void
alloc_to_toast_vfree_detoasted(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	int			totalNatts = slot->tts_tupleDescriptor->natts;

	Assert(!oslot->to_toast && !oslot->vfree);
	oslot->to_toast = MemoryContextAllocZero(slot->tts_mcxt,
											 MAXALIGN(sizeof(bool) * totalNatts * 2) +
											 sizeof(Datum) * totalNatts);
	oslot->vfree = (bool *) (oslot->to_toast + totalNatts);
	oslot->detoasted = (Datum *) ((Pointer) oslot->to_toast + MAXALIGN(sizeof(char) * totalNatts + sizeof(bool) * totalNatts));
}

/*
 * This function is designed to populate the attributes of a tuple table slot
 * from an OrioleDB tuple.  It selectively retrieves attributes based on
 * the provided number of attributes (__natts) and updates the slot's values
 * and null flags accordingly.
 */
static void
tts_orioledb_getsomeattrs(TupleTableSlot *slot, int __natts)
{
	/*
	 * Cast the generic TupleTableSlot to an OTableSlot for OrioleDB specific
	 * operations.
	 */
	OTableSlot *oslot = (OTableSlot *) slot;

	/* Declaration of variables used throughout the function. */
	int			natts,
				attnum,
				ctid_off = 0;
	OTableDescr *descr = oslot->descr;	/* Descriptor for the table. */
	Datum	   *values = slot->tts_values;	/* Array to store attribute
											 * values. */
	bool	   *isnull = slot->tts_isnull;	/* Array to store null flags for
											 * attributes. */
	bool		hastoast = false;	/* Flag to indicate presence of TOASTed
									 * attributes. */
	OIndexDescr *idx;
	bool		index_order;
	int			cur_tbl_attnum = 0;

	/*
	 * Early return if the requested number of attributes is already valid or
	 * the tuple is null.
	 */
	if (__natts <= slot->tts_nvalid || O_TUPLE_IS_NULL(oslot->tuple))
		return;

	/* Ensure the descriptor is not NULL. */
	Assert(descr);
	if (oslot->ixnum == BridgeIndexNumber)
		idx = descr->bridge;
	else
		idx = descr->indices[oslot->ixnum];

	/* Determine if the attributes should be fetched in index order. */
	index_order = slot->tts_tupleDescriptor->tdtypeid == RECORDOID;
	if (oslot->ixnum == PrimaryIndexNumber)
		index_order = index_order &&
			slot->tts_tupleDescriptor->natts == idx->nFields;

	/*
	 * Ensure that if there are valid attributes, the slot is for the primary
	 * index.
	 */
	Assert(slot->tts_nvalid == 0 || oslot->ixnum == PrimaryIndexNumber);

	/*
	 * Determine the offset of the attributes due to the possible presence of
	 * ctid column.
	 */
	if (GET_PRIMARY(descr)->primaryIsCtid && oslot->ixnum == PrimaryIndexNumber)
		ctid_off++;

	/*
	 * Determine the offset of the attributes due to the possible presence of
	 * index_bridging_ctid column.
	 */
	if (GET_PRIMARY(descr)->bridging && oslot->ixnum == PrimaryIndexNumber)
		ctid_off++;

	/*
	 * Determine the number of attributes to process based on the index type
	 * and the order of attributes.
	 */
	if (oslot->ixnum == PrimaryIndexNumber && oslot->leafTuple)
	{
		if (index_order)
		{
			/*
			 * The attributes are stored in the index order.  So fetch all the
			 * attributes at once.
			 */
			natts = descr->tupdesc->natts;
		}
		else
		{
			natts = Min(__natts, descr->tupdesc->natts);
		}
	}
	else
	{
		/*
		 * For secondary indexes, the attributes are also stored in the index
		 * order.  So fetch all the attributes at once.
		 */
		natts = oslot->state.desc->natts;
	}

	/* Iterate over the attributes to populate values and null flags. */
	for (attnum = slot->tts_nvalid; attnum < natts; attnum++)
	{
		Form_pg_attribute thisatt;
		int			res_attnum = 0;

		/*
		 * Determine the result attribute number based on the index type and
		 * the order of attributes.
		 */
		if (oslot->ixnum == PrimaryIndexNumber)
		{
			if (index_order)
			{
				if (cur_tbl_attnum >= idx->nFields ||
					attnum != idx->tbl_attnums[cur_tbl_attnum].key)
					res_attnum = -2;
				else
				{
					res_attnum = idx->tbl_attnums[cur_tbl_attnum].value;
					cur_tbl_attnum++;
				}
			}
			else
				res_attnum = attnum;
		}
		else if (index_order)
		{
			if (GET_PRIMARY(descr)->primaryIsCtid && attnum == natts - 1)
				res_attnum = -1;
			else
				res_attnum = attnum;
		}
		else
		{
			Assert(false);
		}

		/* Ensure the result attribute number is valid. */
		Assert(res_attnum >= -2);
		if (res_attnum >= 0)
		{
			if (oslot->ixnum == BridgeIndexNumber && attnum == 0)
			{
				/*
				 * first bridge_ctid attribute was already read in
				 * tts_orioledb_init_reader
				 */
				values[res_attnum] = PointerGetDatum(&oslot->bridge_ctid);
				isnull[res_attnum] = false;
				continue;
			}

			/*
			 * Read the next field value and update the slot's value and null
			 * arrays.
			 */
			values[res_attnum] = o_tuple_read_next_field(&oslot->state,
														 &isnull[res_attnum]);

			/* Determine the attribute metadata based on the index and order. */
			if (oslot->ixnum == PrimaryIndexNumber && !index_order)
				thisatt = TupleDescAttr(slot->tts_tupleDescriptor, attnum);
			else
				thisatt = TupleDescAttr(idx->leafTupdesc, attnum);

			/*
			 * Check for TOASTed attributes and adjust the number of
			 * attributes if necessary.
			 */
			if (!isnull[res_attnum] && !thisatt->attbyval && thisatt->attlen < 0)
			{
				Pointer		p = DatumGetPointer(values[res_attnum]);

				Assert(p);
				if (IS_TOAST_POINTER(p) && !VARATT_IS_EXTERNAL_ORIOLEDB(p))
				{
					hastoast = true;
					natts = Max(natts, idx->maxTableAttnum - ctid_off);
				}
			}
		}
		else if (res_attnum == -1)
		{
			if (!idx->bridging)
			{
				/* Special handling for ctid attribute. */
				Datum		iptr_value PG_USED_FOR_ASSERTS_ONLY;
				bool		iptr_null;

				iptr_value = o_tuple_read_next_field(&oslot->state,
													 &iptr_null);

				Assert(iptr_null == false);
				Assert(memcmp(&slot->tts_tid,
							  (ItemPointer) iptr_value, sizeof(ItemPointerData)) == 0);
			}
		}
		else if (res_attnum == -2)
		{
			/* Handle dropped attributes by reading and ignoring the value. */
			bool		dropped_null;

			(void) o_tuple_read_next_field(&oslot->state, &dropped_null);
		}
	}

	/* Process TOASTed attributes if any were found. */
	if (hastoast)
	{
		OTuple		pkey;

		/* Allocate memory for TOASTed attributes if not already done. */
		if (!oslot->to_toast)
			alloc_to_toast_vfree_detoasted(slot);

		/* Generate a primary key for the TOASTed attributes. */
		if (oslot->ixnum == PrimaryIndexNumber)
			pkey = tts_orioledb_make_key(slot, descr);
		else
			pkey = make_key_from_secondary_slot(slot, idx, descr);

		/* Iterate over attributes to process TOASTed values. */
		for (attnum = 0; attnum < natts; attnum++)
		{
			Form_pg_attribute thisatt;

			thisatt = TupleDescAttr(slot->tts_tupleDescriptor, attnum);
			if (!isnull[attnum] && !thisatt->attbyval && thisatt->attlen < 0)
			{
				Pointer		p = DatumGetPointer(values[attnum]);

				if (IS_TOAST_POINTER(p))
				{
					/* Replace TOASTed value with a detoasted version. */
					MemoryContext mcxt = MemoryContextSwitchTo(slot->tts_mcxt);
					OToastValue toastValue;

					memcpy(&toastValue, p, sizeof(toastValue));
					values[attnum] = create_o_toast_external(descr, pkey,
															 attnum + 1 + ctid_off,
															 &toastValue,
															 oslot->csn);
					oslot->vfree[attnum] = true;
					MemoryContextSwitchTo(mcxt);
				}
			}
		}
		/* Free the primary key memory except for bump context */
		if (!is_bump_memory_context(CurrentMemoryContext))
			pfree(pkey.data);
	}

	/* Ensure the number of processed attributes matches the expected count. */
	Assert(attnum == natts);

	/* Update the slot's valid attribute count. */
	slot->tts_nvalid = natts;
}

static Datum
tts_orioledb_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	const FormData_pg_attribute *att;

	if (attnum == RowIdAttributeNumber)
	{
		Datum		values[2 * INDEX_MAX_KEYS];
		bool		isnulls[2 * INDEX_MAX_KEYS];
		int			result_size,
					tuple_size;
		bytea	   *result;
		OTableDescr *descr = oslot->descr;
		OIndexDescr *id;
		int			ctid_off;
		OTuple		tuple;
		ORowIdAddendumNonCtid addNonCtid;
		Pointer		ptr;

		if (oslot->rowid)
		{
			*isnull = false;
			return datumCopy(PointerGetDatum(oslot->rowid), false, -1);
		}

		if (!descr)
		{
			*isnull = true;
			return (Datum) 0;
		}

		id = GET_PRIMARY(descr);
		ctid_off = id->primaryIsCtid ? 1 : 0;

		if (id->primaryIsCtid)
		{
			ORowIdAddendumCtid addCtid;

			addCtid.hint = oslot->hint;
			addCtid.csn = oslot->csn;
			addCtid.version = oslot->version;

			/* Ctid primary key: give hint + tid as rowid */
			result_size = MAXALIGN(VARHDRSZ) +
				MAXALIGN(sizeof(ORowIdAddendumCtid)) +
				MAXALIGN(sizeof(ItemPointerData));
			if (id->bridging)
				result_size += MAXALIGN(sizeof(ItemPointerData));
			result = (bytea *) MemoryContextAllocZero(slot->tts_mcxt, result_size);
			SET_VARSIZE(result, result_size);
			ptr = (Pointer) result + MAXALIGN(VARHDRSZ);
			memcpy(ptr, &addCtid, sizeof(ORowIdAddendumCtid));
			ptr += MAXALIGN(sizeof(ORowIdAddendumCtid));
			memcpy(ptr, &slot->tts_tid, sizeof(ItemPointerData));
			if (id->bridging)
			{
				ptr += MAXALIGN(sizeof(ItemPointerData));
				memcpy(ptr, &oslot->bridge_ctid, sizeof(ItemPointerData));
			}
			*isnull = false;
			oslot->rowid = result;
			return datumCopy(PointerGetDatum(result), false, -1);
		}

		/*
		 * General-case primary key: prepend tuple with maxaligned hint.
		 */
		result_size = MAXALIGN(VARHDRSZ) + MAXALIGN(sizeof(ORowIdAddendumNonCtid));
		if (id->bridging)
			result_size += MAXALIGN(sizeof(ItemPointerData));
		tts_orioledb_getsomeattrs(slot, id->maxTableAttnum - ctid_off);
		tts_orioledb_get_index_values(slot, id, values, isnulls, false);
		tuple_size = o_new_tuple_size(id->nonLeafTupdesc,
									  &id->nonLeafSpec,
									  NULL, NULL, oslot->version,
									  values, isnulls, NULL);
		result_size += MAXALIGN(tuple_size);
		result = (bytea *) MemoryContextAllocZero(slot->tts_mcxt, result_size);
		SET_VARSIZE(result, result_size);
		ptr = (Pointer) result + MAXALIGN(VARHDRSZ);
		if (id->bridging)
			memcpy(ptr + MAXALIGN(sizeof(ORowIdAddendumNonCtid)), &oslot->bridge_ctid, sizeof(ItemPointerData));

		tuple.data = ptr + MAXALIGN(sizeof(ORowIdAddendumNonCtid));
		if (id->bridging)
			tuple.data += MAXALIGN(sizeof(ItemPointerData));
		o_tuple_fill(id->nonLeafTupdesc, &id->nonLeafSpec,
					 &tuple, tuple_size, NULL, NULL, oslot->version, values, isnulls, NULL);

		addNonCtid.csn = oslot->csn;
		addNonCtid.flags = tuple.formatFlags;
		addNonCtid.hint = oslot->hint;

		memcpy(ptr, &addNonCtid, sizeof(ORowIdAddendumNonCtid));

		*isnull = false;
		oslot->rowid = result;
		return datumCopy(PointerGetDatum(result), false, -1);
	}

	att = SystemAttributeDefinition(attnum);
	elog(ERROR, "orioledb tuples does not have system attribute: %s",
		 att->attname.data);

	return 0;					/* silence compiler warnings */
}

/*
 * To materialize a virtual slot all the datums that aren't passed by value
 * have to be copied into the slot's memory context.  To do so, compute the
 * required size, and allocate enough memory to store all attributes.  That's
 * good for cache hit ratio, but more importantly requires only memory
 * allocation/deallocation.
 */
static void
tts_orioledb_materialize(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	Size		sz = 0;
	char	   *data;

	/* already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	slot_getallattrs(slot);

	/* compute size of memory required */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			sz = att_align_nominal(sz, att->attalign);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_align_nominal(sz, att->attalign);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	/* all data is byval */
	if (sz == 0)
		return;

	/* allocate memory */
	oslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	/* and copy all attributes into the pre-allocated space */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			Size		data_length;

			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);

			data = (char *) att_align_nominal(data,
											  att->attalign);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size		data_length = 0;

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}

	if (oslot->to_toast)
	{
		memset(oslot->vfree, 0, desc->natts * sizeof(bool));
		memset(oslot->to_toast, 0, desc->natts * sizeof(char));
	}
}

void
tts_orioledb_detoast(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	int			natts = tupleDesc->natts;
	int			i;

	slot_getallattrs(slot);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDesc, i);
		Datum		tmp;

		if (!slot->tts_isnull[i] && att->attlen == -1 &&
			VARATT_IS_EXTENDED(slot->tts_values[i]))
		{
			MemoryContext mctx;

			if (!oslot->vfree)
				alloc_to_toast_vfree_detoasted(slot);

			mctx = MemoryContextSwitchTo(slot->tts_mcxt);
			tmp = PointerGetDatum(PG_DETOAST_DATUM(slot->tts_values[i]));
			MemoryContextSwitchTo(mctx);
			Assert(slot->tts_values[i] != tmp);
			if (oslot->vfree[i])
				pfree(DatumGetPointer(slot->tts_values[i]));
			slot->tts_values[i] = tmp;
			oslot->vfree[i] = true;
		}
	}
}

static void
tts_orioledb_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	srcdesc = srcslot->tts_tupleDescriptor;
	OTableSlot *dstoslot = (OTableSlot *) dstslot;

	Assert(srcdesc->natts <= dstslot->tts_tupleDescriptor->natts);
	tts_orioledb_clear(dstslot);

	if (srcslot->tts_ops == &TTSOpsOrioleDB)
	{
		OTableSlot *srcoslot = (OTableSlot *) srcslot;

		dstoslot->version = srcoslot->version;
		if (!O_TUPLE_IS_NULL(srcoslot->tuple))
		{
			MemoryContext mctx = MemoryContextSwitchTo(dstslot->tts_mcxt);
			OTuple		tup = srcoslot->tuple;
			uint32		tupLen = o_tuple_size(tup, &GET_PRIMARY(srcoslot->descr)->leafSpec);

			dstoslot->tuple.data = (Pointer) palloc(tupLen);
			memcpy(dstoslot->tuple.data, srcoslot->tuple.data, tupLen);
			dstoslot->tuple.formatFlags = srcoslot->tuple.formatFlags;
			dstoslot->descr = srcoslot->descr;
			if (srcoslot->rowid)
			{
				dstoslot->rowid = (bytea *) palloc(VARSIZE_ANY(srcoslot->rowid));
				memcpy(dstoslot->rowid, srcoslot->rowid,
					   VARSIZE_ANY(srcoslot->rowid));
			}
			MemoryContextSwitchTo(mctx);
			dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
			dstslot->tts_flags |= TTS_FLAG_SHOULDFREE;
			dstslot->tts_nvalid = 0;
			dstoslot->csn = srcoslot->csn;
			dstoslot->ixnum = srcoslot->ixnum;
			dstoslot->leafTuple = srcoslot->leafTuple;
			tts_orioledb_init_reader(dstslot);
			return;
		}
	}

	slot_getallattrs(srcslot);

	for (int natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_orioledb_materialize(dstslot);
}

static HeapTuple
tts_orioledb_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple	result;

	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	result = heap_form_tuple(slot->tts_tupleDescriptor,
							 slot->tts_values,
							 slot->tts_isnull);

	ItemPointerCopy(&slot->tts_tid, &result->t_self);

	return result;
}

static MinimalTuple
tts_orioledb_copy_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull);
}

static void
tts_orioledb_init_reader(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	OIndexDescr *idx;

	if (oslot->ixnum == BridgeIndexNumber)
		idx = oslot->descr->bridge;
	else
		idx = oslot->descr->indices[oslot->ixnum];

	if (oslot->leafTuple)
		o_tuple_init_reader(&oslot->state, oslot->tuple,
							idx->leafTupdesc, &idx->leafSpec);
	else
		o_tuple_init_reader(&oslot->state, oslot->tuple,
							idx->nonLeafTupdesc, &idx->nonLeafSpec);

	if (idx->primaryIsCtid)
	{
		if (oslot->ixnum == PrimaryIndexNumber && oslot->leafTuple)
		{
			Datum		value;
			bool		isnull;

			value = o_tuple_read_next_field(&oslot->state, &isnull);
			slot->tts_tid = *((ItemPointer) value);
		}
		else if (!(idx->bridging &&
				   (oslot->ixnum == BridgeIndexNumber || oslot->ixnum == PrimaryIndexNumber)))
		{
			ItemPointer iptr;
			bool		isnull;

			if (oslot->leafTuple)
				iptr = o_tuple_get_last_iptr(idx->leafTupdesc, &idx->leafSpec,
											 oslot->tuple, &isnull);
			else
				iptr = o_tuple_get_last_iptr(idx->nonLeafTupdesc,
											 &idx->nonLeafSpec,
											 oslot->tuple, &isnull);
			Assert(!isnull && iptr);
			slot->tts_tid = *iptr;
		}
	}

	if (idx->bridging && (oslot->ixnum == BridgeIndexNumber || oslot->ixnum == PrimaryIndexNumber))
	{
		Datum		value;
		bool		isnull;

		value = o_tuple_read_next_field(&oslot->state, &isnull);
		oslot->bridge_ctid = *((ItemPointer) value);
	}

	slot->tts_tableOid = oslot->descr->oids.reloid;
}

static void
tts_orioledb_store_tuple_internal(TupleTableSlot *slot, OTuple tuple,
								  OTableDescr *descr, CommitSeqNo csn,
								  int ixnum, bool leafTuple, bool shouldfree,
								  BTreeLocationHint *hint)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	Assert(COMMITSEQNO_IS_NORMAL(csn) || COMMITSEQNO_IS_INPROGRESS(csn));
	Assert(slot->tts_ops == &TTSOpsOrioleDB);

	tts_orioledb_clear(slot);

	Assert(!TTS_SHOULDFREE(slot));
	Assert(TTS_EMPTY(slot));

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;

	oslot->tuple = tuple;
	oslot->descr = descr;
	oslot->csn = csn;
	oslot->ixnum = ixnum;
	oslot->leafTuple = leafTuple;
	oslot->version = o_tuple_get_version(tuple);

	if (hint)
		oslot->hint = *hint;

	tts_orioledb_init_reader(slot);

	if (shouldfree)
		slot->tts_flags |= TTS_FLAG_SHOULDFREE;
}

void
tts_orioledb_store_tuple(TupleTableSlot *slot, OTuple tuple,
						 OTableDescr *descr, CommitSeqNo csn,
						 int ixnum, bool shouldfree, BTreeLocationHint *hint)
{
	tts_orioledb_store_tuple_internal(slot, tuple, descr, csn, ixnum, true,
									  shouldfree, hint);
}

void
tts_orioledb_store_non_leaf_tuple(TupleTableSlot *slot, OTuple tuple,
								  OTableDescr *descr, CommitSeqNo csn,
								  int ixnum, bool shouldfree,
								  BTreeLocationHint *hint)
{
	tts_orioledb_store_tuple_internal(slot, tuple, descr, csn, ixnum, false,
									  shouldfree, hint);
}

static Datum
get_tbl_att(TupleTableSlot *slot, int attnum, bool primaryIsCtid,
			bool *isnull, Oid *typid)
{
	int			i;
	Datum		value;
	Form_pg_attribute att;
	OTableSlot *oSlot = (OTableSlot *) slot;

	if (primaryIsCtid)
	{
		if (attnum == 1)
		{
			*isnull = false;
			if (typid)
				*typid = TIDOID;
			return PointerGetDatum(&slot->tts_tid);
		}
		else if (attnum == -1)
		{
			*isnull = false;
			if (typid)
				*typid = TIDOID;
			return PointerGetDatum(&oSlot->bridge_ctid);
		}
		else
		{
			i = attnum - 2;
		}
	}
	else
	{
		if (attnum == -1)
		{
			*isnull = false;
			if (typid)
				*typid = TIDOID;
			return PointerGetDatum(&oSlot->bridge_ctid);
		}
		else
			i = attnum - 1;
	}

	att = TupleDescAttr(slot->tts_tupleDescriptor, i);
	if (typid)
		*typid = att->atttypid;
	*isnull = slot->tts_isnull[i];
	value = slot->tts_values[i];

	if (!*isnull && att->attlen < 0 && VARATT_IS_EXTENDED(value))
	{
		if (!oSlot->to_toast)
			alloc_to_toast_vfree_detoasted(&oSlot->base);

		if (!oSlot->detoasted[i])
		{
			MemoryContext mcxt = MemoryContextSwitchTo(slot->tts_mcxt);

			oSlot->detoasted[i] = PointerGetDatum(PG_DETOAST_DATUM(value));
			MemoryContextSwitchTo(mcxt);

		}
		value = oSlot->detoasted[i];
	}
	return value;
}

static Datum
get_idx_expr_att(TupleTableSlot *slot, OIndexDescr *idx,
				 ExprState *exp_state, bool *isnull)
{
	Datum		result;

	idx->econtext->ecxt_scantuple = slot;

	result = ExecEvalExprSwitchContext(exp_state,
									   idx->econtext, isnull);
	return result;
}

/*
 * Prepares values for index tuple.  Works for leaf and non-leaf tuples of
 * secondary index and non-leaf tuple of primary index.
 *
 * Detoasts all the values and marks detoasted values in 'detoasted' array.
 * If 'detoasted' array isn't given, asserts not values are toasted.
 */
static void
tts_orioledb_get_index_values(TupleTableSlot *slot, OIndexDescr *idx,
							  Datum *values, bool *isnull, bool leaf)
{
	TupleDesc	tupleDesc = leaf ? idx->leafTupdesc : idx->nonLeafTupdesc;
	int			natts = tupleDesc->natts;
	int			i;
	ListCell   *indexpr_item = list_head(idx->expressions_state);

	Assert(natts <= 2 * INDEX_MAX_KEYS);

	for (i = 0; i < natts; i++)
	{
		int			attnum = idx->fields[i].tableAttnum;

		if (attnum != EXPR_ATTNUM)
			values[i] = get_tbl_att(slot, attnum, idx->primaryIsCtid,
									&isnull[i], NULL);
		else
		{
			values[i] = get_idx_expr_att(slot, idx,
										 (ExprState *) lfirst(indexpr_item),
										 &isnull[i]);
			indexpr_item = lnext(idx->expressions_state, indexpr_item);
		}
	}
}

OTuple
tts_orioledb_make_secondary_tuple(TupleTableSlot *slot, OIndexDescr *idx, bool leaf)
{
	Datum		values[2 * INDEX_MAX_KEYS];
	bool		isnull[2 * INDEX_MAX_KEYS];
	TupleDesc	tupleDesc;
	OTupleFixedFormatSpec *spec;
	int			ctid_off = idx->primaryIsCtid ? 1 : 0;
	OTableSlot *oslot = (OTableSlot *) slot;
	BrigeData	bridge_data;
	BrigeData  *bridge_data_arg = NULL;

	slot_getsomeattrs(slot, idx->maxTableAttnum - ctid_off);

	tts_orioledb_get_index_values(slot, idx, values, isnull, leaf);

	if (leaf)
	{
		tupleDesc = idx->leafTupdesc;
		spec = &idx->leafSpec;
	}
	else
	{
		tupleDesc = idx->nonLeafTupdesc;
		spec = &idx->nonLeafSpec;
	}

	if (leaf && idx->bridging && idx->desc.type == oIndexBridge)
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = false;
		bridge_data.attnum = 1;
		bridge_data_arg = &bridge_data;
	}

	return o_form_tuple(tupleDesc, spec, 0, values, isnull, bridge_data_arg);
}

/* fills key bound from tuple or index tuple that belongs to current BTree */
void
tts_orioledb_fill_key_bound(TupleTableSlot *slot, OIndexDescr *idx,
							OBTreeKeyBound *bound)
{
	int			i;
	int			ctid_off = idx->primaryIsCtid ? 1 : 0;
	ListCell   *indexpr_item = list_head(idx->expressions_state);

	slot_getsomeattrs(slot, idx->maxTableAttnum - ctid_off);

	bound->nkeys = idx->nonLeafTupdesc->natts;
	for (i = 0; i < bound->nkeys; i++)
	{
		Datum		value;
		bool		isnull;
		int			attnum;
		Oid			typid;

		attnum = idx->fields[i].tableAttnum;

		if (attnum != EXPR_ATTNUM)
			value = get_tbl_att(slot, attnum, idx->primaryIsCtid,
								&isnull, &typid);
		else
		{
			value = get_idx_expr_att(slot, idx,
									 (ExprState *) lfirst(indexpr_item),
									 &isnull);
			typid = idx->nonLeafTupdesc->attrs[i].atttypid;
			indexpr_item = lnext(idx->expressions_state, indexpr_item);
		}

		bound->keys[i].value = value;
		bound->keys[i].type = typid;
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = idx->fields[i].comparator;
	}
}

/*
 * Appends index key stored in the tuple slot to the given string.
 */
void
appendStringInfoIndexKey(StringInfo str, TupleTableSlot *slot, OIndexDescr *id)
{
	int			i;
	ListCell   *indexpr_item = list_head(id->expressions_state);

	slot_getallattrs(slot);

	appendStringInfo(str, "(");
	for (i = 0; i < id->nUniqueFields; i++)
	{
		Datum		value;
		bool		isnull;
		int			attnum = id->fields[i].tableAttnum;

		if (attnum != EXPR_ATTNUM)
			value = get_tbl_att(slot, attnum, id->primaryIsCtid,
								&isnull, NULL);
		else
		{
			value = get_idx_expr_att(slot, id,
									 (ExprState *) lfirst(indexpr_item),
									 &isnull);
			indexpr_item = lnext(id->expressions_state, indexpr_item);
		}

		if (i != 0)
			appendStringInfo(str, ", ");
		if (isnull)
			appendStringInfo(str, "null");
		else
		{
			Oid			typoutput;
			bool		typisvarlena;
			char	   *res;

			getTypeOutputInfo(id->nonLeafTupdesc->attrs[i].atttypid,
							  &typoutput, &typisvarlena);
			res = OidOutputFunctionCall(typoutput, value);
			appendStringInfo(str, "'%s'", res);
		}
	}
	appendStringInfo(str, ")");
}

/*
 * Returns a string representation of the index key that is stored in the
 * tuple slot.
 */
char *
tss_orioledb_print_idx_key(TupleTableSlot *slot, OIndexDescr *id)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoIndexKey(&buf, slot, id);

	return buf.data;
}

/*
 * Returns the expected length of the tuple that will be stored in the primary
 * key index.
 */
static inline int
expected_tuple_len(TupleTableSlot *slot, OTableDescr *descr)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	OIndexDescr *idx = GET_PRIMARY(descr);
	int			tup_size;
	BrigeData	bridge_data;
	BrigeData  *bridge_data_arg = NULL;

	if (idx->bridging)
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = true;
		bridge_data.attnum = idx->primaryIsCtid ? 2 : 1;
		bridge_data_arg = &bridge_data;
	}
	tup_size = o_new_tuple_size(idx->leafTupdesc,
								&idx->leafSpec,
								idx->primaryIsCtid ? &slot->tts_tid : NULL,
								bridge_data_arg,
								oslot->version,
								slot->tts_values,
								slot->tts_isnull,
								oslot->to_toast);

	return tup_size;
}

/*
 * Returns true if the tuple stored in the slot fits the maximum size to be
 * stored in the index.
 */
static inline bool
can_be_stored_in_index(TupleTableSlot *slot, OTableDescr *descr)
{
	int			tup_size = expected_tuple_len(slot, descr);

	Assert(tup_size > 0);

	if (tup_size <= O_BTREE_MAX_TUPLE_SIZE)
		return true;
	return false;
}

/*
 * Apply TOAST including compression and out-of-line storage to the tuple
 * stored in the slot if necessary.
 */
void
tts_orioledb_toast(TupleTableSlot *slot, OTableDescr *descr)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	Form_pg_attribute att;
	int			i,
				full_size = 0,
				to_toastn,
				natts;
	AttrNumber	toast_attn;
	bool		has_toasted = false;
	TupleDesc	tupdesc = slot->tts_tupleDescriptor;
	bool		primaryIsCtid;
	int			ctid_off;

	primaryIsCtid = GET_PRIMARY(descr)->primaryIsCtid;
	ctid_off = primaryIsCtid ? 1 : 0;
	slot_getallattrs(slot);

	/* temporary, pointers to TupleDesc attributes */
	natts = tupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		att = TupleDescAttr(tupdesc, i);
		if (att->attlen <= 0 && !slot->tts_isnull[i]
			&& VARATT_IS_EXTERNAL_ORIOLEDB(slot->tts_values[i]))
			has_toasted = true;
	}

	if (!has_toasted)
		full_size = expected_tuple_len(slot, descr);

	/* we do not need use TOAST */
	if (full_size <= O_BTREE_MAX_TUPLE_SIZE && !has_toasted)
	{
		return;
	}

	/* if we there than tuple's values should be TOASTed or compressed */
	if (!oslot->to_toast)
		alloc_to_toast_vfree_detoasted(slot);

	full_size = 0;
	for (i = 0; i < descr->ntoastable; i++)
		oslot->to_toast[descr->toastable[i] - ctid_off] = ORIOLEDB_TO_TOAST_ON;

	full_size = expected_tuple_len(slot, descr);

	memset(oslot->to_toast, ORIOLEDB_TO_TOAST_OFF, sizeof(bool) * natts);

	/* if we can not compress tuple, we do not try do it */
	if (full_size > O_BTREE_MAX_TUPLE_SIZE)
	{
		return;
	}

	/*
	 * If we there than we must calculate which values should be compressed or
	 * TOASTed.
	 */
	to_toastn = 0;
	/* to make it easy now all values must be reTOASTed */
	for (i = 0; i < descr->ntoastable; i++)
	{
		toast_attn = descr->toastable[i] - ctid_off;

		if (slot->tts_isnull[toast_attn])
			continue;

		if (VARATT_IS_EXTERNAL_ORIOLEDB(slot->tts_values[toast_attn]))
		{
			oslot->to_toast[toast_attn] = ORIOLEDB_TO_TOAST_ON;
			to_toastn++;
		}
	}

	while (to_toastn < descr->ntoastable &&
		   !can_be_stored_in_index(slot, descr))
	{
		Datum		tmp;
		int			max = 0,
					max_attn = -1,
					var_size;
		MemoryContext oldMctx;

		/* search max unprocessed value */
		for (i = 0; i < descr->ntoastable; i++)
		{
			toast_attn = descr->toastable[i] - ctid_off;
			if (!slot->tts_isnull[toast_attn] &&
				oslot->to_toast[toast_attn] == ORIOLEDB_TO_TOAST_OFF)
			{
				att = TupleDescAttr(tupdesc, toast_attn);

				Assert(att->attstorage != TYPSTORAGE_PLAIN);

				if (att->attstorage == TYPSTORAGE_MAIN &&
					VARATT_IS_COMPRESSED(slot->tts_values[toast_attn]))
					continue;

				var_size = VARSIZE_ANY(slot->tts_values[toast_attn]);
				if (var_size > max)
				{
					max = var_size;
					max_attn = toast_attn;
				}
			}
			/* else we already process it or it is NULL */
		}

		/* we have no values which can be toasted */
		if (max_attn == -1)
			break;

		att = TupleDescAttr(tupdesc, max_attn);

		/*
		 * If the value is already compressed or can not be compressed - it
		 * must be toasted
		 */
		if (VARATT_IS_COMPRESSED(slot->tts_values[max_attn])
			|| att->attstorage == TYPSTORAGE_EXTERNAL)
		{
			oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_ON;
			to_toastn++;
			continue;
		}

		oldMctx = MemoryContextSwitchTo(slot->tts_mcxt);
		tmp = toast_compress_datum(slot->tts_values[max_attn],
								   TOAST_PGLZ_COMPRESSION);
		MemoryContextSwitchTo(oldMctx);

		if (DatumGetPointer(tmp) != NULL)
		{
			/* Suceessfully compressed, replace the value */

			/* free the old value */
			if (oslot->vfree[max_attn])
				pfree(DatumGetPointer(slot->tts_values[max_attn]));
			/* store the new value and mark to free it later */
			slot->tts_values[max_attn] = tmp;
			oslot->vfree[max_attn] = true;
		}
		else if (att->attstorage != TYPSTORAGE_MAIN)
		{
			/* Compression failed, try to TOAST it */
			oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_ON;
			to_toastn++;
		}
		else
		{
			/* Compression failed, but we can not TOAST it */
			Assert(att->attstorage == TYPSTORAGE_MAIN);
			oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_COMPRESSION_TRIED;
			to_toastn++;
		}
	}
}

OTuple
tts_orioledb_form_tuple(TupleTableSlot *slot,
						OTableDescr *descr)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	OTuple		tuple;			/* return tuple */
	Size		len;
	OIndexDescr *idx = GET_PRIMARY(descr);
	TupleDesc	tupleDescriptor = idx->leafTupdesc;
	OTupleFixedFormatSpec *spec = &idx->leafSpec;
	bool		primaryIsCtid = idx->primaryIsCtid;
	ItemPointer iptr;
	BrigeData	bridge_data;
	BrigeData  *bridge_data_arg = NULL;

	if (!O_TUPLE_IS_NULL(oslot->tuple) && oslot->descr == descr &&
		oslot->ixnum == PrimaryIndexNumber && oslot->leafTuple)
		return oslot->tuple;

	if (idx->leafTupdesc->natts > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of columns (%d) exceeds limit (%d)",
						idx->leafTupdesc->natts, MaxTupleAttributeNumber)));

	if (primaryIsCtid)
		iptr = &slot->tts_tid;
	else
		iptr = NULL;

	if (idx->bridging && (idx->desc.type == oIndexPrimary || idx->desc.type == oIndexBridge))
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = idx->desc.type == oIndexPrimary;
		bridge_data.attnum = idx->desc.type == oIndexBridge ? 1 : idx->primaryIsCtid ? 2 : 1;
		bridge_data_arg = &bridge_data;
	}

	len = o_new_tuple_size(tupleDescriptor, spec, iptr, bridge_data_arg,
						   0, slot->tts_values, slot->tts_isnull,
						   oslot->to_toast);

	tuple.data = (Pointer) MemoryContextAllocZero(slot->tts_mcxt, len);

	o_tuple_fill(tupleDescriptor, spec, &tuple, len,
				 iptr, bridge_data_arg, 0,
				 slot->tts_values, slot->tts_isnull, oslot->to_toast);

	if (TTS_SHOULDFREE(slot) && !O_TUPLE_IS_NULL(oslot->tuple))
	{
		slot->tts_nvalid = 0;
		pfree(oslot->tuple.data);

		if (oslot->vfree)
		{
			int			natts = slot->tts_tupleDescriptor->natts;
			int			i;

			for (i = 0; i < natts; i++)
			{
				if (oslot->vfree[i])
					pfree(DatumGetPointer(slot->tts_values[i]));
			}
			memset(oslot->vfree, 0, natts * sizeof(bool));
		}
	}
	oslot->tuple = tuple;
	oslot->descr = descr;
	oslot->ixnum = PrimaryIndexNumber;
	oslot->leafTuple = true;
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;
	tts_orioledb_init_reader(slot);

	return tuple;
}

OTuple
tts_orioledb_form_orphan_tuple(TupleTableSlot *slot,
							   OTableDescr *descr)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	OTuple		tuple;
	Size		len;
	OIndexDescr *idx = GET_PRIMARY(descr);
	TupleDesc	tupleDescriptor = idx->leafTupdesc;
	OTupleFixedFormatSpec *spec = &idx->leafSpec;
	bool		primaryIsCtid = idx->primaryIsCtid;
	ItemPointer iptr;
	BrigeData	bridge_data;
	BrigeData  *bridge_data_arg = NULL;

	if (idx->leafTupdesc->natts > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of columns (%d) exceeds limit (%d)",
						idx->leafTupdesc->natts, MaxTupleAttributeNumber)));

	if (primaryIsCtid)
		iptr = &slot->tts_tid;
	else
		iptr = NULL;

	if (idx->bridging)
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = true;
		bridge_data.attnum = idx->primaryIsCtid ? 2 : 1;
		bridge_data_arg = &bridge_data;
	}

	len = o_new_tuple_size(tupleDescriptor, spec,
						   iptr, bridge_data_arg, oslot->version,
						   slot->tts_values, slot->tts_isnull, oslot->to_toast);

	tuple.data = (Pointer) palloc0(len);

	o_tuple_fill(tupleDescriptor, spec, &tuple, len,
				 iptr, bridge_data_arg, oslot->version,
				 slot->tts_values, slot->tts_isnull, oslot->to_toast);

	return tuple;
}

bool
tts_orioledb_insert_toast_values(TupleTableSlot *slot,
								 OTableDescr *descr,
								 OXid oxid, CommitSeqNo csn)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	OTuple		idx_tup;
	int			i;
	bool		result = true;
	int			ctid_off = GET_PRIMARY(descr)->primaryIsCtid ? 1 : 0;

	if (oslot->to_toast == NULL)
		return true;

	idx_tup = tts_orioledb_make_key(slot, descr);

	for (i = 0; i < tupleDesc->natts; i++)
	{
		if (oslot->to_toast[i])
		{
			Datum		value;
			Pointer		p;
			bool		free;

			value = o_get_src_value(slot->tts_values[i], &free);
			p = DatumGetPointer(value);

			o_btree_load_shmem(&descr->toast->desc);
			result = o_toast_insert(GET_PRIMARY(descr), descr->toast,
									idx_tup, i + 1 + ctid_off, p,
									toast_datum_size(value), oxid, csn);
			if (free)
				pfree(p);
			if (!result)
				break;
		}
	}
	pfree(idx_tup.data);
	return result;
}

void
tts_orioledb_toast_sort_add(TupleTableSlot *slot,
							OTableDescr *descr,
							Tuplesortstate *sortstate)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	OTuple		idx_tup;
	int			i;
	int			ctid_off = GET_PRIMARY(descr)->primaryIsCtid ? 1 : 0;

	if (oslot->to_toast == NULL)
		return;

	idx_tup = tts_orioledb_make_key(slot, descr);

	for (i = 0; i < tupleDesc->natts; i++)
	{
		if (oslot->to_toast[i])
		{
			Datum		value;
			Pointer		p;
			bool		free;

			value = o_get_src_value(slot->tts_values[i], &free);
			p = DatumGetPointer(value);

			o_toast_sort_add(GET_PRIMARY(descr), descr->toast,
							 idx_tup, i + 1 + ctid_off, p,
							 toast_datum_size(value), sortstate);
			if (free)
				pfree(p);
		}
	}
	pfree(idx_tup.data);
}

bool
tts_orioledb_remove_toast_values(TupleTableSlot *slot,
								 OTableDescr *descr,
								 OXid oxid, CommitSeqNo csn)
{
	int			i;
	bool		result = true;
	int			ctid_off = GET_PRIMARY(descr)->primaryIsCtid ? 1 : 0;

	slot_getallattrs(slot);

	for (i = 0; i < descr->ntoastable; i++)
	{
		int			toast_attn;
		Datum		value;

		toast_attn = descr->toastable[i] - ctid_off;

		if (slot->tts_isnull[toast_attn])
			continue;

		value = slot->tts_values[toast_attn];
		if (VARATT_IS_EXTERNAL_ORIOLEDB(value))
		{
			OToastExternal ote;
			OFixedKey	key;

			memcpy(&ote, VARDATA_EXTERNAL(DatumGetPointer(value)), O_TOAST_EXTERNAL_SZ);
			key.tuple.formatFlags = ote.formatFlags;
			key.tuple.data = key.fixedData;
			memcpy(key.fixedData,
				   VARDATA_EXTERNAL(DatumGetPointer(value)) + O_TOAST_EXTERNAL_SZ,
				   ote.data_size);
			o_btree_load_shmem(&descr->toast->desc);

			result = o_toast_delete(GET_PRIMARY(descr),
									descr->toast,
									key.tuple,
									toast_attn + 1 + ctid_off,
									oxid,
									csn);
			if (!result)
				break;
		}
	}
	return result;
}

bool
tts_orioledb_update_toast_values(TupleTableSlot *oldSlot,
								 TupleTableSlot *newSlot,
								 OTableDescr *descr,
								 OXid oxid, CommitSeqNo csn)
{
	OTableSlot *newOSlot = (OTableSlot *) newSlot;
	OTuple		idx_tup;
	OTuple		old_idx_tup PG_USED_FOR_ASSERTS_ONLY;
	int			i;
	bool		result = true;
	OIndexDescr *primary = GET_PRIMARY(descr);
	int			ctid_off = primary->primaryIsCtid ? 1 : 0;

	if (descr->bridge)
		ctid_off++;

	slot_getallattrs(oldSlot);

	idx_tup = tts_orioledb_make_key(newSlot, descr);

#ifdef USE_ASSERT_CHECKING
	{
		int			natts;

		old_idx_tup = tts_orioledb_make_key(oldSlot, descr);
		o_tuple_set_version(&primary->nonLeafSpec, &old_idx_tup,
							o_tuple_get_version(idx_tup));
		/* old_idx_tup and idx_tup are equal */
		Assert(o_tuple_size(old_idx_tup, &primary->nonLeafSpec) ==
			   o_tuple_size(idx_tup, &primary->nonLeafSpec));
		Assert(old_idx_tup.formatFlags == idx_tup.formatFlags);

		/*
		 * Cannot use simple memcmp(old_idx_tup.data, idx_tup.data, ...)
		 * because of included fields and also equality of such special values
		 * as '0.0' and '-0.0' for float
		 */
		if (old_idx_tup.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
			natts = primary->nonLeafSpec.natts;
		else
			natts = primary->nonLeafTupdesc->natts;
		for (i = 0; i < natts; i++)
		{
			if (!OIgnoreColumn(primary, i))
			{
				Datum		old_value;
				Datum		new_value;
				bool		isnull;
				OIndexField *pkfield = &primary->fields[i];
				int			cmp;

				old_value = o_fastgetattr(old_idx_tup, i + 1,
										  primary->nonLeafTupdesc,
										  &primary->nonLeafSpec, &isnull);
				Assert(!isnull);
				new_value = o_fastgetattr(idx_tup, i + 1,
										  primary->nonLeafTupdesc,
										  &primary->nonLeafSpec, &isnull);
				Assert(!isnull);

				cmp = o_call_comparator(pkfield->comparator,
										old_value, new_value);
				Assert(cmp == 0);
			}
		}
		pfree(old_idx_tup.data);
	}
#endif

	for (i = 0; i < descr->ntoastable; i++)
	{
		int			toast_attn;
		Datum		oldValue = 0,
					newValue = 0;
		bool		newToast = false,
					oldToast = false;
		bool		insertNew = false;
		bool		deleteOld = false;

		toast_attn = descr->toastable[i] - ctid_off;
		if (!oldSlot->tts_isnull[toast_attn])
		{
			oldValue = oldSlot->tts_values[toast_attn];
			if (VARATT_IS_EXTERNAL_ORIOLEDB(oldValue))
				oldToast = true;
		}

		if (newOSlot->to_toast && newOSlot->to_toast[toast_attn])
		{
			newToast = true;
			newValue = newSlot->tts_values[toast_attn];
		}

		if (!newToast && !oldToast)
			continue;

		if (newToast && !oldToast)
		{
			insertNew = true;
		}
		else if (!newToast && oldToast)
		{
			deleteOld = true;
		}
		else if (o_toast_equal(&GET_PRIMARY(descr)->desc,
							   newValue,
							   oldValue))
		{
			/* if it is the same toast value than nothing to do */
			continue;
		}
		else
		{
			/* update value if it does not equal */
			bool		equal;
			int			rawSize;

			rawSize = o_get_raw_size(newValue);
			equal = (rawSize == o_get_raw_size(oldValue));
			if (equal)
			{
				Datum		newRawValue;
				Datum		oldRawValue;
				Pointer		newPtr;
				Pointer		oldPtr;
				bool		freeNew;
				bool		freeOld;

				newRawValue = o_get_raw_value(newValue, &freeNew);
				oldRawValue = o_get_raw_value(oldValue, &freeOld);
				newPtr = DatumGetPointer(newRawValue);
				oldPtr = DatumGetPointer(oldRawValue);

				Assert(VARSIZE_ANY_EXHDR(newPtr) == VARSIZE_ANY_EXHDR(oldPtr));
				Assert(VARSIZE_ANY_EXHDR(newPtr) == rawSize);
				equal = memcmp(VARDATA_ANY(oldPtr),
							   VARDATA_ANY(newPtr),
							   rawSize) == 0;
				if (freeNew)
					pfree(newPtr);
				if (freeOld)
					pfree(oldPtr);

				if (equal)
					continue;
			}

			insertNew = true;
			deleteOld = true;
		}

		if (deleteOld)
		{
			OToastExternal ote;
			OFixedKey	key;

			memcpy(&ote, VARDATA_EXTERNAL(DatumGetPointer(oldValue)), O_TOAST_EXTERNAL_SZ);
			key.tuple.formatFlags = ote.formatFlags;
			key.tuple.data = key.fixedData;
			memcpy(key.fixedData,
				   VARDATA_EXTERNAL(DatumGetPointer(oldValue)) + O_TOAST_EXTERNAL_SZ,
				   ote.data_size);
			o_btree_load_shmem(&descr->toast->desc);
			result = o_toast_delete(GET_PRIMARY(descr),
									descr->toast,
									key.tuple,
									toast_attn + 1 + ctid_off,
									oxid,
									csn);
			if (!result)
				break;
		}

		if (insertNew)
		{
			Datum		value;
			Pointer		p;
			bool		free;

			value = o_get_src_value(newValue, &free);
			p = DatumGetPointer(value);

			o_btree_load_shmem(&descr->toast->desc);
			result = o_toast_insert(GET_PRIMARY(descr),
									descr->toast,
									idx_tup,
									toast_attn + 1 + ctid_off,
									p,
									toast_datum_size(value),
									oxid,
									csn);
			if (free)
				pfree(p);
			if (!result)
				break;
		}
	}

	pfree(idx_tup.data);
	return result;
}

bool
tts_orioledb_modified(TupleTableSlot *oldSlot,
					  TupleTableSlot *newSlot,
					  Bitmapset *attrs)
{
	TupleDesc	tupdesc = oldSlot->tts_tupleDescriptor;
	int			attnum,
				maxAttr;

	maxAttr = bms_prev_member(attrs, -1) + FirstLowInvalidHeapAttributeNumber - 1;

	if (maxAttr < 0)
		return false;

	slot_getsomeattrs(oldSlot, maxAttr + 1);
	slot_getsomeattrs(newSlot, maxAttr + 1);

	attnum = -1;
	while ((attnum = bms_next_member(attrs, attnum)) >= 0)
	{
		int			i = attnum + FirstLowInvalidHeapAttributeNumber - 1;
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Datum		val1 = oldSlot->tts_values[i],
					val2 = newSlot->tts_values[i];
		bool		isnull1 = oldSlot->tts_isnull[i],
					isnull2 = newSlot->tts_isnull[i];

		Assert(i >= 0);

		if (isnull1 || isnull2)
		{
			if (isnull1 != isnull2)
				return true;
		}

		if (!datumIsEqual(val1, val2, att->attbyval, att->attlen))
			return true;
	}
	return false;
}

void
tts_orioledb_set_ctid(TupleTableSlot *slot, ItemPointer iptr)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	slot->tts_tid = *iptr;
	if (!O_TUPLE_IS_NULL(oslot->tuple) &&
		oslot->ixnum == PrimaryIndexNumber &&
		oslot->leafTuple)
		o_tuple_set_ctid(oslot->tuple, iptr);
}

const TupleTableSlotOps TTSOpsOrioleDB = {
	.base_slot_size = sizeof(OTableSlot),
	.init = tts_orioledb_init,
	.release = tts_orioledb_release,
	.clear = tts_orioledb_clear,
	.getsomeattrs = tts_orioledb_getsomeattrs,
	.getsysattr = tts_orioledb_getsysattr,
	.materialize = tts_orioledb_materialize,
	.copyslot = tts_orioledb_copyslot,

	/*
	 * A virtual tuple table slot can not "own" a heap tuple or a minimal
	 * tuple.
	 */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_orioledb_copy_heap_tuple,
	.copy_minimal_tuple = tts_orioledb_copy_minimal_tuple
};
