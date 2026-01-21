/*-------------------------------------------------------------------------
 *
 * logical.c
 *		Support for logical decoding of OrioleDB tables.
 *
 * Copyright (c) 2024-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/logical.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/page_contents.h"
#include "catalog/sys_trees.h"
#include "recovery/logical.h"
#include "recovery/recovery.h"
#include "recovery/internal.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tuple/slot.h"

#include "catalog/pg_tablespace.h"
#include "replication/origin.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "access/toast_compression.h"
#include "access/detoast.h"
#include "tuple/toast.h"

/*
 * Copy identity attributes from srcSlot to dstSlot.
 */
static void
tts_copy_identity(TupleTableSlot *srcSlot, TupleTableSlot *dstSlot,
				  OIndexDescr *idx)
{
	int			i;
	int			nattrs = dstSlot->tts_tupleDescriptor->natts;

	slot_getallattrs(srcSlot);

	dstSlot->tts_nvalid = nattrs;
	for (i = 0; i < nattrs; i++)
	{
		dstSlot->tts_isnull[i] = true;
		dstSlot->tts_values[i] = (Datum) 0;
	}

	for (i = 0; i < idx->nonLeafTupdesc->natts; i++)
	{
		int			attnum;

		attnum = idx->tableAttnums[i] - 1;
		if (attnum >= 0)
		{
			dstSlot->tts_values[i] = srcSlot->tts_values[i];
			dstSlot->tts_isnull[i] = srcSlot->tts_isnull[i];
		}
		else if (attnum == -1)
		{
			dstSlot->tts_tid = srcSlot->tts_tid;
		}
	}

	dstSlot->tts_flags &= ~TTS_FLAG_EMPTY;
}

/*
 * Saves the heap tuple to the reorder buffer.
 */
static REORDER_BUFFER_TUPLE_TYPE
record_buffer_tuple(ReorderBuffer *reorder, HeapTuple htup, bool freeHtup)
{
	HeapTuple	changeTup;
	REORDER_BUFFER_TUPLE_TYPE result;

	result = ReorderBufferGetTupleBuf(reorder, htup->t_len);

#if PG_VERSION_NUM >= 170000
	changeTup = result;
#else
	changeTup = &result->tuple;
#endif
	changeTup->t_tableOid = InvalidOid;
	changeTup->t_len = htup->t_len;
	changeTup->t_self = htup->t_self;
	memcpy(changeTup->t_data, htup->t_data, htup->t_len);
	if (freeHtup)
		heap_freetuple(htup);

	return result;
}

/*
 * Saves the tuple from the slot to the reorder buffer.
 */
static REORDER_BUFFER_TUPLE_TYPE
record_buffer_tuple_slot(ReorderBuffer *reorder, TupleTableSlot *slot)
{
	HeapTuple	htup;
	bool		freeHtup;

	htup = ExecFetchSlotHeapTuple(slot, false, &freeHtup);

	return record_buffer_tuple(reorder, htup, freeHtup);
}

/* entry for a hash table we use to map from xid to our transaction state */
typedef struct ReorderBufferTXNByIdEnt
{
	TransactionId xid;
	ReorderBufferTXN *txn;
} ReorderBufferTXNByIdEnt;

static ReorderBufferTXN *
get_reorder_buffer_txn(ReorderBuffer *rb, TransactionId xid)
{
	ReorderBufferTXNByIdEnt *ent;
	bool		found;

	if (TransactionIdIsValid(rb->by_txn_last_xid) &&
		rb->by_txn_last_xid == xid)
		return rb->by_txn_last_txn;

	/* search the lookup table */
	ent = (ReorderBufferTXNByIdEnt *)
		hash_search(rb->by_txn, &xid, HASH_FIND, &found);
	if (found)
		return ent->txn;
	else
		return NULL;
}

/*
 * Convert tuples from the main relation from OrioleDB to heap format with
 * conversion of TOAST pointers to PG format.
 */
static HeapTuple
o_convert_toast_pointers(OTableDescr *descr, OIndexDescr *indexDescr,
						 OTuple tuple)
{
	int			natts = descr->tupdesc->natts;
	Datum	   *old_values = palloc0(natts * sizeof(Datum));
	Datum	   *new_values = palloc0(natts * sizeof(Datum));
	bool	   *isnull = palloc0(natts * sizeof(bool));
	int			ctid_off = 0;

	if (indexDescr->primaryIsCtid)
		ctid_off++;
	if (indexDescr->bridging)
		ctid_off++;

	/*
	 * Decode original tuple. Drop ctid attribute if exists.
	 */
	Assert(descr->toast);
	for (int i = 0; i < natts; i++)
	{
		old_values[i] = o_fastgetattr(tuple, i + ctid_off + 1,
									  indexDescr->leafTupdesc,
									  &indexDescr->leafSpec,
									  &isnull[i]);
		new_values[i] = old_values[i];
	}

	/* Convert TOAST pointers */
	for (int i = 0; i < descr->ntoastable; i++)
	{
		int			toast_attn = descr->toastable[i] - ctid_off;
		struct varlena *old_toastptr;
		struct varlena *new_toastptr;
		OToastValue otv;
		varatt_external ve;

		if (isnull[toast_attn])
		{
			elog(DEBUG4, "NULL attr %u", toast_attn);
			continue;
		}

		old_toastptr = (struct varlena *) DatumGetPointer(old_values[toast_attn]);
		if (old_toastptr == NULL || !VARATT_IS_EXTERNAL(old_toastptr))
		{
			elog(DEBUG4, "NON-toast or empty attr %u", toast_attn);
			continue;
		}

		memcpy(&otv, old_toastptr, sizeof(otv));
		elog(DEBUG4, "reloid: Old toast value: %u toast_attn: %u compression %u, raw_size, %u, toasted_size %u",
			 descr->oids.reloid, toast_attn + 1, otv.compression,
			 otv.raw_size, otv.toasted_size);

		ve.va_rawsize = otv.raw_size + VARHDRSZ;
		ve.va_extinfo = (otv.toasted_size - VARHDRSZ) | (otv.compression << VARLENA_EXTSIZE_BITS);
		ve.va_toastrelid = descr->toast->oids.reloid;
		ve.va_valueid = ObjectIdGetDatum(toast_attn + 1 + 8000);

		elog(DEBUG4, "New toast pointer compression: %u rawsize: %u, extinfo_size: %u, toastrelid %u, valueid %u  ",
			 (ve.va_extinfo >> VARLENA_EXTSIZE_BITS), ve.va_rawsize,
			 (ve.va_extinfo & VARLENA_EXTSIZE_MASK),
			 ve.va_toastrelid, ve.va_valueid);

		new_toastptr = palloc0(TOAST_POINTER_SIZE);
		SET_VARTAG_EXTERNAL(new_toastptr, VARTAG_ONDISK);
		memcpy(VARDATA_EXTERNAL(new_toastptr), &ve, sizeof(ve));
		new_values[toast_attn] = PointerGetDatum(new_toastptr);
	}
	return heap_form_tuple(descr->tupdesc, new_values, isnull);
}

static bool
set_snapshot(LogicalDecodingContext *ctx,
			 TransactionId logicalXid, XLogRecPtr changeXLogPtr)
{
	/*
	 * If the reorderbuffer doesn't yet have a snapshot, add one now, it will
	 * be needed to decode the change we're currently processing.
	 */
	if (!ReorderBufferXidHasBaseSnapshot(ctx->reorder, logicalXid))
	{
		Snapshot	snap;

		if (SnapBuildCurrentState(ctx->snapshot_builder) < SNAPBUILD_CONSISTENT)
			return false;

		snap = SnapBuildGetOrBuildSnapshot(ctx->snapshot_builder);
		ReorderBufferSetBaseSnapshot(ctx->reorder, logicalXid,
									 changeXLogPtr,
									 snap);
		snap->active_count++;
	}

	return true;
}

/*
 * Make reorder buffer tuple from OrioleDB tuple. Convert toast pointers is needed.
 *
 * If caller requests force_store_to_slot it warrantied that tuple is also stored into provided
 * heap slot. Also heaptuple filled by this function is left valid as the slot can depend on
 * tuple contents. heaptuple should be freed by the caller but only after the slot is not needed
 * anymore.
 *
 * If store to slot is not requested, HeapTuple is freed inside o_convert_non_toast_tuple() and this
 * function only returns reodrer buffer tuple without any side effects.
 */
static REORDER_BUFFER_TUPLE_TYPE
o_convert_non_toast_tuple(ReorderBuffer *reorderbuf, OTableDescr *descr, OIndexDescr *indexDescr, OTuple tuple, bool force_store_to_slot, TupleTableSlot *store_slot, HeapTuple *heaptuple, bool leafTuple)
{
	REORDER_BUFFER_TUPLE_TYPE result;

	if (leafTuple)
	{
		if (descr->ntoastable > 0)
		{
			/*
			 * Primary table can have TOAST pointers. Convert them from Oriole
			 * to heap format
			 */
			elog(DEBUG4, "Convert LEAF NON-TOAST toastable");

			*heaptuple = o_convert_toast_pointers(descr, indexDescr, tuple);
			if (force_store_to_slot)
			{
				/*
				 * Caller should free heaptuple after slot is not needed
				 * anymore
				 */
				ExecForceStoreHeapTuple(*heaptuple, store_slot, false);
				result = record_buffer_tuple(reorderbuf, *heaptuple, false);
			}
			else
				result = record_buffer_tuple(reorderbuf, *heaptuple, true);

			Assert(result);
		}
		else
		{
			/* Primary table without TOASTable attrs */
			elog(DEBUG4, "Convert LEAF NON-TOAST plain");

			tts_orioledb_store_tuple(store_slot, tuple,
									 descr, COMMITSEQNO_INPROGRESS,
									 PrimaryIndexNumber, false,
									 NULL);
			result = record_buffer_tuple_slot(reorderbuf, store_slot);
		}
	}
	else						/* Non-leaf tuple in a primary table can't
								 * have TOAST pointers */
	{
		elog(DEBUG4, "Convert NON-LEAF");
		tts_orioledb_store_non_leaf_tuple(store_slot, tuple,
										  descr, COMMITSEQNO_INPROGRESS,
										  PrimaryIndexNumber, false,
										  NULL);
		result = record_buffer_tuple_slot(reorderbuf, store_slot);
	}

	return result;
}

/* Convert chunk in a TOAST relation from OrioleDB format to reorder buffer (heap) format */
static REORDER_BUFFER_TUPLE_TYPE
o_convert_toast_chunk(ReorderBuffer *reorderbuf, OIndexDescr *indexDescr, OTuple tuple, TupleDescData *o_toast_tupDesc, TupleDescData *heap_toast_tupDesc, OffsetNumber debug_length)
{
	uint16		attnum;
	uint32		chunk_seq;
	uint32		old_chunk_size, /* without header */
				new_chunk_size; /* without header */
	Pointer		old_chunk;
	Pointer		new_chunk = NULL;
	HeapTuple	toasttup;
	Datum		t_values[3];
	bool		t_isnull[3];
	bool		attnum_isnull,
				chunk_seq_isnull;
	int			pk_natts;
	bool		need_to_free = false;
	REORDER_BUFFER_TUPLE_TYPE result;

	Assert(o_toast_tupDesc);
	pk_natts = o_toast_tupDesc->natts - TOAST_LEAF_FIELDS_NUM;
	attnum = (uint16) o_fastgetattr(tuple, pk_natts + ATTN_POS, o_toast_tupDesc, &indexDescr->leafSpec, &attnum_isnull);
	chunk_seq = (uint32) o_fastgetattr(tuple, pk_natts + CHUNKN_POS, o_toast_tupDesc, &indexDescr->leafSpec, &chunk_seq_isnull);
	Assert((!attnum_isnull) && (!chunk_seq_isnull));
	Assert(attnum > 0);

	/* Toast chunk in VARATT_4B uncompressed format */
	old_chunk = o_fastgetattr_ptr(tuple, pk_natts + DATA_POS, o_toast_tupDesc, &indexDescr->leafSpec);
	old_chunk_size = VARSIZE(old_chunk) - VARHDRSZ;

	/*
	 * ReorderBufferToastReplace() expects first toasted chunk without the
	 * header of the whole toasted value and reconstructs it itself. Cut this
	 * extra header from OrioleDB toasted chunk.
	 */
	if (chunk_seq == 0)
	{
		int			extra_header_size;

		Assert(VARATT_IS_4B(VARDATA(old_chunk)));
		extra_header_size = VARHDRSZ;
		new_chunk_size = old_chunk_size - extra_header_size;
		need_to_free = true;
		new_chunk = palloc0(new_chunk_size + VARHDRSZ);
		memcpy(new_chunk, old_chunk, VARHDRSZ);
		SET_VARSIZE(new_chunk, new_chunk_size + VARHDRSZ);
		memcpy(VARDATA(new_chunk), VARDATA(old_chunk) + extra_header_size, new_chunk_size);
	}
	else
	{
		new_chunk = old_chunk;
		new_chunk_size = old_chunk_size;
	}

	t_values[0] = ObjectIdGetDatum(attnum + 8000);
	t_values[1] = Int32GetDatum(chunk_seq);
	t_values[2] = PointerGetDatum(new_chunk);
	t_isnull[0] = false;
	t_isnull[1] = false;
	t_isnull[2] = false;
	elog(DEBUG4, "(attnum, seq, oldsize, newsize): (%u, %u, %u, %u) length_get: %u pk_natts: %u",
		 attnum,
		 chunk_seq,
		 old_chunk_size, new_chunk_size,
		 debug_length, pk_natts);

	Assert(heap_toast_tupDesc);
	toasttup = heap_form_tuple(heap_toast_tupDesc, t_values, t_isnull);

	result = record_buffer_tuple(reorderbuf, toasttup, true);

	if (need_to_free && new_chunk)
	{
		pfree(new_chunk);
		need_to_free = false;
	}

	return result;
}


static void
o_decode_modify_tuples(ReorderBuffer *reorderbuf, uint8 rec_type, OIndexType ix_type, ReorderBufferChange *change, OTableDescr *descr, OIndexDescr *indexDescr,
					   TupleDescData *o_toast_tupDesc, TupleDescData *heap_toast_tupDesc, OTuple tuple1, OTuple tuple2, OffsetNumber debug_length, char relreplident)
{
	if (relreplident != REPLICA_IDENTITY_DEFAULT)
		elog(DEBUG4, "REPLICA IDENTITY %c", relreplident);

	Assert(!O_TUPLE_IS_NULL(tuple1));
	if (rec_type == WAL_REC_INSERT)
	{
		change->action = REORDER_BUFFER_CHANGE_INSERT;
		Assert(O_TUPLE_IS_NULL(tuple2));

		/* Decode TOAST chunks */
		if (ix_type == oIndexToast)
		{
			elog(DEBUG4, "WAL_REC_INSERT TOAST");
			change->data.tp.newtuple = o_convert_toast_chunk(reorderbuf, indexDescr, tuple1, o_toast_tupDesc, heap_toast_tupDesc, debug_length);
			change->data.tp.clear_toast_afterwards = false;
		}
		else
		{
			HeapTuple	newheaptuple = NULL;

			Assert(ix_type != oIndexToast);

			elog(DEBUG4, "WAL_REC_INSERT NON-TOAST");
			/* Store full new tuple into reorderbuffer */
			change->data.tp.newtuple = o_convert_non_toast_tuple(reorderbuf, descr, indexDescr, tuple1, false, descr->newTuple, &newheaptuple, true);
			change->data.tp.clear_toast_afterwards = true;
		}
	}
	else if (rec_type == WAL_REC_UPDATE)
	{
		HeapTuple	newheaptuple = NULL;

		Assert(ix_type != oIndexToast);

		elog(DEBUG4, "WAL_REC_UPDATE NEWTUPLE");

		change->action = REORDER_BUFFER_CHANGE_UPDATE;
		change->data.tp.clear_toast_afterwards = true;
		change->data.tp.newtuple = o_convert_non_toast_tuple(reorderbuf, descr, indexDescr, tuple1, (relreplident != REPLICA_IDENTITY_FULL), descr->newTuple, &newheaptuple, true);

		if (relreplident == REPLICA_IDENTITY_FULL)
		{
			HeapTuple	oldheaptuple = NULL;

			elog(DEBUG4, "WAL_REC_UPDATE OLDTUPLE REPLIDENT FULL");

			/* Store full old tuple into reorderbuffer */
			Assert(!O_TUPLE_IS_NULL(tuple2));
			change->data.tp.oldtuple = o_convert_non_toast_tuple(reorderbuf, descr, indexDescr, tuple2, false, descr->oldTuple, &oldheaptuple, true);
		}
		else
		{
			elog(DEBUG4, "WAL_REC_UPDATE OLDTUPLE REPLIDENT DEFAULT");

			/*
			 * Reconstruct old tuple key from new tuple and store into
			 * reorderbuffer
			 */
			tts_copy_identity(descr->newTuple, descr->oldTuple, GET_PRIMARY(descr));
			change->data.tp.oldtuple = record_buffer_tuple_slot(reorderbuf, descr->oldTuple);

			if (newheaptuple)
				heap_freetuple(newheaptuple);
		}
	}
	else if (rec_type == WAL_REC_DELETE)
	{
		change->action = REORDER_BUFFER_CHANGE_DELETE;
		Assert(O_TUPLE_IS_NULL(tuple2));

		if (ix_type == oIndexToast)
		{
			elog(DEBUG4, "WAL_REC_DELETE TOAST");

			change->data.tp.clear_toast_afterwards = false;
			tts_orioledb_store_non_leaf_tuple(descr->oldTuple, tuple1,
											  descr, COMMITSEQNO_INPROGRESS,
											  PrimaryIndexNumber, false,
											  NULL);
			change->data.tp.oldtuple = record_buffer_tuple_slot(reorderbuf, descr->oldTuple);
		}
		else
		{
			HeapTuple	oldheaptuple = NULL;

			elog(DEBUG4, "WAL_REC_DELETE NON-TOAST");

			change->data.tp.clear_toast_afterwards = true;
			change->data.tp.oldtuple = o_convert_non_toast_tuple(reorderbuf, descr, indexDescr, tuple1, false, descr->oldTuple, &oldheaptuple, (relreplident == REPLICA_IDENTITY_FULL));
		}
	}
	else if (rec_type == WAL_REC_REINSERT)
	{
		HeapTuple	oldheaptuple = NULL;
		HeapTuple	newheaptuple = NULL;

		Assert(ix_type != oIndexToast);
		Assert(!O_TUPLE_IS_NULL(tuple2));

		change->action = REORDER_BUFFER_CHANGE_UPDATE;
		change->data.tp.clear_toast_afterwards = true;

		elog(DEBUG4, "WAL_REC_REINSERT");

		/* Store old tuple or key into reorderbuffer */
		change->data.tp.oldtuple = o_convert_non_toast_tuple(reorderbuf, descr, indexDescr, tuple2, false, descr->oldTuple, &oldheaptuple, (relreplident == REPLICA_IDENTITY_FULL));
		/* Store full new tuple into reorderbuffer */
		change->data.tp.newtuple = o_convert_non_toast_tuple(reorderbuf, descr, indexDescr, tuple1, false, descr->newTuple, &newheaptuple, true);
	}
	else
	{
		elog(FATAL, "Unknown modify WAL record");
	}
}

static ORelOids cur_oids = {0, 0, 0};
static uint32 cur_version = O_TABLE_INVALID_VERSION;

/*
 * Handle OrioleDB records for LogicalDecodingProcessRecord().
 */
void
orioledb_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	XLogReaderState *record = buf->record;
	XLogRecPtr	startXLogPtr = record->ReadRecPtr;
	XLogRecPtr	endXLogPtr = record->EndRecPtr;
	Pointer		startPtr = (Pointer) XLogRecGetData(record);
	Pointer		endPtr = startPtr + XLogRecGetDataLen(record);
	Pointer		ptr = startPtr;
	OTableDescr *descr = NULL;
	OIndexDescr *indexDescr = NULL;
	ORelOids	latest_oids = {0, 0, 0};
	uint32		latest_version = O_TABLE_INVALID_VERSION;
	char		relreplident = REPLICA_IDENTITY_DEFAULT;
	OXid		oxid = InvalidOXid;
	TransactionId logicalXid = InvalidTransactionId,
				heapXid = InvalidTransactionId;
	uint8		rec_type;
	OIndexType	ix_type = oIndexInvalid;
	TupleDescData *o_toast_tupDesc = NULL;
	TupleDescData *heap_toast_tupDesc = NULL;
	uint16		wal_version;
	uint8		wal_flags;

	ptr = wal_container_read_header(ptr, &wal_version, &wal_flags);

	if (wal_version > ORIOLEDB_WAL_VERSION)
	{
		/* WAL from future version */
		elog(ERROR, "Can't logically decode WAL version %u that is newer than supported %u", wal_version, ORIOLEDB_WAL_VERSION);
		return;
	}

	if (wal_flags & WAL_CONTAINER_HAS_XACT_INFO)
	{
		/* Skip WAL_REC_XACT_INFO */
		ptr = wal_parse_container_xact_info(ptr, NULL, NULL);
	}

	elog(DEBUG4, "OrioleDB decode started startXLogPtr %X/%X endXLogPtr %X/%X", LSN_FORMAT_ARGS(startXLogPtr), LSN_FORMAT_ARGS(endXLogPtr));

	while (ptr < endPtr)
	{
		const char *rec_type_str;
		XLogRecPtr	changeXLogPtr = startXLogPtr + (ptr - startPtr);

		rec_type = *ptr;
		ptr++;

		rec_type_str = wal_record_type_to_string(rec_type);

		elog(DEBUG4, "RECEIVE record type %d (%s)", rec_type, rec_type_str);

		if (rec_type == WAL_REC_XID)
		{
			ptr = wal_parse_rec_xid(ptr, &oxid, &logicalXid, &heapXid, wal_version);

			if (TransactionIdIsValid(logicalXid))
			{
				CSNSnapshotData *csnSnapshot;

				elog(DEBUG4, "RECEIVE record type %d (%s) oxid %lu logicalXId %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);

				csnSnapshot = SnapBuildGetCSNSnaphot(ctx->snapshot_builder);
				csnSnapshot->nextXid = Max(csnSnapshot->nextXid, oxid);
			}
			else
			{
				/*
				 * Oriole logical xid stays invalid for an autonomous
				 * transactions (internal Oriole's mechanism intended for
				 * independed commit of Oriole system catalog changes)
				 */
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu", rec_type, rec_type_str, oxid);
			}
		}
		else if (rec_type == WAL_REC_SWITCH_LOGICAL_XID)
		{
			TransactionId topXid,
						subXid;

			ptr = wal_parse_rec_switch_logical_xid(ptr, &topXid, &subXid);

			elog(DEBUG4, "RECEIVE record type %d (%s) %u=>%u oxid %lu logicalXId %u heapXid %u changeXLogPtr %X/%X",
				 rec_type, rec_type_str,
				 topXid, subXid,
				 oxid, logicalXid, heapXid, LSN_FORMAT_ARGS(changeXLogPtr));

			Assert(TransactionIdIsValid(topXid));
			Assert(TransactionIdIsValid(subXid));

			ReorderBufferAssignChild(ctx->reorder,
									 topXid, subXid,
									 changeXLogPtr);
		}
		else if (rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK)
		{
			OXid		xmin;
			dlist_iter	cur_txn_i;
			ReorderBufferTXN *txn;
			CommitSeqNo csn;
			CSNSnapshotData *csnSnapshot;

			ptr = wal_parse_rec_finish(ptr, &xmin, &csn);

			elog(DEBUG4, "RECEIVE record type %d (%s) oxid %lu logicalXId %u heapXid %u",
				 rec_type, rec_type_str, oxid, logicalXid, heapXid);

			if (!TransactionIdIsValid(logicalXid))
			{
				/*
				 * Oriole logical xid stays invalid for an autonomous
				 * transactions
				 */
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu", rec_type, rec_type_str, oxid);

				UpdateDecodingStats(ctx);
				continue;
			}

			csnSnapshot = SnapBuildGetCSNSnaphot(ctx->snapshot_builder);
			csnSnapshot->snapshotcsn = csn;
			csnSnapshot->xlogptr = endXLogPtr;
			csnSnapshot->xmin = xmin;

			if (!set_snapshot(ctx, logicalXid, changeXLogPtr))
				continue;

			txn = get_reorder_buffer_txn(ctx->reorder, logicalXid);

			if (txn == NULL)
			{
				elog(DEBUG4, "RECEIVE record type %d (%s) oxid %lu logicalXId %u :: unknown transaction, nothing to replay",
					 rec_type, rec_type_str, oxid, logicalXid);
			}

			/* unknown transaction, nothing to replay */
			if (txn != NULL)
			{
				Assert(TransactionIdIsValid(logicalXid));

				if (rec_type == WAL_REC_COMMIT)
				{
					/*
					 * SnapBuildXactNeedsSkip() does strict comparison.  So,
					 * subtract endXLogPtr by one to fit snapshot just taken
					 * after commit.
					 */
					if (SnapBuildXactNeedsSkip(ctx->snapshot_builder,
											   endXLogPtr - 1) ||
						ctx->fast_forward)
					{
						elog(DEBUG4, "FORGET record type %d (%s) oxid %lu logicalXid %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);

						dlist_foreach(cur_txn_i, &txn->subtxns)
						{
							ReorderBufferTXN *cur_txn;

							cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);
							ReorderBufferForget(ctx->reorder, cur_txn->xid,
												startXLogPtr);
						}
						ReorderBufferForget(ctx->reorder, txn->xid,
											startXLogPtr);

						oxid = InvalidOXid;
						logicalXid = InvalidTransactionId;

						/* Skip processing of this transaction */
						continue;
					}

					/*
					 * Proceed to commit this transaction and subtransactions
					 */
					dlist_foreach(cur_txn_i, &txn->subtxns)
					{
						ReorderBufferTXN *cur_txn;

						cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);
						ReorderBufferCommitChild(ctx->reorder, txn->xid, cur_txn->xid,
												 startXLogPtr, endXLogPtr);
					}
					elog(DEBUG4, "COMMIT record type %d (%s) oxid %lu logicalXid %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);
					ReorderBufferCommit(ctx->reorder, logicalXid,
										startXLogPtr, endXLogPtr,
										0, XLogRecGetOrigin(buf->record),
										buf->origptr);
				}
				else
				{
					Assert(rec_type == WAL_REC_ROLLBACK);

					dlist_foreach(cur_txn_i, &txn->subtxns)
					{
						ReorderBufferTXN *cur_txn;

						cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);
						ReorderBufferAbort(ctx->reorder, cur_txn->xid,
										   startXLogPtr, 0);
					}
					elog(DEBUG4, "ABORT record type %d (%s) oxid %lu logicalXid %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);
					ReorderBufferAbort(ctx->reorder, logicalXid,
									   startXLogPtr, 0);
				}
			}

			UpdateDecodingStats(ctx);

			oxid = InvalidOXid;
			logicalXid = InvalidTransactionId;
		}
		else if (rec_type == WAL_REC_JOINT_COMMIT)
		{
			TransactionId xid;
			OXid		xmin;
			CommitSeqNo csn;
			CSNSnapshotData *csnSnapshot;

			ptr = wal_parse_rec_joint_commit(ptr, &xid, &xmin, &csn);

			elog(DEBUG4, "RECEIVE record type %d (%s) oxid %lu logicalXId %u heapXid %u",
				 rec_type, rec_type_str, oxid, logicalXid, heapXid);

			if (!TransactionIdIsValid(logicalXid))
			{
				/*
				 * Oriole logical xid stays invalid for an autonomous
				 * transactions
				 */
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu", rec_type, rec_type_str, oxid);

				UpdateDecodingStats(ctx);
				continue;
			}

			csnSnapshot = SnapBuildGetCSNSnaphot(ctx->snapshot_builder);
			csnSnapshot->snapshotcsn = csn;
			csnSnapshot->xlogptr = endXLogPtr;
			csnSnapshot->xmin = xmin;

			if (!set_snapshot(ctx, logicalXid, changeXLogPtr))
				continue;

			/* Skip actual commit processing */
			if (SnapBuildXactNeedsSkip(ctx->snapshot_builder, endXLogPtr - 1) ||
				ctx->fast_forward)
			{
				elog(DEBUG4, "FORGET record type %d (%s) oxid %lu logicalXid %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);

				ReorderBufferForget(ctx->reorder, logicalXid, startXLogPtr);
			}
			else
			{
				if (TransactionIdIsValid(heapXid) && TransactionIdIsValid(logicalXid))
				{
					/*
					 * Oriole txn acts as a sub-txn to heap txn, needs
					 * ReorderBufferCommitChild
					 */
					elog(DEBUG4, "ReorderBufferCommitChild on record type %d (%s) oxid %lu logicalXid %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);

					ReorderBufferCommitChild(ctx->reorder, heapXid, logicalXid, buf->origptr, buf->endptr);
				}

				UpdateDecodingStats(ctx);
			}

			oxid = InvalidOXid;
			logicalXid = InvalidTransactionId;
		}
		else if (rec_type == WAL_REC_RELATION)
		{
			int			sys_tree_num = -1;
			uint8		treeType = 0;
			OXid		xmin;

			OSnapshot	snapshot;
			ORelOids	cur_ixoids = {0, 0, 0};
			uint32		cur_ixversion = O_TABLE_INVALID_VERSION;

			ptr = wal_parse_rec_relation(ptr, &treeType, &latest_oids, &xmin, &snapshot.csn, &snapshot.cid, &latest_version, wal_version);

			snapshot.xmin = xmin;
			snapshot.xlogptr = changeXLogPtr;

			if (wal_version < 17)
			{
				/* Override undefined values from WAL */
				snapshot = o_non_deleted_snapshot;
				latest_version = O_TABLE_INVALID_VERSION;
			}

			ix_type = treeType;
			relreplident = REPLICA_IDENTITY_DEFAULT;
			descr = NULL;

			if (ix_type == oIndexInvalid)
			{
				cur_oids = latest_oids;
				cur_version = latest_version;
			}
			else if (ix_type == oIndexToast)
			{
				cur_ixoids = latest_oids;
				cur_ixversion = latest_version;
			}

			elog(DEBUG4, "WAL_REC_RELATION");

			if (!TransactionIdIsValid(logicalXid))
			{
				/*
				 * Oriole logical xid stays invalid for an autonomous
				 * transactions
				 */
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu", rec_type, rec_type_str, oxid);
				continue;
			}

			/* Skip actual relation processing in fast_forward mode */
			if (ctx->fast_forward)
			{
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu", rec_type, rec_type_str, oxid);
				continue;
			}

			if (IS_SYS_TREE_OIDS(latest_oids))
				sys_tree_num = latest_oids.relnode;
			else
				sys_tree_num = -1;

			if (sys_tree_num > 0)
			{
				descr = NULL;
				/* indexDescr = NULL; */
				Assert(sys_tree_get_storage_type(sys_tree_num) == BTreeStoragePersistence);
			}
			else if (ix_type == oIndexInvalid)
			{
				elog(DEBUG4, "WAL_REC_RELATION oIndexInvalid :: FETCH RELATION [ %u %u %u ] version %u",
					 latest_oids.datoid, latest_oids.reloid, latest_oids.relnode,
					 latest_version);

				descr = o_fetch_table_descr_extended(latest_oids, snapshot, latest_version);
				indexDescr = descr ? GET_PRIMARY(descr) : NULL;
			}
			else if (ix_type == oIndexToast)
			{
				elog(DEBUG4, "WAL_REC_RELATION [1] oIndexToast :: FETCH INDEX oids [ %u %u %u ] version %u :: for RELATION [ %u %u %u ] version %u",
					 cur_ixoids.datoid, cur_ixoids.reloid, cur_ixoids.relnode,
					 cur_ixversion,
					 cur_oids.datoid, cur_oids.reloid, cur_oids.relnode,
					 cur_version);

				indexDescr = o_fetch_index_descr_extended(cur_ixoids, ix_type, false, NULL, snapshot, cur_ixversion);

				elog(DEBUG4, "WAL_REC_RELATION [2] oIndexToast :: FETCH RELATION [ %u %u %u ] version %u -> retrieved [ %u %u %u ]",
					 cur_oids.datoid, cur_oids.reloid, cur_oids.relnode,
					 cur_version,
					 indexDescr->tableOids.datoid, indexDescr->tableOids.reloid, indexDescr->tableOids.relnode);

				Assert(indexDescr->tableOids.datoid == cur_oids.datoid);
				Assert(indexDescr->tableOids.reloid == cur_oids.reloid);
				Assert(indexDescr->tableOids.relnode == cur_oids.relnode);

				descr = o_fetch_table_descr_extended(indexDescr->tableOids, snapshot, cur_version);
				o_toast_tupDesc = descr->toast->leafTupdesc;
				/* Init heap tupledesc for toast table */
				heap_toast_tupDesc = CreateTemplateTupleDesc(3);
				o_tables_tupdesc_init_builtin(heap_toast_tupDesc, (AttrNumber) 1, "chunk_id", OIDOID);
				o_tables_tupdesc_init_builtin(heap_toast_tupDesc, (AttrNumber) 2, "chunk_seq", INT4OID);
				o_tables_tupdesc_init_builtin(heap_toast_tupDesc, (AttrNumber) 3, "chunk_data", BYTEAOID);
				/* Ensure that the toast table doesn't itself get toasted */
				TupleDescAttr(heap_toast_tupDesc, 0)->attstorage = TYPSTORAGE_PLAIN;
				TupleDescAttr(heap_toast_tupDesc, 1)->attstorage = TYPSTORAGE_PLAIN;
				TupleDescAttr(heap_toast_tupDesc, 2)->attstorage = TYPSTORAGE_PLAIN;
				/* Toast field should not be compressed */
				TupleDescAttr(heap_toast_tupDesc, 0)->attcompression = InvalidCompressionMethod;
				TupleDescAttr(heap_toast_tupDesc, 1)->attcompression = InvalidCompressionMethod;
				TupleDescAttr(heap_toast_tupDesc, 2)->attcompression = InvalidCompressionMethod;

				/*
				 * indexDescr = o_fetch_index_descr(cur_oids, ix_type, false,
				 * NULL);
				 */
			}
			else
			{
				Assert(ix_type == oIndexBridge);
			}

			if (descr && descr->toast)
				elog(DEBUG4, "reloid: %d natts: %u toast natts: %u", latest_oids.reloid, descr->tupdesc->natts, descr->toast->leafTupdesc->natts);

		}
		else if (rec_type == WAL_REC_RELREPLIDENT)
		{
			Oid			relreplident_ix_oid;	/* Unused yet */

			ptr = wal_parse_rec_relreplident(ptr, &relreplident, &relreplident_ix_oid);

			/* Skip */
		}
		else if (rec_type == WAL_REC_O_TABLES_META_LOCK || rec_type == WAL_REC_REPLAY_FEEDBACK)
		{
			/* Skip */
		}
		else if (rec_type == WAL_REC_O_TABLES_META_UNLOCK)
		{
			ORelOids	oids;
			Oid			oldRelnode;

			ptr = wal_parse_rec_o_tables_meta_unlock(ptr, &oids, &oldRelnode);

			/* Skip */
		}
		else if (rec_type == WAL_REC_TRUNCATE)
		{
			ORelOids	oids;

			ptr = wal_parse_rec_truncate(ptr, &oids);

			/* Skip */
		}
		else if (rec_type == WAL_REC_SAVEPOINT)
		{
			SubTransactionId parentSubid;
			TransactionId parentLogicalXid;

			ptr = wal_parse_rec_savepoint(ptr, &parentSubid, &logicalXid, &parentLogicalXid);

			elog(DEBUG4, "APPLY record type %d (%s) oxid %lu logicalXid %u parentLogicalXid %u", rec_type, rec_type_str, oxid, logicalXid, parentLogicalXid);

			if (!ctx->fast_forward)
				ReorderBufferAssignChild(ctx->reorder, parentLogicalXid, logicalXid, buf->origptr);

			/* Skip */
		}
		else if (rec_type == WAL_REC_ROLLBACK_TO_SAVEPOINT)
		{
			SubTransactionId parentSubid;
			OXid		xmin;
			dlist_iter	cur_txn_i;
			ReorderBufferTXN *txn;
			CommitSeqNo csn;
			CSNSnapshotData *csnSnapshot;

			ptr = wal_parse_rec_rollback_to_savepoint(ptr, &parentSubid, &xmin, &csn, wal_version);

			if (wal_version < 17)
			{
				/* Skip */
				continue;
			}

			elog(DEBUG4, "RECEIVE record type %d (%s) oxid %lu logicalXId %u heapXid %u parentSubid %u",
				 rec_type, rec_type_str, oxid, logicalXid, heapXid, parentSubid);

			if (!TransactionIdIsValid(logicalXid))
			{
				/*
				 * Oriole logical xid stays invalid for an autonomous
				 * transactions
				 */
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu", rec_type, rec_type_str, oxid);

				UpdateDecodingStats(ctx);
				continue;
			}

			csnSnapshot = SnapBuildGetCSNSnaphot(ctx->snapshot_builder);
			csnSnapshot->snapshotcsn = csn;
			csnSnapshot->xlogptr = endXLogPtr;
			csnSnapshot->xmin = xmin;

			if (!set_snapshot(ctx, logicalXid, changeXLogPtr))
				continue;

			txn = get_reorder_buffer_txn(ctx->reorder, logicalXid);

			if (txn == NULL)
			{
				elog(DEBUG4, "RECEIVE record type %d (%s) oxid %lu logicalXId %u heapXid %u :: unknown transaction, nothing to replay",
					 rec_type, rec_type_str, oxid, logicalXid, heapXid);
			}

			/* unknown transaction, nothing to replay */
			if (txn != NULL)
			{
				Assert(TransactionIdIsValid(logicalXid));

				dlist_foreach(cur_txn_i, &txn->subtxns)
				{
					ReorderBufferTXN *cur_txn;

					cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);
					ReorderBufferAbort(ctx->reorder, cur_txn->xid,
									   startXLogPtr, 0);
				}
				elog(DEBUG4, "ABORT record type %d (%s) oxid %lu logicalXid %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);
				ReorderBufferAbort(ctx->reorder, logicalXid,
								   startXLogPtr, 0);
			}

			UpdateDecodingStats(ctx);

			oxid = InvalidOXid;
			logicalXid = InvalidTransactionId;
		}
		else if (rec_type == WAL_REC_INSERT || rec_type == WAL_REC_UPDATE || rec_type == WAL_REC_DELETE || rec_type == WAL_REC_REINSERT)
		{
			OFixedTuple tuple1;
			OFixedTuple tuple2;
			OffsetNumber debug_length;
			ReorderBufferTXN *txn;
			bool		read_two_tuples;

			ReorderBufferProcessXid(ctx->reorder, logicalXid, changeXLogPtr);

			read_two_tuples = (rec_type == WAL_REC_REINSERT || (rec_type == WAL_REC_UPDATE && relreplident == REPLICA_IDENTITY_FULL));
			ptr = wal_parse_rec_modify(ptr, &tuple1, &tuple2, &debug_length, read_two_tuples);

			elog(DEBUG4, "RECEIVE record type %d (%s) oids [ %u %u %u ] oxid %lu logicalXId %u heapXid %u",
				 rec_type, rec_type_str,
				 latest_oids.datoid, latest_oids.reloid, latest_oids.relnode,
				 oxid, logicalXid, heapXid);

			if (!TransactionIdIsValid(logicalXid))
			{
				/*
				 * Oriole logical xid stays invalid for an autonomous
				 * transactions
				 */
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu", rec_type, rec_type_str, oxid);
				continue;
			}

			ReorderBufferProcessXid(ctx->reorder, logicalXid, changeXLogPtr);

			if (SnapBuildCurrentState(ctx->snapshot_builder) < SNAPBUILD_FULL_SNAPSHOT)
				continue;

			(void) set_snapshot(ctx, logicalXid, changeXLogPtr);

			txn = get_reorder_buffer_txn(ctx->reorder, logicalXid);
			txn->txn_flags |= RBTXN_DISTR_SKIP_CLEANUP;

			/* Skip actual record processing in fast_forward mode */
			if (ctx->fast_forward)
				continue;

			if ((ix_type == oIndexInvalid || ix_type == oIndexToast) &&
				latest_oids.datoid == ctx->slot->data.database)
			{
				ReorderBufferChange *change;

				Assert(descr != NULL);
				Assert(!O_TUPLE_IS_NULL(tuple1.tuple));
				change = ReorderBufferGetChange(ctx->reorder);
				change->data.tp.rlocator.spcOid = DEFAULTTABLESPACE_OID;
				change->data.tp.rlocator.dbOid = latest_oids.datoid;
				change->data.tp.rlocator.relNumber = latest_oids.relnode;
				elog(DEBUG4, "reloid: %u", latest_oids.reloid);

				o_decode_modify_tuples(ctx->reorder, rec_type, ix_type, change, descr, indexDescr, o_toast_tupDesc, heap_toast_tupDesc, tuple1.tuple, tuple2.tuple, debug_length, relreplident);

				ReorderBufferQueueChange(ctx->reorder, logicalXid,
										 changeXLogPtr,
										 change, (ix_type == oIndexToast));
			}
			else
			{
				/* Do nothing */
				elog(DEBUG4, "Logical decoding modify ix_type, %u", ix_type);
			}
		}
		else
		{
			elog(FATAL, "Unknown WAL record");
		}
	}
}
