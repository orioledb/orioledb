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
 * Convert tuples from the main relation from OrioleDB to haep format with
 * conversion of TOAST pointers to PG format.
 */
static HeapTuple
convert_toast_pointers(OTableDescr *descr, OIndexDescr *indexDescr,
					   OFixedTuple *tuple)
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
		old_values[i] = o_fastgetattr(tuple->tuple, i + ctid_off + 1,
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

static void
decode_modify_wal_tuples(LogicalDecodingContext *ctx, uint8 rec_type, OIndexType ix_type, ReorderBufferChange *change, OTableDescr *descr, OIndexDescr *indexDescr,
						 TupleDescData *o_toast_tupDesc, TupleDescData *heap_toast_tupDesc, OFixedTuple tuple1, OFixedTuple tuple2, OffsetNumber debug_length)
{
	if (rec_type == WAL_REC_INSERT)
	{
		change->action = REORDER_BUFFER_CHANGE_INSERT;

		/* Decode TOAST chunks */
		if (ix_type == oIndexToast)
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

			elog(DEBUG4, "WAL_REC_INSERT TOAST");

			Assert(o_toast_tupDesc);
			pk_natts = o_toast_tupDesc->natts - TOAST_LEAF_FIELDS_NUM;
			attnum = (uint16) o_fastgetattr(tuple1.tuple, pk_natts + ATTN_POS, o_toast_tupDesc, &indexDescr->leafSpec, &attnum_isnull);
			chunk_seq = (uint32) o_fastgetattr(tuple1.tuple, pk_natts + CHUNKN_POS, o_toast_tupDesc, &indexDescr->leafSpec, &chunk_seq_isnull);
			Assert((!attnum_isnull) && (!chunk_seq_isnull));
			Assert(attnum > 0);

			/* Toast chunk in VARATT_4B uncompressed format */
			old_chunk = o_fastgetattr_ptr(tuple1.tuple, pk_natts + DATA_POS, o_toast_tupDesc, &indexDescr->leafSpec);
			old_chunk_size = VARSIZE(old_chunk) - VARHDRSZ;

			/*
			 * ReorderBufferToastReplace() expects first toasted chunk without
			 * the header of the whole toasted value and reconstructs it
			 * itself. Cut this extra header from OrioleDB toasted chunk.
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
			change->data.tp.newtuple = record_buffer_tuple(ctx->reorder, toasttup, true);
			change->data.tp.clear_toast_afterwards = false;

			if (need_to_free && new_chunk)
			{
				pfree(new_chunk);
				need_to_free = false;
			}
		}
		else
		{
			Assert(ix_type != oIndexToast);

			/*
			 * Primary table contains TOASTed attributes needs conversion of
			 * them
			 */
			if (descr->ntoastable > 0)
			{
				HeapTuple	newheaptuple;

				elog(DEBUG4, "WAL_REC_INSERT NON-TOAST toastable");
				newheaptuple = convert_toast_pointers(descr, indexDescr, &tuple1);
				change->data.tp.newtuple = record_buffer_tuple(ctx->reorder, newheaptuple, true);
				Assert(change->data.tp.newtuple);
			}
			else				/* Tuple without TOASTed attrs */
			{
				elog(DEBUG4, "WAL_REC_INSERT NON-TOAST plain");
				tts_orioledb_store_tuple(descr->newTuple, tuple1.tuple,
										 descr, COMMITSEQNO_INPROGRESS,
										 PrimaryIndexNumber, false,
										 NULL);
				change->data.tp.newtuple = record_buffer_tuple_slot(ctx->reorder, descr->newTuple);
			}
			change->data.tp.clear_toast_afterwards = true;

		}
	}
	else if (rec_type == WAL_REC_UPDATE)
	{
		Assert(ix_type != oIndexToast);

		change->action = REORDER_BUFFER_CHANGE_UPDATE;
		change->data.tp.clear_toast_afterwards = true;

		/*
		 * Primary table contains TOASTed attributes needs conversion of them
		 */
		if (descr->ntoastable > 0)
		{
			HeapTuple	newheaptuple;

			elog(DEBUG4, "WAL_REC_UPDATE toastable");

			newheaptuple = convert_toast_pointers(descr, indexDescr, &tuple1);
			ExecForceStoreHeapTuple(newheaptuple, descr->newTuple, false);
			change->data.tp.newtuple = record_buffer_tuple(ctx->reorder, newheaptuple, true);
		}
		else					/* Tuple without TOASTed attrs */
		{
			elog(DEBUG4, "WAL_REC_UPDATE plain");

			tts_orioledb_store_tuple(descr->newTuple, tuple1.tuple,
									 descr, COMMITSEQNO_INPROGRESS,
									 PrimaryIndexNumber, false,
									 NULL);
			change->data.tp.newtuple = record_buffer_tuple_slot(ctx->reorder, descr->newTuple);
		}
		tts_copy_identity(descr->newTuple, descr->oldTuple,
						  GET_PRIMARY(descr));
		change->data.tp.oldtuple = record_buffer_tuple_slot(ctx->reorder, descr->oldTuple);
	}
	else if (rec_type == WAL_REC_DELETE)
	{
		change->action = REORDER_BUFFER_CHANGE_DELETE;
		if (ix_type == oIndexToast)
		{
			elog(DEBUG4, "WAL_REC_DELETE TOAST");

			change->data.tp.clear_toast_afterwards = false;
			tts_orioledb_store_non_leaf_tuple(descr->oldTuple, tuple1.tuple,
											  descr, COMMITSEQNO_INPROGRESS,
											  PrimaryIndexNumber, false,
											  NULL);
			change->data.tp.oldtuple = record_buffer_tuple_slot(ctx->reorder, descr->oldTuple);
		}
		else
		{
			change->data.tp.clear_toast_afterwards = true;

			/*
			 * Primary table contains TOASTed attributes needs conversion of
			 * them
			 */
			if (descr->ntoastable > 0)
			{
				HeapTuple	oldheaptuple;

				elog(DEBUG4, "WAL_REC_DELETE NON-TOAST toastable");
				oldheaptuple = convert_toast_pointers(descr, indexDescr, &tuple1);
				change->data.tp.oldtuple = record_buffer_tuple(ctx->reorder, oldheaptuple, true);
			}
			else				/* Tuple without TOASTed attrs */
			{
				elog(DEBUG4, "WAL_REC_DELETE NON-TOAST plain");
				tts_orioledb_store_non_leaf_tuple(descr->oldTuple, tuple1.tuple,
												  descr, COMMITSEQNO_INPROGRESS,
												  PrimaryIndexNumber, false,
												  NULL);
				change->data.tp.oldtuple = record_buffer_tuple_slot(ctx->reorder, descr->oldTuple);
			}
		}
	}
	else if (rec_type == WAL_REC_REINSERT)
	{
		Assert(ix_type != oIndexToast);
		Assert(!O_TUPLE_IS_NULL(tuple2.tuple));

		change->action = REORDER_BUFFER_CHANGE_UPDATE;
		change->data.tp.clear_toast_afterwards = true;


		/*
		 * Primary table contains TOASTed attributes needs conversion of them
		 */
		if (descr->ntoastable > 0)
		{
			HeapTuple	newheaptuple;
			HeapTuple	oldheaptuple;

			elog(DEBUG4, "WAL_REC_REINSERT toastable");
			/* Store primary key into reorderbuffer oldtuple */
			oldheaptuple = convert_toast_pointers(descr, indexDescr, &tuple2);
			ExecForceStoreHeapTuple(oldheaptuple, descr->newTuple, false);
			tts_copy_identity(descr->newTuple, descr->oldTuple,
							  GET_PRIMARY(descr));
			change->data.tp.oldtuple = record_buffer_tuple_slot(ctx->reorder, descr->oldTuple);

			/* Store full tuple into reorderbuffer newtuple */
			newheaptuple = convert_toast_pointers(descr, indexDescr, &tuple1);
			change->data.tp.newtuple = record_buffer_tuple(ctx->reorder, newheaptuple, true);
		}
		else					/* Tuple without TOASTed attrs */
		{
			elog(DEBUG4, "WAL_REC_REINSERT plain");
			tts_orioledb_store_non_leaf_tuple(descr->oldTuple, tuple2.tuple,
											  descr, COMMITSEQNO_INPROGRESS,
											  PrimaryIndexNumber, false,
											  NULL);
			change->data.tp.oldtuple = record_buffer_tuple_slot(ctx->reorder, descr->oldTuple);

			tts_orioledb_store_tuple(descr->newTuple, tuple1.tuple,
									 descr, COMMITSEQNO_INPROGRESS,
									 PrimaryIndexNumber, false,
									 NULL);

			change->data.tp.newtuple = record_buffer_tuple_slot(ctx->reorder, descr->newTuple);
		}
	}
	else
	{
		elog(FATAL, "Unknown modify WAL record");
	}
}

#define LOGICAL_XID_ASSIGNED_FROM_HEAP(logicalXid, heapXid) (TransactionIdIsValid(heapXid) && logicalXid == heapXid)

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
	ORelOids	cur_oids = {0, 0, 0};
	OXid		oxid = InvalidOXid;
	TransactionId logicalXid = InvalidTransactionId;
	TransactionId heapXid = InvalidTransactionId;
	uint8		rec_type;
	OIndexType	ix_type = oIndexInvalid;
	TupleDescData *o_toast_tupDesc = NULL;
	TupleDescData *heap_toast_tupDesc = NULL;
	uint16		wal_version;

	wal_version = check_wal_container_version(&ptr);

	if (wal_version > CURRENT_WAL_VERSION)
	{
		/* WAL from future version */
		elog(ERROR, "Can't logically decode WAL version %u that is newer than supported %u", wal_version, CURRENT_WAL_VERSION);
		return;
	}

	elog(DEBUG4, "[%s] STARTED", __func__);

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
			ptr = wal_parse_rec_xid(ptr, &oxid, &logicalXid, &heapXid);

			if (TransactionIdIsValid(logicalXid))
			{
				CSNSnapshotData *csnSnapshot;

				elog(DEBUG4, "RECEIVE record type %d (%s) oxid %lu logicalXId %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);

				csnSnapshot = SnapBuildGetCSNSnaphot(ctx->snapshot_builder);
				csnSnapshot->nextXid = Max(csnSnapshot->nextXid, oxid);
			}
			else
			{
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu heapXid %u", rec_type, rec_type_str, oxid, heapXid);
			}
		}
		else if (rec_type == WAL_REC_SWITCH_LOGICAL_XID)
		{
			TransactionId topXid,
						subXid;

			ptr = wal_parse_rec_switch_logical_xid(ptr, &topXid, &subXid);

			elog(DEBUG4, "RECEIVE record type %d (%s) %u=>%u oxid %lu logicalXId %u heapXid %u",
				 rec_type, rec_type_str,
				 topXid, subXid,
				 oxid, logicalXid, heapXid);

			Assert(TransactionIdIsValid(topXid));
			Assert(TransactionIdIsValid(subXid));
			///Assert(logicalXid == oldXid);

			ReorderBufferAssignChild(ctx->reorder,
									 topXid, subXid, // @NOTE XID_NOCHANGE
									 buf->origptr);

			//logicalXid = oldXid;
			//heapXid = newXid;
		}
		else if (rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK)
		{
			OXid		xmin;
			dlist_iter	cur_txn_i;
			ReorderBufferTXN *txn;
			CommitSeqNo csn;
			CSNSnapshotData *csnSnapshot;

			elog(DEBUG4, "RECEIVE record type %d (%s) oxid %lu logicalXId %u heapXid %u",
				 rec_type, rec_type_str,
				 oxid, logicalXid, heapXid);

			ptr = wal_parse_rec_finish(ptr, &xmin, &csn);

			if (!TransactionIdIsValid(logicalXid))
			{
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu heapXid %u", rec_type, rec_type_str, oxid, heapXid);

				UpdateDecodingStats(ctx);
				oxid = InvalidOXid;
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
					rec_type, rec_type_str,
					oxid, logicalXid, heapXid);
			}

			/* unknown transaction, nothing to replay */
			if (txn != NULL)
			{
				if (txn->toptxn)
					txn = txn->toptxn;

				Assert(TransactionIdIsValid(logicalXid));

				if (LOGICAL_XID_ASSIGNED_FROM_HEAP(logicalXid, heapXid))
				{
					/*
					 * Finalize (commit/abort) as heap transaction so ignore
					 * here
					 */
					elog(DEBUG4, "IGNORED on record type %d (%s) oxid %lu logicalXId %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);
					continue;
				}

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
							if (cur_txn) /* may becomes NULL if sub-txn is heap and finalized before */
							{
								ReorderBufferForget(ctx->reorder, cur_txn->xid,
													startXLogPtr);
							}
						}
						ReorderBufferForget(ctx->reorder, txn->xid,
											startXLogPtr);

						oxid = InvalidOXid;
						logicalXid = InvalidTransactionId;

						/* Skip processing of this transaction */
						continue;
					}

					/*
						* Proceed to commit this transaction and
						* subtransactions
						*/
					dlist_foreach(cur_txn_i, &txn->subtxns)
					{
						ReorderBufferTXN *cur_txn;

						cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);
						if (cur_txn) /* may becomes NULL if sub-txn is heap and finalized before */
						{
							ReorderBufferCommitChild(ctx->reorder, txn->xid, cur_txn->xid,
													startXLogPtr, endXLogPtr);
						}
					}
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
						if (cur_txn) /* may becomes NULL if sub-txn is heap and finalized before */
						{
							ReorderBufferAbort(ctx->reorder, cur_txn->xid,
											startXLogPtr, 0);
						}
					}
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
#if 0
			TransactionId xid;
			OXid		xmin;
			CommitSeqNo csn;
			CSNSnapshotData *csnSnapshot;

			ptr = wal_parse_rec_joint_commit(ptr, &xid, &xmin, &csn);

			if (!TransactionIdIsValid(logicalXid))
			{
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu heapXid %u", rec_type, rec_type_str, oxid, heapXid);

				UpdateDecodingStats(ctx);
				oxid = InvalidOXid;
				continue;
			}

			csnSnapshot = SnapBuildGetCSNSnaphot(ctx->snapshot_builder);
			csnSnapshot->snapshotcsn = csn;
			csnSnapshot->xlogptr = endXLogPtr;
			csnSnapshot->xmin = xmin;

			if (!set_snapshot(ctx, logicalXid, changeXLogPtr))
				continue;

			if (LOGICAL_XID_ASSIGNED_FROM_HEAP(logicalXid, heapXid))
			{
				/* Finalize (commit/abort) as heap transaction so ignore here */
				elog(DEBUG4, "IGNORED on record type %d (%s) oxid %lu logicalXId %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);
				continue;
			}

			/* Skip actual commit processing */
			if (SnapBuildXactNeedsSkip(ctx->snapshot_builder, endXLogPtr - 1) ||
				ctx->fast_forward)
			{
				elog(DEBUG4, "FORGET record type %d (%s) oxid %lu logicalXid %u heapXid %u", rec_type, rec_type_str, oxid, logicalXid, heapXid);
				ReorderBufferForget(ctx->reorder, logicalXid, startXLogPtr);
			}
			else
			{
				//if (TXN_NEED_FINALIZE(logicalXid, heapXid))
				//{
					ReorderBufferCommit(ctx->reorder, logicalXid,
										startXLogPtr, endXLogPtr,
										0, XLogRecGetOrigin(buf->record),
										buf->origptr);
					UpdateDecodingStats(ctx);
				//}
			}

			oxid = InvalidOXid;
			logicalXid = InvalidTransactionId;
#endif
		}
		else if (rec_type == WAL_REC_RELATION)
		{
			int			sys_tree_num = -1;
			uint8		treeType = 0;

			ptr = WAL_PARSE_REC_RELATION(ptr, treeType, cur_oids);

			ix_type = treeType;

			if (!TransactionIdIsValid(logicalXid))
			{
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu heapXid %u", rec_type, rec_type_str, oxid, heapXid);
				continue;
			}

			elog(DEBUG4, "WAL_REC_RELATION");

			/* Skip actual relation processing in fast_forward mode */
			if (ctx->fast_forward)
				continue;

			if (IS_SYS_TREE_OIDS(cur_oids))
				sys_tree_num = cur_oids.relnode;
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
				descr = o_fetch_table_descr(cur_oids);
				indexDescr = descr ? GET_PRIMARY(descr) : NULL;
				elog(DEBUG4, "WAL_REC_RELATION oIndexInvalid");

			}
			else if (ix_type == oIndexToast)
			{
				elog(DEBUG4, "WAL_REC_RELATION oIndexToast");

				indexDescr = o_fetch_index_descr(cur_oids, ix_type, false, NULL);
				descr = o_fetch_table_descr(indexDescr->tableOids);
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
				elog(DEBUG4, "reloid: %d natts: %u toast natts: %u", cur_oids.reloid, descr->tupdesc->natts, descr->toast->leafTupdesc->natts);

		}
		else if (rec_type == WAL_REC_O_TABLES_META_LOCK)
		{
			/* Skip */
		}
		else if (rec_type == WAL_REC_O_TABLES_META_UNLOCK)
		{
			ORelOids	oids;
			Oid			oldRelnode;

			ptr = WAL_PARSE_REC_O_TABLES_META_UNLOCK(ptr, oids, oldRelnode);

			/* Skip */
		}
		else if (rec_type == WAL_REC_TRUNCATE)
		{
			ORelOids	oids;

			ptr = WAL_PARSE_REC_TRUNCATE(ptr, oids);

			/* Skip */
		}
		else if (rec_type == WAL_REC_SAVEPOINT)
		{
			SubTransactionId parentSubid;
			TransactionId parentLogicalXid;

			ptr = wal_parse_rec_savepoint(ptr, &parentSubid, &logicalXid, &parentLogicalXid);

			if (!ctx->fast_forward)
				ReorderBufferAssignChild(ctx->reorder, parentLogicalXid, logicalXid, buf->origptr);

			/* Skip */
		}
		else if (rec_type == WAL_REC_ROLLBACK_TO_SAVEPOINT)
		{
			SubTransactionId parentSubid;

			ptr = wal_parse_rec_rollback_to_savepoint(ptr, &parentSubid);

			/* Skip */
		}
		else if (rec_type == WAL_REC_INSERT || rec_type == WAL_REC_UPDATE || rec_type == WAL_REC_DELETE || rec_type == WAL_REC_REINSERT)
		{
			OFixedTuple tuple1;
			OFixedTuple tuple2;
			OffsetNumber debug_length;
			ReorderBufferTXN *txn;

			read_modify_wal_tuples(rec_type, &ptr, &tuple1, &tuple2, &debug_length);

			if (!TransactionIdIsValid(logicalXid))
			{
				elog(DEBUG4, "IGNORED record type %d (%s) invalid logicalXid for oxid %lu heapXid %u", rec_type, rec_type_str, oxid, heapXid);
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
				cur_oids.datoid == ctx->slot->data.database)
			{
				ReorderBufferChange *change;

				Assert(descr != NULL);
				Assert(!O_TUPLE_IS_NULL(tuple1.tuple));
				change = ReorderBufferGetChange(ctx->reorder);
				change->data.tp.rlocator.spcOid = DEFAULTTABLESPACE_OID;
				change->data.tp.rlocator.dbOid = cur_oids.datoid;
				change->data.tp.rlocator.relNumber = cur_oids.relnode;
				elog(DEBUG4, "reloid: %u", cur_oids.reloid);

				decode_modify_wal_tuples(ctx, rec_type, ix_type, change, descr, indexDescr, o_toast_tupDesc, heap_toast_tupDesc, tuple1, tuple2, debug_length);

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
