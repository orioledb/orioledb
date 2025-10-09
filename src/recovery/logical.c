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
	int			sys_tree_num = -1;
	ORelOids	cur_oids = {0, 0, 0};
	OXid		oxid = InvalidOXid;
	TransactionId logicalXid = InvalidTransactionId;
	uint8		rec_type;
	OffsetNumber length;
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

	while (ptr < endPtr)
	{
		XLogRecPtr	changeXLogPtr = startXLogPtr + (ptr - startPtr);

		rec_type = *ptr;
		ptr++;

		elog(DEBUG4, "RECEIVE: %s", rec_type == WAL_REC_XID ? "XID" :
			 rec_type == WAL_REC_COMMIT ? "COMMIT" :
			 rec_type == WAL_REC_ROLLBACK ? "ROLLBACK" :
			 rec_type == WAL_REC_JOINT_COMMIT ? "JOINT COMMIT" :
			 rec_type == WAL_REC_RELATION ? "RELATION" :
			 rec_type == WAL_REC_O_TABLES_META_LOCK ? "META LOCK" :
			 rec_type == WAL_REC_O_TABLES_META_UNLOCK ? "META_UNLOCK" :
			 rec_type == WAL_REC_TRUNCATE ? "TRUNCATE" :
			 rec_type == WAL_REC_SAVEPOINT ? "SAVEPOINT" :
			 rec_type == WAL_REC_ROLLBACK_TO_SAVEPOINT ? "ROLLBACK TO SAVEPOINT" :
			 rec_type == WAL_REC_INSERT ? "INSERT" :
			 rec_type == WAL_REC_UPDATE ? "UPDATE" :
			 rec_type == WAL_REC_DELETE ? "DELETE" : "_UNKNOWN");

		if (rec_type == WAL_REC_XID)
		{
			CSNSnapshotData *csnSnapshot;

			memcpy(&oxid, ptr, sizeof(oxid));
			ptr += sizeof(oxid);

			memcpy(&logicalXid, ptr, sizeof(TransactionId));
			ptr += sizeof(TransactionId);

			csnSnapshot = SnapBuildGetCSNSnaphot(ctx->snapshot_builder);
			csnSnapshot->nextXid = Max(csnSnapshot->nextXid, oxid);
		}
		else if (rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK)
		{
			OXid		xmin;
			dlist_iter	cur_txn_i;
			ReorderBufferTXN *txn;
			CommitSeqNo csn;
			CSNSnapshotData *csnSnapshot;

			memcpy(&xmin, ptr, sizeof(xmin));
			ptr += sizeof(xmin);
			memcpy(&csn, ptr, sizeof(csn));
			ptr += sizeof(csn);

			csnSnapshot = SnapBuildGetCSNSnaphot(ctx->snapshot_builder);
			csnSnapshot->snapshotcsn = csn;
			csnSnapshot->xlogptr = endXLogPtr;
			csnSnapshot->xmin = xmin;

			if (!set_snapshot(ctx, logicalXid, changeXLogPtr))
				continue;

			txn = get_reorder_buffer_txn(ctx->reorder, logicalXid);

			/* unknown transaction, nothing to replay */
			if (txn != NULL)
			{
				if (txn->toptxn)
					txn = txn->toptxn;

				dlist_foreach(cur_txn_i, &txn->subtxns)
				{
					ReorderBufferTXN *cur_txn;

					cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);

					ReorderBufferCommitChild(ctx->reorder, txn->xid, cur_txn->xid,
											 startXLogPtr, endXLogPtr);

				}

				/*
				 * SnapBuildXactNeedsSkip() does strict comparison.  So,
				 * subtract endXLogPtr by one to fit snapshot just taken after
				 * commit.
				 */
				if (rec_type == WAL_REC_COMMIT &&
					!SnapBuildXactNeedsSkip(ctx->snapshot_builder, endXLogPtr - 1))
				{
					ReorderBufferCommit(ctx->reorder, logicalXid,
										startXLogPtr, endXLogPtr,
										0, XLogRecGetOrigin(buf->record),
										buf->origptr);
				}
				else
				{
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

			memcpy(&xid, ptr, sizeof(xid));
			ptr += sizeof(xid);
			memcpy(&xmin, ptr, sizeof(xmin));
			ptr += sizeof(xmin);
			memcpy(&csn, ptr, sizeof(csn));
			ptr += sizeof(csn);

			csnSnapshot = SnapBuildGetCSNSnaphot(ctx->snapshot_builder);
			csnSnapshot->snapshotcsn = csn;
			csnSnapshot->xlogptr = endXLogPtr;
			csnSnapshot->xmin = xmin;

			if (!set_snapshot(ctx, logicalXid, changeXLogPtr))
				continue;

			if (!SnapBuildXactNeedsSkip(ctx->snapshot_builder, endXLogPtr))
			{
				ReorderBufferCommit(ctx->reorder, logicalXid,
									startXLogPtr, endXLogPtr,
									0, XLogRecGetOrigin(buf->record),
									buf->origptr);
			}
			else
			{
				ReorderBufferAbort(ctx->reorder, logicalXid,
								   startXLogPtr, 0);
			}
			UpdateDecodingStats(ctx);

			oxid = InvalidOXid;
			logicalXid = InvalidTransactionId;
		}
		else if (rec_type == WAL_REC_RELATION)
		{
			ix_type = *ptr;
			ptr++;
			memcpy(&cur_oids.datoid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&cur_oids.reloid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&cur_oids.relnode, ptr, sizeof(Oid));
			ptr += sizeof(Oid);

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
			}
			else if (ix_type == oIndexToast)
			{
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

			memcpy(&oids.datoid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oids.reloid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oldRelnode, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oids.relnode, ptr, sizeof(Oid));
			ptr += sizeof(Oid);

			/* Skip */
		}
		else if (rec_type == WAL_REC_TRUNCATE)
		{
			ORelOids	oids;

			memcpy(&oids.datoid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oids.reloid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oids.relnode, ptr, sizeof(Oid));
			ptr += sizeof(Oid);

			/* Skip */
		}
		else if (rec_type == WAL_REC_SAVEPOINT)
		{
			SubTransactionId parentSubid;
			TransactionId parentLogicalXid;

			memcpy(&parentSubid, ptr, sizeof(SubTransactionId));
			ptr += sizeof(SubTransactionId);
			memcpy(&logicalXid, ptr, sizeof(TransactionId));
			ptr += sizeof(TransactionId);
			memcpy(&parentLogicalXid, ptr, sizeof(TransactionId));
			ptr += sizeof(TransactionId);

			ReorderBufferAssignChild(ctx->reorder, parentLogicalXid, logicalXid, buf->origptr);

			/* Skip */
		}
		else if (rec_type == WAL_REC_ROLLBACK_TO_SAVEPOINT)
		{
			SubTransactionId parentSubid;

			memcpy(&parentSubid, ptr, sizeof(SubTransactionId));
			ptr += sizeof(SubTransactionId);

			/* Skip */
		}
		else
		{
			OFixedTuple tuple;
			ReorderBufferChange *change;

			Assert(rec_type == WAL_REC_INSERT || rec_type == WAL_REC_UPDATE || rec_type == WAL_REC_DELETE);

			ReorderBufferProcessXid(ctx->reorder, logicalXid, changeXLogPtr);

			tuple.tuple.formatFlags = *ptr;
			ptr++;

			memcpy(&length, ptr, sizeof(OffsetNumber));
			ptr += sizeof(OffsetNumber);

			if (SnapBuildCurrentState(ctx->snapshot_builder) < SNAPBUILD_FULL_SNAPSHOT)
			{
				ptr += length;
				continue;
			}

			(void) set_snapshot(ctx, logicalXid, changeXLogPtr);

			if ((ix_type == oIndexInvalid || ix_type == oIndexToast) &&
				cur_oids.datoid == ctx->slot->data.database)
			{
				Assert(descr != NULL);
				memcpy(tuple.fixedData, ptr, length);
				tuple.tuple.data = tuple.fixedData;

				if (rec_type == WAL_REC_INSERT)
				{

					change = ReorderBufferGetChange(ctx->reorder);
					change->action = REORDER_BUFFER_CHANGE_INSERT;
					change->data.tp.rlocator.spcOid = DEFAULTTABLESPACE_OID;
					change->data.tp.rlocator.dbOid = cur_oids.datoid;
					change->data.tp.rlocator.relNumber = cur_oids.relnode;

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

						Assert(o_toast_tupDesc);
						pk_natts = o_toast_tupDesc->natts - TOAST_LEAF_FIELDS_NUM;
						attnum = (uint16) o_fastgetattr(tuple.tuple, pk_natts + ATTN_POS, o_toast_tupDesc, &indexDescr->leafSpec, &attnum_isnull);
						chunk_seq = (uint32) o_fastgetattr(tuple.tuple, pk_natts + CHUNKN_POS, o_toast_tupDesc, &indexDescr->leafSpec, &chunk_seq_isnull);
						Assert((!attnum_isnull) && (!chunk_seq_isnull));
						Assert(attnum > 0);

						/* Toast chunk in VARATT_4B uncompressed format */
						old_chunk = o_fastgetattr_ptr(tuple.tuple, pk_natts + DATA_POS, o_toast_tupDesc, &indexDescr->leafSpec);
						old_chunk_size = VARSIZE(old_chunk) - VARHDRSZ;

						/*
						 * ReorderBufferToastReplace() expects first toasted
						 * chunk without the header of the whole toasted value
						 * and reconstructs it itself. Cut this extra header
						 * from OrioleDB toasted chunk.
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
						elog(DEBUG4, "reloid: %u (attnum, seq, oldsize, newsize): (%u, %u, %u, %u) length_get: %u pk_natts: %u",
							 cur_oids.reloid, attnum,
							 chunk_seq,
							 old_chunk_size, new_chunk_size,
							 length, pk_natts);

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
						 * Primary table contains TOASTed attributes needs
						 * conversion of them
						 */
						if (descr->ntoastable > 0)
						{
							HeapTuple	newheaptuple;

							newheaptuple = convert_toast_pointers(descr, indexDescr, &tuple);
							change->data.tp.newtuple = record_buffer_tuple(ctx->reorder, newheaptuple, true);
							Assert(change->data.tp.newtuple);
						}
						else	/* Tuple without TOASTed attrs */
						{
							tts_orioledb_store_tuple(descr->newTuple, tuple.tuple,
													 descr, COMMITSEQNO_INPROGRESS,
													 PrimaryIndexNumber, false,
													 NULL);
							change->data.tp.newtuple = record_buffer_tuple_slot(ctx->reorder, descr->newTuple);
						}
						change->data.tp.clear_toast_afterwards = true;

					}

					ReorderBufferQueueChange(ctx->reorder, logicalXid,
											 changeXLogPtr,
											 change, (ix_type == oIndexToast));
				}
				else if (rec_type == WAL_REC_UPDATE)
				{
					Assert(ix_type != oIndexToast);

					change = ReorderBufferGetChange(ctx->reorder);
					change->action = REORDER_BUFFER_CHANGE_UPDATE;
					change->data.tp.clear_toast_afterwards = true;
					change->data.tp.rlocator.spcOid = DEFAULTTABLESPACE_OID;
					change->data.tp.rlocator.dbOid = cur_oids.datoid;
					change->data.tp.rlocator.relNumber = cur_oids.relnode;

					elog(DEBUG4, "reloid: %u", cur_oids.reloid);

					/*
					 * Primary table contains TOASTed attributes needs
					 * conversion of them
					 */
					if (descr->ntoastable > 0)
					{
						HeapTuple	newheaptuple;

						newheaptuple = convert_toast_pointers(descr, indexDescr, &tuple);
						ExecForceStoreHeapTuple(newheaptuple, descr->newTuple, false);
						change->data.tp.newtuple = record_buffer_tuple(ctx->reorder, newheaptuple, true);
					}
					else		/* Tuple without TOASTed attrs */
					{
						tts_orioledb_store_tuple(descr->newTuple, tuple.tuple,
												 descr, COMMITSEQNO_INPROGRESS,
												 PrimaryIndexNumber, false,
												 NULL);

						change->data.tp.newtuple = record_buffer_tuple_slot(ctx->reorder, descr->newTuple);
					}
					tts_copy_identity(descr->newTuple, descr->oldTuple,
									  GET_PRIMARY(descr));
					change->data.tp.oldtuple = record_buffer_tuple_slot(ctx->reorder, descr->oldTuple);

					ReorderBufferQueueChange(ctx->reorder, logicalXid,
											 changeXLogPtr,
											 change, false);
				}
				else if (rec_type == WAL_REC_DELETE)
				{
					change = ReorderBufferGetChange(ctx->reorder);
					change->action = REORDER_BUFFER_CHANGE_DELETE;
					change->data.tp.rlocator.spcOid = DEFAULTTABLESPACE_OID;
					change->data.tp.rlocator.dbOid = cur_oids.datoid;
					change->data.tp.rlocator.relNumber = cur_oids.relnode;
					elog(DEBUG4, "reloid: %u", cur_oids.reloid);
					if (ix_type == oIndexToast)
					{
						change->data.tp.clear_toast_afterwards = false;
						tts_orioledb_store_non_leaf_tuple(descr->oldTuple, tuple.tuple,
														  descr, COMMITSEQNO_INPROGRESS,
														  PrimaryIndexNumber, false,
														  NULL);
						change->data.tp.oldtuple = record_buffer_tuple_slot(ctx->reorder, descr->oldTuple);
					}
					else
					{
						change->data.tp.clear_toast_afterwards = true;

						/*
						 * Primary table contains TOASTed attributes needs
						 * conversion of them
						 */
						if (descr->ntoastable > 0)
						{
							HeapTuple	oldheaptuple;

							oldheaptuple = convert_toast_pointers(descr, indexDescr, &tuple);
							change->data.tp.oldtuple = record_buffer_tuple(ctx->reorder, oldheaptuple, true);
						}
						else	/* Tuple without TOASTed attrs */
						{
							tts_orioledb_store_non_leaf_tuple(descr->oldTuple, tuple.tuple,
															  descr, COMMITSEQNO_INPROGRESS,
															  PrimaryIndexNumber, false,
															  NULL);
							change->data.tp.oldtuple = record_buffer_tuple_slot(ctx->reorder, descr->oldTuple);

						}
					}
					ReorderBufferQueueChange(ctx->reorder, logicalXid,
											 changeXLogPtr,
											 change, (ix_type == oIndexToast));
				}
			}

			ptr += length;
		}
	}
}
