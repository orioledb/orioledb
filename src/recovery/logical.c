/*-------------------------------------------------------------------------
 *
 * recovery.c
 *		Support for logical decoding of OrioleDB tables.
 *
 * Copyright (c) 2024, Oriole DB Inc.
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

		attnum = idx->fields[i].tableAttnum - 1;
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
}

static ReorderBufferTupleBuf *
record_buffer_tuple(ReorderBuffer *reorder, TupleTableSlot *slot)
{
	HeapTuple	htup,
				changeTup;
	bool		freeHtup;
	ReorderBufferTupleBuf *result;

	htup = ExecFetchSlotHeapTuple(slot, false, &freeHtup);
	result = ReorderBufferGetTupleBuf(reorder, htup->t_len);

	changeTup = &result->tuple;
	changeTup->t_tableOid = InvalidOid;
	changeTup->t_len = htup->t_len;
	changeTup->t_self = htup->t_self;
	memcpy(changeTup->t_data, htup->t_data, htup->t_len);
	if (freeHtup)
		heap_freetuple(htup);

	return result;
}

/*
 * Handle OrioleDB records for LogicalDecodingProcessRecord().
 */
void
orioledb_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	XLogReaderState *record = buf->record;
	Pointer		startPtr = (Pointer) XLogRecGetData(record);
	Pointer		endPtr = startPtr + XLogRecGetDataLen(record);
	Pointer		ptr = startPtr;
	OTableDescr *descr = NULL;

	/* OIndexDescr *indexDescr = NULL; */
	int			sys_tree_num = -1;
	ORelOids	cur_oids = {0, 0, 0};
	OXid		oxid = InvalidOXid;
	TransactionId logicalXid = InvalidTransactionId;
	uint8		rec_type;
	OffsetNumber length;
	OIndexType	ix_type = oIndexInvalid;

	while (ptr < endPtr)
	{

		rec_type = *ptr;
		ptr++;

		if (rec_type == WAL_REC_XID)
		{
			memcpy(&oxid, ptr, sizeof(oxid));
			ptr += sizeof(oxid);

			memcpy(&logicalXid, ptr, sizeof(TransactionId));
			ptr += sizeof(TransactionId);


		}
		else if (rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK)
		{
			OXid		xmin;
#if PG_VERSION_NUM >= 160000
			Snapshot	snap = SnapBuildGetOrBuildSnapshot(ctx->snapshot_builder);
#else
			Snapshot	snap = SnapBuildGetOrBuildSnapshot(ctx->snapshot_builder, InvalidTransactionId);
#endif

			memcpy(&xmin, ptr, sizeof(xmin));
			ptr += sizeof(xmin);

			ReorderBufferSetBaseSnapshot(ctx->reorder, logicalXid,
										 buf->origptr + (ptr - startPtr),
										 snap);
			snap->active_count++;

			ReorderBufferCommit(ctx->reorder, logicalXid, buf->origptr, buf->endptr,
								0, InvalidRepOriginId, buf->origptr + (ptr - startPtr));
			UpdateDecodingStats(ctx);

			oxid = InvalidOXid;
			logicalXid = InvalidTransactionId;
		}
		else if (rec_type == WAL_REC_JOINT_COMMIT)
		{
			TransactionId xid;
			OXid		xmin;
#if PG_VERSION_NUM >= 160000
			Snapshot	snap = SnapBuildGetOrBuildSnapshot(ctx->snapshot_builder);
#else
			Snapshot	snap = SnapBuildGetOrBuildSnapshot(ctx->snapshot_builder, InvalidTransactionId);
#endif

			memcpy(&xid, ptr, sizeof(xid));
			ptr += sizeof(xid);
			memcpy(&xmin, ptr, sizeof(xmin));
			ptr += sizeof(xmin);

			ReorderBufferSetBaseSnapshot(ctx->reorder, logicalXid,
										 buf->origptr + (ptr - startPtr),
										 snap);
			snap->active_count++;

			ReorderBufferCommit(ctx->reorder, logicalXid, buf->origptr, buf->endptr,
								0, InvalidRepOriginId, buf->origptr + (ptr - startPtr));
			UpdateDecodingStats(ctx);
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
				/* indexDescr = descr ? GET_PRIMARY(descr) : NULL; */
			}
			else
			{
				Assert(ix_type == oIndexToast);
				descr = NULL;

				/*
				 * indexDescr = o_fetch_index_descr(cur_oids, ix_type, false,
				 * NULL);
				 */
			}
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

			memcpy(&parentSubid, ptr, sizeof(SubTransactionId));
			ptr += sizeof(SubTransactionId);

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

			ReorderBufferProcessXid(ctx->reorder, logicalXid, buf->origptr + (ptr - startPtr));

			tuple.tuple.formatFlags = *ptr;
			ptr++;

			memcpy(&length, ptr, sizeof(OffsetNumber));
			ptr += sizeof(OffsetNumber);

			if (ix_type == oIndexInvalid &&
				cur_oids.datoid == ctx->slot->data.database)
			{
				Assert(descr != NULL);
				memcpy(tuple.fixedData, ptr, length);
				tuple.tuple.data = tuple.fixedData;

				if (rec_type == WAL_REC_INSERT)
				{
					tts_orioledb_store_tuple(descr->newTuple, tuple.tuple,
											 descr, COMMITSEQNO_INPROGRESS,
											 PrimaryIndexNumber, false,
											 NULL);

					change = ReorderBufferGetChange(ctx->reorder);
					change->action = REORDER_BUFFER_CHANGE_INSERT;
					change->data.tp.newtuple = record_buffer_tuple(ctx->reorder, descr->newTuple);
					change->data.tp.clear_toast_afterwards = true;
#if PG_VERSION_NUM >= 160000
					change->data.tp.rlocator.spcOid = DEFAULTTABLESPACE_OID;
					change->data.tp.rlocator.dbOid = cur_oids.datoid;
					change->data.tp.rlocator.relNumber = cur_oids.relnode;
#else
					change->data.tp.relnode.spcNode = DEFAULTTABLESPACE_OID;
					change->data.tp.relnode.dbNode = cur_oids.datoid;
					change->data.tp.relnode.relNode = cur_oids.relnode;
#endif

					ReorderBufferQueueChange(ctx->reorder, logicalXid,
											 buf->origptr + (ptr - startPtr),
											 change, false);

				}
				else if (rec_type == WAL_REC_UPDATE)
				{
					tts_orioledb_store_tuple(descr->newTuple, tuple.tuple,
											 descr, COMMITSEQNO_INPROGRESS,
											 PrimaryIndexNumber, false,
											 NULL);
					tts_copy_identity(descr->newTuple, descr->oldTuple,
									  GET_PRIMARY(descr));

					change = ReorderBufferGetChange(ctx->reorder);
					change->action = REORDER_BUFFER_CHANGE_UPDATE;
					change->data.tp.oldtuple = record_buffer_tuple(ctx->reorder, descr->oldTuple);
					change->data.tp.newtuple = record_buffer_tuple(ctx->reorder, descr->newTuple);
					change->data.tp.clear_toast_afterwards = true;
#if PG_VERSION_NUM >= 160000
					change->data.tp.rlocator.spcOid = DEFAULTTABLESPACE_OID;
					change->data.tp.rlocator.dbOid = cur_oids.datoid;
					change->data.tp.rlocator.relNumber = cur_oids.relnode;
#else
					change->data.tp.relnode.spcNode = DEFAULTTABLESPACE_OID;
					change->data.tp.relnode.dbNode = cur_oids.datoid;
					change->data.tp.relnode.relNode = cur_oids.relnode;
#endif

					ReorderBufferQueueChange(ctx->reorder, logicalXid,
											 buf->origptr + (ptr - startPtr),
											 change, false);


				}
				else if (rec_type == WAL_REC_DELETE)
				{
					tts_orioledb_store_non_leaf_tuple(descr->oldTuple, tuple.tuple,
													  descr, COMMITSEQNO_INPROGRESS,
													  PrimaryIndexNumber, false,
													  NULL);

					change = ReorderBufferGetChange(ctx->reorder);
					change->action = REORDER_BUFFER_CHANGE_DELETE;
					change->data.tp.oldtuple = record_buffer_tuple(ctx->reorder, descr->oldTuple);
					change->data.tp.clear_toast_afterwards = true;
#if PG_VERSION_NUM >= 160000
					change->data.tp.rlocator.spcOid = DEFAULTTABLESPACE_OID;
					change->data.tp.rlocator.dbOid = cur_oids.datoid;
					change->data.tp.rlocator.relNumber = cur_oids.relnode;
#else
					change->data.tp.relnode.spcNode = DEFAULTTABLESPACE_OID;
					change->data.tp.relnode.dbNode = cur_oids.datoid;
					change->data.tp.relnode.relNode = cur_oids.relnode;
#endif

					ReorderBufferQueueChange(ctx->reorder, logicalXid,
											 buf->origptr + (ptr - startPtr),
											 change, false);
				}
			}

			ptr += length;
		}
	}
}
