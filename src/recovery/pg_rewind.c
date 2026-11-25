/*-------------------------------------------------------------------------
 *
 * rewind.c
 *		Routines for pg_rewind support.
 *
 * Copyright (c) 2017-2021, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/pg_rewind.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "orioledb.h"

#include "btree/io.h"
#include "btree/iterator.h"
#include "btree/modify.h"
#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "checkpoint/control.h"
#include "recovery/internal.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/operations.h"
#include "tuple/slot.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/timeline.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/pg_bswap.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "utils/pg_lsn.h"
#include "utils/stopevent.h"

#define O_REWIND_FOUND '\1'
#define O_REWIND_NOT_FOUND '\0'

PG_FUNCTION_INFO_V1(orioledb_pg_rewind_sorted_keys);
PG_FUNCTION_INFO_V1(orioledb_pg_rewind_new_row_versions);

typedef struct KeyArrayElement
{
	uint8		tupleFormatFlags;
	uint32		dataLength;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} KeyArrayElement;

typedef struct IteratorStackItem
{
	OXid		oxid;
	uint8		deleted;
	uint8		formatFlags;
	uint32		len;
	char	   *data;
}			IteratorStackItem;

typedef struct TableRow
{
	ORelOids	oids;
	OIndexType	ix_type;
	uint32		nkeys;
	bytea	   *keys;
} TableRow;

typedef struct KeyIterator
{
	OTuple		new_tup;
	OTuple		first_key_tup;
	OTuple		last_key_tup;
	uint8		last_deleted;
	BTreeIterator *iter;
} KeyIterator;

static BTreeDescr *cmp_tree_desc;

static void
appendHton16StringInfo(StringInfo str, uint16 data)
{
	uint16		u16data = pg_hton16(data);

	appendBinaryStringInfoNT(str, (const char *) &u16data, sizeof(u16data));
}

static void
appendHton32StringInfo(StringInfo str, uint32 data)
{
	uint32		u32data = pg_hton32(data);

	appendBinaryStringInfoNT(str, (const char *) &u32data, sizeof(u32data));
}

static void
appendHton64StringInfo(StringInfo str, uint64 data)
{
	uint64		u64data = pg_hton64(data);

	appendBinaryStringInfoNT(str, (const char *) &u64data, sizeof(u64data));
}

static void
appendOidStringInfo(StringInfo str, Oid oid)
{
	appendHton32StringInfo(str, sizeof(Oid));
	appendHton32StringInfo(str, oid);
}

static void
append_tuple(StringInfo str, uint8 formatFlags, Pointer data,
			 uint32 tup_len, uint8 *deleted)
{
	if (deleted)
		appendStringInfoChar(str, *deleted);
	appendStringInfoChar(str, formatFlags);
	appendHton32StringInfo(str, tup_len);
	appendBinaryStringInfoNT(str, data, tup_len);
}

static int
key_array_cmp(const void *p1, const void *p2)
{
	KeyArrayElement *key1 = *(KeyArrayElement **) p1;
	KeyArrayElement *key2 = *(KeyArrayElement **) p2;
	OTuple		key_tup1 = {.formatFlags = key1->tupleFormatFlags,
	.data = key1->data};
	OTuple		key_tup2 = {.formatFlags = key2->tupleFormatFlags,
	.data = key2->data};

	return o_btree_cmp(cmp_tree_desc,
					   (void *) &key_tup1, BTreeKeyNonLeafKey,
					   (void *) &key_tup2, BTreeKeyNonLeafKey);
}

static TableRow *
table_next_row(Relation rel, TableScanDesc scan)
{
	HeapTuple	htuple;
	static TableRow row;
	TableRow   *result = NULL;

	htuple = heap_getnext(scan, ForwardScanDirection);
	if (htuple)
	{
		bool		isnull;
		Datum		attr;

		result = &row;
		attr = heap_getattr(htuple, 1, rel->rd_att, &isnull);
		result->ix_type = DatumGetInt32(attr);

		attr = heap_getattr(htuple, 2, rel->rd_att, &isnull);
		result->oids.datoid = DatumGetObjectId(attr);

		attr = heap_getattr(htuple, 3, rel->rd_att, &isnull);
		result->oids.reloid = DatumGetObjectId(attr);

		attr = heap_getattr(htuple, 4, rel->rd_att, &isnull);
		result->oids.relnode = DatumGetObjectId(attr);

		attr = heap_getattr(htuple, 5, rel->rd_att, &isnull);
		result->oids.spcoid = DatumGetObjectId(attr);

		attr = heap_getattr(htuple, 6, rel->rd_att, &isnull);
		result->nkeys = DatumGetUInt32(attr);

		attr = heap_getattr(htuple, 7, rel->rd_att, &isnull);
		result->keys = DatumGetByteaP(attr);
	}

	return result;
}

static int
fill_descrs(ORelOids oids, OIndexType ix_type,
			OTableDescr **descr, OIndexDescr **indexDescr)
{
	int			sys_tree_num = -1;

	if (IS_SYS_TREE_OIDS(oids))
		sys_tree_num = oids.relnode;

	if (sys_tree_num > 0)
	{
		*descr = NULL;
		Assert(sys_tree_get_storage_type(sys_tree_num) ==
			   BTreeStoragePersistence);
	}
	else if (ix_type == oIndexInvalid)
	{
		*descr = o_fetch_table_descr(oids);
		if (*descr)
			*indexDescr = GET_PRIMARY(*descr);
	}
	else
	{
		*descr = NULL;
		*indexDescr = o_fetch_index_descr(oids, ix_type,
										  false, NULL);
	}

	return sys_tree_num;
}

static KeyArrayElement **
create_key_array(uint32 nkeys, size_t extension_size, char **ptr)
{
	int			i;
	KeyArrayElement **key_array;

	key_array = palloc0(sizeof(KeyArrayElement *) * nkeys);
	for (i = 0; i < nkeys; i++)
	{
		uint32		keyLength;
		uint8		tupleFormatFlags;
		uint32		u32data;

		memcpy(&tupleFormatFlags, *ptr, sizeof(uint8));
		*ptr += sizeof(uint8);

		memcpy(&u32data, *ptr, sizeof(uint32));
		keyLength = pg_ntoh32(u32data);
		*ptr += sizeof(uint32);

		keyLength += extension_size;

		key_array[i] = palloc0(offsetof(KeyArrayElement, data) +
							   keyLength);
		key_array[i]->tupleFormatFlags = tupleFormatFlags;
		key_array[i]->dataLength = keyLength;
		memcpy(&key_array[i]->data, *ptr, keyLength);
		*ptr += keyLength;
	}

	return key_array;
}

static uint32
sort_key_array(KeyArrayElement **key_array, uint32 nkeys)
{
	uint32		i,
				dst,
				new_nkeys;

	pg_qsort(key_array, nkeys, sizeof(KeyArrayElement *),
			 key_array_cmp);

	/* Remove duplicates */
	dst = 0;
	for (i = 1; i < nkeys; i++)
	{
		if (key_array_cmp(&key_array[dst], &key_array[i]) != 0)
		{
			dst++;
			key_array[dst] = key_array[i];
		}
		else
		{
			pfree(key_array[i]);
		}
	}
	new_nkeys = dst + 1;

	return new_nkeys;
}

Datum
orioledb_pg_rewind_sorted_keys(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	Relation	rel;
	TableScanDesc scan;
	TableRow   *row;
	bytea	   *result;
	StringInfo	result_str;

	rel = table_open(reloid, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	result_str = makeStringInfo();

	while ((row = table_next_row(rel, scan)) != NULL)
	{
		OTableDescr *descr = NULL;
		OIndexDescr *indexDescr = NULL;
		int			sys_tree_num;

		sys_tree_num = fill_descrs(row->oids, row->ix_type,
								   &descr, &indexDescr);

		if ((indexDescr) || (sys_tree_num > 0))
		{
			int			i;
			char	   *keys_data_start = VARDATA(row->keys);
			char	   *ptr = keys_data_start;
			KeyArrayElement **key_array;
			StringInfo	keys_str;
			uint32		new_nkeys;

			key_array = create_key_array(row->nkeys, 0, &ptr);
			Assert((ptr - keys_data_start) == (VARSIZE(row->keys) - VARHDRSZ));
			if (sys_tree_num > 0)
				cmp_tree_desc = get_sys_tree(sys_tree_num);
			else
				cmp_tree_desc = &indexDescr->desc;

			new_nkeys = sort_key_array(key_array, row->nkeys);

			appendHton16StringInfo(result_str, 7);
			appendHton32StringInfo(result_str, sizeof(row->ix_type));
			appendHton32StringInfo(result_str, row->ix_type);
			appendOidStringInfo(result_str, row->oids.datoid);
			appendOidStringInfo(result_str, row->oids.reloid);
			appendOidStringInfo(result_str, row->oids.relnode);
			appendOidStringInfo(result_str, row->oids.spcoid);
			appendHton32StringInfo(result_str, sizeof(new_nkeys));
			appendHton32StringInfo(result_str, new_nkeys);

			keys_str = makeStringInfo();
			for (i = 0; i < new_nkeys; i++)
			{
				append_tuple(keys_str, key_array[i]->tupleFormatFlags,
							 key_array[i]->data, key_array[i]->dataLength,
							 NULL);

				if (sys_tree_num == SYS_TREES_O_INDICES)
				{
					OIndexChunkKey *ix_key = (OIndexChunkKey *)
						key_array[i]->data;
					OIndexDescr *id;
					uint8		index_exist;

					id = o_fetch_index_descr(ix_key->oids, ix_key->type,
											 false, NULL);
					index_exist = id != NULL;
					appendBinaryStringInfoNT(keys_str, (Pointer) &index_exist,
											 sizeof(uint8));
				}

				pfree(key_array[i]);
			}
			pfree(key_array);

			appendHton32StringInfo(result_str, keys_str->len);
			appendBinaryStringInfoNT(result_str, keys_str->data,
									 keys_str->len);
			pfree(keys_str->data);
			pfree(keys_str);
		}
	}
	table_endscan(scan);
	table_close(rel, AccessShareLock);

	result = (bytea *) palloc(VARHDRSZ + result_str->len);
	SET_VARSIZE(result, VARHDRSZ + result_str->len);
	memcpy(VARDATA(result), result_str->data, result_str->len);
	pfree(result_str->data);
	pfree(result_str);
	PG_RETURN_BYTEA_P(result);
}

static TupleFetchCallbackResult
get_tup_oxid_callback(OTuple tuple, OXid tupOxid, OSnapshot *oSnapshot,
					  bool deleted, void *arg, bool oxidIsFinished)
{
	uint8	   *deleted_result = arg;

	*deleted_result = deleted;

	if (!(COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn) &&
		  tupOxid == get_current_oxid_if_any()))
		return OTupleFetchNext;

	return OTupleFetchMatch;
}

static void
append_revived_tree(StringInfo str, OIndexChunkKey *ix_key)
{
	OIndexDescr *ix_descr = NULL;
	BTreeIterator *iter;
	uint8		deleted = false;
	OTuple		tup;
	uint32		ix_nitems;
	StringInfo	rows_str;

	ix_descr = o_fetch_index_descr(ix_key->oids, ix_key->type, false, NULL);
	o_btree_load_shmem(&ix_descr->desc);

	iter = o_btree_iterator_create(&ix_descr->desc, NULL, BTreeKeyNone,
								   &o_non_deleted_snapshot,
								   ForwardScanDirection);

	o_btree_iterator_set_callback(iter, get_tup_oxid_callback, &deleted);
	ix_nitems = 0;
	tup = o_btree_iterator_fetch(iter, NULL, NULL, BTreeKeyNone, true, NULL);
	rows_str = makeStringInfo();
	while (!O_TUPLE_IS_NULL(tup))
	{
		int			tup_len = o_btree_len(&ix_descr->desc, tup, OTupleLength);

		ix_nitems++;
		append_tuple(rows_str, tup.formatFlags, tup.data, tup_len, &deleted);
		deleted = false;
		tup = o_btree_iterator_fetch(iter, NULL, NULL,
									 BTreeKeyNone, true, NULL);
	}
	appendHton32StringInfo(str, ix_nitems);
	appendBinaryStringInfoNT(str, rows_str->data, rows_str->len);
	btree_iterator_free(iter);
	pfree(rows_str->data);
	pfree(rows_str);
}

static KeyIterator *
create_key_iterator(BTreeDescr *td,
					KeyArrayElement *first,
					KeyArrayElement *last)
{
	KeyIterator *result = palloc0(sizeof(KeyIterator));
	OSnapshot	committed_snapshot = {
		pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo),
		InvalidXLogRecPtr, 0, 0
	};

	result->last_deleted = false;
	O_TUPLE_SET_NULL(result->new_tup);
	result->first_key_tup.formatFlags = first->tupleFormatFlags;
	result->first_key_tup.data = first->data;
	result->last_key_tup.formatFlags = last->tupleFormatFlags;
	result->last_key_tup.data = last->data;

	result->iter = o_btree_iterator_create(td, &result->first_key_tup,
										   BTreeKeyNonLeafKey,
										   &committed_snapshot,
										   ForwardScanDirection);
	o_btree_iterator_set_callback(result->iter, get_tup_oxid_callback,
								  &result->last_deleted);
	return result;
}

static void
key_iterator_iterate(KeyIterator *it)
{
	it->last_deleted = false;
	if (!O_TUPLE_IS_NULL(it->new_tup))
		pfree(it->new_tup.data);
	O_TUPLE_SET_NULL(it->new_tup);
	it->new_tup = o_btree_iterator_fetch(it->iter, NULL, &it->last_key_tup,
										 BTreeKeyNonLeafKey, true, NULL);
}

static void
free_key_iterator(KeyIterator *it)
{
	btree_iterator_free(it->iter);
	pfree(it);
}

static void
process_key(StringInfo str, KeyArrayElement *old_key,
			KeyIterator *it, BTreeDescr *td)
{
	bool		found = false;
	OTuple		old_tup = {.data = old_key->data,
	.formatFlags = old_key->tupleFormatFlags};
	int			cmp = -1;

	if (O_TUPLE_IS_NULL(it->new_tup))
		key_iterator_iterate(it);

	if (!O_TUPLE_IS_NULL(it->new_tup))
		cmp = o_btree_cmp(td, (void *) &old_tup, BTreeKeyNonLeafKey,
						  (void *) &it->new_tup, BTreeKeyLeafTuple);

	while (!O_TUPLE_IS_NULL(it->new_tup) && (cmp > 0))
	{
		key_iterator_iterate(it);
		if (!O_TUPLE_IS_NULL(it->new_tup))
			cmp = o_btree_cmp(td, (void *) &old_tup, BTreeKeyNonLeafKey,
							  (void *) &it->new_tup, BTreeKeyLeafTuple);
	}
	found = cmp == 0;

	if (found)
	{
		int			tup_len = o_btree_len(td, it->new_tup, OTupleLength);

		appendStringInfoChar(str, O_REWIND_FOUND);
		append_tuple(str, it->new_tup.formatFlags, it->new_tup.data, tup_len,
					 &it->last_deleted);

		if (IS_SYS_TREE_OIDS(td->oids) &&
			td->oids.reloid == SYS_TREES_O_INDICES)
		{
			Pointer		found_ptr = old_key->data + old_key->dataLength -
				sizeof(uint8);
			uint8		target_found = *(uint8 *) found_ptr;

			if (target_found)
				appendHton32StringInfo(str, 0);
			else
				append_revived_tree(str, (OIndexChunkKey *) old_key->data);
		}
		key_iterator_iterate(it);
	}
	else
	{
		uint8		deleted = true;

		appendStringInfoChar(str, O_REWIND_NOT_FOUND);
		append_tuple(str, old_key->tupleFormatFlags,
					 old_key->data, old_key->dataLength, &deleted);
	}
}

static void
process_tree(StringInfo str, TableRow *row)
{
	int			i;
	char	   *keys_data_start = VARDATA(row->keys);
	char	   *ptr = keys_data_start;
	KeyArrayElement **key_array;
	KeyIterator *key_iter;
	BTreeDescr *td;
	OTableDescr *descr = NULL;
	OIndexDescr *indexDescr = NULL;
	int			sys_tree_num = -1;

	sys_tree_num = fill_descrs(row->oids, row->ix_type,
							   &descr, &indexDescr);

	if (indexDescr || (sys_tree_num > 0))
	{
		/* orioledb_pg_rewind_sorted_keys adds additional */
		/* uint8 field for SYS_TREES_O_INDICES tree, which specifies that */
		/* tree was dropped on target so full tree rewind needed */
		key_array = create_key_array(row->nkeys,
									 sys_tree_num == SYS_TREES_O_INDICES ?
									 sizeof(uint8) : 0,
									 &ptr);

		Assert((ptr - keys_data_start) == (VARSIZE(row->keys) - VARHDRSZ));

		appendStringInfoChar(str, O_REWIND_FOUND);
		appendHton32StringInfo(str, row->nkeys);

		td = sys_tree_num > 0 ? get_sys_tree(sys_tree_num) : &indexDescr->desc;

		o_btree_load_shmem(td);

		key_iter = create_key_iterator(td, key_array[0],
									   key_array[row->nkeys - 1]);
		for (i = 0; i < row->nkeys; i++)
		{
			process_key(str, key_array[i], key_iter, td);
			pfree(key_array[i]);
		}
		pfree(key_array);
		free_key_iterator(key_iter);
	}
	else
	{
		appendStringInfoChar(str, O_REWIND_NOT_FOUND);
	}
}

Datum
orioledb_pg_rewind_new_row_versions(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	Relation	rel;
	TableScanDesc scan;
	TableRow   *row;
	bytea	   *result;
	StringInfo	result_str;

	rel = table_open(reloid, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	result_str = makeStringInfo();

	while ((row = table_next_row(rel, scan)) != NULL)
	{
		Assert(row->nkeys > 0);

		appendHton32StringInfo(result_str, row->ix_type);
		appendHton32StringInfo(result_str, row->oids.datoid);
		appendHton32StringInfo(result_str, row->oids.reloid);
		appendHton32StringInfo(result_str, row->oids.relnode);
		appendHton32StringInfo(result_str, row->oids.spcoid);

		process_tree(result_str, row);
	}
	table_endscan(scan);
	table_close(rel, AccessShareLock);

	result = (bytea *) palloc(VARHDRSZ + result_str->len);
	SET_VARSIZE(result, VARHDRSZ + result_str->len);
	memcpy(VARDATA(result), result_str->data, result_str->len);
	pfree(result_str->data);
	pfree(result_str);
	PG_RETURN_BYTEA_P(result);
}

#define REWIND_OXID 0

/*
 * The rewind file is applied before replay, one row per transaction under
 * REWIND_OXID.  The images the promotion logged are applied mid-stream and
 * take no transaction at all, see apply_image_row().
 */
static void
start_rewind_oxid(void)
{
	advance_oxids(REWIND_OXID);
	recovery_switch_to_oxid(REWIND_OXID, -1);
}

static void
finish_rewind_oxid(void)
{
	bool		single = *recovery_single_process;
	XLogRecPtr	rec;

	rec = recovery_get_current_ptr();
	if (!single)
	{
		workers_send_oxid_finish(rec, false, true);
		workers_synchronize(rec, false);
	}
	else
	{
		pg_atomic_write_u64(recovery_ptr, rec);
	}

	recovery_finish_current_oxid(COMMITSEQNO_FROZEN, rec, -1, true);
}

static void
apply_rewind_row(OTableDescr *descr, OIndexDescr *indexDescr,
				 int sys_tree_num,
				 OTuple rewind_row,
				 bool deleted)
{
	if (sys_tree_num < 0)
		apply_modify_record(descr, indexDescr,
							deleted ? RecoveryMsgTypeDelete : RecoveryMsgTypeInsert,
							rewind_row);
	else
	{
		Assert(sys_tree_supports_transactions(sys_tree_num));
		apply_sys_tree_modify_record(sys_tree_num,
									 deleted ? RecoveryMsgTypeDelete : RecoveryMsgTypeInsert,
									 rewind_row, REWIND_OXID,
									 COMMITSEQNO_INPROGRESS);
	}
}

/*
 * Bring the secondary indexes in line with one image row.
 *
 * The pre-image comes straight off the primary leaf page (see
 * o_btree_frozen_modify()), not from the undo chain, so this cannot use
 * apply_tbl_modify_record(): every entry is written frozen and raw too.
 * 'oldTuple' and 'newTuple' are NULL tuples when the row was absent before
 * or is absent after.
 */
static void
apply_image_secondary_indexes(OTableDescr *descr, OTuple oldTuple,
							  OTuple newTuple)
{
	TupleTableSlot *oldSlot = descr->oldTuple;
	TupleTableSlot *newSlot = descr->newTuple;
	bool		haveOld = !O_TUPLE_IS_NULL(oldTuple);
	bool		haveNew = !O_TUPLE_IS_NULL(newTuple);
	int			i;

	if (descr->nIndices < 2)
		return;

	/*
	 * The tuples are owned by the caller: the old one is its copy of the page
	 * image, the new one points into the record being replayed.
	 */
	if (haveOld)
		tts_orioledb_store_tuple(oldSlot, oldTuple, descr,
								 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
								 false, NULL);
	if (haveNew)
		tts_orioledb_store_tuple(newSlot, newTuple, descr,
								 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
								 false, NULL);

	for (i = 1; i < descr->nIndices; i++)
	{
		OIndexDescr *sk = descr->indices[i];
		OBTreeKeyBound oldKey,
					newKey;
		OTuple		nullTup;
		bool		needDelete,
					needInsert;

		/* CIC: skip BUILDING secondaries.  See apply_tbl_insert(). */
		if (sk->state != OINDEX_STATE_VALID)
			continue;

		O_TUPLE_SET_NULL(nullTup);

		needDelete = haveOld &&
			o_is_index_predicate_satisfied(sk, oldSlot, sk->econtext);
		needInsert = haveNew &&
			o_is_index_predicate_satisfied(sk, newSlot, sk->econtext);

		if (needDelete)
			tts_orioledb_fill_key_bound(oldSlot, sk, &oldKey);
		if (needInsert)
			tts_orioledb_fill_key_bound(newSlot, sk, &newKey);

		o_btree_load_shmem(&sk->desc);

		/* An entry that keeps its key is replaced where it is */
		if (needDelete && needInsert &&
			o_btree_cmp(&sk->desc, (Pointer) &oldKey, BTreeKeyBound,
						(Pointer) &newKey, BTreeKeyBound) == 0)
			needDelete = false;

		if (needDelete)
			(void) o_btree_frozen_modify(&sk->desc, BTreeOperationDelete,
										 nullTup, BTreeKeyNone,
										 (Pointer) &oldKey, BTreeKeyBound,
										 NULL);

		if (needInsert)
		{
			OTuple		newSkTup;

			newSkTup = tts_orioledb_make_secondary_tuple(newSlot, sk, true);

			/*
			 * The primary logged the row even though the secondary tuple did
			 * not fit, so replay has to tolerate the same.  See
			 * apply_tbl_insert().
			 */
			if (o_btree_len(&sk->desc, newSkTup, OTupleLength) <=
				O_BTREE_MAX_TUPLE_SIZE)
				(void) o_btree_frozen_modify(&sk->desc, BTreeOperationInsert,
											 newSkTup, BTreeKeyLeafTuple,
											 (Pointer) &newKey, BTreeKeyBound,
											 NULL);
			pfree(newSkTup.data);
		}
	}

	if (haveOld)
		ExecClearTuple(oldSlot);
	if (haveNew)
		ExecClearTuple(newSlot);
}

/*
 * Apply one row of the images the promotion logged.
 *
 * The rewind file is applied before replay; the images land in the middle of
 * it, onto pages that still carry the versions of the transaction the
 * promotion rolled back.  Those versions are committed on this node and their
 * undo was reclaimed long ago, while every ordinary modify path starts by
 * walking that chain -- so the row is written frozen and raw instead, and the
 * secondary indexes are maintained from the pre-image the primary tree hands
 * back.  See o_btree_frozen_modify().
 */
static void
apply_image_row(OTableDescr *descr, OIndexDescr *indexDescr,
				int sys_tree_num, OTuple rewind_row, bool deleted)
{
	BTreeDescr *td;
	OTuple		oldTuple;
	OTuple		nullTup;

	O_TUPLE_SET_NULL(nullTup);
	O_TUPLE_SET_NULL(oldTuple);

	td = sys_tree_num > 0 ? get_sys_tree(sys_tree_num) : &indexDescr->desc;
	o_btree_load_shmem(td);

	if (deleted)
		(void) o_btree_frozen_modify(td, BTreeOperationDelete,
									 rewind_row, BTreeKeyNonLeafKey,
									 NULL, BTreeKeyNone,
									 descr ? &oldTuple : NULL);
	else
		(void) o_btree_frozen_modify(td, BTreeOperationInsert,
									 rewind_row, BTreeKeyLeafTuple,
									 NULL, BTreeKeyNone,
									 descr ? &oldTuple : NULL);

	if (descr == NULL)
		return;

	apply_image_secondary_indexes(descr, oldTuple,
								  deleted ? nullTup : rewind_row);

	if (!O_TUPLE_IS_NULL(oldTuple))
		pfree(oldTuple.data);
}

/*
 * Reads the rewind rows in order: from the rewind file, or from memory when
 * the promoted node logged them (see rewind_images_redo()).
 */
typedef struct RewindReader
{
	File		file;			/* -1 to read from memory */
	const char *name;
	const char *buf;
	uint64		len;
	off_t		offset;
	bool		frozen;			/* write the rows frozen and raw */
} RewindReader;

static void replay_rewind_trees(RewindReader *r);

static int
rewind_read(RewindReader *r, void *dst, int len)
{
	int			readed;

	if (r->file >= 0)
		readed = OFileRead(r->file, (Pointer) dst, len, r->offset,
						   WAIT_EVENT_DATA_FILE_READ);
	else
	{
		readed = (int) Min((uint64) len, r->len - r->offset);
		memcpy(dst, r->buf + r->offset, readed);
	}
	if (readed > 0)
		r->offset += readed;
	return readed;
}

static void
ereport_rewind_error(RewindReader *r, OIndexDescr *indexDescr,
					 int sys_tree_num, char *name)
{
	OIndexType	err_ix_type;
	ORelOids	err_oids;

	if (indexDescr != NULL)
	{
		err_ix_type = indexDescr->desc.type;
		err_oids = indexDescr->oids;
	}
	else
	{
		err_ix_type = oIndexPrimary;
		err_oids.datoid = SYS_TREES_DATOID;
		err_oids.reloid = sys_tree_num;
		err_oids.relnode = sys_tree_num;
	}
	ereport(FATAL, (errcode_for_file_access(),
					errmsg("could not read "
						   "%s for "
						   "tree (%u %u %u %u) "
						   "from rewind rows %s",
						   name,
						   err_ix_type,
						   err_oids.datoid,
						   err_oids.reloid,
						   err_oids.relnode,
						   r->name)));
}

static OTuple
replay_rewind_row(RewindReader *r, char *read_buf,
				  OTableDescr *descr, OIndexDescr *indexDescr,
				  int sys_tree_num, bool add_new)
{
	int			item_header_size = sizeof(uint8) * 2 +
		sizeof(uint32);
	uint8		deleted;
	uint32		item_len;
	OTuple		rewind_row;
	uint32		u32data;

	Assert(item_header_size < O_BTREE_MAX_TUPLE_SIZE * 2);
	if (rewind_read(r, read_buf, item_header_size) != item_header_size)
		ereport_rewind_error(r, indexDescr, sys_tree_num, "item header");

	deleted = *(uint8 *) (read_buf);
	rewind_row.formatFlags = *(uint8 *) (read_buf +
										 sizeof(uint8));
	memcpy(&u32data, read_buf + sizeof(uint8) + sizeof(uint8), sizeof(uint32));
	item_len = pg_ntoh32(u32data);

	Assert(item_len < O_BTREE_MAX_TUPLE_SIZE * 2);
	if (rewind_read(r, read_buf, item_len) != item_len)
		ereport_rewind_error(r, indexDescr, sys_tree_num, "item data");
	rewind_row.data = read_buf;

	if (indexDescr || (sys_tree_num > 0))
	{
		bool		old_toast_consistent = toast_consistent;

		toast_consistent = true;
/* TODO: Find out are we need real */
		/* toast_consistent value */

		if (r->frozen)
		{
			o_set_syscache_hooks();
			apply_image_row(descr, indexDescr, sys_tree_num, rewind_row,
							!add_new || deleted);
			o_unset_syscache_hooks();
		}
		else
		{
			/*
			 * Delete the target's version of the row and insert the source's
			 * one in a single transaction.  The insert finds the row deleted
			 * by its own transaction and rolls the deletion back first, which
			 * needs the undo record of the deletion.  The undo record is kept
			 * only while the transaction runs.
			 */
			start_rewind_oxid();
			apply_rewind_row(descr, indexDescr, sys_tree_num,
							 rewind_row,
							 true);
			if (add_new)
				apply_rewind_row(descr, indexDescr, sys_tree_num,
								 rewind_row,
								 deleted);
			finish_rewind_oxid();
		}
		toast_consistent = old_toast_consistent;
	}
	return rewind_row;
}

/*
 * Keys of the rows the promotion rolled back.
 *
 * At promotion recovery_finish() rolls back the transactions still in
 * progress.  The old primary key have committed them after the divergence
 * point, so pg_rewind has to take this node's version of every row they
 * changed, including the rows changed before the last common checkpoint.
 *
 * The keys travel like this:
 *
 * 1. Every recovery process captures the keys it rolls back
 *    (rewind_keys_capture()).  Workers hand them over to the startup process
 *    through a plain file each (rewind_keys_finish_process()).
 * 2. The startup process merges them into a durable pending file
 *    (rewind_keys_collect()).  Nothing can be logged yet: WAL insertion is
 *    not allowed until the end of recovery.
 * 3. The startup process logs the pending file as
 *    ORIOLEDB_XLOG_REWIND_KEYS records right after the end of recovery
 *    (rewind_keys_save()).  This is the first OrioleDB WAL of the new
 *    timeline, so the standbys get the keys too, and pg_rewind reads them
 *    from there.  The pending file only covers a crash in between: the next
 *    start is crash recovery, which can't capture the keys again.
 *
 * pg_rewind reads the records of the promotion that forked at its divergence
 * point, see add_divergence_rewind_keys().
 */
bool		rewind_keys_capturing = false;

static StringInfo rewind_keys = NULL;
static bool rewind_keys_collected = false;

#define REWIND_KEYS_PART_FORMAT		ORIOLEDB_DATA_DIR"/pg_rewind_keys.%d.part"
#define REWIND_KEYS_PENDING_FORMAT	ORIOLEDB_DATA_DIR"/pg_rewind_keys.%08X%08X.pending"
/* Type, the four oids of the tree, format flags and key length */
#define REWIND_KEY_ENTRY_HEADER_SIZE	(5 * sizeof(uint32) + sizeof(uint8) + sizeof(uint32))
#define REWIND_KEYS_WAL_CHUNK			(1024 * 1024)

#ifdef IS_DEV
PG_FUNCTION_INFO_V1(orioledb_pg_rewind_divergence_keys);
#endif

void
rewind_keys_capture(BTreeDescr *desc, BTreeOperationType action, OTuple tuple)
{
	OIndexType	type = desc->type;
	ORelOids	oids = desc->oids;
	OTuple		key = tuple;
	bool		key_allocated = false;
	int			len;
	MemoryContext oldcxt;

	if (action == BTreeOperationLock)
		return;
	if (action != BTreeOperationInsert &&
		action != BTreeOperationUpdate &&
		action != BTreeOperationDelete)
		elog(ERROR, "unsupported undo action %d in rewind key capture", action);

	/* System trees are logged under their own oids.  */
	if (!IS_SYS_TREE_OIDS(oids))
	{
		/*
		 * Record only the rows of the trees whose changes go to WAL, and name
		 * their trees just like WAL do, so pg_rewind takes these keys just
		 * like the ones it finds it at WAL.
		 *
		 * Callers of add_modify_wal_record_extended logs: - Primary index as
		 * a table row. - TOAST and bridge index rows are logged by records on
		 * their own.  They are registered under their own oids, so neither
		 * follows from the table rows, so the rewind needs their keys too. -
		 * Secondary index rows are not logged at all.  A table row is applied
		 * via apply_tbl_modify_record() -- this function updates the
		 * secondary indexes too.
		 */
		if (type == oIndexPrimary)
		{
			oids = ((OIndexDescr *) desc->arg)->tableOids;
			type = oIndexInvalid;
		}
		else if (type != oIndexToast && type != oIndexBridge)
			return;
	}

	/* Update undo keeps the whole old tuple, the others keep the key.  */
	if (action == BTreeOperationUpdate)
		key = o_btree_tuple_make_key(desc, tuple, NULL, true, &key_allocated);
	len = o_btree_len(desc, key, OKeyLength);

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	if (rewind_keys == NULL)
		rewind_keys = makeStringInfo();
	appendHton32StringInfo(rewind_keys, (uint32) type);
	appendHton32StringInfo(rewind_keys, oids.datoid);
	appendHton32StringInfo(rewind_keys, oids.reloid);
	appendHton32StringInfo(rewind_keys, oids.relnode);
	appendHton32StringInfo(rewind_keys, oids.spcoid);
	appendStringInfoChar(rewind_keys, (char) key.formatFlags);
	appendHton32StringInfo(rewind_keys, (uint32) len);
	appendBinaryStringInfoNT(rewind_keys, key.data, len);
	MemoryContextSwitchTo(oldcxt);

	if (key_allocated)
		pfree(key.data);
}

static void
rewind_keys_write_file(const char *path, const char *data, uint64 len,
					   bool sync)
{
	int			fd;
	uint64		offset = 0;

	fd = OpenTransientFile(path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open rewind keys file \"%s\": %m", path)));
	while (offset < len)
	{
		ssize_t		written = write(fd, data + offset, len - offset);

		if (written < 0 && errno == EINTR)
			continue;
		if (written <= 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not write rewind keys file \"%s\": %m", path)));
		offset += written;
	}
	if (sync && pg_fsync(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not sync rewind keys file \"%s\": %m", path)));
	if (CloseTransientFile(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close rewind keys file \"%s\": %m", path)));
}

/* Reads the whole file, returns NULL if it does not exist.  */
static char *
rewind_keys_read_file(const char *path, uint64 *len)
{
	struct stat st;
	char	   *data;
	uint64		offset = 0;
	int			fd;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return NULL;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open rewind keys file \"%s\": %m", path)));
	}
	if (fstat(fd, &st) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not stat rewind keys file \"%s\": %m", path)));
	*len = st.st_size;
	data = palloc_extended(Max(*len, 1), MCXT_ALLOC_HUGE);
	while (offset < *len)
	{
		ssize_t		done = read(fd, data + offset, *len - offset);

		if (done < 0 && errno == EINTR)
			continue;
		if (done <= 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read rewind keys file \"%s\": %m", path)));
		offset += done;
	}
	if (CloseTransientFile(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close rewind keys file \"%s\": %m", path)));
	return data;
}

/* Removes the rewind keys files of the data dir that end with suffix.  */
static void
rewind_keys_remove_by_suffix(const char *suffix)
{
	DIR		   *dir = AllocateDir(ORIOLEDB_DATA_DIR);
	struct dirent *de;
	size_t		suffix_len = strlen(suffix);

	while ((de = ReadDir(dir, ORIOLEDB_DATA_DIR)) != NULL)
	{
		size_t		namelen = strlen(de->d_name);
		char		path[MAXPGPATH];

		if (strncmp(de->d_name, "pg_rewind_keys.", 15) != 0 ||
			namelen < suffix_len ||
			strcmp(de->d_name + namelen - suffix_len, suffix) != 0)
			continue;
		snprintf(path, sizeof(path), ORIOLEDB_DATA_DIR "/%s", de->d_name);
		if (unlink(path) < 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove rewind keys file \"%s\": %m", path)));
	}
	FreeDir(dir);
}

/*
 * Remove the files the recovery processes handed their keys over in: the
 * ones left by an earlier start must not be taken for the current ones.
 */
void
rewind_keys_remove_parts(void)
{
	rewind_keys_remove_by_suffix(".part");
}

/*
 * Called by each recovery process at the end of its undo.  The workers hand
 * their keys over to the startup process, which keeps its own in memory.
 */
void
rewind_keys_finish_process(int worker_id)
{
	char		path[MAXPGPATH];

	rewind_keys_capturing = false;
	if (worker_id < 0 || rewind_keys == NULL)
		return;

	snprintf(path, sizeof(path), REWIND_KEYS_PART_FORMAT, worker_id);
	/* It is read at the same start by the startup process: no need to sync */
	rewind_keys_write_file(path, rewind_keys->data, rewind_keys->len, false);
	pfree(rewind_keys->data);
	pfree(rewind_keys);
	rewind_keys = NULL;
}

/*
 * Called by the startup process after all the recovery processes finished:
 * merge their keys and keep them durably until they are logged.  The
 * switchpoint is the end of recovery.
 */
static void
rewind_keys_collect(XLogRecPtr switchpoint)
{
	DIR		   *dir;
	struct dirent *de;
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	if (rewind_keys == NULL)
		rewind_keys = makeStringInfo();

	dir = AllocateDir(ORIOLEDB_DATA_DIR);
	while ((de = ReadDir(dir, ORIOLEDB_DATA_DIR)) != NULL)
	{
		int			worker_id;
		char		stop;
		char	   *data;
		uint64		len;

		if (sscanf(de->d_name, "pg_rewind_keys.%d.part%c", &worker_id,
				   &stop) != 1)
			continue;
		snprintf(path, sizeof(path), ORIOLEDB_DATA_DIR "/%s", de->d_name);
		data = rewind_keys_read_file(path, &len);
		Assert(data != NULL);
		appendBinaryStringInfoNT(rewind_keys, data, len);
		pfree(data);
	}
	FreeDir(dir);
	rewind_keys_remove_parts();

	snprintf(path, sizeof(path), REWIND_KEYS_PENDING_FORMAT,
			 LSN_FORMAT_ARGS(switchpoint));
	snprintf(tmppath, sizeof(tmppath), "%s.tmp", path);
	rewind_keys_write_file(tmppath, rewind_keys->data, rewind_keys->len, true);
	if (durable_rename(tmppath, path, ERROR) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not publish rewind keys file \"%s\": %m", path)));
	pfree(rewind_keys->data);
	pfree(rewind_keys);
	rewind_keys = NULL;
	rewind_keys_collected = true;
	MemoryContextSwitchTo(oldcxt);

	STOPEVENT(STOPEVENT_PG_REWIND_KEYS_AFTER_PENDING, NULL);
}

/*
 * Log data as records of the given kind (ORIOLEDB_XLOG_REWIND_KEYS or
 * ORIOLEDB_XLOG_REWIND_IMAGES), and flush them.  The keys are split by whole
 * entries, the images by bytes: redo gathers them up.  There is at least one
 * record, even for no data, so pg_rewind can tell this promotion from the
 * one without the keys.
 */
static void
log_rewind_payload(uint8 info, XLogRecPtr switchpoint, const char *data,
				   uint64 len)
{
	XLogRecPtr	lsn = InvalidXLogRecPtr;
	uint64		offset = 0;

	do
	{
		StringInfoData header;
		uint64		chunk = 0;
		uint16		flags;

		if (info == ORIOLEDB_XLOG_REWIND_KEYS)
		{
			/* Take the entries up to the chunk size.  */
			while (offset + chunk < len && chunk < REWIND_KEYS_WAL_CHUNK)
			{
				uint32		key_len;

				if (len - offset - chunk < REWIND_KEY_ENTRY_HEADER_SIZE)
					ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
									errmsg("truncated entry in rewind keys")));
				memcpy(&key_len,
					   data + offset + chunk + REWIND_KEY_ENTRY_HEADER_SIZE -
					   sizeof(uint32), sizeof(key_len));
				key_len = pg_ntoh32(key_len);
				if (len - offset - chunk - REWIND_KEY_ENTRY_HEADER_SIZE < key_len)
					ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
									errmsg("truncated entry in rewind keys")));
				chunk += REWIND_KEY_ENTRY_HEADER_SIZE + key_len;
			}
		}
		else
			chunk = Min(len - offset, REWIND_KEYS_WAL_CHUNK);

		flags = (offset == 0 ? REWIND_KEYS_FIRST : 0) |
			(offset + chunk == len ? REWIND_KEYS_LAST : 0);
		initStringInfo(&header);
		appendHton16StringInfo(&header, REWIND_KEYS_WAL_VERSION);
		appendHton16StringInfo(&header, flags);
		appendHton64StringInfo(&header, switchpoint);
		appendHton32StringInfo(&header, (uint32) chunk);
		Assert(header.len == REWIND_KEYS_WAL_HEADER_SIZE);

		XLogBeginInsert();
		XLogRegisterData(header.data, header.len);
		XLogRegisterData((char *) data + offset, chunk);
		lsn = XLogInsert(ORIOLEDB_RMGR_ID, info);
		pfree(header.data);
		offset += chunk;
	} while (offset < len);

	XLogFlush(lsn);
}

/*
 * The rows of the captured keys as the promotion left them, in the format of
 * the rewind file: what the old primary has to take instead of its own
 * version.  They come from the trees right after the undo ran, so it is
 * the state the transaction's rollback produced.
 */
typedef struct RewindTreeKeys
{
	OIndexType	type;
	ORelOids	oids;
	uint32		nkeys;
	StringInfo	keys;			/* flags, length and bytes of each key */
} RewindTreeKeys;

typedef struct RewindImages
{
	XLogRecPtr	switchpoint;
	StringInfo	images;
} RewindImages;

/* Of RewindImages, until rewind_keys_save() logs them */
static List *rewind_images = NIL;

static int
rewind_tree_keys_cmp(const ListCell *a, const ListCell *b)
{
	RewindTreeKeys *t1 = (RewindTreeKeys *) lfirst(a);
	RewindTreeKeys *t2 = (RewindTreeKeys *) lfirst(b);

	/* System trees first, like pg_rewind sorts the trees it takes */
	if (t1->oids.datoid <= SYS_TREES_DATOID && t2->oids.datoid > SYS_TREES_DATOID)
		return -1;
	if (t1->oids.datoid > SYS_TREES_DATOID && t2->oids.datoid <= SYS_TREES_DATOID)
		return 1;
	if (t1->type != t2->type)
		return t1->type < t2->type ? -1 : 1;
	if (t1->oids.datoid != t2->oids.datoid)
		return t1->oids.datoid < t2->oids.datoid ? -1 : 1;
	if (t1->oids.reloid != t2->oids.reloid)
		return t1->oids.reloid < t2->oids.reloid ? -1 : 1;
	if (t1->oids.relnode != t2->oids.relnode)
		return t1->oids.relnode < t2->oids.relnode ? -1 : 1;
	if (t1->oids.spcoid != t2->oids.spcoid)
		return t1->oids.spcoid < t2->oids.spcoid ? -1 : 1;
	return 0;
}

/* Group the keys of rewind_keys_capture() by tree.  */
static List *
rewind_keys_by_tree(const char *data, uint64 len)
{
	List	   *trees = NIL;
	uint64		offset = 0;

	while (offset < len)
	{
		uint32		u32;
		OIndexType	type;
		ORelOids	oids;
		uint8		flags;
		uint32		key_len;
		ListCell   *lc;
		RewindTreeKeys *tree = NULL;

		if (len - offset < REWIND_KEY_ENTRY_HEADER_SIZE)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("truncated entry in rewind keys")));
		memcpy(&u32, data + offset, sizeof(u32));
		type = (OIndexType) pg_ntoh32(u32);
		memcpy(&u32, data + offset + 4, sizeof(u32));
		oids.datoid = pg_ntoh32(u32);
		memcpy(&u32, data + offset + 8, sizeof(u32));
		oids.reloid = pg_ntoh32(u32);
		memcpy(&u32, data + offset + 12, sizeof(u32));
		oids.relnode = pg_ntoh32(u32);
		memcpy(&u32, data + offset + 16, sizeof(u32));
		oids.spcoid = pg_ntoh32(u32);
		flags = *(uint8 *) (data + offset + 20);
		memcpy(&u32, data + offset + 21, sizeof(u32));
		key_len = pg_ntoh32(u32);
		offset += REWIND_KEY_ENTRY_HEADER_SIZE;
		if (len - offset < key_len)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("truncated entry in rewind keys")));

		foreach(lc, trees)
		{
			RewindTreeKeys *t = (RewindTreeKeys *) lfirst(lc);

			if (t->type == type && ORelOidsIsEqual(t->oids, oids))
			{
				tree = t;
				break;
			}
		}
		if (tree == NULL)
		{
			tree = palloc0(sizeof(RewindTreeKeys));
			tree->type = type;
			tree->oids = oids;
			tree->keys = makeStringInfo();
			trees = lappend(trees, tree);
		}
		appendStringInfoChar(tree->keys, (char) flags);
		appendHton32StringInfo(tree->keys, key_len);
		appendBinaryStringInfoNT(tree->keys, data + offset, key_len);
		tree->nkeys++;
		offset += key_len;
	}
	return trees;
}

/*
 * Append the rows of the tree's keys: what orioledb_pg_rewind_sorted_keys()
 * and orioledb_pg_rewind_new_row_versions() make of them for pg_rewind.
 * The node does not know what the old primary has, so a tree of the system
 * catalog that is there is taken whole.
 */
static void
rewind_tree_images(StringInfo out, RewindTreeKeys *tree)
{
	OTableDescr *descr = NULL;
	OIndexDescr *indexDescr = NULL;
	int			sys_tree_num;
	KeyArrayElement **key_array;
	char	   *ptr = tree->keys->data;
	StringInfo	keys_str;
	uint32		new_nkeys;
	uint32		i;
	TableRow	row;

	sys_tree_num = fill_descrs(tree->oids, tree->type, &descr, &indexDescr);
	if (!indexDescr && sys_tree_num <= 0)
		return;

	key_array = create_key_array(tree->nkeys, 0, &ptr);
	if (sys_tree_num > 0)
		cmp_tree_desc = get_sys_tree(sys_tree_num);
	else
		cmp_tree_desc = &indexDescr->desc;
	new_nkeys = sort_key_array(key_array, tree->nkeys);

	keys_str = makeStringInfo();
	for (i = 0; i < new_nkeys; i++)
	{
		append_tuple(keys_str, key_array[i]->tupleFormatFlags,
					 key_array[i]->data, key_array[i]->dataLength, NULL);
		if (sys_tree_num == SYS_TREES_O_INDICES)
		{
			uint8		index_exist = false;

			appendBinaryStringInfoNT(keys_str, (Pointer) &index_exist,
									 sizeof(uint8));
		}
		pfree(key_array[i]);
	}
	pfree(key_array);

	row.ix_type = tree->type;
	row.oids = tree->oids;
	row.nkeys = new_nkeys;
	row.keys = (bytea *) palloc(VARHDRSZ + keys_str->len);
	SET_VARSIZE(row.keys, VARHDRSZ + keys_str->len);
	memcpy(VARDATA(row.keys), keys_str->data, keys_str->len);

	appendHton32StringInfo(out, row.ix_type);
	appendHton32StringInfo(out, row.oids.datoid);
	appendHton32StringInfo(out, row.oids.reloid);
	appendHton32StringInfo(out, row.oids.relnode);
	appendHton32StringInfo(out, row.oids.spcoid);
	process_tree(out, &row);

	pfree(row.keys);
	pfree(keys_str->data);
	pfree(keys_str);
}

/*
 * Called by the startup process when every recovery process has rolled
 * its transactions back.  The keys of the rows that went through it are
 * merged and kept durably, and the rows as they are now are put
 * aside to be logged after the end of recovery, with the keys.  They are
 * made from the pending keys files, so a crash before the logging gets them
 * again at the next start: the rollback is repeated then.
 */
void
rewind_images_prepare(void)
{
	XLogRecPtr	switchpoint = pg_atomic_read_u64(recovery_rewind_keys_switchpoint);
	DIR		   *dir;
	struct dirent *de;
	List	   *pending = NIL;
	ListCell   *lc;
	MemoryContext oldcxt;

	if (XLogRecPtrIsValid(switchpoint))
		rewind_keys_collect(switchpoint);

	dir = AllocateDir(ORIOLEDB_DATA_DIR);
	while ((de = ReadDir(dir, ORIOLEDB_DATA_DIR)) != NULL)
	{
		uint32		hi;
		uint32		lo;
		char		stop;

		if (sscanf(de->d_name, "pg_rewind_keys.%08X%08X.pending%c",
				   &hi, &lo, &stop) != 2)
			continue;
		pending = lappend(pending, pstrdup(de->d_name));
	}
	FreeDir(dir);

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	foreach(lc, pending)
	{
		const char *name = (const char *) lfirst(lc);
		uint32		hi;
		uint32		lo;
		char		path[MAXPGPATH];
		char	   *data;
		uint64		len;
		List	   *trees;
		ListCell   *tc;
		RewindImages *images;

		sscanf(name, "pg_rewind_keys.%08X%08X.pending", &hi, &lo);
		snprintf(path, sizeof(path), ORIOLEDB_DATA_DIR "/%s", name);
		data = rewind_keys_read_file(path, &len);
		Assert(data != NULL);

		images = palloc0(sizeof(RewindImages));
		images->switchpoint = ((uint64) hi << 32) | lo;
		images->images = makeStringInfo();

		trees = rewind_keys_by_tree(data, len);
		list_sort(trees, rewind_tree_keys_cmp);
		foreach(tc, trees)
			rewind_tree_images(images->images, (RewindTreeKeys *) lfirst(tc));
		list_free_deep(trees);
		pfree(data);

		rewind_images = lappend(rewind_images, images);
	}
	MemoryContextSwitchTo(oldcxt);
	list_free_deep(pending);
}

/*
 * Called by the startup process at the end of recovery, when WAL insertion
 * is allowed.  The end of archive recovery is a promotion, so log the keys
 * it collected.  Besides, log the ones an earlier start was not able to.
 */
void
rewind_keys_save(XLogRecPtr switchpoint)
{
	DIR		   *dir;
	struct dirent *de;
	List	   *pending = NIL;
	ListCell   *lc;
	ListCell   *ic;
	bool		logged_current = false;

	/* Collect the names first: the files are removed below.  */
	dir = AllocateDir(ORIOLEDB_DATA_DIR);
	while ((de = ReadDir(dir, ORIOLEDB_DATA_DIR)) != NULL)
	{
		uint32		hi;
		uint32		lo;
		char		stop;

		if (sscanf(de->d_name, "pg_rewind_keys.%08X%08X.pending%c",
				   &hi, &lo, &stop) != 2)
			continue;
		pending = lappend(pending, pstrdup(de->d_name));
	}
	FreeDir(dir);

	foreach(lc, pending)
	{
		const char *name = (const char *) lfirst(lc);
		uint32		hi;
		uint32		lo;
		XLogRecPtr	pending_switchpoint;
		char		path[MAXPGPATH];
		char	   *data;
		uint64		len;

		sscanf(name, "pg_rewind_keys.%08X%08X.pending", &hi, &lo);
		pending_switchpoint = ((uint64) hi << 32) | lo;
		snprintf(path, sizeof(path), ORIOLEDB_DATA_DIR "/%s", name);
		data = rewind_keys_read_file(path, &len);
		Assert(data != NULL);

		log_rewind_payload(ORIOLEDB_XLOG_REWIND_KEYS, pending_switchpoint, data,
						   len);
		foreach(ic, rewind_images)
		{
			RewindImages *images = (RewindImages *) lfirst(ic);

			if (images->switchpoint == pending_switchpoint)
				log_rewind_payload(ORIOLEDB_XLOG_REWIND_IMAGES,
								   pending_switchpoint,
								   images->images->data,
								   images->images->len);
		}
		STOPEVENT(STOPEVENT_PG_REWIND_KEYS_AFTER_WAL_FLUSH, NULL);
		pfree(data);
		if (durable_unlink(path, ERROR) != 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not remove rewind keys file \"%s\": %m", path)));
		logged_current |= pending_switchpoint == switchpoint;
	}
	list_free_deep(pending);

	/*
	 * Recovery that replayed no records at all never ran
	 * rewind_keys_collect(), but it still forks a timeline: it has no keys.
	 */
	if (ArchiveRecoveryRequested && !rewind_keys_collected && !logged_current)
	{
		log_rewind_payload(ORIOLEDB_XLOG_REWIND_KEYS, switchpoint, "", 0);
		STOPEVENT(STOPEVENT_PG_REWIND_KEYS_AFTER_WAL_FLUSH, NULL);
	}
	if (rewind_keys_collected && !logged_current)
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("rewind keys switchpoint differs from the end of recovery %X/%X",
							   LSN_FORMAT_ARGS(switchpoint))));
	rewind_keys_collected = false;
	list_free_deep(rewind_images);
	rewind_images = NIL;

	/* Parts left by a start that crashed before collecting them */
	rewind_keys_remove_parts();
}

#ifdef IS_DEV
/*
 * Reads the keys the promotion that forked at switchpoint logged.  Returns
 * NULL if this server is not on a timeline forked at the switchpoint, so it
 * did not promote there.  The result is the entries of the records of the
 * first complete set, in the format of rewind_keys_capture().
 *
 * For the tests only.  pg_rewind finds these records in the target's WAL as
 * it scans it, and replay reads them out of the WAL stream.
 */
Datum
orioledb_pg_rewind_divergence_keys(PG_FUNCTION_ARGS)
{
	XLogRecPtr	switchpoint = PG_GETARG_LSN(0);
	TimeLineID	tli;
	List	   *history;
	ListCell   *lc;
	bool		forked = false;
	XLogReaderState *reader;
	ReadLocalXLogPageNoWaitPrivate *private_data;
	StringInfoData keys;
	bool		complete = false;
	bytea	   *result;

	if (RecoveryInProgress())
		(void) GetXLogReplayRecPtr(&tli);
	else
		tli = GetWALInsertionTimeLine();
	history = readTimeLineHistory(tli);
	foreach(lc, history)
	{
		if (((TimeLineHistoryEntry *) lfirst(lc))->end == switchpoint)
			forked = true;
	}
	list_free_deep(history);
	if (!forked)
		PG_RETURN_NULL();

	private_data = (ReadLocalXLogPageNoWaitPrivate *)
		palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								private_data);
	if (reader == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("out of memory")));
	XLogBeginRead(reader, switchpoint);

	initStringInfo(&keys);
	while (!complete)
	{
		char	   *errormsg;
		XLogRecord *record;
		const char *rec;
		uint16		version;
		uint16		flags;
		uint32		payload_len;
		XLogRecPtr	rec_switchpoint;

		CHECK_FOR_INTERRUPTS();
		record = XLogReadRecord(reader, &errormsg);
		if (record == NULL)
			break;
		if (XLogRecGetRmid(reader) != ORIOLEDB_RMGR_ID ||
			(XLogRecGetInfo(reader) & ~XLR_INFO_MASK) != ORIOLEDB_XLOG_REWIND_KEYS)
			continue;

		rec = XLogRecGetData(reader);
		if (XLogRecGetDataLen(reader) < REWIND_KEYS_WAL_HEADER_SIZE)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("truncated OrioleDB rewind keys WAL record at %X/%X",
								   LSN_FORMAT_ARGS(reader->ReadRecPtr))));
		memcpy(&version, rec, sizeof(version));
		memcpy(&flags, rec + 2, sizeof(flags));
		memcpy(&rec_switchpoint, rec + 4, sizeof(rec_switchpoint));
		memcpy(&payload_len, rec + 12, sizeof(payload_len));
		version = pg_ntoh16(version);
		flags = pg_ntoh16(flags);
		rec_switchpoint = pg_ntoh64(rec_switchpoint);
		payload_len = pg_ntoh32(payload_len);
		if (version != REWIND_KEYS_WAL_VERSION ||
			XLogRecGetDataLen(reader) != REWIND_KEYS_WAL_HEADER_SIZE + payload_len)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("invalid OrioleDB rewind keys WAL record at %X/%X",
								   LSN_FORMAT_ARGS(reader->ReadRecPtr))));
		if (rec_switchpoint != switchpoint)
			continue;

		/* An attempt a crash interrupted has no last record: start over.  */
		if (flags & REWIND_KEYS_FIRST)
			resetStringInfo(&keys);
		appendBinaryStringInfoNT(&keys, rec + REWIND_KEYS_WAL_HEADER_SIZE,
								 payload_len);
		complete = (flags & REWIND_KEYS_LAST) != 0;
	}
	XLogReaderFree(reader);
	if (!complete)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("no complete OrioleDB rewind keys of the promotion at %X/%X in WAL",
						LSN_FORMAT_ARGS(switchpoint))));

	result = (bytea *) palloc_extended(VARHDRSZ + keys.len, MCXT_ALLOC_HUGE);
	SET_VARSIZE(result, VARHDRSZ + keys.len);
	memcpy(VARDATA(result), keys.data, keys.len);
	PG_RETURN_BYTEA_P(result);
}
#endif							/* IS_DEV */

/*
 * Redo of ORIOLEDB_XLOG_REWIND_IMAGES.  Only a node pg_rewind rewound onto
 * the promotion these images belong to applies them: its own version of
 * those rows is the old primary's, and the rows the promotion left behind
 * are what everything the source did afterwards builds on.  Every other node
 * -- a standby of the promoted one, or a target rewound onto a different
 * switchpoint -- has undone the transaction from its own undo and ignores
 * the records.  Which promotion that is comes from the control file, see
 * rewind_images_pending().
 */
static StringInfo rewind_images_redo_buf = NULL;

/*
 * The images the rewind target still owes, or InvalidXLogRecPtr.  Read from
 * the control file, so a restartpoint between the rollback markers and the
 * images -- or a crash -- does not lose it.
 */
XLogRecPtr
rewind_images_pending(void)
{
	return pg_atomic_read_u64(&checkpoint_state->rewindImagesSwitchpoint);
}

void
rewind_images_redo(XLogReaderState *record)
{
	const char *rec = XLogRecGetData(record);
	uint16		flags;
	uint32		payload_len;
	XLogRecPtr	switchpoint;
	XLogRecPtr	pending = rewind_images_pending();

	if (XLogRecPtrIsInvalid(pending))
		return;

	if (XLogRecGetDataLen(record) < REWIND_KEYS_WAL_HEADER_SIZE)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("truncated OrioleDB rewind images WAL record")));
	memcpy(&flags, rec + 2, sizeof(flags));
	memcpy(&switchpoint, rec + 4, sizeof(switchpoint));
	memcpy(&payload_len, rec + 12, sizeof(payload_len));
	flags = pg_ntoh16(flags);
	switchpoint = pg_ntoh64(switchpoint);
	payload_len = pg_ntoh32(payload_len);
	if (XLogRecGetDataLen(record) != REWIND_KEYS_WAL_HEADER_SIZE + payload_len)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid OrioleDB rewind images WAL record")));

	/*
	 * Only the promotion this node was rewound onto left rows behind that its
	 * own WAL does not account for.  A later promotion in the source's
	 * history rolled back transactions that lie wholly inside the WAL being
	 * replayed, and ordinary replay has already undone them.
	 */
	if (switchpoint != pending)
		return;

	if (rewind_images_redo_buf == NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

		rewind_images_redo_buf = makeStringInfo();
		MemoryContextSwitchTo(oldcxt);
	}
	if (flags & REWIND_KEYS_FIRST)
		resetStringInfo(rewind_images_redo_buf);
	appendBinaryStringInfo(rewind_images_redo_buf,
						   rec + REWIND_KEYS_WAL_HEADER_SIZE, payload_len);

	if (flags & REWIND_KEYS_LAST)
	{
		RewindReader reader;

		/* Everything before the images is applied under them */
		if (!*recovery_single_process)
			workers_synchronize(record->ReadRecPtr, true);

		reader.file = -1;
		reader.name = "logged by the promotion";
		reader.buf = rewind_images_redo_buf->data;
		reader.len = rewind_images_redo_buf->len;
		reader.offset = 0;
		reader.frozen = true;

		elog(LOG, "orioledb rewind: applying the row images of the promotion at %X/%X",
			 LSN_FORMAT_ARGS(switchpoint));
		replay_rewind_trees(&reader);
		pg_atomic_write_u64(&checkpoint_state->rewindImagesSwitchpoint,
							InvalidXLogRecPtr);
		resetStringInfo(rewind_images_redo_buf);
	}
}

void
rewind_keys_desc(StringInfo buf, XLogReaderState *record)
{
	const char *rec = XLogRecGetData(record);
	uint16		flags;
	XLogRecPtr	switchpoint;

	if (XLogRecGetDataLen(record) < REWIND_KEYS_WAL_HEADER_SIZE)
	{
		appendStringInfoString(buf, "truncated rewind keys");
		return;
	}
	memcpy(&flags, rec + 2, sizeof(flags));
	memcpy(&switchpoint, rec + 4, sizeof(switchpoint));
	flags = pg_ntoh16(flags);
	switchpoint = pg_ntoh64(switchpoint);
	appendStringInfo(buf, "rewind %s of the promotion at %X/%X: %u bytes%s%s",
					 (XLogRecGetInfo(record) & ~XLR_INFO_MASK) ==
					 ORIOLEDB_XLOG_REWIND_IMAGES ? "images" : "keys",
					 LSN_FORMAT_ARGS(switchpoint),
					 (unsigned) (XLogRecGetDataLen(record) - REWIND_KEYS_WAL_HEADER_SIZE),
					 (flags & REWIND_KEYS_FIRST) ? ", first" : "",
					 (flags & REWIND_KEYS_LAST) ? ", last" : "");
}

/*
 * Replays the trees of the rewind rows: the rewind file, or the images the
 * promoted node logged.
 */
static void
replay_rewind_trees(RewindReader *r)
{
	char		read_buf[O_BTREE_MAX_TUPLE_SIZE * 2];
	OIndexType	ix_type = oIndexInvalid;
	ORelOids	cur_oids = {0, 0, 0};
	int			readed;
	const int	tree_header_size = sizeof(OIndexType) + 4 * sizeof(Oid) +
		sizeof(uint8);
	uint32		nkeys;
	uint8		found;

	Assert(tree_header_size < O_BTREE_MAX_TUPLE_SIZE * 2);

	readed = rewind_read(r, read_buf, tree_header_size);
	while (readed == tree_header_size)
	{
		int			sys_tree_num = -1;

		ix_type = pg_ntoh32(*((uint32 *) (read_buf)));
		cur_oids.datoid = pg_ntoh32(*((Oid *) (read_buf + sizeof(uint32))));
		cur_oids.reloid = pg_ntoh32(*((Oid *) (read_buf + sizeof(uint32) +
											   sizeof(Oid))));
		cur_oids.relnode = pg_ntoh32(*((Oid *) (read_buf + sizeof(uint32) +
												sizeof(Oid) * 2)));
		cur_oids.spcoid = pg_ntoh32(*((Oid *) (read_buf + sizeof(uint32) +
											   sizeof(Oid) * 3)));
		found = *((uint8 *) (read_buf + sizeof(uint32) +
							 sizeof(Oid) * 4));

		elog(LOG, "REWIND TREE: %u %u %u: %c",
			 cur_oids.datoid,
			 cur_oids.reloid,
			 cur_oids.relnode,
			 found ? 'Y' : 'N');
		if (IS_SYS_TREE_OIDS(cur_oids))
			sys_tree_num = cur_oids.relnode;

		if (found)
		{
			OTableDescr *descr = NULL;
			OIndexDescr *indexDescr = NULL;
			int			i;

			if (rewind_read(r, read_buf, sizeof(uint32)) != sizeof(uint32))
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not read keys amount for "
									   "tree (%u %u %u %u) "
									   "from rewind rows %s",
									   ix_type,
									   cur_oids.datoid,
									   cur_oids.reloid,
									   cur_oids.relnode,
									   r->name)));
			nkeys = pg_ntoh32(*((uint32 *) (read_buf)));

			sys_tree_num = fill_descrs(cur_oids, ix_type, &descr, &indexDescr);

			for (i = 0; i < nkeys; i++)
			{
				int			j;
				int			row_header_size = sizeof(uint8);
				OTuple		rewind_row = {0};

				if (rewind_read(r, read_buf, row_header_size) !=
					row_header_size)
					ereport(FATAL, (errcode_for_file_access(),
									errmsg("could not read row header for "
										   "tree (%u %u %u %u) "
										   "from rewind rows %s",
										   ix_type,
										   cur_oids.datoid,
										   cur_oids.reloid,
										   cur_oids.relnode,
										   r->name)));

				found = *((uint8 *) (read_buf));

				rewind_row = replay_rewind_row(r, read_buf, descr, indexDescr,
											   sys_tree_num, found);

				if (sys_tree_num == SYS_TREES_O_INDICES && found)
				{
					uint32		ix_nitems;
					OIndexChunkKey *ix_key;

					ix_key = palloc0(sizeof(OIndexChunkKey));
					memcpy(ix_key, rewind_row.data, sizeof(OIndexChunkKey));

					indexDescr = o_fetch_index_descr(ix_key->oids,
													 ix_key->type,
													 false, NULL);
					Assert(indexDescr);

					if (rewind_read(r, read_buf, sizeof(uint32)) !=
						sizeof(uint32))
						ereport(FATAL, (errcode_for_file_access(),
										errmsg("could not read keys amount for "
											   "removed tree (%u %u %u %u) "
											   "from rewind rows %s",
											   ix_key->type,
											   ix_key->oids.datoid,
											   ix_key->oids.reloid,
											   ix_key->oids.relnode,
											   r->name)));
					ix_nitems = pg_ntoh32(*(uint32 *) read_buf);

					for (j = 0; j < ix_nitems; j++)
						replay_rewind_row(r, read_buf, NULL, indexDescr, -1,
										  true);
					pfree(ix_key);
				}
			}
		}
		readed = rewind_read(r, read_buf, tree_header_size);
	}

	if (readed > 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read tree header from rewind "
							   "rows %s: expected %d bytes, got %d",
							   r->name, tree_header_size, readed)));
}

/*
 * Replays rewind file.
 */
void
replay_rewind(void)
{
	File		rewind_file;
	RewindReader reader;
	XLogRecPtr	startpoint;
	OXid		lastXid;

	rewind_file = PathNameOpenFile(ORIOLEDB_DATA_DIR "/rewind",
								   O_RDONLY | PG_BINARY);

	if (rewind_file < 0)
		return;

	elog(LOG, "orioledb rewind started");

	reader.file = rewind_file;
	reader.name = FilePathName(rewind_file);
	reader.buf = NULL;
	reader.len = 0;
	reader.offset = 0;
	reader.frozen = false;

	if (rewind_read(&reader, &startpoint, sizeof(XLogRecPtr)) !=
		sizeof(XLogRecPtr))
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read startpoint from rewind file %s",
							   reader.name)));
	if (rewind_read(&reader, &lastXid, sizeof(OXid)) != sizeof(OXid))
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read lastXid from rewind file %s",
							   reader.name)));

	replay_rewind_trees(&reader);

	/*
	 * Replay starts where the rewind file was taken from: the last common
	 * checkpoint.  The stage-0 window is empty, since the rows above brought
	 * the sys trees to that point as well.
	 */
	checkpoint_state->controlToastConsistentPtr = startpoint;
	checkpoint_state->controlReplayStartPtr = startpoint;
	checkpoint_state->controlSysTreesStartPtr = startpoint;

	/*
	 * The xid horizons are already seeded from divXid: pg_rewind wrote it
	 * into the control file as checkpointRetainXmin, checkpointRetainXmax and
	 * lastXid, and checkpoint_shmem_init() ran before this.  lastXid here is
	 * the same value, written into the rewind file by the same run, so there
	 * is nothing left to do but say so.
	 *
	 * What those horizons buy: divXid is the lowest xid the source's WAL will
	 * reference, so it is both the runXmin / globalXmin floor and the nextXid
	 * to assign -- advance_oxids() only initialises a slot to INPROGRESS when
	 * the incoming oxid is >= nextXid, so the first replayed transaction gets
	 * a fresh slot instead of inheriting the stale FROZEN one the target's
	 * own xidmap left behind.  An empty retain range means no target xidmap
	 * slot is trusted either: every oxid below divXid reads as settled, and
	 * every oxid at or above it is initialised as WAL_REC_XID arrives.
	 */
	Assert(OXidIsValid(lastXid));
	Assert(pg_atomic_read_u64(&xid_meta->nextXid) == lastXid);
	Assert(pg_atomic_read_u64(&xid_meta->runXmin) == lastXid);

	/*
	 * The undo metadata is left alone.  It was loaded from the target's
	 * control file and has since been advanced by the rows applied above.
	 * Other processes (e.g. the orioledb bgwriter) already reserve undo space
	 * at this point, so resetting these counters would corrupt their
	 * reservations and let new undo records overwrite existing ones.
	 */

	/*
	 * Nothing above ran a checkpoint, so the rest of the checkpoint state is
	 * as checkpoint_shmem_init() left it -- except for 'completed', which
	 * says the data on disk is a finished checkpoint rather than one this
	 * process still has to drive.
	 */
	checkpoint_state->completed = true;

	elog(LOG, "orioledb rewind ended");
}
