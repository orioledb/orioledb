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

#include "orioledb.h"

#include "btree/io.h"
#include "btree/iterator.h"
#include "catalog/sys_trees.h"
#include "checkpoint/control.h"
#include "recovery/internal.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/timeline.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogrecovery.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/pg_bswap.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
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

typedef struct RewindKeysWalData
{
	uint16		version;
	uint16		flags;
	XLogRecPtr	switchpoint;
	uint64		total_length;
	uint32		total_checksum;
	uint64		offset;
	uint32		chunk_length;
} RewindKeysWalData;

static void
rewind_keys_wal_encode(WALRecRewindKeys *record, RewindKeysWalData *data)
{
	uint16		u16;
	uint32		u32;
	uint64		u64;
	char	   *ptr = (char *) record->data;

	u16 = pg_hton16(data->version);
	memcpy(ptr, &u16, sizeof(u16));
	ptr += sizeof(u16);
	u16 = pg_hton16(data->flags);
	memcpy(ptr, &u16, sizeof(u16));
	ptr += sizeof(u16);
	u64 = pg_hton64(data->switchpoint);
	memcpy(ptr, &u64, sizeof(u64));
	ptr += sizeof(u64);
	u64 = pg_hton64(data->total_length);
	memcpy(ptr, &u64, sizeof(u64));
	ptr += sizeof(u64);
	u32 = pg_hton32(data->total_checksum);
	memcpy(ptr, &u32, sizeof(u32));
	ptr += sizeof(u32);
	u64 = pg_hton64(data->offset);
	memcpy(ptr, &u64, sizeof(u64));
	ptr += sizeof(u64);
	u32 = pg_hton32(data->chunk_length);
	memcpy(ptr, &u32, sizeof(u32));
	ptr += sizeof(u32);
	Assert(ptr - (char *) record->data == REWIND_KEYS_WAL_HEADER_SIZE);
}

static void
rewind_keys_wal_decode(WALRecRewindKeys *record, RewindKeysWalData *data)
{
	uint16		u16;
	uint32		u32;
	uint64		u64;
	char	   *ptr = (char *) record->data;

	memcpy(&u16, ptr, sizeof(u16));
	data->version = pg_ntoh16(u16);
	ptr += sizeof(u16);
	memcpy(&u16, ptr, sizeof(u16));
	data->flags = pg_ntoh16(u16);
	ptr += sizeof(u16);
	memcpy(&u64, ptr, sizeof(u64));
	data->switchpoint = pg_ntoh64(u64);
	ptr += sizeof(u64);
	memcpy(&u64, ptr, sizeof(u64));
	data->total_length = pg_ntoh64(u64);
	ptr += sizeof(u64);
	memcpy(&u32, ptr, sizeof(u32));
	data->total_checksum = pg_ntoh32(u32);
	ptr += sizeof(u32);
	memcpy(&u64, ptr, sizeof(u64));
	data->offset = pg_ntoh64(u64);
	ptr += sizeof(u64);
	memcpy(&u32, ptr, sizeof(u32));
	data->chunk_length = pg_ntoh32(u32);
	ptr += sizeof(u32);
	Assert(ptr - (char *) record->data == REWIND_KEYS_WAL_HEADER_SIZE);
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
	OSnapshot committed_snapshot = {
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
process_key(StringInfo str, TableRow *row, KeyArrayElement *old_key,
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
			process_key(str, row, key_array[i], key_iter, td);
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
	bool		sync = false;
	XLogRecPtr	rec;

	rec = recovery_get_current_ptr();
	if (!single)
	{
		workers_send_oxid_finish(rec, false, true);
		sync = true;
		workers_synchronize(rec, false);
	}
	else
	{
		sync = true;
		pg_atomic_write_u64(recovery_ptr, rec);
	}

	recovery_finish_current_oxid(COMMITSEQNO_FROZEN, rec, -1, sync);
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

static void
ereport_rewind_error(File rewind_file, OIndexDescr *indexDescr,
					 int sys_tree_num, char *name)
{
	OIndexType	err_ix_type;
	ORelOids	err_oids;

	if (sys_tree_num > 0)
	{
		err_ix_type = oIndexPrimary;
		err_oids.datoid = SYS_TREES_DATOID;
		err_oids.reloid = sys_tree_num;
		err_oids.relnode = sys_tree_num;
	}
	else
	{
		err_ix_type = indexDescr->desc.type;
		err_oids = indexDescr->oids;
	}
	ereport(FATAL, (errcode_for_file_access(),
					errmsg("could not read "
						   "%s for "
						   "tree (%u %u %u %u) "
						   "from rewind file %s",
						   name,
						   err_ix_type,
						   err_oids.datoid,
						   err_oids.reloid,
						   err_oids.relnode,
						   FilePathName(rewind_file))));
}

static OTuple
replay_rewind_row(File rewind_file, char *read_buf, off_t *offset,
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
	if (OFileRead(rewind_file, (Pointer) read_buf,
				  item_header_size, *offset,
				  WAIT_EVENT_DATA_FILE_READ) !=
		item_header_size)
		ereport_rewind_error(rewind_file, indexDescr, sys_tree_num,
							 "item header");
	(*offset) += item_header_size;

	deleted = *(uint8 *) (read_buf);
	rewind_row.formatFlags = *(uint8 *) (read_buf +
										 sizeof(uint8));
	memcpy(&u32data, read_buf + sizeof(uint8) + sizeof(uint8), sizeof(uint32));
	item_len = pg_ntoh32(u32data);

	Assert(item_len < O_BTREE_MAX_TUPLE_SIZE * 2);
	if (OFileRead(rewind_file, (Pointer) read_buf,
				  item_len, *offset,
				  WAIT_EVENT_DATA_FILE_READ) !=
		item_len)
		ereport_rewind_error(rewind_file, indexDescr, sys_tree_num,
							 "item data");
	(*offset) += item_len;
	rewind_row.data = read_buf;

	if (indexDescr || (sys_tree_num > 0))
	{
		bool		old_toast_consistent = toast_consistent;

		toast_consistent = true;
/* TODO: Find out are we need real */
		/* toast_consistent value */

		/*
		 * Delete the target's version of the row and insert the source's one
		 * in a single transaction.  The insert finds the row deleted by its
		 * own transaction and rolls the deletion back first, which needs the
		 * undo record of the deletion.  The undo record is kept only while
		 * the transaction runs.
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
 * See add_divergence_rewind_keys() scheme.
 */
bool		rewind_keys_capturing = false;
static StringInfo rewind_keys = NULL;
static uint64 rewind_keys_count = 0;

typedef struct RewindKeysRedoState
{
	bool		active;
	XLogRecPtr	switchpoint;
	uint64		total_length;
	uint32		total_checksum;
	uint64		next_offset;
} RewindKeysRedoState;

static RewindKeysRedoState rewind_keys_redo_state = {0};

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
		 * their trees just like WAL do, so pg_rewind takes these keys just like
		 * the ones it finds it at WAL.
		 *
		 * Callers of add_modify_wal_record_extended logs:
		 *  - Primary index as a table row.
		 *	  - TOAST and bridge index rows are logged by records on their own.  They
		 *		are registered under their own oids, so neither follows from the table
		 *		rows, so the rewind needs their keys too.
		 *  - Secondary index rows are not logged at all.  A table row is applied
		 *	  via apply_tbl_modify_record() -- this function updates the secondary
		 *	  indexes too.
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
	rewind_keys_count++;
	MemoryContextSwitchTo(oldcxt);

	if (key_allocated)
		pfree(key.data);
}


static uint32
rewind_keys_checksum(const char *data, uint64 len)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, data, len);
	FIN_CRC32C(crc);
	return (uint32) crc;
}

static void
rewind_keys_write_all(int fd, const char *path, const char *data, uint64 len)
{
	uint64		offset = 0;

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
}

static void
rewind_keys_pread_all(int fd, const char *path, char *data, uint64 len,
					  off_t offset)
{
	uint64		done = 0;

	while (done < len)
	{
		ssize_t		read_len = pg_pread(fd, data + done, len - done,
									 offset + done);

		if (read_len < 0 && errno == EINTR)
			continue;
		if (read_len < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read rewind keys file \"%s\" at offset %lld: %m",
							path, (long long) (offset + done))));
		if (read_len == 0)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unexpected end of rewind keys file \"%s\" at offset %lld",
							path, (long long) (offset + done))));
		done += read_len;
	}
}

static void
rewind_keys_pwrite_all(int fd, const char *path, const char *data, uint64 len,
					   off_t offset, int elevel)
{
	uint64		done = 0;

	while (done < len)
	{
		ssize_t		written = pg_pwrite(fd, data + done, len - done,
									 offset + done);

		if (written < 0 && errno == EINTR)
			continue;
		if (written <= 0)
			ereport(elevel,
					(errcode_for_file_access(),
					 errmsg("could not write rewind keys file \"%s\" at offset %lld: %m",
							path, (long long) (offset + done))));
		done += written;
	}
}

static void
rewind_keys_write_file(const char *path, const char *data, uint64 len)
{
	int			fd;

	fd = OpenTransientFile(path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open rewind keys file \"%s\": %m", path)));
	rewind_keys_write_all(fd, path, data, len);
	if (pg_fsync(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not sync rewind keys file \"%s\": %m", path)));
	if (CloseTransientFile(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close rewind keys file \"%s\": %m", path)));
}

static StringInfo
rewind_keys_part_header(XLogRecPtr generation, int participant,
						int participants, uint64 count, uint64 payload_length,
						uint32 checksum)
{
	StringInfo	header = makeStringInfo();

	appendHton32StringInfo(header, REWIND_KEYS_PART_MAGIC);
	appendHton16StringInfo(header, REWIND_KEYS_FORMAT_VERSION);
	appendHton16StringInfo(header, REWIND_KEYS_PART_HEADER_SIZE);
	appendHton64StringInfo(header, generation);
	appendHton32StringInfo(header, (uint32) participant);
	appendHton32StringInfo(header, (uint32) participants);
	appendHton64StringInfo(header, count);
	appendHton64StringInfo(header, payload_length);
	appendHton32StringInfo(header, checksum);
	appendHton32StringInfo(header, 0);
	Assert(header->len == REWIND_KEYS_PART_HEADER_SIZE);
	return header;
}

#define REWIND_KEY_ENTRY_HEADER_SIZE	(5 * sizeof(uint32) + sizeof(uint8) + sizeof(uint32))
#define REWIND_KEYS_COPY_BUFFER_SIZE	(64 * 1024)

static StringInfo
rewind_keys_final_header(XLogRecPtr switchpoint, uint64 count,
						 uint64 payload_length, uint32 checksum)
{
	StringInfo	header = makeStringInfo();

	appendHton32StringInfo(header, REWIND_KEYS_MAGIC);
	appendHton16StringInfo(header, REWIND_KEYS_FORMAT_VERSION);
	appendHton16StringInfo(header, REWIND_KEYS_FINAL_HEADER_SIZE);
	appendHton64StringInfo(header, switchpoint);
	appendHton64StringInfo(header, count);
	appendHton64StringInfo(header, payload_length);
	appendHton32StringInfo(header, checksum);
	Assert(header->len == REWIND_KEYS_FINAL_HEADER_SIZE);
	return header;
}

static void
rewind_keys_validate_final_file(int fd, const char *path,
							XLogRecPtr switchpoint, uint64 file_length,
							uint64 *entry_count, uint32 *whole_checksum)
{
	char		header[REWIND_KEYS_FINAL_HEADER_SIZE];
	char		entry_header[REWIND_KEY_ENTRY_HEADER_SIZE];
	char		buffer[REWIND_KEYS_COPY_BUFFER_SIZE];
	uint16		u16;
	uint32		u32;
	uint64		u64;
	uint64		declared_count;
	uint64		payload_length;
	uint32		payload_checksum;
	uint64		offset = 0;
	uint64		count = 0;
	pg_crc32c	payload_crc;
	pg_crc32c	whole_crc;

	if (file_length < sizeof(header))
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("rewind keys file \"%s\" is truncated", path)));
	rewind_keys_pread_all(fd, path, header, sizeof(header), 0);
	memcpy(&u32, header, 4);
	if (pg_ntoh32(u32) != REWIND_KEYS_MAGIC)
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("invalid rewind keys file \"%s\"", path)));
	memcpy(&u16, header + 4, 2);
	if (pg_ntoh16(u16) != REWIND_KEYS_FORMAT_VERSION)
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("unsupported rewind keys file version in \"%s\"", path)));
	memcpy(&u16, header + 6, 2);
	if (pg_ntoh16(u16) != REWIND_KEYS_FINAL_HEADER_SIZE)
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("invalid rewind keys header length in \"%s\"", path)));
	memcpy(&u64, header + 8, 8);
	if (pg_ntoh64(u64) != switchpoint)
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("wrong switchpoint in rewind keys file \"%s\"", path)));
	memcpy(&u64, header + 16, 8);
	declared_count = pg_ntoh64(u64);
	memcpy(&u64, header + 24, 8);
	payload_length = pg_ntoh64(u64);
	memcpy(&u32, header + 32, 4);
	payload_checksum = pg_ntoh32(u32);
	if (file_length != sizeof(header) + payload_length)
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("invalid payload length in rewind keys file \"%s\"", path)));

	INIT_CRC32C(payload_crc);
	INIT_CRC32C(whole_crc);
	COMP_CRC32C(whole_crc, header, sizeof(header));
	while (offset < payload_length)
	{
		ORelOids	oids;
		OIndexType	type;
		uint32		key_length;
		uint64		key_offset = 0;

		if (payload_length - offset < sizeof(entry_header))
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("truncated entry in rewind keys file \"%s\"", path)));
		rewind_keys_pread_all(fd, path, entry_header, sizeof(entry_header),
							  sizeof(header) + offset);
		memcpy(&u32, entry_header, 4);
		type = (OIndexType) pg_ntoh32(u32);
		memcpy(&u32, entry_header + 4, 4);
		oids.datoid = pg_ntoh32(u32);
		memcpy(&u32, entry_header + 8, 4);
		oids.reloid = pg_ntoh32(u32);
		memcpy(&u32, entry_header + 12, 4);
		oids.relnode = pg_ntoh32(u32);
		memcpy(&u32, entry_header + 16, 4);
		oids.spcoid = pg_ntoh32(u32);
		memcpy(&u32, entry_header + 21, 4);
		key_length = pg_ntoh32(u32);
		if (type < oIndexInvalid || type > oIndexExclusion ||
			(!IS_SYS_TREE_OIDS(oids) && type != oIndexInvalid &&
			 type != oIndexToast && type != oIndexBridge) ||
			key_length == 0 ||
			payload_length - offset - sizeof(entry_header) < key_length)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("invalid entry in rewind keys file \"%s\"", path)));
		COMP_CRC32C(payload_crc, entry_header, sizeof(entry_header));
		COMP_CRC32C(whole_crc, entry_header, sizeof(entry_header));
		offset += sizeof(entry_header);
		while (key_offset < key_length)
		{
			uint32		chunk = Min((uint64) sizeof(buffer),
								 key_length - key_offset);

			rewind_keys_pread_all(fd, path, buffer, chunk,
								  sizeof(header) + offset + key_offset);
			COMP_CRC32C(payload_crc, buffer, chunk);
			COMP_CRC32C(whole_crc, buffer, chunk);
			key_offset += chunk;
		}
		offset += key_length;
		count++;
	}
	FIN_CRC32C(payload_crc);
	FIN_CRC32C(whole_crc);
	if (count != declared_count || (uint32) payload_crc != payload_checksum)
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("entry count or checksum mismatch in rewind keys file \"%s\"",
							   path)));
	if (entry_count != NULL)
		*entry_count = count;
	if (whole_checksum != NULL)
		*whole_checksum = (uint32) whole_crc;
}

void
rewind_keys_finish_process(int worker_id)
{
	char		writing[MAXPGPATH];
	char		part[MAXPGPATH];
	XLogRecPtr	generation = pg_atomic_read_u64(recovery_rewind_keys_generation);
	int			participants = pg_atomic_read_u32(recovery_rewind_keys_participants);
	const char *payload = rewind_keys == NULL ? "" : rewind_keys->data;
	uint64		payload_length = rewind_keys == NULL ? 0 : rewind_keys->len;
	StringInfo	header;
	StringInfo	file;

	rewind_keys_capturing = false;
	if (!XLogRecPtrIsValid(generation))
	{
		Assert(participants == 0);
		if (rewind_keys != NULL)
		{
			pfree(rewind_keys->data);
			pfree(rewind_keys);
			rewind_keys = NULL;
		}
		rewind_keys_count = 0;
		return;
	}
	if (participants <= 0)
		elog(FATAL, "invalid rewind key participant count %d", participants);
	header = rewind_keys_part_header(generation, worker_id, participants,
								 rewind_keys_count, payload_length,
								 rewind_keys_checksum(payload, payload_length));
	file = makeStringInfo();
	appendBinaryStringInfoNT(file, header->data, header->len);
	appendBinaryStringInfoNT(file, payload, payload_length);

	snprintf(writing, sizeof(writing), REWIND_KEYS_WRITING_FORMAT,
			 LSN_FORMAT_ARGS(generation), worker_id);
	snprintf(part, sizeof(part), REWIND_KEYS_PART_FORMAT,
			 LSN_FORMAT_ARGS(generation), worker_id);
	rewind_keys_write_file(writing, file->data, file->len);
	if (durable_rename(writing, part, ERROR) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not publish rewind keys part \"%s\": %m", part)));

	if (rewind_keys != NULL)
	{
		pfree(rewind_keys->data);
		pfree(rewind_keys);
	}
	pfree(header->data);
	pfree(header);
	pfree(file->data);
	pfree(file);

	rewind_keys = NULL;
	rewind_keys_count = 0;
}

void
rewind_keys_prepare(XLogRecPtr generation, int participants)
{
	char		ready[MAXPGPATH];
	char		newpath[MAXPGPATH];
	char		zero_header[REWIND_KEYS_FINAL_HEADER_SIZE] = {0};
	char		entry_header[REWIND_KEY_ENTRY_HEADER_SIZE];
	char		copy_buffer[REWIND_KEYS_COPY_BUFFER_SIZE];
	uint64		total_count = 0;
	uint64		total_length = 0;
	pg_crc32c	total_crc;
	int			output;
	int			part_no;

	if (!XLogRecPtrIsValid(generation) || participants <= 0)
		elog(FATAL, "invalid rewind key generation or participant count");
	if (generation != pg_atomic_read_u64(recovery_rewind_keys_generation) ||
		participants != pg_atomic_read_u32(recovery_rewind_keys_participants))
		elog(FATAL, "rewind key generation changed during recovery finish");

	snprintf(ready, sizeof(ready), REWIND_KEYS_READY_FORMAT,
			 LSN_FORMAT_ARGS(generation));
	if (access(ready, F_OK) == 0)
	{
		struct stat st;
		int			fd = OpenTransientFile(ready, O_RDONLY | PG_BINARY);

		if (fd < 0 || fstat(fd, &st) != 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not open rewind keys ready file \"%s\": %m", ready)));
		rewind_keys_validate_final_file(fd, ready, generation, st.st_size,
									NULL, NULL);
		if (CloseTransientFile(fd) != 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not close rewind keys ready file \"%s\": %m", ready)));
		for (part_no = 0; part_no < participants; part_no++)
		{
			int			participant = part_no == 0 ? -1 : part_no - 1;
			char		part[MAXPGPATH];

			snprintf(part, sizeof(part), REWIND_KEYS_PART_FORMAT,
					 LSN_FORMAT_ARGS(generation), participant);
			if (unlink(part) < 0 && errno != ENOENT)
				ereport(WARNING, (errcode_for_file_access(),
								errmsg("could not remove rewind keys part \"%s\": %m", part)));
		}
		return;
	}
	STOPEVENT(STOPEVENT_REWIND_KEYS_BEFORE_READY, NULL);
	snprintf(newpath, sizeof(newpath), "%s.new", ready);
	if (unlink(newpath) < 0 && errno != ENOENT)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not remove incomplete rewind keys file \"%s\": %m",
							   newpath)));
	output = OpenTransientFile(newpath,
							   O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (output < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not create rewind keys file \"%s\": %m", newpath)));
	rewind_keys_write_all(output, newpath, zero_header, sizeof(zero_header));
	INIT_CRC32C(total_crc);

	for (part_no = 0; part_no < participants; part_no++)
	{
		int			participant = part_no == 0 ? -1 : part_no - 1;
		char		part[MAXPGPATH];
		char		header[REWIND_KEYS_PART_HEADER_SIZE];
		uint16		u16;
		uint32		u32;
		uint64		u64;
		uint64		part_count;
		uint64		part_length;
		uint32		part_checksum;
		uint64		part_offset = 0;
		uint64		entries = 0;
		pg_crc32c	part_crc;
		struct stat st;
		int			input;

		snprintf(part, sizeof(part), REWIND_KEYS_PART_FORMAT,
				 LSN_FORMAT_ARGS(generation), participant);
		input = OpenTransientFile(part, O_RDONLY | PG_BINARY);
		if (input < 0 || fstat(input, &st) != 0 ||
			st.st_size < REWIND_KEYS_PART_HEADER_SIZE)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("missing or invalid rewind keys part \"%s\"", part)));
		rewind_keys_pread_all(input, part, header, sizeof(header), 0);
		memcpy(&u32, header, sizeof(u32));
		if (pg_ntoh32(u32) != REWIND_KEYS_PART_MAGIC)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("invalid rewind keys part \"%s\"", part)));
		memcpy(&u16, header + 4, sizeof(u16));
		if (pg_ntoh16(u16) != REWIND_KEYS_FORMAT_VERSION)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("unsupported rewind keys part version in \"%s\"", part)));
		memcpy(&u16, header + 6, sizeof(u16));
		if (pg_ntoh16(u16) != REWIND_KEYS_PART_HEADER_SIZE)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("invalid rewind keys header length in \"%s\"", part)));
		memcpy(&u64, header + 8, sizeof(u64));
		if (pg_ntoh64(u64) != generation)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("stale rewind keys part \"%s\"", part)));
		memcpy(&u32, header + 16, sizeof(u32));
		if ((int32) pg_ntoh32(u32) != participant)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("wrong participant in rewind keys part \"%s\"", part)));
		memcpy(&u32, header + 20, sizeof(u32));
		if (pg_ntoh32(u32) != (uint32) participants)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("wrong participant count in rewind keys part \"%s\"", part)));
		memcpy(&u64, header + 24, sizeof(u64));
		part_count = pg_ntoh64(u64);
		memcpy(&u64, header + 32, sizeof(u64));
		part_length = pg_ntoh64(u64);
		memcpy(&u32, header + 40, sizeof(u32));
		part_checksum = pg_ntoh32(u32);
		if ((uint64) st.st_size != REWIND_KEYS_PART_HEADER_SIZE + part_length)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("invalid length in rewind keys part \"%s\"", part)));

		INIT_CRC32C(part_crc);
		while (part_offset < part_length)
		{
			ORelOids	oids;
			OIndexType	type;
			uint32		key_length;
			uint64		key_offset = 0;

			if (part_length - part_offset < sizeof(entry_header))
				ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("truncated entry in rewind keys part \"%s\"", part)));
			rewind_keys_pread_all(input, part, entry_header,
								  sizeof(entry_header),
								  REWIND_KEYS_PART_HEADER_SIZE + part_offset);
			memcpy(&u32, entry_header, sizeof(u32));
			type = (OIndexType) pg_ntoh32(u32);
			memcpy(&u32, entry_header + 4, sizeof(u32));
			oids.datoid = pg_ntoh32(u32);
			memcpy(&u32, entry_header + 8, sizeof(u32));
			oids.reloid = pg_ntoh32(u32);
			memcpy(&u32, entry_header + 12, sizeof(u32));
			oids.relnode = pg_ntoh32(u32);
			memcpy(&u32, entry_header + 16, sizeof(u32));
			oids.spcoid = pg_ntoh32(u32);
			memcpy(&u32, entry_header + 21, sizeof(u32));
			key_length = pg_ntoh32(u32);
			if (type < oIndexInvalid || type > oIndexExclusion ||
				(!IS_SYS_TREE_OIDS(oids) && type != oIndexInvalid &&
				 type != oIndexToast && type != oIndexBridge) ||
				key_length == 0 ||
				part_length - part_offset - sizeof(entry_header) < key_length)
				ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("invalid entry in rewind keys part \"%s\"", part)));

			rewind_keys_write_all(output, newpath, entry_header,
							  sizeof(entry_header));
			COMP_CRC32C(part_crc, entry_header, sizeof(entry_header));
			COMP_CRC32C(total_crc, entry_header, sizeof(entry_header));
			part_offset += sizeof(entry_header);
			total_length += sizeof(entry_header);
			while (key_offset < key_length)
			{
				uint32		chunk = Min((uint64) sizeof(copy_buffer),
									 key_length - key_offset);

				rewind_keys_pread_all(input, part, copy_buffer, chunk,
									  REWIND_KEYS_PART_HEADER_SIZE +
									  part_offset + key_offset);
				rewind_keys_write_all(output, newpath, copy_buffer, chunk);
				COMP_CRC32C(part_crc, copy_buffer, chunk);
				COMP_CRC32C(total_crc, copy_buffer, chunk);
				key_offset += chunk;
			}
			part_offset += key_length;
			total_length += key_length;
			entries++;
		}
		FIN_CRC32C(part_crc);
		if (entries != part_count || (uint32) part_crc != part_checksum)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("entry count or checksum mismatch in rewind keys part \"%s\"",
								   part)));
		total_count += entries;
		if (CloseTransientFile(input) != 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not close rewind keys part \"%s\": %m", part)));
	}

	FIN_CRC32C(total_crc);
	{
		StringInfo	header = rewind_keys_final_header(generation, total_count,
											   total_length,
											   (uint32) total_crc);

		rewind_keys_pwrite_all(output, newpath, header->data, header->len, 0,
							  FATAL);
		pfree(header->data);
		pfree(header);
	}
	if (pg_fsync(output) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not sync rewind keys file \"%s\": %m", newpath)));
	if (CloseTransientFile(output) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not close rewind keys file \"%s\": %m", newpath)));
	if (durable_rename(newpath, ready, ERROR) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not publish rewind keys ready file \"%s\": %m", ready)));
	STOPEVENT(STOPEVENT_REWIND_KEYS_AFTER_READY, NULL);

	for (part_no = 0; part_no < participants; part_no++)
	{
		int			participant = part_no == 0 ? -1 : part_no - 1;
		char		part[MAXPGPATH];

		snprintf(part, sizeof(part), REWIND_KEYS_PART_FORMAT,
				 LSN_FORMAT_ARGS(generation), participant);
		if (unlink(part) < 0 && errno != ENOENT)
			ereport(WARNING, (errcode_for_file_access(),
							errmsg("could not remove rewind keys part \"%s\": %m", part)));
	}
}

void
rewind_keys_resume_prepare(void)
{
	DIR		   *dir;
	struct dirent *de;

	dir = AllocateDir(ORIOLEDB_DATA_DIR);
	while ((de = ReadDir(dir, ORIOLEDB_DATA_DIR)) != NULL)
	{
		uint32		hi;
		uint32		lo;
		char		stop;
		size_t		namelen = strlen(de->d_name);
		char		path[MAXPGPATH];
		char		header[REWIND_KEYS_PART_HEADER_SIZE];
		uint32		u32;
		uint64		u64;
		XLogRecPtr	generation;
		int			participants;
		int			fd;

		if (namelen < strlen(".-1.part") ||
			strcmp(de->d_name + namelen - strlen(".-1.part"),
				   ".-1.part") != 0 ||
			sscanf(de->d_name, "rewind_keys.%08X%08X.-1.part%c",
				   &hi, &lo, &stop) != 2)
			continue;
		generation = ((uint64) hi << 32) | lo;
		snprintf(path, sizeof(path), ORIOLEDB_DATA_DIR "/%s", de->d_name);
		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not open rewind keys part \"%s\": %m", path)));
		rewind_keys_pread_all(fd, path, header, sizeof(header), 0);
		memcpy(&u64, header + 8, sizeof(u64));
		if (pg_ntoh64(u64) != generation)
			ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("stale rewind keys part \"%s\"", path)));
		memcpy(&u32, header + 20, sizeof(u32));
		participants = pg_ntoh32(u32);
		if (CloseTransientFile(fd) != 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not close rewind keys part \"%s\": %m", path)));
		pg_atomic_write_u64(recovery_rewind_keys_generation, generation);
		pg_atomic_write_u32(recovery_rewind_keys_participants, participants);
		rewind_keys_prepare(generation, participants);
	}
	FreeDir(dir);
}

#define PG_REWIND_KEYS_WAL_CHUNK (1024 * 1024)

static void
log_rewind_keys(XLogRecPtr switchpoint, int fd, const char *path, uint64 len,
				uint32 checksum)
{
	XLogRecPtr	lsn = InvalidXLogRecPtr;
	uint64		offset = 0;
	char	   *data = palloc(PG_REWIND_KEYS_WAL_CHUNK);

	do
	{
		WALRecRewindKeys rec;
		RewindKeysWalData rec_data;
		uint32		chunk = Min(len - offset, PG_REWIND_KEYS_WAL_CHUNK);

		rec_data.version = REWIND_KEYS_FORMAT_VERSION;
		rec_data.switchpoint = switchpoint;
		rec_data.total_length = len;
		rec_data.total_checksum = checksum;
		rec_data.offset = offset;
		rec_data.chunk_length = chunk;
		rec_data.flags = (offset == 0 ? REWIND_KEYS_FIRST : 0) |
					((offset + chunk) == len ? REWIND_KEYS_LAST : 0);
		rewind_keys_wal_encode(&rec, &rec_data);
		rewind_keys_pread_all(fd, path, data, chunk, offset);

		XLogBeginInsert();
		XLogRegisterData((char *)&rec, sizeof(rec));
		XLogRegisterData(data, chunk);
		lsn = XLogInsert(ORIOLEDB_RMGR_ID, ORIOLEDB_XLOG_REWIND_KEYS);
		offset += chunk;
	} while (offset < len || !XLogRecPtrIsValid(lsn));

	XLogFlush(lsn);
	pfree(data);
}


/*
 * Redo of ORIOLEDB_XLOG_REWIND_KEYS: rebuild on this node the rewind
 * keys file that the node promoted at rec.switchpoint saved (see
 * rewind_keys_save()).
 */
void
rewind_keys_redo(XLogReaderState *record)
{
	WALRecRewindKeys rec;
	RewindKeysWalData rec_data;
	char *data;
	int len;
	char path [MAXPGPATH];
	char tmppath [MAXPGPATH];
	int fd;
	struct stat st;

	if (XLogRecGetDataLen(record) < sizeof(rec))
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("truncated OrioleDB rewind keys WAL record")));
	data = XLogRecGetData(record) + sizeof(rec);
	len = XLogRecGetDataLen(record) - sizeof(rec);
	memcpy(&rec, XLogRecGetData(record), sizeof (rec));
	rewind_keys_wal_decode(&rec, &rec_data);
	if (rec_data.version != REWIND_KEYS_FORMAT_VERSION ||
		rec_data.chunk_length != len ||
		rec_data.offset > rec_data.total_length ||
		len > rec_data.total_length - rec_data.offset ||
		(rec_data.flags & ~(REWIND_KEYS_FIRST | REWIND_KEYS_LAST)) != 0 ||
		((rec_data.flags & REWIND_KEYS_FIRST) != 0) !=
		(rec_data.offset == 0) ||
		((rec_data.flags & REWIND_KEYS_LAST) != 0) !=
		(rec_data.offset + len == rec_data.total_length))
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid OrioleDB rewind keys WAL record")));
	snprintf(path, sizeof (path), REWIND_KEYS_FILENAME_FORMAT,
			 LSN_FORMAT_ARGS(rec_data.switchpoint));
	snprintf(tmppath, sizeof(tmppath), "%s.new", path);

	if (rec_data.flags & REWIND_KEYS_FIRST)
	{
		/*
		 * Start the file over: it may be left over from an earlier replay
		 * of this record.
		 */
		fd = OpenTransientFile(tmppath, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
		rewind_keys_redo_state.active = true;
		rewind_keys_redo_state.switchpoint = rec_data.switchpoint;
		rewind_keys_redo_state.total_length = rec_data.total_length;
		rewind_keys_redo_state.total_checksum = rec_data.total_checksum;
		rewind_keys_redo_state.next_offset = 0;
	}
	else
	{
		/* First chunk created the file, so later chunks go into this.  */
		fd = OpenTransientFile(tmppath, O_WRONLY | PG_BINARY);
		if (fd < 0 || fstat(fd, &st) != 0 ||
			(uint64) st.st_size != rec_data.offset)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("non-contiguous rewind keys WAL at offset " UINT64_FORMAT,
							rec_data.offset)));
		if (!rewind_keys_redo_state.active)
		{
			/* Restartpoint may resume from a durably fsynced .new file. */
			rewind_keys_redo_state.active = true;
			rewind_keys_redo_state.switchpoint = rec_data.switchpoint;
			rewind_keys_redo_state.total_length = rec_data.total_length;
			rewind_keys_redo_state.total_checksum = rec_data.total_checksum;
			rewind_keys_redo_state.next_offset = rec_data.offset;
		}
	}
	if (!rewind_keys_redo_state.active ||
		rewind_keys_redo_state.switchpoint != rec_data.switchpoint ||
		rewind_keys_redo_state.total_length != rec_data.total_length ||
		rewind_keys_redo_state.total_checksum != rec_data.total_checksum ||
		rewind_keys_redo_state.next_offset != rec_data.offset)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("inconsistent rewind keys WAL sequence")));

	if (fd < 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open rewind keys file \"%s\": %m", tmppath)));
	}
	rewind_keys_pwrite_all(fd, tmppath, data, len, rec_data.offset, PANIC);
	if (pg_fsync(fd) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not sync rewind keys file \"%s\": %m", tmppath)));
	rewind_keys_redo_state.next_offset += len;

	if (CloseTransientFile(fd) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close rewind keys file \"%s\": %m", tmppath)));

	if (rec_data.flags & REWIND_KEYS_LAST)
	{
		uint32		whole_checksum;
		fd = OpenTransientFile(tmppath, O_RDONLY | PG_BINARY);
		if (fd < 0 || fstat(fd, &st) != 0 ||
			st.st_size != rec_data.total_length)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid rewind keys redo file \"%s\"", tmppath)));
		rewind_keys_validate_final_file(fd, tmppath, rec_data.switchpoint,
									st.st_size, NULL, &whole_checksum);
		if (whole_checksum != rec_data.total_checksum)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("rewind keys redo checksum mismatch for \"%s\"", tmppath)));
		if (CloseTransientFile(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not close rewind keys file \"%s\": %m", tmppath)));
		if (durable_rename(tmppath, path, PANIC) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not publish rewind keys file \"%s\": %m", path)));
		rewind_keys_redo_state.active = false;
	}
}

static void
rewind_keys_publish_ready(const char *ready, XLogRecPtr switchpoint)
{
	char		path[MAXPGPATH];
	struct stat st;
	uint32		whole_checksum;
	int			fd;

	fd = OpenTransientFile(ready, O_RDONLY | PG_BINARY);
	if (fd < 0 || fstat(fd, &st) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read rewind keys ready file \"%s\"", ready)));
	rewind_keys_validate_final_file(fd, ready, switchpoint, st.st_size,
									NULL, &whole_checksum);

	log_rewind_keys(switchpoint, fd, ready, st.st_size, whole_checksum);
	STOPEVENT(STOPEVENT_REWIND_KEYS_AFTER_WAL_FLUSH, NULL);
	if (CloseTransientFile(fd) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not close rewind keys ready file \"%s\": %m", ready)));
	snprintf(path, sizeof(path), REWIND_KEYS_FILENAME_FORMAT,
			 LSN_FORMAT_ARGS(switchpoint));
	if (durable_rename(ready, path, ERROR) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not publish rewind keys file \"%s\": %m", path)));
}

/*
 * Called by the startup process at the end of recovery.  A new timeline
 * means a promotion: save the keys under its switchpoint, and log them
 * for the standbys.
 */
void
rewind_keys_save(XLogRecPtr switchpoint)
{
	char		path[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	/* Complete publication left pending by a crash in an earlier startup. */
	dir = AllocateDir(ORIOLEDB_DATA_DIR);
	while ((de = ReadDir(dir, ORIOLEDB_DATA_DIR)) != NULL)
	{
		uint32		hi;
		uint32		lo;
		char		stop;
		XLogRecPtr	pending_switchpoint;
		char		pending[MAXPGPATH];
		size_t		namelen = strlen(de->d_name);

		if (namelen < strlen(".ready") ||
			strcmp(de->d_name + namelen - strlen(".ready"), ".ready") != 0 ||
			sscanf(de->d_name, "rewind_keys.%08X%08X.ready%c",
				   &hi, &lo, &stop) != 2)
			continue;
		pending_switchpoint = ((uint64) hi << 32) | lo;
		snprintf(pending, sizeof(pending), ORIOLEDB_DATA_DIR "/%s",
				 de->d_name);
		rewind_keys_publish_ready(pending, pending_switchpoint);
	}
	FreeDir(dir);

	if (!ArchiveRecoveryRequested)
		return;
	if (switchpoint != pg_atomic_read_u64(recovery_rewind_keys_generation))
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("rewind key switchpoint changed from %X/%X to %X/%X",
							   LSN_FORMAT_ARGS(pg_atomic_read_u64(recovery_rewind_keys_generation)),
							   LSN_FORMAT_ARGS(switchpoint))));
	snprintf(path, sizeof (path), REWIND_KEYS_FILENAME_FORMAT,
			 LSN_FORMAT_ARGS(switchpoint));
	if (access(path, F_OK) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("rewind keys file \"%s\" was not published: %m", path)));
}

/*
 * Remove the rewind keys files nobody can ask for anymore.  The file of a
 * switchpoint might be necessary until another node could be rewound onto
 * this one with the timeline forket here.  Two options here:
 *
 * - a timeline of this node that begins at the switchpoint.  After this node
 *   is rewound itself its history changes, and nobody forks from the old one.
 * - the WAL around the switchpoint, which the rewound node replays from this
 *   one.  Once it is removed, no one can catch up from here.
 */
void
rewind_keys_cleanup(void)
{
	TimeLineID tli;
	List *history;
	XLogSegNo lastRemovedSegNo = XLogGetLastRemovedSegno();
	DIR *dir;
	struct dirent *de;

	if (RecoveryInProgress())
		(void)GetXLogReplayRecPtr(&tli);
	else
		tli = GetWALInsertionTimeLine();

	tli = findNewestTimeLine(tli);
	history = readTimeLineHistory(tli);

	dir = AllocateDir(ORIOLEDB_DATA_DIR);
	while ((de = ReadDir(dir, ORIOLEDB_DATA_DIR)) != NULL)
	{
		uint32 hi, lo;
		char stop_sym;
		XLogRecPtr switchpoint;
		ListCell *lc;
		bool in_history = false;
		const char *reason = NULL;
		char path [MAXPGPATH];
		XLogSegNo segno;

		/*
		 * Here we intentionally skip rewind_keys_<switchpoint>.{new, tmp}.
		 * The final version is rewind_keys_<switchpoint> format.
		 */
		if (sscanf(de->d_name, "rewind_keys_%08X%08X%c", &hi, &lo, &stop_sym) != 2)
			continue;

		/* Check that this switchpoint is still actual.  */
		switchpoint = ((uint64)hi << 32) | lo;

		foreach(lc, history)
		{
			TimeLineHistoryEntry *entry = (TimeLineHistoryEntry *)lfirst(lc);
			if (entry->begin == switchpoint)
				in_history = true;
		}
		XLByteToSeg(switchpoint, segno, wal_segment_size);

		if (!in_history)
			reason = "no timeline of this server begins at its switchpoint";
		else if (!XLogArchivingActive() &&
				 lastRemovedSegNo > 0 &&
				 segno <= lastRemovedSegNo)
			reason = "the WAL after its switchpoint is removed";
		else
			continue;

		snprintf(path, sizeof(path), ORIOLEDB_DATA_DIR "/%s", de->d_name);
		if (unlink(path) == 0)
			elog(LOG,  "OrioleDB: removed rewind keys file \"%s\": %s",
				 path, reason);
		else if (errno != ENOENT)
			ereport(WARNING, (errcode_for_file_access(),
							  errmsg("could not remove rewind keys file \"%s\"", path)));
	}

	FreeDir(dir);
	list_free_deep(history);
}

void
rewind_keys_desc(StringInfo buf, XLogReaderState *record)
{
	WALRecRewindKeys rec;
	RewindKeysWalData rec_data;

	memcpy(&rec, XLogRecGetData(record), sizeof(rec));
	rewind_keys_wal_decode(&rec, &rec_data);
	appendStringInfo(buf, "rewind keys of the promotion at %X/%X: %u bytes at offset " UINT64_FORMAT "%s%s",
					 LSN_FORMAT_ARGS(rec_data.switchpoint),
					 (unsigned) (XLogRecGetDataLen(record) - sizeof(rec)),
					 rec_data.offset,
					 (rec_data.flags & REWIND_KEYS_FIRST) ? ", first" : "",
					 (rec_data.flags & REWIND_KEYS_LAST) ? ", last" : "");
}

/*
 * Replays rewind file.
 */
void
replay_rewind(uint32 chkp_num, bool single)
{
	File		rewind_file;
	char		read_buf[O_BTREE_MAX_TUPLE_SIZE * 2];
	OIndexType	ix_type = oIndexInvalid;
	ORelOids	cur_oids = {0, 0, 0};
	off_t		offset = 0;
	int			readed;
	const int	tree_header_size = sizeof(OIndexType) + 4 * sizeof(Oid) +
		sizeof(uint8);
	uint32		nkeys;
	uint8		found;
	XLogRecPtr	startpoint;
	OXid		lastXid;

	rewind_file = PathNameOpenFile(ORIOLEDB_DATA_DIR "/rewind",
								   O_RDONLY | PG_BINARY);

	if (rewind_file < 0)
		return;

	elog(LOG, "orioledb rewind started");

	Assert(tree_header_size < O_BTREE_MAX_TUPLE_SIZE * 2);

	readed = OFileRead(rewind_file, (Pointer) &read_buf, sizeof(XLogRecPtr),
					   offset, WAIT_EVENT_DATA_FILE_READ);
	offset += sizeof(XLogRecPtr);
	if (readed != sizeof(XLogRecPtr))
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read startpoint from rewind file %s",
							   FilePathName(rewind_file))));

	startpoint = *((XLogRecPtr *) (read_buf));

	readed = OFileRead(rewind_file, (Pointer) &read_buf, sizeof(OXid),
					   offset, WAIT_EVENT_DATA_FILE_READ);
	offset += sizeof(OXid);
	if (readed != sizeof(OXid))
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read lastXid from rewind file %s",
							   FilePathName(rewind_file))));
	lastXid = *((OXid *) (read_buf));

	readed = OFileRead(rewind_file, (Pointer) &read_buf, tree_header_size,
					   offset, WAIT_EVENT_DATA_FILE_READ);
	while (readed == tree_header_size)
	{
		int			sys_tree_num = -1;

		offset += tree_header_size;

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

			if (OFileRead(rewind_file, (Pointer) &read_buf, sizeof(uint32),
						  offset, WAIT_EVENT_DATA_FILE_READ) != sizeof(uint32))
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not read keys amount for "
									   "tree (%u %u %u %u) "
									   "from rewind file %s",
									   ix_type,
									   cur_oids.datoid,
									   cur_oids.reloid,
									   cur_oids.relnode,
									   FilePathName(rewind_file))));
			offset += sizeof(uint32);
			nkeys = pg_ntoh32(*((uint32 *) (read_buf)));

			sys_tree_num = fill_descrs(cur_oids, ix_type, &descr, &indexDescr);

			for (i = 0; i < nkeys; i++)
			{
				int			j;
				int			row_header_size = sizeof(uint8);
				OTuple		rewind_row = {0};

				if (OFileRead(rewind_file, (Pointer) &read_buf,
							  row_header_size, offset,
							  WAIT_EVENT_DATA_FILE_READ) != row_header_size)
					ereport(FATAL, (errcode_for_file_access(),
									errmsg("could not read row header for "
										   "tree (%u %u %u %u) "
										   "from rewind file %s",
										   ix_type,
										   cur_oids.datoid,
										   cur_oids.reloid,
										   cur_oids.relnode,
										   FilePathName(rewind_file))));
				offset += row_header_size;

				found = *((uint8 *) (read_buf));

				rewind_row = replay_rewind_row(rewind_file, read_buf, &offset,
											   descr, indexDescr,
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

					if (OFileRead(rewind_file, (Pointer) &read_buf,
								  sizeof(uint32), offset,
								  WAIT_EVENT_DATA_FILE_READ) !=
						sizeof(uint32))
						ereport(FATAL, (errcode_for_file_access(),
										errmsg("could not read keys amount for "
											   "removed tree (%u %u %u %u) "
											   "from rewind file %s",
											   ix_key->type,
											   ix_key->oids.datoid,
											   ix_key->oids.reloid,
											   ix_key->oids.relnode,
											   FilePathName(rewind_file))));
					offset += sizeof(uint32);
					ix_nitems = pg_ntoh32(*(uint32 *) read_buf);

					for (j = 0; j < ix_nitems; j++)
						replay_rewind_row(rewind_file, read_buf, &offset,
										  NULL, indexDescr, -1, true);
					pfree(ix_key);
				}
			}
		}
		readed = OFileRead(rewind_file, (Pointer) &read_buf, tree_header_size,
						   offset, WAIT_EVENT_DATA_FILE_READ);
	}

	if (readed > 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read tree header from rewind "
							   "file %s: expected %d bytes, got %d",
							   FilePathName(rewind_file),
							   tree_header_size, readed)));

	/* TODO: Basically mimic checkpoint_shmem_init */
	checkpoint_state->controlToastConsistentPtr = startpoint;
	checkpoint_state->controlReplayStartPtr = startpoint;
	checkpoint_state->controlSysTreesStartPtr = startpoint;
	Assert(OXidIsValid(lastXid));

	/*
	 * Seed the xid horizons from divXid (lastXid).  divXid is the lowest
	 * xid the source's WAL will reference (see pg_rewind_orioledb.c), so it
	 * is both the runXmin / globalXmin floor and the nextXid to assign:
	 * the first replayed transaction carries exactly this oxid, and
	 * advance_oxids() only initialises a slot to INPROGRESS when the
	 * incoming oxid is >= nextXid.  Seeding nextXid at divXid (rather
	 * than divXid + 1) is what lets the first replayed transaction get a
	 * fresh INPROGRESS slot instead of inheriting the stale FROZEN slot
	 * the target's own xidmap left behind.
	 *
	 * checkpointRetainXmin == checkpointRetainXmax == divXid makes the
	 * xidmap range loaded by checkpoint_shmem_init() empty, so no target
	 * xidmap slots are trusted: every oxid < divXid is settled below the
	 * floor (handled via the o_buffers write path, which never trips the
	 * FROZEN assert), and every oxid >= divXid is initialised fresh by
	 * advance_oxids() as WAL_REC_XID arrives.
	 */
	pg_atomic_init_u64(&xid_meta->nextXid, lastXid);
	pg_atomic_init_u64(&xid_meta->runXmin, lastXid);
	pg_atomic_init_u64(&xid_meta->globalXmin, lastXid);
	pg_atomic_init_u64(&xid_meta->lastXidWhenUpdatedGlobalXmin, lastXid);
	pg_atomic_init_u64(&xid_meta->writtenXmin, lastXid);
	pg_atomic_init_u64(&xid_meta->writeInProgressXmin, lastXid);
	pg_atomic_init_u64(&xid_meta->checkpointRetainXmin, lastXid);
	pg_atomic_init_u64(&xid_meta->checkpointRetainXmax, lastXid);
	pg_atomic_init_u64(&xid_meta->cleanedXmin, lastXid);
	pg_atomic_init_u64(&xid_meta->cleanedCheckpointXmin, lastXid);
	pg_atomic_init_u64(&xid_meta->cleanedCheckpointXmax, lastXid);

	/*
	 * The undo metadata is left alone.  It was loaded from the target's
	 * control file and has since been advanced by the rows applied above.
	 * Other processes (e.g. the orioledb bgwriter) already reserve undo space
	 * at this point, so resetting these counters would corrupt their
	 * reservations and let new undo records overwrite existing ones.
	 */

	/*
	 * Mark the checkpoint state as finished and reset the stack so that
	 * everything is ready for the next real checkpoint.
	 */
	checkpoint_state->curKeyType = CurKeyFinished;
	checkpoint_state->completed = true;
	checkpoint_state->treeType = oIndexInvalid;
	checkpoint_state->datoid = InvalidOid;
	checkpoint_state->reloid = InvalidOid;
	checkpoint_state->relnode = InvalidOid;
	checkpoint_state->tablespace = InvalidOid;

	elog(LOG, "orioledb rewind ended");
}
