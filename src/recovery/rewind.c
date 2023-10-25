/*-------------------------------------------------------------------------
 *
 * rewind.c
 *		Routines for pg_rewind support.
 *
 * Copyright (c) 2017-2021, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/rewind.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "btree/iterator.h"
#include "catalog/sys_trees.h"
#include "recovery/internal.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"

#include "access/heapam.h"
#include "access/table.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/pg_bswap.h"

#define O_REWIND_FOUND '\1'
#define O_REWIND_NOT_FOUND '\0'

PG_FUNCTION_INFO_V1(orioledb_pg_rewind_sorted_keys);
PG_FUNCTION_INFO_V1(orioledb_pg_rewind_new_row_versions);

typedef struct KeyArrayElement
{
	uint8	tupleFormatFlags;
	uint32	dataLength;
	char	data[FLEXIBLE_ARRAY_MEMBER];
} KeyArrayElement;

typedef struct IteratorStackItem
{
	OXid		oxid;
	uint8		deleted;
	uint8		formatFlags;
	uint32		len;
	char	   *data;
} IteratorStackItem;

typedef struct TableRow
{
	ORelOids		oids;
	OIndexType		ix_type;
	uint32			nkeys;
	bytea		   *keys;
} TableRow;

typedef struct KeyIterator
{
	OTuple new_tup;
	OTuple first_key_tup;
	OTuple last_key_tup;
	uint8 last_deleted;
	BTreeIterator *iter;
} KeyIterator;

static BTreeDescr *cmp_tree_desc;

static void
appendHton16StringInfo(StringInfo str, uint16 data)
{
	uint16 u16data = pg_hton16(data);
	appendBinaryStringInfoNT(str, (const char *) &u16data, sizeof(u16data));
}

static void
appendHton32StringInfo(StringInfo str, uint32 data)
{
	uint32 u32data = pg_hton32(data);
	appendBinaryStringInfoNT(str, (const char *) &u32data, sizeof(u32data));
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
	OTuple key_tup1 = {.formatFlags = key1->tupleFormatFlags,
					   .data = key1->data};
	OTuple key_tup2 = {.formatFlags = key2->tupleFormatFlags,
					   .data = key2->data};

	return o_btree_cmp(cmp_tree_desc,
					   (void *) &key_tup1, BTreeKeyNonLeafKey,
					   (void *) &key_tup2, BTreeKeyNonLeafKey);
}

static TableRow *
table_next_row(Relation rel, TableScanDesc scan)
{
	HeapTuple		htuple;
	static TableRow	row;
	TableRow	   *result = NULL;

	htuple = heap_getnext(scan, ForwardScanDirection);
	if (htuple)
	{
		bool	isnull;
		Datum	attr;

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
		result->nkeys = DatumGetUInt32(attr);

		attr = heap_getattr(htuple, 6, rel->rd_att, &isnull);
		result->keys = DatumGetByteaP(attr);
	}

	return result;
}

static int
fill_descrs(ORelOids oids, OIndexType ix_type,
			OTableDescr **descr, OIndexDescr **indexDescr)
{
	int sys_tree_num = -1;

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
	int i;
	KeyArrayElement	  **key_array;

	key_array = palloc0(sizeof(KeyArrayElement *) * nkeys);
	for (i = 0; i < nkeys; i++)
	{
		uint32 keyLength;
		uint8 tupleFormatFlags;
		uint32 u32data;

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
	uint32	i,
			dst,
			new_nkeys;

	pg_qsort(key_array, nkeys, sizeof(KeyArrayElement *),
			 key_array_cmp);

	// Remove duplicates
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
	Oid				reloid = PG_GETARG_OID(0);
	Relation		rel;
	TableScanDesc	scan;
	TableRow	   *row;
	bytea		   *result;
	StringInfo		result_str;

	rel = table_open(reloid, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	result_str = makeStringInfo();

	while ((row = table_next_row(rel, scan)) != NULL)
	{
		OTableDescr	   *descr = NULL;
		OIndexDescr	   *indexDescr = NULL;
		int				sys_tree_num;

		sys_tree_num = fill_descrs(row->oids, row->ix_type,
								   &descr, &indexDescr);

		if ((indexDescr) || (sys_tree_num > 0))
		{
			int					i;
			char			   *keys_data_start = VARDATA(row->keys);
			char			   *ptr = keys_data_start;
			KeyArrayElement	  **key_array;
			StringInfo			keys_str;
			uint32				new_nkeys;

			key_array = create_key_array(row->nkeys, 0, &ptr);
			Assert((ptr - keys_data_start) == (VARSIZE(row->keys) - VARHDRSZ));
			if (sys_tree_num > 0)
				cmp_tree_desc = get_sys_tree(sys_tree_num);
			else
				cmp_tree_desc = &indexDescr->desc;

			new_nkeys = sort_key_array(key_array, row->nkeys);

			appendHton16StringInfo(result_str, 6);
			appendHton32StringInfo(result_str, sizeof(row->ix_type));
			appendHton32StringInfo(result_str, row->ix_type);
			appendOidStringInfo(result_str, row->oids.datoid);
			appendOidStringInfo(result_str, row->oids.reloid);
			appendOidStringInfo(result_str, row->oids.relnode);
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
					OIndexChunkKey	   *ix_key = (OIndexChunkKey *)
													key_array[i]->data;
					OIndexDescr		   *id;
					uint8				index_exist;

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
get_tup_oxid_callback(OTuple tuple, OXid tupOxid, CommitSeqNo csn,
					  bool deleted, void *arg,
					  TupleFetchCallbackCheckType check_type)
{
	uint8	   *deleted_result = arg;

	if (!(COMMITSEQNO_IS_INPROGRESS(csn) &&
		  tupOxid == get_current_oxid_if_any()))
		return OTupleFetchNext;

	*deleted_result = deleted;

	return OTupleFetchMatch;
}

static void
append_revived_tree(StringInfo str, OIndexChunkKey *ix_key)
{
	OIndexDescr		   *ix_descr = NULL;
	BTreeIterator	   *iter;
	uint8				deleted = false;
	OTuple				tup;
	uint32				ix_nitems;
	StringInfo			rows_str;

	ix_descr = o_fetch_index_descr(ix_key->oids, ix_key->type, false, NULL);
	o_btree_load_shmem(&ix_descr->desc);

	iter = o_btree_iterator_create(&ix_descr->desc, NULL, BTreeKeyNone,
								   COMMITSEQNO_NON_DELETED,
								   ForwardScanDirection);

	o_btree_iterator_set_callback(iter, get_tup_oxid_callback, &deleted);
	ix_nitems = 0;
	tup = o_btree_iterator_fetch(iter, NULL, NULL, BTreeKeyNone, true, NULL);
	rows_str = makeStringInfo();
	while (!O_TUPLE_IS_NULL(tup))
	{
		int tup_len = o_btree_len(&ix_descr->desc, tup, OTupleLength);

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

	result->last_deleted = false;
	O_TUPLE_SET_NULL(result->new_tup);
	result->first_key_tup.formatFlags = first->tupleFormatFlags;
	result->first_key_tup.data = first->data;
	result->last_key_tup.formatFlags = last->tupleFormatFlags;
	result->last_key_tup.data = last->data;

	result->iter = o_btree_iterator_create(td, &result->first_key_tup,
										   BTreeKeyNonLeafKey,
										   COMMITSEQNO_NON_DELETED,
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
	bool	found = false;
	OTuple	old_tup = {.data = old_key->data,
					  .formatFlags = old_key->tupleFormatFlags};
	int		cmp = -1;

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
		int tup_len = o_btree_len(td, it->new_tup, OTupleLength);
		appendStringInfoChar(str, O_REWIND_FOUND);
		append_tuple(str, it->new_tup.formatFlags, it->new_tup.data, tup_len,
					 &it->last_deleted);

		if (IS_SYS_TREE_OIDS(td->oids) &&
			td->oids.reloid == SYS_TREES_O_INDICES)
		{
			Pointer found_ptr = old_key->data + old_key->dataLength -
								sizeof(uint8);
			uint8 target_found = *(uint8 *) found_ptr;

			if (target_found)
				appendHton32StringInfo(str, 0);
			else
				append_revived_tree(str, (OIndexChunkKey *) old_key->data);
		}
		key_iterator_iterate(it);
	}
	else
	{
		uint8 deleted = true;
		appendStringInfoChar(str, O_REWIND_NOT_FOUND);
		append_tuple(str, old_key->tupleFormatFlags,
					 old_key->data, old_key->dataLength, &deleted);
	}
}

static void
process_tree(StringInfo str, TableRow *row)
{
	int i;
	char *keys_data_start = VARDATA(row->keys);
	char *ptr = keys_data_start;
	KeyArrayElement **key_array;
	KeyIterator		*key_iter;
	BTreeDescr *td;
	OTableDescr	   *descr = NULL;
	OIndexDescr	   *indexDescr = NULL;
	int				sys_tree_num = -1;

	sys_tree_num = fill_descrs(row->oids, row->ix_type,
								&descr, &indexDescr);

	if (indexDescr || (sys_tree_num > 0))
	{
		// orioledb_pg_rewind_sorted_keys adds additional
		// uint8 field for SYS_TREES_O_INDICES tree, which specifies that
		// tree was dropped on target so full tree rewind needed
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
	Oid				reloid = PG_GETARG_OID(0);
	Relation		rel;
	TableScanDesc	scan;
	TableRow	   *row;
	bytea		   *result;
	StringInfo		result_str;

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


static void
apply_rewind_row(OTableDescr *descr, OIndexDescr *indexDescr,
				 int sys_tree_num,
				 OTuple rewind_row,
				 bool deleted)
{
	const int	temp_oxid = 0;

	bool		single = *recovery_single_process;
	bool		sync = false;
	XLogRecPtr  rec;

	advance_oxids(temp_oxid);
	recovery_switch_to_oxid(temp_oxid, -1);

	if (sys_tree_num < 0)
		apply_modify_record(descr, indexDescr,
							deleted ? RECOVERY_DELETE : RECOVERY_INSERT,
							rewind_row);
	else
	{
		Assert(sys_tree_supports_transactions(sys_tree_num));
		apply_sys_tree_modify_record(sys_tree_num,
									 deleted ? RECOVERY_DELETE :
											   RECOVERY_INSERT,
									 rewind_row, temp_oxid,
									 COMMITSEQNO_INPROGRESS);
	}
	rec = recovery_get_current_ptr();
	if (!single)
	{
		workers_send_oxid_finish(rec, true);
		sync = true;
		workers_synchronize(rec, false);
	}
	else
	{
		sync = true;
		pg_atomic_write_u64(recovery_ptr, rec);
	}

	recovery_finish_current_oxid(COMMITSEQNO_MAX_NORMAL - 1, rec, -1, sync);
}

static void
ereport_rewind_error(File rewind_file, OIndexDescr *indexDescr,
					 int sys_tree_num, char *name)
{
	OIndexType err_ix_type;
	ORelOids err_oids;

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
	int item_header_size = sizeof(uint8) * 2 +
						   sizeof(uint32);
	uint8 deleted;
	uint32 item_len;
	OTuple rewind_row;

	Assert(item_header_size < O_BTREE_MAX_TUPLE_SIZE * 2);
	if (OFileRead(rewind_file, (Pointer)read_buf,
				  item_header_size, *offset,
				  WAIT_EVENT_DATA_FILE_READ) !=
		item_header_size)
		ereport_rewind_error(rewind_file, indexDescr, sys_tree_num,
							 "item header");
	(*offset) += item_header_size;

	deleted = *(uint8 *)(read_buf);
	rewind_row.formatFlags = *(uint8 *)(read_buf +
									  sizeof(uint8));
	item_len = pg_ntoh32(*(uint32 *)(read_buf +
									 sizeof(uint8) +
									 sizeof(uint8)));

	Assert(item_len < O_BTREE_MAX_TUPLE_SIZE * 2);
	if (OFileRead(rewind_file, (Pointer)read_buf,
				  item_len, *offset,
				  WAIT_EVENT_DATA_FILE_READ) !=
		item_len)
		ereport_rewind_error(rewind_file, indexDescr, sys_tree_num,
							 "item data");
	(*offset) += item_len;
	rewind_row.data = read_buf;

	if (indexDescr || (sys_tree_num > 0))
	{
		bool old_toast_consistent = toast_consistent;
		toast_consistent = true; // TODO: Find out are we need real
								 // toast_consistent value
		apply_rewind_row(descr, indexDescr, sys_tree_num,
						 rewind_row,
						 true);
		if (add_new)
			apply_rewind_row(descr, indexDescr, sys_tree_num,
							 rewind_row,
							 deleted);
		toast_consistent = old_toast_consistent;
	}
	return rewind_row;
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
	const int	tree_header_size = sizeof(OIndexType) + 3 * sizeof(Oid) +
								   sizeof(uint8);
	uint32		nkeys;
	uint8		found;
	XLogRecPtr	startpoint;

	rewind_file = PathNameOpenFile(ORIOLEDB_DATA_DIR "/rewind",
								   O_RDONLY | PG_BINARY);

	if (rewind_file < 0)
		return;

	elog(LOG, "orioledb rewind started");

	Assert(tree_header_size < O_BTREE_MAX_TUPLE_SIZE * 2);

	readed = OFileRead(rewind_file, (Pointer)&read_buf, sizeof(XLogRecPtr),
					   offset, WAIT_EVENT_DATA_FILE_READ);
	offset += sizeof(XLogRecPtr);
	if (readed != sizeof(XLogRecPtr))
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read startpoint from rewind file %s",
							   FilePathName(rewind_file))));

	startpoint = *((XLogRecPtr *) (read_buf));
	readed = OFileRead(rewind_file, (Pointer)&read_buf, tree_header_size,
					   offset, WAIT_EVENT_DATA_FILE_READ);
	while (readed == tree_header_size)
	{
		int	sys_tree_num = -1;

		offset += tree_header_size;

		ix_type = pg_ntoh32(*((uint32 *) (read_buf)));
		cur_oids.datoid = pg_ntoh32(*((Oid *) (read_buf + sizeof(uint32))));
		cur_oids.reloid = pg_ntoh32(*((Oid *)(read_buf + sizeof(uint32) +
											  sizeof(Oid))));
		cur_oids.relnode = pg_ntoh32(*((Oid *) (read_buf + sizeof(uint32) +
											  sizeof(Oid) * 2)));
		found = *((uint8 *)(read_buf + sizeof(uint32) +
							sizeof(Oid) * 3));

		if (IS_SYS_TREE_OIDS(cur_oids))
			sys_tree_num = cur_oids.relnode;

		if (found)
		{
			OTableDescr	   *descr = NULL;
			OIndexDescr	   *indexDescr = NULL;
			int				i;

			if (OFileRead(rewind_file, (Pointer)&read_buf, sizeof(uint32),
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
			nkeys = pg_ntoh32(*((uint32 *)(read_buf)));

			sys_tree_num = fill_descrs(cur_oids, ix_type, &descr, &indexDescr);

			for (i = 0; i < nkeys; i++)
			{
				int		j;
				int		row_header_size = sizeof(uint8);
				OTuple	rewind_row = {0};

				if (OFileRead(rewind_file, (Pointer)&read_buf,
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

				found = *((uint8 *)(read_buf));

				rewind_row = replay_rewind_row(rewind_file, read_buf, &offset,
											   descr, indexDescr,
											   sys_tree_num, found);

				if (sys_tree_num == SYS_TREES_O_INDICES && found)
				{
					uint32	ix_nitems;
					OIndexChunkKey *ix_key;

					ix_key = palloc0(sizeof(OIndexChunkKey));
					memcpy(ix_key, rewind_row.data, sizeof(OIndexChunkKey));

					indexDescr = o_fetch_index_descr(ix_key->oids,
													 ix_key->type,
													 false, NULL);
					Assert(indexDescr);

					if (OFileRead(rewind_file, (Pointer)&read_buf,
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
					ix_nitems = pg_ntoh32(*(uint32 *)read_buf);

					for (j = 0; j < ix_nitems; j++)
						replay_rewind_row(rewind_file, read_buf, &offset,
										  NULL, indexDescr, -1, true);
					pfree(ix_key);
				}
			}
		}
		readed = OFileRead(rewind_file, (Pointer)&read_buf, tree_header_size,
						   offset, WAIT_EVENT_DATA_FILE_READ);
	}

	if (readed > 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read tree header from rewind "
							   "file %s: expected %d bytes, got %d",
							   FilePathName(rewind_file),
							   tree_header_size, readed)));

	checkpoint_state->controlToastConsistentPtr = startpoint;
	checkpoint_state->controlReplayStartPtr = startpoint;
	elog(LOG, "orioledb rewind ended");
}
