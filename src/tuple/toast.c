/*-------------------------------------------------------------------------
 *
 * toast.h
 * 		Routines for orioledb TOAST implementation
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tuple/toast.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/modify.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/toast.h"
#include "transam/oxid.h"
#include "tuple/toast.h"
#include "tuple/format.h"
#include "tuple/sort.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/detoast.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "miscadmin.h"

typedef struct
{
	OIndexDescr *pk;
	OIndexDescr *toast;
} OTableToastArg;

/*
 * Help functions.
 */
/* creates table tuple which can be stored in TOAST BTree */
static OTuple o_create_toast_tuple(OToastKey tkey,
								   Pointer data, Size data_length,
								   OTableToastArg *arg);

/* creates index tuple which can be stored in TOAST BTree */
static OTuple o_create_toast_key(OToastKey tkey, OTableToastArg *arg);

/*
 * prints TOAST table tuple (is_tuple = true)
 * or TOAST index tuple (is_tuple = false
 */
static void toast_tuple_print(TupleDesc tupDesc, OTupleFixedFormatSpec *spec,
							  FmgrInfo *outputFns, StringInfo buf, OTuple tup,
							  Datum *values, bool *nulls, bool is_tuple,
							  bool printRowVersion);

void
o_toast_init_tupdescs(OIndexDescr *toast, TupleDesc ix_primary)
{
	int			i,
				pidx_natts = ix_primary->natts;

	toast->leafTupdesc = CreateTemplateTupleDesc(pidx_natts + TOAST_LEAF_FIELDS_NUM);
	toast->nonLeafTupdesc = CreateTemplateTupleDesc(pidx_natts + TOAST_NON_LEAF_FIELDS_NUM);

	/* copies entries from primary index TupleDesc */
	for (i = 0; i < pidx_natts; i++)
	{
		TupleDescCopyEntry(toast->leafTupdesc, i + 1, ix_primary, i + 1);
		TupleDescCopyEntry(toast->nonLeafTupdesc, i + 1, ix_primary, i + 1);
	}

	/*
	 * adds new entries
	 */
	/* attribute number */
	o_tables_tupdesc_init_builtin(toast->leafTupdesc, pidx_natts + ATTN_POS, "attnum", INT2OID);
	o_tables_tupdesc_init_builtin(toast->nonLeafTupdesc, pidx_natts + ATTN_POS, "attnum", INT2OID);
	/* offset */
	o_tables_tupdesc_init_builtin(toast->leafTupdesc, pidx_natts + OFFSET_POS, "offset", INT4OID);
	o_tables_tupdesc_init_builtin(toast->nonLeafTupdesc, pidx_natts + OFFSET_POS, "offset", INT4OID);
	/* data only in leaf tuples */
	o_tables_tupdesc_init_builtin(toast->leafTupdesc, pidx_natts + DATA_POS, "data", BYTEAOID);
}

int
o_toast_cmp(BTreeDescr *desc,
			void *p1, BTreeKeyType k1,
			void *p2, BTreeKeyType k2)
{
	OIndexDescr *toastd = (OIndexDescr *) desc->arg;
	int			pkAttnum = toastd->nonLeafTupdesc->natts - TOAST_NON_LEAF_FIELDS_NUM;
	int			i;
	OTuple		pk1;
	OTuple		pk2;
	int16		attnum1,
				attnum2;
	int32		offset1,
				offset2;

	if (k1 == BTreeKeyBound)
		pk1 = ((OToastKey *) p1)->pk_tuple;
	else
		pk1 = *((OTuple *) p1);

	if (k2 == BTreeKeyBound)
		pk2 = ((OToastKey *) p2)->pk_tuple;
	else
		pk2 = *((OTuple *) p2);

	for (i = 0; i < pkAttnum; i++)
	{
		Datum		v1,
					v2;
		bool		null1,
					null2;
		OIndexField *field = &toastd->fields[i];
		int			cmp;

		v1 = o_fastgetattr(pk1, i + 1, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null1);
		v2 = o_fastgetattr(pk2, i + 1, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null2);

		if (null1 || null2)
		{
			if (null1 && null2)
				continue;
			else if (null1)
				return field->nullfirst ? -1 : 1;
			else
				return field->nullfirst ? 1 : -1;
		}
		cmp = o_call_comparator(field->comparator, v1, v2);
		if (cmp)
			return field->ascending ? cmp : -cmp;
	}

	if (k1 == BTreeKeyBound)
	{
		attnum1 = ((OToastKey *) p1)->attnum;
		offset1 = ((OToastKey *) p1)->offset;
	}
	else
	{
		bool		null;

		attnum1 = DatumGetInt16(o_fastgetattr(pk1, pkAttnum + ATTN_POS, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null));
		Assert(!null);
		offset1 = DatumGetInt32(o_fastgetattr(pk1, pkAttnum + OFFSET_POS, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null));
		Assert(!null);
	}

	if (k2 == BTreeKeyBound)
	{
		attnum2 = ((OToastKey *) p2)->attnum;
		offset2 = ((OToastKey *) p2)->offset;
	}
	else
	{
		bool		null;

		attnum2 = DatumGetInt16(o_fastgetattr(pk2, pkAttnum + ATTN_POS, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null));
		Assert(!null);
		offset2 = DatumGetInt32(o_fastgetattr(pk2, pkAttnum + OFFSET_POS, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null));
		Assert(!null);
	}

	if (attnum1 != attnum2)
		return (attnum1 < attnum2) ? -1 : 1;
	if (offset1 != offset2)
		return (offset1 < offset2) ? -1 : 1;
	return 0;
}

bool
o_toast_needs_undo(BTreeDescr *desc, BTreeOperationType action,
				   OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
				   OTuple newTuple, OXid newOxid)
{
	if (action == BTreeOperationDelete)
		return true;

	if (!XACT_INFO_OXID_EQ(oldXactInfo, newOxid))
		return false;

	if (oldDeleted && o_tuple_get_version(oldTuple) + 1 == o_tuple_get_version(newTuple))
		return false;

	if (!O_TUPLE_IS_NULL(newTuple) &&
		o_tuple_get_version(oldTuple) >= o_tuple_get_version(newTuple))
		return false;

	return true;
}


struct varlena *
o_detoast(struct varlena *attr)
{
	OToastExternal ote;
	ORelOids	oids;
	OTableDescr *descr;
	OFixedKey	key;

	memcpy(&ote, VARDATA_EXTERNAL(attr), O_TOAST_EXTERNAL_SZ);
	oids.datoid = ote.datoid;
	oids.reloid = ote.relid;
	oids.relnode = ote.relnode;
	descr = o_fetch_table_descr(oids);

	Assert(descr);

	o_btree_load_shmem(&descr->toast->desc);
	key.tuple.formatFlags = ote.formatFlags;
	key.tuple.data = key.fixedData;
	memcpy(key.fixedData,
		   VARDATA_EXTERNAL(attr) + O_TOAST_EXTERNAL_SZ,
		   ote.data_size);
	return (struct varlena *) o_toast_get(GET_PRIMARY(descr), descr->toast,
										  key.tuple, ote.attnum,
										  ote.toasted_size, ote.csn);
}

static BTreeDescr *
tableGetBTreeDesc(void *arg)
{
	OIndexDescr *toast = ((OTableToastArg *) arg)->toast;

	return &toast->desc;
}

static uint32
tableGetMaxChunkSize(void *key, void *arg)
{
	OToastKey  *tkey = (OToastKey *) key;
	OIndexDescr *toast = ((OTableToastArg *) arg)->toast;
	OIndexDescr *primary = ((OTableToastArg *) arg)->pk;
	Datum		values[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM];
	bool		isnull[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM] = {false};
	int			i,
				natts;
	varattrib_4b data;
	uint32		minTupleSize;

	natts = primary->nonLeafTupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		int			attnum = i + 1;

		values[i] = o_fastgetattr(tkey->pk_tuple, attnum,
								  primary->nonLeafTupdesc,
								  &primary->nonLeafSpec,
								  &isnull[i]);
	}
	values[natts] = 0;
	values[natts + 1] = 0;
	SET_VARSIZE(&data, VARHDRSZ);
	values[natts + 2] = PointerGetDatum(&data);

	minTupleSize = o_new_tuple_size(toast->leafTupdesc, &toast->leafSpec,
									NULL, 1, values, isnull, NULL);

	return MAXALIGN_DOWN(O_BTREE_MAX_TUPLE_SIZE * 3 - MAXALIGN(minTupleSize)) / 3 - minTupleSize - sizeof(LocationIndex);
}

static void
tableUpdateKey(void *key, uint32 offset, void *arg)
{
	OToastKey  *tkey = (OToastKey *) key;

	tkey->offset = offset;
}

static void *
tableGetNextKey(void *key, void *arg)
{
	OToastKey  *tkey = (OToastKey *) key;
	static OToastKey nextKey;

	nextKey = *tkey;
	nextKey.attnum += 1;
	nextKey.offset = 0;

	return (Pointer) &nextKey;
}

static OTuple
tableCreateTuple(void *key, Pointer data, uint32 offset, int length, void *arg)
{
	OToastKey  *tkey = (OToastKey *) key;
	OTuple		result;

	tkey->offset = offset;

	result = o_create_toast_tuple(*tkey,
								  data + offset,
								  length,
								  (OTableToastArg *) arg);

	return result;
}

static OTuple
tableCreateKey(void *key, uint32 offset, void *arg)
{
	OToastKey  *tkey = (OToastKey *) key;
	OTuple		result;

	tkey->offset = offset;

	result = o_create_toast_key(*tkey, (OTableToastArg *) arg);

	return result;
}

static bytea *
get_data(OIndexDescr *toast, OTuple tuple)
{
	int			natts = toast->leafTupdesc->natts;

	return DatumGetByteaPP(PointerGetDatum(o_fastgetattr_ptr(tuple, natts, toast->leafTupdesc, &toast->leafSpec)));
}

static Pointer
tableGetTupleData(OTuple tuple, void *arg)
{
	OIndexDescr *toast = ((OTableToastArg *) arg)->toast;

	return VARDATA_ANY(get_data(toast, tuple));
}

static uint32
tableGetTupleOffset(OTuple tuple, void *arg)
{
	OIndexDescr *toast = ((OTableToastArg *) arg)->toast;
	bool		isnull;
	Datum		result;

	result = o_fastgetattr(tuple,
						   toast->leafTupdesc->natts + OFFSET_POS - DATA_POS,
						   toast->leafTupdesc,
						   &toast->leafSpec,
						   &isnull);
	Assert(!isnull);
	return DatumGetInt32(result);
}

static uint32
tableGetTupleDataSize(OTuple tuple, void *arg)
{
	OIndexDescr *toast = ((OTableToastArg *) arg)->toast;

	return VARSIZE_ANY_EXHDR(get_data(toast, tuple));
}

static TupleFetchCallbackResult
tableVersionCallback(OTuple tuple, OXid tupOxid, CommitSeqNo csn, void *arg,
					 TupleFetchCallbackCheckType check_type)
{
	OToastKey  *key = (OToastKey *) arg;

	if (check_type != OTupleFetchCallbackVersionCheck)
		return OTupleFetchNext;

	if (!(COMMITSEQNO_IS_INPROGRESS(csn) && tupOxid == get_current_oxid_if_any()))
		return OTupleFetchNext;

	if (o_tuple_get_version(tuple) <= o_tuple_get_version(key->pk_tuple))
		return OTupleFetchMatch;
	else
		return OTupleFetchNext;
}


ToastAPI	tableToastAPI = {
	.getBTreeDesc = tableGetBTreeDesc,
	.getKeySize = NULL,
	.getMaxChunkSize = tableGetMaxChunkSize,
	.updateKey = tableUpdateKey,
	.getNextKey = tableGetNextKey,
	.createTuple = tableCreateTuple,
	.createKey = tableCreateKey,
	.getTupleData = tableGetTupleData,
	.getTupleOffset = tableGetTupleOffset,
	.getTupleDataSize = tableGetTupleDataSize,
	.deleteLogFullTuple = false,
	.versionCallback = tableVersionCallback
};


bool
generic_toast_insert(ToastAPI *api, void *key, Pointer data, Size data_size,
					 OXid oxid, CommitSeqNo csn, void *arg)
{
	BTreeDescr *desc = api->getBTreeDesc(arg);
	uint32		max_length = api->getMaxChunkSize(key, arg);
	uint32		offset = 0;
	bool		inserted;
	BTreeModifyCallbackInfo callbackInfo = nullCallbackInfo;

	inserted = false;

	Assert(data_size > 0);

	while (data_size > 0)
	{
		OTuple		tup;
		int			length = 0;

		if (data_size < max_length)
		{
			length = data_size;
		}
		else
		{
			length = max_length;
		}

		tup = api->createTuple(key, data, offset, length, arg);

		inserted = o_btree_modify(desc, BTreeOperationInsert,
								  tup, BTreeKeyLeafTuple,
								  key, BTreeKeyBound,
								  oxid, csn, RowLockUpdate,
								  NULL, &callbackInfo) == OBTreeModifyResultInserted;

		if (!inserted)
		{
			pfree(tup.data);
			break;
		}

		add_modify_wal_record(WAL_REC_INSERT, desc,
							  tup, o_btree_len(desc, tup, OTupleLength));

		pfree(tup.data);

		offset += length;
		data_size -= length;
	}

	return inserted;
}

void
generic_toast_sort_add(ToastAPI *api, void *key,
					   Pointer data, Size data_size,
					   Tuplesortstate *sortstate, void *arg)
{
	uint32		max_length = api->getMaxChunkSize(key, arg);
	uint32		offset = 0;

	Assert(data_size > 0);

	while (data_size > 0)
	{
		OTuple		tup;
		int			length = 0;

		if (data_size < max_length)
		{
			length = data_size;
		}
		else
		{
			length = max_length;
		}

		tup = api->createTuple(key, data, offset, length, arg);
		tuplesort_putotuple(sortstate, tup);

		offset += length;
		data_size -= length;
	}
}

static OBTreeModifyCallbackAction
o_update_callback(BTreeDescr *descr,
				  OTuple tup, OTuple *newtup, OXid oxid,
				  OTupleXactInfo xactInfo,
				  UndoLocation location, RowLockMode *lock_mode,
				  BTreeLocationHint *hint, void *arg)
{
	return OBTreeCallbackActionUpdate;
}

static OBTreeModifyCallbackAction
o_update_deleted_callback(BTreeDescr *descr,
						  OTuple tup, OTuple *newtup, OXid oxid,
						  OTupleXactInfo xactInfo,
						  BTreeLeafTupleDeletedStatus deleted,
						  UndoLocation location, RowLockMode *lock_mode,
						  BTreeLocationHint *hint, void *arg)
{
	return OBTreeCallbackActionUpdate;
}

static OBTreeModifyCallbackAction
o_delete_callback(BTreeDescr *descr,
				  OTuple tup, OTuple *newtup, OXid oxid,
				  OTupleXactInfo xactInfo, UndoLocation location,
				  RowLockMode *lock_mode, BTreeLocationHint *hint, void *arg)
{
	return OBTreeCallbackActionDelete;
}

bool
generic_toast_update(ToastAPI *api, void *key, Pointer data, Size data_size,
					 OXid oxid, CommitSeqNo csn, void *arg)
{
	BTreeDescr *desc = api->getBTreeDesc(arg);
	int			max_length = api->getMaxChunkSize(key, arg);
	uint32		offset = 0,
				length;
	bool		success = true;
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = NULL,
		.modifyDeletedCallback = o_update_deleted_callback,
		.modifyCallback = o_update_callback,
		.needsUndoForSelfCreated = false,
		.arg = NULL
	};

	Assert(data_size > 0);

	while (data_size > 0)
	{
		OBTreeModifyResult result;
		OTuple		tup;

		if (data_size < max_length)
		{
			length = data_size;
		}
		else
		{
			length = max_length;
		}

		tup = api->createTuple(key, data, offset, length, arg);

		result = o_btree_modify(desc, BTreeOperationInsert,
								tup, BTreeKeyLeafTuple,
								key, BTreeKeyBound,
								oxid, csn, RowLockUpdate,
								NULL, &callbackInfo);

		if (result != OBTreeModifyResultInserted && result != OBTreeModifyResultUpdated)
		{
			pfree(tup.data);
			success = false;
			break;
		}


		add_modify_wal_record((result == OBTreeModifyResultUpdated) ? WAL_REC_UPDATE :
							  WAL_REC_INSERT,
							  desc, tup, o_btree_len(desc, tup, OTupleLength));

		offset += length;
		data_size -= length;
	}

	/*
	 * There might be tailing tuples.  We need to delete them.
	 */
	api->updateKey(key, offset + 1, arg);
	(void) generic_toast_delete(api, key, oxid, csn, arg);

	return success;
}

bool
generic_toast_delete(ToastAPI *api, void *key, OXid oxid,
					 CommitSeqNo csn, void *arg)
{
	BTreeDescr *desc = api->getBTreeDesc(arg);
	void	   *nextKey;
	BTreeIterator *it;
	bool		deleted = false;
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = NULL,
		.modifyDeletedCallback = NULL,
		.modifyCallback = o_delete_callback,
		.needsUndoForSelfCreated = false,
		.arg = NULL
	};

	nextKey = api->getNextKey(key, arg);
	it = o_btree_iterator_create(desc, key, BTreeKeyBound,
								 csn, ForwardScanDirection);

	do
	{
		BTreeLocationHint hint;
		bool		key_allocated;
		OTuple		walKey;
		OTuple		tuple;
		uint32		offset;
		OTuple		nullTup;

		tuple = o_btree_iterator_fetch(it, NULL, nextKey, BTreeKeyBound, false, &hint);

		/* if tuple not found */
		if (O_TUPLE_IS_NULL(tuple))
			break;

		offset = api->getTupleOffset(tuple, arg);
		api->updateKey(key, offset, arg);

		O_TUPLE_SET_NULL(nullTup);
		if (o_btree_modify(desc, BTreeOperationDelete,
						   nullTup, BTreeKeyNone,
						   key, BTreeKeyBound, oxid, csn, RowLockUpdate,
						   &hint, &callbackInfo) != OBTreeModifyResultDeleted)
		{
			elog(ERROR, "Unexpected missing TOAST chunk");
		}
		else
		{
			deleted = true;
		}

		if (!api->deleteLogFullTuple)
		{
			walKey = o_btree_tuple_make_key(desc, tuple, NULL, true, &key_allocated);
			add_modify_wal_record(WAL_REC_DELETE, desc,
								  walKey, o_btree_len(desc, walKey, OKeyLength));
			if (key_allocated)
				pfree(walKey.data);
		}
		else
		{
			add_modify_wal_record(WAL_REC_DELETE, desc,
								  tuple, o_btree_len(desc, tuple, OTupleLength));
		}

		pfree(tuple.data);
	} while (true);

	return deleted;
}

Pointer
generic_toast_get(ToastAPI *api, void *key, Size data_size,
				  CommitSeqNo csn, void *arg)
{
	BTreeDescr *desc = api->getBTreeDesc(arg);
	BTreeIterator *it;
	void	   *nextKey;
	int			max_length = api->getMaxChunkSize(key, arg);
	int			actual_size;
	Pointer		data;

	nextKey = api->getNextKey(key, arg);

	it = o_btree_iterator_create(desc, key, BTreeKeyBound,
								 csn, ForwardScanDirection);
	if (api->versionCallback)
		o_btree_iterator_set_callback(it, api->versionCallback, (void *) key);

	data = palloc(data_size);
	actual_size = 0;

	do
	{
		OTuple		tup;
		int			iter_data_size;

		tup = o_btree_iterator_fetch(it, NULL, nextKey, BTreeKeyBound, false, NULL);

		/* if tuple not found */
		if (O_TUPLE_IS_NULL(tup))
			break;

		iter_data_size = api->getTupleDataSize(tup, arg);

		if (actual_size + iter_data_size > data_size)
		{
			/* avoid memcpy to unallocated memory */
			actual_size += iter_data_size;
			break;
		}

		memcpy(data + actual_size,
			   api->getTupleData(tup, arg),
			   iter_data_size);
		pfree(tup.data);

		actual_size += iter_data_size;

		if (iter_data_size < max_length)
			break;

		Assert(actual_size <= data_size);
	} while (true);

	btree_iterator_free(it);

	Assert(actual_size == data_size);
	if (actual_size != data_size)
	{
		pfree(data);
		return NULL;
	}
	return data;
}

/*
 * Common code for
 * generic_toast_get_any_with_callback and generic_toast_get_any_with_key
 */
static Pointer
generic_toast_get_any_common(ToastAPI *api,
							 Pointer key,
							 Size *data_size,
							 CommitSeqNo csn,
							 void *arg,
							 BTreeIterator *it,
							 Pointer *found_key)
{
	Pointer		nextKey;
	OTuple		tuple;
	StringInfoData str;

	nextKey = api->getNextKey(key, arg);

	*data_size = 0;

	do
	{
		uint32		chunk_size;

		tuple = o_btree_iterator_fetch(it, NULL, nextKey, BTreeKeyBound, false, NULL);

		/* if tuple not found */
		if (O_TUPLE_IS_NULL(tuple))
			break;

		if (*data_size == 0)
		{
			initStringInfo(&str);
			if (found_key)
			{
				Size		key_size = api->getKeySize(arg);

				*found_key = palloc(key_size);
				memcpy(*found_key, tuple.data, key_size);
			}
		}
		chunk_size = api->getTupleDataSize(tuple, arg);
		appendBinaryStringInfo(&str, api->getTupleData(tuple, arg),
							   chunk_size);
		*data_size += chunk_size;
		pfree(tuple.data);
	} while (true);

	if (*data_size == 0)
		return NULL;

	return str.data;
}

Pointer
generic_toast_get_any_with_callback(ToastAPI *api, Pointer key,
									Size *data_size, CommitSeqNo csn,
									void *arg,
									TupleFetchCallback fetch_callback,
									void *callback_arg)
{
	BTreeDescr *desc = api->getBTreeDesc(arg);
	BTreeIterator *it;
	Pointer		data;

	it = o_btree_iterator_create(desc, key, BTreeKeyBound,
								 csn, ForwardScanDirection);
	if (fetch_callback && callback_arg)
		o_btree_iterator_set_callback(it, fetch_callback, callback_arg);

	data = generic_toast_get_any_common(api, key, data_size,
										csn, arg, it, NULL);

	btree_iterator_free(it);

	return data;
}

Pointer
generic_toast_get_any_with_key(ToastAPI *api, void *key, Size *data_size,
							   CommitSeqNo csn, void *arg, Pointer *found_key)
{
	BTreeDescr *desc = api->getBTreeDesc(arg);
	BTreeIterator *it;
	Pointer		data;

	it = o_btree_iterator_create(desc, key, BTreeKeyBound,
								 csn, ForwardScanDirection);
	if (api->versionCallback && found_key && *found_key)
		o_btree_iterator_set_callback(it, api->versionCallback,
									  (void *) *found_key);

	data = generic_toast_get_any_common(api, key, data_size, csn, arg, it,
										found_key);

	btree_iterator_free(it);

	return data;
}

Pointer
generic_toast_get_any(ToastAPI *api, void *key, Size *data_size,
					  CommitSeqNo csn, void *arg)
{
	return generic_toast_get_any_with_key(api, key, data_size, csn, arg, NULL);
}

bool
o_toast_insert(OIndexDescr *primary, OIndexDescr *toast, OTuple pk, uint16 attn,
			   Pointer data, Size data_size,
			   OXid oxid, CommitSeqNo csn)
{
	OToastKey	tkey;
	bool		result;
	OTableToastArg arg = {primary, toast};

	tkey.pk_tuple = pk;
	tkey.attnum = attn;
	tkey.offset = 0;

	Assert(toast->desc.type == oIndexToast);

	result = generic_toast_insert(&tableToastAPI, &tkey, data,
								  data_size, oxid, csn, &arg);

	return result;
}

void
o_toast_sort_add(OIndexDescr *primary, OIndexDescr *toast,
				 OTuple pk, uint16 attn,
				 Pointer data, Size data_size,
				 Tuplesortstate *sortstate)
{
	OToastKey	tkey;
	OTableToastArg arg = {primary, toast};

	tkey.pk_tuple = pk;
	tkey.attnum = attn;
	tkey.offset = 0;

	Assert(toast->desc.type == oIndexToast);

	generic_toast_sort_add(&tableToastAPI, (Pointer) &tkey, data,
						   data_size, sortstate, &arg);

}

bool
o_toast_delete(OIndexDescr *primary, OIndexDescr *toast,
			   OTuple pk, uint16 attn,
			   OXid oxid, CommitSeqNo csn)
{
	OToastKey	tkey;
	bool		result;
	OTableToastArg arg = {primary, toast};

	tkey.pk_tuple = pk;
	tkey.attnum = attn;
	tkey.offset = 0;

	Assert(toast->desc.type == oIndexToast);

	result = generic_toast_delete(&tableToastAPI, (Pointer) &tkey,
								  oxid, csn, &arg);

	return result;
}

Pointer
o_toast_get(OIndexDescr *primary, OIndexDescr *toast,
			OTuple pk, uint16 attn,
			Size data_size, CommitSeqNo csn)
{
	OToastKey	tkey;
	Pointer		result;
	OTableToastArg arg = {primary, toast};

	tkey.pk_tuple = pk;
	tkey.attnum = attn;
	tkey.offset = 0;

	Assert(toast->desc.type == oIndexToast);

	result = generic_toast_get(&tableToastAPI, (Pointer) &tkey, data_size,
							   csn, &arg);

	return result;
}

static OTuple
o_create_toast_tuple(OToastKey tkey, Pointer data_ptr, Size data_length,
					 OTableToastArg *arg)
{
	Datum		key[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM];
	bool		isnull[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM] = {false};
	OTuple		result;
	int			i,
				natts;
	bytea	   *data;

	natts = arg->pk->nonLeafTupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		int			attnum = i + 1;

		key[i] = o_fastgetattr(tkey.pk_tuple, attnum,
							   arg->pk->nonLeafTupdesc,
							   &arg->pk->nonLeafSpec,
							   &isnull[i]);
	}
	data = (bytea *) palloc(VARHDRSZ + data_length);
	memcpy(VARDATA(data), data_ptr, data_length);
	SET_VARSIZE(data, VARHDRSZ + data_length);
	key[natts] = tkey.attnum;
	key[natts + 1] = tkey.offset;
	key[natts + 2] = PointerGetDatum(data);

	result = o_form_tuple(arg->toast->leafTupdesc,
						  &arg->toast->leafSpec,
						  o_tuple_get_version(tkey.pk_tuple),
						  key, isnull);
	pfree(data);

	return result;
}

static OTuple
o_create_toast_key(OToastKey tkey,
				   OTableToastArg *arg)
{
	Datum		key[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM];
	bool		isnull[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM] = {false};
	int			i,
				natts;

	memset(isnull, 0, sizeof(isnull));

	natts = arg->pk->nonLeafTupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		int			attnum = i + 1;

		key[i] = o_fastgetattr(tkey.pk_tuple, attnum,
							   arg->pk->nonLeafTupdesc,
							   &arg->pk->nonLeafSpec,
							   &isnull[i]);
	}
	key[natts] = tkey.offset;
	key[natts + 1] = tkey.attnum;

	return o_form_tuple(arg->toast->nonLeafTupdesc,
						&arg->toast->nonLeafSpec,
						o_tuple_get_version(tkey.pk_tuple),
						key, isnull);
}

bool
o_toast_equal(BTreeDescr *primary, Datum left, Datum right)
{
	OToastExternal left_ote,
				right_ote;

	if (!VARATT_IS_EXTERNAL_ORIOLEDB(left) ||
		!VARATT_IS_EXTERNAL_ORIOLEDB(right))
	{
		/* left or right is not orioledb TOAST value */
		return false;
	}

	if (left == right)
	{
		/* easy case: it's same pointers */
		return true;
	}

	memcpy(&left_ote,
		   VARDATA_EXTERNAL(DatumGetPointer(left)),
		   O_TOAST_EXTERNAL_SZ);
	memcpy(&right_ote,
		   VARDATA_EXTERNAL(DatumGetPointer(right)),
		   O_TOAST_EXTERNAL_SZ);

	if (left_ote.datoid != right_ote.datoid ||
		left_ote.relid != right_ote.relid ||
		left_ote.relnode != right_ote.relnode)
	{
		/* values are not from the same index */
		return false;
	}

	if (left_ote.raw_size != right_ote.raw_size ||
		left_ote.toasted_size != right_ote.toasted_size ||
		left_ote.data_size != right_ote.data_size)
	{
		/* sizes are not equal */
		return false;
	}

	if (left_ote.attnum != right_ote.attnum ||
		left_ote.csn != right_ote.csn)
	{
		/* it's a different attribute */
		return false;
	}

	/* now we can make final check: compare primary keys */
	if (left_ote.formatFlags != right_ote.formatFlags)
		return false;

	return memcmp(VARDATA_EXTERNAL(DatumGetPointer(left)) + O_TOAST_EXTERNAL_SZ,
				  VARDATA_EXTERNAL(DatumGetPointer(right)) + O_TOAST_EXTERNAL_SZ,
				  left_ote.data_size) == 0;
}

Datum
o_get_raw_value(Datum value, bool *free)
{
	struct varlena *tmp,
			   *result;

	result = (struct varlena *) DatumGetPointer(value);
	*free = false;

	if (VARATT_IS_EXTERNAL(value))
	{
		if (VARATT_IS_EXTERNAL_ORIOLEDB(value))
		{
			result = o_detoast(result);
			*free = true;
			Assert(result != NULL);
		}
		else
		{
			result = detoast_attr(result);
			*free = true;
		}
	}

	if (VARATT_IS_COMPRESSED(result))
	{
		tmp = result;
		result = toast_decompress_datum(tmp);
		if (*free)
			pfree(tmp);
		*free = true;
	}

	Assert(VARSIZE_ANY_EXHDR(result) == o_get_raw_size(value));
	return PointerGetDatum(result);
}

Datum
o_get_src_value(Datum value, bool *free)
{
	struct varlena *result;

	result = (struct varlena *) DatumGetPointer(value);
	*free = false;

	if (VARATT_IS_EXTERNAL(value))
	{
		if (VARATT_IS_EXTERNAL_ORIOLEDB(value))
		{
			result = o_detoast(result);
			Assert(result != NULL);
		}
		else
		{
			result = detoast_external_attr(result);
		}
		*free = true;
	}

	Assert(VARSIZE_ANY(result) == o_get_src_size(value));
	return PointerGetDatum(result);
}

void
o_toast_key_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	TuplePrintOpaque *opaque = (TuplePrintOpaque *) arg;

	toast_tuple_print(opaque->keyDesc, opaque->keySpec, opaque->keyOutputFns,
					  buf, tup, opaque->values, opaque->nulls, false, false);
}

void
o_toast_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	TuplePrintOpaque *opaque = (TuplePrintOpaque *) arg;

	toast_tuple_print(opaque->desc, opaque->spec, opaque->outputFns, buf,
					  tup, opaque->values, opaque->nulls, true, opaque->printRowVersion);
}

Datum
create_o_toast_external(OTableDescr *descr,
						OTuple idx_tup,
						AttrNumber attnum,
						OToastValue *toasted,
						CommitSeqNo csn)
{
	Pointer		result;
	OIndexDescr *id = GET_PRIMARY(descr);
	OToastExternal ote;
	uint32		tupSize = o_tuple_size(idx_tup, &id->nonLeafSpec);

	result = palloc0(VARHDRSZ_EXTERNAL + O_TOAST_EXTERNAL_SZ + tupSize);

	SET_VARTAG_EXTERNAL(result, VARTAG_ORIOLEDB);

	memset(&ote, 0, sizeof(ote));
	ote.raw_size = toasted->raw_size;
	ote.toasted_size = toasted->toasted_size;
	ote.datoid = descr->oids.datoid;
	ote.relid = descr->oids.reloid;
	ote.relnode = descr->oids.relnode;
	ote.csn = csn;
	ote.attnum = attnum;
	ote.data_size = tupSize;
	ote.formatFlags = idx_tup.formatFlags;
#if PG_VERSION_NUM >= 140000
	ote.formatFlags |= toasted->compression << ORIOLEDB_EXT_FORMAT_FLAGS_BITS;
#endif

	memcpy(VARDATA_EXTERNAL(result), &ote, sizeof(ote));
	memcpy(VARDATA_EXTERNAL(result) + O_TOAST_EXTERNAL_SZ, idx_tup.data, tupSize);
	return PointerGetDatum(result);
}

static void
toast_tuple_print(TupleDesc tupDesc, OTupleFixedFormatSpec *spec,
				  FmgrInfo *outputFns, StringInfo buf,
				  OTuple tup, Datum *values, bool *nulls, bool is_tuple,
				  bool printRowVersion)
{
	int			attnum,
				i,
				offset_pos,
				attn_pos,
				datasz_pos;
	int			pk_natts = tupDesc->natts - (is_tuple ? TOAST_LEAF_FIELDS_NUM
											 : TOAST_NON_LEAF_FIELDS_NUM);

	appendStringInfo(buf, "(");
	if (printRowVersion)
		appendStringInfo(buf, "(%u) ", o_tuple_get_version(tup));
	appendStringInfo(buf, "PK: (");
	for (i = 0; i < pk_natts; i++)
	{
		if (i > 0)
			appendStringInfo(buf, ", ");
		attnum = i + 1;
		values[i] = o_fastgetattr(tup, attnum, tupDesc, spec, &nulls[i]);
		if (nulls[i])
			appendStringInfo(buf, "null");
		else
			appendStringInfo(buf, "'%s'",
							 OutputFunctionCall(&outputFns[i], values[i]));
	}
	appendStringInfo(buf, "), ");

	offset_pos = pk_natts + OFFSET_POS - 1;
	attn_pos = pk_natts + ATTN_POS - 1;
	values[attn_pos] = o_fastgetattr(tup, pk_natts + ATTN_POS, tupDesc, spec,
									 &nulls[attn_pos]);
	values[offset_pos] = o_fastgetattr(tup, pk_natts + OFFSET_POS, tupDesc, spec,
									   &nulls[offset_pos]);
	if (is_tuple)
	{
		Datum		data;

		datasz_pos = pk_natts + DATA_POS - 1;
		data = o_fastgetattr(tup, pk_natts + DATA_POS, tupDesc, spec,
							 &nulls[datasz_pos]);
		if (!nulls[datasz_pos])
			values[datasz_pos] = UInt32GetDatum(VARSIZE_ANY_EXHDR(data));
		appendStringInfo(buf, "attnum %hu, offset %u, data_length %u",
						 DatumGetUInt16(values[attn_pos]),
						 DatumGetUInt32(values[offset_pos]),
						 DatumGetUInt32(values[datasz_pos]));
	}
	else
	{
		appendStringInfo(buf, "attnum %hu, offset %u",
						 DatumGetUInt16(values[attn_pos]),
						 DatumGetUInt32(values[offset_pos]));
	}
	appendStringInfo(buf, ") ");
}
