/*-------------------------------------------------------------------------
 *
 * sys_trees.c
 *		Defitions for system trees.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/sys_trees.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/check.h"
#include "btree/iterator.h"
#include "catalog/sys_trees.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "transam/undo.h"
#include "utils/page_pool.h"

#include "common/hashfn.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"

typedef struct
{
	BTreeRootInfo rootInfo;
} SysTreeShmemHeader;

typedef struct
{
	int			keyLength;
	OBTreeKeyCmp cmpFunc;
	int			tupleLength;
	int			(*tupleLengthFunc) (BTreeDescr *desc, OTuple tuple);
	JsonbValue *(*keyToJsonb) (BTreeDescr *desc, OTuple key, JsonbParseState **state);
	PrintFunc	keyPrint;
	PrintFunc	tupPrint;
	OPagePoolType poolType;
	UndoReserveType undoReserveType;
	BTreeStorageType storageType;
	bool		(*needs_undo) (BTreeDescr *desc, BTreeOperationType action,
							   OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
							   OTuple newTuple, OXid newOxid);
} SysTreeMeta;

typedef struct
{
	BTreeDescr	descr;
	BTreeOps	ops;
	bool		initialized;
} SysTreeDescr;

static void sys_tree_init_if_needed(int i);
static void sys_tree_init(int i, bool init_shmem);
static int	sys_tree_len(BTreeDescr *desc, OTuple tuple, OLengthType type);
static uint32 sys_tree_hash(BTreeDescr *desc, OTuple tuple, BTreeKeyType kind);
static void check_tree_num_input(int num);
static OTuple sys_tree_tuple_make_key(BTreeDescr *desc, OTuple tuple,
									  Pointer data, bool keep_version,
									  bool *allocated);
static int	shared_root_info_key_cmp(BTreeDescr *desc,
									 void *p1, BTreeKeyType k1,
									 void *p2, BTreeKeyType k2);
static void idx_descr_key_print(BTreeDescr *desc, StringInfo buf,
								OTuple tup, Pointer arg);
static void idx_descr_tup_print(BTreeDescr *desc, StringInfo buf,
								OTuple tup, Pointer arg);
static JsonbValue *idx_descr_key_to_jsonb(BTreeDescr *desc, OTuple tup,
										  JsonbParseState **state);
static int	o_table_chunk_cmp(BTreeDescr *desc,
							  void *p1, BTreeKeyType k1,
							  void *p2, BTreeKeyType k2);
static void o_table_chunk_key_print(BTreeDescr *desc, StringInfo buf,
									OTuple tup, Pointer arg);
static void o_table_chunk_tup_print(BTreeDescr *desc, StringInfo buf,
									OTuple tup, Pointer arg);
static int	o_table_chunk_length(BTreeDescr *desc, OTuple tuple);
static JsonbValue *o_table_chunk_key_to_jsonb(BTreeDescr *desc, OTuple tup,
											  JsonbParseState **state);
static bool o_table_chunk_needs_undo(BTreeDescr *desc, BTreeOperationType action,
									 OTuple oldTuple, OTupleXactInfo oldXactInfo,
									 bool oldDeleted, OTuple newTuple,
									 OXid newOxid);
static int	o_index_chunk_cmp(BTreeDescr *desc,
							  void *p1, BTreeKeyType k1,
							  void *p2, BTreeKeyType k2);
static void o_index_chunk_key_print(BTreeDescr *desc, StringInfo buf,
									OTuple tup, Pointer arg);
static void o_index_chunk_tup_print(BTreeDescr *desc, StringInfo buf,
									OTuple tup, Pointer arg);
static int	o_index_chunk_length(BTreeDescr *desc, OTuple tuple);
static JsonbValue *o_index_chunk_key_to_jsonb(BTreeDescr *desc, OTuple tup,
											  JsonbParseState **state);
static int	o_opclass_cmp(BTreeDescr *desc,
						  void *p1, BTreeKeyType k1,
						  void *p2, BTreeKeyType k2);
static void o_opclass_key_print(BTreeDescr *desc, StringInfo buf,
								OTuple tup, Pointer arg);
static void o_opclass_tup_print(BTreeDescr *desc, StringInfo buf,
								OTuple tup, Pointer arg);
static JsonbValue *o_opclass_key_to_jsonb(BTreeDescr *desc, OTuple tup,
										  JsonbParseState **state);
static int	o_type_cache_cmp(BTreeDescr *desc, void *p1, BTreeKeyType k1,
							 void *p2, BTreeKeyType k2);
static void o_type_cache_key_print(BTreeDescr *desc, StringInfo buf,
								   OTuple key_tup, Pointer arg);
static JsonbValue *o_type_cache_key_to_jsonb(BTreeDescr *desc, OTuple tup,
											 JsonbParseState **state);

static int	o_type_cache_toast_chunk_length(BTreeDescr *desc, OTuple tuple);
static int	o_type_cache_toast_cmp(BTreeDescr *desc, void *p1,
								   BTreeKeyType k1, void *p2,
								   BTreeKeyType k2);
static void o_type_cache_toast_key_print(BTreeDescr *desc, StringInfo buf,
										 OTuple tup, Pointer arg);
static JsonbValue *o_type_cache_toast_key_to_jsonb(BTreeDescr *desc,
												   OTuple tup,
												   JsonbParseState **state);
static void o_type_cache_toast_tup_print(BTreeDescr *desc, StringInfo buf,
										 OTuple tup, Pointer arg);

static void o_range_cache_tup_print(BTreeDescr *desc, StringInfo buf,
									OTuple tup, Pointer arg);
static void o_type_element_cache_tup_print(BTreeDescr *desc, StringInfo buf,
										   OTuple tup, Pointer arg);
static void o_enumoid_cache_tup_print(BTreeDescr *desc, StringInfo buf,
									  OTuple tup, Pointer arg);

static int	free_tree_off_len_cmp(BTreeDescr *desc,
								  void *p1, BTreeKeyType k1,
								  void *p2, BTreeKeyType k2);
static int	free_tree_len_off_cmp(BTreeDescr *desc,
								  void *p1, BTreeKeyType k1,
								  void *p2, BTreeKeyType k2);
static void free_tree_print(BTreeDescr *desc, StringInfo buf,
							OTuple tup, Pointer arg);
static JsonbValue *free_tree_key_to_jsonb(BTreeDescr *desc, OTuple tup,
										  JsonbParseState **state);


static SysTreeMeta sysTreesMeta[] =
{
	{							/* SYS_TREES_SHARED_ROOT_INFO */
		.keyLength = sizeof(SharedRootInfoKey),
		.tupleLength = sizeof(SharedRootInfo),
		.cmpFunc = shared_root_info_key_cmp,
		.keyPrint = idx_descr_key_print,
		.tupPrint = idx_descr_tup_print,
		.keyToJsonb = idx_descr_key_to_jsonb,
		.poolType = OPagePoolMain,
		.undoReserveType = UndoReserveNone,
		.storageType = BTreeStorageInMemory,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_O_TABLES */
		.keyLength = sizeof(OTableChunkKey),
		.tupleLength = -1,
		.tupleLengthFunc = o_table_chunk_length,
		.cmpFunc = o_table_chunk_cmp,
		.keyPrint = o_table_chunk_key_print,
		.tupPrint = o_table_chunk_tup_print,
		.keyToJsonb = o_table_chunk_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = o_table_chunk_needs_undo
	},
	{							/* SYS_TREES_O_INDICES */
		.keyLength = sizeof(OIndexChunkKey),
		.tupleLength = -1,
		.tupleLengthFunc = o_index_chunk_length,
		.cmpFunc = o_index_chunk_cmp,
		.keyPrint = o_index_chunk_key_print,
		.tupPrint = o_index_chunk_tup_print,
		.keyToJsonb = o_index_chunk_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_OPCLASSES */
		.keyLength = sizeof(OOpclassKey),
		.tupleLength = sizeof(OOpclass),
		.cmpFunc = o_opclass_cmp,
		.keyPrint = o_opclass_key_print,
		.tupPrint = o_opclass_tup_print,
		.keyToJsonb = o_opclass_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_ENUM_CACHE */
		.keyLength = sizeof(OTypeToastChunkKey),
		.tupleLength = -1,
		.tupleLengthFunc = o_type_cache_toast_chunk_length,
		.cmpFunc = o_type_cache_toast_cmp,
		.keyPrint = o_type_cache_toast_key_print,
		.tupPrint = o_type_cache_toast_tup_print,
		.keyToJsonb = o_type_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_ENUMOID_CACHE */
		.keyLength = sizeof(OTypeKey),
		.tupleLength = sizeof(OEnumOid),
		.cmpFunc = o_type_cache_cmp,
		.keyPrint = o_type_cache_key_print,
		.tupPrint = o_enumoid_cache_tup_print,
		.keyToJsonb = o_type_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_RANGE_CACHE */
		.keyLength = sizeof(OTypeKey),
		.tupleLength = sizeof(ORangeType),
		.cmpFunc = o_type_cache_cmp,
		.keyPrint = o_type_cache_key_print,
		.tupPrint = o_range_cache_tup_print,
		.keyToJsonb = o_type_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_RECORD_CACHE */
		.keyLength = sizeof(OTypeToastChunkKey),
		.tupleLength = -1,
		.tupleLengthFunc = o_type_cache_toast_chunk_length,
		.cmpFunc = o_type_cache_toast_cmp,
		.keyPrint = o_type_cache_toast_key_print,
		.tupPrint = o_type_cache_toast_tup_print,
		.keyToJsonb = o_type_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_TYPE_ELEMENT_CACHE */
		.keyLength = sizeof(OTypeKey),
		.tupleLength = sizeof(OTypeElement),
		.cmpFunc = o_type_cache_cmp,
		.keyPrint = o_type_cache_key_print,
		.tupPrint = o_type_element_cache_tup_print,
		.keyToJsonb = o_type_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_EXTENTS_OFF_LEN */
		.keyLength = sizeof(FreeTreeTuple),
		.tupleLength = MAXALIGN(sizeof(FreeTreeTuple)),
		.cmpFunc = free_tree_off_len_cmp,
		.keyPrint = free_tree_print,
		.tupPrint = free_tree_print,
		.keyToJsonb = free_tree_key_to_jsonb,
		.poolType = OPagePoolFreeTree,
		.undoReserveType = UndoReserveNone,
		.storageType = BTreeStorageTemporary,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_EXTENTS_LEN_OFF */
		.keyLength = sizeof(FreeTreeTuple),
		.tupleLength = MAXALIGN(sizeof(FreeTreeTuple)),
		.cmpFunc = free_tree_len_off_cmp,
		.keyPrint = free_tree_print,
		.tupPrint = free_tree_print,
		.keyToJsonb = free_tree_key_to_jsonb,
		.poolType = OPagePoolFreeTree,
		.undoReserveType = UndoReserveNone,
		.storageType = BTreeStorageTemporary,
		.needs_undo = NULL
	}
};

static SysTreeShmemHeader *sysTreesShmemHeaders = NULL;
static SysTreeDescr sysTreesDescrs[SYS_TREES_NUM];

PG_FUNCTION_INFO_V1(orioledb_sys_tree_structure);
PG_FUNCTION_INFO_V1(orioledb_sys_tree_check);
PG_FUNCTION_INFO_V1(orioledb_sys_tree_rows);

/*
 * Returns size of the shared memory needed for enum tree header.
 */
Size
sys_trees_shmem_needs(void)
{
	StaticAssertStmt(SYS_TREES_NUM == sizeof(sysTreesMeta) / sizeof(SysTreeMeta),
					 "mismatch between size of sysTreesMeta and SYS_TREES_NUM");

	return mul_size(sizeof(SysTreeShmemHeader), SYS_TREES_NUM);
}

/*
 * Initializes the enum B-tree memory.
 */
void
sys_trees_shmem_init(Pointer ptr, bool found)
{
	sysTreesShmemHeaders = (SysTreeShmemHeader *) ptr;

	if (!found)
	{
		int			i;
		SysTreeShmemHeader *header;

		for (i = 0; i < SYS_TREES_NUM; i++)
		{
			header = &sysTreesShmemHeaders[i];
			header->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
			header->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
			header->rootInfo.rootPageChangeCount = 0;
		}
	}
	memset(sysTreesDescrs, 0, sizeof(sysTreesDescrs));
}


BTreeDescr *
get_sys_tree(int tree_num)
{
	Assert(tree_num >= 1 && tree_num <= SYS_TREES_NUM);
	sys_tree_init_if_needed(tree_num - 1);

	return &sysTreesDescrs[tree_num - 1].descr;
}

PrintFunc
sys_tree_key_print(BTreeDescr *desc)
{
	SysTreeMeta *meta = (SysTreeMeta *) desc->arg;

	return meta->keyPrint;
}

PrintFunc
sys_tree_tup_print(BTreeDescr *desc)
{
	SysTreeMeta *meta = (SysTreeMeta *) desc->arg;

	return meta->tupPrint;
}

static void
check_tree_num_input(int num)
{
	if (!(num >= 1 && num <= SYS_TREES_NUM))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Value num must be in the range from 1 to %d",
						SYS_TREES_NUM)));
}

/*
 * Prints structure of sys trees.
 */
Datum
orioledb_sys_tree_structure(PG_FUNCTION_ARGS)
{
	int			num = PG_GETARG_INT32(0);
	VarChar    *optionsArg = (VarChar *) PG_GETARG_VARCHAR_P(1);
	int			depth = PG_GETARG_INT32(2);
	BTreePrintOptions printOptions = {0};
	StringInfoData buf;

	check_tree_num_input(num);

	orioledb_check_shmem();
	init_print_options(&printOptions, optionsArg);

	initStringInfo(&buf);
	o_print_btree_pages(get_sys_tree(num), &buf,
						sys_tree_key_print(get_sys_tree(num)),
						sys_tree_tup_print(get_sys_tree(num)),
						NULL, &printOptions, depth);

	PG_RETURN_POINTER(cstring_to_text(buf.data));
}

Datum
orioledb_sys_tree_check(PG_FUNCTION_ARGS)
{
	int			num = PG_GETARG_INT32(0);
	bool		force_map_check = PG_GETARG_OID(1);
	bool		result = true;

	check_tree_num_input(num);

	orioledb_check_shmem();
	o_btree_load_shmem(get_sys_tree(num));

	result = check_btree(get_sys_tree(num), force_map_check);

	PG_RETURN_BOOL(result);
}

/*
 * Returns amount of all rows and dead rows
 */
Datum
orioledb_sys_tree_rows(PG_FUNCTION_ARGS)
{
	int			num = PG_GETARG_INT32(0);
	int64		total = 0,
				dead = 0;
	BTreeIterator *it;
	BTreeDescr *td;
	HeapTuple	tuple;
	TupleDesc	tupleDesc;
	Datum		values[2];
	bool		nulls[2];

	check_tree_num_input(num);
	orioledb_check_shmem();

	td = get_sys_tree(num);
	o_btree_load_shmem(get_sys_tree(num));

	/*
	 * Build a tuple descriptor for our result type
	 */
	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	it = o_btree_iterator_create(td, NULL, BTreeKeyNone,
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	do
	{
		bool		end;
		OTuple		tup = btree_iterate_raw(it, NULL, BTreeKeyNone,
											false, &end, NULL);

		if (end)
			break;
		if (O_TUPLE_IS_NULL(tup))
			dead += 1;
		total += 1;
	} while (true);

	btree_iterator_free(it);

	tupleDesc = BlessTupleDesc(tupleDesc);

	/*
	 * Build and return the tuple
	 */
	MemSet(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(total);
	values[1] = Int64GetDatum(dead);
	tuple = heap_form_tuple(tupleDesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

bool
sys_tree_supports_transactions(int tree_num)
{
	return sysTreesMeta[tree_num - 1].undoReserveType != UndoReserveNone;
}

BTreeStorageType
sys_tree_get_storage_type(int tree_num)
{
	return sysTreesMeta[tree_num - 1].storageType;
}

/*
 * Initializes the system B-tree if it is not already done.
 *
 * We can not initialize it on the shared memory startup because it uses
 * postgres file descriptors for BTreeDescr.file.
 */
static void
sys_tree_init_if_needed(int i)
{
	SysTreeShmemHeader *header;

	if (sysTreesDescrs[i].initialized)
		return;

	/*
	 * Try to initialize every system tree (avoid possible problem when
	 * walk_page() initializes system tree).  Given we initialize them at
	 * once, they all should be already initialized when walk_page() is
	 * called.
	 */
	for (i = 0; i < SYS_TREES_NUM; i++)
	{
		if (sysTreesDescrs[i].initialized)
			continue;

		header = &sysTreesShmemHeaders[i];

		LWLockAcquire(&checkpoint_state->oSharedRootInfoInsertLocks[0],
					  LW_EXCLUSIVE);
		if (!OInMemoryBlknoIsValid(header->rootInfo.rootPageBlkno))
		{
			Assert(!OInMemoryBlknoIsValid(header->rootInfo.metaPageBlkno));
			sys_tree_init(i, true);
		}
		else
		{
			sys_tree_init(i, false);
		}
		LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[0]);
	}
}

/*
 * Initializes the system B-tree.
 *
 * We can not initialize system BTree on shmem startup because it uses
 * postgres file descriptors and functions to work with them.
 *
 * Recovery worker should initialize system BTree with init_shmem = true on
 * startup. Backends should call it only with init_shmem = false.
 */
static void
sys_tree_init(int i, bool init_shmem)
{
	OPagePool  *pool;
	SysTreeShmemHeader *header;
	SysTreeMeta *meta;
	BTreeDescr *descr;
	BTreeOps   *ops;

	header = &sysTreesShmemHeaders[i];
	meta = &sysTreesMeta[i];
	pool = get_ppool(meta->poolType);
	descr = &sysTreesDescrs[i].descr;
	ops = &sysTreesDescrs[i].ops;
	descr->ops = ops;

	if (init_shmem)
	{
		header->rootInfo.rootPageBlkno = ppool_get_metapage(pool);
		header->rootInfo.metaPageBlkno = ppool_get_metapage(pool);
		header->rootInfo.rootPageChangeCount = O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(header->rootInfo.rootPageBlkno));
	}
	descr->rootInfo = header->rootInfo;

	descr->type = oIndexPrimary;
	descr->oids.datoid = SYS_TREES_DATOID;
	descr->oids.reloid = i + 1;
	descr->oids.relnode = i + 1;

	descr->arg = meta;
	ops->key_to_jsonb = meta->keyToJsonb;
	ops->len = sys_tree_len;
	ops->tuple_make_key = sys_tree_tuple_make_key;
	ops->needs_undo = meta->needs_undo;
	ops->cmp = meta->cmpFunc;
	ops->unique_hash = NULL;
	ops->hash = sys_tree_hash;

	descr->compress = InvalidOCompress;
	descr->ppool = pool;
	descr->undoType = meta->undoReserveType;
	descr->storageType = meta->storageType;

	if (descr->storageType == BTreeStoragePersistence)
	{
		checkpointable_tree_init(descr, init_shmem, NULL);
	}
	else if (descr->storageType == BTreeStorageTemporary)
	{
		evictable_tree_init(descr, init_shmem);
	}
	else if (descr->storageType == BTreeStorageInMemory)
	{
		if (init_shmem)
			o_btree_init(descr);
	}

	sysTreesDescrs[i].initialized = true;
}

static int
sys_tree_len(BTreeDescr *desc, OTuple tuple, OLengthType type)
{
	SysTreeMeta *meta = (SysTreeMeta *) desc->arg;

	if (type == OTupleLength)
	{
		if (meta->tupleLength > 0)
			return meta->tupleLength;
		else
			return meta->tupleLengthFunc(desc, tuple);
	}
	else
	{
		Assert(type == OKeyLength ||
			   type == OTupleKeyLength ||
			   type == OTupleKeyLengthNoVersion);
		return meta->keyLength;
	}
}

static uint32
sys_tree_hash(BTreeDescr *desc, OTuple tuple, BTreeKeyType kind)
{
	SysTreeMeta *meta = (SysTreeMeta *) desc->arg;

	return tag_hash(tuple.data, meta->keyLength);
}

static OTuple
sys_tree_tuple_make_key(BTreeDescr *desc, OTuple tuple, Pointer data,
						bool keep_version, bool *allocated)
{
	SysTreeMeta *meta = (SysTreeMeta *) desc->arg;

	if (data)
	{
		memcpy(data, tuple.data, meta->keyLength);
		tuple.data = data;
	}
	*allocated = false;
	return tuple;
}

static int
shared_root_info_key_cmp(BTreeDescr *desc,
						 void *p1, BTreeKeyType k1,
						 void *p2, BTreeKeyType k2)
{
	SharedRootInfoKey *key1 = (SharedRootInfoKey *) (((OTuple *) p1)->data);
	SharedRootInfoKey *key2 = (SharedRootInfoKey *) (((OTuple *) p2)->data);

	Assert(k1 != BTreeKeyBound && k2 != BTreeKeyBound);

	if (key1->datoid < key2->datoid)
		return -1;
	else if (key1->datoid > key2->datoid)
		return 1;

	if (key1->relnode < key2->relnode)
		return -1;
	else if (key1->relnode > key2->relnode)
		return 1;

	return 0;
}

static void
idx_descr_key_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	SharedRootInfoKey *key = (SharedRootInfoKey *) tup.data;

	appendStringInfo(buf, "(%u, %u)", key->datoid, key->relnode);
}

static void
idx_descr_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	SharedRootInfo *sh_descr = (SharedRootInfo *) tup.data;

	appendStringInfo(buf, "((%u, %u), %u, %u)",
					 sh_descr->key.datoid,
					 sh_descr->key.relnode,
					 sh_descr->rootInfo.rootPageBlkno,
					 sh_descr->rootInfo.metaPageBlkno);
}

static JsonbValue *
idx_descr_key_to_jsonb(BTreeDescr *desc, OTuple tup, JsonbParseState **state)
{
	SharedRootInfoKey *key = (SharedRootInfoKey *) tup.data;

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "datoid", key->datoid);
	jsonb_push_int8_key(state, "relnode", key->relnode);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

static int
o_table_chunk_cmp(BTreeDescr *desc,
				  void *p1, BTreeKeyType k1,
				  void *p2, BTreeKeyType k2)
{
	OTableChunkKey *key1;
	OTableChunkKey *key2;

	if (k1 == BTreeKeyBound)
		key1 = (OTableChunkKey *) p1;
	else
		key1 = (OTableChunkKey *) (((OTuple *) p1)->data);

	if (k2 == BTreeKeyBound)
		key2 = (OTableChunkKey *) p2;
	else
		key2 = (OTableChunkKey *) (((OTuple *) p2)->data);

	if (key1->oids.datoid < key2->oids.datoid)
		return -1;
	else if (key1->oids.datoid > key2->oids.datoid)
		return 1;

	if (key1->oids.relnode < key2->oids.relnode)
		return -1;
	else if (key1->oids.relnode > key2->oids.relnode)
		return 1;

	if (key1->offset < key2->offset)
		return -1;
	else if (key1->offset > key2->offset)
		return 1;

	return 0;
}

static int
o_table_chunk_length(BTreeDescr *desc, OTuple tuple)
{
	OTableChunk *chunk = (OTableChunk *) tuple.data;

	return offsetof(OTableChunk, data) + chunk->dataLength;
}

static void
o_table_chunk_key_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	OTableChunkKey *key = (OTableChunkKey *) tup.data;

	appendStringInfo(buf, "((%u, %u, %u), %u, %u)", key->oids.datoid,
					 key->oids.relnode, key->oids.reloid, key->offset,
					 key->version);
}

static void
o_table_chunk_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	OTableChunk *chunk = (OTableChunk *) tup.data;

	appendStringInfo(buf, "(((%u, %u, %u), %u, %u), %u)",
					 chunk->key.oids.datoid,
					 chunk->key.oids.relnode,
					 chunk->key.oids.reloid,
					 chunk->key.offset,
					 chunk->key.version,
					 chunk->dataLength);
}

static JsonbValue *
o_table_chunk_key_to_jsonb(BTreeDescr *desc, OTuple tup, JsonbParseState **state)
{
	OTableChunkKey *key = (OTableChunkKey *) tup.data;

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "datoid", key->oids.datoid);
	jsonb_push_int8_key(state, "reloid", key->oids.reloid);
	jsonb_push_int8_key(state, "relnode", key->oids.relnode);
	jsonb_push_int8_key(state, "offset", key->offset);
	jsonb_push_int8_key(state, "version", key->version);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

static bool
o_table_chunk_needs_undo(BTreeDescr *desc, BTreeOperationType action,
						 OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
						 OTuple newTuple, OXid newOxid)
{
	OTableChunkKey *old_tuple_key = (OTableChunkKey *) oldTuple.data;
	OTableChunkKey *new_tuple_key = (OTableChunkKey *) newTuple.data;

	if (action == BTreeOperationDelete)
		return true;

	if (!XACT_INFO_OXID_EQ(oldXactInfo, newOxid))
		return false;

	if (oldDeleted && old_tuple_key->version + 1 == new_tuple_key->version)
		return false;

	if (new_tuple_key && old_tuple_key->version >= new_tuple_key->version)
		return false;

	return true;
}

static int
o_index_chunk_cmp(BTreeDescr *desc,
				  void *p1, BTreeKeyType k1,
				  void *p2, BTreeKeyType k2)
{
	OIndexChunkKey *key1;
	OIndexChunkKey *key2;

	if (k1 == BTreeKeyBound)
		key1 = (OIndexChunkKey *) p1;
	else
		key1 = (OIndexChunkKey *) (((OTuple *) p1)->data);

	if (k2 == BTreeKeyBound)
		key2 = (OIndexChunkKey *) p2;
	else
		key2 = (OIndexChunkKey *) (((OTuple *) p2)->data);

	if (key1->type != key2->type)
		return (key1->type < key2->type) ? -1 : 1;

	if (key1->oids.datoid != key2->oids.datoid)
		return (key1->oids.datoid < key2->oids.datoid) ? -1 : 1;

	if (key1->oids.relnode != key2->oids.relnode)
		return (key1->oids.relnode < key2->oids.relnode) ? -1 : 1;

	if (key1->offset != key2->offset)
		return (key1->offset < key2->offset) ? -1 : 1;

	return 0;
}

static int
o_index_chunk_length(BTreeDescr *desc, OTuple tuple)
{
	OIndexChunk *chunk = (OIndexChunk *) tuple.data;

	return offsetof(OIndexChunk, data) + chunk->dataLength;
}

static void
o_index_chunk_key_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	OIndexChunkKey *key = (OIndexChunkKey *) tup.data;

	appendStringInfo(buf, "(%d, (%u, %u, %u), %u)", (int) key->type, key->oids.datoid, key->oids.relnode, key->oids.reloid, key->offset);
}

static void
o_index_chunk_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	OIndexChunk *chunk = (OIndexChunk *) tup.data;

	appendStringInfo(buf, "((%d, (%u, %u, %u), %u), %u)",
					 (int) chunk->key.type,
					 chunk->key.oids.datoid,
					 chunk->key.oids.relnode,
					 chunk->key.oids.reloid,
					 chunk->key.offset,
					 chunk->dataLength);
}

static JsonbValue *
o_index_chunk_key_to_jsonb(BTreeDescr *desc, OTuple tup, JsonbParseState **state)
{
	OIndexChunkKey *key = (OIndexChunkKey *) tup.data;

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "type", (int64) key->type);
	jsonb_push_int8_key(state, "datoid", key->oids.datoid);
	jsonb_push_int8_key(state, "reloid", key->oids.reloid);
	jsonb_push_int8_key(state, "relnode", key->oids.relnode);
	jsonb_push_int8_key(state, "offset", key->offset);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

/*
 * Comparison function for the opclass B-tree.
 */
static int
o_opclass_cmp(BTreeDescr *desc,
			  void *p1, BTreeKeyType k1,
			  void *p2, BTreeKeyType k2)
{
	OOpclassKey *key1 = (OOpclassKey *) (((OTuple *) p1)->data);
	OOpclassKey *key2 = (OOpclassKey *) (((OTuple *) p2)->data);

	Assert(k1 != BTreeKeyBound);
	Assert(k2 != BTreeKeyBound);

	if (key1->datoid != key2->datoid)
		return key1->datoid < key2->datoid ? -1 : 1;

	if (key1->opclassoid != key2->opclassoid)
		return key1->opclassoid < key2->opclassoid ? -1 : 1;

	return 0;
}

/*
 * A key print function for o_print_btree_pages()
 */
static void
o_opclass_key_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	OOpclassKey *key = (OOpclassKey *) tup.data;

	appendStringInfo(buf, "(%u, %u)", key->datoid, key->opclassoid);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
static void
o_opclass_tup_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	OOpclass   *comparator = (OOpclass *) tup.data;

	appendStringInfo(
					 buf, "((%u, %u), opfamily: %u, has_ssup: %c,"
					 " inputtype: %u, cmpproc: ('%s', '%s'),"
					 " sortsupportproc: ('%s', '%s'))",
					 comparator->key.datoid, comparator->key.opclassoid,
					 comparator->opfamily, comparator->hasSsup ? 'Y' : 'N',
					 comparator->inputtype, comparator->cmpProc.prosrc,
					 comparator->cmpProc.probin, comparator->ssupProc.prosrc,
					 comparator->ssupProc.probin);
}

static JsonbValue *
o_opclass_key_to_jsonb(BTreeDescr *desc, OTuple tup, JsonbParseState **state)
{
	OOpclassKey *key = (OOpclassKey *) tup.data;

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "datoid", key->datoid);
	jsonb_push_int8_key(state, "opclassoid", key->opclassoid);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

/*
 * Comparison function for non-TOAST typecache B-tree.
 *
 * If none of the arguments is BTreeKeyBound it comparses by both
 * oid and lsn. It make possible to insert values with same oid.
 * Else it comparses only by oid, which is used by other operations than
 * insert, to find all rows with exact oid.
 * If key kind is not BTreeKeyBound it expects that OTuple passed.
 */
static int
o_type_cache_cmp(BTreeDescr *desc, void *p1, BTreeKeyType k1, void *p2,
				 BTreeKeyType k2)
{
	OTypeKey   *key1;
	OTypeKey   *key2;

	OTypeKey	_key;
	bool		lsn_cmp = true;

	if (k1 == BTreeKeyBound)
	{
		key1 = &_key;
		key1->datoid = ((OTypeKeyBound *) p1)->datoid;
		key1->oid = ((OTypeKeyBound *) p1)->oid;
		lsn_cmp = false;
	}
	else
		key1 = (OTypeKey *) (((OTuple *) p1)->data);

	if (k2 == BTreeKeyBound)
	{
		key2 = &_key;
		key2->datoid = ((OTypeKeyBound *) p2)->datoid;
		key2->oid = ((OTypeKeyBound *) p2)->oid;
		lsn_cmp = false;
	}
	else
		key2 = (OTypeKey *) (((OTuple *) p2)->data);

	if (key1->datoid != key2->datoid)
		return key1->datoid < key2->datoid ? -1 : 1;

	if (key1->oid != key2->oid)
		return key1->oid < key2->oid ? -1 : 1;

	if (lsn_cmp)
		if (key1->insert_lsn != key2->insert_lsn)
			return key1->insert_lsn < key2->insert_lsn ? -1 : 1;

	return 0;
}

/*
 * Generic non-TOAST typecache key print function for o_print_btree_pages()
 */
static void
o_type_cache_key_print(BTreeDescr *desc, StringInfo buf,
					   OTuple key_tup, Pointer arg)
{
	OTypeKey   *key = (OTypeKey *) key_tup.data;
	uint32		id,
				off;

	/* Decode ID and offset */
	id = (uint32) (key->insert_lsn >> 32);
	off = (uint32) key->insert_lsn;

	appendStringInfo(buf, "(%u, %u, %X/%X, %c)",
					 key->datoid,
					 key->oid,
					 id,
					 off,
					 key->deleted ? 'Y' : 'N');
}

static void
o_type_cache_key_push_to_jsonb_state(OTypeKey *key, JsonbParseState **state)
{
	jsonb_push_int8_key(state, "datoid", key->datoid);
	jsonb_push_int8_key(state, "oid", key->oid);
	jsonb_push_int8_key(state, "insert_lsn", key->insert_lsn);
	jsonb_push_bool_key(state, "deleted", key->deleted);
}

static JsonbValue *
o_type_cache_key_to_jsonb(BTreeDescr *desc, OTuple tup,
						  JsonbParseState **state)
{
	OTypeKey   *key = (OTypeKey *) tup.data;

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	o_type_cache_key_push_to_jsonb_state(key, state);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

static int
o_type_cache_toast_chunk_length(BTreeDescr *desc, OTuple tuple)
{
	OTypeToastChunk *chunk = (OTypeToastChunk *) tuple.data;

	return offsetof(OTypeToastChunk, data) + chunk->dataLength;
}

/*
 * Comparison function for TOAST typecache B-tree.
 *
 * If key kind BTreeKeyBound it expects OTypeToastKeyBound.
 * Otherwise it expects that OTuple passed.
 * It wraps OTypeToastChunkKey to OTuple to pass it to o_type_cache_cmp.
 */
static int
o_type_cache_toast_cmp(BTreeDescr *desc, void *p1, BTreeKeyType k1,
					   void *p2, BTreeKeyType k2)
{
	OTypeToastChunkKey *key1 = NULL;
	OTypeToastChunkKey *key2 = NULL;
	OTypeToastChunkKey _key = {0};
	OTuple		key_tuple1 = {0},
				key_tuple2 = {0};
	Pointer		type_key_cmp_arg1 = NULL,
				type_key_cmp_arg2 = NULL;
	int			type_key_cmp_result;

	if (k1 == BTreeKeyBound)
	{
		Assert(k2 != BTreeKeyBound);
		key1 = &_key;
		*key1 = ((OTypeToastKeyBound *) p1)->chunk_key;
		if (((OTypeToastKeyBound *) p1)->lsn_cmp)
			k1 = BTreeKeyNonLeafKey;	/* make o_type_cache_cmp to compare by
										 * lsn */
		else
			type_key_cmp_arg1 = p1;
	}
	else
		key1 = (OTypeToastChunkKey *) ((OTuple *) p1)->data;

	if (!type_key_cmp_arg1)
	{
		key_tuple1.data = (Pointer) key1;
		type_key_cmp_arg1 = (Pointer) &key_tuple1;
	}

	if (k2 == BTreeKeyBound)
	{
		Assert(k1 != BTreeKeyBound);
		key2 = &_key;
		*key2 = ((OTypeToastKeyBound *) p2)->chunk_key;
		if (((OTypeToastKeyBound *) p2)->lsn_cmp)
			k2 = BTreeKeyNonLeafKey;	/* make o_type_cache_cmp to compare by
										 * lsn */
		else
			type_key_cmp_arg2 = p2;
	}
	else
		key2 = (OTypeToastChunkKey *) ((OTuple *) p2)->data;

	if (!type_key_cmp_arg2)
	{
		key_tuple2.data = (Pointer) key2;
		type_key_cmp_arg2 = (Pointer) &key_tuple2;
	}

	type_key_cmp_result = o_type_cache_cmp(desc, type_key_cmp_arg1, k1,
										   type_key_cmp_arg2, k2);

	if (type_key_cmp_result != 0)
		return type_key_cmp_result;

	if (key1->offset != key2->offset)
		return key1->offset < key2->offset ? -1 : 1;

	return 0;
}

/*
 * Generic TOAST typecache key print function for o_print_btree_pages()
 */
static void
o_type_cache_toast_key_print(BTreeDescr *desc, StringInfo buf,
							 OTuple tup, Pointer arg)
{
	OTypeToastChunkKey *key = (OTypeToastChunkKey *) tup.data;

	appendStringInfo(buf, "(");
	o_type_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", %u)",
					 key->offset);
}

static JsonbValue *
o_type_cache_toast_key_to_jsonb(BTreeDescr *desc, OTuple tup,
								JsonbParseState **state)
{
	OTypeToastChunkKey *key = (OTypeToastChunkKey *) tup.data;

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	o_type_cache_key_push_to_jsonb_state(&key->type_key, state);
	jsonb_push_int8_key(state, "offset", key->offset);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
static void
o_range_cache_tup_print(BTreeDescr *desc, StringInfo buf,
						OTuple tup, Pointer arg)
{
	ORangeType *o_range = (ORangeType *) tup.data;

	appendStringInfo(buf, "(");
	o_type_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", elem_typlen: %d, elem_typbyval: %c, elem_typalign: %d,"
					 "rng_collation: %d, rng_cmp_proc: ('%s', '%s'))",
					 o_range->elem_typlen,
					 o_range->elem_typbyval ? 'Y' : 'N',
					 o_range->elem_typalign,
					 o_range->rng_collation,
					 o_range->rng_cmp_proc.prosrc,
					 o_range->rng_cmp_proc.probin);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
static void
o_type_cache_toast_tup_print(BTreeDescr *desc, StringInfo buf,
							 OTuple tup, Pointer arg)
{
	OTypeToastChunk *o_record_chunk = (OTypeToastChunk *) tup.data;

	appendStringInfo(buf, "(");
	o_type_cache_toast_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", %u)", o_record_chunk->dataLength);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
static void
o_type_element_cache_tup_print(BTreeDescr *desc, StringInfo buf,
							   OTuple tup, Pointer arg)
{
	OTypeElement *o_type_element = (OTypeElement *) tup.data;

	appendStringInfo(buf, "(");
	o_type_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", typlen: %d, typbyval: %c, typalign: %d, "
					 "cmp_proc: ('%s', '%s'))",
					 o_type_element->typlen,
					 o_type_element->typbyval ? 'Y' : 'N',
					 o_type_element->typalign,
					 o_type_element->cmp_proc.prosrc,
					 o_type_element->cmp_proc.probin);
}

/*
 * A tuple print function for o_print_btree_pages()
 */
static void
o_enumoid_cache_tup_print(BTreeDescr *desc, StringInfo buf,
						  OTuple tup, Pointer arg)
{
	OEnumOid   *o_enumoid = (OEnumOid *) tup.data;

	appendStringInfo(buf, "(");
	o_type_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", %d)", o_enumoid->enumtypid);
}

/*
 * Compares oids and ix_num of FreeTreeTuples.
 */
static inline int
free_tree_id_cmp(FreeTreeTuple *left, FreeTreeTuple *right)
{
	if (left->ixType != right->ixType)
		return left->ixType < right->ixType ? -1 : 1;
	if (left->datoid != right->datoid)
		return left->datoid < right->datoid ? -1 : 1;
	if (left->relnode != right->relnode)
		return left->relnode < right->relnode ? -1 : 1;
	return 0;
}

/*
 * Comparator for sort order inside a B-tree:
 * 1. FreeTreeTuple.datoid
 * 2. FreeTreeTuple.relnode
 * 3. FreeTreeTuple.ix_num
 * 4. FreeTreeTuple.extent.off
 */
static int
free_tree_off_len_cmp(BTreeDescr *desc,
					  void *p1, BTreeKeyType k1,
					  void *p2, BTreeKeyType k2)
{
	FreeTreeTuple *left = (FreeTreeTuple *) ((OTuple *) p1)->data,
			   *right = (FreeTreeTuple *) ((OTuple *) p2)->data;
	int			cmp = free_tree_id_cmp(left, right);

	if (cmp != 0)
		return cmp;

	if (left->extent.offset != right->extent.offset)
		return left->extent.offset < right->extent.offset ? -1 : 1;

	return 0;
}

/*
 * Comparator for sort order inside a B-tree:
 * 1. FreeTreeTuple.datoid
 * 2. FreeTreeTuple.relnode
 * 3. FreeTreeTuple.ix_num
 * 4. FreeTreeTuple.extent.len
 * 5. FreeTreeTuple.extent.off
 */
static int
free_tree_len_off_cmp(BTreeDescr *desc,
					  void *p1, BTreeKeyType k1,
					  void *p2, BTreeKeyType k2)
{
	FreeTreeTuple *left = (FreeTreeTuple *) ((OTuple *) p1)->data,
			   *right = (FreeTreeTuple *) ((OTuple *) p2)->data;
	int			cmp = free_tree_id_cmp(left, right);

	if (cmp != 0)
		return cmp;

	if (left->extent.length != right->extent.length)
		return left->extent.length < right->extent.length ? -1 : 1;

	if (left->extent.offset != right->extent.offset)
		return left->extent.offset < right->extent.offset ? -1 : 1;

	return 0;
}

static void
free_tree_print(BTreeDescr *desc, StringInfo buf, OTuple tup, Pointer arg)
{
	FreeTreeTuple *f_tree_tup = (FreeTreeTuple *) tup.data;

	appendStringInfo(
					 buf, "((%u, %u, %u), %lu, %lu)",
					 f_tree_tup->ixType,
					 f_tree_tup->datoid,
					 f_tree_tup->relnode,
					 f_tree_tup->extent.offset,
					 f_tree_tup->extent.length);
}

static JsonbValue *
free_tree_key_to_jsonb(BTreeDescr *desc, OTuple tup, JsonbParseState **state)
{
	FreeTreeTuple *key = (FreeTreeTuple *) tup.data;

	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "ixType", (int64) key->ixType);
	jsonb_push_int8_key(state, "datoid", key->datoid);
	jsonb_push_int8_key(state, "relnode", key->relnode);
	jsonb_push_int8_key(state, "offset", key->extent.offset);
	jsonb_push_int8_key(state, "length", key->extent.length);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}
