/*-------------------------------------------------------------------------
 *
 * sys_trees.c
 *		Defitions for system trees.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
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
#include "catalog/o_sys_cache.h"
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
	int			(*keyLengthFunc) (BTreeDescr *desc, OTuple tuple);
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
	Pointer		extra;
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
static bool o_index_chunk_needs_undo(BTreeDescr *desc, BTreeOperationType action,
									 OTuple oldTuple, OTupleXactInfo oldXactInfo,
									 bool oldDeleted, OTuple newTuple,
									 OXid newOxid);

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
		.needs_undo = o_index_chunk_needs_undo
	},
	{							/* SYS_TREES_OPCLASS_CACHE */
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OOpclass),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_opclass_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_ENUM_CACHE */
		.keyLength = -1,
		.keyLengthFunc = o_sys_cache_key_length,
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_tup_length,
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_enum_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_ENUMOID_CACHE */
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OEnumOid),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_enumoid_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_RANGE_CACHE */
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(ORange),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_range_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_CLASS_CACHE */
		.keyLength = sizeof(OSysCacheToastChunkKey1),
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_toast_chunk_length,
		.cmpFunc = o_sys_cache_toast_cmp,
		.keyPrint = o_sys_cache_toast_key_print,
		.tupPrint = o_sys_cache_toast_tup_print,
		.keyToJsonb = o_sys_cache_toast_key_to_jsonb,
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
	},
	{							/* SYS_TREES_PROC_CACHE */
		.keyLength = sizeof(OSysCacheToastChunkKey1),
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_toast_chunk_length,
		.cmpFunc = o_sys_cache_toast_cmp,
		.keyPrint = o_sys_cache_toast_key_print,
		.tupPrint = o_sys_cache_toast_tup_print,
		.keyToJsonb = o_sys_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_TYPE_CACHE */
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OType),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_type_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_AGG_CACHE */
		.keyLength = sizeof(OSysCacheToastChunkKey1),
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_toast_chunk_length,
		.cmpFunc = o_sys_cache_toast_cmp,
		.keyPrint = o_sys_cache_toast_key_print,
		.tupPrint = o_sys_cache_toast_tup_print,
		.keyToJsonb = o_sys_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_OPER_CACHE */
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OOperator),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_operator_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_AMOP_CACHE */
		.keyLength = sizeof(OSysCacheKey3),
		.tupleLength = sizeof(OAmOp),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_amop_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_AMPROC_CACHE */
		.keyLength = sizeof(OSysCacheKey4),
		.tupleLength = sizeof(OAmProc),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_amproc_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_COLLATION_CACHE */
		.keyLength = sizeof(OSysCacheToastChunkKey1),
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_toast_chunk_length,
		.cmpFunc = o_sys_cache_toast_cmp,
		.keyPrint = o_sys_cache_toast_key_print,
		.tupPrint = o_sys_cache_toast_tup_print,
		.keyToJsonb = o_sys_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_DATABASE_CACHE */
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(ODatabase),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_database_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							/* SYS_TREES_AMOP_STRAT_CACHE */
		.keyLength = sizeof(OSysCacheKey4),
		.tupleLength = sizeof(OAmOpStrat),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_amop_strat_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
#if PG_VERSION_NUM >= 140000
	{							/* SYS_TREES_MULTIRANGE_CACHE */
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OMultiRange),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_multirange_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoReserveType = UndoReserveTxn,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	}
#endif
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

static JsonbValue *
o_tuphdr_to_jsonb(BTreeLeafTuphdr *tupHdr, JsonbParseState **state)
{
	(void) pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_bool_key(state, "deleted", tupHdr->deleted);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

/*
 * Returns content of sys tree as table
 */
Datum
orioledb_sys_tree_rows(PG_FUNCTION_ARGS)
{
	int			num = PG_GETARG_INT32(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	BTreeIterator *it;
	BTreeDescr *td;
	Datum		values[1];
	bool		nulls[1] = {false};
	Oid			funcrettype;

	check_tree_num_input(num);
	orioledb_check_shmem();

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, &funcrettype, NULL) != TYPEFUNC_SCALAR)
		elog(ERROR, "return type must be a scalar type");

	/* Base data type, i.e. scalar */
	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, NULL, funcrettype, -1, 0);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	td = get_sys_tree(num);
	o_btree_load_shmem(get_sys_tree(num));

	it = o_btree_iterator_create(td, NULL, BTreeKeyNone,
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	do
	{
		bool		end;
		OTuple		key;
		bool		allocated;
		JsonbParseState *state = NULL;
		Jsonb	   *res;
		BTreeLeafTuphdr *tupHdr;
		OTuple		tup;

		tup = btree_iterate_all(it, NULL, BTreeKeyNone, false, &end, NULL,
								&tupHdr);

		if (O_TUPLE_IS_NULL(tup))
			break;

		(void) pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		key = o_btree_tuple_make_key(td, tup, NULL, true, &allocated);
		jsonb_push_key(&state, "tupHdr");
		(void) o_tuphdr_to_jsonb(tupHdr, &state);
		jsonb_push_key(&state, "key");
		(void) o_btree_key_to_jsonb(td, key, &state);
		res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
		if (allocated)
			pfree(key.data);

		values[0] = PointerGetDatum(res);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values,
							 nulls);
	} while (true);

	btree_iterator_free(it);

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
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

void
sys_tree_set_extra(int tree_num, Pointer extra)
{
	sysTreesMeta[tree_num - 1].extra = extra;
}

Pointer
sys_tree_get_extra(int tree_num)
{
	return sysTreesMeta[tree_num - 1].extra;
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
	descr->createOxid = InvalidOXid;

	if (descr->storageType == BTreeStoragePersistence)
	{
		checkpointable_tree_init(descr, init_shmem, NULL);
	}
	else if (descr->storageType == BTreeStorageTemporary)
	{
		evictable_tree_init(descr, init_shmem, NULL);
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
		if (meta->keyLength > 0)
			return meta->keyLength;
		else
			return meta->keyLengthFunc(desc, tuple);
	}
}

static uint32
sys_tree_hash(BTreeDescr *desc, OTuple tuple, BTreeKeyType kind)
{
	int			keyLength = sys_tree_len(desc, tuple, OTupleKeyLength);

	return tag_hash(tuple.data, keyLength);
}

static OTuple
sys_tree_tuple_make_key(BTreeDescr *desc, OTuple tuple, Pointer data,
						bool keep_version, bool *allocated)
{
	if (data)
	{
		int			keyLength = sys_tree_len(desc, tuple, OTupleKeyLength);

		memcpy(data, tuple.data, keyLength);
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

static bool
o_index_chunk_needs_undo(BTreeDescr *desc, BTreeOperationType action,
						 OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
						 OTuple newTuple, OXid newOxid)
{
	return true;
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
