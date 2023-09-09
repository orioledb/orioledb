/*-------------------------------------------------------------------------
 *
 * o_sys_cache.h
 *		Generic interface for system catalog duplicate trees.
 *
 * Generic system catalog tree interface that used to prevent syscache
 * usage during recovery. System catalog cache trees shoud use o_sys_cache_*
 * functions in sysTreesMeta (sys_trees.c), but if sys cache is
 * not TOAST tup_print function should be also provided.
 * Sys cache lookups are also cached in local backend mamory.
 * Cache entry invalidation is performed by syscache hook.
 * Instead of physical deletion of sys cache entry we mark it as deleted.
 * Normally only not deleted entries used. During recovery we use
 * sys cache entries accroding to current WAL position.
 * Physical deletion of deleted values is performed during checkpoint,
 * which is also called after successed recovery.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/o_sys_cache.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __O_SYS_CACHE_H__
#define __O_SYS_CACHE_H__

#include "orioledb.h"

#include "catalog/o_tables.h"
#include "catalog/sys_trees.h"
#include "miscadmin.h"
#include "recovery/recovery.h"
#include "utils/catcache.h"
#include "utils/pg_locale.h"
#include "utils/resowner_private.h"

#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif

/*
 * Database oid to be used for sys cache entries comparison.
 */
extern Oid	o_sys_cache_search_datoid;

typedef uint32 OSysCacheHashKey;	/* GetSysCacheHashValue result type */

typedef struct OSysCache OSysCache;

typedef struct OSysCacheKeyCommon
{
	Oid			datoid;
	XLogRecPtr	lsn;
	bool		deleted;
	int			dataLength;
} OSysCacheKeyCommon;

typedef struct OSysCacheKey
{
	OSysCacheKeyCommon common;
	Datum		keys[FLEXIBLE_ARRAY_MEMBER];
} OSysCacheKey;

#define O_KEY_GET_NAME(key, att_num) (((key)->common.dataLength == 0) ? \
		DatumGetName((key)->keys[att_num]) : \
		((Name) (((Pointer) (key)) + (key)->keys[att_num])))

typedef struct OSysCacheBound
{
	OSysCacheKey *key;
	int			nkeys;
} OSysCacheBound;

/* Key of entry stored in non-TOAST sys cache tree */
typedef struct
{
	OSysCacheKeyCommon common;
	Datum		keys[1];
} OSysCacheKey1;

typedef struct
{
	OSysCacheKeyCommon common;
	Datum		keys[2];
} OSysCacheKey2;

typedef struct
{
	OSysCacheKeyCommon common;
	Datum		keys[3];
} OSysCacheKey3;

typedef struct
{
	OSysCacheKeyCommon common;
	Datum		keys[4];
} OSysCacheKey4;

/* Key of chunks of entry stored in TOAST sys cache tree */
typedef struct OSysCacheToastChunkKeyCommon
{
	uint32		offset;
} OSysCacheToastChunkKeyCommon;

/* Key of chunks of entry stored in TOAST sys cache tree */
typedef struct
{
	OSysCacheToastChunkKeyCommon common;
	OSysCacheKey sys_cache_key;
} OSysCacheToastChunkKey;

/* Key of chunks of entry stored in TOAST sys cache tree */
typedef struct
{
	OSysCacheToastChunkKeyCommon common;
	OSysCacheKey1 sys_cache_key;
}			OSysCacheToastChunkKey1;

/* Key of chunks of entry stored in TOAST sys cache tree */
typedef struct
{
	OSysCacheToastChunkKeyCommon common;
	OSysCacheKey2 sys_cache_key;
}			OSysCacheToastChunkKey2;

/* Key of chunks of entry stored in TOAST sys cache tree */
typedef struct
{
	OSysCacheToastChunkKeyCommon common;
	OSysCacheKey3 sys_cache_key;
}			OSysCacheToastChunkKey3;

/* Key of chunks of entry stored in TOAST sys cache tree */
typedef struct
{
	OSysCacheToastChunkKeyCommon common;
	OSysCacheKey4 sys_cache_key;
}			OSysCacheToastChunkKey4;

/* Key by which entry searched in TOAST sys cache tree */
typedef struct
{
	OSysCacheToastChunkKeyCommon common;
	OSysCacheKey *key;
	bool		lsn_cmp;
} OSysCacheToastKeyBound;

/* Chunks of entry stored in TOAST sys cache tree */
typedef struct OSysCacheToastChunkCommon
{
	uint32		dataLength;
} OSysCacheToastChunkCommon;

/* Chunks of entry stored in TOAST sys cache tree */
typedef struct OSysCacheToastChunk1
{
	OSysCacheToastChunkKey1 key;
	OSysCacheToastChunkCommon common;
	char		data[FLEXIBLE_ARRAY_MEMBER];
}			OSysCacheToastChunk1;

/* Chunks of entry stored in TOAST sys cache tree */
typedef struct OSysCacheToastChunk2
{
	OSysCacheToastChunkKey2 key;
	OSysCacheToastChunkCommon common;
	char		data[FLEXIBLE_ARRAY_MEMBER];
}			OSysCacheToastChunk2;

/* Chunks of entry stored in TOAST sys cache tree */
typedef struct OSysCacheToastChunk3
{
	OSysCacheToastChunkKey3 key;
	OSysCacheToastChunkCommon common;
	char		data[FLEXIBLE_ARRAY_MEMBER];
}			OSysCacheToastChunk3;

/* Chunks of entry stored in TOAST sys cache tree */
typedef struct OSysCacheToastChunk4
{
	OSysCacheToastChunkKey4 key;
	OSysCacheToastChunkCommon common;
	char		data[FLEXIBLE_ARRAY_MEMBER];
}			OSysCacheToastChunk4;

typedef struct OSysCacheFuncs
{
	/*
	 * Should be always set. Used in invalidation hook to cleanup entry saved
	 * in fastcache. Also used inside o_sys_cache_add_if_needed.
	 */
	void		(*free_entry) (Pointer entry);

	/*
	 * Should be always set. Used inside o_sys_cache_add_if_needed and
	 * o_sys_cache_update_if_needed. On add entry_ptr is NULL, entry should be
	 * created and returned.
	 */
	void		(*fill_entry) (Pointer *entry_ptr, OSysCacheKey *key,
							   Pointer arg);

	/*
	 * Used in toast sys cache trees. Should return pointer to binary
	 * serialized data and it's length.
	 */
	Pointer		(*toast_serialize_entry) (Pointer entry, int *len);

	/*
	 * Used in toast sys cache trees. Should return pointer to constructed
	 * entry of a tree.
	 */
	Pointer		(*toast_deserialize_entry) (MemoryContext mcxt,
											Pointer data,
											Size length);
} OSysCacheFuncs;

typedef uint32 (*O_CCHashFN) (OSysCacheKey *key, int att_num);

typedef struct OSysCache
{
	int			sys_tree_num;
	bool		is_toast;
	Oid			cc_indexoid;
	int			cacheId;
	int			nkeys;
	Oid			keytypes[CATCACHE_MAXKEYS];
	int			data_len;
	MemoryContext mcxt;			/* context where stored entries from fast
								 * cache */
	HTAB	   *fast_cache;		/* contains OSysCacheHashEntry-s */
	O_CCHashFN	cc_hashfunc[CATCACHE_MAXKEYS];
	OSysCacheHashKey last_fast_cache_key;
	Pointer		last_fast_cache_entry;
	OSysCacheFuncs *funcs;
} OSysCache;

/*
 * Initializes all sys catalog caches.
 */
extern void o_sys_caches_init(void);

extern OSysCache *o_create_sys_cache(int sys_tree_num, bool is_toast,
									 Oid cc_indexoid,	/* cacheinfo indoid */
									 int cacheId,	/* cacheinfo array index */
									 int nkeys,
									 Oid *keytypes,
									 int data_len,
									 HTAB *fast_cache,
									 MemoryContext mcxt,
									 OSysCacheFuncs *funcs);
extern Pointer o_sys_cache_search(OSysCache *sys_cache, int nkeys,
								  OSysCacheKey *key);
extern void o_sys_cache_add_if_needed(OSysCache *sys_cache, OSysCacheKey *key,
									  Pointer arg);
extern void o_sys_cache_update_if_needed(OSysCache *sys_cache,
										 OSysCacheKey *key, Pointer arg);
extern bool o_sys_cache_delete(OSysCache *sys_cache, OSysCacheKey *key);

extern void custom_types_add_all(OTable *o_table, OTableIndex *o_table_index);
extern void custom_type_add_if_needed(Oid datoid, Oid typoid,
									  XLogRecPtr insert_lsn);

extern void o_sys_caches_delete_by_lsn(XLogRecPtr checkPointRedo);

extern void orioledb_setup_syscache_hooks(void);

extern int	o_sys_cache_key_length(BTreeDescr *desc, OTuple tuple);
extern int	o_sys_cache_tup_length(BTreeDescr *desc, OTuple tuple);
extern int	o_sys_cache_cmp(BTreeDescr *desc, void *p1, BTreeKeyType k1,
							void *p2, BTreeKeyType k2);
extern void o_sys_cache_key_print(BTreeDescr *desc, StringInfo buf,
								  OTuple key_tup, Pointer arg);
extern JsonbValue *o_sys_cache_key_to_jsonb(BTreeDescr *desc, OTuple tup,
											JsonbParseState **state);

extern int	o_sys_cache_toast_chunk_length(BTreeDescr *desc, OTuple tuple);
extern int	o_sys_cache_toast_cmp(BTreeDescr *desc, void *p1,
								  BTreeKeyType k1, void *p2,
								  BTreeKeyType k2);
extern void o_sys_cache_toast_key_print(BTreeDescr *desc, StringInfo buf,
										OTuple tup, Pointer arg);
extern JsonbValue *o_sys_cache_toast_key_to_jsonb(BTreeDescr *desc,
												  OTuple tup,
												  JsonbParseState **state);
extern void o_sys_cache_toast_tup_print(BTreeDescr *desc, StringInfo buf,
										OTuple tup, Pointer arg);

#define O_SYS_CACHE_INIT_FUNC(cache_name) \
void o_##cache_name##_init(MemoryContext mcxt, HTAB *fastcache)

#define OSC_REP_PREF1(PREFIX,X) PREFIX X##1
#define OSC_REP_PREF2(PREFIX,X) OSC_REP_PREF1(PREFIX,X), PREFIX X##2
#define OSC_REP_PREF3(PREFIX,X) OSC_REP_PREF2(PREFIX,X), PREFIX X##3
#define OSC_REP_PREF4(PREFIX,X) OSC_REP_PREF3(PREFIX,X), PREFIX X##4

#define OSC_REP_DATUM(ONES,X) OSC_REP_PREF##ONES(Datum,X)

#define OSC_REP_ARGS(ONES) OSC_REP_PREF##ONES(,arg)

#define O_SYS_CACHE_ARGS(nkeys) OSC_REP_DATUM(nkeys,arg)

#define O_SYS_CACHE_DECLS(cache_name, elem_type, nkeys)						\
extern O_SYS_CACHE_INIT_FUNC(cache_name);									\
extern bool o_##cache_name##_delete(Oid datoid, O_SYS_CACHE_ARGS(nkeys));	\
void o_##cache_name##_update_if_needed(Oid datoid, O_SYS_CACHE_ARGS(nkeys),	\
									   Pointer arg);						\
void o_##cache_name##_add_if_needed(Oid datoid, O_SYS_CACHE_ARGS(nkeys),	\
									XLogRecPtr insert_lsn,					\
									Pointer arg);							\
extern int no_such_variable

#define O_SYS_CACHE_FUNCS(cache_name, elem_type, nkeys)						\
	static inline elem_type *o_##cache_name##_search(						\
		Oid datoid,	O_SYS_CACHE_ARGS(nkeys), XLogRecPtr search_lsn,			\
		int nkeys_arg);														\
	elem_type *																\
	o_##cache_name##_search(Oid datoid, O_SYS_CACHE_ARGS(nkeys),			\
							XLogRecPtr search_lsn, int nkeys_arg)			\
	{																		\
		OSysCacheKey##nkeys key = {.common = {.datoid = datoid,				\
											  .lsn = search_lsn},			\
								   .keys = {OSC_REP_ARGS(nkeys)}};			\
		return (elem_type *)												\
			o_sys_cache_search(cache_name, nkeys_arg,						\
							   (OSysCacheKey *) &key);						\
	}																		\
	bool																	\
	o_##cache_name##_delete(Oid datoid, O_SYS_CACHE_ARGS(nkeys))			\
	{																		\
		OSysCacheKey##nkeys key = {.common = {.datoid = datoid},			\
								   .keys = {OSC_REP_ARGS(nkeys)}};			\
		return o_sys_cache_delete(cache_name, (OSysCacheKey *) &key);		\
	}																		\
	void																	\
	o_##cache_name##_update_if_needed(Oid datoid, O_SYS_CACHE_ARGS(nkeys),	\
									  Pointer arg)							\
	{																		\
		OSysCacheKey##nkeys key = {.common = {.datoid = datoid},			\
								   .keys = {OSC_REP_ARGS(nkeys)}};			\
		o_sys_cache_update_if_needed(cache_name, (OSysCacheKey *) &key,		\
									 arg);									\
	}																		\
	void																	\
	o_##cache_name##_add_if_needed(Oid datoid, O_SYS_CACHE_ARGS(nkeys),		\
								   XLogRecPtr insert_lsn,					\
								   Pointer arg)								\
	{																		\
		OSysCacheKeyCommon	common = { 0 };									\
		OSysCacheKey##nkeys	key = {.keys = {OSC_REP_ARGS(nkeys)}};			\
		common.datoid = datoid;												\
		common.lsn = insert_lsn;											\
		key.common = common;												\
		o_sys_cache_add_if_needed(cache_name, (OSysCacheKey *) &key, arg);	\
	}																		\
	extern int no_such_variable

static inline void
o_sys_cache_set_datoid_lsn(XLogRecPtr *cur_lsn, Oid *datoid)
{
	if (cur_lsn)
		*cur_lsn = is_recovery_in_progress() ? GetXLogReplayRecPtr(NULL) :
			GetXLogWriteRecPtr();

	if (datoid)
	{
		if (OidIsValid(MyDatabaseId))
		{
			*datoid = MyDatabaseId;
		}
		else
		{
			Assert(OidIsValid(o_sys_cache_search_datoid));
			*datoid = o_sys_cache_search_datoid;
		}
	}
}

extern void o_composite_type_element_save(Oid datoid, Oid oid,
										  XLogRecPtr insert_lsn);
extern void o_set_syscache_hooks(void);
extern void o_unset_syscache_hooks(void);
extern void o_reset_syscache_hooks(void);
extern bool o_is_syscache_hooks_set(void);

/* o_enum_cache.c */

typedef struct OEnumData
{
	Oid			oid;
	float4		enumsortorder;
} OEnumData;

typedef struct
{
	OSysCacheKey2 key;
	OEnumData	data;
} OEnum;

typedef struct
{
	OSysCacheKey1 key;
	Oid			enumtypid;
} OEnumOid;

O_SYS_CACHE_DECLS(enum_cache, OEnum, 2);
O_SYS_CACHE_DECLS(enumoid_cache, OEnumOid, 1);
extern void o_enum_cache_add_all(Oid datoid, Oid enum_oid,
								 XLogRecPtr insert_lsn);
extern HeapTuple o_enum_cache_search_htup(TupleDesc tupdesc, Oid enumtypid,
										  Name enumlabel);
extern void o_enum_cache_tup_print(BTreeDescr *desc, StringInfo buf,
								   OTuple tup, Pointer arg);
extern void o_enum_cache_delete_all(Oid datoid, Oid enum_oid);

extern HeapTuple o_enumoid_cache_search_htup(TupleDesc tupdesc, Oid enum_oid);
extern void o_enumoid_cache_tup_print(BTreeDescr *desc, StringInfo buf,
									  OTuple tup, Pointer arg);

extern void o_load_enum_cache_data_hook(TypeCacheEntry *tcache);

/* o_range_cache.c */
typedef struct
{
	OSysCacheKey1 key;
	Oid			rngsubtype;
	Oid			rngcollation;
	Oid			rngsubopc;
} ORange;

O_SYS_CACHE_DECLS(range_cache, ORange, 1);
extern HeapTuple o_range_cache_search_htup(TupleDesc tupdesc, Oid rngtypid);
extern void o_range_cache_add_rngsubopc(Oid datoid, Oid rngtypid,
										XLogRecPtr insert_lsn);
extern void o_range_cache_tup_print(BTreeDescr *desc, StringInfo buf,
									OTuple tup, Pointer arg);

#if PG_VERSION_NUM >= 140000
typedef struct
{
	OSysCacheKey1 key;
	Oid			rngtypid;
} OMultiRange;

O_SYS_CACHE_DECLS(multirange_cache, OMultiRange, 1);
extern HeapTuple o_multirange_cache_search_htup(TupleDesc tupdesc,
												Oid rngmultitypid);
extern void o_multirange_cache_tup_print(BTreeDescr *desc, StringInfo buf,
										 OTuple tup, Pointer arg);
#endif

/* o_class_cache.c */
typedef struct OClass OClass;

typedef struct OClassArg
{
	bool		column_drop;
	bool		sys_table;
	int			dropped;
} OClassArg;

O_SYS_CACHE_DECLS(class_cache, OClass, 1);
extern TupleDesc o_class_cache_search_tupdesc(Oid cc_reloid);

/* o_opclass_cache.c */
typedef struct OOpclass
{
	OSysCacheKey1 key;
	Oid			opfamily;
	Oid			inputtype;

	/*
	 * We do not want to set FmgrInfo.fn_oid as random value.
	 */
	Oid			cmpOid;
	Oid			ssupOid;
} OOpclass;

O_SYS_CACHE_DECLS(opclass_cache, OOpclass, 1);
extern void o_opclass_cache_add_table(OTable *o_table);
extern OOpclass *o_opclass_get(Oid opclassoid);
extern HeapTuple o_opclass_cache_search_htup(TupleDesc tupdesc,
											 Oid opclassoid);
extern void o_opclass_cache_tup_print(BTreeDescr *desc, StringInfo buf,
									  OTuple tup, Pointer arg);

/* o_proc_cache.c */
typedef struct OProc OProc;

O_SYS_CACHE_DECLS(proc_cache, OProc, 1);
extern Datum o_fmgr_sql(PG_FUNCTION_ARGS);
extern void o_proc_cache_validate_add(Oid datoid, Oid procoid, Oid fncollation,
									  char *func_type, char *used_for);
extern void o_proc_cache_fill_finfo(FmgrInfo *finfo, Oid procoid);
extern HeapTuple o_proc_cache_search_htup(TupleDesc tupdesc, Oid procoid);

/* o_type_cache.c */
typedef struct OType
{
	OSysCacheKey1 key;
	NameData	typname;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typstorage;
	Oid			typcollation;
	Oid			typrelid;
	char		typtype;
	char		typcategory;
	bool		typispreferred;
	bool		typisdefined;
	regproc		typinput;
	regproc		typoutput;
	regproc		typreceive;
	regproc		typsend;
	Oid			typelem;
	char		typdelim;
	Oid			typbasetype;
	int32		typtypmod;
#if PG_VERSION_NUM >= 140000
	Oid			typsubscript;
#endif
	Oid			default_btree_opclass;
	Oid			default_hash_opclass;
} OType;

O_SYS_CACHE_DECLS(type_cache, OType, 1);
extern Oid	o_type_cache_get_typrelid(Oid typeoid);
extern HeapTuple o_type_cache_search_htup(TupleDesc tupdesc, Oid typeoid);
extern void o_type_cache_fill_info(Oid typeoid, int16 *typlen, bool *typbyval,
								   char *typalign, char *typstorage,
								   Oid *typcollation);
extern Oid	o_type_cache_default_opclass(Oid typeoid, Oid am_id);
extern void o_type_cache_tup_print(BTreeDescr *desc, StringInfo buf,
								   OTuple tup, Pointer arg);

/* o_aggregate_cache.c */
typedef struct OAggregate OAggregate;

O_SYS_CACHE_DECLS(aggregate_cache, OAggregate, 1);
extern HeapTuple o_aggregate_cache_search_htup(TupleDesc tupdesc, Oid aggfnoid);

/* o_operator_cache.c */
typedef struct OOperator
{
	OSysCacheKey1 key;
	regproc		oprcode;
} OOperator;

O_SYS_CACHE_DECLS(operator_cache, OOperator, 1);
extern HeapTuple o_operator_cache_search_htup(TupleDesc tupdesc, Oid operoid);
extern void o_operator_cache_tup_print(BTreeDescr *desc, StringInfo buf,
									   OTuple tup, Pointer arg);

/* o_amop_cache.c */
typedef struct OAmOp
{
	OSysCacheKey3 key;
	Oid			amopmethod;
	int16		amopstrategy;
	Oid			amopfamily;
	Oid			amoplefttype;
	Oid			amoprighttype;
} OAmOp;

typedef struct OAmOpStrat
{
	OSysCacheKey4 key;
	Oid			amopopr;
} OAmOpStrat;

O_SYS_CACHE_DECLS(amop_cache, OAmOp, 3);
extern HeapTuple o_amop_cache_search_htup(TupleDesc tupdesc, Oid amopopr,
										  char amoppurpose, Oid amopfamily);
extern List *o_amop_cache_search_htup_list(TupleDesc tupdesc, Oid amopopr);
extern void o_amop_cache_tup_print(BTreeDescr *desc, StringInfo buf,
								   OTuple tup, Pointer arg);

O_SYS_CACHE_DECLS(amop_strat_cache, OAmOpStrat, 4);
extern HeapTuple o_amop_strat_cache_search_htup(TupleDesc tupdesc,
												Oid amopfamily,
												Oid amoplefttype,
												Oid amoprighttype,
												int16 amopstrategy);
extern void o_amop_strat_cache_tup_print(BTreeDescr *desc, StringInfo buf,
										 OTuple tup, Pointer arg);

/* o_amproc_cache.c */
typedef struct OAmProc
{
	OSysCacheKey4 key;
	regproc		amproc;
} OAmProc;

O_SYS_CACHE_DECLS(amproc_cache, OAmProc, 4);
extern HeapTuple o_amproc_cache_search_htup(TupleDesc tupdesc,
											Oid amprocfamily,
											Oid amproclefttype,
											Oid amprocrighttype,
											int16 amprocnum);
extern void o_amproc_cache_tup_print(BTreeDescr *desc, StringInfo buf,
									 OTuple tup, Pointer arg);

/* o_collation_cache.c */
O_SYS_CACHE_DECLS(collation_cache, OCollation, 1);
extern HeapTuple o_collation_cache_search_htup(TupleDesc tupdesc, Oid colloid);
extern void orioledb_save_collation(Oid colloid);

/* o_database_cache.c */
typedef struct ODatabase
{
	OSysCacheKey1 key;
	int32		encoding;
} ODatabase;

O_SYS_CACHE_DECLS(database_cache, ODatabase, 1);
extern void o_database_cache_set_database_encoding(Oid dboid);
extern void o_database_cache_tup_print(BTreeDescr *desc, StringInfo buf,
									   OTuple tup, Pointer arg);

static inline void
o_set_sys_cache_search_datoid(Oid datoid)
{
	if (o_sys_cache_search_datoid != datoid)
	{
		o_sys_cache_search_datoid = datoid;
		if (is_recovery_process())
		{
			o_database_cache_set_database_encoding(datoid);
		}
	}
}

#endif							/* __O_SYS_CACHE_H__ */
