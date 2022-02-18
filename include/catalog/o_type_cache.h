/*-------------------------------------------------------------------------
 *
 * o_type_cache.h
 *		Generic interface for type cache duplicate trees.
 *
 * Generic type cache tree interface that used to prevent syscache
 * usage during recovery. Type cache trees shoud use o_type_cache_*
 * functions in sysTreesMeta (sys_trees.c), but if type cache is
 * not TOAST tup_print function should be also provided.
 * Type cache lookups are also cached in local backend mamory.
 * Cache entry invalidation is performed by syscache hook.
 * Instead of physical deletion of type cache entry we mark it as deleted.
 * Normally only not deleted entries used. During recovery we use
 * type cache entries accroding to current WAL position.
 * Physical deletion of deleted values is performed during checkpoint,
 * which is also called after successed recovery.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/o_type_cache.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __O_TYPE_CACHE_H__
#define __O_TYPE_CACHE_H__

#include "orioledb.h"

#include "catalog/o_tables.h"
#include "catalog/sys_trees.h"

/*
 * Database oid to be used for type cache entries comparison.
 */
extern Oid	o_type_cmp_datoid;

typedef uint32 OTypeHashKey;	/* GetSysCacheHashValue result type */

typedef struct OTypeCache OTypeCache;
typedef struct OTypeHashTreeEntry
{
	OTypeCache *type_cache;		/* If NULL only link stored */
	Pointer		entry;
	OTypeHashKey link;			/* stored hash of another entry of fastcache */
} OTypeHashTreeEntry;
typedef struct OTypeHashEntry
{
	OTypeHashKey key;
	List	   *tree_entries;	/* list of OTypeHashTreeEntry */
} OTypeHashEntry;

typedef struct OTypeCacheFuncs
{
	/*
	 * Should be always set. Used in invalidation hook to cleanup entry saved
	 * in fastcache. Also used inside o_type_cache_add_if_needed.
	 */
	void		(*free_entry) (Pointer entry);

	/*
	 * if set it called in o_type_cache_delete before entry update with entry
	 * as argument
	 */
	void		(*delete_hook) (Pointer entry);

	/*
	 * Should be always set. Used inside o_type_cache_add_if_needed and
	 * o_type_cache_update_if_needed. On add entry_ptr is NULL, entry should
	 * be created and returned.
	 */
	void		(*fill_entry) (Pointer *entry_ptr, Oid datoid, Oid typoid,
							   XLogRecPtr insert_lsn, Pointer arg);

	/*
	 * if set, it called when a entry is created and must return oid of
	 * another object, invalidation of which shoud invalidate entry in
	 * fastcache
	 */
	Oid			(*fastcache_get_link_oid) (Pointer entry);

	/*
	 * Used in toast type cache trees. Should return pointer to binary
	 * serialized data and it's length.
	 */
	Pointer		(*toast_serialize_entry) (Pointer entry, int *len);

	/*
	 * Used in toast type cache trees. Should return pointer to constructed
	 * entry of a tree.
	 */
	Pointer		(*toast_deserialize_entry) (MemoryContext mcxt,
											Pointer data,
											Size length);
} OTypeCacheFuncs;

typedef struct OTypeCache
{
	int			sys_tree_num;
	bool		is_toast;
	Oid			classoid;
	MemoryContext mcxt;			/* context where stored entries from fast
								 * cache */
	HTAB	   *fast_cache;		/* contains OTypeHashEntry */
	OTypeHashKey last_fast_cache_key;
	Pointer		last_fast_cache_entry;
	OTypeCacheFuncs *funcs;
} OTypeCache;

/*
 * Initializes all type caches.
 */
extern void o_typecaches_init(void);

extern OTypeCache *o_create_type_cache(int sys_tree_num, bool is_toast,
									   Oid classoid,
									   HTAB *fast_cache, MemoryContext mcxt,
									   OTypeCacheFuncs *funcs);
extern Pointer o_type_cache_search(OTypeCache *type_cache, Oid datoid,
								   Oid oid, XLogRecPtr cur_lsn);
extern void o_type_cache_add_if_needed(OTypeCache *type_cache, Oid datoid,
									   Oid oid, XLogRecPtr insert_lsn,
									   Pointer arg);
extern void o_type_cache_update_if_needed(OTypeCache *type_cache,
										  Oid datoid, Oid oid, Pointer arg);
extern bool o_type_cache_delete(OTypeCache *type_cache, Oid datoid, Oid oid);

extern void o_type_cache_delete_by_lsn(OTypeCache *type_cache, XLogRecPtr lsn);

extern void custom_types_add_all(OTable *o_table);
extern void custom_type_add_if_needed(Oid datoid, Oid typoid,
									  XLogRecPtr insert_lsn);

extern void orioledb_syscache_type_hook(Datum arg, int cacheid,
										uint32 hashvalue);

#define O_TYPE_CACHE_INIT_FUNC(cache_name) \
void o_##cache_name##_init(MemoryContext mcxt, HTAB *fastcache)

#define O_TYPE_CACHE_DECLS(cache_name, elem_type) \
extern O_TYPE_CACHE_INIT_FUNC(cache_name); \
extern void o_##cache_name##_delete_by_lsn (XLogRecPtr lsn);		\
extern bool o_##cache_name##_delete(Oid datoid, Oid oid);			\
void o_##cache_name##_update_if_needed(Oid datoid, Oid oid,			\
									   Pointer arg);				\
void o_##cache_name##_add_if_needed(Oid datoid, Oid oid,			\
									XLogRecPtr insert_lsn,			\
									Pointer arg);					\
extern int no_such_variable

#define O_TYPE_CACHE_FUNCS(cache_name, elem_type)					\
	static inline elem_type *o_##cache_name##_search(				\
		Oid datoid,	Oid oid, Oid lsn);								\
	elem_type *														\
	o_##cache_name##_search(Oid datoid, Oid oid, Oid lsn)			\
	{																\
		return (elem_type *)o_type_cache_search(cache_name, datoid, \
												oid, lsn);			\
	}																\
	void															\
	o_##cache_name##_delete_by_lsn(XLogRecPtr lsn)					\
	{																\
		o_type_cache_delete_by_lsn(cache_name, lsn);				\
	}																\
	bool															\
	o_##cache_name##_delete(Oid datoid, Oid oid)					\
	{																\
		return o_type_cache_delete(cache_name, datoid, oid);		\
	}																\
	void															\
	o_##cache_name##_update_if_needed(Oid datoid, Oid oid,			\
									  Pointer arg)					\
	{																\
		o_type_cache_update_if_needed(cache_name, datoid, oid,		\
									  arg);							\
	}																\
	void															\
	o_##cache_name##_add_if_needed(Oid datoid, Oid oid,				\
								   XLogRecPtr insert_lsn,			\
								   Pointer arg)						\
	{																\
		o_type_cache_add_if_needed(cache_name, datoid, oid,			\
								   insert_lsn, arg);				\
	}																\
	extern int no_such_variable

/* o_enum_cache.c */
typedef struct OEnum OEnum;

O_TYPE_CACHE_DECLS(enum_cache, OEnum);
O_TYPE_CACHE_DECLS(enumoid_cache, OEnumOid);
extern TypeCacheEntry *o_enum_cmp_internal_hook(Oid type_id,
												MemoryContext mcxt);

/* o_range_cache.c */
O_TYPE_CACHE_DECLS(range_cache, ORangeType);
extern TypeCacheEntry *o_range_cmp_hook(FunctionCallInfo fcinfo,
										Oid rngtypid,
										MemoryContext mcxt);

/* o_record_cache.c */
typedef struct ORecord ORecord;

O_TYPE_CACHE_DECLS(record_cache, ORecord);
extern TupleDesc o_record_cmp_hook(Oid type_id, MemoryContext mcxt);

/* o_type_element_cache.c */
O_TYPE_CACHE_DECLS(type_element_cache, OTypeElement);
extern TypeCacheEntry *o_type_elements_cmp_hook(Oid elemtype,
												MemoryContext mcxt);

#endif							/* __O_TYPE_CACHE_H__ */
