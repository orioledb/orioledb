/*-------------------------------------------------------------------------
 *
 * sys_trees.h
 *		Headers for system trees
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/sys_trees.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __SYS_TREES_H__
#define __SYS_TREES_H__

#include "btree/btree.h"
#include "btree/print.h"

#define SYS_TREES_DATOID				(1)

#define SYS_TREES_SHARED_ROOT_INFO		(1)
#define SYS_TREES_O_TABLES				(2)
#define SYS_TREES_O_INDICES				(3)
#define SYS_TREES_OPCLASSES				(4)
#define SYS_TREES_ENUM_CACHE			(5)
#define SYS_TREES_ENUMOID_CACHE			(6)
#define SYS_TREES_RANGE_CACHE			(7)
#define SYS_TREES_RECORD_CACHE			(8)
#define SYS_TREES_TYPE_ELEMENT_CACHE	(9)
#define SYS_TREES_EXTENTS_OFF_LEN		(10)
#define SYS_TREES_EXTENTS_LEN_OFF		(11)
#define SYS_TREES_NUM					(11)

#define IS_SYS_TREE_OIDS(oids) \
	((oids).datoid == SYS_TREES_DATOID)

#define O_OPCLASS_PROSRC_MAXLEN 512

typedef struct
{
	Oid			datoid;
	Oid			relnode;
} SharedRootInfoKey;

typedef struct
{
	SharedRootInfoKey key;
	BTreeRootInfo rootInfo;
} SharedRootInfo;

#define O_TABLE_INVALID_VERSION UINT32_MAX

typedef struct
{
	ORelOids	oids;
	uint32		offset;
	uint32		version;
} OTableChunkKey;

typedef struct
{
	OTableChunkKey key;
	uint32		dataLength;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} OTableChunk;

typedef struct
{
	OIndexType	type;
	ORelOids	oids;
	uint32		offset;
} OIndexChunkKey;

typedef struct
{
	OIndexChunkKey key;
	uint32		dataLength;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} OIndexChunk;

typedef struct
{
	Oid			datoid;
	Oid			opclassoid;
} OOpclassKey;

typedef struct
{
	char		prosrc[O_OPCLASS_PROSRC_MAXLEN];
	char		probin[O_OPCLASS_PROSRC_MAXLEN];
} OProcedure;

extern void o_type_procedure_fill(Oid procoid, OProcedure *proc);

extern void o_type_procedure_fill_finfo(FmgrInfo *finfo,
										OProcedure *cmp_proc,
										Oid cmp_oid,
										short fn_args);

typedef struct
{
	OOpclassKey key;
	bool		hasSsup;
	Oid			opfamily;
	Oid			inputtype;

	/*
	 * We do not want to set FmgrInfo.fn_oid as random value.
	 */
	Oid			cmpOid;
	Oid			ssupOid;
	OProcedure	cmpProc;
	OProcedure	ssupProc;
} OOpclass;

/* OTypeKey is key of entry stored in non-TOAST type cache tree */
typedef struct
{
	Oid			datoid;
	Oid			oid;
	XLogRecPtr	insert_lsn;
	bool		deleted;
} OTypeKey;

/* Key by which entry searched in non-TOAST type cache tree ignoring lsn */
typedef struct
{
	Oid			datoid;
	Oid			oid;
} OTypeKeyBound;

/* Key of chunks of entry stored in TOAST type cache tree */
typedef struct
{
	OTypeKey	type_key;
	uint32		offset;
} OTypeToastChunkKey;

/* Key by which entry searched in TOAST type cache tree */
typedef struct
{
	OTypeToastChunkKey chunk_key;
	bool		lsn_cmp;
} OTypeToastKeyBound;

/* Chunks of entry stored in TOAST type cache tree */
typedef struct
{
	OTypeToastChunkKey key;
	uint32		dataLength;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} OTypeToastChunk;

typedef struct
{
	OTypeKey	key;
	/* OID of owning enum type that cached in o_enum_cache */
	Oid			enumtypid;
} OEnumOid;

typedef struct
{
	OTypeKey	key;
	/* cached TypeCacheEntry->rngelemtype->typlen */
	int16		elem_typlen;
	/* cached TypeCacheEntry->rngelemtype->typbyval */
	bool		elem_typbyval;
	/* cached TypeCacheEntry->rngelemtype->typalign */
	char		elem_typalign;
	/* cached TypeCacheEntry->rng_collation */
	Oid			rng_collation;
	/* cached TypeCacheEntry->rng_cmp_proc_finfo prosrc, probin and oid */
	OProcedure	rng_cmp_proc;
	Oid			rng_cmp_oid;
} ORangeType;

typedef struct
{
	OTypeKey	key;
	/* cached TypeCacheEntry->typlen */
	int16		typlen;
	/* cached TypeCacheEntry->typbyval */
	bool		typbyval;
	/* cached TypeCacheEntry->typalign */
	char		typalign;
	/* cached TypeCacheEntry->cmp_proc_finfo prosrc, probin and oid */
	OProcedure	cmp_proc;
	Oid			cmp_oid;
} OTypeElement;

/*
 * FileExtent type stores length of an extent inside unsigned 16-bit value.
 * It enough for FileExtent purposes but extents inside free B-trees
 * can be more than 2^16.
 */
typedef struct
{
	uint64		offset;
	uint64		length;
} FreeTreeFileExtent;

/*
 * Tuple stored in a free B-tree nodes and tuples.
 */
typedef struct
{
	FreeTreeFileExtent extent;
	OIndexType	ixType;
	Oid			datoid;
	Oid			relnode;
} FreeTreeTuple;

extern Size sys_trees_shmem_needs(void);
extern void sys_trees_shmem_init(Pointer ptr, bool found);
extern BTreeDescr *get_sys_tree(int tree_num);
extern BTreeStorageType sys_tree_get_storage_type(int tree_num);
extern bool sys_tree_is_temporary(int tree_num);
extern bool sys_tree_supports_transactions(int tree_num);
extern PrintFunc sys_tree_key_print(BTreeDescr *desc);
extern PrintFunc sys_tree_tup_print(BTreeDescr *desc);

#endif							/* __SYS_TREES_H__ */
