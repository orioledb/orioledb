/*-------------------------------------------------------------------------
 *
 * o_indices.h
 * 		Declarations for orioledb indices system tree.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/o_indices.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __O_INDICES_H__
#define __O_INDICES_H__

#include "orioledb.h"

#include "catalog/o_tables.h"
#include "tuple/format.h"

typedef struct
{
	ORelOids	indexOids;
	OIndexType	indexType;
	uint32		indexVersion;
	ORelOids	tableOids;
	char		table_persistence;
	uint8		fillfactor;
	uint16		data_version;
	OXid		createOxid;
	NameData	name;
	bool		primaryIsCtid;
	bool		bridging;
	OCompress	compress;
	bool		nulls_not_distinct;
	/* number of fields added using INCLUDE command explicitly */
	/* pkey fields added implicitly in o_o_define_index_validate not counted */
	uint16		nIncludedFields;
	uint16		nLeafFields;
	uint16		nNonLeafFields;

	/*
	 * TOAST index: pkey field amount, excluding included fields, including 2
	 * fields: attnum and chunknum Primary index: amount of uniq fields in
	 * index Unique index: field amount, excluding included and pkey fields
	 * Regular index: all field amount
	 */
	uint16		nUniqueFields;
	/* non-TOAST index: field amount, excluding included and pkey fields */
	/* TOAST index: pkey field amount, excluding included fields */
	uint16		nKeyFields;
	/* size of primaryFieldsAttnums */
	uint16		nPrimaryFields;
	/* where primary key fields located in index tuple */
	AttrNumber	primaryFieldsAttnums[INDEX_MAX_KEYS];

	/*
	 * Fields above are stored in SYS_TREES_O_INDICES and
	 * serialized/deserialized by serialize_o_index()/deserialize_o_index().
	 * Fields below are also stored in SYS_TREES_O_INDICES, but they are
	 * palloc'ed by deserialize_o_index().
	 *
	 * Be careful while adding new fields in order to not break binary
	 * backward compatibility of the database.
	 */

	OTableField *leafTableFields;
	OTableIndexField *leafFields;
	List	   *predicate;		/* list of Expr */
	char	   *predicate_str;
	List	   *expressions;	/* list of Expr */

	/*
	 * duplicated non-pkey fields, elements: lists of 2 elements: (fieldnum,
	 * original fieldnum) primary index cannot have duplicate fields in
	 * postgres
	 */
	List	   *duplicates;
	Oid			tablespace;
	Oid		   *exclops;
	bool		immediate;
	MemoryContext index_mctx;
} OIndex;

/*
 * OTableProviderFn
 *
 * Callback used by index descriptor initialization code to obtain a base table
 * metadata object (OTable) for a given table OIDs.
 *
 * The provider returns either:
 * - a borrowed pointer to an existing OTable object (must_free=false), or
 * - a newly loaded OTable object (must_free=true), in which case the caller
 *   becomes responsible for freeing it with o_table_free().
 *
 * The provider must return NULL if the table definition cannot be obtained
 * under the requested catalog state.
 *
 * Arguments:
 * - tableOids: OIDs identifying the base table for which OTable is requested.
 * - arg: opaque provider-specific state.
 * - must_free: OUT parameter, set to true if the returned OTable must be freed
 *   by the caller; must be set to false on NULL return.
 */
typedef OTable *(*OTableProviderFn) (ORelOids tableOids, void *arg, bool *must_free);

/*
 * OIndexDescrFillSource
 *
 * Pluggable source of base table metadata needed to fully initialize an
 * OIndexDescr.
 *
 * Some index descriptor fields (e.g. primary index constraints and
 * primary_init_nfields) depend on the base table definition (OTable) and thus
 * cannot be derived from OIndex alone.
 *
 * OIndexDescrFillSource provides a uniform way to supply this dependency:
 * either by passing an already available OTable, or by loading OTable on demand
 * using a catalog fetch context.
 *
 * The get_oTable callback must follow OTableProviderFn ownership rules.
 *
 * Lifetime requirements:
 * - The OIndexDescrFillSource object and its arg must remain valid for the
 *   duration of o_index_fill_descr() call.
 */
typedef struct
{
	OTableProviderFn get_oTable;
	void	   *arg;
} OIndexDescrFillSource;

/*
 * provider_external()
 *
 * OTable provider that returns a borrowed OTable pointer passed via arg.
 * The returned object is not owned by the caller of the provider.
 *
 * Invariants:
 * - arg must be a non-NULL pointer to a valid OTable.
 * - *must_free is set to false.
 */
extern OTable *provider_external(ORelOids tableOids, void *arg, bool *must_free);

/*
 * provider_loaded()
 *
 * OTable provider that loads OTable from OrioleDB system catalogs using the
 * fetch context passed via arg (ORelFetchContext pointer).
 *
 * The returned OTable is owned by the caller of the provider and must be freed
 * with o_table_free() if must_free is set to true.
 *
 * Invariants:
 * - arg must point to a valid ORelFetchContext for base table fetch.
 * - On success, *must_free is set to true.
 * - On failure (NULL return), *must_free is set to false.
 */
extern OTable *provider_loaded(ORelOids tableOids, void *arg, bool *must_free);

/*
 * fill_idescr_from_table()
 *
 * Constructs a fill source that uses an already available OTable object.
 *
 * The passed OTable is treated as borrowed; it must remain valid for the whole
 * duration of o_index_fill_descr() call. The fill code will never free it.
 */
extern OIndexDescrFillSource fill_idescr_from_table(OTable *oTable);

/*
 * fill_idescr_from_ctx()
 *
 * Constructs a fill source that loads OTable on demand using the provided base
 * relation fetch context.
 *
 * The fetch context must describe the intended catalog view of the base table
 * (e.g. correct snapshot/version pair during WAL replay or logical decoding).
 *
 * NOTE: If this function stores only a pointer to base_ctx in the fill source,
 * callers must ensure that base_ctx outlives o_index_fill_descr() call.
 */
extern OIndexDescrFillSource fill_idescr_from_ctx(ORelFetchContext *base_ctx_ptr);

/* callback for o_indices_foreach_oids() */
typedef void (*OIndexOidsCallback) (OIndexType type, ORelOids treeOids,
									ORelOids tableOids, void *arg);

extern OIndex *make_o_index(OTable *table, OIndexNumber ixNum);

/*
 * o_index_fill_descr()
 *
 * Initializes an OIndexDescr from a catalog OIndex object.
 *
 * This function:
 * - resets all fields of *descr,
 * - copies identity information (OIDs, name, version, flags),
 * - builds leaf and non-leaf tuple descriptors according to index type,
 * - populates index field metadata (collations, ordering, null ordering,
 *   opclasses, comparators),
 * - initializes predicate/expressions state and per-descriptor execution
 *   context,
 * - fills fixed-format specs and derived fields (fillfactor, maxTableAttnum,
 *   etc.).
 *
 * Some index types require access to the base table definition:
 * - For oIndexPrimary, constraints and primary_init_nfields are loaded from
 *   OTable. The OTable is obtained via fill_source.get_oTable().
 * - For other index types the fill source is currently unused.
 *
 * Ownership and memory:
 * - The function allocates descriptor-owned memory in the index memory context
 *   returned by OGetIndexContext(descr).
 * - If the OTable provider returns must_free=true, this function will free the
 *   OTable before returning.
 *
 * Caller responsibilities / invariants:
 * - oIndex must be non-NULL and describe a valid catalog index entry.
 * - descr must be writable memory.
 * - For oIndexPrimary, fill_source.get_oTable must be non-NULL and must return
 *   an OTable consistent with descr->tableOids and the intended catalog state.
 * - The fill_source and its arg must remain valid for the duration of the call.
 */
extern void o_index_fill_descr(OIndexDescr *descr, OIndex *oIndex, OIndexDescrFillSource fill_source);

extern void free_o_index(OIndex *o_index);
extern bool o_indices_add(OTable *table, OIndexNumber ixNum, OXid oxid,
						  CommitSeqNo csn);
extern bool o_indices_del(OTable *table, OIndexNumber ixNum, OXid oxid,
						  CommitSeqNo csn);
extern OIndex *o_indices_get(ORelOids oids, OIndexType type);
extern OIndex *o_indices_get_extended(ORelOids oids, OIndexType type, ORelFetchContext ctx);

extern bool o_indices_update(OTable *table, OIndexNumber ixNum,
							 OXid oxid, CommitSeqNo csn);
extern bool o_indices_find_table_oids(ORelOids indexOids, OIndexType type,
									  OSnapshot *oSnapshot,
									  ORelOids *tableOids);
extern void o_indices_foreach_oids(OIndexOidsCallback callback, void *arg);

#endif
