/*-------------------------------------------------------------------------
 *
 * o_indices.h
 * 		Declarations for orioledb indices system tree.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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

/*
 * Lifecycle state of an index.  Non-VALID states are used by
 * CREATE INDEX CONCURRENTLY to expose the index to writers (capture
 * phase) before it becomes available to readers.  Persisted as one
 * trailing byte in serialize_o_index(); records written by versions
 * predating this field are read as OINDEX_STATE_VALID.
 */
typedef enum
{
	OINDEX_STATE_VALID = 0,		/* fully built, available to all readers */
	OINDEX_STATE_BUILDING_PHASE_1,	/* meta installed, capture started, no DML
									 * writes to index */
	OINDEX_STATE_BUILDING_PHASE_2,	/* phase-1 + snapshot scan/build in
									 * progress */
	OINDEX_STATE_BUILDING_PHASE_3,	/* build done, catchup phase: writes go to
									 * index + spool on collisions */
	OINDEX_STATE_BUILDING_PHASE_4	/* final spool drain under WaitForLockers */
} OIndexState;

typedef struct
{
	ORelOids	indexOids;
	OIndexType	indexType;
	uint32		indexVersion;
	ORelOids	tableOids;
	char		table_persistence;
	uint8		fillfactor;
	uint16		data_version;
	Oid			tablespace;
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
	Oid		   *exclops;
	bool		immediate;
	MemoryContext index_mctx;

	/*
	 * Lifecycle state for CREATE INDEX CONCURRENTLY.  Stored at the trailing
	 * end of the serialized record; old records (without the trailing byte)
	 * deserialize to OINDEX_STATE_VALID.
	 */
	OIndexState state;
} OIndex;

/* callback for o_indices_foreach_oids() */
typedef void (*OIndexOidsCallback) (OIndexType type, ORelOids treeOids,
									ORelOids tableOids, Oid tablespace, void *arg);

typedef enum
{
	OIndexVersionReset,
	OIndexVersionPass,
} OIndexVersionMode;

extern OIndex *make_o_index(OTable *table, OIndexNumber ixNum, OIndexVersionMode ixVerMode);

typedef enum
{
	oTableSourceTable = 0,
	oTableSourceContext = 1
} OTableSource;

extern void o_index_fill_descr(OIndexDescr *descr, OIndex *oIndex, void *o_table_source, OTableSource source);

extern void free_o_index(OIndex *o_index);
extern bool o_indices_add(OTable *table, OIndexNumber ixNum, OXid oxid,
						  CommitSeqNo csn);
extern bool o_indices_del(OTable *table, OIndexNumber ixNum, OXid oxid,
						  CommitSeqNo csn);
extern OIndex *o_indices_get(ORelOids oids, OIndexType type);
extern OIndex *o_indices_get_extended(ORelOids oids, OIndexType type, OTableFetchContext ctx);

extern bool o_indices_update(OTable *table, OIndexNumber ixNum,
							 OXid oxid, CommitSeqNo csn);
extern bool o_indices_set_state(ORelOids indexOids, OIndexType type,
								char table_persistence,
								OIndexState newState,
								OXid oxid, CommitSeqNo csn);
extern bool o_indices_find_table_oids(ORelOids indexOids, OIndexType type,
									  OSnapshot *oSnapshot,
									  ORelOids *tableOids);
extern void o_indices_foreach_oids(OIndexOidsCallback callback, void *arg);

#endif
