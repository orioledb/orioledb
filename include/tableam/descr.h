/*-------------------------------------------------------------------------
 *
 * descr.h
 *		Declarations of descriptors used for table access method definiton.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/descr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_DESCR_H__
#define __TABLEAM_DESCR_H__

#include "checkpoint/checkpoint.h"
#include "catalog/sys_trees.h"
#include "utils/seq_buf.h"
#include "s3/queue.h"
#include "tableam/handler.h"
#include "transam/undo.h"
#include "tuple/format.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/skey.h"
#include "commands/explain.h"
#include "executor/tuptable.h"
#include "nodes/pathnodes.h"

/* tableam/descr.c */

typedef struct
{
	SharedRootInfoKey key;

	CheckpointFileHeader file_header;
	S3TaskLocation maxLocation[2];
	EvictedSeqBufData freeBuf;

	/*
	 * we can hold just offsets here, but SeqBufTag info can be useful on
	 * debug
	 */
	EvictedSeqBufData nextChkp;
	EvictedSeqBufData tmpBuf;

	bool		dirtyFlag1;
	bool		dirtyFlag2;

	uint32		punchHolesChkpNum;
} EvictedTreeData;


typedef struct OComparator OComparator;
typedef struct OComparatorKey OComparatorKey;

/*
 * The index field descriptor
 */
typedef struct
{
	Oid			inputtype;
	Oid			opfamily;
	Oid			opclass;
	Oid			collation;
	bool		ascending;
	bool		nullfirst;

	/*
	 * A cached comparator to compare inputtype values according to opfamily
	 * and opclass.
	 */
	OComparator *comparator;
} OIndexField;

typedef struct AttrNumberMap
{
	AttrNumber	key;
	AttrNumber	value;
} AttrNumberMap;

/*
 * The index descriptor
 */
struct OIndexDescr
{
	ORelOids	oids;
	ORelOids	tableOids;

	/* reference count */
	int			refcnt;
	bool		valid;

	BTreeDescr	desc;

	/* Name of the index */
	NameData	name;

	MemoryContext index_mctx;
	List	   *expressions;	/* list of Expr */
	List	   *predicate;		/* list of Expr */
	char	   *predicate_str;

	List	   *expressions_state;	/* list of ExprState */
	ExprState  *predicate_state;
	ExprContext *econtext;

	/* Tuple descriptor and format specifier for non-leaf tuples */
	TupleDesc	nonLeafTupdesc;
	OTupleFixedFormatSpec nonLeafSpec;

	/* Tuple descriptor and format specifier for leaf tuples */
	TupleDesc	leafTupdesc;
	OTupleFixedFormatSpec leafSpec;

	/*
	 * Flag to indicate unique index and number of unique fields for unique
	 * index.
	 */
	bool		unique;
	bool		nulls_not_distinct;
	int			nUniqueFields;

	/*
	 * Flag indicates that primary key index on the table is surrogate index
	 * on ctid (no primary key is explicitly defined).
	 */
	bool		primaryIsCtid;

	/*
	 * Indicates that bridging enabled for table: i.e. there is bridge_ctid
	 * column in pkey and also bridge index
	 */
	bool		bridging;

	uint8		fillfactor;

	/* Description of the index fields */
	int			nFields;
	int			nKeyFields;
	int			nIncludedFields;
	OIndexField *fields;

	/*
	 * Attnums for primary key values in the secondary index tuples. We may
	 * assume that secondary index tuple just contain primary key values in
	 * the tail.  But we would like to save the space if secondary index
	 * shares some attributes with primary key.
	 */
	int			nPrimaryFields;
	AttrNumber	primaryFieldsAttnums[INDEX_MAX_KEYS];

	/* Compression rate used in this index */
	OCompress	compress;

	/*
	 * Attribute numbers of fields in table tupdesc.  Counts from 1.  Ctid
	 * counts as the first column if used.
	 */
	AttrNumber *tableAttnums;
	/* The maximal value in tableAttnums */
	int			maxTableAttnum;

	/* Used in getsomeattrs to reorder fields during index only scan of pkey  */
	AttrNumberMap *pk_tbl_field_map;

	/* Cached comparators used to fill key for pkey tuple search */
	OComparator **pk_comparators;

	/* tupdesc and slots needed for indexam operations */
	TupleDesc	itupdesc;
	TupleTableSlot *index_slot;
	TupleTableSlot *old_leaf_slot;
	TupleTableSlot *new_leaf_slot;
	/* Copy of duplicates from OIndex */
	List	   *duplicates;
};

/*
 * ItemPointerGetOffsetNumber
 *		As above, but verifies that the item pointer looks valid.
 */
static inline OffsetNumber
OIndexKeyAttnumToTupleAttnum(BTreeKeyType keyType, OIndexDescr *idx, int attnum)
{
	if (keyType == BTreeKeyLeafTuple && idx->desc.type == oIndexPrimary)
	{
		Assert((attnum - 1) < idx->leafTupdesc->natts);
		return idx->tableAttnums[attnum - 1] + (idx->bridging && !idx->primaryIsCtid ? 1 : 0);
	}
	else
	{
		Assert((keyType == BTreeKeyLeafTuple && attnum <= idx->leafTupdesc->natts) ||
			   (keyType == BTreeKeyNonLeafKey && attnum <= idx->nFields));
		return attnum;
	}
}

#define OGetIndexContext(index) \
	((index)->index_mctx ? \
	 (index)->index_mctx : \
		((index)->index_mctx = AllocSetContextCreate(TopMemoryContext, \
													 "OIndexContext", \
													 ALLOCSET_DEFAULT_SIZES)))

#define OIgnoreColumn(descr, attnum) \
	((descr->desc.type != oIndexToast && descr->desc.type != oIndexBridge) && \
		(attnum >= descr->nKeyFields) && \
	 (attnum < (descr->nKeyFields + descr->nIncludedFields)))

struct OTableDescr
{
	ORelOids	oids;

	/* reference count */
	int			refcnt;

	/* Source table tupdesc (without ctid, etc) */
	TupleDesc	tupdesc;

	/* Slots for handling the modifications */
	TupleTableSlot *oldTuple;
	TupleTableSlot *newTuple;

	/*
	 * Description of table indices and toast.  indices[0] always points to
	 * the primary key, reset of indeces array point to the secondary indices.
	 */
	OIndexDescr **indices;
	OIndexDescr *bridge;
	OIndexDescr *toast;

	/* list of TOASTable values */
	AttrNumber *toastable;
	/* number of toastable fields */
	int			ntoastable;
	/* number of trees */
	int			nIndices;
	/* number of unique trees */
	int			nUniqueIndices;
};

typedef struct
{
	Datum	   *values;
	bool	   *nulls;
	TupleDesc	desc;
	OTupleFixedFormatSpec *spec;
	FmgrInfo   *outputFns;
	TupleDesc	keyDesc;
	OTupleFixedFormatSpec *keySpec;
	FmgrInfo   *keyOutputFns;
	bool		printRowVersion;
} TuplePrintOpaque;

#define O_INVALIDATE_OIDS_ON_COMMIT	1
#define O_INVALIDATE_OIDS_ON_ABORT	2

typedef struct
{
	OnCommitUndoStackItem header;
	ORelOids	oids;
	uint32		flags;
} InvalidateUndoStackItem;

#define GET_PRIMARY(descr) ((descr)->indices[PrimaryIndexNumber])

/*
 * Please, read commit before o_bree_load_shmemd() definition.
 */
extern OTableDescr *o_fetch_table_descr(ORelOids oids);
extern OIndexDescr *o_fetch_index_descr(ORelOids oids, OIndexType type,
										bool lock, bool *nested);

extern void recreate_table_descr_by_oids(ORelOids oids);
extern void o_fill_tmp_table_descr(OTableDescr *descr, OTable *o_table);
extern void o_free_tmp_table_descr(OTableDescr *descr);

static inline bool
is_explain_analyze(PlanState *ps)
{
	return ps->state->es_instrument & INSTRUMENT_BUFFERS;
}

extern void o_btree_load_shmem(BTreeDescr *desc);
extern bool o_btree_load_shmem_checkpoint(BTreeDescr *desc);
extern bool o_btree_try_use_shmem(BTreeDescr *desc);

extern SharedRootInfo *o_find_shared_root_info(SharedRootInfoKey *key);
extern void o_insert_shared_root_placeholder(Oid datoid, Oid relnode);

extern OComparator *o_find_comparator(Oid opfamily,
									  Oid lefttype,
									  Oid righttype,
									  Oid collation);
extern int	o_call_comparator(OComparator *comparator, Datum left,
							  Datum right);
extern void o_invalidate_comparator_cache(Oid opfamily, Oid lefttype,
										  Oid righttype);

extern EvictedTreeData *read_evicted_data(Oid datoid, Oid relnode, bool delete);
extern void insert_evicted_data(EvictedTreeData *data);

extern void oFillFieldOpClassAndComparator(OIndexField *field, Oid datoid, Oid opclassoid);
extern void o_finish_sort_support_function(OComparator *comparator, SortSupport ssup);

extern void o_add_invalidate_undo_item(ORelOids oids, uint32 flags);
extern void o_invalidate_undo_item_callback(UndoLogType undoType,
											UndoLocation location,
											UndoStackItem *baseItem,
											OXid oxid, bool abort,
											bool changeCountsValid);

extern void o_add_invalidate_comparator_undo_item(Oid opfamily, Oid lefttype, Oid righttype);
extern void o_invalidate_comparator_callback(UndoLogType undoType, UndoLocation location,
											 UndoStackItem *baseItem,
											 OXid oxid, bool abort, bool changeCountsValid);

#endif
