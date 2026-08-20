/*-------------------------------------------------------------------------
 *
 * descr.h
 *		Declarations of descriptors used for table access method definition.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/descr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_DESCR_H__
#define __TABLEAM_DESCR_H__

#include "checkpoint/checkpoint.h"
#include "catalog/o_indices.h"
#include "catalog/sys_trees.h"
#include "utils/seq_buf.h"
#include "s3/queue.h"
#include "tableam/handler.h"
#include "transam/undo.h"
#include "tuple/format.h"

#include "access/htup_details.h"
#include "utils/resowner.h"
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

typedef struct OExclusionFn
{
	Oid			operator;

	FmgrInfo	finfo;
} OExclusionFn;

typedef struct OHashFnKey
{
	Oid			datoid;
	Oid			hash_fn_oid;
} OHashFnKey;

typedef struct OHashFn
{
	OHashFnKey	key;
	FmgrInfo	finfo;
} OHashFn;

#define O_DEFAULT_HASH_FN_OID 1 /* Using 1 here to distinct from uninitialized
								 * fields */
extern OHashFn o_default_hash_fn;

/*
 * Datum comparisons that can bypass the cached comparator.
 *
 * o_call_comparator() has to guard against catalog invalidations and set up a
 * SortSupportData before every call, which dwarfs the comparison itself for
 * plain fixed-width types.  When a field's type and operator class are one of
 * the builtins below, and no cross-type comparison is involved, the datums
 * can be compared inline instead.  Only types whose btree ordering is the
 * plain arithmetic one are listed: float and numeric orderings have NaN /
 * display-scale rules that the comparator implements.
 */
typedef enum
{
	OIndexFieldFastCmpNone = 0,
	OIndexFieldFastCmpInt2,
	OIndexFieldFastCmpInt4,
	OIndexFieldFastCmpInt8,
	OIndexFieldFastCmpOid
} OIndexFieldFastCmp;

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
	OExclusionFn *exclusion_fn;
	OHashFn    *hash_fn;

	/* Inline comparison this field's datums qualify for, if any. */
	OIndexFieldFastCmp fastCmp;
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
	uint32		version;

	/* reference count */
	int			refcnt;
	bool		valid;

	/*
	 * Set while get_index_descr() is (re)filling this cache entry.  Between
	 * HASH_ENTER and o_index_fill_descr() the entry is not yet initialized (a
	 * fresh dynahash slot still holds a previously-freed descriptor's stale
	 * pointers), and o_indices_get_extended() can run
	 * AcceptInvalidationMessages -> o_invalidate_descrs() reentrantly.
	 * o_invalidate_descrs_internal() skips entries with this flag so it never
	 * frees a half-built descriptor (which would FreeTupleDesc() a stale
	 * leafTupdesc and corrupt/double-free the shared descrCxt chunk). Cleared
	 * by reset_filling_descrs() on error.
	 */
	bool		fill_in_progress;

	/*
	 * CIC lifecycle.  Mirrored from OIndex.state at descriptor fill time.
	 * builderOxid is the oxid of the CIC build session (== OIndex.createOxid
	 * when state != OINDEX_STATE_VALID); used to locate the spool dir on the
	 * DML hot path.  For VALID indexes both fields are unused.
	 */
	OIndexState state;
	OXid		builderOxid;

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
	bool		immediate;
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
 * TODO: Remove usage of this function or document its purpose
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
		/*
		 * A page hikey is stored in the non-leaf tuple format, so it maps the
		 * same way as BTreeKeyNonLeafKey (e.g. the fastpath downlink search
		 * decomposes a BTreeKeyPageHiKey when an iterator steps to a
		 * sibling).
		 */
		Assert((keyType == BTreeKeyLeafTuple && attnum <= idx->leafTupdesc->natts) ||
			   ((keyType == BTreeKeyNonLeafKey || keyType == BTreeKeyPageHiKey) &&
				attnum <= idx->nFields));
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
	uint32		version;

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
	bool		noInvalidation;
	/* see OIndexDescr.fill_in_progress -- same guard for the table entry */
	bool		fill_in_progress;
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
	bool		truncateValues;
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

extern OTableFetchContext default_table_fetch_context;

/*
 * Please, read commit before o_bree_load_shmemd() definition.
 */

extern OTableDescr *o_fetch_table_descr_extended(ORelOids oids, OTableFetchContext ctx);

extern OIndexDescr *o_fetch_index_descr_extended(ORelOids oids, OIndexType type,
												 bool lock, OTableFetchContext ctx, OTableFetchContext base_ctx);

extern OTableDescr *o_fetch_table_descr(ORelOids oids);
extern OIndexDescr *o_fetch_index_descr(ORelOids oids, OIndexType type,
										bool lock, bool *nested);
extern OIndexDescr *get_cached_index_descr(ORelOids oids);

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
extern void o_insert_shared_root_placeholder(Oid datoid, Oid relnode, Oid tablespace);
extern void o_move_tree_meta(Oid datoid, Oid relnode, Oid old_tablespace,
							 Oid new_tablespace);

extern OComparator *o_find_comparator(Oid opfamily,
									  Oid lefttype,
									  Oid righttype,
									  Oid collation);
extern int	o_call_comparator(OComparator *comparator, Datum left,
							  Datum right);
extern int	o_call_exclusion_fn(OExclusionFn *exclusion_fn, Datum left, Datum right, Oid collation);
extern uint32 o_call_hash_fn(OHashFn *hash_fn, Oid collation, Datum val);

extern EvictedTreeData *read_evicted_data(Oid datoid, Oid relnode, Oid tablespace, bool delete);
extern void insert_evicted_data(EvictedTreeData *data);

extern void oFillFieldOpClassAndComparator(OIndexField *field, Oid datoid,
										   Oid opclassoid, Oid exacttype,
										   Oid exclusion_op, Oid hash_fn_oid);
extern void o_finish_sort_support_function(OComparator *comparator, SortSupport ssup);

extern void o_add_invalidate_undo_item(ORelOids oids, uint32 flags);
extern void o_invalidate_undo_item_callback(UndoLogType undoType,
											UndoLocation location,
											UndoStackItem *baseItem,
											OXid oxid, OUndoCallbackStage stage,
											bool changeCountsValid);

extern void o_add_invalidate_comparator_undo_item(Oid opfamily, Oid lefttype, Oid righttype);
extern void o_invalidate_comparator_callback(UndoLogType undoType, UndoLocation location,
											 UndoStackItem *baseItem,
											 OXid oxid, OUndoCallbackStage stage,
											 bool changeCountsValid);
extern void reset_saving_inval_messages(void);
extern void reset_filling_descrs(void);

extern void ResourceOwnerRememberOTableDescr(ResourceOwner owner, OTableDescr *descr);
extern void ResourceOwnerForgetOTableDescr(ResourceOwner owner, OTableDescr *descr);
extern void ResourceOwnerRememberOIndexDescr(ResourceOwner owner, OIndexDescr *descr);
extern void ResourceOwnerForgetOIndexDescr(ResourceOwner owner, OIndexDescr *descr);

#endif
