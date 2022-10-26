/*-------------------------------------------------------------------------
 *
 * toast.h
 * 		Low-level declarations for orioledb TOAST implementation
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tuple/toast.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TOAST_H__
#define __TOAST_H__

#include "access/htup.h"
#include "access/detoast.h"
#if PG_VERSION_NUM >= 140000
#include "access/toast_compression.h"
#endif

#include "orioledb.h"

#include "btree/iterator.h"
#include "tableam/descr.h"

/* position after primary ix tuple */
#define ATTN_POS (1)
#define OFFSET_POS (2)
#define DATA_POS (3)

#define TOAST_LEAF_FIELDS_NUM (3)
#define TOAST_NON_LEAF_FIELDS_NUM (2)

typedef struct Tuplesortstate Tuplesortstate;

/*
 * Low-level orioledb TOAST interface.
 */

/* Key bound for TOAST BTree */
typedef struct OToastKey
{
	/* primary index tuple */
	OTuple		pk_tuple;
	/* current offset */
	uint32		offset;
	/* attribute number of toasted value in table */
	uint16		attnum;
} OToastKey;

/*
 * Stores into orioledb table tuples in primary index instead TOASTed values.
 */
typedef struct OToastValue
{
	/* always TOAST pointer (0x80 for big-endian or 0x01 for little-endian) */
	uint8		pointer;
#if PG_VERSION_NUM >= 140000
	/* compression method of TOASTed data */
	uint8		compression;
#endif
	/* raw size of TOASTed data without headers */
	int32		raw_size;
	/* size of TOASTed data */
	int32		toasted_size;
} OToastValue;

/*
 * API, which encapsulates TOAST key and tuple format.
 */
typedef struct
{
	BTreeDescr *(*getBTreeDesc) (void *arg);
	uint32		(*getKeySize) (void *arg);
	uint32		(*getMaxChunkSize) (void *key, void *arg);
	void		(*updateKey) (void *key, uint32 offset, void *arg);
	void	   *(*getNextKey) (void *key, void *arg);
	OTuple		(*createTuple) (void *key, Pointer data, uint32 offset,
								int length, void *arg);
	OTuple		(*createKey) (void *key, uint32 offset, void *arg);
	Pointer		(*getTupleData) (OTuple tuple, void *arg);
	uint32		(*getTupleOffset) (OTuple tuple, void *arg);
	uint32		(*getTupleDataSize) (OTuple tuple, void *arg);
	bool		deleteLogFullTuple;
	TupleFetchCallback versionCallback;
} ToastAPI;

extern ToastAPI tableToastAPI;

/*
 * Generic function for working with TOAST, which can work with different
 * API implementations.
 */
extern bool generic_toast_insert(ToastAPI *api, void *key, Pointer data,
								 Size data_size, OXid oxid, CommitSeqNo csn,
								 void *arg);
extern void generic_toast_sort_add(ToastAPI *api, void *key, Pointer data,
								   Size data_size, Tuplesortstate *sortstate,
								   void *arg);
extern bool generic_toast_update(ToastAPI *api, void *key, Pointer data,
								 Size data_size, OXid oxid, CommitSeqNo csn,
								 void *arg);
extern bool generic_toast_delete(ToastAPI *api, void *key, OXid oxid,
								 CommitSeqNo csn, void *arg);

/* Returns tuple only if its size equals data_size, or NULL otherwise */
extern Pointer generic_toast_get(ToastAPI *api, void *key, Size data_size,
								 CommitSeqNo csn, void *arg);

/* Returns tuple and size of data if found, or NULL otherwise */
extern Pointer generic_toast_get_any(ToastAPI *api, void *key,
									 Size *data_size, CommitSeqNo csn,
									 void *arg);

/*
 * Same as generic_toast_get_any but:
 * - if found_key not NULL, on success it will contain found key
 * - if found_key contains valid pointer it used as version callback arg
 */
extern Pointer generic_toast_get_any_with_key(ToastAPI *api, void *key,
											  Size *data_size, CommitSeqNo csn,
											  void *arg, Pointer *found_key);

/*
 * Same as generic_toast_get_any but uses fetch_callback to filter tuples
 */
extern Pointer generic_toast_get_any_with_callback(ToastAPI *api, Pointer key,
												   Size *data_size, CommitSeqNo csn, void *arg,
												   TupleFetchCallback fetch_callback, void *callback_arg);

/* Copies TupleDescs to toast definition */
extern void o_toast_init_tupdescs(OIndexDescr *toast, TupleDesc ix_primary);

/*
 * Functions dealing with tableam TOAST trees.
 */
extern bool o_toast_insert(OIndexDescr *primary, OIndexDescr *toast,
						   OTuple pk, uint16 attn,
						   Pointer data, Size data_size,
						   OXid oxid, CommitSeqNo csn);
extern void o_toast_sort_add(OIndexDescr *primary, OIndexDescr *toast,
							 OTuple pk, uint16 attn,
							 Pointer data, Size data_size,
							 Tuplesortstate *sortstate);
extern bool o_toast_delete(OIndexDescr *primary, OIndexDescr *toast,
						   OTuple pk, uint16 attn,
						   OXid oxid, CommitSeqNo csn);
extern Pointer o_toast_get(OIndexDescr *primary, OIndexDescr *toast,
						   OTuple pk, uint16 attn, Size data_size,
						   CommitSeqNo csn);

extern int	o_toast_cmp(BTreeDescr *desc, void *p1, BTreeKeyType k1,
						void *p2, BTreeKeyType k2);
extern bool o_toast_needs_undo(BTreeDescr *desc, BTreeOperationType action,
							   OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
							   OTuple newTuple, OXid newOxid);

extern Datum create_o_toast_external(OTableDescr *descr,
									 OTuple idx_tup,
									 AttrNumber attnum,
									 OToastValue *toasted,
									 CommitSeqNo csn);

/* gets raw value size without header */
static inline int32
o_get_raw_size(Datum value)
{
	struct varlena *attr = (struct varlena *) DatumGetPointer(value);

	if (VARATT_IS_EXTERNAL_ORIOLEDB(DatumGetPointer(value)))
	{
		OToastExternal ote;

		memcpy(&ote, VARDATA_EXTERNAL(DatumGetPointer(value)), O_TOAST_EXTERNAL_SZ);
		return ote.raw_size;
	}
	else if (VARATT_IS_EXTERNAL(value))
		return toast_raw_datum_size(value) - VARHDRSZ;
	else if (VARATT_IS_COMPRESSED(attr))
#if PG_VERSION_NUM >= 140000
		return VARDATA_COMPRESSED_GET_EXTSIZE(attr);
#else
		return VARRAWSIZE_4B_C(attr);
#endif
	else
		return VARSIZE_ANY_EXHDR(value);
}

/* gets full value size */
static inline int32
o_get_src_size(Datum value)
{
	if (VARATT_IS_EXTERNAL_ORIOLEDB(DatumGetPointer(value)))
	{
		OToastExternal ote;

		memcpy(&ote, VARDATA_EXTERNAL(DatumGetPointer(value)), O_TOAST_EXTERNAL_SZ);
		return ote.toasted_size;
	}
	else if (VARATT_IS_EXTERNAL(value))
		return toast_datum_size(value);
	else
		return VARSIZE_ANY(value);
}


/* gets raw decompressed value */
extern Datum o_get_raw_value(Datum value, bool *free);

extern Datum o_get_src_value(Datum value, bool *free);

/* returns true if left and right are equal orioledb TOAST values */
extern bool o_toast_equal(BTreeDescr *primary, Datum left, Datum right);

#endif							/* __TOAST_H__ */
