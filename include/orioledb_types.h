/*-------------------------------------------------------------------------
 *
 * orioledb.h
 *		Common type declarations for orioledb engine.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/orioledb_types.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __ORIOLEDB_TYPES_H__
#define __ORIOLEDB_TYPES_H__

/* Number of orioledb page */
typedef uint32 OInMemoryBlkno;
#define OInvalidInMemoryBlkno		((OInMemoryBlkno) 0xFFFFFFFF)
#define OInMemoryBlknoIsValid(blockNumber) \
	((bool) ((OInMemoryBlkno) (blockNumber) != OInvalidInMemoryBlkno))
#define ORootPageIsValid(desc) (OInMemoryBlknoIsValid((desc)->rootInfo.rootPageBlkno))
#define OMetaPageIsValid(desc) (OInMemoryBlknoIsValid((desc)->rootInfo.metaPageBlkno))

/* Undo log location */
typedef uint64 UndoLocation;
#define	InvalidUndoLocation		UINT64CONST(0x2000000000000000)
#define	UndoLocationValueMask	UINT64CONST(0x1FFFFFFFFFFFFFFF)
#define UndoLocationIsValid(loc)	(((loc) & InvalidUndoLocation) == 0)
#define UndoLocationGetValue(loc)	((loc) & UndoLocationValueMask)

/* Identifier for orioledb transaction */
typedef uint64 OXid;
#define	InvalidOXid					UINT64CONST(0x7FFFFFFFFFFFFFFF)
#define OXidIsValid(oxid)			((oxid) != InvalidOXid)
#define LXID_NORMAL_FROM			(1)

/* Index number */
typedef uint16 OIndexNumber;

/* Index type */
typedef enum
{
	oIndexInvalid = 0,
	oIndexToast = 1,
	oIndexBridge = 2,
	oIndexPrimary = 3,
	oIndexUnique = 4,
	oIndexRegular = 5,
} OIndexType;

typedef struct
{
	Oid			datoid;
	Oid			reloid;
	Oid			relnode;
} ORelOids;

/* btree/btree.h */
typedef struct BTreeDescr BTreeDescr;

typedef struct
{
	Pointer		data;
	uint8		formatFlags;
} OTuple;

#define O_TUPLE_IS_NULL(tup) ((tup).data == NULL)
#define O_TUPLE_SET_NULL(tup) \
	do { \
        (tup).data = NULL; \
        (tup).formatFlags = 0; \
    } while (false)

typedef struct
{
	OInMemoryBlkno rootPageBlkno;
	uint32		rootPageChangeCount;
	OInMemoryBlkno metaPageBlkno;
} BTreeRootInfo;

typedef enum
{
	/* just in memory BTree, no eviction and no checkpoint support */
	BTreeStorageInMemory,
	/* no checkpoint support, but pages can be evicted into a disk */
	BTreeStorageTemporary,
	/* like BTreeStoragePersistence, but no wal for data modifications */
	BTreeStorageUnlogged,
	/* checkpoint and eviction for pages support */
	BTreeStoragePersistence
} BTreeStorageType;

/* btree/print.h */
/* Tuples and keys printing func */
typedef void (*PrintFunc) (BTreeDescr *desc, StringInfo buf,
						   OTuple tup, Pointer arg);

/* recovery/wal.h */
#define ORIOLEDB_RMGR_ID (129)
#define ORIOLEDB_XLOG_CONTAINER (0x00)

/* tableam/handler.c */
typedef struct OTableDescr OTableDescr;
typedef struct OIndexDescr OIndexDescr;

#endif							/* __ORIOLEDB_TYPES_H__ */
