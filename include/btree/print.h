/*-------------------------------------------------------------------------
 *
 * print.h
 *		Declarations of OrioleDB B-tree printing routines.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/print.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_PRINT_H__
#define __BTREE_PRINT_H__

#include "fmgr.h"

#include "btree/btree.h"
#include "tuple/format.h"

#include "access/tupdesc.h"

typedef enum
{
	BTreeNotPrint = 0,
	BTreePrintAbsolute,
	BTreePrintRelative
} BTreePrintOption;

typedef struct
{
	BTreePrintOption pagePrintType;
	BTreePrintOption csnPrintType;
	BTreePrintOption backendIdPrintType;
	BTreePrintOption undoLogLocationPrintType;
	BTreePrintOption idsPrintType;
	BTreePrintOption changeCountPrintType;
	BTreePrintOption checkpointNumPrintType;
	bool		printRowVersion;
	bool		printStateValue;
	bool		printFileOffset;
	bool		printFormatFlags;
} BTreePrintOptions;

/* Tuples and keys printing func */
typedef void (*PrintFunc) (BTreeDescr *desc, StringInfo buf,
						   OTuple tup, Pointer arg);

extern void o_print_btree_pages(BTreeDescr *desc, StringInfo outbuf,
								PrintFunc keyPrintFunc,
								PrintFunc tuplePrintFunc,
								Pointer printArg,
								BTreePrintOptions *options, int depth);

extern void o_tuple_print(TupleDesc tupDesc, OTupleFixedFormatSpec *spec,
						  FmgrInfo *outputFns, StringInfo buf, OTuple tup,
						  Datum *values, bool *nulls, bool printVersion);

#endif							/* __BTREE_PRINT_H__ */
