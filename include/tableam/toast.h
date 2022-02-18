/*-------------------------------------------------------------------------
 *
 * toast.h
 * 		Table-level declarations for orioledb TOAST implementation
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/toast.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_TOAST_H__
#define __TABLEAM_TOAST_H__

#include "postgres.h"

#include "btree/btree.h"
#include "tableam/descr.h"

/*
 * Table-level interface for work with orioledb TOAST with OTableDescr.
 */

/* external function used by toast_fetch_datum() */
extern struct varlena *o_detoast(struct varlena *attr);

/*
 * BTree functions.
 */

/* Prints toast index tuple */
extern void o_toast_key_print(BTreeDescr *desc, StringInfo buf,
							  OTuple tup, Pointer arg);

/* Prints toast table tuple */
extern void o_toast_tup_print(BTreeDescr *desc, StringInfo buf,
							  OTuple tup, Pointer arg);

/*
 * Useful definitions and functions which can be used by external code.
 */

#ifdef WORDS_BIGENDIAN

#define SET_TOAST_POINTER(PTR) \
	(*((uint8 *) (PTR)) = 0x80)
#define IS_TOAST_POINTER(PTR) \
	(*((uint8 *) (PTR)) == 0x80)

#else							/* !WORDS_BIGENDIAN */

#define SET_TOAST_POINTER(PTR) \
	(*((uint8 *) (PTR)) = 0x01)
#define IS_TOAST_POINTER(PTR) \
	(*((uint8 *) (PTR)) == 0x01)

#endif

#endif							/* __TABLE_TOAST_H__ */
