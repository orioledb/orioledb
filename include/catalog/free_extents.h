/*-------------------------------------------------------------------------
 *
 * free_extents.h
 * 		Routines for an orioledb free file extents list.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/free_extents.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __FREE_EXTENTS_H__
#define __FREE_EXTENTS_H__

#include "catalog/sys_trees.h"

extern FileExtent get_extent(BTreeDescr *desc, uint16 len);
extern void free_extent(BTreeDescr *desc, FileExtent extent);

typedef void (*ForEachExtentCallback) (BTreeDescr *desc, FileExtent extent, void *arg);
extern void foreach_free_extent(BTreeDescr *desc, ForEachExtentCallback callback,
								void *arg);
extern void add_free_extents_from_tmp(BTreeDescr *desc, bool remove);

#endif							/* __FREE_EXTENTS_H__ */
