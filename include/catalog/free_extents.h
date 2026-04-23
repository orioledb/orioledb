/*-------------------------------------------------------------------------
 *
 * free_extents.h
 * 		Routines for an orioledb free file extents list.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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

/*
 * Returns true if `desc` uses the backend-local page pool.  Only such trees
 * (user temporary tables) need the backend-local free space map below;
 * system trees that happen to be BTreeStorageTemporary still use a shared
 * pool and continue to rely on the checkpoint-tagged seq bufs.
 */
extern bool btree_desc_is_local_temp(BTreeDescr *desc);

/*
 * Backend-local free extent list for user temporary trees.  These helpers
 * are process-local and do not consult any shared checkpoint state.
 */
extern void local_free_extents_push(BTreeDescr *desc, FileExtent extent);
extern bool local_free_extents_pop(BTreeDescr *desc, uint16 len,
								   FileExtent *extent);
extern void local_free_extents_cleanup(BTreeDescr *desc);

#endif							/* __FREE_EXTENTS_H__ */
