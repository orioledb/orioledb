/*-------------------------------------------------------------------------
 *
 * stopevent.h
 *		Declarations of stopevent utility functions for B-tree.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/chunk_ops.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_STOPEVENT_H__
#define __BTREE_STOPEVENT_H__

#include "btree/btree.h"
#include "btree/chunk_ops.h"

extern void btree_desc_stopevent_params_internal(BTreeDescr *desc,
												 JsonbParseState **state);
extern void btree_page_stopevent_params_internal(BTreePageLocator *pageContext,
												 JsonbParseState **state);
extern Jsonb *btree_page_stopevent_params(BTreePageLocator *pageContext);
extern Jsonb *btree_downlink_stopevent_params(BTreePageLocator *pageContext,
											  BTreePageItemLocator *loc);

#endif							/* __BTREE_STOPEVENT_H__ */
