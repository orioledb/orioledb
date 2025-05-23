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
#include "postgres.h"

#include "orioledb.h"

#include "btree/stopevent.h"
#include "catalog/sys_trees.h"
#include "tableam/descr.h"
#include "utils/stopevent.h"

static inline OIndexDescr *
o_get_tree_def(BTreeDescr *desc)
{
	return desc->arg;
}

void
btree_desc_stopevent_params_internal(BTreeDescr *desc, JsonbParseState **state)
{
	jsonb_push_int8_key(state, "datoid", desc->oids.datoid);
	jsonb_push_int8_key(state, "reloid", desc->oids.reloid);
	jsonb_push_int8_key(state, "relnode", desc->oids.relnode);

	if (IS_SYS_TREE_OIDS(desc->oids))
		jsonb_push_string_key(state, "treeName", "sys_tree");
	else if (desc->type == oIndexToast)
		jsonb_push_string_key(state, "treeName", "toast");
	else
		jsonb_push_string_key(state, "treeName", o_get_tree_def(desc)->name.data);
}

void
btree_page_stopevent_params_internal(BTreePageLocator *pageContext,
									 JsonbParseState **state)
{
	jsonb_push_int8_key(state, "level", PAGE_GET_LEVEL(pageContext->page));
	jsonb_push_int8_key(state, "pageChangeCount",
						O_PAGE_GET_CHANGE_COUNT(pageContext->page));

	jsonb_push_key(state, "hikey");
	if (!O_PAGE_IS(pageContext->page, RIGHTMOST))
	{
		OTuple		hikey;

		hikey = btree_get_hikey(pageContext);
		(void) o_btree_key_to_jsonb(pageContext->treeDesc, hikey, state);
	}
	else
	{
		JsonbValue	jval;

		jval.type = jbvNull;
		(void) pushJsonbValue(state, WJB_VALUE, &jval);
	}
}

Jsonb *
btree_page_stopevent_params(BTreePageLocator *pageContext)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;
	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(pageContext->treeDesc, &state);
	btree_page_stopevent_params_internal(pageContext, &state);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

Jsonb *
btree_downlink_stopevent_params(BTreePageLocator *pageContext, BTreePageItemLocator *loc)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;
	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);
	BTreeNonLeafTuphdr *internal_ptr;

	internal_ptr = (BTreeNonLeafTuphdr *)
		BTREE_PAGE_LOCATOR_GET_ITEM(pageContext->page, loc);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(pageContext->treeDesc, &state);
	btree_page_stopevent_params_internal(pageContext, &state);

	jsonb_push_key(&state, "downlink");
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(&state, "blkno", DOWNLINK_GET_IN_MEMORY_BLKNO(internal_ptr->downlink));
	jsonb_push_int8_key(&state, "pageChangeCount", DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(internal_ptr->downlink));
	jsonb_push_key(&state, "key");
	if (BTREE_PAGE_LOCATOR_GET_OFFSET(pageContext->page, loc) > 0)
	{
		OTuple		key;

		BTREE_PAGE_READ_INTERNAL_TUPLE(key, pageContext->page, loc);
		(void) o_btree_key_to_jsonb(pageContext->treeDesc, key, &state);
	}
	else
	{
		JsonbValue	jval;

		jval.type = jbvNull;
		(void) pushJsonbValue(&state, WJB_VALUE, &jval);
	}
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}
