/*-------------------------------------------------------------------------
 *
 * o_opclass.h
 *		Routines for orioledb operator classes list.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/o_opclass.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __O_OPCLASS_H__
#define __O_OPCLASS_H__

#include "catalog/o_tables.h"
#include "catalog/sys_trees.h"

extern void o_opclass_init(void);
extern void o_opclass_add_all(OTable *o_table);
extern OOpclass *o_opclass_get(Oid datoid, Oid opclass);

#endif							/* __O_OPCLASS_H__ */
