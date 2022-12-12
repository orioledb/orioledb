/*-------------------------------------------------------------------------
 *
 * indices.h
 *		Indices routines.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/indices.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __INDICES_H__
#define __INDICES_H__

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_tables.h"

typedef struct ODefineIndexContext {
	Oid			oldNode;
} ODefineIndexContext;

extern void o_define_index_validate(Relation rel, IndexStmt *stmt,
									ODefineIndexContext **arg);
extern void o_define_index(Relation rel, Oid indoid, bool reindex,
						   bool skip_constraint_checks, bool skip_build,
						   ODefineIndexContext *context);

extern void o_index_drop(Relation tbl, OIndexNumber ix_num);
extern OIndexNumber o_find_ix_num_by_name(OTableDescr *descr,
										  char *ix_name);
extern bool is_in_indexes_rebuild(void);

extern void rebuild_indices(OTable *old_o_table, OTableDescr *old_descr,
							OTable *o_table, OTableDescr *descr);
extern void assign_new_oids(OTable *oTable, Relation rel);
extern void recreate_o_table(OTable *old_o_table, OTable *o_table);
extern void build_secondary_index(OTable *o_table, OTableDescr *descr,
								  OIndexNumber ix_num);

#endif							/* __INDICES_H__ */
