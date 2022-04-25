/*-------------------------------------------------------------------------
 *
 * ddl.c
 *		Rountines for DDL handling.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/ddl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/undo.h"
#include "catalog/indices.h"
#include "catalog/o_indices.h"
#include "catalog/o_opclass.h"
#include "catalog/o_tables.h"
#include "catalog/o_type_cache.h"
#include "tableam/toast.h"
#include "transam/oxid.h"
#include "utils/compress.h"

#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static object_access_hook_type old_objectaccess_hook = NULL;

static void orioledb_utility_command(PlannedStmt *pstmt,
									 const char *queryString,
#if PG_VERSION_NUM >= 140000
									 bool readOnlyTree,
#endif
									 ProcessUtilityContext context,
									 ParamListInfo params,
									 QueryEnvironment *env,
									 DestReceiver *dest,
									 struct QueryCompletion *qc);
static void orioledb_object_access_hook(ObjectAccessType access, Oid classId,
										Oid objectId, int subId, void *arg);

void
orioledb_setup_ddl_hooks(void)
{
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = orioledb_utility_command;
	old_objectaccess_hook = object_access_hook;
	object_access_hook = orioledb_object_access_hook;
}

List *
extract_compress_rel_option(List *defs, char *option, int *value)
{
	bool		founded = false;
	int			i;

	i = 0;
	while (i < list_length(defs))
	{
		DefElem    *def = (DefElem *) list_nth(defs, i);

		if (strcmp(def->defname, option) == 0)
		{
			if (def->arg == NULL)
				*value = O_COMPRESS_DEFAULT;
			else if (!IsA(def->arg, Integer))
				elog(ERROR, "Option %s must be integer value.", option);
			else
				*value = intVal(def->arg);
			founded = true;
		}

		if (founded)
		{
			defs = list_delete_nth_cell(defs, i);
			break;
		}
		i++;
	}

	return defs;
}

void
validate_compress(OCompress compress, char *prefix)
{
	OCompress	max_compress = o_compress_max_lvl();

	if (compress < -1 || compress > max_compress)
	{
		elog(ERROR, "%s compression level must be between %d and %d",
			 prefix, -1, max_compress);
	}
}

static const char *
deparse_alter_table_cmd_subtype(AlterTableCmd *cmd)
{
	const char *strtype;

	switch (cmd->subtype)
	{
	case AT_AddColumn:
		strtype = "ADD COLUMN";
		break;
	case AT_AddColumnRecurse:
		strtype = "ADD COLUMN (and recurse)";
		break;
	case AT_AddColumnToView:
		strtype = "ADD COLUMN TO VIEW";
		break;
	case AT_ColumnDefault:
		strtype = "ALTER COLUMN SET DEFAULT";
		break;
	case AT_CookedColumnDefault:
		strtype = "ALTER COLUMN SET DEFAULT (precooked)";
		break;
	case AT_DropNotNull:
		strtype = "DROP NOT NULL";
		break;
	case AT_SetNotNull:
		strtype = "SET NOT NULL";
		break;
	case AT_CheckNotNull:
		strtype = "CHECK NOT NULL";
		break;
	case AT_SetStatistics:
		strtype = "SET STATS";
		break;
	case AT_SetOptions:
		strtype = "SET OPTIONS";
		break;
	case AT_ResetOptions:
		strtype = "RESET OPTIONS";
		break;
	case AT_SetStorage:
		strtype = "SET STORAGE";
		break;
	case AT_DropColumn:
		strtype = "DROP COLUMN";
		break;
	case AT_DropColumnRecurse:
		strtype = "DROP COLUMN (and recurse)";
		break;
	case AT_AddIndex:
		strtype = "ADD INDEX";
		break;
	case AT_ReAddIndex:
		strtype = "(re) ADD INDEX";
		break;
	case AT_AddConstraint:
		strtype = "ADD CONSTRAINT";
		break;
	case AT_AddConstraintRecurse:
		strtype = "ADD CONSTRAINT (and recurse)";
		break;
	case AT_ReAddConstraint:
		strtype = "(re) ADD CONSTRAINT";
		break;
	case AT_AlterConstraint:
		strtype = "ALTER CONSTRAINT";
		break;
	case AT_ValidateConstraint:
		strtype = "VALIDATE CONSTRAINT";
		break;
	case AT_ValidateConstraintRecurse:
		strtype = "VALIDATE CONSTRAINT (and recurse)";
		break;
	case AT_AddIndexConstraint:
		strtype = "ADD CONSTRAINT (using index)";
		break;
	case AT_DropConstraint:
		strtype = "DROP CONSTRAINT";
		break;
	case AT_DropConstraintRecurse:
		strtype = "DROP CONSTRAINT (and recurse)";
		break;
	case AT_ReAddComment:
		strtype = "(re) ADD COMMENT";
		break;
	case AT_AlterColumnType:
		strtype = "ALTER COLUMN SET TYPE";
		break;
	case AT_AlterColumnGenericOptions:
		strtype = "ALTER COLUMN SET OPTIONS";
		break;
	case AT_ChangeOwner:
		strtype = "CHANGE OWNER";
		break;
	case AT_ClusterOn:
		strtype = "CLUSTER";
		break;
	case AT_DropCluster:
		strtype = "DROP CLUSTER";
		break;
	case AT_SetLogged:
		strtype = "SET LOGGED";
		break;
	case AT_SetUnLogged:
		strtype = "SET UNLOGGED";
		break;
	case AT_DropOids:
		strtype = "DROP OIDS";
		break;
	case AT_SetTableSpace:
		strtype = "SET TABLESPACE";
		break;
	case AT_SetRelOptions:
		strtype = "SET RELOPTIONS";
		break;
	case AT_ResetRelOptions:
		strtype = "RESET RELOPTIONS";
		break;
	case AT_ReplaceRelOptions:
		strtype = "REPLACE RELOPTIONS";
		break;
	case AT_EnableTrig:
		strtype = "ENABLE TRIGGER";
		break;
	case AT_EnableAlwaysTrig:
		strtype = "ENABLE TRIGGER (always)";
		break;
	case AT_EnableReplicaTrig:
		strtype = "ENABLE TRIGGER (replica)";
		break;
	case AT_DisableTrig:
		strtype = "DISABLE TRIGGER";
		break;
	case AT_EnableTrigAll:
		strtype = "ENABLE TRIGGER (all)";
		break;
	case AT_DisableTrigAll:
		strtype = "DISABLE TRIGGER (all)";
		break;
	case AT_EnableTrigUser:
		strtype = "ENABLE TRIGGER (user)";
		break;
	case AT_DisableTrigUser:
		strtype = "DISABLE TRIGGER (user)";
		break;
	case AT_EnableRule:
		strtype = "ENABLE RULE";
		break;
	case AT_EnableAlwaysRule:
		strtype = "ENABLE RULE (always)";
		break;
	case AT_EnableReplicaRule:
		strtype = "ENABLE RULE (replica)";
		break;
	case AT_DisableRule:
		strtype = "DISABLE RULE";
		break;
	case AT_AddInherit:
		strtype = "ADD INHERIT";
		break;
	case AT_DropInherit:
		strtype = "DROP INHERIT";
		break;
	case AT_AddOf:
		strtype = "OF";
		break;
	case AT_DropOf:
		strtype = "NOT OF";
		break;
	case AT_ReplicaIdentity:
		strtype = "REPLICA IDENTITY";
		break;
	case AT_EnableRowSecurity:
		strtype = "ENABLE ROW SECURITY";
		break;
	case AT_DisableRowSecurity:
		strtype = "DISABLE ROW SECURITY";
		break;
	case AT_ForceRowSecurity:
		strtype = "FORCE ROW SECURITY";
		break;
	case AT_NoForceRowSecurity:
		strtype = "NO FORCE ROW SECURITY";
		break;
	case AT_GenericOptions:
		strtype = "SET OPTIONS";
		break;
	case AT_AttachPartition:
		strtype = "ATTACH PARTITION";
		break;
	case AT_DetachPartition:
		strtype = "DETACH PARTITION";
		break;
#if PG_VERSION_NUM >= 140000
	case AT_DetachPartitionFinalize:
		strtype = "DETACH PARTITION FINALIZE";
		break;
	case AT_ReAddStatistics:
		strtype = "ADD STATISTICS";
		break;
#endif
	case AT_AddIdentity:
		strtype = "ADD IDENTITY";
		break;
	case AT_SetIdentity:
		strtype = "SET IDENTITY";
		break;
	case AT_DropIdentity:
		strtype = "DROP IDENTITY";
		break;
	default:
		strtype = "unrecognized";
		break;
	}

	return strtype;
}

static bool
validate_at_utility(PlannedStmt *pstmt,
					const char *queryString,
					Oid relid, List *cmds, Relation rel)
{
	OTable	   *o_table;
	OTableField *o_field = NULL;
	ListCell   *lc;
	int			i;
	ColumnDef  *coldef;
	Oid			type;
	bool		updated;
	bool		tupdesc_changed;
	ORelOids	oids = {MyDatabaseId, relid, rel->rd_node.relNode};
	CommitSeqNo csn;
	OXid		oxid;
	bool		call_next = true;

	fill_current_oxid_csn(&oxid, &csn);

	o_table = o_tables_get(oids);
	if (o_table == NULL)
	{
		/* table does not exist */
		elog(NOTICE, "orioledb table \"%s\" not found", RelationGetRelationName(rel));
		return call_next;
	}

	updated = false;
	tupdesc_changed = false;
	foreach(lc, cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
		case AT_AlterColumnType:
		case AT_DropColumn:
		case AT_DropNotNull:
		case AT_SetNotNull:
		case AT_ColumnDefault:
			o_field = o_table_field_by_name(o_table, cmd->name);
			break;

		default:
			break;
		}

		/* make checks */
		switch (cmd->subtype)
		{
			case AT_AlterColumnType:
			case AT_DropColumn:
			case AT_DropNotNull:

				/*
				 * We don't support rewriting the relation for now.  So, we
				 * can only change the type if new type is binary coersible
				 * with the old one.
				 */
				if (cmd->subtype == AT_AlterColumnType)
				{
					coldef = (ColumnDef *) cmd->def;
					type = typenameTypeId(NULL, coldef->typeName);

					if (!IsBinaryCoercible(o_field->typid, type))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("could not change the column type")),
								errdetail("Column \"%s\" of OrioleDB table \"%s\" has type \"%s\". Can't change to \"%s\", because it's not binary coersible.",
										  cmd->name,
										  RelationGetRelationName(rel),
										  format_type_be(o_field->typid),
										  TypeNameToString(coldef->typeName)));
				}

				if (o_table->nindices == 0)
					break;

				for (i = 0; i < o_table->nindices; i++)
				{
					OTableIndex *index = &o_table->indices[i];
					int			j;

					for (j = 0; j < index->nfields; j++)
					{
						OTableField *field = &o_table->fields[index->fields[j].attnum];

						if (pg_strcasecmp(NameStr(field->name), cmd->name) != 0)
							continue;

						if (cmd->subtype == AT_DropColumn)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("could not drop the column")),
									errdetail("Column \"%s\" of OrioleDB table \"%s\" id used in \"%s\" index definition.",
											  cmd->name,
											  RelationGetRelationName(rel),
											  NameStr(index->name)));
						else if (cmd->subtype == AT_AlterColumnType)
						{
							coldef = (ColumnDef *) cmd->def;

							if (OidIsValid(index->fields[j].collation))
								continue;

							if (coldef->collClause != NULL)
							{
								Oid			collid = get_collation_oid(coldef->collClause->collname, false);

								if (collid != field->collation)
									ereport(ERROR,
											(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
											 errmsg("could not change the column collation")),
											errdetail("Column \"%s\" of OrioleDB table \"%s\" id used in \"%s\" index definition.",
													  cmd->name,
													  RelationGetRelationName(rel),
													  NameStr(index->name)));
							}
							else if (field->collation != DEFAULT_COLLATION_OID)
							{
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("could not change the column collation")),
										errdetail("Column \"%s\" of OrioleDB table \"%s\" id used in \"%s\" index definition.",
												  cmd->name,
												  RelationGetRelationName(rel),
												  NameStr(index->name)));
							}
						}
					}
				}
				break;
			case AT_AddIndex:
			case AT_AddColumn:
			case AT_ColumnDefault:
			case AT_AddConstraint:
			case AT_DropConstraint:
			case AT_GenericOptions:
			case AT_SetNotNull:
			case AT_ChangeOwner:
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unsupported alter table subcommand")),
						errdetail("Subcommand \"%s\" is not "
								  "supported on OrioleDB tables.",
								  deparse_alter_table_cmd_subtype(cmd)));
				break;
		}

		/* all checks are passed, confirm changes for field */
		switch (cmd->subtype)
		{
			case AT_AlterColumnType:
				if (o_field)
				{
					ColumnDef	   *coldef = (ColumnDef *) cmd->def;
					Oid				type = typenameTypeId(NULL,
														  coldef->typeName);

					if (o_field->typid != type)
					{
						o_field->typid = type;
						updated = true;
					}
					if (coldef->collClause != NULL)
					{
						List	   *collname = coldef->collClause->collname;
						Oid			collid;

						collid = get_collation_oid(collname, false);

						if (o_field->collation != collid)
						{
							o_field->collation = collid;
							updated = true;
						}
					}
				}
				break;
			case AT_DropColumn:
				if (o_field && !o_field->droped)
				{
					o_field->droped = true;
					updated = true;
				}
				break;
			case AT_DropNotNull:
				if (o_field && o_field->notnull)
				{
					o_field->notnull = false;
					updated = true;
				}
				break;
			case AT_SetNotNull:
				if (o_field && !o_field->notnull)
				{
					o_field->notnull = true;
					updated = true;
				}
				break;
			case AT_DropConstraint:
				{
					OIndexNumber ix_num;
					OTableDescr *descr = relation_get_descr(rel);

					Assert(descr != NULL);
					ix_num = o_find_ix_num_by_name(descr, cmd->name);

					if (ix_num == PrimaryIndexNumber)
						o_index_drop(rel, PrimaryIndexNumber);
					break;
				}
			case AT_AddIndex:
				{
					o_index_create(rel, (IndexStmt *) cmd->def,
								   queryString, pstmt->utilityStmt);
					call_next = false;
					break;
				}
			case AT_AddColumn:
				{
					OTableField *field;
					ColumnDef  *col_def = (ColumnDef *) cmd->def;
					HeapTuple	typeTuple;
					Form_pg_type tform;
					Oid			typeOid;
					Oid			collOid;
					AttrMissing *attrmiss = NULL;
					AttrMissing attrmiss_temp;
					Expr	   *defval = NULL;

					o_table->nfields++;
					o_table->fields = repalloc(o_table->fields,
											   o_table->nfields *
											   sizeof(OTableField));
					memset(&o_table->fields[o_table->nfields - 1], 0,
						   sizeof(OTableField));
					field = &o_table->fields[o_table->nfields - 1];
					typeTuple = typenameType(NULL, col_def->typeName,
											 &field->typmod);
					tform = (Form_pg_type) GETSTRUCT(typeTuple);
					typeOid = tform->oid;
					collOid = GetColumnDefCollation(NULL, col_def, typeOid);
					/* make sure datatype is legal for a column */
					CheckAttributeType(col_def->colname, typeOid, collOid,
									   list_make1_oid(rel->rd_rel->reltype),
									   0);

					strlcpy(field->name.data, col_def->colname, NAMEDATALEN);
					field->typid = typeOid;
					field->collation = collOid;
					field->typlen = tform->typlen;
					field->ndims = list_length(col_def->typeName->arrayBounds);
					field->byval = tform->typbyval;
					field->align = tform->typalign;
					field->storage = tform->typstorage;
					field->droped = false;
					field->notnull = col_def->is_not_null;
					field->hasdef = col_def->raw_default != NULL;
					field->hasmissing = !col_def->generated && field->hasdef;

					if (field->hasmissing)
					{
						Expr	   *expr2;
						ParseNamespaceItem *nsitem;
						ParseState *pstate;

						pstate = make_parsestate(NULL);
						pstate->p_sourcetext = queryString;
						nsitem = addRangeTableEntryForRelation(pstate,
															   rel,
															   AccessShareLock,
															   NULL,
															   false,
															   true);
						addNSItemToQuery(pstate, nsitem, true, true, true);

						expr2 = (Expr *) cookDefault(pstate,
													 col_def->raw_default,
													 tform->oid, tform->typtypmod,
													 NameStr(field->name),
													 col_def->generated);

						if (field->hasmissing &&
							contain_volatile_functions((Node *) expr2))
							field->hasmissing = false;

						if (field->hasmissing)
						{
							EState	   *estate = NULL;
							ExprContext *econtext;
							ExprState  *exprState;
							MemoryContext oldcxt;
							bool		missingIsNull = true;
							Datum		missingval = (Datum) 0;

							expr2 = expression_planner(expr2);

							oldcxt = MemoryContextSwitchTo(TopMemoryContext);
							estate = CreateExecutorState();
							exprState = ExecPrepareExpr(expr2, estate);
							econtext = GetPerTupleExprContext(estate);

							missingval = ExecEvalExpr(exprState, econtext,
													  &missingIsNull);

							FreeExecutorState(estate);
							free_parsestate(pstate);

							attrmiss_temp.am_value = datumCopy(missingval,
															   field->byval,
															   field->typlen);
							MemoryContextSwitchTo(oldcxt);
							attrmiss_temp.am_present = true;
							attrmiss = &attrmiss_temp;
							defval = expr2;
						}
						else if (field->hasdef)
						{
							defval = expression_planner(expr2);
						}
					}
					o_table_fill_constr(o_table, o_table->nfields - 1,
										attrmiss, defval);
					ReleaseSysCache(typeTuple);

					tupdesc_changed = true;
					updated = true;
				}
				break;
			case AT_AddConstraint:
			case AT_ColumnDefault:
			case AT_GenericOptions:
			case AT_ChangeOwner:
				break;
			default:
				/* handled by check */
				Assert(false);
				break;
		}
	}

	if (updated)
	{
		o_tables_validate_tupdesc(o_table_tupdesc(o_table));
		o_tables_update(o_table, oxid, csn);
		if (tupdesc_changed)
		{
			o_opclass_add_all(o_table);
			custom_types_add_all(o_table);
			o_indices_update(o_table, PrimaryIndexNumber, oxid, csn);
			if (o_table->has_primary)
				o_invalidate_oids(o_table->indices[PrimaryIndexNumber].oids);
			o_invalidate_oids(o_table->oids);
		}
	}
	o_table_free(o_table);
	return call_next;
}

static bool
is_alter_table_partition(PlannedStmt *pstmt)
{
	AlterTableStmt *top_atstmt = (AlterTableStmt *) pstmt->utilityStmt;

	if (list_length(top_atstmt->cmds) == 1)
	{
		AlterTableCmd *cmd = linitial(top_atstmt->cmds);

		if (cmd->subtype == AT_AttachPartition ||
			cmd->subtype == AT_DetachPartition)
			return true;
	}
	return false;
}

#if PG_VERSION_NUM < 140000
#define objtype relkind
#endif

static void
orioledb_utility_command(PlannedStmt *pstmt,
						 const char *queryString,
#if PG_VERSION_NUM >= 140000
						 bool readOnlyTree,
#endif
						 ProcessUtilityContext context,
						 ParamListInfo params,
						 QueryEnvironment *env,
						 DestReceiver *dest,
						 struct QueryCompletion *qc)
{
	bool		call_next = true;

	if (IsA(pstmt->utilityStmt, AlterTableStmt) &&
		!is_alter_table_partition(pstmt))
	{
		AlterTableStmt *top_atstmt = (AlterTableStmt *) pstmt->utilityStmt;
		Relation	rel;
		Oid			relid;
		LOCKMODE	lockmode;

		/*
		 * Figure out lock mode, and acquire lock.  This also does basic
		 * permissions checks, so that we won't wait for a lock on (for
		 * example) a relation on which we have no permissions.
		 */
		lockmode = AlterTableGetLockLevel(top_atstmt->cmds);
		relid = AlterTableLookupRelation(top_atstmt, lockmode);

		if (OidIsValid(relid))
		{
			AlterTableUtilityContext atcontext;
			ListCell   *l;
			Node	   *stmt;
			List	   *beforeStmts;
			List	   *afterStmts;
			List	   *querytree_list = NIL;

			/* Set up info needed for recursive callbacks ... */
			atcontext.pstmt = pstmt;
			atcontext.queryString = queryString;
			atcontext.relid = relid;
			atcontext.params = params;
			atcontext.queryEnv = env;

			/* ... ensure we have an event trigger context ... */
			EventTriggerAlterTableStart(pstmt->utilityStmt);
			EventTriggerAlterTableRelid(relid);

			/* Run parse analysis for ALTER TABLE */
			stmt = (Node *) transformAlterTableStmt(relid, top_atstmt,
													queryString,
													&beforeStmts,
													&afterStmts);

			querytree_list = list_concat(querytree_list, beforeStmts);
			querytree_list = lappend(querytree_list, stmt);
			querytree_list = list_concat(querytree_list, afterStmts);


			/* Loop trough the parse analysis results */
			foreach(l, querytree_list)
			{
				stmt = (Node *) lfirst(l);

				if (IsA(stmt, AlterTableStmt))
				{
					AlterTableStmt *atstmt = (AlterTableStmt *) stmt;
					Oid			myrelid;
					LOCKMODE	mode;

					if (atstmt->objtype != OBJECT_TABLE)
						goto done_alter_table;

					mode = AlterTableGetLockLevel(atstmt->cmds);
					if (mode != AccessExclusiveLock)
						goto done_alter_table;
					myrelid = AlterTableLookupRelation(atstmt, AccessExclusiveLock);

					if (!OidIsValid(myrelid))
						goto done_alter_table;

					rel = table_open(myrelid, NoLock);
					if (atstmt->objtype == OBJECT_TABLE && !is_orioledb_rel(rel))
					{
						UnlockRelationOid(myrelid, AccessExclusiveLock);
						table_close(rel, NoLock);
						goto done_alter_table;
					}
					call_next = validate_at_utility(pstmt, queryString, myrelid, atstmt->cmds, rel);
					table_close(rel, NoLock);
			done_alter_table:
					if (call_next)
						AlterTable(atstmt, lockmode, &atcontext);
				}
				else
				{
					/*
					 * Recurse for anything else.  If we need to do so,
					 * "close" the current complex-command set, and start a
					 * new one at the bottom; this is needed to ensure the
					 * ordering of queued commands is consistent with the way
					 * they are executed here.
					 */
					PlannedStmt *wrapper;

					EventTriggerAlterTableEnd();
					wrapper = makeNode(PlannedStmt);
					wrapper->commandType = CMD_UTILITY;
					wrapper->canSetTag = false;
					wrapper->utilityStmt = stmt;
					wrapper->stmt_location = pstmt->stmt_location;
					wrapper->stmt_len = pstmt->stmt_len;
					ProcessUtility(wrapper,
								   queryString,
#if PG_VERSION_NUM >= 140000
								   readOnlyTree,
#endif
								   PROCESS_UTILITY_SUBCOMMAND,
								   params,
								   NULL,
								   None_Receiver,
								   NULL);
					EventTriggerAlterTableStart(stmt);
					EventTriggerAlterTableRelid(relid);
				}

				/* Need CCI between commands */
				if (lnext(querytree_list, l) != NULL)
					CommandCounterIncrement();
			}

			/* done */
			EventTriggerAlterTableEnd();
		}
		else
		{
			ereport(NOTICE,
					(errmsg("relation \"%s\" does not exist, skipping",
							top_atstmt->relation->relname)));
		}
		call_next = false;
	}
	else if (IsA(pstmt->utilityStmt, CreateStmt))
	{
		/*
		 * copy-paste with new_o_tables list from ProcessUtilitySlow in
		 * utility.c
		 */
		List	   *stmts;
		RangeVar   *table_rv = NULL;
		CommitSeqNo csn = COMMITSEQNO_INPROGRESS;
		OXid		oxid = InvalidOXid;
		bool		isCompleteQuery = (context <= PROCESS_UTILITY_QUERY);
		bool		needCleanup = isCompleteQuery && EventTriggerBeginCompleteQuery();

		stmts = transformCreateStmt((CreateStmt *) pstmt->utilityStmt, queryString);

		/*
		 * ... and do it.  We can't use foreach() because we may modify the
		 * list midway through, so pick off the elements one at a time, the
		 * hard way.
		 */
		while (stmts != NIL)
		{
			Node	   *stmt = (Node *) linitial(stmts);

			stmts = list_delete_first(stmts);

			ObjectAddress address;
			ObjectAddress secondaryObject = InvalidObjectAddress;

			if (IsA(stmt, CreateStmt))
			{
				OCompress	compress = default_compress,
							primary_compress = default_primary_compress,
							toast_compress = default_toast_compress;
				Datum		toast_options;
				static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
				CreateStmt *cstmt = (CreateStmt *) stmt;
				bool		orioledb;

				/* Remember transformed RangeVar for LIKE */
				table_rv = cstmt->relation;

				if (cstmt->accessMethod)
					orioledb = (strcmp(cstmt->accessMethod, "orioledb") == 0);
				else
					orioledb = (strcmp(default_table_access_method, "orioledb") == 0);

				if (orioledb)
				{
					cstmt->options = extract_compress_rel_option(cstmt->options,
																 "compress",
																 &compress);
					cstmt->options = extract_compress_rel_option(cstmt->options,
																 "primary_compress",
																 &primary_compress);
					cstmt->options = extract_compress_rel_option(cstmt->options,
																 "toast_compress",
																 &toast_compress);
				}
				validate_compress(compress, "Default");
				validate_compress(primary_compress, "Primary index");
				validate_compress(toast_compress, "TOAST");

				/* Create the table itself */
				address = DefineRelation(cstmt,
										 RELKIND_RELATION,
										 InvalidOid, NULL,
										 queryString);
				EventTriggerCollectSimpleCommand(address,
												 secondaryObject,
												 stmt);

				/*
				 * Let NewRelationCreateToastTable decide if this one needs a
				 * secondary relation too.
				 */
				CommandCounterIncrement();

				/*
				 * parse and validate reloptions for the toast table
				 */
				toast_options = transformRelOptions((Datum) 0,
													((CreateStmt *) stmt)->options,
													"toast",
													validnsps,
													true,
													false);
				(void) heap_reloptions(RELKIND_TOASTVALUE,
									   toast_options,
									   true);

				NewRelationCreateToastTable(address.objectId,
											toast_options);

				/*
				 * orioledb table have no need in PostgreSQL TOAST and this
				 * calls have no sense for us (see needs_toast_table(rel)
				 * check inside create_toast_table()), but call
				 * NewRelationCreateToastTable() always gets
				 * AccessExclusiveLock on the relation. So we just skip it.
				 */

				if (!OXidIsValid(oxid))
					fill_current_oxid_csn(&oxid, &csn);

				if (orioledb)
				{
					Relation	rel,
								toastRel;
					ORelOids	oids,
								toastOids,
							   *treeOids;
					TupleDesc	tupdesc;
					OTable	   *o_table;
					int			numTreeOids;

					rel = table_open(address.objectId, AccessShareLock);
					Assert(rel->rd_node.dbNode == MyDatabaseId);
					oids.datoid = MyDatabaseId;
					oids.reloid = rel->rd_id;
					oids.relnode = rel->rd_node.relNode;
					tupdesc = RelationGetDescr(rel);
					toastRel = table_open(rel->rd_rel->reltoastrelid,
										  AccessShareLock);
					toastOids.datoid = MyDatabaseId;
					toastOids.reloid = toastRel->rd_id;
					toastOids.relnode = toastRel->rd_node.relNode;

					o_tables_validate_tupdesc(tupdesc);
					o_table = o_table_tableam_create(oids, toastOids, tupdesc,
													 compress, primary_compress,
													 toast_compress);
					o_opclass_add_all(o_table);

					LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);
					o_tables_add(o_table, oxid, csn);
					LWLockRelease(&checkpoint_state->oTablesAddLock);

					treeOids = o_table_make_index_oids(o_table, &numTreeOids);
					add_undo_create_relnode(oids, treeOids, numTreeOids);
					pfree(treeOids);

					table_close(rel, AccessShareLock);
					table_close(toastRel, AccessShareLock);
				}
			}
			else if (IsA(stmt, TableLikeClause))
			{
				/*
				 * Do delayed processing of LIKE options.  This will result in
				 * additional sub-statements for us to process.  Those should
				 * get done before any remaining actions, so prepend them to
				 * "stmts".
				 */
				TableLikeClause *like = (TableLikeClause *) stmt;
				List	   *morestmts;

				Assert(table_rv != NULL);

				morestmts = expandTableLikeClause(table_rv, like);
				stmts = list_concat(morestmts, stmts);
			}
			else
			{
				/*
				 * Recurse for anything else.  Note the recursive call will
				 * stash the objects so created into our event trigger
				 * context.
				 */
				PlannedStmt *wrapper;

				wrapper = makeNode(PlannedStmt);
				wrapper->commandType = CMD_UTILITY;
				wrapper->canSetTag = false;
				wrapper->utilityStmt = stmt;
				wrapper->stmt_location = pstmt->stmt_location;
				wrapper->stmt_len = pstmt->stmt_len;

				ProcessUtility(wrapper,
							   queryString,
#if PG_VERSION_NUM >= 140000
							   readOnlyTree,
#endif
							   PROCESS_UTILITY_SUBCOMMAND,
							   params,
							   NULL,
							   None_Receiver,
							   NULL);
			}

			if (stmts != NIL)
				CommandCounterIncrement();
		}

		if (isCompleteQuery)
		{
			EventTriggerSQLDrop(pstmt->utilityStmt);
			EventTriggerDDLCommandEnd(pstmt->utilityStmt);
		}

		if (needCleanup)
			EventTriggerEndCompleteQuery();
		call_next = false;
	}
	else if (IsA(pstmt->utilityStmt, IndexStmt))
	{
		IndexStmt  *stmt = (IndexStmt *) pstmt->utilityStmt;
		Relation	rel;

		rel = table_openrv(stmt->relation, AccessExclusiveLock);

		if ((rel->rd_rel->relkind == RELKIND_RELATION) && is_orioledb_rel(rel))
		{
			o_index_create(rel, stmt, queryString, pstmt->utilityStmt);

			call_next = false;
		}
		table_close(rel, AccessExclusiveLock);
	}
	else if (IsA(pstmt->utilityStmt, RenameStmt))
	{
		RenameStmt *stmt = (RenameStmt *) pstmt->utilityStmt;

		if (stmt->renameType == OBJECT_INDEX)
		{
			Relation	idx = relation_openrv(stmt->relation, AccessExclusiveLock);
			Relation	tbl = relation_open(idx->rd_index->indrelid,
											AccessShareLock);

			if (tbl->rd_rel->relkind == RELKIND_RELATION &&
				is_orioledb_rel(tbl))
			{
				OTable	   *o_table;
				ORelOids	table_oids = {MyDatabaseId, tbl->rd_rel->oid, tbl->rd_node.relNode};

				o_table = o_tables_get(table_oids);
				if (o_table == NULL)
				{
					elog(NOTICE, "orioledb table %s not found", RelationGetRelationName(tbl));
				}
				else
				{
					int			ix_num;
					CommitSeqNo csn;
					OXid		oxid;
					ORelOids	idx_oids = {MyDatabaseId, idx->rd_rel->oid, idx->rd_node.relNode};

					for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
					{
						OTableIndex *index = &o_table->indices[ix_num];

						if (ORelOidsIsEqual(index->oids, idx_oids))
						{
							namestrcpy(&index->name, stmt->newname);
							break;
						}
					}
					Assert(ix_num < o_table->nindices);
					fill_current_oxid_csn(&oxid, &csn);
					o_tables_update(o_table, oxid, csn);
					o_indices_update(o_table, ix_num, oxid, csn);
					o_invalidate_oids(idx_oids);
					o_add_invalidate_undo_item(idx_oids,
											   O_INVALIDATE_OIDS_ON_ABORT);
					if (!ORelOidsIsEqual(idx_oids, table_oids))
					{
						o_invalidate_oids(table_oids);
						o_add_invalidate_undo_item(table_oids,
												   O_INVALIDATE_OIDS_ON_ABORT);
					}
					AcceptInvalidationMessages();
					o_table_free(o_table);
				}
			}
			relation_close(tbl, AccessShareLock);
			relation_close(idx, AccessExclusiveLock);
		}
		else if (stmt->renameType == OBJECT_COLUMN)
		{
			Relation	tbl = relation_openrv(stmt->relation, AccessExclusiveLock);

			if (tbl->rd_rel->relkind == RELKIND_RELATION &&
				is_orioledb_rel(tbl))
			{
				OTable	   *o_table;
				ORelOids	table_oids = {MyDatabaseId, tbl->rd_rel->oid, tbl->rd_node.relNode};

				o_table = o_tables_get(table_oids);
				if (o_table == NULL)
				{
					elog(NOTICE, "orioledb table %s not found", RelationGetRelationName(tbl));
				}
				else
				{
					CommitSeqNo csn;
					OXid		oxid;
					OTableField *field;
					int			ix_num,
								renamed_num;

					renamed_num = o_table_fieldnum(o_table, stmt->subname);
					if (renamed_num < o_table->nfields)
					{
						field = &o_table->fields[renamed_num];
						namestrcpy(&field->name, stmt->newname);
						fill_current_oxid_csn(&oxid, &csn);
						o_tables_update(o_table, oxid, csn);

						for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
						{
							OTableIndex *index = &o_table->indices[ix_num];
							int			field_num;

							for (field_num = 0; field_num < index->nfields;
								 field_num++)
							{
								if (index->fields[field_num].attnum ==
									renamed_num)
								{
									o_indices_update(o_table, ix_num,
													 oxid, csn);
									o_invalidate_oids(index->oids);
									break;
								}
							}
						}
						o_invalidate_oids(table_oids);
						AcceptInvalidationMessages();
					}
					o_table_free(o_table);
				}
			}
			relation_close(tbl, AccessExclusiveLock);
		}
	}

	if (call_next)
	{
		if (next_ProcessUtility_hook)
			(*next_ProcessUtility_hook) (pstmt, queryString,
#if PG_VERSION_NUM >= 140000
										 readOnlyTree,
#endif
										 context, params, env,
										 dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
									readOnlyTree,
#endif
									context, params, env,
									dest, qc);
	}

	/* Update type caches from catalogs after CommandCounterIncrement() */
	if (IsA(pstmt->utilityStmt, AlterTableStmt))
	{
		AlterTableStmt *top_atstmt = (AlterTableStmt *) pstmt->utilityStmt;
		Relation	rel;
		Oid			relid;
		LOCKMODE	lockmode;

		/*
		 * Figure out lock mode, and acquire lock.  This also does basic
		 * permissions checks, so that we won't wait for a lock on (for
		 * example) a relation on which we have no permissions.
		 */
		lockmode = AlterTableGetLockLevel(top_atstmt->cmds);
		relid = AlterTableLookupRelation(top_atstmt, lockmode);

		if (OidIsValid(relid))
		{
			Node	   *stmt;
			List	   *beforeStmts;
			List	   *afterStmts;
			List	   *querytree_list = NIL;
			ListCell   *l;

			/* Run parse analysis for ALTER TABLE */
			stmt = (Node *) transformAlterTableStmt(relid, top_atstmt,
													queryString,
													&beforeStmts,
													&afterStmts);

			querytree_list = list_concat(querytree_list, beforeStmts);
			querytree_list = lappend(querytree_list, stmt);
			querytree_list = list_concat(querytree_list, afterStmts);

			/* Loop trough the parse analysis results */
			foreach(l, querytree_list)
			{
				stmt = (Node *) lfirst(l);

				if (IsA(stmt, AlterTableStmt))
				{
					AlterTableStmt *atstmt = (AlterTableStmt *) stmt;
					Oid			myrelid;

					if (atstmt->objtype == OBJECT_TYPE)
					{
						myrelid = AlterTableLookupRelation(atstmt, AccessShareLock);

						if (OidIsValid(myrelid))
						{
							rel = relation_open(myrelid, NoLock);

							if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
							{
								Oid			reltype = rel->rd_rel->reltype;

								relation_close(rel, NoLock);
								o_record_cache_update_if_needed(MyDatabaseId,
																reltype, NULL);
							}
							else
							{
								relation_close(rel, NoLock);
							}

							UnlockRelationOid(myrelid, AccessShareLock);
						}
					}
				}
			}
		}
		else
		{
			ereport(NOTICE,
					(errmsg("relation \"%s\" does not exist, skipping",
							top_atstmt->relation->relname)));
		}
	}
	else if (IsA(pstmt->utilityStmt, AlterEnumStmt))
	{
		AlterEnumStmt *enum_stmt = (AlterEnumStmt *) pstmt->utilityStmt;
		Oid			enum_type_oid;
		TypeName   *typename;

		typename = makeTypeNameFromNameList(enum_stmt->typeName);
		enum_type_oid = typenameTypeId(NULL, typename);

		o_enum_cache_update_if_needed(MyDatabaseId, enum_type_oid, NULL);
	}
}

static void
o_find_composite_type_dependencies(Oid typeOid, Relation origRelation)
{
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc depScan;
	HeapTuple	depTup;

	/* since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	/*
	 * We scan pg_depend to find those things that depend on the given type.
	 * (We assume we can ignore refobjsubid for a type.)
	 */
	depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(TypeRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(typeOid));

	depScan = systable_beginscan(depRel, DependReferenceIndexId, true,
								 NULL, 2, key);

	while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		Relation	rel;

		/* Check for directly dependent types */
		if (pg_depend->classid == TypeRelationId)
		{
			/*
			 * This must be an array, domain, or range containing the given
			 * type, so recursively check for uses of this type.  Note that
			 * any error message will mention the original type not the
			 * container; this is intentional.
			 */
			o_find_composite_type_dependencies(pg_depend->objid, origRelation);
			continue;
		}

		/* Else, ignore dependees that aren't user columns of relations */
		/* (we assume system columns are never of interesting types) */
		if (pg_depend->classid != RelationRelationId ||
			pg_depend->objsubid <= 0)
			continue;

		rel = relation_open(pg_depend->objid, AccessShareLock);

		if (rel->rd_rel->relkind == RELKIND_RELATION && is_orioledb_rel(rel))
		{
			OTable	   *table;
			ORelOids	table_oids = {MyDatabaseId, rel->rd_rel->oid,
			rel->rd_node.relNode};
			bool		found = false;
			int			i;

			table = o_tables_get(table_oids);
			if (table == NULL)
			{
				elog(NOTICE, "orioledb table %s not found", RelationGetRelationName(rel));
			}
			else
			{
				for (i = 0; i < table->nindices && !found; i++)
				{
					int			j;

					for (j = 0; j < table->indices[i].nfields && !found; j++)
					{
						if (table->indices[i].fields[j].attnum ==
							pg_depend->objsubid - 1)
							found = true;
					}

				}

				if (found)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot alter type \"%s\" because index "
									"\"%s\" uses it",
									RelationGetRelationName(origRelation),
									NameStr(table->indices[i - 1].name))));
				o_table_free(table);
			}
		}
		else if (OidIsValid(rel->rd_rel->reltype))
		{
			/*
			 * A view or composite type itself isn't a problem, but we must
			 * recursively check for indirect dependencies via its rowtype.
			 */
			o_find_composite_type_dependencies(rel->rd_rel->reltype,
											   origRelation);
		}

		relation_close(rel, AccessShareLock);
	}

	systable_endscan(depScan);

	relation_close(depRel, AccessShareLock);
}

static void
orioledb_object_access_hook(ObjectAccessType access, Oid classId, Oid objectId,
							int subId, void *arg)
{
	Relation	rel;

	if (access == OAT_DROP && classId == RelationRelationId)
	{
		ObjectAccessDrop *drop_arg = (ObjectAccessDrop *) arg;

		if (subId != 0)
			return;

#ifdef USE_ASSERT_CHECKING
		{
			LOCKTAG		locktag;

			memset(&locktag, 0, sizeof(LOCKTAG));
			SET_LOCKTAG_RELATION(locktag, MyDatabaseId, objectId);

			Assert(DoLocalLockExist(&locktag));
		}
#endif

		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL)
		{
			bool		is_open = true;

			if (rel->rd_rel->relkind == RELKIND_RELATION
				&& is_orioledb_rel(rel))
			{
				CommitSeqNo csn;
				OXid		oxid;
				OTable	   *table;
				ORelOids   *treeOids;
				int			numTreeOids;
				ORelOids	oids = {MyDatabaseId, objectId,
				rel->rd_node.relNode};

				fill_current_oxid_csn(&oxid, &csn);
				Assert(relation_get_descr(rel) != NULL);

				table = o_tables_drop_by_oids(oids, oxid, csn);
				treeOids = o_table_make_index_oids(table, &numTreeOids);
				add_undo_drop_relnode(oids, treeOids, numTreeOids);
				pfree(treeOids);
				o_table_free(table);
			}
			else if (rel->rd_rel->relkind == RELKIND_INDEX &&
					 drop_arg->dropflags == 0)

				/*
				 * dropflags == PERFORM_DELETION_OF_RELATION also ignored, to
				 * not drop indices when whole table dropped
				 */
			{
				Relation	tbl = relation_open(rel->rd_index->indrelid,
												AccessShareLock);

				if (tbl->rd_rel->relkind == RELKIND_RELATION &&
					is_orioledb_rel(tbl))
				{
					OIndexNumber ix_num;
					OTableDescr *descr = relation_get_descr(tbl);

					Assert(descr != NULL);
					ix_num = o_find_ix_num_by_name(descr, rel->rd_rel->relname.data);
					if (ix_num != InvalidIndexNumber)
					{
						OIndexNumber ix_num;
						OTableDescr *descr = relation_get_descr(tbl);

						Assert(descr != NULL);
						ix_num = o_find_ix_num_by_name(descr, rel->rd_rel->relname.data);
						Assert(ix_num != InvalidIndexNumber);
						if (descr->indices[ix_num]->primaryIsCtid)
							ix_num--;
						relation_close(rel, AccessShareLock);
						is_open = false;

						o_index_drop(tbl, ix_num);
					}
				}
				relation_close(tbl, AccessShareLock);
			}
			if (is_open)
				relation_close(rel, AccessShareLock);
		}
	}
	else if (access == OAT_DROP && classId == DatabaseRelationId)
	{
		CommitSeqNo csn;
		OXid		oxid;

		Assert(OidIsValid(objectId));

		fill_current_oxid_csn(&oxid, &csn);

		o_tables_drop_all(oxid, csn, objectId);
	}
	else if (access == OAT_DROP && classId == TypeRelationId &&
			 ActiveSnapshotSet())
	{
		CommitSeqNo csn;
		OXid		oxid;
		Form_pg_type typeform;
		HeapTuple	tuple = NULL;

		Assert(OidIsValid(objectId));

		fill_current_oxid_csn(&oxid, &csn);

		o_tables_drop_columns_by_type(oxid, csn, objectId);

		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
		Assert(tuple);
		typeform = (Form_pg_type) GETSTRUCT(tuple);

		switch (typeform->typtype)
		{
			case TYPTYPE_COMPOSITE:
				if (typeform->typtypmod == -1)
				{
					o_record_cache_delete(MyDatabaseId, typeform->oid);
				}
				break;
			case TYPTYPE_RANGE:
				o_range_cache_delete(MyDatabaseId, typeform->oid);
				break;
			case TYPTYPE_ENUM:
				o_enum_cache_delete(MyDatabaseId, typeform->oid);
				break;
		}
		if (typeform->typtype != TYPTYPE_BASE &&
			typeform->typtype != TYPTYPE_PSEUDO)
			o_type_element_cache_delete(MyDatabaseId, typeform->oid);
		if (tuple != NULL)
			ReleaseSysCache(tuple);
	}
	else if (access == OAT_POST_CREATE && classId == RelationRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL)
		{
			if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
				o_find_composite_type_dependencies(rel->rd_rel->reltype,
												   rel);
			relation_close(rel, AccessShareLock);
		}
	}
	else if (access == OAT_POST_ALTER && classId == RelationRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL)
		{
			if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
				o_find_composite_type_dependencies(rel->rd_rel->reltype,
												   rel);
			relation_close(rel, AccessShareLock);
		}
	}

	if (old_objectaccess_hook)
		old_objectaccess_hook(access, classId, objectId, subId, arg);
}
