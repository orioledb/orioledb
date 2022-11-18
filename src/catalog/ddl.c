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
#include "catalog/o_tables.h"
#include "catalog/o_sys_cache.h"
#include "tableam/toast.h"
#include "transam/oxid.h"
#include "utils/compress.h"
#include "recovery/wal.h"

#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#if PG_VERSION_NUM >= 140000
#include "access/toast_compression.h"
#endif
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/tablespace.h"
#include "commands/view.h"
#include "commands/tablecmds.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/smgr.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rls.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static object_access_hook_type old_objectaccess_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;

UndoLocation	saved_undo_location = InvalidUndoLocation;
static List	   *saved_undo_locations = NIL; /* list of UndoLocation* */
static bool		isTopLevel PG_USED_FOR_ASSERTS_ONLY = false;

bool	alter_column_reuse = false;

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

static void orioledb_ExecutorStart_hook(QueryDesc *queryDesc, int eflags);
static void orioledb_ExecutorEnd_hook(QueryDesc *queryDesc);
static void orioledb_ExecutorRun_hook(QueryDesc *queryDesc,
									  ScanDirection direction,
									  uint64 count,
									  bool execute_once);
static bool o_intorel_receive(TupleTableSlot *slot, DestReceiver *self);
static ObjectAddress o_define_relation(CreateStmt *cstmt, char relkind,
									   const char *queryString);
static DestReceiver *OCreateIntoRelDestReceiver(IntoClause *intoClause);

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	IntoClause *into;			/* target relation specification */
	/* These fields are filled by intorel_startup: */
	Relation	rel;			/* relation to write to */
	ObjectAddress reladdr;		/* address of rel, for ExecCreateTableAs */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
	EState	   *estate;
} o_data_receiver;

void
orioledb_setup_ddl_hooks(void)
{
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = orioledb_utility_command;
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = orioledb_ExecutorRun_hook;
	old_objectaccess_hook = object_access_hook;
	object_access_hook = orioledb_object_access_hook;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = orioledb_ExecutorStart_hook;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = orioledb_ExecutorEnd_hook;
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

static void
orioledb_ExecutorStart_hook(QueryDesc *queryDesc, int eflags)
{
	UndoLocation		lastUsedLocation;
	UndoLocation	   *cur_undo_location;
	MemoryContext		oldcxt;

#ifdef USE_ASSERT_CHECKING
	{
		uint32	depth;
		bool	top_level = isTopLevel;

		isTopLevel = false;
		depth = DatumGetUInt32(DirectFunctionCall1(pg_trigger_depth,
												   (Datum) 0));
		if (top_level && depth == 0)
			Assert(saved_undo_locations == NIL &&
				   saved_undo_location == InvalidUndoLocation);
	}
#endif

	lastUsedLocation = pg_atomic_read_u64(&undo_meta->lastUsedLocation);
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	cur_undo_location = (UndoLocation *) palloc0(sizeof(UndoLocation));
	saved_undo_location = lastUsedLocation;
	*cur_undo_location = lastUsedLocation;
	saved_undo_locations = lappend(saved_undo_locations, cur_undo_location);
	MemoryContextSwitchTo(oldcxt);
	if (prev_ExecutorStart)
		return prev_ExecutorStart(queryDesc, eflags);
	else
		return standard_ExecutorStart(queryDesc, eflags);
}

static void
orioledb_ExecutorEnd_hook(QueryDesc *queryDesc)
{
	ListCell   *last;

	last = list_tail(saved_undo_locations);
	pfree(lfirst(last));
	saved_undo_locations = list_delete_last(saved_undo_locations);
	if (saved_undo_locations != NIL)
	{
		last = list_tail(saved_undo_locations);
		saved_undo_location = *(UndoLocation *) lfirst(last);
	}
	else
		saved_undo_location = InvalidUndoLocation;

	if (prev_ExecutorEnd)
		return prev_ExecutorEnd(queryDesc);
	else
		return standard_ExecutorEnd(queryDesc);
}

void
cleanup_saved_undo_locations()
{
	while (saved_undo_locations != NIL)
	{
		ListCell   *last;

		last = list_tail(saved_undo_locations);
		pfree(lfirst(last));
		saved_undo_locations = list_delete_last(saved_undo_locations);
	}
	saved_undo_location = InvalidUndoLocation;
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

/*
 * Common RangeVarGetRelid callback for rename, set schema, and alter table
 * processing.
 */
static void
RangeVarCallbackForAlterRelation(const RangeVar *rv, Oid relid, Oid oldrelid,
								 void *arg)
{
	Node	   *stmt = (Node *) arg;
	ObjectType	reltype;
	HeapTuple	tuple;
	Form_pg_class classform;
	AclResult	aclresult;
	char		relkind;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return;					/* concurrently dropped */
	classform = (Form_pg_class) GETSTRUCT(tuple);
	relkind = classform->relkind;

	/* Must own relation. */
	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relid)), rv->relname);

	/* No system table modifications unless explicitly allowed. */
	if (!allowSystemTableMods && IsSystemClass(relid, classform))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						rv->relname)));

	/*
	 * Extract the specified relation type from the statement parse tree.
	 *
	 * Also, for ALTER .. RENAME, check permissions: the user must (still)
	 * have CREATE rights on the containing namespace.
	 */
	if (IsA(stmt, RenameStmt))
	{
		aclresult = pg_namespace_aclcheck(classform->relnamespace,
										  GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_SCHEMA,
						   get_namespace_name(classform->relnamespace));
		reltype = ((RenameStmt *) stmt)->renameType;
	}
	else if (IsA(stmt, AlterObjectSchemaStmt))
		reltype = ((AlterObjectSchemaStmt *) stmt)->objectType;

	else if (IsA(stmt, AlterTableStmt))
		reltype = ((AlterTableStmt *) stmt)->objtype;
	else
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(stmt));
		reltype = OBJECT_TABLE; /* placate compiler */
	}

	/*
	 * For compatibility with prior releases, we allow ALTER TABLE to be used
	 * with most other types of relations (but not composite types). We allow
	 * similar flexibility for ALTER INDEX in the case of RENAME, but not
	 * otherwise.  Otherwise, the user must select the correct form of the
	 * command for the relation at issue.
	 */
	if (reltype == OBJECT_SEQUENCE && relkind != RELKIND_SEQUENCE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sequence", rv->relname)));

	if (reltype == OBJECT_VIEW && relkind != RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a view", rv->relname)));

	if (reltype == OBJECT_MATVIEW && relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a materialized view", rv->relname)));

	if (reltype == OBJECT_FOREIGN_TABLE && relkind != RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a foreign table", rv->relname)));

	if (reltype == OBJECT_TYPE && relkind != RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a composite type", rv->relname)));

	if (reltype == OBJECT_INDEX && relkind != RELKIND_INDEX &&
		relkind != RELKIND_PARTITIONED_INDEX
		&& !IsA(stmt, RenameStmt))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index", rv->relname)));

	/*
	 * Don't allow ALTER TABLE on composite types. We want people to use ALTER
	 * TYPE for that.
	 */
	if (reltype != OBJECT_TYPE && relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a composite type", rv->relname),
				 errhint("Use ALTER TYPE instead.")));

	/*
	 * Don't allow ALTER TABLE .. SET SCHEMA on relations that can't be moved
	 * to a different schema, such as indexes and TOAST tables.
	 */
	if (IsA(stmt, AlterObjectSchemaStmt) &&
		relkind != RELKIND_RELATION &&
		relkind != RELKIND_VIEW &&
		relkind != RELKIND_MATVIEW &&
		relkind != RELKIND_SEQUENCE &&
		relkind != RELKIND_FOREIGN_TABLE &&
		relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table, view, materialized view, sequence, or foreign table",
						rv->relname)));

	ReleaseSysCache(tuple);
}

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

#ifdef USE_ASSERT_CHECKING
	isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);
#endif

#if PG_VERSION_NUM >= 140000
	/* copied from standard_ProcessUtility */
	if (readOnlyTree)
		pstmt = copyObject(pstmt);
#endif

	if (IsA(pstmt->utilityStmt, AlterTableStmt) &&
		!is_alter_table_partition(pstmt))
	{
		AlterTableStmt	   *atstmt = (AlterTableStmt *) pstmt->utilityStmt;
		Oid					relid;
		LOCKMODE			lockmode;

		/*
		 * Figure out lock mode, and acquire lock.  This also does
		 * basic permissions checks, so that we won't wait for a
		 * lock on (for example) a relation on which we have no
		 * permissions.
		 */
		lockmode = AlterTableGetLockLevel(atstmt->cmds);
		relid = AlterTableLookupRelation(atstmt, lockmode);

		if (OidIsValid(relid) &&
			atstmt->objtype == OBJECT_TABLE &&
			lockmode == AccessExclusiveLock)
		{
			Relation rel = table_open(relid, lockmode);
			if (is_orioledb_rel(rel))
			{
				ListCell   *lc;

				foreach(lc, atstmt->cmds)
				{
					AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

					/* make checks */
					switch (cmd->subtype)
					{
					case AT_AlterColumnType:
					case AT_AddIndex:
					case AT_AddColumn:
					case AT_DropColumn:
					case AT_ColumnDefault:
					case AT_AddConstraint:
					case AT_DropConstraint:
					case AT_GenericOptions:
					case AT_SetNotNull:
					case AT_ChangeOwner:
					case AT_DropNotNull:
					case AT_AddInherit:
					case AT_DropInherit:
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("unsupported alter table subcommand")),
								errdetail("Subcommand \"%s\" is not "
											"supported on OrioleDB tables yet. "
											"Please send a bug report.",
											deparse_alter_table_cmd_subtype(cmd)));
						break;
					}
				}
			}
			table_close(rel, lockmode);
		}
	}
	else if (IsA(pstmt->utilityStmt, CreateStmt))
	{
		/*
		 * copy-paste with new_o_tables list from ProcessUtilitySlow in
		 * utility.c
		 */
		List	   *stmts;
		RangeVar   *table_rv = NULL;
		bool		isCompleteQuery = (context <= PROCESS_UTILITY_QUERY);
		bool		needCleanup;

		needCleanup = isCompleteQuery && EventTriggerBeginCompleteQuery();

		/* PG_TRY block is to ensure we call EventTriggerEndCompleteQuery */
		PG_TRY();
		{
			if (isCompleteQuery)
				EventTriggerDDLCommandStart(pstmt->utilityStmt);

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

				if (IsA(stmt, CreateStmt))
				{
					CreateStmt *cstmt = (CreateStmt *) stmt;

					/* Remember transformed RangeVar for LIKE */
					table_rv = cstmt->relation;

					o_define_relation(cstmt, RELKIND_RELATION, queryString);
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
		}
		PG_FINALLY();
		{
			if (needCleanup)
				EventTriggerEndCompleteQuery();
		}
		PG_END_TRY();
		call_next = false;
	}
	else if (IsA(pstmt->utilityStmt, RenameStmt))
	{
		RenameStmt *stmt = (RenameStmt *) pstmt->utilityStmt;

		if (stmt->renameType == OBJECT_INDEX)
		{
			bool is_index_stmt = stmt->renameType == OBJECT_INDEX;
			Oid relid;
			Relation idx;

			/*
			 * Grab an exclusive lock on the target table, index, sequence, view,
			 * materialized view, or foreign table, which we will NOT release until
			 * end of transaction.
			 *
			 * Lock level used here should match RenameRelationInternal, to avoid lock
			 * escalation.  However, because ALTER INDEX can be used with any relation
			 * type, we mustn't believe without verification.
			 */
			for (;;)
			{
				LOCKMODE lockmode;
				char relkind;
				bool obj_is_index;

				lockmode = is_index_stmt ? ShareUpdateExclusiveLock : AccessExclusiveLock;

				relid = RangeVarGetRelidExtended(stmt->relation, lockmode,
												 stmt->missing_ok ? RVR_MISSING_OK : 0,
												 RangeVarCallbackForAlterRelation,
												 (void *)stmt);

				if (!OidIsValid(relid))
				{
					ereport(NOTICE,
							(errmsg("relation \"%s\" does not exist, skipping",
									stmt->relation->relname)));
					return;
				}

				/*
				 * We allow mismatched statement and object types (e.g., ALTER INDEX
				 * to rename a table), but we might've used the wrong lock level.  If
				 * that happens, retry with the correct lock level.  We don't bother
				 * if we already acquired AccessExclusiveLock with an index, however.
				 */
				relkind = get_rel_relkind(relid);
				obj_is_index = (relkind == RELKIND_INDEX ||
								relkind == RELKIND_PARTITIONED_INDEX);
				if (obj_is_index || is_index_stmt == obj_is_index)
					break;

				UnlockRelationOid(relid, lockmode);
				is_index_stmt = obj_is_index;
			}
			idx = relation_openrv(stmt->relation, AccessExclusiveLock);
			if (idx->rd_rel->relkind == RELKIND_INDEX)
			{
				Relation	tbl = relation_open(idx->rd_index->indrelid,
												AccessShareLock);

				if ((tbl->rd_rel->relkind == RELKIND_RELATION ||
					 tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
					is_orioledb_rel(tbl))
				{
					OTable	   *o_table;
					ORelOids	table_oids = {MyDatabaseId, tbl->rd_rel->oid,
											  tbl->rd_node.relNode};

					o_table = o_tables_get(table_oids);
					if (o_table == NULL)
					{
						elog(NOTICE, "orioledb table %s not found",
							 RelationGetRelationName(tbl));
					}
					else
					{
						int			ix_num;
						CommitSeqNo csn;
						OXid		oxid;
						ORelOids	idx_oids = {MyDatabaseId, idx->rd_rel->oid,
												idx->rd_node.relNode};

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
			}
			relation_close(idx, AccessExclusiveLock);
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
}

static void
orioledb_ExecutorRun_hook(QueryDesc *queryDesc,
						  ScanDirection direction,
						  uint64 count,
						  bool execute_once)
{
	if (queryDesc->dest->mydest == DestIntoRel)
	{
		/* "into" has same offset in o_data_receiver as in DR_intorel */
		IntoClause	   *into = ((o_data_receiver *) queryDesc->dest)->into;
		bool			orioledb;

		if (into->accessMethod)
			orioledb = (strcmp(into->accessMethod, "orioledb") == 0);
		else
			orioledb = (strcmp(default_table_access_method, "orioledb") == 0);
		if (orioledb && queryDesc->dest->receiveSlot != o_intorel_receive)
		{
			pfree(queryDesc->dest);
			queryDesc->dest = OCreateIntoRelDestReceiver(into);
		}
	}
	if (prev_ExecutorRun_hook)
		(*prev_ExecutorRun_hook) (queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);
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

		if ((rel->rd_rel->relkind == RELKIND_RELATION ||
			 rel->rd_rel->relkind == RELKIND_MATVIEW) &&
			is_orioledb_rel(rel))
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

			if ((rel->rd_rel->relkind == RELKIND_RELATION ||
				 rel->rd_rel->relkind == RELKIND_MATVIEW) &&
				(subId == 0) && is_orioledb_rel(rel))
			{
				CommitSeqNo csn;
				OXid		oxid;
				OTable	   *table;
				ORelOids   *treeOids;
				int			numTreeOids;
				ORelOids	oids = {MyDatabaseId,
									objectId,
									rel->rd_node.relNode};

				fill_current_oxid_csn(&oxid, &csn);
				Assert(relation_get_descr(rel) != NULL);

				table = o_tables_drop_by_oids(oids, oxid, csn);
				treeOids = o_table_make_index_oids(table, &numTreeOids);
				add_undo_drop_relnode(oids, treeOids, numTreeOids);
				pfree(treeOids);
				o_table_free(table);
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId != 0) && is_orioledb_rel(rel))
			{
				OTable	   *o_table;
				OTableField *o_field = NULL;
				ORelOids	oids = {MyDatabaseId, rel->rd_rel->oid,
									rel->rd_node.relNode};

				o_table = o_tables_get(oids);
				if (o_table == NULL)
				{
					/* table does not exist */
					elog(NOTICE, "orioledb table \"%s\" not found",
						RelationGetRelationName(rel));
				}
				else
				{
					o_field = &o_table->fields[subId - 1];

					if (o_field && !o_field->droped)
					{
						CommitSeqNo	csn;
						OXid		oxid;

						o_field->droped = true;

						fill_current_oxid_csn(&oxid, &csn);
						o_tables_update(o_table, oxid, csn);
						o_tables_after_update(o_table, oxid, csn);
					}
					o_table_free(o_table);
				}
			}
			else if (rel->rd_rel->relkind == RELKIND_INDEX &&
					 drop_arg->dropflags != PERFORM_DELETION_OF_RELATION)
			{
				/*
				 * dropflags == PERFORM_DELETION_OF_RELATION ignored, to
				 * not drop indices when whole table dropped
				 */
				Relation	tbl = relation_open(rel->rd_index->indrelid,
												AccessShareLock);

				if ((tbl->rd_rel->relkind == RELKIND_RELATION ||
					 tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
					is_orioledb_rel(tbl))
				{
					OIndexNumber ix_num;
					OTableDescr *descr = relation_get_descr(tbl);

					Assert(descr != NULL);
					ix_num = o_find_ix_num_by_name(descr, rel->rd_rel->relname.data);
					if (ix_num != InvalidIndexNumber)
					{
						if (descr->indices[ix_num]->primaryIsCtid)
							ix_num--;
						relation_close(rel, AccessShareLock);
						is_open = false;

						if (!alter_column_reuse)
							o_index_drop(tbl, ix_num);
						alter_column_reuse = false;
					}
				}
				relation_close(tbl, AccessShareLock);
			}
			else if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE &&
					 (subId != 0))
			{
				OClassArg	arg = {.column_drop = true,.dropped = subId};

				o_find_composite_type_dependencies(rel->rd_rel->reltype, rel);
				CommandCounterIncrement();
				o_class_cache_update_if_needed(MyDatabaseId, rel->rd_rel->oid,
											   (Pointer) &arg);
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
					o_class_cache_delete(MyDatabaseId, typeform->typrelid);
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
			o_type_cache_delete(MyDatabaseId, typeform->oid);
		if (tuple != NULL)
			ReleaseSysCache(tuple);
	}
	else if (access == OAT_POST_CREATE && classId == RelationRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL)
		{
			if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
			{
				o_find_composite_type_dependencies(rel->rd_rel->reltype, rel);
				CommandCounterIncrement();
				o_class_cache_update_if_needed(MyDatabaseId, rel->rd_rel->oid,
											   NULL);
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId != 0) && is_orioledb_rel(rel))
			{
				OTableField			   *field;
				Form_pg_attribute		attr;
				OTable				   *o_table;
				ORelOids				oids = {MyDatabaseId,
												rel->rd_rel->oid,
												rel->rd_node.relNode};
				CommitSeqNo				csn;
				OXid					oxid;

				o_table = o_tables_get(oids);
				if (o_table == NULL)
				{
					/* table does not exist */
					elog(NOTICE, "orioledb table \"%s\" not found", RelationGetRelationName(rel));
				}
				else
				{
					fill_current_oxid_csn(&oxid, &csn);

					o_table->nfields++;
					o_table->fields = repalloc(o_table->fields,
											   o_table->nfields *
												   sizeof(OTableField));
					memset(&o_table->fields[o_table->nfields - 1], 0,
						sizeof(OTableField));

					CommandCounterIncrement();
					field = &o_table->fields[o_table->nfields - 1];
					attr = &rel->rd_att->attrs[rel->rd_att->natts - 1];
					orioledb_attr_to_field(field, attr);

					o_table_resize_constr(o_table);

					o_tables_update(o_table, oxid, csn);
					o_tables_after_update(o_table, oxid, csn);
					o_table_free(o_table);
				}
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId == 0) && is_orioledb_rel(rel))
			{
				ORelOids	oids;
				TupleDesc	tupdesc;
				OTable	   *o_table;
				CommitSeqNo	csn = COMMITSEQNO_INPROGRESS;
				OXid		oxid = InvalidOXid;

				fill_current_oxid_csn(&oxid, &csn);

				Assert(rel->rd_node.dbNode == MyDatabaseId);
				oids.datoid = MyDatabaseId;
				oids.reloid = rel->rd_id;
				oids.relnode = rel->rd_node.relNode;
				tupdesc = RelationGetDescr(rel);

				o_table = o_table_tableam_create(oids, tupdesc);
				o_opclass_cache_add_table(o_table);
				o_tables_add(o_table, oxid, csn);
			}
			else if ((rel->rd_rel->relkind == RELKIND_TOASTVALUE) &&
					 (subId == 0))
			{
				Oid			tbl_oid;
				Relation	tbl = NULL;

				/* This is faster than dependency scan */
				tbl_oid = pg_strtouint64(strrchr(rel->rd_rel->relname.data,
												 '_') + 1, NULL, 0);

				tbl = table_open(tbl_oid, AccessShareLock);
				if (tbl && is_orioledb_rel(tbl))
				{
					ORelOids	oids,
								toastOids,
							*treeOids;
					OTable	   *o_table;
					int			numTreeOids;
					CommitSeqNo	csn;
					OXid		oxid;

					Assert(tbl->rd_node.dbNode == MyDatabaseId);
					oids.datoid = MyDatabaseId;
					oids.reloid = tbl->rd_id;
					oids.relnode = tbl->rd_node.relNode;
					toastOids.datoid = MyDatabaseId;
					toastOids.reloid = rel->rd_id;
					toastOids.relnode = rel->rd_node.relNode;

					o_table = o_tables_get(oids);
					o_table->toast_oids = toastOids;
					o_table->toast_compress = InvalidOCompress;

					fill_current_oxid_csn(&oxid, &csn);
					o_tables_update(o_table, oxid, csn);
					o_tables_after_update(o_table, oxid, csn);

					treeOids = o_table_make_index_oids(o_table, &numTreeOids);
					add_undo_create_relnode(oids, treeOids, numTreeOids);
					pfree(treeOids);
				}
				if (tbl)
					table_close(tbl, AccessShareLock);
			}
			relation_close(rel, AccessShareLock);
		}
	}
	else if (access == OAT_POST_CREATE && classId == AttrDefaultRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL && (rel->rd_rel->relkind == RELKIND_RELATION) &&
			(subId != 0) && is_orioledb_rel(rel))
		{
			OTableField				   *field;
			Form_pg_attribute			attr;
			OTable					   *o_table;
			ORelOids					oids = {MyDatabaseId,
												rel->rd_rel->oid,
												rel->rd_node.relNode};
			Node					   *defaultexpr;
			AttrMissing				   *attrmiss = NULL;
			AttrMissing					attrmiss_temp;
			CommitSeqNo					csn;
			OXid						oxid;

			o_table = o_tables_get(oids);
			if (o_table == NULL)
			{
				/* table does not exist */
				elog(NOTICE, "orioledb table \"%s\" not found",
					 RelationGetRelationName(rel));
			}
			else
			{
				bool missing_before;

				fill_current_oxid_csn(&oxid, &csn);
				attr = &rel->rd_att->attrs[subId - 1];
				missing_before = attr->atthasmissing;
				CommandCounterIncrement();
				field = &o_table->fields[subId - 1];
				attr = &rel->rd_att->attrs[subId - 1];
				orioledb_attr_to_field(field, attr);

				if (attr->atthasdef)
					defaultexpr = build_column_default(rel, attr->attnum);
				else
					defaultexpr = NULL;

				if (!missing_before && attr->atthasmissing)
				{
					Datum					missingval;
					Expr				   *expr2;
					ParseNamespaceItem	   *nsitem;
					ParseState			   *pstate;
					EState				   *estate = NULL;
					ExprContext			   *econtext;
					ExprState			   *exprState;
					MemoryContext			tbl_cxt;
					MemoryContext			oldcxt;
					bool					missingIsNull = true;

					pstate = make_parsestate(NULL);
					pstate->p_sourcetext = NULL;
					nsitem = addRangeTableEntryForRelation(pstate,
															rel,
															AccessShareLock,
															NULL,
															false,
															true);
					addNSItemToQuery(pstate, nsitem, true, true, true);

					expr2 = expression_planner((Expr *) defaultexpr);

					tbl_cxt = OGetTableContext(o_table);
					oldcxt = MemoryContextSwitchTo(tbl_cxt);
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
					defaultexpr = (Node *) expr2;
				}
				o_table_fill_constr(o_table, subId - 1, attrmiss,
									(Expr *) defaultexpr);

				o_tables_update(o_table, oxid, csn);
				o_tables_after_update(o_table, oxid, csn);
				o_table_free(o_table);
			}
		}
		relation_close(rel, AccessShareLock);
	}
	else if (access == OAT_POST_ALTER && classId == RelationRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL)
		{
			if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
			{
				o_find_composite_type_dependencies(rel->rd_rel->reltype, rel);
				CommandCounterIncrement();
				o_class_cache_update_if_needed(MyDatabaseId, rel->rd_rel->oid,
											   NULL);
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId != 0) && is_orioledb_rel(rel))
			{
				OTable *o_table;
				ORelOids oids = {MyDatabaseId,
								 rel->rd_rel->oid,
								 rel->rd_node.relNode};

				o_table = o_tables_get(oids);
				if (o_table == NULL)
				{
					/* table does not exist */
					elog(NOTICE, "orioledb table \"%s\" not found",
						 RelationGetRelationName(rel));
				}
				else
				{
					OTableField			old_field;
					OTableField		   *field;
					Form_pg_attribute	attr;
					bool				rewrite;
					CommitSeqNo			csn;
					OXid				oxid;
					int					ix_num;

					old_field = o_table->fields[subId - 1];
					CommandCounterIncrement();
					field = &o_table->fields[subId - 1];
					attr = &rel->rd_att->attrs[subId - 1];
					orioledb_attr_to_field(field, attr);

					rewrite = !can_coerce_type(1, &old_field.typid,
											   &field->typid,
											   COERCION_ASSIGNMENT);

					if (!rewrite)
					{
						alter_column_reuse = old_field.typid ==
												 field->typid &&
											 old_field.collation ==
												 field->collation;

						fill_current_oxid_csn(&oxid, &csn);
						o_tables_update(o_table, oxid, csn);
						o_tables_after_update(o_table, oxid, csn);
						for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
						{
							int				field_num;
							int				ctid_off;
							OTableIndex	   *index;

							ctid_off = o_table->has_primary ? 0 : 1;
							index = &o_table->indices[ix_num];

							for (field_num = 0; field_num < index->nfields;
								 field_num++)
							{
								if ((index->type == oIndexPrimary) ||
									(index->fields[field_num].attnum ==
									 subId - 1))
								{
									o_indices_update(o_table,
													 ix_num + ctid_off,
													 oxid, csn);
									o_invalidate_oids(index->oids);
									o_add_invalidate_undo_item(
										index->oids,
										O_INVALIDATE_OIDS_ON_ABORT);
									break;
								}
							}
						}
					}
					o_table_free(o_table);
				}
			}
			else if (rel->rd_rel->relkind == RELKIND_RELATION &&
					 OidIsValid(rel->rd_rel->relrewrite) &&
					 (subId == 0) && is_orioledb_rel(rel))
			{
				Relation	old_rel;
				ORelOids	old_oids,
							new_oids;
				OTable	   *old_o_table,
						   *new_o_table;
				CommitSeqNo	csn;
				OXid		oxid;

				old_rel = relation_open(rel->rd_rel->relrewrite, NoLock);

				old_oids.datoid = MyDatabaseId;
				old_oids.reloid = old_rel->rd_id;
				old_oids.relnode = old_rel->rd_node.relNode;
				old_o_table = o_tables_get(old_oids);
				if (old_o_table == NULL)
				{
					/* it does not exist */
					elog(ERROR, "orioledb table \"%s\" not found",
						RelationGetRelationName(old_rel));
				}

				new_oids.datoid = MyDatabaseId;
				new_oids.reloid = rel->rd_id;
				new_oids.relnode = rel->rd_node.relNode;
				new_o_table = o_tables_get(new_oids);
				if (new_o_table == NULL)
				{
					/* it does not exist */
					elog(ERROR, "orioledb table \"%s\" not found",
						RelationGetRelationName(rel));
				}


				LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);
				fill_current_oxid_csn(&oxid, &csn);
				o_tables_drop_by_oids(old_oids, oxid, csn);
				o_tables_swap_relnodes(old_o_table, new_o_table);
				o_tables_add(old_o_table, oxid, csn);
				o_tables_update_without_oids_indexes(new_o_table, oxid, csn);
				o_indices_update(new_o_table, TOASTIndexNumber, oxid, csn);

				add_invalidate_wal_record(new_o_table->oids, new_oids.relnode);
				LWLockRelease(&checkpoint_state->oTablesAddLock);

				o_table_free(old_o_table);
				o_table_free(new_o_table);
				orioledb_free_rd_amcache(old_rel);
				orioledb_free_rd_amcache(rel);
				relation_close(old_rel, NoLock);
			}
			relation_close(rel, AccessShareLock);
		}
	}
	else if (access == OAT_POST_ALTER && classId == TypeRelationId)
	{
		HeapTuple	typeTuple;
		Form_pg_type tform;

		typeTuple = typeidType(objectId);

		tform = (Form_pg_type) GETSTRUCT(typeTuple);

		switch (tform->typtype)
		{
			case TYPTYPE_ENUM:
				CommandCounterIncrement();
				o_enum_cache_update_if_needed(MyDatabaseId, objectId, NULL);
				break;

			case TYPTYPE_COMPOSITE:
				rel = relation_open(typeidTypeRelid(objectId), AccessShareLock);
				o_find_composite_type_dependencies(objectId, rel);
				relation_close(rel, AccessShareLock);
				CommandCounterIncrement();
				o_class_cache_update_if_needed(MyDatabaseId, rel->rd_rel->oid,
											   NULL);
				break;

			default:
				break;
		}
		ReleaseSysCache(typeTuple);
	}
	else if (access == OAT_DROP && classId == OperatorClassRelationId)
	{
		OOpclass   *o_opclass = o_opclass_get(objectId);

		if (o_opclass)
			o_invalidate_comparator_cache(o_opclass->opfamily,
										  o_opclass->inputtype,
										  o_opclass->inputtype);
	}

	if (old_objectaccess_hook)
		old_objectaccess_hook(access, classId, objectId, subId, arg);
}

static ObjectAddress
o_define_relation(CreateStmt *cstmt, char relkind, const char *queryString)
{
	ObjectAddress address;
	ObjectAddress secondaryObject = InvalidObjectAddress;
	OCompress	compress = default_compress,
				primary_compress = default_primary_compress,
				toast_compress = default_toast_compress;
	Datum		toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	bool		orioledb;
	CommitSeqNo csn = COMMITSEQNO_INPROGRESS;
	OXid		oxid = InvalidOXid;
	int 		old_max_parallel_maintenance_workers;

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
		validate_compress(compress, "Default");
		validate_compress(primary_compress, "Primary index");
		validate_compress(toast_compress, "TOAST");
		LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);
	}

	/* Create the table itself */
	address = DefineRelation(cstmt, relkind, InvalidOid, NULL,
							 queryString);
	EventTriggerCollectSimpleCommand(address, secondaryObject, (Node *) cstmt);

	/*
	 * Let NewRelationCreateToastTable decide if this one needs a secondary
	 * relation too.
	 */
	CommandCounterIncrement();

	/*
	 * parse and validate reloptions for the toast table
	 */
	toast_options = transformRelOptions((Datum) 0,
										cstmt->options,
										"toast",
										validnsps,
										true,
										false);
	(void) heap_reloptions(RELKIND_TOASTVALUE,
						   toast_options,
						   true);

	if (orioledb)
	{
		old_max_parallel_maintenance_workers =
			max_parallel_maintenance_workers;
		max_parallel_maintenance_workers = 0;
	}
	NewRelationCreateToastTable(address.objectId,
								toast_options);
	if (orioledb)
		max_parallel_maintenance_workers =
			old_max_parallel_maintenance_workers;

	/*
	 * orioledb table have no need in PostgreSQL TOAST and this calls have no
	 * sense for us (see needs_toast_table(rel) check inside
	 * create_toast_table()), but call NewRelationCreateToastTable() always
	 * gets AccessExclusiveLock on the relation. So we just skip it.
	 */

	if (!OXidIsValid(oxid))
		fill_current_oxid_csn(&oxid, &csn);

	if (orioledb)
	{
		Relation	rel;
		ORelOids	oids,
				   *treeOids;
		OTable	   *o_table;
		int			numTreeOids;

		rel = table_open(address.objectId, AccessShareLock);
		Assert(rel->rd_node.dbNode == MyDatabaseId);
		oids.datoid = MyDatabaseId;
		oids.reloid = rel->rd_id;
		oids.relnode = rel->rd_node.relNode;
		o_table = o_tables_get(oids);
		Assert(o_table);

		if (OCompressIsValid(compress))
		{
			if (!OCompressIsValid(primary_compress))
				primary_compress = compress;
			if (!OCompressIsValid(toast_compress))
				toast_compress = compress;
		}
		o_table->default_compress = compress;
		o_table->toast_compress = toast_compress;
		o_table->primary_compress = primary_compress;

		o_indices_update(o_table, TOASTIndexNumber, oxid, csn);
		o_tables_update(o_table, oxid, csn);
		o_tables_after_update(o_table, oxid, csn);
		LWLockRelease(&checkpoint_state->oTablesAddLock);

		treeOids = o_table_make_index_oids(o_table, &numTreeOids);
		add_undo_create_relnode(oids, treeOids, numTreeOids);
		pfree(treeOids);

		table_close(rel, AccessShareLock);
	}

	return address;
}

/*
 * o_create_ctas_internal
 *
 * Internal utility used for the creation of the definition of a relation
 * created via CREATE TABLE AS or a materialized view.  Caller needs to
 * provide a list of attributes (ColumnDef nodes).
 */
static ObjectAddress
o_create_ctas_internal(List *attrList, IntoClause *into)
{
	CreateStmt *cstmt = makeNode(CreateStmt);
	bool		is_matview;
	char		relkind;
	ObjectAddress intoRelationAddr;

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	is_matview = (into->viewQuery != NULL);
	relkind = is_matview ? RELKIND_MATVIEW : RELKIND_RELATION;

	/*
	 * Create the target relation by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 */
	cstmt->relation = into->rel;
	cstmt->tableElts = attrList;
	cstmt->inhRelations = NIL;
	cstmt->ofTypename = NULL;
	cstmt->constraints = NIL;
	cstmt->options = into->options;
	cstmt->oncommit = into->onCommit;
	cstmt->tablespacename = into->tableSpaceName;
	cstmt->if_not_exists = false;
	cstmt->accessMethod = into->accessMethod;

	intoRelationAddr = o_define_relation(cstmt, relkind, NULL);

	/* Create the "view" part of a materialized view. */
	if (is_matview)
	{
		/* StoreViewQuery scribbles on tree, so make a copy */
		Query	   *query = (Query *) copyObject(into->viewQuery);

		StoreViewQuery(intoRelationAddr.objectId, query, false);
		CommandCounterIncrement();
	}

	return intoRelationAddr;
}

/*
 * o_intorel_startup --- executor startup
 */
static void
o_intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	o_data_receiver *myState = (o_data_receiver *) self;
	IntoClause *into = myState->into;
	bool		is_matview;
	List	   *attrList;
	ObjectAddress intoRelationAddr;
	Relation	intoRelationDesc;
	ListCell   *lc;
	int			attnum;

	Assert(into != NULL);		/* else somebody forgot to set it */

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	is_matview = (into->viewQuery != NULL);

	/*
	 * Build column definitions using "pre-cooked" type and collation info. If
	 * a column name list was specified in CREATE TABLE AS, override the
	 * column names derived from the query.  (Too few column names are OK, too
	 * many are not.)
	 */
	attrList = NIL;
	lc = list_head(into->colNames);
	for (attnum = 0; attnum < typeinfo->natts; attnum++)
	{
		Form_pg_attribute attribute = TupleDescAttr(typeinfo, attnum);
		ColumnDef  *col;
		char	   *colname;

		if (lc)
		{
			colname = strVal(lfirst(lc));
			lc = lnext(into->colNames, lc);
		}
		else
			colname = NameStr(attribute->attname);

		col = makeColumnDef(colname,
							attribute->atttypid,
							attribute->atttypmod,
							attribute->attcollation);

		/*
		 * It's possible that the column is of a collatable type but the
		 * collation could not be resolved, so double-check.  (We must check
		 * this here because DefineRelation would adopt the type's default
		 * collation rather than complaining.)
		 */
		if (!OidIsValid(col->collOid) &&
			type_is_collatable(col->typeName->typeOid))
			ereport(ERROR,
					(errcode(ERRCODE_INDETERMINATE_COLLATION),
					 errmsg("no collation was derived for column \"%s\" with collatable type %s",
							col->colname,
							format_type_be(col->typeName->typeOid)),
					 errhint("Use the COLLATE clause to set the collation explicitly.")));

		attrList = lappend(attrList, col);
	}

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/*
	 * Actually create the target table
	 */
	intoRelationAddr = o_create_ctas_internal(attrList, into);

	/*
	 * Finally we can open the target table
	 */
	intoRelationDesc = table_open(intoRelationAddr.objectId, AccessExclusiveLock);

	/*
	 * Make sure the constructed table does not have RLS enabled.
	 *
	 * check_enable_rls() will ereport(ERROR) itself if the user has requested
	 * something invalid, and otherwise will return RLS_ENABLED if RLS should
	 * be enabled here.  We don't actually support that currently, so throw
	 * our own ereport(ERROR) if that happens.
	 */
	if (check_enable_rls(intoRelationAddr.objectId, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("policies not yet implemented for this command")));

	/*
	 * Tentatively mark the target as populated, if it's a matview and we're
	 * going to fill it; otherwise, no change needed.
	 */
	if (is_matview && !into->skipData)
		SetMatViewPopulatedState(intoRelationDesc, true);

	/*
	 * Fill private fields of myState for use by later routines
	 */
	myState->rel = intoRelationDesc;
	myState->reladdr = intoRelationAddr;
	myState->output_cid = GetCurrentCommandId(true);
	myState->ti_options = TABLE_INSERT_SKIP_FSM;

	/*
	 * If WITH NO DATA is specified, there is no need to set up the state for
	 * bulk inserts as there are no tuples to insert.
	 */
	if (!into->skipData)
		myState->bistate = GetBulkInsertState();
	else
		myState->bistate = NULL;

	myState->estate = CreateExecutorState();

	/*
	 * Valid smgr_targblock implies something already wrote to the relation.
	 * This may be harmless, but this function hasn't planned for it.
	 */
	Assert(RelationGetTargetBlock(intoRelationDesc) == InvalidBlockNumber);
}

/*
 * o_intorel_receive --- receive one tuple
 */
static bool
o_intorel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	o_data_receiver *myState = (o_data_receiver *) self;

	/* Nothing to insert if WITH NO DATA is specified. */
	if (!myState->into->skipData)
	{
		/*
		 * Note that the input slot might not be of the type of the target
		 * relation. That's supported by table_tuple_insert(), but slightly
		 * less efficient than inserting with the right slot - but the
		 * alternative would be to copy into a slot of the right type, which
		 * would not be cheap either. This also doesn't allow accessing per-AM
		 * data (say a tuple's xmin), but since we don't do that here...
		 */
		table_tuple_insert(myState->rel, slot, myState->output_cid,
						   myState->ti_options, myState->bistate);
	}

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * o_intorel_shutdown --- executor end
 */
static void
o_intorel_shutdown(DestReceiver *self)
{
	o_data_receiver *myState = (o_data_receiver *) self;
	IntoClause *into = myState->into;

	if (!into->skipData)
	{
		FreeBulkInsertState(myState->bistate);
		table_finish_bulk_insert(myState->rel, myState->ti_options);
	}

	/* close rel, but keep lock until commit */
	table_close(myState->rel, NoLock);
	myState->rel = NULL;

	FreeExecutorState(myState->estate);
}

/*
 * o_intorel_destroy --- release DestReceiver object
 */
static void
o_intorel_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * OCreateIntoRelDestReceiver -- create a suitable DestReceiver object
 */
static DestReceiver *
OCreateIntoRelDestReceiver(IntoClause *intoClause)
{
	o_data_receiver *self;

	self = (o_data_receiver *) palloc0(sizeof(o_data_receiver));
	self->pub.receiveSlot = o_intorel_receive;
	self->pub.rStartup = o_intorel_startup;
	self->pub.rShutdown = o_intorel_shutdown;
	self->pub.rDestroy = o_intorel_destroy;
	self->pub.mydest = DestIntoRel;
	self->into = intoClause;
	/* other private fields will be set during intorel_startup */

	return (DestReceiver *) self;
}
