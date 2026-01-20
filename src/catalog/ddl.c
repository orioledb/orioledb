/*-------------------------------------------------------------------------
 *
 * ddl.c
 *		Routines for DDL handling.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/ddl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/scan.h"
#include "btree/undo.h"
#include "catalog/indices.h"
#include "catalog/o_indices.h"
#include "catalog/o_tables.h"
#include "catalog/o_sys_cache.h"
#include "storage/lockdefs.h"
#include "tableam/operations.h"
#include "catalog/pg_am.h"
#include "tableam/toast.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "tuple/slot.h"
#include "utils/compress.h"
#include "recovery/wal.h"

#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "commands/createas.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/tablespace.h"
#include "commands/vacuum.h"
#include "commands/view.h"
#include "commands/tablecmds.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "partitioning/partdesc.h"
#include "pgstat.h"
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

#include <sys/stat.h>
#include <unistd.h>

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static object_access_hook_type old_objectaccess_hook = NULL;

List	   *drop_index_list = NIL;
List	   *partition_drop_index_list = NIL;
static List *alter_type_exprs = NIL;
static List *o_alter_generated_column_id = NIL;
static List *dropped_attrs = NIL;
Oid			o_saved_relrewrite = InvalidOid;
Oid			o_saved_reltablespace = InvalidOid;
List	   *o_reuse_indices = NIL;
static ORelOids saved_oids;
static bool in_rewrite = false;
List	   *reindex_list = NIL;
Query	   *savedDataQuery = NULL;
IndexBuildResult o_pkey_result = {0};
bool		o_in_add_column = false;

static void orioledb_utility_command(PlannedStmt *pstmt,
									 const char *queryString,
									 bool readOnlyTree,
									 ProcessUtilityContext context,
									 ParamListInfo params,
									 QueryEnvironment *env,
									 DestReceiver *dest,
									 struct QueryCompletion *qc);
static void orioledb_object_access_hook(ObjectAccessType access, Oid classId,
										Oid objectId, int subId, void *arg);

static void o_alter_column_type(AlterTableCmd *cmd, const char *queryString,
								Relation rel);
static void o_find_collation_dependencies(Oid colloid);
static void redefine_indices(Relation rel, OTable *new_o_table, bool primary, bool set_tablespace);

static bool get_db_info(const char *name, LOCKMODE lockmode, Oid *dbIdP);
static Oid	o_createdb(ParseState *pstate, const CreatedbStmt *stmt);
static void o_validate_replica_identity(Relation rel, ReplicaIdentityStmt *stmt);

void
orioledb_setup_ddl_hooks(void)
{
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = orioledb_utility_command;
	old_objectaccess_hook = object_access_hook;
	object_access_hook = orioledb_object_access_hook;
}


static const char *
alter_table_type_to_string(AlterTableType cmdtype)
{
	switch (cmdtype)
	{
		case AT_AddColumn:
		case AT_AddColumnToView:
			return "ADD COLUMN";
		case AT_ColumnDefault:
		case AT_CookedColumnDefault:
			return "ALTER COLUMN ... SET DEFAULT";
		case AT_DropNotNull:
			return "ALTER COLUMN ... DROP NOT NULL";
		case AT_SetNotNull:
			return "ALTER COLUMN ... SET NOT NULL";
		case AT_DropExpression:
			return "ALTER COLUMN ... DROP EXPRESSION";
		case AT_CheckNotNull:
			return NULL;		/* not real grammar */
		case AT_SetStatistics:
			return "ALTER COLUMN ... SET STATISTICS";
		case AT_SetOptions:
			return "ALTER COLUMN ... SET";
		case AT_ResetOptions:
			return "ALTER COLUMN ... RESET";
		case AT_SetStorage:
			return "ALTER COLUMN ... SET STORAGE";
		case AT_SetCompression:
			return "ALTER COLUMN ... SET COMPRESSION";
		case AT_DropColumn:
		case AT_AddIndex:
		case AT_ReAddIndex:
			return NULL;		/* not real grammar */
		case AT_AddConstraint:
		case AT_ReAddConstraint:
		case AT_ReAddDomainConstraint:
		case AT_AddIndexConstraint:
			return "ADD CONSTRAINT";
		case AT_AlterConstraint:
			return "ALTER CONSTRAINT";
		case AT_ValidateConstraint:
			return "VALIDATE CONSTRAINT";
		case AT_DropConstraint:
		case AT_ReAddComment:
			return NULL;		/* not real grammar */
		case AT_AlterColumnType:
			return "ALTER COLUMN ... SET DATA TYPE";
		case AT_AlterColumnGenericOptions:
			return "ALTER COLUMN ... OPTIONS";
		case AT_ChangeOwner:
			return "OWNER TO";
		case AT_ClusterOn:
			return "CLUSTER ON";
		case AT_DropCluster:
			return "SET WITHOUT CLUSTER";
		case AT_SetAccessMethod:
			return "SET ACCESS METHOD";
		case AT_SetLogged:
			return "SET LOGGED";
		case AT_SetUnLogged:
			return "SET UNLOGGED";
		case AT_DropOids:
			return "SET WITHOUT OIDS";
		case AT_SetTableSpace:
			return "SET TABLESPACE";
		case AT_SetRelOptions:
			return "SET";
		case AT_ResetRelOptions:
			return "RESET";
		case AT_ReplaceRelOptions:
			return NULL;		/* not real grammar */
		case AT_EnableTrig:
			return "ENABLE TRIGGER";
		case AT_EnableAlwaysTrig:
			return "ENABLE ALWAYS TRIGGER";
		case AT_EnableReplicaTrig:
			return "ENABLE REPLICA TRIGGER";
		case AT_DisableTrig:
			return "DISABLE TRIGGER";
		case AT_EnableTrigAll:
			return "ENABLE TRIGGER ALL";
		case AT_DisableTrigAll:
			return "DISABLE TRIGGER ALL";
		case AT_EnableTrigUser:
			return "ENABLE TRIGGER USER";
		case AT_DisableTrigUser:
			return "DISABLE TRIGGER USER";
		case AT_EnableRule:
			return "ENABLE RULE";
		case AT_EnableAlwaysRule:
			return "ENABLE ALWAYS RULE";
		case AT_EnableReplicaRule:
			return "ENABLE REPLICA RULE";
		case AT_DisableRule:
			return "DISABLE RULE";
		case AT_AddInherit:
			return "INHERIT";
		case AT_DropInherit:
			return "NO INHERIT";
		case AT_AddOf:
			return "OF";
		case AT_DropOf:
			return "NOT OF";
		case AT_ReplicaIdentity:
			return "REPLICA IDENTITY";
		case AT_EnableRowSecurity:
			return "ENABLE ROW SECURITY";
		case AT_DisableRowSecurity:
			return "DISABLE ROW SECURITY";
		case AT_ForceRowSecurity:
			return "FORCE ROW SECURITY";
		case AT_NoForceRowSecurity:
			return "NO FORCE ROW SECURITY";
		case AT_GenericOptions:
			return "OPTIONS";
		case AT_AttachPartition:
			return "ATTACH PARTITION";
		case AT_DetachPartition:
			return "DETACH PARTITION";
		case AT_DetachPartitionFinalize:
			return "DETACH PARTITION ... FINALIZE";
		case AT_AddIdentity:
			return "ALTER COLUMN ... ADD IDENTITY";
		case AT_SetIdentity:
			return "ALTER COLUMN ... SET";
		case AT_DropIdentity:
			return "ALTER COLUMN ... DROP IDENTITY";
#if PG_VERSION_NUM >= 170000
		case AT_SetExpression:
			return "ALTER COLUMN ... SET EXPRESSION";
#endif
		case AT_ReAddStatistics:
			return NULL;		/* not real grammar */
	}

	return NULL;
}

static bool
is_alter_table_partition(PlannedStmt *pstmt)
{
	AlterTableStmt *top_atstmt = (AlterTableStmt *) pstmt->utilityStmt;

	if (list_length(top_atstmt->cmds) == 1)
	{
		AlterTableCmd *cmd = linitial(top_atstmt->cmds);

		if (cmd->subtype == AT_AttachPartition ||
			cmd->subtype == AT_DetachPartition ||
			cmd->subtype == AT_DetachPartitionFinalize)
			return true;
	}
	return false;
}


/*
 * Given a VacuumRelation, fill in the table OID if it wasn't specified,
 * and optionally add VacuumRelations for partitions of the table.
 *
 * If a VacuumRelation does not have an OID supplied and is a partitioned
 * table, an extra entry will be added to the output for each partition.
 * Presently, only autovacuum supplies OIDs when calling vacuum(), and
 * it does not want us to expand partitioned tables.
 */
static List *
expand_vacuum_rel(VacuumRelation *vrel, int options)
{
	List	   *vacrels = NIL;

	/* If caller supplied OID, there's nothing we need do here. */
	if (OidIsValid(vrel->oid))
	{
		vacrels = lappend(vacrels, vrel);
	}
	else
	{
		/* Process a specific relation, and possibly partitions thereof */
		Oid			relid;
		HeapTuple	tuple;
		Form_pg_class classForm;
		bool		include_parts;
		int			rvr_opts;

		/*
		 * We transiently take AccessShareLock to protect the syscache lookup
		 * below, as well as find_all_inheritors's expectation that the caller
		 * holds some lock on the starting relation.
		 */
		rvr_opts = (options & VACOPT_SKIP_LOCKED) ? RVR_SKIP_LOCKED : 0;
		relid = RangeVarGetRelidExtended(vrel->relation,
										 AccessShareLock,
										 rvr_opts,
										 NULL, NULL);

		/*
		 * If the lock is unavailable, emit the same log statement that
		 * vacuum_rel() and analyze_rel() would.
		 */
		if (!OidIsValid(relid))
		{
			return vacrels;
		}

		/*
		 * To check whether the relation is a partitioned table and its
		 * ownership, fetch its syscache entry.
		 */
		tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", relid);
		classForm = (Form_pg_class) GETSTRUCT(tuple);

		/*
		 * Make a returnable VacuumRelation for this rel if user is a proper
		 * owner.
		 */
		if (vacuum_is_relation_owner(relid, classForm, options))
		{
			vacrels = lappend(vacrels, makeVacuumRelation(vrel->relation,
														  relid,
														  vrel->va_cols));
		}

		include_parts = (classForm->relkind == RELKIND_PARTITIONED_TABLE);
		ReleaseSysCache(tuple);

		/*
		 * If it is, make relation list entries for its partitions.  Note that
		 * the list returned by find_all_inheritors() includes the passed-in
		 * OID, so we have to skip that.  There's no point in taking locks on
		 * the individual partitions yet, and doing so would just add
		 * unnecessary deadlock risk.  For this last reason we do not check
		 * yet the ownership of the partitions, which get added to the list to
		 * process.  Ownership will be checked later on anyway.
		 */
		if (include_parts)
		{
			List	   *part_oids = find_all_inheritors(relid, NoLock, NULL);
			ListCell   *part_lc;

			foreach(part_lc, part_oids)
			{
				Oid			part_oid = lfirst_oid(part_lc);

				if (part_oid == relid)
					continue;	/* ignore original table */

				/*
				 * We omit a RangeVar since it wouldn't be appropriate to
				 * complain about failure to open one of these relations
				 * later.
				 */
				vacrels = lappend(vacrels, makeVacuumRelation(NULL,
															  part_oid,
															  vrel->va_cols));
			}
		}

		/*
		 * Release lock again.  This means that by the time we actually try to
		 * process the table, it might be gone or renamed.  In the former case
		 * we'll silently ignore it; in the latter case we'll process it
		 * anyway, but we must beware that the RangeVar doesn't necessarily
		 * identify it anymore.  This isn't ideal, perhaps, but there's little
		 * practical alternative, since we're typically going to commit this
		 * transaction and begin a new one between now and then.  Moreover,
		 * holding locks on multiple relations would create significant risk
		 * of deadlock.
		 */
		UnlockRelationOid(relid, AccessShareLock);
	}

	return vacrels;
}

/*
 * Construct a list of VacuumRelations for all vacuumable rels in
 * the current database.
 */
static List *
get_all_vacuum_rels(int options)
{
	List	   *vacrels = NIL;
	Relation	pgclass;
	TableScanDesc scan;
	HeapTuple	tuple;

	pgclass = table_open(RelationRelationId, AccessShareLock);

	scan = table_beginscan_catalog(pgclass, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relid = classForm->oid;

		/* check permissions of relation */
		if (!vacuum_is_relation_owner(relid, classForm, options))
			continue;

		/*
		 * We include partitioned tables here; depending on which operation is
		 * to be performed, caller will decide whether to process or ignore
		 * them.
		 */
		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW &&
			classForm->relkind != RELKIND_PARTITIONED_TABLE)
			continue;

		/*
		 * Build VacuumRelation(s) specifying the table OIDs to be processed.
		 * We omit a RangeVar since it wouldn't be appropriate to complain
		 * about failure to open one of these relations later.
		 */
		vacrels = lappend(vacrels, makeVacuumRelation(NULL,
													  relid,
													  NIL));
	}

	table_endscan(scan);
	table_close(pgclass, AccessShareLock);
	return vacrels;
}

/* Based on postgres function ReindexMultipleTables */
static bool
check_multiple_tables(const char *objectName, ReindexObjectType objectKind, bool concurrently)
{
	Oid			objectOid;
	Relation	relationRelation;
	TableScanDesc scan;
	ScanKeyData scan_keys[1];
	HeapTuple	tuple;
	MemoryContext private_context;
	int			num_keys;
	bool		has_orioledb = false;

	Assert(objectKind == REINDEX_OBJECT_SCHEMA ||
		   objectKind == REINDEX_OBJECT_SYSTEM ||
		   objectKind == REINDEX_OBJECT_DATABASE);

	/*
	 * This matches the options enforced by the grammar, where the object name
	 * is optional for DATABASE and SYSTEM.
	 */
	Assert(objectName || objectKind != REINDEX_OBJECT_SCHEMA);

	if (objectKind == REINDEX_OBJECT_SYSTEM && concurrently)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot reindex system catalogs concurrently")));

	/*
	 * Get OID of object to reindex, being the database currently being used
	 * by session for a database or for system catalogs, or the schema defined
	 * by caller. At the same time do permission checks that need different
	 * processing depending on the object type.
	 */
	if (objectKind == REINDEX_OBJECT_SCHEMA)
	{
		objectOid = get_namespace_oid(objectName, false);

		if (!object_ownercheck(NamespaceRelationId, objectOid, GetUserId())
#if PG_VERSION_NUM >= 170000
			&& !has_privs_of_role(GetUserId(), ROLE_PG_MAINTAIN)
#endif
			)
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SCHEMA,
						   objectName);
	}
	else
	{
		objectOid = MyDatabaseId;

		if (objectName && strcmp(objectName, get_database_name(objectOid)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("can only reindex the currently open database")));
		if (!object_ownercheck(DatabaseRelationId, objectOid, GetUserId())
#if PG_VERSION_NUM >= 170000
			&& !has_privs_of_role(GetUserId(), ROLE_PG_MAINTAIN)
#endif
			)
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE,
						   get_database_name(objectOid));
	}

	/*
	 * Create a memory context that will survive forced transaction commits we
	 * do below.  Since it is a child of PortalContext, it will go away
	 * eventually even if we suffer an error; there's no need for special
	 * abort cleanup logic.
	 */
	private_context = AllocSetContextCreate(PortalContext,
											"check_multiple_tables",
											ALLOCSET_SMALL_SIZES);

	/*
	 * Define the search keys to find the objects to reindex. For a schema, we
	 * select target relations using relnamespace, something not necessary for
	 * a database-wide operation.
	 */
	if (objectKind == REINDEX_OBJECT_SCHEMA)
	{
		num_keys = 1;
		ScanKeyInit(&scan_keys[0],
					Anum_pg_class_relnamespace,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(objectOid));
	}
	else
		num_keys = 0;

	/*
	 * Scan pg_class to build a list of the relations we need to reindex.
	 *
	 * We only consider plain relations and materialized views here (toast
	 * rels will be processed indirectly by reindex_relation).
	 */
	relationRelation = table_open(RelationRelationId, AccessShareLock);
	scan = table_beginscan_catalog(relationRelation, num_keys, scan_keys);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classtuple = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relid = classtuple->oid;
		Relation	tbl;

		/*
		 * Only regular tables and matviews can have indexes, so ignore any
		 * other kind of relation.
		 *
		 * Partitioned tables/indexes are skipped but matching leaf partitions
		 * are processed.
		 */
		if (classtuple->relkind != RELKIND_RELATION &&
			classtuple->relkind != RELKIND_MATVIEW)
			continue;

		/* Skip temp tables of other backends; we can't reindex them at all */
		if (classtuple->relpersistence == RELPERSISTENCE_TEMP &&
			!isTempNamespace(classtuple->relnamespace))
			continue;

		/*
		 * Check user/system classification.  SYSTEM processes all the
		 * catalogs, and DATABASE processes everything that's not a catalog.
		 */
		if (objectKind == REINDEX_OBJECT_SYSTEM &&
			!IsCatalogRelationOid(relid))
			continue;
		else if (objectKind == REINDEX_OBJECT_DATABASE &&
				 IsCatalogRelationOid(relid))
			continue;

		/*
		 * The table can be reindexed if the user is superuser, the table
		 * owner, or the database/schema owner (but in the latter case, only
		 * if it's not a shared relation).  object_ownercheck includes the
		 * superuser case, and depending on objectKind we already know that
		 * the user has permission to run REINDEX on this database or schema
		 * per the permission checks at the beginning of this routine.
		 */
		if (classtuple->relisshared &&
			object_ownercheck(RelationRelationId, relid, GetUserId()))
			continue;

		/*
		 * Skip system tables, since index_create() would reject indexing them
		 * concurrently (and it would likely fail if we tried).
		 */
		if (concurrently && IsCatalogRelationOid(relid))
		{
			continue;
		}

		tbl = relation_open(relid, AccessShareLock);
		if (is_orioledb_rel(tbl))
		{
			ListCell   *index;

			foreach(index, RelationGetIndexList(tbl))
			{
				Oid			indexOid = lfirst_oid(index);
				Relation	ind = relation_open(indexOid, AccessShareLock);
				OBTOptions *options = (OBTOptions *) ind->rd_options;

				if (ind->rd_rel->relam == BTREE_AM_OID && !(options && !options->orioledb_index))
				{
					String	   *ix_name = makeString(pstrdup(ind->rd_rel->relname.data));

					reindex_list = list_append_unique(reindex_list, ix_name);
				}
				relation_close(ind, AccessShareLock);
			}

			if (concurrently)
				has_orioledb = true;
		}
		relation_close(tbl, AccessShareLock);
	}
	table_endscan(scan);
	table_close(relationRelation, AccessShareLock);

	MemoryContextDelete(private_context);
	return has_orioledb;
}

#if PG_VERSION_NUM >= 170000
/*
 * create_ctas_internal
 *
 * Internal utility used for the creation of the definition of a relation
 * created via CREATE TABLE AS or a materialized view.  Caller needs to
 * provide a list of attributes (ColumnDef nodes).
 */
static ObjectAddress
create_ctas_internal(List *attrList, IntoClause *into)
{
	CreateStmt *create = makeNode(CreateStmt);
	bool		is_matview;
	char		relkind;
	Datum		toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	ObjectAddress intoRelationAddr;

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	is_matview = (into->viewQuery != NULL);
	relkind = is_matview ? RELKIND_MATVIEW : RELKIND_RELATION;

	/*
	 * Create the target relation by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 */
	create->relation = into->rel;
	create->tableElts = attrList;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = into->options;
	create->oncommit = into->onCommit;
	create->tablespacename = into->tableSpaceName;
	create->if_not_exists = false;
	create->accessMethod = into->accessMethod;

	/*
	 * Create the relation.  (This will error out if there's an existing view,
	 * so we don't need more code to complain if "replace" is false.)
	 */
	intoRelationAddr = DefineRelation(create, relkind, InvalidOid, NULL, NULL);

	/*
	 * If necessary, create a TOAST table for the target table.  Note that
	 * NewRelationCreateToastTable ends with CommandCounterIncrement(), so
	 * that the TOAST table will be visible for insertion.
	 */
	CommandCounterIncrement();

	/* parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum) 0,
										create->options,
										"toast",
										validnsps,
										true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	NewRelationCreateToastTable(intoRelationAddr.objectId, toast_options);

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
 * create_ctas_nodata
 *
 * Create CTAS or materialized view when WITH NO DATA is used, starting from
 * the targetlist of the SELECT or view definition.
 */
static ObjectAddress
create_ctas_nodata(List *tlist, IntoClause *into)
{
	List	   *attrList;
	ListCell   *t,
			   *lc;

	/*
	 * Build list of ColumnDefs from non-junk elements of the tlist.  If a
	 * column name list was specified in CREATE TABLE AS, override the column
	 * names in the query.  (Too few column names are OK, too many are not.)
	 */
	attrList = NIL;
	lc = list_head(into->colNames);
	foreach(t, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(t);

		if (!tle->resjunk)
		{
			ColumnDef  *col;
			char	   *colname;

			if (lc)
			{
				colname = strVal(lfirst(lc));
				lc = lnext(into->colNames, lc);
			}
			else
				colname = tle->resname;

			col = makeColumnDef(colname,
								exprType((Node *) tle->expr),
								exprTypmod((Node *) tle->expr),
								exprCollation((Node *) tle->expr));

			/*
			 * It's possible that the column is of a collatable type but the
			 * collation could not be resolved, so double-check.  (We must
			 * check this here because DefineRelation would adopt the type's
			 * default collation rather than complaining.)
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
	}

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/* Create the relation definition using the ColumnDef list */
	return create_ctas_internal(attrList, into);
}
#endif

static bool
ReindexPartitions(Oid relid, bool concurrently)
{
	List	   *inhoids;
	ListCell   *lc;
	bool		has_orioledb = false;

	inhoids = find_all_inheritors(relid, ShareLock, NULL);

	foreach(lc, inhoids)
	{
		Oid			partoid = lfirst_oid(lc);
		Relation	part_rel = relation_open(partoid, AccessShareLock);

		/*
		 * This discards partitioned tables, partitioned indexes and foreign
		 * tables.
		 */
		if (!RELKIND_HAS_STORAGE(part_rel->rd_rel->relkind))
		{
			relation_close(part_rel, AccessShareLock);
			continue;
		}

		Assert(part_rel->rd_rel->relkind == RELKIND_INDEX ||
			   part_rel->rd_rel->relkind == RELKIND_RELATION);

		if (concurrently)
		{
			if ((part_rel->rd_rel->relkind == RELKIND_RELATION ||
				 part_rel->rd_rel->relkind == RELKIND_MATVIEW) &&
				is_orioledb_rel(part_rel))
			{
				has_orioledb = true;
			}
			else if (part_rel->rd_rel->relkind == RELKIND_INDEX)
			{
				Relation	tbl;

				tbl = relation_open(part_rel->rd_index->indrelid, AccessShareLock);

				if ((tbl->rd_rel->relkind == RELKIND_RELATION) &&
					is_orioledb_rel(tbl))
				{
					has_orioledb = true;
				}
				relation_close(tbl, AccessShareLock);
			}
		}
		relation_close(part_rel, AccessShareLock);
	}
	return has_orioledb;
}

static void
orioledb_utility_command(PlannedStmt *pstmt,
						 const char *queryString,
						 bool readOnlyTree,
						 ProcessUtilityContext context,
						 ParamListInfo params,
						 QueryEnvironment *env,
						 DestReceiver *dest,
						 struct QueryCompletion *qc)
{
	bool		isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);
	ParseState *pstate;
	bool		call_next = true;

	/* copied from standard_ProcessUtility */
	if (readOnlyTree)
		pstmt = copyObject(pstmt);

	in_rewrite = false;
	o_saved_relrewrite = InvalidOid;
	o_saved_reltablespace = InvalidOid;
	savedDataQuery = NULL;
	in_nontransactional_truncate = false;

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;
	pstate->p_queryEnv = env;

	/*
	 * DDL WAL ordering barrier.
	 *
	 * OrioleDB does not emit its WAL records immediately and directly into
	 * PostgreSQL WAL.  We first accumulate Oriole records in a per-backend
	 * local WAL buffer and flush that buffer either: - when it overflows, or
	 * - at transaction finalization.
	 *
	 * This deferred flushing can reorder OrioleDB WAL records relative to
	 * PostgreSQL-native WAL records generated by utility commands (DDL). In
	 * particular, a utility command may generate PG WAL immediately, while
	 * Oriole's related records stay buffered until later.  On crash/recovery
	 * or logical decoding this may surface as observing PG DDL changes before
	 * the corresponding Oriole metadata/state changes, which breaks
	 * assumptions about atomicity and visibility of DDL boundaries.
	 *
	 * To enforce a stable ordering across the two WAL streams, we treat entry
	 * into ProcessUtility as a barrier: before executing any utility command
	 * we flush any pending Oriole local WAL so that all Oriole records
	 * produced by prior statements become durable/visible in WAL *before*
	 * this DDL starts producing PostgreSQL WAL.
	 *
	 * Note: recovery workers do not produce local WAL in the same way and
	 * must not perform this flush here.
	 */
	if (!is_recovery_process() && !local_wal_is_empty())
		flush_local_wal(false, false);

	if (IsA(pstmt->utilityStmt, AlterTableStmt) &&
		!is_alter_table_partition(pstmt))
	{
		AlterTableStmt *atstmt = (AlterTableStmt *) pstmt->utilityStmt;
		Oid			relid;
		LOCKMODE	lockmode;
		ObjectType	objtype;

		objtype = atstmt->objtype;

		/*
		 * alter_type_exprs is expected to be allocated in PortalContext so it
		 * isn't freed by us and pointer may be invalid there
		 */
		alter_type_exprs = NIL;
		dropped_attrs = NIL;

		/*
		 * Figure out lock mode, and acquire lock.  This also does basic
		 * permissions checks, so that we won't wait for a lock on (for
		 * example) a relation on which we have no permissions.
		 */
		lockmode = AlterTableGetLockLevel(atstmt->cmds);
		relid = AlterTableLookupRelation(atstmt, lockmode);

		if (OidIsValid(relid) && objtype == OBJECT_TABLE &&
			(lockmode == AccessExclusiveLock || lockmode == ShareUpdateExclusiveLock))
		{
			Relation	rel = table_open(relid, lockmode);

			if (is_orioledb_rel(rel))
			{
				ListCell   *lc;

				foreach(lc, atstmt->cmds)
				{
					AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

					/* make checks */
					switch (cmd->subtype)
					{
						case AT_AddColumn:
						case AT_AddConstraint:
						case AT_AddIdentity:
						case AT_AddIndex:
						case AT_AddInherit:
						case AT_AlterColumnType:
						case AT_ChangeOwner:
						case AT_ColumnDefault:
						case AT_CookedColumnDefault:
						case AT_DisableRowSecurity:
						case AT_DropColumn:
						case AT_DropConstraint:
						case AT_DropExpression:
						case AT_DropIdentity:
						case AT_DropInherit:
						case AT_DropNotNull:
						case AT_EnableRowSecurity:
						case AT_GenericOptions:
						case AT_ResetRelOptions:
						case AT_SetIdentity:
						case AT_SetNotNull:
						case AT_SetRelOptions:
						case AT_EnableRule:
						case AT_EnableAlwaysRule:
						case AT_EnableReplicaRule:
						case AT_DisableRule:
						case AT_SetTableSpace:
						case AT_SetStorage:
						case AT_ReplicaIdentity:
						case AT_AddIndexConstraint:
						case AT_AddOf:
						case AT_AlterColumnGenericOptions:
						case AT_AlterConstraint:
						case AT_DisableTrig:
						case AT_DisableTrigAll:
						case AT_DisableTrigUser:
						case AT_DropOf:
						case AT_EnableAlwaysTrig:
						case AT_EnableReplicaTrig:
						case AT_EnableTrig:
						case AT_EnableTrigAll:
						case AT_EnableTrigUser:
						case AT_ForceRowSecurity:
						case AT_NoForceRowSecurity:
						case AT_ReplaceRelOptions:
						case AT_ResetOptions:
						case AT_SetLogged:
						case AT_SetOptions:
						case AT_SetStatistics:
						case AT_SetUnLogged:
						case AT_ValidateConstraint:
#if PG_VERSION_NUM >= 170000
						case AT_SetExpression:
#endif
							break;
						case AT_DropOids:
							ereport(WARNING,
									(errmsg("alter table subcommand \"%s\" has no effect on OrioleDB tables since they do not use OIDs",
											alter_table_type_to_string(cmd->subtype))));
							break;
						case AT_ClusterOn:
						case AT_DropCluster:
							ereport(WARNING,
									(errmsg("alter table subcommand \"%s\" has no performance effect on OrioleDB tables with primary key",
											alter_table_type_to_string(cmd->subtype))));
							break;
						case AT_SetAccessMethod:
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("changing access method is not supported for OrioleDB tables")));
							break;
						case AT_SetCompression:
						default:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("unsupported alter table subcommand")),
									errdetail("Subcommand \"%s\" is not supported on OrioleDB tables yet.  This will be implemented in future.",
											  alter_table_type_to_string(cmd->subtype)));
							break;
					}

					switch (cmd->subtype)
					{
						case AT_AlterColumnType:
							o_alter_column_type(cmd, queryString, rel);
							break;
						case AT_ReplicaIdentity:
							o_validate_replica_identity(rel, (ReplicaIdentityStmt *) cmd->def);
							break;
						default:
							break;
					}
				}
			}
			table_close(rel, lockmode);
		}
	}
	else if (IsA(pstmt->utilityStmt, ClusterStmt))
	{
		ClusterStmt *stmt = (ClusterStmt *) pstmt->utilityStmt;

		if (stmt->relation != NULL)
		{
			/* This is the single-relation case. */
			Oid			tableOid;
			Relation	rel = NULL;
			bool		orioledb;

			tableOid = RangeVarGetRelid(stmt->relation, AccessShareLock,
										false);
			rel = table_open(tableOid, AccessShareLock);
			orioledb = is_orioledb_rel(rel);
			table_close(rel, AccessShareLock);
			if (orioledb)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("orioledb tables does not support CLUSTER")),
						errdetail("CLUSTER makes no much sense for index-organized tables."));
		}
	}
	else if (IsA(pstmt->utilityStmt, VacuumStmt))
	{
		VacuumStmt *vacstmt = (VacuumStmt *) pstmt->utilityStmt;
		ListCell   *lc;
		bool		full = false,
					skip_locked = false,
					analyze = false;
		int			options;

		foreach(lc, vacstmt->options)
		{
			DefElem    *opt = (DefElem *) lfirst(lc);

			if (strcmp(opt->defname, "full") == 0)
				full = defGetBoolean(opt);
			else if (strcmp(opt->defname, "skip_locked") == 0)
				skip_locked = defGetBoolean(opt);
			else if (strcmp(opt->defname, "analyze") == 0)
				analyze = defGetBoolean(opt);
		}
		options =
			(vacstmt->is_vacuumcmd ? VACOPT_VACUUM : VACOPT_ANALYZE) |
			(skip_locked ? VACOPT_SKIP_LOCKED : 0) |
			(analyze ? VACOPT_ANALYZE : 0) |
			(full ? VACOPT_FULL : 0);
		if (full)
		{
			List	   *relations = vacstmt->rels;

			if (relations != NIL)
			{
				List	   *newrels = NIL;

				foreach(lc, relations)
				{
					VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);
					List	   *sublist;

					sublist = expand_vacuum_rel(vrel, options);
					newrels = list_concat(newrels, sublist);
				}
				relations = newrels;
			}
			else
				relations = get_all_vacuum_rels(options);
			foreach(lc, relations)
			{
				VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);
				Relation	rel;
				bool		orioledb;

				if (options & VACOPT_SKIP_LOCKED)
				{
					if (ConditionalLockRelationOid(vrel->oid, AccessShareLock))
						rel = relation_open(vrel->oid, NoLock);
					else
						continue;
				}
				else
				{
					rel = relation_open(vrel->oid, AccessShareLock);
				}

				orioledb = is_orioledb_rel(rel);
				if (orioledb)
				{
					if (orioledb_strict_mode)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("orioledb table \"%s\" does not support VACUUM FULL",
										RelationGetRelationName(rel))),
								errdetail("VACUUM FULL is not supported for OrioleDB tables yet."));
					}
					else
					{
						ListCell   *lc2;

						ereport(WARNING,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("orioledb table \"%s\" does not support VACUUM FULL, using a plain VACUUM instead",
										RelationGetRelationName(rel))));

						foreach(lc2, vacstmt->options)
						{
							DefElem    *opt = (DefElem *) lfirst(lc2);

							if (strcmp(opt->defname, "full") == 0)
								opt->arg = (Node *) makeInteger(0);
						}
					}
				}
				relation_close(rel, AccessShareLock);
			}
		}
	}
	else if (IsA(pstmt->utilityStmt, ReindexStmt))
	{
		ReindexStmt *stmt = (ReindexStmt *) pstmt->utilityStmt;
		char	   *tablespacename = NULL;
		bool		concurrently = false;
		bool		has_orioledb = false;
		ListCell   *lc;

		foreach(lc, stmt->params)
		{
			DefElem    *opt = (DefElem *) lfirst(lc);

			if (strcmp(opt->defname, "concurrently") == 0)
				concurrently = defGetBoolean(opt);
			else if (strcmp(opt->defname, "tablespace") == 0)
				tablespacename = defGetString(opt);
			else if (strcmp(opt->defname, "verbose") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("unrecognized REINDEX option \"%s\"",
								opt->defname),
						 parser_errposition(pstate, opt->location)));
		}

		/* Show same error as in ExecReindex */
		if (concurrently)
			PreventInTransactionBlock(isTopLevel,
									  "REINDEX CONCURRENTLY");

		if (tablespacename != NULL)
		{
			Oid			tablespaceOid = get_tablespace_oid(tablespacename, false);

			/* Check permissions except when moving to database's default */
			if (OidIsValid(tablespaceOid) &&
				tablespaceOid != MyDatabaseTableSpace)
			{
				AclResult	aclresult;

				aclresult = object_aclcheck(TableSpaceRelationId, tablespaceOid,
											GetUserId(), ACL_CREATE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, OBJECT_TABLESPACE,
								   get_tablespace_name(tablespaceOid));
			}
		}

		switch (stmt->kind)
		{
			case REINDEX_OBJECT_INDEX:
				{
					Oid			indOid = RangeVarGetRelid(stmt->relation,
														  AccessShareLock,
														  false);
					Relation	iRel,
								tbl;
					OBTOptions *options;

					if (get_rel_relkind(indOid) == RELKIND_PARTITIONED_INDEX)
					{
						has_orioledb = ReindexPartitions(indOid, concurrently);
						break;
					}

					iRel = index_open(indOid, AccessShareLock);
					tbl = relation_open(iRel->rd_index->indrelid,
										AccessShareLock);
					options = (OBTOptions *) iRel->rd_options;
					if (is_orioledb_rel(tbl) &&
						iRel->rd_rel->relam == BTREE_AM_OID &&
						!(options && !options->orioledb_index))
					{
						String	   *ix_name;

						ix_name = makeString(pstrdup(iRel->rd_rel->relname.data));
						reindex_list = list_append_unique(reindex_list, ix_name);
						if (concurrently)
							has_orioledb = true;
					}
					relation_close(tbl, AccessShareLock);
					relation_close(iRel, AccessShareLock);
				}
				break;
			case REINDEX_OBJECT_TABLE:
				{
					Oid			tblOid = RangeVarGetRelid(stmt->relation,
														  AccessShareLock,
														  false);
					Relation	tbl;

					if (get_rel_relkind(tblOid) == RELKIND_PARTITIONED_TABLE)
					{
						has_orioledb = ReindexPartitions(tblOid, concurrently);
						break;
					}
					tbl = relation_open(tblOid, AccessShareLock);
					if (is_orioledb_rel(tbl))
					{
						ListCell   *index;

						foreach(index, RelationGetIndexList(tbl))
						{
							Oid			indexOid = lfirst_oid(index);
							Relation	ind = relation_open(indexOid, AccessShareLock);
							OBTOptions *options = (OBTOptions *) ind->rd_options;

							if (ind->rd_rel->relam == BTREE_AM_OID && !(options && !options->orioledb_index))
							{
								String	   *ix_name = makeString(pstrdup(ind->rd_rel->relname.data));

								reindex_list = list_append_unique(reindex_list, ix_name);
							}
							relation_close(ind, AccessShareLock);
							if (concurrently)
								has_orioledb = true;
						}
					}
					relation_close(tbl, AccessShareLock);
				}
				break;
			case REINDEX_OBJECT_SCHEMA:
			case REINDEX_OBJECT_SYSTEM:
			case REINDEX_OBJECT_DATABASE:
				has_orioledb = check_multiple_tables(stmt->name, stmt->kind, concurrently);
				break;
			default:
				elog(ERROR, "unrecognized object type: %d",
					 (int) stmt->kind);
				break;
		}

		if (has_orioledb && concurrently)
		{
			if (tablespacename != NULL)
			{
				Oid			tablespaceOid = get_tablespace_oid(tablespacename, false);

				if (tablespaceOid == GLOBALTABLESPACE_OID)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot move non-shared relation to tablespace \"%s\"",
									get_tablespace_name(tablespaceOid))));
			}

			if (orioledb_strict_mode)
				elog(ERROR, "REINDEX CONCURRENTLY is not supported for orioledb tables yet");
			else
				elog(WARNING, "REINDEX CONCURRENTLY is not supported for orioledb tables yet, using a plain REINDEX instead");

			foreach(lc, stmt->params)
			{
				DefElem    *opt = (DefElem *) lfirst(lc);

				if (strcmp(opt->defname, "concurrently") == 0)
					stmt->params = foreach_delete_current(stmt->params, lc);
			}
		}
	}
	else if (IsA(pstmt->utilityStmt, TransactionStmt))
	{
		TransactionStmt *tstmt = (TransactionStmt *) pstmt->utilityStmt;

		if (tstmt->kind == TRANS_STMT_PREPARE && have_retained_undo_location())
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot use PREPARE TRANSACTION in transaction that uses orioledb table")),
					errdetail("OrioleDB does not support prepared transactions yet."));
		}
	}
	else if (IsA(pstmt->utilityStmt, AlterCollationStmt))
	{
		AlterCollationStmt *astmt = (AlterCollationStmt *) pstmt->utilityStmt;
		Oid			collOid = get_collation_oid(astmt->collname, false);

		o_find_collation_dependencies(collOid);
	}
#if PG_VERSION_NUM >= 170000
	else if (IsA(pstmt->utilityStmt, CreateTableAsStmt))
	{
		CreateTableAsStmt *stmt = (CreateTableAsStmt *) pstmt->utilityStmt;
		IntoClause *into = stmt->into;

		if (!into->skipData)
		{
			bool		is_matview = (into->viewQuery != NULL);

			if (is_matview &&
				((into->accessMethod && strcmp(into->accessMethod, "orioledb") == 0) ||
				 (!into->accessMethod && strcmp(default_table_access_method, "orioledb") == 0)))
			{
				Query	   *query = castNode(Query, stmt->query);
				ObjectAddress address;

				Assert(query->commandType == CMD_SELECT);

				address = create_ctas_nodata(query->targetList, into);

				/*
				 * We cannot just use rel->rd_rules in access hook, because it
				 * recalculates expression two times if it executes postgreses
				 * code, even if it skips insertion to table
				 */
				savedDataQuery = (Query *) copyObject(into->viewQuery);
				RefreshMatViewByOid(address.objectId, true, false,
									queryString, NULL, qc);
				savedDataQuery = NULL;

				if (qc)
					qc->commandTag = CMDTAG_SELECT;

				call_next = false;
			}
		}
	}
#endif
	else if (IsA(pstmt->utilityStmt, RefreshMatViewStmt))
	{
		RefreshMatViewStmt *stmt = (RefreshMatViewStmt *) pstmt->utilityStmt;
		Oid			matviewOid;
		Relation	matviewRel;
#if PG_VERSION_NUM >= 170000
		matviewOid = RangeVarGetRelidExtended(stmt->relation, NoLock, 0,
											  RangeVarCallbackMaintainsTable, NULL);
#else
		matviewOid = RangeVarGetRelidExtended(stmt->relation, NoLock, 0,
											  RangeVarCallbackOwnsTable, NULL);
#endif
		matviewRel = table_open(matviewOid, AccessShareLock);

		if (matviewRel->rd_rel->relkind == RELKIND_MATVIEW &&
			is_orioledb_rel(matviewRel))
		{
			if (!stmt->skipData)
			{
				savedDataQuery = linitial_node(Query, matviewRel->rd_rules->rules[0]->actions);
				if (stmt->concurrent)
				{
					if (orioledb_strict_mode)
					{
						elog(ERROR, "REFRESH MATERIALIZED VIEW CONCURRENTLY is not supported for orioledb tables yet");
					}
					else
					{
						stmt->concurrent = false;
						elog(WARNING, "REFRESH MATERIALIZED VIEW CONCURRENTLY is not supported for orioledb tables yet, using a plain REFRESH MATERIALIZED VIEW instead");
					}
				}
			}
			stmt->skipData = true;
		}
		table_close(matviewRel, AccessShareLock);
	}
	else if (IsA(pstmt->utilityStmt, IndexStmt))
	{
		IndexStmt  *stmt = (IndexStmt *) pstmt->utilityStmt;

		if (stmt->concurrent)
		{
			Oid			relid;
			Relation	rel;
			LOCKMODE	lockmode;

			PreventInTransactionBlock(context == PROCESS_UTILITY_TOPLEVEL,
									  "CREATE INDEX CONCURRENTLY");

			lockmode = ShareUpdateExclusiveLock;
			relid =
				RangeVarGetRelidExtended(stmt->relation, lockmode,
										 0,
										 RangeVarCallbackOwnsRelation,
										 NULL);
			rel = table_open(relid, lockmode);

			if (is_orioledb_rel(rel))
			{
				if (orioledb_strict_mode)
				{
					table_close(rel, lockmode);
					elog(ERROR, "concurrent index creation is not supported for orioledb tables yet");
				}
				else
				{
					stmt->concurrent = false;
					elog(WARNING, "concurrent index creation is not supported for orioledb tables yet, using a plain CREATE INDEX instead");
				}
			}
			table_close(rel, lockmode);
		}
	}
	else if (IsA(pstmt->utilityStmt, CreatedbStmt))
	{
		/* no event triggers for global objects */
		PreventInTransactionBlock(isTopLevel, "CREATE DATABASE");
		o_createdb(pstate, (CreatedbStmt *) pstmt->utilityStmt);

		call_next = false;
	}

	if (call_next)
	{
		if (next_ProcessUtility_hook)
			(*next_ProcessUtility_hook) (pstmt, queryString,
										 readOnlyTree,
										 context, params, env,
										 dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString,
									readOnlyTree,
									context, params, env,
									dest, qc);
	}

	if (IsA(pstmt->utilityStmt, ReindexStmt))
	{
		if (reindex_list)
		{
			list_free_deep(reindex_list);
			reindex_list = NIL;
		}
	}
	else if (IsA(pstmt->utilityStmt, DropStmt))
	{
		if (partition_drop_index_list)
		{
			list_free(partition_drop_index_list);
			partition_drop_index_list = NIL;
		}
		if (dropped_attrs)
		{
			list_free(dropped_attrs);
			dropped_attrs = NIL;
		}
	}
	else if (IsA(pstmt->utilityStmt, AlterTableStmt))
	{
		if (alter_type_exprs)
		{
			list_free(alter_type_exprs);
			alter_type_exprs = NIL;
		}
		if (dropped_attrs)
		{
			list_free(dropped_attrs);
			dropped_attrs = NIL;
		}
	}

	free_parsestate(pstate);
}

static void
o_validate_replica_identity(Relation rel, ReplicaIdentityStmt *stmt)
{
	elog(DEBUG4, "Current replident %c, setting replident %c", rel->rd_rel->relreplident, stmt->identity_type);

	if (stmt->identity_type == REPLICA_IDENTITY_DEFAULT)
	{
		return;
	}
	else if (stmt->identity_type == REPLICA_IDENTITY_FULL)
	{
		return;
	}
	else if (stmt->identity_type == REPLICA_IDENTITY_NOTHING)
	{
		elog(ERROR, "replica identity type NOTHING is not supported for OrioleDB tables yet");
	}
	else if (stmt->identity_type == REPLICA_IDENTITY_INDEX)
	{
		elog(ERROR, "replica identity type INDEX is not supported for OrioleDB tables yet");
	}
}

static void
o_alter_column_type(AlterTableCmd *cmd, const char *queryString, Relation rel)
{
	ColumnDef  *def = (ColumnDef *) cmd->def;

	if (def->raw_default)
	{
		Node	   *cooked_default;
		ParseState *pstate;
		ParseNamespaceItem *nsitem;
		AttrNumber	attnum;

		pstate = make_parsestate(NULL);
		pstate->p_sourcetext = queryString;
		nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
											   NULL, false, true);
		addNSItemToQuery(pstate, nsitem, false, true, true);
		cooked_default = transformExpr(pstate, def->raw_default,
									   EXPR_KIND_ALTER_COL_TRANSFORM);
		attnum = get_attnum(RelationGetRelid(rel), cmd->name);
		alter_type_exprs =
			lappend(alter_type_exprs,
		/* cppcheck-suppress unknownEvaluationOrder */
					list_make4(makeInteger(attnum), makeInteger(rel->rd_rel->oid), cooked_default, makeString(cmd->name)));
	}
}

static void
o_find_collation_dependencies(Oid colloid)
{
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc depScan;
	HeapTuple	depTup;
	HeapTuple	collationtup;
	Form_pg_collation collform;

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
				ObjectIdGetDatum(CollationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(colloid));

	depScan = systable_beginscan(depRel, DependReferenceIndexId, true,
								 NULL, 2, key);

	collationtup = SearchSysCache1(COLLOID, colloid);
	if (!HeapTupleIsValid(collationtup))
		elog(ERROR, "cache lookup failed for collation (%u)", colloid);
	collform = (Form_pg_collation) GETSTRUCT(collationtup);

	while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		Relation	rel;

		/* Else, ignore dependees that aren't user columns of relations */
		/* (we assume system columns are never of interesting types) */
		if (pg_depend->classid != RelationRelationId)
			continue;

		rel = relation_open(pg_depend->objid, AccessShareLock);

		if ((rel->rd_rel->relkind == RELKIND_RELATION ||
			 rel->rd_rel->relkind == RELKIND_MATVIEW) &&
			is_orioledb_rel(rel))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot refresh collation \"%s\" because "
							"orioledb table \"%s\" uses it",
							collform->collname.data,
							RelationGetRelationName(rel))));
		}
		else if (rel->rd_rel->relkind == RELKIND_INDEX)
		{
			Relation	tbl;

			tbl = relation_open(rel->rd_index->indrelid, AccessShareLock);

			if ((tbl->rd_rel->relkind == RELKIND_RELATION ||
				 tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
				is_orioledb_rel(tbl))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot refresh collation \"%s\" because "
								"orioledb index \"%s\" uses it",
								collform->collname.data,
								RelationGetRelationName(rel))));
			}
			relation_close(tbl, AccessShareLock);
		}

		relation_close(rel, AccessShareLock);
	}
	ReleaseSysCache(collationtup);
	systable_endscan(depScan);

	relation_close(depRel, AccessShareLock);
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
			ORelOids	table_oids;
			bool		found = false;
			int			i;

			ORelOidsSetFromRel(table_oids, rel);

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
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot alter type \"%s\" because index \"%s\" uses it",
									RelationGetRelationName(origRelation),
									NameStr(table->indices[i - 1].name))));
				}
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

static bool
ATColumnChangeRequiresRewrite(OTableField *old_field, OTableField *field, Oid objectId,
							  int subId)
{
	ParseState *pstate = make_parsestate(NULL);
	Node	   *expr = NULL;
	bool		rewrite = false;
	ListCell   *lc;
	bool		append_transform = false;

	foreach(lc, alter_type_exprs)
	{
		AttrNumber	attnum = intVal(linitial((List *) lfirst(lc)));
		const char *attname = ((String *) (lfourth((List *) lfirst(lc))))->sval;

		if (attnum == subId)
		{
			expr = (Node *) lthird((List *) lfirst(lc));
			append_transform = strcmp(attname, field->name.data) == 0;
			break;
		}
	}

	/* code from ATPrepAlterColumnType */
	if (!expr)
	{
		expr = (Node *) makeVar(1, subId, old_field->typid, old_field->typmod,
								old_field->collation, 0);
		append_transform = true;
	}
	expr = coerce_to_target_type(pstate, expr, exprType(expr), field->typid,
								 field->typmod, COERCION_EXPLICIT,
								 COERCE_IMPLICIT_CAST, -1);
	if (expr != NULL)
	{
		if (append_transform)
			/* cppcheck-suppress unknownEvaluationOrder */
			alter_type_exprs = lappend(alter_type_exprs, list_make4(makeInteger(subId), makeInteger(objectId), expr, makeString(field->name.data)));
		assign_expr_collations(pstate, expr);
		expr = (Node *) expression_planner((Expr *) expr);

		while (!rewrite)
		{
			/* only one varno, so no need to check that */
			if (IsA(expr, Var) && ((Var *) expr)->varattno == subId)
				break;
			else if (IsA(expr, RelabelType))
				expr = (Node *) ((RelabelType *) expr)->arg;
			else if (IsA(expr, CoerceToDomain))
			{
				CoerceToDomain *d = (CoerceToDomain *) expr;

				if (DomainHasConstraints(d->resulttype))
					rewrite = true;
				expr = (Node *) d->arg;
			}
			else if (IsA(expr, FuncExpr))
			{
				FuncExpr   *f = (FuncExpr *) expr;

				switch (f->funcid)
				{
					case F_TIMESTAMPTZ_TIMESTAMP:
					case F_TIMESTAMP_TIMESTAMPTZ:
						if (TimestampTimestampTzRequiresRewrite())
							rewrite = true;
						else
							expr = linitial(f->args);
						break;
					default:
						rewrite = true;
				}
			}
			else
				rewrite = true;
		}
	}

	return rewrite;
}

static void
set_toast_oids_and_options(Relation rel, Relation toast_rel, bool only_fillfactor, bool index_bridging)
{
	ORelOids	oids,
				toastOids,
			   *treeOids;
	int			numTreeOids;
	OTable	   *o_table;
	ORelOptions *options = (ORelOptions *) rel->rd_options;
	OCompress	compress = default_compress,
				primary_compress = default_primary_compress,
				toast_compress = default_toast_compress;
	uint8		fillfactor = BTREE_DEFAULT_FILLFACTOR;
	OXid		oxid = InvalidOXid;
	OSnapshot	oSnapshot;
	bool		is_temp;

	Assert(RelIsInMyDatabase(rel));
	ORelOidsSetFromRel(oids, rel);
	ORelOidsSetFromRel(toastOids, toast_rel);

	o_table = o_tables_get(oids);

	if (!only_fillfactor)
	{
		o_table->toast_oids = toastOids;
		o_tablespace_cache_add_relnode(o_table->toast_oids.datoid,
									   o_table->toast_oids.relnode,
									   o_table->tablespace);
	}

	if (options)
	{
		if (!only_fillfactor)
		{
			if (options->compress_offset > 0)
			{
				char	   *str;

				str = (char *) (((Pointer) options) +
								options->compress_offset);
				if (str)
					compress = o_parse_compress(str);
			}
			if (options->primary_compress_offset > 0)
			{
				char	   *str;

				str = (char *) (((Pointer) options) +
								options->primary_compress_offset);
				if (str)
					primary_compress = o_parse_compress(str);
			}
			if (options->toast_compress_offset > 0)
			{
				char	   *str;

				str = (char *) (((Pointer) options) +
								options->toast_compress_offset);
				if (str)
					toast_compress = o_parse_compress(str);
			}
			index_bridging = index_bridging || options->index_bridging;
		}
		fillfactor = options->std_options.fillfactor;
	}

	if (!only_fillfactor)
	{
		if (rel->rd_rel->relpersistence !=
			RELPERSISTENCE_PERMANENT &&
			(OCompressIsValid(compress) ||
			 OCompressIsValid(primary_compress) ||
			 OCompressIsValid(toast_compress)))
		{
			o_table_free(o_table);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("temp and unlogged orioledb tables does not "
							"support compression options")));
		}

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
		o_table->index_bridging = index_bridging;

		if (index_bridging)
		{
			o_table->bridge_oids.datoid = MyDatabaseId;
			o_table->bridge_oids.relnode = GetNewRelFileNumber(MyDatabaseTableSpace, NULL,
															   rel->rd_rel->relpersistence);
			o_table->bridge_oids.reloid = o_table->bridge_oids.relnode;
			o_tablespace_cache_add_relnode(o_table->bridge_oids.datoid, o_table->bridge_oids.relnode, o_table->tablespace);
		}
	}

	o_table->fillfactor = fillfactor;

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	o_tables_rel_meta_lock(rel);
	o_indices_update(o_table, PrimaryIndexNumber, oxid, oSnapshot.csn);
	o_tables_update(o_table, oxid, oSnapshot.csn);
	o_tables_after_update(o_table, oxid, oSnapshot.csn);

	treeOids = o_table_make_index_oids(o_table, &numTreeOids);
	is_temp = o_table->persistence == RELPERSISTENCE_TEMP;
	add_undo_create_relnode(oids, treeOids, numTreeOids, !is_temp);
	o_tables_rel_meta_unlock(rel, InvalidOid);
	pfree(treeOids);
	o_table_free(o_table);
}

static void
create_o_table_for_rel(Relation rel)
{
	ORelOids	oids;
	TupleDesc	tupdesc;
	OTable	   *o_table;
	OSnapshot	oSnapshot;
	OXid		oxid = InvalidOXid;
	XLogRecPtr	cur_lsn;
	Oid			datoid;

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	Assert(RelIsInMyDatabase(rel));
	ORelOidsSetFromRel(oids, rel);
	tupdesc = RelationGetDescr(rel);

	o_tables_rel_meta_lock(rel);
	o_table = o_table_tableam_create(oids, tupdesc,
									 rel->rd_rel->relpersistence,
									 RelationGetFillFactor(rel, BTREE_DEFAULT_FILLFACTOR),
									 rel->rd_rel->reltablespace,
									 false);
	o_opclass_cache_add_table(o_table);

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_database_cache_add_if_needed(datoid, datoid, cur_lsn, NULL);

	o_tables_add(o_table, oxid, oSnapshot.csn);
	o_tables_rel_meta_unlock(rel, InvalidOid);
	o_table_free(o_table);
}

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Relation	rel;
	OTableDescr *descr;
	CommitSeqNo csn;
	OXid		oxid;
} DR_transientrel;

/*
 * transientrel_startup --- executor startup
 */
static void
transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_transientrel *myState = (DR_transientrel *) self;
	OSnapshot	oSnapshot;

	fill_current_oxid_osnapshot(&myState->oxid, &oSnapshot);
	myState->csn = oSnapshot.csn;
}

/*
 * transientrel_receive --- receive one tuple
 */
static bool
transientrel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	o_tbl_insert(myState->descr, myState->rel, slot, myState->oxid, myState->csn);

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * transientrel_shutdown --- executor end
 */
static void
transientrel_shutdown(DestReceiver *self)
{
}

/*
 * transientrel_destroy --- release DestReceiver object
 */
static void
transientrel_destroy(DestReceiver *self)
{
	pfree(self);
}

static DestReceiver *
CreateOrioledbDestReceiver(Relation rel)
{
	DR_transientrel *self = (DR_transientrel *) palloc0(sizeof(DR_transientrel));

	self->pub.receiveSlot = transientrel_receive;
	self->pub.rStartup = transientrel_startup;
	self->pub.rShutdown = transientrel_shutdown;
	self->pub.rDestroy = transientrel_destroy;
	self->pub.mydest = DestTransientRel;
	self->rel = rel;
	self->descr = relation_get_descr(rel);
	Assert(self->descr != NULL);

	return (DestReceiver *) self;
}

void
o_drop_table(ORelOids oids)
{
	OSnapshot	oSnapshot;
	OXid		oxid;
	OTable	   *table;
	ORelOids   *treeOids;
	int			numTreeOids;

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	o_tables_table_meta_lock(NULL);
	table = o_tables_drop_by_oids(oids, oxid, oSnapshot.csn);
	o_tables_table_meta_unlock(NULL, InvalidOid);
	treeOids = o_table_make_index_oids(table, &numTreeOids);
	add_undo_drop_relnode(oids, treeOids, numTreeOids);
	pfree(treeOids);
	o_table_free(table);
}

static void
rewrite_matview(Relation rel, OTable *old_o_table, OTable *new_o_table)
{
	DestReceiver *dest = CreateOrioledbDestReceiver(rel);
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *copied_query;
	Query	   *query;

	/* Lock and rewrite, using a copy to preserve the original query. */
	copied_query = copyObject(savedDataQuery);
	AcquireRewriteLocks(copied_query, true, false);
	rewritten = QueryRewrite(copied_query);

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for REFRESH MATERIALIZED VIEW");
	query = (Query *) linitial(rewritten);

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/* Plan the query which will generate data for the refresh. */
	plan = pg_plan_query(query, "ORIOLEDB rewrite_matview", CURSOR_OPT_PARALLEL_OK, NULL);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be safe.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, "ORIOLEDB rewrite_matview",
								GetActiveSnapshot(), InvalidSnapshot,
								dest, NULL, NULL, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan */
	ExecutorRun(queryDesc, ForwardScanDirection, 0, true);

	pgstat_count_heap_insert(rel, queryDesc->estate->es_processed);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	SetMatViewPopulatedState(rel, true);
}

static void
rewrite_table(Relation rel, OTable *old_o_table, OTable *new_o_table)
{
	OTableDescr *old_descr = NULL;
	void	   *sscan;
	TupleTableSlot *old_slot;
	TupleTableSlot *new_slot;
	OTuple		tup;
	CommitSeqNo tupleCsn;
	BTreeLocationHint hint;
	OTableDescr *descr;
	OSnapshot	oSnapshot;
	OXid		oxid;
	int			primary_init_nfields = old_o_table->primary_init_nfields;

	if (!old_o_table->has_primary)
		primary_init_nfields--;

	old_descr = o_fetch_table_descr(old_o_table->oids);
	descr = relation_get_descr(rel);
	old_slot = MakeSingleTupleTableSlot(old_descr->tupdesc, &TTSOpsOrioleDB);
	new_slot = MakeSingleTupleTableSlot(descr->tupdesc, &TTSOpsOrioleDB);
	sscan = make_btree_seq_scan(&GET_PRIMARY(old_descr)->desc, &o_in_progress_snapshot, NULL);

	/*
	 * OrioleDB engine change execution order when relation is rewrited. So
	 * real data transfer from old relation ti the new one executed after
	 * dropping. So in statments with moving data from one column to another
	 * via ALTER COLUMN and DROP we gather an error that collumn already
	 * dropped. To avoid this behavior mark column dropped in current
	 * statement as not dropped. This is ugly solution actually need refactor
	 * handling of ALTER TABLE to avoid global vars and lists that brings alot
	 * of bugs.
	 */
	if (OidIsValid(o_saved_relrewrite))
	{
		for (int i = 0; i < old_slot->tts_tupleDescriptor->natts; i++)
		{
			ListCell   *lc;

			foreach(lc, dropped_attrs)
			{
				Oid			relOid = intVal(linitial((List *) lfirst(lc)));
				AttrNumber	attnum = intVal(lsecond((List *) lfirst(lc)));

				if (relOid == rel->rd_rel->oid && attnum == i + 1)
				{
					old_slot->tts_tupleDescriptor->attrs[i].attisdropped = false;
					break;
				}
			}
		}
	}

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	while (!O_TUPLE_IS_NULL(tup = btree_seq_scan_getnext(sscan, old_slot->tts_mcxt, &tupleCsn, &hint)))
	{
		tts_orioledb_store_tuple(old_slot, tup, old_descr,
								 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
								 true, &hint);
		slot_getallattrs(old_slot);
		tts_orioledb_detoast(old_slot);

		for (int i = 0; i < old_slot->tts_tupleDescriptor->natts; i++)
		{
			Node	   *expr = NULL;
			Form_pg_attribute attr = &old_slot->tts_tupleDescriptor->attrs[i];

			ListCell   *lc;

			foreach(lc, alter_type_exprs)
			{
				AttrNumber	attnum = intVal(linitial((List *) lfirst(lc)));
				Oid			relOid = intVal(lsecond((List *) lfirst(lc)));

				/*
				 * To get correct expresion we need check both relation and
				 * attribute number. Because of postgres inheritence allows
				 * different attribute number for the same column in parent
				 * and child relations.
				 */
				if (relOid == rel->rd_rel->oid && attnum == i + 1)
				{
					expr = (Node *) lthird((List *) lfirst(lc));
					break;
				}
			}

			if (!expr && attr->atthasdef && !attr->atthasmissing &&
				i >= primary_init_nfields &&
				old_slot->tts_isnull[i])
			{
				Node	   *defaultexpr = build_column_default(rel, i + 1);

				expr = defaultexpr;
			}

			/*
			 * Build new value for GENERATED column if calculating formula has
			 * been updated using ALTER TABLE ... SET EXPRESSION ... or if
			 * value was not present in existing row
			 */
			if (!expr && attr->attgenerated && (old_slot->tts_isnull[i] ||
												(o_alter_generated_column_id != NIL && list_member_int(o_alter_generated_column_id, i + 1))))
			{
				Node	   *defaultexpr = build_column_default(rel, i + 1);

				expr = defaultexpr;
			}

			if (!attr->attisdropped && !expr && DomainHasConstraints(attr->atttypid) &&
				old_slot->tts_isnull[i])
			{
				Oid			baseTypeId;
				int32		baseTypeMod;
				Oid			baseTypeColl;
				Node	   *defval;

				defval = build_column_default(rel, i + 1);

				if (!defval)
				{
					baseTypeMod = attr->atttypmod;
					baseTypeId = getBaseTypeAndTypmod(attr->atttypid, &baseTypeMod);
					baseTypeColl = get_typcollation(baseTypeId);
					defval = (Node *) makeNullConst(baseTypeId, baseTypeMod, baseTypeColl);
				}
				else
				{
					baseTypeId = exprType(defval);
				}
				defval = (Node *) coerce_to_target_type(NULL,
														defval,
														baseTypeId,
														attr->atttypid,
														attr->atttypmod,
														COERCION_ASSIGNMENT,
														COERCE_IMPLICIT_CAST,
														-1);
				if (defval == NULL) /* should not happen */
					elog(ERROR, "failed to coerce base type to domain");
				expr = defval;
			}

			attr = &new_slot->tts_tupleDescriptor->attrs[i];
			if (expr)
			{
				new_slot->tts_values[i] = o_eval_default(new_o_table, rel, expr, old_slot,
														 attr->attbyval, attr->attlen,
														 &new_slot->tts_isnull[i]);
			}
			else
			{
				if (!old_slot->tts_isnull[i])
				{
					new_slot->tts_values[i] = datumCopy(old_slot->tts_values[i], attr->attbyval, attr->attlen);
					new_slot->tts_isnull[i] = old_slot->tts_isnull[i];
				}
				else
				{
					new_slot->tts_values[i] = 0;
					new_slot->tts_isnull[i] = true;
				}
			}
		}
		new_slot->tts_nvalid = new_slot->tts_tupleDescriptor->natts;

		o_tbl_insert(descr, rel, new_slot, oxid, oSnapshot.csn);

		ExecClearTuple(old_slot);
		ExecClearTuple(new_slot);
	}

	list_free(o_alter_generated_column_id);
	o_alter_generated_column_id = NIL;

	ExecDropSingleTupleTableSlot(old_slot);
	ExecDropSingleTupleTableSlot(new_slot);
	free_btree_seq_scan(sscan);

	o_drop_table(old_o_table->oids);
}

static void
redefine_indices(Relation rel, OTable *new_o_table, bool primary, bool set_tablespace)
{
	ListCell   *index;

	foreach(index, RelationGetIndexList(rel))
	{
		bool		closed = false;
		Oid			indexOid = lfirst_oid(index);
		Relation	ind = relation_open(indexOid, AccessShareLock);

		if ((primary && ind->rd_index->indisprimary) || (!primary && !ind->rd_index->indisprimary))
		{
			OBTOptions *options = (OBTOptions *) ind->rd_options;

			if (ind->rd_rel->relam != BTREE_AM_OID || (options && !options->orioledb_index))
			{
				ReindexParams reindex_params = {0};

				relation_close(ind, AccessShareLock);
				reindex_index(
#if PG_VERSION_NUM >= 170000
							  NULL,
#endif
							  indexOid, 0, ind->rd_rel->relpersistence, &reindex_params);
				closed = true;
			}
			else
			{
				o_define_index_validate(new_o_table->oids, ind, NULL, NULL);
				relation_close(ind, AccessShareLock);
				o_define_index(rel, NULL, ind->rd_rel->oid, false, InvalidIndexNumber, set_tablespace, NULL);
				closed = true;
			}
		}
		if (!closed)
			relation_close(ind, AccessShareLock);
	}

	if (primary)
	{
		ORelOids	oids;
		OTable	   *updated_o_table;

		/*
		 * Partial reimplementation of assign_new_oids just for toast, because
		 * it isn't called for tables without pkeys here, but it should
		 */

		ORelOidsSetFromRel(oids, rel);
		updated_o_table = o_tables_get(oids);
		Assert(updated_o_table != NULL);

		if (!updated_o_table->has_primary)
		{
			Oid			toast_relid;
			Relation	toast_rel;
			OSnapshot	oSnapshot;
			OXid		oxid;

			toast_relid = rel->rd_rel->reltoastrelid;
			toast_rel = table_open(toast_relid, AccessExclusiveLock);
			RelationSetNewRelfilenode(toast_rel, toast_rel->rd_rel->relpersistence);
			ORelOidsSetFromRel(updated_o_table->toast_oids, toast_rel);
			o_tablespace_cache_add_relnode(updated_o_table->toast_oids.datoid,
										   updated_o_table->toast_oids.relnode,
										   updated_o_table->tablespace);
			table_close(toast_rel, AccessExclusiveLock);
			fill_current_oxid_osnapshot(&oxid, &oSnapshot);
			o_tables_table_meta_lock(updated_o_table);
			o_indices_update(updated_o_table, PrimaryIndexNumber, oxid, oSnapshot.csn);
			o_tables_update(updated_o_table, oxid, oSnapshot.csn);
			o_tables_after_update(updated_o_table, oxid, oSnapshot.csn);
			o_tables_table_meta_unlock(updated_o_table, InvalidOid);
			recreate_table_descr_by_oids(updated_o_table->oids);
			orioledb_free_rd_amcache(rel);
		}
		o_table_free(updated_o_table);
	}

}

void
redefine_pkey_for_rel(Relation rel)
{
	ORelOids	oids;
	OTable	   *o_table;

	ORelOidsSetFromRel(oids, rel);
	o_table = o_tables_get(oids);
	Assert(o_table != NULL);

	redefine_indices(rel, o_table, true, false);

	o_table_free(o_table);
}

static void
change_bridging_option(Relation rel, bool value, bool isReset)
{
	Oid			relid;
	Relation	pgclass;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		datum;
	bool		isnull;
	Datum		newOptions;
	Datum		repl_val[Natts_pg_class];
	bool		repl_null[Natts_pg_class];
	bool		repl_repl[Natts_pg_class];
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	DefElem    *bridging_def;

	pgclass = table_open(RelationRelationId, RowExclusiveLock);

	/* Fetch heap tuple */
	relid = RelationGetRelid(rel);
	tuple = SearchSysCacheLocked1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	/* Get the old reloptions */
	datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);

	/* Generate new proposed reloptions (text array) */
	bridging_def = makeDefElem("index_bridging", isReset ? NULL : (Node *) makeBoolean(value), -1);
	newOptions = transformRelOptions(isnull ? (Datum) 0 : datum,
									 list_make1(bridging_def), NULL, validnsps, false, isReset);

	/* Validate */
	(void) table_reloptions(rel, rel->rd_rel->relkind, newOptions, true);

	/*
	 * All we need do here is update the pg_class row; the new options will be
	 * propagated into relcaches during post-commit cache inval.
	 */
	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	if (newOptions != (Datum) 0)
		repl_val[Anum_pg_class_reloptions - 1] = newOptions;
	else
		repl_null[Anum_pg_class_reloptions - 1] = true;

	repl_repl[Anum_pg_class_reloptions - 1] = true;

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(pgclass),
								 repl_val, repl_null, repl_repl);

	CatalogTupleUpdate(pgclass, &newtuple->t_self, newtuple);
	UnlockTuple(pgclass, &tuple->t_self, InplaceUpdateTupleLock);

	heap_freetuple(newtuple);

	ReleaseSysCache(tuple);

	table_close(pgclass, RowExclusiveLock);
}

static void
add_bridge_index(Relation tbl, OTable *o_table, bool manually, Oid amoid)
{
	OSnapshot	oSnapshot;
	OXid		oxid;
	OTable	   *old_o_table;
	OTableDescr *descr;
	OTableDescr *old_descr;
	int			ix_num = InvalidIndexNumber;

	if (!manually)
	{
		HeapTuple	tuple;
		Form_pg_am	amform;

		tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for access method %u", amoid);

		amform = (Form_pg_am) GETSTRUCT(tuple);

		if (amoid != BTREE_AM_OID)
		{
			ereport(NOTICE,
					errmsg("index bridging is enabled for orioledb table '%s'",
						   RelationGetRelationName(tbl)),
					errdetail("index access method '%s' is supported only via index bridging for OrioleDB table", NameStr(amform->amname)));
		}
		else
		{
			ereport(NOTICE,
					errmsg("index bridging is enabled for orioledb table '%s'",
						   RelationGetRelationName(tbl)),
					errdetail("index access method '%s' is requested with index bridging for OrioleDB table", NameStr(amform->amname)));
		}

		ReleaseSysCache(tuple);
	}

	old_o_table = o_table;
	o_table = o_tables_get(o_table->oids);
	o_table->index_bridging = true;
	assign_new_oids(o_table, tbl, false);

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);
	o_table->primary_init_nfields = o_table->nfields + 1;

	o_tables_table_meta_lock(NULL);
	old_descr = o_fetch_table_descr(old_o_table->oids);
	recreate_o_table(old_o_table, o_table);
	descr = o_fetch_table_descr(o_table->oids);
	o_tablespace_cache_add_table(o_table);
	rebuild_indices_insert_placeholders(descr);
	o_tables_table_meta_unlock(NULL, InvalidOid);

	rebuild_indices(old_o_table, old_descr, o_table, descr, false, NULL);
	o_tables_rel_meta_lock(tbl);
	for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
	{
		int			ctid_idx_off;
		OTableIndex *index;

		ctid_idx_off = o_table->has_primary ? 0 : 1;
		index = &o_table->indices[ix_num];

		o_indices_update(o_table, ix_num + ctid_idx_off, oxid, oSnapshot.csn);
		o_invalidate_oids(index->oids);
		o_add_invalidate_undo_item(index->oids, O_INVALIDATE_OIDS_ON_ABORT);
	}
	o_tables_update(o_table, oxid, oSnapshot.csn);
	o_tables_rel_meta_unlock(tbl, InvalidOid);
	o_invalidate_oids(o_table->bridge_oids);
	o_add_invalidate_undo_item(o_table->bridge_oids, O_INVALIDATE_OIDS_ON_ABORT);
	o_invalidate_oids(o_table->oids);
	o_add_invalidate_undo_item(o_table->oids, O_INVALIDATE_OIDS_ON_ABORT);

	change_bridging_option(tbl, true, false);

	o_table_free(old_o_table);
	o_table_free(o_table);
}

static void
drop_bridge_index(Relation tbl, OTable *o_table)
{
	OSnapshot	oSnapshot;
	OXid		oxid;
	OTable	   *old_o_table;
	OTableDescr *descr;
	OTableDescr *old_descr;
	int			ix_num = InvalidIndexNumber;

	old_o_table = o_table;
	o_table = o_tables_get(o_table->oids);
	o_table->index_bridging = false;
	ORelOidsSetInvalid(o_table->bridge_oids);
	assign_new_oids(o_table, tbl, false);

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);
	o_table->primary_init_nfields = o_table->nfields - 1;

	o_tables_table_meta_lock(NULL);
	old_descr = o_fetch_table_descr(old_o_table->oids);
	recreate_o_table(old_o_table, o_table);
	descr = o_fetch_table_descr(o_table->oids);
	o_tablespace_cache_add_table(o_table);
	rebuild_indices_insert_placeholders(descr);
	o_tables_table_meta_unlock(NULL, InvalidOid);

	rebuild_indices(old_o_table, old_descr, o_table, descr, false, NULL);
	o_tables_rel_meta_lock(tbl);
	for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
	{
		int			ctid_idx_off;
		OTableIndex *index;

		ctid_idx_off = o_table->has_primary ? 0 : 1;
		index = &o_table->indices[ix_num];

		o_indices_update(o_table, ix_num + ctid_idx_off, oxid, oSnapshot.csn);
		o_invalidate_oids(index->oids);
		o_add_invalidate_undo_item(index->oids, O_INVALIDATE_OIDS_ON_ABORT);
	}
	o_tables_update(o_table, oxid, oSnapshot.csn);
	o_tables_rel_meta_unlock(tbl, InvalidOid);
	o_invalidate_oids(old_o_table->bridge_oids);
	o_add_invalidate_undo_item(old_o_table->bridge_oids, O_INVALIDATE_OIDS_ON_ABORT);
	o_invalidate_oids(o_table->oids);
	o_add_invalidate_undo_item(o_table->oids, O_INVALIDATE_OIDS_ON_ABORT);

	change_bridging_option(tbl, false, true);

	o_table_free(old_o_table);
	o_table_free(o_table);
}

static void
cleanup_tablespace_dir(char *tablespace_path)
{
	DIR		   *dir;
	struct dirent *file;

	dir = opendir(tablespace_path);
	if (dir == NULL)
		return;

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		Oid			dbOid;
		char	   *dbDirName;

		if (sscanf(file->d_name, "%u", &dbOid) != 1)
			continue;

		dbDirName = psprintf("%s/%u", tablespace_path, dbOid);

		/* We assume that postgres throws it's own errors on not empty dirs */
		if (rmdir(dbDirName) < 0 && errno != ENOTEMPTY)
		{
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not remove orioledb db dir \"%s\": %m",
							dbDirName)));
		}
		pfree(dbDirName);
	}
	fsync_fname_ext(tablespace_path, true, false, FATAL);

	/* We assume that postgres throws it's own errors on not empty dirs */
	if (rmdir(tablespace_path) < 0 && errno != ENOTEMPTY)
	{
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not remove tablespace orioledb dir \"%s\": %m",
						tablespace_path)));
	}

	/* We assume that postgres throws it's own errors on not empty dirs */
	if (errno != 0 && errno != ENOTEMPTY)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("unable to clean up orioledb tablespace: %m")));
	}
	closedir(dir);
}

/*
 * get_collation		- fetch qualified name of a collation
 *
 * If collation is InvalidOid or is the default for the given actual_datatype,
 * then the return value is NIL.
 */
static List *
get_collation(Oid collation, Oid actual_datatype)
{
	List	   *result;
	HeapTuple	ht_coll;
	Form_pg_collation coll_rec;
	char	   *nsp_name;
	char	   *coll_name;

	if (!OidIsValid(collation))
		return NIL;				/* easy case */
	if (collation == get_typcollation(actual_datatype))
		return NIL;				/* just let it default */

	ht_coll = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
	if (!HeapTupleIsValid(ht_coll))
		elog(ERROR, "cache lookup failed for collation %u", collation);
	coll_rec = (Form_pg_collation) GETSTRUCT(ht_coll);

	/* For simplicity, we always schema-qualify the name */
	nsp_name = get_namespace_name(coll_rec->collnamespace);
	coll_name = pstrdup(NameStr(coll_rec->collname));
	/* cppcheck-suppress unknownEvaluationOrder */
	result = list_make2(makeString(nsp_name), makeString(coll_name));

	ReleaseSysCache(ht_coll);
	return result;
}

/*
 * get_opclass			- fetch qualified name of an index operator class
 *
 * If the opclass is the default for the given actual_datatype, then
 * the return value is NIL.
 */
static List *
get_opclass(Oid opclass, Oid actual_datatype)
{
	List	   *result = NIL;
	HeapTuple	ht_opc;
	Form_pg_opclass opc_rec;

	ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	if (!HeapTupleIsValid(ht_opc))
		elog(ERROR, "cache lookup failed for opclass %u", opclass);
	opc_rec = (Form_pg_opclass) GETSTRUCT(ht_opc);

	if (GetDefaultOpClass(actual_datatype, opc_rec->opcmethod) != opclass)
	{
		/* For simplicity, we always schema-qualify the name */
		char	   *nsp_name = get_namespace_name(opc_rec->opcnamespace);
		char	   *opc_name = pstrdup(NameStr(opc_rec->opcname));

		/* cppcheck-suppress unknownEvaluationOrder */
		result = list_make2(makeString(nsp_name), makeString(opc_name));
	}

	ReleaseSysCache(ht_opc);
	return result;
}

static void
orioledb_object_access_hook(ObjectAccessType access, Oid classId, Oid objectId,
							int subId, void *arg)
{
	Relation	rel;

	if (access == OAT_POST_CREATE && classId == ExtensionRelationId)
	{
#if PG_VERSION_NUM >= 170000
		if (IsTransactionState())
		{
			XLogRecPtr	cur_lsn;

			o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
			o_database_cache_add_if_needed(Template1DbOid, Template1DbOid, cur_lsn, NULL);
		}
#endif
	}
	else if (access == OAT_DROP && classId == RelationRelationId)
	{
		ObjectAccessDrop *drop_arg = (ObjectAccessDrop *) arg;

		ASAN_UNPOISON_MEMORY_REGION(drop_arg, sizeof(*drop_arg));

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
				(subId == 0) && is_orioledb_rel(rel) &&
				!OidIsValid(rel->rd_rel->relrewrite))
			{
				ListCell   *lc;
				ORelOids	oids;

				ORelOidsSetFromRel(oids, rel);
				Assert(relation_get_descr(rel) != NULL);
				foreach(lc, partition_drop_index_list)
				{
					List	   *drop_oids = (List *) lfirst(lc);

					if (lsecond_oid(drop_oids) == rel->rd_rel->oid)
						partition_drop_index_list = foreach_delete_current(partition_drop_index_list, lc);
				}
				o_drop_table(oids);
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId != 0) && is_orioledb_rel(rel))
			{
				OTable	   *o_table;
				OTableField *o_field = NULL;
				ORelOids	oids;

				ORelOidsSetFromRel(oids, rel);
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
						OSnapshot	oSnapshot;
						OXid		oxid;

						o_field->droped = true;

						fill_current_oxid_osnapshot(&oxid, &oSnapshot);
						o_tables_rel_meta_lock(rel);
						o_indices_update(o_table, PrimaryIndexNumber, oxid, oSnapshot.csn);
						o_tables_update(o_table, oxid, oSnapshot.csn);
						o_tables_after_update(o_table, oxid, oSnapshot.csn);
						o_tables_rel_meta_unlock(rel, InvalidOid);
						/* cppcheck-suppress unknownEvaluationOrder */
						dropped_attrs = lappend(dropped_attrs, list_make2(makeInteger(objectId), makeInteger(subId)));
					}
					o_table_free(o_table);
				}
			}
			else if (rel->rd_rel->relkind == RELKIND_INDEX &&
					 !(drop_arg->dropflags & PERFORM_DELETION_OF_RELATION))
			{
				/*
				 * dropflags == PERFORM_DELETION_OF_RELATION ignored, to not
				 * drop indices when whole table dropped
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
					ix_num = o_find_ix_num_by_name(descr,
												   rel->rd_rel->relname.data);
					if (ix_num != InvalidIndexNumber)
					{
						String	   *relname;

						if (descr->indices[ix_num]->primaryIsCtid)
							ix_num--;
						relation_close(rel, AccessShareLock);
						is_open = false;

						relname = makeString(rel->rd_rel->relname.data);
						if (list_member_oid(o_reuse_indices, objectId))
						{
							/* Do not drop index if it is set for reuse */
							elog(DEBUG1, "object_access_hook: skipping index %d drop as it is set for reuse", objectId);
						}
						else if (!(drop_arg->dropflags &
								   PERFORM_DELETION_INTERNAL) ||
								 list_member(drop_index_list, relname))
						{
							drop_index_list = list_delete(drop_index_list,
														  relname);
							o_index_drop(tbl, ix_num);
						}
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
			else if ((rel->rd_rel->relkind == RELKIND_INDEX) &&
					 (drop_arg->dropflags & PERFORM_DELETION_OF_RELATION))
			{
				Relation	tbl = relation_open(rel->rd_index->indrelid,
												AccessShareLock);

				if ((tbl->rd_rel->relkind == RELKIND_RELATION ||
					 tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
					is_orioledb_rel(tbl))
				{
					/*
					 * TODO: probably better way would be to add hook to
					 * findDependentObjects and filter partition index
					 * dependencies there, but for now
					 * PERFORM_DELETION_OF_RELATION passed for partiton index
					 * dependency and I'm not sure how to properly filter out
					 * only this kind of dependency and do not touch behaviour
					 * that not drops indices during table drop to not rebuild
					 * them
					 */

					Relation	depRel;
					ObjectAddress object;
					ScanKeyData key[2];
					int			nkeys;
					SysScanDesc scan;
					HeapTuple	tup;

					depRel = table_open(DependRelationId, RowExclusiveLock);

					object.classId = classId;
					object.objectId = objectId;
					object.objectSubId = subId;

					ScanKeyInit(&key[0],
								Anum_pg_depend_classid,
								BTEqualStrategyNumber, F_OIDEQ,
								ObjectIdGetDatum(object.classId));
					ScanKeyInit(&key[1],
								Anum_pg_depend_objid,
								BTEqualStrategyNumber, F_OIDEQ,
								ObjectIdGetDatum(object.objectId));
					nkeys = 2;

					scan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, nkeys, key);

					while (HeapTupleIsValid(tup = systable_getnext(scan)))
					{
						Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);

						if (foundDep->deptype == DEPENDENCY_PARTITION_PRI ||
							foundDep->deptype == DEPENDENCY_PARTITION_SEC)
						{
							partition_drop_index_list = list_append_unique(partition_drop_index_list,
							/* cppcheck-suppress unknownEvaluationOrder */
																		   list_make2_oid(rel->rd_rel->oid,
																						  rel->rd_index->indrelid));
							break;
						}
					}

					systable_endscan(scan);

					table_close(depRel, RowExclusiveLock);
				}
				relation_close(tbl, AccessShareLock);
			}
			else if (rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
			{
				ListCell   *lc;
				Relation	tbl = relation_open(rel->rd_index->indrelid,
												AccessShareLock);

				/*
				 * We don't have secondary index dependencies at this moment
				 * so we are passing them in partition_drop_index_list from
				 * before
				 */
				if (partition_drop_index_list != NIL)
				{
					foreach(lc, partition_drop_index_list)
					{
						List	   *oids = (List *) lfirst(lc);
						Relation	part_tbl = relation_open(lsecond_oid(oids), AccessShareLock);
						OIndexNumber ix_num;
						OTableDescr *descr;
						int			i;

						Assert((part_tbl->rd_rel->relkind == RELKIND_RELATION ||
								part_tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
							   is_orioledb_rel(part_tbl));

						descr = relation_get_descr(part_tbl);
						Assert(descr != NULL);

						ix_num = InvalidIndexNumber;
						for (i = 0; i < descr->nIndices; i++)
						{
							if (descr->indices[i]->oids.reloid == linitial_oid(oids))
							{
								ix_num = i;
								break;
							}
						}
						if (ix_num != InvalidIndexNumber)
						{
							if (descr->indices[ix_num]->primaryIsCtid)
								ix_num--;

							o_index_drop(part_tbl, ix_num);
						}
						relation_close(part_tbl, AccessShareLock);
					}
					list_free(partition_drop_index_list);
					partition_drop_index_list = NIL;
				}
				relation_close(tbl, AccessShareLock);
			}
			if (is_open)
				relation_close(rel, AccessShareLock);
		}
	}
	else if (access == OAT_DROP && classId == DatabaseRelationId)
	{
		OSnapshot	oSnapshot;
		OXid		oxid;

		Assert(OidIsValid(objectId));

		fill_current_oxid_osnapshot(&oxid, &oSnapshot);

		o_tables_table_meta_lock(NULL);
		o_tables_drop_all(oxid, oSnapshot.csn, objectId);
		o_tables_table_meta_unlock(NULL, InvalidOid);
	}
	else if (access == OAT_DROP && classId == TypeRelationId &&
			 ActiveSnapshotSet())
	{
		OSnapshot	oSnapshot;
		OXid		oxid;
		Form_pg_type typeform;
		HeapTuple	tuple = NULL;

		Assert(OidIsValid(objectId));

		fill_current_oxid_osnapshot_no_check(&oxid, &oSnapshot);

		o_tables_drop_columns_by_type(oxid, oSnapshot.csn, objectId);

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
				o_enum_cache_delete_all(MyDatabaseId, typeform->oid);
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
		bool		closed = false;

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
				OTableField *field;
				Form_pg_attribute attr;
				OTable	   *o_table;
				ORelOids	oids;
				OSnapshot	oSnapshot;
				OXid		oxid;

				ORelOidsSetFromRel(oids, rel);

				o_table = o_tables_get(oids);
				if (o_table == NULL)
				{
					/* table does not exist */
					elog(NOTICE, "orioledb table \"%s\" not found", RelationGetRelationName(rel));
				}
				else
				{
					fill_current_oxid_osnapshot(&oxid, &oSnapshot);

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

					o_tables_rel_meta_lock(rel);
					o_indices_update(o_table, PrimaryIndexNumber, oxid, oSnapshot.csn);
					o_tables_update(o_table, oxid, oSnapshot.csn);
					o_tables_after_update(o_table, oxid, oSnapshot.csn);
					o_tables_rel_meta_unlock(rel, InvalidOid);

					o_table_free(o_table);

					o_in_add_column = true;
				}
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId == 0) && is_orioledb_rel(rel))
			{
				if (!OidIsValid(rel->rd_rel->relrewrite))
				{
					create_o_table_for_rel(rel);
				}
				else
				{
					Relation	old_rel = relation_open(rel->rd_rel->relrewrite, AccessShareLock);

					o_saved_relrewrite = rel->rd_rel->relrewrite;
					ORelOidsSetFromRel(saved_oids, old_rel);
					relation_close(old_rel, AccessShareLock);
				}
			}
			else if ((rel->rd_rel->relkind == RELKIND_TOASTVALUE) &&
					 (subId == 0) && !OidIsValid(rel->rd_rel->relrewrite))
			{
				Oid			tbl_oid;
				Relation	tbl = NULL;

				/* This is faster than dependency scan */
				tbl_oid = pg_strtoint64(strrchr(rel->rd_rel->relname.data,
												'_') + 1);

				tbl = try_table_open(tbl_oid, AccessShareLock);
				if (tbl && is_orioledb_rel(tbl))
				{
					set_toast_oids_and_options(tbl, rel, false, false);
				}
				if (tbl)
					table_close(tbl, AccessShareLock);
			}
			else if (rel->rd_rel->relkind == RELKIND_INDEX)
			{
				/* Checks and adds bridged indexes */
				Relation	tbl;

				CommandCounterIncrement();
				tbl = relation_open(rel->rd_index->indrelid, AccessShareLock);

				if ((tbl->rd_rel->relkind == RELKIND_RELATION ||
					 tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
					is_orioledb_rel(tbl))
				{
					OSnapshot	oSnapshot;
					OXid		oxid;
					OTable	   *o_table;
					ORelOids	table_oids;

					fill_current_oxid_osnapshot(&oxid, &oSnapshot);

					ORelOidsSetFromRel(table_oids, tbl);
					o_table = o_tables_get(table_oids);
					if (o_table == NULL)
					{
						elog(NOTICE, "orioledb table %s not found",
							 RelationGetRelationName(tbl));
					}
					else
					{
						int			ix_num = InvalidIndexNumber;
						int			i;
						bool		add_bridging = false;
						bool		btree_bridging = false;

						for (i = 0; i < o_table->nindices; i++)
						{
							if (strcmp(o_table->indices[i].name.data, rel->rd_rel->relname.data) == 0)
							{
								ix_num = i;
								break;
							}
						}

						Assert(rel->rd_rel->relkind == RELKIND_INDEX);

						/* In case of index reuse, update the index oid */
						if (ix_num != InvalidIndexNumber && list_member_oid(o_reuse_indices, o_table->indices[ix_num].oids.reloid))
						{
							Oid			old_oid = o_table->indices[ix_num].oids.reloid;

							elog(DEBUG1, "object_access_hook: updating index oid %d to %d", old_oid, objectId);
							o_table->indices[ix_num].oids.reloid = objectId;
							o_tables_rel_meta_lock(tbl);
							o_tables_update(o_table, oxid, oSnapshot.csn);
							o_tables_rel_meta_unlock(tbl, InvalidOid);
							o_invalidate_oids(o_table->oids);
							o_reuse_indices = list_delete_oid(o_reuse_indices, old_oid);
							drop_index_list = list_delete(drop_index_list, makeString(rel->rd_rel->relname.data));
						}

						if (!o_table->index_bridging)
						{
							if (rel->rd_rel->relam == BTREE_AM_OID)
							{
								OBTOptions *options = (OBTOptions *) rel->rd_options;

								if (options && !options->orioledb_index)
								{
									add_bridging = true;
									btree_bridging = true;
								}
							}
							else
							{
								add_bridging = true;
							}
						}
						else if (rel->rd_rel->relam == BTREE_AM_OID)
						{
							if (!in_rewrite && !rel->rd_index->indisprimary && ix_num == InvalidIndexNumber)
							{
								OBTOptions *options = (OBTOptions *) rel->rd_options;

								if (options && !options->orioledb_index)
									btree_bridging = true;
							}
						}

						if (btree_bridging)
						{
							ereport(WARNING,
									errcode(ERRCODE_WARNING),
									errmsg("using bridged btree index for orioledb"),
									errdetail("This feature is intended for testing purposes and is not recommended for normal usage."));
						}

						if (add_bridging)
							add_bridge_index(tbl, o_table, false, rel->rd_rel->relam);
						else
							o_table_free(o_table);
					}
				}
				relation_close(tbl, AccessShareLock);
			}
			if (!closed)
				relation_close(rel, AccessShareLock);
		}
	}
	else if (access == OAT_POST_CREATE && classId == AttrDefaultRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if (rel != NULL && (rel->rd_rel->relkind == RELKIND_RELATION) &&
			(subId != 0) && is_orioledb_rel(rel))
		{
			Form_pg_attribute attr;
			OTable	   *o_table;
			ORelOids	oids;
			OSnapshot	oSnapshot;
			OXid		oxid;

			ORelOidsSetFromRel(oids, rel);
			o_table = o_tables_get(oids);
			if (o_table == NULL)
			{
				/* table does not exist */
				elog(NOTICE, "orioledb table \"%s\" not found",
					 RelationGetRelationName(rel));
			}
			else
			{
				OTableField old_field;
				OTableField *field;
				bool		changed;

				old_field = o_table->fields[subId - 1];
				CommandCounterIncrement();
				field = &o_table->fields[subId - 1];
				attr = &rel->rd_att->attrs[subId - 1];
				orioledb_attr_to_field(field, attr);

				/* TODO: Probably use CheckIndexCompatible here */
				changed = old_field.typid != field->typid ||
					old_field.typmod != field->typmod ||
					old_field.collation != field->collation;

				if (changed)
				{
					if (ATColumnChangeRequiresRewrite(&old_field, field, objectId,
													  subId))
						in_rewrite = true;
				}

				if (!in_rewrite)
				{
					o_table_fill_constr(o_table, rel, subId - 1,
										&old_field, field);

					fill_current_oxid_osnapshot(&oxid, &oSnapshot);
					o_tables_rel_meta_lock(rel);
					o_indices_update(o_table, PrimaryIndexNumber, oxid, oSnapshot.csn);
					o_tables_update(o_table, oxid, oSnapshot.csn);
					o_tables_after_update(o_table, oxid, oSnapshot.csn);
					o_tables_rel_meta_unlock(rel, InvalidOid);

					/* This has no effect? */
					o_table->fields[subId - 1] = old_field;
					o_table_free(o_table);
				}
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
				OTable	   *o_table;
				ORelOids	oids;

				ORelOidsSetFromRel(oids, rel);
				o_table = o_tables_get(oids);
				if (o_table == NULL)
				{
					/* table does not exist */
					elog(NOTICE, "orioledb table \"%s\" not found",
						 RelationGetRelationName(rel));
				}
				else
				{
					OTableField old_field;
					OTableField *field;
					Form_pg_attribute attr;
					OSnapshot	oSnapshot;
					OXid		oxid;
					int			ix_num;
					bool		changed_ty;

					old_field = o_table->fields[subId - 1];
					CommandCounterIncrement();
					field = &o_table->fields[subId - 1];
					attr = &rel->rd_att->attrs[subId - 1];
					orioledb_attr_to_field(field, attr);

					/* TODO: Probably use CheckIndexCompatible here */
					changed_ty = old_field.typid != field->typid ||
						old_field.typmod != field->typmod ||
						old_field.collation != field->collation;

					if (changed_ty)
					{
						if (ATColumnChangeRequiresRewrite(&old_field, field, objectId,
														  subId))
							in_rewrite = true;
					}

					/*
					 * Alter table on generated column triggers table rewrite
					 * due to need of recalculating column value for existing
					 * rows
					 */
					if (old_field.generated)
					{
						in_rewrite = true;
						o_alter_generated_column_id = lappend_int(o_alter_generated_column_id, subId);
					}

					if (!in_rewrite)
					{
						orioledb_save_collation(field->collation);
						fill_current_oxid_osnapshot(&oxid, &oSnapshot);
						o_tables_rel_meta_lock(rel);
						for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
						{
							bool		compatible = false;
							int			field_num;
							int			ctid_idx_off;
							OTableIndex *o_table_index;
							List	   *attributeList = NIL;
							int			expr_field = 0;
							ListCell   *indexpr;

							ctid_idx_off = o_table->has_primary ? 0 : 1;
							o_table_index = &o_table->indices[ix_num];
							indexpr = list_head(o_table_index->expressions);

							for (field_num = 0; field_num < o_table_index->nkeyfields; field_num++)
							{
								IndexElem  *iparam;
								OTableIndexField *iField = &o_table_index->fields[field_num];
								int			attnum = iField->attnum;
								OTableField *table_field;

								iparam = makeNode(IndexElem);
								if (attnum != EXPR_ATTNUM)
								{
									table_field = &o_table->fields[attnum];
									iparam->name = table_field->name.data;
									iparam->expr = NULL;
								}
								else
								{
									table_field = &o_table_index->exprfields[expr_field++];
									iparam->name = NULL;
									iparam->expr = lfirst(indexpr);
									indexpr = lnext(o_table_index->expressions, indexpr);
								}

								iparam->collation = get_collation(table_field->collation, table_field->typid);
								iparam->opclass = get_opclass(iField->opclass, table_field->typid);
								iparam->ordering = iField->ordering;
								iparam->nulls_ordering = iField->nullsOrdering;

								attributeList = lappend(attributeList, iparam);
							}

							compatible = CheckIndexCompatible(o_table_index->oids.reloid, "btree", attributeList, NIL);

							for (field_num = 0; field_num < o_table_index->nkeyfields;
								 field_num++)
							{
								bool		has_field;

								has_field = o_table_index->fields[field_num].attnum ==
									subId - 1;
								if (o_table_index->type == oIndexPrimary || has_field)
								{
									o_indices_update(o_table,
													 ix_num + ctid_idx_off,
													 oxid, oSnapshot.csn);
									o_invalidate_oids(o_table_index->oids);
									o_add_invalidate_undo_item(o_table_index->oids,
															   O_INVALIDATE_OIDS_ON_ABORT);
								}
								if (changed_ty && has_field && (o_table_index->type == oIndexPrimary || !compatible))
								{
									String	   *ix_name;

									ix_name =
										makeString(pstrdup(o_table_index->name.data));
									drop_index_list =
										list_append_unique(drop_index_list,
														   ix_name);
								}
							}
						}
						o_indices_update(o_table, PrimaryIndexNumber, oxid, oSnapshot.csn);
						o_tables_update(o_table, oxid, oSnapshot.csn);
						o_tables_after_update(o_table, oxid, oSnapshot.csn);
						o_tables_rel_meta_unlock(rel, InvalidOid);
					}
					o_table->fields[subId - 1] = old_field;
					o_table_free(o_table);
				}
			}
			else if (rel->rd_rel->relkind == RELKIND_INDEX)
			{
				Relation	tbl = relation_open(rel->rd_index->indrelid,
												AccessShareLock);

				if ((tbl->rd_rel->relkind == RELKIND_RELATION ||
					 tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
					is_orioledb_rel(tbl))
				{
					OTable	   *o_table;
					ORelOids	table_oids;

					ORelOidsSetFromRel(table_oids, tbl);
					o_table = o_tables_get(table_oids);
					if (o_table == NULL)
					{
						elog(NOTICE, "orioledb table %s not found",
							 RelationGetRelationName(tbl));
					}
					else if (rel->rd_rel->relam == BTREE_AM_OID &&
							 !(rel->rd_options &&
							   !((OBTOptions *) rel->rd_options)->orioledb_index))
					{
						int			ix_num;
						OSnapshot	oSnapshot;
						OXid		oxid;
						ORelOids	idx_oids;
						OBTOptions *options = (OBTOptions *) rel->rd_options;

						ORelOidsSetFromRel(idx_oids, rel);
						CommandCounterIncrement();
						if (rel->rd_options && !((OBTOptions *) rel->rd_options)->orioledb_index)
							elog(ERROR, "Cannot change 'orioledb_index' option for existing indices");
						for (ix_num = 0; ix_num < o_table->nindices; ix_num++)
						{
							OTableIndex *index = &o_table->indices[ix_num];

							if (ORelOidsIsEqual(index->oids, idx_oids))
							{
								namestrcpy(&index->name,
										   rel->rd_rel->relname.data);
								if (options)
									index->fillfactor = options->bt_options.fillfactor;
								break;
							}
						}
						Assert(ix_num < o_table->nindices);
						if (o_table->indices[ix_num].tablespace == rel->rd_rel->reltablespace)
						{
							fill_current_oxid_osnapshot(&oxid, &oSnapshot);
							o_tables_rel_meta_lock(tbl);
							o_indices_update(o_table, ix_num, oxid, oSnapshot.csn);
							o_tables_update(o_table, oxid, oSnapshot.csn);
							o_tables_rel_meta_unlock(tbl, InvalidOid);
							o_invalidate_oids(idx_oids);
							o_add_invalidate_undo_item(idx_oids,
													   O_INVALIDATE_OIDS_ON_ABORT);
							if (!ORelOidsIsEqual(idx_oids, table_oids))
							{
								o_invalidate_oids(table_oids);
								o_add_invalidate_undo_item(table_oids,
														   O_INVALIDATE_OIDS_ON_ABORT);
							}
							o_table_free(o_table);
						}
						else
						{
							o_index_drop(tbl, ix_num);
							o_table_free(o_table);
							o_define_index(tbl, rel, InvalidOid, false, InvalidIndexNumber, false, NULL);
						}
					}
					else if (rel->rd_options)
					{
						bool		old_orioledb_index = false;
						bool		new_orioledb_index = false;

						switch (rel->rd_amhandler)
						{
							case F_BTHANDLER:
								old_orioledb_index = ((OBTOptions *) rel->rd_options)->orioledb_index;
								CommandCounterIncrement();
								new_orioledb_index = rel->rd_options &&
									((OBTOptions *) rel->rd_options)->orioledb_index;
								break;
							default:
								break;
						}
						if (old_orioledb_index != new_orioledb_index || !rel->rd_options)
							elog(ERROR, "Cannot change 'orioledb_index' option for existing indices");

					}
				}
				relation_close(tbl, AccessShareLock);
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION ||
					  rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 (subId == 0))
			{
				/*
				 * We come here during "ALTER TABLE ... SET TABLESPACE" after
				 * orioledb_relation_copy_data
				 */
				if (is_orioledb_rel(rel))
				{
					ORelOids	old_oids;
					Oid			old_reltablespace = rel->rd_rel->reltablespace;

					ORelOidsSetFromRel(old_oids, rel);
					CommandCounterIncrement();
					if (old_reltablespace != rel->rd_rel->reltablespace)
					{
						o_saved_reltablespace = old_reltablespace;
						saved_oids = old_oids;
					}
				}
			}
			else if (rel->rd_rel->relkind == RELKIND_TOASTVALUE &&
					 OidIsValid(o_saved_relrewrite) &&
					 OidIsValid(rel->rd_rel->relrewrite) &&
					 (subId == 0))
			{
				Relation	tbl = NULL;

				tbl = table_open(o_saved_relrewrite, AccessShareLock);
				if (is_orioledb_rel(tbl))
				{
					ORelOids	new_oids;
					OTable	   *old_o_table,
							   *new_o_table;

					CommandCounterIncrement();

					old_o_table = o_tables_get(saved_oids);
					Assert(old_o_table != NULL);

					create_o_table_for_rel(tbl);

					set_toast_oids_and_options(tbl, rel, false, old_o_table->index_bridging);

					ORelOidsSetFromRel(new_oids, tbl);
					new_o_table = o_tables_get(new_oids);
					Assert(new_o_table != NULL);

					relation_close(tbl, AccessShareLock);
					CommandCounterIncrement();
					tbl = relation_open(o_saved_relrewrite, AccessShareLock);

					/*
					 * Redefinig primary key here to not do rebuild after
					 * rewrite_table
					 */
					redefine_indices(tbl, new_o_table, true, false);

					o_table_free(new_o_table);
					new_o_table = o_tables_get(new_oids);
					Assert(new_o_table != NULL);

					switch (tbl->rd_rel->relkind)
					{
						case RELKIND_RELATION:
							rewrite_table(tbl, old_o_table, new_o_table);
							break;
						case RELKIND_MATVIEW:
							o_saved_relrewrite = InvalidOid;
							if (savedDataQuery != NULL)
								rewrite_matview(tbl, old_o_table, new_o_table);
							o_drop_table(old_o_table->oids);
							break;
						default:
							Assert(false);
							break;
					}

					redefine_indices(tbl, new_o_table, false, false);

					o_table_free(old_o_table);
					o_table_free(new_o_table);
					o_saved_relrewrite = InvalidOid;
				}
				table_close(tbl, AccessShareLock);
			}
			else if (rel->rd_rel->relkind == RELKIND_TOASTVALUE &&
					 (subId == 0))
			{
				Oid			tbl_oid;
				Relation	tbl = NULL;

				/* This is faster than dependency scan */
				tbl_oid = pg_strtoint64(strrchr(rel->rd_rel->relname.data,
												'_') + 1);
				CommandCounterIncrement();

				tbl = try_table_open(tbl_oid, AccessShareLock);
				if (tbl && is_orioledb_rel(tbl))
				{
					ORelOids	oids;
					OTableDescr *descr;
					ORelOptions *options = (ORelOptions *) tbl->rd_options;
					uint8		new_fillfactor;
					bool		new_index_bridging;

					CommandCounterIncrement();
					ORelOidsSetFromRel(oids, tbl);
					if (o_saved_reltablespace != rel->rd_rel->reltablespace)
					{
						/*
						 * We come here during "ALTER TABLE ... SET
						 * TABLESPACE"
						 */
						OTable	   *old_o_table,
								   *new_o_table;

						Assert(ORelOidsIsValid(saved_oids));
						old_o_table = o_tables_get(saved_oids);
						Assert(old_o_table != NULL);

						create_o_table_for_rel(tbl);

						set_toast_oids_and_options(tbl, rel, false, old_o_table->index_bridging);

						new_o_table = o_tables_get(oids);
						Assert(new_o_table != NULL);

						relation_close(tbl, AccessShareLock);
						CommandCounterIncrement();
						tbl = relation_open(tbl_oid, AccessShareLock);

						/*
						 * Redefinig primary key here to not do rebuild after
						 * rewrite_table
						 */
						redefine_indices(tbl, new_o_table, true, true);

						o_table_free(new_o_table);
						new_o_table = o_tables_get(oids);
						Assert(new_o_table != NULL);

						switch (tbl->rd_rel->relkind)
						{
							case RELKIND_RELATION:
							case RELKIND_MATVIEW:

								/*
								 * for matview we just copy data to not
								 * recalculate expressions
								 */
								Assert(alter_type_exprs == NIL);
								rewrite_table(tbl, old_o_table, new_o_table);
								break;
							default:
								Assert(false);
								break;
						}

						redefine_indices(tbl, new_o_table, false, true);

						o_table_free(old_o_table);
						o_table_free(new_o_table);

						o_saved_reltablespace = InvalidOid;
					}
					else
					{
						/*
						 * We come here during "ALTER TABLE ... SET <OPTION>"
						 */
						descr = o_fetch_table_descr(oids);
						Assert(descr);

						if (options)
							new_fillfactor = options->std_options.fillfactor;
						else
							new_fillfactor = BTREE_DEFAULT_FILLFACTOR;

						if (options)
							new_index_bridging = options->index_bridging;
						else
							new_index_bridging = false;

						if (GET_PRIMARY(descr)->bridging != new_index_bridging)
						{
							OTable	   *o_table;
							ORelOids	table_oids;
							ListCell   *index;
							bool		has_bridged = false;

							foreach(index, RelationGetIndexList(tbl))
							{
								Oid			indexOid = lfirst_oid(index);
								Relation	ind = relation_open(indexOid, AccessShareLock);
								OBTOptions *options = (OBTOptions *) ind->rd_options;

								if (ind->rd_rel->relam != BTREE_AM_OID || (options && !options->orioledb_index))
									has_bridged = true;
								relation_close(ind, AccessShareLock);
								if (has_bridged)
									break;
							}

							ORelOidsSetFromRel(table_oids, tbl);
							o_table = o_tables_get(table_oids);
							if (o_table == NULL)
							{
								elog(ERROR, "orioledb table %s not found",
									 RelationGetRelationName(tbl));
							}

							if (!has_bridged)
							{
								if (new_index_bridging)
									add_bridge_index(tbl, o_table, true, InvalidOid);
								else
									drop_bridge_index(tbl, o_table);
							}
							else
								elog(ERROR, "cannot disable 'index_bridging' for a table with bridged indices");
						}
						if (GET_PRIMARY(descr)->fillfactor != new_fillfactor)
							set_toast_oids_and_options(tbl, rel, true, false);
					}
				}
				if (tbl)
					table_close(tbl, AccessShareLock);
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
				{
					XLogRecPtr	cur_lsn;
					Oid			datoid;

					CommandCounterIncrement();
					o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
					o_enum_cache_add_all(datoid, objectId, cur_lsn);
				}
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
	else if (access == OAT_POST_CREATE && classId == DatabaseRelationId)
	{
		HeapTuple	dbTuple;
		Form_pg_database dbform;
		int32		cluster_encoding;


		if (IsTransactionState())
		{
			XLogRecPtr	cur_lsn;

			o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
			o_database_cache_add_if_needed(Template1DbOid, Template1DbOid, cur_lsn, NULL);
		}

		CommandCounterIncrement();
		dbTuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(objectId));

		if (!HeapTupleIsValid(dbTuple))
			elog(ERROR, "cache lookup failed for database %u", objectId);
		dbform = (Form_pg_database) GETSTRUCT(dbTuple);

		cluster_encoding = o_database_cache_get_database_encoding();
		if (cluster_encoding != dbform->encoding)
			ereport(ERROR,
					errmsg("Cannot create database with encoding \"%s\" "
						   "that is different from cluster encoding \"%s\"",
						   pg_encoding_to_char(dbform->encoding),
						   pg_encoding_to_char(cluster_encoding)),
					errdetail("OrioleDB now only supports single encoding for all databases. "
							  "It is easier to use single one during checkpoint."));
		ReleaseSysCache(dbTuple);
	}
	else if (access == OAT_POST_ALTER && classId == IndexRelationId)
	{
		bool		old_indisprimary;

		rel = relation_open(objectId, AccessShareLock);
		old_indisprimary = rel->rd_index->indisprimary;
		CommandCounterIncrement();
		if (!old_indisprimary && rel->rd_index->indisprimary)
		{
			/* Executed during ADD PRIMARY KEY USING INDEX */
			Relation	tbl;

			tbl = relation_open(rel->rd_index->indrelid, AccessShareLock);
			if ((tbl->rd_rel->relkind == RELKIND_RELATION ||
				 tbl->rd_rel->relkind == RELKIND_MATVIEW) &&
				is_orioledb_rel(tbl))
			{
				int			i;
				int			ix_num = InvalidIndexNumber;
				OTableDescr *descr = relation_get_descr(tbl);

				Assert(RelIsInMyDatabase(tbl));
				Assert(descr != NULL);

				for (i = 0; i < descr->nIndices; i++)
				{
					if (descr->indices[i]->oids.reloid == rel->rd_rel->oid)
					{
						ix_num = i;
						break;
					}
				}

				elog(WARNING, "We cannot just reuse index for primary key in orioledb, because secondary indices contain primary index fields, rebuilding all indices");
				if (!in_rewrite)
				{
					Assert(ix_num != InvalidIndexNumber);

					if (descr->indices[ix_num]->primaryIsCtid)
						ix_num--;
					o_index_drop(tbl, ix_num);
					o_define_index(tbl, NULL, rel->rd_rel->oid, false, InvalidIndexNumber, false, NULL);
				}
			}
			relation_close(tbl, AccessShareLock);
		}
		relation_close(rel, AccessShareLock);
	}
	else if (access == OAT_DROP && classId == OperatorClassRelationId)
	{
		OOpclass   *o_opclass = o_opclass_get(objectId);

		if (o_opclass)
			o_add_invalidate_comparator_undo_item(o_opclass->opfamily,
												  o_opclass->inputtype,
												  o_opclass->inputtype);
	}
	else if (access == OAT_DROP && classId == TableSpaceRelationId)
	{
		DIR		   *dir;
		char		path[MAXPGPATH];
		char		targetpath[MAXPGPATH];
		struct dirent *file;

#define PG_TBLSPC "pg_tblspc"

		dir = opendir(PG_TBLSPC);
		while (errno = 0, (file = readdir(dir)) != NULL)
		{
			struct stat st;
			int			rllen;

			/* Skip special stuff */
			if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
				continue;

			path[0] = '\0';
			pg_snprintf(path, MAXPGPATH,
						PG_TBLSPC "/%s/" TABLESPACE_VERSION_DIRECTORY,
						file->d_name);
			if (lstat(path, &st) < 0)
			{
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								file->d_name)));
			}

			if (!S_ISLNK(st.st_mode))
			{
				strlcat(path, "/" ORIOLEDB_DATA_DIR, MAXPGPATH);
				cleanup_tablespace_dir(path);
			}
			else
			{
				rllen = readlink(path, targetpath, sizeof(targetpath));
				if (rllen < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not read symbolic link \"%s\": %m",
									path)));
				if (rllen >= sizeof(targetpath))
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("symbolic link \"%s\" target is too long",
									path)));
				targetpath[rllen] = '\0';

				path[0] = '\0';
				pg_snprintf(path, MAXPGPATH,
							"%s/" ORIOLEDB_DATA_DIR,
							targetpath);
				cleanup_tablespace_dir(path);
			}
		}
		closedir(dir);
#undef PG_TBLSPC
	}

	if (old_objectaccess_hook)
		old_objectaccess_hook(access, classId, objectId, subId, arg);
}

/*
 * Look up info about the database named "name".  If the database exists,
 * obtain the specified lock type on it, fill in any of the remaining
 * parameters that aren't NULL, and return true.  If no such database,
 * return false.
 */
static bool
get_db_info(const char *name, LOCKMODE lockmode, Oid *dbIdP)
{
	bool		result = false;
	Relation	relation;

	Assert(name);

	/* Caller may wish to grab a better lock on pg_database beforehand... */
	relation = table_open(DatabaseRelationId, AccessShareLock);

	/*
	 * Loop covers the rare case where the database is renamed before we can
	 * lock it.  We try again just in case we can find a new one of the same
	 * name.
	 */
	for (;;)
	{
		ScanKeyData scanKey;
		SysScanDesc scan;
		HeapTuple	tuple;
		Oid			dbOid;

		/*
		 * there's no syscache for database-indexed-by-name, so must do it the
		 * hard way
		 */
		ScanKeyInit(&scanKey,
					Anum_pg_database_datname,
					BTEqualStrategyNumber, F_NAMEEQ,
					CStringGetDatum(name));

		scan = systable_beginscan(relation, DatabaseNameIndexId, true,
								  NULL, 1, &scanKey);

		tuple = systable_getnext(scan);

		if (!HeapTupleIsValid(tuple))
		{
			/* definitely no database of that name */
			systable_endscan(scan);
			break;
		}

		dbOid = ((Form_pg_database) GETSTRUCT(tuple))->oid;

		systable_endscan(scan);

		/*
		 * Now that we have a database OID, we can try to lock the DB.
		 */
		if (lockmode != NoLock)
			LockSharedObject(DatabaseRelationId, dbOid, 0, lockmode);

		/*
		 * And now, re-fetch the tuple by OID.  If it's still there and still
		 * the same name, we win; else, drop the lock and loop back to try
		 * again.
		 */
		tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbOid));
		if (HeapTupleIsValid(tuple))
		{
			Form_pg_database dbform = (Form_pg_database) GETSTRUCT(tuple);

			if (strcmp(name, NameStr(dbform->datname)) == 0)
			{
				/* oid of the database */
				if (dbIdP)
					*dbIdP = dbOid;

				ReleaseSysCache(tuple);
				result = true;
				break;
			}
			/* can only get here if it was just renamed */
			ReleaseSysCache(tuple);
		}

		if (lockmode != NoLock)
			UnlockSharedObject(DatabaseRelationId, dbOid, 0, lockmode);
	}

	table_close(relation, AccessShareLock);

	return result;
}

/*
 * OrioleDB implementation of CREATE DATABASE.
 */
static Oid
o_createdb(ParseState *pstate, const CreatedbStmt *stmt)
{
	Oid			src_dboid;
	ListCell   *option;
	DefElem    *dtemplate = NULL;
	const char *dbtemplate = NULL;
	Oid			result;

	/*
	 * Currently we don't support a template database which has OrioleDB
	 * tables. The function raises an error otherwise.
	 */

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "template") == 0)
			dtemplate = defel;
	}

	if (dtemplate && dtemplate->arg)
		dbtemplate = defGetString(dtemplate);
	if (!dbtemplate)
		dbtemplate = "template1";	/* Default template database name */

	/*
	 * Similar to createdb() we obtain share lock on the template database
	 * within get_db_info().
	 */
	if (!get_db_info(dbtemplate, ShareLock, &src_dboid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("template database \"%s\" does not exist",
						dbtemplate)));

	if (o_tables_num(src_dboid) > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("template database \"%s\" has OrioleDB tables",
						dbtemplate)));

	/*
	 * Call standard PostgreSQL createdb().  It will create and copy
	 * PostgreSQL catalog and user objects.
	 *
	 * createdb() will leave source pg_database entry in ShareLock mode and
	 * therefore no new connections will be allowed until end of transaction.
	 */
	result = createdb(pstate, stmt);

	/*
	 * Now we need to copy OrioleDB objects.
	 */

	return result;
}

int16
o_parse_compress(const char *value)
{
	const char *ptr = value;
	int16		result = 0;
	bool		neg = false;
	bool		invalid_syntax = false;
	bool		out_of_range = false;

	/* skip leading spaces */
	while (likely(*ptr) && isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		invalid_syntax = true;

	if (!invalid_syntax)
	{
		/* process digits */
		while (*ptr && isdigit((unsigned char) *ptr))
		{
			int8		digit = (*ptr++ - '0');

			if (unlikely(pg_mul_s16_overflow(result, 10, &result)) ||
				unlikely(pg_sub_s16_overflow(result, digit, &result)))
				out_of_range = true;
		}

		if (!out_of_range)
		{
			/* allow trailing whitespace, but not other trailing chars */
			while (*ptr != '\0' && isspace((unsigned char) *ptr))
				ptr++;

			if (unlikely(*ptr != '\0'))
				invalid_syntax = true;

			if (!invalid_syntax)
			{
				if (!neg)
				{
					/* could fail if input is most negative number */
					if (unlikely(result == PG_INT16_MIN))
						out_of_range = true;
					if (!out_of_range)
						result = -result;
				}
			}
		}
	}

	if (out_of_range)
		ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("value \"%s\" is out of range for type %s",
							   value, "smallint")));

	if (invalid_syntax)
	{
		if (strcmp(value, "auto") == 0 ||
			strcmp(value, "on") == 0 ||
			strcmp(value, "true") == 0)
			result = O_COMPRESS_DEFAULT;
		else if (strcmp(value, "off") == 0)
			result = InvalidOCompress;
		else
			ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							errmsg("invalid compression value: \"%s\"",
								   value)));
	}

	return result;
}

void
o_ddl_cleanup(void)
{
	if (drop_index_list)
	{
		list_free_deep(drop_index_list);
		drop_index_list = NIL;
	}
	if (partition_drop_index_list)
	{
		list_free(partition_drop_index_list);
		partition_drop_index_list = NIL;
	}
	if (reindex_list)
	{
		list_free_deep(reindex_list);
		reindex_list = NIL;
	}
	if (dropped_attrs)
	{
		list_free(dropped_attrs);
		dropped_attrs = NIL;
	}
	if (alter_type_exprs)
	{
		list_free(alter_type_exprs);
		alter_type_exprs = NIL;
	}
	memset(&o_pkey_result, 0, sizeof(o_pkey_result));
	o_saved_relrewrite = InvalidOid;
	in_rewrite = false;
	o_alter_generated_column_id = NIL;
	o_in_add_column = false;
}
