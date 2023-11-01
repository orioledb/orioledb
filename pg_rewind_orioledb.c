#include <unistd.h>

#define FRONTEND 1
#include "postgres.h"
#include "postgres_fe.h"

#include "orioledb_types.h"

#include "catalog/sys_trees.h"
#include "recovery/wal.h"

#include "access/xlogreader.h"
#include "common/file_perm.h"
#include "common/logging.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "replication/message.h"

PG_MODULE_MAGIC;

// TODO: Add header for this function. pg_rewind.h or something
extern void SimpleXLogRead(const char *datadir, XLogRecPtr startpoint,
						   int tliIndex, XLogRecPtr endpoint,
						   const char *restoreCommand,
						   void (*page_callback)(XLogReaderState *,
												 void *arg),
						   void *arg);

// TODO: Add header to contain this exports. pg_rewind.h or something
extern PGDLLEXPORT void _PG_rewind(const char *datadir_target,
								   char *datadir_source,
								   char *connstr_source, XLogRecPtr startpoint,
								   int tliIndex, XLogRecPtr endpoint,
								   const char *restoreCommand,
								   const char *argv0, bool debug);

// TODO: Add header for this function. pg_rewind.h or something
extern void extensions_exclude_add(char **exclude_dir);

typedef struct OrioledbKey
{
	uint8	tupleFormatFlags;
	uint32	dataLength;
	char	data[FLEXIBLE_ARRAY_MEMBER];
} OrioledbKey;

typedef struct
{
	OIndexType	type;
	Oid			datoid;
	Oid			reloid;
	Oid			relnode;
} OrioledbTreeKey;

typedef struct OrioledbTree
{
	OrioledbTreeKey		tree_key;
	uint32				nkeys;
	int					allocated;
	OrioledbKey		  **keys;
} OrioledbTree;

typedef struct OrioledbKeyMap
{
	int				ntrees;
	int				allocated;
	OXid			first_xid;
	OrioledbTree   *trees;
	XLogRecPtr		new_startpoint;
	bool			fill_map;
	OrioledbTreeKey	tree_key;
} OrioledbKeyMap;

static void orioledb_tree_add(OrioledbTree *tree, OrioledbKey *key);

static char copy_header[11] = "PGCOPY\n\377\r\n\0";

static const char *orioledb_exclude_dir_contents[] =
{
	"orioledb_data",
	"orioledb_undo",
	/* end of  list*/
	NULL
};

static OrioledbKeyMap *
create_orioledb_key_map(XLogRecPtr startpoint)
{
	OrioledbKeyMap *result = palloc0(sizeof(OrioledbKeyMap));

	result->allocated = 8;
	result->ntrees = 0;
	result->first_xid = InvalidOXid;
	result->new_startpoint = startpoint;
	result->fill_map = false;
	result->trees = palloc0(sizeof(OrioledbTree) * result->allocated);
	return result;
}

static void
fill_orioledb_tree(OrioledbTree *tree, OrioledbTreeKey tree_key)
{
	tree->tree_key = tree_key;
	tree->allocated = 8;
	tree->nkeys = 0;
	tree->keys = palloc0(sizeof(OrioledbKey *) * tree->allocated);
}

static OrioledbKey *
create_orioledb_key(size_t dataLength, char data[], uint8 tupleFormatFlags)
{
	OrioledbKey *result = palloc0(offsetof(OrioledbKey, data) + dataLength);

	result->tupleFormatFlags = tupleFormatFlags;
	result->dataLength = dataLength;
	memcpy(&result->data, data, dataLength);
	return result;
}

static void
free_orioledb_key_map(OrioledbKeyMap *map)
{
	int i;
	for (i = 0; i < map->ntrees; i++)
	{
		int				j;
		OrioledbTree   *tree = &map->trees[i];
		for (j = 0; j < tree->nkeys; j++)
			pfree(tree->keys[j]);
		pfree(tree->keys);
	}
	pfree(map->trees);
	pfree(map);
}

static int
orioledb_key_cmp(const void *k1, const void *k2)
{
	OrioledbTreeKey *key1 = (OrioledbTreeKey *) k1;
	OrioledbTreeKey *key2 = (OrioledbTreeKey *) k2;

	if (key1->datoid <= SYS_TREES_DATOID && key2->datoid > SYS_TREES_DATOID)
		return -1;

	if (key1->datoid > SYS_TREES_DATOID && key2->datoid <= SYS_TREES_DATOID)
		return 1;

	if (key1->type != key2->type)
		return key1->type < key2->type ? -1 : 1;

	if (key1->datoid != key2->datoid)
		return key1->datoid < key2->datoid ? -1 : 1;

	if (key1->reloid != key2->reloid)
		return key1->reloid < key2->reloid ? -1 : 1;

	if (key1->relnode != key2->relnode)
		return key1->relnode < key2->relnode ? -1 : 1;

	return 0;
}

typedef struct
{
	const void *key;
	int (*const compar)(const void *, const void *);
	void *last_visited;
	int last_compar_result;
} bsearch_insertion_state;

static int bsearch_insertion_compare(const void *a, const void *b)
{
	bsearch_insertion_state *state = (bsearch_insertion_state *) a;
	state->last_visited = (void *) b;
	state->last_compar_result = state->compar(state->key, b);
	return state->last_compar_result;
}

static void
orioledb_key_map_add_tree(OrioledbKeyMap *map, OrioledbTreeKey tree_key)
{
	int insert_index = 0;

	if (map->ntrees > 0)
	{
		bsearch_insertion_state state = {&tree_key, orioledb_key_cmp, NULL};
		void *result = bsearch(&state, map->trees, map->ntrees,
							   sizeof(OrioledbTree), bsearch_insertion_compare);
		if (result != NULL)
			return;
		insert_index = (((char *)state.last_visited - (char *)map->trees) /
					  sizeof(OrioledbTree)) +
					 (state.last_compar_result > 0 ? 1 : 0);
	}

	map->ntrees++;
	if (map->ntrees == map->allocated)
	{
		map->allocated *= 2;
		map->trees = repalloc(map->trees,
							  sizeof(OrioledbTree) * map->allocated);
	}
	if (insert_index < map->ntrees - 1)
	{
		memmove(&map->trees[insert_index + 1], &map->trees[insert_index],
				sizeof(OrioledbTree) * (map->ntrees - insert_index));
	}
	fill_orioledb_tree(&map->trees[insert_index], tree_key);
}

static void
orioledb_key_map_add_key(OrioledbKeyMap *map, OrioledbKey *key)
{
	void *result = bsearch(&map->tree_key, map->trees, map->ntrees,
						   sizeof(OrioledbTree), orioledb_key_cmp);
	Assert(result != NULL);
	orioledb_tree_add(result, key);
}

static void
orioledb_key_map_print(OrioledbKeyMap *map)
{
	int i;
	for (i = 0; i < map->ntrees; i++)
	{
		OrioledbTreeKey *key = &map->trees[i].tree_key;

		pg_log_debug("(%d, %d, %d, %d): %d keys",
					 key->type, key->datoid, key->reloid, key->relnode,
					 map->trees[i].nkeys);
	}
}

static void
orioledb_tree_add(OrioledbTree *tree, OrioledbKey *key)
{
	tree->keys[tree->nkeys] = key;
	tree->nkeys++;
	if (tree->nkeys == tree->allocated)
	{
		tree->allocated *= 2;
		tree->keys = repalloc(tree->keys,
							  sizeof(OrioledbKey *) * tree->allocated);
	}
}

static void
appendHton16StringInfo(StringInfo str, uint16 data)
{
	uint16 u16data = pg_hton16(data);
	appendBinaryStringInfoNT(str, (const char *) &u16data, sizeof(u16data));
}

static void
appendHton32StringInfo(StringInfo str, uint32 data)
{
	uint32 u32data = pg_hton32(data);
	appendBinaryStringInfoNT(str, (const char *) &u32data, sizeof(u32data));
}

static void
appendOidStringInfo(StringInfo str, Oid oid)
{
	appendHton32StringInfo(str, sizeof(Oid));
	appendHton32StringInfo(str, oid);
}

static void
serialize_tree(StringInfo str, OrioledbTree *tree)
{
	int j;
	StringInfo keys_str;

	/* type, datoid, reloid, relnode, nkeys, keys */
	appendHton16StringInfo(str, 6);

	appendHton32StringInfo(str, sizeof(tree->tree_key.type));
	appendHton32StringInfo(str, tree->tree_key.type);

	appendOidStringInfo(str, tree->tree_key.datoid);
	appendOidStringInfo(str, tree->tree_key.reloid);
	appendOidStringInfo(str, tree->tree_key.relnode);

	appendHton32StringInfo(str, sizeof(tree->nkeys));
	appendHton32StringInfo(str, tree->nkeys);

	keys_str = makeStringInfo();
	for (j = 0; j < tree->nkeys; j++)
	{
		appendStringInfoChar(keys_str, tree->keys[j]->tupleFormatFlags);
		appendHton32StringInfo(keys_str, tree->keys[j]->dataLength);
		appendBinaryStringInfoNT(keys_str, tree->keys[j]->data,
								 tree->keys[j]->dataLength);
	}
	appendHton32StringInfo(str, keys_str->len);
	appendBinaryStringInfoNT(str, keys_str->data, keys_str->len);
	pfree(keys_str->data);
	pfree(keys_str);
}

static void
serialize_key_map(StringInfo str, OrioledbKeyMap *map)
{
	int i;

	for (i = 0; i < map->ntrees; i++)
	{
		serialize_tree(str, &map->trees[i]);
	}

	appendHton16StringInfo(str, -1);
}

static void
run_pg_ctl(const char *argv0, const char *target_args[])
{
#define MAXCMDLEN (4 * MAXPGPATH)
#define PGCTL_VERSIONSTR "pg_ctl (PostgreSQL) " PG_VERSION "\n"
	int				ret;
	char			exec_path[MAXPGPATH + 1];
	char			cmd[MAXCMDLEN + 1];
	const char	   *progname;
	const char	  **target_arg;

	progname = get_progname(argv0);

	if ((ret = find_other_exec(argv0, "pg_ctl",
							   PGCTL_VERSIONSTR,
							   exec_path)) < 0)
	{
		char full_path[MAXPGPATH];

		if (find_my_exec(argv0, full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (ret == -1)
			pg_fatal("The program \"%s\" is needed by %s but was not found in the\n"
					 "same directory as \"%s\".\n"
					 "Check your installation.",
					 "postgres", progname, full_path);
		else
			pg_fatal("The program \"%s\" was found by \"%s\"\n"
					 "but was not the same version as %s.\n"
					 "Check your installation.",
					 "postgres", full_path, progname);
	}

    // TODO: Ommit for now, probably we do not even need to run pg_ctl now
	// if (dry_run)
	// 	return;

	cmd[0] = '\0';
	strncat(cmd, exec_path, MAXCMDLEN);
	strncat(cmd, " ", MAXCMDLEN);
	target_arg = target_args;
	while (*target_arg)
	{
		strncat(cmd, *target_arg, MAXCMDLEN);
		strncat(cmd, " ", MAXCMDLEN);
		target_arg++;
	}
	pg_log_debug("executing \"%s\"", cmd);
	if (system(cmd) != 0)
		pg_fatal("Failed command: %s", cmd);
	return;
}

static void
start_target_postgres(const char *argv0, const char *datadir, bool debug)
{
	const char	  **pg_ctl_args;
	char	  *orioledb_rewind_log;
	const char *log_file;

	orioledb_rewind_log = getenv("ORIOLEDB_REWIND_LOG");
	log_file = orioledb_rewind_log ? orioledb_rewind_log : DEVNULL;
	pg_ctl_args =
		(const char *[]){"start", "-D", datadir, "-l", log_file, NULL};
	run_pg_ctl(argv0,  pg_ctl_args);
}

static void
stop_target_postgres(const char *argv0, const char *datadir, bool debug)
{
	run_pg_ctl(argv0,
			   (const char *[]){"stop", "-D", datadir, NULL});
}

/*
 * Runs a command.
 * In the event of a failure, exit immediately.
 */
static void
run_simple_command(PGconn *connection, const char *sql, bool target)
{
	PGresult   *res;

	res = PQexec(connection, sql);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("error running query (%s) in %s server: %s",
				 sql, target ? "target" : "source",
				 PQresultErrorMessage(res));

	PQclear(res);
}

static void
libpq_create_table_and_copy_data(PGconn *connection,
								 StringInfo str,
								 bool target)
{
	PGresult	   *res;

	run_simple_command(connection, "create temporary table target_keys("
								   "	ix_type int4,"
								   "	datoid Oid,"
								   "	reloid Oid,"
								   "	relnode Oid,"
								   "	nkeys int4,"
								   "	keys bytea"
								   ");",
					   target);
	res = PQexec(connection, "COPY target_keys FROM STDIN (FORMAT binary);");
	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		pg_fatal("could not send key map: %s", PQresultErrorMessage(res));
	}
	else
	{
		int flag = 0;
		int extension = 0;
		int			copyRes;

		PQclear(res);

		PQputCopyData(connection, copy_header, sizeof(copy_header));
		PQputCopyData(connection, (const char *)&flag, sizeof(flag));
		PQputCopyData(connection, (const char *)&extension, sizeof(extension));
		copyRes = PQputCopyData(connection, str->data, str->len);
		if (copyRes == 1)
		{
			if (PQputCopyEnd(connection, NULL) == 1)
			{
				res = PQgetResult(connection);
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
					pg_fatal("unexpected result while sending key map: %s",
							PQresultErrorMessage(res));
				PQclear(res);
			}
			else
			{
				pg_fatal("could not send end-of-COPY: %s",
						PQerrorMessage(connection));
			}
		}
		else
		{
			pg_fatal("could not send COPY data: %s",
					 PQerrorMessage(connection));
		}
	}
}

static void
libpq_function_copy_out(PGconn *connection, const char *query,
						StringInfo result_str)
{
	PGresult	   *res;
	StringInfo		str;

	str = makeStringInfo();
	appendStringInfo(str, "COPY (SELECT %s) TO stdout (FORMAT binary);",
					 query);
	res = PQexec(connection, str->data);
	pfree(str->data);
	pfree(str);
	if ( PQresultStatus(res) != PGRES_COPY_OUT ) {
		pg_fatal("could not recieve COPY data: %s",
					PQresultErrorMessage(res));
	} else {
		bool first = true;
		char *copybuf = NULL;
		int row_len;

		PQclear(res);

		while ( (row_len = PQgetCopyData(connection, &copybuf, 0)) > 0 ) {
			if (first)
			{
				const int result_header_size = sizeof(copy_header) +
											   sizeof(uint32) +
											   sizeof(uint32) +
											   sizeof(uint16) +
											   sizeof(uint32);
				appendBinaryStringInfoNT(result_str,
										 copybuf + result_header_size,
										 row_len - result_header_size);
				first = false;
			}
			else
				appendBinaryStringInfoNT(result_str, copybuf, row_len);
			if (copybuf != NULL)
			{
				PQfreemem(copybuf);
				copybuf = NULL;
			}
		}
		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}
		if ( row_len == -2 )
			pg_fatal("could not receive COPY data: %s",
					 PQerrorMessage(connection));
	}
}

typedef struct TargetInfo
{
	long	port;
	bool	is_tcp;
	char	host[FLEXIBLE_ARRAY_MEMBER];
} TargetInfo;

static TargetInfo *
get_target_info(const char *datadir)
{
	FILE	   *postmaster_pid_file;
	long		port = 0;
	TargetInfo *info = NULL;
	char		fullpath[MAXPGPATH];

#define			MAX_LINE 1024
	char		buffer[MAX_LINE];
	int			linenum;
	snprintf(fullpath, sizeof(fullpath), "%s/%s", datadir, "postmaster.pid");
	if ((postmaster_pid_file = fopen(fullpath, "r")) == NULL)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);
	linenum = 0;
	while (fgets(buffer, MAX_LINE, postmaster_pid_file))
	{
		if (linenum == 3)
			port = strtol(buffer, NULL, 10);
		else if (linenum == 4 || linenum == 5)
		{
			int len = strnlen(buffer, MAX_LINE);
			if (buffer[len - 1] == '\n')
				buffer[len - 1] = '\0';
			len--;
			if (len > 0)
			{
				info = palloc0(offsetof(TargetInfo, host) + (len + 1));
				info->is_tcp = linenum == 5;
				Assert(port != 0);
				info->port = port;
				strncpy(info->host, buffer, len);
				break;
			}
		}
		linenum++;
	}

	fclose(postmaster_pid_file);
	Assert(info);
	return info;
}

static bool
libpq_is_orioledb_available(PGconn *connection, bool target)
{
	bool	result = true;
	PGresult *res;
	res = PQexec(connection, "select name from pg_available_extensions "
							 "WHERE name = 'orioledb';");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		result = false;
	else
		result = PQntuples(res) != 0;
	PQclear(res);

	if (!result)
		pg_log_debug("extension 'orioledb' is not available on %s",
					 target ? "target" : "source");
	return result;
}

static bool
libpq_is_readonly(PGconn *connection, bool target)
{
	bool result = true;
	PGresult *res;
	res = PQexec(connection, "SHOW transaction_read_only;");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		result = false;
	else
	{
		result = strcmp(PQgetvalue(res, 0, 0), "on") == 0;
	}
	PQclear(res);

	if (result)
		pg_log_debug("%s is in read-only mode", target ? "target" : "source");
	return result;
}

static bool
libpq_is_orioledb_installed(PGconn *connection, bool target)
{
	bool	result = true;
	PGresult *res;
	res = PQexec(connection, "select extname from pg_extension "
							 "WHERE extname = 'orioledb';");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		result = false;
	else
		result = PQntuples(res) != 0;
	PQclear(res);

	if (!result)
		pg_log_debug("extension 'orioledb' is not installed on %s",
					 target ? "target" : "source");
	return result;
}

static bool
sort_rows_on_target(OrioledbKeyMap *orioledb_map, const char *argv0,
					const char *datadir, bool debug, StringInfo result)
{
	PGconn	   *target_conn;
	StringInfo	str;
	PGresult   *res;
	TargetInfo *target_info;
	bool		sorted = false;

	start_target_postgres(argv0, datadir, debug);
	str = makeStringInfo();
	target_info = get_target_info(datadir);
	appendStringInfo(str, "host='%s' port=%ld dbname=template1",
					 target_info->host,
					 target_info->port);
	target_conn = PQconnectdb(str->data);
	if (PQstatus(target_conn) == CONNECTION_BAD)
		pg_fatal("%s: %s",
				 str->data,
				 PQerrorMessage(target_conn));
	pfree(target_info);
	pfree(str->data);
	pfree(str);

	if (libpq_is_orioledb_available(target_conn, true) &&
		!libpq_is_readonly(target_conn, true))
	{
		res = PQexec(target_conn, "CREATE EXTENSION orioledb;");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			pg_fatal("could not create extension on target: %d: %s",
					 PQresultStatus(res),
					 PQresultErrorMessage(res));
		PQclear(res);

		str = makeStringInfo();
		serialize_key_map(str, orioledb_map);
		libpq_create_table_and_copy_data(target_conn, str, true);
		pfree(str->data);
		pfree(str);
		libpq_function_copy_out(target_conn,
								"public.orioledb_pg_rewind_sorted_keys("
								"'target_keys'::regclass)",
								result);

		res = PQexec(target_conn, "DROP EXTENSION orioledb;");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			pg_fatal("could not drop extension on target: %s",
					PQresultErrorMessage(res));
		PQclear(res);
		sorted = true;
	}

	stop_target_postgres(argv0, datadir, debug);
	return sorted;
}

static void
write_binary(int fd, char *path, const void *buf, size_t nbyte)
{
	int			writeleft = nbyte;
	char	   *p = (char *) buf;

	while (writeleft > 0)
	{
		int writelen;

		errno = 0;
		writelen = write(fd, p, writeleft);
		if (writelen < 0)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			pg_fatal("could not write file \"%s\": %m",
					 path);
		}

		p += writelen;
		writeleft -= writelen;
	}
}

static void
orioledb_process_row_map(OrioledbKeyMap *orioledb_map, const char *argv0,
						 const char *datadir, bool debug, PGconn *conn,
						 XLogRecPtr startpoint)
{
	StringInfo	str;
	int			fd = -1;
	char		path[MAXPGPATH] = "";
	uint32		datalen;
	bool		sorted;

	str = makeStringInfo();
	pg_log_debug("Sorting rows on target");
	sorted = sort_rows_on_target(orioledb_map, argv0, datadir, debug, str);

	if (sorted &&
		libpq_is_orioledb_available(conn, false) &&
		libpq_is_orioledb_installed(conn, false))
	{
		pg_log_debug("Copying sorted rows to source");
		libpq_create_table_and_copy_data(conn, str, false);
		pfree(str->data);
		pfree(str);

		str = makeStringInfo();
		pg_log_debug("Getting new versions of changed rows from source");
		libpq_function_copy_out(conn,
								"public.orioledb_pg_rewind_new_row_versions("
								"'target_keys'::regclass)", str);

		snprintf(path, sizeof(path), "%s/%s", datadir, "orioledb_data/rewind");
		fd = open(path, O_WRONLY | O_CREAT | PG_BINARY | O_TRUNC,
				  pg_file_create_mode);
		if (fd < 0)
			pg_fatal("could not open target file \"%s\": %m",
					 path);

		datalen = str->len - sizeof(uint16); /* - COPY OUT message end */

		write_binary(fd, path, &startpoint, sizeof(XLogRecPtr));
		write_binary(fd, path, str->data, datalen);

		if (close(fd) != 0)
			pg_fatal("could not close target file \"%s\": %m",
					 path);
		pfree(str->data);
		pfree(str);
	}
}

static void
rewind_record_callback(uint8 rec_type, Pointer ptr, XLogRecPtr xlogPtr,
					   void *arg)
{
	OrioledbKeyMap *orioledb_map = arg;

	switch (rec_type)
	{
		case WAL_REC_XID:
			{
				OXid	   cur_oxid;
				XLogRecPtr cur_trx_start;

				memcpy(&cur_oxid, ptr, sizeof(OXid));
				if (orioledb_map->first_xid == InvalidOXid)
					orioledb_map->first_xid = cur_oxid;
				ptr += sizeof(OXid);
				memcpy(&cur_trx_start, ptr, sizeof(XLogRecPtr));

				if (!XLogRecPtrIsInvalid(cur_trx_start) &&
					cur_trx_start < orioledb_map->new_startpoint)
				{
					orioledb_map->new_startpoint = cur_trx_start;
				}
			}
			break;
		case WAL_REC_RELATION:
			{
				if (orioledb_map->fill_map)
				{
					OIndexType ix_type;
					ix_type = (uint32) *ptr;
					orioledb_map->tree_key.type = ix_type;
					ptr++;
					memcpy(&orioledb_map->tree_key.datoid, ptr, sizeof(Oid));
					ptr += sizeof(Oid);
					memcpy(&orioledb_map->tree_key.reloid, ptr, sizeof(Oid));
					ptr += sizeof(Oid);
					memcpy(&orioledb_map->tree_key.relnode, ptr, sizeof(Oid));

					orioledb_key_map_add_tree(orioledb_map,
											  orioledb_map->tree_key);
				}
			}
			break;
		case WAL_REC_INSERT:
		case WAL_REC_UPDATE:
		case WAL_REC_DELETE:
			{
				OffsetNumber length;
				OrioledbKey *key;
				uint8		 tupleFormatFlags;

				if (orioledb_map->fill_map)
					tupleFormatFlags = *ptr;
				ptr++;
				memcpy(&length, ptr, sizeof(OffsetNumber));
				ptr += sizeof(OffsetNumber);
				if (orioledb_map->fill_map)
				{
					key = create_orioledb_key(length, ptr, tupleFormatFlags);
					orioledb_key_map_add_key(orioledb_map, key);
				}
				ptr += length;
			}
			break;
		default:
			break;
	}
}

/*
 * Extract information on which rows the current record modifies.
 */
static void
extract_row_info(XLogReaderState *record, void *arg)
{
	RmgrId		rmid = XLogRecGetRmid(record);
	OrioledbKeyMap *orioledb_map = (OrioledbKeyMap *) arg;

#if PG_VERSION_NUM < 150000
	if (rmid == RM_LOGICALMSG_ID)
#else
	if (rmid == ORIOLEDB_RMGR_ID)
#endif
	{
#if PG_VERSION_NUM < 150000
		char			   *rec = XLogRecGetData(record);
		xl_logical_message *xlrec = (xl_logical_message *) rec;

		if (xlrec->prefix_size == (WAL_PREFIX_SIZE + 1) &&
			strncmp(xlrec->message, WAL_PREFIX, WAL_PREFIX_SIZE) == 0)
		{
			Pointer startPtr = xlrec->message + xlrec->prefix_size;
			Pointer endPtr = startPtr + xlrec->message_size;
#else
			Pointer		startPtr = (Pointer) XLogRecGetData(record);
			int			msg_len = XLogRecGetDataLen(record);
			Pointer		endPtr = startPtr + msg_len;
#endif
			wal_iterate(startPtr, endPtr, InvalidXLogRecPtr,
						rewind_record_callback, (void *) orioledb_map);
#if PG_VERSION_NUM < 150000
		}
#endif
	}
}

void
_PG_rewind(const char *datadir_target, char *datadir_source,
		   char *connstr_source, XLogRecPtr startpoint, int tliIndex,
		   XLogRecPtr endpoint, const char *restoreCommand, const char *argv0,
		   bool debug)
{
	XLogRecPtr old_startpoint = startpoint;
	OrioledbKeyMap *orioledb_map = create_orioledb_key_map(startpoint);
	PGconn	   *source_conn; // TODO: Close connection atexit

	source_conn = PQconnectdb(connstr_source);

	if (PQstatus(source_conn) == CONNECTION_BAD)
		pg_fatal("%s", PQerrorMessage(source_conn));

	SimpleXLogRead(datadir_target, startpoint, tliIndex, endpoint,
				   restoreCommand, extract_row_info, orioledb_map);
	pg_log_debug("Old startpoint: %X/%X", LSN_FORMAT_ARGS(startpoint));
	pg_log_debug("New startpoint: %X/%X",
				 LSN_FORMAT_ARGS(orioledb_map->new_startpoint));
	if (orioledb_map->new_startpoint != startpoint)
		startpoint = orioledb_map->new_startpoint;
	orioledb_map->fill_map = true;
	SimpleXLogRead(datadir_target, startpoint, tliIndex, endpoint,
				   restoreCommand, extract_row_info, orioledb_map);
	if (debug)
		orioledb_key_map_print(orioledb_map);
	orioledb_process_row_map(orioledb_map, argv0, datadir_target, debug,
							 source_conn, old_startpoint);
	free_orioledb_key_map(orioledb_map);
	PQfinish(source_conn);

	extensions_exclude_add((char **) orioledb_exclude_dir_contents);
}