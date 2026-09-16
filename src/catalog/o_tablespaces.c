/*-------------------------------------------------------------------------
 *
 * o_tablespaces.c
 * 		Tablespace data directory routines
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_tablespaces.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/undo.h"
#include "catalog/o_tablespaces.h"
#include "tableam/descr.h"

#include "catalog/pg_tablespace_d.h"
#include "common/relpath.h"
#include "recovery/recovery.h"
#include "utils/syscache.h"

#include <sys/stat.h>
#include <unistd.h>

/* Silent cppcheck */
#ifndef TABLESPACE_VERSION_DIRECTORY
#define TABLESPACE_VERSION_DIRECTORY
#endif

#ifndef PG_TBLSPC_DIR
#define PG_TBLSPC_DIR "pg_tblspc"
#endif

void
o_get_prefixes_for_tablespace(Oid datoid, Oid tablespace,
							  char **prefix, char **db_prefix)
{
	static char pathbuf[MAXPGPATH];
	Datum		path_datum;
	text	   *path;
	char	   *path_str;

	/*
	 * Treat InvalidOid as the default tablespace.  System trees and trees
	 * whose tablespace has not been set yet use tablespace = 0.
	 */
	if (!OidIsValid(tablespace))
		tablespace = DEFAULTTABLESPACE_OID;
	path_datum = DirectFunctionCall1(pg_tablespace_location, ObjectIdGetDatum(tablespace));
	path = DatumGetTextP(path_datum);
	path_str = text_to_cstring(path);

	if (path_str[0] == '\0')
		snprintf(pathbuf, sizeof(pathbuf), "%s", ORIOLEDB_DATA_DIR);
	else
		snprintf(pathbuf, sizeof(pathbuf), "%s/" TABLESPACE_VERSION_DIRECTORY "/%s", path_str, ORIOLEDB_DATA_DIR);
	pfree(path_str);
	pfree(path);
	if (prefix)
		*prefix = pathbuf;
	if (db_prefix)
		*db_prefix = psprintf("%s/%u", pathbuf, datoid);
}

void
o_tablespaces_foreach_prefix(OTablespacesPrefixCallback callback, void *arg)
{
	DIR		   *dir;
	char		path[MAXPGPATH];
	char		targetpath[MAXPGPATH];
	struct dirent *file;
	struct stat st;
	int			rllen;
	Oid			tablespace;

	path[0] = '\0';
	strlcat(path, ORIOLEDB_DATA_DIR, MAXPGPATH);
	callback(DEFAULTTABLESPACE_OID, path, arg);

	dir = opendir(PG_TBLSPC_DIR);
	if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m",
						PG_TBLSPC_DIR)));

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		if (strcmp(file->d_name, ".") == 0 ||
			strcmp(file->d_name, "..") == 0)
			continue;

		tablespace = pg_strtoint64(file->d_name);
		Assert(OidIsValid(tablespace));

		path[0] = '\0';
		pg_snprintf(path, MAXPGPATH,
					PG_TBLSPC_DIR "/%s/" TABLESPACE_VERSION_DIRECTORY,
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
		}
		else
		{
			rllen = readlink(path, targetpath, sizeof(targetpath));
			if (rllen < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read symbolic link \"%s\": %m",
								path)));
			if (rllen >= (int) sizeof(targetpath))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("symbolic link \"%s\" target is too long",
								path)));
			targetpath[rllen] = '\0';

			path[0] = '\0';
			pg_snprintf(path, MAXPGPATH,
						"%s/" ORIOLEDB_DATA_DIR,
						targetpath);
		}
		callback(tablespace, path, arg);
	}
	closedir(dir);
}

bool
o_tablespace_foreach_database(Oid tablespace, const char *prefix,
							  OTablespaceDatabaseCallback callback,
							  void *arg, int elevel)
{
	DIR		   *dir;
	struct dirent *file;
	int			saved_errno;

	dir = opendir(prefix);
	if (dir == NULL)
		return false;

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		Oid			datoid;
		char	   *db_path;

		if (sscanf(file->d_name, "%u", &datoid) != 1)
			continue;

		db_path = psprintf("%s/%u", prefix, datoid);
		callback(tablespace, datoid, db_path, arg);
		pfree(db_path);
	}

	saved_errno = errno;
	closedir(dir);
	if (saved_errno != 0)
	{
		errno = saved_errno;
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not read directory \"%s\": %m", prefix)));
	}

	return true;
}

static void
destroy_tablespace_database_dir_cb(Oid tablespace, Oid datoid,
								   const char *db_path, void *arg)
{
	/* We assume that postgres throws its own errors on nonempty directories. */
	if (rmdir(db_path) < 0 && errno != ENOTEMPTY)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not remove orioledb db dir \"%s\": %m",
						db_path)));
}

bool
o_tablespace_resolve_prefix(Oid tablespace, char *path, size_t pathlen)
{
	char		targetpath[MAXPGPATH];
	struct stat st;
	int			rllen;

	snprintf(path, pathlen, "%s/%u/%s", PG_TBLSPC_DIR, tablespace,
			 TABLESPACE_VERSION_DIRECTORY);

	if (lstat(path, &st) < 0)
	{
		if (errno == ENOENT)
			return false;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", path)));
	}

	if (!S_ISLNK(st.st_mode))
	{
		strlcat(path, "/" ORIOLEDB_DATA_DIR, pathlen);
	}
	else
	{
		rllen = readlink(path, targetpath, sizeof(targetpath));
		if (rllen < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read symbolic link \"%s\": %m", path)));
		if (rllen >= (int) sizeof(targetpath))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("symbolic link \"%s\" target is too long",
							path)));
		targetpath[rllen] = '\0';

		snprintf(path, pathlen, "%s/" ORIOLEDB_DATA_DIR, targetpath);
	}
	return true;
}

bool
o_tablespace_destroy_orioledb_dir(Oid tablespace, const char *path)
{
	if (!o_tablespace_foreach_database(tablespace, path,
									   destroy_tablespace_database_dir_cb,
									   NULL, ERROR))
		return false;

	fsync_fname_ext(path, true, false, FATAL);

	/* We assume that postgres throws its own errors on nonempty directories. */
	if (rmdir(path) < 0 && errno != ENOTEMPTY)
	{
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not remove tablespace orioledb dir \"%s\": %m",
						path)));
	}

	/* We assume that postgres throws its own errors on nonempty directories. */
	if (errno != 0 && errno != ENOTEMPTY)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("unable to clean up orioledb tablespace: %m")));
	}
	return true;
}
