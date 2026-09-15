/*-------------------------------------------------------------------------
 *
 * o_tablespaces.h
 * 		Declarations for tablespace data directory routines.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/o_tablespaces.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __O_TABLESPACES_H__
#define __O_TABLESPACES_H__

extern void o_get_prefixes_for_tablespace(Oid datoid, Oid tablespace,
										  char **prefix, char **db_prefix);

/* callback for o_tablespaces_foreach_prefix() */
typedef void (*OTablespacesPrefixCallback) (Oid tablespace,
											const char *prefix,
											void *arg);

extern void o_tablespaces_foreach_prefix(OTablespacesPrefixCallback callback,
										 void *arg);

/* callback for o_tablespace_foreach_database() */
typedef void (*OTablespaceDatabaseCallback) (Oid tablespace, Oid datoid,
											 const char *db_path,
											 void *arg);

extern bool o_tablespace_foreach_database(Oid tablespace, const char *prefix,
										  OTablespaceDatabaseCallback callback,
										  void *arg, int elevel);

/*
 * Resolve the orioledb data directory path for a single tablespace.
 * Returns false if the tablespace directory does not exist (ENOENT).
 * Errors on other failures.
 */
extern bool o_tablespace_resolve_prefix(Oid tablespace, char *path,
										size_t pathlen);

/*
 * Remove all per-database subdirectories found in the given orioledb data
 * directory, fsync and rmdir the directory itself.  Returns false when the
 * directory does not exist.
 */
extern bool o_tablespace_destroy_orioledb_dir(Oid tablespace, const char *path);

#endif							/* __O_TABLESPACES_H__ */
