/*-------------------------------------------------------------------------
 *
 * archive.c
 *		Routines for S3 WAL archiving.
 *
 * Copyright (c) 2023, OrioleDATA Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/archive.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "s3/requests.h"

#if PG_VERSION_NUM >= 160000
#include "archive/archive_module.h"
#else
#include "postmaster/pgarch.h"
#endif

#if PG_VERSION_NUM >= 160000
static bool s3_archive_configured(ArchiveModuleState *state);
static bool s3_archive_file(ArchiveModuleState *state,
							const char *file, const char *path);

static const ArchiveModuleCallbacks s3_archive_callbacks = {
	.check_configured_cb = s3_archive_configured,
	.archive_file_cb = s3_archive_file
};

/*
 * _PG_archive_module_init
 *
 * Returns the module's archiving callbacks.
 */
const ArchiveModuleCallbacks *
_PG_archive_module_init(void)
{
	return &s3_archive_callbacks;
}
#else
static bool s3_archive_configured(void);
static bool s3_archive_file(const char *file, const char *path);

extern void _PG_archive_module_init(ArchiveModuleCallbacks *callbacks);

/*
 * _PG_archive_module_init
 *
 * Returns the module's archiving callbacks.
 */
void
_PG_archive_module_init(ArchiveModuleCallbacks *callbacks)
{
	callbacks->check_configured_cb = s3_archive_configured;
	callbacks->archive_file_cb = s3_archive_file;
}
#endif

/*
 * We only allow S3 archiving if we're in S3 mode.
 */
static bool
#if PG_VERSION_NUM >= 160000
s3_archive_configured(ArchiveModuleState *state)
#else
s3_archive_configured(void)
#endif
{
	return orioledb_s3_mode;
}

/*
 * This callback archieves given WAL file into S3.  This function have to
 * return the result syncronously, and it works in dedicated arhiving process.
 * So, no point to schedule this for S3 worker.  Make the S3 request right-away.
 */
static bool
#if PG_VERSION_NUM >= 160000
s3_archive_file(ArchiveModuleState *state,
				const char *file, const char *path)
#else
s3_archive_file(const char *file, const char *path)
#endif
{
	char	   *objectname;

	objectname = psprintf("wal/%s", file);

	elog(DEBUG1, "archive %s %s", path, objectname);
	s3_put_file(objectname, (char *) path);

	pfree(objectname);
	return true;
}
