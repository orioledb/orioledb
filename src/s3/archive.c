/*-------------------------------------------------------------------------
 *
 * archive.c
 *		Routines for S3 WAL archiving.
 *
 * Copyright (c) 2024-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/archive.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "s3/queue.h"
#include "s3/requests.h"
#include "s3/worker.h"

#include "archive/archive_module.h"
#include "common/hashfn.h"

typedef struct
{
	char	   *fileName;
	S3TaskLocation location;
} PreloadHashItem;

HTAB	   *preloadHash = NULL;


static uint32
preload_item_hash(const void *key, Size keysize)
{
	const char **filename = (const char **) key;

	/* We don't bother to include the payload's trailing null in the hash */
	return DatumGetUInt32(hash_any((const unsigned char *) *filename,
								   strlen(*filename)));
}

/*
 * notification_match: match function to use with notification_hash
 */
static int
preload_item_match(const void *key1, const void *key2, Size keysize)
{
	const char **f1 = (const char **) key1;
	const char **f2 = (const char **) key2;
	int			l1 = strlen(*f1),
				l2 = strlen(*f2);

	if (l1 == l2 && memcmp((void *) *f1, (void *) *f2, l1) == 0)
		return 0;
	else
		return 1;
}

static void
make_preload_hash(void)
{
	HASHCTL		hash_ctl;

	/* Create the hash table */
	hash_ctl.keysize = sizeof(char *);
	hash_ctl.entrysize = sizeof(PreloadHashItem);
	hash_ctl.hash = preload_item_hash;
	hash_ctl.match = preload_item_match;
	hash_ctl.hcxt = TopMemoryContext;
	preloadHash =
		hash_create("WAL files to be archieved to S3",
					32L,
					&hash_ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
}

static bool s3_archive_configured(ArchiveModuleState *state);
static void s3_archive_preload_file(ArchiveModuleState *state,
									const char *file, const char *path);
static bool s3_archive_file(ArchiveModuleState *state,
							const char *file, const char *path);

static const ArchiveModuleCallbacks s3_archive_callbacks = {
	.check_configured_cb = s3_archive_configured,
	.archive_preload_file_cb = s3_archive_preload_file,
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
	if (!preloadHash)
		make_preload_hash();

	return &s3_archive_callbacks;
}

/*
 * We only allow S3 archiving if we're in S3 mode.
 */
static bool
s3_archive_configured(ArchiveModuleState *state)
{
	return orioledb_s3_mode;
}

/*
 * This callback archieves given WAL file into S3.  This function have to
 * return the result syncronously, and it works in dedicated arhiving process.
 * So, no point to schedule this for S3 worker.  Make the S3 request right-away.
 */
static void
s3_archive_preload_file(ArchiveModuleState *state,
						const char *file, const char *path)
{
	bool		found;
	PreloadHashItem *item;

	if (!orioledb_s3_mode)
		return;

	elog(DEBUG1, "archive preload %s", file);

	item = hash_search(preloadHash, &file, HASH_ENTER, &found);

	if (found)
	{
		elog(WARNING, "double call of archive_file_preload_cb() for %s", file);
		return;
	}

	item->location = s3_schedule_wal_file_write((char *) file);
}

/*
 * This callback archieves given WAL file into S3.  This function have to
 * return the result syncronously, and it works in dedicated arhiving process.
 * So, no point to schedule this for S3 worker.  Make the S3 request right-away.
 */
static bool
s3_archive_file(ArchiveModuleState *state,
				const char *file, const char *path)
{
	S3TaskLocation location;
	bool		found;
	PreloadHashItem *item;

	if (!orioledb_s3_mode)
		return false;

	elog(DEBUG1, "archive %s", file);

	item = hash_search(preloadHash, &file, HASH_FIND, &found);
	if (item)
	{
		location = item->location;
		if (!hash_search(preloadHash, &file, HASH_REMOVE, &found))
			elog(ERROR, "can't delete item from preloadHash");
	}
	else
	{
		location = s3_schedule_wal_file_write((char *) file);
	}

	s3_queue_wait_for_location(location);
	return true;
}
