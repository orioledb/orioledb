/*-------------------------------------------------------------------------
 *
 * checkpoint.c
 *		Implementation for checkpointing to S3.
 *
 * Copyright (c) 2025-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/checkpoint.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "checkpoint/checkpoint.h"
#include "s3/checkpoint.h"
#include "s3/checksum.h"
#include "s3/headers.h"
#include "s3/requests.h"
#include "s3/worker.h"

#include "utils/wait_event.h"

#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#include "access/xlog_internal.h"
#include "access/xlogarchive.h"
#include "access/xlogbackup.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "commands/defrem.h"
#include "common/controldata_utils.h"
#include "common/compression.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "pgtar.h"
#include "port.h"
#include "postmaster/syslogger.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/dsm_impl.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/reinit.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

/*
 * How much data do we want to send in one CopyData message? Note that
 * this may also result in reading the underlying files in chunks of this
 * size.
 *
 * NB: The buffer size is required to be a multiple of the system block
 * size, so use that value instead if it's bigger than our preference.
 */
#define SINK_BUFFER_LENGTH			Max(32768, BLCKSZ)

/* Was the backup currently in-progress initiated in recovery mode? */
static bool backup_started_in_recovery = false;

/* Total number of checksum failures during base backup. */
static long long int total_checksum_failures;

static S3TaskLocation maxLocation;

/*
 * Definition of one element part of an exclusion list, used for paths part
 * of checksum validation or base backups.  "name" is the name of the file
 * or path to check for exclusion.  If "match_prefix" is true, any items
 * matching the name as prefix are excluded.
 */
struct exclude_list_item
{
	const char *name;
	bool		match_prefix;
};

/*
 * The contents of these directories are removed or recreated during server
 * start so they are not included in backups.  The directories themselves are
 * kept and included as empty to preserve access permissions.
 *
 * Note: this list should be kept in sync with the filter lists in pg_rewind's
 * filemap.c.
 */
static const char *const excludeDirContents[] =
{
	/*
	 * Skip temporary statistics files. PG_STAT_TMP_DIR must be skipped
	 * because extensions like pg_stat_statements store data there.
	 */
	PG_STAT_TMP_DIR,

	/*
	 * It is generally not useful to backup the contents of this directory
	 * even if the intention is to restore to another primary. See backup.sgml
	 * for a more detailed description.
	 */
	"pg_replslot",

	/* Contents removed on startup, see dsm_cleanup_for_mmap(). */
	PG_DYNSHMEM_DIR,

	/* Contents removed on startup, see AsyncShmemInit(). */
	"pg_notify",

	/*
	 * Old contents are loaded for possible debugging but are not required for
	 * normal operation, see SerialInit().
	 */
	"pg_serial",

	/* Contents removed on startup, see DeleteAllExportedSnapshotFiles(). */
	"pg_snapshots",

	/* Contents zeroed on startup, see StartupSUBTRANS(). */
	"pg_subtrans",

	/* Contents of OrioleDB data is handled in a different way */
	"orioledb_data",

	/* Contents of OrioleDB undo is also handled separately */
	"orioledb_undo",

	/* end of list */
	NULL
};

/*
 * List of files excluded from backups.
 */
static const struct exclude_list_item excludeFiles[] =
{
	/* Skip auto conf temporary file. */
	{PG_AUTOCONF_FILENAME ".tmp", false},

	/* Skip current log file temporary file */
	{LOG_METAINFO_DATAFILE_TMP, false},

	/*
	 * Skip relation cache because it is rebuilt on startup.  This includes
	 * temporary files.
	 */
	{RELCACHE_INIT_FILENAME, true},

	/*
	 * backup_label and tablespace_map should not exist in a running cluster
	 * capable of doing an online backup, but exclude them just in case.
	 */
	{BACKUP_LABEL_FILE, false},
	{TABLESPACE_MAP, false},

	/*
	 * If there's a backup_manifest, it belongs to a backup that was used to
	 * start this server. It is *not* correct for this backup. Our
	 * backup_manifest is injected into the backup separately if users want
	 * it.
	 */
	{"backup_manifest", false},

	{"postmaster.pid", false},
	{"postmaster.opts", false},

	/* end of list */
	{NULL, false}
};

/*
 * Information about a tablespace
 *
 * In some usages, "path" can be NULL to denote the PGDATA directory itself.
 */
typedef struct
{
	char	   *oid;			/* tablespace's OID, as a decimal string */
	char	   *path;			/* full path to tablespace's directory */
	char	   *rpath;			/* relative path if it's within PGDATA, else
								 * NULL */
	int64		size;			/* total size as sent; -1 if not known */
} tablespaceinfo;

typedef struct
{
	List	   *tablespaces;
	List	   *smallFileNames;
	List	   *smallFileSizes;
	int			smallFilesTotalSize;
	int			smallFilesNum;
	S3ChecksumState *checksumState;
	uint32		chkpNum;
} S3BackupState;

#define SMALL_FILE_THRESHOLD		0x10000
#define SMALL_FILES_TOTAL_THRESHOLD 0x100000

#define FILE_CHECKSUMS_MAX_LEN	100
#define SMALL_FILE_CHECKSUMS_FILENAME	ORIOLEDB_DATA_DIR "/small_file_checksums"
#define SMALL_FILE_CHECKSUMS_TMP_FILENAME	SMALL_FILE_CHECKSUMS_FILENAME ".0"

static S3TaskLocation flush_small_files(S3BackupState *state);
static S3TaskLocation accumulate_small_file(S3BackupState *state,
											const char *path,
											int size);
static int64 s3_backup_scan_dir(S3BackupState *state,
								const char *path, int basepathlen,
								const char *spcoid);
static List *get_tablespaces(StringInfo tblspcmapfile);

/*
 * Actually do a base backup for the specified tablespaces.
 *
 * This is split out mainly to avoid complaints about "variable might be
 * clobbered by longjmp" from stupider versions of gcc.
 */
void
s3_perform_backup(int flags, S3TaskLocation maxLocation)
{
	uint32		chkpNum = checkpoint_state->lastCheckpointNumber;
	S3BackupState state;
	StringInfoData tablespaceMapData;
	ListCell   *lc;
	tablespaceinfo *newti;
	S3FileChecksum *fileChecksums;
	S3TaskLocation location;

	backup_started_in_recovery = RecoveryInProgress();

	total_checksum_failures = 0;

	s3_headers_sync();

	s3_workers_checkpoint_init();

	/* Just in case delete a leftover file */
	unlink(SMALL_FILE_CHECKSUMS_TMP_FILENAME);

	initStringInfo(&tablespaceMapData);
	state.tablespaces = get_tablespaces(&tablespaceMapData);

	/* Add a node for the base directory at the end */
	newti = palloc0(sizeof(tablespaceinfo));
	newti->size = -1;

	fileChecksums = palloc(sizeof(S3FileChecksum) * FILE_CHECKSUMS_MAX_LEN);

	state.tablespaces = lappend(state.tablespaces, newti);
	state.chkpNum = chkpNum;
	state.smallFileNames = NIL;
	state.smallFileSizes = NIL;
	state.smallFilesTotalSize = sizeof(int);
	state.smallFilesNum = 0;
	state.checksumState = makeS3ChecksumState(chkpNum, fileChecksums,
											  FILE_CHECKSUMS_MAX_LEN,
											  SMALL_FILE_CHECKSUMS_FILENAME);

	/* Send off our tablespaces one by one */
	foreach(lc, state.tablespaces)
	{
		tablespaceinfo *ti = (tablespaceinfo *) lfirst(lc);

		if (ti->path == NULL)
		{
			char	   *xidFilename;

			/* Then the bulk of the files... */
			s3_backup_scan_dir(&state, ".", 1, NULL);

			xidFilename = psprintf(XID_FILENAME_FORMAT,
								   checkpoint_state->lastCheckpointNumber);
			location = s3_schedule_file_write(chkpNum, xidFilename, false);
			maxLocation = Max(maxLocation, location);
			pfree(xidFilename);

		}
		else
		{
			char		pathbuf[MAXPGPATH];

			snprintf(pathbuf, sizeof(pathbuf), "%s", ti->path);

			s3_backup_scan_dir(&state, pathbuf, strlen(pathbuf), ti->oid);
		}
	}
	location = flush_small_files(&state);
	maxLocation = Max(maxLocation, location);

	freeS3ChecksumState(state.checksumState);
	list_free_deep(state.tablespaces);
	pfree(tablespaceMapData.data);
	s3_queue_wait_for_location(maxLocation);

	/* Wait until all S3 workers finish flushing and compact checksum files */
	s3_workers_checkpoint_finish();

	location = s3_schedule_file_write(chkpNum, FILE_CHECKSUMS_FILENAME, false);
	maxLocation = Max(maxLocation, location);

	durable_rename(SMALL_FILE_CHECKSUMS_TMP_FILENAME, SMALL_FILE_CHECKSUMS_FILENAME, ERROR);
	location = s3_schedule_file_write(chkpNum, SMALL_FILE_CHECKSUMS_FILENAME, false);
	maxLocation = Max(maxLocation, location);

	s3_queue_wait_for_location(maxLocation);
}

static int64
s3_backup_scan_dir(S3BackupState *state, const char *path,
				   int basepathlen, const char *spcoid)
{
	DIR		   *dir;
	struct dirent *de;
	char		pathbuf[MAXPGPATH * 2];
	struct stat statbuf;
	int64		size = 0;
	const char *lastDir = NULL; /* Split last dir from parent path. */
	bool		isDbDir = false;	/* Does this directory contain relations? */

	Assert(path != NULL);

	/*
	 * Determine if the current path is a database directory that can contain
	 * relations.
	 *
	 * Start by finding the location of the delimiter between the parent path
	 * and the current path.
	 */
	lastDir = last_dir_separator(path);

	/* Does this path look like a database path (i.e. all digits)? */
	if (lastDir != NULL &&
		strspn(lastDir + 1, "0123456789") == strlen(lastDir + 1))
	{
		/* Part of path that contains the parent directory. */
		int			parentPathLen = lastDir - path;

		/*
		 * Mark path as a database directory if the parent path is either
		 * $PGDATA/base or a tablespace version path.
		 */
		if (strncmp(path, "./base", parentPathLen) == 0 ||
			(parentPathLen >= (sizeof(TABLESPACE_VERSION_DIRECTORY) - 1) &&
			 strncmp(lastDir - (sizeof(TABLESPACE_VERSION_DIRECTORY) - 1),
					 TABLESPACE_VERSION_DIRECTORY,
					 sizeof(TABLESPACE_VERSION_DIRECTORY) - 1) == 0))
			isDbDir = true;
	}

	dir = AllocateDir(path);
	while ((de = ReadDir(dir, path)) != NULL)
	{
		int			excludeIdx;
		bool		excludeFound;
		ForkNumber	relForkNum; /* Type of fork if file is a relation */
#if PG_VERSION_NUM >= 170000
		unsigned	segno;
		RelFileNumber relNumber;
#else
		int			relnumchars;	/* Chars in filename that are the
									 * relnumber */
#endif
		/* Skip special stuff */
		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		/* Skip temporary files */
		if (strncmp(de->d_name,
					PG_TEMP_FILE_PREFIX,
					strlen(PG_TEMP_FILE_PREFIX)) == 0)
			continue;

		/*
		 * Check if the postmaster has signaled us to exit, and abort with an
		 * error in that case. The error handler further up will call
		 * do_pg_abort_backup() for us. Also check that if the backup was
		 * started while still in recovery, the server wasn't promoted.
		 * do_pg_backup_stop() will check that too, but it's better to stop
		 * the backup early than continue to the end and fail there.
		 */
		CHECK_FOR_INTERRUPTS();
		if (RecoveryInProgress() != backup_started_in_recovery)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the standby was promoted during online backup"),
					 errhint("This means that the backup being taken is corrupt "
							 "and should not be used. "
							 "Try taking another online backup.")));

		/* Scan for files that should be excluded */
		excludeFound = false;
		for (excludeIdx = 0; excludeFiles[excludeIdx].name != NULL; excludeIdx++)
		{
			int			cmplen = strlen(excludeFiles[excludeIdx].name);

			if (!excludeFiles[excludeIdx].match_prefix)
				cmplen++;
			if (strncmp(de->d_name, excludeFiles[excludeIdx].name, cmplen) == 0)
			{
				elog(DEBUG1, "file \"%s\" excluded from backup", de->d_name);
				excludeFound = true;
				break;
			}
		}

		if (excludeFound)
			continue;

		/* Exclude all forks for unlogged tables except the init fork */
#if PG_VERSION_NUM >= 170000
		if (isDbDir &&
			parse_filename_for_nontemp_relation(de->d_name, &relNumber,
												&relForkNum, &segno))
#else
		if (isDbDir &&
			parse_filename_for_nontemp_relation(de->d_name, &relnumchars,
												&relForkNum))
#endif
		{
			/* Never exclude init forks */
			if (relForkNum != INIT_FORKNUM)
			{
				char		initForkFile[MAXPGPATH];
#if PG_VERSION_NUM >= 170000
				if (segno == 0)
					snprintf(initForkFile, sizeof(initForkFile), "%s/%u_init",
							 path, relNumber);
				else
					snprintf(initForkFile, sizeof(initForkFile), "%s/%u.%u_init",
							 path, relNumber, segno);

#else
				char		relNumber[OIDCHARS + 1];

				/*
				 * If any other type of fork, check if there is an init fork
				 * with the same RelFileNumber. If so, the file can be
				 * excluded.
				 */
				memcpy(relNumber, de->d_name, relnumchars);
				relNumber[relnumchars] = '\0';
				snprintf(initForkFile, sizeof(initForkFile), "%s/%s_init",
						 path, relNumber);
#endif

				if (lstat(initForkFile, &statbuf) == 0)
				{
					elog(DEBUG2,
						 "unlogged relation file \"%s\" excluded from backup",
						 de->d_name);

					continue;
				}
			}
		}

		/* Exclude temporary relations */
		if (isDbDir && looks_like_temp_rel_name(de->d_name))
		{
			elog(DEBUG2,
				 "temporary relation file \"%s\" excluded from backup",
				 de->d_name);

			continue;
		}

		snprintf(pathbuf, sizeof(pathbuf), "%s/%s", path, de->d_name);

		/* Skip pg_control here to back up it last */
		if (strcmp(pathbuf, "./global/pg_control") == 0)
			continue;

		if (lstat(pathbuf, &statbuf) != 0)
		{
			if (errno != ENOENT)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file or directory \"%s\": %m",
								pathbuf)));

			/* If the file went away while scanning, it's not an error. */
			continue;
		}

		/* Scan for directories whose contents should be excluded */
		excludeFound = false;
		for (excludeIdx = 0; excludeDirContents[excludeIdx] != NULL; excludeIdx++)
		{
			if (strcmp(de->d_name, excludeDirContents[excludeIdx]) == 0)
			{
				elog(DEBUG1, "contents of directory \"%s\" excluded from backup", de->d_name);
				excludeFound = true;
				break;
			}
		}

		if (excludeFound)
		{
			S3TaskLocation location;

			location = s3_schedule_empty_dir_write(state->chkpNum, pathbuf);
			maxLocation = Max(maxLocation, location);
			continue;
		}

		/*
		 * We can skip pg_wal, the WAL segments need to be fetched from the
		 * WAL archive anyway. But include it as an empty directory anyway, so
		 * we get permissions right.
		 */
		if (strcmp(pathbuf, "./pg_wal") == 0)
		{
			S3TaskLocation location;

			location = s3_schedule_empty_dir_write(state->chkpNum, pathbuf);
			maxLocation = Max(maxLocation, location);

			location = s3_schedule_empty_dir_write(state->chkpNum,
												   "./pg_wal/archive_status");
			maxLocation = Max(maxLocation, location);

			continue;			/* don't recurse into pg_wal */
		}

		/* Allow symbolic links in pg_tblspc only */
		if (strcmp(path, "./pg_tblspc") == 0 && S_ISLNK(statbuf.st_mode))
		{
			char		linkpath[MAXPGPATH];
			int			rllen;

			rllen = readlink(pathbuf, linkpath, sizeof(linkpath));
			if (rllen < 0)
			{
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read symbolic link \"%s\": %m",
								pathbuf)));
				return 0;		/* keep cppcheck quiet */
			}
			if (rllen >= sizeof(linkpath))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("symbolic link \"%s\" target is too long",
								pathbuf)));
			linkpath[rllen] = '\0';
		}
		else if (S_ISDIR(statbuf.st_mode))
		{
			bool		skip_this_dir = false;
			ListCell   *lc;
			S3TaskLocation location;

			location = s3_schedule_empty_dir_write(state->chkpNum, pathbuf);
			maxLocation = Max(maxLocation, location);

			/*
			 * Call ourselves recursively for a directory, unless it happens
			 * to be a separate tablespace located within PGDATA.
			 */
			foreach(lc, state->tablespaces)
			{
				tablespaceinfo *ti = (tablespaceinfo *) lfirst(lc);

				/*
				 * ti->rpath is the tablespace relative path within PGDATA, or
				 * NULL if the tablespace has been properly located somewhere
				 * else.
				 *
				 * Skip past the leading "./" in pathbuf when comparing.
				 */
				if (ti->rpath && strcmp(ti->rpath, pathbuf + 2) == 0)
				{
					skip_this_dir = true;
					break;
				}
			}

			/*
			 * skip sending directories inside pg_tblspc, if not required.
			 */
			if (strcmp(pathbuf, "./pg_tblspc") == 0)
				skip_this_dir = true;

			if (skip_this_dir)
			{
				location = s3_schedule_empty_dir_write(state->chkpNum, pathbuf);
				maxLocation = Max(maxLocation, location);
			}
			else
				size += s3_backup_scan_dir(state, pathbuf,
										   basepathlen, spcoid);
		}
		else if (S_ISREG(statbuf.st_mode))
		{
			S3TaskLocation location;

			if (statbuf.st_size < SMALL_FILE_THRESHOLD)
				location = accumulate_small_file(state, pathbuf, statbuf.st_size);
			else
				location = s3_schedule_pg_file_write(state->chkpNum, pathbuf);
			maxLocation = Max(maxLocation, location);
		}
		else
			ereport(WARNING,
					(errmsg("skipping special file \"%s\"", pathbuf)));
	}
	FreeDir(dir);
	return size;
}

static List *
get_tablespaces(StringInfo tblspcmapfile)
{
	DIR		   *tblspcdir;
	struct dirent *de;
	tablespaceinfo *ti;
	int			datadirpathlen;
	List	   *tablespaces = NIL;

	/*
	 * Construct tablespace_map file.
	 */
	datadirpathlen = strlen(DataDir);

	/* Collect information about all tablespaces */
	tblspcdir = AllocateDir("pg_tblspc");
	while ((de = ReadDir(tblspcdir, "pg_tblspc")) != NULL)
	{
		char		fullpath[MAXPGPATH + 10];
		char		linkpath[MAXPGPATH];
		char	   *relpath = NULL;
		char	   *s;
		PGFileType	de_type;

		/* Skip anything that doesn't look like a tablespace */
		if (strspn(de->d_name, "0123456789") != strlen(de->d_name))
			continue;

		snprintf(fullpath, sizeof(fullpath), "pg_tblspc/%s", de->d_name);

		de_type = get_dirent_type(fullpath, de, false, ERROR);

		if (de_type == PGFILETYPE_LNK)
		{
			StringInfoData escapedpath;
			int			rllen;

			rllen = readlink(fullpath, linkpath, sizeof(linkpath));
			if (rllen < 0)
			{
				ereport(WARNING,
						(errmsg("could not read symbolic link \"%s\": %m",
								fullpath)));
				continue;
			}
			else if (rllen >= sizeof(linkpath))
			{
				ereport(WARNING,
						(errmsg("symbolic link \"%s\" target is too long",
								fullpath)));
				continue;
			}
			linkpath[rllen] = '\0';

			/*
			 * Relpath holds the relative path of the tablespace directory
			 * when it's located within PGDATA, or NULL if it's located
			 * elsewhere.
			 */
			if (rllen > datadirpathlen &&
				strncmp(linkpath, DataDir, datadirpathlen) == 0 &&
				IS_DIR_SEP(linkpath[datadirpathlen]))
				relpath = pstrdup(linkpath + datadirpathlen + 1);

			/*
			 * Add a backslash-escaped version of the link path to the
			 * tablespace map file.
			 */
			initStringInfo(&escapedpath);
			for (s = linkpath; *s; s++)
			{
				if (*s == '\n' || *s == '\r' || *s == '\\')
					appendStringInfoChar(&escapedpath, '\\');
				appendStringInfoChar(&escapedpath, *s);
			}
			appendStringInfo(tblspcmapfile, "%s %s\n",
							 de->d_name, escapedpath.data);
			pfree(escapedpath.data);
		}
		else if (de_type == PGFILETYPE_DIR)
		{
			/*
			 * It's possible to use allow_in_place_tablespaces to create
			 * directories directly under pg_tblspc, for testing purposes
			 * only.
			 *
			 * In this case, we store a relative path rather than an absolute
			 * path into the tablespaceinfo.
			 */
			snprintf(linkpath, sizeof(linkpath), "pg_tblspc/%s",
					 de->d_name);
			relpath = pstrdup(linkpath);
		}
		else
		{
			/* Skip any other file type that appears here. */
			continue;
		}

		ti = palloc(sizeof(tablespaceinfo));
		ti->oid = pstrdup(de->d_name);
		ti->path = pstrdup(linkpath);
		ti->rpath = relpath;
		ti->size = -1;

		tablespaces = lappend(tablespaces, ti);
	}
	FreeDir(tblspcdir);

	return tablespaces;
}

static void
write_int(File file, char *filename, int offset, int value)
{
	if (FileWrite(file, (char *) &value, sizeof(value), offset, WAIT_EVENT_DATA_FILE_WRITE) != sizeof(value))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write temporary file \"%s\": %m",
						filename)));
}

static void
write_data(File file, char *filename, int offset, Pointer ptr, int length)
{
	if (FileWrite(file, ptr, length, offset, WAIT_EVENT_DATA_FILE_WRITE) != length)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write temporary file \"%s\": %m",
						filename)));
}

static Pointer
read_small_file(const char *filename, int size)
{
	File		file;
	Pointer		buffer;
	int			rc;

	buffer = (Pointer) palloc(size);

	file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
	if (file < 0)
		return NULL;

	rc = FileRead(file, buffer, size, 0, WAIT_EVENT_DATA_FILE_READ);

	if (rc < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read small file \"%s\": %m",
						filename)));

	FileClose(file);

	return buffer;
}

static S3TaskLocation
flush_small_files(S3BackupState *state)
{
	ListCell   *lc,
			   *lc2;
	int			totalNamesLen = 0;
	int			offset = 0;
	int			nameOffset;
	int			dataOffset;
	char	   *filename;
	File		file;
	S3TaskLocation location;

	foreach(lc, state->smallFileNames)
		totalNamesLen += strlen(lfirst(lc)) + 1;

	nameOffset = sizeof(int) * (1 + 3 * list_length(state->smallFileNames));
	dataOffset = nameOffset + totalNamesLen;

	filename = psprintf(ORIOLEDB_DATA_DIR "/small_files_%d", state->smallFilesNum);

	file = PathNameOpenFile(filename, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (file <= 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create temporary file \"%s\": %m",
						filename)));
	}

	write_int(file, filename, 0, list_length(state->smallFileNames));
	offset = sizeof(int);

	forboth(lc, state->smallFileNames, lc2, state->smallFileSizes)
	{
		write_int(file, filename, offset, nameOffset);
		write_int(file, filename, offset + sizeof(int), dataOffset);
		write_int(file, filename, offset + 2 * sizeof(int), lfirst_int(lc2));

		nameOffset += strlen(lfirst(lc)) + 1;
		dataOffset += lfirst_int(lc2);
		offset += 3 * sizeof(int);
	}

	foreach(lc, state->smallFileNames)
	{
		int			len = strlen(lfirst(lc)) + 1;

		write_data(file, filename, offset, lfirst(lc), len);
		offset += len;
	}

	forboth(lc, state->smallFileNames, lc2, state->smallFileSizes)
	{
		int			len = lfirst_int(lc2);
		Pointer		data = read_small_file(lfirst(lc), len);

		write_data(file, filename, offset, data, len);
		offset += len;
	}

	FileClose(file);

	location = s3_schedule_file_write(state->chkpNum, filename, true);

	pfree(filename);

	list_free_deep(state->smallFileNames);
	list_free(state->smallFileSizes);
	state->smallFileNames = NIL;
	state->smallFileSizes = NIL;
	state->smallFilesTotalSize = sizeof(int);
	state->smallFilesNum++;

	return location;
}

static S3TaskLocation
accumulate_small_file(S3BackupState *state, const char *path, int size)
{
	int			sizeRequired = 3 * sizeof(int) + strlen(path) + 1 + size;
	S3TaskLocation location = 0;
	Pointer		data;
	uint64		dataSize;

	/* First check if the file changed */
	data = read_file(path, &dataSize);

	if (data != NULL)
	{
		S3FileChecksum *entry;

		if (state->checksumState->fileChecksumsLen == state->checksumState->fileChecksumsMaxLen)
			flushS3ChecksumState(state->checksumState,
								 SMALL_FILE_CHECKSUMS_TMP_FILENAME);

		entry = getS3FileChecksum(state->checksumState, path, data, dataSize);

		/*
		 * If the file didn't change just exit the function and return 0.  We
		 * could return maxLocation here, but it might be better to a caller
		 * to decide.
		 */
		if (!entry->changed)
		{
			pfree(data);
			return 0;
		}

		pfree(data);
	}

	/* The file changed, put it into the smallFileNames list */

	if (state->smallFilesTotalSize + sizeRequired > SMALL_FILES_TOTAL_THRESHOLD)
		location = flush_small_files(state);

	state->smallFileNames = lappend(state->smallFileNames, pstrdup(path));
	state->smallFileSizes = lappend_int(state->smallFileSizes, size);
	state->smallFilesTotalSize += sizeRequired;

	return location;
}
