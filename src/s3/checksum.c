/*-------------------------------------------------------------------------
 *
 * checksum.c
 * 		Declarations for calculating checksums of S3-specific data.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/checksum.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "s3/checksum.h"

#include "common/string.h"
#include "storage/fd.h"
#include "utils/hsearch.h"

#include "openssl/sha.h"

static void initHashTable(S3ChecksumState *state, const char *filename);

/*
 * Allocate S3ChecksumState and initalize hashTable by reading filename.  A
 * caller should prepare a buffer for S3FileChecksum entries, the caller is
 * responsible to release the buffer.
 */
S3ChecksumState *
makeS3ChecksumState(uint32 checkpointNumber, S3FileChecksum *fileChecksums,
					uint32 fileChecksumsMaxLen, const char *filename)
{
	S3ChecksumState *res;

	res = (S3ChecksumState *) palloc(sizeof(S3ChecksumState));
	res->hashTable = NULL;
	res->checkpointNumber = checkpointNumber;
	res->fileChecksums = fileChecksums;
	res->fileChecksumsMaxLen = fileChecksumsMaxLen;
	res->fileChecksumsLen = 0;

	initHashTable(res, filename);

	return res;
}

/*
 * Free S3ChecksumState and hashTable.  A caller is responsible to release the
 * buffer of S3FileChecksum entries.
 */
void
freeS3ChecksumState(S3ChecksumState *state)
{
	if (state == NULL)
		return;

	hash_destroy(state->hashTable);
	pfree(state);
}

/*
 * Initialize a hash table to store checksums of database files.
 */
static void
initHashTable(S3ChecksumState *state, const char *filename)
{
	FILE	   *file;
	StringInfoData buf;
	HASHCTL		ctl;

	Assert(state->hashTable == NULL);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = MAXPGPATH;
	ctl.entrysize = sizeof(S3FileChecksum);
	ctl.hcxt = CurrentMemoryContext;

	file = AllocateFile(filename, "r");
	if (file == NULL)
	{
		/* Just ignore if the file doesn't exist */
		if (errno == ENOENT)
			return;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", filename)));
	}

	state->hashTable = hash_create("S3FileChecksum hash table",
								   32,	/* arbitrary initial size */
								   &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	initStringInfo(&buf);

	while (pg_get_line_buf(file, &buf))
	{
		S3FileChecksum fileEntry;

		if (sscanf(buf.data, "FILE: %1023[^,], CHECKSUM: %64[^,], CHECKPOINT: %d",
				   fileEntry.filename, fileEntry.checksum, &fileEntry.checkpointNumber) == 3)
		{
			char		key[MAXPGPATH];
			S3FileChecksum *newEntry;
			bool		found;

			MemSet(key, 0, sizeof(key));
			strlcpy(key, fileEntry.filename, sizeof(key));

			if (fileEntry.checkpointNumber >= state->checkpointNumber)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("unexpected checkpoint number in the checksum file \"%s\": %s",
								filename, buf.data)));

			/* Put the filename and its checksum into the hash table */
			newEntry = (S3FileChecksum *) hash_search(state->hashTable,
													  key, HASH_ENTER, &found);
			/* Normally we shouldn't have duplicated keys in the file */
			if (found)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("the file name is duplicated in the checksum file \"%s\": %s",
								filename, buf.data)));

			/* filename is already filled in */
			strlcpy(newEntry->checksum, fileEntry.checksum, sizeof(fileEntry.checksum));
			newEntry->checkpointNumber = fileEntry.checkpointNumber;
			newEntry->changed = false;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("invalid line format of the checksum file \"%s\": %s",
							filename, buf.data)));
	}

	pfree(buf.data);

	if (ferror(file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", filename)));

	FreeFile(file);
}

/*
 * Save current workers' fileChecksums array into a temporary file.
 */
void
flushS3ChecksumState(S3ChecksumState *state, const char *filename)
{
	FILE	   *file;

	Assert(state->fileChecksums != NULL);

	/*
	 * We open the file for append in case if previous flush already created
	 * the file.
	 */
	file = AllocateFile(filename, "a");
	if (file == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", filename)));

	for (int i = 0; i < state->fileChecksumsLen; i++)
	{
		if (fprintf(file, "FILE: %s, CHECKSUM: %s, CHECKPOINT: %u\n",
					state->fileChecksums[i].filename, state->fileChecksums[i].checksum,
					state->fileChecksums[i].checkpointNumber) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							filename)));
	}

	if (fflush(file) || ferror(file) || FreeFile(file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						filename)));

	state->fileChecksumsLen = 0;
}

/*
 * Check if a PostgreSQL file changed since last checkpoint and return
 * S3FileChecksum.
 */
S3FileChecksum *
getS3FileChecksum(S3ChecksumState *state, const char *filename,
				  Pointer data, uint64 size)
{
	S3FileChecksum *prevEntry = NULL;
	S3FileChecksum *newEntry;
	unsigned char checksumbuf[SHA256_DIGEST_LENGTH];
	char		checksumstringbuf[O_SHA256_DIGEST_STRING_LENGTH];

	/*
	 * hashTable might not have been initialized before if a checksum file
	 * hasn't been found.
	 */
	if (state->hashTable != NULL)
	{
		char		key[MAXPGPATH];

		MemSet(key, 0, sizeof(key));
		strlcpy(key, filename, MAXPGPATH);

		prevEntry = (S3FileChecksum *) hash_search(state->hashTable, key,
												   HASH_FIND, NULL);
	}

	(void) SHA256((unsigned char *) data, size, checksumbuf);

	hex_encode((char *) checksumbuf, sizeof(checksumbuf), checksumstringbuf);
	checksumstringbuf[O_SHA256_DIGEST_STRING_LENGTH - 1] = '\0';

	newEntry = (S3FileChecksum *) palloc0(sizeof(S3FileChecksum));

	strlcpy(newEntry->filename, filename, sizeof(newEntry->filename));
	strlcpy(newEntry->checksum, checksumstringbuf, sizeof(newEntry->checksum));

	if (prevEntry == NULL ||
		strncmp(prevEntry->checksum, newEntry->checksum, sizeof(newEntry->checksum)) != 0)
	{
		newEntry->changed = true;
		newEntry->checkpointNumber = state->checkpointNumber;
	}
	else
	{
		Assert(prevEntry != NULL);

		newEntry->changed = false;
		newEntry->checkpointNumber = prevEntry->checkpointNumber;
	}

	/* Store new entry into the checksum state */
	if (state->fileChecksumsLen >= state->fileChecksumsMaxLen)
		elog(ERROR, "size of S3FileChecksum buffer is smaller than requested, "
			 "current size is %d", state->fileChecksumsMaxLen);

	memcpy(state->fileChecksums + state->fileChecksumsLen, newEntry, sizeof(S3FileChecksum));
	state->fileChecksumsLen += 1;

	return newEntry;
}
