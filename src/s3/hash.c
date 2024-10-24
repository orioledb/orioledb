/*-------------------------------------------------------------------------
 *
 * hash.c
 *		Declarations for hashing of S3-specific data.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/hash.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "s3/hash.h"

#include "common/string.h"
#include "storage/fd.h"
#include "utils/hsearch.h"

#include "openssl/sha.h"

static void initS3PGFilesHash(S3HashState *state, const char *filename);

/*
 * Allocate S3HashState and initalize hashTable by reading hashFilename.  A
 * caller should prepare a buffer for S3FileHash entries, the caller is
 * responsible to release the buffer.
 */
S3HashState *
makeS3HashState(uint32 checkpointNumber, S3FileHash *pgFiles, uint32 pgFilesSize,
				const char *hashFilename)
{
	S3HashState *res;

	res = (S3HashState *) palloc(sizeof(S3HashState));
	res->hashTable = NULL;
	res->checkpointNumber = checkpointNumber;
	res->pgFiles = pgFiles;
	res->pgFilesSize = pgFilesSize;
	res->pgFilesLen = 0;

	initS3PGFilesHash(res, hashFilename);

	return res;
}

/*
 * Free S3HashState and hashTable.  A caller is responsible to release the
 * buffer of S3FileHash entries.
 */
void
freeS3HashState(S3HashState *state)
{
	if (state == NULL)
		return;

	hash_destroy(state->hashTable);
	pfree(state);
}

/*
 * Initialize a hash table to store checksums of PGDATA files.
 */
static void
initS3PGFilesHash(S3HashState *state, const char *filename)
{
	FILE	   *file;
	StringInfoData buf;
	HASHCTL		ctl;

	Assert(state->hashTable == NULL);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = MAXPGPATH;
	ctl.entrysize = sizeof(S3FileHash);
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

	state->hashTable = hash_create("S3PGFiles hash",
								   32,	/* arbitrary initial size */
								   &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	initStringInfo(&buf);

	while (pg_get_line_buf(file, &buf))
	{
		S3FileHash	fileEntry;

		if (sscanf(buf.data, "FILE: %1023[^,], HASH: %64[^,], CHECKPOINT: %d",
				   fileEntry.filename, fileEntry.hash, &fileEntry.checkpointNumber) == 3)
		{
			char		key[MAXPGPATH];
			S3FileHash *newEntry;
			bool		found;

			MemSet(key, 0, sizeof(key));
			strlcpy(key, fileEntry.filename, sizeof(key));

			if (fileEntry.checkpointNumber >= state->checkpointNumber)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("unexpected checkpoint number in the hash file \"%s\": %s",
								filename, buf.data)));

			/* Put the filename and its checksum into the hash table */
			newEntry = (S3FileHash *) hash_search(state->hashTable,
												  key, HASH_ENTER, &found);
			/* Normally we shouldn't have duplicated keys in the file */
			if (found)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("the file name is duplicated in the hash file \"%s\": %s",
								filename, buf.data)));

			/* filename is already filled in */
			strlcpy(newEntry->hash, fileEntry.hash, sizeof(fileEntry.hash));
			newEntry->checkpointNumber = fileEntry.checkpointNumber;
			newEntry->changed = false;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("invalid line format of the hash file \"%s\": %s",
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
 * Save current workers' PGDATA files hash into a temporary file.
 */
void
flushS3PGFilesHash(S3HashState *state, const char *filename)
{
	FILE	   *file;

	Assert(state->pgFiles != NULL);

	/*
	 * We open the file for append in case if previous flush already created
	 * the file.
	 */
	file = AllocateFile(filename, "a");
	if (file == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", filename)));

	for (int i = 0; i < state->pgFilesLen; i++)
	{
		if (fprintf(file, "FILE: %s, HASH: %s, CHECKPOINT: %u\n",
					state->pgFiles[i].filename, state->pgFiles[i].hash,
					state->pgFiles[i].checkpointNumber) < 0)
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

	state->pgFilesLen = 0;
}

/*
 * Check if a PGDATA file changed since last checkpoint and return PGFileHash.
 */
S3FileHash *
getS3PGFileHash(S3HashState *state, const char *filename,
				Pointer data, uint64 size)
{
	S3FileHash *prevEntry = NULL;
	S3FileHash *newEntry;
	unsigned char hashbuf[PG_SHA256_DIGEST_LENGTH];
	char		hashstringbuf[PG_SHA256_DIGEST_STRING_LENGTH];

	/*
	 * hashTable might not have been initialized before if a hash file hasn't
	 * been found.
	 */
	if (state->hashTable != NULL)
	{
		char		key[MAXPGPATH];

		MemSet(key, 0, sizeof(key));
		strlcpy(key, filename, MAXPGPATH);

		prevEntry = (S3FileHash *) hash_search(state->hashTable, key,
											   HASH_FIND, NULL);
	}

	(void) SHA256((unsigned char *) data, size, hashbuf);

	hex_encode((char *) hashbuf, sizeof(hashbuf), hashstringbuf);
	hashstringbuf[PG_SHA256_DIGEST_STRING_LENGTH - 1] = '\0';

	newEntry = (S3FileHash *) palloc0(sizeof(S3FileHash));

	strlcpy(newEntry->filename, filename, sizeof(newEntry->filename));
	strlcpy(newEntry->hash, hashstringbuf, sizeof(newEntry->hash));

	newEntry->changed = ((prevEntry == NULL) ||
						 (strncmp(prevEntry->hash, newEntry->hash, sizeof(newEntry->hash)) != 0)) ?
		1 : 0;
	newEntry->checkpointNumber = (newEntry->changed) ?
		state->checkpointNumber : prevEntry->checkpointNumber;

	/* Store new entry into the hash state */
	if (state->pgFilesLen >= state->pgFilesSize)
		elog(ERROR, "size of S3FileHash buffer is smaller than requested, "
			 "current size is %d", state->pgFilesSize);

	memcpy(state->pgFiles + state->pgFilesLen, newEntry, sizeof(S3FileHash));
	state->pgFilesLen += 1;

	return newEntry;
}
