/*-------------------------------------------------------------------------
 *
 * hash.h
 * 		Declarations for hashing of S3-specific data.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/hash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_HASH_H__
#define __S3_HASH_H__

#include "common/sha2.h"
#include "utils/hsearch.h"

typedef struct S3FileHash
{
	char		filename[MAXPGPATH];
	char		hash[PG_SHA256_DIGEST_STRING_LENGTH];
	bool		changed;		/* true if crc changed since last checkpoint */
	uint32		checkpointNumber;
} S3FileHash;

typedef struct S3HashState
{
	HTAB	   *hashTable;
	uint32		checkpointNumber;
	S3FileHash *pgFiles;		/* Buffer of S3FilesHash entries */
	uint32		pgFilesSize;
	uint32		pgFilesLen;
} S3HashState;

extern S3HashState *makeS3HashState(uint32 checkpointNumber,
									S3FileHash *pgFiles, uint32 pgFilesSize,
									const char *hashFilename);
extern void freeS3HashState(S3HashState *state);
extern void flushS3PGFilesHash(S3HashState *state, const char *filename);

extern S3FileHash *getS3PGFileHash(S3HashState *state, const char *filename,
								   Pointer data, uint64 size);

#endif							/* __S3_HASH_H__ */
