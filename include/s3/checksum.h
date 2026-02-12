/*-------------------------------------------------------------------------
 *
 * checksum.h
 * 		Declarations for calculating checksums of S3-specific data.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/checksum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_CHECKSUM_H__
#define __S3_CHECKSUM_H__

#include "utils/hsearch.h"

#include "openssl/sha.h"

#define O_SHA256_DIGEST_STRING_LENGTH	(SHA256_DIGEST_LENGTH * 2 + 1)

typedef struct S3FileChecksum
{
	char		filename[MAXPGPATH];
	char		checksum[O_SHA256_DIGEST_STRING_LENGTH];

	bool		changed;		/* true if the checksum changed since last
								 * checkpoint */
	uint32		checkpointNumber;
} S3FileChecksum;

typedef struct S3ChecksumState
{
	HTAB	   *hashTable;
	uint32		checkpointNumber;

	S3FileChecksum *fileChecksums;	/* Buffer of S3FileChecksum entries */
	uint32		fileChecksumsMaxLen;
	uint32		fileChecksumsLen;
} S3ChecksumState;

extern S3ChecksumState *makeS3ChecksumState(uint32 checkpointNumber,
											S3FileChecksum *fileChecksums,
											uint32 fileChecksumsMaxLen,
											const char *filename);
extern void freeS3ChecksumState(S3ChecksumState *state);
extern void flushS3ChecksumState(S3ChecksumState *state, const char *filename);

extern S3FileChecksum *getS3FileChecksum(S3ChecksumState *state,
										 const char *filename,
										 Pointer data, uint64 size);

#endif							/* __S3_CHECKSUM_H__ */
