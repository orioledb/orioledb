/*-------------------------------------------------------------------------
 *
 * requests.h
 *		Declarations for S3 requests.
 *
 * Copyright (c) 2025-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/requests.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_REQUESTS_H__
#define __S3_REQUESTS_H__

#define S3_RESPONSE_OK					200
#define S3_RESPONSE_NOT_FOUND			404
#define S3_RESPONSE_CONDITION_CONFLICT	409
#define S3_RESPONSE_CONDITION_FAILED	412

extern long s3_put_file(char *objectname, char *filename, bool ifNoneMatch);
extern void s3_get_file(char *objectname, char *filename);
extern void s3_put_empty_dir(char *objectname);
extern long s3_put_file_part(char *objectname, char *filename, int partnum);
extern void s3_get_file_part(char *objectname, char *filename, int partnum);
extern long s3_put_object_with_contents(char *objectname, Pointer data,
										uint64 dataSize, char *dataChecksum,
										bool ifNoneMatch);
extern long s3_get_object(char *objectname, StringInfo str, bool missing_ok);
extern void s3_delete_object(char *objectname);

extern Pointer read_file(const char *filename, uint64 *size);

#endif							/* __S3_REQUESTS_H__ */
