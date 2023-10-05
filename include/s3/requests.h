/*-------------------------------------------------------------------------
 *
 * requests.h
 *		Declarations for S3 requests.
 *
 * Copyright (c) 2023, OrioleDATA Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/requests.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_REQUESTS_H__
#define __S3_REQUESTS_H__

extern void s3_put_file(char *objectname, char *filename);
extern void s3_put_empty_dir(char *objectname);
extern void s3_put_file_part(char *objectname, char *filename, int partnum);
extern void s3_get_file_part(char *objectname, char *filename, int partnum);

#endif							/* __S3_REQUESTS_H__ */
