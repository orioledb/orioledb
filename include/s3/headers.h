/*-------------------------------------------------------------------------
 *
 * headers.h
 * 		Declarations for handling of S3-specific data file headers.
 *
 * Copyright (c) 2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/headers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_HEADERS_H__
#define __S3_HEADERS_H__

typedef struct
{
	Oid			datoid;
	Oid			relnode;
	uint32		checkpointNum;
	int			segNum;
} S3HeaderTag;

typedef enum
{
	S3PartStatusNotLoaded = 0,
	S3PartStatusLoading = 1,
	S3PartStatusLoaded = 2,
	S3PartStatusEvicting = 3
} S3PartStatus;

#define S3HeaderTagsIsEqual(t1, t2) \
	((t1).datoid == (t2).datoid && \
	 (t1).relnode == (t2).relnode && \
	 (t1).checkpointNum == (t2).checkpointNum && \
	 (t1).segNum == (t2).segNum)

extern int	s3_headers_buffers_size;

extern Size s3_headers_shmem_needs(void);
extern void s3_headers_shmem_init(Pointer buf, bool found);
extern void s3_headers_increase_loaded_parts(uint64 inc);
extern bool s3_header_lock_part(S3HeaderTag tag, int index);
extern S3PartStatus s3_header_mark_part_loading(S3HeaderTag tag, int index);
extern void s3_header_mark_part_loaded(S3HeaderTag tag, int index);
extern void s3_header_unlock_part(S3HeaderTag tag, int index, bool setDirty);
extern void s3_header_mark_part_writing(S3HeaderTag tag, int index);
extern void s3_header_mark_part_written(S3HeaderTag tag, int index);
extern void s3_headers_sync(void);
extern void s3_headers_error_cleanup(void);
extern void s3_headers_try_eviction_cycle(void);

#endif							/* __S3_HEADERS_H__ */
