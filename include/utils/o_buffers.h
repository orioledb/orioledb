/*-------------------------------------------------------------------------
 *
 * o_buffers.h
 * 		Declarations for buffering layer for file access.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/o_buffers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __O_BUFFERS_H__
#define __O_BUFFERS_H__

typedef struct OBuffersMeta OBuffersMeta;
typedef struct OBuffersGroup OBuffersGroup;

#define	OBuffersMaxTags	(4)
#define OBuffersMaxTagIsValid(tag) \
	((tag) >= 0 && (tag) < OBuffersMaxTags)

typedef struct
{
	/* these fields are initilized by user */
	uint64		singleFileSize;
	const char *filenameTemplate[OBuffersMaxTags];
	const char *groupCtlTrancheName;
	const char *bufferCtlTrancheName;
	uint32		buffersCount;

	/* these fields are initilized in o_buffers.c */
	uint32		groupsCount;
	OBuffersMeta *metaPageBlkno;
	OBuffersGroup *groups;
	File		curFile;
	char		curFileName[MAXPGPATH];
	uint32		curFileTag;
	uint64		curFileNum;
} OBuffersDesc;

extern Size o_buffers_shmem_needs(OBuffersDesc *desc);
extern void o_buffers_shmem_init(OBuffersDesc *desc, void *buf, bool found);
extern void o_buffers_read(OBuffersDesc *desc, Pointer buf,
						   uint32 tag, int64 offset, int64 size);
extern void o_buffers_write(OBuffersDesc *desc, Pointer buf,
							uint32 tag, int64 offset, int64 size);
extern void o_buffers_sync(OBuffersDesc *desc, uint32 tag, int64 fromOffset,
						   int64 toOffset, uint32 wait_event_info);
extern void o_buffers_unlink_files_range(OBuffersDesc *desc,
										 uint32 tag,
										 int64 firstFileNumber,
										 int64 lastFileNumber);

#endif
