/*-------------------------------------------------------------------------
 *
 * worker.h
 *		Declarations for S3 worker process.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_WORKER_H__
#define __S3_WORKER_H__

#include "orioledb.h"
#include "s3/queue.h"

typedef enum
{
	S3TaskTypeWriteFile,
	S3TaskTypeWriteFilePart,
	S3TaskTypeReadFilePart,
	S3TaskTypeWriteWALFile,
	S3TaskTypeWriteUndoFile,
	S3TaskTypeWriteEmptyDir,
	S3TaskTypeWriteRootFile,
	S3TaskTypeWritePGFile
} S3TaskType;

/*
 * The data structure representing the task for S3 worker.
 */
typedef struct
{
	S3TaskType	type;
	union
	{
		struct
		{
			uint32		chkpNum;
			bool		delete;
			char		filename[FLEXIBLE_ARRAY_MEMBER];
		}			writeFile;
		struct
		{
			UndoLogType undoType;
			uint64		fileNum;
		}			writeUndoFile;
		struct
		{
			uint32		chkpNum;
			Oid			datoid;
			Oid			relnode;
			int32		segNum;
			int32		partNum;
		}			filePart;
		char		walFilename[1];
		struct
		{
			uint32		chkpNum;
			char		dirname[FLEXIBLE_ARRAY_MEMBER];
		}			writeEmptyDir;
		struct
		{
			bool		delete;
			char		filename[FLEXIBLE_ARRAY_MEMBER];
		}			writeRootFile;
		struct
		{
			uint32		chkpNum;
			char		filename[FLEXIBLE_ARRAY_MEMBER];
		}			writePGFile;
	}			typeSpecific;
} S3Task;

#define FILE_CHECKSUMS_FILENAME		ORIOLEDB_DATA_DIR "/file_checksums"

extern Size s3_workers_shmem_needs(void);
extern void s3_workers_init_shmem(Pointer ptr, bool found);
extern void register_s3worker(int num);
extern void s3_workers_checkpoint_init(void);
extern void s3_workers_checkpoint_finish(void);
PGDLLEXPORT void s3worker_main(Datum);

extern S3TaskLocation s3_schedule_file_write(uint32 chkpNum, char *filename,
											 bool delete);
extern S3TaskLocation s3_schedule_empty_dir_write(uint32 chkpNum,
												  char *dirname);
extern S3TaskLocation s3_schedule_file_part_write(uint32 chkpNum, Oid datoid,
												  Oid relnode, int32 segNum,
												  int32 partNum);
extern S3TaskLocation s3_schedule_file_part_read(uint32 chkpNum, Oid datoid,
												 Oid relnode, int32 segNum,
												 int32 partNum);
extern S3TaskLocation s3_schedule_wal_file_write(char *filename);
extern S3TaskLocation s3_schedule_undo_file_write(UndoLogType undoType,
												  uint64 fileNum);
extern S3TaskLocation s3_schedule_downlink_load(struct BTreeDescr *desc,
												uint64 downlink);
extern S3TaskLocation s3_schedule_root_file_write(char *filename, bool delete);
extern S3TaskLocation s3_schedule_pg_file_write(uint32 chkpNum, char *filename);
extern void s3_load_file_part(uint32 chkpNum, Oid datoid, Oid relnode,
							  int32 segNum, int32 partNum);
extern void s3_load_map_file(uint32 chkpNum, Oid datoid, Oid relnode);


#endif							/* __S3_WORKER_H__ */
