/*-------------------------------------------------------------------------
 *
 * worker.c
 *		Routines for S3 worker process.
 *
 * Copyright (c) 2025-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "s3/checksum.h"
#include "s3/headers.h"
#include "s3/queue.h"
#include "s3/requests.h"
#include "s3/worker.h"

#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/sinvaladt.h"
#include "transam/undo.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

#include "pgstat.h"

#include "openssl/sha.h"
#include <fcntl.h>
#include <unistd.h>


#define WORKERS_FILE_CHECKSUMS_MAX_LEN 100

typedef struct S3WorkerCtl
{
	pg_atomic_uint32 fileChecksumsCnt;
	ConditionVariable fileChecksumsFlushedCV;

	/* S3 workers are in progress of putting PostgreSQL files into S3 bucket */
	pg_atomic_flag workersInProgress[FLEXIBLE_ARRAY_MEMBER];
} S3WorkerCtl;

static volatile sig_atomic_t shutdown_requested = false;
static volatile S3TaskLocation *workers_locations = NULL;
static S3FileChecksum *workers_file_checksums = NULL;

static S3WorkerCtl *workers_ctl = NULL;
static S3ChecksumState *checksum_state = NULL;

static int	worker_num;

Size
s3_workers_shmem_needs(void)
{
	Size		size;

	size = CACHELINEALIGN(offsetof(S3WorkerCtl, workersInProgress) +
						  sizeof(pg_atomic_flag) * s3_num_workers);
	size = add_size(size,
					CACHELINEALIGN(mul_size(sizeof(S3TaskLocation), s3_num_workers)));
	size = add_size(size,
					CACHELINEALIGN(mul_size(sizeof(S3FileChecksum),
											s3_num_workers *
											WORKERS_FILE_CHECKSUMS_MAX_LEN)));

	return size;
}

void
s3_workers_init_shmem(Pointer ptr, bool found)
{
	int			i;

	workers_ctl = (S3WorkerCtl *) ptr;
	ptr += CACHELINEALIGN(offsetof(S3WorkerCtl, workersInProgress) +
						  sizeof(pg_atomic_flag) * s3_num_workers);

	workers_locations = (S3TaskLocation *) ptr;
	ptr += CACHELINEALIGN(mul_size(sizeof(S3TaskLocation), s3_num_workers));

	workers_file_checksums = (S3FileChecksum *) ptr;

	if (!found)
	{
		pg_atomic_init_u32(&workers_ctl->fileChecksumsCnt, 0);

		ConditionVariableInit(&workers_ctl->fileChecksumsFlushedCV);

		for (i = 0; i < s3_num_workers; i++)
		{
			workers_locations[i] = InvalidS3TaskLocation;
			pg_atomic_init_flag(&workers_ctl->workersInProgress[i]);
		}
	}
}

static void
handle_sigterm(SIGNAL_ARGS)
{
	shutdown_requested = true;
	SetLatch(MyLatch);
}

void
register_s3worker(int num)
{
	BackgroundWorker worker;

	/* Set up background worker parameters */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_CLASS_SYSTEM;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 0;
	worker.bgw_main_arg = Int32GetDatum(num);
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "s3worker_main");
	pg_snprintf(worker.bgw_name, sizeof(worker.bgw_name),
				"orioledb s3 worker %d", num);
	strcpy(worker.bgw_type, "orioledb s3 worker");
	RegisterBackgroundWorker(&worker);
}

/*
 * Wait until all S3 workers flushed their checksum files.
 */
static void
s3_workers_wait_for_flush(void)
{
	Assert(workers_ctl != NULL);

	for (;;)
	{
		int			all_flushed = pg_atomic_read_u32(&workers_ctl->fileChecksumsCnt) == 0;

		for (int i = 0; (i < s3_num_workers) && all_flushed; i++)
		{
			if (!pg_atomic_unlocked_test_flag(&workers_ctl->workersInProgress[i]))
				all_flushed = false;
		}
		if (all_flushed)
			break;

		ConditionVariableTimedSleep(&workers_ctl->fileChecksumsFlushedCV, BgWriterDelay,
									WAIT_EVENT_BGWRITER_MAIN);
	}

	ConditionVariableCancelSleep();
}

/*
 * Prepare S3 workers to checkpoint database files.
 */
void
s3_workers_checkpoint_init(void)
{
	/* Just in case delete any leftover files */
	for (int i = 0; i < s3_num_workers; i++)
	{
		char		worker_filename[MAXPGPATH];

		pg_atomic_clear_flag(&workers_ctl->workersInProgress[i]);

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 FILE_CHECKSUMS_FILENAME, i);

		unlink(worker_filename);
	}
}

/*
 * Compact all S3 workers checksum files into one file.
 */
void
s3_workers_checkpoint_finish(void)
{
	int			file;

	s3_workers_wait_for_flush();

	file = BasicOpenFile(FILE_CHECKSUMS_FILENAME, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY);
	if (file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", FILE_CHECKSUMS_FILENAME)));

	for (int i = 0; i < s3_num_workers; i++)
	{
		int			worker_file;
		char		worker_filename[MAXPGPATH];
		Size		readBytes;
		char		buffer[8192];

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 FILE_CHECKSUMS_FILENAME, i);

		worker_file = BasicOpenFile(worker_filename, O_RDONLY | PG_BINARY);
		if (worker_file < 0)
		{
			/*
			 * In case if this worker didn't manage to process any
			 * S3TaskTypeWritePGFile just skip it.
			 */
			if (errno == ENOENT)
				continue;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", worker_filename)));
		}

		while ((readBytes = read(worker_file, buffer, sizeof(buffer))) > 0)
		{
			if (write(file, buffer, readBytes) != readBytes)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write file \"%s\": %m", FILE_CHECKSUMS_FILENAME)));
		}

		if (readBytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", worker_filename)));

		close(worker_file);
	}

	if (pg_fsync(file) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", FILE_CHECKSUMS_FILENAME)));

	close(file);

	/* The compaction is completed, now we can remove worker files */
	for (int i = 0; i < s3_num_workers; i++)
	{
		char		worker_filename[MAXPGPATH];

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 FILE_CHECKSUMS_FILENAME, i);

		unlink(worker_filename);
	}
}

static S3FileChecksum *
get_worker_file_checksums(void)
{
	return workers_file_checksums + WORKERS_FILE_CHECKSUMS_MAX_LEN * worker_num;
}

static void
flush_worker_checksum_state(void)
{
	char		filename[MAXPGPATH];

	Assert(checksum_state != NULL);
	Assert(checksum_state->fileChecksumsLen > 0);

	snprintf(filename, MAXPGPATH, "%s.%d", FILE_CHECKSUMS_FILENAME, worker_num);

	flushS3ChecksumState(checksum_state, filename);
}

/*
 * Process the task at given location.
 */
static void
s3process_task(uint64 taskLocation)
{
	S3Task	   *task = (S3Task *) s3_queue_get_task(taskLocation);
	char	   *objectname;

	Assert(workers_ctl != NULL);

	if (task->type == S3TaskTypeWriteFile)
	{
		char	   *filename = task->typeSpecific.writeFile.filename;
		long		result;

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%u/%s",
							  task->typeSpecific.writeFile.chkpNum,
							  filename);

		elog(DEBUG1, "S3 put %s %s", objectname, filename);

		result = s3_put_file(objectname, filename, false);

		pfree(objectname);

		if ((result == S3_RESPONSE_OK) && task->typeSpecific.writeFile.delete)
			unlink(filename);
	}
	else if (task->type == S3TaskTypeWriteEmptyDir)
	{
		char	   *dirname = task->typeSpecific.writeEmptyDir.dirname;

		if (dirname[0] == '.' && dirname[1] == '/')
			dirname += 2;

		objectname = psprintf("data/%u/%s/",
							  task->typeSpecific.writeFile.chkpNum,
							  dirname);

		elog(DEBUG1, "S3 dir put %s %s", objectname, dirname);

		s3_put_empty_dir(objectname);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeReadFilePart &&
			 task->typeSpecific.filePart.segNum < 0)
	{
		char	   *filename;
		SeqBufTag	chkp_tag;

		memset(&chkp_tag, 0, sizeof(chkp_tag));
		chkp_tag.datoid = task->typeSpecific.filePart.datoid;
		chkp_tag.relnode = task->typeSpecific.filePart.relnode;
		chkp_tag.num = task->typeSpecific.filePart.chkpNum;
		chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&chkp_tag);

		objectname = psprintf("orioledb_data/%u/%u/%u.map",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode);

		elog(DEBUG1, "S3 map get %s %s", objectname, filename);

		s3_get_file(objectname, filename);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeReadFilePart)
	{
		char	   *filename;
		S3HeaderTag tag;

		filename = btree_filename(task->typeSpecific.filePart.datoid,
								  task->typeSpecific.filePart.relnode,
								  task->typeSpecific.filePart.segNum,
								  task->typeSpecific.filePart.chkpNum);

		objectname = psprintf("orioledb_data/%u/%u/%u.%u.%u",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode,
							  task->typeSpecific.filePart.segNum,
							  task->typeSpecific.filePart.partNum);

		elog(DEBUG1, "S3 part get %s %s", objectname, filename);

		s3_get_file_part(objectname, filename, task->typeSpecific.filePart.partNum);

		tag.datoid = task->typeSpecific.filePart.datoid;
		tag.relnode = task->typeSpecific.filePart.relnode;
		tag.checkpointNum = task->typeSpecific.filePart.chkpNum;
		tag.segNum = task->typeSpecific.filePart.segNum;

		s3_header_mark_part_loaded(tag, task->typeSpecific.filePart.partNum);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.filePart.segNum >= 0)
	{
		char	   *filename;
		S3HeaderTag tag;

		filename = btree_filename(task->typeSpecific.filePart.datoid,
								  task->typeSpecific.filePart.relnode,
								  task->typeSpecific.filePart.segNum,
								  task->typeSpecific.filePart.chkpNum);

		objectname = psprintf("orioledb_data/%u/%u/%u.%u.%u",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode,
							  task->typeSpecific.filePart.segNum,
							  task->typeSpecific.filePart.partNum);

		elog(DEBUG1, "S3 part put %s %s", objectname, filename);

		tag.datoid = task->typeSpecific.filePart.datoid;
		tag.relnode = task->typeSpecific.filePart.relnode;
		tag.checkpointNum = task->typeSpecific.filePart.chkpNum;
		tag.segNum = task->typeSpecific.filePart.segNum;

		s3_header_mark_part_writing(tag, task->typeSpecific.filePart.partNum);

		PG_TRY();
		{
			(void) s3_put_file_part(objectname, filename, task->typeSpecific.filePart.partNum);
		}
		PG_CATCH();
		{
			s3_header_mark_part_not_written(tag, task->typeSpecific.filePart.partNum);
			PG_RE_THROW();
		}
		PG_END_TRY();

		s3_header_mark_part_written(tag, task->typeSpecific.filePart.partNum);
		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.filePart.segNum < 0)
	{
		char	   *filename;
		SeqBufTag	chkp_tag;

		memset(&chkp_tag, 0, sizeof(chkp_tag));
		chkp_tag.datoid = task->typeSpecific.filePart.datoid;
		chkp_tag.relnode = task->typeSpecific.filePart.relnode;
		chkp_tag.num = task->typeSpecific.filePart.chkpNum;
		chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&chkp_tag);

		objectname = psprintf("orioledb_data/%u/%u/%u.map",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode);

		elog(DEBUG1, "S3 map put %s %s", objectname, filename);

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteWALFile)
	{
		char	   *filename;

		filename = psprintf(XLOGDIR "/%s", task->typeSpecific.walFilename);
		objectname = psprintf("wal/%s", task->typeSpecific.walFilename);

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteUndoFile)
	{
		uint64		fileNum = task->typeSpecific.writeUndoFile.fileNum;
		char	   *filename;

		if (task->typeSpecific.writeUndoFile.undoType == UndoLogRegular)
		{
			filename = psprintf(ORIOLEDB_UNDO_DATA_ROW_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xrow",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum);
		}
		else if (task->typeSpecific.writeUndoFile.undoType == UndoLogRegularPageLevel)
		{
			filename = psprintf(ORIOLEDB_UNDO_DATA_PAGE_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xpage",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum);
		}
		else if (task->typeSpecific.writeUndoFile.undoType == UndoLogSystem)
		{
			filename = psprintf(ORIOLEDB_UNDO_SYSTEM_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xsystem",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum);
		}
		else
		{
			Assert(false);
			filename = NULL;
			objectname = NULL;
		}

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteRootFile)
	{
		char	   *filename = task->typeSpecific.writeRootFile.filename;
		long		result;

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%s", filename);

		elog(DEBUG1, "S3 put %s %s", objectname, filename);

		result = s3_put_file(objectname, filename, false);

		pfree(objectname);

		if ((result == S3_RESPONSE_OK) && task->typeSpecific.writeRootFile.delete)
			unlink(filename);
	}
	else if (task->type == S3TaskTypeWritePGFile)
	{
		char	   *filename = task->typeSpecific.writePGFile.filename;
		Pointer		data;
		uint64		size;

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%u/%s",
							  task->typeSpecific.writePGFile.chkpNum,
							  filename);

		elog(DEBUG1, "S3 PG file put %s %s", objectname, filename);

		data = read_file(filename, &size);

		if (data != NULL)
		{
			S3FileChecksum *entry;

			pg_atomic_test_set_flag(&workers_ctl->workersInProgress[worker_num]);

			if (checksum_state == NULL)
				checksum_state = makeS3ChecksumState(task->typeSpecific.writePGFile.chkpNum,
													 get_worker_file_checksums(),
													 WORKERS_FILE_CHECKSUMS_MAX_LEN,
													 FILE_CHECKSUMS_FILENAME);

			Assert(checksum_state->checkpointNumber == task->typeSpecific.writePGFile.chkpNum);

			if (checksum_state->fileChecksumsLen == WORKERS_FILE_CHECKSUMS_MAX_LEN)
				flush_worker_checksum_state();

			entry = getS3FileChecksum(checksum_state, filename, data, size);

			if (entry->changed)
				(void) s3_put_object_with_contents(objectname, data, size,
												   entry->checksum, false);

			pfree(data);
		}

		pfree(objectname);

		/* Mark this task as processed */
		pg_atomic_fetch_sub_u32(&workers_ctl->fileChecksumsCnt, 1);
	}

	pfree(task);
	s3_queue_erase_task(taskLocation);
}

/*
 * Schedule a synchronization of given data file to S3.
 */
S3TaskLocation
s3_schedule_file_write(uint32 chkpNum, char *filename, bool delete)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteFile;
	task->typeSpecific.writeFile.chkpNum = chkpNum;
	task->typeSpecific.writeFile.delete = delete;
	memcpy(task->typeSpecific.writeFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule file write: %s %u %u (%llu)",
		 filename, chkpNum, delete ? 1 : 0, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given empty directory to S3.
 */
S3TaskLocation
s3_schedule_empty_dir_write(uint32 chkpNum, char *dirname)
{
	S3Task	   *task;
	int			dirnameLen,
				taskLen;
	S3TaskLocation location;

	dirnameLen = strlen(dirname);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeEmptyDir.dirname) +
					   dirnameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteEmptyDir;
	task->typeSpecific.writeEmptyDir.chkpNum = chkpNum;
	memcpy(task->typeSpecific.writeEmptyDir.dirname, dirname, dirnameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule empty dir write: %s %u (%llu)",
		 dirname, chkpNum, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given data file part to S3.
 */
S3TaskLocation
s3_schedule_file_part_write(uint32 chkpNum, Oid datoid, Oid relnode,
							int32 segNum, int32 partNum)
{
	S3Task	   *task;
	S3TaskLocation location;
	S3HeaderTag tag = {.datoid = datoid,.relnode = relnode,.checkpointNum = chkpNum,.segNum = segNum};

	if (partNum >= 0 && !s3_header_mark_part_scheduled_for_write(tag, partNum))
		return (S3TaskLocation) 0;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.datoid = datoid;
	task->typeSpecific.filePart.relnode = relnode;
	task->typeSpecific.filePart.segNum = segNum;
	task->typeSpecific.filePart.partNum = partNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule file part write: %u %u %u %d %d (%llu)",
		 datoid, relnode, chkpNum, segNum, partNum, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule the read of given data file part from S3.
 */
S3TaskLocation
s3_schedule_file_part_read(uint32 chkpNum, Oid datoid, Oid relnode,
						   int32 segNum, int32 partNum)
{
	S3Task	   *task;
	S3TaskLocation location;
	S3PartStatus status;
	S3HeaderTag tag = {.datoid = datoid,.relnode = relnode,.checkpointNum = chkpNum,.segNum = segNum};

	status = s3_header_mark_part_loading(tag, partNum);
	if (status == S3PartStatusLoading)
	{
		/*
		 * The task is already scheduled.  We don't know the location, but we
		 * know it's lower than current insert location.
		 */
		return s3_queue_get_insert_location();
	}
	else if (status == S3PartStatusLoaded)
	{
		return 0;
	}
	Assert(status == S3PartStatusNotLoaded);

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeReadFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.datoid = datoid;
	task->typeSpecific.filePart.relnode = relnode;
	task->typeSpecific.filePart.segNum = segNum;
	task->typeSpecific.filePart.partNum = partNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule file part read: %u %u %u %d %d (%llu)",
		 datoid, relnode, chkpNum, segNum, partNum, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given WAL file to S3.
 */
S3TaskLocation
s3_schedule_wal_file_write(char *filename)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.walFilename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteWALFile;
	memcpy(task->typeSpecific.walFilename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule WAL file write: %s (%llu)",
		 filename, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given UNDO file to S3.
 */
S3TaskLocation
s3_schedule_undo_file_write(UndoLogType undoType, uint64 fileNum)
{
	S3Task	   *task;
	S3TaskLocation location;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteUndoFile;
	task->typeSpecific.writeUndoFile.undoType = undoType;
	task->typeSpecific.writeUndoFile.fileNum = fileNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule UNDO file write: %llu (%llu)",
		 (unsigned long long) fileNum, (unsigned long long) location);

	pfree(task);

	return location;
}


/*
 * Schedule the load of given downlink from S3 to local storage.
 */
S3TaskLocation
s3_schedule_downlink_load(BTreeDescr *desc, uint64 downlink)
{
	uint64		offset = DOWNLINK_GET_DISK_OFF(downlink);
	uint16		len = DOWNLINK_GET_DISK_LEN(downlink);
	off_t		byte_offset,
				read_size;
	uint32		chkpNum;
	int32		segNum,
				partNum;
	S3TaskLocation result = 0;

	chkpNum = S3_GET_CHKP_NUM(offset);
	offset &= S3_OFFSET_MASK;

	if (!OCompressIsValid(desc->compress))
	{
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_BLCKSZ;
		read_size = ORIOLEDB_BLCKSZ;
	}
	else
	{
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
		read_size = len * ORIOLEDB_COMP_BLCKSZ;
	}

	while (true)
	{
		S3TaskLocation location;

		segNum = byte_offset / ORIOLEDB_SEGMENT_SIZE;
		partNum = (byte_offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
		location = s3_schedule_file_part_read(chkpNum,
											  desc->oids.datoid, desc->oids.relnode,
											  segNum, partNum);
		result = Max(result, location);
		if (byte_offset % ORIOLEDB_S3_PART_SIZE + read_size > ORIOLEDB_S3_PART_SIZE)
		{
			uint64		shift = ORIOLEDB_S3_PART_SIZE - byte_offset % ORIOLEDB_S3_PART_SIZE;

			byte_offset += shift;
			read_size -= shift;
		}
		else
		{
			break;
		}
	}

	return result;
}

/*
 * Schedule a synchronization of given file to S3.
 */
S3TaskLocation
s3_schedule_root_file_write(char *filename, bool delete)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeRootFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteRootFile;
	task->typeSpecific.writeRootFile.delete = delete;
	memcpy(task->typeSpecific.writeRootFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule root file write: %s %u (%llu)",
		 filename, delete ? 1 : 0, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given PGDATA file to S3.
 */
S3TaskLocation
s3_schedule_pg_file_write(uint32 chkpNum, char *filename)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	Assert(workers_ctl != NULL);

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writePGFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWritePGFile;
	task->typeSpecific.writePGFile.chkpNum = chkpNum;
	memcpy(task->typeSpecific.writePGFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule PGDATA file write: %s %u (%llu)",
		 filename, chkpNum, (unsigned long long) location);

	pfree(task);

	pg_atomic_fetch_add_u32(&workers_ctl->fileChecksumsCnt, 1);

	return location;
}

void
s3_load_file_part(uint32 chkpNum, Oid datoid, Oid relnode,
				  int32 segNum, int32 partNum)
{
	S3TaskLocation location;

	location = s3_schedule_file_part_read(chkpNum, datoid, relnode,
										  segNum, partNum);

	s3_queue_wait_for_location(location);
}

void
s3_load_map_file(uint32 chkpNum, Oid datoid, Oid relnode)
{
	S3TaskLocation location;

	location = s3_schedule_file_part_read(chkpNum, datoid, relnode,
										  -1, 0);

	s3_queue_wait_for_location(location);
}

void
s3worker_main(Datum main_arg)
{
	int			rc,
				wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT;

	worker_num = Int32GetDatum(main_arg);

	/* enable timeout for relation lock */
	RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);

	/* enable relation cache invalidation (remove old OTableDescr) */
	RelationCacheInitialize();
	InitCatalogCache();
	SharedInvalBackendInit(false);

	/* show the s3 worker in pg_stat_activity, */
	InitializeSessionUserIdStandalone();
	pgstat_beinit();
	pgstat_bestart();

	SetProcessingMode(NormalProcessing);

	/* catch SIGTERM signal for reason to not interupt background writing */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	elog(LOG, "orioledb s3 worker %d started", worker_num);

	CurTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb s3worker current transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb s3worker top transaction context",
												  ALLOCSET_DEFAULT_SIZES);

	ResetLatch(MyLatch);

	PG_TRY();
	{
		MemoryContextSwitchTo(CurTransactionContext);

		/*
		 * There might be task to process saved into shared memory.  If so,
		 * pick and process it.
		 */
		if (workers_locations[worker_num] != InvalidS3TaskLocation)
		{
			s3process_task(workers_locations[worker_num]);
			workers_locations[worker_num] = InvalidS3TaskLocation;
		}

		while (true)
		{
			uint64		taskLocation;

			if (shutdown_requested)
				break;

			/*
			 * Sleep until we are signaled or it's time to check the queue.
			 */
			rc = WaitLatch(MyLatch, wake_events,
						   BgWriterDelay,
						   WAIT_EVENT_BGWRITER_MAIN);

			if (rc & WL_POSTMASTER_DEATH)
				shutdown_requested = true;

			/*
			 * Task processing loop.  It might happend that error occurs and
			 * worker restarts.  We save the task location to the shared
			 * memory to be able to process it after restart.
			 */
			while ((taskLocation = s3_queue_try_pick_task()) != InvalidS3TaskLocation)
			{
				workers_locations[worker_num] = taskLocation;
				s3process_task(taskLocation);
				workers_locations[worker_num] = InvalidS3TaskLocation;
			}

			if (!pg_atomic_unlocked_test_flag(&workers_ctl->workersInProgress[worker_num]) &&
				pg_atomic_read_u32(&workers_ctl->fileChecksumsCnt) == 0)
			{
				/* checksum_state might be NULL if the worker restarted */
				if (checksum_state != NULL)
				{
					if (checksum_state->fileChecksumsLen > 0)
						flush_worker_checksum_state();

					freeS3ChecksumState(checksum_state);
					checksum_state = NULL;
				}

				pg_atomic_clear_flag(&workers_ctl->workersInProgress[worker_num]);
				ConditionVariableBroadcast(&workers_ctl->fileChecksumsFlushedCV);
			}

			ResetLatch(MyLatch);
		}
		elog(LOG, "orioledb s3 worker %d is shut down", worker_num);
	}
	PG_CATCH();
	{
		LockReleaseSession(DEFAULT_LOCKMETHOD);
		PG_RE_THROW();
	}
	PG_END_TRY();
}
