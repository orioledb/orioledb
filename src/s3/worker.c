/*-------------------------------------------------------------------------
 *
 * worker.c
 *		Routines for S3 worker process.
 *
 * Copyright (c) 2023, OrioleDATA Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
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
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

#include "pgstat.h"

#include <unistd.h>

static volatile sig_atomic_t shutdown_requested = false;
static volatile S3TaskLocation *workers_locations = NULL;

Size
s3_workers_shmem_needs(void)
{
	Size		size;

	size = CACHELINEALIGN(sizeof(S3TaskLocation) * s3_num_workers);

	return size;
}

void
s3_workers_init_shmem(Pointer ptr, bool found)
{
	int			i;

	workers_locations = (S3TaskLocation *) ptr;

	for (i = 0; i < s3_num_workers; i++)
		workers_locations[i] = InvalidS3TaskLocation;
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
 * Process the task at given location.
 */
static void
s3process_task(uint64 taskLocation)
{
	S3Task	   *task = (S3Task *) s3_queue_get_task(taskLocation);
	char	   *objectname;

	if (task->type == S3TaskTypeWriteFile)
	{
		char	   *filename = task->typeSpecific.writeFile.filename;

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%u/%s",
							  task->typeSpecific.writeFile.chkpNum,
							  filename);

		elog(DEBUG1, "S3 put %s %s", objectname, filename);

		s3_put_file(objectname, filename);

		pfree(objectname);

		if (task->typeSpecific.writeFile.delete)
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
	else if (task->type == S3TaskTypeReadFilePart)
	{
		char	   *filename;
		S3HeaderTag tag;

		filename = btree_filename(task->typeSpecific.filePart.datoid,
								  task->typeSpecific.filePart.relnode,
								  task->typeSpecific.filePart.segNum,
								  task->typeSpecific.filePart.chkpNum);

		objectname = psprintf("orioledb_data/%u/%u/%u.%u.%u",
							  task->typeSpecific.writeFile.chkpNum,
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

		filename = btree_filename(task->typeSpecific.filePart.datoid,
								  task->typeSpecific.filePart.relnode,
								  task->typeSpecific.filePart.segNum,
								  task->typeSpecific.filePart.chkpNum);

		objectname = psprintf("orioledb_data/%u/%u/%u.%u.%u",
							  task->typeSpecific.writeFile.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode,
							  task->typeSpecific.filePart.segNum,
							  task->typeSpecific.filePart.partNum);

		elog(DEBUG1, "S3 part put %s %s", objectname, filename);

		s3_put_file_part(objectname, filename, task->typeSpecific.filePart.partNum);

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
							  task->typeSpecific.writeFile.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode);

		elog(DEBUG1, "S3 map put %s %s", objectname, filename);

		s3_put_file(objectname, filename);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteWALFile)
	{
		char	   *filename;

		filename = psprintf(XLOGDIR "/%s", task->typeSpecific.walFilename);
		objectname = psprintf("wal/%s", task->typeSpecific.walFilename);

		s3_put_file(objectname, filename);

		pfree(filename);
		pfree(objectname);
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

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.datoid = datoid;
	task->typeSpecific.filePart.relnode = relnode;
	task->typeSpecific.filePart.segNum = segNum;
	task->typeSpecific.filePart.partNum = partNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

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
		return InvalidS3TaskLocation;
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
		segNum = byte_offset / ORIOLEDB_SEGMENT_SIZE;
		partNum = (byte_offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
		s3_schedule_file_part_read(chkpNum, desc->oids.datoid, desc->oids.relnode,
								   segNum, partNum);
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

	return InvalidS3TaskLocation;
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
s3worker_main(Datum main_arg)
{
	int			rc,
				wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
				num = Int32GetDatum(main_arg);

	/* enable timeout for relation lock */
	RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);

	/* enable relation cache invalidation (remove old OTableDescr) */
	RelationCacheInitialize();
	InitCatalogCache();
	SharedInvalBackendInit(false);

	/* show the s3 worker in pg_stat_activity, */
	InitializeSessionUserIdStandalone();
#if PG_VERSION_NUM >= 140000
	pgstat_beinit();
#else
	pgstat_initialize();
#endif
	pgstat_bestart();

	SetProcessingMode(NormalProcessing);

	/* catch SIGTERM signal for reason to not interupt background writing */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	elog(LOG, "orioledb s3 worker %d started", num);

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
		if (workers_locations[num] != InvalidS3TaskLocation)
		{
			s3process_task(workers_locations[num]);
			workers_locations[num] = InvalidS3TaskLocation;
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
				workers_locations[num] = taskLocation;
				s3process_task(taskLocation);
				workers_locations[num] = InvalidS3TaskLocation;
			}

			ResetLatch(MyLatch);
		}
		elog(LOG, "orioledb s3 worker %d is shut down", num);
	}
	PG_CATCH();
	{
		LockReleaseSession(DEFAULT_LOCKMETHOD);
		PG_RE_THROW();
	}
	PG_END_TRY();
}
