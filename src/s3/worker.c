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
#include "s3/queue.h"
#include "s3/requests.h"
#include "s3/worker.h"

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
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.writeFilePart.segNum >= 0)
	{
		char	   *filename;

		filename = btree_filename(task->typeSpecific.writeFilePart.datoid,
								  task->typeSpecific.writeFilePart.relnode,
								  task->typeSpecific.writeFilePart.segNum,
								  task->typeSpecific.writeFilePart.chkpNum);

		objectname = psprintf("orioledb_data/%u/%u/%u.%u.%u",
							  task->typeSpecific.writeFile.chkpNum,
							  task->typeSpecific.writeFilePart.datoid,
							  task->typeSpecific.writeFilePart.relnode,
							  task->typeSpecific.writeFilePart.segNum,
							  task->typeSpecific.writeFilePart.partNum);

		elog(DEBUG1, "S3 part put %s %s", objectname, filename);

		s3_put_file_part(objectname, filename, task->typeSpecific.writeFilePart.partNum);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.writeFilePart.segNum < 0)
	{
		char	   *filename;
		SeqBufTag	chkp_tag;

		memset(&chkp_tag, 0, sizeof(chkp_tag));
		chkp_tag.datoid = task->typeSpecific.writeFilePart.datoid;
		chkp_tag.relnode = task->typeSpecific.writeFilePart.relnode;
		chkp_tag.num = task->typeSpecific.writeFilePart.chkpNum;
		chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&chkp_tag);

		objectname = psprintf("orioledb_data/%u/%u/%u.map",
							  task->typeSpecific.writeFile.chkpNum,
							  task->typeSpecific.writeFilePart.datoid,
							  task->typeSpecific.writeFilePart.relnode);

		elog(DEBUG1, "S3 map put %s %s", objectname, filename);

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
s3_schedule_file_write(uint32 chkpNum, char *filename)
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
	memcpy(task->typeSpecific.writeFile.filename, filename, filenameLen + 1);

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
	task->typeSpecific.writeFilePart.chkpNum = chkpNum;
	task->typeSpecific.writeFilePart.datoid = datoid;
	task->typeSpecific.writeFilePart.relnode = relnode;
	task->typeSpecific.writeFilePart.segNum = segNum;
	task->typeSpecific.writeFilePart.partNum = partNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	pfree(task);

	return location;
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
