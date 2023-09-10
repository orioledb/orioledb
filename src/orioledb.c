/*-------------------------------------------------------------------------
 *
 * orioledb.c
 *		Main file: setup shared memory, hooks and other general-purpose
 *		routines.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/orioledb.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/io.h"
#include "btree/scan.h"
#include "catalog/o_tables.h"
#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "s3/queue.h"
#include "s3/worker.h"
#include "tableam/handler.h"
#include "tableam/scan.h"
#include "tableam/toast.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "tuple/toast.h"
#include "utils/compress.h"
#include "utils/memdebug.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"
#include "utils/ucm.h"
#include "workers/bgwriter.h"

#include "access/table.h"
#include "access/xlog_internal.h"
#include "catalog/pg_enum.h"
#include "executor/execExpr.h"
#include "funcapi.h"
#include "libpq/auth.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "postmaster/startup.h"
#include "replication/message.h"
#include "replication/walsender.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proclist.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/rangetypes.h"
#include "utils/pg_locale.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>

PG_MODULE_MAGIC;

void		_PG_init(void);

static bool debug_disable_pools_limit = false;
static Pointer shared_segment = NULL;
static bool shared_segment_initialized = false;
static int	free_tree_buffers_guc;
static Size free_tree_buffers_count;
static int	catalog_buffers_guc;
static Size catalog_buffers_count;
static Size main_buffers_offset;

Pointer		o_shared_buffers = NULL;
Pointer		o_undo_buffers = NULL;
OrioleDBPageDesc *page_descs = NULL;

/* Custom GUC variables */
int			main_buffers_guc;
static int	undo_buffers_guc;
static int	xid_buffers_guc;
int			max_procs;
Size		orioledb_buffers_size;
Size		orioledb_buffers_count;
Size		page_descs_size;
Size		undo_circular_buffer_size;
uint32		undo_buffers_count;
Size		xid_circular_buffer_size;
uint32		xid_buffers_count;
bool		remove_old_checkpoint_files = true;
bool		debug_disable_bgwriter = false;
bool		use_mmap = false;
bool		use_device = false;
char	   *device_filename = NULL;
Pointer		mmap_data = NULL;
int			device_fd;
int			device_length_guc = 0;
Size		device_length = 0;
double		o_checkpoint_completion_ratio;
int			bgwriter_num_workers = 1;
int			max_io_concurrency = 0;
ODBProcData *oProcData;
int			default_compress = InvalidOCompress;
int			default_primary_compress = InvalidOCompress;
int			default_toast_compress = InvalidOCompress;
#if PG_VERSION_NUM >= 140000
bool		orioledb_table_description_compress = false;
#endif
bool		orioledb_s3_mode = false;
int			s3_num_workers = 3;
int			s3_queue_size_guc;
char	   *s3_host = NULL;
char	   *s3_region = NULL;
char	   *s3_accesskey = NULL;
char	   *s3_secretkey = NULL;

/* Previous values of hooks to chain call them */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static void (*prev_shmem_request_hook) (void) = NULL;
static get_relation_info_hook_type prev_get_relation_info_hook = NULL;
CheckPoint_hook_type next_CheckPoint_hook = NULL;
static bool o_newlocale_from_collation(void);

/*
 * Temporary memory context for BTree operations. Helps us to avoid
 * excessive code complexity.
 */
MemoryContext btree_insert_context = NULL;

/*
 * Memory context for btree sequential scans.  Scans needs to survive till
 * seq_scans_cleanup().
 */
MemoryContext btree_seqscan_context = NULL;

OPagePool	page_pools[OPagePoolTypesCount];

static size_t page_pools_size[OPagePoolTypesCount];

static Size o_proc_shmem_needs(void);
static void o_proc_shmem_init(Pointer ptr, bool found);
static Size ppools_shmem_needs(void);
static void ppools_shmem_init(Pointer ptr, bool found);

typedef struct
{
	Size		(*shmem_size) (void);
	void		(*shmem_init) (Pointer ptr, bool found);
} ShmemItem;

/*
 * checkpoint_shmem_init() should be before recovery_shmem_init().
 * See recovery_shmem_init() for description.
 */
static ShmemItem shmemItems[] = {
	{btree_io_shmem_needs, btree_io_shmem_init},
	{oxid_shmem_needs, oxid_init_shmem},
	{sys_trees_shmem_needs, sys_trees_shmem_init},
	{StopEventShmemSize, StopEventShmemInit},
	{undo_shmem_needs, undo_shmem_init},
	{checkpoint_shmem_size, checkpoint_shmem_init},
	{recovery_shmem_needs, recovery_shmem_init},
	{o_proc_shmem_needs, o_proc_shmem_init},
	{ppools_shmem_needs, ppools_shmem_init},
	{btree_scan_shmem_needs, btree_scan_init_shmem},
	{s3_queue_shmem_needs, s3_queue_init_shmem},
	{s3_workers_shmem_needs, s3_workers_init_shmem}
};


static Size orioledb_memsize(void);
static void orioledb_shmem_request(void);
static void orioledb_shmem_startup(void);
static bool verify_dir_is_empty_or_create(char *dirname, bool *created, bool *found);
static void orioledb_usercache_hook(Datum arg, Oid arg1, Oid arg2, Oid arg3);
static void orioledb_error_cleanup_hook(void);
static void orioledb_get_relation_info_hook(PlannerInfo *root,
											Oid relationObjectId,
											bool inhparent,
											RelOptInfo *rel);

PG_FUNCTION_INFO_V1(orioledb_page_stats);
PG_FUNCTION_INFO_V1(orioledb_version);
PG_FUNCTION_INFO_V1(orioledb_commit_hash);
PG_FUNCTION_INFO_V1(orioledb_ucm_check);
PG_FUNCTION_INFO_V1(orioledb_parallel_debug_start);
PG_FUNCTION_INFO_V1(orioledb_parallel_debug_stop);

#if PG_VERSION_NUM >= 150000
static void
orioledb_rm_desc(StringInfo buf, XLogReaderState *record)
{
	appendStringInfo(buf, "OrioleDB WAL container");
}

static const char *
orioledb_rm_identify(uint8 info)
{
	return "OrioleDB WAL container";
}

static void
o_recovery_shutdown_hook(void)
{
	o_recovery_finish_hook(false);
}

static void
o_recovery_cleanup(void)
{
	o_recovery_finish_hook(true);
}

static RmgrData rmgr =
{
	.rm_name = "OrioleDB resource manager",
	.rm_startup = o_recovery_start_hook,
	.rm_cleanup = o_recovery_cleanup,
	.rm_redo = orioledb_redo,
	.rm_desc = orioledb_rm_desc,
	.rm_identify = orioledb_rm_identify,
	.rm_mask = NULL,
	.rm_decode = NULL
};
#endif

void
_PG_init(void)
{
	Size		main_buffers_count;
	int			i;
	int			min_pool_size;

	if (!process_shared_preload_libraries_in_progress)
		return;

	verify_dir_is_empty_or_create(pstrdup(ORIOLEDB_DATA_DIR), NULL, NULL);
	verify_dir_is_empty_or_create(pstrdup(ORIOLEDB_UNDO_DIR), NULL, NULL);
	verify_dir_is_empty_or_create(psprintf("%s/1", ORIOLEDB_DATA_DIR), NULL, NULL);

	/* See InitializeMaxBackends(), InitProcGlobal() */
	max_procs = MaxConnections + autovacuum_max_workers + 2 +
		max_worker_processes + max_wal_senders + NUM_AUXILIARY_PROCS;

	min_pool_size = Max(PPOOL_MIN_SIZE_BLCKS, max_procs * 4);

	DefineCustomBoolVariable("orioledb.debug_disable_pools_limit",
							 "Disables pools minimal limit for debug.",
							 NULL,
							 &debug_disable_pools_limit,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.main_buffers",
							"Size of orioledb engine shared buffers for main data.",
							NULL,
							&main_buffers_guc,
							Max(8192, min_pool_size),
							debug_disable_pools_limit ? 1 : min_pool_size,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.free_tree_buffers",
							"Size of orioledb engine shared buffers for free extents BTrees.",
							NULL,
							&free_tree_buffers_guc,
							min_pool_size,
							debug_disable_pools_limit ? 1 : min_pool_size,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.catalog_buffers",
							"Size of orioledb engine shared buffers for free extents BTrees.",
							NULL,
							&catalog_buffers_guc,
							min_pool_size,
							debug_disable_pools_limit ? 1 : min_pool_size,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.undo_buffers",
							"Size of orioledb engine undo log buffers.",
							NULL,
							&undo_buffers_guc,
							Max(128, 4 * max_procs),
							4 * max_procs,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.xid_buffers",
							"Size of orioledb engine xid buffers.",
							NULL,
							&xid_buffers_guc,
							128,
							128,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("orioledb.enable_stopevents",
							 "Enable stop events.",
							 NULL,
							 &enable_stopevents,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("orioledb.trace_stopevents",
							 "Trace all the stop events to the system log.",
							 NULL,
							 &trace_stopevents,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("orioledb.remove_old_checkpoint_files",
							 "Remove temporary *.tmp and *.map files after checkpoint.",
							 NULL,
							 &remove_old_checkpoint_files,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("orioledb.debug_disable_bgwriter",
							 "Disables bgwriter for debug.",
							 NULL,
							 &debug_disable_bgwriter,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.recovery_queue_size",
							"Size of orioledb recovery queue per worker.",
							NULL,
							(int *) &recovery_queue_size_guc,
							1024,
							512,
							MAX_KILOBYTES,
							PGC_POSTMASTER,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.recovery_pool_size",
							"Sets the number of recovery workers.",
							NULL,
							&recovery_pool_size_guc,
							3,
							1,
							128,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.recovery_idx_pool_size",
							"Sets the number of recovery index build workers.",
							NULL,
							&recovery_idx_pool_size_guc,
#if PG_VERSION_NUM >= 140000
							3,
							1,
							128,
#else
							0,
							0,
							0,
#endif
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	/*
	 * This variable added because we need values less than minimum value of
	 * checkpoint_timeout(30s) for tests.
	 */
	DefineCustomIntVariable("orioledb.debug_checkpoint_timeout",
							"Sets the maximum time between automatic WAL checkpoints.",
							NULL,
							&CheckPointTimeout,
							CheckPointTimeout,
							1,
							86400,
							PGC_POSTMASTER,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	/*
	 * How much time orioledb checkpoint can take relative to PostgreSQL
	 * checkpoint.
	 */
	DefineCustomRealVariable("orioledb.checkpoint_completion_ratio",
							 "ratio of orioledb checkpoint to postgres checkpoint.",
							 NULL,
							 &o_checkpoint_completion_ratio,
							 0.5,
							 0.0,
							 1.0,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.bgwriter_num_workers",
							"Number of background writers.",
							NULL,
							&bgwriter_num_workers,
							1,
							1,
							MAX_BACKENDS,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.max_io_concurrency",
							"Number of maximum concurrent IO operations.",
							NULL,
							&max_io_concurrency,
							0,
							0,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("orioledb.use_mmap",
							 "Store data in the mmap'ed file.",
							 NULL,
							 &use_mmap,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("orioledb.device_filename",
							   "Data file for mmap.",
							   NULL,
							   &device_filename,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomIntVariable("orioledb.device_length",
							"Size of mmap.",
							NULL,
							&device_length_guc,
							0,
							0,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.default_compress",
							"Default compression level.",
							NULL,
							&default_compress,
							-1,
							-1,
							o_compress_max_lvl(),
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.default_primary_compress",
							"Default compression level of primary index.",
							NULL,
							&default_primary_compress,
							-1,
							-1,
							o_compress_max_lvl(),
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.default_toast_compress",
							"Default compression level of TOAST.",
							NULL,
							&default_toast_compress,
							-1,
							-1,
							o_compress_max_lvl(),
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

#if PG_VERSION_NUM >= 140000
	DefineCustomBoolVariable("orioledb.table_description_compress",
							 "Display compression column in "
							 "orioledb_table_description",
							 NULL,
							 &orioledb_table_description_compress,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
#endif

	DefineCustomBoolVariable("orioledb.s3_mode",
							 "The OrioleDB function mode on top of S3 storage",
							 NULL,
							 &orioledb_s3_mode,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.s3_queue_size",
							"The size of queue for S3 tasks",
							NULL,
							&s3_queue_size_guc,
							1024,
							128,
							MAX_KILOBYTES,
							PGC_POSTMASTER,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.s3_num_workers",
							"The number of workers to make S3 requests",
							NULL,
							&s3_num_workers,
							3,
							1,
							MAX_BACKENDS,
							PGC_POSTMASTER,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("orioledb.s3_host",
							   "S3 host",
							   NULL,
							   &s3_host,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("orioledb.s3_region",
							   "S3 region",
							   NULL,
							   &s3_region,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("orioledb.s3_accesskey",
							   "S3 access key",
							   NULL,
							   &s3_accesskey,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("orioledb.s3_secretkey",
							   "S3 secret key",
							   NULL,
							   &s3_secretkey,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	main_buffers_count = ((Size) main_buffers_guc * (Size) BLCKSZ) / ORIOLEDB_BLCKSZ;
	free_tree_buffers_count = ((Size) free_tree_buffers_guc * (Size) BLCKSZ) / ORIOLEDB_BLCKSZ;
	catalog_buffers_count = ((Size) catalog_buffers_guc * (Size) BLCKSZ) / ORIOLEDB_BLCKSZ;

	main_buffers_offset = free_tree_buffers_count + catalog_buffers_count;

	orioledb_buffers_count = main_buffers_count + free_tree_buffers_count + catalog_buffers_count;
	orioledb_buffers_size = mul_size(orioledb_buffers_count, ORIOLEDB_BLCKSZ);

	undo_circular_buffer_size = ((Size) undo_buffers_guc * BLCKSZ) / 2;
	undo_circular_buffer_size /= ORIOLEDB_BLCKSZ;
	undo_buffers_count = (uint32) undo_circular_buffer_size;
	undo_circular_buffer_size *= ORIOLEDB_BLCKSZ;

	xid_circular_buffer_size = ((Size) xid_buffers_guc * BLCKSZ) / 2;
	xid_circular_buffer_size /= ORIOLEDB_BLCKSZ;
	xid_buffers_count = (uint32) xid_circular_buffer_size;
	xid_circular_buffer_size *= ORIOLEDB_BLCKSZ / sizeof(pg_atomic_uint64);

	recovery_queue_size_guc *= 1024;

	page_descs_size = CACHELINEALIGN(mul_size(orioledb_buffers_count, sizeof(OrioleDBPageDesc)));

	EmitWarningsOnPlaceholders("pg_stat_statements");

	memset(page_pools, 0, OPagePoolTypesCount * sizeof(OPagePool));
	page_pools_size[OPagePoolFreeTree] = ppool_estimate_space(&page_pools[OPagePoolFreeTree],
															  0,
															  free_tree_buffers_count,
															  debug_disable_pools_limit);

	page_pools_size[OPagePoolCatalog] = ppool_estimate_space(&page_pools[OPagePoolCatalog],
															 free_tree_buffers_count,
															 catalog_buffers_count,
															 debug_disable_pools_limit);

	page_pools_size[OPagePoolMain] = ppool_estimate_space(&page_pools[OPagePoolMain],
														  main_buffers_offset,
														  main_buffers_count,
														  debug_disable_pools_limit);

	for (i = 0; i < OPagePoolTypesCount; i++)
		page_pools_size[i] = CACHELINEALIGN(page_pools_size[i]);

#if PG_VERSION_NUM < 150000
	orioledb_shmem_request();
#endif

	if (device_filename)
	{
		device_fd = BasicOpenFile(device_filename, O_RDWR);
		device_length = (Size) device_length_guc * BLCKSZ;
		if (device_fd < 0)
		{
			elog(LOG, "can't open device file %s", device_filename);
		}
		else if (use_mmap)
		{
			mmap_data = mmap(NULL,
							 device_length,
							 PROT_READ | PROT_WRITE,
							 MAP_FILE | MAP_SHARED,
							 device_fd,
							 0);
			if (!mmap_data)
				elog(LOG, "can't map device file %s", device_filename);

		}
		if (device_fd >= 0)
			use_device = true;
		if (!mmap_data)
			use_mmap = false;
	}
	else
	{
		use_mmap = false;
		use_device = false;
	}

	/* Register background writers */
	for (i = 0; i < bgwriter_num_workers; i++)
		register_bgwriter();

	/* Register S3 workers */
	for (i = 0; orioledb_s3_mode && (i < s3_num_workers); i++)
		register_s3worker(i);

	/* Register custom deTOAST function */
	register_o_detoast_func(o_detoast);

	RegisterCustomScanMethods(&o_scan_methods);

	/* Setup the required hooks. */
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = orioledb_shmem_request;
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = orioledb_shmem_startup;
	next_CheckPoint_hook = CheckPoint_hook;
	old_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = orioledb_set_rel_pathlist_hook;
	set_plain_rel_pathlist_hook = orioledb_set_plain_rel_pathlist_hook;
	RegisterXactCallback(undo_xact_callback, NULL);
	RegisterSubXactCallback(undo_subxact_callback, NULL);
	CacheRegisterUsercacheCallback(orioledb_usercache_hook, PointerGetDatum(NULL));
	CheckPoint_hook = o_perform_checkpoint;
	after_checkpoint_cleanup_hook = o_after_checkpoint_cleanup_hook;

#if PG_VERSION_NUM >= 150000
	RegisterCustomRmgr(ORIOLEDB_RMGR_ID, &rmgr);
	RedoShutdownHook = o_recovery_shutdown_hook;
#else
	logicalmsg_redo_hook = o_recovery_logicalmsg_redo_hook;
	RedoStartHook = o_recovery_start_hook;
	RedoFinishHook = o_recovery_finish_hook;
#endif
	snapshot_hook = orioledb_snapshot_hook;
	CustomErrorCleanupHook = orioledb_error_cleanup_hook;
	snapshot_register_hook = undo_snapshot_register_hook;
	snapshot_deregister_hook = undo_snapshot_deregister_hook;
	reset_xmin_hook = orioledb_reset_xmin_hook;
	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = orioledb_get_relation_info_hook;
	xact_redo_hook = o_xact_redo_hook;
	pg_newlocale_from_collation_hook = o_newlocale_from_collation;
	orioledb_setup_ddl_hooks();
	stopevents_make_cxt();
}

void
o_check_init_db_dir(Oid dbOid)
{
	static bool initializedOid = InvalidOid;

	if (initializedOid == dbOid)
		return;

	verify_dir_is_empty_or_create(psprintf("%s/%u",
										   ORIOLEDB_DATA_DIR,
										   dbOid), NULL, NULL);
	initializedOid = dbOid;
}

static Size
o_proc_shmem_needs(void)
{
	return mul_size(max_procs, sizeof(ODBProcData));
}

static void
o_proc_shmem_init(Pointer ptr, bool found)
{
	oProcData = (ODBProcData *) ptr;
	if (!found)
	{
		int			i;

		for (i = 0; i < max_procs; i++)
		{
			int			j;

			pg_atomic_init_u64(&oProcData[i].reservedUndoLocation, InvalidUndoLocation);
			pg_atomic_init_u64(&oProcData[i].snapshotRetainUndoLocation, InvalidUndoLocation);
			pg_atomic_init_u64(&oProcData[i].transactionUndoRetainLocation, InvalidUndoLocation);
			pg_atomic_init_u64(&oProcData[i].commitInProgressXlogLocation, OWalInvalidCommitPos);
			pg_atomic_init_u64(&oProcData[i].xmin, InvalidOXid);
			oProcData[i].autonomousNestingLevel = 0;
			memset(&oProcData[i].vxids, 0, sizeof(oProcData[i].vxids));
			LWLockInitialize(&oProcData[i].undoStackLocationsFlushLock,
							 undo_meta->undoStackLocationsFlushLockTrancheId);
			oProcData[i].flushUndoLocations = false;
			for (j = 0; j < PROC_XID_ARRAY_SIZE; j++)
			{
				pg_atomic_init_u64(&oProcData[i].undoStackLocations[j].location, InvalidUndoLocation);
				pg_atomic_init_u64(&oProcData[i].undoStackLocations[j].branchLocation, InvalidUndoLocation);
				pg_atomic_init_u64(&oProcData[i].undoStackLocations[j].subxactLocation, InvalidUndoLocation);
				pg_atomic_init_u64(&oProcData[i].undoStackLocations[j].onCommitLocation, InvalidUndoLocation);
				oProcData[i].vxids[j].oxid = InvalidOXid;
			}
		}
	}
}

static Size
ppools_shmem_needs(void)
{
	Size		size = 0;
	int			i;

	for (i = 0; i < OPagePoolTypesCount; i++)
		size = add_size(size, page_pools_size[i]);
	size = add_size(size, orioledb_buffers_size);
	size = add_size(size, page_descs_size);
	return size;
}

static void
ppools_shmem_init(Pointer ptr, bool found)
{
	int64		i;
	Pointer		page_pools_ptr[OPagePoolTypesCount];

	for (i = 0; i < OPagePoolTypesCount; i++)
	{
		page_pools_ptr[i] = ptr;
		ptr += page_pools_size[i];
	}
	o_shared_buffers = ptr;
	ptr += orioledb_buffers_size;
	page_descs = (OrioleDBPageDesc *) ptr;

	for (i = 0; i < OPagePoolTypesCount; i++)
		ppool_shmem_init(&page_pools[i], page_pools_ptr[i], found);

	if (!found)
	{
		for (i = 0; i < orioledb_buffers_count; i++)
		{
			Page		p = O_GET_IN_MEMORY_PAGE(i);
			OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;

			pg_atomic_init_u32(&(O_PAGE_HEADER(p)->state), 0);
			pg_atomic_init_u32(&(O_PAGE_HEADER(p)->usageCount), UCM_FREE_PAGES_LEVEL);
			header->pageChangeCount = 0;
		}

		for (i = 0; i < page_descs_size / sizeof(OrioleDBPageDesc); i++)
		{
			page_descs[i].fileExtent.len = InvalidFileExtentLen;
			page_descs[i].fileExtent.off = InvalidFileExtentOff;
			page_descs[i].oids.datoid = InvalidOid;
			page_descs[i].oids.reloid = InvalidOid;
			page_descs[i].oids.relnode = InvalidOid;
			page_descs[i].ionum = -1;
			page_descs[i].type = 0;
			page_descs[i].flags = 0;
			proclist_init(&page_descs[i].waitersList);
		}
	}
}

/*
 * Estimate amount of shared memory required by OrioleDB extension.
 */
static Size
orioledb_memsize(void)
{
	Size		size = 0;
	int			i,
				count = sizeof(shmemItems) / sizeof(shmemItems[0]);

	for (i = 0; i < count; i++)
		size = add_size(size, CACHELINEALIGN(shmemItems[i].shmem_size()));

	return size;
}

static void
orioledb_on_shmem_exit(int code, Datum arg)
{
	if (MyProc)
		pg_atomic_write_u64(&oProcData[MyProc->pgprocno].xmin, InvalidOXid);
}

/*
 * Request for shared memory and lwlocks
 */
static void
orioledb_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(orioledb_memsize());
	request_btree_io_lwlocks();
	RequestNamedLWLockTranche("orioledb_unique_locks", max_procs * 4);
}

/*
 * Initialize OrioleDB's shared memory.  Called on database instanse start
 * or restart.
 */
static void
orioledb_shmem_startup(void)
{
	Pointer		ptr;
	bool		found;
	int			i,
				count = sizeof(shmemItems) / sizeof(shmemItems[0]);

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	shared_segment = NULL;

	/*
	 * We must hold AddinShmemInitLock while initilization of our shared
	 * memory.
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	shared_segment = ShmemInitStruct("orioledb_enigne",
									 orioledb_memsize(),
									 &found);
	ptr = shared_segment;

	for (i = 0; i < count; i++)
	{
		shmemItems[i].shmem_init(ptr, found);
		ptr += CACHELINEALIGN(shmemItems[i].shmem_size());
	}

	init_btree_io_lwlocks();
	o_btree_init_unique_lwlocks();

	o_tableam_descr_init();
	o_compress_init();
	o_sys_caches_init();

	before_shmem_exit(orioledb_on_shmem_exit, (Datum) 0);

	LWLockRelease(AddinShmemInitLock);

	shared_segment_initialized = true;

	if (!found)
		elog(LOG, "%s started", ORIOLEDB_VERSION);

	btree_insert_context = AllocSetContextCreate(TopMemoryContext,
												 "orioledb B-tree insert context",
												 ALLOCSET_DEFAULT_SIZES);

	btree_seqscan_context = AllocSetContextCreate(TopTransactionContext,
												  "orioledb B-tree seqential scans context",
												  ALLOCSET_DEFAULT_SIZES);

	if (remove_old_checkpoint_files)
		recovery_cleanup_old_files(checkpoint_state->lastCheckpointNumber,
								   false);
}

uint64
orioledb_device_alloc(struct BTreeDescr *descr, uint32 size)
{
	uint64		result;

	result = pg_atomic_fetch_add_u64(&checkpoint_state->mmapDataLength, size);

	if (result + size > device_length)
		elog(ERROR, "device file overflow");

	return result;
}

void
orioledb_check_shmem(void)
{
	if (!shared_segment_initialized)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("orioledb must be loaded via shared_preload_libraries")));
}

/*
 * Verify that the given directory exists and is empty. If it does not
 * exist, it is created. If it exists but is not empty, an error will
 * be give and the process ended.
 */
static bool
verify_dir_is_empty_or_create(char *dirname, bool *created, bool *found)
{
	const char *errstr;

	switch (pg_check_dir(dirname))
	{
		case 0:

			/*
			 * Does not exist, so create
			 */
			if (pg_mkdir_p(dirname, S_IRWXU) == -1)
			{
				errstr = strerror(errno);
				elog(WARNING, "could not access directory \"%s\": %s",
					 dirname, errstr);
				return false;
			}
			if (created)
				*created = true;
			return true;
		case 1:

			/*
			 * Exists, empty
			 */
			if (found)
				*found = true;
			return true;
		case 2:
		case 3:
		case 4:

			/*
			 * Exists, not empty
			 */
			return false;
		case -1:

			/*
			 * Access problem
			 */
			errstr = strerror(errno);
			elog(WARNING, "could not access directory \"%s\": %s",
				 dirname, errstr);
			return false;
	}
	return false;				/* keep compiler quiet */
}

/*
 * Test to see if a directory exists and is empty or not.
 *
 * Returns:
 *		0 if nonexistent
 *		1 if exists and empty
 *		2 if exists and contains _only_ dot files
 *		3 if exists and contains a mount point
 *		4 if exists and not empty
 *		-1 if trouble accessing directory (errno reflects the error)
 */
int
pg_check_dir(const char *dir)
{
	int			result = 1;
	DIR		   *chkdir;
	struct dirent *file;
	bool		dot_found = false;
	bool		mount_found = false;
	int			readdir_errno;

	chkdir = opendir(dir);
	if (chkdir == NULL)
		return (errno == ENOENT) ? 0 : -1;

	while (errno = 0, (file = readdir(chkdir)) != NULL)
	{
		if (strcmp(".", file->d_name) == 0 ||
			strcmp("..", file->d_name) == 0)
		{
			/* skip this and parent directory */
			continue;
		}
#ifndef WIN32
		/* file starts with "." */
		else if (file->d_name[0] == '.')
		{
			dot_found = true;
		}
		/* lost+found directory */
		else if (strcmp("lost+found", file->d_name) == 0)
		{
			mount_found = true;
		}
#endif
		else
		{
			result = 4;			/* not empty */
			break;
		}
	}

	if (errno)
		result = -1;			/* some kind of I/O error? */

	/* Close chkdir and avoid overwriting the readdir errno on success */
	readdir_errno = errno;
	if (closedir(chkdir))
		result = -1;			/* error executing closedir */
	else
		errno = readdir_errno;

	/* We report on mount point if we find a lost+found directory */
	if (result == 1 && mount_found)
		result = 3;

	/* We report on dot-files if we _only_ find dot files */
	if (result == 1 && dot_found)
		result = 2;

	return result;
}

/*
 * pg_mkdir_p --- create a directory and, if necessary, parent directories
 *
 * This is equivalent to "mkdir -p" except we don't complain if the target
 * directory already exists.
 *
 * We assume the path is in canonical form, i.e., uses / as the separator.
 *
 * omode is the file permissions bits for the target directory.  Note that any
 * parent directories that have to be created get permissions according to the
 * prevailing umask, but with u+wx forced on to ensure we can create there.
 * (We declare omode as int, not mode_t, to minimize dependencies for port.h.)
 *
 * Returns 0 on success, -1 (with errno set) on failure.
 *
 * Note that on failure, the path arg has been modified to show the particular
 * directory level we had problems with.
 */
int
pg_mkdir_p(char *path, int omode)
{
	struct stat sb;
	mode_t		numask,
				oumask;
	int			last,
				retval;
	char	   *p;

	retval = 0;
	p = path;

#ifdef WIN32
	/* skip network and drive specifiers for win32 */
	if (strlen(p) >= 2)
	{
		if (p[0] == '/' && p[1] == '/')
		{
			/* network drive */
			p = strstr(p + 2, "/");
			if (p == NULL)
			{
				errno = EINVAL;
				return -1;
			}
		}
		else if (p[1] == ':' &&
				 ((p[0] >= 'a' && p[0] <= 'z') ||
				  (p[0] >= 'A' && p[0] <= 'Z')))
		{
			/* local drive */
			p += 2;
		}
	}
#endif

	/*
	 * POSIX 1003.2: For each dir operand that does not name an existing
	 * directory, effects equivalent to those caused by the following command
	 * shall occur:
	 *
	 * mkdir -p -m $(umask -S),u+wx $(dirname dir) && mkdir [-m mode] dir
	 *
	 * We change the user's umask and then restore it, instead of doing
	 * chmod's.  Note we assume umask() can't change errno.
	 */
	oumask = umask(0);
	numask = oumask & ~(S_IWUSR | S_IXUSR);
	(void) umask(numask);

	if (p[0] == '/')			/* Skip leading '/'. */
		++p;
	for (last = 0; !last; ++p)
	{
		if (p[0] == '\0')
			last = 1;
		else if (p[0] != '/')
			continue;
		*p = '\0';
		if (!last && p[1] == '\0')
			last = 1;

		if (last)
			(void) umask(oumask);

		/* check for pre-existing directory */
		if (stat(path, &sb) == 0)
		{
			if (!S_ISDIR(sb.st_mode))
			{
				if (last)
					errno = EEXIST;
				else
					errno = ENOTDIR;
				retval = -1;
				break;
			}
		}
		else if (mkdir(path, last ? omode : S_IRWXU | S_IRWXG | S_IRWXO) < 0)
		{
			retval = -1;
			break;
		}
		if (!last)
			*p = '/';
	}

	/* ensure we restored umask */
	(void) umask(oumask);

	return retval;
}

Datum
orioledb_page_stats(PG_FUNCTION_ARGS)
{
	Datum		values[5];
	bool		nulls[5];
	int			i;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	orioledb_check_shmem();

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Build and return the tuple
	 */
	MemSet(nulls, 0, sizeof(nulls));
	for (i = 0; i < OPagePoolTypesCount; i++)
	{
		int64		num_free_pages,
					total_num_pages;

		total_num_pages = (int64) page_pools[i].size;

		if (i == OPagePoolMain)
			values[0] = PointerGetDatum(cstring_to_text("main"));
		else if (i == OPagePoolFreeTree)
			values[0] = PointerGetDatum(cstring_to_text("free_tree"));
		else if (i == OPagePoolCatalog)
			values[0] = PointerGetDatum(cstring_to_text("catalog"));
		num_free_pages = (int64) ppool_free_pages_count(&page_pools[i]);
		values[1] = Int64GetDatum(total_num_pages - num_free_pages);
		values[2] = Int64GetDatum(num_free_pages);
		values[3] = Int64GetDatum((int64) ppool_dirty_pages_count(&page_pools[i]));
		values[4] = Int64GetDatum(total_num_pages);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

Datum
orioledb_ucm_check(PG_FUNCTION_ARGS)
{
	bool		result = true;
	int			i;

	for (i = 0; i < OPagePoolTypesCount && result; i++)
		result = ucm_check_map(&page_pools[i].ucm);

	PG_RETURN_BOOL(result);
}

static void
orioledb_usercache_hook(Datum arg, Oid arg1, Oid arg2, Oid arg3)
{
	o_invalidate_descrs(arg1, arg2, arg3);
}

void
o_invalidate_oids(ORelOids oids)
{
	SharedInvalidationMessage msg;

	Assert(ORelOidsIsValid(oids));

	msg.usr.id = SHAREDINVALUSERCACHE_ID;
	msg.usr.arg1 = oids.datoid;
	msg.usr.arg2 = oids.reloid;
	msg.usr.arg3 = oids.relnode;

	/* check AddCatcacheInvalidationMessage() for an explanation */
	VALGRIND_MAKE_MEM_DEFINED(&msg, sizeof(msg));

	SendSharedInvalidMessages(&msg, 1);
}

Datum
orioledb_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(ORIOLEDB_VERSION));
}

#define COMMIT_HASH_STRING #COMMIT_HASH

#define STRINGIZE2(s) #s
#define STRINGIZE(s) STRINGIZE2(s)

Datum
orioledb_commit_hash(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(STRINGIZE(COMMIT_HASH)));
}

/*
 * Returns a page pool by the type.
 */
OPagePool *
get_ppool(OPagePoolType type)
{
	Assert((int) type < OPagePoolTypesCount);
	return &page_pools[type];
}

/*
 * Returns a page pool for the page number.
 */
OPagePool *
get_ppool_by_blkno(OInMemoryBlkno blkno)
{
	Assert(blkno < orioledb_buffers_count);

	if (blkno >= main_buffers_offset)
		return &page_pools[OPagePoolMain];

	if (blkno < free_tree_buffers_count)
		return &page_pools[OPagePoolFreeTree];

	return &page_pools[OPagePoolCatalog];
}

/*
 * Returns count of all dirty pages (sum of dirty pages for all page pools).
 */
OInMemoryBlkno
get_dirty_pages_count_sum(void)
{
	OInMemoryBlkno result = 0;
	int			i;

	for (i = 0; i < OPagePoolTypesCount; i++)
		result += ppool_dirty_pages_count(&page_pools[i]);

	return result;
}

void
jsonb_push_key(JsonbParseState **state, char *key)
{
	JsonbValue	jval;

	jval.type = jbvString;
	jval.val.string.len = strlen(key);
	jval.val.string.val = key;
	(void) pushJsonbValue(state, WJB_KEY, &jval);
}

void
jsonb_push_int8_key(JsonbParseState **state, char *key, int64 value)
{
	JsonbValue	jval;

	jsonb_push_key(state, key);

	jval.type = jbvNumeric;
	jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum(value)));
	(void) pushJsonbValue(state, WJB_VALUE, &jval);

}

void
jsonb_push_null_key(JsonbParseState **state, char *key)
{
	JsonbValue	jval;

	jsonb_push_key(state, key);

	jval.type = jbvNull;
	(void) pushJsonbValue(state, WJB_VALUE, &jval);

}

void
jsonb_push_bool_key(JsonbParseState **state, char *key, bool value)
{
	JsonbValue	jval;

	jsonb_push_key(state, key);

	jval.type = jbvBool;
	jval.val.boolean = value;
	(void) pushJsonbValue(state, WJB_VALUE, &jval);

}

void
jsonb_push_string_key(JsonbParseState **state, const char *key,
					  const char *value)
{
	JsonbValue	jval;

	jsonb_push_key(state, (char *) key);

	jval.type = jbvString;
	jval.val.string.len = strlen(value);
	jval.val.string.val = (char *) value;
	(void) pushJsonbValue(state, WJB_VALUE, &jval);
}

static void
orioledb_error_cleanup_hook(void)
{
	GET_CUR_PROCDATA()->waitingForOxid = false;
	release_all_page_locks();
	ppool_release_all_pages();
	release_undo_size(UndoReserveTxn);
	btree_mark_incomplete_splits();
	unset_skip_ucm();
	btree_io_error_cleanup();
	o_reset_syscache_hooks();
	if (drop_index_list)
	{
		list_free_deep(drop_index_list);
		drop_index_list = NIL;
	}
}

static void
orioledb_get_relation_info_hook(PlannerInfo *root,
								Oid relationObjectId,
								bool inhparent,
								RelOptInfo *rel)
{
	Relation	relation;

	relation = table_open(relationObjectId, NoLock);

	if (is_orioledb_rel(relation))
	{
		/* Evade parallel scan of OrioleDB's tables */
		rel->rel_parallel_workers = RelationGetParallelWorkers(relation, -1);
		if (rel->rel_parallel_workers > 0)
			elog(WARNING, "Rel parallel workers = %d", rel->rel_parallel_workers);

		if (relation->rd_rel->relhasindex)
		{
			int			i;
			ListCell   *lc;
			OTableDescr *descr = relation_get_descr(relation);
			OIndexDescr *primary;

			if (descr)
			{
				primary = GET_PRIMARY(descr);

				foreach(lc, rel->indexlist)
				{
					IndexOptInfo *info = lfirst_node(IndexOptInfo, lc);
					bool		hasbitmap;

					/*
					 * TODO: Remove when parallel index scan will be
					 * implemented
					 */
					info->amcanparallel = false;
					hasbitmap = info->indexoid != primary->oids.reloid &&
						primary->nFields <= 1;
					for (i = 0;
						 hasbitmap && i < primary->nFields; i++)
					{
						Oid			typeoid = primary->fields[i].inputtype;
						bool		valid = typeoid == INT4OID ||
							typeoid == INT8OID ||
							typeoid == TIDOID;

						hasbitmap = hasbitmap && valid;
					}
					info->amhasgetbitmap = hasbitmap;
				}
			}
		}
	}

	table_close(relation, NoLock);
}

Datum
orioledb_parallel_debug_start(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 160000
	debug_parallel_query = DEBUG_PARALLEL_ON;
#else
	force_parallel_mode = FORCE_PARALLEL_ON;
#endif
	PG_RETURN_VOID();
}

Datum
orioledb_parallel_debug_stop(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 160000
	debug_parallel_query = DEBUG_PARALLEL_OFF;
#else
	force_parallel_mode = FORCE_PARALLEL_OFF;
#endif
	PG_RETURN_VOID();
}

static bool
o_newlocale_from_collation()
{
	return shared_segment_initialized;
}
