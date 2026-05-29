/*-------------------------------------------------------------------------
 *
 * safe_recovery.c
 *		Per-OXID buffering recovery for OrioleDB.
 *
 * The legacy redo path applies every modify-class WAL record the moment
 * it is parsed, before the transaction's fate (COMMIT/ROLLBACK/joint
 * commit) is known.  A crash that flushed Oriole containers but never
 * reached the COMMIT leaves a crash loop on subsequent recoveries.
 *
 * In safe_recovery mode we instead:
 *   - hash buffer raw container payloads per OXID,
 *   - on COMMIT, replay the buffered containers via the unchanged
 *     replay_container() path,
 *   - on JOINT_COMMIT, mark the buffer pending until heap recovery fires
 *     o_xact_redo_hook for the matching xid,
 *   - on ROLLBACK or end-of-WAL with no commit, discard and LOG the
 *     rejected operations.
 *
 * One WAL container belongs to one OXID by construction (wal.c emits
 * WAL_REC_XID at most once per container; reset_local_wal_buffer() clears
 * the per-backend state on every flush_local_wal()).  Therefore the
 * buffering "splitter" is trivial: walk the container once to extract
 * oxid + fate, then append the whole container payload to that oxid's
 * buffer.
 *
 * Copyright (c) 2026, Oriole DB Inc.
 * Copyright (c) 2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/safe_recovery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "recovery/internal.h"
#include "recovery/recovery.h"
#include "recovery/safe_recovery.h"
#include "recovery/wal.h"
#include "recovery/wal_reader.h"

#include "access/xlog.h"
#include "btree/io.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#define SAFE_RECOVERY_DIR	ORIOLEDB_DATA_DIR "/safe_recovery"
#define SAFE_RECOVERY_FILE_FMT	SAFE_RECOVERY_DIR "/oxid_" UINT64_FORMAT ".buf"

/*
 * One flushed container belonging to a single OXID.  The payload is the
 * complete WAL record data delivered to safe_recovery_replay_container():
 * version + flags + container-flag payloads + record stream.  Stored raw
 * so the apply path can call replay_container() directly without any
 * re-encoding.
 */
typedef struct BufferedContainer
{
	dlist_node	node;
	XLogRecPtr	xlogRecPtr;
	XLogRecPtr	xlogRecEndPtr;
	Size		len;
	/* payload bytes follow inline */
} BufferedContainer;

#define BUFFERED_CONTAINER_PAYLOAD(c) ((Pointer) (((BufferedContainer *) (c)) + 1))

typedef enum
{
	SAFE_FATE_NONE,
	SAFE_FATE_COMMIT,
	SAFE_FATE_ROLLBACK,
	SAFE_FATE_JOINT_COMMIT
} SafeFate;

/*
 * Per-OXID buffer entry.
 *
 * Each entry owns a MemoryContext whose reset frees the entire slice
 * chain in one shot.
 */
typedef struct SafeXactBuf
{
	OXid		oxid;			/* hash key */

	MemoryContext mcxt;
	dlist_head	containers;		/* in-memory list (empty when spilled) */
	int			container_count;
	Size		total_bytes;

	XLogRecPtr	first_lsn;
	XLogRecPtr	last_lsn;

	/*
	 * Set when WAL_REC_JOINT_COMMIT is buffered.  We wait until heap
	 * recovery fires o_xact_redo_hook for joint_heap_xid before applying.
	 */
	bool		joint_commit_pending;
	TransactionId joint_heap_xid;

	dlist_node	pending_node;	/* in safe_pending_joint_list */
	bool		in_pending_list;

	/* Spill state.  When spilled == true, containers list is empty. */
	bool		spilled;
	char	   *spill_path;		/* palloc'd in buf->mcxt */
	off_t		spill_offset;
} SafeXactBuf;

/* On-disk header per container. */
typedef struct
{
	XLogRecPtr	xlogRecPtr;
	XLogRecPtr	xlogRecEndPtr;
	Size		len;
} SpillHeader;

static HTAB *safe_xact_hash = NULL;
static MemoryContext safe_recovery_mcxt = NULL;
static Size safe_recovery_total_bytes = 0;

/*
 * Buffers waiting for heap recovery to confirm their joint commit.
 * Walked in safe_recovery_xact_redo().
 */
static dlist_head safe_pending_joint_list;

/* ---- buffering pass: extract oxid + fate from a container ---- */

typedef struct
{
	OXid		oxid;
	TransactionId heap_xid;
	SafeFate	fate;
	int			record_count;
} SafeScanCtx;

static WalParseResult
safe_scan_check_version(const WalReaderState *r)
{
	if (r->container.version > ORIOLEDB_WAL_VERSION)
		return WALPARSE_BAD_VERSION;
	return WALPARSE_OK;
}

static WalParseResult
safe_scan_on_record(WalReaderState *r, WalRecord *rec)
{
	SafeScanCtx *ctx = (SafeScanCtx *) r->ctx;

	ctx->record_count++;

	switch (rec->type)
	{
		case WAL_REC_XID:
			ctx->oxid = rec->oxid;
			break;

		case WAL_REC_COMMIT:
			ctx->fate = SAFE_FATE_COMMIT;
			break;

		case WAL_REC_ROLLBACK:
			ctx->fate = SAFE_FATE_ROLLBACK;
			break;

		case WAL_REC_JOINT_COMMIT:
			ctx->fate = SAFE_FATE_JOINT_COMMIT;
			ctx->heap_xid = rec->u.joint_commit.xid;
			break;

		default:
			break;
	}

	return WALPARSE_OK;
}

/* ---- log-only pass for rejected transactions ---- */

typedef struct
{
	OXid		oxid;
	XLogRecPtr	containerLsn;
	int			emitted;		/* per-record lines emitted */
	int			counted;		/* per-record lines that would have been emitted */
	int			max_emit;		/* rate limit (from GUC) */
} SafeLogCtx;

static WalParseResult
safe_log_on_record(WalReaderState *r, WalRecord *rec)
{
	SafeLogCtx *ctx = (SafeLogCtx *) r->ctx;

	switch (rec->type)
	{
		case WAL_REC_XID:
		case WAL_REC_COMMIT:
		case WAL_REC_ROLLBACK:
		case WAL_REC_JOINT_COMMIT:
		case WAL_REC_RELATION:
		case WAL_REC_RELREPLIDENT:
		case WAL_REC_REPLAY_FEEDBACK:
		case WAL_REC_SWITCH_LOGICAL_XID:
			/* not user-visible mutations; skip */
			break;

		default:
			ctx->counted++;
			if (ctx->emitted < ctx->max_emit)
			{
				ereport(LOG,
						(errmsg("safe_recovery: rejecting oxid=" UINT64_FORMAT
								" lsn=%X/%X op=%s oids=[%u %u %u]",
								ctx->oxid,
								LSN_FORMAT_ARGS(ctx->containerLsn),
								wal_type_name(rec->type),
								rec->oids.datoid,
								rec->oids.reloid,
								rec->oids.relnode)));
				ctx->emitted++;
			}
			break;
	}

	return WALPARSE_OK;
}

/* ---- spill to disk ---- */

static void
safe_spill_write_container(File f, off_t *offset, Pointer payload, Size len,
						   XLogRecPtr xlogRecPtr, XLogRecPtr xlogRecEndPtr,
						   const char *path)
{
	SpillHeader header;

	header.xlogRecPtr = xlogRecPtr;
	header.xlogRecEndPtr = xlogRecEndPtr;
	header.len = len;

	if (OFileWrite(f, (char *) &header, sizeof(header), *offset,
				   WAIT_EVENT_DATA_FILE_WRITE) != sizeof(header))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("safe_recovery: could not write header to \"%s\": %m",
						path)));
	*offset += sizeof(header);

	if (OFileWrite(f, (char *) payload, len, *offset,
				   WAIT_EVENT_DATA_FILE_WRITE) != (int) len)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("safe_recovery: could not write payload to \"%s\": %m",
						path)));
	*offset += len;
}

/* Open or create the spill file for buf in append mode.  Caller closes. */
static File
safe_spill_open(SafeXactBuf *buf, int mode)
{
	File		f;

	Assert(buf->spill_path != NULL);
	f = PathNameOpenFile(buf->spill_path, mode | PG_BINARY);
	if (f < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("safe_recovery: could not open \"%s\": %m",
						buf->spill_path)));
	return f;
}

/*
 * Spill an in-memory buffer to disk: write each container to the spill file
 * then free the in-memory chain.  Total accounting bytes are unchanged
 * (still counted as "owned" by safe_recovery), but they no longer contribute
 * to RSS / palloc'd memory.
 */
static void
safe_buf_spill(SafeXactBuf *buf)
{
	File		f;
	off_t		offset = 0;
	dlist_iter	iter;

	Assert(!buf->spilled);
	if (buf->container_count == 0)
		return;

	if (buf->spill_path == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(buf->mcxt);

		buf->spill_path = psprintf(SAFE_RECOVERY_FILE_FMT, buf->oxid);
		MemoryContextSwitchTo(old);
	}

	f = safe_spill_open(buf, O_WRONLY | O_CREAT | O_TRUNC);
	dlist_foreach(iter, &buf->containers)
	{
		BufferedContainer *c = dlist_container(BufferedContainer, node, iter.cur);

		safe_spill_write_container(f, &offset,
								   BUFFERED_CONTAINER_PAYLOAD(c), c->len,
								   c->xlogRecPtr, c->xlogRecEndPtr,
								   buf->spill_path);
	}
	if (FileSync(f, WAIT_EVENT_DATA_FILE_WRITE) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("safe_recovery: could not fsync \"%s\": %m",
						buf->spill_path)));
	FileClose(f);

	buf->spill_offset = offset;
	buf->spilled = true;

	/* Free in-memory chain by resetting the buffer's MemoryContext children. */
	MemoryContextReset(buf->mcxt);
	/* spill_path was just freed; rebuild it. */
	{
		MemoryContext old = MemoryContextSwitchTo(buf->mcxt);

		buf->spill_path = psprintf(SAFE_RECOVERY_FILE_FMT, buf->oxid);
		MemoryContextSwitchTo(old);
	}
	dlist_init(&buf->containers);
	/* container_count and total_bytes intentionally retained. */

	elog(DEBUG1,
		 "safe_recovery: spilled oxid=" UINT64_FORMAT " (%d containers, "
		 UINT64_FORMAT " bytes) to %s",
		 buf->oxid, buf->container_count, (uint64) buf->total_bytes,
		 buf->spill_path);
}

/*
 * Find the largest in-memory buffer (excluding `keep`) and spill it.
 * Returns the number of bytes freed from memory, or 0 if no candidate.
 */
static Size
safe_spill_largest(SafeXactBuf *keep)
{
	HASH_SEQ_STATUS seq;
	SafeXactBuf *buf;
	SafeXactBuf *victim = NULL;

	hash_seq_init(&seq, safe_xact_hash);
	while ((buf = (SafeXactBuf *) hash_seq_search(&seq)) != NULL)
	{
		if (buf == keep || buf->spilled || buf->container_count == 0)
			continue;
		if (victim == NULL || buf->total_bytes > victim->total_bytes)
			victim = buf;
	}

	if (victim == NULL)
		return 0;

	{
		Size		freed = victim->total_bytes;

		safe_buf_spill(victim);
		return freed;
	}
}

static void
safe_spill_unlink(SafeXactBuf *buf)
{
	if (buf->spill_path == NULL)
		return;
	if (unlink(buf->spill_path) != 0 && errno != ENOENT)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("safe_recovery: could not unlink \"%s\": %m",
						buf->spill_path)));
}

/* ---- iteration over containers (works for in-memory or spilled) ---- */

typedef void (*ContainerVisitor) (void *arg, Pointer payload, Size len,
								  XLogRecPtr xlogRecPtr, XLogRecPtr xlogRecEndPtr);

static void
safe_buf_foreach_container(SafeXactBuf *buf, ContainerVisitor cb, void *arg)
{
	if (!buf->spilled)
	{
		dlist_iter	iter;

		dlist_foreach(iter, &buf->containers)
		{
			BufferedContainer *c = dlist_container(BufferedContainer, node, iter.cur);

			cb(arg, BUFFERED_CONTAINER_PAYLOAD(c), c->len,
			   c->xlogRecPtr, c->xlogRecEndPtr);
		}
	}
	else
	{
		File		f;
		off_t		offset = 0;

		f = safe_spill_open(buf, O_RDONLY);
		while (offset < buf->spill_offset)
		{
			SpillHeader header;
			Pointer		payload;

			if (OFileRead(f, (char *) &header, sizeof(header), offset,
						  WAIT_EVENT_DATA_FILE_READ) != sizeof(header))
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("safe_recovery: could not read header from \"%s\": %m",
								buf->spill_path)));
			offset += sizeof(header);

			payload = palloc(header.len);
			if (OFileRead(f, (char *) payload, header.len, offset,
						  WAIT_EVENT_DATA_FILE_READ) != (int) header.len)
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("safe_recovery: could not read payload from \"%s\": %m",
								buf->spill_path)));
			offset += header.len;

			cb(arg, payload, header.len, header.xlogRecPtr, header.xlogRecEndPtr);
			pfree(payload);
		}
		FileClose(f);
	}
}

/* ---- buffer lifecycle ---- */

static SafeXactBuf *
safe_buf_lookup(OXid oxid, bool create)
{
	SafeXactBuf *buf;
	bool		found;

	if (safe_xact_hash == NULL)
		return NULL;

	buf = (SafeXactBuf *) hash_search(safe_xact_hash, &oxid,
									  create ? HASH_ENTER : HASH_FIND,
									  &found);
	if (!found && create)
	{
		char		ident[64];

		buf->mcxt = AllocSetContextCreate(safe_recovery_mcxt,
										  "safe_recovery oxid",
										  ALLOCSET_DEFAULT_SIZES);
		snprintf(ident, sizeof(ident), UINT64_FORMAT, oxid);
		MemoryContextCopyAndSetIdentifier(buf->mcxt, ident);
		dlist_init(&buf->containers);
		buf->container_count = 0;
		buf->total_bytes = 0;
		buf->first_lsn = InvalidXLogRecPtr;
		buf->last_lsn = InvalidXLogRecPtr;
		buf->joint_commit_pending = false;
		buf->joint_heap_xid = InvalidTransactionId;
		buf->in_pending_list = false;
		buf->spilled = false;
		buf->spill_path = NULL;
		buf->spill_offset = 0;
	}
	return buf;
}

static void
safe_buf_free(SafeXactBuf *buf)
{
	if (buf->in_pending_list)
	{
		dlist_delete(&buf->pending_node);
		buf->in_pending_list = false;
	}

	if (buf->spilled || buf->spill_path != NULL)
		safe_spill_unlink(buf);

	safe_recovery_total_bytes -= buf->total_bytes;
	MemoryContextDelete(buf->mcxt);
	hash_search(safe_xact_hash, &buf->oxid, HASH_REMOVE, NULL);
}

/*
 * Make room for `needed` bytes by spilling the largest in-memory buffers
 * other than `keep` until the limit is satisfied.  Errors out if no more
 * candidates remain and we still exceed the limit.
 */
static void
safe_recovery_ensure_room(SafeXactBuf *keep, Size needed)
{
	Size		limit = (Size) safe_recovery_buf_size_guc * (Size) 1024;

	while (safe_recovery_total_bytes + needed > limit)
	{
		Size		freed = safe_spill_largest(keep);

		if (freed == 0)
			elog(ERROR,
				 "safe_recovery: buffer limit %d KB reached (used " UINT64_FORMAT
				 " bytes, need " UINT64_FORMAT
				 ") and no in-memory buffers left to spill.  Raise "
				 "orioledb.safe_recovery_buf_size.",
				 safe_recovery_buf_size_guc,
				 (uint64) safe_recovery_total_bytes, (uint64) needed);
	}
}

static void
safe_buf_append(SafeXactBuf *buf, Pointer startPtr, Pointer endPtr,
				XLogRecPtr xlogRecPtr, XLogRecPtr xlogRecEndPtr)
{
	Size		len = endPtr - startPtr;

	safe_recovery_ensure_room(buf, len);

	if (buf->spilled)
	{
		File		f = safe_spill_open(buf, O_WRONLY | O_APPEND);

		safe_spill_write_container(f, &buf->spill_offset,
								   startPtr, len, xlogRecPtr, xlogRecEndPtr,
								   buf->spill_path);
		FileClose(f);
	}
	else
	{
		MemoryContext old = MemoryContextSwitchTo(buf->mcxt);
		BufferedContainer *c;

		c = (BufferedContainer *) palloc(sizeof(BufferedContainer) + len);
		c->xlogRecPtr = xlogRecPtr;
		c->xlogRecEndPtr = xlogRecEndPtr;
		c->len = len;
		memcpy(BUFFERED_CONTAINER_PAYLOAD(c), startPtr, len);
		dlist_push_tail(&buf->containers, &c->node);
		MemoryContextSwitchTo(old);
	}

	buf->container_count++;
	buf->total_bytes += len;
	if (XLogRecPtrIsInvalid(buf->first_lsn))
		buf->first_lsn = xlogRecPtr;
	buf->last_lsn = xlogRecEndPtr;
	safe_recovery_total_bytes += len;
}

/* ---- apply / discard ---- */

static void
safe_apply_visitor(void *arg, Pointer payload, Size len,
				   XLogRecPtr xlogRecPtr, XLogRecPtr xlogRecEndPtr)
{
	OXid		oxid = *(OXid *) arg;

	if (!replay_container(payload, payload + len, true,
						  xlogRecPtr, xlogRecEndPtr))
		elog(ERROR,
			 "safe_recovery: replay_container failed for oxid="
			 UINT64_FORMAT " at %X/%X",
			 oxid, LSN_FORMAT_ARGS(xlogRecPtr));
}

static void
safe_buf_apply(SafeXactBuf *buf)
{
	elog(DEBUG1,
		 "safe_recovery: applying oxid=" UINT64_FORMAT
		 " containers=%d bytes=" UINT64_FORMAT " lsn=%X/%X..%X/%X spilled=%d",
		 buf->oxid, buf->container_count, (uint64) buf->total_bytes,
		 LSN_FORMAT_ARGS(buf->first_lsn), LSN_FORMAT_ARGS(buf->last_lsn),
		 buf->spilled);

	{
		OXid		oxid = buf->oxid;

		safe_buf_foreach_container(buf, safe_apply_visitor, &oxid);
	}

	safe_buf_free(buf);
}

static void
safe_discard_visitor(void *arg, Pointer payload, Size len,
					 XLogRecPtr xlogRecPtr, XLogRecPtr xlogRecEndPtr)
{
	SafeLogCtx *logctx = (SafeLogCtx *) arg;
	WalReaderState r = {
		.start = payload,
		.end = payload + len,
		.ptr = payload,
		.container = {0},
		.ctx = logctx,
		.check_version = safe_scan_check_version,
		.on_container = NULL,
		.on_record = safe_log_on_record
	};

	logctx->containerLsn = xlogRecPtr;
	(void) wal_parse_container(&r, false);
}

static void
safe_buf_discard(SafeXactBuf *buf, const char *reason)
{
	SafeLogCtx	logctx = {
		.oxid = buf->oxid,
		.containerLsn = InvalidXLogRecPtr,
		.emitted = 0,
		.counted = 0,
		.max_emit = safe_recovery_log_per_xact_guc
	};

	ereport(LOG,
			(errmsg("safe_recovery: rejecting transaction oxid=" UINT64_FORMAT
					" (%d containers, lsn %X/%X..%X/%X, spilled=%d): %s",
					buf->oxid, buf->container_count,
					LSN_FORMAT_ARGS(buf->first_lsn),
					LSN_FORMAT_ARGS(buf->last_lsn),
					buf->spilled, reason)));

	safe_buf_foreach_container(buf, safe_discard_visitor, &logctx);

	if (logctx.counted > logctx.emitted)
		elog(LOG,
			 "safe_recovery: rejected oxid=" UINT64_FORMAT
			 " total_data_records=%d (first %d logged, %d elided)",
			 buf->oxid, logctx.counted, logctx.emitted,
			 logctx.counted - logctx.emitted);
	else
		elog(LOG,
			 "safe_recovery: rejected oxid=" UINT64_FORMAT
			 " total_data_records=%d",
			 buf->oxid, logctx.counted);

	safe_buf_free(buf);
}

/* ---- public entry points ---- */

/* Remove stale spill files from a previous run. */
static void
safe_recovery_clean_dir(void)
{
	DIR		   *dir;
	struct dirent *de;

	dir = AllocateDir(SAFE_RECOVERY_DIR);
	if (dir == NULL)
	{
		if (errno == ENOENT)
			return;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("safe_recovery: could not open \"%s\": %m",
						SAFE_RECOVERY_DIR)));
		return;
	}

	while ((de = ReadDir(dir, SAFE_RECOVERY_DIR)) != NULL)
	{
		char		path[MAXPGPATH];

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", SAFE_RECOVERY_DIR, de->d_name);
		if (unlink(path) != 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("safe_recovery: could not unlink \"%s\": %m",
							path)));
	}

	FreeDir(dir);
}

void
safe_recovery_startup(void)
{
	HASHCTL		ctl;

	if (safe_xact_hash != NULL)
		return;

	o_verify_dir_exists_or_create(pstrdup(SAFE_RECOVERY_DIR), NULL, NULL);
	safe_recovery_clean_dir();

	safe_recovery_mcxt = AllocSetContextCreate(TopMemoryContext,
											   "safe_recovery",
											   ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OXid);
	ctl.entrysize = sizeof(SafeXactBuf);
	ctl.hcxt = safe_recovery_mcxt;
	safe_xact_hash = hash_create("safe_recovery oxid buffers",
								 256, &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	dlist_init(&safe_pending_joint_list);
	safe_recovery_total_bytes = 0;
}

bool
safe_recovery_replay_container(Pointer startPtr, Pointer endPtr,
							   XLogRecPtr xlogRecPtr, XLogRecPtr xlogRecEndPtr)
{
	SafeScanCtx scan = {
		.oxid = InvalidOXid,
		.heap_xid = InvalidTransactionId,
		.fate = SAFE_FATE_NONE,
		.record_count = 0
	};
	WalReaderState r = {
		.start = startPtr,
		.end = endPtr,
		.ptr = startPtr,
		.container = {0},
		.ctx = &scan,
		.check_version = safe_scan_check_version,
		.on_container = NULL,
		.on_record = safe_scan_on_record
	};
	WalParseResult st;
	SafeXactBuf *buf;

	if (safe_xact_hash == NULL)
		safe_recovery_startup();

	st = wal_parse_container(&r, true);
	if (st != WALPARSE_OK)
		return false;

	if (!OXidIsValid(scan.oxid))
	{
		/*
		 * Container with no WAL_REC_XID.  Shouldn't happen in practice
		 * (every non-empty container starts with XID; see wal.c
		 * add_xid_wal_record_if_needed) but guard defensively.
		 */
		elog(DEBUG2, "safe_recovery: container at %X/%X has no XID record, skipping",
			 LSN_FORMAT_ARGS(xlogRecPtr));
		return true;
	}

	buf = safe_buf_lookup(scan.oxid, true);
	safe_buf_append(buf, startPtr, endPtr, xlogRecPtr, xlogRecEndPtr);

	switch (scan.fate)
	{
		case SAFE_FATE_NONE:
			/* keep buffering */
			break;

		case SAFE_FATE_COMMIT:
			safe_buf_apply(buf);
			break;

		case SAFE_FATE_ROLLBACK:
			safe_buf_discard(buf, "explicit ROLLBACK");
			break;

		case SAFE_FATE_JOINT_COMMIT:
			buf->joint_commit_pending = true;
			buf->joint_heap_xid = scan.heap_xid;
			if (!buf->in_pending_list)
			{
				dlist_push_tail(&safe_pending_joint_list, &buf->pending_node);
				buf->in_pending_list = true;
			}
			break;
	}

	return true;
}

void
safe_recovery_xact_redo(TransactionId xid, XLogRecPtr lsn, bool commit)
{
	dlist_mutable_iter miter;

	if (safe_xact_hash == NULL)
		return;

	dlist_foreach_modify(miter, &safe_pending_joint_list)
	{
		SafeXactBuf *buf = dlist_container(SafeXactBuf, pending_node, miter.cur);

		if (!TransactionIdEquals(buf->joint_heap_xid, xid))
			continue;

		dlist_delete(&buf->pending_node);
		buf->in_pending_list = false;

		if (commit)
			safe_buf_apply(buf);
		else
			safe_buf_discard(buf, "joint heap ROLLBACK");
	}
}

void
safe_recovery_finish(void)
{
	HASH_SEQ_STATUS seq;
	SafeXactBuf *buf;

	if (safe_xact_hash == NULL)
		return;

	/*
	 * Anything still buffered at end-of-WAL never committed.  Treat as
	 * implicit abort and emit LOG output.
	 */
	hash_seq_init(&seq, safe_xact_hash);
	while ((buf = (SafeXactBuf *) hash_seq_search(&seq)) != NULL)
	{
		const char *reason = buf->joint_commit_pending
			? "no heap COMMIT observed for JOINT_COMMIT"
			: "no COMMIT observed";

		hash_seq_term(&seq);
		safe_buf_discard(buf, reason);
		hash_seq_init(&seq, safe_xact_hash);
	}

	MemoryContextDelete(safe_recovery_mcxt);
	safe_recovery_mcxt = NULL;
	safe_xact_hash = NULL;
	safe_recovery_total_bytes = 0;

	safe_recovery_clean_dir();
}
