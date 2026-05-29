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

#include "lib/ilist.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/*
 * Hard upper bound for buffered bytes in commit 3.  A later commit replaces
 * this with a GUC and spill-to-disk path.
 */
#define SAFE_RECOVERY_BUF_HARD_LIMIT	((Size) 256 * 1024 * 1024)

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
	dlist_head	containers;
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
} SafeXactBuf;

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
	int			emitted;
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
			break;
	}

	return WALPARSE_OK;
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

	safe_recovery_total_bytes -= buf->total_bytes;
	MemoryContextDelete(buf->mcxt);
	hash_search(safe_xact_hash, &buf->oxid, HASH_REMOVE, NULL);
}

static void
safe_buf_append(SafeXactBuf *buf, Pointer startPtr, Pointer endPtr,
				XLogRecPtr xlogRecPtr, XLogRecPtr xlogRecEndPtr)
{
	BufferedContainer *c;
	Size		len = endPtr - startPtr;
	MemoryContext old;

	if (safe_recovery_total_bytes + len > SAFE_RECOVERY_BUF_HARD_LIMIT)
		elog(ERROR,
			 "safe_recovery: buffer hard limit reached (used " UINT64_FORMAT
			 " bytes, adding " UINT64_FORMAT ").  Disable orioledb.safe_recovery "
			 "or rebuild with spill support.",
			 (uint64) safe_recovery_total_bytes, (uint64) len);

	old = MemoryContextSwitchTo(buf->mcxt);
	c = (BufferedContainer *) palloc(sizeof(BufferedContainer) + len);
	c->xlogRecPtr = xlogRecPtr;
	c->xlogRecEndPtr = xlogRecEndPtr;
	c->len = len;
	memcpy(BUFFERED_CONTAINER_PAYLOAD(c), startPtr, len);
	dlist_push_tail(&buf->containers, &c->node);
	MemoryContextSwitchTo(old);

	buf->container_count++;
	buf->total_bytes += len;
	if (XLogRecPtrIsInvalid(buf->first_lsn))
		buf->first_lsn = xlogRecPtr;
	buf->last_lsn = xlogRecEndPtr;
	safe_recovery_total_bytes += len;
}

/* ---- apply / discard ---- */

static void
safe_buf_apply(SafeXactBuf *buf)
{
	dlist_iter	iter;

	elog(DEBUG1,
		 "safe_recovery: applying oxid=" UINT64_FORMAT
		 " containers=%d bytes=" UINT64_FORMAT " lsn=%X/%X..%X/%X",
		 buf->oxid, buf->container_count, (uint64) buf->total_bytes,
		 LSN_FORMAT_ARGS(buf->first_lsn), LSN_FORMAT_ARGS(buf->last_lsn));

	dlist_foreach(iter, &buf->containers)
	{
		BufferedContainer *c = dlist_container(BufferedContainer, node, iter.cur);
		Pointer		payload = BUFFERED_CONTAINER_PAYLOAD(c);

		if (!replay_container(payload, payload + c->len, true,
							  c->xlogRecPtr, c->xlogRecEndPtr))
			elog(ERROR,
				 "safe_recovery: replay_container failed for oxid="
				 UINT64_FORMAT " at %X/%X",
				 buf->oxid, LSN_FORMAT_ARGS(c->xlogRecPtr));
	}

	safe_buf_free(buf);
}

static void
safe_buf_discard(SafeXactBuf *buf, const char *reason)
{
	dlist_iter	iter;
	int			total_emitted = 0;

	ereport(LOG,
			(errmsg("safe_recovery: rejecting transaction oxid=" UINT64_FORMAT
					" (%d records across %d containers, lsn %X/%X..%X/%X): %s",
					buf->oxid, 0, buf->container_count,
					LSN_FORMAT_ARGS(buf->first_lsn),
					LSN_FORMAT_ARGS(buf->last_lsn), reason)));

	dlist_foreach(iter, &buf->containers)
	{
		BufferedContainer *c = dlist_container(BufferedContainer, node, iter.cur);
		Pointer		payload = BUFFERED_CONTAINER_PAYLOAD(c);
		SafeLogCtx	logctx = {.oxid = buf->oxid,.containerLsn = c->xlogRecPtr,.emitted = 0};
		WalReaderState r = {
			.start = payload,
			.end = payload + c->len,
			.ptr = payload,
			.container = {0},
			.ctx = &logctx,
			.check_version = safe_scan_check_version,
			.on_container = NULL,
			.on_record = safe_log_on_record
		};

		(void) wal_parse_container(&r, false);
		total_emitted += logctx.emitted;
	}

	elog(LOG,
		 "safe_recovery: rejected oxid=" UINT64_FORMAT " total_data_records=%d",
		 buf->oxid, total_emitted);

	safe_buf_free(buf);
}

/* ---- public entry points ---- */

void
safe_recovery_startup(void)
{
	HASHCTL		ctl;

	if (safe_xact_hash != NULL)
		return;

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
}
