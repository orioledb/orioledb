/*-------------------------------------------------------------------------
 *
 * cic_spool.c
 *		Per-backend spool files for CREATE INDEX CONCURRENTLY.
 *
 * See include/catalog/cic_spool.h for the high-level model.  This file
 * implements:
 *
 *	 - Path construction and directory creation/destruction.
 *	 - Backend-local file caching: each backend lazily opens its
 *	   spool_<backendid>.bin on first append, holds the FD until
 *	   cic_spool_close_local() or backend exit, then flushes + closes.
 *	 - K-way merge reader using a simple binary heap of stream readers,
 *	   ordered by (undoPosition, fileSeq).
 *
 * Spool data is NOT WAL-logged and NOT crash-recoverable: if CIC does
 * not finish, the half-built index plus the entire CIC directory are
 * discarded at startup.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/cic_spool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"
#include "catalog/cic_spool.h"

#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

/* ---------------------------------------------------------------------------
 * Local writer state.  One per backend.
 * ---------------------------------------------------------------------------
 */
typedef struct
{
	bool		valid;			/* fields below are populated */
	ORelOids	tableOids;		/* identifies which CIC this fd belongs to */
	OXid		builderOxid;	/* completes the CIC identity */
	File		file;			/* PG VFD; -1 when not yet opened */
	uint32		seqCounter;		/* monotonic, embedded into each entry */
	off_t		offset;			/* current end of file */
} CICSpoolWriter;

static CICSpoolWriter local_writer = {.valid = false,.file = -1};

/* ---------------------------------------------------------------------------
 * Reader state.
 * ---------------------------------------------------------------------------
 */
typedef struct
{
	File		file;
	off_t		offset;
	off_t		endOffset;
	bool		atEnd;
	/* lookahead */
	bool		havePeek;
	CICSpoolEntry peek;
} CICSpoolStream;

struct CICSpoolReader
{
	int			nStreams;
	CICSpoolStream *streams;
	/* scratch buffer for the most-recently-returned entry */
	char	   *lastKey;
	Size		lastKeyCap;
	char	   *tupBuf;
	Size		tupBufCap;
};

/* ---------------------------------------------------------------------------
 * Path helpers.
 * ---------------------------------------------------------------------------
 */
void
cic_spool_build_dirpath(char *buf, Size buflen,
						ORelOids tableOids, OXid builderOxid)
{
	int			n;

	n = snprintf(buf, buflen,
				 ORIOLEDB_DATA_DIR "/cic_%u_%u_%lu",
				 tableOids.datoid, tableOids.relnode,
				 (unsigned long) builderOxid);
	if (n < 0 || (Size) n >= buflen)
		elog(ERROR, "cic_spool: path buffer too small (%d/%zu)", n, buflen);
}

static void
build_filepath(char *buf, Size buflen,
			   ORelOids tableOids, OXid builderOxid, int backendId)
{
	int			n;

	n = snprintf(buf, buflen,
				 ORIOLEDB_DATA_DIR "/cic_%u_%u_%lu/spool_%d.bin",
				 tableOids.datoid, tableOids.relnode,
				 (unsigned long) builderOxid, backendId);
	if (n < 0 || (Size) n >= buflen)
		elog(ERROR, "cic_spool: file path buffer too small (%d/%zu)", n, buflen);
}

void
cic_spool_ensure_dir(ORelOids tableOids, OXid builderOxid)
{
	char		dirpath[MAXPGPATH];

	cic_spool_build_dirpath(dirpath, sizeof(dirpath), tableOids, builderOxid);
	o_verify_dir_exists_or_create(dirpath, NULL, NULL);
}

/* ---------------------------------------------------------------------------
 * Append path.
 * ---------------------------------------------------------------------------
 */

static void
ensure_writer_open(ORelOids tableOids, OXid builderOxid)
{
	char		filepath[MAXPGPATH];
	struct stat st;

	if (local_writer.valid &&
		local_writer.builderOxid == builderOxid &&
		ORelOidsIsEqual(local_writer.tableOids, tableOids) &&
		local_writer.file >= 0)
		return;

	/*
	 * Different CIC than the one we previously wrote to (or first call).
	 * Close any stale fd and reset.
	 */
	cic_spool_close_local();

	build_filepath(filepath, sizeof(filepath),
				   tableOids, builderOxid, MyBackendId);

	local_writer.file = PathNameOpenFile(filepath,
										 O_RDWR | O_CREAT | PG_BINARY);
	if (local_writer.file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open CIC spool file \"%s\": %m",
						filepath)));

	if (fstat(FileGetRawDesc(local_writer.file), &st) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat CIC spool file \"%s\": %m",
						filepath)));

	local_writer.valid = true;
	local_writer.tableOids = tableOids;
	local_writer.builderOxid = builderOxid;
	local_writer.offset = st.st_size;
	local_writer.seqCounter = 0;
}

void
cic_spool_append(ORelOids tableOids, OXid builderOxid,
				 UndoLocation undoPos, CICOpType opType,
				 OTuple key, uint16 keyLen,
				 OTuple tuple, uint16 tupleLen)
{
	CICSpoolEntryHdr hdr;
	int			written;
	Size		recLen;

	ensure_writer_open(tableOids, builderOxid);

	/*
	 * Zero the struct first so any compiler-inserted padding bytes are
	 * deterministic when we write the whole sizeof(hdr) block to disk. pwrite
	 * over uninitialised padding makes valgrind complain and would also be a
	 * hazard if the spool file were ever shipped across hosts.
	 */
	memset(&hdr, 0, sizeof(hdr));
	hdr.undoPosition = undoPos;
	hdr.fileSeq = local_writer.seqCounter++;
	hdr.opType = (uint8) opType;
	hdr.keyFormatFlags = O_TUPLE_IS_NULL(key) ? 0 : key.formatFlags;
	hdr.tupleFormatFlags = O_TUPLE_IS_NULL(tuple) ? 0 : tuple.formatFlags;
	hdr.pad0 = 0;
	hdr.keyLength = keyLen;
	hdr.tupleLength = tupleLen;

	recLen = sizeof(hdr) + keyLen + tupleLen;

	written = FileWrite(local_writer.file, (char *) &hdr, sizeof(hdr),
						local_writer.offset, WAIT_EVENT_DATA_FILE_WRITE);
	if (written != sizeof(hdr))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("short CIC spool write (header): %m")));
	local_writer.offset += sizeof(hdr);

	if (keyLen > 0)
	{
		written = FileWrite(local_writer.file, key.data, keyLen,
							local_writer.offset, WAIT_EVENT_DATA_FILE_WRITE);
		if (written != keyLen)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("short CIC spool write (key): %m")));
		local_writer.offset += keyLen;
	}

	if (tupleLen > 0)
	{
		written = FileWrite(local_writer.file, tuple.data, tupleLen,
							local_writer.offset, WAIT_EVENT_DATA_FILE_WRITE);
		if (written != tupleLen)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("short CIC spool write (tuple): %m")));
		local_writer.offset += tupleLen;
	}

	(void) recLen;				/* currently unused; retained for symmetry */
}

void
cic_spool_close_local(void)
{
	if (!local_writer.valid)
		return;

	if (local_writer.file >= 0)
	{
		FileSync(local_writer.file, WAIT_EVENT_DATA_FILE_SYNC);
		FileClose(local_writer.file);
	}

	local_writer.valid = false;
	local_writer.file = -1;
	local_writer.offset = 0;
	local_writer.seqCounter = 0;
	ORelOidsSetInvalid(local_writer.tableOids);
	local_writer.builderOxid = InvalidOXid;
}

/* ---------------------------------------------------------------------------
 * Reader (k-way merge).
 * ---------------------------------------------------------------------------
 */

static bool
stream_read_one(CICSpoolStream *s, CICSpoolReader *reader,
				CICSpoolEntry *out)
{
	int			n;
	Size		needed;
	char	   *keyBuf = NULL;
	char	   *tupBuf = NULL;

	if (s->atEnd)
		return false;
	if (s->offset >= s->endOffset)
	{
		s->atEnd = true;
		return false;
	}

	n = FileRead(s->file, (char *) &out->hdr, sizeof(out->hdr),
				 s->offset, WAIT_EVENT_DATA_FILE_READ);
	if (n != sizeof(out->hdr))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("short CIC spool read (header): got %d of %zu",
						n, sizeof(out->hdr))));
	s->offset += sizeof(out->hdr);

	if (out->hdr.keyLength > 0)
	{
		needed = (Size) out->hdr.keyLength;
		if (reader->lastKeyCap < needed)
		{
			if (reader->lastKey)
				pfree(reader->lastKey);
			reader->lastKey = MemoryContextAlloc(TopMemoryContext, needed);
			reader->lastKeyCap = needed;
		}
		keyBuf = reader->lastKey;
		n = FileRead(s->file, keyBuf, needed,
					 s->offset, WAIT_EVENT_DATA_FILE_READ);
		if ((Size) n != needed)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("short CIC spool read (key): got %d of %zu", n, needed)));
		s->offset += needed;
	}

	if (out->hdr.tupleLength > 0)
	{
		needed = (Size) out->hdr.tupleLength;
		if (reader->tupBufCap < needed)
		{
			if (reader->tupBuf)
				pfree(reader->tupBuf);
			reader->tupBuf = MemoryContextAlloc(TopMemoryContext, needed);
			reader->tupBufCap = needed;
		}
		tupBuf = reader->tupBuf;
		n = FileRead(s->file, tupBuf, needed,
					 s->offset, WAIT_EVENT_DATA_FILE_READ);
		if ((Size) n != needed)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("short CIC spool read (tuple): got %d of %zu", n, needed)));
		s->offset += needed;
	}

	out->keyData = keyBuf;
	out->tupleData = tupBuf;
	return true;
}

static bool
stream_peek(CICSpoolStream *s, CICSpoolReader *reader)
{
	if (s->havePeek)
		return true;
	if (s->atEnd)
		return false;
	if (!stream_read_one(s, reader, &s->peek))
		return false;
	s->havePeek = true;
	return true;
}

CICSpoolReader *
cic_spool_open_reader(ORelOids tableOids, OXid builderOxid)
{
	char		dirpath[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	CICSpoolReader *reader;
	int			cap = 8;
	int			n = 0;
	CICSpoolStream *streams = NULL;

	cic_spool_build_dirpath(dirpath, sizeof(dirpath), tableOids, builderOxid);

	dir = AllocateDir(dirpath);
	if (dir == NULL)
		return NULL;

	streams = palloc(sizeof(CICSpoolStream) * cap);

	while ((de = ReadDir(dir, dirpath)) != NULL)
	{
		char		filepath[MAXPGPATH];
		File		file;
		struct stat st;

		if (strncmp(de->d_name, "spool_", 6) != 0)
			continue;

		snprintf(filepath, sizeof(filepath), "%s/%s", dirpath, de->d_name);

		file = PathNameOpenFile(filepath, O_RDONLY | PG_BINARY);
		if (file < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open CIC spool \"%s\": %m", filepath)));

		if (fstat(FileGetRawDesc(file), &st) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat CIC spool \"%s\": %m", filepath)));

		if (n >= cap)
		{
			cap *= 2;
			streams = repalloc(streams, sizeof(CICSpoolStream) * cap);
		}
		streams[n].file = file;
		streams[n].offset = 0;
		streams[n].endOffset = st.st_size;
		streams[n].atEnd = (st.st_size == 0);
		streams[n].havePeek = false;
		memset(&streams[n].peek, 0, sizeof(CICSpoolEntry));
		n++;
	}
	FreeDir(dir);

	if (n == 0)
	{
		pfree(streams);
		return NULL;
	}

	reader = palloc0(sizeof(*reader));
	reader->nStreams = n;
	reader->streams = streams;
	return reader;
}

bool
cic_spool_read_next(CICSpoolReader *reader, CICSpoolEntry *out)
{
	int			i;
	int			bestIdx = -1;

	if (reader == NULL)
		return false;

	for (i = 0; i < reader->nStreams; i++)
	{
		CICSpoolStream *s = &reader->streams[i];

		if (!stream_peek(s, reader))
			continue;
		if (bestIdx < 0)
			bestIdx = i;
		else
		{
			CICSpoolStream *best = &reader->streams[bestIdx];

			if (s->peek.hdr.undoPosition < best->peek.hdr.undoPosition)
				bestIdx = i;
			else if (s->peek.hdr.undoPosition == best->peek.hdr.undoPosition)
			{
				if (s->peek.hdr.fileSeq < best->peek.hdr.fileSeq)
					bestIdx = i;
				else if (s->peek.hdr.fileSeq == best->peek.hdr.fileSeq && i < bestIdx)
					bestIdx = i;
			}
		}
	}

	if (bestIdx < 0)
		return false;

	/*
	 * Materialise the chosen stream's peek into `out`.  Because peek's
	 * keyData/tupleData point at reader->lastKey / reader->tupBuf which are
	 * shared, the caller must consume the entry before the next call (we
	 * re-read into those buffers next iteration).
	 */
	*out = reader->streams[bestIdx].peek;
	reader->streams[bestIdx].havePeek = false;
	return true;
}

void
cic_spool_close_reader(CICSpoolReader *reader)
{
	int			i;

	if (reader == NULL)
		return;
	for (i = 0; i < reader->nStreams; i++)
	{
		if (reader->streams[i].file >= 0)
			FileClose(reader->streams[i].file);
	}
	if (reader->lastKey)
		pfree(reader->lastKey);
	if (reader->tupBuf)
		pfree(reader->tupBuf);
	pfree(reader->streams);
	pfree(reader);
}

/* ---------------------------------------------------------------------------
 * Abort-side REVERSE-op emission via the orioledb undo stack.
 * ---------------------------------------------------------------------------
 *
 * Each user DML that captures a forward op also pushes a small undo
 * item carrying the same payload.  On txn / subxact abort,
 * apply_undo_stack walks the chain and fires cic_capture_undo_callback,
 * which writes a REVERSE_* entry into the spool.  Forward and reverse
 * entries share the same undoPosition, so a merge-sort by
 * (undoPosition, fileSeq) pairs them adjacent and they cancel via the
 * permissive replay callback.
 *
 * If the spool dir is no longer there (e.g. CIC already finished and
 * cleaned up before this stale undo callback runs, or crash-time
 * cleanup at startup pre-empted us), we silently skip.
 */
typedef struct
{
	UndoStackItem header;
	ORelOids	tableOids;
	OXid		builderOxid;
	UndoLocation undoPosition;
	uint8		opType;
	uint8		keyFormatFlags;
	uint8		tupleFormatFlags;
	uint8		pad0;
	uint16		keyLength;
	uint16		tupleLength;
	/* keyLength bytes, then tupleLength bytes, follow */
} CICCaptureUndoStackItem;

void
cic_spool_track_for_abort(ORelOids tableOids, OXid builderOxid,
						  UndoLocation undoPos, CICOpType opType,
						  OTuple key, uint16 keyLen,
						  OTuple tuple, uint16 tupleLen)
{
	UndoLocation location;
	CICCaptureUndoStackItem *item;
	LocationIndex size;
	char	   *payload;

	size = MAXALIGN(sizeof(CICCaptureUndoStackItem) + keyLen + tupleLen);

	item = (CICCaptureUndoStackItem *) get_undo_record_unreserved(UndoLogRegular,
																  &location,
																  size);
	item->header.type = CICCaptureUndoItemType;
	item->header.indexType = 0;
	item->header.itemSize = size;

	item->tableOids = tableOids;
	item->builderOxid = builderOxid;
	item->undoPosition = undoPos;
	item->opType = (uint8) opType;
	item->keyFormatFlags = O_TUPLE_IS_NULL(key) ? 0 : key.formatFlags;
	item->tupleFormatFlags = O_TUPLE_IS_NULL(tuple) ? 0 : tuple.formatFlags;
	item->pad0 = 0;
	item->keyLength = keyLen;
	item->tupleLength = tupleLen;

	payload = (char *) item + sizeof(CICCaptureUndoStackItem);
	if (keyLen > 0)
		memcpy(payload, key.data, keyLen);
	if (tupleLen > 0)
		memcpy(payload + keyLen, tuple.data, tupleLen);

	add_new_undo_stack_item(UndoLogRegular, location);
	release_undo_size(UndoLogRegular);
}

static bool
cic_dir_exists(ORelOids tableOids, OXid builderOxid)
{
	char		dirpath[MAXPGPATH];
	struct stat st;

	cic_spool_build_dirpath(dirpath, sizeof(dirpath), tableOids, builderOxid);
	if (stat(dirpath, &st) != 0)
		return false;
	return S_ISDIR(st.st_mode);
}

void
cic_capture_undo_callback(UndoLogType undoType,
						  UndoLocation location,
						  UndoStackItem *baseItem,
						  OXid oxid,
						  OUndoCallbackStage stage,
						  bool changeCountsValid)
{
	CICCaptureUndoStackItem *item = (CICCaptureUndoStackItem *) baseItem;
	OTuple		key;
	OTuple		tup;
	CICOpType	reverseOp;
	char	   *payload;

	if (stage != OUndoCallbackStageAbort)
		return;
	if (!cic_dir_exists(item->tableOids, item->builderOxid))
	{
		/*
		 * CIC dir is gone (cleaned at startup after a crash mid-CIC, or CIC
		 * already finished and we are a late stragglerundo). Nothing to do.
		 */
		return;
	}

	payload = (char *) item + sizeof(CICCaptureUndoStackItem);

	if (item->keyLength > 0)
	{
		key.data = payload;
		key.formatFlags = item->keyFormatFlags;
	}
	else
		O_TUPLE_SET_NULL(key);

	if (item->tupleLength > 0)
	{
		tup.data = payload + item->keyLength;
		tup.formatFlags = item->tupleFormatFlags;
	}
	else
		O_TUPLE_SET_NULL(tup);

	switch ((CICOpType) item->opType)
	{
		case CIC_OP_INSERT:
			reverseOp = CIC_OP_REVERSE_INSERT;
			break;
		case CIC_OP_DELETE:
			reverseOp = CIC_OP_REVERSE_DELETE;
			break;
		default:
			/* Reverse-of-reverse is not meaningful. */
			return;
	}

	cic_spool_append(item->tableOids, item->builderOxid,
					 item->undoPosition, reverseOp,
					 key, item->keyLength,
					 tup, item->tupleLength);
}

/* ---------------------------------------------------------------------------
 * Cleanup.
 * ---------------------------------------------------------------------------
 */
void
cic_spool_drop_dir(ORelOids tableOids, OXid builderOxid)
{
	char		dirpath[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	cic_spool_build_dirpath(dirpath, sizeof(dirpath), tableOids, builderOxid);

	dir = AllocateDir(dirpath);
	if (dir == NULL)
		return;					/* nothing to do */

	while ((de = ReadDir(dir, dirpath)) != NULL)
	{
		char		filepath[MAXPGPATH];

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(filepath, sizeof(filepath), "%s/%s", dirpath, de->d_name);
		if (unlink(filepath) != 0)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not unlink CIC spool file \"%s\": %m",
							filepath)));
	}
	FreeDir(dir);

	if (rmdir(dirpath) != 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove CIC spool directory \"%s\": %m",
						dirpath)));
}
