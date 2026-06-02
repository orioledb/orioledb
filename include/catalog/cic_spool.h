/*-------------------------------------------------------------------------
 *
 * cic_spool.h
 *		Per-backend spool files for CREATE INDEX CONCURRENTLY (CIC).
 *
 * During CIC, every backend that touches a row in the table being indexed
 * appends a capture entry to its own spool file under
 *
 *	   orioledb_data/cic_<datoid>_<reloid>_<builderOxid>/spool_<backendid>.bin
 *
 * Entries record the originating undo position so the CIC builder can do
 * a k-way merge across all per-backend files and replay them in a single
 * globally-ordered stream into the new index.
 *
 * Spool files are NOT crash-recoverable.  If CIC does not finish the
 * whole directory is dropped at startup along with the half-built index
 * record.  See spec section 2 (architectural principles).
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/cic_spool.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __CIC_SPOOL_H__
#define __CIC_SPOOL_H__

#include "orioledb.h"

#include "btree/btree.h"
#include "transam/undo.h"

/*
 * Op type stored in each spool entry.  REVERSE_* are produced when a
 * concurrent writer's transaction aborts and apply_undo_stack walks back
 * one of its DML records; they cancel the forward op when merged.
 */
typedef enum
{
	CIC_OP_INSERT = 0,
	CIC_OP_DELETE = 1,
	CIC_OP_REVERSE_INSERT = 2,	/* abort path: cancels a prior INSERT */
	CIC_OP_REVERSE_DELETE = 3	/* abort path: cancels a prior DELETE */
} CICOpType;

/*
 * On-disk header of one spool entry.  Followed by `keyLength` bytes of
 * the key tuple's data, then `tupleLength` bytes of the leaf tuple's
 * data.  `keyFormatFlags` / `tupleFormatFlags` carry the OTuple flags so
 * the bytes can be reconstituted at read time.
 */
typedef struct
{
	UndoLocation undoPosition;	/* position in UndoLogRegular that produced
								 * this entry */
	uint32		fileSeq;		/* per-file monotonic counter, ties on equal
								 * undoPosition */
	uint8		opType;			/* CICOpType */
	uint8		keyFormatFlags;
	uint8		tupleFormatFlags;
	uint8		pad0;
	uint16		keyLength;		/* OffsetNumber-range */
	uint16		tupleLength;
} CICSpoolEntryHdr;

/*
 * In-memory representation of a fully-read entry.  `keyData` and
 * `tupleData` are owned by the reader and live until cic_spool_read_next
 * is called again or cic_spool_close is invoked.
 */
typedef struct
{
	CICSpoolEntryHdr hdr;
	char	   *keyData;		/* keyLength bytes (or NULL when keyLength ==
								 * 0) */
	char	   *tupleData;		/* tupleLength bytes (or NULL when tupleLength
								 * == 0) */
} CICSpoolEntry;

/*
 * Build the per-CIC spool directory path under ORIOLEDB_DATA_DIR.  The
 * caller-owned buffer is filled in and `*size` returned by the helper.
 */
extern void cic_spool_build_dirpath(char *buf, Size buflen,
									ORelOids tableOids, OXid builderOxid);

/*
 * Ensure the CIC spool directory exists.  Idempotent.
 */
extern void cic_spool_ensure_dir(ORelOids tableOids, OXid builderOxid);

/*
 * Append one entry on behalf of MyBackendId.  Opens (and caches) the
 * per-backend file on first call; subsequent calls write to the same
 * fd.  Caller must already hold an OXid-level guarantee that the CIC
 * directory exists (cic_spool_ensure_dir is called once by the CIC
 * driver at phase-1 start).
 */
extern void cic_spool_append(ORelOids tableOids, OXid builderOxid,
							 UndoLocation undoPos, CICOpType opType,
							 OTuple key, uint16 keyLen,
							 OTuple tuple, uint16 tupleLen);

/*
 * Push a UndoLogRegular item that, when applied (transaction or
 * subxact abort), will emit a compensating REVERSE_* entry to the
 * spool.  Forward-op bytes are duplicated into the undo item so the
 * abort callback can re-emit them verbatim with the op flipped.
 * Subxact correctness comes for free: orioledb's apply_undo_stack
 * already segments per subxact.
 */
extern void cic_spool_track_for_abort(ORelOids tableOids, OXid builderOxid,
									  UndoLocation undoPos, CICOpType opType,
									  OTuple key, uint16 keyLen,
									  OTuple tuple, uint16 tupleLen);

/*
 * Public undo callback (registered in undoItemTypeDescrs).  Declared
 * here so undo.c can grab the function pointer at module init.
 */
struct UndoStackItem;
extern void cic_capture_undo_callback(UndoLogType undoType,
									  UndoLocation location,
									  struct UndoStackItem *baseItem,
									  OXid oxid,
									  OUndoCallbackStage stage,
									  bool changeCountsValid);

/*
 * Close this backend's cached fd, if any.  Called at end of CIC
 * (phase 5) and on backend exit.
 */
extern void cic_spool_close_local(void);

/*
 * Open a k-way merge reader over every spool file in the CIC directory.
 * cic_spool_read_next yields entries in (undoPosition, fileSeq) order.
 * Returns NULL when there are no spool files (nothing to replay).
 */
typedef struct CICSpoolReader CICSpoolReader;

extern CICSpoolReader *cic_spool_open_reader(ORelOids tableOids, OXid builderOxid);
extern bool cic_spool_read_next(CICSpoolReader *reader, CICSpoolEntry *out);
extern void cic_spool_close_reader(CICSpoolReader *reader);

/*
 * Drop the entire CIC directory and all files inside.  Called on phase 5
 * (success), CIC abort, and startup cleanup for orphan dirs.
 */
extern void cic_spool_drop_dir(ORelOids tableOids, OXid builderOxid);

#endif							/* __CIC_SPOOL_H__ */
